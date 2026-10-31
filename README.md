# filecopy

单文件 C11 文件传输 CLI 工具：**发送端 / 接收端合一**，仅依赖 C11 标准库、系统线程/互斥量（pthread 或 Win32）与系统 socket（Winsock / POSIX），无第三方库。

- 发送端把文件/目录共享出去，接收端拉取并并发下载。
- 大文件自动切分（最多 64 片），默认 8 个下载线程并发拉取。
- 断点续传：分片进度保存在 `.filecopy.state`，中断后重跑自动跳过已完成分片。
- 未下载完成的文件以 `.filecopy` 后缀保存，完成后原子重命名去掉后缀。
- 所有连接先做 apikey 握手，失败立即断开；收发全程带超时；Ctrl+C 保存进度。

## 编译

### GCC（MinGW-w64 / Linux）

```bash
# Windows（MinGW-w64 / MSYS2，在 main.c 所在目录执行）
gcc -O2 -pthread -std=c11 -o filecopy.exe main.c -lws2_32

# Linux（glibc）
gcc -O2 -pthread -std=c11 -o filecopy main.c
```

> 老版本 glibc（< 2.17）若提示 `clock_gettime` 未定义，追加 `-lrt`。

### Visual Studio 2026

直接用 VS2026 打开 `filecopy.sln`（需安装“使用 C++ 的桌面开发”工作负载）：

- 空 C++ 控制台项目 `filecopy.vcxproj`，`main.c` 按 **CompileAsC**（按 C 编译），C 语言标准 `/std:c11`；
- 字符集 **MultiByte**，预处理器定义 `_CRT_SECURE_NO_WARNINGS`；
- 链接 `ws2_32.lib`，输出文件名为 `filecopy.exe`（默认生成在 `bin\<Platform>\<Configuration>\filecopy.exe`）；
- 提供 Debug/Release × Win32/x64 四种配置，命令行构建：

```bat
msbuild filecopy.sln /p:Configuration=Release /p:Platform=x64
```

> 项目按 Visual Studio 2026（版本 18）的 v145 工具集配置；若使用其他版本的 Visual Studio，可在“项目属性 → 常规 → 平台工具集”改为已安装的版本，无需改代码。
> MSVC 不提供 `pthread` 和 `<stdatomic.h>`，`main.c` 在 `_MSC_VER` 下自动切换为 Win32 `_beginthreadex` + `SRWLOCK` 的线程/互斥量封装，业务逻辑不变。

## 用法

```text
发送端（先启动）：
  filecopy --send --port <P> --apikey <KEY> <文件或目录> [更多文件/目录 ...]

接收端：
  filecopy --receive --ip <IP> --port <P> --apikey <KEY> [-c <下载目录>] [-t <线程数>]
```

| 参数 | 说明 |
| --- | --- |
| `--send` | 发送端模式，共享后面的文件/目录 |
| `--receive` | 接收端模式 |
| `--port <P>` | TCP 端口（双端一致） |
| `--apikey <KEY>` | 握手密钥，长度 1..1024 |
| `--ip <IP>` | 发送端地址（接收端使用，支持主机名） |
| `-c, --dir <DIR>` | 下载目录，默认当前目录 |
| `-t, --threads <N>` | 并发下载线程数，默认 8，最大 64 |

示例：

```bash
# 机器 A：共享 photos/ 目录、archive.zip 和 notes.txt
filecopy --send --port 9000 --apikey s3cret photos archive.zip notes.txt

# 机器 B：下载到 ./downloads，使用 16 个线程
filecopy --receive --ip 192.168.1.10 --port 9000 --apikey s3cret -c downloads -t 16
```

发送端日志会打印接入的接收端 IP：

```text
[send] incoming connection from 192.168.1.20:52431
[send] client 192.168.1.20:52431 authenticated
```

接收端先打印完整文件列表（目录已递归展开、保留相对路径），再逐文件下载：

```text
[recv] remote files (3):
  [  0] photos/2024/a.jpg                                              2.10 MiB
  [  1] archive.zip                                                   120.56 MiB
  [  2] notes.txt                                                      4.20 KiB
[recv] total 122.66 MiB
```

## 行为细节

**相对路径**：目录参数会递归展开，共享名 = 参数 basename + 子路径（如 `photos/a/b.jpg`）；单个文件参数取 basename。符号链接 / 重解析点会被跳过。

**分片规则**（收发两端共用同一算法）：

- `size <= 1 MiB`：1 片；
- 否则 `nchunks = ceil(size / 1 MiB)`，并封顶 64 片；
- 第 i 片偏移 `i * ceil(size/nchunks)`，长度取 `min(chunk, size-start)`，最后一片收尾。

**断点续传**：

- 未完成的文件保存为 `<文件名>.filecopy`；分片完成位图保存在下载目录的 `.filecopy.state`（文本格式：`FCSTATE1`，每行 `16位十六进制位图 <十进制大小> <相对路径>`）。
- 重跑接收端会先读状态文件：已完成分片跳过，未完成分片并发下载；状态文件中的过期条目自动清理。
- 文件全部完成后：`rename` 去掉 `.filecopy` 后缀、删除该文件的状态记录；所有文件完成后删除状态文件。
- 若目标正式文件已存在且大小一致，直接跳过。
- Ctrl+C 时保存当前分片进度并退出（`_Exit` / `TerminateProcess`，不再等待阻塞中的网络线程）。

**线程模型**：

- 发送端：每个 TCP 连接一个 detached 服务线程，连接内顺序处理 `LIST` / `GET` 请求。
- 接收端：控制连接拉列表；每个下载线程使用**独立的 TCP 连接**并发拉取分片，因此多线程是真正的网络并发。
- MSVC 构建时线程使用 Win32 `_beginthreadex`，互斥量使用 `SRWLOCK`；MinGW / Linux 使用 pthread（`main.c` 内封装了同名接口）。

## 协议

13 字节定长头 + payload，所有整数使用大端（`uint64` 长度字段）：

```text
 0        4        5                             13
 +--------+--------+-----------------------------+
 | "FCP1" | command | payload length (u64 BE)    |
 +--------+--------+-----------------------------+
```

| 命令 | 方向 | payload |
| --- | --- | --- |
| `HELLO 0x01` | 客户端 → 服务端 | apikey 明文 |
| `OK 0x02` | 服务端 → 客户端 | 空 |
| `ERR 0x03` | 服务端 → 客户端 | UTF-8 错误文本 |
| `LIST 0x04` | 客户端 → 服务端 | 空 |
| `LIST_DATA 0x05` | 服务端 → 客户端 | `u32 N` + N × (`u32 路径长度` + 路径 + `u64 文件大小`) |
| `GET 0x06` | 客户端 → 服务端 | `u32 file_id` + `u32 chunk_idx` |
| `DATA 0x07` | 服务端 → 客户端 | 分片原始字节（流式发送，长度即头中 length） |

收发均设置了 30 秒 socket 超时（connect 为 10 秒 + `select` 超时），对端断开、超时、协议头不合法都会关闭连接并做错误处理；接收端对单个分片最多重试 8 次（自动重连并重新握手）。

## 局限与注意事项

- 仅 IPv4（`--ip` 支持域名解析）。
- apikey 为明文传输，仅用于简单握手校验，不提供加密；请仅在可信网络使用，或自行套一层 SSH/VPN 隧道。
- 发送端在启动时固化共享列表，之后新增/删除文件需重启发送端。
- 接收端按文件顺序处理，同一文件内多分片并发；多个接收端可同时连接（各自独立线程，带宽共享）。
- 断点续传以分片为最小单位（不提供分片内偏移续传）。
- 若发送端文件在传输过程中被修改/截断，接收端该分片会重试失败并中止该文件；重新运行接收端会重新拉取最新大小并重下。

## 本地验证过的场景

- 多文件 + 多级目录 + 空文件 + 大文件多分片的完整传输，逐文件 SHA-256 一致，完成后无 `.filecopy` / `.filecopy.state` 残留。
- 伪造状态文件（标记部分分片已完成）后重跑：已完成分片不再请求、待下载分片补齐，最终文件除人为破坏的已完成分片区域外与原文件逐字节一致。
- 目标文件已存在时跳过；错误 apikey 在握手阶段被拒绝（接收端退出码 1、不落任何文件）。
- 含空格的文件/目录路径。
- 用 Visual Studio 项目文件（MSVC，Release x64 / Win32）编译出的 `filecopy.exe` 完成上述完整传输与断点续传验证，结果与 GCC 构建一致。
