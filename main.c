/*
 * filecopy —— 单文件 C11 文件传输工具（发送端 / 接收端合一）
 * ============================================================================
 *
 * 编译命令：
 *   Linux (glibc)：
 *       gcc -O2 -pthread -std=c11 -o filecopy main.c
 *   Windows (MinGW-w64，推荐 MSYS2)：
 *       gcc -O2 -pthread -std=c11 -o filecopy.exe main.c -lws2_32
 *   Visual Studio 2026：
 *       打开 filecopy.sln（main.c 按 C 编译，MultiByte 字符集，链接 ws2_32.lib）
 *
 * 用法：
 *   发送端：
 *       ./filecopy --send --port <P> --apikey <K> <文件或目录> [更多文件/目录 ...]
 *   接收端：
 *       ./filecopy --receive --ip <IP> --port <P> --apikey <K> [-c <下载目录>] [-t <线程数>]
 *
 * 功能要点：
 *   - 发送端把参数中的文件/目录（递归展开）共享给接收端，并打印接入的接收端 IP。
 *   - 接收端连接发送端、拉取并打印完整文件列表（保留相对路径），然后逐文件下载。
 *   - 每个文件按【固定 8 MiB 块】切分（不足 8 MiB 视为 1 块），由 N 个下载线程
 *     （默认 8，可 -t 配置）并发拉取；每线程一条独立的阻塞数据连接。
 *   - 传输层：阻塞 Socket + SO_SNDBUF/SO_RCVBUF 256KB（connect 前设置）+ TCP_NODELAY，
 *     send/recv 按 64KB 为单位循环直到整块完成，处理部分发送与
 *     WSAEWOULDBLOCK（Windows）/ EAGAIN（POSIX）。
 *   - 断点续传：分片完成情况写入本地状态文件 <下载目录>/.filecopy.state，
 *     未完成的文件以 "<文件名>.filecopy" 保存；重新运行会跳过已完成分片，
 *     全部完成后原子重命名为正式文件并删除状态记录。
 *   - 所有收发均带 30 秒超时；Ctrl+C（SIGINT）时保存进度后退出。
 *   - 所有连接先做 apikey 握手，校验失败立即断开。
 *   - 环境变量 FILECOPY_TRACE=1 打开逐包 send/recv 耗时日志（排查慢点用）。
 *
 * 协议（13 字节定长头 + payload，长度字段为 uint64 大端）：
 *   +--------+---------+-------------------+
 *   | magic  | command | payload length    |
 *   | 4 字节 | 1 字节  | 8 字节大端 (BE)   |
 *   +--------+---------+-------------------+
 *   命令字：
 *     HELLO(0x01)     客户端 -> 服务端，payload 为 apikey 明文
 *     OK(0x02)        服务端握手成功
 *     ERR(0x03)       服务端错误，payload 为 UTF-8 错误文本
 *     LIST(0x04)      客户端请求文件列表，payload 为空
 *     LIST_DATA(0x05) 服务端返回列表：
 *                        u32 文件数 N
 *                        重复 N 次：u32 路径长度 L + L 字节路径 + u64 文件大小
 *                      （以上整数均大端）
 *     GET(0x06)       客户端请求分片：u32 file_id + u32 chunk_idx（大端）
 *     DATA(0x07)      服务端返回分片：payload 长度即分片字节数（流式发送）
 *
 * 线程模型：
 *   - 发送端：主线程 accept，每个连接一个 detached 服务线程，顺序处理该连接上的
 *     请求（LIST / GET）。多个下载线程各自建立独立数据连接并发下载。
 *   - 接收端：主线程负责列表、进度显示与收尾；每个文件创建 N 个下载线程，
 *     通过各自的连接领取分片、断点落盘。
 */

#define _POSIX_C_SOURCE 200809L
#define _FILE_OFFSET_BITS 64

/* MSVC：关闭安全函数告警，并默认链接 Winsock（vcxproj 里也会显式链接 ws2_32.lib） */
#ifdef _MSC_VER
#  ifndef _CRT_SECURE_NO_WARNINGS
#    define _CRT_SECURE_NO_WARNINGS
#  endif
#  pragma comment(lib, "ws2_32.lib")
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <stddef.h>

#ifndef SIZE_MAX
#  define SIZE_MAX ((size_t)-1)
#endif

#ifndef _MSC_VER
#  include <pthread.h>   /* MinGW / Linux 使用 pthread 线程库 */
#endif

#ifdef _WIN32
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <windows.h>
#  include <process.h>   /* _beginthreadex：MSVC 线程封装使用 */
#  include <direct.h>
#  include <io.h>
#else
#  include <unistd.h>
#  include <dirent.h>
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <arpa/inet.h>
#  include <netdb.h>
#endif

/* ======================== 常量 / 全局状态 ======================== */

#define FC_NAME            "filecopy"
#define PROTO_MAGIC        "FCP1"
#define HDR_LEN            13      /* 4 魔数 + 1 命令 + 8 大端长度 */
#define DEFAULT_THREADS    4       /* 并发数据连接数（越大窗口竞争越严重） */
#define MAX_THREADS        64
#define CHUNK_BYTES        (8ull * 1024ull * 1024ull)  /* 固定分片大小 8 MiB */
#define MAX_CHUNKS         65536  /* 分片索引上限保护（8 MiB * 65536 = 512 GiB） */
#define NET_BUF_BYTES      (256 * 1024)  /* SO_SNDBUF / SO_RCVBUF */
#define NET_IO_UNIT        (64 * 1024)   /* 单次 send/recv 的块大小 */
#define IO_TIMEOUT_SEC     30      /* 收发超时（秒） */
#define CONNECT_TIMEOUT_SEC 10     /* connect 超时（秒） */
#define MAX_APIKEY_LEN     1024
#define MAX_LIST_BYTES     (256u * 1024u * 1024u)  /* 列表 payload 上限 */
#define MAX_FILES          262144
#define MAX_REL_PATH       4096
#define MAX_PATH_BUF       8192
#define MAX_CHUNK_ATTEMPTS 8       /* 单个分片最多重试次数 */
#define MAX_CONNECT_TRIES  10      /* 初次连接服务端最多尝试次数 */
#define QUEUE_DEPTH        16      /* 接收->写盘队列深度（每块 CHUNK_BYTES=8MiB，共 128MiB） */

/* 命令字 */
enum {
    CMD_HELLO     = 0x01,
    CMD_OK        = 0x02,
    CMD_ERR       = 0x03,
    CMD_LIST      = 0x04,
    CMD_LIST_DATA = 0x05,
    CMD_GET       = 0x06,
    CMD_DATA      = 0x07
};

enum { MODE_NONE = 0, MODE_SEND = 1, MODE_RECV = 2 };

/* SIGINT 标志：主循环与下载线程都检查它 */
static volatile sig_atomic_t g_stop = 0;

static void on_sigint(int sig)
{
    (void)sig;
    g_stop = 1;
}

/* ======================== 通用小工具 ======================== */

static uint64_t now_ms(void)
{
#ifdef _WIN32
    return (uint64_t)GetTickCount64();
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)(ts.tv_nsec / 1000000L);
#endif
}

static void sleep_ms(int ms)
{
#ifdef _WIN32
    Sleep((DWORD)ms);
#else
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
#endif
}

/* 立即终止进程：Ctrl+C 已把分片进度写盘后使用，不等后台线程 join。
 * Windows 上直接 TerminateProcess，避免在 CRT 清理线程状态时死锁。 */
static void hard_exit(int code)
{
#ifdef _WIN32
    fflush(stdout);
    TerminateProcess(GetCurrentProcess(), (UINT)code);
#else
    _Exit(code);
#endif
}

static char *xstrdup(const char *s)
{
    size_t n = strlen(s) + 1;
    char *p = (char *)malloc(n);
    if (p) memcpy(p, s, n);
    return p;
}

/* 把 "1.23 MiB" 之类的可读大小写入 buf 并返回 buf */
static const char *human_size(uint64_t n, char *buf, size_t bufsz)
{
    static const char *units[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
    double v = (double)n;
    int i = 0;
    while (v >= 1024.0 && i < 5) {
        v /= 1024.0;
        i++;
    }
    if (i == 0)
        snprintf(buf, bufsz, "%.0f %s", v, units[i]);
    else
        snprintf(buf, bufsz, "%.2f %s", v, units[i]);
    return buf;
}

/* 统一替换反斜杠、去掉末尾分隔符（便于比较 / 做相对路径） */
static void normalize_path(char *p)
{
    for (char *q = p; *q; q++)
        if (*q == '\\') *q = '/';
    size_t n = strlen(p);
    while (n > 1 && p[n - 1] == '/') p[--n] = '\0';
}

static const char *path_basename(const char *p)
{
    const char *s = strrchr(p, '/');
    return s ? s + 1 : p;
}

static char *strrchr_any(char *s, char a, char b)
{
    char *last = NULL;
    for (char *p = s; *p; p++)
        if (*p == a || *p == b) last = p;
    return last;
}

/* 拼接目录与相对路径，带溢出检查 */
static int join_path_buf(char *out, size_t outsz, const char *dir, const char *rel)
{
    size_t ld = strlen(dir);
    int need_sep = (ld > 0 && dir[ld - 1] != '/' && dir[ld - 1] != '\\');
    int n = snprintf(out, outsz, "%s%s%s", dir, need_sep ? "/" : "", rel);
    return (n < 0 || (size_t)n >= outsz) ? -1 : 0;
}

/* 相对路径（传输列表里的名字）拼接：前缀为空时直接用 name，避免出现 "/x" */
static int join_rel(char *out, size_t outsz, const char *prefix, const char *name)
{
    int n = prefix[0] ? snprintf(out, outsz, "%s/%s", prefix, name)
                      : snprintf(out, outsz, "%s", name);
    return (n < 0 || (size_t)n >= outsz) ? -1 : 0;
}

/* 校验远端给出的相对路径是否安全（不能是绝对路径、不能含 .. 或非法字符） */
static int relpath_safe(const char *p)
{
    if (!p || !p[0]) return 0;
    if (p[0] == '/') return 0;
    const char *s = p;
    while (*s) {
        const char *e = strchr(s, '/');
        size_t n = e ? (size_t)(e - s) : strlen(s);
        if (n == 0) return 0;
        if (n == 1 && s[0] == '.') return 0;
        if (n == 2 && s[0] == '.' && s[1] == '.') return 0;
        for (size_t i = 0; i < n; i++) {
            unsigned char c = (unsigned char)s[i];
            if (c < 0x20) return 0;
            if (c == '\\' || c == ':' || c == '?' || c == '*' ||
                c == '"' || c == '<' || c == '>' || c == '|')
                return 0;
        }
        if (!e) break;
        s = e + 1;
    }
    return 1;
}

/* ======================== 大端收发工具 ======================== */

static void put_u32be(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static void put_u64be(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (56 - 8 * i));
}

static uint32_t get_u32be(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static uint64_t get_u64be(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = (v << 8) | p[i];
    return v;
}

/* ======================== socket 封装（Windows / POSIX） ========================
 *
 * 数据连接全部使用【阻塞】套接字：
 *   - 非阻塞 + 每次 select 等待会让每 64KB 数据都多一次内核往返，
 *     在小块发送时会把吞吐压到几十 KB/s（原实现 3MB/分钟的主因之一）。
 *   - 阻塞模式下 send/recv 由内核直接等待窗口，CPU 占用低、吞吐高。
 * 但阻塞套接字仍可能出现“部分发送/部分接收”（受发送缓冲区与接收窗口限制），
 * 因此 send/recv 必须循环到整块完成；同时兼容 WSAEWOULDBLOCK / EAGAIN
 * （套接字被临时设为非阻塞，或设置了 SO_SNDTIMEO 时 Windows 也会返回该错误）。
 */

#ifdef _WIN32
typedef SOCKET sock_t;
#  define SOCK_INVALID INVALID_SOCKET
typedef int fc_socklen_t;   /* MinGW 一般有 socklen_t，MSVC 没有，统一用 int */

/* Windows：判断是否“暂时无法收发，稍后重试” */
static int wsa_would_block(int err) { return err == WSAEWOULDBLOCK; }
/* Windows：send/recv 超时（设置了 SO_SNDTIMEO/SO_RCVTIMEO） */
static int wsa_timed_out(int err) { return err == WSAETIMEDOUT; }
#else
typedef int sock_t;
#  define SOCK_INVALID (-1)
typedef socklen_t fc_socklen_t;

/* POSIX：EAGAIN 与 EWOULDBLOCK 通常是同一个值，两个都判 */
static int wsa_would_block(int err) { return err == EAGAIN || err == EWOULDBLOCK; }
static int wsa_timed_out(int err) { return err == EAGAIN || err == EWOULDBLOCK; }
#endif

/* 取最近一次 socket 错误码（Windows 是 WSA*，POSIX 是 errno） */
static int sock_last_error(void)
{
#ifdef _WIN32
    return WSAGetLastError();
#else
    return errno;
#endif
}

static int sock_valid(sock_t s) { return s != SOCK_INVALID; }

static void sock_close(sock_t s)
{
    if (!sock_valid(s)) return;
#ifdef _WIN32
    closesocket(s);
#else
    close(s);
#endif
}

/* 同时设置收 / 发超时，避免半开连接导致线程永久阻塞 */
static void sock_set_timeouts(sock_t s, int sec)
{
#ifdef _WIN32
    DWORD ms = (DWORD)sec * 1000u;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char *)&ms, (int)sizeof ms);
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (const char *)&ms, (int)sizeof ms);
#else
    struct timeval tv;
    tv.tv_sec = sec;
    tv.tv_usec = 0;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
#endif
}

/* 传输层统计（用于 *_TRACE 调试日志与整块耗时统计） */
typedef struct {
    uint64_t calls;     /* send/recv 调用次数 */
    uint64_t bytes;     /* 实际传输字节数 */
    uint64_t wall_ms;   /* 累计耗时 */
    uint64_t max_ms;    /* 单次最慢调用耗时 */
} io_stat;

/* 连接级调试日志开关：环境变量 FILECOPY_TRACE=1 打开（每秒/每块的日志会很多） */
static int g_trace = 0;

/* 关闭 Nagle：小包场景下 Nagle + 延迟 ACK 会造成 40ms 级的停顿 */
static void sock_no_delay(sock_t s)
{
    int one = 1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const char *)&one, (int)sizeof one);
}

/* 设置收发缓冲区（必须在 connect() 之前设置，否则内核窗口已经协商好） */
static void sock_set_buffers(sock_t s, int bytes)
{
    int sz = bytes;
    setsockopt(s, SOL_SOCKET, SO_SNDBUF, (const char *)&sz, (int)sizeof sz);
    setsockopt(s, SOL_SOCKET, SO_RCVBUF, (const char *)&sz, (int)sizeof sz);
}

/* 切换阻塞 / 非阻塞 */
static int sock_set_nonblock(sock_t s, int on)
{
#ifdef _WIN32
    u_long mode = on ? 1 : 0;
    return ioctlsocket(s, FIONBIO, &mode) == 0 ? 0 : -1;
#else
    int fl = fcntl(s, F_GETFL, 0);
    if (fl < 0) return -1;
    if (on) fl |= O_NONBLOCK;
    else fl &= ~O_NONBLOCK;
    return fcntl(s, F_SETFL, fl) < 0 ? -1 : 0;
#endif
}

/* 数据连接统一初始化：阻塞模式 + 256KB 收发缓冲 + TCP_NODELAY + 超时 */
static void sock_tune_data(sock_t s)
{
    int sz = NET_BUF_BYTES;
    sock_set_nonblock(s, 0);           /* 强制回到阻塞模式 */
    sock_no_delay(s);
    setsockopt(s, SOL_SOCKET, SO_SNDBUF, (const char *)&sz, (int)sizeof sz);
    setsockopt(s, SOL_SOCKET, SO_RCVBUF, (const char *)&sz, (int)sizeof sz);
    sock_set_timeouts(s, IO_TIMEOUT_SEC);
}

/* 控制连接（列表 / 握手）也统一初始化，保持原有行为 */
static void sock_tune(sock_t s)
{
    sock_set_nonblock(s, 0);
    sock_no_delay(s);
    sock_set_timeouts(s, IO_TIMEOUT_SEC);
}

/* 完整发送：循环直到整块发完。
 *
 * - 每次最多发 NET_IO_UNIT(64KB)，避免一次超大 send 让内核拆包浪费；
 * - 部分发送（返回值 < 本次请求）继续循环；
 * - WSAEWOULDBLOCK / EAGAIN：Windows 在设置了 SO_SNDTIMEO 时，缓冲区满会返回
 *   WSAEWOULDBLOCK（Linux 阻塞套接字遇超时返回 EAGAIN），此时 Sleep(1) 后重试；
 * - 其他错误：记录并返回 -1（调用方断开连接）。
 * trace 非空时按 64KB 记录字节数与耗时。
 */
static int send_all_ex(sock_t s, const void *buf, size_t len, io_stat *st, const char *tag)
{
    const char *p = (const char *)buf;
    size_t sent = 0;

    while (sent < len) {
        size_t unit = len - sent;
        if (unit > (size_t)NET_IO_UNIT) unit = (size_t)NET_IO_UNIT;
        if (g_stop) return -1;

        uint64_t t0 = now_ms();
        int n = send(s, p + sent, (int)unit, 0);
        uint64_t t1 = now_ms();

        if (n > 0) {
            sent += (size_t)n;
            if (st) {
                st->calls++;
                st->bytes += (uint64_t)n;
                st->wall_ms += (t1 - t0);
                if (t1 - t0 > st->max_ms) st->max_ms = t1 - t0;
            }
            if (g_trace && tag)
                fprintf(stdout, "[trace] %s send %d B in %llu ms\n", tag, n,
                        (unsigned long long)(t1 - t0));
            continue;
        }
        if (n == 0) {
            fprintf(stderr, "[net] send returned 0, peer gone\n");
            return -1;
        }
        int err = sock_last_error();
        if (wsa_would_block(err) || wsa_timed_out(err)) {
            /* 临时无法发送（窗口满 / 超时）：退让一小会儿再重试 */
            if (g_trace && tag)
                fprintf(stderr, "[trace] %s send would-block after %zu B, retrying\n",
                        tag, sent);
            sleep_ms(1);
            continue;
        }
        fprintf(stderr, "[net] send error %d after %zu/%zu bytes\n", err, sent, len);
        return -1;
    }
    return 0;
}

static int send_all(sock_t s, const void *buf, size_t len)
{
    return send_all_ex(s, buf, len, NULL, NULL);
}

/* 完整接收：循环直到读满 len（协议头声明的长度）。
 *
 * - 每次最多收 NET_IO_UNIT(64KB)；
 * - 返回 0 表示对端正常关闭（连接中断）；
 * - WSAEWOULDBLOCK / EAGAIN：SO_RCVTIMEO 到期，退让后继续等，避免误判为断线；
 * - 其他错误：记录并返回 -1。
 */
static int recv_all_ex(sock_t s, void *buf, size_t len, io_stat *st, const char *tag)
{
    char *p = (char *)buf;
    size_t got = 0;
    int timeouts = 0;

    while (got < len) {
        size_t unit = len - got;
        if (unit > (size_t)NET_IO_UNIT) unit = (size_t)NET_IO_UNIT;
        if (g_stop) return -1;

        uint64_t t0 = now_ms();
        int n = recv(s, p + got, (int)unit, 0);
        uint64_t t1 = now_ms();

        if (n > 0) {
            got += (size_t)n;
            timeouts = 0;
            if (st) {
                st->calls++;
                st->bytes += (uint64_t)n;
                st->wall_ms += (t1 - t0);
                if (t1 - t0 > st->max_ms) st->max_ms = t1 - t0;
            }
            if (g_trace && tag && (t1 - t0) > 50)
                fprintf(stdout, "[trace] %s recv %d B in %llu ms\n", tag, n,
                        (unsigned long long)(t1 - t0));
            continue;
        }
        if (n == 0) {
            fprintf(stderr, "[net] peer closed connection after %zu/%zu bytes\n", got, len);
            return -1;
        }
        int err = sock_last_error();
        if (wsa_would_block(err) || wsa_timed_out(err)) {
            /* 超时重试：连续多次仍无数据才判定失败 */
            if (++timeouts > IO_TIMEOUT_SEC * 4) {
                fprintf(stderr, "[net] recv stalled at %zu/%zu bytes\n", got, len);
                return -1;
            }
            if (g_trace && tag)
                fprintf(stderr, "[trace] %s recv would-block at %zu/%zu B\n", tag, got, len);
            sleep_ms(1);
            continue;
        }
        fprintf(stderr, "[net] recv error %d after %zu/%zu bytes\n", err, got, len);
        return -1;
    }
    return 0;
}

static int recv_all(sock_t s, void *buf, size_t len)
{
    return recv_all_ex(s, buf, len, NULL, NULL);
}

/* connect：先设收发缓冲再做（非阻塞 connect + select 超时），成功后转为阻塞。
 * 说明：SO_SNDBUF/SO_RCVBUF 必须在 connect 之前设置才会参与窗口协商，
 * 否则可能一直使用默认 64KB 窗口，长肥管道（局域网低延迟）下吞吐被限制。 */
static sock_t connect_tcp(const char *host, uint16_t port, int timeout_sec)
{
    struct addrinfo hints, *res = NULL, *ai;
    char portstr[16];
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    snprintf(portstr, sizeof portstr, "%u", (unsigned)port);
    if (getaddrinfo(host, portstr, &hints, &res) != 0) return SOCK_INVALID;

    sock_t s = SOCK_INVALID;
    for (ai = res; ai; ai = ai->ai_next) {
        s = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (!sock_valid(s)) continue;

        /* 关键：connect 之前把缓冲区开到 256KB */
        sock_set_buffers(s, NET_BUF_BYTES);
        sock_no_delay(s);

        sock_set_nonblock(s, 1);
        int need_wait = 0;
        if (connect(s, ai->ai_addr, (int)ai->ai_addrlen) != 0) {
#ifdef _WIN32
            int err = WSAGetLastError();
            if (err == WSAEWOULDBLOCK) need_wait = 1;
#else
            if (errno == EINPROGRESS) need_wait = 1;
#endif
            if (!need_wait) {
                sock_close(s);
                s = SOCK_INVALID;
                continue;
            }
        }
        if (need_wait) {
            fd_set wf, ef;
            struct timeval tv;
            FD_ZERO(&wf);
            FD_SET(s, &wf);
            FD_ZERO(&ef);
            FD_SET(s, &ef);
            tv.tv_sec = timeout_sec;
            tv.tv_usec = 0;
#ifdef _WIN32
            int sel = select(0, NULL, &wf, &ef, &tv);
#else
            int sel = select((int)s + 1, NULL, &wf, &ef, &tv);
#endif
            if (sel <= 0) {
                sock_close(s);
                s = SOCK_INVALID;
                continue;
            }
            int soerr = 0;
            fc_socklen_t slen = sizeof soerr;
            if (getsockopt(s, SOL_SOCKET, SO_ERROR, (char *)&soerr, &slen) != 0 ||
                soerr != 0) {
                sock_close(s);
                s = SOCK_INVALID;
                continue;
            }
        }
        sock_set_nonblock(s, 0);
        sock_tune(s);
        break;
    }
    freeaddrinfo(res);
    return s;
}

/* 带超时等待 + accept：用 select 定期唤醒以便响应 Ctrl+C */
static sock_t accept_with_wait(sock_t ls, struct sockaddr_storage *peer)
{
    fd_set rf;
    struct timeval tv;
    FD_ZERO(&rf);
    FD_SET(ls, &rf);
    tv.tv_sec = 0;
    tv.tv_usec = 200 * 1000;
#ifdef _WIN32
    int rc = select(0, &rf, NULL, NULL, &tv);
#else
    int rc = select((int)ls + 1, &rf, NULL, NULL, &tv);
#endif
    if (rc <= 0) return SOCK_INVALID;
    fc_socklen_t slen = sizeof(*peer);
    sock_t s = accept(ls, (struct sockaddr *)peer, &slen);
    if (sock_valid(s)) {
        /* 数据连接：256KB 收发缓冲 + TCP_NODELAY + 阻塞模式 */
        sock_tune_data(s);
    }
    return s;
}

/* 把对端地址格式化为 "ip:port"（只用 IPv4，避免依赖 inet_ntop/ws2tcpip 版本） */
static void peer_to_string(const struct sockaddr_storage *ss, char *buf, size_t buflen)
{
    char host[64] = "?";
    unsigned port = 0;
    if (ss->ss_family == AF_INET) {
        const struct sockaddr_in *a = (const struct sockaddr_in *)ss;
        uint32_t v = ntohl(a->sin_addr.s_addr);
        snprintf(host, sizeof host, "%u.%u.%u.%u",
                 (unsigned)(v >> 24) & 255u, (unsigned)(v >> 16) & 255u,
                 (unsigned)(v >> 8) & 255u, (unsigned)v & 255u);
        port = (unsigned)ntohs(a->sin_port);
    }
    snprintf(buf, buflen, "%s:%u", host, port);
}

/* ======================== 线程 / 互斥量封装 ========================
 * MinGW / Linux 使用 pthread；MSVC 不提供 pthread.h 和 stdatomic.h，
 * 这里用 Win32 的 _beginthreadex + SRWLOCK 做同名封装，
 * 让 main.c 不改业务代码即可同时被 gcc 和 Visual Studio 编译。 */

typedef void *(*thread_fn)(void *);

#ifdef _MSC_VER
typedef HANDLE thread_t;
typedef SRWLOCK mutex_t;

typedef struct {
    thread_fn fn;
    void *arg;
} thread_boot;

static unsigned __stdcall thread_boot_proc(void *p)
{
    thread_boot *b = (thread_boot *)p;
    thread_fn fn = b->fn;
    void *arg = b->arg;
    free(b);
    fn(arg);
    return 0;
}

static int thread_create(thread_t *t, thread_fn fn, void *arg)
{
    thread_boot *b = (thread_boot *)malloc(sizeof *b);
    if (!b) return -1;
    b->fn = fn;
    b->arg = arg;
    *t = (HANDLE)_beginthreadex(NULL, 0, thread_boot_proc, b, 0, NULL);
    if (*t == NULL) {
        free(b);
        return -1;
    }
    return 0;
}

static void thread_join(thread_t t)
{
    WaitForSingleObject(t, INFINITE);
    CloseHandle(t);
}

static void thread_detach(thread_t t)
{
    CloseHandle(t);
}

static int mutex_init(mutex_t *m) { InitializeSRWLock(m); return 0; }
static void mutex_lock(mutex_t *m) { AcquireSRWLockExclusive(m); }
static void mutex_unlock(mutex_t *m) { ReleaseSRWLockExclusive(m); }
/* 返回 0 表示成功获取（与 pthread_mutex_trylock 的 0/EBUSY 语义对齐） */
static int mutex_trylock(mutex_t *m) { return TryAcquireSRWLockExclusive(m) ? 0 : 1; }
static void mutex_destroy(mutex_t *m) { (void)m; }

/* 条件变量：Win32 CONDITION_VARIABLE + SRWLOCK（队列用） */
typedef CONDITION_VARIABLE cond_t;
static int cond_init(cond_t *c) { InitializeConditionVariable(c); return 0; }
static void cond_destroy(cond_t *c) { (void)c; }
static void cond_signal(cond_t *c) { WakeConditionVariable(c); }
static void cond_broadcast(cond_t *c) { WakeAllConditionVariable(c); }
static void cond_wait(cond_t *c, mutex_t *m)
{
    SleepConditionVariableSRW(c, m, INFINITE, 0);
}
#else
typedef pthread_t thread_t;
typedef pthread_mutex_t mutex_t;
typedef pthread_cond_t cond_t;

static int thread_create(thread_t *t, thread_fn fn, void *arg)
{
    return pthread_create(t, NULL, fn, arg);
}
static void thread_join(thread_t t) { pthread_join(t, NULL); }
static void thread_detach(thread_t t) { pthread_detach(t); }

static int mutex_init(mutex_t *m) { return pthread_mutex_init(m, NULL); }
static void mutex_lock(mutex_t *m) { pthread_mutex_lock(m); }
static void mutex_unlock(mutex_t *m) { pthread_mutex_unlock(m); }
static int mutex_trylock(mutex_t *m) { return pthread_mutex_trylock(m); }
static void mutex_destroy(mutex_t *m) { pthread_mutex_destroy(m); }

static int cond_init(cond_t *c) { return pthread_cond_init(c, NULL); }
static void cond_destroy(cond_t *c) { pthread_cond_destroy(c); }
static void cond_signal(cond_t *c) { pthread_cond_signal(c); }
static void cond_broadcast(cond_t *c) { pthread_cond_broadcast(c); }
static void cond_wait(cond_t *c, mutex_t *m) { pthread_cond_wait(c, m); }
#endif

/* ======================== 协议头收发与握手 ======================== */

static void hdr_encode(uint8_t out[HDR_LEN], uint8_t cmd, uint64_t len)
{
    out[0] = 'F';
    out[1] = 'C';
    out[2] = 'P';
    out[3] = '1';
    out[4] = cmd;
    put_u64be(out + 5, len);
}

static int proto_send_msg(sock_t s, uint8_t cmd, const void *payload, uint64_t len)
{
    uint8_t h[HDR_LEN];
    hdr_encode(h, cmd, len);
    if (send_all(s, h, HDR_LEN) != 0) return -1;
    if (len > 0 && payload) return send_all(s, payload, (size_t)len);
    return 0;
}

/* 读取定长头；max_len 为 0 表示不限制 payload 长度，否则超限视为协议错误 */
static int proto_recv_header(sock_t s, uint8_t *cmd, uint64_t *len, uint64_t max_len)
{
    uint8_t h[HDR_LEN];
    if (recv_all(s, h, HDR_LEN) != 0) return -1;
    if (memcmp(h, PROTO_MAGIC, 4) != 0) return -2;
    *cmd = h[4];
    *len = get_u64be(h + 5);
    if (max_len != 0 && *len > max_len) return -3;
    return 0;
}

/* 接收一段 payload 到堆上（返回的缓冲区不保证结尾有 '\0'） */
static int recv_payload(sock_t s, uint64_t len, uint8_t **out)
{
    if (len > (uint64_t)SIZE_MAX - 1) return -1;
    uint8_t *buf = (uint8_t *)malloc((size_t)(len ? len : 1));
    if (!buf) return -1;
    if (len != 0 && recv_all(s, buf, (size_t)len) != 0) {
        free(buf);
        return -1;
    }
    *out = buf;
    return 0;
}

/* 简单恒定时长比较（避免长度信息通过提前退出泄露；实际 apikey 明文本身不加密） */
static int api_matches(const char *a, const char *b)
{
    size_t la = strlen(a), lb = strlen(b);
    unsigned char diff = (unsigned char)(la ^ lb);
    size_t n = la < lb ? la : lb;
    for (size_t i = 0; i < n; i++)
        diff |= (unsigned char)(a[i] ^ b[i]);
    return diff == 0;
}

/* 服务端握手：等 HELLO + apikey；失败则回 ERR 并断开 */
static int server_handshake(sock_t s, const char *apikey)
{
    uint8_t cmd = 0;
    uint64_t len = 0;
    if (proto_recv_header(s, &cmd, &len, MAX_APIKEY_LEN) != 0) return -1;
    if (cmd != CMD_HELLO) {
        const char *msg = "protocol error: HELLO expected";
        proto_send_msg(s, CMD_ERR, msg, strlen(msg));
        return -1;
    }
    char *key = (char *)malloc((size_t)len + 1);
    if (!key) return -1;
    if (len != 0 && recv_all(s, key, (size_t)len) != 0) {
        free(key);
        return -1;
    }
    key[len] = '\0';
    int ok = api_matches(key, apikey);
    free(key);
    if (!ok) {
        const char *msg = "authentication failed: bad apikey";
        proto_send_msg(s, CMD_ERR, msg, strlen(msg));
        return -1;
    }
    return proto_send_msg(s, CMD_OK, NULL, 0);
}

/* 客户端握手：发送 apikey 并校验服务端应答 */
static int client_handshake(sock_t s, const char *apikey)
{
    if (proto_send_msg(s, CMD_HELLO, apikey, strlen(apikey)) != 0) return -1;

    uint8_t cmd = 0;
    uint64_t len = 0;
    if (proto_recv_header(s, &cmd, &len, 4096) != 0) return -1;

    if (cmd == CMD_OK) {
        if (len != 0) {
            uint8_t *tmp = NULL;
            if (recv_payload(s, len, &tmp) != 0) return -1;
            free(tmp);
        }
        return 0;
    }
    if (cmd == CMD_ERR) {
        if (len != 0 && len < (1u << 20)) {
            char *msg = (char *)malloc((size_t)len + 1);
            if (msg && recv_all(s, msg, (size_t)len) == 0) {
                msg[len] = '\0';
                fprintf(stderr, "[recv] server rejected handshake: %s\n", msg);
            }
            free(msg);
        } else {
            fprintf(stderr, "[recv] server rejected handshake (bad apikey?)\n");
        }
        return -1;
    }
    return -1;
}

/* ======================== 分片划分规则（双端必须一致） ========================
 *
 * 改为【固定块大小】而不是固定块数：
 *   - 每块 CHUNK_BYTES = 8 MiB；不足 8 MiB 的小文件视为 1 块；
 *   - 原来“最多 64 块”会让 198MB 文件出现 3.1MB 以上的大块，
 *     且块数与文件大小耦合（小文件分片过碎、大文件分片过大），
 *     固定块大小让每个请求的数据量可预期，也便于断点位图计算。
 */

static uint32_t chunk_count(uint64_t size)
{
    if (size == 0) return 0;                    /* 空文件：没有分片，直接建空文件 */
    uint64_t n = (size + CHUNK_BYTES - 1) / CHUNK_BYTES;
    if (n == 0) n = 1;
    if (n > (uint64_t)MAX_CHUNKS) n = MAX_CHUNKS;
    return (uint32_t)n;
}

/* 第 idx 块在文件中的起始偏移 */
static uint64_t chunk_offset(uint32_t idx)
{
    return (uint64_t)idx * CHUNK_BYTES;
}

/* 第 idx 块的字节数：除最后一块外都是 CHUNK_BYTES */
static uint64_t chunk_bytes(uint64_t size, uint32_t nchunks, uint32_t idx)
{
    if (nchunks == 0 || idx >= nchunks) return 0;
    uint64_t start = chunk_offset(idx);
    if (start >= size) return 0;
    uint64_t remain = size - start;
    return remain < CHUNK_BYTES ? remain : CHUNK_BYTES;
}

static uint64_t mask_for(uint32_t nchunks)
{
    return (nchunks >= 64) ? UINT64_MAX : (((uint64_t)1 << nchunks) - 1);
}

/* ======================== 文件 / 目录工具 ======================== */

static int mkdir_one(const char *p)
{
#ifdef _WIN32
    if (_mkdir(p) == 0) return 0;
    return (errno == EEXIST) ? 0 : -1;
#else
    if (mkdir(p, 0755) == 0) return 0;
    return (errno == EEXIST) ? 0 : -1;
#endif
}

/* 递归创建目录（mkdir -p），兼容 "/" 与 "\\" 分隔符 */
static int mkdir_p(const char *path)
{
    char tmp[MAX_PATH_BUF];
    size_t len = strlen(path);
    if (len == 0) return 0;
    if (len >= sizeof tmp) return -1;
    memcpy(tmp, path, len + 1);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/' || tmp[i] == '\\') {
            char c = tmp[i];
            tmp[i] = '\0';
            /* 跳过 Windows 盘符 "C:" */
            if (!(strlen(tmp) == 2 && tmp[1] == ':')) mkdir_one(tmp);
            tmp[i] = c;
        }
    }
    return mkdir_one(tmp);
}

static int fs_file_size(const char *path, uint64_t *out)
{
#ifdef _WIN32
    struct __stat64 st;
    if (_stat64(path, &st) != 0) return -1;
    if ((st.st_mode & _S_IFMT) != _S_IFREG) return -1;
    *out = (uint64_t)st.st_size;
#else
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    if (!S_ISREG(st.st_mode)) return -1;
    *out = (uint64_t)st.st_size;
#endif
    return 0;
}

static int fs_is_dir(const char *path)
{
#ifdef _WIN32
    struct __stat64 st;
    return _stat64(path, &st) == 0 && (st.st_mode & _S_IFMT) == _S_IFDIR;
#else
    struct stat st;
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
#endif
}

/* 原子替换重命名：POSIX 用 rename，Windows 用 MoveFileEx(REPLACE_EXISTING) */
static int replace_rename(const char *from, const char *to)
{
#ifdef _WIN32
    if (MoveFileExA(from, to, MOVEFILE_REPLACE_EXISTING)) return 0;
    return -1;
#else
    return rename(from, to);
#endif
}

/* 磁盘文件 64 位读取定位 */
static int file_seek64(FILE *f, uint64_t off)
{
#ifdef _WIN32
    return _fseeki64(f, (__int64)off, SEEK_SET) == 0 ? 0 : -1;
#else
    return fseeko(f, (off_t)off, SEEK_SET) == 0 ? 0 : -1;
#endif
}

/* 以读写方式打开（接收端部分文件）；truncate_it 为真时清空内容 */
static int open_file_rw_fd(const char *path, int truncate_it)
{
#ifdef _WIN32
    int flags = _O_RDWR | _O_CREAT | _O_BINARY;
    if (truncate_it) flags |= _O_TRUNC;
    return _open(path, flags, _S_IREAD | _S_IWRITE);
#else
    int flags = O_RDWR | O_CREAT;
    if (truncate_it) flags |= O_TRUNC;
    return open(path, flags, 0644);
#endif
}

static void close_file_fd(int fd)
{
#ifdef _WIN32
    _close(fd);
#else
    close(fd);
#endif
}

/* 在指定偏移写入完整数据（保留给需要定位写的场景，例如旧版合并逻辑）。
 * 当前接收端改为“每分片独立临时文件 + 顺序合并”，不再调用本函数，
 * 因此用可移植的“未使用”标记避免 -Wunused-function 告警。 */
#if defined(__GNUC__) && !defined(_MSC_VER)
__attribute__((unused))
#endif
static int write_at(int fd, const void *buf, size_t len, uint64_t off)
{
    const char *p = (const char *)buf;
    size_t done = 0;
#ifdef _WIN32
    if (_lseeki64(fd, (__int64)off, SEEK_SET) != (__int64)off) return -1;
    while (done < len) {
        size_t n = len - done;
        if (n > (1u << 20)) n = 1u << 20;
        int w = _write(fd, p + done, (unsigned)n);
        if (w <= 0) return -1;
        done += (size_t)w;
    }
#else
    if (lseek(fd, (off_t)off, SEEK_SET) == (off_t)-1) return -1;
    while (done < len) {
        size_t n = len - done;
        if (n > (1u << 20)) n = 1u << 20;
        ssize_t w = write(fd, p + done, n);
        if (w <= 0) return -1;
        done += (size_t)w;
    }
#endif
    return 0;
}

/* ======================== 发送端共享文件列表 ======================== */

typedef struct {
    char *path;     /* 共享给接收端的相对路径（'/' 分隔） */
    char *fs_path;  /* 发送端本地真实路径，用于打开读取 */
    uint64_t size;
} file_entry;

typedef struct {
    file_entry *items;
    size_t count;
    size_t cap;
    uint64_t total_bytes;
} file_list;

static int list_add(file_list *l, const char *path, const char *fs_path, uint64_t size)
{
    if (l->count >= MAX_FILES) return -1;
    if (l->count == l->cap) {
        size_t ncap = l->cap ? l->cap * 2 : 64;
        file_entry *ni = (file_entry *)realloc(l->items, ncap * sizeof *ni);
        if (!ni) return -1;
        l->items = ni;
        l->cap = ncap;
    }
    char *p = xstrdup(path);
    char *f = xstrdup(fs_path);
    if (!p || !f) {
        free(p);
        free(f);
        return -1;
    }
    l->items[l->count].path = p;
    l->items[l->count].fs_path = f;
    l->items[l->count].size = size;
    l->count++;
    l->total_bytes += size;
    return 0;
}

static void free_file_list(file_list *l)
{
    for (size_t i = 0; i < l->count; i++) {
        free(l->items[i].path);
        free(l->items[i].fs_path);
    }
    free(l->items);
    memset(l, 0, sizeof *l);
}

/* 递归遍历目录，把每个普通文件加入列表；跳过符号链接 / 重解析点，避免成环 */
static void list_dir_recursive(file_list *l, const char *fsdir, const char *relprefix)
{
    char fspath[MAX_PATH_BUF];
    char relpath[MAX_REL_PATH + 1];

#ifndef _WIN32
    DIR *d = opendir(fsdir);
    if (!d) {
        fprintf(stderr, "[send] cannot open directory %s: %s\n", fsdir, strerror(errno));
        return;
    }
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (g_stop) break;
        if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")) continue;
        if (snprintf(fspath, sizeof fspath, "%s/%s", fsdir, de->d_name) >= (int)sizeof fspath) continue;
        if (join_rel(relpath, sizeof relpath, relprefix, de->d_name) != 0) continue;

        struct stat st;
        if (lstat(fspath, &st) != 0) continue;
        if (S_ISLNK(st.st_mode)) continue;
        if (S_ISDIR(st.st_mode)) {
            list_dir_recursive(l, fspath, relpath);
        } else if (S_ISREG(st.st_mode)) {
            if (list_add(l, relpath, fspath, (uint64_t)st.st_size) != 0)
                fprintf(stderr, "[send] too many files or out of memory\n");
        }
    }
    closedir(d);
#else
    char pattern[MAX_PATH_BUF];
    if (snprintf(pattern, sizeof pattern, "%s/*", fsdir) >= (int)sizeof pattern) return;
    WIN32_FIND_DATAA fd;
    HANDLE h = FindFirstFileA(pattern, &fd);
    if (h == INVALID_HANDLE_VALUE) return;
    do {
        if (g_stop) break;
        if (!strcmp(fd.cFileName, ".") || !strcmp(fd.cFileName, "..")) continue;
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) continue;
        if (snprintf(fspath, sizeof fspath, "%s/%s", fsdir, fd.cFileName) >= (int)sizeof fspath) continue;
        if (join_rel(relpath, sizeof relpath, relprefix, fd.cFileName) != 0) continue;

        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            list_dir_recursive(l, fspath, relpath);
        } else {
            uint64_t sz = ((uint64_t)fd.nFileSizeHigh << 32) | (uint64_t)fd.nFileSizeLow;
            if (list_add(l, relpath, fspath, sz) != 0)
                fprintf(stderr, "[send] too many files or out of memory\n");
        }
    } while (FindNextFileA(h, &fd));
    FindClose(h);
#endif
}

/* 根据命令行参数收集共享文件 */
static int build_share(file_list *l, char *const *paths, int npaths)
{
    for (int i = 0; i < npaths; i++) {
        char *p = xstrdup(paths[i]);
        if (!p) return -1;
        normalize_path(p);
        if (!p[0]) {
            free(p);
            fprintf(stderr, "[send] empty path\n");
            return -1;
        }
        if (fs_is_dir(p)) {
            const char *base = path_basename(p);
            if (!base[0] || !strcmp(base, ".") || !strcmp(base, "..")) base = "";
            list_dir_recursive(l, p, base);
        } else {
            uint64_t sz = 0;
            if (fs_file_size(p, &sz) != 0) {
                fprintf(stderr, "[send] not a regular file or not found: %s\n", p);
                free(p);
                return -1;
            }
            const char *base = path_basename(p);
            if (!base[0] || !strcmp(base, ".") || !strcmp(base, "..")) base = "file";
            if (list_add(l, base, p, sz) != 0) {
                fprintf(stderr, "[send] too many files or out of memory\n");
                free(p);
                return -1;
            }
        }
        free(p);
    }
    return 0;
}

/* 序列化文件列表（协议里的 LIST_DATA payload） */
static int build_list_payload(const file_list *l, uint8_t **out, uint64_t *outlen)
{
    uint64_t total = 4;
    for (size_t i = 0; i < l->count; i++) {
        size_t pl = strlen(l->items[i].path);
        if (pl > UINT32_MAX) return -1;
        total += 4 + (uint64_t)pl + 8;
        if (total > MAX_LIST_BYTES) return -1;
    }
    uint8_t *buf = (uint8_t *)malloc((size_t)total);
    if (!buf) return -1;
    uint8_t *w = buf;
    put_u32be(w, (uint32_t)l->count);
    w += 4;
    for (size_t i = 0; i < l->count; i++) {
        size_t pl = strlen(l->items[i].path);
        put_u32be(w, (uint32_t)pl);
        w += 4;
        memcpy(w, l->items[i].path, pl);
        w += pl;
        put_u64be(w, l->items[i].size);
        w += 8;
    }
    *out = buf;
    *outlen = total;
    return 0;
}

/* 解析文件列表 payload（接收端） */
static int parse_list_payload(const uint8_t *buf, uint64_t len, file_list *out)
{
    if (len < 4) return -1;
    uint32_t n = get_u32be(buf);
    if (n > MAX_FILES) return -1;
    uint64_t off = 4;
    for (uint32_t i = 0; i < n; i++) {
        if (off + 4 > len) return -1;
        uint32_t pl = get_u32be(buf + off);
        off += 4;
        if (pl > MAX_REL_PATH) return -1;
        if (off + pl + 8 > len) return -1;
        char path[MAX_REL_PATH + 1];
        memcpy(path, buf + off, pl);
        path[pl] = '\0';
        off += pl;
        uint64_t sz = get_u64be(buf + off);
        off += 8;
        if (!relpath_safe(path)) return -1;
        if (list_add(out, path, "", sz) != 0) return -1;
    }
    return 0;
}

static int share_has_path(const file_list *l, const char *path)
{
    for (size_t i = 0; i < l->count; i++)
        if (!strcmp(l->items[i].path, path)) return 1;
    return 0;
}

/* ======================== 断点续传状态文件 ======================== */
/*
 * 格式（文本，路径为相对路径，允许空格）：
 *   FCSTATE1
 *   <16 位十六进制位图> <十进制文件大小> <相对路径>
 * 每个未完成文件一行；每完成一个分片更新位图并落盘。
 */

typedef struct {
    char *path;
    uint64_t size;
    uint64_t bitmap;
} state_entry;

typedef struct {
    state_entry *items;
    size_t count;
    size_t cap;
} state_store;

static state_entry *state_find(state_store *st, const char *path)
{
    for (size_t i = 0; i < st->count; i++)
        if (!strcmp(st->items[i].path, path)) return &st->items[i];
    return NULL;
}

static int state_set(state_store *st, const char *path, uint64_t size, uint64_t bitmap)
{
    state_entry *e = state_find(st, path);
    if (e) {
        e->size = size;
        e->bitmap = bitmap;
        return 0;
    }
    if (st->count == st->cap) {
        size_t ncap = st->cap ? st->cap * 2 : 16;
        state_entry *ni = (state_entry *)realloc(st->items, ncap * sizeof *ni);
        if (!ni) return -1;
        st->items = ni;
        st->cap = ncap;
    }
    char *p = xstrdup(path);
    if (!p) return -1;
    st->items[st->count].path = p;
    st->items[st->count].size = size;
    st->items[st->count].bitmap = bitmap;
    st->count++;
    return 0;
}

static void state_remove_at(state_store *st, size_t idx)
{
    if (idx >= st->count) return;
    free(st->items[idx].path);
    if (idx + 1 < st->count)
        memmove(&st->items[idx], &st->items[idx + 1],
                (st->count - idx - 1) * sizeof *st->items);
    st->count--;
}

static void state_remove(state_store *st, const char *path)
{
    for (size_t i = 0; i < st->count; i++) {
        if (!strcmp(st->items[i].path, path)) {
            state_remove_at(st, i);
            return;
        }
    }
}

static void state_free(state_store *st)
{
    for (size_t i = 0; i < st->count; i++) free(st->items[i].path);
    free(st->items);
    memset(st, 0, sizeof *st);
}

/* 原子写入：先写 .tmp 再 rename 覆盖 */
static int state_save(const state_store *st, const char *file_path)
{
    char tmp[MAX_PATH_BUF + 8];
    if (snprintf(tmp, sizeof tmp, "%s.tmp", file_path) >= (int)sizeof tmp) return -1;

    FILE *f = fopen(tmp, "wb");
    if (!f) return -1;
    fputs("FCSTATE1\n", f);
    for (size_t i = 0; i < st->count; i++) {
        fprintf(f, "%016llx %llu %s\n",
                (unsigned long long)st->items[i].bitmap,
                (unsigned long long)st->items[i].size,
                st->items[i].path);
    }
    fflush(f);
    fclose(f);
    if (replace_rename(tmp, file_path) != 0) {
        remove(tmp);
        return -1;
    }
    return 0;
}

static void state_load(state_store *st, const char *file_path)
{
    FILE *f = fopen(file_path, "rb");
    if (!f) return;
    char line[MAX_REL_PATH + 256];
    if (!fgets(line, sizeof line, f)) {
        fclose(f);
        return;
    }
    if (strncmp(line, "FCSTATE1", 8) != 0) {
        fclose(f);
        return;
    }
    while (fgets(line, sizeof line, f)) {
        size_t n = strlen(line);
        while (n && (line[n - 1] == '\n' || line[n - 1] == '\r')) line[--n] = '\0';
        if (!n || line[0] == '#') continue;

        char *end = NULL;
        errno = 0;
        unsigned long long bm = strtoull(line, &end, 16);
        if (errno || end == line) continue;
        while (*end == ' ') end++;
        char *end2 = NULL;
        errno = 0;
        unsigned long long sz = strtoull(end, &end2, 10);
        if (errno || end2 == end) continue;
        while (*end2 == ' ') end2++;
        if (!*end2) continue;
        if (!relpath_safe(end2)) continue;
        state_set(st, end2, (uint64_t)sz, (uint64_t)bm);
    }
    fclose(f);
}

/* ======================== 接收端：下载分片 ======================== */

typedef struct {
    const char *ip;
    uint16_t port;
    const char *apikey;
} server_info;

/* ======================== 接收端流水线：recv 与写盘解耦 ========================
 *
 * 问题：原来的下载线程在一条路径里 先 recv(64KB) 再 write(64KB)，
 * 只要磁盘稍慢（杀毒扫描、机械盘、SMB），recv 就会停下来，
 * 接收缓冲区被填满 => 接收窗口降到 0 => 发送端 send() 阻塞等待窗口，
 * 表现为 send 端单次 send 卡 10~16 秒、单分片 25~150 秒。
 *
 * 方案：接收线程只负责 recv 并入队（队列深度 QUEUE_DEPTH 块），
 * 独立的写盘线程从队列取块写文件；网络吞吐与磁盘延迟彻底解耦。
 */

typedef struct {
    uint32_t chunk_idx;   /* 分片序号 */
    uint64_t off;         /* 在文件中的偏移 */
    uint64_t len;         /* 实际字节数 */
    uint8_t *data;        /* 数据（CHUNK_BYTES 预分配） */
} qslot;

typedef struct {
    qslot *items;
    unsigned cap;
    unsigned head;        /* 写盘线程取走的位置 */
    unsigned tail;        /* 入队位置 */
    unsigned count;       /* 队列内元素数 */
    int closed;           /* 生产者结束（不再入队） */
    int aborted;          /* 写盘失败，通知生产者停止 */
    mutex_t mu;
    cond_t not_empty;
    cond_t not_full;
} chunk_queue;

typedef struct {
    const server_info *srv;
    uint32_t file_id;
    const char *rel_path;
    const char *partial_path;
    uint64_t file_size;
    uint32_t nchunks;       /* 分片数 = ceil(file_size / CHUNK_BYTES) */
    uint32_t pending[MAX_CHUNKS];
    uint32_t npending;

    /* 以下共享计数都由 state_mu 保护（MSVC 没有 stdatomic.h，统一用互斥量） */
    unsigned int next;              /* 线程领取分片用的游标 */
    unsigned int done;              /* 已下载分片数 */
    unsigned int finished;          /* 已退出的线程数 */
    uint64_t done_bytes;            /* 已下载字节数（仅用于进度显示） */
    int failed;                     /* 是否有线程判定失败 */

    mutex_t state_mu;               /* 保护计数、状态位图与状态文件写入 */
    mutex_t rcv_mu;                 /* 保护 done_bytes（每 64KB 实时累加，供进度显示） */
    chunk_queue *q;                 /* recv -> 写盘 队列（可为 NULL） */
    state_store *st;
    const char *state_file;
    state_entry *entry;
    uint64_t last_save_ms;
} dl_ctx;

/* 建立数据连接 + 握手 */
static sock_t connect_and_auth(const server_info *srv)
{
    sock_t s = connect_tcp(srv->ip, srv->port, CONNECT_TIMEOUT_SEC);
    if (!sock_valid(s)) return SOCK_INVALID;
    if (client_handshake(s, srv->apikey) != 0) {
        sock_close(s);
        return SOCK_INVALID;
    }
    return s;
}

/* 队列操作实现（类型定义见上方，须先于 dl_ctx 声明） */

static int queue_init(chunk_queue *q, unsigned depth)
{
    memset(q, 0, sizeof *q);
    q->items = (qslot *)calloc(depth, sizeof(qslot));
    if (!q->items) return -1;
    q->cap = depth;
    mutex_init(&q->mu);
    cond_init(&q->not_empty);
    cond_init(&q->not_full);
    return 0;
}

static void queue_destroy(chunk_queue *q)
{
    if (!q->items) return;
    for (unsigned i = 0; i < q->cap; i++) free(q->items[i].data);
    free(q->items);
    q->items = NULL;
    mutex_destroy(&q->mu);
    cond_destroy(&q->not_empty);
    cond_destroy(&q->not_full);
}

/* 队列操作实现（类型定义见上方，须先于 dl_ctx 声明）。
 *
 * 关键设计约束（修复随机丢块）：
 *   队列里的每一格必须是“数据已经完全填好”的块。
 *   因此接收线程先用 queue_alloc_buf() 拿到一块独立缓冲区（不受队列容量限制），
 *   recv 填满后再用 queue_push() 一次性挂到队尾；写盘线程只从队首取。
 *   之前“先占位后填充”的写法会让写盘线程取到正在填充的槽位，导致随机丢块。
 */

/* 申请一块接收缓冲区；队列已满则阻塞，防止内存无限增长 */
static uint8_t *queue_alloc_buf(chunk_queue *q)
{
    uint8_t *buf = NULL;
    mutex_lock(&q->mu);
    while (q->count >= q->cap && !q->closed && !q->aborted && !g_stop)
        cond_wait(&q->not_full, &q->mu);
    if (!q->closed && !q->aborted && !g_stop) {
        buf = (uint8_t *)malloc((size_t)CHUNK_BYTES);
        if (!buf) {
            fprintf(stderr, "[recv] out of memory allocating %llu B chunk buffer\n",
                    (unsigned long long)CHUNK_BYTES);
            q->aborted = 1;
            cond_broadcast(&q->not_full);
            cond_broadcast(&q->not_empty);
        }
    }
    mutex_unlock(&q->mu);
    return buf;
}

/* 把已填满的缓冲区连同元信息入队 */
static int queue_push(chunk_queue *q, uint8_t *buf, uint32_t ci, uint64_t off,
                      uint64_t len)
{
    int ok = 0;
    mutex_lock(&q->mu);
    if (!q->closed && !q->aborted) {
        unsigned idx = q->tail % q->cap;
        qslot *s = &q->items[idx];
        free(s->data);              /* 上一轮遗留的缓冲区（理论上已被释放成 NULL） */
        s->data = buf;
        s->chunk_idx = ci;
        s->off = off;
        s->len = len;
        q->tail = (q->tail + 1) % q->cap;
        q->count++;
        cond_signal(&q->not_empty);
        ok = 1;
    }
    mutex_unlock(&q->mu);
    if (!ok) free(buf);
    return ok ? 0 : -1;
}

/* 取队首的已完成块；返回 NULL 表示队列已关闭且为空 */
static qslot *queue_pop(chunk_queue *q)
{
    qslot *s = NULL;
    mutex_lock(&q->mu);
    while (q->count == 0 && !q->closed) cond_wait(&q->not_empty, &q->mu);
    if (q->count > 0) {
        s = &q->items[q->head % q->cap];
        q->head = (q->head + 1) % q->cap;
        q->count--;
        cond_signal(&q->not_full);
    }
    mutex_unlock(&q->mu);
    return s;
}

/* 写盘完成，归还该块缓冲区（防止重复释放，置 NULL） */
static void queue_release_slot(chunk_queue *q, qslot *slot)
{
    (void)q;
    free(slot->data);
    slot->data = NULL;
}

static void queue_close(chunk_queue *q)
{
    mutex_lock(&q->mu);
    q->closed = 1;
    cond_broadcast(&q->not_empty);
    cond_broadcast(&q->not_full);
    mutex_unlock(&q->mu);
}


/* 下面三个小函数在 state_mu 保护下访问共享计数与失败标志。
 * （MSVC 不提供 stdatomic.h，统一用互斥量，计数操作开销可忽略。） */
static int dl_failed(dl_ctx *c)
{
    int f;
    mutex_lock(&c->state_mu);
    f = c->failed;
    mutex_unlock(&c->state_mu);
    return f;
}

static void dl_set_failed(dl_ctx *c)
{
    mutex_lock(&c->state_mu);
    c->failed = 1;
    mutex_unlock(&c->state_mu);
}

static int dl_claim_chunk(dl_ctx *c, uint32_t *out)
{
    int ok = 0;
    mutex_lock(&c->state_mu);
    if (c->next < c->npending) {
        *out = c->pending[c->next];
        c->next++;
        ok = 1;
    }
    mutex_unlock(&c->state_mu);
    return ok ? 0 : -1;
}

/* ---------- 写盘线程：从队列取整块数据，写入本分片自己的临时文件 ----------
 *
 * 每个分片只写自己的 chunk_N.filecopy，写完整块后直接关闭，
 * 避免多个线程对同一个大文件频繁 fseek + write（Windows 上还有锁竞争）。
 * 全部块写完后由 download_one_file 按序号合并成正式文件。
 */
typedef struct {
    chunk_queue *q;
    const char *work_dir;    /* 临时分片目录 */
    uint64_t parts_written;  /* 已写字节 */
    uint32_t blocks_written; /* 已写块数 */
    int failed;
} writer_ctx;

static char *chunk_part_path(const writer_ctx *w, uint32_t ci)
{
    char name[64];
    char full[MAX_PATH_BUF];
    snprintf(name, sizeof name, "chunk_%06u.filecopy", (unsigned)ci);
    if (join_path_buf(full, sizeof full, w->work_dir, name) != 0) return NULL;
    return xstrdup(full);
}

static void *writer_thread(void *arg)
{
    writer_ctx *w = (writer_ctx *)arg;
    chunk_queue *q = w->q;

    for (;;) {
        qslot *s = queue_pop(q);
        if (!s) break;                    /* 生产者结束且队列空 */

        uint64_t t_w = now_ms();
        char *ppath = chunk_part_path(w, s->chunk_idx);
        FILE *f = ppath ? fopen(ppath, "wb") : NULL;
        if (!f) {
            fprintf(stderr, "[recv] cannot open chunk part file: %s\n",
                    ppath ? ppath : "(path too long)");
            free(ppath);
            queue_release_slot(q, s);
            w->failed = 1;
            queue_close(q);
            break;
        }
        /* 整块一次顺序写出（8MiB），随后关闭，落盘由 OS 合并 */
        size_t wrote = fwrite(s->data, 1, (size_t)s->len, f);
        fflush(f);
        fclose(f);
        free(ppath);

        if (wrote != (size_t)s->len) {
            fprintf(stderr, "[recv] write failed for chunk %u (%zu/%lu B)\n",
                    s->chunk_idx, wrote, (unsigned long)s->len);
            queue_release_slot(q, s);
            w->failed = 1;
            queue_close(q);
            break;
        }
        w->parts_written += s->len;
        w->blocks_written++;

        if (g_trace) {
            fprintf(stdout, "[recv] write chunk %u %lu B in %llu ms\n",
                    s->chunk_idx, (unsigned long)s->len,
                    (unsigned long long)(now_ms() - t_w));
        }
        queue_release_slot(q, s);   /* 归还缓冲区 */
    }
    return NULL;
}

/* 登记一个分片已完成（在 state_mu 保护下，只做内存计数，不落盘）。
 * 注意：done_bytes 的累加不在这里做——它由接收路径按 64KB 实时累加，
 * 这样进度条才能在接收过程中平滑前进。 */
static void dl_mark_chunk_done(dl_ctx *c, uint32_t ci, uint64_t len)
{
    (void)len;
    mutex_lock(&c->state_mu);
    c->entry->bitmap |= ((uint64_t)1 << ci);
    c->done++;
    mutex_unlock(&c->state_mu);
}

/* 实时累加已接收字节（每个 64 KiB 读完后调用，供进度显示）。
 * 用独立互斥量 rcv_mu，避免与 state_mu（状态文件写入）互相拖慢。 */
static void dl_add_received(dl_ctx *c, uint64_t bytes)
{
    mutex_lock(&c->rcv_mu);
    c->done_bytes += bytes;
    mutex_unlock(&c->rcv_mu);
}

/* 单个下载线程：循环领取分片 => 建/复用自己的连接 => recv 入队 => 记录状态 */
static void *dl_worker_thread(void *arg)
{
    dl_ctx *c = (dl_ctx *)arg;
    sock_t sock = SOCK_INVALID;
    uint32_t ci = 0;

    while (!g_stop) {
        if (dl_failed(c)) break;
        if (dl_claim_chunk(c, &ci) != 0) break;

        uint64_t off = chunk_offset(ci);
        uint64_t len = chunk_bytes(c->file_size, c->nchunks, ci);
        int ok = 0;
        uint64_t t_chunk = now_ms();

        for (int attempt = 1; attempt <= MAX_CHUNK_ATTEMPTS && !ok; attempt++) {
            if (g_stop || dl_failed(c)) break;

            if (!sock_valid(sock)) {
                sock = connect_and_auth(c->srv);
                if (!sock_valid(sock)) {
                    if (g_stop) break;
                    sleep_ms(250);
                    continue;
                }
            }

            uint8_t req[8];
            put_u32be(req, c->file_id);
            put_u32be(req + 4, ci);
            if (proto_send_msg(sock, CMD_GET, req, 8) != 0) {
                sock_close(sock);
                sock = SOCK_INVALID;
                continue;
            }

            uint8_t rcmd = 0;
            uint64_t rlen = 0;
            if (proto_recv_header(sock, &rcmd, &rlen, (uint64_t)1 << 62) != 0) {
                sock_close(sock);
                sock = SOCK_INVALID;
                continue;
            }
            if (rcmd == CMD_ERR) {
                if (rlen > 0 && rlen < (1u << 20)) {
                    char *msg = (char *)malloc((size_t)rlen + 1);
                    if (msg && recv_all(sock, msg, (size_t)rlen) == 0) {
                        msg[rlen] = '\0';
                        fprintf(stderr, "[recv] server error: %s\n", msg);
                    }
                    free(msg);
                }
                sock_close(sock);
                sock = SOCK_INVALID;
                continue;
            }
            if (rcmd != CMD_DATA || rlen != len) {
                sock_close(sock);
                sock = SOCK_INVALID;
                continue;
            }

            /* ---------- 纯接收路径：只 recv 入队，不碰磁盘 ---------- */
            if (len > (uint64_t)CHUNK_BYTES) {
                dl_set_failed(c);
                break;
            }
            uint8_t *buf = queue_alloc_buf(c->q);
            if (!buf) {
                if (g_stop) break;
                dl_set_failed(c);
                break;
            }
            uint64_t t_recv = now_ms();
            /* 关键：按 64 KiB 分段 recv 并实时累加已收字节，
             * 这样 8 MiB 大块下载期间进度条也能平滑前进，
             * 不再出现“卡在 0 字节 / 长时间不动”的假象。 */
            uint64_t got = 0;
            int recv_err = 0;
            while (got < len) {
                size_t want = (size_t)(len - got);
                if (want > (size_t)NET_IO_UNIT) want = (size_t)NET_IO_UNIT;
                if (recv_all_ex(sock, buf + got, want, NULL, NULL) != 0) {
                    recv_err = 1;
                    break;
                }
                got += want;
                dl_add_received(c, want);
            }
            if (recv_err) {
                /* 本次数据作废：把已计入的字节退回，缓冲丢弃，不入队 */
                mutex_lock(&c->rcv_mu);
                c->done_bytes -= got;
                mutex_unlock(&c->rcv_mu);
                free(buf);
                sock_close(sock);
                sock = SOCK_INVALID;
                continue;
            }
            uint64_t recv_ms = now_ms() - t_recv;
            uint64_t t_q = now_ms();
            if (queue_push(c->q, buf, ci, off, len) != 0) {
                if (g_stop) break;
                dl_set_failed(c);
                break;
            }
            uint64_t queue_ms = now_ms() - t_q;

            /* 每收满一个 8MB 分片打印：recv 耗时与入队耗时（证明 recv 本身不慢） */
            if (g_trace || len == (uint64_t)CHUNK_BYTES) {
                double secs = (double)recv_ms / 1000.0;
                double mbps = secs > 0.0 ? (double)len / secs / (1024.0 * 1024.0) : 0.0;
                fprintf(stdout, "[recv] recv chunk %u %lu B in %llu ms (%.1f MiB/s), "
                                "enqueue %llu ms\n",
                        ci, (unsigned long)len, (unsigned long long)recv_ms, mbps,
                        (unsigned long long)queue_ms);
                fflush(stdout);
            }
            ok = 1;
        }

        if (!ok) {
            if (!g_stop) {
                dl_set_failed(c);
                fprintf(stderr, "[recv] chunk %u of %s failed after %d attempts\n",
                        ci, c->rel_path, MAX_CHUNK_ATTEMPTS);
            }
            break;
        }

        if (g_trace) {
            uint64_t ms = now_ms() - t_chunk;
            double secs = (double)ms / 1000.0;
            double mbps = secs > 0.0 ? (double)len / secs / (1024.0 * 1024.0) : 0.0;
            fprintf(stdout, "[trace] %s chunk %u (%lu B) end-to-end in %llu ms (%.1f MiB/s)\n",
                    c->rel_path, ci, (unsigned long)len,
                    (unsigned long long)ms, mbps);
        }

        /* 分片已入队；写盘由写盘线程完成，这里只登记计数（不落状态文件） */
        dl_mark_chunk_done(c, ci, len);
    }

    sock_close(sock);
    mutex_lock(&c->state_mu);
    c->finished++;
    mutex_unlock(&c->state_mu);
    return NULL;
}

/* 把各个 chunk_N.filecopy 按序号顺序拼接到 partial_path，然后删除分片文件。
 * 用大块顺序读写（1 MiB），合并速度接近磁盘顺序复制。 */
static int merge_chunk_parts(const char *work_dir, const char *partial_path,
                             uint32_t nchunks, uint64_t expected_size)
{
    char path[MAX_PATH_BUF];
    char name[64];
    if (join_path_buf(path, sizeof path, work_dir, "chunk") != 0) return -1;

    /* 临时目录就是下载目录，分片文件名为 chunk_%06u.filecopy */
    uint8_t *buf = (uint8_t *)malloc(1024u * 1024u);
    if (!buf) return -1;

    char out_path[MAX_PATH_BUF];
    if (snprintf(out_path, sizeof out_path, "%s.part", partial_path) >=
        (int)sizeof out_path) {
        free(buf);
        return -1;
    }
    FILE *out = fopen(out_path, "wb");
    if (!out) {
        free(buf);
        return -1;
    }

    uint64_t total = 0;
    int rc = 0;
    for (uint32_t i = 0; i < nchunks; i++) {
        snprintf(name, sizeof name, "chunk_%06u.filecopy", (unsigned)i);
        if (join_path_buf(path, sizeof path, work_dir, name) != 0) {
            rc = -1;
            break;
        }
        FILE *in = fopen(path, "rb");
        if (!in) {
            fprintf(stderr, "[recv] missing chunk part: %s\n", name);
            rc = -1;
            break;
        }
        for (;;) {
            size_t got = fread(buf, 1, 1024u * 1024u, in);
            if (got == 0) break;
            if (fwrite(buf, 1, got, out) != got) {
                rc = -1;
                break;
            }
            total += got;
        }
        fclose(in);
        if (rc != 0) break;
        remove(path);   /* 已合并，删除分片文件释放空间 */
    }
    fflush(out);
    fclose(out);
    free(buf);

    if (rc != 0) {
        remove(out_path);
        return -1;
    }
    if (expected_size != 0 && total != expected_size) {
        fprintf(stderr, "[recv] merged size %llu != expected %llu\n",
                (unsigned long long)total, (unsigned long long)expected_size);
        remove(out_path);
        return -1;
    }
    if (replace_rename(out_path, partial_path) != 0) {
        remove(out_path);
        return -1;
    }
    return 0;
}

/* 下载一个文件（含断点续传），成功返回 0 */
static int download_one_file(const server_info *srv, const char *dl_dir,
                             const char *state_file, const file_list *share,
                             uint32_t fid, state_store *st, int nthreads)
{
    const file_entry *fe = &share->items[fid];
    const char *rel = fe->path;
    uint64_t size = fe->size;

    char final_path[MAX_PATH_BUF];
    if (join_path_buf(final_path, sizeof final_path, dl_dir, rel) != 0) {
        fprintf(stderr, "[recv] download path too long: %s\n", rel);
        return -1;
    }
    char partial_path[MAX_PATH_BUF + 16];
    if (snprintf(partial_path, sizeof partial_path, "%s.filecopy", final_path) >=
        (int)sizeof partial_path) {
        fprintf(stderr, "[recv] download path too long: %s\n", rel);
        return -1;
    }

    /* 已存在同大小正式文件：直接跳过（并清理旧状态） */
    uint64_t existing = 0;
    if (fs_file_size(final_path, &existing) == 0 && existing == size) {
        char hs[32];
        fprintf(stdout, "[recv] skip %s (already complete, %s)\n", rel,
                human_size(size, hs, sizeof hs));
        if (state_find(st, rel)) {
            state_remove(st, rel);
            state_save(st, state_file);
        }
        return 0;
    }

    /* 确保父目录存在 */
    char parent[MAX_PATH_BUF];
    memcpy(parent, final_path, strlen(final_path) + 1);
    char *sep = strrchr_any(parent, '/', '\\');
    if (sep) *sep = '\0';
    if (parent[0] && mkdir_p(parent) != 0) {
        fprintf(stderr, "[recv] cannot create directory: %s\n", parent);
        return -1;
    }

    /* 空文件：直接创建并改名 */
    uint32_t nchunks = chunk_count(size);
    if (nchunks == 0) {
        int fd = open_file_rw_fd(partial_path, 1);
        if (fd < 0) {
            fprintf(stderr, "[recv] cannot create file: %s\n", partial_path);
            return -1;
        }
        close_file_fd(fd);
        if (replace_rename(partial_path, final_path) != 0) {
            fprintf(stderr, "[recv] rename failed: %s -> %s\n", partial_path, final_path);
            return -1;
        }
        if (state_find(st, rel)) {
            state_remove(st, rel);
            state_save(st, state_file);
        }
        fprintf(stdout, "[recv] done %s (empty file)\n", rel);
        return 0;
    }

    /* 读取 / 初始化该文件的断点位图 */
    state_entry *entry = state_find(st, rel);
    if (!entry) {
        if (state_set(st, rel, size, 0) != 0) {
            fprintf(stderr, "[recv] cannot allocate state for %s\n", rel);
            return -1;
        }
        entry = state_find(st, rel);
        if (!entry) return -1;
    } else if (entry->size != size) {
        entry->size = size;
        entry->bitmap = 0;
    }
    uint64_t bitmap = entry->bitmap & mask_for(nchunks);
    if (bitmap != 0) {
        /* 位图显示有已完成分片，但部分文件丢失 => 作废重下 */
        uint64_t ps = 0;
        if (fs_file_size(partial_path, &ps) != 0) bitmap = 0;
    }
    entry->bitmap = bitmap;

    uint32_t pending[MAX_CHUNKS];
    uint32_t npend = 0;
    for (uint32_t i = 0; i < nchunks; i++) {
        if (!(bitmap & ((uint64_t)1 << i))) pending[npend++] = i;
    }

    if (npend == 0) {
        /* 全部分片都已完成（可能上次中断在改名之前） */
        if (replace_rename(partial_path, final_path) != 0) {
            fprintf(stderr, "[recv] rename failed: %s -> %s\n", partial_path, final_path);
            return -1;
        }
        state_remove(st, rel);
        state_save(st, state_file);
        fprintf(stdout, "[recv] done %s (all chunks already present)\n", rel);
        return 0;
    }

    /* 准备部分文件：新下载或位图失效时清空旧内容 */
    {
        uint64_t ps = 0;
        int need_trunc = (bitmap == 0) || (fs_file_size(partial_path, &ps) != 0);
        int fd = open_file_rw_fd(partial_path, need_trunc ? 1 : 0);
        if (fd < 0) {
            fprintf(stderr, "[recv] cannot create partial file: %s\n", partial_path);
            return -1;
        }
        close_file_fd(fd);
    }

    dl_ctx c;
    memset(&c, 0, sizeof c);
    c.srv = srv;
    c.file_id = fid;
    c.rel_path = rel;
    c.partial_path = partial_path;
    c.file_size = size;
    c.nchunks = nchunks;
    memcpy(c.pending, pending, npend * sizeof pending[0]);
    c.npending = npend;
    c.st = st;
    c.state_file = state_file;
    c.entry = entry;
    c.last_save_ms = 0;
    mutex_init(&c.state_mu);
    mutex_init(&c.rcv_mu);

    int nworkers = nthreads;
    if ((uint32_t)nworkers > npend) nworkers = (int)npend;

    {
        char hs[32];
        fprintf(stdout, "[recv] download %s (%s, %u chunks, %u pending%s)\n",
                rel, human_size(size, hs, sizeof hs), nchunks, npend,
                nchunks - npend ? ", resuming" : "");
    }
    fflush(stdout);

    /* ---- 建队列 + 写盘线程（recv 与写盘解耦） ---- */
    chunk_queue q;
    if (queue_init(&q, QUEUE_DEPTH) != 0) {
        fprintf(stderr, "[recv] cannot allocate recv queue\n");
        return -1;
    }
    writer_ctx wr;
    memset(&wr, 0, sizeof wr);
    wr.q = &q;
    wr.work_dir = dl_dir;

    thread_t wth;
    if (thread_create(&wth, writer_thread, &wr) != 0) {
        fprintf(stderr, "[recv] cannot create writer thread\n");
        queue_destroy(&q);
        return -1;
    }

    c.q = &q;

    thread_t tids[MAX_THREADS];
    int started = 0;
    for (int i = 0; i < nworkers; i++) {
        if (thread_create(&tids[i], dl_worker_thread, &c) != 0) {
            fprintf(stderr, "[recv] cannot create download thread\n");
            dl_set_failed(&c);
            break;
        }
        started++;
    }

    /* 进度监控：每 100ms 刷新一次。
     * done_bytes 由接收线程按 64 KiB 实时累加，因此进度条能平滑前进，
     * 并给出实时速率与预计剩余时间（ETA）。 */
    uint64_t t0 = now_ms();
    uint64_t last_db = 0, last_t = t0;
    double rate = 0.0;              /* 平滑后的速率（字节/秒） */
    for (;;) {
        unsigned int done, finished;
        uint64_t db;
        if (g_stop) break;
        mutex_lock(&c.state_mu);
        done = c.done;
        finished = c.finished;
        mutex_unlock(&c.state_mu);
        mutex_lock(&c.rcv_mu);
        db = c.done_bytes;
        mutex_unlock(&c.rcv_mu);
        if (finished >= (unsigned)started) break;

        uint64_t now = now_ms();
        if (now > last_t) {
            double inst = (double)(db - last_db) * 1000.0 / (double)(now - last_t);
            rate = (rate <= 0.0) ? inst : (rate * 0.7 + inst * 0.3);  /* 指数平滑 */
            last_db = db;
            last_t = now;
        }

        char hd[32], ht[32], hr[32], he[32];
        double pct = size ? (double)db * 100.0 / (double)size : 100.0;
        if (pct > 100.0) pct = 100.0;
        double eta = (rate > 1.0 && size > db) ? (double)(size - db) / rate : -1.0;
        if (eta >= 0.0)
            snprintf(he, sizeof he, "%02d:%02d", (int)(eta / 60.0), (int)eta % 60);
        else
            snprintf(he, sizeof he, "--:--");
        fprintf(stdout, "\r    progress %u/%u chunks (%.1f%%), %s / %s, %s/s, ETA %s     ",
                done, npend, pct,
                human_size(db, hd, sizeof hd),
                human_size(size, ht, sizeof ht),
                human_size((uint64_t)(rate < 0.0 ? 0.0 : rate), hr, sizeof hr),
                he);
        fflush(stdout);
        sleep_ms(100);
    }

    if (g_stop) {
        if (mutex_trylock(&c.state_mu) == 0) {
            state_save(st, state_file);
            mutex_unlock(&c.state_mu);
        }
        fprintf(stdout, "\n[recv] interrupted; chunk progress saved, rerun to resume\n");
        fflush(stdout);
        hard_exit(0);   /* 立即结束进程：已落盘的状态不依赖 join */
    }

    for (int i = 0; i < started; i++) thread_join(tids[i]);
    /* 接收线程全部结束 => 关闭队列（唤醒阻塞在 queue_pop 的写盘线程），
     * 再等写盘线程把队列里剩余的块全部写完，才能安全合并。 */
    queue_close(&q);
    thread_join(wth);
    mutex_destroy(&c.state_mu);
    mutex_destroy(&c.rcv_mu);

    if (!c.failed) {
        char hs[32], ht[32];
        fprintf(stdout, "\r    progress %u/%u chunks (100.0%%), %s / %s\n",
                c.done, npend,
                human_size(c.done_bytes, hs, sizeof hs),
                human_size(size, ht, sizeof ht));
    } else {
        fputc('\n', stdout);
    }

    if (c.failed || wr.failed) {
        /* 失败：保存当前位图以便下次续传（已写好的分片文件会在合并时复用） */
        state_save(st, state_file);
        queue_destroy(&q);
        fprintf(stderr, "[recv] failed to download %s\n", rel);
        return -1;
    }

    /* ---- 合并：按分片序号把 chunk_N.filecopy 顺序拼接成正式文件 ---- */
    uint64_t t_merge = now_ms();
    {
        if (merge_chunk_parts(dl_dir, partial_path, nchunks, size) != 0) {
            state_save(st, state_file);
            queue_destroy(&q);
            fprintf(stderr, "[recv] merge failed for %s\n", rel);
            return -1;
        }
    }
    {
        char hs[32];
        double msecs = (double)(now_ms() - t_merge) / 1000.0;
        fprintf(stdout, "[recv] merged %s (%s) in %.2f s\n", rel,
                human_size(size, hs, sizeof hs), msecs);
        fflush(stdout);
    }

    /* 校验大小后改名（.filecopy -> 正式名），并清除状态记录 */
    uint64_t ps = 0;
    if (fs_file_size(partial_path, &ps) != 0 || ps != size) {
        fprintf(stderr, "[recv] size mismatch for %s\n", rel);
        queue_destroy(&q);
        return -1;
    }
    if (replace_rename(partial_path, final_path) != 0) {
        fprintf(stderr, "[recv] rename failed: %s -> %s\n", partial_path, final_path);
        queue_destroy(&q);
        return -1;
    }
    state_remove(st, rel);
    state_save(st, state_file);
    queue_destroy(&q);
    {
        char hs[32];
        double secs = (double)(now_ms() - t0) / 1000.0;
        double mbps = secs > 0.0 ? (double)size / secs / (1024.0 * 1024.0) : 0.0;
        fprintf(stdout, "[recv] done %s (%s, %u chunks, %.2f s, %.1f MiB/s)\n", rel,
                human_size(size, hs, sizeof hs), nchunks, secs, mbps);
    }
    return 0;
}

/* ======================== 接收端：主流程 ======================== */

typedef struct {
    int mode;                 /* MODE_SEND / MODE_RECV */
    const char *ip;           /* --ip，仅接收端使用 */
    uint16_t port;            /* --port */
    int have_port;
    const char *apikey;       /* --apikey */
    const char *dl_dir;       /* -c，默认当前目录 */
    int threads;              /* -t，默认 DEFAULT_THREADS */
    char *paths[1024];        /* --send 的输入路径 */
    int npaths;
} options;

static int run_receive(const options *opt)
{
    const char *dl = (opt->dl_dir && opt->dl_dir[0]) ? opt->dl_dir : ".";

    if (mkdir_p(dl) != 0) {
        fprintf(stderr, "[recv] cannot create download directory: %s\n", dl);
        return 1;
    }

    char state_file[MAX_PATH_BUF];
    if (join_path_buf(state_file, sizeof state_file, dl, ".filecopy.state") != 0) {
        fprintf(stderr, "[recv] state file path too long\n");
        return 1;
    }

    server_info srv;
    srv.ip = opt->ip;
    srv.port = opt->port;
    srv.apikey = opt->apikey;

    /* 连接发送端（应先启动），失败重试 MAX_CONNECT_TRIES 次 */
    sock_t ctl = SOCK_INVALID;
    int connected = 0;
    for (int attempt = 1; attempt <= MAX_CONNECT_TRIES && !g_stop; attempt++) {
        ctl = connect_tcp(srv.ip, srv.port, CONNECT_TIMEOUT_SEC);
        if (!sock_valid(ctl)) {
            fprintf(stderr, "[recv] connect %s:%u failed (%d/%d), retrying...\n",
                    srv.ip, (unsigned)srv.port, attempt, MAX_CONNECT_TRIES);
            sleep_ms(1000);
            continue;
        }
        if (client_handshake(ctl, srv.apikey) != 0) {
            sock_close(ctl);
            ctl = SOCK_INVALID;
            fprintf(stderr, "[recv] handshake rejected by server\n");
            break;
        }
        connected = 1;
        break;
    }
    if (!connected) {
        fprintf(stderr, "[recv] unable to connect/authenticate to %s:%u\n",
                srv.ip, (unsigned)srv.port);
        return 1;
    }
    fprintf(stdout, "[recv] connected to %s:%u, handshake ok\n", srv.ip,
            (unsigned)srv.port);
    fflush(stdout);

    /* 拉取远端共享文件列表 */
    file_list share;
    memset(&share, 0, sizeof share);
    uint8_t cmd = 0;
    uint64_t len = 0;
    if (proto_send_msg(ctl, CMD_LIST, NULL, 0) != 0 ||
        proto_recv_header(ctl, &cmd, &len, MAX_LIST_BYTES) != 0 ||
        cmd != CMD_LIST_DATA) {
        fprintf(stderr, "[recv] failed to fetch remote file list\n");
        sock_close(ctl);
        return 1;
    }
    uint8_t *payload = NULL;
    if (recv_payload(ctl, len, &payload) != 0) {
        fprintf(stderr, "[recv] failed to receive remote file list\n");
        sock_close(ctl);
        return 1;
    }
    sock_close(ctl);
    if (parse_list_payload(payload, len, &share) != 0) {
        fprintf(stderr, "[recv] invalid remote file list\n");
        free(payload);
        free_file_list(&share);
        return 1;
    }
    free(payload);

    if (share.count == 0) {
        fprintf(stdout, "[recv] remote shares no files\n");
        free_file_list(&share);
        return 0;
    }

    /* 打印列表（递归展开后的文件列表，保留相对路径） */
    {
        char tot[32];
        fprintf(stdout, "[recv] remote files (%lu):\n", (unsigned long)share.count);
        for (size_t i = 0; i < share.count; i++) {
            char hs[32];
            fprintf(stdout, "  [%3lu] %-62s %s\n", (unsigned long)i,
                    share.items[i].path,
                    human_size(share.items[i].size, hs, sizeof hs));
        }
        fprintf(stdout, "[recv] total %s\n",
                human_size(share.total_bytes, tot, sizeof tot));
    }
    fflush(stdout);

    /* 加载断点状态，清理不在本次共享列表中的旧条目 */
    state_store st;
    memset(&st, 0, sizeof st);
    state_load(&st, state_file);
    for (size_t i = st.count; i-- > 0;) {
        if (!share_has_path(&share, st.items[i].path)) state_remove_at(&st, i);
    }
    if (st.count != 0) state_save(&st, state_file);

    int rc = 0;
    size_t done_files = 0;
    uint64_t t_all = now_ms();
    for (size_t i = 0; i < share.count; i++) {
        if (g_stop) break;
        if (download_one_file(&srv, dl, state_file, &share, (uint32_t)i, &st,
                              opt->threads) != 0) {
            rc = 1;
            break;
        }
        done_files++;
    }
    if (g_stop) {
        fprintf(stdout, "[recv] interrupted\n");
        rc = 1;
    }

    /* 收尾：仍有未完成条目则写盘；全部完成则删除状态文件 */
    if (st.count != 0) {
        state_save(&st, state_file);
    } else {
        remove(state_file);
    }
    if (rc == 0) {
        char tot[32];
        double secs = (double)(now_ms() - t_all) / 1000.0;
        fprintf(stdout, "[recv] all done: %lu/%lu files, %s, %.2f s\n",
                (unsigned long)done_files, (unsigned long)share.count,
                human_size(share.total_bytes, tot, sizeof tot), secs);
    }

    free_file_list(&share);
    state_free(&st);
    return rc;
}

/* ======================== 发送端：服务实现 ======================== */

static file_list g_share;               /* 启动时收集，之后只读 */
static uint8_t *g_share_payload = NULL; /* 预序列化的 LIST_DATA payload */
static uint64_t g_share_payload_len = 0;

/* 处理一个 GET 分片请求：先发 DATA 头（含分片长度），再流式发送文件内容。
 *
 * 发送按 64KB 为单位循环 send（由 send_all_ex 保证），单次请求的整块耗时
 * 与吞吐写入日志，便于定位是网络慢还是磁盘慢。 */
static int serve_chunk(sock_t s, uint32_t fid, uint32_t cidx, const char *peer)
{
    if ((size_t)fid >= g_share.count) {
        const char *m = "bad file id";
        proto_send_msg(s, CMD_ERR, m, strlen(m));
        return -1;
    }
    const file_entry *fe = &g_share.items[fid];
    uint32_t nchunks = chunk_count(fe->size);
    if (cidx >= nchunks) {
        const char *m = "chunk out of range";
        proto_send_msg(s, CMD_ERR, m, strlen(m));
        return -1;
    }
    uint64_t off = chunk_offset(cidx);
    uint64_t len = chunk_bytes(fe->size, nchunks, cidx);

    uint64_t t_all = now_ms();
    FILE *f = fopen(fe->fs_path, "rb");
    if (!f) {
        const char *m = "file open failed on sender";
        proto_send_msg(s, CMD_ERR, m, strlen(m));
        return -1;
    }
    if (file_seek64(f, off) != 0) {
        fclose(f);
        return -1;
    }
    if (proto_send_msg(s, CMD_DATA, NULL, len) != 0) {
        fclose(f);
        return -1;
    }

    /* 64KB 读盘缓冲：与 NET_IO_UNIT 对齐，避免一次 8MB 的大块 send */
    uint8_t *buf = (uint8_t *)malloc((size_t)NET_IO_UNIT);
    if (!buf) {
        fclose(f);
        return -1;
    }
    io_stat stt;
    memset(&stt, 0, sizeof stt);
    uint64_t read_ms = 0;
    uint64_t sent = 0;
    int rc = 0;
    while (sent < len) {
        size_t want = (size_t)(len - sent);
        if (want > (size_t)NET_IO_UNIT) want = (size_t)NET_IO_UNIT;
        uint64_t tr = now_ms();
        size_t got = fread(buf, 1, want, f);
        read_ms += now_ms() - tr;
        if (got != want) {
            rc = -1; /* 文件在传输途中被截断：断开让客户端重试 */
            break;
        }
        if (send_all_ex(s, buf, got, &stt, g_trace ? peer : NULL) != 0) {
            rc = -1;
            break;
        }
        sent += got;
    }
    free(buf);
    fclose(f);

    {
        uint64_t total_ms = now_ms() - t_all;
        double mbps = 0.0;
        double secs = (double)total_ms / 1000.0;
        if (secs > 0.0) mbps = (double)sent / secs / (1024.0 * 1024.0);
        fprintf(stdout, "[send] chunk %u/%u of %s (%lu B) sent in %llu ms "
                        "(read %llu ms, send calls %llu, slowest %llu ms, %.1f MiB/s)%s\n",
                cidx + 1, nchunks, fe->path,
                (unsigned long)sent,
                (unsigned long long)total_ms,
                (unsigned long long)read_ms,
                (unsigned long long)stt.calls,
                (unsigned long long)stt.max_ms,
                mbps, rc == 0 ? "" : " [FAILED]");
        fflush(stdout);
    }
    return rc;
}

typedef struct {
    sock_t s;
    struct sockaddr_storage peer;
    const char *apikey;
} conn_ctx;

/* 每个客户端连接一个线程：握手成功后顺序处理 LIST / GET 请求 */
static void *serve_conn_thread(void *arg)
{
    conn_ctx *ctx = (conn_ctx *)arg;
    sock_t s = ctx->s;
    char peer[128];
    peer_to_string(&ctx->peer, peer, sizeof peer);

    fprintf(stdout, "[send] incoming connection from %s\n", peer);
    fflush(stdout);

    if (server_handshake(s, ctx->apikey) != 0) {
        fprintf(stderr, "[send] handshake failed with %s (bad apikey?)\n", peer);
        sock_close(s);
        free(ctx);
        return NULL;
    }
    fprintf(stdout, "[send] client %s authenticated\n", peer);
    fflush(stdout);

    for (;;) {
        uint8_t cmd = 0;
        uint64_t len = 0;
        int hrc = proto_recv_header(s, &cmd, &len, (uint64_t)1 << 32);
        if (hrc != 0) break;

        if (cmd == CMD_LIST) {
            if (len != 0) {
                const char *m = "bad LIST payload";
                proto_send_msg(s, CMD_ERR, m, strlen(m));
                break;
            }
            if (proto_send_msg(s, CMD_LIST_DATA, g_share_payload, g_share_payload_len) != 0)
                break;
        } else if (cmd == CMD_GET) {
            if (len != 8) {
                const char *m = "bad GET payload";
                proto_send_msg(s, CMD_ERR, m, strlen(m));
                break;
            }
            uint8_t req[8];
            if (recv_all(s, req, 8) != 0) break;
            uint32_t fid = get_u32be(req);
            uint32_t cidx = get_u32be(req + 4);
            if (serve_chunk(s, fid, cidx, peer) != 0) break;
        } else {
            const char *m = "unknown command";
            proto_send_msg(s, CMD_ERR, m, strlen(m));
            break;
        }
    }

    fprintf(stdout, "[send] connection from %s closed\n", peer);
    fflush(stdout);
    sock_close(s);
    free(ctx);
    return NULL;
}

static int run_send(const options *opt)
{
    file_list share;
    memset(&share, 0, sizeof share);
    if (build_share(&share, opt->paths, opt->npaths) != 0) {
        free_file_list(&share);
        return 1;
    }
    if (share.count == 0) {
        fprintf(stderr, "[send] no files to share\n");
        free_file_list(&share);
        return 1;
    }

    uint8_t *payload = NULL;
    uint64_t plen = 0;
    if (build_list_payload(&share, &payload, &plen) != 0) {
        fprintf(stderr, "[send] file list too large\n");
        free_file_list(&share);
        return 1;
    }
    g_share = share;
    g_share_payload = payload;
    g_share_payload_len = plen;

    {
        char tot[32];
        fprintf(stdout, "[send] sharing %lu files, total %s\n",
                (unsigned long)share.count,
                human_size(share.total_bytes, tot, sizeof tot));
        for (size_t i = 0; i < share.count; i++) {
            char hs[32];
            fprintf(stdout, "  [%3lu] %-62s %s\n", (unsigned long)i,
                    share.items[i].path,
                    human_size(share.items[i].size, hs, sizeof hs));
        }
    }
    fflush(stdout);

    sock_t ls = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (!sock_valid(ls)) {
        fprintf(stderr, "[send] cannot create socket\n");
        free_file_list(&share);
        free(payload);
        return 1;
    }
    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, (const char *)&one, (int)sizeof one);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(opt->port);
    if (bind(ls, (struct sockaddr *)&addr, (int)sizeof addr) != 0) {
        fprintf(stderr, "[send] bind port %u failed\n", (unsigned)opt->port);
        sock_close(ls);
        free_file_list(&share);
        free(payload);
        return 1;
    }
    if (listen(ls, 16) != 0) {
        fprintf(stderr, "[send] listen failed\n");
        sock_close(ls);
        free_file_list(&share);
        free(payload);
        return 1;
    }

    fprintf(stdout, "[send] listening on 0.0.0.0:%u, waiting for receiver...\n",
            (unsigned)opt->port);
    fflush(stdout);

    while (!g_stop) {
        struct sockaddr_storage peer;
        memset(&peer, 0, sizeof peer);
        sock_t s = accept_with_wait(ls, &peer);
        if (!sock_valid(s)) continue;

        conn_ctx *ctx = (conn_ctx *)malloc(sizeof *ctx);
        if (!ctx) {
            sock_close(s);
            continue;
        }
        ctx->s = s;
        ctx->peer = peer;
        ctx->apikey = opt->apikey;

        thread_t t;
        if (thread_create(&t, serve_conn_thread, ctx) != 0) {
            sock_close(s);
            free(ctx);
            continue;
        }
        thread_detach(t);
    }

    fprintf(stdout, "\n[send] shutting down\n");
    fflush(stdout);
    /* 直接结束进程：detached 服务线程可能仍在阻塞读写，无需 join */
    sock_close(ls);
    hard_exit(0);
    return 0; /* 不会到达 */
}

/* ======================== 参数解析 ======================== */

static long parse_long(const char *s, long lo, long hi)
{
    char *end = NULL;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v < lo || v > hi) return -1;
    return v;
}

static void usage(void)
{
    fprintf(stdout,
        "%s - single-binary file transfer tool (sender + receiver)\n"
        "\n"
        "Usage:\n"
        "  Send:    %s --send --port <P> --apikey <KEY> <file|dir> [more...]\n"
        "  Receive: %s --receive --ip <IP> --port <P> --apikey <KEY>\n"
        "                    [-c <dir>] [-t <threads>]\n"
        "\n"
        "Options:\n"
        "  --send               share files (server side)\n"
        "  --receive            download files (client side)\n"
        "  --port <P>           TCP port used by both sides\n"
        "  --apikey <KEY>       shared secret for the handshake (1..%d bytes)\n"
        "  --ip <IP>            sender address (receive mode)\n"
        "  -c, --dir <DIR>      download directory (default: current dir)\n"
        "  -t, --threads <N>    concurrent chunk download threads "
        "(default %d, max %d)\n"
        "  -h, --help           show this help\n"
        "\n"
        "Notes:\n"
        "  - Directories are expanded recursively, relative paths preserved.\n"
        "  - Unfinished files are saved as \"<name>.filecopy\"; per-chunk progress\n"
        "    is stored in \"<dir>/.filecopy.state\" and resumed on next run.\n"
        "  - Ctrl+C saves chunk progress before exiting.\n",
        FC_NAME, FC_NAME, FC_NAME, MAX_APIKEY_LEN, DEFAULT_THREADS, MAX_THREADS);
}

static int parse_options(int argc, char **argv, options *o)
{
    if (argc < 2) {
        usage();
        return -1;
    }
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        if (!strcmp(a, "-h") || !strcmp(a, "--help")) {
            usage();
            return 1;
        } else if (!strcmp(a, "--send")) {
            o->mode = MODE_SEND;
        } else if (!strcmp(a, "--receive")) {
            o->mode = MODE_RECV;
        } else if (!strcmp(a, "--port")) {
            long v;
            if (++i >= argc) {
                fprintf(stderr, "error: --port needs a value\n");
                return -1;
            }
            v = parse_long(argv[i], 1, 65535);
            if (v < 0) {
                fprintf(stderr, "error: invalid port: %s\n", argv[i]);
                return -1;
            }
            o->port = (uint16_t)v;
            o->have_port = 1;
        } else if (!strcmp(a, "--ip")) {
            if (++i >= argc) {
                fprintf(stderr, "error: --ip needs a value\n");
                return -1;
            }
            o->ip = argv[i];
        } else if (!strcmp(a, "--apikey")) {
            size_t kl;
            if (++i >= argc) {
                fprintf(stderr, "error: --apikey needs a value\n");
                return -1;
            }
            kl = strlen(argv[i]);
            if (kl == 0 || kl > MAX_APIKEY_LEN) {
                fprintf(stderr, "error: apikey length must be 1..%d\n", MAX_APIKEY_LEN);
                return -1;
            }
            o->apikey = argv[i];
        } else if (!strcmp(a, "-c") || !strcmp(a, "--dir")) {
            if (++i >= argc) {
                fprintf(stderr, "error: -c needs a value\n");
                return -1;
            }
            normalize_path(argv[i]);
            o->dl_dir = argv[i];
        } else if (!strcmp(a, "-t") || !strcmp(a, "--threads")) {
            long v;
            if (++i >= argc) {
                fprintf(stderr, "error: -t needs a value\n");
                return -1;
            }
            v = parse_long(argv[i], 1, MAX_THREADS);
            if (v < 0) {
                fprintf(stderr, "error: thread count must be 1..%d\n", MAX_THREADS);
                return -1;
            }
            o->threads = (int)v;
        } else if (a[0] == '-' && a[1] != '\0') {
            fprintf(stderr, "error: unknown option: %s\n", a);
            return -1;
        } else {
            if (o->npaths >= (int)(sizeof o->paths / sizeof o->paths[0])) {
                fprintf(stderr, "error: too many input paths\n");
                return -1;
            }
            o->paths[o->npaths++] = argv[i];
        }
    }

    if (o->mode == MODE_NONE) {
        fprintf(stderr, "error: specify --send or --receive\n");
        return -1;
    }
    if (!o->have_port) {
        fprintf(stderr, "error: --port is required\n");
        return -1;
    }
    if (!o->apikey) {
        fprintf(stderr, "error: --apikey is required\n");
        return -1;
    }
    if (o->mode == MODE_SEND && o->npaths == 0) {
        fprintf(stderr, "error: --send needs at least one file or directory\n");
        return -1;
    }
    if (o->mode == MODE_RECV) {
        if (!o->ip) {
            fprintf(stderr, "error: --receive needs --ip <sender-ip>\n");
            return -1;
        }
        if (!o->dl_dir) o->dl_dir = ".";
    }
    return 0;
}

/* ======================== main ======================== */

#ifdef _WIN32
static void wsa_cleanup(void)
{
    WSACleanup();
}
#endif

int main(int argc, char **argv)
{
#ifdef _WIN32
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        fprintf(stderr, "error: WSAStartup failed\n");
        return 1;
    }
    atexit(wsa_cleanup);
#endif

    signal(SIGINT, on_sigint);
#ifndef _WIN32
    signal(SIGPIPE, SIG_IGN);
#endif
    setvbuf(stdout, NULL, _IONBF, 0);

    /* FILECOPY_TRACE=1 打开逐包 send/recv 调试日志（默认关闭） */
    {
        const char *tr = getenv("FILECOPY_TRACE");
        if (tr && tr[0] && strcmp(tr, "0") != 0) {
            g_trace = 1;
            fprintf(stdout, "[net] trace mode on: sndbuf/rcvbuf %d KiB, io unit %d KiB\n",
                    NET_BUF_BYTES / 1024, NET_IO_UNIT / 1024);
        }
    }

    options opt;
    memset(&opt, 0, sizeof opt);
    opt.threads = DEFAULT_THREADS;

    int prc = parse_options(argc, argv, &opt);
    if (prc > 0) return 0;   /* 打印了帮助 */
    if (prc < 0) return 2;   /* 参数错误 */

    if (opt.mode == MODE_SEND) return run_send(&opt);
    return run_receive(&opt);
}
