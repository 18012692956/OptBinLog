#include "optbinlog_shared.h"
#include "optbinlog_eventlog.h"
#include "optbinlog_binlog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>

typedef enum {
    MODE_TEXT = 0,
    MODE_CSV = 1,
    MODE_JSONL = 2,
} TextLikeMode;

typedef struct {
    uint64_t ts_ns;
    uint32_t seq;
    uint32_t code;
    int32_t temp_x10;
    uint16_t tag;
    uint16_t level;
} NanoPackedRecord;

static void usage(const char* prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s --mode text|csv|jsonl|binary|syslog|ftrace|nanolog_like|zephyr_deferred_like|nanolog_semantic_like|zephyr_deferred_semantic_like --eventlog-dir <dir> --out <file> --records N [--shared <file>] [--strict-perm]\n",
        prog
    );
}

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static long max_rss_kb(void) {
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) != 0) return -1;
#if defined(__APPLE__)
    return ru.ru_maxrss / 1024; /* bytes -> KB */
#else
    return ru.ru_maxrss; /* already KB on Linux */
#endif
}

static uint64_t xorshift64(uint64_t* state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static uint64_t max_for_bits(int bits) {
    if (bits >= 64) return UINT64_MAX;
    return (1ULL << bits) - 1ULL;
}

static uint32_t crc32_update(uint32_t crc, uint8_t b) {
    crc ^= b;
    for (int i = 0; i < 8; i++) {
        uint32_t mask = (uint32_t)(-(int32_t)(crc & 1u));
        crc = (crc >> 1) ^ (0xEDB88320u & mask);
    }
    return crc;
}

static uint32_t crc32_compute(const uint8_t* data, size_t len) {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        crc = crc32_update(crc, data[i]);
    }
    return ~crc;
}

static void write_le16(uint8_t* dst, uint16_t v) {
    dst[0] = (uint8_t)(v & 0xFFu);
    dst[1] = (uint8_t)((v >> 8) & 0xFFu);
}

static void write_le32(uint8_t* dst, uint32_t v) {
    dst[0] = (uint8_t)(v & 0xFFu);
    dst[1] = (uint8_t)((v >> 8) & 0xFFu);
    dst[2] = (uint8_t)((v >> 16) & 0xFFu);
    dst[3] = (uint8_t)((v >> 24) & 0xFFu);
}

static NanoPackedRecord make_nano_record(long i, uint64_t* rnd) {
    NanoPackedRecord r;
    r.ts_ns = now_ns();
    r.seq = (uint32_t)(xorshift64(rnd) & 0xFFFFFFFFu);
    r.code = (uint32_t)(1000u + (xorshift64(rnd) % 250u));
    r.temp_x10 = (int32_t)(200 + (int32_t)(xorshift64(rnd) % 80u));
    r.tag = (uint16_t)(3000 + (i % 32));
    r.level = (uint16_t)(i % 8);
    return r;
}

static size_t encode_semantic_payload(uint8_t* out, const NanoPackedRecord* r) {
    size_t off = 0;
    memcpy(out + off, &r->ts_ns, sizeof(uint64_t));
    off += sizeof(uint64_t);
    write_le16(out + off, r->tag);
    off += sizeof(uint16_t);
    out[off++] = 4u; /* ele_count */
    write_le32(out + off, r->seq);
    off += sizeof(uint32_t);
    write_le32(out + off, r->code);
    off += sizeof(uint32_t);
    write_le32(out + off, (uint32_t)r->temp_x10);
    off += sizeof(uint32_t);
    write_le16(out + off, r->level);
    off += sizeof(uint16_t);
    return off;
}

static size_t encode_semantic_frame(uint8_t* out, const NanoPackedRecord* r) {
    uint8_t payload[64];
    size_t payload_len = encode_semantic_payload(payload, r);
    write_le32(out, (uint32_t)payload_len);
    memcpy(out + 4, payload, payload_len);
    uint32_t crc = crc32_compute(payload, payload_len);
    write_le32(out + 4 + payload_len, crc);
    return 4 + payload_len + 4;
}

static int appendf(char** p, size_t* left, const char* fmt, ...) {
    if (*left == 0) return -1;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(*p, *left, fmt, ap);
    va_end(ap);
    if (n < 0) return -1;
    if ((size_t)n >= *left) return -1;
    *p += n;
    *left -= (size_t)n;
    return 0;
}

static int format_plain_text_payload(long i, uint64_t* rnd, char* out, size_t cap) {
    uint64_t seq = xorshift64(rnd);
    uint64_t code = xorshift64(rnd) % 100000ULL;
    double temp = (double)(xorshift64(rnd) % 8000ULL) / 100.0;
    const char* lvl = (seq % 10ULL == 0ULL) ? "W" : "I";
    return snprintf(
               out,
               cap,
               "ts=%lld lvl=%s module=bench dev=dev-%02lld seq=%llu code=%llu temp=%.2f msg=\"plain text log\"",
               (long long)(1710000000 + i),
               lvl,
               (long long)(i % 100),
               (unsigned long long)seq,
               (unsigned long long)code,
               temp) < (int)cap
               ? 0
               : -1;
}

typedef struct {
    char** items;
    size_t len;
    size_t cap;
} SyslogLineList;

static void syslog_line_list_init(SyslogLineList* list) {
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

static void syslog_line_list_free(SyslogLineList* list) {
    if (!list) return;
    for (size_t i = 0; i < list->len; i++) {
        free(list->items[i]);
    }
    free(list->items);
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

static int syslog_line_list_push(SyslogLineList* list, const char* line) {
    if (list->len == list->cap) {
        size_t next_cap = list->cap ? list->cap * 2 : 16;
        char** next = realloc(list->items, next_cap * sizeof(char*));
        if (!next) return -1;
        list->items = next;
        list->cap = next_cap;
    }
    list->items[list->len] = strdup(line);
    if (!list->items[list->len]) return -1;
    list->len++;
    return 0;
}

static char* trim_ascii(char* s) {
    while (*s == ' ' || *s == '\t' || *s == '\r' || *s == '\n') s++;
    char* end = s + strlen(s);
    while (end > s && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\r' || end[-1] == '\n')) {
        end--;
    }
    *end = '\0';
    return s;
}

static int load_syslog_lines_from_file(const char* path, SyslogLineList* out) {
    FILE* fp = fopen(path, "rb");
    if (!fp) return -1;
    char line[1024];
    int rc = 0;
    while (fgets(line, sizeof(line), fp)) {
        char* s = trim_ascii(line);
        if (!s[0] || s[0] == '#') continue;
        if (syslog_line_list_push(out, s) != 0) {
            rc = -1;
            break;
        }
    }
    fclose(fp);
    return rc;
}

static int load_syslog_lines(const char* eventlog_dir, SyslogLineList* out) {
    static const char* fallback[] = {
        "kernel: thermal throttling event cleared on cpu0",
        "sshd[2421]: Accepted password for ops from 10.10.8.12 port 52240 ssh2",
        "systemd[1]: Started Session 42 of user service.",
        "cron[841]: (root) CMD (run-parts /etc/cron.hourly)",
        "nginx[1392]: 10.1.2.3 - - \"GET /healthz HTTP/1.1\" 200 2",
        "dockerd[553]: container 6f9c4f2d restarting due to healthcheck failure",
    };

    syslog_line_list_init(out);
    const char* source = getenv("OPTBINLOG_SYSLOG_SOURCE");
    if (!source || !source[0]) {
        size_t n = strlen(eventlog_dir) + strlen("/syslog_messages.log") + 1;
        char* local = malloc(n);
        if (!local) return -1;
        snprintf(local, n, "%s/syslog_messages.log", eventlog_dir);
        int rc = load_syslog_lines_from_file(local, out);
        free(local);
        if (rc == 0 && out->len > 0) return 0;
    } else {
        int rc = load_syslog_lines_from_file(source, out);
        if (rc == 0 && out->len > 0) return 0;
    }

    for (size_t i = 0; i < sizeof(fallback) / sizeof(fallback[0]); i++) {
        if (syslog_line_list_push(out, fallback[i]) != 0) {
            syslog_line_list_free(out);
            return -1;
        }
    }
    return 0;
}

static void derive_ftrace_read_path(const char* trace_path, char* out, size_t out_cap) {
    if (!trace_path || !trace_path[0]) {
        snprintf(out, out_cap, "%s", "/sys/kernel/debug/tracing/trace");
        return;
    }
    const char* marker = "/trace_marker";
    size_t n = strlen(trace_path);
    size_t m = strlen(marker);
    if (n > m && strcmp(trace_path + n - m, marker) == 0) {
        size_t base = n - m;
        if (base + strlen("/trace") + 1 > out_cap) {
            snprintf(out, out_cap, "%s", trace_path);
            return;
        }
        memcpy(out, trace_path, base);
        out[base] = '\0';
        strcat(out, "/trace");
        return;
    }
    snprintf(out, out_cap, "%s", trace_path);
}

static void derive_ftrace_tracing_on_path(const char* trace_path, char* out, size_t out_cap) {
    if (!trace_path || !trace_path[0]) {
        snprintf(out, out_cap, "%s", "/sys/kernel/debug/tracing/tracing_on");
        return;
    }
    const char* marker = "/trace_marker";
    size_t n = strlen(trace_path);
    size_t m = strlen(marker);
    if (n > m && strcmp(trace_path + n - m, marker) == 0) {
        size_t base = n - m;
        if (base + strlen("/tracing_on") + 1 > out_cap) {
            snprintf(out, out_cap, "%s", "/sys/kernel/debug/tracing/tracing_on");
            return;
        }
        memcpy(out, trace_path, base);
        out[base] = '\0';
        strcat(out, "/tracing_on");
        return;
    }
    snprintf(out, out_cap, "%s", "/sys/kernel/debug/tracing/tracing_on");
}

static int read_tracing_on(const char* path) {
    FILE* fp = fopen(path, "rb");
    if (!fp) return -1;
    int ch = fgetc(fp);
    fclose(fp);
    if (ch == '0') return 0;
    if (ch == '1') return 1;
    return -1;
}

static int write_tracing_on(const char* path, int on) {
    int fd = open(path, O_WRONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    const char v = on ? '1' : '0';
    ssize_t n = write(fd, &v, 1);
    close(fd);
    return n == 1 ? 0 : -1;
}

static unsigned long long scan_ftrace_observed_bytes(const char* trace_read_path, const char* token) {
    if (!trace_read_path || !token || !token[0]) return 0;
    FILE* fp = fopen(trace_read_path, "rb");
    if (!fp) return 0;

    unsigned long long total = 0;
    char* line = NULL;
    size_t cap = 0;
    ssize_t n = 0;
    while ((n = getline(&line, &cap, fp)) != -1) {
        if (strstr(line, token) != NULL) {
            total += (unsigned long long)n;
        }
    }
    free(line);
    fclose(fp);
    return total;
}

static void clear_ftrace_trace(const char* trace_read_path) {
    if (!trace_read_path || !trace_read_path[0]) return;
    int fd = open(trace_read_path, O_WRONLY | O_TRUNC | O_CLOEXEC);
    if (fd < 0) return;
    close(fd);
}

static int format_text_payload(const OptbinlogTagDef* tag, long i, uint64_t* rnd, char* out, size_t cap) {
    char* p = out;
    size_t left = cap;

    if (appendf(&p, &left, "ts=%lld id=%d name=%s ", (long long)(1710000000 + i), tag->tag_id, tag->name) != 0) {
        return -1;
    }
    for (int e = 0; e < tag->ele_num; e++) {
        const OptbinlogTagEleDef* ele = &tag->eles[e];
        if (e > 0) {
            if (appendf(&p, &left, ",") != 0) return -1;
        }
        if (ele->type_char == 'L') {
            uint64_t v = xorshift64(rnd) & max_for_bits(ele->bits);
            if (appendf(&p, &left, "%s=%llu", ele->name, (unsigned long long)v) != 0) return -1;
        } else if (ele->type_char == 'D') {
            double v = (double)(xorshift64(rnd) % 10000) / 100.0;
            if (appendf(&p, &left, "%s=%.2f", ele->name, v) != 0) return -1;
        } else if (ele->type_char == 'S') {
            char buf[32];
            snprintf(buf, sizeof(buf), "dev-%02ld", i % 100);
            if (appendf(&p, &left, "%s=\"%s\"", ele->name, buf) != 0) return -1;
        }
    }
    return 0;
}

static int write_text_record(FILE* fp, const OptbinlogTagDef* tag, long i, uint64_t* rnd) {
    char line[1024];
    if (format_text_payload(tag, i, rnd, line, sizeof(line)) != 0) return -1;
    fputs(line, fp);
    fputc('\n', fp);
    return 0;
}

static int write_csv_record(FILE* fp, const OptbinlogTagDef* tag, long i, uint64_t* rnd) {
    fprintf(fp, "%lld,%d,%s", (long long)(1710000000 + i), tag->tag_id, tag->name);
    for (int e = 0; e < tag->ele_num; e++) {
        const OptbinlogTagEleDef* ele = &tag->eles[e];
        if (ele->type_char == 'L') {
            uint64_t v = xorshift64(rnd) & max_for_bits(ele->bits);
            fprintf(fp, ",%llu", (unsigned long long)v);
        } else if (ele->type_char == 'D') {
            double v = (double)(xorshift64(rnd) % 10000) / 100.0;
            fprintf(fp, ",%.2f", v);
        } else if (ele->type_char == 'S') {
            char buf[32];
            snprintf(buf, sizeof(buf), "dev-%02ld", i % 100);
            fprintf(fp, ",\"%s\"", buf);
        }
    }
    fputc('\n', fp);
    return 0;
}

static int write_jsonl_record(FILE* fp, const OptbinlogTagDef* tag, long i, uint64_t* rnd) {
    fprintf(fp, "{\"ts\":%lld,\"id\":%d,\"name\":\"%s\",\"values\":{", (long long)(1710000000 + i), tag->tag_id, tag->name);
    for (int e = 0; e < tag->ele_num; e++) {
        const OptbinlogTagEleDef* ele = &tag->eles[e];
        if (e > 0) fputc(',', fp);
        fprintf(fp, "\"%s\":", ele->name);
        if (ele->type_char == 'L') {
            uint64_t v = xorshift64(rnd) & max_for_bits(ele->bits);
            fprintf(fp, "%llu", (unsigned long long)v);
        } else if (ele->type_char == 'D') {
            double v = (double)(xorshift64(rnd) % 10000) / 100.0;
            fprintf(fp, "%.2f", v);
        } else if (ele->type_char == 'S') {
            char buf[32];
            snprintf(buf, sizeof(buf), "dev-%02ld", i % 100);
            fprintf(fp, "\"%s\"", buf);
        }
    }
    fprintf(fp, "}}\n");
    return 0;
}

static int bench_textlike(const char* mode_name, TextLikeMode mode, const char* eventlog_dir, const char* out_path, long records) {
    uint64_t t_e2e0 = now_ns();

    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    int need_tags = (mode != MODE_TEXT);
    if (need_tags) {
        if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0 || tags.len == 0) {
            fprintf(stderr, "no tags parsed\n");
            optbinlog_taglist_free(&tags);
            return -1;
        }
    }

    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        optbinlog_taglist_free(&tags);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL;
    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        int rc = 0;
        if (mode == MODE_TEXT) {
            char line[1024];
            rc = format_plain_text_payload(i, &rnd, line, sizeof(line));
            if (rc == 0) {
                fputs(line, fp);
                fputc('\n', fp);
            }
        } else {
            OptbinlogTagDef* tag = &tags.items[i % tags.len];
            if (mode == MODE_CSV) rc = write_csv_record(fp, tag, i, &rnd);
            else if (mode == MODE_JSONL) rc = write_jsonl_record(fp, tag, i, &rnd);
        }
        if (rc != 0) {
            fclose(fp);
            optbinlog_taglist_free(&tags);
            fprintf(stderr, "format/write failed\n");
            return -1;
        }
    }
    uint64_t t_write1 = now_ns();

    fclose(fp);
    optbinlog_taglist_free(&tags);

    uint64_t t_e2e1 = now_ns();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;

    struct stat st;
    if (stat(out_path, &st) != 0) {
        fprintf(stderr, "stat failed\n");
        return -1;
    }
    long rss = max_rss_kb();
    printf("mode,%s,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%lld,shared_bytes,0,total_bytes,%lld,peak_kb,%ld\n",
           mode_name, records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms, (long long)st.st_size, (long long)st.st_size, rss);
    return 0;
}

static int bench_syslog(const char* eventlog_dir, long records) {
    uint64_t t_e2e0 = now_ns();

    SyslogLineList lines;
    if (load_syslog_lines(eventlog_dir, &lines) != 0 || lines.len == 0) {
        fprintf(stderr, "no syslog lines loaded\n");
        return -1;
    }

    int prio = LOG_DEBUG;
    const char* prio_env = getenv("OPTBINLOG_SYSLOG_PRIO");
    if (prio_env && prio_env[0]) {
        prio = atoi(prio_env);
    }

    openlog("optbinlog_bench", LOG_NDELAY, LOG_USER);
    uint64_t bytes = 0;

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        const char* line = lines.items[(size_t)(i % (long)lines.len)];
        syslog(prio, "%s", line);
        bytes += (uint64_t)strlen(line) + 1;
    }
    uint64_t t_write1 = now_ns();

    closelog();
    syslog_line_list_free(&lines);

    uint64_t t_e2e1 = now_ns();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    long rss = max_rss_kb();

    printf("mode,syslog,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_ftrace(const char* eventlog_dir, long records) {
    SyslogLineList lines;
    if (load_syslog_lines(eventlog_dir, &lines) != 0 || lines.len == 0) {
        fprintf(stderr, "no ftrace lines loaded\n");
        return -1;
    }

    const char* trace_path = getenv("OPTBINLOG_TRACE_MARKER");
    if (!trace_path || !trace_path[0]) {
        trace_path = "/sys/kernel/debug/tracing/trace_marker";
    }
    char tracing_on_path[PATH_MAX];
    derive_ftrace_tracing_on_path(trace_path, tracing_on_path, sizeof(tracing_on_path));
    int tracing_prev = read_tracing_on(tracing_on_path);
    (void)write_tracing_on(tracing_on_path, 1);

    int fd = open(trace_path, O_WRONLY | O_CLOEXEC);
    if (fd < 0) {
        fprintf(stderr, "open trace_marker failed: %s\n", strerror(errno));
        if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
        syslog_line_list_free(&lines);
        return -1;
    }

    char token[96];
    snprintf(token, sizeof(token), "OBENCH_FTRACE_%d_%llu",
             (int)getpid(), (unsigned long long)now_ns());
    char trace_read_path[PATH_MAX];
    derive_ftrace_read_path(trace_path, trace_read_path, sizeof(trace_read_path));
    clear_ftrace_trace(trace_read_path);

    uint64_t bytes_payload = 0;
    uint64_t t_e2e0 = now_ns();

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        const char* line = lines.items[(size_t)(i % (long)lines.len)];
        char buf[1400];
        int nn = snprintf(buf, sizeof(buf), "%s %s", token, line);
        if (nn < 0 || (size_t)nn >= sizeof(buf)) {
            close(fd);
            if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
            syslog_line_list_free(&lines);
            return -1;
        }
        size_t len = (size_t)nn;
        if (write(fd, buf, len) != (ssize_t)len || write(fd, "\n", 1) != 1) {
            fprintf(stderr, "write trace_marker failed: %s\n", strerror(errno));
            close(fd);
            if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
            syslog_line_list_free(&lines);
            return -1;
        }
        bytes_payload += (uint64_t)len + 1;
    }
    uint64_t t_write1 = now_ns();

    close(fd);
    if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
    syslog_line_list_free(&lines);

    uint64_t t_e2e1 = now_ns();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    long rss = max_rss_kb();
    unsigned long long bytes_observed = scan_ftrace_observed_bytes(trace_read_path, token);
    unsigned long long bytes = (unsigned long long)bytes_payload;
    if (bytes_observed > bytes) {
        bytes = bytes_observed;
    }

    printf("mode,ftrace,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_nanolog_like(const char* out_path, long records) {
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    uint64_t t_write0 = now_ns();
    NanoPackedRecord* staging = malloc((size_t)records * sizeof(NanoPackedRecord));
    if (!staging) {
        fclose(fp);
        fprintf(stderr, "OOM\n");
        return -1;
    }
    uint64_t rnd = 0x123456789abcdef0ULL;
    for (long i = 0; i < records; i++) {
        staging[i] = make_nano_record(i, &rnd);
    }
    size_t written = fwrite(staging, sizeof(NanoPackedRecord), (size_t)records, fp);
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free(staging);
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    uint64_t bytes = (uint64_t)written * (uint64_t)sizeof(NanoPackedRecord);
    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,nanolog_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_zephyr_deferred_like(const char* out_path, long records) {
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    const size_t batch_cap = 1024;
    NanoPackedRecord* batch = malloc(batch_cap * sizeof(NanoPackedRecord));
    if (!batch) {
        fclose(fp);
        fprintf(stderr, "OOM\n");
        return -1;
    }

    uint64_t bytes = 0;
    uint64_t rnd = 0x123456789abcdef0ULL;
    size_t n_batch = 0;

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        batch[n_batch++] = make_nano_record(i, &rnd);
        if (n_batch == batch_cap) {
            size_t n = fwrite(batch, sizeof(NanoPackedRecord), n_batch, fp);
            bytes += (uint64_t)n * (uint64_t)sizeof(NanoPackedRecord);
            n_batch = 0;
        }
    }
    if (n_batch > 0) {
        size_t n = fwrite(batch, sizeof(NanoPackedRecord), n_batch, fp);
        bytes += (uint64_t)n * (uint64_t)sizeof(NanoPackedRecord);
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free(batch);
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,zephyr_deferred_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_nanolog_semantic_like(const char* out_path, long records) {
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    size_t cap = (size_t)records * 40u;
    uint8_t* staging = malloc(cap);
    if (!staging) {
        fclose(fp);
        fprintf(stderr, "OOM\n");
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL;
    size_t off = 0;
    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        NanoPackedRecord r = make_nano_record(i, &rnd);
        off += encode_semantic_frame(staging + off, &r);
    }
    size_t written = fwrite(staging, 1, off, fp);
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free(staging);
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,nanolog_semantic_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)written, (unsigned long long)written, rss);
    return 0;
}

static int bench_zephyr_deferred_semantic_like(const char* out_path, long records) {
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    const size_t batch_cap = 1024;
    uint8_t* batch = malloc(batch_cap * 40u);
    if (!batch) {
        fclose(fp);
        fprintf(stderr, "OOM\n");
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL;
    uint64_t bytes = 0;
    size_t off = 0;
    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        NanoPackedRecord r = make_nano_record(i, &rnd);
        size_t n = encode_semantic_frame(batch + off, &r);
        off += n;
        if (off + 40u > batch_cap * 40u) {
            size_t w = fwrite(batch, 1, off, fp);
            bytes += (uint64_t)w;
            off = 0;
        }
    }
    if (off > 0) {
        size_t w = fwrite(batch, 1, off, fp);
        bytes += (uint64_t)w;
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free(batch);
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,zephyr_deferred_semantic_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_binary(const char* eventlog_dir, const char* shared_path, const char* out_path, long records, int strict_perm) {
    uint64_t t_e2e0 = now_ns();

    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0 || tags.len == 0) {
        fprintf(stderr, "no tags parsed\n");
        optbinlog_taglist_free(&tags);
        return -1;
    }

    if (optbinlog_shared_init_from_dir(eventlog_dir, shared_path, strict_perm) != 0) {
        fprintf(stderr, "shared init failed\n");
        optbinlog_taglist_free(&tags);
        return -1;
    }

    OptbinlogRecord* recs = calloc((size_t)records, sizeof(OptbinlogRecord));
    if (!recs) {
        fprintf(stderr, "OOM\n");
        optbinlog_taglist_free(&tags);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL;
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        OptbinlogValue* values = calloc((size_t)tag->ele_num, sizeof(OptbinlogValue));
        if (!values) {
            fprintf(stderr, "OOM\n");
            optbinlog_taglist_free(&tags);
            return -1;
        }
        for (int e = 0; e < tag->ele_num; e++) {
            OptbinlogTagEleDef* ele = &tag->eles[e];
            if (ele->type_char == 'L') {
                uint64_t v = xorshift64(&rnd) & max_for_bits(ele->bits);
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_U, v, 0.0, NULL};
            } else if (ele->type_char == 'D') {
                double v = (double)(xorshift64(&rnd) % 10000) / 100.0;
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_D, 0, v, NULL};
            } else if (ele->type_char == 'S') {
                char* buf = malloc(16);
                if (!buf) {
                    fprintf(stderr, "OOM\n");
                    optbinlog_taglist_free(&tags);
                    return -1;
                }
                snprintf(buf, 16, "dev-%02ld", i % 100);
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_S, 0, 0.0, buf};
            }
        }
        recs[i].timestamp = 1710000000 + i;
        recs[i].tag_id = tag->tag_id;
        recs[i].ele_count = tag->ele_num;
        recs[i].values = values;
    }

    uint64_t t_write0 = now_ns();
    int rc = optbinlog_binlog_write(shared_path, out_path, recs, (size_t)records);
    uint64_t t_write1 = now_ns();

    for (long i = 0; i < records; i++) {
        for (int e = 0; e < recs[i].ele_count; e++) {
            if (recs[i].values[e].kind == OPTBINLOG_VAL_S) {
                free((void*)recs[i].values[e].s);
            }
        }
        free(recs[i].values);
    }
    free(recs);
    optbinlog_taglist_free(&tags);

    if (rc != 0) return -1;

    uint64_t t_e2e1 = now_ns();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;

    struct stat st;
    if (stat(out_path, &st) != 0) {
        fprintf(stderr, "stat failed\n");
        return -1;
    }
    long rss = max_rss_kb();
    struct stat st_shared;
    long long shared_bytes = 0;
    if (stat(shared_path, &st_shared) == 0) {
        shared_bytes = (long long)st_shared.st_size;
    }
    printf("mode,binary,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%lld,shared_bytes,%lld,total_bytes,%lld,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (long long)st.st_size, shared_bytes, (long long)st.st_size + shared_bytes, rss);
    return 0;
}

int main(int argc, char** argv) {
    const char* mode = NULL;
    const char* eventlog_dir = NULL;
    const char* out_path = NULL;
    const char* shared_path = OPTBINLOG_EVENTTAG_FILENAME;
    long records = 10000;
    int strict_perm = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            mode = argv[++i];
        } else if (strcmp(argv[i], "--eventlog-dir") == 0 && i + 1 < argc) {
            eventlog_dir = argv[++i];
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            out_path = argv[++i];
        } else if (strcmp(argv[i], "--records") == 0 && i + 1 < argc) {
            records = atol(argv[++i]);
        } else if (strcmp(argv[i], "--shared") == 0 && i + 1 < argc) {
            shared_path = argv[++i];
        } else if (strcmp(argv[i], "--strict-perm") == 0) {
            strict_perm = 1;
        }
    }

    if (!mode || !eventlog_dir || records <= 0) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(mode, "text") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_textlike("text", MODE_TEXT, eventlog_dir, out_path, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "csv") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_textlike("csv", MODE_CSV, eventlog_dir, out_path, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "jsonl") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_textlike("jsonl", MODE_JSONL, eventlog_dir, out_path, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "binary") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_binary(eventlog_dir, shared_path, out_path, records, strict_perm) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "syslog") == 0) {
        return bench_syslog(eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "ftrace") == 0) {
        return bench_ftrace(eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "nanolog_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_nanolog_like(out_path, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "zephyr_deferred_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_zephyr_deferred_like(out_path, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "nanolog_semantic_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_nanolog_semantic_like(out_path, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "zephyr_deferred_semantic_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_zephyr_deferred_semantic_like(out_path, records) == 0 ? 0 : 1;
    }

    usage(argv[0]);
    return 1;
}
