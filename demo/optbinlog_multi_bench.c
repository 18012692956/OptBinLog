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
#include <sys/wait.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
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

static void usage(const char* prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s --mode text|binary|syslog|ftrace|nanolog_like|zephyr_like|zephyr_deferred_like|ulog_async_like|hilog_lite_like --eventlog-dir <dir> --out-dir <dir> --devices N --records-per-device N [--shared <file>] [--strict-perm]\n",
        prog
    );
}

typedef struct {
    uint64_t ts_ns;
    uint32_t seq;
    uint32_t code;
    int32_t temp_x10;
    uint16_t tag;
    uint16_t level;
} NanoPackedRecord;

typedef struct {
    uint32_t sec;
    uint32_t nsec;
    uint16_t domain;
    uint16_t tag;
    uint8_t level;
    uint8_t reserved;
    uint16_t msg_len;
} HiLogLiteHeader;

static NanoPackedRecord make_nano_record(long i, int device_id, uint64_t* rnd) {
    NanoPackedRecord r;
    r.ts_ns = now_ns();
    r.seq = (uint32_t)(xorshift64(rnd) & 0xFFFFFFFFu);
    r.code = (uint32_t)(1000u + (xorshift64(rnd) % 250u));
    r.temp_x10 = (int32_t)(200 + (int32_t)(xorshift64(rnd) % 80u));
    r.tag = (uint16_t)(3000 + ((device_id + (int)i) % 32));
    r.level = (uint16_t)(i % 8);
    return r;
}

static int ensure_dir(const char* path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) return 0;
        return -1;
    }
    if (mkdir(path, 0755) != 0) return -1;
    return 0;
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

static int text_profile_semantic_enabled(void) {
    const char* profile = getenv("OPTBINLOG_TEXT_PROFILE");
    if (!profile || !profile[0]) return 0;
    return strncmp(profile, "semantic", 8) == 0;
}

static int format_plain_text_payload(long i, int device_id, uint64_t* rnd, char* out, size_t cap) {
    uint64_t seq = xorshift64(rnd);
    uint64_t code = xorshift64(rnd) % 100000ULL;
    double temp = (double)(xorshift64(rnd) % 8000ULL) / 100.0;
    const char* lvl = (seq % 10ULL == 0ULL) ? "W" : "I";
    int semantic = text_profile_semantic_enabled();
    if (semantic) {
        return snprintf(
                   out,
                   cap,
                   "ts=%lld level=%s seq=%llu code=%llu temp=%.2f",
                   (long long)(1710000000 + i),
                   lvl,
                   (unsigned long long)seq,
                   (unsigned long long)code,
                   temp) < (int)cap
                   ? 0
                   : -1;
    }
    return snprintf(
               out,
               cap,
               "ts=%lld lvl=%s module=multi dev=dev-%02d seq=%llu code=%llu temp=%.2f msg=\"plain text log\"",
               (long long)(1710000000 + i),
               lvl,
               device_id,
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

static int format_text_payload(const OptbinlogTagDef* tag, long i, int device_id, uint64_t* rnd, char* out, size_t cap) {
    char* p = out;
    size_t left = cap;
    if (appendf(&p, &left, "ts=%lld id=%d name=%s ", (long long)(1710000000 + i), tag->tag_id, tag->name) != 0) {
        return -1;
    }
    for (int e = 0; e < tag->ele_num; e++) {
        const OptbinlogTagEleDef* ele = &tag->eles[e];
        if (e > 0 && appendf(&p, &left, ",") != 0) return -1;
        if (ele->type_char == 'L') {
            uint64_t v = xorshift64(rnd) & max_for_bits(ele->bits);
            if (appendf(&p, &left, "%s=%llu", ele->name, (unsigned long long)v) != 0) return -1;
        } else if (ele->type_char == 'D') {
            double v = (double)(xorshift64(rnd) % 10000) / 100.0;
            if (appendf(&p, &left, "%s=%.2f", ele->name, v) != 0) return -1;
        } else if (ele->type_char == 'S') {
            char buf[32];
            snprintf(buf, sizeof(buf), "dev-%02d", device_id);
            if (appendf(&p, &left, "%s=\"%s\"", ele->name, buf) != 0) return -1;
        }
    }
    return 0;
}

static int write_counter_file(const char* out_dir, int device_id, unsigned long long bytes) {
    char path[512];
    snprintf(path, sizeof(path), "%s/device_%02d.bytes", out_dir, device_id);
    FILE* fp = fopen(path, "wb");
    if (!fp) return -1;
    if (fprintf(fp, "%llu\n", bytes) < 0) {
        fclose(fp);
        return -1;
    }
    fclose(fp);
    return 0;
}

static int write_text_logs(const char* eventlog_dir, const char* out_dir, int device_id, long records) {
    char path[512];
    snprintf(path, sizeof(path), "%s/device_%02d.txt", out_dir, device_id);
    FILE* fp = fopen(path, "wb");
    if (!fp) {
        return -1;
    }

    int semantic = text_profile_semantic_enabled();
    SyslogLineList semantic_lines;
    int semantic_lines_loaded = 0;
    if (semantic) {
        if (load_syslog_lines(eventlog_dir, &semantic_lines) != 0 || semantic_lines.len == 0) {
            fclose(fp);
            return -1;
        }
        semantic_lines_loaded = 1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    for (long i = 0; i < records; i++) {
        int rc = 0;
        if (semantic) {
            const char* line = semantic_lines.items[(size_t)(i % (long)semantic_lines.len)];
            if (fputs(line, fp) == EOF || fputc('\n', fp) == EOF) {
                rc = -1;
            }
        } else {
            char line[1024];
            rc = format_plain_text_payload(i, device_id, &rnd, line, sizeof(line));
            if (rc == 0) {
                if (fputs(line, fp) == EOF || fputc('\n', fp) == EOF) {
                    rc = -1;
                }
            }
        }
        if (rc != 0) {
            fclose(fp);
            if (semantic_lines_loaded) syslog_line_list_free(&semantic_lines);
            return -1;
        }
    }

    fclose(fp);
    if (semantic_lines_loaded) syslog_line_list_free(&semantic_lines);
    return 0;
}

static int write_nanolog_like_logs(const char* out_dir, int device_id, long records) {
    char path[512];
    snprintf(path, sizeof(path), "%s/device_%02d.nlog", out_dir, device_id);
    FILE* fp = fopen(path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    const size_t batch_cap = 1024;
    NanoPackedRecord* batch = malloc(batch_cap * sizeof(NanoPackedRecord));
    if (!batch) {
        fclose(fp);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    size_t n_batch = 0;
    for (long i = 0; i < records; i++) {
        batch[n_batch++] = make_nano_record(i, device_id, &rnd);
        if (n_batch == batch_cap) {
            if (fwrite(batch, sizeof(NanoPackedRecord), n_batch, fp) != n_batch) {
                free(batch);
                fclose(fp);
                return -1;
            }
            n_batch = 0;
        }
    }
    if (n_batch > 0) {
        if (fwrite(batch, sizeof(NanoPackedRecord), n_batch, fp) != n_batch) {
            free(batch);
            fclose(fp);
            return -1;
        }
    }
    free(batch);
    fclose(fp);
    return 0;
}

static int write_zephyr_deferred_like_logs(const char* out_dir, int device_id, long records) {
    char path[512];
    snprintf(path, sizeof(path), "%s/device_%02d.zlog", out_dir, device_id);
    FILE* fp = fopen(path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    const size_t queue_cap = 256;
    NanoPackedRecord* queue = malloc(queue_cap * sizeof(NanoPackedRecord));
    if (!queue) {
        fclose(fp);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    size_t qn = 0;
    for (long i = 0; i < records; i++) {
        queue[qn++] = make_nano_record(i, device_id, &rnd);
        if (qn == queue_cap) {
            if (fwrite(queue, sizeof(NanoPackedRecord), qn, fp) != qn) {
                free(queue);
                fclose(fp);
                return -1;
            }
            qn = 0;
        }
    }
    if (qn > 0) {
        if (fwrite(queue, sizeof(NanoPackedRecord), qn, fp) != qn) {
            free(queue);
            fclose(fp);
            return -1;
        }
    }
    free(queue);
    fclose(fp);
    return 0;
}

static int write_ulog_async_like_logs(const char* out_dir, int device_id, long records) {
    char path[512];
    snprintf(path, sizeof(path), "%s/device_%02d.ulg", out_dir, device_id);
    FILE* fp = fopen(path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    enum { BATCH_CAP = 512 };
    char batch[BATCH_CAP][192];
    size_t batch_len[BATCH_CAP];
    size_t nb = 0;
    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    for (long i = 0; i < records; i++) {
        NanoPackedRecord r = make_nano_record(i, device_id, &rnd);
        int n = snprintf(batch[nb],
                         sizeof(batch[nb]),
                         "I/%u(%u): seq=%u code=%u temp=%d.%d ts=%llu\n",
                         (unsigned)r.tag,
                         (unsigned)r.level,
                         (unsigned)r.seq,
                         (unsigned)r.code,
                         (int)(r.temp_x10 / 10),
                         (int)abs(r.temp_x10 % 10),
                         (unsigned long long)r.ts_ns);
        if (n < 0 || (size_t)n >= sizeof(batch[nb])) {
            fclose(fp);
            return -1;
        }
        batch_len[nb++] = (size_t)n;
        if (nb == BATCH_CAP) {
            for (size_t j = 0; j < nb; j++) {
                if (fwrite(batch[j], 1, batch_len[j], fp) != batch_len[j]) {
                    fclose(fp);
                    return -1;
                }
            }
            nb = 0;
        }
    }
    for (size_t j = 0; j < nb; j++) {
        if (fwrite(batch[j], 1, batch_len[j], fp) != batch_len[j]) {
            fclose(fp);
            return -1;
        }
    }
    fclose(fp);
    return 0;
}

static int write_hilog_lite_like_logs(const char* out_dir, int device_id, long records) {
    char path[512];
    snprintf(path, sizeof(path), "%s/device_%02d.hlg", out_dir, device_id);
    FILE* fp = fopen(path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    for (long i = 0; i < records; i++) {
        NanoPackedRecord r = make_nano_record(i, device_id, &rnd);
        char msg[64];
        int n = snprintf(msg, sizeof(msg), "c=%u s=%u t=%d", r.code, r.seq, r.temp_x10);
        if (n <= 0 || (size_t)n > sizeof(msg)) {
            fclose(fp);
            return -1;
        }
        HiLogLiteHeader h;
        h.sec = (uint32_t)(r.ts_ns / 1000000000ull);
        h.nsec = (uint32_t)(r.ts_ns % 1000000000ull);
        h.domain = 0xD001u;
        h.tag = r.tag;
        h.level = (uint8_t)(r.level & 0x7u);
        h.reserved = 0u;
        h.msg_len = (uint16_t)n;
        if (fwrite(&h, sizeof(h), 1, fp) != 1 || fwrite(msg, 1, (size_t)n, fp) != (size_t)n) {
            fclose(fp);
            return -1;
        }
    }
    fclose(fp);
    return 0;
}

static int write_binary_logs(const char* eventlog_dir, const char* shared_path, const char* out_dir, int device_id, long records, int strict_perm) {
    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0 || tags.len == 0) {
        optbinlog_taglist_free(&tags);
        return -1;
    }

    (void)shared_path;
    (void)strict_perm;

    size_t total_values = 0;
    size_t total_string_fields = 0;
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        total_values += (size_t)tag->ele_num;
        for (int e = 0; e < tag->ele_num; e++) {
            if (tag->eles[e].type_char == 'S') {
                total_string_fields++;
            }
        }
    }

    OptbinlogRecord* recs = calloc((size_t)records, sizeof(OptbinlogRecord));
    OptbinlogValue* values_pool = calloc(total_values ? total_values : 1, sizeof(OptbinlogValue));
    char* string_pool = total_string_fields ? malloc(total_string_fields * 16u) : NULL;
    if (!recs || !values_pool || (total_string_fields && !string_pool)) {
        free(recs);
        free(values_pool);
        free(string_pool);
        optbinlog_taglist_free(&tags);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    size_t val_off = 0;
    size_t str_off = 0;
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        OptbinlogValue* values = values_pool + val_off;
        for (int e = 0; e < tag->ele_num; e++) {
            OptbinlogTagEleDef* ele = &tag->eles[e];
            if (ele->type_char == 'L') {
                uint64_t v = xorshift64(&rnd) & max_for_bits(ele->bits);
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_U, v, 0.0, NULL};
            } else if (ele->type_char == 'D') {
                double v = (double)(xorshift64(&rnd) % 10000) / 100.0;
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_D, 0, v, NULL};
            } else if (ele->type_char == 'S') {
                char* buf = string_pool + str_off * 16u;
                str_off++;
                snprintf(buf, 16, "dev-%02d", device_id);
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_S, 0, 0.0, buf};
            }
        }
        recs[i].timestamp = 1710000000 + i;
        recs[i].tag_id = tag->tag_id;
        recs[i].ele_count = tag->ele_num;
        recs[i].values = values;
        val_off += (size_t)tag->ele_num;
    }

    char path[512];
    snprintf(path, sizeof(path), "%s/device_%02d.bin", out_dir, device_id);
    int rc = optbinlog_binlog_write(shared_path, path, recs, (size_t)records);

    free(string_pool);
    free(values_pool);
    free(recs);
    optbinlog_taglist_free(&tags);
    return rc;
}

static int write_syslog_logs(const char* eventlog_dir, const char* out_dir, int device_id, long records) {
    SyslogLineList lines;
    if (load_syslog_lines(eventlog_dir, &lines) != 0 || lines.len == 0) {
        return -1;
    }

    int prio = LOG_DEBUG;
    const char* prio_env = getenv("OPTBINLOG_SYSLOG_PRIO");
    if (prio_env && prio_env[0]) prio = atoi(prio_env);

    openlog("optbinlog_multi_bench", LOG_NDELAY | LOG_PID, LOG_USER);
    unsigned long long bytes = 0;

    for (long i = 0; i < records; i++) {
        const char* line = lines.items[(size_t)(i % (long)lines.len)];
        syslog(prio, "%s", line);
        bytes += (unsigned long long)strlen(line) + 1ULL;
    }

    closelog();
    syslog_line_list_free(&lines);
    return write_counter_file(out_dir, device_id, bytes);
}

static int write_ftrace_logs(const char* eventlog_dir, const char* out_dir, int device_id, long records, const char* run_token) {
    SyslogLineList lines;
    if (load_syslog_lines(eventlog_dir, &lines) != 0 || lines.len == 0) {
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
        if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
        syslog_line_list_free(&lines);
        return -1;
    }

    (void)device_id;
    unsigned long long bytes = 0;
    for (long i = 0; i < records; i++) {
        const char* line = lines.items[(size_t)(i % (long)lines.len)];
        char buf[1536];
        int nn = snprintf(buf, sizeof(buf), "%s dev=%02d %s",
                          run_token ? run_token : "OBENCH_FTRACE", device_id, line);
        if (nn < 0 || (size_t)nn >= sizeof(buf)) {
            close(fd);
            if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
            syslog_line_list_free(&lines);
            return -1;
        }
        size_t len = (size_t)nn;
        if (write(fd, buf, len) != (ssize_t)len || write(fd, "\n", 1) != 1) {
            close(fd);
            if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
            syslog_line_list_free(&lines);
            return -1;
        }
        bytes += (unsigned long long)len + 1ULL;
    }

    close(fd);
    if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
    syslog_line_list_free(&lines);
    return write_counter_file(out_dir, device_id, bytes);
}

static long long sum_sizes(const char* out_dir, const char* ext) {
    long long total = 0;
    for (int d = 0;; d++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/device_%02d.%s", out_dir, d, ext);
        struct stat st;
        if (stat(path, &st) != 0) {
            if (d == 0) return -1;
            break;
        }
        total += (long long)st.st_size;
    }
    return total;
}

static long long sum_counters(const char* out_dir) {
    long long total = 0;
    for (int d = 0;; d++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/device_%02d.bytes", out_dir, d);
        FILE* fp = fopen(path, "rb");
        if (!fp) {
            if (d == 0) return -1;
            break;
        }
        unsigned long long v = 0;
        if (fscanf(fp, "%llu", &v) != 1) {
            fclose(fp);
            return -1;
        }
        fclose(fp);
        total += (long long)v;
    }
    return total;
}

int main(int argc, char** argv) {
    const char* mode = NULL;
    const char* eventlog_dir = NULL;
    const char* out_dir = NULL;
    const char* shared_path = OPTBINLOG_EVENTTAG_FILENAME;
    int devices = 10;
    long records = 1000;
    int strict_perm = 0;
    char ftrace_run_token[96] = {0};
    char ftrace_read_path[PATH_MAX] = {0};

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            mode = argv[++i];
        } else if (strcmp(argv[i], "--eventlog-dir") == 0 && i + 1 < argc) {
            eventlog_dir = argv[++i];
        } else if (strcmp(argv[i], "--out-dir") == 0 && i + 1 < argc) {
            out_dir = argv[++i];
        } else if (strcmp(argv[i], "--devices") == 0 && i + 1 < argc) {
            devices = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--records-per-device") == 0 && i + 1 < argc) {
            records = atol(argv[++i]);
        } else if (strcmp(argv[i], "--shared") == 0 && i + 1 < argc) {
            shared_path = argv[++i];
        } else if (strcmp(argv[i], "--strict-perm") == 0) {
            strict_perm = 1;
        }
    }

    if (!mode || !eventlog_dir || !out_dir) {
        usage(argv[0]);
        return 1;
    }
    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "failed to create out dir %s: %s\n", out_dir, strerror(errno));
        return 1;
    }

    uint64_t t0 = 0;
    if (strcmp(mode, "binary") == 0) {
        if (optbinlog_shared_init_from_dir(eventlog_dir, shared_path, strict_perm) != 0) {
            fprintf(stderr, "shared init failed\n");
            return 1;
        }
    } else if (strcmp(mode, "ftrace") == 0) {
        snprintf(ftrace_run_token, sizeof(ftrace_run_token),
                 "OBENCH_FTRACE_MULTI_%d_%llu",
                 (int)getpid(), (unsigned long long)now_ns());
        const char* trace_path = getenv("OPTBINLOG_TRACE_MARKER");
        if (!trace_path || !trace_path[0]) {
            trace_path = "/sys/kernel/debug/tracing/trace_marker";
        }
        derive_ftrace_read_path(trace_path, ftrace_read_path, sizeof(ftrace_read_path));
        clear_ftrace_trace(ftrace_read_path);
    }
    t0 = now_ns();

    for (int d = 0; d < devices; d++) {
        pid_t pid = fork();
        if (pid < 0) {
            fprintf(stderr, "fork failed: %s\n", strerror(errno));
            return 1;
        }
        if (pid == 0) {
            int rc = 0;
            if (strcmp(mode, "text") == 0) {
                rc = write_text_logs(eventlog_dir, out_dir, d, records);
            } else if (strcmp(mode, "binary") == 0) {
                rc = write_binary_logs(eventlog_dir, shared_path, out_dir, d, records, strict_perm);
            } else if (strcmp(mode, "syslog") == 0) {
                rc = write_syslog_logs(eventlog_dir, out_dir, d, records);
            } else if (strcmp(mode, "ftrace") == 0) {
                rc = write_ftrace_logs(eventlog_dir, out_dir, d, records, ftrace_run_token);
            } else if (strcmp(mode, "nanolog_like") == 0) {
                rc = write_nanolog_like_logs(out_dir, d, records);
            } else if (strcmp(mode, "zephyr_deferred_like") == 0 || strcmp(mode, "zephyr_like") == 0) {
                rc = write_zephyr_deferred_like_logs(out_dir, d, records);
            } else if (strcmp(mode, "ulog_async_like") == 0) {
                rc = write_ulog_async_like_logs(out_dir, d, records);
            } else if (strcmp(mode, "hilog_lite_like") == 0) {
                rc = write_hilog_lite_like_logs(out_dir, d, records);
            } else {
                rc = -1;
            }
            _exit(rc == 0 ? 0 : 2);
        }
    }

    int status = 0;
    for (int d = 0; d < devices; d++) {
        int st = 0;
        wait(&st);
        if (st != 0) status = 1;
    }
    uint64_t t1 = now_ns();
    double elapsed_ms = (double)(t1 - t0) / 1e6;

    if (status != 0) {
        fprintf(stderr, "one or more writers failed\n");
        return 1;
    }

    long long total_bytes = 0;
    long long shared_bytes = 0;
    if (strcmp(mode, "text") == 0) {
        total_bytes = sum_sizes(out_dir, "txt");
    } else if (strcmp(mode, "binary") == 0) {
        total_bytes = sum_sizes(out_dir, "bin");
        struct stat st;
        if (stat(shared_path, &st) == 0) {
            shared_bytes = (long long)st.st_size;
        }
    } else if (strcmp(mode, "syslog") == 0) {
        total_bytes = sum_counters(out_dir);
    } else if (strcmp(mode, "ftrace") == 0) {
        unsigned long long observed = scan_ftrace_observed_bytes(ftrace_read_path, ftrace_run_token);
        long long payload_total = sum_counters(out_dir);
        if (payload_total < 0) payload_total = 0;
        if ((unsigned long long)payload_total > observed) total_bytes = payload_total;
        else total_bytes = (long long)observed;
    } else if (strcmp(mode, "nanolog_like") == 0) {
        total_bytes = sum_sizes(out_dir, "nlog");
    } else if (strcmp(mode, "zephyr_deferred_like") == 0 || strcmp(mode, "zephyr_like") == 0) {
        total_bytes = sum_sizes(out_dir, "zlog");
    } else if (strcmp(mode, "ulog_async_like") == 0) {
        total_bytes = sum_sizes(out_dir, "ulg");
    } else if (strcmp(mode, "hilog_lite_like") == 0) {
        total_bytes = sum_sizes(out_dir, "hlg");
    } else {
        fprintf(stderr, "unsupported mode: %s\n", mode);
        return 1;
    }

    if (total_bytes < 0) {
        fprintf(stderr, "failed to collect output bytes for mode %s\n", mode);
        return 1;
    }

    printf("mode,%s,devices,%d,records_per_device,%ld,elapsed_ms,%.3f,bytes,%lld,shared_bytes,%lld,total_bytes,%lld\n",
           mode, devices, records, elapsed_ms, total_bytes, shared_bytes, total_bytes + shared_bytes);

    return 0;
}
