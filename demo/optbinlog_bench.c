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

typedef struct {
    uint32_t sec;
    uint32_t nsec;
    uint16_t domain;
    uint16_t tag;
    uint8_t level;
    uint8_t reserved;
    uint16_t msg_len;
} HiLogLiteHeader;

#define GENERATED_STRING_SLOT 128u

typedef struct {
    OptbinlogTagList tags;
    OptbinlogRecord* recs;
    OptbinlogValue* values_pool;
    char* string_pool;
} GeneratedRecordSet;

static void usage(const char* prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s --mode text|text_semantic_like|csv|jsonl|binary|binary_crc32_legacy|binary_crc32c|binary_hotpath|binary_nocrc|binary_varstr|binary_crc32c_varstr|binary_nocrc_varstr|syslog|ftrace|nanolog_like|zephyr_like|zephyr_deferred_like|ulog_async_like|hilog_lite_like|nanolog_semantic_like|zephyr_deferred_semantic_like|ulog_semantic_like|hilog_semantic_like --eventlog-dir <dir> --out <file> --records N [--shared <file>] [--strict-perm]\n",
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

static int text_profile_semantic_enabled(void) {
    const char* profile = getenv("OPTBINLOG_TEXT_PROFILE");
    if (!profile || !profile[0]) return 0;
    return strncmp(profile, "semantic", 8) == 0;
}

static int format_plain_text_payload(long i, uint64_t* rnd, char* out, size_t cap) {
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

static size_t ele_storage_bytes(const OptbinlogTagEleDef* ele) {
    if (ele->type_char == 'D') return sizeof(double);
    if (ele->type_char == 'S') {
        if (ele->bits <= 0) return 16u;
        return (size_t)ele->bits;
    }
    if (ele->bits <= 8) return 1u;
    return (size_t)((ele->bits + 7) / 8);
}

static void generate_string_value(const OptbinlogTagDef* tag,
                                  const OptbinlogTagEleDef* ele,
                                  long i,
                                  char* out,
                                  size_t cap) {
    static const char* hosts[] = {"edge01", "edge02", "edge03", "gateway01"};
    static const char* apps[] = {"sensor", "logger", "power", "storage"};
    static const char* modules[] = {"net", "storage", "sensor", "power"};
    static const char* events[] = {"irq_handler_entry", "irq_handler_exit", "workqueue_start", "workqueue_end"};
    static const char* states[] = {"running", "sleeping", "runnable", "waiting"};
    static const char* sources[] = {"uart0", "i2c1", "spi2", "can0"};
    static const char* statuses[] = {"ok", "warn", "retry", "drop"};
    static const char* msgs[] = {
        "socket timeout while reconnecting",
        "flash erase latency exceeded threshold",
        "imu sample window checksum mismatch",
        "battery profile updated from bms",
    };
    const char* name = ele->name;
    if (!out || cap == 0) return;

    if (strstr(name, "host")) {
        snprintf(out, cap, "%s", hosts[(size_t)(i % 4)]);
    } else if (strstr(name, "app")) {
        snprintf(out, cap, "%s", apps[(size_t)(i % 4)]);
    } else if (strstr(name, "module")) {
        snprintf(out, cap, "%s", modules[(size_t)(i % 4)]);
    } else if (strstr(name, "event")) {
        snprintf(out, cap, "%s", events[(size_t)(i % 4)]);
    } else if (strstr(name, "state")) {
        snprintf(out, cap, "%s", states[(size_t)(i % 4)]);
    } else if (strstr(name, "source")) {
        snprintf(out, cap, "%s", sources[(size_t)(i % 4)]);
    } else if (strstr(name, "status")) {
        snprintf(out, cap, "%s", statuses[(size_t)(i % 4)]);
    } else if (strstr(name, "msg")) {
        snprintf(out, cap, "%s", msgs[(size_t)(i % 4)]);
    } else {
        snprintf(out, cap, "%s_%02ld", tag->name, i % 100);
    }
}

static void free_generated_records(GeneratedRecordSet* set) {
    if (!set) return;
    free(set->string_pool);
    free(set->values_pool);
    free(set->recs);
    optbinlog_taglist_free(&set->tags);
    memset(set, 0, sizeof(*set));
}

static int env_require_native_alignment(void) {
    const char* raw = getenv("OPTBINLOG_NATIVE_ALIGN_REQUIRED");
    return raw && (strcmp(raw, "1") == 0 || strcmp(raw, "true") == 0 || strcmp(raw, "yes") == 0);
}

static int lookup_u_field(const OptbinlogTagDef* tag,
                          const OptbinlogRecord* rec,
                          const char* name,
                          uint64_t* out) {
    if (!tag || !rec || !name || !out) return -1;
    for (int e = 0; e < rec->ele_count; e++) {
        const OptbinlogTagEleDef* ele = &tag->eles[e];
        const OptbinlogValue* v = &rec->values[e];
        if (strcmp(ele->name, name) != 0) continue;
        if (v->kind != OPTBINLOG_VAL_U) return -1;
        *out = v->u;
        return 0;
    }
    return -1;
}

static int map_record_to_native_nano(const OptbinlogTagDef* tag,
                                     const OptbinlogRecord* rec,
                                     NanoPackedRecord* out) {
    uint64_t seq = 0;
    uint64_t code = 0;
    uint64_t temp_x10 = 0;
    uint64_t tag_v = 0;
    uint64_t level = 0;
    if (!tag || !rec || !out) return -1;
    if (lookup_u_field(tag, rec, "seq", &seq) != 0) return -1;
    if (lookup_u_field(tag, rec, "code", &code) != 0) return -1;
    if (lookup_u_field(tag, rec, "temp_x10", &temp_x10) != 0) return -1;
    if (lookup_u_field(tag, rec, "tag", &tag_v) != 0) {
        tag_v = (uint64_t)(uint16_t)rec->tag_id;
    }
    if (lookup_u_field(tag, rec, "level", &level) != 0) {
        level = 0;
    }
    out->ts_ns = (uint64_t)rec->timestamp * 1000000000ull;
    out->seq = (uint32_t)seq;
    out->code = (uint32_t)code;
    out->temp_x10 = (int32_t)(uint32_t)temp_x10;
    out->tag = (uint16_t)tag_v;
    out->level = (uint16_t)level;
    return 0;
}

static int map_record_to_native_hilog(const OptbinlogTagDef* tag,
                                      const OptbinlogRecord* rec,
                                      NanoPackedRecord* out,
                                      uint16_t* out_domain) {
    uint64_t domain = 0xD001u;
    if (map_record_to_native_nano(tag, rec, out) != 0) return -1;
    if (lookup_u_field(tag, rec, "domain", &domain) != 0) {
        domain = 0xD001u;
    }
    if (out_domain) *out_domain = (uint16_t)domain;
    return 0;
}

static int build_generated_records(const char* eventlog_dir, long records, GeneratedRecordSet* out) {
    size_t total_values = 0;
    size_t total_string_fields = 0;
    uint64_t rnd = 0x123456789abcdef0ULL;
    if (!out) return -1;
    memset(out, 0, sizeof(*out));
    optbinlog_taglist_init(&out->tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &out->tags) != 0 || out->tags.len == 0) {
        free_generated_records(out);
        fprintf(stderr, "no tags parsed\n");
        return -1;
    }

    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &out->tags.items[i % (long)out->tags.len];
        total_values += (size_t)tag->ele_num;
        for (int e = 0; e < tag->ele_num; e++) {
            if (tag->eles[e].type_char == 'S') {
                total_string_fields++;
            }
        }
    }

    out->recs = calloc((size_t)records, sizeof(OptbinlogRecord));
    out->values_pool = calloc(total_values ? total_values : 1u, sizeof(OptbinlogValue));
    out->string_pool = total_string_fields ? calloc(total_string_fields, GENERATED_STRING_SLOT) : NULL;
    if (!out->recs || !out->values_pool || (total_string_fields && !out->string_pool)) {
        fprintf(stderr, "OOM\n");
        free_generated_records(out);
        return -1;
    }

    size_t val_off = 0;
    size_t str_off = 0;
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &out->tags.items[i % (long)out->tags.len];
        OptbinlogValue* values = out->values_pool + val_off;
        for (int e = 0; e < tag->ele_num; e++) {
            OptbinlogTagEleDef* ele = &tag->eles[e];
            if (ele->type_char == 'L') {
                uint64_t v = xorshift64(&rnd) & max_for_bits(ele->bits);
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_U, v, 0.0, NULL};
            } else if (ele->type_char == 'D') {
                double v = (double)(xorshift64(&rnd) % 10000u) / 100.0;
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_D, 0, v, NULL};
            } else if (ele->type_char == 'S') {
                char* buf = out->string_pool + str_off * GENERATED_STRING_SLOT;
                size_t cap = GENERATED_STRING_SLOT;
                size_t schema_cap = ele_storage_bytes(ele);
                if (schema_cap > 0 && schema_cap + 1 < cap) {
                    cap = schema_cap + 1;
                }
                generate_string_value(tag, ele, i, buf, cap);
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_S, 0, 0.0, buf};
                str_off++;
            }
        }
        out->recs[i].timestamp = 1710000000 + i;
        out->recs[i].tag_id = tag->tag_id;
        out->recs[i].ele_count = tag->ele_num;
        out->recs[i].values = values;
        val_off += (size_t)tag->ele_num;
    }
    return 0;
}

static int append_record_fields(char** p,
                                size_t* left,
                                const OptbinlogTagDef* tag,
                                const OptbinlogRecord* rec,
                                const char* sep,
                                int quote_strings) {
    (void)tag;
    for (int e = 0; e < rec->ele_count; e++) {
        const OptbinlogTagEleDef* ele = &tag->eles[e];
        const OptbinlogValue* v = &rec->values[e];
        if (e > 0 && appendf(p, left, "%s", sep) != 0) return -1;
        if (v->kind == OPTBINLOG_VAL_U) {
            if (appendf(p, left, "%s=%llu", ele->name, (unsigned long long)v->u) != 0) return -1;
        } else if (v->kind == OPTBINLOG_VAL_D) {
            if (appendf(p, left, "%s=%.2f", ele->name, v->d) != 0) return -1;
        } else if (v->kind == OPTBINLOG_VAL_S) {
            if (quote_strings) {
                if (appendf(p, left, "%s=\"%s\"", ele->name, v->s ? v->s : "") != 0) return -1;
            } else {
                if (appendf(p, left, "%s=%s", ele->name, v->s ? v->s : "") != 0) return -1;
            }
        }
    }
    return 0;
}

static int format_record_text_payload(const OptbinlogTagDef* tag,
                                      const OptbinlogRecord* rec,
                                      char* out,
                                      size_t cap) {
    char* p = out;
    size_t left = cap;
    if (appendf(&p, &left, "ts=%lld id=%d name=%s ",
                (long long)rec->timestamp, tag->tag_id, tag->name) != 0) {
        return -1;
    }
    return append_record_fields(&p, &left, tag, rec, ",", 1);
}

static int format_record_ulog_payload(const OptbinlogTagDef* tag,
                                      const OptbinlogRecord* rec,
                                      char* out,
                                      size_t cap) {
    char* p = out;
    size_t left = cap;
    unsigned int level = (unsigned int)(rec->values[0].kind == OPTBINLOG_VAL_U ? (rec->values[0].u & 0x7u) : 0u);
    if (appendf(&p, &left, "I/%s(%u): ", tag->name, level) != 0) return -1;
    return append_record_fields(&p, &left, tag, rec, " ", 0);
}

static int format_record_hilog_message(const OptbinlogTagDef* tag,
                                       const OptbinlogRecord* rec,
                                       char* out,
                                       size_t cap) {
    char* p = out;
    size_t left = cap;
    if (appendf(&p, &left, "%s ", tag->name) != 0) return -1;
    return append_record_fields(&p, &left, tag, rec, " ", 0);
}

static size_t write_uint_le_n(uint8_t* dst, uint64_t v, size_t nbytes) {
    for (size_t i = 0; i < nbytes; i++) {
        dst[i] = (uint8_t)((v >> (i * 8u)) & 0xFFu);
    }
    return nbytes;
}

static size_t encode_compact_record(uint8_t* out,
                                    size_t cap,
                                    const OptbinlogTagDef* tag,
                                    const OptbinlogRecord* rec) {
    size_t off = 0;
    if (cap < 16u) return 0;
    memcpy(out + off, &rec->timestamp, sizeof(int64_t));
    off += sizeof(int64_t);
    write_le16(out + off, (uint16_t)tag->tag_id);
    off += sizeof(uint16_t);
    out[off++] = (uint8_t)rec->ele_count;
    for (int e = 0; e < rec->ele_count; e++) {
        const OptbinlogTagEleDef* ele = &tag->eles[e];
        const OptbinlogValue* v = &rec->values[e];
        if (v->kind == OPTBINLOG_VAL_U) {
            size_t nbytes = ele_storage_bytes(ele);
            if (off + nbytes > cap) return 0;
            off += write_uint_le_n(out + off, v->u, nbytes);
        } else if (v->kind == OPTBINLOG_VAL_D) {
            if (off + sizeof(double) > cap) return 0;
            memcpy(out + off, &v->d, sizeof(double));
            off += sizeof(double);
        } else if (v->kind == OPTBINLOG_VAL_S) {
            size_t schema_cap = ele_storage_bytes(ele);
            size_t slen = v->s ? strnlen(v->s, schema_cap) : 0u;
            if (off + 2u + slen > cap) return 0;
            write_le16(out + off, (uint16_t)slen);
            off += 2u;
            if (slen > 0u) {
                memcpy(out + off, v->s, slen);
                off += slen;
            }
        }
    }
    return off;
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
    int semantic_text = (mode == MODE_TEXT) && text_profile_semantic_enabled();

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

    SyslogLineList semantic_lines;
    int semantic_lines_loaded = 0;
    if (semantic_text) {
        if (load_syslog_lines(eventlog_dir, &semantic_lines) != 0 || semantic_lines.len == 0) {
            fprintf(stderr, "no semantic text lines loaded\n");
            optbinlog_taglist_free(&tags);
            return -1;
        }
        semantic_lines_loaded = 1;
    }

    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        if (semantic_lines_loaded) syslog_line_list_free(&semantic_lines);
        optbinlog_taglist_free(&tags);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL;
    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        int rc = 0;
        if (mode == MODE_TEXT) {
            if (semantic_text) {
                const char* line = semantic_lines.items[(size_t)(i % (long)semantic_lines.len)];
                if (fputs(line, fp) == EOF || fputc('\n', fp) == EOF) {
                    rc = -1;
                }
            } else {
                char line[1024];
                rc = format_plain_text_payload(i, &rnd, line, sizeof(line));
                if (rc == 0) {
                    if (fputs(line, fp) == EOF || fputc('\n', fp) == EOF) {
                        rc = -1;
                    }
                }
            }
        } else {
            OptbinlogTagDef* tag = &tags.items[i % tags.len];
            if (mode == MODE_CSV) rc = write_csv_record(fp, tag, i, &rnd);
            else if (mode == MODE_JSONL) rc = write_jsonl_record(fp, tag, i, &rnd);
        }
        if (rc != 0) {
            fclose(fp);
            if (semantic_lines_loaded) syslog_line_list_free(&semantic_lines);
            optbinlog_taglist_free(&tags);
            fprintf(stderr, "format/write failed\n");
            return -1;
        }
    }
    uint64_t t_write1 = now_ns();

    fclose(fp);
    if (semantic_lines_loaded) syslog_line_list_free(&semantic_lines);
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

    GeneratedRecordSet set;
    if (build_generated_records(eventlog_dir, records, &set) != 0) {
        fprintf(stderr, "failed to build semantic records for syslog\n");
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
        const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
        const OptbinlogRecord* rec = &set.recs[i];
        char line[2048];
        if (format_record_text_payload(tag, rec, line, sizeof(line)) != 0) {
            closelog();
            free_generated_records(&set);
            return -1;
        }
        syslog(prio, "%s", line);
        bytes += (uint64_t)strlen(line) + 1;
    }
    uint64_t t_write1 = now_ns();

    closelog();
    free_generated_records(&set);

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
    GeneratedRecordSet set;
    if (build_generated_records(eventlog_dir, records, &set) != 0) {
        fprintf(stderr, "failed to build semantic records for ftrace\n");
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
        free_generated_records(&set);
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
        const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
        const OptbinlogRecord* rec = &set.recs[i];
        char line[2048];
        if (format_record_text_payload(tag, rec, line, sizeof(line)) != 0) {
            close(fd);
            if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
            free_generated_records(&set);
            return -1;
        }

        char buf[4096];
        int nn = snprintf(buf, sizeof(buf), "%s %s", token, line);
        if (nn < 0 || (size_t)nn >= sizeof(buf)) {
            close(fd);
            if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
            free_generated_records(&set);
            return -1;
        }
        size_t len = (size_t)nn;
        if (write(fd, buf, len) != (ssize_t)len || write(fd, "\n", 1) != 1) {
            fprintf(stderr, "write trace_marker failed: %s\n", strerror(errno));
            close(fd);
            if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
            free_generated_records(&set);
            return -1;
        }
        bytes_payload += (uint64_t)len + 1;
    }
    uint64_t t_write1 = now_ns();

    close(fd);
    if (tracing_prev == 0) (void)write_tracing_on(tracing_on_path, 0);
    free_generated_records(&set);

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

static int bench_nanolog_like(const char* out_path, const char* eventlog_dir, long records) {
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    uint64_t t_write0 = now_ns();
    NanoPackedRecord* staging = malloc((size_t)records * sizeof(NanoPackedRecord));
    GeneratedRecordSet set;
    int use_generated = 0;
    if (!staging) {
        fclose(fp);
        fprintf(stderr, "OOM\n");
        return -1;
    }
    memset(&set, 0, sizeof(set));
    if (eventlog_dir && eventlog_dir[0] && build_generated_records(eventlog_dir, records, &set) == 0) {
        use_generated = 1;
    }
    uint64_t rnd = 0x123456789abcdef0ULL;
    for (long i = 0; i < records; i++) {
        if (use_generated) {
            const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
            const OptbinlogRecord* rec = &set.recs[i];
            if (map_record_to_native_nano(tag, rec, &staging[i]) != 0) {
                free_generated_records(&set);
                free(staging);
                fclose(fp);
                if (env_require_native_alignment()) {
                    fprintf(stderr, "native nanolog alignment failed for schema %s\n", eventlog_dir);
                    return -1;
                }
                use_generated = 0;
                rnd = 0x123456789abcdef0ULL;
                for (long j = 0; j <= i; j++) {
                    staging[j] = make_nano_record(j, &rnd);
                }
            }
        } else {
            staging[i] = make_nano_record(i, &rnd);
        }
    }
    size_t written = fwrite(staging, sizeof(NanoPackedRecord), (size_t)records, fp);
    fflush(fp);
    uint64_t t_write1 = now_ns();

    if (use_generated) {
        free_generated_records(&set);
    }
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

static int bench_zephyr_deferred_like(const char* out_path, const char* eventlog_dir, long records) {
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    const size_t batch_cap = 1024;
    NanoPackedRecord* batch = malloc(batch_cap * sizeof(NanoPackedRecord));
    GeneratedRecordSet set;
    int use_generated = 0;
    if (!batch) {
        fclose(fp);
        fprintf(stderr, "OOM\n");
        return -1;
    }
    memset(&set, 0, sizeof(set));
    if (eventlog_dir && eventlog_dir[0] && build_generated_records(eventlog_dir, records, &set) == 0) {
        use_generated = 1;
    }

    uint64_t bytes = 0;
    uint64_t rnd = 0x123456789abcdef0ULL;
    size_t n_batch = 0;

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        if (use_generated) {
            const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
            const OptbinlogRecord* rec = &set.recs[i];
            if (map_record_to_native_nano(tag, rec, &batch[n_batch]) != 0) {
                free_generated_records(&set);
                free(batch);
                fclose(fp);
                if (env_require_native_alignment()) {
                    fprintf(stderr, "native zephyr alignment failed for schema %s\n", eventlog_dir);
                    return -1;
                }
                use_generated = 0;
                rnd = 0x123456789abcdef0ULL;
                batch[n_batch] = make_nano_record(i, &rnd);
            }
        } else {
            batch[n_batch] = make_nano_record(i, &rnd);
        }
        n_batch++;
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

    if (use_generated) {
        free_generated_records(&set);
    }
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

static int bench_ulog_async_like(const char* out_path, const char* eventlog_dir, long records) {
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    enum { BATCH_CAP = 512 };
    char batch[BATCH_CAP][192];
    size_t batch_len[BATCH_CAP];
    size_t nb = 0;
    uint64_t bytes = 0;
    uint64_t rnd = 0x123456789abcdef0ULL;
    GeneratedRecordSet set;
    int use_generated = 0;
    memset(&set, 0, sizeof(set));
    if (eventlog_dir && eventlog_dir[0] && build_generated_records(eventlog_dir, records, &set) == 0) {
        use_generated = 1;
    }

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        NanoPackedRecord r;
        if (use_generated) {
            const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
            const OptbinlogRecord* rec = &set.recs[i];
            if (map_record_to_native_nano(tag, rec, &r) != 0) {
                free_generated_records(&set);
                fclose(fp);
                if (env_require_native_alignment()) {
                    fprintf(stderr, "native ulog alignment failed for schema %s\n", eventlog_dir);
                    return -1;
                }
                use_generated = 0;
                rnd = 0x123456789abcdef0ULL;
                r = make_nano_record(i, &rnd);
            }
        } else {
            r = make_nano_record(i, &rnd);
        }
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
                bytes += (uint64_t)batch_len[j];
            }
            nb = 0;
        }
    }
    for (size_t j = 0; j < nb; j++) {
        if (fwrite(batch[j], 1, batch_len[j], fp) != batch_len[j]) {
            fclose(fp);
            return -1;
        }
        bytes += (uint64_t)batch_len[j];
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();

    if (use_generated) {
        free_generated_records(&set);
    }
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,ulog_async_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_hilog_lite_like(const char* out_path, const char* eventlog_dir, long records) {
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    uint64_t bytes = 0;
    uint64_t rnd = 0x123456789abcdef0ULL;
    GeneratedRecordSet set;
    int use_generated = 0;
    memset(&set, 0, sizeof(set));
    if (eventlog_dir && eventlog_dir[0] && build_generated_records(eventlog_dir, records, &set) == 0) {
        use_generated = 1;
    }
    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        NanoPackedRecord r;
        uint16_t domain = 0xD001u;
        if (use_generated) {
            const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
            const OptbinlogRecord* rec = &set.recs[i];
            if (map_record_to_native_hilog(tag, rec, &r, &domain) != 0) {
                free_generated_records(&set);
                fclose(fp);
                if (env_require_native_alignment()) {
                    fprintf(stderr, "native hilog alignment failed for schema %s\n", eventlog_dir);
                    return -1;
                }
                use_generated = 0;
                rnd = 0x123456789abcdef0ULL;
                r = make_nano_record(i, &rnd);
                domain = 0xD001u;
            }
        } else {
            r = make_nano_record(i, &rnd);
        }
        char msg[64];
        int n = snprintf(msg, sizeof(msg), "c=%u s=%u t=%d", r.code, r.seq, r.temp_x10);
        if (n <= 0 || (size_t)n > sizeof(msg)) {
            fclose(fp);
            return -1;
        }
        HiLogLiteHeader h;
        h.sec = (uint32_t)(r.ts_ns / 1000000000ull);
        h.nsec = (uint32_t)(r.ts_ns % 1000000000ull);
        h.domain = domain;
        h.tag = r.tag;
        h.level = (uint8_t)(r.level & 0x7u);
        h.reserved = 0u;
        h.msg_len = (uint16_t)n;
        if (fwrite(&h, sizeof(h), 1, fp) != 1 || fwrite(msg, 1, (size_t)n, fp) != (size_t)n) {
            fclose(fp);
            return -1;
        }
        bytes += (uint64_t)sizeof(h) + (uint64_t)n;
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();

    if (use_generated) {
        free_generated_records(&set);
    }
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,hilog_lite_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_text_semantic_like(const char* out_path, const char* eventlog_dir, long records) {
    GeneratedRecordSet set;
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    if (build_generated_records(eventlog_dir, records, &set) != 0) {
        fclose(fp);
        return -1;
    }

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
        const OptbinlogRecord* rec = &set.recs[i];
        char line[2048];
        if (format_record_text_payload(tag, rec, line, sizeof(line)) != 0) {
            free_generated_records(&set);
            fclose(fp);
            return -1;
        }
        if (fputs(line, fp) == EOF || fputc('\n', fp) == EOF) {
            free_generated_records(&set);
            fclose(fp);
            return -1;
        }
    }
    uint64_t t_write1 = now_ns();

    free_generated_records(&set);
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    struct stat st;
    if (stat(out_path, &st) != 0) {
        fprintf(stderr, "stat failed\n");
        return -1;
    }
    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,text_semantic_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%lld,shared_bytes,0,total_bytes,%lld,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (long long)st.st_size, (long long)st.st_size, rss);
    return 0;
}

static int bench_nanolog_semantic_like(const char* out_path, const char* eventlog_dir, long records) {
    GeneratedRecordSet set;
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);
    if (build_generated_records(eventlog_dir, records, &set) != 0) {
        fclose(fp);
        return -1;
    }

    uint8_t scratch[2048];
    uint64_t bytes = 0;
    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
        const OptbinlogRecord* rec = &set.recs[i];
        size_t n = encode_compact_record(scratch, sizeof(scratch), tag, rec);
        if (n == 0 || fwrite(scratch, 1, n, fp) != n) {
            free_generated_records(&set);
            fclose(fp);
            return -1;
        }
        bytes += (uint64_t)n;
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free_generated_records(&set);
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,nanolog_semantic_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_zephyr_deferred_semantic_like(const char* out_path, const char* eventlog_dir, long records) {
    GeneratedRecordSet set;
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);
    if (build_generated_records(eventlog_dir, records, &set) != 0) {
        fclose(fp);
        return -1;
    }

    uint8_t* batch = malloc(256u * 2048u);
    if (!batch) {
        fprintf(stderr, "OOM\n");
        free_generated_records(&set);
        fclose(fp);
        return -1;
    }

    uint64_t bytes = 0;
    size_t off = 0;
    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
        const OptbinlogRecord* rec = &set.recs[i];
        size_t n = encode_compact_record(batch + off, 256u * 2048u - off, tag, rec);
        if (n == 0) {
            free(batch);
            free_generated_records(&set);
            fclose(fp);
            return -1;
        }
        off += n;
        if (off + 2048u > 256u * 2048u) {
            if (fwrite(batch, 1, off, fp) != off) {
                free(batch);
                free_generated_records(&set);
                fclose(fp);
                return -1;
            }
            bytes += (uint64_t)off;
            off = 0;
        }
    }
    if (off > 0) {
        if (fwrite(batch, 1, off, fp) != off) {
            free(batch);
            free_generated_records(&set);
            fclose(fp);
            return -1;
        }
        bytes += (uint64_t)off;
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free(batch);
    free_generated_records(&set);
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

static int bench_ulog_semantic_like(const char* out_path, const char* eventlog_dir, long records) {
    GeneratedRecordSet set;
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);
    if (build_generated_records(eventlog_dir, records, &set) != 0) {
        fclose(fp);
        return -1;
    }

    uint64_t bytes = 0;
    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
        const OptbinlogRecord* rec = &set.recs[i];
        char line[2048];
        int n = format_record_ulog_payload(tag, rec, line, sizeof(line));
        if (n != 0) {
            free_generated_records(&set);
            fclose(fp);
            return -1;
        }
        size_t len = strlen(line);
        if (fwrite(line, 1, len, fp) != len || fputc('\n', fp) == EOF) {
            free_generated_records(&set);
            fclose(fp);
            return -1;
        }
        bytes += (uint64_t)len + 1u;
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free_generated_records(&set);
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,ulog_semantic_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_hilog_semantic_like(const char* out_path, const char* eventlog_dir, long records) {
    GeneratedRecordSet set;
    uint64_t t_e2e0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", out_path, strerror(errno));
        return -1;
    }
    setvbuf(fp, NULL, _IOFBF, 1 << 20);
    if (build_generated_records(eventlog_dir, records, &set) != 0) {
        fclose(fp);
        return -1;
    }

    uint64_t bytes = 0;
    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        const OptbinlogTagDef* tag = &set.tags.items[i % (long)set.tags.len];
        const OptbinlogRecord* rec = &set.recs[i];
        char msg[2048];
        int n = format_record_hilog_message(tag, rec, msg, sizeof(msg));
        if (n != 0) {
            free_generated_records(&set);
            fclose(fp);
            return -1;
        }
        size_t len = strlen(msg);
        if (len > 0xFFFFu) len = 0xFFFFu;
        HiLogLiteHeader h;
        h.sec = (uint32_t)(rec->timestamp);
        h.nsec = 0u;
        h.domain = 0xD001u;
        h.tag = (uint16_t)tag->tag_id;
        h.level = (uint8_t)(i % 8);
        h.reserved = 0u;
        h.msg_len = (uint16_t)len;
        if (fwrite(&h, sizeof(h), 1, fp) != 1 || fwrite(msg, 1, len, fp) != len) {
            free_generated_records(&set);
            fclose(fp);
            return -1;
        }
        bytes += (uint64_t)sizeof(h) + (uint64_t)len;
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free_generated_records(&set);
    fclose(fp);
    uint64_t t_e2e1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,hilog_semantic_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
           (unsigned long long)bytes, (unsigned long long)bytes, rss);
    return 0;
}

static int bench_binary_variant(const char* mode_name,
                                const char* eventlog_dir,
                                const char* shared_path,
                                const char* out_path,
                                long records,
                                int strict_perm,
                                const char* checksum_mode,
                                int disable_crc,
                                int varstr_mode,
                                int hotpath_only) {
    GeneratedRecordSet set;
    uint64_t t_e2e0 = hotpath_only ? 0u : now_ns();
    if (build_generated_records(eventlog_dir, records, &set) != 0) {
        return -1;
    }

    if (optbinlog_shared_init_from_dir(eventlog_dir, shared_path, strict_perm) != 0) {
        fprintf(stderr, "shared init failed\n");
        free_generated_records(&set);
        return -1;
    }

    if (disable_crc) {
        setenv("OPTBINLOG_BINLOG_DISABLE_CRC", "1", 1);
    } else {
        unsetenv("OPTBINLOG_BINLOG_DISABLE_CRC");
    }
    if (checksum_mode && checksum_mode[0]) {
        setenv("OPTBINLOG_BINLOG_CHECKSUM", checksum_mode, 1);
    } else {
        unsetenv("OPTBINLOG_BINLOG_CHECKSUM");
    }
    if (varstr_mode > 0) {
        setenv("OPTBINLOG_BINLOG_VARSTR", "1", 1);
    } else if (varstr_mode == 0) {
        setenv("OPTBINLOG_BINLOG_VARSTR", "0", 1);
    } else {
        unsetenv("OPTBINLOG_BINLOG_VARSTR");
    }

    if (hotpath_only) {
        t_e2e0 = now_ns();
    }
    uint64_t t_write0 = now_ns();
    int rc = optbinlog_binlog_write(shared_path, out_path, set.recs, (size_t)records);
    uint64_t t_write1 = now_ns();
    uint64_t t_e2e1 = now_ns();

    free_generated_records(&set);
    unsetenv("OPTBINLOG_BINLOG_DISABLE_CRC");
    unsetenv("OPTBINLOG_BINLOG_CHECKSUM");
    unsetenv("OPTBINLOG_BINLOG_VARSTR");

    if (rc != 0) return -1;

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
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = hotpath_only ? 0.0 : (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = hotpath_only ? 0.0 : (double)(t_e2e1 - t_write1) / 1e6;
    printf("mode,%s,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%lld,shared_bytes,%lld,total_bytes,%lld,peak_kb,%ld\n",
           mode_name, records, write_ms, write_ms, end_to_end_ms, prep_ms, post_ms,
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
    if (strcmp(mode, "text_semantic_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_text_semantic_like(out_path, eventlog_dir, records) == 0 ? 0 : 1;
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
        return bench_binary_variant("binary", eventlog_dir, shared_path, out_path, records, strict_perm, NULL, 0, -1, 0) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "binary_crc32_legacy") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_binary_variant("binary_crc32_legacy", eventlog_dir, shared_path, out_path, records, strict_perm, "crc32", 0, 0, 0) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "binary_crc32c") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_binary_variant("binary_crc32c", eventlog_dir, shared_path, out_path, records, strict_perm, "crc32c", 0, 0, 0) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "binary_hotpath") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_binary_variant("binary_hotpath", eventlog_dir, shared_path, out_path, records, strict_perm, NULL, 0, -1, 1) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "binary_nocrc") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_binary_variant("binary_nocrc", eventlog_dir, shared_path, out_path, records, strict_perm, "none", 1, -1, 0) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "binary_varstr") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_binary_variant("binary_varstr", eventlog_dir, shared_path, out_path, records, strict_perm, NULL, 0, 1, 0) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "binary_crc32c_varstr") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_binary_variant("binary_crc32c_varstr", eventlog_dir, shared_path, out_path, records, strict_perm, "crc32c", 0, 1, 0) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "binary_nocrc_varstr") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_binary_variant("binary_nocrc_varstr", eventlog_dir, shared_path, out_path, records, strict_perm, "none", 1, 1, 0) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "syslog") == 0) {
        return bench_syslog(eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "ftrace") == 0) {
        return bench_ftrace(eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "nanolog_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_nanolog_like(out_path, eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "zephyr_deferred_like") == 0 || strcmp(mode, "zephyr_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_zephyr_deferred_like(out_path, eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "ulog_async_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_ulog_async_like(out_path, eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "hilog_lite_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_hilog_lite_like(out_path, eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "nanolog_semantic_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_nanolog_semantic_like(out_path, eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "zephyr_deferred_semantic_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_zephyr_deferred_semantic_like(out_path, eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "ulog_semantic_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_ulog_semantic_like(out_path, eventlog_dir, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "hilog_semantic_like") == 0) {
        if (!out_path) { usage(argv[0]); return 1; }
        return bench_hilog_semantic_like(out_path, eventlog_dir, records) == 0 ? 0 : 1;
    }

    usage(argv[0]);
    return 1;
}
