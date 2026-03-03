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

typedef enum {
    MODE_TEXT = 0,
    MODE_CSV = 1,
    MODE_JSONL = 2,
} TextLikeMode;

static void usage(const char* prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s --mode text|csv|jsonl|binary|syslog|ftrace --eventlog-dir <dir> --out <file> --records N [--shared <file>] [--strict-perm]\n",
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
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0 || tags.len == 0) {
        fprintf(stderr, "no tags parsed\n");
        optbinlog_taglist_free(&tags);
        return -1;
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
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        int rc = 0;
        if (mode == MODE_TEXT) rc = write_text_record(fp, tag, i, &rnd);
        else if (mode == MODE_CSV) rc = write_csv_record(fp, tag, i, &rnd);
        else if (mode == MODE_JSONL) rc = write_jsonl_record(fp, tag, i, &rnd);
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

    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0 || tags.len == 0) {
        fprintf(stderr, "no tags parsed\n");
        optbinlog_taglist_free(&tags);
        return -1;
    }

    int prio = LOG_DEBUG;
    const char* prio_env = getenv("OPTBINLOG_SYSLOG_PRIO");
    if (prio_env && prio_env[0]) {
        prio = atoi(prio_env);
    }

    openlog("optbinlog_bench", LOG_NDELAY, LOG_USER);
    uint64_t bytes = 0;
    uint64_t rnd = 0x123456789abcdef0ULL;

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        char line[1024];
        if (format_text_payload(tag, i, &rnd, line, sizeof(line)) != 0) {
            closelog();
            optbinlog_taglist_free(&tags);
            fprintf(stderr, "format failed\n");
            return -1;
        }
        syslog(prio, "%s", line);
        bytes += (uint64_t)strlen(line) + 1;
    }
    uint64_t t_write1 = now_ns();

    closelog();
    optbinlog_taglist_free(&tags);

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
    uint64_t t_e2e0 = now_ns();

    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0 || tags.len == 0) {
        fprintf(stderr, "no tags parsed\n");
        optbinlog_taglist_free(&tags);
        return -1;
    }

    const char* trace_path = getenv("OPTBINLOG_TRACE_MARKER");
    if (!trace_path || !trace_path[0]) {
        trace_path = "/sys/kernel/debug/tracing/trace_marker";
    }

    int fd = open(trace_path, O_WRONLY | O_CLOEXEC);
    if (fd < 0) {
        fprintf(stderr, "open trace_marker failed: %s\n", strerror(errno));
        optbinlog_taglist_free(&tags);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL;
    uint64_t bytes = 0;

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        char line[1024];
        if (format_text_payload(tag, i, &rnd, line, sizeof(line)) != 0) {
            close(fd);
            optbinlog_taglist_free(&tags);
            fprintf(stderr, "format failed\n");
            return -1;
        }
        size_t len = strlen(line);
        if (write(fd, line, len) != (ssize_t)len || write(fd, "\n", 1) != 1) {
            fprintf(stderr, "write trace_marker failed: %s\n", strerror(errno));
            close(fd);
            optbinlog_taglist_free(&tags);
            return -1;
        }
        bytes += (uint64_t)len + 1;
    }
    uint64_t t_write1 = now_ns();

    close(fd);
    optbinlog_taglist_free(&tags);

    uint64_t t_e2e1 = now_ns();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double end_to_end_ms = (double)(t_e2e1 - t_e2e0) / 1e6;
    double prep_ms = (double)(t_write0 - t_e2e0) / 1e6;
    double post_ms = (double)(t_e2e1 - t_write1) / 1e6;
    long rss = max_rss_kb();

    printf("mode,ftrace,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
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

    usage(argv[0]);
    return 1;
}
