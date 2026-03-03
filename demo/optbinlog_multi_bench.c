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
        "  %s --mode text|binary|syslog|ftrace --eventlog-dir <dir> --out-dir <dir> --devices N --records-per-device N [--shared <file>] [--strict-perm]\n",
        prog
    );
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
    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0 || tags.len == 0) {
        optbinlog_taglist_free(&tags);
        return -1;
    }

    char path[512];
    snprintf(path, sizeof(path), "%s/device_%02d.txt", out_dir, device_id);
    FILE* fp = fopen(path, "wb");
    if (!fp) {
        optbinlog_taglist_free(&tags);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        char line[1024];
        if (format_text_payload(tag, i, device_id, &rnd, line, sizeof(line)) != 0) {
            fclose(fp);
            optbinlog_taglist_free(&tags);
            return -1;
        }
        fputs(line, fp);
        fputc('\n', fp);
    }

    fclose(fp);
    optbinlog_taglist_free(&tags);
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

    OptbinlogRecord* recs = calloc((size_t)records, sizeof(OptbinlogRecord));
    if (!recs) {
        optbinlog_taglist_free(&tags);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        OptbinlogValue* values = calloc((size_t)tag->ele_num, sizeof(OptbinlogValue));
        if (!values) {
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
                    optbinlog_taglist_free(&tags);
                    return -1;
                }
                snprintf(buf, 16, "dev-%02d", device_id);
                values[e] = (OptbinlogValue){OPTBINLOG_VAL_S, 0, 0.0, buf};
            }
        }
        recs[i].timestamp = 1710000000 + i;
        recs[i].tag_id = tag->tag_id;
        recs[i].ele_count = tag->ele_num;
        recs[i].values = values;
    }

    char path[512];
    snprintf(path, sizeof(path), "%s/device_%02d.bin", out_dir, device_id);
    int rc = optbinlog_binlog_write(shared_path, path, recs, (size_t)records);

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
    return rc;
}

static int write_syslog_logs(const char* eventlog_dir, const char* out_dir, int device_id, long records) {
    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0 || tags.len == 0) {
        optbinlog_taglist_free(&tags);
        return -1;
    }

    int prio = LOG_DEBUG;
    const char* prio_env = getenv("OPTBINLOG_SYSLOG_PRIO");
    if (prio_env && prio_env[0]) prio = atoi(prio_env);

    openlog("optbinlog_multi_bench", LOG_NDELAY | LOG_PID, LOG_USER);
    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    unsigned long long bytes = 0;

    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        char line[1024];
        if (format_text_payload(tag, i, device_id, &rnd, line, sizeof(line)) != 0) {
            closelog();
            optbinlog_taglist_free(&tags);
            return -1;
        }
        syslog(prio, "%s", line);
        bytes += (unsigned long long)strlen(line) + 1ULL;
    }

    closelog();
    optbinlog_taglist_free(&tags);
    return write_counter_file(out_dir, device_id, bytes);
}

static int write_ftrace_logs(const char* eventlog_dir, const char* out_dir, int device_id, long records) {
    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0 || tags.len == 0) {
        optbinlog_taglist_free(&tags);
        return -1;
    }

    const char* trace_path = getenv("OPTBINLOG_TRACE_MARKER");
    if (!trace_path || !trace_path[0]) {
        trace_path = "/sys/kernel/debug/tracing/trace_marker";
    }

    int fd = open(trace_path, O_WRONLY | O_CLOEXEC);
    if (fd < 0) {
        optbinlog_taglist_free(&tags);
        return -1;
    }

    uint64_t rnd = 0x123456789abcdef0ULL ^ (uint64_t)device_id;
    unsigned long long bytes = 0;
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        char line[1024];
        if (format_text_payload(tag, i, device_id, &rnd, line, sizeof(line)) != 0) {
            close(fd);
            optbinlog_taglist_free(&tags);
            return -1;
        }
        size_t len = strlen(line);
        if (write(fd, line, len) != (ssize_t)len || write(fd, "\n", 1) != 1) {
            close(fd);
            optbinlog_taglist_free(&tags);
            return -1;
        }
        bytes += (unsigned long long)len + 1ULL;
    }

    close(fd);
    optbinlog_taglist_free(&tags);
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

    uint64_t t0 = now_ns();
    if (strcmp(mode, "binary") == 0) {
        if (optbinlog_shared_init_from_dir(eventlog_dir, shared_path, strict_perm) != 0) {
            fprintf(stderr, "shared init failed\n");
            return 1;
        }
    }

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
                rc = write_ftrace_logs(eventlog_dir, out_dir, d, records);
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
    } else if (strcmp(mode, "syslog") == 0 || strcmp(mode, "ftrace") == 0) {
        total_bytes = sum_counters(out_dir);
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
