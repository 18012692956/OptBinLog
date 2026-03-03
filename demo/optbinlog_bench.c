#include "optbinlog_shared.h"
#include "optbinlog_eventlog.h"
#include "optbinlog_binlog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

static void usage(const char* prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s --mode text|binary --eventlog-dir <dir> --out <file> --records N [--shared <file>] [--strict-perm]\n",
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

static int bench_text(const char* eventlog_dir, const char* out_path, long records) {
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
    uint64_t t0 = now_ns();
    for (long i = 0; i < records; i++) {
        OptbinlogTagDef* tag = &tags.items[i % tags.len];
        fprintf(fp, "ts=%lld id=%d name=%s ", (long long)(1710000000 + i), tag->tag_id, tag->name);
        for (int e = 0; e < tag->ele_num; e++) {
            OptbinlogTagEleDef* ele = &tag->eles[e];
            if (e > 0) fputc(',', fp);
            if (ele->type_char == 'L') {
                uint64_t v = xorshift64(&rnd) & max_for_bits(ele->bits);
                fprintf(fp, "%s=%llu", ele->name, (unsigned long long)v);
            } else if (ele->type_char == 'D') {
                double v = (double)(xorshift64(&rnd) % 10000) / 100.0;
                fprintf(fp, "%s=%.2f", ele->name, v);
            } else if (ele->type_char == 'S') {
                char buf[32];
                snprintf(buf, sizeof(buf), "dev-%02ld", i % 100);
                fprintf(fp, "%s=\"%s\"", ele->name, buf);
            }
        }
        fputc('\n', fp);
    }
    uint64_t t1 = now_ns();

    fclose(fp);
    optbinlog_taglist_free(&tags);

    double ms = (double)(t1 - t0) / 1e6;
    struct stat st;
    if (stat(out_path, &st) != 0) {
        fprintf(stderr, "stat failed\n");
        return -1;
    }
    long rss = max_rss_kb();
    printf("mode,text,records,%ld,elapsed_ms,%.3f,bytes,%lld,shared_bytes,0,total_bytes,%lld,peak_kb,%ld\n",
           records, ms, (long long)st.st_size, (long long)st.st_size, rss);
    return 0;
}

static int bench_binary(const char* eventlog_dir, const char* shared_path, const char* out_path, long records, int strict_perm) {
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

    uint64_t t0 = now_ns();
    int rc = optbinlog_binlog_write(shared_path, out_path, recs, (size_t)records);
    uint64_t t1 = now_ns();

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

    double ms = (double)(t1 - t0) / 1e6;
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
    printf("mode,binary,records,%ld,elapsed_ms,%.3f,bytes,%lld,shared_bytes,%lld,total_bytes,%lld,peak_kb,%ld\n",
           records, ms, (long long)st.st_size, shared_bytes, (long long)st.st_size + shared_bytes, rss);
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

    if (!mode || !eventlog_dir || !out_path || records <= 0) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(mode, "text") == 0) {
        return bench_text(eventlog_dir, out_path, records) == 0 ? 0 : 1;
    }
    if (strcmp(mode, "binary") == 0) {
        return bench_binary(eventlog_dir, shared_path, out_path, records, strict_perm) == 0 ? 0 : 1;
    }

    usage(argv[0]);
    return 1;
}
