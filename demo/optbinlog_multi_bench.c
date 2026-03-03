#include "optbinlog_shared.h"
#include "optbinlog_eventlog.h"
#include "optbinlog_binlog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

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
        "  %s --mode text|binary --eventlog-dir <dir> --out-dir <dir> --devices N --records-per-device N [--shared <file>] [--strict-perm]\n",
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
                snprintf(buf, sizeof(buf), "dev-%02d", device_id);
                fprintf(fp, "%s=\"%s\"", ele->name, buf);
            }
        }
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
        printf("mode,text,devices,%d,records_per_device,%ld,elapsed_ms,%.3f,bytes,%lld,shared_bytes,0,total_bytes,%lld\n",
               devices, records, elapsed_ms, total_bytes, total_bytes);
    } else if (strcmp(mode, "binary") == 0) {
        total_bytes = sum_sizes(out_dir, "bin");
        struct stat st;
        if (stat(shared_path, &st) == 0) {
            shared_bytes = (long long)st.st_size;
        }
        printf("mode,binary,devices,%d,records_per_device,%ld,elapsed_ms,%.3f,bytes,%lld,shared_bytes,%lld,total_bytes,%lld\n",
               devices, records, elapsed_ms, total_bytes, shared_bytes, total_bytes + shared_bytes);
    }

    return 0;
}
