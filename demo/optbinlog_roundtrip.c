#include "optbinlog_eventlog.h"
#include "optbinlog_shared.h"
#include "optbinlog_binlog.h"

#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct {
    OptbinlogValueKind kind;
    uint64_t u;
    double d;
    char* s;
} ExpectedValue;

typedef struct {
    int64_t timestamp;
    int tag_id;
    int ele_count;
    ExpectedValue* values;
} ExpectedRecord;

static ExpectedRecord* g_expected = NULL;
static size_t g_expected_count = 0;
static size_t g_read_count = 0;
static int g_mismatch = 0;

static void usage(const char* prog) {
    fprintf(stderr,
            "Usage:\n"
            "  %s --eventlog-dir <dir> --shared <file> --log <file> --bad-log <file> --trunc-log <file>\n",
            prog);
}

static uint64_t max_for_bits(int bits) {
    if (bits >= 64) return UINT64_MAX;
    if (bits <= 0) return 0;
    return (1ULL << bits) - 1ULL;
}

static int copy_file(const char* src, const char* dst) {
    FILE* in = fopen(src, "rb");
    if (!in) return -1;
    FILE* out = fopen(dst, "wb");
    if (!out) {
        fclose(in);
        return -1;
    }
    char buf[4096];
    while (1) {
        size_t n = fread(buf, 1, sizeof(buf), in);
        if (n > 0) {
            if (fwrite(buf, 1, n, out) != n) {
                fclose(in);
                fclose(out);
                return -1;
            }
        }
        if (n < sizeof(buf)) {
            if (ferror(in)) {
                fclose(in);
                fclose(out);
                return -1;
            }
            break;
        }
    }
    fclose(in);
    fclose(out);
    return 0;
}

static int mutate_first_tag_id(const char* path) {
    FILE* fp = fopen(path, "r+b");
    if (!fp) return -1;
    if (fseek(fp, 8, SEEK_SET) != 0) {
        fclose(fp);
        return -1;
    }
    unsigned char bad[2] = {0xFF, 0xFF};
    if (fwrite(bad, 1, 2, fp) != 2) {
        fclose(fp);
        return -1;
    }
    fclose(fp);
    return 0;
}

static int truncate_tail(const char* path, off_t tail) {
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    off_t next = st.st_size - tail;
    if (next < 0) next = 0;
    return truncate(path, next);
}

static void free_records(OptbinlogRecord* recs, size_t n) {
    if (!recs) return;
    for (size_t i = 0; i < n; i++) {
        if (!recs[i].values) continue;
        for (int e = 0; e < recs[i].ele_count; e++) {
            if (recs[i].values[e].kind == OPTBINLOG_VAL_S) {
                free((void*)recs[i].values[e].s);
            }
        }
        free(recs[i].values);
    }
    free(recs);
}

static void free_expected(ExpectedRecord* recs, size_t n) {
    if (!recs) return;
    for (size_t i = 0; i < n; i++) {
        if (!recs[i].values) continue;
        for (int e = 0; e < recs[i].ele_count; e++) {
            if (recs[i].values[e].kind == OPTBINLOG_VAL_S) {
                free(recs[i].values[e].s);
            }
        }
        free(recs[i].values);
    }
    free(recs);
}

static int fill_test_records(const OptbinlogTagList* tags, OptbinlogRecord** out_recs, size_t* out_n, ExpectedRecord** out_exp) {
    size_t cap = tags->len < 32 ? tags->len : 32;
    if (cap == 0) return -1;

    OptbinlogRecord* recs = calloc(cap, sizeof(OptbinlogRecord));
    ExpectedRecord* exp = calloc(cap, sizeof(ExpectedRecord));
    if (!recs || !exp) {
        free(recs);
        free(exp);
        return -1;
    }

    size_t n = 0;
    for (size_t i = 0; i < tags->len && n < cap; i++) {
        const OptbinlogTagDef* tag = &tags->items[i];
        if (tag->ele_num <= 0) continue;

        recs[n].timestamp = 1712000000 + (int64_t)n;
        recs[n].tag_id = tag->tag_id;
        recs[n].ele_count = tag->ele_num;
        recs[n].values = calloc((size_t)tag->ele_num, sizeof(OptbinlogValue));

        exp[n].timestamp = recs[n].timestamp;
        exp[n].tag_id = recs[n].tag_id;
        exp[n].ele_count = recs[n].ele_count;
        exp[n].values = calloc((size_t)tag->ele_num, sizeof(ExpectedValue));

        if (!recs[n].values || !exp[n].values) {
            free_records(recs, cap);
            free_expected(exp, cap);
            return -1;
        }

        for (int e = 0; e < tag->ele_num; e++) {
            const OptbinlogTagEleDef* ele = &tag->eles[e];
            if (ele->type_char == 'L') {
                uint64_t maxv = max_for_bits(ele->bits);
                uint64_t v = maxv;
                uint64_t delta = (uint64_t)((n + (size_t)e) % 7);
                if (maxv > delta) v = maxv - delta;

                recs[n].values[e] = (OptbinlogValue){OPTBINLOG_VAL_U, v, 0.0, NULL};
                exp[n].values[e] = (ExpectedValue){OPTBINLOG_VAL_U, v, 0.0, NULL};
            } else if (ele->type_char == 'D') {
                double v = ((double)(n * 17 + (size_t)e) / 3.0) + 0.125;
                recs[n].values[e] = (OptbinlogValue){OPTBINLOG_VAL_D, 0, v, NULL};
                exp[n].values[e] = (ExpectedValue){OPTBINLOG_VAL_D, 0, v, NULL};
            } else if (ele->type_char == 'S') {
                const char* src = "";
                if (e % 3 == 1) src = "rt";
                if (e % 3 == 2) src = "roundtrip-overflow-sample-string";

                char* store = strdup(src);
                if (!store) {
                    free_records(recs, cap);
                    free_expected(exp, cap);
                    return -1;
                }

                int slen = ele->bits / 8;
                if (slen <= 0) slen = 1;
                char* exs = calloc((size_t)slen + 1, 1);
                if (!exs) {
                    free(store);
                    free_records(recs, cap);
                    free_expected(exp, cap);
                    return -1;
                }
                size_t copy_n = strnlen(src, (size_t)slen);
                memcpy(exs, src, copy_n);

                recs[n].values[e] = (OptbinlogValue){OPTBINLOG_VAL_S, 0, 0.0, store};
                exp[n].values[e] = (ExpectedValue){OPTBINLOG_VAL_S, 0, 0.0, exs};
            } else {
                free_records(recs, cap);
                free_expected(exp, cap);
                return -1;
            }
        }

        n++;
    }

    if (n == 0) {
        free_records(recs, cap);
        free_expected(exp, cap);
        return -1;
    }

    *out_recs = recs;
    *out_n = n;
    *out_exp = exp;
    return 0;
}

static int verify_cb(const OptbinlogRecord* rec, void* user) {
    (void)user;
    if (g_read_count >= g_expected_count) {
        g_mismatch = 1;
        return 1;
    }
    const ExpectedRecord* exp = &g_expected[g_read_count];
    if (rec->timestamp != exp->timestamp || rec->tag_id != exp->tag_id || rec->ele_count != exp->ele_count) {
        g_mismatch = 1;
        return 1;
    }

    for (int i = 0; i < rec->ele_count; i++) {
        if (rec->values[i].kind != exp->values[i].kind) {
            g_mismatch = 1;
            return 1;
        }
        if (rec->values[i].kind == OPTBINLOG_VAL_U) {
            if (rec->values[i].u != exp->values[i].u) {
                g_mismatch = 1;
                return 1;
            }
        } else if (rec->values[i].kind == OPTBINLOG_VAL_D) {
            if (fabs(rec->values[i].d - exp->values[i].d) > 1e-9) {
                g_mismatch = 1;
                return 1;
            }
        } else if (rec->values[i].kind == OPTBINLOG_VAL_S) {
            const char* got = rec->values[i].s ? rec->values[i].s : "";
            const char* want = exp->values[i].s ? exp->values[i].s : "";
            if (strcmp(got, want) != 0) {
                g_mismatch = 1;
                return 1;
            }
        }
    }

    g_read_count++;
    return 0;
}

int main(int argc, char** argv) {
    const char* eventlog_dir = NULL;
    const char* shared_path = NULL;
    const char* log_path = NULL;
    const char* bad_path = NULL;
    const char* trunc_path = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--eventlog-dir") == 0 && i + 1 < argc) {
            eventlog_dir = argv[++i];
        } else if (strcmp(argv[i], "--shared") == 0 && i + 1 < argc) {
            shared_path = argv[++i];
        } else if (strcmp(argv[i], "--log") == 0 && i + 1 < argc) {
            log_path = argv[++i];
        } else if (strcmp(argv[i], "--bad-log") == 0 && i + 1 < argc) {
            bad_path = argv[++i];
        } else if (strcmp(argv[i], "--trunc-log") == 0 && i + 1 < argc) {
            trunc_path = argv[++i];
        }
    }

    if (!eventlog_dir || !shared_path || !log_path || !bad_path || !trunc_path) {
        usage(argv[0]);
        return 1;
    }

    unlink(shared_path);
    unlink(log_path);
    unlink(bad_path);
    unlink(trunc_path);

    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0) {
        fprintf(stderr, "parse eventlog failed\n");
        return 1;
    }

    OptbinlogRecord* recs = NULL;
    size_t rec_count = 0;
    ExpectedRecord* exp = NULL;
    if (fill_test_records(&tags, &recs, &rec_count, &exp) != 0) {
        fprintf(stderr, "build test records failed\n");
        optbinlog_taglist_free(&tags);
        return 1;
    }

    if (optbinlog_shared_init_from_dir(eventlog_dir, shared_path, 0) != 0) {
        fprintf(stderr, "shared init failed: %s\n", strerror(errno));
        free_records(recs, rec_count);
        free_expected(exp, rec_count);
        optbinlog_taglist_free(&tags);
        return 1;
    }

    if (optbinlog_binlog_write(shared_path, log_path, recs, rec_count) != 0) {
        fprintf(stderr, "write failed\n");
        free_records(recs, rec_count);
        free_expected(exp, rec_count);
        optbinlog_taglist_free(&tags);
        return 1;
    }

    g_expected = exp;
    g_expected_count = rec_count;
    g_read_count = 0;
    g_mismatch = 0;
    (void)optbinlog_binlog_read(shared_path, log_path, verify_cb, NULL);
    int roundtrip_ok = (g_mismatch == 0 && g_read_count == g_expected_count) ? 1 : 0;

    int bad_tag_detected = 0;
    if (copy_file(log_path, bad_path) == 0 && mutate_first_tag_id(bad_path) == 0) {
        int rc_bad = optbinlog_binlog_read(shared_path, bad_path, NULL, NULL);
        bad_tag_detected = (rc_bad != 0) ? 1 : 0;
    }

    int truncated_detected = 0;
    if (copy_file(log_path, trunc_path) == 0 && truncate_tail(trunc_path, 1) == 0) {
        int rc_trunc = optbinlog_binlog_read(shared_path, trunc_path, NULL, NULL);
        truncated_detected = (rc_trunc != 0) ? 1 : 0;
    }

    printf("roundtrip_ok,%d,bad_tag_detected,%d,truncated_detected,%d,records_checked,%zu\n",
           roundtrip_ok, bad_tag_detected, truncated_detected, rec_count);

    free_records(recs, rec_count);
    free_expected(exp, rec_count);
    optbinlog_taglist_free(&tags);

    return (roundtrip_ok && bad_tag_detected) ? 0 : 1;
}
