#include "optbinlog_binlog.h"
#include "optbinlog_shared.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    OUTPUT_TABLE = 0,
    OUTPUT_JSONL = 1,
} OutputFormat;

typedef struct {
    int tag_id;
    uint64_t count;
    char tag_name[48];
} TagCounter;

typedef struct {
    void* base;
    size_t map_size;
    OptbinlogSharedTag* header;

    OutputFormat format;
    size_t limit;
    int show_summary;
    size_t seen;

    TagCounter* counters;
    size_t counters_len;
    size_t counters_cap;
} ReaderCtx;

static void usage(const char* prog) {
    fprintf(stderr,
            "Usage:\n"
            "  %s --shared <shared_eventtag.bin> --log <run.bin> [--format table|jsonl] [--limit N] [--summary] [--repair-tail]\n"
            "\n"
            "Examples:\n"
            "  %s --shared ./shared_eventtag.bin --log ./binary_run_000.bin\n"
            "  %s --shared ./shared_eventtag.bin --log ./binary_run_000.bin --format jsonl --limit 20 --summary\n"
            "  %s --shared ./shared_eventtag.bin --log ./binary_run_000.bin --repair-tail\n",
            prog, prog, prog, prog);
}

static const char* safe_tag_name(const OptbinlogEventTag* tag) {
    return (tag && tag->tag_name[0]) ? tag->tag_name : "unknown_tag";
}

static const char* safe_ele_name(const OptbinlogEventTagEle* ele, int idx, char* buf, size_t buf_len) {
    if (ele && ele->name[0]) return ele->name;
    if (buf && buf_len > 0) {
        (void)snprintf(buf, buf_len, "field_%d", idx);
        return buf;
    }
    return "field";
}

static void print_json_string(const char* s) {
    const unsigned char* p = (const unsigned char*)(s ? s : "");
    putchar('"');
    while (*p) {
        unsigned char c = *p++;
        switch (c) {
            case '\"':
                fputs("\\\"", stdout);
                break;
            case '\\':
                fputs("\\\\", stdout);
                break;
            case '\b':
                fputs("\\b", stdout);
                break;
            case '\f':
                fputs("\\f", stdout);
                break;
            case '\n':
                fputs("\\n", stdout);
                break;
            case '\r':
                fputs("\\r", stdout);
                break;
            case '\t':
                fputs("\\t", stdout);
                break;
            default:
                if (c < 0x20) {
                    (void)printf("\\u%04x", (unsigned)c);
                } else {
                    putchar((int)c);
                }
                break;
        }
    }
    putchar('"');
}

static int touch_counter(ReaderCtx* ctx, int tag_id, const char* tag_name) {
    for (size_t i = 0; i < ctx->counters_len; i++) {
        if (ctx->counters[i].tag_id == tag_id) {
            ctx->counters[i].count++;
            return 0;
        }
    }
    if (ctx->counters_len == ctx->counters_cap) {
        size_t next_cap = ctx->counters_cap == 0 ? 16 : ctx->counters_cap * 2;
        TagCounter* next = (TagCounter*)realloc(ctx->counters, next_cap * sizeof(TagCounter));
        if (!next) return -1;
        ctx->counters = next;
        ctx->counters_cap = next_cap;
    }
    TagCounter* c = &ctx->counters[ctx->counters_len++];
    c->tag_id = tag_id;
    c->count = 1;
    memset(c->tag_name, 0, sizeof(c->tag_name));
    if (tag_name) {
        strncpy(c->tag_name, tag_name, sizeof(c->tag_name) - 1);
    }
    return 0;
}

static int print_table_record(const OptbinlogRecord* rec, const OptbinlogEventTag* tag, const OptbinlogEventTagEle* eles, size_t idx) {
    (void)printf("#%zu ts=%" PRId64 " tag=%s(id=%d) ",
                 idx,
                 rec->timestamp,
                 safe_tag_name(tag),
                 rec->tag_id);
    fputs("values=", stdout);
    for (int i = 0; i < rec->ele_count; i++) {
        char fallback[32];
        const char* key = safe_ele_name(eles ? &eles[i] : NULL, i, fallback, sizeof(fallback));
        if (i > 0) fputs(", ", stdout);
        (void)printf("%s=", key);
        if (rec->values[i].kind == OPTBINLOG_VAL_U) {
            (void)printf("%" PRIu64, rec->values[i].u);
        } else if (rec->values[i].kind == OPTBINLOG_VAL_D) {
            (void)printf("%.17g", rec->values[i].d);
        } else if (rec->values[i].kind == OPTBINLOG_VAL_S) {
            (void)printf("\"%s\"", rec->values[i].s ? rec->values[i].s : "");
        } else {
            fputs("null", stdout);
        }
    }
    putchar('\n');
    return 0;
}

static int print_jsonl_record(const OptbinlogRecord* rec, const OptbinlogEventTag* tag, const OptbinlogEventTagEle* eles, size_t idx) {
    (void)printf("{\"index\":%zu,\"timestamp\":%" PRId64 ",\"tag_id\":%d,\"tag\":",
                 idx,
                 rec->timestamp,
                 rec->tag_id);
    print_json_string(safe_tag_name(tag));
    fputs(",\"fields\":[", stdout);
    for (int i = 0; i < rec->ele_count; i++) {
        char fallback[32];
        const char* key = safe_ele_name(eles ? &eles[i] : NULL, i, fallback, sizeof(fallback));
        if (i > 0) putchar(',');
        fputs("{\"name\":", stdout);
        print_json_string(key);
        fputs(",\"kind\":", stdout);
        if (rec->values[i].kind == OPTBINLOG_VAL_U) {
            fputs("\"u\",\"value\":", stdout);
            (void)printf("%" PRIu64, rec->values[i].u);
        } else if (rec->values[i].kind == OPTBINLOG_VAL_D) {
            fputs("\"d\",\"value\":", stdout);
            (void)printf("%.17g", rec->values[i].d);
        } else if (rec->values[i].kind == OPTBINLOG_VAL_S) {
            fputs("\"s\",\"value\":", stdout);
            print_json_string(rec->values[i].s ? rec->values[i].s : "");
        } else {
            fputs("\"unknown\",\"value\":null", stdout);
        }
        putchar('}');
    }
    fputs("]}\n", stdout);
    return 0;
}

static int read_cb(const OptbinlogRecord* rec, void* user) {
    ReaderCtx* ctx = (ReaderCtx*)user;
    if (ctx->limit > 0 && ctx->seen >= ctx->limit) {
        return 1;
    }

    OptbinlogEventTag* tag = NULL;
    OptbinlogEventTagEle* eles = NULL;
    if (ctx->base && ctx->header) {
        tag = optbinlog_lookup_tag(ctx->base, ctx->header, rec->tag_id, rec->ele_count);
        if (tag) {
            eles = (OptbinlogEventTagEle*)((unsigned char*)ctx->base + tag->tag_ele_offset);
        }
    }
    if (touch_counter(ctx, rec->tag_id, safe_tag_name(tag)) != 0) {
        fprintf(stderr, "counter OOM\n");
        return -1;
    }

    ctx->seen++;
    if (ctx->format == OUTPUT_JSONL) {
        return print_jsonl_record(rec, tag, eles, ctx->seen);
    }
    return print_table_record(rec, tag, eles, ctx->seen);
}

static int counter_cmp(const void* a, const void* b) {
    const TagCounter* x = (const TagCounter*)a;
    const TagCounter* y = (const TagCounter*)b;
    if (x->count < y->count) return 1;
    if (x->count > y->count) return -1;
    if (x->tag_id < y->tag_id) return -1;
    if (x->tag_id > y->tag_id) return 1;
    return 0;
}

static void print_summary(ReaderCtx* ctx) {
    if (!ctx->show_summary) return;
    if (ctx->counters_len > 1) {
        qsort(ctx->counters, ctx->counters_len, sizeof(TagCounter), counter_cmp);
    }
    (void)printf("\nsummary: records=%zu, tags=%zu\n", ctx->seen, ctx->counters_len);
    for (size_t i = 0; i < ctx->counters_len; i++) {
        const char* name = ctx->counters[i].tag_name[0] ? ctx->counters[i].tag_name : "unknown_tag";
        (void)printf("  - %s (id=%d): %" PRIu64 "\n", name, ctx->counters[i].tag_id, ctx->counters[i].count);
    }
}

int main(int argc, char** argv) {
    const char* shared_path = NULL;
    const char* log_path = NULL;
    ReaderCtx ctx;
    int repair_tail = 0;
    memset(&ctx, 0, sizeof(ctx));
    ctx.format = OUTPUT_TABLE;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--shared") == 0 && i + 1 < argc) {
            shared_path = argv[++i];
        } else if (strcmp(argv[i], "--log") == 0 && i + 1 < argc) {
            log_path = argv[++i];
        } else if (strcmp(argv[i], "--format") == 0 && i + 1 < argc) {
            const char* fmt = argv[++i];
            if (strcmp(fmt, "table") == 0) {
                ctx.format = OUTPUT_TABLE;
            } else if (strcmp(fmt, "jsonl") == 0) {
                ctx.format = OUTPUT_JSONL;
            } else {
                fprintf(stderr, "unknown format: %s\n", fmt);
                usage(argv[0]);
                return 1;
            }
        } else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc) {
            char* end = NULL;
            unsigned long long v = strtoull(argv[++i], &end, 10);
            if (!end || *end != '\0') {
                fprintf(stderr, "invalid --limit value\n");
                return 1;
            }
            ctx.limit = (size_t)v;
        } else if (strcmp(argv[i], "--summary") == 0) {
            ctx.show_summary = 1;
        } else if (strcmp(argv[i], "--repair-tail") == 0) {
            repair_tail = 1;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "unknown arg: %s\n", argv[i]);
            usage(argv[0]);
            return 1;
        }
    }

    if (!shared_path || !log_path) {
        usage(argv[0]);
        return 1;
    }

    if (optbinlog_shared_open(shared_path, &ctx.base, &ctx.map_size, &ctx.header) != 0) {
        fprintf(stderr, "open shared file failed: %s\n", shared_path);
        return 1;
    }

    if (repair_tail) {
        size_t before_bytes = 0;
        size_t after_bytes = 0;
        int repair_rc = optbinlog_binlog_recover_tail(log_path, &before_bytes, &after_bytes);
        if (repair_rc < 0) {
            fprintf(stderr, "tail repair failed: %s\n", log_path);
            free(ctx.counters);
            optbinlog_shared_close(ctx.base, ctx.map_size);
            return 1;
        }
        if (repair_rc > 0) {
            fprintf(stderr,
                    "tail repair applied: %s, %zu -> %zu (drop %zu bytes)\n",
                    log_path,
                    before_bytes,
                    after_bytes,
                    before_bytes - after_bytes);
        } else {
            fprintf(stderr, "tail repair clean: %s\n", log_path);
        }
    }

    int rc = optbinlog_binlog_read(shared_path, log_path, read_cb, &ctx);
    if (rc != 0) {
        fprintf(stderr, "read failed: %s\n", log_path);
    }

    print_summary(&ctx);
    free(ctx.counters);
    optbinlog_shared_close(ctx.base, ctx.map_size);
    return rc == 0 ? 0 : 1;
}
