#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define QUEUE_CAP 4096

typedef struct {
    uint64_t ts_ns;
    uint32_t seq;
    uint32_t code;
    int32_t temp_x10;
    uint16_t tag;
    uint16_t level;
} PackedLog;

typedef struct {
    PackedLog* buf;
    size_t cap;
    size_t head;
    size_t tail;
    size_t count;
    int producer_done;
    pthread_mutex_t mu;
    pthread_cond_t cv_not_empty;
    pthread_cond_t cv_not_full;
} LogQueue;

typedef struct {
    LogQueue* q;
    FILE* fp;
    uint64_t bytes_written;
} ConsumerCtx;

typedef struct {
    uint64_t realtime_ns;
    uint64_t mono_ns;
    uint32_t seq;
    uint16_t fields;
    uint16_t priority;
    uint32_t payload_len;
    uint64_t payload_hash;
} JournalObjHeader;

typedef struct {
    uint32_t sec;
    uint32_t nsec;
    uint16_t domain;
    uint16_t tag;
    uint8_t level;
    uint8_t reserved;
    uint16_t msg_len;
} HiLogLiteHeader;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
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

static uint64_t fnv1a64(const uint8_t* data, size_t len) {
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint64_t)data[i];
        h *= 1099511628211ull;
    }
    return h;
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

static size_t encode_semantic_payload(uint8_t* out, const PackedLog* r) {
    size_t off = 0;
    memcpy(out + off, &r->ts_ns, sizeof(uint64_t));
    off += sizeof(uint64_t);

    write_le16(out + off, r->tag);
    off += sizeof(uint16_t);

    out[off++] = 4u;

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

static size_t encode_semantic_frame(uint8_t* out, const PackedLog* r) {
    uint8_t payload[64];
    size_t payload_len = encode_semantic_payload(payload, r);
    write_le32(out, (uint32_t)payload_len);
    memcpy(out + 4, payload, payload_len);
    uint32_t crc = crc32_compute(payload, payload_len);
    write_le32(out + 4 + payload_len, crc);
    return 4 + payload_len + 4;
}

static long max_rss_kb(void) {
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) != 0) {
        return 0;
    }
#ifdef __APPLE__
    return ru.ru_maxrss / 1024;
#else
    return ru.ru_maxrss;
#endif
}

static int ensure_parent_dir(const char* path) {
    if (!path || !path[0]) return -1;
    char tmp[1024];
    size_t n = strlen(path);
    if (n >= sizeof(tmp)) return -1;
    memcpy(tmp, path, n + 1);
    for (size_t i = 1; i < n; i++) {
        if (tmp[i] == '/') {
            tmp[i] = '\0';
            if (tmp[0] != '\0') {
                if (mkdir(tmp, 0777) != 0 && errno != EEXIST) {
                    return -1;
                }
            }
            tmp[i] = '/';
        }
    }
    return 0;
}

static PackedLog make_log(long i) {
    PackedLog r;
    r.ts_ns = now_ns();
    r.seq = (uint32_t)i;
    r.code = (uint32_t)(1000 + (i % 250));
    r.temp_x10 = (int32_t)(200 + (i % 80));
    r.tag = (uint16_t)(3000 + (i % 32));
    r.level = (uint16_t)(i % 8);
    return r;
}

static int queue_init(LogQueue* q, size_t cap) {
    memset(q, 0, sizeof(*q));
    q->buf = (PackedLog*)calloc(cap, sizeof(PackedLog));
    if (!q->buf) return -1;
    q->cap = cap;
    if (pthread_mutex_init(&q->mu, NULL) != 0) return -1;
    if (pthread_cond_init(&q->cv_not_empty, NULL) != 0) return -1;
    if (pthread_cond_init(&q->cv_not_full, NULL) != 0) return -1;
    return 0;
}

static void queue_destroy(LogQueue* q) {
    if (!q) return;
    pthread_cond_destroy(&q->cv_not_empty);
    pthread_cond_destroy(&q->cv_not_full);
    pthread_mutex_destroy(&q->mu);
    free(q->buf);
    memset(q, 0, sizeof(*q));
}

static int queue_push(LogQueue* q, PackedLog item) {
    if (pthread_mutex_lock(&q->mu) != 0) return -1;
    while (q->count == q->cap) {
        if (pthread_cond_wait(&q->cv_not_full, &q->mu) != 0) {
            pthread_mutex_unlock(&q->mu);
            return -1;
        }
    }
    q->buf[q->tail] = item;
    q->tail = (q->tail + 1) % q->cap;
    q->count++;
    pthread_cond_signal(&q->cv_not_empty);
    pthread_mutex_unlock(&q->mu);
    return 0;
}

static int queue_pop(LogQueue* q, PackedLog* out, int* done) {
    if (pthread_mutex_lock(&q->mu) != 0) return -1;
    while (q->count == 0 && !q->producer_done) {
        if (pthread_cond_wait(&q->cv_not_empty, &q->mu) != 0) {
            pthread_mutex_unlock(&q->mu);
            return -1;
        }
    }
    if (q->count == 0 && q->producer_done) {
        *done = 1;
        pthread_mutex_unlock(&q->mu);
        return 0;
    }
    *out = q->buf[q->head];
    q->head = (q->head + 1) % q->cap;
    q->count--;
    *done = 0;
    pthread_cond_signal(&q->cv_not_full);
    pthread_mutex_unlock(&q->mu);
    return 0;
}

static void queue_mark_done(LogQueue* q) {
    pthread_mutex_lock(&q->mu);
    q->producer_done = 1;
    pthread_cond_broadcast(&q->cv_not_empty);
    pthread_mutex_unlock(&q->mu);
}

static void* consumer_main(void* arg) {
    ConsumerCtx* c = (ConsumerCtx*)arg;
    for (;;) {
        PackedLog item;
        int done = 0;
        if (queue_pop(c->q, &item, &done) != 0) return NULL;
        if (done) break;
        size_t n = fwrite(&item, sizeof(item), 1, c->fp);
        if (n == 1) {
            c->bytes_written += sizeof(item);
        }
    }
    fflush(c->fp);
    return NULL;
}

static void* consumer_semantic_main(void* arg) {
    ConsumerCtx* c = (ConsumerCtx*)arg;
    for (;;) {
        PackedLog item;
        int done = 0;
        if (queue_pop(c->q, &item, &done) != 0) return NULL;
        if (done) break;
        uint8_t frame[96];
        size_t n = encode_semantic_frame(frame, &item);
        if (fwrite(frame, 1, n, c->fp) == n) {
            c->bytes_written += (uint64_t)n;
        }
    }
    fflush(c->fp);
    return NULL;
}

static void* consumer_ulog_text_main(void* arg) {
    ConsumerCtx* c = (ConsumerCtx*)arg;
    for (;;) {
        PackedLog item;
        int done = 0;
        if (queue_pop(c->q, &item, &done) != 0) return NULL;
        if (done) break;
        char line[192];
        int n = snprintf(line,
                         sizeof(line),
                         "I/%u(%u): seq=%u code=%u temp=%d.%d ts=%llu\n",
                         item.tag,
                         item.level,
                         item.seq,
                         item.code,
                         item.temp_x10 / 10,
                         abs(item.temp_x10 % 10),
                         (unsigned long long)item.ts_ns);
        if (n > 0) {
            size_t w = fwrite(line, 1, (size_t)n, c->fp);
            if (w == (size_t)n) c->bytes_written += (uint64_t)w;
        }
    }
    fflush(c->fp);
    return NULL;
}

static int bench_text(const char* out_path, long records) {
    if (ensure_parent_dir(out_path) != 0) return -1;
    uint64_t t0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    uint64_t t_write0 = now_ns();
    uint64_t bytes = 0;
    for (long i = 0; i < records; i++) {
        PackedLog r = make_log(i);
        int n = fprintf(fp,
                        "seq=%u code=%u temp=%.1f tag=%u lvl=%u ts=%llu\n",
                        r.seq,
                        r.code,
                        (double)r.temp_x10 / 10.0,
                        r.tag,
                        r.level,
                        (unsigned long long)r.ts_ns);
        if (n < 0) {
            fclose(fp);
            return -1;
        }
        bytes += (uint64_t)n;
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();
    fclose(fp);
    uint64_t t1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double e2e_ms = (double)(t1 - t0) / 1e6;
    double prep_ms = (double)(t_write0 - t0) / 1e6;
    double post_ms = (double)(t1 - t_write1) / 1e6;

    printf("mode,research_text,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records,
           write_ms,
           write_ms,
           e2e_ms,
           prep_ms,
           post_ms,
           (unsigned long long)bytes,
           (unsigned long long)bytes,
           rss);
    return 0;
}

static int bench_nanolog_like(const char* out_path, long records) {
    if (ensure_parent_dir(out_path) != 0) return -1;
    uint64_t t0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    PackedLog* staging = (PackedLog*)malloc((size_t)records * sizeof(PackedLog));
    if (!staging) {
        fclose(fp);
        return -1;
    }

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        staging[i] = make_log(i);
    }
    size_t written = fwrite(staging, sizeof(PackedLog), (size_t)records, fp);
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free(staging);
    fclose(fp);
    uint64_t t1 = now_ns();

    uint64_t bytes = (uint64_t)written * (uint64_t)sizeof(PackedLog);
    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double e2e_ms = (double)(t1 - t0) / 1e6;
    double prep_ms = (double)(t_write0 - t0) / 1e6;
    double post_ms = (double)(t1 - t_write1) / 1e6;

    printf("mode,nanolog_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records,
           write_ms,
           write_ms,
           e2e_ms,
           prep_ms,
           post_ms,
           (unsigned long long)bytes,
           (unsigned long long)bytes,
           rss);
    return 0;
}

static int bench_zephyr_deferred_like(const char* out_path, long records) {
    if (ensure_parent_dir(out_path) != 0) return -1;
    uint64_t t0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    LogQueue q;
    if (queue_init(&q, QUEUE_CAP) != 0) {
        fclose(fp);
        return -1;
    }

    ConsumerCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.q = &q;
    ctx.fp = fp;

    pthread_t tid;
    if (pthread_create(&tid, NULL, consumer_main, &ctx) != 0) {
        queue_destroy(&q);
        fclose(fp);
        return -1;
    }

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        PackedLog r = make_log(i);
        if (queue_push(&q, r) != 0) {
            queue_mark_done(&q);
            pthread_join(tid, NULL);
            queue_destroy(&q);
            fclose(fp);
            return -1;
        }
    }
    queue_mark_done(&q);
    uint64_t t_write1 = now_ns();

    pthread_join(tid, NULL);
    queue_destroy(&q);
    fclose(fp);
    uint64_t t1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double e2e_ms = (double)(t1 - t0) / 1e6;
    double prep_ms = (double)(t_write0 - t0) / 1e6;
    double post_ms = (double)(t1 - t_write1) / 1e6;

    printf("mode,zephyr_deferred_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records,
           write_ms,
           write_ms,
           e2e_ms,
           prep_ms,
           post_ms,
           (unsigned long long)ctx.bytes_written,
           (unsigned long long)ctx.bytes_written,
           rss);
    return 0;
}

static int bench_nanolog_semantic_like(const char* out_path, long records) {
    if (ensure_parent_dir(out_path) != 0) return -1;
    uint64_t t0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    const size_t frame_cap = 96;
    uint8_t* staging = (uint8_t*)malloc((size_t)records * frame_cap);
    if (!staging) {
        fclose(fp);
        return -1;
    }

    uint64_t t_write0 = now_ns();
    size_t off = 0;
    for (long i = 0; i < records; i++) {
        PackedLog r = make_log(i);
        off += encode_semantic_frame(staging + off, &r);
    }
    size_t written = fwrite(staging, 1, off, fp);
    fflush(fp);
    uint64_t t_write1 = now_ns();

    free(staging);
    fclose(fp);
    uint64_t t1 = now_ns();

    uint64_t bytes = (uint64_t)written;
    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double e2e_ms = (double)(t1 - t0) / 1e6;
    double prep_ms = (double)(t_write0 - t0) / 1e6;
    double post_ms = (double)(t1 - t_write1) / 1e6;

    printf("mode,nanolog_semantic_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records,
           write_ms,
           write_ms,
           e2e_ms,
           prep_ms,
           post_ms,
           (unsigned long long)bytes,
           (unsigned long long)bytes,
           rss);
    return 0;
}

static int bench_zephyr_deferred_semantic_like(const char* out_path, long records) {
    if (ensure_parent_dir(out_path) != 0) return -1;
    uint64_t t0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    LogQueue q;
    if (queue_init(&q, QUEUE_CAP) != 0) {
        fclose(fp);
        return -1;
    }

    ConsumerCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.q = &q;
    ctx.fp = fp;

    pthread_t tid;
    if (pthread_create(&tid, NULL, consumer_semantic_main, &ctx) != 0) {
        queue_destroy(&q);
        fclose(fp);
        return -1;
    }

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        PackedLog r = make_log(i);
        if (queue_push(&q, r) != 0) {
            queue_mark_done(&q);
            pthread_join(tid, NULL);
            queue_destroy(&q);
            fclose(fp);
            return -1;
        }
    }
    queue_mark_done(&q);
    uint64_t t_write1 = now_ns();

    pthread_join(tid, NULL);
    queue_destroy(&q);
    fclose(fp);
    uint64_t t1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double e2e_ms = (double)(t1 - t0) / 1e6;
    double prep_ms = (double)(t_write0 - t0) / 1e6;
    double post_ms = (double)(t1 - t_write1) / 1e6;

    printf("mode,zephyr_deferred_semantic_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records,
           write_ms,
           write_ms,
           e2e_ms,
           prep_ms,
           post_ms,
           (unsigned long long)ctx.bytes_written,
           (unsigned long long)ctx.bytes_written,
           rss);
    return 0;
}

static int bench_ulog_async_like(const char* out_path, long records) {
    if (ensure_parent_dir(out_path) != 0) return -1;
    uint64_t t0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    LogQueue q;
    if (queue_init(&q, QUEUE_CAP) != 0) {
        fclose(fp);
        return -1;
    }

    ConsumerCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.q = &q;
    ctx.fp = fp;

    pthread_t tid;
    if (pthread_create(&tid, NULL, consumer_ulog_text_main, &ctx) != 0) {
        queue_destroy(&q);
        fclose(fp);
        return -1;
    }

    uint64_t t_write0 = now_ns();
    for (long i = 0; i < records; i++) {
        PackedLog r = make_log(i);
        if (queue_push(&q, r) != 0) {
            queue_mark_done(&q);
            pthread_join(tid, NULL);
            queue_destroy(&q);
            fclose(fp);
            return -1;
        }
    }
    queue_mark_done(&q);
    uint64_t t_write1 = now_ns();

    pthread_join(tid, NULL);
    queue_destroy(&q);
    fclose(fp);
    uint64_t t1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double e2e_ms = (double)(t1 - t0) / 1e6;
    double prep_ms = (double)(t_write0 - t0) / 1e6;
    double post_ms = (double)(t1 - t_write1) / 1e6;

    printf("mode,ulog_async_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records,
           write_ms,
           write_ms,
           e2e_ms,
           prep_ms,
           post_ms,
           (unsigned long long)ctx.bytes_written,
           (unsigned long long)ctx.bytes_written,
           rss);
    return 0;
}

static int bench_journald_like(const char* out_path, long records) {
    if (ensure_parent_dir(out_path) != 0) return -1;
    uint64_t t0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    uint64_t t_write0 = now_ns();
    uint64_t bytes = 0;
    for (long i = 0; i < records; i++) {
        PackedLog r = make_log(i);
        char payload[256];
        int n = snprintf(payload,
                         sizeof(payload),
                         "PRIORITY=%u\nSYSLOG_IDENTIFIER=optbinlog\nCODE=%u\nSEQ=%u\nTEMP_X10=%d\nTAG=%u\nLEVEL=%u\n",
                         (unsigned)(r.level & 0x7u),
                         (unsigned)r.code,
                         (unsigned)r.seq,
                         (int)r.temp_x10,
                         (unsigned)r.tag,
                         (unsigned)r.level);
        if (n <= 0) {
            fclose(fp);
            return -1;
        }
        size_t payload_len = (size_t)n;
        JournalObjHeader h;
        h.realtime_ns = r.ts_ns;
        h.mono_ns = now_ns();
        h.seq = r.seq;
        h.fields = 7;
        h.priority = (uint16_t)(r.level & 0x7u);
        h.payload_len = (uint32_t)payload_len;
        h.payload_hash = fnv1a64((const uint8_t*)payload, payload_len);

        if (fwrite(&h, sizeof(h), 1, fp) != 1) {
            fclose(fp);
            return -1;
        }
        if (fwrite(payload, 1, payload_len, fp) != payload_len) {
            fclose(fp);
            return -1;
        }

        size_t aligned = (payload_len + 7u) & ~((size_t)7u);
        if (aligned > payload_len) {
            static const uint8_t zeros[8] = {0};
            if (fwrite(zeros, 1, aligned - payload_len, fp) != aligned - payload_len) {
                fclose(fp);
                return -1;
            }
        }
        bytes += (uint64_t)sizeof(h) + (uint64_t)aligned;
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();
    fclose(fp);
    uint64_t t1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double e2e_ms = (double)(t1 - t0) / 1e6;
    double prep_ms = (double)(t_write0 - t0) / 1e6;
    double post_ms = (double)(t1 - t_write1) / 1e6;

    printf("mode,journald_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records,
           write_ms,
           write_ms,
           e2e_ms,
           prep_ms,
           post_ms,
           (unsigned long long)bytes,
           (unsigned long long)bytes,
           rss);
    return 0;
}

static int bench_hilog_lite_like(const char* out_path, long records) {
    if (ensure_parent_dir(out_path) != 0) return -1;
    uint64_t t0 = now_ns();
    FILE* fp = fopen(out_path, "wb");
    if (!fp) return -1;
    setvbuf(fp, NULL, _IOFBF, 1 << 20);

    uint64_t t_write0 = now_ns();
    uint64_t bytes = 0;
    for (long i = 0; i < records; i++) {
        PackedLog r = make_log(i);
        char msg[64];
        int n = snprintf(msg,
                         sizeof(msg),
                         "c=%u s=%u t=%d",
                         (unsigned)r.code,
                         (unsigned)r.seq,
                         (int)r.temp_x10);
        if (n <= 0) {
            fclose(fp);
            return -1;
        }
        if (n > (int)sizeof(msg)) n = (int)sizeof(msg);

        HiLogLiteHeader h;
        h.sec = (uint32_t)(r.ts_ns / 1000000000ull);
        h.nsec = (uint32_t)(r.ts_ns % 1000000000ull);
        h.domain = 0xD001u;
        h.tag = r.tag;
        h.level = (uint8_t)(r.level & 0x7u);
        h.reserved = 0;
        h.msg_len = (uint16_t)n;

        if (fwrite(&h, sizeof(h), 1, fp) != 1) {
            fclose(fp);
            return -1;
        }
        if (fwrite(msg, 1, (size_t)n, fp) != (size_t)n) {
            fclose(fp);
            return -1;
        }
        bytes += (uint64_t)sizeof(h) + (uint64_t)n;
    }
    fflush(fp);
    uint64_t t_write1 = now_ns();
    fclose(fp);
    uint64_t t1 = now_ns();

    long rss = max_rss_kb();
    double write_ms = (double)(t_write1 - t_write0) / 1e6;
    double e2e_ms = (double)(t1 - t0) / 1e6;
    double prep_ms = (double)(t_write0 - t0) / 1e6;
    double post_ms = (double)(t1 - t_write1) / 1e6;

    printf("mode,hilog_lite_like,records,%ld,elapsed_ms,%.3f,write_only_ms,%.3f,end_to_end_ms,%.3f,prep_ms,%.3f,post_ms,%.3f,bytes,%llu,shared_bytes,0,total_bytes,%llu,peak_kb,%ld\n",
           records,
           write_ms,
           write_ms,
           e2e_ms,
           prep_ms,
           post_ms,
           (unsigned long long)bytes,
           (unsigned long long)bytes,
           rss);
    return 0;
}

static void usage(const char* prog) {
    fprintf(stderr,
            "Usage: %s --mode research_text|nanolog_like|zephyr_deferred_like|ulog_async_like|hilog_lite_like|journald_like|nanolog_semantic_like|zephyr_deferred_semantic_like --out <file> --records N\n",
            prog);
}

int main(int argc, char** argv) {
    const char* mode = NULL;
    const char* out = NULL;
    long records = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            mode = argv[++i];
        } else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) {
            out = argv[++i];
        } else if (strcmp(argv[i], "--records") == 0 && i + 1 < argc) {
            records = strtol(argv[++i], NULL, 10);
        }
    }

    if (!mode || !out || records <= 0) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(mode, "research_text") == 0) return bench_text(out, records) == 0 ? 0 : 1;
    if (strcmp(mode, "nanolog_like") == 0) return bench_nanolog_like(out, records) == 0 ? 0 : 1;
    if (strcmp(mode, "zephyr_deferred_like") == 0) return bench_zephyr_deferred_like(out, records) == 0 ? 0 : 1;
    if (strcmp(mode, "ulog_async_like") == 0) return bench_ulog_async_like(out, records) == 0 ? 0 : 1;
    if (strcmp(mode, "hilog_lite_like") == 0) return bench_hilog_lite_like(out, records) == 0 ? 0 : 1;
    if (strcmp(mode, "journald_like") == 0) return bench_journald_like(out, records) == 0 ? 0 : 1;
    if (strcmp(mode, "nanolog_semantic_like") == 0) return bench_nanolog_semantic_like(out, records) == 0 ? 0 : 1;
    if (strcmp(mode, "zephyr_deferred_semantic_like") == 0) return bench_zephyr_deferred_semantic_like(out, records) == 0 ? 0 : 1;

    usage(argv[0]);
    return 1;
}
