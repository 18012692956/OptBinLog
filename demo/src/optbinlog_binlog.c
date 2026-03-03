#include "optbinlog_binlog.h"
#include "optbinlog_shared.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>

typedef struct {
    OptbinlogEventTag* tag;
    OptbinlogEventTagEle* eles;
    int ele_count;
    int record_size;
} OptbinlogTagCacheEntry;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int build_tag_cache(void* base, OptbinlogSharedTag* header, OptbinlogTagCacheEntry** out_cache, int* out_len) {
    int total_ids = (int)header->num_arrays * OPTBINLOG_EVENT_TAG_ARRAY_LEN;
    OptbinlogTagCacheEntry* cache = calloc((size_t)total_ids, sizeof(OptbinlogTagCacheEntry));
    if (!cache) return -1;

    OptbinlogBitmap* bitmap = (OptbinlogBitmap*)((uint8_t*)base + header->bitmap_offset);
    OptbinlogEventTag* tags = (OptbinlogEventTag*)((uint8_t*)base + header->eventtag_offset);

    int prefix = 0;
    for (unsigned int arr = 0; arr < header->num_arrays; arr++) {
        int arr_max = optbinlog_bitmap_get_max(&bitmap[arr]);
        for (int idx = 0; idx < arr_max; idx++) {
            int tag_id = (int)(arr * OPTBINLOG_EVENT_TAG_ARRAY_LEN + idx);
            if (!optbinlog_bitmap_get(&bitmap[arr], idx)) continue;
            OptbinlogEventTag* tag = &tags[prefix + idx];
            OptbinlogEventTagEle* eles = (OptbinlogEventTagEle*)((uint8_t*)base + tag->tag_ele_offset);

            int size = 8 + 2 + 1;
            for (int e = 0; e < tag->tag_ele_num; e++) {
                if (eles[e].type == 2) size += 8;
                else size += (int)eles[e].len;
            }

            cache[tag_id].tag = tag;
            cache[tag_id].eles = eles;
            cache[tag_id].ele_count = (int)tag->tag_ele_num;
            cache[tag_id].record_size = size;
        }
        prefix += arr_max;
    }

    *out_cache = cache;
    *out_len = total_ids;
    return 0;
}

static uint64_t read_uint_n(const uint8_t* data, int nbytes) {
    uint64_t v = 0;
    for (int i = 0; i < nbytes; i++) {
        v |= ((uint64_t)data[i]) << (i * 8);
    }
    return v;
}

int optbinlog_binlog_write(const char* shared_path, const char* log_path, const OptbinlogRecord* records, size_t count) {
    const char* prof_env = getenv("OPTBINLOG_PROFILE");
    int profile = (prof_env && prof_env[0] == '1') ? 1 : 0;
    uint64_t t_cache = 0;
    uint64_t t_pack = 0;
    uint64_t t_write = 0;

    void* base = NULL;
    size_t map_size = 0;
    OptbinlogSharedTag* header = NULL;
    if (optbinlog_shared_open(shared_path, &base, &map_size, &header) != 0) {
        fprintf(stderr, "open shared file failed\n");
        return -1;
    }

    OptbinlogTagCacheEntry* cache = NULL;
    int cache_len = 0;
    uint64_t t0 = profile ? now_ns() : 0;
    if (build_tag_cache(base, header, &cache, &cache_len) != 0) {
        fprintf(stderr, "build cache failed\n");
        optbinlog_shared_close(base, map_size);
        return -1;
    }
    if (profile) t_cache += now_ns() - t0;

    FILE* fp = fopen(log_path, "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", log_path, strerror(errno));
        free(cache);
        optbinlog_shared_close(base, map_size);
        return -1;
    }

    const size_t buf_cap = 256 * 1024;
    uint8_t* buf = malloc(buf_cap);
    if (!buf) {
        fprintf(stderr, "OOM\n");
        fclose(fp);
        free(cache);
        optbinlog_shared_close(base, map_size);
        return -1;
    }
    size_t buf_used = 0;
    uint8_t* rec_buf = NULL;
    size_t rec_cap = 0;

    for (size_t i = 0; i < count; i++) {
        const OptbinlogRecord* rec = &records[i];
        if (rec->tag_id < 0 || rec->tag_id >= cache_len || !cache[rec->tag_id].tag) {
            fprintf(stderr, "invalid tag %d\n", rec->tag_id);
            buf_used = 0;
            free(buf);
            free(rec_buf);
            fclose(fp);
            free(cache);
            optbinlog_shared_close(base, map_size);
            return -1;
        }
        OptbinlogTagCacheEntry* entry = &cache[rec->tag_id];
        if (rec->ele_count != entry->ele_count) {
            fprintf(stderr, "element count mismatch for tag %d\n", rec->tag_id);
            buf_used = 0;
            free(buf);
            free(rec_buf);
            fclose(fp);
            free(cache);
            optbinlog_shared_close(base, map_size);
            return -1;
        }

        uint16_t tag_id_u16 = (uint16_t)rec->tag_id;
        uint8_t ele_count = (uint8_t)rec->ele_count;
        int record_size = entry->record_size;
        uint8_t* dst = NULL;
        if (record_size <= (int)buf_cap) {
            if (buf_used + (size_t)record_size > buf_cap) {
                uint64_t t4 = profile ? now_ns() : 0;
                fwrite(buf, 1, buf_used, fp);
                if (profile) t_write += now_ns() - t4;
                buf_used = 0;
            }
            dst = buf + buf_used;
        } else {
            if (rec_cap < (size_t)record_size) {
                size_t new_cap = (size_t)record_size;
                uint8_t* next = realloc(rec_buf, new_cap);
                if (!next) {
                    fprintf(stderr, "OOM\n");
                    free(buf);
                    free(rec_buf);
                    fclose(fp);
                    free(cache);
                    optbinlog_shared_close(base, map_size);
                    return -1;
                }
                rec_buf = next;
                rec_cap = new_cap;
            }
            dst = rec_buf;
        }
        uint64_t t1 = profile ? now_ns() : 0;
        size_t off = 0;
        memcpy(dst + off, &rec->timestamp, sizeof(int64_t));
        off += sizeof(int64_t);
        memcpy(dst + off, &tag_id_u16, sizeof(uint16_t));
        off += sizeof(uint16_t);
        memcpy(dst + off, &ele_count, sizeof(uint8_t));
        off += sizeof(uint8_t);

        OptbinlogEventTagEle* eles = entry->eles;
        for (int e = 0; e < rec->ele_count; e++) {
            OptbinlogEventTagEle* ele = &eles[e];
            const OptbinlogValue* v = &rec->values[e];
            if (ele->type == 1) {
                if (v->kind != OPTBINLOG_VAL_U) {
                    fprintf(stderr, "type mismatch for tag %d\n", rec->tag_id);
                    free(rec_buf);
                    free(buf);
                    fclose(fp);
                    free(cache);
                    optbinlog_shared_close(base, map_size);
                    return -1;
                }
                for (int b = 0; b < (int)ele->len; b++) {
                    dst[off + b] = (uint8_t)((v->u >> (b * 8)) & 0xFF);
                }
                off += (size_t)ele->len;
            } else if (ele->type == 2) {
                if (v->kind != OPTBINLOG_VAL_D) {
                    fprintf(stderr, "type mismatch for tag %d\n", rec->tag_id);
                    free(rec_buf);
                    free(buf);
                    fclose(fp);
                    free(cache);
                    optbinlog_shared_close(base, map_size);
                    return -1;
                }
                memcpy(dst + off, &v->d, sizeof(double));
                off += sizeof(double);
            } else if (ele->type == 3) {
                if (v->kind != OPTBINLOG_VAL_S) {
                    fprintf(stderr, "type mismatch for tag %d\n", rec->tag_id);
                    free(buf);
                    free(rec_buf);
                    fclose(fp);
                    free(cache);
                    optbinlog_shared_close(base, map_size);
                    return -1;
                }
                memset(dst + off, 0, (size_t)ele->len);
                if (v->s) {
                    size_t slen = strnlen(v->s, (size_t)ele->len);
                    if (slen > (size_t)ele->len) slen = (size_t)ele->len;
                    memcpy(dst + off, v->s, slen);
                }
                off += (size_t)ele->len;
            }
        }
        if (profile) t_pack += now_ns() - t1;

        if (record_size > (int)buf_cap) {
            uint64_t t3 = profile ? now_ns() : 0;
            fwrite(dst, 1, (size_t)record_size, fp);
            if (profile) t_write += now_ns() - t3;
        } else {
            buf_used += (size_t)record_size;
        }
    }

    if (buf_used > 0) {
        uint64_t t5 = profile ? now_ns() : 0;
        fwrite(buf, 1, buf_used, fp);
        if (profile) t_write += now_ns() - t5;
    }

    fclose(fp);
    free(buf);
    free(rec_buf);
    free(cache);
    optbinlog_shared_close(base, map_size);

    if (profile) {
        double ms_cache = (double)t_cache / 1e6;
        double ms_pack = (double)t_pack / 1e6;
        double ms_write = (double)t_write / 1e6;
        fprintf(stderr, "OPTBINLOG_PROFILE cache_ms=%.3f pack_ms=%.3f write_ms=%.3f\n", ms_cache, ms_pack, ms_write);
    }
    return 0;
}

int optbinlog_binlog_read(const char* shared_path, const char* log_path, OptbinlogRecordCallback cb, void* user) {
    void* base = NULL;
    size_t map_size = 0;
    OptbinlogSharedTag* header = NULL;
    if (optbinlog_shared_open(shared_path, &base, &map_size, &header) != 0) {
        fprintf(stderr, "open shared file failed\n");
        return -1;
    }

    FILE* fp = fopen(log_path, "rb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", log_path, strerror(errno));
        optbinlog_shared_close(base, map_size);
        return -1;
    }

    while (1) {
        OptbinlogRecord rec;
        uint16_t tag_id;
        uint8_t ele_count;
        if (fread(&rec.timestamp, sizeof(int64_t), 1, fp) != 1) break;
        if (fread(&tag_id, sizeof(uint16_t), 1, fp) != 1) break;
        if (fread(&ele_count, sizeof(uint8_t), 1, fp) != 1) break;

        rec.tag_id = (int)tag_id;
        rec.ele_count = (int)ele_count;

        OptbinlogEventTag* tag = optbinlog_lookup_tag(base, header, rec.tag_id, rec.ele_count);
        if (!tag) {
            fprintf(stderr, "invalid record tag %u\n", (unsigned)tag_id);
            fclose(fp);
            optbinlog_shared_close(base, map_size);
            return -1;
        }

        OptbinlogValue* values = calloc((size_t)rec.ele_count, sizeof(OptbinlogValue));
        if (!values) {
            fprintf(stderr, "OOM\n");
            fclose(fp);
            optbinlog_shared_close(base, map_size);
            return -1;
        }
        rec.values = values;

        OptbinlogEventTagEle* eles = (OptbinlogEventTagEle*)((uint8_t*)base + tag->tag_ele_offset);
        for (int e = 0; e < rec.ele_count; e++) {
            OptbinlogEventTagEle* ele = &eles[e];
            if (ele->type == 1) {
                uint8_t buf[8] = {0};
                fread(buf, 1, ele->len, fp);
                values[e].kind = OPTBINLOG_VAL_U;
                values[e].u = read_uint_n(buf, ele->len);
            } else if (ele->type == 2) {
                double v;
                fread(&v, sizeof(double), 1, fp);
                values[e].kind = OPTBINLOG_VAL_D;
                values[e].d = v;
            } else if (ele->type == 3) {
                char* buf = calloc((size_t)ele->len + 1, 1);
                if (!buf) {
                    fprintf(stderr, "OOM\n");
                    fclose(fp);
                    optbinlog_shared_close(base, map_size);
                    return -1;
                }
                fread(buf, 1, ele->len, fp);
                values[e].kind = OPTBINLOG_VAL_S;
                values[e].s = buf;
            }
        }

        if (cb) {
            if (cb(&rec, user) != 0) {
                for (int e = 0; e < rec.ele_count; e++) {
                    if (values[e].kind == OPTBINLOG_VAL_S) free((void*)values[e].s);
                }
                free(values);
                break;
            }
        }

        for (int e = 0; e < rec.ele_count; e++) {
            if (values[e].kind == OPTBINLOG_VAL_S) free((void*)values[e].s);
        }
        free(values);
    }

    fclose(fp);
    optbinlog_shared_close(base, map_size);
    return 0;
}
