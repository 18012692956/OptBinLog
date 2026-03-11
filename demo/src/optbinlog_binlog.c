#include "optbinlog_binlog.h"
#include "optbinlog_shared.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define OPTBINLOG_MIN_PAYLOAD_LEN 11u
#define OPTBINLOG_MAX_PAYLOAD_LEN (1024u * 1024u)

typedef struct {
    OptbinlogEventTag* tag;
    OptbinlogEventTagEle* eles;
    int ele_count;
    int payload_size;
} OptbinlogTagCacheEntry;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint32_t crc32_table[256];
static int crc32_table_ready = 0;

static void crc32_init_table(void) {
    for (uint32_t i = 0; i < 256u; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        crc32_table[i] = c;
    }
    crc32_table_ready = 1;
}

static uint32_t crc32_compute(const uint8_t* data, size_t len) {
    if (!crc32_table_ready) {
        crc32_init_table();
    }
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        crc = crc32_table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return ~crc;
}

static void write_le32(uint8_t* dst, uint32_t v) {
    dst[0] = (uint8_t)(v & 0xFFu);
    dst[1] = (uint8_t)((v >> 8) & 0xFFu);
    dst[2] = (uint8_t)((v >> 16) & 0xFFu);
    dst[3] = (uint8_t)((v >> 24) & 0xFFu);
}

static uint16_t read_le16(const uint8_t* src) {
    return (uint16_t)((uint16_t)src[0] | ((uint16_t)src[1] << 8));
}

static uint32_t read_le32(const uint8_t* src) {
    return (uint32_t)src[0] |
           ((uint32_t)src[1] << 8) |
           ((uint32_t)src[2] << 16) |
           ((uint32_t)src[3] << 24);
}

static int build_tag_cache(void* base, OptbinlogSharedTag* header, OptbinlogTagCacheEntry** out_cache, int* out_len) {
    if (!header || header->tag_count == 0 || header->num_arrays == 0) return -1;
    int total_ids = (int)header->num_arrays * OPTBINLOG_EVENT_TAG_ARRAY_LEN;
    OptbinlogTagCacheEntry* cache = calloc((size_t)total_ids, sizeof(OptbinlogTagCacheEntry));
    if (!cache) return -1;

    OptbinlogBitmap* bitmap = (OptbinlogBitmap*)((uint8_t*)base + header->bitmap_offset);
    OptbinlogEventTag* tags = (OptbinlogEventTag*)((uint8_t*)base + header->eventtag_offset);

    int prefix = 0;
    for (unsigned int arr = 0; arr < header->num_arrays; arr++) {
        int local_slot = 0;
        for (int idx = 0; idx < OPTBINLOG_EVENT_TAG_ARRAY_LEN; idx++) {
            if (!optbinlog_bitmap_get(&bitmap[arr], idx)) continue;
            if (prefix + local_slot >= (int)header->tag_count) {
                free(cache);
                return -1;
            }

            int tag_id = (int)(arr * OPTBINLOG_EVENT_TAG_ARRAY_LEN + idx);
            OptbinlogEventTag* tag = &tags[prefix + local_slot];
            if ((int)tag->tag_index != tag_id) {
                free(cache);
                return -1;
            }
            OptbinlogEventTagEle* eles = (OptbinlogEventTagEle*)((uint8_t*)base + tag->tag_ele_offset);

            int size = 8 + 2 + 1;
            for (int e = 0; e < tag->tag_ele_num; e++) {
                if (eles[e].type == 2) size += 8;
                else size += (int)eles[e].len;
            }

            cache[tag_id].tag = tag;
            cache[tag_id].eles = eles;
            cache[tag_id].ele_count = (int)tag->tag_ele_num;
            cache[tag_id].payload_size = size;
            local_slot++;
        }
        prefix += local_slot;
    }
    if (prefix != (int)header->tag_count) {
        free(cache);
        return -1;
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

static int read_exact(FILE* fp, void* out, size_t n) {
    size_t got = fread(out, 1, n, fp);
    return got == n ? 0 : -1;
}

static int write_exact(FILE* fp, const void* data, size_t n) {
    if (n == 0) return 0;
    return fwrite(data, 1, n, fp) == n ? 0 : -1;
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
    uint8_t* rec_buf = NULL;
    size_t rec_cap = 0;
    size_t buf_used = 0;
    if (!buf) {
        fprintf(stderr, "OOM\n");
        fclose(fp);
        free(cache);
        optbinlog_shared_close(base, map_size);
        return -1;
    }

    for (size_t i = 0; i < count; i++) {
        const OptbinlogRecord* rec = &records[i];
        if (rec->tag_id < 0 || rec->tag_id >= cache_len || !cache[rec->tag_id].tag) {
            fprintf(stderr, "invalid tag %d\n", rec->tag_id);
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
            free(buf);
            free(rec_buf);
            fclose(fp);
            free(cache);
            optbinlog_shared_close(base, map_size);
            return -1;
        }
        if (rec->tag_id > 0xFFFF || rec->ele_count > 0xFF) {
            fprintf(stderr, "record field overflow for tag %d\n", rec->tag_id);
            free(buf);
            free(rec_buf);
            fclose(fp);
            free(cache);
            optbinlog_shared_close(base, map_size);
            return -1;
        }

        int payload_size = entry->payload_size;
        size_t frame_size = sizeof(uint32_t) + (size_t)payload_size + sizeof(uint32_t);
        uint8_t* dst = NULL;
        if (frame_size <= buf_cap) {
            if (buf_used + frame_size > buf_cap) {
                uint64_t t4 = profile ? now_ns() : 0;
                if (write_exact(fp, buf, buf_used) != 0) {
                    fprintf(stderr, "write %s failed: %s\n", log_path, strerror(errno));
                    free(buf);
                    free(rec_buf);
                    fclose(fp);
                    free(cache);
                    optbinlog_shared_close(base, map_size);
                    return -1;
                }
                if (profile) t_write += now_ns() - t4;
                buf_used = 0;
            }
            dst = buf + buf_used;
        } else {
            if (rec_cap < frame_size) {
                uint8_t* next = realloc(rec_buf, frame_size);
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
                rec_cap = frame_size;
            }
            dst = rec_buf;
        }

        uint64_t t1 = profile ? now_ns() : 0;
        write_le32(dst, (uint32_t)payload_size);
        size_t off = sizeof(uint32_t);
        memcpy(dst + off, &rec->timestamp, sizeof(int64_t));
        off += sizeof(int64_t);

        uint16_t tag_id_u16 = (uint16_t)rec->tag_id;
        uint8_t ele_count_u8 = (uint8_t)rec->ele_count;
        memcpy(dst + off, &tag_id_u16, sizeof(uint16_t));
        off += sizeof(uint16_t);
        memcpy(dst + off, &ele_count_u8, sizeof(uint8_t));
        off += sizeof(uint8_t);

        OptbinlogEventTagEle* eles = entry->eles;
        for (int e = 0; e < rec->ele_count; e++) {
            OptbinlogEventTagEle* ele = &eles[e];
            const OptbinlogValue* v = &rec->values[e];
            if (ele->type == 1) {
                if (v->kind != OPTBINLOG_VAL_U) {
                    fprintf(stderr, "type mismatch for tag %d\n", rec->tag_id);
                    free(buf);
                    free(rec_buf);
                    fclose(fp);
                    free(cache);
                    optbinlog_shared_close(base, map_size);
                    return -1;
                }
                for (int b = 0; b < (int)ele->len; b++) {
                    dst[off + (size_t)b] = (uint8_t)((v->u >> (b * 8)) & 0xFFu);
                }
                off += (size_t)ele->len;
            } else if (ele->type == 2) {
                if (v->kind != OPTBINLOG_VAL_D) {
                    fprintf(stderr, "type mismatch for tag %d\n", rec->tag_id);
                    free(buf);
                    free(rec_buf);
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
                    memcpy(dst + off, v->s, slen);
                }
                off += (size_t)ele->len;
            }
        }

        if (off != sizeof(uint32_t) + (size_t)payload_size) {
            fprintf(stderr, "internal payload size mismatch\n");
            free(buf);
            free(rec_buf);
            fclose(fp);
            free(cache);
            optbinlog_shared_close(base, map_size);
            return -1;
        }

        uint32_t crc = crc32_compute(dst + sizeof(uint32_t), (size_t)payload_size);
        write_le32(dst + off, crc);
        off += sizeof(uint32_t);

        if (profile) t_pack += now_ns() - t1;

        if (frame_size > buf_cap) {
            uint64_t t3 = profile ? now_ns() : 0;
            if (write_exact(fp, dst, frame_size) != 0) {
                fprintf(stderr, "write %s failed: %s\n", log_path, strerror(errno));
                free(buf);
                free(rec_buf);
                fclose(fp);
                free(cache);
                optbinlog_shared_close(base, map_size);
                return -1;
            }
            if (profile) t_write += now_ns() - t3;
        } else {
            buf_used += frame_size;
        }
    }

    if (buf_used > 0) {
        uint64_t t5 = profile ? now_ns() : 0;
        if (write_exact(fp, buf, buf_used) != 0) {
            fprintf(stderr, "write %s failed: %s\n", log_path, strerror(errno));
            free(buf);
            free(rec_buf);
            fclose(fp);
            free(cache);
            optbinlog_shared_close(base, map_size);
            return -1;
        }
        if (profile) t_write += now_ns() - t5;
    }

    if (fclose(fp) != 0) {
        fprintf(stderr, "close %s failed: %s\n", log_path, strerror(errno));
        free(buf);
        free(rec_buf);
        free(cache);
        optbinlog_shared_close(base, map_size);
        return -1;
    }
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

    uint8_t* payload = NULL;
    size_t payload_cap = 0;
    int rc = 0;

    for (;;) {
        uint8_t len_buf[4];
        size_t n = fread(len_buf, 1, sizeof(len_buf), fp);
        if (n == 0) {
            if (feof(fp)) break;
            fprintf(stderr, "read frame length failed\n");
            rc = -1;
            break;
        }
        if (n != sizeof(len_buf)) {
            fprintf(stderr, "truncated frame length\n");
            rc = -1;
            break;
        }

        uint32_t payload_len = read_le32(len_buf);
        if (payload_len < OPTBINLOG_MIN_PAYLOAD_LEN || payload_len > OPTBINLOG_MAX_PAYLOAD_LEN) {
            fprintf(stderr, "invalid frame length %u\n", payload_len);
            rc = -1;
            break;
        }

        if (payload_cap < (size_t)payload_len) {
            uint8_t* next = realloc(payload, (size_t)payload_len);
            if (!next) {
                fprintf(stderr, "OOM\n");
                rc = -1;
                break;
            }
            payload = next;
            payload_cap = (size_t)payload_len;
        }

        if (read_exact(fp, payload, (size_t)payload_len) != 0) {
            fprintf(stderr, "truncated payload\n");
            rc = -1;
            break;
        }

        uint8_t crc_buf[4];
        if (read_exact(fp, crc_buf, sizeof(crc_buf)) != 0) {
            fprintf(stderr, "truncated crc\n");
            rc = -1;
            break;
        }

        uint32_t expect_crc = read_le32(crc_buf);
        uint32_t got_crc = crc32_compute(payload, (size_t)payload_len);
        if (expect_crc != got_crc) {
            fprintf(stderr, "crc mismatch\n");
            rc = -1;
            break;
        }

        size_t off = 0;
        if ((size_t)payload_len - off < sizeof(int64_t) + sizeof(uint16_t) + sizeof(uint8_t)) {
            fprintf(stderr, "payload too short\n");
            rc = -1;
            break;
        }

        OptbinlogRecord rec;
        memcpy(&rec.timestamp, payload + off, sizeof(int64_t));
        off += sizeof(int64_t);
        rec.tag_id = (int)read_le16(payload + off);
        off += sizeof(uint16_t);
        rec.ele_count = (int)payload[off];
        off += sizeof(uint8_t);

        OptbinlogEventTag* tag = optbinlog_lookup_tag(base, header, rec.tag_id, rec.ele_count);
        if (!tag) {
            fprintf(stderr, "invalid record tag %d\n", rec.tag_id);
            rc = -1;
            break;
        }

        OptbinlogValue* values = calloc((size_t)rec.ele_count, sizeof(OptbinlogValue));
        if (!values) {
            fprintf(stderr, "OOM\n");
            rc = -1;
            break;
        }
        rec.values = values;

        int row_rc = 0;
        OptbinlogEventTagEle* eles = (OptbinlogEventTagEle*)((uint8_t*)base + tag->tag_ele_offset);
        for (int e = 0; e < rec.ele_count; e++) {
            OptbinlogEventTagEle* ele = &eles[e];
            size_t need = (ele->type == 2) ? sizeof(double) : (size_t)ele->len;
            if ((size_t)payload_len - off < need) {
                fprintf(stderr, "truncated record body\n");
                row_rc = -1;
                break;
            }

            if (ele->type == 1) {
                values[e].kind = OPTBINLOG_VAL_U;
                values[e].u = read_uint_n(payload + off, ele->len);
                off += (size_t)ele->len;
            } else if (ele->type == 2) {
                double v = 0.0;
                memcpy(&v, payload + off, sizeof(double));
                values[e].kind = OPTBINLOG_VAL_D;
                values[e].d = v;
                off += sizeof(double);
            } else if (ele->type == 3) {
                char* s = calloc((size_t)ele->len + 1, 1);
                if (!s) {
                    fprintf(stderr, "OOM\n");
                    row_rc = -1;
                    break;
                }
                memcpy(s, payload + off, (size_t)ele->len);
                values[e].kind = OPTBINLOG_VAL_S;
                values[e].s = s;
                off += (size_t)ele->len;
            } else {
                fprintf(stderr, "unknown element type\n");
                row_rc = -1;
                break;
            }
        }

        if (row_rc == 0 && off != (size_t)payload_len) {
            fprintf(stderr, "payload trailing bytes\n");
            row_rc = -1;
        }

        if (row_rc == 0 && cb) {
            if (cb(&rec, user) != 0) {
                row_rc = 1;
            }
        }

        for (int e = 0; e < rec.ele_count; e++) {
            if (values[e].kind == OPTBINLOG_VAL_S) free((void*)values[e].s);
        }
        free(values);

        if (row_rc < 0) {
            rc = -1;
            break;
        }
        if (row_rc > 0) {
            break;
        }
    }

    free(payload);
    fclose(fp);
    optbinlog_shared_close(base, map_size);
    return rc;
}
