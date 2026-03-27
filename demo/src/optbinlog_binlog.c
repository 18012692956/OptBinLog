#include "optbinlog_binlog.h"
#include "optbinlog_shared.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#define OPTBINLOG_HAVE_HW_CRC32C 1
#elif defined(__SSE4_2__) && (defined(__x86_64__) || defined(__i386__))
#include <nmmintrin.h>
#define OPTBINLOG_HAVE_HW_CRC32C 1
#else
#define OPTBINLOG_HAVE_HW_CRC32C 0
#endif

#define OPTBINLOG_MIN_PAYLOAD_LEN 11u
#define OPTBINLOG_MAX_PAYLOAD_LEN (1024u * 1024u)
#define OPTBINLOG_FRAME_LEN_MASK 0x1FFFFFFFu
#define OPTBINLOG_FRAME_VARSTR_BIT 0x20000000u
#define OPTBINLOG_FRAME_CHECKSUM_SHIFT 30u
/* frame_header(32位)：[checksum_type:2][varstr:1][payload_len:29] */

typedef struct {
    OptbinlogEventTag* tag;
    OptbinlogEventTagEle* eles;
    int ele_count;
    int payload_size;
} OptbinlogTagCacheEntry;

typedef struct {
    int tag_count;
    int total_payload_bytes;
    int total_string_fixed_bytes;
    int string_field_count;
    int max_string_fixed_bytes;
} OptbinlogTagCacheStats;

typedef struct {
    int ready;
    dev_t st_dev;
    ino_t st_ino;
    off_t st_size;
    time_t mtime_sec;
#if defined(__APPLE__) || defined(__MACH__)
    long mtime_nsec;
#elif defined(st_mtim)
    long mtime_nsec;
#else
    long mtime_nsec;
#endif
    void* base;
    size_t map_size;
    OptbinlogSharedTag* header;
    OptbinlogTagCacheEntry* cache;
    int cache_len;
    OptbinlogTagCacheStats stats;
} OptbinlogSharedViewCache;
/* 进程内缓存：共享映射 + 预构建 tag 索引，用于热路径复用。 */

typedef enum {
    OPTBINLOG_CHECKSUM_CRC32 = 0,
    OPTBINLOG_CHECKSUM_CRC32C = 1,
    OPTBINLOG_CHECKSUM_NONE = 2,
} OptbinlogChecksumType;

/* 获取单调时钟纳秒值（用于性能统计）。 */
static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint32_t crc32_table[256];
static int crc32_table_ready = 0;
#if !OPTBINLOG_HAVE_HW_CRC32C
static uint32_t crc32c_table[256];
static int crc32c_table_ready = 0;
#endif

static OptbinlogSharedViewCache g_shared_view_cache;
static int g_shared_view_cache_registered = 0;

typedef struct {
    int valid;
    dev_t st_dev;
    ino_t st_ino;
} OptbinlogRepairSeen;

static OptbinlogRepairSeen g_repair_seen[32];

/* 初始化 CRC32 查找表。 */
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

/* 按字节增量更新 CRC32 状态。 */
static uint32_t crc32_update_state(uint32_t crc, const uint8_t* data, size_t len) {
    if (!crc32_table_ready) {
        crc32_init_table();
    }
    for (size_t i = 0; i < len; i++) {
        crc = crc32_table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc;
}

/* 计算完整 CRC32。 */
static uint32_t crc32_compute(const uint8_t* data, size_t len) {
    uint32_t crc = 0xFFFFFFFFu;
    crc = crc32_update_state(crc, data, len);
    return ~crc;
}

#if !OPTBINLOG_HAVE_HW_CRC32C
/* 初始化 CRC32C 软件查找表。 */
static void crc32c_init_table(void) {
    for (uint32_t i = 0; i < 256u; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c & 1u) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
        }
        crc32c_table[i] = c;
    }
    crc32c_table_ready = 1;
}
#endif

#if !OPTBINLOG_HAVE_HW_CRC32C
/* 软件路径：增量更新 CRC32C。 */
static uint32_t crc32c_sw_update_state(uint32_t crc, const uint8_t* data, size_t len) {
    if (!crc32c_table_ready) {
        crc32c_init_table();
    }
    for (size_t i = 0; i < len; i++) {
        crc = crc32c_table[(crc ^ data[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc;
}
#endif

#if OPTBINLOG_HAVE_HW_CRC32C
/* 硬件路径：利用指令集加速 CRC32C。 */
static uint32_t crc32c_hw_update_state(uint32_t crc, const uint8_t* data, size_t len) {
#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    while (len >= sizeof(uint64_t)) {
        uint64_t v = 0;
        memcpy(&v, data, sizeof(v));
        crc = __crc32cd(crc, v);
        data += sizeof(uint64_t);
        len -= sizeof(uint64_t);
    }
    while (len >= sizeof(uint32_t)) {
        uint32_t v = 0;
        memcpy(&v, data, sizeof(v));
        crc = __crc32cw(crc, v);
        data += sizeof(uint32_t);
        len -= sizeof(uint32_t);
    }
    while (len >= sizeof(uint16_t)) {
        uint16_t v = 0;
        memcpy(&v, data, sizeof(v));
        crc = __crc32ch(crc, v);
        data += sizeof(uint16_t);
        len -= sizeof(uint16_t);
    }
    while (len > 0) {
        crc = __crc32cb(crc, *data++);
        len--;
    }
#else
    while (len >= sizeof(uint64_t)) {
        uint64_t v = 0;
        memcpy(&v, data, sizeof(v));
        crc = (uint32_t)_mm_crc32_u64((uint64_t)crc, v);
        data += sizeof(uint64_t);
        len -= sizeof(uint64_t);
    }
    while (len >= sizeof(uint32_t)) {
        uint32_t v = 0;
        memcpy(&v, data, sizeof(v));
        crc = _mm_crc32_u32(crc, v);
        data += sizeof(uint32_t);
        len -= sizeof(uint32_t);
    }
    while (len > 0) {
        crc = _mm_crc32_u8(crc, *data++);
        len--;
    }
#endif
    return crc;
}
#endif

/* 以小端写入 32 位整数。 */
static void write_le32(uint8_t* dst, uint32_t v) {
    dst[0] = (uint8_t)(v & 0xFFu);
    dst[1] = (uint8_t)((v >> 8) & 0xFFu);
    dst[2] = (uint8_t)((v >> 16) & 0xFFu);
    dst[3] = (uint8_t)((v >> 24) & 0xFFu);
}

/* 以小端读取 16 位整数。 */
static uint16_t read_le16(const uint8_t* src) {
    return (uint16_t)((uint16_t)src[0] | ((uint16_t)src[1] << 8));
}

/* 以小端读取 32 位整数。 */
static uint32_t read_le32(const uint8_t* src) {
    return (uint32_t)src[0] |
           ((uint32_t)src[1] << 8) |
           ((uint32_t)src[2] << 16) |
           ((uint32_t)src[3] << 24);
}

/* 读取布尔环境变量（缺省为 false）。 */
static int env_flag_enabled(const char* name) {
    const char* raw = getenv(name);
    if (!raw || !raw[0]) return 0;
    if (strcmp(raw, "0") == 0) return 0;
    if (strcmp(raw, "false") == 0) return 0;
    if (strcmp(raw, "FALSE") == 0) return 0;
    return 1;
}

/* 读取布尔环境变量（支持传入默认值）。 */
static int env_flag_enabled_default(const char* name, int default_value) {
    const char* raw = getenv(name);
    if (!raw || !raw[0]) return default_value ? 1 : 0;
    if (strcmp(raw, "0") == 0) return 0;
    if (strcmp(raw, "false") == 0) return 0;
    if (strcmp(raw, "FALSE") == 0) return 0;
    return 1;
}

/* 读取三态环境变量：-1=auto, 0=off, 1=on。 */
static int env_tristate(const char* name) {
    const char* raw = getenv(name);
    if (!raw || !raw[0] || strcmp(raw, "auto") == 0 || strcmp(raw, "AUTO") == 0) return -1;
    if (strcmp(raw, "0") == 0 || strcmp(raw, "false") == 0 || strcmp(raw, "FALSE") == 0) return 0;
    return 1;
}

/* 按环境变量解析校验模式。 */
static OptbinlogChecksumType checksum_type_from_env(int disable_crc) {
    const char* raw = getenv("OPTBINLOG_BINLOG_CHECKSUM");
    if (disable_crc) return OPTBINLOG_CHECKSUM_NONE;
    if (!raw || !raw[0]) return OPTBINLOG_CHECKSUM_CRC32C;
    if (strcmp(raw, "crc32") == 0) return OPTBINLOG_CHECKSUM_CRC32;
    if (strcmp(raw, "crc32c") == 0) return OPTBINLOG_CHECKSUM_CRC32C;
    if (strcmp(raw, "none") == 0) return OPTBINLOG_CHECKSUM_NONE;
    return OPTBINLOG_CHECKSUM_CRC32C;
}

static int compute_frame_checksum(OptbinlogChecksumType checksum_type,
                                  const uint8_t* frame_header,
                                  const uint8_t* payload,
                                  size_t payload_len,
                                  uint32_t* out_checksum) {
    /*
     * 校验覆盖规则：
     * - crc32   ：只覆盖 payload（兼容旧模式）
     * - crc32c  ：覆盖 frame_header + payload（默认）
     * - none    ：校验字段写 0
     */
    if (checksum_type == OPTBINLOG_CHECKSUM_NONE) {
        *out_checksum = 0u;
        return 0;
    }
    if (checksum_type == OPTBINLOG_CHECKSUM_CRC32) {
        *out_checksum = crc32_compute(payload, payload_len);
        return 0;
    }
    uint32_t crc = 0xFFFFFFFFu;
#if OPTBINLOG_HAVE_HW_CRC32C
    crc = crc32c_hw_update_state(crc, frame_header, sizeof(uint32_t));
    crc = crc32c_hw_update_state(crc, payload, payload_len);
#else
    crc = crc32c_sw_update_state(crc, frame_header, sizeof(uint32_t));
    crc = crc32c_sw_update_state(crc, payload, payload_len);
#endif
    *out_checksum = ~crc;
    return 0;
}

/* 提取文件 mtime 的纳秒部分（跨平台）。 */
static long stat_mtime_nsec(const struct stat* st) {
#if defined(__APPLE__) || defined(__MACH__)
    return st->st_mtimespec.tv_nsec;
#elif defined(st_mtim)
    return st->st_mtim.tv_nsec;
#else
    (void)st;
    return 0;
#endif
}

static int schema_prefers_varstr(const OptbinlogTagCacheStats* stats) {
    /*
     * auto-varstr 启发式：
     * 当字符串字段占比高或最大定长字符串过宽时启用。
     */
    if (!stats || stats->string_field_count == 0) return 0;
    if (stats->max_string_fixed_bytes >= 128) return 1;
    return stats->total_string_fixed_bytes * 4 >= stats->total_payload_bytes;
}

/* 释放进程内共享视图缓存。 */
static void shared_view_cache_close(void) {
    if (g_shared_view_cache.base && g_shared_view_cache.map_size > 0) {
        optbinlog_shared_close(g_shared_view_cache.base, g_shared_view_cache.map_size);
    }
    free(g_shared_view_cache.cache);
    memset(&g_shared_view_cache, 0, sizeof(g_shared_view_cache));
}

/* 判断缓存是否仍与共享文件版本一致。 */
static int shared_view_cache_matches(const struct stat* st) {
    if (!g_shared_view_cache.ready) return 0;
    if (!st) return 0;
    if (g_shared_view_cache.st_dev != st->st_dev) return 0;
    if (g_shared_view_cache.st_ino != st->st_ino) return 0;
    if (g_shared_view_cache.st_size != st->st_size) return 0;
    if (g_shared_view_cache.mtime_sec != st->st_mtime) return 0;
    if (g_shared_view_cache.mtime_nsec != stat_mtime_nsec(st)) return 0;
    return 1;
}

static int build_tag_cache(void* base,
                           OptbinlogSharedTag* header,
                           OptbinlogTagCacheEntry** out_cache,
                           int* out_len,
                           OptbinlogTagCacheStats* out_stats) {
    /*
     * 将共享区元数据预展开为 tag_id -> {tag,ele,payload_size} 索引。
     * 这样写路径可避免每条记录重复做位图/rank 查找。
     */
    if (!header || header->tag_count == 0 || header->num_arrays == 0) return -1;
    int total_ids = (int)header->num_arrays * OPTBINLOG_EVENT_TAG_ARRAY_LEN;
    OptbinlogTagCacheEntry* cache = calloc((size_t)total_ids, sizeof(OptbinlogTagCacheEntry));
    if (!cache) return -1;
    OptbinlogTagCacheStats stats = {0};
    stats.tag_count = (int)header->tag_count;

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
                if (eles[e].type == 3) {
                    stats.total_string_fixed_bytes += (int)eles[e].len;
                    stats.string_field_count++;
                    if ((int)eles[e].len > stats.max_string_fixed_bytes) {
                        stats.max_string_fixed_bytes = (int)eles[e].len;
                    }
                }
            }

            cache[tag_id].tag = tag;
            cache[tag_id].eles = eles;
            cache[tag_id].ele_count = (int)tag->tag_ele_num;
            cache[tag_id].payload_size = size;
            stats.total_payload_bytes += size;
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
    if (out_stats) *out_stats = stats;
    return 0;
}

/* 获取共享视图：命中缓存则复用，否则重新映射并重建缓存。 */
static int acquire_shared_view(const char* shared_path,
                               void** out_base,
                               size_t* out_map_size,
                               OptbinlogSharedTag** out_header,
                               OptbinlogTagCacheEntry** out_cache,
                               int* out_cache_len,
                               OptbinlogTagCacheStats* out_stats,
                               int* out_cache_reused) {
    struct stat st;
    if (stat(shared_path, &st) != 0) {
        return -1;
    }
    if (shared_view_cache_matches(&st)) {
        *out_base = g_shared_view_cache.base;
        *out_map_size = g_shared_view_cache.map_size;
        *out_header = g_shared_view_cache.header;
        *out_cache = g_shared_view_cache.cache;
        *out_cache_len = g_shared_view_cache.cache_len;
        if (out_stats) *out_stats = g_shared_view_cache.stats;
        if (out_cache_reused) *out_cache_reused = 1;
        return 0;
    }

    void* base = NULL;
    size_t map_size = 0;
    OptbinlogSharedTag* header = NULL;
    OptbinlogTagCacheEntry* cache = NULL;
    int cache_len = 0;
    OptbinlogTagCacheStats stats = {0};
    if (optbinlog_shared_open(shared_path, &base, &map_size, &header) != 0) {
        return -1;
    }
    if (build_tag_cache(base, header, &cache, &cache_len, &stats) != 0) {
        optbinlog_shared_close(base, map_size);
        return -1;
    }

    if (!g_shared_view_cache_registered) {
        atexit(shared_view_cache_close);
        g_shared_view_cache_registered = 1;
    }
    shared_view_cache_close();
    g_shared_view_cache.ready = 1;
    g_shared_view_cache.st_dev = st.st_dev;
    g_shared_view_cache.st_ino = st.st_ino;
    g_shared_view_cache.st_size = st.st_size;
    g_shared_view_cache.mtime_sec = st.st_mtime;
    g_shared_view_cache.mtime_nsec = stat_mtime_nsec(&st);
    g_shared_view_cache.base = base;
    g_shared_view_cache.map_size = map_size;
    g_shared_view_cache.header = header;
    g_shared_view_cache.cache = cache;
    g_shared_view_cache.cache_len = cache_len;
    g_shared_view_cache.stats = stats;

    *out_base = g_shared_view_cache.base;
    *out_map_size = g_shared_view_cache.map_size;
    *out_header = g_shared_view_cache.header;
    *out_cache = g_shared_view_cache.cache;
    *out_cache_len = g_shared_view_cache.cache_len;
    if (out_stats) *out_stats = g_shared_view_cache.stats;
    if (out_cache_reused) *out_cache_reused = 0;
    return 0;
}

/* 按小端读取 n 字节无符号整数。 */
static uint64_t read_uint_n(const uint8_t* data, int nbytes) {
    uint64_t v = 0;
    for (int i = 0; i < nbytes; i++) {
        v |= ((uint64_t)data[i]) << (i * 8);
    }
    return v;
}

/* 精确读取 n 字节，不足即失败。 */
static int read_exact(FILE* fp, void* out, size_t n) {
    size_t got = fread(out, 1, n, fp);
    return got == n ? 0 : -1;
}

/* 精确写入 n 字节，不足即失败。 */
static int write_exact(FILE* fp, const void* data, size_t n) {
    if (n == 0) return 0;
    return fwrite(data, 1, n, fp) == n ? 0 : -1;
}

/* 判断 checksum 类型是否在支持范围内。 */
static int checksum_type_valid(OptbinlogChecksumType checksum_type) {
    return checksum_type == OPTBINLOG_CHECKSUM_CRC32 ||
           checksum_type == OPTBINLOG_CHECKSUM_CRC32C ||
           checksum_type == OPTBINLOG_CHECKSUM_NONE;
}

/* 查询某个文件 inode 是否已做过尾修复。 */
static int repair_seen_contains(dev_t st_dev, ino_t st_ino) {
    for (size_t i = 0; i < sizeof(g_repair_seen) / sizeof(g_repair_seen[0]); i++) {
        if (!g_repair_seen[i].valid) continue;
        if (g_repair_seen[i].st_dev == st_dev && g_repair_seen[i].st_ino == st_ino) return 1;
    }
    return 0;
}

/* 记录某个文件 inode 已执行过尾修复。 */
static void repair_seen_add(dev_t st_dev, ino_t st_ino) {
    for (size_t i = 0; i < sizeof(g_repair_seen) / sizeof(g_repair_seen[0]); i++) {
        if (g_repair_seen[i].valid &&
            g_repair_seen[i].st_dev == st_dev &&
            g_repair_seen[i].st_ino == st_ino) {
            return;
        }
    }
    for (size_t i = 0; i < sizeof(g_repair_seen) / sizeof(g_repair_seen[0]); i++) {
        if (!g_repair_seen[i].valid) {
            g_repair_seen[i].valid = 1;
            g_repair_seen[i].st_dev = st_dev;
            g_repair_seen[i].st_ino = st_ino;
            return;
        }
    }
    /* 环形覆盖，避免该状态集合无限增长。 */
    static size_t g_repair_seen_next = 0;
    size_t idx = g_repair_seen_next % (sizeof(g_repair_seen) / sizeof(g_repair_seen[0]));
    g_repair_seen[idx].valid = 1;
    g_repair_seen[idx].st_dev = st_dev;
    g_repair_seen[idx].st_ino = st_ino;
    g_repair_seen_next++;
}

/* 扫描并修复日志坏尾：保留到最后一条完整帧为止。 */
int optbinlog_binlog_recover_tail(const char* log_path, size_t* before_bytes, size_t* after_bytes) {
    if (!log_path || !log_path[0]) {
        errno = EINVAL;
        return -1;
    }

    struct stat st;
    if (stat(log_path, &st) != 0) {
        if (errno == ENOENT) {
            if (before_bytes) *before_bytes = 0;
            if (after_bytes) *after_bytes = 0;
            return 0;
        }
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        errno = EINVAL;
        return -1;
    }

    size_t total = (size_t)st.st_size;
    if (before_bytes) *before_bytes = total;
    if (after_bytes) *after_bytes = total;
    if (total == 0) return 0;

    FILE* fp = fopen(log_path, "rb");
    if (!fp) return -1;

    uint8_t* payload = NULL;
    size_t payload_cap = 0;
    size_t valid_bytes = 0;
    int dirty_tail = 0;
    int hard_error = 0;

    for (;;) {
        uint8_t len_buf[4];
        size_t n = fread(len_buf, 1, sizeof(len_buf), fp);
        if (n == 0) {
            if (feof(fp)) break;
            hard_error = 1;
            break;
        }
        if (n != sizeof(len_buf)) {
            dirty_tail = 1;
            break;
        }

        uint32_t frame_header = read_le32(len_buf);
        OptbinlogChecksumType checksum_type = (OptbinlogChecksumType)(frame_header >> OPTBINLOG_FRAME_CHECKSUM_SHIFT);
        uint32_t payload_len = frame_header & OPTBINLOG_FRAME_LEN_MASK;
        if (payload_len < OPTBINLOG_MIN_PAYLOAD_LEN || payload_len > OPTBINLOG_MAX_PAYLOAD_LEN) {
            dirty_tail = 1;
            break;
        }
        if (!checksum_type_valid(checksum_type)) {
            dirty_tail = 1;
            break;
        }

        if (payload_cap < (size_t)payload_len) {
            uint8_t* next = realloc(payload, (size_t)payload_len);
            if (!next) {
                hard_error = 1;
                break;
            }
            payload = next;
            payload_cap = (size_t)payload_len;
        }

        if (read_exact(fp, payload, (size_t)payload_len) != 0) {
            dirty_tail = 1;
            break;
        }

        uint8_t crc_buf[4];
        if (read_exact(fp, crc_buf, sizeof(crc_buf)) != 0) {
            dirty_tail = 1;
            break;
        }

        uint32_t expect_crc = read_le32(crc_buf);
        uint32_t got_crc = 0u;
        if (compute_frame_checksum(checksum_type, len_buf, payload, (size_t)payload_len, &got_crc) != 0) {
            hard_error = 1;
            break;
        }
        if (expect_crc != got_crc) {
            dirty_tail = 1;
            break;
        }

        valid_bytes += sizeof(uint32_t) + (size_t)payload_len + sizeof(uint32_t);
    }

    free(payload);
    fclose(fp);
    if (hard_error) return -1;
    if (!dirty_tail || valid_bytes >= total) {
        if (after_bytes) *after_bytes = total;
        return 0;
    }

    if (truncate(log_path, (off_t)valid_bytes) != 0) {
        return -1;
    }
    if (after_bytes) *after_bytes = valid_bytes;
    return 1;
}

/* 按策略自动执行一次坏尾修复（append 模式前）。 */
static int auto_repair_tail_if_needed(const char* log_path) {
    if (!env_flag_enabled_default("OPTBINLOG_BINLOG_RECOVER_TAIL", 1)) {
        return 0;
    }
    if (!log_path || !log_path[0]) return 0;

    struct stat st;
    if (stat(log_path, &st) != 0) {
        if (errno == ENOENT) return 0;
        fprintf(stderr, "stat %s failed before tail-recover: %s\n", log_path, strerror(errno));
        return -1;
    }
    if (!S_ISREG(st.st_mode) || st.st_size == 0) return 0;
    if (repair_seen_contains(st.st_dev, st.st_ino)) return 0;

    size_t before_bytes = 0;
    size_t after_bytes = 0;
    int rc = optbinlog_binlog_recover_tail(log_path, &before_bytes, &after_bytes);
    if (rc < 0) {
        fprintf(stderr, "tail-recover failed for %s: %s\n", log_path, strerror(errno));
        return -1;
    }
    if (rc > 0) {
        fprintf(stderr,
                "tail-recover applied on %s: %zu -> %zu (drop %zu bytes)\n",
                log_path,
                before_bytes,
                after_bytes,
                before_bytes - after_bytes);
    }

    struct stat st_after;
    if (stat(log_path, &st_after) == 0 && S_ISREG(st_after.st_mode)) {
        repair_seen_add(st_after.st_dev, st_after.st_ino);
    }
    return 0;
}

/* 写入记录数组为二进制帧流。 */
int optbinlog_binlog_write(const char* shared_path, const char* log_path, const OptbinlogRecord* records, size_t count) {
    const char* prof_env = getenv("OPTBINLOG_PROFILE");
    int profile = (prof_env && prof_env[0] == '1') ? 1 : 0;
    int disable_crc = env_flag_enabled("OPTBINLOG_BINLOG_DISABLE_CRC");
    int varstr_mode = env_tristate("OPTBINLOG_BINLOG_VARSTR");
    OptbinlogChecksumType checksum_type = checksum_type_from_env(disable_crc);
    uint64_t t_cache = 0;
    uint64_t t_pack = 0;
    uint64_t t_write = 0;

    void* base = NULL;
    size_t map_size = 0;
    OptbinlogSharedTag* header = NULL;
    OptbinlogTagCacheEntry* cache = NULL;
    int cache_len = 0;
    OptbinlogTagCacheStats cache_stats = {0};
    int cache_reused = 0;
    uint64_t t0 = profile ? now_ns() : 0;
    if (acquire_shared_view(shared_path, &base, &map_size, &header, &cache, &cache_len, &cache_stats, &cache_reused) != 0) {
        fprintf(stderr, "open shared file failed\n");
        return -1;
    }
    if (profile) t_cache += now_ns() - t0;
    int varstr = (varstr_mode < 0) ? schema_prefers_varstr(&cache_stats) : varstr_mode;

    int append_mode = env_flag_enabled("OPTBINLOG_BINLOG_APPEND");
    if (append_mode) {
        if (auto_repair_tail_if_needed(log_path) != 0) {
            return -1;
        }
    }
    FILE* fp = fopen(log_path, append_mode ? "ab" : "wb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", log_path, strerror(errno));
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
        return -1;
    }

    for (size_t i = 0; i < count; i++) {
        const OptbinlogRecord* rec = &records[i];
        if (rec->tag_id < 0 || rec->tag_id >= cache_len || !cache[rec->tag_id].tag) {
            fprintf(stderr, "invalid tag %d\n", rec->tag_id);
            free(buf);
            free(rec_buf);
            fclose(fp);
            return -1;
        }

        OptbinlogTagCacheEntry* entry = &cache[rec->tag_id];
        if (rec->ele_count != entry->ele_count) {
            fprintf(stderr, "element count mismatch for tag %d\n", rec->tag_id);
            free(buf);
            free(rec_buf);
            fclose(fp);
            return -1;
        }
        if (rec->tag_id > 0xFFFF || rec->ele_count > 0xFF) {
            fprintf(stderr, "record field overflow for tag %d\n", rec->tag_id);
            free(buf);
            free(rec_buf);
            fclose(fp);
            return -1;
        }

        int payload_size = entry->payload_size;
        if (varstr) {
            payload_size = 8 + 2 + 1;
            for (int e = 0; e < rec->ele_count; e++) {
                OptbinlogEventTagEle* ele = &entry->eles[e];
                const OptbinlogValue* v = &rec->values[e];
                if (ele->type == 2) {
                    payload_size += 8;
                } else if (ele->type == 3) {
                    size_t slen = 0;
                    if (v->s) {
                        slen = strnlen(v->s, (size_t)ele->len);
                    }
                    payload_size += 2 + (int)slen;
                } else {
                    payload_size += (int)ele->len;
                }
            }
        }
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
                    return -1;
                }
                rec_buf = next;
                rec_cap = frame_size;
            }
            dst = rec_buf;
        }

        uint64_t t1 = profile ? now_ns() : 0;
        /* 组装帧头并写入：长度 + varstr 标志 + checksum 类型。 */
        uint32_t frame_header = (uint32_t)payload_size;
        if (varstr) frame_header |= OPTBINLOG_FRAME_VARSTR_BIT;
        frame_header |= ((uint32_t)checksum_type << OPTBINLOG_FRAME_CHECKSUM_SHIFT);
        write_le32(dst, frame_header);
        size_t off = sizeof(uint32_t);
        /* payload 固定前缀：timestamp(8) + tag_id(2) + ele_count(1)。 */
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
                    return -1;
                }
                if (varstr) {
                    /* 变长字符串：2 字节长度前缀 + 实际内容。 */
                    size_t slen = 0;
                    if (v->s) {
                        slen = strnlen(v->s, (size_t)ele->len);
                    }
                    if (slen > 0xFFFFu) slen = 0xFFFFu;
                    dst[off] = (uint8_t)(slen & 0xFFu);
                    dst[off + 1] = (uint8_t)((slen >> 8) & 0xFFu);
                    off += 2;
                    if (slen > 0) {
                        memcpy(dst + off, v->s, slen);
                        off += slen;
                    }
                } else {
                    /* 定长字符串：不足补零，超长截断。 */
                    memset(dst + off, 0, (size_t)ele->len);
                    if (v->s) {
                        size_t slen = strnlen(v->s, (size_t)ele->len);
                        memcpy(dst + off, v->s, slen);
                    }
                    off += (size_t)ele->len;
                }
            }
        }

        if (off != sizeof(uint32_t) + (size_t)payload_size) {
            fprintf(stderr, "internal payload size mismatch\n");
            free(buf);
            free(rec_buf);
            fclose(fp);
            return -1;
        }

        uint32_t crc = 0u;
        if (compute_frame_checksum(checksum_type, dst, dst + sizeof(uint32_t), (size_t)payload_size, &crc) != 0) {
            fprintf(stderr, "OOM\n");
            free(buf);
            free(rec_buf);
            fclose(fp);
            return -1;
        }
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
            return -1;
        }
        if (profile) t_write += now_ns() - t5;
    }

    if (fclose(fp) != 0) {
        fprintf(stderr, "close %s failed: %s\n", log_path, strerror(errno));
        free(buf);
        free(rec_buf);
        return -1;
    }
    free(buf);
    free(rec_buf);

    if (profile) {
        double ms_cache = (double)t_cache / 1e6;
        double ms_pack = (double)t_pack / 1e6;
        double ms_write = (double)t_write / 1e6;
        fprintf(stderr,
                "OPTBINLOG_PROFILE cache_ms=%.3f pack_ms=%.3f write_ms=%.3f cache_reused=%d varstr=%d checksum=%u\n",
                ms_cache,
                ms_pack,
                ms_write,
                cache_reused,
                varstr,
                (unsigned)checksum_type);
    }
    return 0;
}

/* 读取并解码二进制帧流，逐条回调输出。 */
int optbinlog_binlog_read(const char* shared_path, const char* log_path, OptbinlogRecordCallback cb, void* user) {
    void* base = NULL;
    size_t map_size = 0;
    OptbinlogSharedTag* header = NULL;
    OptbinlogTagCacheEntry* cache = NULL;
    int cache_len = 0;
    OptbinlogTagCacheStats cache_stats = {0};
    int cache_reused = 0;
    if (acquire_shared_view(shared_path, &base, &map_size, &header, &cache, &cache_len, &cache_stats, &cache_reused) != 0) {
        (void)cache;
        (void)cache_len;
        (void)cache_stats;
        (void)cache_reused;
        fprintf(stderr, "open shared file failed\n");
        return -1;
    }

    FILE* fp = fopen(log_path, "rb");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", log_path, strerror(errno));
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

        /* 先读帧头，再按帧头声明规则读取 payload 与 checksum。 */
        uint32_t frame_header = read_le32(len_buf);
        OptbinlogChecksumType checksum_type = (OptbinlogChecksumType)(frame_header >> OPTBINLOG_FRAME_CHECKSUM_SHIFT);
        int varstr = (frame_header & OPTBINLOG_FRAME_VARSTR_BIT) ? 1 : 0;
        uint32_t payload_len = frame_header & OPTBINLOG_FRAME_LEN_MASK;
        if (payload_len < OPTBINLOG_MIN_PAYLOAD_LEN || payload_len > OPTBINLOG_MAX_PAYLOAD_LEN) {
            fprintf(stderr, "invalid frame length %u\n", payload_len);
            rc = -1;
            break;
        }
        if (checksum_type != OPTBINLOG_CHECKSUM_CRC32 &&
            checksum_type != OPTBINLOG_CHECKSUM_CRC32C &&
            checksum_type != OPTBINLOG_CHECKSUM_NONE) {
            fprintf(stderr, "invalid checksum type %u\n", (unsigned)(frame_header >> OPTBINLOG_FRAME_CHECKSUM_SHIFT));
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
        uint32_t got_crc = 0u;
        if (compute_frame_checksum(checksum_type, len_buf, payload, (size_t)payload_len, &got_crc) != 0) {
            fprintf(stderr, "OOM\n");
            rc = -1;
            break;
        }
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
            if (ele->type == 3 && varstr) need = 2u;
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
                /* 字符串字段：依据 varstr 标志走变长或定长解码分支。 */
                size_t slen = (size_t)ele->len;
                if (varstr) {
                    slen = (size_t)read_le16(payload + off);
                    off += 2u;
                    if (slen > (size_t)ele->len || (size_t)payload_len - off < slen) {
                        fprintf(stderr, "invalid varstr field\n");
                        row_rc = -1;
                        break;
                    }
                }
                char* s = calloc(slen + 1u, 1);
                if (!s) {
                    fprintf(stderr, "OOM\n");
                    row_rc = -1;
                    break;
                }
                if (slen > 0) {
                    memcpy(s, payload + off, slen);
                }
                values[e].kind = OPTBINLOG_VAL_S;
                values[e].s = s;
                off += varstr ? slen : (size_t)ele->len;
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
    return rc;
}
