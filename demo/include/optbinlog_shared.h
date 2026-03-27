#ifndef OPTBINLOG_SHARED_H
#define OPTBINLOG_SHARED_H

#include <stdint.h>
#include <stddef.h>

#define OPTBINLOG_EVENT_TAG_ARRAY_LEN 100
#define OPTBINLOG_EVENTTAG_FILENAME "/tmp/__eventtagdata__"
#define OPTBINLOG_SHARED_MAGIC "OptLog77"
#define OPTBINLOG_SHARED_HEADER_VERSION 4u

typedef enum {
    OPTBINLOG_INITIALIZING = 0,
    OPTBINLOG_INITIALIZED = 1,
} OptbinlogSharedTagState;

/* tag 是否存在的位图（每组 100 个 tag id）。 */
typedef struct {
    uint8_t bits[OPTBINLOG_EVENT_TAG_ARRAY_LEN / 8 + 1];
} OptbinlogBitmap;

#pragma pack(push, 1)
/* 共享区中的字段元数据：类型/长度/字段名。 */
typedef struct {
    unsigned int type : 2; /* 1: long 2: double 3: string */
    unsigned int len  : 6; /* bytes */
    char name[32];
} OptbinlogEventTagEle;

/* 共享区中的 tag 元数据：tag id、字段数量、字段偏移、tag 名。 */
typedef struct {
    unsigned int tag_index   : 12;
    unsigned int tag_ele_num : 4;
    int tag_ele_offset; /* offset from shared base */
    char tag_name[48];
} OptbinlogEventTag;
#pragma pack(pop)

/* 共享元数据文件头。 */
typedef struct {
    char magic[8];
    uint32_t header_version;
    OptbinlogSharedTagState state;
    unsigned int num_arrays;
    unsigned int tag_count;
    int bitmap_offset;
    int eventtag_offset;
    uint32_t schema_hash;
    uint64_t generation;
    uint32_t total_size;
    uint32_t init_wait_loops;
    uint32_t init_wait_ms;
} OptbinlogSharedTag;

/* 从事件定义目录初始化（或复用）共享元数据文件。 */
int optbinlog_shared_init_from_dir(const char* eventlog_dir, const char* shared_path, int strict_perm);
/* 设置共享文件权限校验策略。 */
int optbinlog_shared_set_strict_perm(int strict_perm);
/* 打开并映射共享元数据文件（只读）。 */
int optbinlog_shared_open(const char* shared_path, void** base, size_t* size, OptbinlogSharedTag** header);
/* 关闭共享元数据映射。 */
void optbinlog_shared_close(void* base, size_t size);

/* 按 tag_id/字段数从共享区查找 tag 元数据。 */
OptbinlogEventTag* optbinlog_lookup_tag(void* base, OptbinlogSharedTag* header, int tag_id, int icnt);
/* 查询位图中某个 tag 是否存在。 */
int optbinlog_bitmap_get(const OptbinlogBitmap* bm, int idx);
/* 查询位图里最大的已使用 tag 下标（+1）。 */
int optbinlog_bitmap_get_max(const OptbinlogBitmap* bm);

#endif
