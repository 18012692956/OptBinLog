#ifndef OPTBINLOG_BINLOG_H
#define OPTBINLOG_BINLOG_H

#include <stdint.h>
#include <stddef.h>

/* 字段值类型：无符号整数/双精度/字符串。 */
typedef enum {
    OPTBINLOG_VAL_U = 0,
    OPTBINLOG_VAL_D = 1,
    OPTBINLOG_VAL_S = 2,
} OptbinlogValueKind;

/* 单字段值容器。 */
typedef struct {
    OptbinlogValueKind kind;
    uint64_t u;
    double d;
    const char* s;
} OptbinlogValue;

/* 一条日志记录：时间戳 + tag + 字段数组。 */
typedef struct {
    int64_t timestamp;
    int tag_id;
    int ele_count;
    OptbinlogValue* values;
} OptbinlogRecord;

/* 读取回调：返回 0 继续，非 0 提前停止。 */
typedef int (*OptbinlogRecordCallback)(const OptbinlogRecord* rec, void* user);

/* 按共享 schema 将记录写为二进制帧。 */
int optbinlog_binlog_write(const char* shared_path, const char* log_path, const OptbinlogRecord* records, size_t count);
/* 读取并校验二进制帧，按 schema 解码后回调。 */
int optbinlog_binlog_read(const char* shared_path, const char* log_path, OptbinlogRecordCallback cb, void* user);
/*
 * 修复掉电导致的日志尾部半帧。
 * 返回值：
 *   0  -> 文件本身干净，无需截断
 *   1  -> 检测到坏尾并已截断到最后一条完整帧
 *  -1  -> 修复失败
 */
int optbinlog_binlog_recover_tail(const char* log_path, size_t* before_bytes, size_t* after_bytes);

#endif
