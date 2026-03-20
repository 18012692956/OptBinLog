#ifndef OPTBINLOG_BINLOG_H
#define OPTBINLOG_BINLOG_H

#include <stdint.h>
#include <stddef.h>

typedef enum {
    OPTBINLOG_VAL_U = 0,
    OPTBINLOG_VAL_D = 1,
    OPTBINLOG_VAL_S = 2,
} OptbinlogValueKind;

typedef struct {
    OptbinlogValueKind kind;
    uint64_t u;
    double d;
    const char* s;
} OptbinlogValue;

typedef struct {
    int64_t timestamp;
    int tag_id;
    int ele_count;
    OptbinlogValue* values;
} OptbinlogRecord;

typedef int (*OptbinlogRecordCallback)(const OptbinlogRecord* rec, void* user);

int optbinlog_binlog_write(const char* shared_path, const char* log_path, const OptbinlogRecord* records, size_t count);
int optbinlog_binlog_read(const char* shared_path, const char* log_path, OptbinlogRecordCallback cb, void* user);
/*
 * Repair tail damage caused by power loss during frame write.
 * Return value:
 *   0  -> file already clean / no truncation needed
 *   1  -> damaged tail detected and truncated to last valid frame
 *  -1  -> repair failed
 */
int optbinlog_binlog_recover_tail(const char* log_path, size_t* before_bytes, size_t* after_bytes);

#endif
