#ifndef OPTBINLOG_EVENTLOG_H
#define OPTBINLOG_EVENTLOG_H

#include <stddef.h>

/* 单个字段定义：名称 + 类型 + 位宽。 */
typedef struct {
    char name[32];
    char type_char; /* L D S */
    int bits;
} OptbinlogTagEleDef;

/* 单个事件(tag)定义：tag_id + 名称 + 字段列表。 */
typedef struct {
    int tag_id;
    char name[48];
    int ele_num;
    OptbinlogTagEleDef* eles;
} OptbinlogTagDef;

/* 事件定义数组容器。 */
typedef struct {
    OptbinlogTagDef* items;
    size_t len;
    size_t cap;
} OptbinlogTagList;

/* 初始化 tag 列表容器。 */
void optbinlog_taglist_init(OptbinlogTagList* list);
/* 释放 tag 列表容器及其内部字段数组。 */
void optbinlog_taglist_free(OptbinlogTagList* list);
/* 解析目录下全部事件定义文本，填充到 tag 列表。 */
int optbinlog_parse_eventlog_dir(const char* dirpath, OptbinlogTagList* out);

#endif
