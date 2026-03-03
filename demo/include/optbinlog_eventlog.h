#ifndef OPTBINLOG_EVENTLOG_H
#define OPTBINLOG_EVENTLOG_H

#include <stddef.h>

typedef struct {
    char name[32];
    char type_char; /* L D S */
    int bits;
} OptbinlogTagEleDef;

typedef struct {
    int tag_id;
    char name[48];
    int ele_num;
    OptbinlogTagEleDef* eles;
} OptbinlogTagDef;

typedef struct {
    OptbinlogTagDef* items;
    size_t len;
    size_t cap;
} OptbinlogTagList;

void optbinlog_taglist_init(OptbinlogTagList* list);
void optbinlog_taglist_free(OptbinlogTagList* list);
int optbinlog_parse_eventlog_dir(const char* dirpath, OptbinlogTagList* out);

#endif
