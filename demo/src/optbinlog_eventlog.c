#include "optbinlog_eventlog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>

static char* trim(char* s) {
    while (*s == ' ' || *s == '\t' || *s == '\r' || *s == '\n') s++;
    char* end = s + strlen(s);
    while (end > s && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\r' || end[-1] == '\n')) end--;
    *end = '\0';
    return s;
}

void optbinlog_taglist_init(OptbinlogTagList* list) {
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

static void taglist_push(OptbinlogTagList* list, const OptbinlogTagDef* tag) {
    if (list->len == list->cap) {
        size_t new_cap = list->cap ? list->cap * 2 : 8;
        OptbinlogTagDef* next = realloc(list->items, new_cap * sizeof(OptbinlogTagDef));
        if (!next) {
            fprintf(stderr, "OOM\n");
            exit(1);
        }
        list->items = next;
        list->cap = new_cap;
    }
    list->items[list->len++] = *tag;
}

void optbinlog_taglist_free(OptbinlogTagList* list) {
    for (size_t i = 0; i < list->len; i++) {
        free(list->items[i].eles);
    }
    free(list->items);
}

static void parse_elements(const char* line, OptbinlogTagDef* tag) {
    const char* p = line;
    int cap = 4;
    tag->eles = calloc((size_t)cap, sizeof(OptbinlogTagEleDef));
    tag->ele_num = 0;
    while ((p = strchr(p, '(')) != NULL) {
        const char* q = strchr(p, ')');
        if (!q) break;
        char buf[128];
        size_t len = (size_t)(q - p - 1);
        if (len >= sizeof(buf)) len = sizeof(buf) - 1;
        memcpy(buf, p + 1, len);
        buf[len] = '\0';

        char name[32] = {0};
        char type_char = 0;
        int bits = 0;
        if (sscanf(buf, " %31[^|]|%c|%d", name, &type_char, &bits) == 3) {
            if (tag->ele_num == cap) {
                cap *= 2;
                OptbinlogTagEleDef* next = realloc(tag->eles, (size_t)cap * sizeof(OptbinlogTagEleDef));
                if (!next) {
                    fprintf(stderr, "OOM\n");
                    exit(1);
                }
                tag->eles = next;
            }
            OptbinlogTagEleDef* ele = &tag->eles[tag->ele_num++];
            strncpy(ele->name, name, sizeof(ele->name) - 1);
            ele->type_char = type_char;
            ele->bits = bits;
        }
        p = q + 1;
    }
}

static void parse_eventlog_file(const char* path, OptbinlogTagList* out) {
    FILE* fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", path, strerror(errno));
        return;
    }
    char line[512];
    while (fgets(line, sizeof(line), fp)) {
        char* s = trim(line);
        if (*s == '\0' || *s == '#') continue;

        char* p = s;
        char* endptr = NULL;
        long tag_id = strtol(p, &endptr, 10);
        if (endptr == p) continue;
        p = endptr;
        while (*p == ' ') p++;
        char name[48] = {0};
        int ni = 0;
        while (*p && *p != ' ' && *p != '(' && ni < (int)sizeof(name) - 1) {
            name[ni++] = *p++;
        }
        name[ni] = '\0';
        if (name[0] == '\0') continue;

        OptbinlogTagDef tag;
        memset(&tag, 0, sizeof(tag));
        tag.tag_id = (int)tag_id;
        strncpy(tag.name, name, sizeof(tag.name) - 1);
        parse_elements(s, &tag);
        taglist_push(out, &tag);
    }
    fclose(fp);
}

int optbinlog_parse_eventlog_dir(const char* dirpath, OptbinlogTagList* out) {
    DIR* dir = opendir(dirpath);
    if (!dir) {
        fprintf(stderr, "opendir %s failed: %s\n", dirpath, strerror(errno));
        return -1;
    }
    struct dirent* dp;
    while ((dp = readdir(dir)) != NULL) {
        if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) continue;
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", dirpath, dp->d_name);
        struct stat st;
        if (stat(path, &st) != 0) continue;
        if (S_ISREG(st.st_mode)) {
            parse_eventlog_file(path, out);
        }
    }
    closedir(dir);
    return 0;
}
