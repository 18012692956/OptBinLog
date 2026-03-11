#include "optbinlog_eventlog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <limits.h>
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

static int taglist_push(OptbinlogTagList* list, const OptbinlogTagDef* tag) {
    if (list->len == list->cap) {
        size_t new_cap = list->cap ? list->cap * 2 : 8;
        OptbinlogTagDef* next = realloc(list->items, new_cap * sizeof(OptbinlogTagDef));
        if (!next) {
            return -1;
        }
        list->items = next;
        list->cap = new_cap;
    }
    list->items[list->len++] = *tag;
    return 0;
}

void optbinlog_taglist_free(OptbinlogTagList* list) {
    for (size_t i = 0; i < list->len; i++) {
        free(list->items[i].eles);
    }
    free(list->items);
}

static int parse_elements(const char* line, OptbinlogTagDef* tag, const char* path, int line_no) {
    const char* p = line;
    int cap = 4;
    tag->eles = calloc((size_t)cap, sizeof(OptbinlogTagEleDef));
    if (!tag->eles) return -1;
    tag->ele_num = 0;
    int saw_paren = 0;
    while ((p = strchr(p, '(')) != NULL) {
        saw_paren = 1;
        const char* q = strchr(p, ')');
        if (!q) {
            fprintf(stderr, "invalid element block in %s:%d\n", path, line_no);
            return -1;
        }
        char buf[128];
        size_t len = (size_t)(q - p - 1);
        if (len >= sizeof(buf)) len = sizeof(buf) - 1;
        memcpy(buf, p + 1, len);
        buf[len] = '\0';

        char name[32] = {0};
        char type_char = 0;
        int bits = 0;
        if (sscanf(buf, " %31[^|]|%c|%d", name, &type_char, &bits) != 3) {
            fprintf(stderr, "invalid element format in %s:%d\n", path, line_no);
            return -1;
        }
        if (tag->ele_num == cap) {
            cap *= 2;
            OptbinlogTagEleDef* next = realloc(tag->eles, (size_t)cap * sizeof(OptbinlogTagEleDef));
            if (!next) return -1;
            tag->eles = next;
        }
        OptbinlogTagEleDef* ele = &tag->eles[tag->ele_num++];
        strncpy(ele->name, name, sizeof(ele->name) - 1);
        ele->type_char = type_char;
        ele->bits = bits;
        p = q + 1;
    }
    if (saw_paren && tag->ele_num == 0) {
        fprintf(stderr, "empty element list in %s:%d\n", path, line_no);
        return -1;
    }
    return 0;
}

static int parse_eventlog_file(const char* path, OptbinlogTagList* out) {
    FILE* fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "open %s failed: %s\n", path, strerror(errno));
        return -1;
    }
    char line[512];
    int line_no = 0;
    int rc = 0;
    while (fgets(line, sizeof(line), fp)) {
        line_no++;
        char* s = trim(line);
        if (*s == '\0' || *s == '#') continue;

        char* p = s;
        char* endptr = NULL;
        errno = 0;
        long tag_id = strtol(p, &endptr, 10);
        if (endptr == p || errno != 0 || tag_id < 0 || tag_id > INT_MAX) {
            fprintf(stderr, "invalid tag id in %s:%d\n", path, line_no);
            rc = -1;
            break;
        }
        p = endptr;
        while (*p == ' ') p++;
        char name[48] = {0};
        int ni = 0;
        while (*p && *p != ' ' && *p != '(' && ni < (int)sizeof(name) - 1) {
            name[ni++] = *p++;
        }
        name[ni] = '\0';
        if (name[0] == '\0') {
            fprintf(stderr, "missing tag name in %s:%d\n", path, line_no);
            rc = -1;
            break;
        }

        OptbinlogTagDef tag;
        memset(&tag, 0, sizeof(tag));
        tag.tag_id = (int)tag_id;
        strncpy(tag.name, name, sizeof(tag.name) - 1);
        if (parse_elements(s, &tag, path, line_no) != 0) {
            free(tag.eles);
            rc = -1;
            break;
        }
        if (taglist_push(out, &tag) != 0) {
            free(tag.eles);
            fprintf(stderr, "OOM\n");
            rc = -1;
            break;
        }
    }
    fclose(fp);
    return rc;
}

typedef struct {
    char** items;
    size_t len;
    size_t cap;
} PathList;

static int path_list_push(PathList* list, const char* path) {
    if (list->len == list->cap) {
        size_t new_cap = list->cap ? list->cap * 2 : 16;
        char** next = realloc(list->items, new_cap * sizeof(char*));
        if (!next) return -1;
        list->items = next;
        list->cap = new_cap;
    }
    char* dup = strdup(path);
    if (!dup) return -1;
    list->items[list->len++] = dup;
    return 0;
}

static void path_list_free(PathList* list) {
    if (!list) return;
    for (size_t i = 0; i < list->len; i++) free(list->items[i]);
    free(list->items);
    list->items = NULL;
    list->len = 0;
    list->cap = 0;
}

static int path_cmp(const void* a, const void* b) {
    const char* const* pa = (const char* const*)a;
    const char* const* pb = (const char* const*)b;
    return strcmp(*pa, *pb);
}

static int has_txt_suffix(const char* name) {
    size_t n = strlen(name);
    return (n >= 4 && strcmp(name + n - 4, ".txt") == 0) ? 1 : 0;
}

int optbinlog_parse_eventlog_dir(const char* dirpath, OptbinlogTagList* out) {
    DIR* dir = opendir(dirpath);
    if (!dir) {
        fprintf(stderr, "opendir %s failed: %s\n", dirpath, strerror(errno));
        return -1;
    }

    PathList files = {0};
    int rc = 0;
    struct dirent* dp;
    while ((dp = readdir(dir)) != NULL) {
        if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) continue;
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", dirpath, dp->d_name);
        struct stat st;
        if (stat(path, &st) != 0) continue;
        if (S_ISREG(st.st_mode) && has_txt_suffix(dp->d_name)) {
            if (path_list_push(&files, path) != 0) {
                rc = -1;
                break;
            }
        }
    }
    closedir(dir);

    if (rc != 0) {
        fprintf(stderr, "OOM while collecting eventlog files\n");
        path_list_free(&files);
        return -1;
    }

    qsort(files.items, files.len, sizeof(files.items[0]), path_cmp);
    for (size_t i = 0; i < files.len; i++) {
        if (parse_eventlog_file(files.items[i], out) != 0) {
            rc = -1;
            break;
        }
    }

    path_list_free(&files);
    return rc;
}
