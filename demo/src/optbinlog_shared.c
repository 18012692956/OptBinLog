#include "optbinlog_shared.h"
#include "optbinlog_eventlog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

_Static_assert(sizeof(OptbinlogBitmap) == (OPTBINLOG_EVENT_TAG_ARRAY_LEN / 8 + 1), "Bitmap size mismatch");
_Static_assert(sizeof(OptbinlogEventTagEle) == 33, "EventTagEle size mismatch");
_Static_assert(sizeof(OptbinlogEventTag) == 54, "EventTag size mismatch");

static void* OPTBINLOG_MALLOC_START = NULL;
static int OPTBINLOG_MALLOC_OFFSET = 0;
static size_t OPTBINLOG_SHAREDMEM = 0;
static int OPTBINLOG_STRICT_PERM = 0;

static void trace_event(const char* event) {
    const char* path = getenv("OPTBINLOG_TRACE_PATH");
    if (!path || !event) return;
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    char buf[256];
    int len = snprintf(buf, sizeof(buf), "%lld pid=%d %s\n",
                       (long long)ts.tv_sec * 1000000000ll + ts.tv_nsec,
                       (int)getpid(), event);
    if (len > 0) {
        (void)write(fd, buf, (size_t)len);
    }
    close(fd);
}

static void* optbinlog_malloc(size_t size) {
    void* addr = NULL;
    if ((size_t)OPTBINLOG_MALLOC_OFFSET + size >= OPTBINLOG_SHAREDMEM) {
        return NULL;
    }
    addr = (uint8_t*)OPTBINLOG_MALLOC_START + OPTBINLOG_MALLOC_OFFSET;
    OPTBINLOG_MALLOC_OFFSET += (int)size;
    return addr;
}

static void bitmap_set(OptbinlogBitmap* bm, int idx) {
    int byte_i = idx / 8;
    int bit_i = idx % 8;
    bm->bits[byte_i] |= (uint8_t)(1u << bit_i);
}

int optbinlog_bitmap_get(const OptbinlogBitmap* bm, int idx) {
    int byte_i = idx / 8;
    int bit_i = idx % 8;
    return (bm->bits[byte_i] & (uint8_t)(1u << bit_i)) ? 1 : 0;
}

int optbinlog_bitmap_get_max(const OptbinlogBitmap* bm) {
    int max_idx = 0;
    for (int i = 0; i < OPTBINLOG_EVENT_TAG_ARRAY_LEN; i++) {
        if (optbinlog_bitmap_get(bm, i)) {
            max_idx = i + 1;
        }
    }
    return max_idx;
}

static int type_code_from_char(char c) {
    if (c == 'L') return 1;
    if (c == 'D') return 2;
    if (c == 'S') return 3;
    return 0;
}

static void* map_file(const char* path, size_t size, int create, int* out_fd) {
    int flags = create ? (O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC | O_EXCL) : (O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    int fd = open(path, flags, 0644);
    if (fd < 0) {
        *out_fd = -1;
        return NULL;
    }
    if (create) {
        if (ftruncate(fd, (off_t)size) != 0) {
            close(fd);
            *out_fd = -1;
            return NULL;
        }
    }
    int prot = create ? (PROT_READ | PROT_WRITE) : PROT_READ;
    void* addr = mmap(NULL, size, prot, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        *out_fd = -1;
        return NULL;
    }
    *out_fd = fd;
    return addr;
}

static size_t sharedmem_size_get(int num_arrays, int num_eles, int array_lens_total) {
    size_t sharedmem = 4;
    sharedmem += sizeof(OptbinlogSharedTag);
    sharedmem += (size_t)num_arrays * sizeof(OptbinlogBitmap);
    sharedmem += (size_t)array_lens_total * sizeof(OptbinlogEventTag);
    sharedmem += (size_t)num_eles * sizeof(OptbinlogEventTagEle);
    return sharedmem;
}

static int validate_shared_file(int fd, const char* shared_path, struct stat* st) {
    if (fstat(fd, st) < 0) return -1;
    if (OPTBINLOG_STRICT_PERM) {
        if ((st->st_uid != 0) || (st->st_gid != 0)) return -1;
        if ((st->st_mode & (S_IWGRP | S_IWOTH)) != 0) return -1;
    }
    if (st->st_size < (off_t)sizeof(OptbinlogSharedTag)) return -1;
    (void)shared_path;
    return 0;
}

int optbinlog_shared_set_strict_perm(int strict_perm) {
    OPTBINLOG_STRICT_PERM = strict_perm ? 1 : 0;
    return 0;
}

static int open_existing_shared(const char* shared_path, void** base, size_t* size, OptbinlogSharedTag** header) {
    int fd = open(shared_path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) return -1;
    struct stat st;
    if (validate_shared_file(fd, shared_path, &st) != 0) {
        close(fd);
        return -1;
    }
    void* addr = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        return -1;
    }
    OptbinlogSharedTag* hdr = (OptbinlogSharedTag*)addr;
    if (memcmp(hdr->magic, OPTBINLOG_SHARED_MAGIC, 8) != 0) {
        munmap(addr, (size_t)st.st_size);
        close(fd);
        return -1;
    }
    if (hdr->state != OPTBINLOG_INITIALIZED) {
        munmap(addr, (size_t)st.st_size);
        close(fd);
        return -1;
    }
    close(fd);
    *base = addr;
    *size = (size_t)st.st_size;
    *header = hdr;
    return 0;
}

int optbinlog_shared_open(const char* shared_path, void** base, size_t* size, OptbinlogSharedTag** header) {
    return open_existing_shared(shared_path, base, size, header);
}

void optbinlog_shared_close(void* base, size_t size) {
    if (base && size > 0) {
        munmap(base, size);
    }
}

int optbinlog_shared_init_from_dir(const char* eventlog_dir, const char* shared_path, int strict_perm) {
    optbinlog_shared_set_strict_perm(strict_perm);
    trace_event("init_start");
    if (access(shared_path, F_OK) == 0) {
        trace_event("shared_exists");
        int tries = 0;
        do {
            int fd = open(shared_path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
            if (fd < 0) break;
            struct stat st;
            if (validate_shared_file(fd, shared_path, &st) != 0) {
                close(fd);
                break;
            }
            size_t size = (size_t)st.st_size;
            void* addr = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
            if (addr == MAP_FAILED) {
                close(fd);
                break;
            }
            OptbinlogSharedTag* hdr = (OptbinlogSharedTag*)addr;
            if (memcmp(hdr->magic, OPTBINLOG_SHARED_MAGIC, 8) != 0) {
                munmap(addr, size);
                close(fd);
                break;
            }
            if (hdr->state == OPTBINLOG_INITIALIZING) {
                trace_event("wait_initializing");
                munmap(addr, size);
                close(fd);
                usleep(10000);
                tries++;
                continue;
            }
            munmap(addr, size);
            close(fd);
            trace_event("open_existing_ok");
            return 0;
        } while (tries < 3);
    }

    OptbinlogTagList tags;
    optbinlog_taglist_init(&tags);
    if (optbinlog_parse_eventlog_dir(eventlog_dir, &tags) != 0) {
        optbinlog_taglist_free(&tags);
        return -1;
    }
    if (tags.len == 0) {
        optbinlog_taglist_free(&tags);
        fprintf(stderr, "no tags found in %s\n", eventlog_dir);
        return -1;
    }

    int max_id = tags.items[0].tag_id;
    for (size_t i = 1; i < tags.len; i++) {
        if (tags.items[i].tag_id > max_id) max_id = tags.items[i].tag_id;
    }
    int num_arrays = max_id / OPTBINLOG_EVENT_TAG_ARRAY_LEN + 1;

    OptbinlogBitmap* bitmap = calloc((size_t)num_arrays, sizeof(OptbinlogBitmap));
    int* array_max = calloc((size_t)num_arrays, sizeof(int));
    if (!bitmap || !array_max) {
        fprintf(stderr, "OOM\n");
        optbinlog_taglist_free(&tags);
        free(bitmap);
        free(array_max);
        return -1;
    }

    int total_eles = 0;
    for (size_t i = 0; i < tags.len; i++) {
        OptbinlogTagDef* tag = &tags.items[i];
        int arr = tag->tag_id / OPTBINLOG_EVENT_TAG_ARRAY_LEN;
        int idx = tag->tag_id % OPTBINLOG_EVENT_TAG_ARRAY_LEN;
        bitmap_set(&bitmap[arr], idx);
        if (idx + 1 > array_max[arr]) array_max[arr] = idx + 1;
        total_eles += tag->ele_num;
    }

    int total_eventtags = 0;
    for (int i = 0; i < num_arrays; i++) total_eventtags += array_max[i];

    size_t total_size = sharedmem_size_get(num_arrays, total_eles, total_eventtags);

    int fd = -1;
    trace_event("create_attempt");
    void* base = map_file(shared_path, total_size, 1, &fd);
    if (!base) {
        if (errno == EEXIST) {
            trace_event("create_eexist");
            int tries = 0;
            do {
                int fd2 = open(shared_path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
                if (fd2 < 0) break;
                struct stat st;
                if (validate_shared_file(fd2, shared_path, &st) != 0) {
                    close(fd2);
                    break;
                }
                size_t size = (size_t)st.st_size;
                void* addr = mmap(NULL, size, PROT_READ, MAP_SHARED, fd2, 0);
                if (addr == MAP_FAILED) {
                    close(fd2);
                    break;
                }
                OptbinlogSharedTag* hdr = (OptbinlogSharedTag*)addr;
                if (memcmp(hdr->magic, OPTBINLOG_SHARED_MAGIC, 8) != 0) {
                    munmap(addr, size);
                    close(fd2);
                    break;
                }
                if (hdr->state == OPTBINLOG_INITIALIZING) {
                    trace_event("wait_initializing");
                    munmap(addr, size);
                    close(fd2);
                    usleep(10000);
                    tries++;
                    continue;
                }
                munmap(addr, size);
                close(fd2);
                trace_event("open_existing_ok");
                optbinlog_taglist_free(&tags);
                free(bitmap);
                free(array_max);
                return 0;
            } while (tries < 3);
        }
        fprintf(stderr, "create shared file failed: %s\n", strerror(errno));
        optbinlog_taglist_free(&tags);
        free(bitmap);
        free(array_max);
        return -1;
    }

    trace_event("create_success");
    memset(base, 0, total_size);

    OPTBINLOG_MALLOC_START = base;
    OPTBINLOG_MALLOC_OFFSET = 0;
    OPTBINLOG_SHAREDMEM = total_size;

    OptbinlogSharedTag* header = (OptbinlogSharedTag*)optbinlog_malloc(sizeof(OptbinlogSharedTag));
    if (!header) {
        munmap(base, total_size);
        close(fd);
        free(bitmap);
        free(array_max);
        optbinlog_taglist_free(&tags);
        return -1;
    }

    header->state = OPTBINLOG_INITIALIZING;
    memcpy(header->magic, OPTBINLOG_SHARED_MAGIC, 8);
    header->num_arrays = (unsigned int)num_arrays;

    OptbinlogBitmap* bitmap_out = (OptbinlogBitmap*)optbinlog_malloc((size_t)num_arrays * sizeof(OptbinlogBitmap));
    if (!bitmap_out) {
        munmap(base, total_size);
        close(fd);
        free(bitmap);
        free(array_max);
        optbinlog_taglist_free(&tags);
        return -1;
    }
    header->bitmap_offset = (int)((uint8_t*)bitmap_out - (uint8_t*)base);
    memcpy(bitmap_out, bitmap, (size_t)num_arrays * sizeof(OptbinlogBitmap));

    OptbinlogEventTag* eventtag_out = (OptbinlogEventTag*)optbinlog_malloc((size_t)total_eventtags * sizeof(OptbinlogEventTag));
    if (!eventtag_out) {
        munmap(base, total_size);
        close(fd);
        free(bitmap);
        free(array_max);
        optbinlog_taglist_free(&tags);
        return -1;
    }
    header->eventtag_offset = (int)((uint8_t*)eventtag_out - (uint8_t*)base);

    int ele_cursor = OPTBINLOG_MALLOC_OFFSET;

    for (int arr = 0; arr < num_arrays; arr++) {
        int arr_base_index = 0;
        for (int i = 0; i < arr; i++) arr_base_index += array_max[i];
        for (int idx = 0; idx < array_max[arr]; idx++) {
            int tag_id = arr * OPTBINLOG_EVENT_TAG_ARRAY_LEN + idx;
            OptbinlogTagDef* found = NULL;
            for (size_t t = 0; t < tags.len; t++) {
                if (tags.items[t].tag_id == tag_id) {
                    found = &tags.items[t];
                    break;
                }
            }
            OptbinlogEventTag* out = &eventtag_out[arr_base_index + idx];
            if (!found) {
                memset(out, 0, sizeof(OptbinlogEventTag));
                continue;
            }
            out->tag_index = (unsigned int)found->tag_id;
            out->tag_ele_num = (unsigned int)found->ele_num;
            out->tag_ele_offset = ele_cursor;
            memset(out->tag_name, 0, sizeof(out->tag_name));
            strncpy(out->tag_name, found->name, sizeof(out->tag_name) - 1);

            for (int e = 0; e < found->ele_num; e++) {
                OptbinlogEventTagEle* eo = (OptbinlogEventTagEle*)((uint8_t*)base + ele_cursor);
                eo->type = (unsigned int)type_code_from_char(found->eles[e].type_char);
                eo->len = (unsigned int)(found->eles[e].bits / 8);
                memset(eo->name, 0, sizeof(eo->name));
                strncpy(eo->name, found->eles[e].name, sizeof(eo->name) - 1);
                ele_cursor += (int)sizeof(OptbinlogEventTagEle);
            }
        }
    }

    header->state = OPTBINLOG_INITIALIZED;
    msync(base, total_size, MS_SYNC);
    munmap(base, total_size);
    close(fd);
    trace_event("init_done");

    free(bitmap);
    free(array_max);
    optbinlog_taglist_free(&tags);

    return 0;
}

OptbinlogEventTag* optbinlog_lookup_tag(void* base, OptbinlogSharedTag* header, int tag_id, int icnt) {
    int arr = tag_id / OPTBINLOG_EVENT_TAG_ARRAY_LEN;
    int idx = tag_id % OPTBINLOG_EVENT_TAG_ARRAY_LEN;
    if (arr >= (int)header->num_arrays) return NULL;

    OptbinlogBitmap* bitmap = (OptbinlogBitmap*)((uint8_t*)base + header->bitmap_offset);
    if (!optbinlog_bitmap_get(&bitmap[arr], idx)) return NULL;

    int arrayoffset = 0;
    for (int i = 0; i < arr; i++) {
        arrayoffset += optbinlog_bitmap_get_max(&bitmap[i]);
    }
    OptbinlogEventTag* tags = (OptbinlogEventTag*)((uint8_t*)base + header->eventtag_offset);
    OptbinlogEventTag* tag = &tags[arrayoffset + idx];
    if (tag->tag_index != (unsigned int)tag_id) return NULL;
    if (icnt != -1 && (int)tag->tag_ele_num != icnt) return NULL;
    return tag;
}
