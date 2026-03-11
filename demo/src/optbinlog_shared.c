#include "optbinlog_shared.h"
#include "optbinlog_eventlog.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

_Static_assert(sizeof(OptbinlogBitmap) == (OPTBINLOG_EVENT_TAG_ARRAY_LEN / 8 + 1), "Bitmap size mismatch");
_Static_assert(sizeof(OptbinlogEventTagEle) == 33, "EventTagEle size mismatch");
_Static_assert(sizeof(OptbinlogEventTag) == 54, "EventTag size mismatch");

#define OPTBINLOG_TAG_ID_MAX 0x0FFF
#define OPTBINLOG_TAG_ELE_NUM_MAX 0x0F
#define OPTBINLOG_ELE_LEN_MAX_BYTES 0x3F

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

static uint64_t monotonic_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static uint64_t realtime_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int getenv_int(const char* name, int default_value, int min_value, int max_value) {
    const char* raw = getenv(name);
    if (!raw || !raw[0]) return default_value;
    char* end = NULL;
    long v = strtol(raw, &end, 10);
    if (end == raw || *end != '\0') return default_value;
    if (v < min_value) v = min_value;
    if (v > max_value) v = max_value;
    return (int)v;
}

static char* make_lock_path(const char* shared_path) {
    size_t n = strlen(shared_path) + 6;
    char* lock_path = malloc(n);
    if (!lock_path) return NULL;
    snprintf(lock_path, n, "%s.lock", shared_path);
    return lock_path;
}

static int acquire_init_lock(const char* shared_path,
                             int* lock_fd,
                             int* lock_mode,
                             char** lock_path_out,
                             uint32_t* wait_loops,
                             uint32_t* wait_ms) {
    int timeout_ms = getenv_int("OPTBINLOG_INIT_LOCK_TIMEOUT_MS", 5000, 100, 120000);
    int sleep_us = getenv_int("OPTBINLOG_INIT_LOCK_SLEEP_US", 10000, 100, 1000000);
    const char* lock_mode_env = getenv("OPTBINLOG_INIT_LOCK_MODE");
    int use_create_excl = (lock_mode_env && strcmp(lock_mode_env, "create_excl") == 0) ? 1 : 0;

    char* lock_path = make_lock_path(shared_path);
    if (!lock_path) return -1;

    uint64_t start_ms = monotonic_ms();
    uint32_t loops = 0;
    if (use_create_excl) {
        for (;;) {
            int fd = open(lock_path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
            if (fd >= 0) {
                uint64_t elapsed = monotonic_ms() - start_ms;
                if (wait_loops) *wait_loops = loops;
                if (wait_ms) *wait_ms = (uint32_t)elapsed;
                if (lock_mode) *lock_mode = 1;
                if (lock_path_out) {
                    *lock_path_out = lock_path;
                } else {
                    free(lock_path);
                }
                *lock_fd = fd;
                return 0;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == ENOENT) {
                uint64_t elapsed = monotonic_ms() - start_ms;
                if ((int)elapsed >= timeout_ms) {
                    trace_event("init_lock_timeout");
                    free(lock_path);
                    errno = ETIMEDOUT;
                    return -1;
                }
                trace_event("wait_initializing");
                usleep((useconds_t)sleep_us);
                loops++;
                continue;
            }
            if (errno != EEXIST) {
                free(lock_path);
                return -1;
            }
            uint64_t elapsed = monotonic_ms() - start_ms;
            if ((int)elapsed >= timeout_ms) {
                trace_event("init_lock_timeout");
                free(lock_path);
                errno = ETIMEDOUT;
                return -1;
            }
            trace_event("wait_initializing");
            usleep((useconds_t)sleep_us);
            loops++;
        }
    }

    int fd = open(lock_path, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) {
        free(lock_path);
        return -1;
    }
    for (;;) {
        if (flock(fd, LOCK_EX | LOCK_NB) == 0) {
            uint64_t elapsed = monotonic_ms() - start_ms;
            if (wait_loops) *wait_loops = loops;
            if (wait_ms) *wait_ms = (uint32_t)elapsed;
            if (lock_mode) *lock_mode = 0;
            if (lock_path_out) {
                *lock_path_out = lock_path;
            } else {
                free(lock_path);
            }
            *lock_fd = fd;
            return 0;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EWOULDBLOCK && errno != EAGAIN) {
            free(lock_path);
            close(fd);
            return -1;
        }
        uint64_t elapsed = monotonic_ms() - start_ms;
        if ((int)elapsed >= timeout_ms) {
            trace_event("init_lock_timeout");
            free(lock_path);
            close(fd);
            errno = ETIMEDOUT;
            return -1;
        }
        trace_event("wait_initializing");
        usleep((useconds_t)sleep_us);
        loops++;
    }
}

static void release_init_lock(int* lock_fd, int lock_mode, char** lock_path) {
    if (!lock_fd || *lock_fd < 0) return;
    if (lock_mode == 0) {
        (void)flock(*lock_fd, LOCK_UN);
    }
    close(*lock_fd);
    if (lock_mode == 1 && lock_path && *lock_path) {
        (void)unlink(*lock_path);
    }
    if (lock_path && *lock_path) {
        free(*lock_path);
        *lock_path = NULL;
    }
    *lock_fd = -1;
}

static void* optbinlog_malloc(size_t size) {
    void* addr = NULL;
    if ((size_t)OPTBINLOG_MALLOC_OFFSET + size > OPTBINLOG_SHAREDMEM) {
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

static int bitmap_count_ones(const OptbinlogBitmap* bm) {
    int cnt = 0;
    for (int i = 0; i < OPTBINLOG_EVENT_TAG_ARRAY_LEN; i++) {
        if (optbinlog_bitmap_get(bm, i)) cnt++;
    }
    return cnt;
}

static int bitmap_rank_inclusive(const OptbinlogBitmap* bm, int idx) {
    int cnt = 0;
    for (int i = 0; i <= idx; i++) {
        if (optbinlog_bitmap_get(bm, i)) cnt++;
    }
    return cnt;
}

static int type_code_from_char(char c) {
    if (c == 'L') return 1;
    if (c == 'D') return 2;
    if (c == 'S') return 3;
    return 0;
}

static int range_within(size_t offset, size_t len, size_t total) {
    if (offset > total) return -1;
    if (len > total - offset) return -1;
    return 0;
}

static int header_layout_valid(const OptbinlogSharedTag* hdr, size_t map_size) {
    if (hdr->header_version != OPTBINLOG_SHARED_HEADER_VERSION) return -1;
    if (hdr->num_arrays == 0 || hdr->num_arrays > 1000000u) return -1;
    if (hdr->tag_count == 0 || hdr->tag_count > 1000000u) return -1;
    if (hdr->total_size != 0 && hdr->total_size != (uint32_t)map_size) return -1;
    if (hdr->bitmap_offset < (int)sizeof(OptbinlogSharedTag)) return -1;
    if (hdr->eventtag_offset < (int)sizeof(OptbinlogSharedTag)) return -1;

    size_t bitmap_offset = (size_t)hdr->bitmap_offset;
    size_t bitmap_bytes = (size_t)hdr->num_arrays * sizeof(OptbinlogBitmap);
    if (range_within(bitmap_offset, bitmap_bytes, map_size) != 0) return -1;

    size_t tag_offset = (size_t)hdr->eventtag_offset;
    size_t tag_bytes = (size_t)hdr->tag_count * sizeof(OptbinlogEventTag);
    if (range_within(tag_offset, tag_bytes, map_size) != 0) return -1;
    if (tag_offset < bitmap_offset + bitmap_bytes) return -1;
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

static size_t sharedmem_size_get(int num_arrays, int tag_count, int num_eles) {
    size_t sharedmem = 0;
    sharedmem += sizeof(OptbinlogSharedTag);
    sharedmem += (size_t)num_arrays * sizeof(OptbinlogBitmap);
    sharedmem += (size_t)tag_count * sizeof(OptbinlogEventTag);
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

static int open_existing_shared_internal(const char* shared_path,
                                         uint32_t expected_schema_hash,
                                         int require_schema_hash,
                                         void** base,
                                         size_t* size,
                                         OptbinlogSharedTag** header) {
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
    if (memcmp(hdr->magic, OPTBINLOG_SHARED_MAGIC, 8) != 0 ||
        hdr->state != OPTBINLOG_INITIALIZED ||
        header_layout_valid(hdr, (size_t)st.st_size) != 0 ||
        (require_schema_hash && hdr->schema_hash != expected_schema_hash)) {
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

static int open_existing_shared(const char* shared_path, void** base, size_t* size, OptbinlogSharedTag** header) {
    return open_existing_shared_internal(shared_path, 0, 0, base, size, header);
}

int optbinlog_shared_open(const char* shared_path, void** base, size_t* size, OptbinlogSharedTag** header) {
    return open_existing_shared(shared_path, base, size, header);
}

void optbinlog_shared_close(void* base, size_t size) {
    if (base && size > 0) {
        munmap(base, size);
    }
}

static int tag_cmp(const void* a, const void* b) {
    const OptbinlogTagDef* ta = (const OptbinlogTagDef*)a;
    const OptbinlogTagDef* tb = (const OptbinlogTagDef*)b;
    if (ta->tag_id < tb->tag_id) return -1;
    if (ta->tag_id > tb->tag_id) return 1;
    return strncmp(ta->name, tb->name, sizeof(ta->name));
}

static uint32_t fnv1a_update(uint32_t h, const void* data, size_t len) {
    const uint8_t* p = (const uint8_t*)data;
    for (size_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

static uint32_t schema_hash_compute(const OptbinlogTagList* tags) {
    uint32_t h = 2166136261u;
    if (!tags || tags->len == 0) return h;

    OptbinlogTagDef* ordered = calloc(tags->len, sizeof(OptbinlogTagDef));
    if (!ordered) return h;
    memcpy(ordered, tags->items, tags->len * sizeof(OptbinlogTagDef));
    qsort(ordered, tags->len, sizeof(OptbinlogTagDef), tag_cmp);

    for (size_t i = 0; i < tags->len; i++) {
        const OptbinlogTagDef* t = &ordered[i];
        h = fnv1a_update(h, &t->tag_id, sizeof(t->tag_id));
        h = fnv1a_update(h, t->name, strnlen(t->name, sizeof(t->name)));
        h = fnv1a_update(h, &t->ele_num, sizeof(t->ele_num));
        for (int e = 0; e < t->ele_num; e++) {
            h = fnv1a_update(h, t->eles[e].name, strnlen(t->eles[e].name, sizeof(t->eles[e].name)));
            h = fnv1a_update(h, &t->eles[e].type_char, sizeof(t->eles[e].type_char));
            h = fnv1a_update(h, &t->eles[e].bits, sizeof(t->eles[e].bits));
        }
    }

    free(ordered);
    return h;
}

static int validate_tag_schema(const OptbinlogTagList* tags) {
    if (!tags || tags->len == 0) return -1;
    OptbinlogTagDef* ordered = calloc(tags->len, sizeof(OptbinlogTagDef));
    if (!ordered) return -1;
    memcpy(ordered, tags->items, tags->len * sizeof(OptbinlogTagDef));
    qsort(ordered, tags->len, sizeof(OptbinlogTagDef), tag_cmp);

    for (size_t i = 0; i < tags->len; i++) {
        const OptbinlogTagDef* t = &ordered[i];
        if (t->tag_id < 0 || t->tag_id > OPTBINLOG_TAG_ID_MAX) {
            fprintf(stderr, "tag id out of supported range [0,%d]: %d\n", OPTBINLOG_TAG_ID_MAX, t->tag_id);
            free(ordered);
            return -1;
        }
        if (i > 0 && t->tag_id == ordered[i - 1].tag_id) {
            fprintf(stderr, "duplicate tag id detected: %d\n", t->tag_id);
            free(ordered);
            return -1;
        }
        if (t->ele_num < 0 || t->ele_num > OPTBINLOG_TAG_ELE_NUM_MAX) {
            fprintf(stderr, "tag %d has invalid element count: %d\n", t->tag_id, t->ele_num);
            free(ordered);
            return -1;
        }
        for (int e = 0; e < t->ele_num; e++) {
            const OptbinlogTagEleDef* ele = &t->eles[e];
            int type = type_code_from_char(ele->type_char);
            if (type == 0) {
                fprintf(stderr, "tag %d has unsupported element type '%c'\n", t->tag_id, ele->type_char);
                free(ordered);
                return -1;
            }
            if (ele->bits <= 0 || (ele->bits % 8) != 0) {
                fprintf(stderr, "tag %d element %d has invalid bits=%d\n", t->tag_id, e, ele->bits);
                free(ordered);
                return -1;
            }
            int bytes = ele->bits / 8;
            if (bytes <= 0 || bytes > OPTBINLOG_ELE_LEN_MAX_BYTES) {
                fprintf(stderr, "tag %d element %d length out of range: %d bytes\n", t->tag_id, e, bytes);
                free(ordered);
                return -1;
            }
            if (type == 1 && bytes > (int)sizeof(uint64_t)) {
                fprintf(stderr, "tag %d element %d integer bytes exceed uint64_t: %d\n", t->tag_id, e, bytes);
                free(ordered);
                return -1;
            }
            if (type == 2 && bytes != (int)sizeof(double)) {
                fprintf(stderr, "tag %d element %d double bytes must be %zu, got %d\n", t->tag_id, e, sizeof(double), bytes);
                free(ordered);
                return -1;
            }
        }
    }
    free(ordered);
    return 0;
}

int optbinlog_shared_init_from_dir(const char* eventlog_dir, const char* shared_path, int strict_perm) {
    optbinlog_shared_set_strict_perm(strict_perm);
    trace_event("init_start");

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
    if (validate_tag_schema(&tags) != 0) {
        optbinlog_taglist_free(&tags);
        return -1;
    }

    uint32_t schema_hash = schema_hash_compute(&tags);
    int tag_count = (int)tags.len;

    int max_id = tags.items[0].tag_id;
    int total_eles = 0;
    for (size_t i = 0; i < tags.len; i++) {
        OptbinlogTagDef* tag = &tags.items[i];
        if (tag->tag_id > max_id) max_id = tag->tag_id;
        total_eles += tag->ele_num;
    }
    int num_arrays = max_id / OPTBINLOG_EVENT_TAG_ARRAY_LEN + 1;

    OptbinlogBitmap* bitmap = calloc((size_t)num_arrays, sizeof(OptbinlogBitmap));
    if (!bitmap) {
        fprintf(stderr, "OOM\n");
        free(bitmap);
        optbinlog_taglist_free(&tags);
        return -1;
    }

    for (size_t i = 0; i < tags.len; i++) {
        int arr = tags.items[i].tag_id / OPTBINLOG_EVENT_TAG_ARRAY_LEN;
        int idx = tags.items[i].tag_id % OPTBINLOG_EVENT_TAG_ARRAY_LEN;
        bitmap_set(&bitmap[arr], idx);
    }

    size_t total_size = sharedmem_size_get(num_arrays, tag_count, total_eles);
    if (total_size > UINT32_MAX) {
        fprintf(stderr, "shared layout too large\n");
        free(bitmap);
        optbinlog_taglist_free(&tags);
        return -1;
    }

    int lock_fd = -1;
    int lock_mode = 0;
    char* lock_path = NULL;
    uint32_t wait_loops = 0;
    uint32_t wait_ms = 0;
    if (acquire_init_lock(shared_path, &lock_fd, &lock_mode, &lock_path, &wait_loops, &wait_ms) != 0) {
        fprintf(stderr, "acquire shared lock failed: %s\n", strerror(errno));
        free(bitmap);
        optbinlog_taglist_free(&tags);
        return -1;
    }

    int rc = -1;
    int fd = -1;
    void* base = NULL;
    OptbinlogTagDef* ordered = NULL;

    void* existing_base = NULL;
    size_t existing_size = 0;
    OptbinlogSharedTag* existing_header = NULL;
    if (open_existing_shared_internal(shared_path, schema_hash, 1, &existing_base, &existing_size, &existing_header) == 0) {
        (void)existing_header;
        optbinlog_shared_close(existing_base, existing_size);
        trace_event("open_existing_ok");
        rc = 0;
        goto out;
    }

    if (access(shared_path, F_OK) == 0) {
        trace_event("shared_mismatch_recreate");
        if (unlink(shared_path) != 0 && errno != ENOENT) {
            fprintf(stderr, "remove stale shared failed: %s\n", strerror(errno));
            goto out;
        }
    }

    trace_event("create_attempt");
    base = map_file(shared_path, total_size, 1, &fd);
    if (!base) {
        fprintf(stderr, "create shared file failed: %s\n", strerror(errno));
        goto out;
    }

    trace_event("create_success");
    memset(base, 0, total_size);

    OPTBINLOG_MALLOC_START = base;
    OPTBINLOG_MALLOC_OFFSET = 0;
    OPTBINLOG_SHAREDMEM = total_size;

    OptbinlogSharedTag* header = (OptbinlogSharedTag*)optbinlog_malloc(sizeof(OptbinlogSharedTag));
    if (!header) {
        goto fail_cleanup_created;
    }

    header->state = OPTBINLOG_INITIALIZING;
    memcpy(header->magic, OPTBINLOG_SHARED_MAGIC, 8);
    header->header_version = OPTBINLOG_SHARED_HEADER_VERSION;
    header->num_arrays = (unsigned int)num_arrays;
    header->tag_count = (unsigned int)tag_count;
    header->schema_hash = schema_hash;
    header->generation = realtime_ns();
    header->total_size = (uint32_t)total_size;
    header->init_wait_loops = wait_loops;
    header->init_wait_ms = wait_ms;

    OptbinlogBitmap* bitmap_out = (OptbinlogBitmap*)optbinlog_malloc((size_t)num_arrays * sizeof(OptbinlogBitmap));
    if (!bitmap_out) {
        goto fail_cleanup_created;
    }
    header->bitmap_offset = (int)((uint8_t*)bitmap_out - (uint8_t*)base);
    memcpy(bitmap_out, bitmap, (size_t)num_arrays * sizeof(OptbinlogBitmap));

    OptbinlogEventTag* eventtag_out = (OptbinlogEventTag*)optbinlog_malloc((size_t)tag_count * sizeof(OptbinlogEventTag));
    if (!eventtag_out) {
        goto fail_cleanup_created;
    }
    header->eventtag_offset = (int)((uint8_t*)eventtag_out - (uint8_t*)base);

    ordered = calloc(tags.len, sizeof(OptbinlogTagDef));
    if (!ordered) {
        fprintf(stderr, "OOM\n");
        goto fail_cleanup_created;
    }
    memcpy(ordered, tags.items, tags.len * sizeof(OptbinlogTagDef));
    qsort(ordered, tags.len, sizeof(OptbinlogTagDef), tag_cmp);

    int ele_cursor = OPTBINLOG_MALLOC_OFFSET;
    for (int i = 0; i < tag_count; i++) {
        const OptbinlogTagDef* t = &ordered[i];
        OptbinlogEventTag* out = &eventtag_out[i];
        out->tag_index = (unsigned int)t->tag_id;
        out->tag_ele_num = (unsigned int)t->ele_num;
        out->tag_ele_offset = ele_cursor;
        memset(out->tag_name, 0, sizeof(out->tag_name));
        strncpy(out->tag_name, t->name, sizeof(out->tag_name) - 1);

        for (int e = 0; e < t->ele_num; e++) {
            OptbinlogEventTagEle* eo = (OptbinlogEventTagEle*)((uint8_t*)base + ele_cursor);
            eo->type = (unsigned int)type_code_from_char(t->eles[e].type_char);
            eo->len = (unsigned int)(t->eles[e].bits / 8);
            memset(eo->name, 0, sizeof(eo->name));
            strncpy(eo->name, t->eles[e].name, sizeof(eo->name) - 1);
            ele_cursor += (int)sizeof(OptbinlogEventTagEle);
        }
    }

    header->state = OPTBINLOG_INITIALIZED;
    if (msync(base, total_size, MS_SYNC) != 0) {
        fprintf(stderr, "msync shared failed: %s\n", strerror(errno));
        goto fail_cleanup_created;
    }

    rc = 0;
    trace_event("init_done");
    goto out;

fail_cleanup_created:
    if (ordered) {
        free(ordered);
        ordered = NULL;
    }
    if (base) {
        munmap(base, total_size);
        base = NULL;
    }
    if (fd >= 0) {
        close(fd);
        fd = -1;
    }
    (void)unlink(shared_path);
    goto out;

out:
    if (ordered) {
        free(ordered);
    }
    if (base) {
        munmap(base, total_size);
    }
    if (fd >= 0) {
        close(fd);
    }
    release_init_lock(&lock_fd, lock_mode, &lock_path);
    free(bitmap);
    optbinlog_taglist_free(&tags);
    return rc;
}

OptbinlogEventTag* optbinlog_lookup_tag(void* base, OptbinlogSharedTag* header, int tag_id, int icnt) {
    if (tag_id < 0) return NULL;
    int arr = tag_id / OPTBINLOG_EVENT_TAG_ARRAY_LEN;
    int idx = tag_id % OPTBINLOG_EVENT_TAG_ARRAY_LEN;
    if (idx < 0 || idx >= OPTBINLOG_EVENT_TAG_ARRAY_LEN) return NULL;
    if (arr < 0 || arr >= (int)header->num_arrays) return NULL;

    OptbinlogBitmap* bitmap = (OptbinlogBitmap*)((uint8_t*)base + header->bitmap_offset);
    if (!optbinlog_bitmap_get(&bitmap[arr], idx)) return NULL;

    int arrayoffset = 0;
    for (int i = 0; i < arr; i++) {
        arrayoffset += bitmap_count_ones(&bitmap[i]);
    }

    int rank = bitmap_rank_inclusive(&bitmap[arr], idx);
    if (rank <= 0) return NULL;
    int slot = arrayoffset + rank - 1;
    if (slot < 0 || slot >= (int)header->tag_count) return NULL;

    OptbinlogEventTag* tags = (OptbinlogEventTag*)((uint8_t*)base + header->eventtag_offset);
    OptbinlogEventTag* tag = &tags[slot];
    if (tag->tag_index != (unsigned int)tag_id) return NULL;
    if (icnt != -1 && (int)tag->tag_ele_num != icnt) return NULL;
    return tag;
}
