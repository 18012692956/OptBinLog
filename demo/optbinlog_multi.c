#include "optbinlog_shared.h"
#include "optbinlog_binlog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static void usage(const char* prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s --eventlog-dir <dir> --out-dir <dir> [--shared <file>] [--devices N] [--strict-perm] [--read]\n",
        prog
    );
}

static int ensure_dir(const char* path) {
    struct stat st;
    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) return 0;
        return -1;
    }
    if (mkdir(path, 0755) != 0) return -1;
    return 0;
}

static void build_records(int device_id, OptbinlogRecord* out, size_t* out_count) {
    static OptbinlogValue e1_vals[1];
    static OptbinlogValue e2_vals[3];
    static OptbinlogValue e3_vals[4];
    static OptbinlogValue e4_vals[2];

    e1_vals[0] = (OptbinlogValue){OPTBINLOG_VAL_U, (uint64_t)(device_id + 1), 0.0, NULL};

    e2_vals[0] = (OptbinlogValue){OPTBINLOG_VAL_U, (uint64_t)(device_id + 10), 0.0, NULL};
    e2_vals[1] = (OptbinlogValue){OPTBINLOG_VAL_D, 0, 10.0 + device_id, NULL};
    e2_vals[2] = (OptbinlogValue){OPTBINLOG_VAL_U, (uint64_t)(device_id + 20), 0.0, NULL};

    e3_vals[0] = (OptbinlogValue){OPTBINLOG_VAL_U, (uint64_t)(device_id + 30), 0.0, NULL};
    e3_vals[1] = (OptbinlogValue){OPTBINLOG_VAL_U, (uint64_t)(device_id + 40), 0.0, NULL};
    e3_vals[2] = (OptbinlogValue){OPTBINLOG_VAL_D, 0, 3.25 + device_id, NULL};
    e3_vals[3] = (OptbinlogValue){OPTBINLOG_VAL_U, (uint64_t)(device_id + 50), 0.0, NULL};

    char* tag_buf = malloc(16);
    if (tag_buf) {
        snprintf(tag_buf, 16, "dev-%02d", device_id);
    }
    e4_vals[0] = (OptbinlogValue){OPTBINLOG_VAL_U, (uint64_t)(device_id + 60), 0.0, NULL};
    e4_vals[1] = (OptbinlogValue){OPTBINLOG_VAL_S, 0, 0.0, tag_buf};

    out[0] = (OptbinlogRecord){1710000000 + device_id, 2724, 1, e1_vals};
    out[1] = (OptbinlogRecord){1710000001 + device_id, 2726, 3, e2_vals};
    out[2] = (OptbinlogRecord){1710000002 + device_id, 2728, 4, e3_vals};
    out[3] = (OptbinlogRecord){1710000003 + device_id, 2729, 2, e4_vals};

    *out_count = 4;
}

static void free_records(OptbinlogRecord* records, size_t count) {
    for (size_t i = 0; i < count; i++) {
        for (int e = 0; e < records[i].ele_count; e++) {
            if (records[i].values[e].kind == OPTBINLOG_VAL_S) {
                free((void*)records[i].values[e].s);
            }
        }
    }
}

static int print_record(const OptbinlogRecord* rec, void* user) {
    int device_id = *(int*)user;
    printf("[dev-%02d] {timestamp:%lld, tag_id:%d, values:[", device_id, (long long)rec->timestamp, rec->tag_id);
    for (int i = 0; i < rec->ele_count; i++) {
        if (i > 0) printf(", ");
        if (rec->values[i].kind == OPTBINLOG_VAL_U) {
            printf("%llu", (unsigned long long)rec->values[i].u);
        } else if (rec->values[i].kind == OPTBINLOG_VAL_D) {
            printf("%g", rec->values[i].d);
        } else if (rec->values[i].kind == OPTBINLOG_VAL_S) {
            printf("\"%s\"", rec->values[i].s ? rec->values[i].s : "");
        }
    }
    printf("]}\n");
    return 0;
}

int main(int argc, char** argv) {
    const char* eventlog_dir = NULL;
    const char* shared_path = OPTBINLOG_EVENTTAG_FILENAME;
    const char* out_dir = NULL;
    int devices = 10;
    int strict_perm = 0;
    int do_read = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--eventlog-dir") == 0 && i + 1 < argc) {
            eventlog_dir = argv[++i];
        } else if (strcmp(argv[i], "--shared") == 0 && i + 1 < argc) {
            shared_path = argv[++i];
        } else if (strcmp(argv[i], "--out-dir") == 0 && i + 1 < argc) {
            out_dir = argv[++i];
        } else if (strcmp(argv[i], "--devices") == 0 && i + 1 < argc) {
            devices = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--strict-perm") == 0) {
            strict_perm = 1;
        } else if (strcmp(argv[i], "--read") == 0) {
            do_read = 1;
        }
    }

    if (!eventlog_dir || !out_dir) {
        usage(argv[0]);
        return 1;
    }

    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "failed to create out dir %s: %s\n", out_dir, strerror(errno));
        return 1;
    }

    if (optbinlog_shared_init_from_dir(eventlog_dir, shared_path, strict_perm) != 0) {
        fprintf(stderr, "shared init failed\n");
        return 1;
    }

    for (int d = 0; d < devices; d++) {
        pid_t pid = fork();
        if (pid < 0) {
            fprintf(stderr, "fork failed: %s\n", strerror(errno));
            return 1;
        }
        if (pid == 0) {
            char log_path[512];
            snprintf(log_path, sizeof(log_path), "%s/device_%02d.bin", out_dir, d);

            OptbinlogRecord records[4];
            size_t count = 0;
            build_records(d, records, &count);
            int rc = optbinlog_binlog_write(shared_path, log_path, records, count);
            free_records(records, count);
            _exit(rc == 0 ? 0 : 2);
        }
    }

    int status = 0;
    for (int d = 0; d < devices; d++) {
        int st = 0;
        wait(&st);
        if (st != 0) status = 1;
    }

    if (status != 0) {
        fprintf(stderr, "one or more writers failed\n");
        return 1;
    }

    if (do_read) {
        for (int d = 0; d < devices; d++) {
            char log_path[512];
            snprintf(log_path, sizeof(log_path), "%s/device_%02d.bin", out_dir, d);
            int device_id = d;
            if (optbinlog_binlog_read(shared_path, log_path, print_record, &device_id) != 0) {
                fprintf(stderr, "read failed for %s\n", log_path);
                return 1;
            }
        }
    }

    return 0;
}
