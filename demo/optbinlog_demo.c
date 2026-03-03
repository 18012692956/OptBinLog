#include "optbinlog_shared.h"
#include "optbinlog_binlog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int print_record(const OptbinlogRecord* rec, void* user) {
    (void)user;
    printf("{timestamp:%lld, tag_id:%d, values:[", (long long)rec->timestamp, rec->tag_id);
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

static void usage(const char* prog) {
    fprintf(stderr,
        "Usage:\n"
        "  %s init --eventlog-dir <dir> [--shared <file>] [--strict-perm]\n"
        "  %s write --shared <file> --log <file>\n"
        "  %s read --shared <file> --log <file>\n"
        "  %s demo --eventlog-dir <dir> [--shared <file>] --log <file> [--strict-perm]\n",
        prog, prog, prog, prog
    );
}

int main(int argc, char** argv) {
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    const char* cmd = argv[1];
    const char* eventlog_dir = NULL;
    const char* shared_path = OPTBINLOG_EVENTTAG_FILENAME;
    const char* log_path = NULL;
    int strict_perm = 0;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--eventlog-dir") == 0 && i + 1 < argc) {
            eventlog_dir = argv[++i];
        } else if (strcmp(argv[i], "--shared") == 0 && i + 1 < argc) {
            shared_path = argv[++i];
        } else if (strcmp(argv[i], "--log") == 0 && i + 1 < argc) {
            log_path = argv[++i];
        } else if (strcmp(argv[i], "--strict-perm") == 0) {
            strict_perm = 1;
        }
    }

    if (strcmp(cmd, "init") == 0) {
        if (!eventlog_dir) {
            usage(argv[0]);
            return 1;
        }
        return optbinlog_shared_init_from_dir(eventlog_dir, shared_path, strict_perm) == 0 ? 0 : 1;
    }

    if (strcmp(cmd, "write") == 0) {
        if (!shared_path || !log_path) {
            usage(argv[0]);
            return 1;
        }
        static OptbinlogValue e1_vals[] = {{OPTBINLOG_VAL_U, 3, 0.0, NULL}};
        static OptbinlogValue e2_vals[] = {{OPTBINLOG_VAL_U, 1, 0.0, NULL}, {OPTBINLOG_VAL_D, 0, 12.5, NULL}, {OPTBINLOG_VAL_U, 7, 0.0, NULL}};
        static OptbinlogValue e3_vals[] = {{OPTBINLOG_VAL_U, 1, 0.0, NULL}, {OPTBINLOG_VAL_U, 2, 0.0, NULL}, {OPTBINLOG_VAL_D, 0, 3.25, NULL}, {OPTBINLOG_VAL_U, 99, 0.0, NULL}};
        static OptbinlogValue e4_vals[] = {{OPTBINLOG_VAL_U, 1, 0.0, NULL}, {OPTBINLOG_VAL_S, 0, 0.0, "tag-abc"}};

        OptbinlogRecord records[] = {
            {1710000000, 2724, 1, e1_vals},
            {1710000001, 2726, 3, e2_vals},
            {1710000002, 2728, 4, e3_vals},
            {1710000003, 2729, 2, e4_vals},
        };
        return optbinlog_binlog_write(shared_path, log_path, records, sizeof(records)/sizeof(records[0])) == 0 ? 0 : 1;
    }

    if (strcmp(cmd, "read") == 0) {
        if (!shared_path || !log_path) {
            usage(argv[0]);
            return 1;
        }
        return optbinlog_binlog_read(shared_path, log_path, print_record, NULL) == 0 ? 0 : 1;
    }

    if (strcmp(cmd, "demo") == 0) {
        if (!eventlog_dir || !log_path) {
            usage(argv[0]);
            return 1;
        }
        if (optbinlog_shared_init_from_dir(eventlog_dir, shared_path, strict_perm) != 0) return 1;

        static OptbinlogValue e1_vals[] = {{OPTBINLOG_VAL_U, 3, 0.0, NULL}};
        static OptbinlogValue e2_vals[] = {{OPTBINLOG_VAL_U, 1, 0.0, NULL}, {OPTBINLOG_VAL_D, 0, 12.5, NULL}, {OPTBINLOG_VAL_U, 7, 0.0, NULL}};
        static OptbinlogValue e3_vals[] = {{OPTBINLOG_VAL_U, 1, 0.0, NULL}, {OPTBINLOG_VAL_U, 2, 0.0, NULL}, {OPTBINLOG_VAL_D, 0, 3.25, NULL}, {OPTBINLOG_VAL_U, 99, 0.0, NULL}};
        static OptbinlogValue e4_vals[] = {{OPTBINLOG_VAL_U, 1, 0.0, NULL}, {OPTBINLOG_VAL_S, 0, 0.0, "tag-abc"}};

        OptbinlogRecord records[] = {
            {1710000000, 2724, 1, e1_vals},
            {1710000001, 2726, 3, e2_vals},
            {1710000002, 2728, 4, e3_vals},
            {1710000003, 2729, 2, e4_vals},
        };

        if (optbinlog_binlog_write(shared_path, log_path, records, sizeof(records)/sizeof(records[0])) != 0) return 1;
        return optbinlog_binlog_read(shared_path, log_path, print_record, NULL) == 0 ? 0 : 1;
    }

    usage(argv[0]);
    return 1;
}
