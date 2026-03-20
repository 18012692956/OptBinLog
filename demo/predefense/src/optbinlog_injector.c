#include "optbinlog_binlog.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TAG_ALERT 2006
#define TAG_NOTE 2007

typedef enum {
    ACTION_FAULT = 0,
    ACTION_DIAG = 1,
    ACTION_RECOVER = 2,
} InjectAction;

static void usage(const char* prog) {
    fprintf(stderr,
            "Usage:\n"
            "  %s --shared <shared.bin> --log <run.bin> --action fault|diag|recover [--uptime-ms N] [--timestamp T]\n",
            prog);
}

static int parse_action(const char* raw, InjectAction* out) {
    if (!raw || !out) return -1;
    if (strcmp(raw, "fault") == 0) {
        *out = ACTION_FAULT;
        return 0;
    }
    if (strcmp(raw, "diag") == 0) {
        *out = ACTION_DIAG;
        return 0;
    }
    if (strcmp(raw, "recover") == 0) {
        *out = ACTION_RECOVER;
        return 0;
    }
    return -1;
}

int main(int argc, char** argv) {
    const char* shared_path = NULL;
    const char* log_path = NULL;
    const char* action_raw = NULL;
    InjectAction action = ACTION_FAULT;
    int64_t timestamp = -1;
    uint32_t uptime_ms = 0;

    OptbinlogRecord records[2];
    OptbinlogValue values[2][4];
    int rec_count = 0;

    int i = 0;
    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--shared") == 0 && i + 1 < argc) {
            shared_path = argv[++i];
        } else if (strcmp(argv[i], "--log") == 0 && i + 1 < argc) {
            log_path = argv[++i];
        } else if (strcmp(argv[i], "--action") == 0 && i + 1 < argc) {
            action_raw = argv[++i];
        } else if (strcmp(argv[i], "--uptime-ms") == 0 && i + 1 < argc) {
            uptime_ms = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--timestamp") == 0 && i + 1 < argc) {
            timestamp = (int64_t)strtoll(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "unknown arg: %s\n", argv[i]);
            usage(argv[0]);
            return 1;
        }
    }

    if (!shared_path || !log_path || !action_raw) {
        usage(argv[0]);
        return 1;
    }
    if (parse_action(action_raw, &action) != 0) {
        fprintf(stderr, "invalid action: %s\n", action_raw);
        return 1;
    }

    if (timestamp < 0) {
        timestamp = 1710000000 + (int64_t)(uptime_ms / 1000u);
    }

    memset(records, 0, sizeof(records));
    memset(values, 0, sizeof(values));

    if (action == ACTION_FAULT || action == ACTION_RECOVER) {
        records[rec_count].timestamp = timestamp;
        records[rec_count].tag_id = TAG_ALERT;
        records[rec_count].ele_count = 4;
        records[rec_count].values = values[rec_count];

        values[rec_count][0].kind = OPTBINLOG_VAL_U;
        values[rec_count][0].u = (action == ACTION_FAULT) ? 2u : 1u;
        values[rec_count][1].kind = OPTBINLOG_VAL_U;
        values[rec_count][1].u = (action == ACTION_FAULT) ? 9901u : 9902u;
        values[rec_count][2].kind = OPTBINLOG_VAL_U;
        values[rec_count][2].u = (action == ACTION_FAULT) ? 860u : 620u;
        values[rec_count][3].kind = OPTBINLOG_VAL_U;
        values[rec_count][3].u = uptime_ms;
        rec_count++;
    }

    records[rec_count].timestamp = timestamp;
    records[rec_count].tag_id = TAG_NOTE;
    records[rec_count].ele_count = 4;
    records[rec_count].values = values[rec_count];

    values[rec_count][0].kind = OPTBINLOG_VAL_U;
    values[rec_count][0].u = (action == ACTION_FAULT) ? 93u : ((action == ACTION_DIAG) ? 94u : 95u);
    values[rec_count][1].kind = OPTBINLOG_VAL_U;
    values[rec_count][1].u = (action == ACTION_FAULT) ? 9901u : ((action == ACTION_DIAG) ? 0u : 9902u);
    values[rec_count][2].kind = OPTBINLOG_VAL_U;
    values[rec_count][2].u = uptime_ms;
    values[rec_count][3].kind = OPTBINLOG_VAL_S;
    if (action == ACTION_FAULT) {
        values[rec_count][3].s = "TEMP_WARN_MANUAL";
    } else if (action == ACTION_DIAG) {
        values[rec_count][3].s = "DIAG_RETRY_FLOW_MANUAL";
    } else {
        values[rec_count][3].s = "TEMP_RECOVERED_MANUAL";
    }
    rec_count++;

    /* 追加写入：保证手动注入事件和运行中的流式事件可共存。 */
    (void)setenv("OPTBINLOG_BINLOG_APPEND", "1", 1);
    if (optbinlog_binlog_write(shared_path, log_path, records, (size_t)rec_count) != 0) {
        fprintf(stderr, "inject write failed\n");
        (void)unsetenv("OPTBINLOG_BINLOG_APPEND");
        return 1;
    }
    (void)unsetenv("OPTBINLOG_BINLOG_APPEND");

    printf("inject_ok,action,%s,records,%d,uptime_ms,%u,timestamp,%lld\n",
           action_raw,
           rec_count,
           uptime_ms,
           (long long)timestamp);
    return 0;
}
