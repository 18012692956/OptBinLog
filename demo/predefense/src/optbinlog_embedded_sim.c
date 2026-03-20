#include "optbinlog_binlog.h"
#include "optbinlog_shared.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TAG_BOOT 2001
#define TAG_SENSOR 2002
#define TAG_CONTROL 2003
#define TAG_NET 2004
#define TAG_POWER 2005
#define TAG_ALERT 2006
#define TAG_NOTE 2007

#define MAX_RECORDS 8192

typedef enum {
    PROFILE_NORMAL = 0,
    PROFILE_STRESS = 1,
} SimProfile;

typedef struct {
    OptbinlogValue values[4];
} ValueSlot;

typedef struct {
    OptbinlogRecord records[MAX_RECORDS];
    ValueSlot slots[MAX_RECORDS];
    size_t count;
    int64_t ts_base;
    uint32_t uptime_ms;
    uint64_t rnd;
} SimBuffer;

typedef struct {
    int cycles;
    int fault_at_cycle;
    int recover_at_cycle;
    SimProfile profile;
    int stream_mode;
    int interval_ms;
} SimPlan;

static void usage(const char* prog) {
    fprintf(stderr,
            "Usage:\n"
            "  %s --eventlog-dir <dir> --shared <shared.bin> --log <run.bin>\n"
            "     [--cycles N] [--profile normal|stress] [--fault-at-cycle N] [--recover-at-cycle N]\n"
            "     [--seed N] [--stream] [--interval-ms N] [--strict-perm]\n",
            prog);
}

static uint64_t xorshift64(uint64_t* state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static int sim_push_u4(SimBuffer* b, int tag_id, uint64_t a, uint64_t c, uint64_t d, uint64_t e) {
    OptbinlogRecord* rec = NULL;
    if (!b || b->count >= MAX_RECORDS) return -1;

    rec = &b->records[b->count];
    rec->timestamp = b->ts_base + (int64_t)(b->uptime_ms / 1000u);
    rec->tag_id = tag_id;
    rec->ele_count = 4;
    rec->values = b->slots[b->count].values;
    rec->values[0].kind = OPTBINLOG_VAL_U;
    rec->values[0].u = a;
    rec->values[1].kind = OPTBINLOG_VAL_U;
    rec->values[1].u = c;
    rec->values[2].kind = OPTBINLOG_VAL_U;
    rec->values[2].u = d;
    rec->values[3].kind = OPTBINLOG_VAL_U;
    rec->values[3].u = e;
    b->count++;
    return 0;
}

static int sim_push_note(SimBuffer* b, uint64_t code, uint64_t reserved, const char* msg) {
    OptbinlogRecord* rec = NULL;
    if (!b || b->count >= MAX_RECORDS) return -1;

    rec = &b->records[b->count];
    rec->timestamp = b->ts_base + (int64_t)(b->uptime_ms / 1000u);
    rec->tag_id = TAG_NOTE;
    rec->ele_count = 4;
    rec->values = b->slots[b->count].values;
    rec->values[0].kind = OPTBINLOG_VAL_U;
    rec->values[0].u = code;
    rec->values[1].kind = OPTBINLOG_VAL_U;
    rec->values[1].u = reserved;
    rec->values[2].kind = OPTBINLOG_VAL_U;
    rec->values[2].u = b->uptime_ms;
    rec->values[3].kind = OPTBINLOG_VAL_S;
    rec->values[3].s = msg ? msg : "";
    b->count++;
    return 0;
}

static int append_boot_sequence(SimBuffer* b) {
    static const uint64_t stage_codes[] = {0, 100, 200, 300, 400, 500};
    static const uint32_t stage_tick_ms[] = {0, 120, 280, 460, 700, 980};
    static const uint16_t temp_x10[] = {268, 271, 279, 286, 292, 297};
    size_t i = 0;

    /* 固定启动阶段，用于稳定展示。 */
    for (i = 0; i < sizeof(stage_codes) / sizeof(stage_codes[0]); i++) {
        b->uptime_ms = stage_tick_ms[i];
        if (sim_push_u4(b, TAG_BOOT, i, stage_codes[i], temp_x10[i], b->uptime_ms) != 0) return -1;
    }
    b->uptime_ms += 40;
    if (sim_push_note(b, 1, 0, "BOOT_OK") != 0) return -1;
    return 0;
}

static int append_network_bringup(SimBuffer* b, SimProfile profile) {
    uint64_t rssi_start = (profile == PROFILE_STRESS) ? 66u : 72u;
    uint64_t rssi_mid = (profile == PROFILE_STRESS) ? 60u : 68u;
    uint64_t rssi_online = (profile == PROFILE_STRESS) ? 54u : 61u;
    uint64_t retry = (profile == PROFILE_STRESS) ? 2u : 1u;

    b->uptime_ms += 120;
    if (sim_push_u4(b, TAG_NET, 1, rssi_start, 0, b->uptime_ms) != 0) return -1;
    b->uptime_ms += 180;
    if (sim_push_u4(b, TAG_NET, 2, rssi_mid, 0, b->uptime_ms) != 0) return -1;
    b->uptime_ms += 150;
    if (sim_push_u4(b, TAG_NET, 2, rssi_online, retry, b->uptime_ms) != 0) return -1;
    b->uptime_ms += 120;
    if (sim_push_u4(b, TAG_NET, 3, rssi_online, retry, b->uptime_ms) != 0) return -1;
    b->uptime_ms += 50;
    if (sim_push_note(b, 2, 0, "NET_ONLINE") != 0) return -1;
    return 0;
}

static int append_runtime_cycle(SimBuffer* b, const SimPlan* plan, int cycle) {
    uint64_t jitter = xorshift64(&b->rnd) % 19u;
    uint64_t stress_mul = (plan->profile == PROFILE_STRESS) ? 2u : 1u;
    uint64_t soc = 97u - (uint64_t)(cycle / 2);
    uint64_t voltage_mv = 4020u - (uint64_t)(cycle * (2 + stress_mul));
    uint64_t current_ma = 170u + (uint64_t)((cycle % 5) * (7 + stress_mul * 3));
    uint64_t retry = (cycle >= plan->fault_at_cycle - 2 && cycle <= plan->recover_at_cycle) ? (1u + stress_mul) : 1u;
    uint64_t state = (cycle % 10 == 0) ? 2u : 3u;
    uint64_t rssi = 56u + (uint64_t)((cycle % 7) * 2) - stress_mul;
    uint64_t latency = 400u + (uint64_t)((cycle % 9) * (29 + stress_mul * 8)) + jitter;
    uint64_t pwm = 35u + (uint64_t)((cycle * 3) % 55);
    uint64_t sensor_a = 2250u + (uint64_t)((cycle * 17 + jitter) % 160);
    uint64_t sensor_b = 5040u + (uint64_t)((cycle * 13 + jitter) % 220);
    uint64_t sensor_c = 320u + (uint64_t)((cycle * 7 + jitter) % 70);

    if (soc < 20u) soc = 20u;

    b->uptime_ms += 80;
    if (sim_push_u4(b, TAG_SENSOR, 1, sensor_a, 0, b->uptime_ms) != 0) return -1;
    b->uptime_ms += 30;
    if (sim_push_u4(b, TAG_SENSOR, 2, sensor_b, 0, b->uptime_ms) != 0) return -1;
    b->uptime_ms += 30;
    if (sim_push_u4(b, TAG_SENSOR, 3, sensor_c, 0, b->uptime_ms) != 0) return -1;

    b->uptime_ms += 40;
    if (sim_push_u4(b, TAG_CONTROL, 1, latency, pwm, b->uptime_ms) != 0) return -1;

    if (cycle % 3 == 0) {
        b->uptime_ms += 20;
        if (sim_push_u4(b, TAG_POWER, soc, voltage_mv, current_ma, b->uptime_ms) != 0) return -1;
    }

    if (cycle % 5 == 0) {
        b->uptime_ms += 25;
        if (sim_push_u4(b, TAG_NET, state, rssi, retry, b->uptime_ms) != 0) return -1;
    }

    /* 故障注入点：生成告警并写 NOTE，便于现场解释“异常发生”。 */
    if (cycle == plan->fault_at_cycle) {
        b->uptime_ms += 15;
        if (sim_push_u4(b, TAG_ALERT, 2, 9001, 780 + stress_mul * 45, b->uptime_ms) != 0) return -1;
        b->uptime_ms += 10;
        if (sim_push_note(b, 3, 9001, "TEMP_WARN") != 0) return -1;
    }
    if (cycle == plan->fault_at_cycle + 6) {
        b->uptime_ms += 15;
        if (sim_push_note(b, 4, 0, "DIAG_RETRY_FLOW") != 0) return -1;
    }

    /* 恢复点：生成恢复事件，形成“异常 -> 解决”闭环。 */
    if (cycle == plan->recover_at_cycle) {
        b->uptime_ms += 15;
        if (sim_push_u4(b, TAG_ALERT, 1, 9002, 610, b->uptime_ms) != 0) return -1;
        b->uptime_ms += 10;
        if (sim_push_note(b, 5, 9002, "TEMP_RECOVERED") != 0) return -1;
    }
    return 0;
}

static int read_stream_control(const char* path, int* pause_flag, long long* step_token, int* interval_ms) {
    FILE* fp = NULL;
    char line[128];

    if (!pause_flag || !step_token || !interval_ms) return -1;
    *pause_flag = 0;
    *step_token = 0;
    *interval_ms = -1;
    if (!path || !path[0]) return 0;

    fp = fopen(path, "r");
    if (!fp) {
        if (errno == ENOENT) return 0;
        return -1;
    }
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "pause=", 6) == 0) {
            *pause_flag = atoi(line + 6) ? 1 : 0;
        } else if (strncmp(line, "step_token=", 11) == 0) {
            *step_token = atoll(line + 11);
        } else if (strncmp(line, "interval_ms=", 12) == 0) {
            *interval_ms = atoi(line + 12);
        }
    }
    fclose(fp);
    return 0;
}

static int write_records_stream(const char* shared_path,
                                const char* log_path,
                                SimBuffer* b,
                                int interval_ms) {
    const char* control_path = getenv("OPTBINLOG_STREAM_CONTROL_FILE");
    size_t idx = 0;
    long long last_step_token = 0;
    int sleep_ms = interval_ms;

    (void)unlink(log_path);
    /* 开启 append 让 binlog 按帧持续增长，供实时监控端读取。 */
    (void)setenv("OPTBINLOG_BINLOG_APPEND", "1", 1);
    for (idx = 0; idx < b->count; idx++) {
        while (control_path && control_path[0]) {
            int pause_flag = 0;
            long long step_token = 0;
            int interval_override = -1;
            (void)read_stream_control(control_path, &pause_flag, &step_token, &interval_override);
            if (interval_override >= 0) sleep_ms = interval_override;
            if (!pause_flag) break;
            if (step_token > last_step_token) {
                last_step_token = step_token;
                break;
            }
            usleep(20u * 1000u);
        }

        OptbinlogRecord* rec = &b->records[idx];
        if (optbinlog_binlog_write(shared_path, log_path, rec, 1) != 0) {
            fprintf(stderr, "binlog stream write failed at idx=%zu\n", idx);
            (void)unsetenv("OPTBINLOG_BINLOG_APPEND");
            return -1;
        }
        printf("stream_event,index,%zu,tag,%d,timestamp,%lld\n",
               idx + 1,
               rec->tag_id,
               (long long)rec->timestamp);
        fflush(stdout);
        if (sleep_ms > 0) {
            usleep((useconds_t)sleep_ms * 1000u);
        }
    }
    (void)unsetenv("OPTBINLOG_BINLOG_APPEND");
    return 0;
}

int main(int argc, char** argv) {
    const char* eventlog_dir = NULL;
    const char* shared_path = NULL;
    const char* log_path = NULL;
    int strict_perm = 0;
    int seed = 20260320;
    SimPlan plan;
    SimBuffer b;
    int i = 0;

    memset(&b, 0, sizeof(b));
    b.ts_base = 1710000000;
    plan.cycles = 26;
    plan.fault_at_cycle = 12;
    plan.recover_at_cycle = 22;
    plan.profile = PROFILE_NORMAL;
    plan.stream_mode = 0;
    plan.interval_ms = 180;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--eventlog-dir") == 0 && i + 1 < argc) {
            eventlog_dir = argv[++i];
        } else if (strcmp(argv[i], "--shared") == 0 && i + 1 < argc) {
            shared_path = argv[++i];
        } else if (strcmp(argv[i], "--log") == 0 && i + 1 < argc) {
            log_path = argv[++i];
        } else if (strcmp(argv[i], "--cycles") == 0 && i + 1 < argc) {
            plan.cycles = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--fault-at-cycle") == 0 && i + 1 < argc) {
            plan.fault_at_cycle = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--recover-at-cycle") == 0 && i + 1 < argc) {
            plan.recover_at_cycle = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--profile") == 0 && i + 1 < argc) {
            const char* p = argv[++i];
            if (strcmp(p, "stress") == 0) {
                plan.profile = PROFILE_STRESS;
            } else {
                plan.profile = PROFILE_NORMAL;
            }
        } else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc) {
            seed = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--stream") == 0) {
            plan.stream_mode = 1;
        } else if (strcmp(argv[i], "--interval-ms") == 0 && i + 1 < argc) {
            plan.interval_ms = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--strict-perm") == 0) {
            strict_perm = 1;
        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "unknown arg: %s\n", argv[i]);
            usage(argv[0]);
            return 1;
        }
    }

    if (!eventlog_dir || !shared_path || !log_path) {
        usage(argv[0]);
        return 1;
    }

    if (plan.cycles < 4) plan.cycles = 4;
    if (plan.cycles > 500) plan.cycles = 500;
    if (plan.fault_at_cycle < 2) plan.fault_at_cycle = 2;
    if (plan.fault_at_cycle > plan.cycles - 1) plan.fault_at_cycle = plan.cycles / 2;
    if (plan.recover_at_cycle <= plan.fault_at_cycle) plan.recover_at_cycle = plan.fault_at_cycle + 4;
    if (plan.recover_at_cycle > plan.cycles) plan.recover_at_cycle = plan.cycles;
    if (plan.interval_ms < 0) plan.interval_ms = 0;
    b.rnd = (uint64_t)(unsigned int)seed;

    if (append_boot_sequence(&b) != 0) {
        fprintf(stderr, "failed to build boot sequence\n");
        return 1;
    }
    if (append_network_bringup(&b, plan.profile) != 0) {
        fprintf(stderr, "failed to build network sequence\n");
        return 1;
    }

    for (i = 1; i <= plan.cycles; i++) {
        if (append_runtime_cycle(&b, &plan, i) != 0) {
            fprintf(stderr, "failed to build runtime sequence\n");
            return 1;
        }
    }

    b.uptime_ms += 60;
    if (sim_push_note(&b, 6, 0, "MISSION_DONE") != 0) {
        fprintf(stderr, "failed to append end note\n");
        return 1;
    }

    if (optbinlog_shared_init_from_dir(eventlog_dir, shared_path, strict_perm) != 0) {
        fprintf(stderr, "shared init failed\n");
        return 1;
    }

    if (plan.stream_mode) {
        if (write_records_stream(shared_path, log_path, &b, plan.interval_ms) != 0) {
            return 1;
        }
    } else {
        if (optbinlog_binlog_write(shared_path, log_path, b.records, b.count) != 0) {
            fprintf(stderr, "binlog write failed\n");
            return 1;
        }
    }

    printf("generated_records,%zu,cycles,%d,uptime_ms,%u,fault_cycle,%d,recover_cycle,%d,profile,%s,stream,%d\n",
           b.count,
           plan.cycles,
           b.uptime_ms,
           plan.fault_at_cycle,
           plan.recover_at_cycle,
           plan.profile == PROFILE_STRESS ? "stress" : "normal",
           plan.stream_mode);
    return 0;
}
