# 可能被问到的代码片段（实时互动版）

## 1) append 的作用（为什么要加）

文件：`src/optbinlog_binlog.c`

```c
int append_mode = env_flag_enabled("OPTBINLOG_BINLOG_APPEND");
FILE* fp = fopen(log_path, append_mode ? "ab" : "wb");
```

回答要点：

1. `wb` 会覆盖历史内容，不适合运行中连续观察；
2. `ab` 会持续追加，才支持“边运行边读取边可视化”；
3. 网页手动注入也依赖 append 才能无缝接入当前日志。

## 2) 流式写入（每条事件立刻可见）

文件：`predefense/src/optbinlog_embedded_sim.c`

```c
(void)setenv("OPTBINLOG_BINLOG_APPEND", "1", 1);
for (idx = 0; idx < b->count; idx++) {
    OptbinlogRecord* rec = &b->records[idx];
    if (optbinlog_binlog_write(shared_path, log_path, rec, 1) != 0) return -1;
    printf("stream_event,index,%zu,tag,%d,timestamp,%lld\n", idx + 1, rec->tag_id, (long long)rec->timestamp);
}
(void)unsetenv("OPTBINLOG_BINLOG_APPEND");
```

回答要点：不需要等待整轮结束，日志是逐条增量落盘的。

## 3) 网页异常注入（不是前端假事件）

文件：`predefense/tools/live_dashboard_server.py`

```python
cmd = [
    str(inject_bin),
    "--shared", self.args.shared,
    "--log", self.args.log,
    "--action", action,
    "--uptime-ms", str(uptime_ms),
]
cp = subprocess.run(cmd, text=True, capture_output=True, check=False)
```

回答要点：按钮会调用注入器向真实 binlog 写入新记录。

## 4) 逐步查看（step 模式）

文件：`predefense/tools/live_dashboard_server.py`（前端脚本）

```javascript
if (mode === 'step') {
  if (stepCursor < 0 || stepCursor >= full.length) stepCursor = full.length - 1;
  const rows = full.slice(Math.max(0, stepCursor - 24), stepCursor + 1);
}
```

回答要点：可以逐事件前后切换，适合答辩逐条讲解。

## 5) 设备级暂停/单步（解决“后台跑太快”）

文件：`predefense/src/optbinlog_embedded_sim.c`

```c
while (control_path && control_path[0]) {
    int pause_flag = 0;
    long long step_token = 0;
    int interval_override = -1;
    (void)read_stream_control(control_path, &pause_flag, &step_token, &interval_override);
    if (!pause_flag) break;
    if (step_token > last_step_token) {
        last_step_token = step_token;
        break; /* 只放行一个事件 */
    }
    usleep(20u * 1000u);
}
```

回答要点：这段是“设备本体节奏控制”，可真正暂停并单步推进事件。

## 6) 可读日志与二进制双视图

文件：`predefense/tools/live_dashboard_server.py`

```python
files["log_hex_tail"] = " ".join(f"{b:02x}" for b in tail)
self.decoded_jsonl_tail = "\n".join(tail_jsonl)
self.decoded_brief_tail = "\n".join(tail_brief)
```

回答要点：同一页面同时显示 hex 与可读日志，不再只看二进制。

## 7) live 模式接入全流程

文件：`predefense/scripts/run_full_showcase.py`

```python
if args.mode == "live":
    sim_line = run_live_stream_stage(...)
else:
    sim_cp = subprocess.run(sim_cmd, ...)
```

回答要点：实时展示和后续 benchmark/robustness 证据链是打通的。

## 8) 断电后坏尾恢复（展示链路）

文件：`predefense/tools/live_dashboard_server.py` + `src/optbinlog_binlog.c`

```python
# 1) 先在真实 run.bin 上截断尾部，制造“写入中断”现场
data = log_path.read_bytes()
log_path.write_bytes(data[:-drop_bytes])

# 2) 再调用 reader 的 --repair-tail 做坏尾修复
cmd = [self.args.read_bin, "--shared", self.args.shared, "--log", self.args.log,
       "--repair-tail", "--format", "table", "--limit", "1"]
cp = subprocess.run(cmd, text=True, capture_output=True, check=False)
```

回答要点：
1. 断电打断时，最后一帧可能只写了一半；
2. 恢复函数会保留“最后完整帧之前”的全部数据，裁剪损坏尾部；
3. 被打断的那条日志会丢失，但修复后可以继续 append，不会把后续日志一起拖垮。
