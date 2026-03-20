# 操作手册（实时展示版）

## A. 展示目标

本手册对应你当前的展示要求：

1. 运行过程中随时读取日志；
2. 运行过程中随时查看共享日志格式文件；
3. 异常注入、异常处理、恢复全过程实时可见；
4. 终端 + 网页窗口并行展示；
5. 所有结果统一落在 `predefense/results/<tag>/`。

## B. 一键实时展示

```bash
cd /Users/sky/Documents/graduation\ design/demo
bash predefense/scripts/run_live_showcase.sh \
  --scenario normal \
  --fault-at-cycle 12 \
  --recover-at-cycle 22 \
  --stream-interval-ms 180 \
  --tag defense_live
```

说明：
- 默认 `stream-interval-ms=180`，相比之前更慢，更适合答辩讲解。
- 如果还想更慢，可改为 `220` 或 `260`。

## C. 运行中你会看到什么

1. 终端
- 持续输出 `stream_event,index,...`（增量写入证据）。
- 输出 `live_dashboard=http://127.0.0.1:8765/`（看板地址）。

2. 网页看板
- 实时阶段与状态（Boot/Network/Runtime/Fault/Recovery）。
- 优化指标（binary 大小、估算 text 大小、空间比、吞吐、解码耗时）。
- 时间线（自动跟随或逐步查看）+ 异常标注条（fault/diag/recover/powercut/repair）。
- 异常注入面板（按钮注入 fault/diag/recover）。
- 断电截断与尾部恢复面板（断电前后 tail 对比 + reader 检测结果）。
- 二进制与可读日志双视图联动（hover/click 同步高亮异常条目）。

## D. 如何在网页注入异常

看板的“异常注入与处理路径”区域提供三个按钮：

1. `注入异常`：写入 `alert_event WARN` + `note TEMP_WARN_MANUAL`
2. `注入诊断重试`：写入 `note DIAG_RETRY_FLOW_MANUAL`
3. `注入恢复`：写入 `alert_event INFO` + `note TEMP_RECOVERED_MANUAL`

这些按钮会调用后端注入器 `optbinlog_injector`，直接追加到当前 binlog，所以不是“假动画”。

## E. 异常处理表现是什么

注入后会同时看到：

1. 时间线新增对应事件；
2. “异常处理路径”状态变化：
- `active`（异常触发）
- `recovering`（诊断/重试）
- `resolved`（恢复完成）
3. 可读日志 tail 里出现对应 note/alert；
4. 二进制文件大小继续增长（说明事件真实写入）。

## F. 网页逐步查看怎么用

1. 在“时间线”卡片把模式切到 `逐步查看`；
2. 用 `上一步` / `下一步` 按钮按事件逐条走；
3. `step=x/y` 显示当前步号。

这适合你在答辩时逐帧解释“发生了什么”。

关键点：  
如果你希望“后台设备也停住”，要同时点击 `暂停设备`。  
之后可以按 `设备单步(1事件)`，每点一次只推进一条新事件，这样就不会来不及讲解。

两种“下一步”的区别：
- `下一步`：只移动网页时间线光标，不驱动设备。
- `设备单步(1事件)`：真正让设备写入下一条事件。

## G. 实时读日志（你要的“可读 + 二进制”）

网页中有两块：

1. 左侧是“所有二进制 frame 段”
- 每一行都展示 `frame#N bytes[a:b)` 与该条 frame 的完整十六进制片段。
- 鼠标悬停任意一行，会自动高亮该行并联动右侧对应可读日志。
- 异常帧会带标签和底色（故障/诊断/恢复/断电）。

2. 右侧是“单一格式解析日志”
- 每一行是对应 `frame#N` 的可读事件摘要（`uptime + tag + summary`）。
- 当鼠标停在左侧二进制行时，右侧对应行同步高亮；反过来也一样。
- 时间线中对应事件行也会显示异常徽标，便于三方对应（时间线 ↔ 二进制 ↔ 可读）。

3. 共享格式卡片（Schema）
- 先展示“共享二进制布局解析结果”（header/bitmap/eventtag）；
- 再展示“源格式文本”，顺序已按答辩讲解习惯调整。

## H. 断电写入丢失的展示与处理

现在网页提供完整闭环，建议按以下顺序演示：

1. 点击 `模拟断电截断`
- 会直接截断当前真实 `run.bin` 的尾部（不是副本）。
- 看板记录 `before/after/drop_bytes`，并展示 `tail_before / tail_dropped / tail_after`。
- 设备进入断电保持态（`powercut hold`）。
- 当你点击 `继续设备` 时，系统会先自动执行尾部恢复，再恢复运行。

2. 观察可视化结果
- 时间线标注条出现 `powercut` 徽标；
- 二进制/可读日志映射中，受影响尾部帧会出现异常高亮；
- `frame_parse_error` 会显示 `truncated ...` 或相关尾部错误；
- `断电恢复状态` 会显示当前 stage（`await_resume/undetected/auto_resume_repaired/repaired`）。

3. 如需手动演示，再点击 `手动恢复(备用)`
- 后端调用 reader 的 `--repair-tail`，执行真实坏尾修复；
- 看板实时更新 `repair_before/repair_after/repair_drop_bytes`；
- 时间线标注条新增 `repair` 徽标；
- 手动恢复是备用路径；默认展示路径是“继续设备时自动恢复”。

恢复语义说明（答辩建议原话）：
- 断电打断的最后一条日志可能只写了一半，这条记录会丢失；
- 修复会保留所有完整帧，裁剪损坏尾部；
- 修复后可以继续 append，不会破坏之前可用日志。

手动命令（和网页“执行尾部恢复”等价）：
```bash
cd /Users/sky/Documents/graduation\ design/demo
./predefense/build/bin/optbinlog_read \
  --shared predefense/results/defense_live/01_lifecycle/shared_eventtag.bin \
  --log predefense/results/defense_live/01_lifecycle/device_runtime.bin \
  --repair-tail --format jsonl --limit 1
```

看到 `tail repair applied: ... before -> after` 即表示已完成坏尾修复。

## I. 常见互动命令

1. 更慢播放：
```bash
bash predefense/scripts/run_live_showcase.sh --stream-interval-ms 260 --tag defense_slow
```

2. 压力场景：
```bash
bash predefense/scripts/run_live_showcase.sh --scenario stress --tag defense_stress
```

3. 手动互动脚本：
```bash
bash predefense/scripts/run_interactive_round.sh
```

4. 断电影响模拟（在网页中点“模拟断电截断”）
- 系统会直接截断当前运行日志尾部字节，模拟“写入中断电”。
- reader 预期会返回非 0，并提示 `truncated ...` 或校验失败。
- 点击“继续设备”会自动恢复后再继续运行，不需要手动恢复。

设备间隔说明：
- “设备间隔(ms)”控制设备写两条事件之间的等待时间；
- 值越大，设备推进越慢，越适合讲解；
- 修改后点“应用间隔”才会生效。

## J. 输出目录结构

`predefense/results/<tag>/`

1. `00_brief/`：开场架构材料
2. `01_lifecycle/`：运行日志、看板状态快照
3. `02_playback/`：离线回放统计
4. `03_advantage/`：优势对比指标
5. `04_robustness/`：鲁棒性验证
6. `05_talk/`：讲稿与 FAQ
7. `00_master_report.md`：总览导航

## K. 看板启动失败时

若环境限制端口（例如 `Operation not permitted`）：

1. 脚本会自动降级为终端流式展示并继续产出完整结果；
2. 可更换端口重试：`--live-port 8877`；
3. 在本机终端运行通常可正常打开看板。
