# 预答辩展示流程（实时互动版，建议 10 分钟）

## 0) 开场前 30 秒

```bash
cd /Users/sky/Documents/graduation\ design/demo
bash predefense/scripts/run_live_showcase.sh --scenario normal --stream-interval-ms 180 --tag predefense_live_main
```

终端出现 `live_dashboard=http://127.0.0.1:8765/` 后打开该地址。

## 1) 00:00-00:50 目标

一句话：

> 不是“跑完后离线看结果”，而是运行中实时看日志、实时注入异常、实时看到恢复闭环。

## 2) 00:50-02:20 运行中流程（窗口主屏）

在看板展示：

1. 阶段（Boot -> Network -> Runtime）；
2. 状态（SOC/Latency/RSSI）实时刷新；
3. 时间线持续增长（证明是在线增量读取）。

## 3) 02:20-04:20 在线异常注入（窗口交互）

现场点击：

1. `注入异常` -> 进入 `active`；
2. `注入诊断重试` -> 进入 `recovering`；
3. `注入恢复` -> 进入 `resolved`。

同时说明：这些按钮是调用注入器向当前 binlog 追加事件，不是前端假数据。

## 4) 04:20-05:20 逐步查看（窗口交互）

先点 `暂停设备`，再切换时间线模式为 `逐步查看`，用 `上一步/下一步` 逐条解说：

1. 当前是第几步（`step=x/y`）；
2. 这条事件对应哪个状态变化；
3. 与异常处理路径如何对应。

如需推进后台设备本体，点击 `设备单步(1事件)`。
注意区分：`下一步` 只是看板光标前进，`设备单步` 才是设备真实前进。

## 5) 05:20-06:20 二进制 + 可读日志双视图联动

同页展示：

1. `shared.bin(head)` / `run.bin(tail)` 十六进制；
2. 二进制帧与可读摘要逐条映射；
3. 鼠标悬停任一二进制帧，右侧可读日志同步高亮；
4. 异常条目会显示故障/诊断/恢复标签，便于直观解释。

说明：现在可同时看底层数据与可读语义。

## 6) 06:20-07:20 优化效果（窗口 + 文件）

先看窗口实时指标：

1. binary 大小
2. 估算 text 大小
3. `space_ratio text/binary`

再补充文件证据：

- `03_advantage/benchmark_summary.md`
- `03_advantage/benchmark_chart.txt`

## 7) 07:20-08:20 异常检测鲁棒性

打开：

- `04_robustness/robustness_summary.md`

可补充现场动作：
- 在网页点“模拟断电截断”，观察时间线 `powercut` 标注 + 尾部十六进制变化；
- 再点“继续设备”，会自动出现 `repair` 标注（先恢复后继续）；如需讲解兜底方案，可再演示“手动恢复(备用)”。

说明：除了实时可视化，异常检测链路也有离线证据支撑。

## 8) 08:20-09:20 评委互动（改参数重跑）

```bash
bash predefense/scripts/run_live_showcase.sh \
  --scenario stress \
  --fault-at-cycle 8 \
  --recover-at-cycle 16 \
  --stream-interval-ms 220 \
  --tag predefense_live_interactive
```

## 9) 09:20-10:00 结论

1. 流程直观：运行中就能看到全过程。
2. 优势直观：实时指标 + benchmark 双证据。
3. 讲解清晰：逐步查看 + 可读日志 + 注入闭环。
