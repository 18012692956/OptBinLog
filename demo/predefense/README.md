# Optbinlog Predefense 展示工程

本目录专用于预答辩展示，和论文实验脚本分离。

## 1. 目录分层

- `scripts/`：展示入口（全流程、实时展示、互动轮次、快速兜底）
- `tools/`：终端回放、实时网页看板、性能对比、鲁棒性验证
- `configs/`：场景参数（`normal` / `stress`）
- `src/`：嵌入式运行模拟器与手动注入器源码（C）
- `eventlog_embedded/`：嵌入式 schema（标签与字段定义）
- `docs/`：展示流程、详细手册、答辩代码片段
- `build/`：编译产物（自动生成）
- `results/`：所有展示输出证据（自动生成）

## 2. 推荐主命令（实时展示）

```bash
cd /Users/sky/Documents/graduation\ design/demo
bash predefense/scripts/run_live_showcase.sh --scenario normal --tag defense_live
```

默认会以较慢节奏流式输出（`--stream-interval-ms` 默认 180ms），便于讲解。

## 3. 你现在能在网页看板做什么

1. 运行中观察：阶段、状态、时间线、优化指标。
2. 逐步查看：切换到“逐步查看”，用“上一步/下一步”看每条事件。
3. 设备节奏控制：`暂停设备/继续设备/设备单步(1事件)`。
4. 手动注入：点击“注入异常 / 诊断重试 / 注入恢复”。
5. 双日志视图：同时看二进制十六进制和可读日志 tail。
6. 断电恢复闭环：点击“模拟断电截断”后，点“继续设备”会自动恢复后再继续；也可用“手动恢复(备用)”演示兜底路径。

## 4. 关键参数

- `--fault-at-cycle N`：自动异常注入周期
- `--recover-at-cycle N`：自动恢复周期
- `--stream-interval-ms N`：流式速度（越大越慢）
- `--live-host H`：看板地址（默认 `127.0.0.1`）
- `--live-port P`：看板端口（默认 `8765`）
- `--live-hold-seconds N`：模拟结束后看板额外保留时间

## 5. 其他命令

- `bash predefense/scripts/run_full_showcase.sh --scenario normal --mode auto`
- `bash predefense/scripts/run_full_showcase.sh --scenario normal --mode live`
- `bash predefense/scripts/run_quick_showcase.sh`

## 6. 输出目录

统一输出到：

- `predefense/results/<tag>/`
- `predefense/results/latest_full`（软链）

实时模式重点文件：

- `01_lifecycle/live_dashboard_url.txt`
- `01_lifecycle/live_state_snapshot.json`
- `01_lifecycle/sim_stdout.txt`
- `01_lifecycle/decoded_runtime.jsonl`
- `01_lifecycle/decoded_runtime_table.txt`

## 7. 文档入口

- 详细操作手册：`docs/OPERATION_MANUAL.md`
- 预答辩流程：`docs/PRESENTATION_FLOW.md`
- 追问代码片段：`docs/CODE_SNIPPETS.md`
