# Experiments Layout

本目录按照“源码 / 运行脚本 / 实验结果”分层：

- `src/`, `include/`: 核心实现代码
- `eventlogst/`: 事件格式输入与 `syslog` 原始文本样本
- `run_*.py`: 基准驱动与可视化脚本
- `results/`: 统一实验输出目录（按时间戳归档）

## One-Click Suite

推荐使用：

```bash
cd demo
python3 run_thesis_suite.py
```

会执行：

1. 单机高负载（本地 + Linux）
2. 多设备模拟（本地 + Linux）
3. 初始化竞争（仅本地）

并生成：

- `results/<tag>/suite_summary.json`
- `results/<tag>/suite_report.md`
- `results/<tag>/key_svgs/*.svg`
- `results/latest`（软链接到最新一次结果）

## Main Visual Outputs

- 单机双平台相对提升图：`results/latest/single_highload/bench_dual_relative.svg`
- 多设备双平台热力图：`results/latest/multi_device/bench_multi_dual_relative.svg`
- 初始化竞争时序图：`results/latest/init_race/init_race_result.svg`
