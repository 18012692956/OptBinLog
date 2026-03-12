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

如果希望 Linux 侧避免 9p 挂载影响（推荐），先把 `demo` 同步到 Linux VM 本地 ext4，再运行：

```bash
cd demo
OPTBINLOG_HYBRID_LINUX_WORKDIR=/home/sky.linux/optbinlog/demo \
OPTBINLOG_HYBRID_MULTI_LINUX_WORKDIR=/home/sky.linux/optbinlog/demo \
python3 run_thesis_suite.py
```

会执行：

1. 单机高负载（本地 + Linux）
2. 多设备模拟（本地 + Linux）
3. 初始化竞争（仅本地）
4. Binary 多设备竞争（仅本地）

并生成：

- `results/<tag>/suite_summary.json`
- `results/<tag>/suite_report.md`
- `results/<tag>/key_svgs/*.svg`
- `results/latest`（软链接到最新一次结果）

## Main Visual Outputs

- 单机双平台相对提升图：`results/latest/single_highload/bench_dual_relative.svg`
- 多设备双平台热力图：`results/latest/multi_device/bench_multi_dual_relative.svg`
- Binary 多设备竞争扫描图：`results/latest/binary_contention/bench_multi_scan.svg`
- 初始化竞争时序图：`results/latest/init_race/init_race_result.svg`

## Strict Fair Semantic Suite

用于“同 schema、同生成值、同平台”的严格公平对照与 `optbinlog` 消融：

```bash
cd demo
python3 run_fair_semantic_suite.py
```

当前套件中 `binary` 表示正式版 Optbinlog 默认实现：

- 进程内共享 `schema/tag cache`
- `per-record CRC32C`（优先硬件加速）
- 在 string-heavy schema 下自动启用 `varstr`

如需对照旧实现，可使用 `binary_crc32_legacy`；其语义为“软件 CRC32 + 固定字符串编码”。

生成：

- `results/fair_semantic_suite_<ts>/single/*`
- `results/fair_semantic_suite_<ts>/multi/*`
- `results/fair_semantic_suite_<ts>/merged/fair_semantic_suite_report.md`
- `results/fair_semantic_suite_<ts>/merged/ablation_phase_overview.svg`
- `results/fair_semantic_suite_<ts>/merged/single_pareto.svg`
- `results/fair_semantic_suite_<ts>/merged/multi_scan.svg`
- `results/fair_semantic_suite_latest`

## Final Aligned Suite

用于论文最终主结论：在同一语义口径下统一生成 single、本地 multi-device 和 10 节点 real multi-node 数据。

```bash
cd demo
python3 run_final_aligned_suite.py --skip-l1

zsh run_final_aligned_l1.sh \
  nanolog "$(pwd)/eventlogst_semantic_nanolog" nanolog_semantic_like \
  "$(pwd)/results/final_aligned_suite_<ts>/l1/nanolog"
zsh run_final_aligned_l1.sh \
  zephyr "$(pwd)/eventlogst_semantic_zephyr" zephyr_deferred_semantic_like \
  "$(pwd)/results/final_aligned_suite_<ts>/l1/zephyr"
zsh run_final_aligned_l1.sh \
  ulog "$(pwd)/eventlogst_semantic_ulog" ulog_semantic_like \
  "$(pwd)/results/final_aligned_suite_<ts>/l1/ulog"
zsh run_final_aligned_l1.sh \
  hilog "$(pwd)/eventlogst_semantic_hilog" hilog_semantic_like \
  "$(pwd)/results/final_aligned_suite_<ts>/l1/hilog"

python3 final_aligned_merge_l1.py \
  --summary-json "$(pwd)/results/final_aligned_suite_<ts>/merged/final_aligned_summary.json" \
  --l1-root "$(pwd)/results/final_aligned_suite_<ts>/l1"
```

生成：

- `results/final_aligned_suite_<ts>/merged/final_aligned_summary.json`
- `results/final_aligned_suite_<ts>/merged/final_aligned_report.md`
- `results/final_aligned_suite_<ts>/merged/single_aligned_overview.svg`
- `results/final_aligned_suite_<ts>/merged/single_binary_vs_peer.svg`
- `results/final_aligned_suite_<ts>/merged/multi_time_scan.svg`
- `results/final_aligned_suite_<ts>/merged/multi_throughput_scan.svg`
- `results/final_aligned_suite_<ts>/merged/multi_space_scan.svg`
- `results/final_aligned_suite_<ts>/merged/l1_aligned_overview.svg`
- `results/final_aligned_suite_<ts>/merged/l1_binary_vs_peer.svg`
- `results/final_aligned_suite_latest`

## L1 Distributed Suite

L1 用于“多节点 + 网络仿真”：

```bash
cd demo
python3 run_l1_suite.py --config l1_config.example.json
```

详见：`README_L1.md`。
