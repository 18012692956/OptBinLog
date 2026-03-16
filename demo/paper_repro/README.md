# 论文复现实验入口（精简）

本目录只保留论文复现实验需要的脚本入口与 JSON 配置。

## scripts

- `run_grouped_semantic_matrix.py`：工程近似层矩阵实验
- `run_final_aligned_suite.py`：严格语义对齐主实验（single / multi / L1）
- `run_space_crossover_scan.py`：日志量扫描与空间交叉点
- `run_init_race.py`：本地多进程初始化竞争
- `run_l1_init_compete.py`：节点级初始化竞争
- `run_l1_suite.py`：L1 多节点执行框架
- `run_bench.py`：单节点基准统计
- `run_multi_bench.py`：本地多设备基准统计
- `build_optbinlog_read.sh`：编译 `optbinlog_read` 便捷读取工具
- `read_optbinlog_sample.sh`：读取论文样例二进制日志（含字段名与统计）
- `../optbinlog_read.c`：Optbinlog 便捷读取工具源码（字段名解析 + JSONL 输出 + 汇总统计）

## configs

- `l1_config.linux_10_all_unaligned_initrace.json`：L1 模板配置（脚本会按节点规模自动扩展）

## 典型执行顺序

1. 运行工程近似层：`python3 demo/paper_repro/scripts/run_grouped_semantic_matrix.py`
2. 运行严格语义对齐主实验：`python3 demo/paper_repro/scripts/run_final_aligned_suite.py`
3. 运行空间交叉点：`python3 demo/paper_repro/scripts/run_space_crossover_scan.py`
4. 运行初始化竞争：`python3 demo/paper_repro/scripts/run_init_race.py` 与 `python3 demo/paper_repro/scripts/run_l1_init_compete.py`

## Optbinlog 便捷读取

在 `demo/` 目录编译：

```bash
clang -O2 -Wall -Wextra -std=c11 -Iinclude \
  -o optbinlog_read optbinlog_read.c \
  src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c
```

读取样例（已内置在 `paper_dataset/samples/optbinlog_read`）：

```bash
./optbinlog_read \
  --shared ./results/paper_dataset/samples/optbinlog_read/shared_eventtag.bin \
  --log ./results/paper_dataset/samples/optbinlog_read/binary_run_000.bin \
  --format table --limit 20 --summary
```
