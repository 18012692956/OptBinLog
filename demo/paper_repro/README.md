# 论文复现辅助目录

为避免脚本重复维护，论文主实验入口统一保留在 `demo/scripts/`：

- `run_grouped_semantic_matrix.py`
- `run_final_aligned_suite.py`
- `run_space_crossover_scan.py`
- `run_init_race.py`
- `run_l1_init_compete.py`
- `run_l1_suite.py`

本目录仅保留 Optbinlog 便捷读取辅助脚本。

## scripts

- `build_optbinlog_read.sh`：编译 `optbinlog_read`
- `read_optbinlog_sample.sh`：读取论文样例二进制日志（含字段名与统计）

## Optbinlog 便捷读取

在 `demo/` 目录编译：

```bash
mkdir -p build/bin
clang -O2 -Wall -Wextra -std=c11 -Iinclude \
  -o build/bin/optbinlog_read optbinlog_read.c \
  src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c
```

读取样例（已内置在 `paper_dataset/samples/optbinlog_read`）：

```bash
./build/bin/optbinlog_read \
 --shared ./results/paper_dataset/samples/optbinlog_read/shared_eventtag.bin \
  --log ./results/paper_dataset/samples/optbinlog_read/binary_run_000.bin \
  --format table --limit 20 --summary
```
