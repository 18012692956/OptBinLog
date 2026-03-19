# Optbinlog 预答辩演示手册（独立目录版）

预答辩展示文件已与实验文件分离，统一放在 `predefense/` 下：

- `predefense/run_demo.sh`：预答辩一键演示脚本
- `predefense/build/`：预答辩专用二进制
- `predefense/results/`：预答辩专用输出

## 你将展示什么

脚本会自动生成三部分证据：

1. 正确性与鲁棒性  
   `roundtrip_ok=1`，并验证“坏 tag / 截断日志”会被识别。
2. 可读回放  
   把二进制日志解码成带字段名的 table 和 JSONL。
3. 空间与写入对比  
   对比 `text_semantic_like` 与 `binary` 的 `total_bytes` 和 `write_only_ms`。

## 现场命令（推荐）

```bash
cd /Users/sky/Documents/graduation\ design/demo
bash predefense/run_demo.sh
cat predefense/results/latest/demo_report.md
cat predefense/results/latest/03_decode_showcase/decoded_bench_table.txt
```

## 参数说明

```bash
bash predefense/run_demo.sh --eventlog-dir eventlogst_semantic_min --records 50000 --tag predefense_custom
```

- `--eventlog-dir`：schema 目录（相对 `demo/` 或绝对路径）。
- `--records`：对比样本数量（越大越稳定，耗时也更长）。
- `--tag`：输出目录名，最终在 `predefense/results/<tag>/`。

## 输出目录说明

默认输出到：

- `predefense/results/predefense_<时间戳>/`
- `predefense/results/latest`（软链，指向最近一次）

关键文件：

1. `demo_report.md`：可直接投屏讲解的总览报告。
2. `01_roundtrip/roundtrip_stdout.txt`：正确性与异常检测输出。
3. `03_decode_showcase/decoded_bench_table.txt`：可读解码结果（建议现场展示）。
4. `03_decode_showcase/decoded_bench.jsonl`：可读解码结果（JSONL）。
5. `03_decode_showcase/decoded_roundtrip_table.txt`：roundtrip 样本解码结果。
6. `02_space_speed/*.csvline`：性能与空间原始数据。

## 快速兜底（时间紧）

```bash
cat predefense/results/latest/demo_report.md
cat predefense/results/latest/03_decode_showcase/decoded_bench_table.txt
```
