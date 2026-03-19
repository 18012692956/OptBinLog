# Experiments Layout

本目录按照“源码 / 运行脚本 / 实验结果”分层：

- `scripts/`: 论文实验脚本入口
- `configs/`: 实验 JSON 配置
- `src/`, `include/`: 核心实现代码
- `build/bin/`: 编译产物（二进制可执行文件）
- `eventlogst*/`: 语义对齐事件格式输入
- `results/`: 统一实验输出目录（按标签归档）
- `predefense/`: 预答辩专用目录（脚本、手册、独立 build、独立结果）

## 预答辩展示入口（独立于实验）

- `predefense/run_demo.sh`：预答辩一键演示（正确性、可读回放、空间/写入对比）
- `predefense/README.md`：预答辩现场运行手册

## 保留的实验脚本

- `scripts/run_grouped_semantic_matrix.py`：工程近似层矩阵实验
- `scripts/run_final_aligned_suite.py`：主实验（single / multi / L1）
- `scripts/run_space_crossover_scan.py`：日志量扫描与空间交叉点
- `scripts/run_init_race.py`：本地初始化竞争
- `scripts/run_l1_init_compete.py`：多节点初始化竞争
- `scripts/run_l1_suite.py`：L1 多节点执行框架
- `scripts/run_bench.py`：单节点统计
- `scripts/run_multi_bench.py`：本地多设备统计
- `scripts/final_aligned_merge_l1.py`：L1 汇总合并

## 推荐执行顺序

```bash
cd demo
python3 scripts/run_grouped_semantic_matrix.py
python3 scripts/run_final_aligned_suite.py
python3 scripts/run_space_crossover_scan.py
python3 scripts/run_init_race.py
python3 scripts/run_l1_init_compete.py
```

## L1 独立运行

```bash
cd demo
python3 scripts/run_l1_suite.py --config configs/l1_config.linux_10_all_unaligned_initrace.json
```

详见：`README_L1.md`。
