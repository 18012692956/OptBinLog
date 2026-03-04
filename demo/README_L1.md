# L1 多节点仿真（VM/容器 + 网络仿真）

`run_l1_suite.py` 用于 L1 级别实验：把“设备”提升为独立节点（local/ssh/prefix），并可在节点上注入 `tc netem` 网络条件，再汇总每节点基准结果。

## 目标

1. 多节点并发（不是单机 `fork`）
2. 网络条件注入（delay/jitter/loss/rate）
3. 节点结果统一回收（JSON + Markdown）

## 快速开始

```bash
cd demo
python3 run_l1_suite.py --config l1_config.example.json
```

输出：

1. `results/<tag>/l1_summary.json`
2. `results/<tag>/l1_report.md`
3. `results/<tag>/nodes/<node_name>/bench_out/*`
4. `results/l1_latest`（软链）

## 配置说明（JSON）

每个节点支持三种执行方式：

1. `transport=local`：本地直接执行
2. `transport=ssh`：通过 `ssh_target` 执行
3. `transport=prefix`：命令前缀执行（如 `["limactl","shell","thesis-linux","--"]`）

核心字段：

1. `name`：节点名称
2. `workdir`：节点上 `demo` 目录
3. `bench_script`：默认 `run_bench.py`
4. `bench_bin`：节点二进制路径（如 `./optbinlog_bench_linux`）
5. `records/repeats/warmup/modes/baseline`：与单机脚本一致
6. `netem`（可选）：
   - `iface` 必填
   - `delay_ms/jitter_ms/loss_pct/rate_mbit/limit` 可选
7. `sudo_prefix`（可选，默认 `sudo -n`）：用于 `tc netem` 注入/清理
8. `bench_prefix`（可选，默认空）：在运行 `run_bench.py` 前添加前缀（例如 `sudo -n`，用于 `ftrace` 需要 root 写 `trace_marker` 的场景）

## 注意事项

1. 节点需具备无交互 sudo（用于 `tc qdisc replace/del`）。
2. `ftrace` 模式需 `trace_marker` 可写（可通过 `trace_marker` 字段指定路径）。
3. 某些 Linux 内核默认 `tracing_on=0` 会导致 `trace_marker` 写入失败（`Bad file descriptor`）。可在节点上先执行：
   - `sudo -n sh -c 'echo 1 > /sys/kernel/tracing/tracing_on'`
   - 或在配置里通过 `build_cmd` 先开启（示例配置已包含）。
4. `ssh` 节点需可免密连接（`BatchMode=yes`）。
5. 脚本默认会在结束后删除节点端临时输出目录；若要保留，使用 `--keep-remote-out`。
