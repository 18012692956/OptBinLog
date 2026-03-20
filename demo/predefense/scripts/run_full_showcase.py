#!/usr/bin/env python3
import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from urllib.error import URLError
from urllib.request import urlopen


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run complete predefense showcase package (story + metrics + robustness)")
    p.add_argument("--scenario", choices=["normal", "stress"], default="normal")
    p.add_argument("--mode", choices=["auto", "step", "off", "live"], default="auto", help="playback mode")
    p.add_argument("--speed", type=float, default=-1.0, help="override playback speed")
    p.add_argument("--cycles", type=int, default=-1, help="override runtime cycles")
    p.add_argument("--fault-at-cycle", type=int, default=-1, help="override fault injection cycle")
    p.add_argument("--recover-at-cycle", type=int, default=-1, help="override recover cycle")
    p.add_argument("--benchmark-records", type=int, default=-1)
    p.add_argument("--benchmark-runs", type=int, default=-1)
    p.add_argument("--stream-interval-ms", type=int, default=180, help="stream mode event interval in milliseconds")
    p.add_argument("--live-host", default="127.0.0.1", help="live dashboard host")
    p.add_argument("--live-port", type=int, default=8765, help="live dashboard port")
    p.add_argument("--live-poll-interval", type=float, default=0.35, help="live dashboard decode polling seconds")
    p.add_argument("--live-hold-seconds", type=int, default=0, help="keep dashboard alive after sim ends")
    p.add_argument("--seed", type=int, default=20260320)
    p.add_argument("--tag", default="", help="output tag (default auto)")
    return p.parse_args()


def run(cmd: List[str], cwd: Path, capture: bool = False) -> subprocess.CompletedProcess:
    cp = subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=capture, check=False)
    if cp.returncode != 0:
        msg = f"command failed ({cp.returncode}): {' '.join(cmd)}"
        if capture:
            msg += f"\nstdout:\n{cp.stdout}\nstderr:\n{cp.stderr}"
        raise RuntimeError(msg)
    return cp


def choose_cc() -> str:
    env_cc = os.environ.get("CC", "").strip()
    if env_cc and shutil.which(env_cc):
        return env_cc
    if platform.system() == "Darwin" and shutil.which("clang"):
        return "clang"
    if shutil.which("gcc"):
        return "gcc"
    if shutil.which("clang"):
        return "clang"
    raise RuntimeError("no compiler found (need gcc or clang)")


def load_scenario(predefense_dir: Path, scenario: str) -> Dict[str, object]:
    cfg = predefense_dir / "configs" / f"scenario_{scenario}.json"
    with cfg.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_architecture_card(path: Path) -> None:
    text = """# 架构总览（用于开场 40 秒）

```mermaid
flowchart LR
    A["设备事件源\\n(boot/sensor/control/net/power/alert)"] --> B["Schema 映射\\n(eventlog + tag/field)"]
    B --> C["Optbinlog Writer\\n结构化二进制帧 + 校验"]
    C --> D["device_runtime.bin"]
    D --> E["Optbinlog Reader\\n字段级解码"]
    E --> F["可视化回放\\n状态仪表 + 时间线"]
    E --> G["性能对比\\ntext vs binary"]
    E --> H["鲁棒性验证\\nbad-tag / truncated"]
```

答辩讲法：
1. 左侧是设备运行时真实事件，不是离线拼接文本。
2. 中间用 schema 把事件语义固定下来，避免日志“字符串漂移”。
3. 右侧同时给出三类证据：流程可见、结果可读、优势可量化。
"""
    path.write_text(text, encoding="utf-8")


def write_presentation_script(path: Path) -> None:
    text = """# 10 分钟展示脚本（逐分钟）

1. 00:00-00:40 目标：强调“运行中实时可视化 + 实时注入 + 实时可读”。
2. 00:40-01:20 架构：打开 `00_brief/architecture_card.md`，讲 6 节点链路。
3. 01:20-03:20 主演示：看板中展示 Boot -> Runtime 连续过程。
4. 03:20-04:40 网页注入：依次点击 fault/diag/recover，展示闭环。
5. 04:40-05:40 逐步讲解：切 step 模式，用上一步/下一步逐条看事件。
6. 05:40-06:40 双日志视图：hex 与 decoded tail 同屏展示。
7. 06:40-08:10 优势：打开 `03_advantage/benchmark_summary.md` 与 `benchmark_chart.txt`。
8. 08:10-09:10 鲁棒性：打开 `04_robustness/robustness_summary.md`。
9. 09:10-10:00 总结：流程直观、优势直观、证据完整。
"""
    path.write_text(text, encoding="utf-8")


def write_faq(path: Path) -> None:
    text = """# 常见追问与回答要点

## Q1: 为什么不是直接打文本日志？
- 文本写入路径长、格式漂移风险高、后处理依赖正则。
- 当前方案直接写结构化字段，读写两端都可校验。

## Q2: append 是为了解决什么问题？
- 如果覆盖写，运行中无法持续读取同一份日志。
- append 让日志增量增长，网页和终端都能实时看到新增事件。

## Q3: 网页里的异常注入是真的吗？
- 不是前端模拟；按钮会调用注入器写入真实 binlog。
- 注入事件会出现在 decoded jsonl/table 和时间线上。

## Q4: 指标是否偶然？
- 采用重复运行，报告中用 median（中位数）汇总。
- 原始每次结果在 `03_advantage/benchmark_raw.csv`。

## Q5: 现场如何互动？
- 网页可直接注入 fault/diag/recover 并切 step 模式逐条讲解。
- 也可重跑修改参数：`--fault-at-cycle`、`--recover-at-cycle`、`--stream-interval-ms`。
"""
    path.write_text(text, encoding="utf-8")


def write_master_report(
    out_dir: Path,
    scenario_cfg: Dict[str, object],
    lifecycle_stdout: str,
    playback_summary: Dict[str, object],
    bench_summary: Dict[str, object],
    robustness_summary: Dict[str, object],
) -> None:
    report = out_dir / "00_master_report.md"
    lines: List[str] = []
    lines.append("# Predefense Master Report")
    lines.append("")
    lines.append(f"- generated_at: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")
    lines.append(f"- scenario: `{scenario_cfg.get('name')}`")
    lines.append(f"- lifecycle_stdout: `{lifecycle_stdout}`")
    lines.append("")
    lines.append("## 1) 流程可见（Lifecycle）")
    lines.append("")
    lines.append(f"- records: `{playback_summary.get('record_count', 0)}`")
    lines.append(f"- max_uptime_ms: `{playback_summary.get('max_uptime_ms', 0)}`")
    lines.append(f"- phase_count: `{len(playback_summary.get('phases', []))}`")
    lines.append("")
    lines.append("## 2) 优势量化（Median）")
    lines.append("")
    lines.append(f"- space ratio text/binary: `{bench_summary['ratios']['space_text_div_binary']:.2f}x`")
    lines.append(f"- write ratio text/binary: `{bench_summary['ratios']['write_time_text_div_binary']:.2f}x`")
    lines.append("")
    lines.append("## 3) 鲁棒性")
    lines.append("")
    lines.append(f"- valid_ok: `{robustness_summary.get('valid_ok')}`")
    lines.append(f"- bad_tag_detected: `{robustness_summary.get('bad_tag_detected')}`")
    lines.append(f"- truncated_detected: `{robustness_summary.get('truncated_detected')}`")
    lines.append("")
    lines.append("## 4) 展示顺序")
    lines.append("")
    lines.append("1. `00_brief/architecture_card.md`")
    lines.append("2. `01_lifecycle/live_dashboard_url.txt`（运行中实时看板）")
    lines.append("3. `02_playback/playback_snapshots.md` + 实时回放")
    lines.append("4. `01_lifecycle/decoded_runtime_table.txt`")
    lines.append("5. `03_advantage/benchmark_summary.md` + `benchmark_chart.txt`")
    lines.append("6. `04_robustness/robustness_summary.md`")
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")


def wait_dashboard_ready(url: str, timeout_sec: float) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with urlopen(f"{url}api/snapshot", timeout=0.8) as resp:
                if getattr(resp, "status", 200) == 200:
                    return True
        except (URLError, OSError):
            pass
        time.sleep(0.2)
    return False


def write_stream_control_file(path: Path, pause: int, step_token: int, interval_ms: int) -> None:
    content = (
        f"pause={1 if pause else 0}\n"
        f"step_token={max(0, int(step_token))}\n"
        f"interval_ms={max(0, int(interval_ms))}\n"
    )
    path.write_text(content, encoding="utf-8")


def run_live_stream_stage(
    root: Path,
    tools: Path,
    build_bin: Path,
    eventlog: Path,
    lifecycle_dir: Path,
    cycles: int,
    fault_cycle: int,
    recover_cycle: int,
    profile: str,
    args: argparse.Namespace,
) -> str:
    shared_path = lifecycle_dir / "shared_eventtag.bin"
    log_path = lifecycle_dir / "device_runtime.bin"
    sim_stdout_path = lifecycle_dir / "sim_stdout.txt"
    dashboard_out = lifecycle_dir / "live_dashboard_stdout.log"
    dashboard_err = lifecycle_dir / "live_dashboard_stderr.log"
    dashboard_state = lifecycle_dir / "live_state_snapshot.json"
    control_file = lifecycle_dir / "stream_control.txt"
    dashboard_url = f"http://{args.live_host}:{args.live_port}/"
    (lifecycle_dir / "live_dashboard_url.txt").write_text(dashboard_url + "\n", encoding="utf-8")
    write_stream_control_file(control_file, pause=0, step_token=0, interval_ms=max(0, args.stream_interval_ms))

    dashboard_cmd = [
        sys.executable,
        str(tools / "live_dashboard_server.py"),
        "--read-bin",
        str(build_bin / "optbinlog_read"),
        "--shared",
        str(shared_path),
        "--log",
        str(log_path),
        "--schema-source",
        str(eventlog / "embedded_tags.txt"),
        "--control-file",
        str(control_file),
        "--inject-bin",
        str(build_bin / "optbinlog_injector"),
        "--fault-at-cycle",
        str(fault_cycle),
        "--recover-at-cycle",
        str(recover_cycle),
        "--host",
        args.live_host,
        "--port",
        str(args.live_port),
        "--poll-interval",
        str(args.live_poll_interval),
        "--state-out",
        str(dashboard_state),
    ]

    sim_cmd = [
        str(build_bin / "optbinlog_embedded_sim"),
        "--eventlog-dir",
        str(eventlog),
        "--shared",
        str(shared_path),
        "--log",
        str(log_path),
        "--cycles",
        str(cycles),
        "--profile",
        profile,
        "--fault-at-cycle",
        str(fault_cycle),
        "--recover-at-cycle",
        str(recover_cycle),
        "--seed",
        str(args.seed),
        "--stream",
        "--interval-ms",
        str(max(0, args.stream_interval_ms)),
    ]

    sim_line = ""
    sim_env = os.environ.copy()
    sim_env["OPTBINLOG_STREAM_CONTROL_FILE"] = str(control_file)
    with dashboard_out.open("w", encoding="utf-8") as f_out, dashboard_err.open("w", encoding="utf-8") as f_err:
        dashboard = subprocess.Popen(
            dashboard_cmd,
            cwd=str(root),
            text=True,
            stdout=f_out,
            stderr=f_err,
        )
        try:
            ready = wait_dashboard_ready(dashboard_url, 8.0)
            if ready:
                print(f"      live_dashboard={dashboard_url}", flush=True)
                print("      tip: open this URL in browser for runtime visualization", flush=True)
            else:
                print(
                    "      warning: live dashboard not reachable, continue with terminal-only stream",
                    flush=True,
                )
                print(f"      see logs: {dashboard_out} / {dashboard_err}", flush=True)
            print("      terminal stream below shows incremental binlog writes", flush=True)

            with sim_stdout_path.open("w", encoding="utf-8") as sim_log:
                sim = subprocess.Popen(
                    sim_cmd,
                    cwd=str(root),
                    env=sim_env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    bufsize=1,
                )
                assert sim.stdout is not None
                for raw in sim.stdout:
                    line = raw.rstrip("\n")
                    sim_log.write(raw)
                    sim_log.flush()
                    if line.startswith("stream_event,"):
                        print(f"[stream] {line}", flush=True)
                    elif line.startswith("generated_records,"):
                        sim_line = line
                        print(f"[sim] {line}", flush=True)
                    elif line:
                        print(f"[sim] {line}", flush=True)
                rc = sim.wait()
                if rc != 0:
                    raise RuntimeError(f"simulator failed in live mode (rc={rc})")

            if args.live_hold_seconds > 0:
                print(
                    f"      stream finished; dashboard will stay for {args.live_hold_seconds}s",
                    flush=True,
                )
                time.sleep(args.live_hold_seconds)
        finally:
            if dashboard.poll() is None:
                dashboard.terminate()
                try:
                    dashboard.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    dashboard.kill()
                    dashboard.wait(timeout=2.0)
    return sim_line


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    predefense = root / "predefense"
    tools = predefense / "tools"
    src = predefense / "src"
    eventlog = predefense / "eventlog_embedded"
    build_bin = predefense / "build" / "bin"
    results_root = predefense / "results"

    scenario_cfg = load_scenario(predefense, args.scenario)
    cycles = args.cycles if args.cycles > 0 else int(scenario_cfg["cycles"])
    fault_cycle = args.fault_at_cycle if args.fault_at_cycle > 0 else int(scenario_cfg["fault_at_cycle"])
    recover_cycle = args.recover_at_cycle if args.recover_at_cycle > 0 else int(scenario_cfg["recover_at_cycle"])
    playback_speed = args.speed if args.speed > 0 else float(scenario_cfg["playback_speed"])
    benchmark_records = args.benchmark_records if args.benchmark_records > 0 else int(scenario_cfg["benchmark_records"])
    benchmark_runs = args.benchmark_runs if args.benchmark_runs > 0 else int(scenario_cfg["benchmark_runs"])
    profile = str(scenario_cfg["profile"])

    tag = args.tag.strip() or f"full_showcase_{args.scenario}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    out_dir = results_root / tag
    brief_dir = out_dir / "00_brief"
    lifecycle_dir = out_dir / "01_lifecycle"
    playback_dir = out_dir / "02_playback"
    advantage_dir = out_dir / "03_advantage"
    robustness_dir = out_dir / "04_robustness"
    talk_dir = out_dir / "05_talk"
    for d in [brief_dir, lifecycle_dir, playback_dir, advantage_dir, robustness_dir, talk_dir]:
        d.mkdir(parents=True, exist_ok=True)

    cc = choose_cc()
    common = ["-O2", "-Wall", "-Wextra", "-std=c11", "-Iinclude"]
    common_src = ["src/optbinlog_shared.c", "src/optbinlog_eventlog.c", "src/optbinlog_binlog.c"]
    build_bin.mkdir(parents=True, exist_ok=True)

    print(f"[1/8] build binaries ({cc})", flush=True)
    run(
        [cc, *common, "-o", str(build_bin / "optbinlog_embedded_sim"), str(src / "optbinlog_embedded_sim.c"), *common_src],
        cwd=root,
    )
    run(
        [cc, *common, "-o", str(build_bin / "optbinlog_injector"), str(src / "optbinlog_injector.c"), *common_src],
        cwd=root,
    )
    run([cc, *common, "-o", str(build_bin / "optbinlog_read"), "optbinlog_read.c", *common_src], cwd=root)
    run([cc, *common, "-o", str(build_bin / "optbinlog_bench"), "optbinlog_bench.c", *common_src], cwd=root)

    print("[2/8] write briefing docs", flush=True)
    write_architecture_card(brief_dir / "architecture_card.md")
    write_presentation_script(talk_dir / "presentation_script.md")
    write_faq(talk_dir / "faq_cheatsheet.md")

    print("[3/8] generate lifecycle binlog", flush=True)
    shared_path = lifecycle_dir / "shared_eventtag.bin"
    log_path = lifecycle_dir / "device_runtime.bin"
    sim_stdout_path = lifecycle_dir / "sim_stdout.txt"
    sim_line = ""
    if args.mode == "live":
        sim_line = run_live_stream_stage(
            root=root,
            tools=tools,
            build_bin=build_bin,
            eventlog=eventlog,
            lifecycle_dir=lifecycle_dir,
            cycles=cycles,
            fault_cycle=fault_cycle,
            recover_cycle=recover_cycle,
            profile=profile,
            args=args,
        )
    else:
        sim_cmd = [
            str(build_bin / "optbinlog_embedded_sim"),
            "--eventlog-dir",
            str(eventlog),
            "--shared",
            str(shared_path),
            "--log",
            str(log_path),
            "--cycles",
            str(cycles),
            "--profile",
            profile,
            "--fault-at-cycle",
            str(fault_cycle),
            "--recover-at-cycle",
            str(recover_cycle),
            "--seed",
            str(args.seed),
        ]
        sim_cp = subprocess.run(sim_cmd, cwd=str(root), capture_output=True, text=True, check=False)
        if sim_cp.returncode != 0:
            raise RuntimeError(f"simulator failed\n{sim_cp.stdout}\n{sim_cp.stderr}")
        sim_stdout_path.write_text((sim_cp.stdout or "") + (sim_cp.stderr or ""), encoding="utf-8")
        for line in (sim_cp.stdout or "").splitlines():
            if line.startswith("generated_records,"):
                sim_line = line.strip()
                break

    print("[4/8] decode lifecycle log", flush=True)
    decoded_jsonl = lifecycle_dir / "decoded_runtime.jsonl"
    decoded_table = lifecycle_dir / "decoded_runtime_table.txt"
    cp_json = run(
        [
            str(build_bin / "optbinlog_read"),
            "--shared",
            str(shared_path),
            "--log",
            str(log_path),
            "--format",
            "jsonl",
            "--limit",
            "0",
        ],
        cwd=root,
        capture=True,
    )
    decoded_jsonl.write_text(cp_json.stdout, encoding="utf-8")
    cp_table = run(
        [
            str(build_bin / "optbinlog_read"),
            "--shared",
            str(shared_path),
            "--log",
            str(log_path),
            "--format",
            "table",
            "--limit",
            "220",
            "--summary",
        ],
        cwd=root,
        capture=True,
    )
    decoded_table.write_text((cp_table.stdout or "") + (cp_table.stderr or ""), encoding="utf-8")

    playback_mode = "off" if args.mode == "live" else args.mode
    print(f"[5/8] playback dashboard ({playback_mode})", flush=True)
    playback_summary_path = playback_dir / "playback_summary.json"
    playback_snapshots_path = playback_dir / "playback_snapshots.md"
    run(
        [
            sys.executable,
            str(tools / "interactive_show.py"),
            "--jsonl",
            str(decoded_jsonl),
            "--mode",
            playback_mode,
            "--speed",
            str(playback_speed),
            "--summary-out",
            str(playback_summary_path),
            "--snapshot-out",
            str(playback_snapshots_path),
        ],
        cwd=root,
    )
    with playback_summary_path.open("r", encoding="utf-8") as f:
        playback_summary = json.load(f)

    print("[6/8] benchmark comparison (median)", flush=True)
    run(
        [
            sys.executable,
            str(tools / "benchmark_compare.py"),
            "--bench-bin",
            str(build_bin / "optbinlog_bench"),
            "--eventlog-dir",
            str(eventlog),
            "--out-dir",
            str(advantage_dir),
            "--records",
            str(benchmark_records),
            "--runs",
            str(benchmark_runs),
            "--shared-path",
            str(advantage_dir / "shared_eventtag.bin"),
        ],
        cwd=root,
    )
    with (advantage_dir / "benchmark_summary.json").open("r", encoding="utf-8") as f:
        bench_summary = json.load(f)

    print("[7/8] robustness check", flush=True)
    run(
        [
            sys.executable,
            str(tools / "robustness_check.py"),
            "--read-bin",
            str(build_bin / "optbinlog_read"),
            "--shared",
            str(shared_path),
            "--log",
            str(log_path),
            "--out-dir",
            str(robustness_dir),
        ],
        cwd=root,
    )
    with (robustness_dir / "robustness_summary.json").open("r", encoding="utf-8") as f:
        robustness_summary = json.load(f)

    print("[8/8] write master report", flush=True)
    write_master_report(out_dir, scenario_cfg, sim_line, playback_summary, bench_summary, robustness_summary)
    latest = results_root / "latest_full"
    if latest.exists() or latest.is_symlink():
        latest.unlink()
    latest.symlink_to(Path(tag))

    print("")
    print("full showcase completed")
    print(f"output_dir={out_dir}")
    print(f"latest_link={latest}")
    print(f"master_report={out_dir / '00_master_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
