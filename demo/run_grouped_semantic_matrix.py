#!/usr/bin/env python3
import argparse
import copy
import datetime as dt
import json
import math
import os
import shlex
import shutil
import statistics
import subprocess
from typing import Dict, List, Tuple

ROOT = os.path.dirname(__file__)
RESULTS_ROOT = os.path.join(ROOT, "results")

LOCAL_GROUPS = [
    {"name": "syslog", "peer": "syslog", "eventlog_dir": "eventlogst_semantic_syslog", "platform": "local"},
    {"name": "nanolog", "peer": "nanolog_like", "eventlog_dir": "eventlogst_semantic_nanolog", "platform": "local"},
    {"name": "zephyr", "peer": "zephyr_like", "eventlog_dir": "eventlogst_semantic_zephyr", "platform": "local"},
    {"name": "ulog", "peer": "ulog_async_like", "eventlog_dir": "eventlogst_semantic_ulog", "platform": "local"},
    {"name": "hilog", "peer": "hilog_lite_like", "eventlog_dir": "eventlogst_semantic_hilog", "platform": "local"},
]

LINUX_GROUP = {"name": "ftrace", "peer": "ftrace", "eventlog_dir": "eventlogst_semantic_ftrace", "platform": "linux"}

L1_GROUPS = [
    {"name": "syslog", "peer": "syslog", "eventlog_dir": "eventlogst_semantic_syslog", "platform": "l1"},
    {"name": "ftrace", "peer": "ftrace", "eventlog_dir": "eventlogst_semantic_ftrace", "platform": "l1"},
    {"name": "nanolog", "peer": "nanolog_like", "eventlog_dir": "eventlogst_semantic_nanolog", "platform": "l1"},
    {"name": "zephyr", "peer": "zephyr_like", "eventlog_dir": "eventlogst_semantic_zephyr", "platform": "l1"},
    {"name": "ulog", "peer": "ulog_async_like", "eventlog_dir": "eventlogst_semantic_ulog", "platform": "l1"},
    {"name": "hilog", "peer": "hilog_lite_like", "eventlog_dir": "eventlogst_semantic_hilog", "platform": "l1"},
]


def run_cmd(cmd: List[str], cwd: str = ROOT, env: Dict[str, str] = None, text: bool = True) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, cwd=cwd, env=env, text=text, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed\ncmd: {}\nstdout:\n{}\nstderr:\n{}".format(
                " ".join(cmd),
                proc.stdout if text else "<binary>",
                proc.stderr if text else "<binary>",
            )
        )
    return proc


def run_linux_shell(
    instance: str,
    script: str,
    cwd: str = ROOT,
    text: bool = True,
    check: bool = True,
) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        ["limactl", "shell", instance, "--", "bash", "-lc", script],
        cwd=cwd,
        text=text,
        capture_output=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            "linux command failed\nscript: {}\nstdout:\n{}\nstderr:\n{}".format(
                script,
                proc.stdout if text else "<binary>",
                proc.stderr if text else "<binary>",
            )
        )
    return proc


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: dict) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    x = sorted(values)
    idx = (len(x) - 1) * (p / 100.0)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return float(x[lo])
    ratio = idx - lo
    return float(x[lo] + (x[hi] - x[lo]) * ratio)


def metric_stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {
            "n": 0,
            "mean": 0.0,
            "median": 0.0,
            "p95": 0.0,
            "std": 0.0,
            "ci95_low": 0.0,
            "ci95_high": 0.0,
            "min": 0.0,
            "max": 0.0,
        }
    n = len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values) if n > 1 else 0.0
    half_ci = 1.96 * std / math.sqrt(n) if n > 1 else 0.0
    return {
        "n": n,
        "mean": mean,
        "median": statistics.median(values),
        "p95": percentile(values, 95),
        "std": std,
        "ci95_low": mean - half_ci,
        "ci95_high": mean + half_ci,
        "min": min(values),
        "max": max(values),
    }


def iqr_filter(values: List[float], mult: float = 1.5) -> List[float]:
    if len(values) < 4:
        return list(values)
    q1 = percentile(values, 25)
    q3 = percentile(values, 75)
    iqr = q3 - q1
    lo = q1 - mult * iqr
    hi = q3 + mult * iqr
    kept = [v for v in values if lo <= v <= hi]
    return kept if kept else list(values)


def build_local_binaries() -> None:
    run_cmd(
        [
            "clang",
            "-O2",
            "-Wall",
            "-Wextra",
            "-std=c11",
            "-Iinclude",
            "-o",
            "optbinlog_bench_macos",
            "optbinlog_bench.c",
            "src/optbinlog_shared.c",
            "src/optbinlog_eventlog.c",
            "src/optbinlog_binlog.c",
        ],
        cwd=ROOT,
    )
    run_cmd(
        [
            "clang",
            "-O2",
            "-Wall",
            "-Wextra",
            "-std=c11",
            "-Iinclude",
            "-o",
            "optbinlog_multi_bench_macos",
            "optbinlog_multi_bench.c",
            "src/optbinlog_shared.c",
            "src/optbinlog_eventlog.c",
            "src/optbinlog_binlog.c",
        ],
        cwd=ROOT,
    )


def build_linux_binaries(instance: str) -> None:
    wd = shlex.quote(ROOT)
    script = (
        "set -euo pipefail; "
        f"cd {wd}; "
        "gcc -O2 -Wall -Wextra -std=c11 -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L "
        "-Iinclude -o optbinlog_bench_linux optbinlog_bench.c "
        "src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c; "
        "gcc -O2 -Wall -Wextra -std=c11 -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L "
        "-Iinclude -o optbinlog_multi_bench_linux optbinlog_multi_bench.c "
        "src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c"
    )
    run_linux_shell(instance, script)


def detect_linux_ftrace_sink(instance: str, preferred: str) -> str:
    cands: List[str] = []
    if preferred.strip():
        cands.append(preferred.strip())
    for p in [
        "/sys/kernel/tracing/trace_marker",
        "/sys/kernel/debug/tracing/trace_marker",
        "/sys/kernel/tracing/trace",
        "/sys/kernel/debug/tracing/trace",
    ]:
        if p not in cands:
            cands.append(p)

    cand_str = " ".join(shlex.quote(x) for x in cands)
    script = (
        "set -euo pipefail; "
        f"for p in {cand_str}; do "
        "  if sudo -n test -e \"$p\"; then "
        "    if sudo -n bash -lc \"echo codex_probe > \\\"$p\\\"\" >/dev/null 2>&1; then "
        "      echo \"$p\"; exit 0; "
        "    fi; "
        "  fi; "
        "done; "
        "exit 1"
    )
    proc = run_linux_shell(instance, script, check=False)
    sink = ""
    for ln in reversed((proc.stdout or "").splitlines()):
        x = ln.strip()
        if x.startswith("/sys/"):
            sink = x
            break
    if not sink:
        raise RuntimeError("no writable ftrace sink detected in linux vm")
    return sink


def run_local_single(group: dict, out_dir: str, args: argparse.Namespace) -> str:
    ensure_dir(out_dir)
    env = os.environ.copy()
    env["OPTBINLOG_BENCH_OUT_DIR"] = out_dir
    env["OPTBINLOG_BENCH_BIN"] = os.path.join(ROOT, "optbinlog_bench_macos")
    env["OPTBINLOG_EVENTLOG_DIR"] = os.path.join(ROOT, group["eventlog_dir"])
    env["OPTBINLOG_TEXT_PROFILE"] = "semantic"
    env["OPTBINLOG_BENCH_MODES"] = f"text,binary,{group['peer']}"
    env["OPTBINLOG_BENCH_BASELINE"] = "text"
    env["OPTBINLOG_BENCH_RECORDS"] = str(args.single_records)
    env["OPTBINLOG_BENCH_REPEATS"] = str(args.single_repeats)
    env["OPTBINLOG_BENCH_WARMUP"] = str(args.single_warmup)
    run_cmd(["python3", "run_bench.py"], cwd=ROOT, env=env)
    return os.path.join(out_dir, "bench_result.json")


def run_local_multi(group: dict, out_dir: str, args: argparse.Namespace) -> str:
    ensure_dir(out_dir)
    env = os.environ.copy()
    env["OPTBINLOG_MULTI_OUT_DIR"] = out_dir
    env["OPTBINLOG_MULTI_BIN"] = os.path.join(ROOT, "optbinlog_multi_bench_macos")
    env["OPTBINLOG_EVENTLOG_DIR"] = os.path.join(ROOT, group["eventlog_dir"])
    env["OPTBINLOG_TEXT_PROFILE"] = "semantic"
    env["OPTBINLOG_MULTI_MODES"] = f"text,binary,{group['peer']}"
    env["OPTBINLOG_MULTI_BASELINE"] = "text"
    env["OPTBINLOG_MULTI_REPEATS"] = str(args.multi_repeats)
    env["OPTBINLOG_MULTI_WARMUP"] = str(args.multi_warmup)
    env["OPTBINLOG_SCAN_DEVICES"] = args.multi_scan_devices
    env["OPTBINLOG_SCAN_RECORDS_PER_DEVICE"] = args.multi_scan_rpd
    run_cmd(["python3", "run_multi_bench.py"], cwd=ROOT, env=env)
    return os.path.join(out_dir, "bench_multi_result.json")


def linux_exports(env_kv: Dict[str, str]) -> str:
    return " ".join(f"{k}={shlex.quote(str(v))}" for k, v in env_kv.items())


def run_linux_single(group: dict, out_dir: str, args: argparse.Namespace, instance: str, ftrace_sink: str) -> str:
    ensure_dir(out_dir)
    remote_out = out_dir
    env = {
        "OPTBINLOG_BENCH_OUT_DIR": remote_out,
        "OPTBINLOG_BENCH_BIN": os.path.join(ROOT, "optbinlog_bench_linux"),
        "OPTBINLOG_EVENTLOG_DIR": os.path.join(ROOT, group["eventlog_dir"]),
        "OPTBINLOG_TEXT_PROFILE": "semantic",
        "OPTBINLOG_BENCH_MODES": f"text,binary,{group['peer']}",
        "OPTBINLOG_BENCH_BASELINE": "text",
        "OPTBINLOG_BENCH_RECORDS": str(args.single_records),
        "OPTBINLOG_BENCH_REPEATS": str(args.single_repeats),
        "OPTBINLOG_BENCH_WARMUP": str(args.single_warmup),
        "OPTBINLOG_TRACE_MARKER": ftrace_sink,
    }
    script = (
        "set -euo pipefail; "
        f"cd {shlex.quote(ROOT)}; "
        f"sudo -n rm -rf {shlex.quote(remote_out)}; "
        f"export {linux_exports(env)}; "
        "sudo -n -E python3 run_bench.py; "
        f"sudo -n chown -R $(id -u):$(id -g) {shlex.quote(remote_out)} || true"
    )
    run_linux_shell(instance, script)
    return os.path.join(out_dir, "bench_result.json")


def run_linux_multi(group: dict, out_dir: str, args: argparse.Namespace, instance: str, ftrace_sink: str) -> str:
    ensure_dir(out_dir)
    remote_out = out_dir
    env = {
        "OPTBINLOG_MULTI_OUT_DIR": remote_out,
        "OPTBINLOG_MULTI_BIN": os.path.join(ROOT, "optbinlog_multi_bench_linux"),
        "OPTBINLOG_EVENTLOG_DIR": os.path.join(ROOT, group["eventlog_dir"]),
        "OPTBINLOG_TEXT_PROFILE": "semantic",
        "OPTBINLOG_MULTI_MODES": f"text,binary,{group['peer']}",
        "OPTBINLOG_MULTI_BASELINE": "text",
        "OPTBINLOG_MULTI_REPEATS": str(args.multi_repeats),
        "OPTBINLOG_MULTI_WARMUP": str(args.multi_warmup),
        "OPTBINLOG_SCAN_DEVICES": args.multi_scan_devices,
        "OPTBINLOG_SCAN_RECORDS_PER_DEVICE": args.multi_scan_rpd,
        "OPTBINLOG_TRACE_MARKER": ftrace_sink,
    }
    script = (
        "set -euo pipefail; "
        f"cd {shlex.quote(ROOT)}; "
        f"sudo -n rm -rf {shlex.quote(remote_out)}; "
        f"export {linux_exports(env)}; "
        "sudo -n -E python3 run_multi_bench.py; "
        f"sudo -n chown -R $(id -u):$(id -g) {shlex.quote(remote_out)} || true"
    )
    run_linux_shell(instance, script)
    return os.path.join(out_dir, "bench_multi_result.json")


def prepare_l1_config(template_path: str, out_path: str, group: dict, args: argparse.Namespace) -> str:
    cfg = load_json(template_path)
    cfg = copy.deepcopy(cfg)
    cfg["tag"] = f"group_l1_{group['name']}"
    cfg["parallel"] = True
    cfg["max_workers"] = int(args.l1_max_workers)
    cfg["start_sync_delay_s"] = float(args.l1_start_sync_delay)
    sem_dir = os.path.join(ROOT, group["eventlog_dir"])

    nodes = cfg.get("nodes", [])
    for node in nodes:
        node["eventlog_dir"] = sem_dir
        node["modes"] = f"text,binary,{group['peer']}"
        node["baseline"] = "text"
        node["records"] = int(args.l1_records)
        node["repeats"] = int(args.l1_repeats)
        node["warmup"] = int(args.l1_warmup)
        node["text_profile"] = "semantic"
        if args.l1_disable_netem:
            node.pop("netem", None)
        if group["peer"] == "ftrace":
            node["trace_marker"] = args.l1_trace_marker
        else:
            node.pop("trace_marker", None)

    save_json(out_path, cfg)
    return out_path


def run_l1_group(config_path: str, tag: str) -> str:
    run_cmd(["python3", "run_l1_suite.py", "--config", config_path, "--tag", tag], cwd=ROOT)
    return os.path.join(ROOT, "results", tag, "l1_summary.json")


def pct_change(base: float, cur: float, higher_better: bool) -> float:
    if base == 0:
        return 0.0
    if higher_better:
        return (cur - base) / base * 100.0
    return (base - cur) / base * 100.0


def extract_single_group(path: str, group: dict) -> dict:
    data = load_json(path)
    summary = data["summary"]
    peer = group["peer"]
    modes = ["text", "binary", peer]
    by_mode = {}
    for m in modes:
        s = summary[m]
        by_mode[m] = {
            "time_ms": s["end_to_end_ms"],
            "bytes": s["total_bytes"],
            "throughput_rps": s["throughput_e2e_rps"],
        }

    base_t = by_mode["text"]["time_ms"]["mean"]
    base_s = by_mode["text"]["bytes"]["mean"]
    base_th = by_mode["text"]["throughput_rps"]["mean"]
    improvements = {}
    for m in modes:
        mt = by_mode[m]["time_ms"]["mean"]
        ms = by_mode[m]["bytes"]["mean"]
        mth = by_mode[m]["throughput_rps"]["mean"]
        improvements[m] = {
            "time_improve_pct": pct_change(base_t, mt, higher_better=False),
            "size_save_pct": pct_change(base_s, ms, higher_better=False),
            "throughput_gain_pct": pct_change(base_th, mth, higher_better=True),
        }

    return {
        "platform": group["platform"],
        "group": group["name"],
        "peer": peer,
        "eventlog_dir": group["eventlog_dir"],
        "modes": by_mode,
        "improvements_vs_text": improvements,
        "source_json": path,
    }


def choose_multi_scenario(data: dict) -> dict:
    scenarios = data.get("scenarios", [])
    if not scenarios:
        raise RuntimeError("multi result has no scenarios")
    scenarios = sorted(scenarios, key=lambda x: (x.get("devices", 0), x.get("records_per_device", 0)))
    return scenarios[-1]


def extract_multi_group(path: str, group: dict) -> dict:
    data = load_json(path)
    peer = group["peer"]
    sc = choose_multi_scenario(data)
    summary = sc["summary"]
    modes = ["text", "binary", peer]
    by_mode = {}
    for m in modes:
        s = summary[m]
        by_mode[m] = {
            "time_ms": s["elapsed_ms"],
            "bytes": s["total_bytes"],
            "throughput_rps": s["throughput_rps"],
        }

    base_t = by_mode["text"]["time_ms"]["mean"]
    base_s = by_mode["text"]["bytes"]["mean"]
    base_th = by_mode["text"]["throughput_rps"]["mean"]
    improvements = {}
    for m in modes:
        mt = by_mode[m]["time_ms"]["mean"]
        ms = by_mode[m]["bytes"]["mean"]
        mth = by_mode[m]["throughput_rps"]["mean"]
        improvements[m] = {
            "time_improve_pct": pct_change(base_t, mt, higher_better=False),
            "size_save_pct": pct_change(base_s, ms, higher_better=False),
            "throughput_gain_pct": pct_change(base_th, mth, higher_better=True),
        }

    return {
        "platform": group["platform"],
        "group": group["name"],
        "peer": peer,
        "eventlog_dir": group["eventlog_dir"],
        "selected_scenario": {
            "scenario": sc["scenario"],
            "devices": sc["devices"],
            "records_per_device": sc["records_per_device"],
        },
        "modes": by_mode,
        "improvements_vs_text": improvements,
        "source_json": path,
    }


def extract_l1_group(path: str, group: dict) -> dict:
    data = load_json(path)
    peer = group["peer"]
    modes = ["text", "binary", peer]
    usable_nodes = []
    for n in data.get("nodes", []):
        by_mode = n.get("summary", {}).get("by_mode", {})
        if by_mode and "text" in by_mode:
            usable_nodes.append(n)
    if not usable_nodes:
        raise RuntimeError(f"no successful nodes in {path}")

    by_mode = {}
    for m in modes:
        tvals = []
        svals = []
        thvals = []
        for n in usable_nodes:
            bm = n.get("summary", {}).get("by_mode", {}).get(m, {})
            if bm:
                tvals.append(float(bm.get("end_to_end_ms_mean", 0.0)))
                svals.append(float(bm.get("total_bytes_mean", 0.0)))
                thvals.append(float(bm.get("throughput_e2e_rps_mean", 0.0)))
        tvals = iqr_filter(tvals)
        svals = iqr_filter(svals)
        thvals = iqr_filter(thvals)
        by_mode[m] = {
            "time_ms": metric_stats(tvals),
            "bytes": metric_stats(svals),
            "throughput_rps": metric_stats(thvals),
        }

    base_t = by_mode["text"]["time_ms"]["mean"]
    base_s = by_mode["text"]["bytes"]["mean"]
    base_th = by_mode["text"]["throughput_rps"]["mean"]
    improvements = {}
    for m in modes:
        mt = by_mode[m]["time_ms"]["mean"]
        ms = by_mode[m]["bytes"]["mean"]
        mth = by_mode[m]["throughput_rps"]["mean"]
        improvements[m] = {
            "time_improve_pct": pct_change(base_t, mt, higher_better=False),
            "size_save_pct": pct_change(base_s, ms, higher_better=False),
            "throughput_gain_pct": pct_change(base_th, mth, higher_better=True),
        }

    return {
        "platform": group["platform"],
        "group": group["name"],
        "peer": peer,
        "eventlog_dir": group["eventlog_dir"],
        "nodes_ok": len([n for n in data.get("nodes", []) if n.get("status") == "ok"]),
        "nodes_used": len(usable_nodes),
        "nodes_total": len(data.get("nodes", [])),
        "modes": by_mode,
        "improvements_vs_text": improvements,
        "source_json": path,
    }


def build_category_svg(title: str, subtitle: str, groups: List[dict], out_path: str) -> None:
    role_color = {"text": "#808080", "binary": "#1f78b4", "peer": "#e6550d"}
    metrics = [
        ("time_ms", "Time", "ms", False),
        ("bytes", "Space", "bytes", False),
        ("throughput_rps", "Throughput", "records/s", True),
    ]

    gcount = max(1, len(groups))
    panel_w = max(460, gcount * 165)
    panel_h = 320
    gap = 40
    margin = 72
    panel_y = 132
    width = int(margin * 2 + panel_w * 3 + gap * 2)
    height = 560

    def all_vals(metric_key: str) -> List[float]:
        vals = []
        for g in groups:
            peer = g["peer"]
            vals.append(g["modes"]["text"][metric_key]["mean"])
            vals.append(g["modes"]["binary"][metric_key]["mean"])
            vals.append(g["modes"][peer][metric_key]["mean"])
        return vals or [1.0]

    lines: List[str] = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="21">{title}</text>')
    lines.append(f'<text x="{width / 2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">{subtitle}</text>')

    for pi, (metric_key, metric_title, unit, higher_better) in enumerate(metrics):
        x0 = margin + pi * (panel_w + gap)
        x1 = x0 + panel_w
        vals = all_vals(metric_key)
        vmax = max(vals + [1.0])

        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y - 12}" text-anchor="middle" font-family="Arial" font-size="15">{metric_title}</text>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y + panel_h + 50}" text-anchor="middle" font-family="Arial" font-size="12">{unit}</text>')

        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            v = vmax * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x1}" y2="{y}" stroke="#f1f1f1"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.2f}</text>')

        group_step = panel_w / gcount
        bar_w = min(24.0, max(10.0, group_step / 5.2))

        for gi, g in enumerate(groups):
            peer = g["peer"]
            gx = x0 + gi * group_step
            cx = gx + group_step / 2.0
            labels = [("text", "text"), ("binary", "binary"), (peer, "peer")]
            for ri, (mode_name, role) in enumerate(labels):
                v = g["modes"][mode_name][metric_key]["mean"]
                h = 0 if vmax <= 0 else (v / vmax) * (panel_h * 0.9)
                bx = cx + (ri - 1) * (bar_w + 4) - bar_w / 2.0
                by = panel_y + panel_h - h
                lines.append(f'<rect x="{bx}" y="{by}" width="{bar_w}" height="{h}" fill="{role_color[role]}"/>')

                base = g["modes"]["text"][metric_key]["mean"]
                if role == "text":
                    lbl = "base"
                    col = "#666"
                else:
                    delta = pct_change(base, v, higher_better=higher_better)
                    sign = "+" if delta >= 0 else "-"
                    lbl = f"{sign}{abs(delta):.1f}%"
                    col = "#1b7837" if delta >= 0 else "#b2182b"
                lines.append(f'<text x="{bx + bar_w/2}" y="{by - 7}" text-anchor="middle" font-family="Arial" font-size="9" fill="{col}">{lbl}</text>')

            g_label = f"{g['platform']}/{g['group']}"
            lines.append(f'<text x="{cx}" y="{panel_y + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="10">{g_label}</text>')

    legend_y = height - 28
    lx = margin
    lines.append(f'<rect x="{lx}" y="{legend_y - 11}" width="12" height="12" fill="{role_color["text"]}"/>')
    lines.append(f'<text x="{lx + 18}" y="{legend_y}" font-family="Arial" font-size="12">text</text>')
    lines.append(f'<rect x="{lx + 90}" y="{legend_y - 11}" width="12" height="12" fill="{role_color["binary"]}"/>')
    lines.append(f'<text x="{lx + 108}" y="{legend_y}" font-family="Arial" font-size="12">binary</text>')
    lines.append(f'<rect x="{lx + 202}" y="{legend_y - 11}" width="12" height="12" fill="{role_color["peer"]}"/>')
    lines.append(f'<text x="{lx + 220}" y="{legend_y}" font-family="Arial" font-size="12">peer mode</text>')
    lines.append(f'<text x="{width - 12}" y="{legend_y}" text-anchor="end" font-family="Arial" font-size="11">label above bars: relative to text baseline in same group</text>')

    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_markdown_report(path: str, merged: dict) -> None:
    lines: List[str] = []
    lines.append("# Grouped Semantic Matrix Report")
    lines.append("")
    lines.append("## Rules")
    lines.append("")
    lines.append("- Baseline: `text` (within each group)")
    lines.append("- Metrics: time=end_to_end/elapsed, bytes=total_bytes, throughput=e2e records/s")
    lines.append("- multi-device summary uses the max-load scenario in each run")
    lines.append("")

    for cat_key, title in [
        ("single_high_load", "Single High-Load"),
        ("multi_device_sim", "Multi-Device Simulation"),
        ("l1_real_multi", "Real-Device Simulation (Multi-VM Nodes)"),
    ]:
        lines.append(f"## {title}")
        lines.append("")
        lines.append("| group | platform | peer | binary time% | binary size% | binary thr% | peer time% | peer size% | peer thr% |")
        lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
        for g in merged["categories"][cat_key]:
            b = g["improvements_vs_text"]["binary"]
            p = g["improvements_vs_text"][g["peer"]]
            lines.append(
                "| {} | {} | {} | {:+.2f}% | {:+.2f}% | {:+.2f}% | {:+.2f}% | {:+.2f}% | {:+.2f}% |".format(
                    g["group"],
                    g["platform"],
                    g["peer"],
                    b["time_improve_pct"],
                    b["size_save_pct"],
                    b["throughput_gain_pct"],
                    p["time_improve_pct"],
                    p["size_save_pct"],
                    p["throughput_gain_pct"],
                )
            )
        lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run grouped semantic matrix for local/linux/L1 and merge visualizations.")
    p.add_argument("--out-dir", default="", help="Output directory root. Default: results/grouped_semantic_matrix_<ts>")
    p.add_argument("--linux-instance", default="thesis-linux")
    p.add_argument("--l1-template", default=os.path.join(ROOT, "l1_config.linux_10_all_unaligned_initrace.json"))

    p.add_argument("--single-records", type=int, default=60000)
    p.add_argument("--single-repeats", type=int, default=4)
    p.add_argument("--single-warmup", type=int, default=1)

    p.add_argument("--multi-repeats", type=int, default=4)
    p.add_argument("--multi-warmup", type=int, default=1)
    p.add_argument("--multi-scan-devices", default="2,5,10")
    p.add_argument("--multi-scan-rpd", default="600")

    p.add_argument("--l1-records", type=int, default=60000)
    p.add_argument("--l1-repeats", type=int, default=2)
    p.add_argument("--l1-warmup", type=int, default=1)
    p.add_argument("--l1-max-workers", type=int, default=10)
    p.add_argument("--l1-start-sync-delay", type=float, default=10.0)
    p.add_argument("--l1-trace-marker", default="/sys/kernel/tracing/trace_marker")
    p.add_argument("--l1-disable-netem", action="store_true", default=True)
    p.add_argument("--skip-single", action="store_true")
    p.add_argument("--skip-multi", action="store_true")
    p.add_argument("--skip-l1", action="store_true")

    return p.parse_args()


def main() -> None:
    args = parse_args()
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = args.out_dir.strip() or os.path.join(RESULTS_ROOT, f"grouped_semantic_matrix_{ts}")
    ensure_dir(out_root)

    # Fresh output layout.
    for sub in ["single", "multi", "l1", "merged", "configs"]:
        ensure_dir(os.path.join(out_root, sub))

    print("[1/7] build local binaries")
    build_local_binaries()

    print("[2/7] build linux binaries + detect ftrace sink")
    build_linux_binaries(args.linux_instance)
    ftrace_sink = detect_linux_ftrace_sink(args.linux_instance, args.l1_trace_marker)
    print(f"ftrace_sink={ftrace_sink}")

    single_groups: List[dict] = []
    multi_groups: List[dict] = []
    l1_groups: List[dict] = []

    print("[3/7] run single high-load groups (local + linux)")
    if not args.skip_single:
        for g in LOCAL_GROUPS:
            out_dir = os.path.join(out_root, "single", f"local_{g['name']}")
            jp = run_local_single(g, out_dir, args)
            single_groups.append(extract_single_group(jp, g))
            print(f"single local done: {g['name']}")

        out_dir = os.path.join(out_root, "single", "linux_ftrace")
        jp = run_linux_single(LINUX_GROUP, out_dir, args, args.linux_instance, ftrace_sink)
        single_groups.append(extract_single_group(jp, LINUX_GROUP))
        print("single linux done: ftrace")
    else:
        print("single skipped")

    print("[4/7] run multi-device groups (local + linux)")
    if not args.skip_multi:
        for g in LOCAL_GROUPS:
            out_dir = os.path.join(out_root, "multi", f"local_{g['name']}")
            jp = run_local_multi(g, out_dir, args)
            multi_groups.append(extract_multi_group(jp, g))
            print(f"multi local done: {g['name']}")

        out_dir = os.path.join(out_root, "multi", "linux_ftrace")
        jp = run_linux_multi(LINUX_GROUP, out_dir, args, args.linux_instance, ftrace_sink)
        multi_groups.append(extract_multi_group(jp, LINUX_GROUP))
        print("multi linux done: ftrace")
    else:
        print("multi skipped")

    print("[5/7] run L1 real multi-device groups")
    if not args.skip_l1:
        for g in L1_GROUPS:
            cfg_path = os.path.join(out_root, "configs", f"l1_{g['name']}.json")
            prepare_l1_config(args.l1_template, cfg_path, g, args)
            tag = f"l1_grouped_{g['name']}_{ts}"
            summary_path = run_l1_group(cfg_path, tag)

            # Copy summary/report/SVG into this run tree for easier navigation.
            l1_src_dir = os.path.join(ROOT, "results", tag)
            l1_dst_dir = os.path.join(out_root, "l1", g["name"])
            if os.path.exists(l1_dst_dir):
                shutil.rmtree(l1_dst_dir)
            shutil.copytree(l1_src_dir, l1_dst_dir)

            l1_groups.append(extract_l1_group(summary_path, g))
            print(f"l1 done: {g['name']}")
    else:
        print("l1 skipped")

    print("[6/7] merge summaries + build category SVGs")
    merged = {
        "generated_at": dt.datetime.now().isoformat(),
        "out_root": out_root,
        "config": {
            "single_records": args.single_records,
            "single_repeats": args.single_repeats,
            "single_warmup": args.single_warmup,
            "multi_repeats": args.multi_repeats,
            "multi_warmup": args.multi_warmup,
            "multi_scan_devices": args.multi_scan_devices,
            "multi_scan_rpd": args.multi_scan_rpd,
            "l1_records": args.l1_records,
            "l1_repeats": args.l1_repeats,
            "l1_warmup": args.l1_warmup,
            "linux_instance": args.linux_instance,
            "ftrace_sink": ftrace_sink,
        },
        "categories": {
            "single_high_load": single_groups,
            "multi_device_sim": multi_groups,
            "l1_real_multi": l1_groups,
        },
    }

    merged_json = os.path.join(out_root, "merged", "grouped_matrix_merged.json")
    save_json(merged_json, merged)

    single_svg = os.path.join(out_root, "merged", "single_high_load_merged.svg")
    if single_groups:
        build_category_svg(
            "Single High-Load: grouped semantic comparison",
            "groups: local(syslog/nanolog/zephyr/ulog/hilog) + linux(ftrace); baseline=text",
            single_groups,
            single_svg,
        )

    multi_svg = os.path.join(out_root, "merged", "multi_device_merged.svg")
    if multi_groups:
        build_category_svg(
            "Multi-Device Simulation: grouped semantic comparison",
            "groups: local(syslog/nanolog/zephyr/ulog/hilog) + linux(ftrace); baseline=text",
            multi_groups,
            multi_svg,
        )

    l1_svg = os.path.join(out_root, "merged", "l1_real_multi_merged.svg")
    if l1_groups:
        build_category_svg(
            "Real-Device Simulation (Multi-VM Nodes): grouped semantic comparison",
            "10-node concurrent start (one node = one device); baseline=text",
            l1_groups,
            l1_svg,
        )

    report_md = os.path.join(out_root, "merged", "grouped_matrix_report.md")
    build_markdown_report(report_md, merged)

    latest = os.path.join(RESULTS_ROOT, "grouped_semantic_matrix_latest")
    if os.path.islink(latest) or os.path.exists(latest):
        if os.path.islink(latest):
            os.unlink(latest)
        elif os.path.isdir(latest):
            shutil.rmtree(latest)
        else:
            os.remove(latest)
    os.symlink(out_root, latest)

    print("[7/7] done")
    print("saved", merged_json)
    if single_groups:
        print("saved", single_svg)
    if multi_groups:
        print("saved", multi_svg)
    if l1_groups:
        print("saved", l1_svg)
    print("saved", report_md)
    print("saved", latest)


if __name__ == "__main__":
    main()
