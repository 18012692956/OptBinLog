#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import copy
from typing import Dict, List


ROOT = os.path.dirname(__file__)
RESULTS_ROOT = os.path.join(ROOT, "results")
RUN_BENCH = os.path.join(ROOT, "run_bench.py")
PROFILES = [
    {"name": "nanolog", "eventlog_dir": "eventlogst_semantic_nanolog", "peer_mode": "nanolog_semantic_like"},
    {"name": "zephyr", "eventlog_dir": "eventlogst_semantic_zephyr", "peer_mode": "zephyr_deferred_semantic_like"},
    {"name": "ulog", "eventlog_dir": "eventlogst_semantic_ulog", "peer_mode": "ulog_semantic_like"},
    {"name": "hilog", "eventlog_dir": "eventlogst_semantic_hilog", "peer_mode": "hilog_semantic_like"},
]

MAIN_MODES = ["text_semantic_like", "binary"]


def l1_bench_build_cmd() -> str:
    # Prefer hardware CRC32C acceleration on ARM/x86 when the ISA supports it.
    return (
        "NEED_BUILD=0; "
        "if [ ! -x ./optbinlog_bench_linux ]; then NEED_BUILD=1; fi; "
        "for SRC in optbinlog_bench.c src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c "
        "include/optbinlog_shared.h include/optbinlog_eventlog.h include/optbinlog_binlog.h; do "
        "if [ -f \"$SRC\" ] && [ \"$SRC\" -nt ./optbinlog_bench_linux ]; then NEED_BUILD=1; break; fi; "
        "done; "
        "if [ \"$NEED_BUILD\" -eq 0 ]; then exit 0; fi; "
        "ARCH=$(uname -m); EXTRA=''; "
        "if [ \"$ARCH\" = \"aarch64\" ] || [ \"$ARCH\" = \"arm64\" ]; then EXTRA='-march=armv8-a+crc'; "
        "elif [ \"$ARCH\" = \"x86_64\" ] || [ \"$ARCH\" = \"i686\" ] || [ \"$ARCH\" = \"i386\" ]; then EXTRA='-msse4.2'; fi; "
        "CC_BIN=''; "
        "if command -v gcc >/dev/null 2>&1; then CC_BIN=gcc; "
        "elif command -v cc >/dev/null 2>&1; then CC_BIN=cc; "
        "elif command -v clang >/dev/null 2>&1; then CC_BIN=clang; fi; "
        "if [ -z \"$CC_BIN\" ]; then echo 'no C compiler found on node'; exit 127; fi; "
        "$CC_BIN -O2 -Wall -Wextra -std=c11 -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L $EXTRA "
        "-Iinclude -o optbinlog_bench_linux optbinlog_bench.c src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run final aligned comparison suite across single/multi/L1.")
    p.add_argument("--out-dir", default="", help="Output root directory")
    p.add_argument("--bench-bin", default=os.path.join(ROOT, "optbinlog_bench_macos"))
    p.add_argument("--single-records", type=int, default=20000)
    p.add_argument("--single-repeats", type=int, default=5)
    p.add_argument("--single-warmup", type=int, default=1)
    p.add_argument("--multi-records-per-device", type=int, default=800)
    p.add_argument("--multi-devices", default="5,10,20,50")
    p.add_argument("--multi-repeats", type=int, default=3)
    p.add_argument("--multi-warmup", type=int, default=1)
    p.add_argument("--l1-template", default=os.path.join(ROOT, "l1_config.linux_10_all_unaligned_initrace.json"))
    p.add_argument("--l1-node-scales", default="5,10,15,20")
    p.add_argument("--l1-records", type=int, default=20000)
    p.add_argument("--l1-repeats", type=int, default=3)
    p.add_argument("--l1-warmup", type=int, default=1)
    p.add_argument("--l1-max-workers", type=int, default=10)
    p.add_argument("--l1-start-sync-delay", type=float, default=10.0)
    p.add_argument("--l1-disable-netem", action="store_true", default=True)
    p.add_argument("--skip-l1", action="store_true")
    return p.parse_args()


def run_cmd(cmd: List[str], cwd: str = ROOT, env: Dict[str, str] = None) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed\ncmd: {}\nstdout:\n{}\nstderr:\n{}".format(" ".join(cmd), proc.stdout, proc.stderr)
        )
    return proc


def ensure_clean_dir(path: str) -> None:
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def parse_scale_list(raw: str) -> List[int]:
    values: List[int] = []
    seen = set()
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        value = int(text)
        if value <= 0:
            raise ValueError(f"invalid scale value: {value}")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    if not values:
        raise ValueError("scale list is empty")
    return values


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


def iqr_filter_rows(rows: List[dict], field: str) -> List[dict]:
    values = [float(r[field]) for r in rows]
    if len(values) < 4:
        return list(rows)
    q1 = percentile(values, 25)
    q3 = percentile(values, 75)
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    kept = [r for r in rows if lo <= float(r[field]) <= hi]
    return kept if len(kept) >= max(2, len(rows) // 2) else list(rows)


def iqr_filter_values(values: List[float]) -> List[float]:
    if len(values) < 4:
        return list(values)
    q1 = percentile(values, 25)
    q3 = percentile(values, 75)
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    kept = [v for v in values if lo <= v <= hi]
    return kept if len(kept) >= max(2, len(values) // 2) else list(values)


def parse_line(line: str) -> Dict[str, str]:
    parts = line.split(",")
    out: Dict[str, str] = {}
    for i in range(0, len(parts) - 1, 2):
        out[parts[i]] = parts[i + 1]
    return out


def mode_ext(mode: str) -> str:
    return {
        "text_semantic_like": "slog",
        "binary": "bin",
        "nanolog_semantic_like": "nslog",
        "zephyr_deferred_semantic_like": "zslog",
        "ulog_semantic_like": "uslog",
        "hilog_semantic_like": "hslog",
    }.get(mode, "out")


def pct_improve(base: float, cur: float, higher_better: bool) -> float:
    if base == 0:
        return 0.0
    if higher_better:
        return (cur - base) / base * 100.0
    return (base - cur) / base * 100.0


def run_single_profile(profile: dict, out_dir: str, args: argparse.Namespace) -> dict:
    modes = MAIN_MODES + [profile["peer_mode"]]
    env = os.environ.copy()
    env["OPTBINLOG_BENCH_OUT_DIR"] = out_dir
    env["OPTBINLOG_BENCH_BIN"] = args.bench_bin
    env["OPTBINLOG_EVENTLOG_DIR"] = os.path.join(ROOT, profile["eventlog_dir"])
    env["OPTBINLOG_BENCH_MODES"] = ",".join(modes)
    env["OPTBINLOG_BENCH_BASELINE"] = "text_semantic_like"
    env["OPTBINLOG_BENCH_RECORDS"] = str(args.single_records)
    env["OPTBINLOG_BENCH_REPEATS"] = str(args.single_repeats)
    env["OPTBINLOG_BENCH_WARMUP"] = str(args.single_warmup)
    run_cmd(["python3", RUN_BENCH], env=env)
    return load_json(os.path.join(out_dir, "bench_result.json"))


def run_multi_mode_once(
    bench_bin: str,
    eventlog_dir: str,
    mode: str,
    devices: int,
    records_per_device: int,
    shared_path: str,
    out_dir: str,
) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    procs = []
    start = dt.datetime.now().timestamp()
    for dev in range(devices):
        out_path = os.path.join(out_dir, f"device_{dev:02d}.{mode_ext(mode)}")
        cmd = [
            bench_bin,
            "--mode",
            mode,
            "--eventlog-dir",
            eventlog_dir,
            "--out",
            out_path,
            "--records",
            str(records_per_device),
            "--shared",
            shared_path,
        ]
        procs.append((dev, out_path, subprocess.Popen(cmd, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)))

    rows = []
    for dev, out_path, proc in procs:
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                "multi mode failed\nmode: {}\ndevice: {}\nstdout:\n{}\nstderr:\n{}".format(mode, dev, stdout, stderr)
            )
        line = stdout.strip().splitlines()[-1]
        rec = parse_line(line)
        rows.append(
            {
                "device": dev,
                "out_path": out_path,
                "bytes": int(rec.get("bytes", 0)),
                "shared_bytes": int(rec.get("shared_bytes", 0)),
                "total_bytes": int(rec.get("total_bytes", rec.get("bytes", 0))),
            }
        )
    end = dt.datetime.now().timestamp()
    elapsed_ms = (end - start) * 1000.0
    payload_bytes = sum(int(r["bytes"]) for r in rows)
    shared_bytes = os.path.getsize(shared_path) if os.path.exists(shared_path) else 0
    total_records = devices * records_per_device
    throughput_rps = (1000.0 * total_records / elapsed_ms) if elapsed_ms > 0 else 0.0
    return {
        "mode": mode,
        "devices": devices,
        "records_per_device": records_per_device,
        "elapsed_ms": elapsed_ms,
        "bytes": payload_bytes,
        "shared_bytes": shared_bytes,
        "total_bytes": payload_bytes + shared_bytes,
        "throughput_rps": throughput_rps,
        "per_device": rows,
    }


def run_multi_profile(profile: dict, out_dir: str, args: argparse.Namespace) -> dict:
    devices_list = parse_scale_list(args.multi_devices)
    modes = MAIN_MODES + [profile["peer_mode"]]
    scenarios = []
    eventlog_dir = os.path.join(ROOT, profile["eventlog_dir"])
    raw_dir = os.path.join(out_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    for devices in devices_list:
        scenario_name = f"d{devices}_r{args.multi_records_per_device}"
        scenario_runs = []
        for run_kind, n_runs in [("warmup", args.multi_warmup), ("run", args.multi_repeats)]:
            for idx in range(n_runs):
                for mode in modes:
                    run_dir = os.path.join(raw_dir, scenario_name, f"{mode}_{run_kind}_{idx:03d}")
                    shared_path = os.path.join(raw_dir, scenario_name, f"{mode}_{run_kind}_{idx:03d}.shared_eventtag.bin")
                    row = run_multi_mode_once(
                        args.bench_bin,
                        eventlog_dir,
                        mode,
                        devices,
                        args.multi_records_per_device,
                        shared_path,
                        run_dir,
                    )
                    row["iteration"] = idx
                    row["run_kind"] = run_kind
                    if run_kind == "run":
                        scenario_runs.append(row)

        summary = {}
        for mode in modes:
            mode_rows = [r for r in scenario_runs if r["mode"] == mode]
            kept = iqr_filter_rows(mode_rows, "elapsed_ms")
            summary[mode] = {
                "elapsed_ms": metric_stats([float(r["elapsed_ms"]) for r in kept]),
                "total_bytes": metric_stats([float(r["total_bytes"]) for r in kept]),
                "throughput_rps": metric_stats([float(r["throughput_rps"]) for r in kept]),
                "kept_iterations": [int(r["iteration"]) for r in kept],
            }
        scenarios.append(
            {
                "scenario": scenario_name,
                "devices": devices,
                "records_per_device": args.multi_records_per_device,
                "modes": modes,
                "runs": scenario_runs,
                "summary": summary,
            }
        )
    return {"profile": profile["name"], "peer_mode": profile["peer_mode"], "scenarios": scenarios}


def renumber_l1_node(base_node: dict, index: int) -> dict:
    node = copy.deepcopy(base_node)
    node["name"] = f"dev-{index:02d}"
    prefix = node.get("prefix")
    if isinstance(prefix, list):
        node["prefix"] = [
            re.sub(r"thesis-dev-\d{2}", f"thesis-dev-{index:02d}", str(item)) for item in prefix
        ]
    ssh_target = node.get("ssh_target")
    if isinstance(ssh_target, str):
        node["ssh_target"] = re.sub(r"thesis-dev-\d{2}", f"thesis-dev-{index:02d}", ssh_target)
    remote_out_dir = node.get("remote_out_dir")
    if isinstance(remote_out_dir, str):
        node["remote_out_dir"] = re.sub(r"dev-\d{2}", f"dev-{index:02d}", remote_out_dir)
    return node


def stabilize_lima_prefix(node: dict) -> dict:
    # In sandboxed environments, direct ssh to ~/.lima sockets can fail with
    # permission errors. Keep limactl transport by default and only switch to
    # ssh when explicitly requested.
    if os.environ.get("OPTBINLOG_L1_USE_SSH_PREFIX", "").strip().lower() not in {"1", "true", "yes"}:
        return node
    prefix = node.get("prefix")
    if not (node.get("transport") == "prefix" and isinstance(prefix, list) and len(prefix) >= 4):
        return node
    if [str(x) for x in prefix[:2]] != ["limactl", "shell"]:
        return node
    instance = str(prefix[2])
    ssh_cfg = os.path.expanduser(os.path.join("~", ".lima", instance, "ssh.config"))
    if not os.path.exists(ssh_cfg):
        return node
    node["prefix"] = ["ssh", "-F", ssh_cfg, f"lima-{instance}"]
    return node


def expand_l1_nodes(nodes: List[dict], target_count: int) -> List[dict]:
    if not nodes:
        raise RuntimeError("l1 template has no nodes")
    expanded: List[dict] = []
    for idx in range(1, target_count + 1):
        base = nodes[(idx - 1) % len(nodes)]
        expanded.append(stabilize_lima_prefix(renumber_l1_node(base, idx)))
    return expanded


def prepare_l1_config(
    template_path: str,
    out_path: str,
    profile: dict,
    args: argparse.Namespace,
    node_count: int,
    shared_tag_path: str = "",
) -> str:
    cfg = copy.deepcopy(load_json(template_path))
    cfg["tag"] = f"aligned_l1_{profile['name']}_{node_count:02d}nodes"
    cfg["parallel"] = True
    cfg["max_workers"] = min(int(args.l1_max_workers), int(node_count))
    cfg["start_sync_delay_s"] = float(args.l1_start_sync_delay)
    eventlog_dir = os.path.join(ROOT, profile["eventlog_dir"])
    modes = ",".join(MAIN_MODES + [profile["peer_mode"]])
    cfg["nodes"] = expand_l1_nodes(cfg.get("nodes", []), int(node_count))
    for node in cfg.get("nodes", []):
        # Use the shared host-mounted workspace path to avoid per-VM path drift.
        node["workdir"] = ROOT
        node["eventlog_dir"] = eventlog_dir
        node["records"] = int(args.l1_records)
        node["repeats"] = int(args.l1_repeats)
        node["warmup"] = int(args.l1_warmup)
        node["modes"] = modes
        node["baseline"] = "text_semantic_like"
        node["build_cmd"] = l1_bench_build_cmd()
        node["bench_bin"] = "./optbinlog_bench_linux"
        node["bench_prefix"] = ""
        node["text_profile"] = "semantic"
        if shared_tag_path:
            node["shared_tag_path"] = shared_tag_path
        else:
            node.pop("shared_tag_path", None)
        node.pop("trace_marker", None)
        node.pop("syslog_source", None)
        if args.l1_disable_netem:
            node.pop("netem", None)
    save_json(out_path, cfg)
    return out_path


def run_l1_profile(config_path: str, tag: str) -> str:
    run_cmd(["python3", os.path.join(ROOT, "run_l1_suite.py"), "--config", config_path, "--tag", tag], cwd=ROOT)
    return os.path.join(ROOT, "results", tag, "l1_summary.json")


def to_float_or_none(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_node_mode_space_from_bench(source_root: str, node_name: str, mode: str) -> Dict[str, float]:
    bench_json = os.path.join(source_root, "nodes", node_name, "bench_out", "bench_result.json")
    if not os.path.exists(bench_json):
        return {"bytes": None, "shared": None, "total": None}
    data = load_json(bench_json)
    ms = data.get("summary", {}).get(mode, {})
    payload = to_float_or_none(ms.get("bytes", {}).get("mean"))
    shared = to_float_or_none(ms.get("shared_bytes", {}).get("mean"))
    total = to_float_or_none(ms.get("total_bytes", {}).get("mean"))
    return {"bytes": payload, "shared": shared, "total": total}


def resolve_node_mode_space(source_root: str, node: dict, mode: str) -> Dict[str, float]:
    bm = node.get("summary", {}).get("by_mode", {}).get(mode, {}) or {}
    payload = to_float_or_none(bm.get("bytes_mean"))
    shared = to_float_or_none(bm.get("shared_bytes_mean"))
    total = to_float_or_none(bm.get("total_bytes_mean"))

    if payload is None or shared is None or total is None:
        from_bench = load_node_mode_space_from_bench(source_root, str(node.get("name", "")), mode)
        if payload is None:
            payload = from_bench["bytes"]
        if shared is None:
            shared = from_bench["shared"]
        if total is None:
            total = from_bench["total"]

    if payload is None and total is not None and shared is not None:
        payload = total - shared
    if shared is None and total is not None and payload is not None:
        shared = max(0.0, total - payload)
    if total is None and payload is not None and shared is not None:
        total = payload + shared

    payload = float(payload or 0.0)
    shared = float(shared or 0.0)
    total = float(total or (payload + shared))
    return {"bytes": payload, "shared": shared, "total": total}


def extract_l1_profile(path: str, profile: dict) -> dict:
    data = load_json(path)
    modes = MAIN_MODES + [profile["peer_mode"]]
    source_root = os.path.dirname(path)
    usable_nodes = []
    for n in data.get("nodes", []):
        by_mode = n.get("summary", {}).get("by_mode", {})
        if by_mode and "text_semantic_like" in by_mode:
            usable_nodes.append(n)
    if not usable_nodes:
        raise RuntimeError(f"no successful nodes in {path}")

    by_mode = {}
    for mode in modes:
        tvals = []
        payload_vals = []
        shared_vals = []
        thvals = []
        for n in usable_nodes:
            bm = n.get("summary", {}).get("by_mode", {}).get(mode, {})
            if bm:
                tvals.append(float(bm.get("end_to_end_ms_mean", 0.0)))
                thvals.append(float(bm.get("throughput_e2e_rps_mean", 0.0)))
                space = resolve_node_mode_space(source_root, n, mode)
                payload_vals.append(float(space["bytes"]))
                shared_vals.append(float(space["shared"]))
        tvals = iqr_filter_values(tvals)
        # Space should use all usable nodes to keep cluster accounting stable.
        payload_vals = list(payload_vals)
        shared_vals = list(shared_vals)
        thvals = iqr_filter_values(thvals)
        # Unified metric: cluster total bytes with shared metadata counted once.
        shared_once = max(shared_vals) if shared_vals else 0.0
        if payload_vals:
            # Normalize to the scenario node count so one missing mode result on
            # an individual node does not distort cluster-space percentages.
            payload_mean = statistics.fmean(payload_vals)
            cluster_bytes = float(payload_mean * len(usable_nodes) + shared_once)
        else:
            cluster_bytes = 0.0
        by_mode[mode] = {
            "time_ms": metric_stats(tvals),
            "payload_bytes": metric_stats(payload_vals),
            "shared_bytes": metric_stats(shared_vals),
            "bytes": metric_stats([cluster_bytes]),
            "cluster_total_bytes": metric_stats([cluster_bytes]),
            "throughput_rps": metric_stats(thvals),
        }

    return {
        "profile": profile["name"],
        "peer_mode": profile["peer_mode"],
        "eventlog_dir": profile["eventlog_dir"],
        "nodes_ok": len([n for n in data.get("nodes", []) if n.get("status") == "ok"]),
        "nodes_used": len(usable_nodes),
        "nodes_total": len(data.get("nodes", [])),
        "modes": by_mode,
        "source_json": path,
    }


def run_l1_scan_profile(profile: dict, out_dir: str, args: argparse.Namespace, ts: str) -> dict:
    node_scales = parse_scale_list(args.l1_node_scales)
    scenarios = []
    for node_count in node_scales:
        cfg_path = os.path.join(out_dir, f"config_{profile['name']}_{node_count:02d}nodes.json")
        shared_tag_path = os.path.join(out_dir, "shared_tag", f"{profile['name']}_{node_count:02d}nodes_shared_eventtag.bin")
        os.makedirs(os.path.dirname(shared_tag_path), exist_ok=True)
        if os.path.exists(shared_tag_path):
            os.remove(shared_tag_path)
        prepare_l1_config(args.l1_template, cfg_path, profile, args, node_count, shared_tag_path=shared_tag_path)
        tag = f"final_aligned_l1_{profile['name']}_{node_count:02d}nodes_{ts}"
        summary_path = run_l1_profile(cfg_path, tag)
        src_dir = os.path.join(ROOT, "results", tag)
        dst_dir = os.path.join(out_dir, f"{node_count:02d}_nodes")
        if os.path.exists(dst_dir):
            shutil.rmtree(dst_dir)
        shutil.copytree(src_dir, dst_dir)
        summary = extract_l1_profile(summary_path, profile)
        if summary["nodes_ok"] != node_count or summary["nodes_used"] != node_count:
            raise RuntimeError(
                f"incomplete l1 run for {profile['name']} at {node_count} nodes: "
                f"ok={summary['nodes_ok']} used={summary['nodes_used']} total={summary['nodes_total']}"
            )
        summary["nodes"] = node_count
        summary["source_dir"] = dst_dir
        save_json(os.path.join(dst_dir, "l1_extracted.json"), summary)
        scenarios.append(summary)
    return {
        "profile": profile["name"],
        "peer_mode": profile["peer_mode"],
        "node_scales": node_scales,
        "scenarios": scenarios,
    }


def build_single_overview_svg(rows: List[dict], out_path: str) -> None:
    role_color = {"text_semantic_like": "#7f7f7f", "binary": "#1f78b4", "peer": "#e31a1c"}
    metrics = [
        ("end_to_end_ms", "Time", "ms", False),
        ("total_bytes", "Space", "bytes", False),
        ("throughput_e2e_rps", "Throughput", "records/s", True),
    ]
    gcount = max(1, len(rows))
    panel_w = max(420, gcount * 170)
    panel_h = 320
    gap = 40
    margin = 72
    panel_y = 132
    width = int(margin * 2 + panel_w * 3 + gap * 2)
    height = 560

    def metric_mean(row: dict, mode: str, key: str) -> float:
        return float(row["single"]["summary"][mode][key]["mean"])

    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="21">Strict Aligned Single High-Load Comparison</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="13">same schema, same generated values, same platform; bars show text vs final binary vs peer semantic_like</text>')

    for pi, (metric_key, metric_title, unit, higher_better) in enumerate(metrics):
        x0 = margin + pi * (panel_w + gap)
        x1 = x0 + panel_w
        vals: List[float] = []
        for row in rows:
            peer = row["peer_mode"]
            vals.extend([metric_mean(row, "text_semantic_like", metric_key), metric_mean(row, "binary", metric_key), metric_mean(row, peer, metric_key)])
        vmax = max(vals + [1.0])
        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y - 12}" text-anchor="middle" font-family="Arial" font-size="15">{metric_title}</text>')
        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            v = vmax * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x1}" y2="{y}" stroke="#f1f1f1"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.0f}</text>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y + panel_h + 48}" text-anchor="middle" font-family="Arial" font-size="12">{unit}</text>')

        group_step = panel_w / gcount
        bar_w = min(24.0, max(10.0, group_step / 5.2))
        for gi, row in enumerate(rows):
            peer = row["peer_mode"]
            cx = x0 + gi * group_step + group_step / 2.0
            for ri, mode in enumerate(["text_semantic_like", "binary", peer]):
                role = "peer" if mode == peer else mode
                value = metric_mean(row, mode, metric_key)
                h = 0 if vmax <= 0 else (value / vmax) * (panel_h * 0.9)
                bx = cx + (ri - 1) * (bar_w + 4) - bar_w / 2.0
                by = panel_y + panel_h - h
                lines.append(f'<rect x="{bx}" y="{by}" width="{bar_w}" height="{h}" fill="{role_color[role]}"/>')
                base = metric_mean(row, "text_semantic_like", metric_key)
                if mode == "text_semantic_like":
                    label = "base"
                    color = "#666"
                else:
                    delta = pct_improve(base, value, higher_better)
                    label = f"{delta:+.1f}%"
                    color = "#1b7837" if delta >= 0 else "#b2182b"
                lines.append(f'<text x="{bx + bar_w/2}" y="{by - 7}" text-anchor="middle" font-family="Arial" font-size="9" fill="{color}">{label}</text>')
            lines.append(f'<text x="{cx}" y="{panel_y + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="10">{row["profile"]}</text>')

    legend_y = height - 28
    lx = margin
    for idx, (label, color) in enumerate([("text_semantic_like", role_color["text_semantic_like"]), ("binary", role_color["binary"]), ("peer semantic_like", role_color["peer"])]):
        x = lx + idx * 170
        lines.append(f'<rect x="{x}" y="{legend_y - 11}" width="12" height="12" fill="{color}"/>')
        lines.append(f'<text x="{x + 18}" y="{legend_y}" font-family="Arial" font-size="12">{label}</text>')
    lines.append('</svg>')

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_multi_svg(rows: List[dict], out_path: str, metric_key: str, title: str, unit: str) -> None:
    width = 1600
    height = 680
    margin = 80
    panel_gap = 30
    panel_w = (width - margin * 2 - panel_gap) / 2.0
    panel_h = 220
    colors = {"text_semantic_like": "#7f7f7f", "binary": "#1f78b4", "peer": "#e31a1c"}
    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="20">{title}</text>')
    lines.append(f'<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="12">strict aligned local multi-device simulation</text>')

    for pi, row in enumerate(rows):
        x0 = margin + (pi % 2) * (panel_w + panel_gap)
        y0 = 100 + (pi // 2) * 270
        peer = row["peer_mode"]
        xs = [float(sc["devices"]) for sc in row["multi"]["scenarios"]]
        ys = []
        for sc in row["multi"]["scenarios"]:
            ys.extend([
                float(sc["summary"]["text_semantic_like"][metric_key]["mean"]),
                float(sc["summary"]["binary"][metric_key]["mean"]),
                float(sc["summary"][peer][metric_key]["mean"]),
            ])
        xmin, xmax = min(xs), max(xs)
        ymin, ymax = min(ys), max(ys)
        if ymax <= ymin:
            ymax = ymin + 1.0
        ypad = max((ymax - ymin) * 0.12, 1.0)
        ymin = max(0.0, ymin - ypad)
        ymax += ypad
        lines.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{y0 - 10}" text-anchor="middle" font-family="Arial" font-size="14">{row["profile"]}</text>')
        for gi in range(6):
            frac = gi / 5.0
            y = y0 + panel_h * (1.0 - frac)
            v = ymin + (ymax - ymin) * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f2f2f2"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.0f}</text>')

        def x_map(v: float) -> float:
            return x0 + ((v - xmin) / (xmax - xmin)) * panel_w if xmax > xmin else x0 + panel_w / 2

        def y_map(v: float) -> float:
            return y0 + panel_h - ((v - ymin) / (ymax - ymin)) * panel_h

        for dev in xs:
            x = x_map(dev)
            lines.append(f'<line x1="{x}" y1="{y0}" x2="{x}" y2="{y0 + panel_h}" stroke="#f9f9f9"/>')
            lines.append(f'<text x="{x}" y="{y0 + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="10">{int(dev)}</text>')

        for mode in ["text_semantic_like", "binary", peer]:
            pts = []
            color = colors["peer"] if mode == peer else colors[mode]
            for sc in row["multi"]["scenarios"]:
                val = float(sc["summary"][mode][metric_key]["mean"])
                pts.append(f"{x_map(float(sc['devices']))},{y_map(val)}")
            lines.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.2" points="{" ".join(pts)}"/>')
            for sc in row["multi"]["scenarios"]:
                val = float(sc["summary"][mode][metric_key]["mean"])
                lines.append(f'<circle cx="{x_map(float(sc["devices"]))}" cy="{y_map(val)}" r="3.5" fill="{color}"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{y0 + panel_h + 42}" text-anchor="middle" font-family="Arial" font-size="11">{unit}</text>')

    legend_y = height - 32
    lx = margin
    for idx, (label, color) in enumerate([("text_semantic_like", colors["text_semantic_like"]), ("binary", colors["binary"]), ("peer semantic_like", colors["peer"])]):
        x = lx + idx * 180
        lines.append(f'<line x1="{x}" y1="{legend_y - 5}" x2="{x + 18}" y2="{legend_y - 5}" stroke="{color}" stroke-width="2.2"/>')
        lines.append(f'<text x="{x + 26}" y="{legend_y}" font-family="Arial" font-size="12">{label}</text>')
    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_direct_delta_svg(rows: List[dict], section_key: str, out_path: str, title: str) -> None:
    metrics = [
        ("time_ms", "Time vs peer (%)", "#1f78b4"),
        ("size_pct", "Space vs peer (%)", "#33a02c"),
        ("thr_pct", "Throughput vs peer (%)", "#e31a1c"),
    ]
    width = 1280
    height = 520
    margin = 90
    panel_gap = 36
    panel_w = (width - margin * 2 - panel_gap * 2) / 3.0
    panel_h = 300
    y0 = 120
    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="21">{title}</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="12">positive means final binary is better than the peer under aligned semantics</text>')

    deltas: Dict[str, Dict[str, float]] = {}
    for row in rows:
        peer = row["peer_mode"]
        if section_key == "single":
            src = row["single"]["summary"]
            deltas[row["profile"]] = {
                "time_ms": pct_improve(float(src[peer]["end_to_end_ms"]["mean"]), float(src["binary"]["end_to_end_ms"]["mean"]), False),
                "size_pct": pct_improve(float(src[peer]["total_bytes"]["mean"]), float(src["binary"]["total_bytes"]["mean"]), False),
                "thr_pct": pct_improve(float(src[peer]["throughput_e2e_rps"]["mean"]), float(src["binary"]["throughput_e2e_rps"]["mean"]), True),
            }
        elif section_key == "l1":
            src = row["l1"]["modes"]
            deltas[row["profile"]] = {
                "time_ms": pct_improve(float(src[peer]["time_ms"]["mean"]), float(src["binary"]["time_ms"]["mean"]), False),
                "size_pct": pct_improve(float(src[peer]["bytes"]["mean"]), float(src["binary"]["bytes"]["mean"]), False),
                "thr_pct": pct_improve(float(src[peer]["throughput_rps"]["mean"]), float(src["binary"]["throughput_rps"]["mean"]), True),
            }

    all_vals = [v for item in deltas.values() for v in item.values()]
    ymax = max(max(all_vals + [5.0]), 5.0)
    ymin = min(min(all_vals + [-5.0]), -5.0)
    bound = max(abs(ymin), abs(ymax))
    ymin, ymax = -bound, bound

    def y_map(v: float) -> float:
        return y0 + panel_h - ((v - ymin) / (ymax - ymin)) * panel_h

    for idx, (metric_key, label, color) in enumerate(metrics):
        x0 = margin + idx * (panel_w + panel_gap)
        lines.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{y0 - 12}" text-anchor="middle" font-family="Arial" font-size="15">{label}</text>')
        zero_y = y_map(0.0)
        lines.append(f'<line x1="{x0}" y1="{zero_y}" x2="{x0 + panel_w}" y2="{zero_y}" stroke="#999" stroke-width="1.2"/>')
        for gi in range(7):
            frac = gi / 6.0
            v = ymin + (ymax - ymin) * frac
            y = y_map(v)
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f3f3f3"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.0f}</text>')

        group_step = panel_w / max(1, len(rows))
        bar_w = min(48.0, max(18.0, group_step * 0.45))
        for gi, row in enumerate(rows):
            profile = row["profile"]
            v = deltas[profile][metric_key]
            cx = x0 + gi * group_step + group_step / 2.0
            by = min(zero_y, y_map(v))
            bh = abs(zero_y - y_map(v))
            lines.append(f'<rect x="{cx - bar_w/2}" y="{by}" width="{bar_w}" height="{bh}" fill="{color}" opacity="0.85"/>')
            label_y = by - 8 if v >= 0 else by + bh + 14
            label_color = "#1b7837" if v >= 0 else "#b2182b"
            lines.append(f'<text x="{cx}" y="{label_y}" text-anchor="middle" font-family="Arial" font-size="10" fill="{label_color}">{v:+.1f}%</text>')
            lines.append(f'<text x="{cx}" y="{y0 + panel_h + 20}" text-anchor="middle" font-family="Arial" font-size="10">{profile}</text>')
    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_l1_overview_svg(rows: List[dict], out_path: str) -> None:
    role_color = {"text_semantic_like": "#7f7f7f", "binary": "#1f78b4", "peer": "#e31a1c"}
    metrics = [
        ("time_ms", "Time", "ms", False),
        ("bytes", "Space (shared counted once)", "bytes (cluster total)", False),
        ("throughput_rps", "Throughput", "records/s", True),
    ]
    gcount = max(1, len(rows))
    panel_w = max(420, gcount * 170)
    panel_h = 320
    gap = 40
    margin = 72
    panel_y = 132
    width = int(margin * 2 + panel_w * 3 + gap * 2)
    height = 560

    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="21">Strict Aligned Real-Device Simulation Overview</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="13">multi-VM nodes; one node = one device; space uses cluster total with shared file counted once</text>')

    for pi, (metric_key, metric_title, unit, higher_better) in enumerate(metrics):
        x0 = margin + pi * (panel_w + gap)
        x1 = x0 + panel_w
        vals: List[float] = []
        for row in rows:
            peer = row["peer_mode"]
            vals.extend([float(row["modes"]["text_semantic_like"][metric_key]["mean"]), float(row["modes"]["binary"][metric_key]["mean"]), float(row["modes"][peer][metric_key]["mean"])])
        vmax = max(vals + [1.0])
        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y - 12}" text-anchor="middle" font-family="Arial" font-size="15">{metric_title}</text>')
        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            v = vmax * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x1}" y2="{y}" stroke="#f1f1f1"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.0f}</text>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y + panel_h + 48}" text-anchor="middle" font-family="Arial" font-size="12">{unit}</text>')

        group_step = panel_w / gcount
        bar_w = min(24.0, max(10.0, group_step / 5.2))
        for gi, row in enumerate(rows):
            peer = row["peer_mode"]
            cx = x0 + gi * group_step + group_step / 2.0
            for ri, mode in enumerate(["text_semantic_like", "binary", peer]):
                role = "peer" if mode == peer else mode
                value = float(row["modes"][mode][metric_key]["mean"])
                h = 0 if vmax <= 0 else (value / vmax) * (panel_h * 0.9)
                bx = cx + (ri - 1) * (bar_w + 4) - bar_w / 2.0
                by = panel_y + panel_h - h
                lines.append(f'<rect x="{bx}" y="{by}" width="{bar_w}" height="{h}" fill="{role_color[role]}"/>')
                base = float(row["modes"]["text_semantic_like"][metric_key]["mean"])
                if mode == "text_semantic_like":
                    label = "base"
                    color = "#666"
                else:
                    delta = pct_improve(base, value, higher_better)
                    label = f"{delta:+.1f}%"
                    color = "#1b7837" if delta >= 0 else "#b2182b"
                lines.append(f'<text x="{bx + bar_w/2}" y="{by - 7}" text-anchor="middle" font-family="Arial" font-size="9" fill="{color}">{label}</text>')
            lines.append(f'<text x="{cx}" y="{panel_y + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="10">{row["profile"]}</text>')
            lines.append(f'<text x="{cx}" y="{panel_y + panel_h + 32}" text-anchor="middle" font-family="Arial" font-size="9">ok {row["nodes_ok"]}/{row["nodes_total"]}</text>')

    legend_y = height - 28
    lx = margin
    for idx, (label, color) in enumerate([("text_semantic_like", role_color["text_semantic_like"]), ("binary", role_color["binary"]), ("peer semantic_like", role_color["peer"])]):
        x = lx + idx * 170
        lines.append(f'<rect x="{x}" y="{legend_y - 11}" width="12" height="12" fill="{color}"/>')
        lines.append(f'<text x="{x + 18}" y="{legend_y}" font-family="Arial" font-size="12">{label}</text>')
    lines.append('</svg>')

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_l1_scan_svg(rows: List[dict], out_path: str, metric_key: str, title: str, unit: str) -> None:
    width = 1600
    height = 680
    margin = 80
    panel_gap = 30
    panel_w = (width - margin * 2 - panel_gap) / 2.0
    panel_h = 220
    colors = {"text_semantic_like": "#7f7f7f", "binary": "#1f78b4", "peer": "#e31a1c"}
    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="20">{title}</text>')
    lines.append(f'<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="12">strict aligned real multi-node simulation; one node = one device</text>')

    for pi, row in enumerate(rows):
        x0 = margin + (pi % 2) * (panel_w + panel_gap)
        y0 = 100 + (pi // 2) * 270
        peer = row["peer_mode"]
        xs = [float(sc["nodes"]) for sc in row["l1_scan"]["scenarios"]]
        ys = []
        for sc in row["l1_scan"]["scenarios"]:
            ys.extend([
                float(sc["modes"]["text_semantic_like"][metric_key]["mean"]),
                float(sc["modes"]["binary"][metric_key]["mean"]),
                float(sc["modes"][peer][metric_key]["mean"]),
            ])
        xmin, xmax = min(xs), max(xs)
        ymin, ymax = min(ys), max(ys)
        if ymax <= ymin:
            ymax = ymin + 1.0
        ypad = max((ymax - ymin) * 0.12, 1.0)
        ymin = max(0.0, ymin - ypad)
        ymax += ypad
        lines.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{y0 - 10}" text-anchor="middle" font-family="Arial" font-size="14">{row["profile"]}</text>')
        for gi in range(6):
            frac = gi / 5.0
            y = y0 + panel_h * (1.0 - frac)
            v = ymin + (ymax - ymin) * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f2f2f2"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.0f}</text>')

        def x_map(v: float) -> float:
            return x0 + ((v - xmin) / (xmax - xmin)) * panel_w if xmax > xmin else x0 + panel_w / 2

        def y_map(v: float) -> float:
            return y0 + panel_h - ((v - ymin) / (ymax - ymin)) * panel_h

        for node_count in xs:
            x = x_map(node_count)
            lines.append(f'<line x1="{x}" y1="{y0}" x2="{x}" y2="{y0 + panel_h}" stroke="#f9f9f9"/>')
            lines.append(f'<text x="{x}" y="{y0 + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="10">{int(node_count)}</text>')

        for mode in ["text_semantic_like", "binary", peer]:
            pts = []
            color = colors["peer"] if mode == peer else colors[mode]
            for sc in row["l1_scan"]["scenarios"]:
                val = float(sc["modes"][mode][metric_key]["mean"])
                pts.append(f"{x_map(float(sc['nodes']))},{y_map(val)}")
            lines.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.2" points="{" ".join(pts)}"/>')
            for sc in row["l1_scan"]["scenarios"]:
                val = float(sc["modes"][mode][metric_key]["mean"])
                lines.append(f'<circle cx="{x_map(float(sc["nodes"]))}" cy="{y_map(val)}" r="3.5" fill="{color}"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{y0 + panel_h + 42}" text-anchor="middle" font-family="Arial" font-size="11">{unit}</text>')

    legend_y = height - 32
    lx = margin
    for idx, (label, color) in enumerate([("text_semantic_like", colors["text_semantic_like"]), ("binary", colors["binary"]), ("peer semantic_like", colors["peer"])]):
        x = lx + idx * 180
        lines.append(f'<line x1="{x}" y1="{legend_y - 5}" x2="{x + 18}" y2="{legend_y - 5}" stroke="{color}" stroke-width="2.2"/>')
        lines.append(f'<text x="{x + 26}" y="{legend_y}" font-family="Arial" font-size="12">{label}</text>')
    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_l1_scan_delta_svg(rows: List[dict], out_path: str, title: str) -> None:
    width = 1600
    height = 680
    margin = 80
    panel_gap = 30
    panel_w = (width - margin * 2 - panel_gap) / 2.0
    panel_h = 220
    metrics = [
        ("time_ms", "Time vs peer (%)", "#1f78b4", False),
        ("bytes", "Space vs peer (%)", "#33a02c", False),
        ("throughput_rps", "Throughput vs peer (%)", "#e31a1c", True),
    ]
    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="20">{title}</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="12">positive means final binary is better than the peer under aligned semantics</text>')

    for pi, row in enumerate(rows):
        x0 = margin + (pi % 2) * (panel_w + panel_gap)
        y0 = 100 + (pi // 2) * 270
        peer = row["peer_mode"]
        xs = [float(sc["nodes"]) for sc in row["l1_scan"]["scenarios"]]
        delta_series = {key: [] for key, _, _, _ in metrics}
        for sc in row["l1_scan"]["scenarios"]:
            for key, _, _, higher_better in metrics:
                bin_v = float(sc["modes"]["binary"][key]["mean"])
                peer_v = float(sc["modes"][peer][key]["mean"])
                delta_series[key].append(pct_improve(peer_v, bin_v, higher_better))
        all_vals = [v for vals in delta_series.values() for v in vals]
        bound = max(5.0, max(abs(v) for v in all_vals) if all_vals else 5.0)
        ymin, ymax = -bound, bound

        lines.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{y0 - 10}" text-anchor="middle" font-family="Arial" font-size="14">{row["profile"]}</text>')

        def x_map(v: float) -> float:
            xmin, xmax = min(xs), max(xs)
            return x0 + ((v - xmin) / (xmax - xmin)) * panel_w if xmax > xmin else x0 + panel_w / 2

        def y_map(v: float) -> float:
            return y0 + panel_h - ((v - ymin) / (ymax - ymin)) * panel_h

        zero_y = y_map(0.0)
        lines.append(f'<line x1="{x0}" y1="{zero_y}" x2="{x0 + panel_w}" y2="{zero_y}" stroke="#999" stroke-width="1.2"/>')
        for gi in range(7):
            frac = gi / 6.0
            v = ymin + (ymax - ymin) * frac
            y = y_map(v)
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f3f3f3"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.0f}</text>')

        for node_count in xs:
            x = x_map(node_count)
            lines.append(f'<line x1="{x}" y1="{y0}" x2="{x}" y2="{y0 + panel_h}" stroke="#f9f9f9"/>')
            lines.append(f'<text x="{x}" y="{y0 + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="10">{int(node_count)}</text>')

        legend_x = x0 + 12
        legend_y = y0 + 18
        for li, (key, label, color, _) in enumerate(metrics):
            pts = []
            for node_count, delta in zip(xs, delta_series[key]):
                pts.append(f"{x_map(node_count)},{y_map(delta)}")
            lines.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.2" points="{" ".join(pts)}"/>')
            for node_count, delta in zip(xs, delta_series[key]):
                lines.append(f'<circle cx="{x_map(node_count)}" cy="{y_map(delta)}" r="3.5" fill="{color}"/>')
            lines.append(f'<line x1="{legend_x}" y1="{legend_y + li * 16}" x2="{legend_x + 18}" y2="{legend_y + li * 16}" stroke="{color}" stroke-width="2.2"/>')
            lines.append(f'<text x="{legend_x + 26}" y="{legend_y + li * 16 + 4}" font-family="Arial" font-size="10">{label}</text>')

    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_report(rows: List[dict], out_path: str, include_l1: bool) -> None:
    lines: List[str] = []
    lines.append("# Final Aligned Comparison Report")
    lines.append("")
    lines.append("## Comparison Design")
    lines.append("")
    lines.append("- Modes: `text_semantic_like`, final `binary`, `peer semantic_like`")
    lines.append("- Binary definition: cached schema/tag cache + per-record CRC32C(hw) + auto-varstr on string-heavy schemas")
    lines.append("- Fairness controls: same schema, same generated values, same platform within each category")
    lines.append("- Categories: single high-load, local multi-device simulation, real-device simulation (multi-VM nodes)")
    lines.append("- Real-device space metric note: `size%` uses cluster total bytes with shared metadata file counted once (not repeated per node).")
    lines.append("- Visuals: single overview + direct binary-vs-peer deltas; multi-device time/throughput/space scans; real-device overview + direct deltas")
    lines.append("")

    lines.append("## Single High-Load")
    lines.append("")
    lines.append("| profile | binary vs text time% | binary vs peer time% | binary vs text size% | binary vs peer size% | binary vs text thr% | binary vs peer thr% |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for row in rows:
        s = row["single"]["summary"]
        peer = row["peer_mode"]
        bt = float(s["binary"]["end_to_end_ms"]["mean"])
        tt = float(s["text_semantic_like"]["end_to_end_ms"]["mean"])
        pt = float(s[peer]["end_to_end_ms"]["mean"])
        bb = float(s["binary"]["total_bytes"]["mean"])
        tb = float(s["text_semantic_like"]["total_bytes"]["mean"])
        pb = float(s[peer]["total_bytes"]["mean"])
        bq = float(s["binary"]["throughput_e2e_rps"]["mean"])
        tq = float(s["text_semantic_like"]["throughput_e2e_rps"]["mean"])
        pq = float(s[peer]["throughput_e2e_rps"]["mean"])
        lines.append(
            f"| {row['profile']} | {pct_improve(tt, bt, False):+.2f}% | {pct_improve(pt, bt, False):+.2f}% | "
            f"{pct_improve(tb, bb, False):+.2f}% | {pct_improve(pb, bb, False):+.2f}% | "
            f"{pct_improve(tq, bq, True):+.2f}% | {pct_improve(pq, bq, True):+.2f}% |"
        )
    lines.append("")

    lines.append("## Multi-Device Simulation")
    lines.append("")
    for row in rows:
        peer = row["peer_mode"]
        lines.append(f"### {row['profile']}")
        lines.append("")
        lines.append("| devices | binary vs text time% | binary vs peer time% | binary vs text thr% | binary vs peer thr% |")
        lines.append("|---|---:|---:|---:|---:|")
        for sc in row["multi"]["scenarios"]:
            text_t = float(sc["summary"]["text_semantic_like"]["elapsed_ms"]["mean"])
            bin_t = float(sc["summary"]["binary"]["elapsed_ms"]["mean"])
            peer_t = float(sc["summary"][peer]["elapsed_ms"]["mean"])
            text_q = float(sc["summary"]["text_semantic_like"]["throughput_rps"]["mean"])
            bin_q = float(sc["summary"]["binary"]["throughput_rps"]["mean"])
            peer_q = float(sc["summary"][peer]["throughput_rps"]["mean"])
            lines.append(
                f"| {sc['devices']} | {pct_improve(text_t, bin_t, False):+.2f}% | {pct_improve(peer_t, bin_t, False):+.2f}% | "
                f"{pct_improve(text_q, bin_q, True):+.2f}% | {pct_improve(peer_q, bin_q, True):+.2f}% |"
            )
        lines.append("")

    if include_l1:
        lines.append("## Real-Device Simulation (Multi-VM Nodes)")
        lines.append("")
        lines.append("| profile | nodes ok/total | binary vs text time% | binary vs peer time% | binary vs text size% | binary vs peer size% | binary vs text thr% | binary vs peer thr% |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for row in rows:
            l1 = row["l1"]
            peer = row["peer_mode"]
            bt = float(l1["modes"]["binary"]["time_ms"]["mean"])
            tt = float(l1["modes"]["text_semantic_like"]["time_ms"]["mean"])
            pt = float(l1["modes"][peer]["time_ms"]["mean"])
            bb = float(l1["modes"]["binary"]["bytes"]["mean"])
            tb = float(l1["modes"]["text_semantic_like"]["bytes"]["mean"])
            pb = float(l1["modes"][peer]["bytes"]["mean"])
            bq = float(l1["modes"]["binary"]["throughput_rps"]["mean"])
            tq = float(l1["modes"]["text_semantic_like"]["throughput_rps"]["mean"])
            pq = float(l1["modes"][peer]["throughput_rps"]["mean"])
            lines.append(
                f"| {row['profile']} | {l1['nodes_ok']}/{l1['nodes_total']} | {pct_improve(tt, bt, False):+.2f}% | {pct_improve(pt, bt, False):+.2f}% | "
                f"{pct_improve(tb, bb, False):+.2f}% | {pct_improve(pb, bb, False):+.2f}% | "
                f"{pct_improve(tq, bq, True):+.2f}% | {pct_improve(pq, bq, True):+.2f}% |"
            )
        lines.append("")
        lines.append("## Real-Device Node-Scale Scan")
        lines.append("")
        for row in rows:
            peer = row["peer_mode"]
            lines.append(f"### {row['profile']}")
            lines.append("")
            lines.append("| nodes | binary vs text time% | binary vs peer time% | binary vs text thr% | binary vs peer thr% | binary vs peer size% |")
            lines.append("|---|---:|---:|---:|---:|---:|")
            for sc in row["l1_scan"]["scenarios"]:
                text_t = float(sc["modes"]["text_semantic_like"]["time_ms"]["mean"])
                bin_t = float(sc["modes"]["binary"]["time_ms"]["mean"])
                peer_t = float(sc["modes"][peer]["time_ms"]["mean"])
                text_q = float(sc["modes"]["text_semantic_like"]["throughput_rps"]["mean"])
                bin_q = float(sc["modes"]["binary"]["throughput_rps"]["mean"])
                peer_q = float(sc["modes"][peer]["throughput_rps"]["mean"])
                peer_b = float(sc["modes"][peer]["bytes"]["mean"])
                bin_b = float(sc["modes"]["binary"]["bytes"]["mean"])
                lines.append(
                    f"| {sc['nodes']} | {pct_improve(text_t, bin_t, False):+.2f}% | {pct_improve(peer_t, bin_t, False):+.2f}% | "
                    f"{pct_improve(text_q, bin_q, True):+.2f}% | {pct_improve(peer_q, bin_q, True):+.2f}% | {pct_improve(peer_b, bin_b, False):+.2f}% |"
                )
            lines.append("")

    lines.append("## Interpretation Summary")
    lines.append("")
    lines.append("- Single high-load isolates local encoding cost and shows whether final binary can beat peer hot paths under equal semantics.")
    lines.append("- Local multi-device simulation adds scheduler and shared-file competition while keeping platform constant.")
    lines.append("- Real-device simulation adds node-level execution skew and transport/noise, so it is the closest engineering deployment proxy in this thesis.")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    args = parse_args()
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = args.out_dir.strip() or os.path.join(RESULTS_ROOT, f"final_aligned_suite_{ts}")
    single_root = os.path.join(out_root, "single")
    multi_root = os.path.join(out_root, "multi")
    l1_root = os.path.join(out_root, "l1")
    merged_root = os.path.join(out_root, "merged")
    os.makedirs(single_root, exist_ok=True)
    os.makedirs(multi_root, exist_ok=True)
    os.makedirs(l1_root, exist_ok=True)
    os.makedirs(merged_root, exist_ok=True)

    rows = []
    for profile in PROFILES:
        single_out = os.path.join(single_root, profile["name"])
        multi_out = os.path.join(multi_root, profile["name"])
        ensure_clean_dir(single_out)
        ensure_clean_dir(multi_out)
        single = run_single_profile(profile, single_out, args)
        multi = run_multi_profile(profile, multi_out, args)
        rows.append({"profile": profile["name"], "peer_mode": profile["peer_mode"], "single": single, "multi": multi})

    include_l1 = not args.skip_l1
    if include_l1:
        for row in rows:
            profile = next(p for p in PROFILES if p["name"] == row["profile"])
            dst_dir = os.path.join(l1_root, profile["name"])
            ensure_clean_dir(dst_dir)
            row["l1_scan"] = run_l1_scan_profile(profile, dst_dir, args, ts)
            row["l1"] = row["l1_scan"]["scenarios"][-1]

    summary_path = os.path.join(merged_root, "final_aligned_summary.json")
    save_json(summary_path, {"generated_at": ts, "rows": rows})

    single_svg = os.path.join(merged_root, "single_aligned_overview.svg")
    build_single_overview_svg(rows, single_svg)
    single_delta_svg = os.path.join(merged_root, "single_binary_vs_peer.svg")
    build_direct_delta_svg(rows, "single", single_delta_svg, "Strict Aligned Single: Final Binary vs Peer")
    multi_time_svg = os.path.join(merged_root, "multi_time_scan.svg")
    build_multi_svg(rows, multi_time_svg, "elapsed_ms", "Strict Aligned Multi-Device Time Scan", "ms")
    multi_thr_svg = os.path.join(merged_root, "multi_throughput_scan.svg")
    build_multi_svg(rows, multi_thr_svg, "throughput_rps", "Strict Aligned Multi-Device Throughput Scan", "records/s")
    multi_space_svg = os.path.join(merged_root, "multi_space_scan.svg")
    build_multi_svg(rows, multi_space_svg, "total_bytes", "Strict Aligned Multi-Device Space Scan", "bytes")
    l1_svg = None
    l1_delta_svg = None
    if include_l1:
        l1_rows = [row["l1"] for row in rows]
        l1_svg = os.path.join(merged_root, "l1_aligned_overview.svg")
        build_l1_overview_svg(l1_rows, l1_svg)
        l1_delta_svg = os.path.join(merged_root, "l1_binary_vs_peer.svg")
        build_direct_delta_svg(rows, "l1", l1_delta_svg, "Strict Aligned Real-Device: Final Binary vs Peer")
        l1_time_scan_svg = os.path.join(merged_root, "l1_node_time_scan.svg")
        build_l1_scan_svg(rows, l1_time_scan_svg, "time_ms", "Strict Aligned Real-Device Time Scan", "ms")
        l1_thr_scan_svg = os.path.join(merged_root, "l1_node_throughput_scan.svg")
        build_l1_scan_svg(rows, l1_thr_scan_svg, "throughput_rps", "Strict Aligned Real-Device Throughput Scan", "records/s")
        l1_space_scan_svg = os.path.join(merged_root, "l1_node_space_scan.svg")
        build_l1_scan_svg(
            rows,
            l1_space_scan_svg,
            "bytes",
            "Strict Aligned Real-Device Cluster Space Scan (shared counted once)",
            "bytes (cluster total, shared counted once)",
        )
        l1_scan_delta_svg = os.path.join(merged_root, "l1_node_binary_vs_peer_scan.svg")
        build_l1_scan_delta_svg(rows, l1_scan_delta_svg, "Strict Aligned Real-Device: Binary vs Peer Across Node Scales")
    report_md = os.path.join(merged_root, "final_aligned_report.md")
    build_report(rows, report_md, include_l1)

    latest = os.path.join(RESULTS_ROOT, "final_aligned_suite_latest")
    if os.path.islink(latest) or os.path.exists(latest):
        if os.path.islink(latest):
            os.unlink(latest)
        elif os.path.isdir(latest):
            shutil.rmtree(latest)
        else:
            os.remove(latest)
    os.symlink(out_root, latest)

    print("saved", summary_path)
    print("saved", single_svg)
    print("saved", single_delta_svg)
    print("saved", multi_time_svg)
    print("saved", multi_thr_svg)
    print("saved", multi_space_svg)
    if l1_svg:
        print("saved", l1_svg)
    if l1_delta_svg:
        print("saved", l1_delta_svg)
    if include_l1:
        print("saved", os.path.join(merged_root, "l1_node_time_scan.svg"))
        print("saved", os.path.join(merged_root, "l1_node_throughput_scan.svg"))
        print("saved", os.path.join(merged_root, "l1_node_space_scan.svg"))
        print("saved", os.path.join(merged_root, "l1_node_binary_vs_peer_scan.svg"))
    print("saved", report_md)
    print("saved", latest)


if __name__ == "__main__":
    main()
