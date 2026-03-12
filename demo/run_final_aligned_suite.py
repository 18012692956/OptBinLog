#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import math
import os
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run final aligned comparison suite across single/multi/L1.")
    p.add_argument("--out-dir", default="", help="Output root directory")
    p.add_argument("--bench-bin", default=os.path.join(ROOT, "optbinlog_bench_macos"))
    p.add_argument("--single-records", type=int, default=20000)
    p.add_argument("--single-repeats", type=int, default=5)
    p.add_argument("--single-warmup", type=int, default=1)
    p.add_argument("--multi-records-per-device", type=int, default=800)
    p.add_argument("--multi-devices", default="2,5,10")
    p.add_argument("--multi-repeats", type=int, default=3)
    p.add_argument("--multi-warmup", type=int, default=1)
    p.add_argument("--l1-template", default=os.path.join(ROOT, "l1_config.linux_10_all_unaligned_initrace.json"))
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
    devices_list = [int(x.strip()) for x in args.multi_devices.split(",") if x.strip()]
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


def prepare_l1_config(template_path: str, out_path: str, profile: dict, args: argparse.Namespace) -> str:
    cfg = copy.deepcopy(load_json(template_path))
    cfg["tag"] = f"aligned_l1_{profile['name']}"
    cfg["parallel"] = True
    cfg["max_workers"] = int(args.l1_max_workers)
    cfg["start_sync_delay_s"] = float(args.l1_start_sync_delay)
    eventlog_dir = os.path.join(ROOT, profile["eventlog_dir"])
    modes = ",".join(MAIN_MODES + [profile["peer_mode"]])
    for node in cfg.get("nodes", []):
        node["eventlog_dir"] = eventlog_dir
        node["records"] = int(args.l1_records)
        node["repeats"] = int(args.l1_repeats)
        node["warmup"] = int(args.l1_warmup)
        node["modes"] = modes
        node["baseline"] = "text_semantic_like"
        node["bench_bin"] = "./optbinlog_bench_linux"
        node["bench_prefix"] = ""
        node["text_profile"] = "semantic"
        node.pop("trace_marker", None)
        node.pop("syslog_source", None)
        if args.l1_disable_netem:
            node.pop("netem", None)
    save_json(out_path, cfg)
    return out_path


def run_l1_profile(config_path: str, tag: str) -> str:
    run_cmd(["python3", os.path.join(ROOT, "run_l1_suite.py"), "--config", config_path, "--tag", tag], cwd=ROOT)
    return os.path.join(ROOT, "results", tag, "l1_summary.json")


def extract_l1_profile(path: str, profile: dict) -> dict:
    data = load_json(path)
    modes = MAIN_MODES + [profile["peer_mode"]]
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
        svals = []
        thvals = []
        for n in usable_nodes:
            bm = n.get("summary", {}).get("by_mode", {}).get(mode, {})
            if bm:
                tvals.append(float(bm.get("end_to_end_ms_mean", 0.0)))
                svals.append(float(bm.get("total_bytes_mean", 0.0)))
                thvals.append(float(bm.get("throughput_e2e_rps_mean", 0.0)))
        tvals = iqr_filter_values(tvals)
        svals = iqr_filter_values(svals)
        thvals = iqr_filter_values(thvals)
        by_mode[mode] = {
            "time_ms": metric_stats(tvals),
            "bytes": metric_stats(svals),
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
        ("bytes", "Space", "bytes", False),
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
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="13">multi-VM nodes; one node = one device; bars show node-aggregated IQR-filtered means</text>')

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
            cfg_path = os.path.join(l1_root, f"config_{profile['name']}.json")
            prepare_l1_config(args.l1_template, cfg_path, profile, args)
            dst_dir = os.path.join(l1_root, profile["name"])
            ensure_clean_dir(dst_dir)
            tag = f"final_aligned_l1_{profile['name']}_{ts}"
            summary_path = run_l1_profile(cfg_path, tag)
            l1_src_dir = os.path.join(ROOT, "results", tag)
            if os.path.exists(dst_dir):
                shutil.rmtree(dst_dir)
            shutil.copytree(l1_src_dir, dst_dir)
            row["l1"] = extract_l1_profile(summary_path, profile)

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
    print("saved", report_md)
    print("saved", latest)


if __name__ == "__main__":
    main()
