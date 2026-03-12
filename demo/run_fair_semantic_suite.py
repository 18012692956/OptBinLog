#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import math
import os
import shutil
import statistics
import subprocess
import time
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

SINGLE_MODES = [
    "text_semantic_like",
    "binary",
    "binary_crc32_legacy",
    "binary_hotpath",
    "binary_nocrc",
    "binary_varstr",
    "binary_nocrc_varstr",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run strict fair semantic suite with optbinlog ablations.")
    p.add_argument("--out-dir", default="", help="Output root directory")
    p.add_argument("--bench-bin", default=os.path.join(ROOT, "optbinlog_bench_macos"))
    p.add_argument("--build", action="store_true", help="Build the local benchmark binary before running")
    p.add_argument("--single-records", type=int, default=30000)
    p.add_argument("--single-repeats", type=int, default=5)
    p.add_argument("--single-warmup", type=int, default=1)
    p.add_argument("--multi-records-per-device", type=int, default=1200)
    p.add_argument("--multi-devices", default="2,5,10")
    p.add_argument("--multi-repeats", type=int, default=3)
    p.add_argument("--multi-warmup", type=int, default=1)
    return p.parse_args()


def run_cmd(cmd: List[str], cwd: str = ROOT, env: Dict[str, str] = None) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed\ncmd: {}\nstdout:\n{}\nstderr:\n{}".format(" ".join(cmd), proc.stdout, proc.stderr)
        )
    return proc


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


def iqr_filter(rows: List[dict], field: str) -> List[dict]:
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
        "binary_crc32_legacy": "bcrc",
        "binary_hotpath": "bhot",
        "binary_nocrc": "bnc",
        "binary_varstr": "bvs",
        "binary_nocrc_varstr": "bnv",
        "nanolog_semantic_like": "nslog",
        "zephyr_deferred_semantic_like": "zslog",
        "ulog_semantic_like": "uslog",
        "hilog_semantic_like": "hslog",
    }.get(mode, "out")


def ensure_clean_dir(path: str) -> None:
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def run_single_profile(profile: dict, out_dir: str, args: argparse.Namespace) -> dict:
    modes = SINGLE_MODES + [profile["peer_mode"]]
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
    start = time.monotonic_ns()
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
    end = time.monotonic_ns()
    elapsed_ms = (end - start) / 1e6
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
    modes = ["text_semantic_like", "binary", profile["peer_mode"]]
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
            kept = iqr_filter(mode_rows, "elapsed_ms")
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


def pct_improve(base: float, cur: float, higher_better: bool) -> float:
    if base == 0:
        return 0.0
    if higher_better:
        return (cur - base) / base * 100.0
    return (base - cur) / base * 100.0


def build_phase_svg(single_rows: List[dict], out_path: str) -> None:
    modes = ["binary_crc32_legacy", "binary", "binary_hotpath", "binary_nocrc", "binary_varstr", "binary_nocrc_varstr"]
    width = 1600
    height = 680
    margin = 70
    panel_gap = 30
    panel_w = (width - margin * 2 - panel_gap) / 2.0
    panel_h = 220
    palette = {"prep_ms": "#8da0cb", "write_only_ms": "#fc8d62", "post_ms": "#66c2a5"}

    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="20">Optbinlog Official Build Ablation Breakdown</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="12">single-run mean; stacked bars = prep + write + post</text>')

    for pi, row in enumerate(single_rows):
        x0 = margin + (pi % 2) * (panel_w + panel_gap)
        y0 = 100 + (pi // 2) * 270
        ymax = max(
            row["single"]["summary"][m]["end_to_end_ms"]["mean"] for m in modes if m in row["single"]["summary"]
        )
        if ymax <= 0:
            ymax = 1.0
        lines.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{y0 - 10}" text-anchor="middle" font-family="Arial" font-size="14">{row["profile"]}</text>')
        for gi in range(6):
            frac = gi / 5.0
            y = y0 + panel_h * (1.0 - frac)
            val = ymax * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f0f0f0"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{val:.1f}</text>')
        step = panel_w / len(modes)
        bar_w = min(46, step * 0.62)
        for mi, mode in enumerate(modes):
            s = row["single"]["summary"][mode]
            prep = float(s["prep_ms"]["mean"])
            write = float(s["write_only_ms"]["mean"])
            post = float(s["post_ms"]["mean"])
            x = x0 + mi * step + (step - bar_w) / 2.0
            y_cursor = y0 + panel_h
            for key, val in [("prep_ms", prep), ("write_only_ms", write), ("post_ms", post)]:
                h = (val / ymax) * (panel_h * 0.9) if ymax > 0 else 0
                y_cursor -= h
                lines.append(f'<rect x="{x}" y="{y_cursor}" width="{bar_w}" height="{max(h,1.0)}" fill="{palette[key]}"/>')
            lines.append(f'<text x="{x + bar_w/2}" y="{y0 + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="10">{mode}</text>')
            lines.append(f'<text x="{x + bar_w/2}" y="{y_cursor - 6}" text-anchor="middle" font-family="Arial" font-size="10">{prep + write + post:.1f}</text>')

    legend_y = height - 32
    lx = margin
    for idx, (key, label) in enumerate([("prep_ms", "prep"), ("write_only_ms", "write"), ("post_ms", "post")]):
        x = lx + idx * 120
        lines.append(f'<rect x="{x}" y="{legend_y - 11}" width="12" height="12" fill="{palette[key]}"/>')
        lines.append(f'<text x="{x + 18}" y="{legend_y}" font-family="Arial" font-size="12">{label}</text>')
    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_pareto_svg(single_rows: List[dict], out_path: str) -> None:
    chosen = ["text_semantic_like", "binary", "binary_nocrc_varstr"]
    width = 1600
    height = 680
    margin = 80
    panel_gap = 30
    panel_w = (width - margin * 2 - panel_gap) / 2.0
    panel_h = 220
    colors = {
        "text_semantic_like": "#7f7f7f",
        "binary": "#1f78b4",
        "binary_nocrc_varstr": "#33a02c",
        "peer": "#e31a1c",
    }

    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="20">Strict-Fair Pareto View</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="12">single-run mean; lower bytes/record and higher throughput are better</text>')

    for pi, row in enumerate(single_rows):
        summary = row["single"]["summary"]
        peer = row["peer_mode"]
        x0 = margin + (pi % 2) * (panel_w + panel_gap)
        y0 = 100 + (pi // 2) * 270
        modes = chosen + [peer]
        xs = []
        ys = []
        for mode in modes:
            s = summary[mode]
            xs.append(float(s["total_bytes"]["mean"]) / float(row["single"]["config"]["records"]))
            ys.append(float(s["throughput_e2e_rps"]["mean"]))
        xmin, xmax = min(xs), max(xs)
        ymin, ymax = min(ys), max(ys)
        if xmax <= xmin:
            xmax = xmin + 1.0
        if ymax <= ymin:
            ymax = ymin + 1.0
        xpad = max((xmax - xmin) * 0.12, 1e-6)
        ypad = max((ymax - ymin) * 0.12, 1e-6)
        xmin -= xpad
        xmax += xpad
        ymin -= ypad
        ymax += ypad
        lines.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{y0 - 10}" text-anchor="middle" font-family="Arial" font-size="14">{row["profile"]}</text>')
        for gi in range(6):
            frac = gi / 5.0
            y = y0 + panel_h * (1.0 - frac)
            v = ymin + (ymax - ymin) * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f2f2f2"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.0f}</text>')
        for gi in range(6):
            frac = gi / 5.0
            x = x0 + panel_w * frac
            v = xmin + (xmax - xmin) * frac
            lines.append(f'<line x1="{x}" y1="{y0}" x2="{x}" y2="{y0 + panel_h}" stroke="#f9f9f9"/>')
            lines.append(f'<text x="{x}" y="{y0 + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="10">{v:.1f}</text>')
        for mode in modes:
            s = summary[mode]
            xval = float(s["total_bytes"]["mean"]) / float(row["single"]["config"]["records"])
            yval = float(s["throughput_e2e_rps"]["mean"])
            x = x0 + ((xval - xmin) / (xmax - xmin)) * panel_w
            y = y0 + panel_h - ((yval - ymin) / (ymax - ymin)) * panel_h
            color = colors["peer"] if mode == peer else colors[mode]
            lines.append(f'<circle cx="{x}" cy="{y}" r="6" fill="{color}"/>')
            lines.append(f'<text x="{x + 8}" y="{y - 8}" font-family="Arial" font-size="10">{mode}</text>')

    legend_y = height - 32
    lx = margin
    for idx, (label, color) in enumerate(
        [
            ("text_semantic_like", colors["text_semantic_like"]),
            ("binary", colors["binary"]),
            ("binary_nocrc_varstr", colors["binary_nocrc_varstr"]),
            ("peer semantic_like", colors["peer"]),
        ]
    ):
        x = lx + idx * 180
        lines.append(f'<circle cx="{x}" cy="{legend_y - 5}" r="5" fill="{color}"/>')
        lines.append(f'<text x="{x + 12}" y="{legend_y}" font-family="Arial" font-size="12">{label}</text>')
    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_multi_scan_svg(rows: List[dict], out_path: str) -> None:
    width = 1600
    height = 680
    margin = 80
    panel_gap = 30
    panel_w = (width - margin * 2 - panel_gap) / 2.0
    panel_h = 220
    colors = {"text_semantic_like": "#7f7f7f", "binary": "#1f78b4", "peer": "#e31a1c"}

    lines = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">']
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="20">Strict-Fair Multi-Device Scan</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="12">throughput mean vs devices; strict fair modes only</text>')

    for pi, row in enumerate(rows):
        x0 = margin + (pi % 2) * (panel_w + panel_gap)
        y0 = 100 + (pi // 2) * 270
        peer = row["peer_mode"]
        xs = [float(sc["devices"]) for sc in row["multi"]["scenarios"]]
        ys = []
        for sc in row["multi"]["scenarios"]:
            ys.extend(
                [
                    float(sc["summary"]["text_semantic_like"]["throughput_rps"]["mean"]),
                    float(sc["summary"]["binary"]["throughput_rps"]["mean"]),
                    float(sc["summary"][peer]["throughput_rps"]["mean"]),
                ]
            )
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
            for sc in row["multi"]["scenarios"]:
                y = float(sc["summary"][mode]["throughput_rps"]["mean"])
                pts.append(f"{x_map(float(sc['devices']))},{y_map(y)}")
            color = colors["peer"] if mode == peer else colors[mode]
            lines.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.2" points="{" ".join(pts)}"/>')
            for sc in row["multi"]["scenarios"]:
                y = float(sc["summary"][mode]["throughput_rps"]["mean"])
                lines.append(f'<circle cx="{x_map(float(sc["devices"]))}" cy="{y_map(y)}" r="3.5" fill="{color}"/>')

    legend_y = height - 32
    lx = margin
    for idx, (label, color) in enumerate(
        [("text_semantic_like", colors["text_semantic_like"]), ("binary", colors["binary"]), ("peer semantic_like", colors["peer"])]
    ):
        x = lx + idx * 180
        lines.append(f'<line x1="{x}" y1="{legend_y - 5}" x2="{x + 18}" y2="{legend_y - 5}" stroke="{color}" stroke-width="2.2"/>')
        lines.append(f'<text x="{x + 26}" y="{legend_y}" font-family="Arial" font-size="12">{label}</text>')
    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_report(rows: List[dict], out_path: str) -> None:
    lines = ["# Strict Fair Semantic Suite Report", ""]
    lines.append("## Fairness Matrix")
    lines.append("")
    lines.append("| profile | baseline | binary | peer | same schema source | same generated values | same platform | note |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for row in rows:
        lines.append(
            f"| {row['profile']} | text_semantic_like | binary(default=cache+CRC32C+auto-varstr) | {row['peer_mode']} | yes | yes | local macOS | strict fair semantic tier |"
        )
    lines.append("")
    lines.append("## Single Summary")
    lines.append("")
    for row in rows:
        summary = row["single"]["summary"]
        peer = row["peer_mode"]
        base_t = float(summary["binary"]["end_to_end_ms"]["mean"])
        base_bytes = float(summary["binary"]["total_bytes"]["mean"])
        base_thr = float(summary["binary"]["throughput_e2e_rps"]["mean"])
        legacy_t = float(summary["binary_crc32_legacy"]["end_to_end_ms"]["mean"])
        ncrc_t = float(summary["binary_nocrc"]["end_to_end_ms"]["mean"])
        var_t = float(summary["binary_varstr"]["end_to_end_ms"]["mean"])
        both_t = float(summary["binary_nocrc_varstr"]["end_to_end_ms"]["mean"])
        hot_prep = float(summary["binary"]["prep_ms"]["mean"])
        legacy_prep = float(summary["binary_crc32_legacy"]["prep_ms"]["mean"])
        auto_varstr_size = pct_improve(float(summary["binary_varstr"]["total_bytes"]["mean"]), base_bytes, False)
        lines.append(f"### {row['profile']}")
        lines.append("")
        lines.append(
            f"- peer direct compare: time={pct_improve(float(summary[peer]['end_to_end_ms']['mean']), base_t, False):+.2f}% "
            f"size={pct_improve(float(summary[peer]['total_bytes']['mean']), base_bytes, False):+.2f}% "
            f"throughput={pct_improve(float(summary[peer]['throughput_e2e_rps']['mean']), base_thr, True):+.2f}%"
        )
        lines.append(f"- official binary vs CRC32 legacy: {pct_improve(legacy_t, base_t, False):+.2f}% end-to-end, prep {legacy_prep:.3f} -> {hot_prep:.3f} ms")
        lines.append(f"- no-crc time delta vs binary: {pct_improve(base_t, ncrc_t, False):+.2f}%")
        lines.append(f"- varstr time delta vs binary: {pct_improve(base_t, var_t, False):+.2f}%")
        lines.append(f"- no-crc+varstr time delta vs binary: {pct_improve(base_t, both_t, False):+.2f}%")
        lines.append(f"- force-varstr size delta vs binary: {auto_varstr_size:+.2f}%")
        lines.append("")

    lines.append("## Multi Summary")
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

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    args = parse_args()
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = args.out_dir.strip() or os.path.join(RESULTS_ROOT, f"fair_semantic_suite_{ts}")
    single_root = os.path.join(out_root, "single")
    multi_root = os.path.join(out_root, "multi")
    merged_root = os.path.join(out_root, "merged")
    os.makedirs(single_root, exist_ok=True)
    os.makedirs(multi_root, exist_ok=True)
    os.makedirs(merged_root, exist_ok=True)

    if args.build:
        run_cmd(
            [
                "clang",
                "-O2",
                "-Wall",
                "-Wextra",
                "-std=c11",
                "-Iinclude",
                "-o",
                os.path.basename(args.bench_bin),
                "optbinlog_bench.c",
                "src/optbinlog_shared.c",
                "src/optbinlog_eventlog.c",
                "src/optbinlog_binlog.c",
            ]
        )

    rows = []
    for profile in PROFILES:
        single_out = os.path.join(single_root, profile["name"])
        multi_out = os.path.join(multi_root, profile["name"])
        ensure_clean_dir(single_out)
        ensure_clean_dir(multi_out)
        single = run_single_profile(profile, single_out, args)
        multi = run_multi_profile(profile, multi_out, args)
        rows.append({"profile": profile["name"], "peer_mode": profile["peer_mode"], "single": single, "multi": multi})

    summary_path = os.path.join(merged_root, "fair_semantic_suite_summary.json")
    save_json(summary_path, {"generated_at": ts, "rows": rows})

    phase_svg = os.path.join(merged_root, "ablation_phase_overview.svg")
    build_phase_svg(rows, phase_svg)
    pareto_svg = os.path.join(merged_root, "single_pareto.svg")
    build_pareto_svg(rows, pareto_svg)
    multi_svg = os.path.join(merged_root, "multi_scan.svg")
    build_multi_scan_svg(rows, multi_svg)
    report_md = os.path.join(merged_root, "fair_semantic_suite_report.md")
    build_report(rows, report_md)

    latest = os.path.join(RESULTS_ROOT, "fair_semantic_suite_latest")
    if os.path.islink(latest) or os.path.exists(latest):
        if os.path.islink(latest):
            os.unlink(latest)
        elif os.path.isdir(latest):
            shutil.rmtree(latest)
        else:
            os.remove(latest)
    os.symlink(out_root, latest)

    print("saved", summary_path)
    print("saved", phase_svg)
    print("saved", pareto_svg)
    print("saved", multi_svg)
    print("saved", report_md)
    print("saved", latest)


if __name__ == "__main__":
    main()
