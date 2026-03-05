#!/usr/bin/env python3
import json
import math
import os
import statistics
import subprocess
from typing import Dict, List

ROOT = os.path.dirname(__file__)
EVENTLOG_DIR = os.path.join(ROOT, "eventlogst")
EVENTLOG_DIR_MIN = os.path.join(ROOT, "eventlogst_semantic_min")
OUT_DIR = os.environ.get("OPTBINLOG_RESEARCH_OUT_DIR", os.path.join(ROOT, "results", "research_compare"))
RUN_DIR = os.path.join(OUT_DIR, "runs")
SHARED = os.path.join(OUT_DIR, "shared_eventtag.bin")

RECORDS = int(os.environ.get("OPTBINLOG_RESEARCH_RECORDS", "80000"))
REPEATS = int(os.environ.get("OPTBINLOG_RESEARCH_REPEATS", "8"))
WARMUP = int(os.environ.get("OPTBINLOG_RESEARCH_WARMUP", "1"))
IQR_MULT = float(os.environ.get("OPTBINLOG_RESEARCH_IQR_MULT", "1.5"))
BASELINE = os.environ.get("OPTBINLOG_RESEARCH_BASELINE", "text")

os.makedirs(RUN_DIR, exist_ok=True)

BENCH_BIN = os.path.join(ROOT, "optbinlog_bench")
REPRO_BIN = os.path.join(ROOT, "research", "research_prototypes")
RESULT_JSON = os.path.join(OUT_DIR, "research_compare_result.json")
RESULT_SVG = os.path.join(OUT_DIR, "research_compare_result.svg")
STATS_SVG = os.path.join(OUT_DIR, "research_compare_stats.svg")


MODE_ORDER = [
    "text",
    "binary",
    "syslog",
    "research_text",
    "nanolog_like",
    "zephyr_deferred_like",
    "ulog_async_like",
    "hilog_lite_like",
    "nanolog_semantic_like",
    "zephyr_deferred_semantic_like",
    "binary_semantic_like",
]

MODE_LABEL = {
    "text": "text",
    "binary": "optbinlog binary",
    "syslog": "syslog",
    "research_text": "research text",
    "nanolog_like": "nanolog-like",
    "zephyr_deferred_like": "zephyr-deferred-like",
    "ulog_async_like": "ulog-async-like",
    "hilog_lite_like": "hilog-lite-like",
    "nanolog_semantic_like": "nanolog-semantic-like",
    "zephyr_deferred_semantic_like": "zephyr-deferred-semantic-like",
    "binary_semantic_like": "optbinlog-binary-semantic-like",
}


def run(cmd: List[str], cwd: str = ROOT) -> str:
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"cmd failed: {' '.join(cmd)}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}")
    return p.stdout


def build_bins() -> None:
    run([
        "clang",
        "-O2",
        "-Wall",
        "-Wextra",
        "-std=c11",
        "-Iinclude",
        "-o",
        BENCH_BIN,
        "optbinlog_bench.c",
        "src/optbinlog_shared.c",
        "src/optbinlog_eventlog.c",
        "src/optbinlog_binlog.c",
    ])
    run([
        "clang",
        "-O2",
        "-Wall",
        "-Wextra",
        "-std=c11",
        "-pthread",
        "-o",
        REPRO_BIN,
        os.path.join("research", "research_prototypes.c"),
    ])


def parse_line(line: str) -> Dict[str, str]:
    parts = line.strip().split(",")
    out = {}
    for i in range(0, len(parts) - 1, 2):
        out[parts[i]] = parts[i + 1]
    return out


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
        }
    n = len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values) if n > 1 else 0.0
    ci = 1.96 * std / math.sqrt(n) if n > 1 else 0.0
    return {
        "n": n,
        "mean": mean,
        "median": statistics.median(values),
        "p95": percentile(values, 95),
        "std": std,
        "ci95_low": mean - ci,
        "ci95_high": mean + ci,
    }


def iqr_filter(rows: List[Dict[str, float]], field: str = "end_to_end_ms"):
    vals = [r[field] for r in rows]
    if len(vals) < 4:
        return rows, {"method": "none", "kept": len(rows), "removed": 0}
    q1 = percentile(vals, 25)
    q3 = percentile(vals, 75)
    iqr = q3 - q1
    lo = q1 - IQR_MULT * iqr
    hi = q3 + IQR_MULT * iqr
    kept = [r for r in rows if lo <= r[field] <= hi]
    if len(kept) < max(3, len(rows) // 2):
        return rows, {
            "method": "iqr_fallback_all",
            "kept": len(rows),
            "removed": 0,
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower": lo,
            "upper": hi,
        }
    return kept, {
        "method": "iqr",
        "kept": len(kept),
        "removed": len(rows) - len(kept),
        "q1": q1,
        "q3": q3,
        "iqr": iqr,
        "lower": lo,
        "upper": hi,
    }


def run_once(mode: str, idx: int, warmup: bool = False):
    suffix = "warmup" if warmup else "run"
    out_path = os.path.join(RUN_DIR, f"{mode}_{suffix}_{idx:03d}.out")
    if mode in {"text", "binary", "syslog", "binary_semantic_like"}:
        bench_mode = "binary" if mode == "binary_semantic_like" else mode
        eventlog_dir = EVENTLOG_DIR_MIN if mode == "binary_semantic_like" else EVENTLOG_DIR
        cmd = [
            BENCH_BIN,
            "--mode",
            bench_mode,
            "--eventlog-dir",
            eventlog_dir,
            "--out",
            out_path,
            "--records",
            str(RECORDS),
            "--shared",
            SHARED,
        ]
        env = os.environ.copy()
        if mode == "syslog" and "OPTBINLOG_SYSLOG_PRIO" not in env:
            env["OPTBINLOG_SYSLOG_PRIO"] = "7"
        p = subprocess.run(cmd, capture_output=True, text=True, env=env)
    else:
        cmd = [
            REPRO_BIN,
            "--mode",
            mode,
            "--out",
            out_path,
            "--records",
            str(RECORDS),
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)

    if p.returncode != 0:
        raise RuntimeError(f"mode={mode} failed\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}")

    line = p.stdout.strip().splitlines()[-1]
    rec = parse_line(line)
    end_to_end_ms = float(rec.get("end_to_end_ms", rec.get("elapsed_ms", "0")))
    write_only_ms = float(rec.get("write_only_ms", rec.get("elapsed_ms", "0")))
    total_bytes = float(rec.get("total_bytes", rec.get("bytes", "0")))
    throughput = (float(RECORDS) / (end_to_end_ms / 1000.0)) if end_to_end_ms > 0 else 0.0
    parsed_mode = rec.get("mode", mode)
    if mode == "binary_semantic_like":
        parsed_mode = "binary_semantic_like"
    return {
        "mode": parsed_mode,
        "iteration": idx,
        "end_to_end_ms": end_to_end_ms,
        "write_only_ms": write_only_ms,
        "total_bytes": total_bytes,
        "throughput_e2e_rps": throughput,
        "raw": line,
    }


def compare(summary: Dict[str, Dict[str, Dict[str, float]]], baseline: str):
    base = summary[baseline]
    out = {}
    for mode, row in summary.items():
        if mode == baseline:
            continue
        out[mode] = {
            "end_to_end_improve_pct": (
                (base["end_to_end_ms"]["mean"] - row["end_to_end_ms"]["mean"]) / base["end_to_end_ms"]["mean"] * 100.0
                if base["end_to_end_ms"]["mean"] > 0 else 0.0
            ),
            "throughput_e2e_gain_pct": (
                (row["throughput_e2e_rps"]["mean"] - base["throughput_e2e_rps"]["mean"]) / base["throughput_e2e_rps"]["mean"] * 100.0
                if base["throughput_e2e_rps"]["mean"] > 0 else 0.0
            ),
            "size_save_pct": (
                (base["total_bytes"]["mean"] - row["total_bytes"]["mean"]) / base["total_bytes"]["mean"] * 100.0
                if base["total_bytes"]["mean"] > 0 else 0.0
            ),
        }
    return out


def esc(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def write_summary_svg(summary: Dict[str, Dict], path: str):
    modes = [m for m in MODE_ORDER if m in summary]
    if not modes:
        return

    W, H = 1440, 840
    margin_l, margin_r = 80, 40
    margin_t, margin_b = 70, 90

    chart_w = W - margin_l - margin_r
    chart_h = (H - margin_t - margin_b - 40) // 3
    gap = 20

    max_time = max(summary[m]["end_to_end_ms"]["mean"] for m in modes) * 1.15
    max_thr = max(summary[m]["throughput_e2e_rps"]["mean"] for m in modes) * 1.15
    max_size = max(summary[m]["total_bytes"]["mean"] for m in modes) * 1.15

    bar_w = chart_w / (len(modes) * 1.6)
    step = chart_w / len(modes)

    colors = {
        "text": "#7f8c8d",
        "binary": "#2ecc71",
        "syslog": "#3498db",
        "research_text": "#95a5a6",
        "nanolog_like": "#e67e22",
        "zephyr_deferred_like": "#9b59b6",
        "ulog_async_like": "#16a085",
        "hilog_lite_like": "#2c3e50",
        "nanolog_semantic_like": "#d35400",
        "zephyr_deferred_semantic_like": "#8e44ad",
        "binary_semantic_like": "#1abc9c",
    }

    def draw_block(y0: int, metric: str, vmax: float, title: str, value_fmt):
        lines = [f'<text x="{margin_l}" y="{y0 - 12}" font-size="15" font-family="sans-serif">{esc(title)}</text>']
        lines.append(f'<line x1="{margin_l}" y1="{y0 + chart_h}" x2="{margin_l + chart_w}" y2="{y0 + chart_h}" stroke="#444" stroke-width="1"/>')
        for i, m in enumerate(modes):
            v = summary[m][metric]["mean"]
            h = 0 if vmax <= 0 else (v / vmax) * (chart_h - 12)
            x = margin_l + i * step + (step - bar_w) / 2
            y = y0 + chart_h - h
            c = colors.get(m, "#666")
            lines.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" fill="{c}"/>')
            lines.append(f'<text x="{x + bar_w/2:.1f}" y="{y - 4:.1f}" text-anchor="middle" font-size="11" font-family="monospace">{value_fmt(v)}</text>')
            lines.append(f'<text x="{x + bar_w/2:.1f}" y="{y0 + chart_h + 16}" text-anchor="middle" font-size="11" font-family="sans-serif">{esc(MODE_LABEL.get(m,m))}</text>')
        return "\n".join(lines)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{W/2}" y="32" text-anchor="middle" font-size="20" font-family="sans-serif">Research Reproduction vs Optbinlog (records={RECORDS})</text>',
        draw_block(margin_t, "end_to_end_ms", max_time, "End-to-end Time (ms, lower is better)", lambda x: f"{x:.2f}"),
        draw_block(margin_t + chart_h + gap, "throughput_e2e_rps", max_thr, "Throughput (records/s, higher is better)", lambda x: f"{x/1e6:.2f}M"),
        draw_block(margin_t + 2 * (chart_h + gap), "total_bytes", max_size, "Total Bytes (lower is better)", lambda x: f"{x/1e6:.2f}M"),
        "</svg>",
    ]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))


def write_stats_svg(summary: Dict[str, Dict], path: str):
    modes = [m for m in MODE_ORDER if m in summary]
    if not modes:
        return
    W, H = 1420, 760
    margin_l = 90
    margin_t = 70
    col_w = (W - margin_l - 30) / 5
    row_h = (H - margin_t - 40) / len(modes)

    headers = ["mode", "mean(ms)", "median(ms)", "p95(ms)", "std(ms)", "95%CI(ms)"]

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{W/2}" y="32" text-anchor="middle" font-size="20" font-family="sans-serif">IQR-filtered Statistics (End-to-end)</text>',
        f'<text x="{30}" y="{margin_t}" font-size="13" font-family="sans-serif">{headers[0]}</text>',
    ]

    for i, h in enumerate(headers[1:]):
        x = margin_l + i * col_w + 8
        parts.append(f'<text x="{x:.1f}" y="{margin_t}" font-size="13" font-family="sans-serif">{esc(h)}</text>')

    for r, m in enumerate(modes):
        y = margin_t + 18 + r * row_h
        if r % 2 == 1:
            parts.append(f'<rect x="20" y="{y-13:.1f}" width="{W-40}" height="{row_h:.1f}" fill="#f8f9fa"/>')
        s = summary[m]["end_to_end_ms"]
        ci = f"[{s['ci95_low']:.2f}, {s['ci95_high']:.2f}]"
        vals = [f"{s['mean']:.3f}", f"{s['median']:.3f}", f"{s['p95']:.3f}", f"{s['std']:.3f}", ci]
        parts.append(f'<text x="30" y="{y:.1f}" font-size="12" font-family="sans-serif">{esc(MODE_LABEL.get(m,m))}</text>')
        for i, v in enumerate(vals):
            x = margin_l + i * col_w + 8
            parts.append(f'<text x="{x:.1f}" y="{y:.1f}" font-size="12" font-family="monospace">{esc(v)}</text>')

    parts.append("</svg>")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))


def main():
    build_bins()

    modes = MODE_ORDER.copy()
    runs = []

    for i in range(WARMUP):
        for m in modes:
            _ = run_once(m, i, warmup=True)

    for i in range(REPEATS):
        for m in modes:
            rec = run_once(m, i, warmup=False)
            runs.append(rec)

    summary = {}
    for m in modes:
        rows = [r for r in runs if r["mode"] == m]
        kept, filt = iqr_filter(rows, "end_to_end_ms")
        summary[m] = {
            "filter": filt,
            "end_to_end_ms": metric_stats([r["end_to_end_ms"] for r in kept]),
            "write_only_ms": metric_stats([r["write_only_ms"] for r in kept]),
            "throughput_e2e_rps": metric_stats([r["throughput_e2e_rps"] for r in kept]),
            "total_bytes": metric_stats([r["total_bytes"] for r in kept]),
            "kept_iterations": [r["iteration"] for r in kept],
        }

    if BASELINE not in summary:
        raise RuntimeError(f"baseline {BASELINE} not in summary")

    comp = compare(summary, BASELINE)

    out = {
        "config": {
            "records": RECORDS,
            "repeats": REPEATS,
            "warmup": WARMUP,
            "iqr_mult": IQR_MULT,
            "baseline_mode": BASELINE,
            "modes": modes,
        },
        "runs": runs,
        "summary": summary,
        "comparison": {"baseline_mode": BASELINE, "by_mode": comp},
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(RESULT_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    write_summary_svg(summary, RESULT_SVG)
    write_stats_svg(summary, STATS_SVG)

    print("saved", RESULT_JSON)
    print("saved", RESULT_SVG)
    print("saved", STATS_SVG)
    for m in modes:
        s = summary[m]
        print(
            f"{m}: e2e_mean={s['end_to_end_ms']['mean']:.3f}ms "
            f"p95={s['end_to_end_ms']['p95']:.3f}ms "
            f"thr={s['throughput_e2e_rps']['mean']:.1f}rps "
            f"bytes={s['total_bytes']['mean']:.1f} kept={s['filter']['kept']}"
        )

    for m, row in comp.items():
        print(
            f"vs {BASELINE} -> {m}: "
            f"e2e={row['end_to_end_improve_pct']:.2f}% "
            f"thr={row['throughput_e2e_gain_pct']:.2f}% "
            f"size={row['size_save_pct']:.2f}%"
        )


if __name__ == "__main__":
    main()
