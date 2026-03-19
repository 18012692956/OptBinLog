import json
import math
import os
import statistics
import subprocess

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(SCRIPTS_DIR)
BUILD_BIN_DIR = os.path.join(ROOT, "build", "bin")
EVENTLOG_DIR = os.environ.get("OPTBINLOG_EVENTLOG_DIR", os.path.join(ROOT, "eventlogst"))
OUT_DIR = os.environ.get("OPTBINLOG_BENCH_OUT_DIR", os.path.join(ROOT, "bench"))
RUN_DIR = os.path.join(OUT_DIR, "runs")
# Allow cross-node/process explicit shared metadata path while keeping
# per-node output files isolated under OUT_DIR.
SHARED = os.environ.get("OPTBINLOG_SHARED_TAG_PATH", os.path.join(OUT_DIR, "shared_eventtag.bin"))
RECORDS = int(os.environ.get("OPTBINLOG_BENCH_RECORDS", "20000"))
REPEATS = int(os.environ.get("OPTBINLOG_BENCH_REPEATS", "20"))
WARMUP = int(os.environ.get("OPTBINLOG_BENCH_WARMUP", "3"))
IQR_MULT = float(os.environ.get("OPTBINLOG_BENCH_IQR_MULT", "1.5"))
FILTER_FIELD = os.environ.get("OPTBINLOG_BENCH_FILTER_FIELD", "end_to_end_ms")
MODES_ENV = os.environ.get("OPTBINLOG_BENCH_MODES", "text,binary,syslog,ftrace")
BASELINE_MODE = os.environ.get("OPTBINLOG_BENCH_BASELINE", "text")

os.makedirs(RUN_DIR, exist_ok=True)
bench = os.environ.get("OPTBINLOG_BENCH_BIN", os.path.join(BUILD_BIN_DIR, "optbinlog_bench"))


def parse_modes(raw):
    seen = set()
    out = []
    for x in raw.split(","):
        m = x.strip()
        if not m:
            continue
        if m in seen:
            continue
        seen.add(m)
        out.append(m)
    return out


def percentile(values, p):
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


def metric_stats(values):
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


def parse_line(line):
    parts = line.split(",")
    out = {}
    for i in range(0, len(parts) - 1, 2):
        out[parts[i]] = parts[i + 1]
    return out


def run_once(mode, idx, warmup=False):
    suffix = "warmup" if warmup else "run"
    ext_map = {
        "text": "log",
        "text_semantic_like": "slog",
        "csv": "csv",
        "jsonl": "jsonl",
        "binary": "bin",
        "binary_crc32_legacy": "bcrc",
        "binary_crc32c": "bc32c",
        "binary_hotpath": "bhot",
        "binary_nocrc": "bnc",
        "binary_varstr": "bvs",
        "binary_crc32c_varstr": "bcv",
        "binary_nocrc_varstr": "bnv",
        "syslog": "syslog",
        "ftrace": "ftrace",
        "nanolog_like": "nlog",
        "zephyr_like": "zlog",
        "zephyr_deferred_like": "zlog",
        "ulog_async_like": "ulg",
        "hilog_lite_like": "hlg",
        "nanolog_semantic_like": "nslog",
        "zephyr_deferred_semantic_like": "zslog",
        "ulog_semantic_like": "uslog",
        "hilog_semantic_like": "hslog",
    }
    ext = ext_map.get(mode, "out")
    out_name = f"{mode}_{suffix}_{idx:03d}.{ext}"
    out_path = os.path.join(RUN_DIR, out_name)

    cmd = [
        bench,
        "--mode",
        mode,
        "--eventlog-dir",
        EVENTLOG_DIR,
        "--out",
        out_path,
        "--records",
        str(RECORDS),
        "--shared",
        SHARED,
    ]

    env = os.environ.copy()
    if mode == "syslog" and "OPTBINLOG_SYSLOG_PRIO" not in env:
        # LOG_DEBUG by default to reduce persistence noise in system log store.
        env["OPTBINLOG_SYSLOG_PRIO"] = "7"

    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr)

    line = proc.stdout.strip().splitlines()[-1]
    rec = parse_line(line)

    write_only_ms = float(rec.get("write_only_ms", rec.get("elapsed_ms", 0.0)))
    end_to_end_ms = float(rec.get("end_to_end_ms", write_only_ms))
    prep_ms = float(rec.get("prep_ms", max(0.0, end_to_end_ms - write_only_ms)))
    post_ms = float(rec.get("post_ms", 0.0))
    throughput_write_rps = float(RECORDS) / (write_only_ms / 1000.0) if write_only_ms > 0 else 0.0
    throughput_e2e_rps = float(RECORDS) / (end_to_end_ms / 1000.0) if end_to_end_ms > 0 else 0.0

    return {
        "mode": mode,
        "mode_reported": rec.get("mode", mode),
        "iteration": idx,
        "elapsed_ms": write_only_ms,
        "write_only_ms": write_only_ms,
        "end_to_end_ms": end_to_end_ms,
        "prep_ms": prep_ms,
        "post_ms": post_ms,
        "bytes": int(rec.get("bytes", 0)),
        "shared_bytes": int(rec.get("shared_bytes", 0)),
        "total_bytes": int(rec.get("total_bytes", rec.get("bytes", 0))),
        "peak_kb": int(rec.get("peak_kb", 0)),
        "throughput_write_rps": throughput_write_rps,
        "throughput_e2e_rps": throughput_e2e_rps,
        "raw": line,
        "out_path": out_path,
    }


def iqr_filter(rows, field):
    values = [r[field] for r in rows]
    if len(values) < 4:
        return rows, {"field": field, "method": "iqr", "kept": len(rows), "removed": 0}
    q1 = percentile(values, 25)
    q3 = percentile(values, 75)
    iqr = q3 - q1
    lo = q1 - IQR_MULT * iqr
    hi = q3 + IQR_MULT * iqr
    kept = [r for r in rows if lo <= r[field] <= hi]
    if len(kept) < max(3, len(rows) // 2):
        return rows, {
            "field": field,
            "method": "iqr_fallback_to_all",
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower": lo,
            "upper": hi,
            "kept": len(rows),
            "removed": 0,
        }
    return kept, {
        "field": field,
        "method": "iqr",
        "q1": q1,
        "q3": q3,
        "iqr": iqr,
        "lower": lo,
        "upper": hi,
        "kept": len(kept),
        "removed": len(rows) - len(kept),
    }


def build_stats_svg(summary, comparison, modes, baseline_mode, out_path):
    width = 1680
    height = 640
    panel_gap = 35
    panel_w = (width - 150 - panel_gap * 2) / 3.0
    panel_h = 360
    panel_y = 130
    panel_x0 = 75

    palette = ["#2b8cbe", "#f03b20", "#31a354", "#756bb1", "#e6550d", "#636363", "#1f78b4", "#33a02c"]
    mode_color = {m: palette[i % len(palette)] for i, m in enumerate(modes)}

    panels = [
        ("end_to_end_ms", "end_to_end_ms", "milliseconds"),
        ("write_only_ms", "write_only_ms", "milliseconds"),
        ("throughput_e2e_rps", "throughput_e2e_rps", "records/s"),
    ]

    def value_range(metric):
        vals = []
        for mode in modes:
            m = summary[mode][metric]
            vals.extend(
                [
                    m["mean"],
                    m["median"],
                    m["p95"],
                    m["ci95_low"],
                    m["ci95_high"],
                    m["mean"] - m["std"],
                    m["mean"] + m["std"],
                ]
            )
        lo = min(vals)
        hi = max(vals)
        if hi <= lo:
            hi = lo + 1.0
        pad = max((hi - lo) * 0.08, 1e-6)
        return lo - pad, hi + pad

    cmp_lines = []
    for mode in modes:
        if mode == baseline_mode:
            continue
        c = comparison["by_mode"][mode]
        cmp_lines.append(
            f"{mode}: e2e={c['end_to_end_improve_pct']:.1f}% write={c['write_only_improve_pct']:.1f}% thr_e2e={c['throughput_e2e_gain_pct']:.1f}%"
        )

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">optbinlog multi-system benchmark summary</text>')
    lines.append(f'<text x="{width / 2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">baseline={baseline_mode} ; modes={", ".join(modes)}</text>')
    if cmp_lines:
        lines.append(f'<text x="{width / 2}" y="80" text-anchor="middle" font-family="Arial" font-size="12">{" | ".join(cmp_lines[:3])}</text>')

    for pi, (metric, title, unit) in enumerate(panels):
        x0 = panel_x0 + pi * (panel_w + panel_gap)
        x1 = x0 + panel_w
        lo, hi = value_range(metric)
        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#ffffff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{panel_y - 12}" text-anchor="middle" font-family="Arial" font-size="15">{title}</text>')

        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            val = lo + (hi - lo) * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x1}" y2="{y}" stroke="#f0f0f0"/>')
            lines.append(f'<text x="{x0 - 7}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="11">{val:.3f}</text>')
        lines.append(
            f'<text x="{x0 - 36}" y="{panel_y + panel_h / 2}" transform="rotate(-90,{x0 - 36},{panel_y + panel_h / 2})" '
            f'text-anchor="middle" font-family="Arial" font-size="12">{unit}</text>'
        )

        def y_map(v):
            return panel_y + panel_h - ((v - lo) / (hi - lo)) * panel_h

        mcount = len(modes)
        for idx, mode in enumerate(modes):
            cx = x0 + panel_w * ((idx + 1) / (mcount + 1))
            m = summary[mode][metric]
            color = mode_color[mode]
            y_mean = y_map(m["mean"])
            y_median = y_map(m["median"])
            y_p95 = y_map(m["p95"])
            y_std_lo = y_map(m["mean"] - m["std"])
            y_std_hi = y_map(m["mean"] + m["std"])
            y_ci_lo = y_map(m["ci95_low"])
            y_ci_hi = y_map(m["ci95_high"])

            lines.append(f'<line x1="{cx}" y1="{y_std_hi}" x2="{cx}" y2="{y_std_lo}" stroke="#777" stroke-width="1.2"/>')
            lines.append(f'<line x1="{cx}" y1="{y_ci_hi}" x2="{cx}" y2="{y_ci_lo}" stroke="{color}" stroke-width="4"/>')
            lines.append(f'<circle cx="{cx}" cy="{y_mean}" r="4.5" fill="{color}"/>')
            lines.append(f'<rect x="{cx - 3.5}" y="{y_median - 3.5}" width="7" height="7" fill="none" stroke="{color}" stroke-width="1.8"/>')
            lines.append(f'<polygon points="{cx},{y_p95 - 4.5} {cx - 4.5},{y_p95 + 3.5} {cx + 4.5},{y_p95 + 3.5}" fill="{color}"/>')
            lines.append(f'<text x="{cx}" y="{panel_y + panel_h + 20}" text-anchor="middle" font-family="Arial" font-size="11">{mode}</text>')

    legend_y = panel_y + panel_h + 54
    lx = 95
    lines.append(f'<line x1="{lx}" y1="{legend_y}" x2="{lx + 24}" y2="{legend_y}" stroke="#777" stroke-width="1.2"/>')
    lines.append(f'<text x="{lx + 30}" y="{legend_y + 4}" font-family="Arial" font-size="12">mean ± std</text>')
    lines.append(f'<line x1="{lx + 145}" y1="{legend_y}" x2="{lx + 169}" y2="{legend_y}" stroke="#333" stroke-width="4"/>')
    lines.append(f'<text x="{lx + 176}" y="{legend_y + 4}" font-family="Arial" font-size="12">95% CI</text>')
    lines.append(f'<circle cx="{lx + 276}" cy="{legend_y}" r="4.5" fill="#333"/>')
    lines.append(f'<text x="{lx + 288}" y="{legend_y + 4}" font-family="Arial" font-size="12">mean</text>')
    lines.append(f'<rect x="{lx + 352}" y="{legend_y - 3.5}" width="7" height="7" fill="none" stroke="#333" stroke-width="1.8"/>')
    lines.append(f'<text x="{lx + 365}" y="{legend_y + 4}" font-family="Arial" font-size="12">median</text>')
    lines.append(f'<polygon points="{lx + 452},{legend_y - 4.5} {lx + 447.5},{legend_y + 3.5} {lx + 456.5},{legend_y + 3.5}" fill="#333"/>')
    lines.append(f'<text x="{lx + 464}" y="{legend_y + 4}" font-family="Arial" font-size="12">p95</text>')

    lines.append('</svg>')
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_result_svg(summary, modes, baseline_mode, out_path):
    width = 1240
    height = 520
    margin = 70
    gap = 60
    panel_w = (width - margin * 2 - gap) / 2.0
    panel_h = 300
    panel_y = 120
    left_x = margin
    right_x = margin + panel_w + gap

    palette = ["#2b8cbe", "#f03b20", "#31a354", "#756bb1", "#e6550d", "#636363", "#1f78b4", "#33a02c"]
    mode_color = {m: palette[i % len(palette)] for i, m in enumerate(modes)}

    base_time = summary[baseline_mode]["end_to_end_ms"]["mean"]
    base_size = summary[baseline_mode]["total_bytes"]["mean"]

    def draw_panel(lines, x0, metric_key, title, unit, base_value):
        vals = [summary[m][metric_key]["mean"] for m in modes]
        vmax = max(vals + [1.0])
        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#ffffff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{panel_y - 12}" text-anchor="middle" font-family="Arial" font-size="16">{title}</text>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{panel_y + panel_h + 48}" text-anchor="middle" font-family="Arial" font-size="12">{unit}</text>')

        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            val = vmax * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f0f0f0"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="11">{val:.2f}</text>')

        n = len(modes)
        group_step = panel_w / n
        bar_w = min(56, group_step * 0.58)
        for i, mode in enumerate(modes):
            v = summary[mode][metric_key]["mean"]
            h = 0 if vmax <= 0 else (v / vmax) * (panel_h * 0.9)
            x = x0 + i * group_step + (group_step - bar_w) / 2
            y = panel_y + panel_h - h
            color = mode_color[mode]
            lines.append(f'<rect x="{x}" y="{y}" width="{bar_w}" height="{h}" fill="{color}"/>')
            lines.append(f'<text x="{x + bar_w / 2}" y="{y - 8}" text-anchor="middle" font-family="Arial" font-size="10">{v:.3f}</text>')
            lines.append(f'<text x="{x + bar_w / 2}" y="{panel_y + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="11">{mode}</text>')

            if mode != baseline_mode and base_value > 0:
                improve = (base_value - v) / base_value * 100.0
                if metric_key.startswith("throughput"):
                    improve = (v - base_value) / base_value * 100.0
                sign = "+" if improve >= 0 else "-"
                txt = f"{sign}{abs(improve):.1f}%"
                lines.append(f'<text x="{x + bar_w / 2}" y="{y - 22}" text-anchor="middle" font-family="Arial" font-size="10" fill="#1b7837">{txt}</text>')
            elif mode == baseline_mode:
                lines.append(f'<text x="{x + bar_w / 2}" y="{y - 22}" text-anchor="middle" font-family="Arial" font-size="10" fill="#666">baseline</text>')

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">bench_result overview</text>')
    lines.append(f'<text x="{width / 2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">baseline={baseline_mode} ; compare by mean after IQR filtering</text>')

    draw_panel(lines, left_x, "end_to_end_ms", "Time Comparison (end_to_end_ms)", "milliseconds", base_time)
    draw_panel(lines, right_x, "total_bytes", "Log Size Comparison (total_bytes)", "bytes", base_size)

    legend_y = height - 28
    lx = margin
    lines.append(f'<text x="{lx}" y="{legend_y}" font-family="Arial" font-size="12">Label above bar: relative change vs baseline (time/size lower is better).</text>')

    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_iqr_svg(runs, summary, modes, out_path):
    width = max(1100, 220 * len(modes) + 180)
    height = 560
    margin_left = 70
    margin_right = 60
    margin_top = 95
    margin_bottom = 90
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    palette = ["#2b8cbe", "#f03b20", "#31a354", "#756bb1", "#e6550d", "#636363", "#1f78b4", "#33a02c"]
    mode_color = {m: palette[i % len(palette)] for i, m in enumerate(modes)}

    values = [r[FILTER_FIELD] for r in runs]
    for mode in modes:
        flt = summary[mode]["filter"]
        if "lower" in flt and "upper" in flt:
            values.extend([flt["lower"], flt["upper"]])
        if "q1" in flt and "q3" in flt:
            values.extend([flt["q1"], flt["q3"]])
    lo = min(values)
    hi = max(values)
    if hi <= lo:
        hi = lo + 1.0
    pad = max((hi - lo) * 0.08, 1e-6)
    lo -= pad
    hi += pad

    def y_map(v):
        return margin_top + plot_h - ((v - lo) / (hi - lo)) * plot_h

    by_mode = {m: [] for m in modes}
    for row in runs:
        by_mode[row["mode"]].append(row)

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">IQR outlier filtering ({FILTER_FIELD})</text>')
    lines.append(f'<text x="{width / 2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">IQR multiplier={IQR_MULT:.2f}</text>')

    lines.append(f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{margin_left + plot_w}" y2="{margin_top + plot_h}" stroke="#333"/>')
    lines.append(f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" stroke="#333"/>')
    for i in range(6):
        frac = i / 5.0
        y = margin_top + plot_h * (1.0 - frac)
        val = lo + (hi - lo) * frac
        lines.append(f'<line x1="{margin_left}" y1="{y}" x2="{margin_left + plot_w}" y2="{y}" stroke="#f0f0f0"/>')
        lines.append(f'<text x="{margin_left - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="11">{val:.3f}</text>')

    lines.append(
        f'<text x="25" y="{margin_top + plot_h / 2}" transform="rotate(-90,25,{margin_top + plot_h / 2})" '
        f'text-anchor="middle" font-family="Arial" font-size="12">{FILTER_FIELD}</text>'
    )

    for idx, mode in enumerate(modes):
        cx = margin_left + plot_w * ((idx + 1) / (len(modes) + 1))
        color = mode_color[mode]
        flt = summary[mode]["filter"]
        kept_set = set(summary[mode]["kept_iterations"])
        rows = sorted(by_mode[mode], key=lambda x: x["iteration"])

        lines.append(f'<text x="{cx}" y="{margin_top + plot_h + 24}" text-anchor="middle" font-family="Arial" font-size="12">{mode}</text>')

        if "q1" in flt and "q3" in flt:
            y_q1 = y_map(flt["q1"])
            y_q3 = y_map(flt["q3"])
            box_y = min(y_q1, y_q3)
            box_h = abs(y_q1 - y_q3)
            lines.append(f'<rect x="{cx - 45}" y="{box_y}" width="90" height="{box_h}" fill="{color}" opacity="0.14" stroke="{color}" stroke-width="1.4"/>')
            y_med = y_map(summary[mode][FILTER_FIELD]["median"])
            lines.append(f'<line x1="{cx - 45}" y1="{y_med}" x2="{cx + 45}" y2="{y_med}" stroke="{color}" stroke-width="2"/>')

        if "lower" in flt and "upper" in flt:
            y_lo = y_map(flt["lower"])
            y_hi = y_map(flt["upper"])
            lines.append(f'<line x1="{cx - 56}" y1="{y_lo}" x2="{cx + 56}" y2="{y_lo}" stroke="#666" stroke-dasharray="5,3"/>')
            lines.append(f'<line x1="{cx - 56}" y1="{y_hi}" x2="{cx + 56}" y2="{y_hi}" stroke="#666" stroke-dasharray="5,3"/>')

        for j, row in enumerate(rows):
            jitter = ((j % 9) - 4) * 5
            x = cx + jitter
            y = y_map(row[FILTER_FIELD])
            if row["iteration"] in kept_set:
                lines.append(f'<circle cx="{x}" cy="{y}" r="3.2" fill="{color}"/>')
            else:
                lines.append(f'<line x1="{x - 4}" y1="{y - 4}" x2="{x + 4}" y2="{y + 4}" stroke="#d7301f" stroke-width="1.8"/>')
                lines.append(f'<line x1="{x - 4}" y1="{y + 4}" x2="{x + 4}" y2="{y - 4}" stroke="#d7301f" stroke-width="1.8"/>')

        removed = flt.get("removed", 0)
        kept = flt.get("kept", len(rows))
        lines.append(f'<text x="{cx}" y="{margin_top - 12}" text-anchor="middle" font-family="Arial" font-size="11">kept={kept}, removed={removed}</text>')

    legend_y = height - 34
    lx = 95
    lines.append(f'<circle cx="{lx}" cy="{legend_y}" r="3.2" fill="#2b8cbe"/>')
    lines.append(f'<text x="{lx + 12}" y="{legend_y + 4}" font-family="Arial" font-size="12">kept sample</text>')
    lines.append(f'<line x1="{lx + 120}" y1="{legend_y - 4}" x2="{lx + 128}" y2="{legend_y + 4}" stroke="#d7301f" stroke-width="1.8"/>')
    lines.append(f'<line x1="{lx + 120}" y1="{legend_y + 4}" x2="{lx + 128}" y2="{legend_y - 4}" stroke="#d7301f" stroke-width="1.8"/>')
    lines.append(f'<text x="{lx + 136}" y="{legend_y + 4}" font-family="Arial" font-size="12">removed by IQR</text>')
    lines.append(f'<line x1="{lx + 290}" y1="{legend_y}" x2="{lx + 314}" y2="{legend_y}" stroke="#666" stroke-dasharray="5,3"/>')
    lines.append(f'<text x="{lx + 322}" y="{legend_y + 4}" font-family="Arial" font-size="12">IQR fences</text>')
    lines.append('</svg>')

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


requested_modes = parse_modes(MODES_ENV)
if not requested_modes:
    raise SystemExit("no valid modes in OPTBINLOG_BENCH_MODES")

mode_errors = {}
disabled_modes = set()


def safe_run(mode, idx, warmup=False):
    try:
        return run_once(mode, idx, warmup=warmup)
    except subprocess.CalledProcessError as e:
        err = (e.stderr or e.output or str(e)).strip()
        if mode not in mode_errors:
            mode_errors[mode] = err[:800]
        disabled_modes.add(mode)
        return None


for i in range(WARMUP):
    for mode in requested_modes:
        if mode in disabled_modes:
            continue
        _ = safe_run(mode, i, warmup=True)

runs = []
for i in range(REPEATS):
    for mode in requested_modes:
        if mode in disabled_modes:
            continue
        row = safe_run(mode, i, warmup=False)
        if row is not None:
            runs.append(row)

by_mode = {m: [] for m in requested_modes}
for r in runs:
    by_mode[r["mode"]].append(r)

active_modes = [m for m in requested_modes if m not in disabled_modes and len(by_mode[m]) == REPEATS]
for m in requested_modes:
    if m not in active_modes and m not in mode_errors:
        mode_errors[m] = f"incomplete data: expected {REPEATS}, got {len(by_mode[m])}"

if not active_modes:
    raise SystemExit("all requested modes failed")

if BASELINE_MODE not in active_modes:
    BASELINE_MODE = active_modes[0]

if FILTER_FIELD not in runs[0]:
    raise SystemExit(f"filter field not found: {FILTER_FIELD}")

summary = {}
for mode in active_modes:
    filtered, filter_info = iqr_filter(by_mode[mode], FILTER_FIELD)
    summary[mode] = {
        "filter": filter_info,
        "end_to_end_ms": metric_stats([r["end_to_end_ms"] for r in filtered]),
        "write_only_ms": metric_stats([r["write_only_ms"] for r in filtered]),
        "prep_ms": metric_stats([r["prep_ms"] for r in filtered]),
        "post_ms": metric_stats([r["post_ms"] for r in filtered]),
        "bytes": metric_stats([float(r["bytes"]) for r in filtered]),
        "shared_bytes": metric_stats([float(r["shared_bytes"]) for r in filtered]),
        "total_bytes": metric_stats([float(r["total_bytes"]) for r in filtered]),
        "peak_kb": metric_stats([float(r["peak_kb"]) for r in filtered]),
        "throughput_write_rps": metric_stats([r["throughput_write_rps"] for r in filtered]),
        "throughput_e2e_rps": metric_stats([r["throughput_e2e_rps"] for r in filtered]),
        "kept_iterations": [r["iteration"] for r in filtered],
    }

base = summary[BASELINE_MODE]
comparison = {
    "baseline_mode": BASELINE_MODE,
    "by_mode": {},
}

for mode in active_modes:
    if mode == BASELINE_MODE:
        continue
    cur = summary[mode]

    base_write = base["write_only_ms"]["mean"]
    cur_write = cur["write_only_ms"]["mean"]
    base_e2e = base["end_to_end_ms"]["mean"]
    cur_e2e = cur["end_to_end_ms"]["mean"]
    base_total = base["total_bytes"]["mean"]
    cur_total = cur["total_bytes"]["mean"]
    base_thr_w = base["throughput_write_rps"]["mean"]
    cur_thr_w = cur["throughput_write_rps"]["mean"]
    base_thr_e = base["throughput_e2e_rps"]["mean"]
    cur_thr_e = cur["throughput_e2e_rps"]["mean"]

    comparison["by_mode"][mode] = {
        "write_only_improve_pct": ((base_write - cur_write) / base_write * 100.0) if base_write else 0.0,
        "end_to_end_improve_pct": ((base_e2e - cur_e2e) / base_e2e * 100.0) if base_e2e else 0.0,
        "size_save_pct": ((base_total - cur_total) / base_total * 100.0) if base_total else 0.0,
        "throughput_write_gain_pct": ((cur_thr_w - base_thr_w) / base_thr_w * 100.0) if base_thr_w else 0.0,
        "throughput_e2e_gain_pct": ((cur_thr_e - base_thr_e) / base_thr_e * 100.0) if base_thr_e else 0.0,
    }

result = {
    "config": {
        "records": RECORDS,
        "repeats": REPEATS,
        "warmup": WARMUP,
        "iqr_mult": IQR_MULT,
        "filter_field": FILTER_FIELD,
        "requested_modes": requested_modes,
        "active_modes": active_modes,
        "baseline_mode": BASELINE_MODE,
    },
    "runs": runs,
    "summary": summary,
    "comparison": comparison,
    "skipped_modes": mode_errors,
}

json_path = os.path.join(OUT_DIR, "bench_result.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)
print("saved", json_path)

stats_svg_path = os.path.join(OUT_DIR, "bench_stats.svg")
build_stats_svg(summary, comparison, active_modes, BASELINE_MODE, stats_svg_path)
print("saved", stats_svg_path)

result_svg_path = os.path.join(OUT_DIR, "bench_result.svg")
build_result_svg(summary, active_modes, BASELINE_MODE, result_svg_path)
print("saved", result_svg_path)

iqr_svg_path = os.path.join(OUT_DIR, "bench_iqr.svg")
build_iqr_svg(runs, summary, active_modes, iqr_svg_path)
print("saved", iqr_svg_path)

old_png = os.path.join(OUT_DIR, "bench_result.png")
if os.path.exists(old_png):
    os.remove(old_png)
    print("removed", old_png)

for mode in active_modes:
    s = summary[mode]
    print(
        f"{mode}: "
        f"e2e_mean={s['end_to_end_ms']['mean']:.3f}ms "
        f"e2e_p95={s['end_to_end_ms']['p95']:.3f}ms "
        f"write_mean={s['write_only_ms']['mean']:.3f}ms "
        f"throughput_e2e={s['throughput_e2e_rps']['mean']:.1f} rps "
        f"kept={s['end_to_end_ms']['n']}"
    )

for mode in active_modes:
    if mode == BASELINE_MODE:
        continue
    c = comparison["by_mode"][mode]
    print(
        f"vs {BASELINE_MODE} -> {mode}: "
        f"e2e={c['end_to_end_improve_pct']:.2f}% "
        f"write={c['write_only_improve_pct']:.2f}% "
        f"size={c['size_save_pct']:.2f}% "
        f"throughput_e2e={c['throughput_e2e_gain_pct']:.2f}%"
    )

if mode_errors:
    print("skipped:")
    for m, err in mode_errors.items():
        print(f"  {m}: {err}")
