import json
import math
import os
import shutil
import statistics
import subprocess

ROOT = os.path.dirname(__file__)
EVENTLOG_DIR = os.environ.get("OPTBINLOG_EVENTLOG_DIR", os.path.join(ROOT, "eventlogst"))
OUT_DIR = os.environ.get("OPTBINLOG_MULTI_OUT_DIR", os.path.join(ROOT, "bench_multi"))
RAW_DIR = os.path.join(OUT_DIR, "raw")

DEFAULT_DEVICES = int(os.environ.get("OPTBINLOG_DEVICES", "10"))
DEFAULT_RECORDS = int(os.environ.get("OPTBINLOG_RECORDS_PER_DEVICE", "2000"))
REPEATS = int(os.environ.get("OPTBINLOG_MULTI_REPEATS", "12"))
WARMUP = int(os.environ.get("OPTBINLOG_MULTI_WARMUP", "2"))
IQR_MULT = float(os.environ.get("OPTBINLOG_MULTI_IQR_MULT", "1.5"))
FILTER_FIELD = os.environ.get("OPTBINLOG_MULTI_FILTER_FIELD", "elapsed_ms")
MODES_ENV = os.environ.get("OPTBINLOG_MULTI_MODES", "text,binary,syslog,ftrace")
BASELINE_MODE = os.environ.get("OPTBINLOG_MULTI_BASELINE", "text")

os.makedirs(RAW_DIR, exist_ok=True)
bench = os.environ.get("OPTBINLOG_MULTI_BIN", os.path.join(ROOT, "optbinlog_multi_bench"))


def parse_modes(raw):
    seen = set()
    out = []
    for x in raw.split(","):
        m = x.strip()
        if not m or m in seen:
            continue
        seen.add(m)
        out.append(m)
    return out


def parse_int_list(env_name):
    raw = os.environ.get(env_name, "").strip()
    if not raw:
        return []
    out = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
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


def run_once(mode, devices, records_per_device, run_idx, scenario_name, warmup=False):
    run_type = "warmup" if warmup else "run"
    out_dir = os.path.join(RAW_DIR, scenario_name, f"{mode}_{run_type}_{run_idx:03d}")
    os.makedirs(out_dir, exist_ok=True)
    shared_path = os.path.join(RAW_DIR, scenario_name, "shared_eventtag.bin")
    cmd = [
        bench,
        "--mode",
        mode,
        "--eventlog-dir",
        EVENTLOG_DIR,
        "--out-dir",
        out_dir,
        "--devices",
        str(devices),
        "--records-per-device",
        str(records_per_device),
        "--shared",
        shared_path,
    ]

    env = os.environ.copy()
    if mode == "syslog" and "OPTBINLOG_SYSLOG_PRIO" not in env:
        env["OPTBINLOG_SYSLOG_PRIO"] = "7"

    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr)

    line = proc.stdout.strip().splitlines()[-1]
    row = parse_line(line)
    elapsed_ms = float(row.get("elapsed_ms", 0.0))
    total_records = devices * records_per_device
    throughput_rps = float(total_records) / (elapsed_ms / 1000.0) if elapsed_ms > 0 else 0.0
    return {
        "scenario": scenario_name,
        "devices": devices,
        "records_per_device": records_per_device,
        "mode": mode,
        "mode_reported": row.get("mode", mode),
        "iteration": run_idx,
        "elapsed_ms": elapsed_ms,
        "bytes": int(row.get("bytes", 0)),
        "shared_bytes": int(row.get("shared_bytes", 0)),
        "total_bytes": int(row.get("total_bytes", row.get("bytes", 0))),
        "throughput_rps": throughput_rps,
        "raw": line,
        "out_dir": out_dir,
    }


def scenario_summary(rows, modes, baseline_mode):
    by_mode = {m: [] for m in modes}
    for r in rows:
        if r["mode"] in by_mode:
            by_mode[r["mode"]].append(r)

    summary = {}
    for mode in modes:
        filtered, filter_info = iqr_filter(by_mode[mode], FILTER_FIELD)
        summary[mode] = {
            "filter": filter_info,
            "elapsed_ms": metric_stats([r["elapsed_ms"] for r in filtered]),
            "bytes": metric_stats([float(r["bytes"]) for r in filtered]),
            "shared_bytes": metric_stats([float(r["shared_bytes"]) for r in filtered]),
            "total_bytes": metric_stats([float(r["total_bytes"]) for r in filtered]),
            "throughput_rps": metric_stats([r["throughput_rps"] for r in filtered]),
            "kept_iterations": [r["iteration"] for r in filtered],
        }

    base = summary[baseline_mode]
    comparison = {"baseline_mode": baseline_mode, "by_mode": {}}
    for mode in modes:
        if mode == baseline_mode:
            continue
        cur = summary[mode]
        base_elapsed = base["elapsed_ms"]["mean"]
        cur_elapsed = cur["elapsed_ms"]["mean"]
        base_total = base["total_bytes"]["mean"]
        cur_total = cur["total_bytes"]["mean"]
        base_thr = base["throughput_rps"]["mean"]
        cur_thr = cur["throughput_rps"]["mean"]

        comparison["by_mode"][mode] = {
            "elapsed_improve_pct": ((base_elapsed - cur_elapsed) / base_elapsed * 100.0) if base_elapsed else 0.0,
            "size_save_pct": ((base_total - cur_total) / base_total * 100.0) if base_total else 0.0,
            "throughput_gain_pct": ((cur_thr - base_thr) / base_thr * 100.0) if base_thr else 0.0,
        }

    summary["comparison"] = comparison
    return summary


def build_stats_svg(summary, modes, baseline_mode, scenario_label, out_path):
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
        ("elapsed_ms", "elapsed_ms", "milliseconds"),
        ("total_bytes", "total_bytes", "bytes"),
        ("throughput_rps", "throughput_rps", "records/s"),
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
    comparison = summary.get("comparison", {}).get("by_mode", {})
    for mode in modes:
        if mode == baseline_mode or mode not in comparison:
            continue
        c = comparison[mode]
        cmp_lines.append(
            f"{mode}: elapsed={c['elapsed_improve_pct']:.1f}% size={c['size_save_pct']:.1f}% thr={c['throughput_gain_pct']:.1f}%"
        )

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">multi-device stats: {scenario_label}</text>')
    lines.append(f'<text x="{width / 2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">baseline={baseline_mode}; metrics=mean/median/p95/std/95%CI</text>')
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

    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_iqr_svg(runs, summary, modes, scenario_label, out_path):
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
    lines.append(f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">IQR filtering ({scenario_label})</text>')
    lines.append(f'<text x="{width / 2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">field={FILTER_FIELD}, IQR multiplier={IQR_MULT:.2f}</text>')

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
    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_result_svg(scenarios, modes, baseline_mode, out_path):
    metrics = [
        ("elapsed_ms", "Time Comparison (elapsed_ms)", "milliseconds", False),
        ("total_bytes", "Log Size Comparison (total_bytes)", "bytes", False),
        ("throughput_rps", "Throughput Comparison", "records/s", True),
    ]

    scenario_count = max(1, len(scenarios))
    mode_count = max(1, len(modes))
    panel_w = max(420, scenario_count * (mode_count * 28 + 34))
    panel_h = 300
    margin = 70
    gap = 42
    panel_y = 120
    width = int(margin * 2 + panel_w * 3 + gap * 2)
    height = 540

    palette = ["#2b8cbe", "#f03b20", "#31a354", "#756bb1", "#e6550d", "#636363", "#1f78b4", "#33a02c"]
    mode_color = {m: palette[i % len(palette)] for i, m in enumerate(modes)}

    def metric_values(metric):
        vals = []
        for sc in scenarios:
            for mode in modes:
                vals.append(sc["summary"][mode][metric]["mean"])
        return vals

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">bench_multi result overview</text>')
    lines.append(f'<text x="{width / 2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">baseline={baseline_mode}; bars=mean after IQR filtering</text>')

    for pi, (metric, title, unit, higher_better) in enumerate(metrics):
        x0 = margin + pi * (panel_w + gap)
        vals = metric_values(metric)
        vmax = max(vals + [1.0])

        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#ffffff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{panel_y - 12}" text-anchor="middle" font-family="Arial" font-size="16">{title}</text>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{panel_y + panel_h + 48}" text-anchor="middle" font-family="Arial" font-size="12">{unit}</text>')

        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            val = vmax * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f0f0f0"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{val:.2f}</text>')

        group_step = panel_w / scenario_count
        bar_w = min(30, max(8, group_step / (mode_count + 1.2)))

        for si, sc in enumerate(scenarios):
            group_left = x0 + si * group_step
            bars_w = bar_w * mode_count
            start_x = group_left + (group_step - bars_w) / 2
            baseline_value = sc["summary"][baseline_mode][metric]["mean"]

            for mi, mode in enumerate(modes):
                val = sc["summary"][mode][metric]["mean"]
                h = (val / vmax) * (panel_h * 0.9) if vmax > 0 else 0.0
                x = start_x + mi * bar_w
                y = panel_y + panel_h - h
                color = mode_color[mode]
                lines.append(f'<rect x="{x}" y="{y}" width="{bar_w}" height="{h}" fill="{color}"/>')

                if mode == baseline_mode:
                    label = "base"
                    txt_color = "#555"
                elif baseline_value > 0:
                    if higher_better:
                        improve = (val - baseline_value) / baseline_value * 100.0
                    else:
                        improve = (baseline_value - val) / baseline_value * 100.0
                    sign = "+" if improve >= 0 else "-"
                    label = f"{sign}{abs(improve):.1f}%"
                    txt_color = "#1b7837" if improve >= 0 else "#b2182b"
                else:
                    label = "n/a"
                    txt_color = "#555"
                lines.append(f'<text x="{x + bar_w / 2}" y="{y - 8}" text-anchor="middle" font-family="Arial" font-size="9" fill="{txt_color}">{label}</text>')

            lines.append(
                f'<text x="{group_left + group_step / 2}" y="{panel_y + panel_h + 19}" '
                f'text-anchor="middle" font-family="Arial" font-size="10">{sc["scenario"]}</text>'
            )

    legend_y = height - 28
    lx = margin
    for i, mode in enumerate(modes):
        x = lx + i * 110
        lines.append(f'<rect x="{x}" y="{legend_y - 11}" width="12" height="12" fill="{mode_color[mode]}"/>')
        lines.append(f'<text x="{x + 18}" y="{legend_y}" font-family="Arial" font-size="12">{mode}</text>')

    lines.append(
        f'<text x="{width - 12}" y="{legend_y}" text-anchor="end" font-family="Arial" font-size="11">'
        f'Label above bar: relative change vs {baseline_mode}</text>'
    )
    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_scan_svg(scan_rows, modes, out_path):
    if len(scan_rows) < 2:
        return False

    devices_sorted = sorted({r["devices"] for r in scan_rows})
    rec_levels = sorted({r["records_per_device"] for r in scan_rows})
    if len(devices_sorted) < 2:
        return False

    max_thr = max([r["throughput_rps"] for r in scan_rows] + [1.0])

    width = 1200
    height = 540
    margin_left = 80
    margin_right = 220
    margin_top = 60
    margin_bottom = 80
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    palette = ["#2b8cbe", "#f03b20", "#31a354", "#756bb1", "#e6550d", "#636363", "#1f78b4", "#33a02c"]
    mode_color = {m: palette[i % len(palette)] for i, m in enumerate(modes)}
    dash_styles = ["", "6,3", "2,2", "10,3,2,3", "1,3"]

    def x_pos(dev):
        idx = devices_sorted.index(dev)
        return margin_left + idx * (plot_w / (len(devices_sorted) - 1))

    def y_pos(v):
        return margin_top + (1.0 - (v / max_thr)) * plot_h

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(
        f'<text x="{width / 2}" y="30" text-anchor="middle" font-family="Arial" font-size="18">'
        f'throughput vs devices (n={REPEATS}, warmup={WARMUP})</text>'
    )

    lines.append(f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{margin_left + plot_w}" y2="{margin_top + plot_h}" stroke="#333"/>')
    lines.append(f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" stroke="#333"/>')

    for t in range(6):
        yv = max_thr * t / 5.0
        y = y_pos(yv)
        lines.append(f'<line x1="{margin_left}" y1="{y}" x2="{margin_left + plot_w}" y2="{y}" stroke="#e5e5e5"/>')
        lines.append(f'<text x="{margin_left - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="11">{yv:.0f}</text>')

    for dev in devices_sorted:
        x = x_pos(dev)
        lines.append(f'<line x1="{x}" y1="{margin_top}" x2="{x}" y2="{margin_top + plot_h}" stroke="#f0f0f0"/>')
        lines.append(f'<text x="{x}" y="{margin_top + plot_h + 20}" text-anchor="middle" font-family="Arial" font-size="11">{dev}</text>')

    lines.append(f'<text x="{margin_left + plot_w / 2}" y="{height - 22}" text-anchor="middle" font-family="Arial" font-size="12">devices</text>')
    lines.append(
        f'<text x="18" y="{margin_top + plot_h / 2}" transform="rotate(-90,18,{margin_top + plot_h / 2})" '
        f'text-anchor="middle" font-family="Arial" font-size="12">records/s</text>'
    )

    legend_y = margin_top + 10
    legend_x = margin_left + plot_w + 18
    legend_step = 18
    li = 0

    for mode in modes:
        for ri, recs in enumerate(rec_levels):
            points = [
                r for r in scan_rows
                if r["mode"] == mode and r["records_per_device"] == recs
            ]
            points.sort(key=lambda x: x["devices"])
            if len(points) < 2:
                continue

            color = mode_color[mode]
            dash = dash_styles[ri % len(dash_styles)]
            poly = " ".join([f"{x_pos(p['devices'])},{y_pos(p['throughput_rps'])}" for p in points])
            dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
            lines.append(f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="2"{dash_attr}/>')
            for p in points:
                lines.append(f'<circle cx="{x_pos(p["devices"])}" cy="{y_pos(p["throughput_rps"])}" r="3" fill="{color}"/>')

            ly = legend_y + li * legend_step
            lines.append(f'<line x1="{legend_x}" y1="{ly}" x2="{legend_x + 18}" y2="{ly}" stroke="{color}" stroke-width="2"{dash_attr}/>')
            lines.append(f'<text x="{legend_x + 24}" y="{ly + 4}" font-family="Arial" font-size="11">{mode} rpd={recs}</text>')
            li += 1

    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return True


requested_modes = parse_modes(MODES_ENV)
if not requested_modes:
    raise SystemExit("no valid modes in OPTBINLOG_MULTI_MODES")

devices_scan = parse_int_list("OPTBINLOG_SCAN_DEVICES")
records_scan = parse_int_list("OPTBINLOG_SCAN_RECORDS_PER_DEVICE")
if not devices_scan:
    devices_scan = [DEFAULT_DEVICES]
if not records_scan:
    records_scan = [DEFAULT_RECORDS]

scenarios = []
for d in sorted(set(devices_scan)):
    for r in sorted(set(records_scan)):
        scenarios.append({"devices": d, "records_per_device": r, "name": f"d{d}_r{r}"})

mode_errors = {}
disabled_modes = set()
all_runs_raw = []
scenario_rows = {s["name"]: [] for s in scenarios}


def safe_run(mode, devices, records_per_device, idx, scenario_name, warmup=False):
    try:
        return run_once(mode, devices, records_per_device, idx, scenario_name, warmup=warmup)
    except subprocess.CalledProcessError as e:
        err = (e.stderr or e.output or str(e)).strip()
        if mode not in mode_errors:
            mode_errors[mode] = err[:800]
        disabled_modes.add(mode)
        return None


for s in scenarios:
    devices = s["devices"]
    records_per_device = s["records_per_device"]
    name = s["name"]

    for i in range(WARMUP):
        order = requested_modes if (i % 2 == 0) else list(reversed(requested_modes))
        for mode in order:
            if mode in disabled_modes:
                continue
            _ = safe_run(mode, devices, records_per_device, i, name, warmup=True)

    for i in range(REPEATS):
        order = requested_modes if (i % 2 == 0) else list(reversed(requested_modes))
        for mode in order:
            if mode in disabled_modes:
                continue
            rec = safe_run(mode, devices, records_per_device, i, name, warmup=False)
            if rec is not None:
                scenario_rows[name].append(rec)
                all_runs_raw.append(rec)

active_modes = []
for mode in requested_modes:
    if mode in disabled_modes:
        continue
    ok = True
    for s in scenarios:
        name = s["name"]
        cnt = sum(1 for r in scenario_rows[name] if r["mode"] == mode)
        if cnt != REPEATS:
            ok = False
            mode_errors.setdefault(mode, f"incomplete data for {name}: expected {REPEATS}, got {cnt}")
            break
    if ok:
        active_modes.append(mode)

if not active_modes:
    raise SystemExit("all requested modes failed")

if BASELINE_MODE not in active_modes:
    BASELINE_MODE = active_modes[0]

sample_row = None
for r in all_runs_raw:
    if r["mode"] in active_modes:
        sample_row = r
        break
if sample_row is None:
    raise SystemExit("no valid benchmark rows")
if FILTER_FIELD not in sample_row:
    raise SystemExit(f"filter field not found: {FILTER_FIELD}")

scenario_results = []
all_runs = []
for s in scenarios:
    name = s["name"]
    rows = [r for r in scenario_rows[name] if r["mode"] in active_modes]
    if not rows:
        continue

    summary = scenario_summary(rows, active_modes, BASELINE_MODE)
    stats_svg = os.path.join(OUT_DIR, f"{name}_stats.svg")
    iqr_svg = os.path.join(OUT_DIR, f"{name}_iqr.svg")
    build_stats_svg(summary, active_modes, BASELINE_MODE, name, stats_svg)
    build_iqr_svg(rows, summary, active_modes, name, iqr_svg)

    scenario_results.append(
        {
            "scenario": name,
            "devices": s["devices"],
            "records_per_device": s["records_per_device"],
            "summary": summary,
            "runs": rows,
            "stats_svg": os.path.basename(stats_svg),
            "iqr_svg": os.path.basename(iqr_svg),
        }
    )
    all_runs.extend(rows)

if not scenario_results:
    raise SystemExit("no scenario produced complete results")

result_svg_path = os.path.join(OUT_DIR, "bench_multi_result.svg")
build_result_svg(scenario_results, active_modes, BASELINE_MODE, result_svg_path)

scan_svg_path = os.path.join(OUT_DIR, "bench_multi_scan.svg")
scan_rows = []
for sc in scenario_results:
    for mode in active_modes:
        scan_rows.append(
            {
                "devices": sc["devices"],
                "records_per_device": sc["records_per_device"],
                "mode": mode,
                "throughput_rps": sc["summary"][mode]["throughput_rps"]["mean"],
                "elapsed_ms": sc["summary"][mode]["elapsed_ms"]["mean"],
            }
        )
scan_svg_created = build_scan_svg(scan_rows, active_modes, scan_svg_path)

if len(scenario_results) == 1:
    only = scenario_results[0]
    shutil.copyfile(os.path.join(OUT_DIR, only["stats_svg"]), os.path.join(OUT_DIR, "bench_multi_stats.svg"))
    shutil.copyfile(os.path.join(OUT_DIR, only["iqr_svg"]), os.path.join(OUT_DIR, "bench_multi_iqr.svg"))

result = {
    "config": {
        "repeats": REPEATS,
        "warmup": WARMUP,
        "iqr_mult": IQR_MULT,
        "filter_field": FILTER_FIELD,
        "scan_devices": sorted(set(devices_scan)),
        "scan_records_per_device": sorted(set(records_scan)),
        "requested_modes": requested_modes,
        "active_modes": active_modes,
        "baseline_mode": BASELINE_MODE,
    },
    "scenarios": scenario_results,
    "runs": all_runs,
    "artifacts": {
        "result_svg": os.path.basename(result_svg_path),
        "scan_svg": os.path.basename(scan_svg_path) if scan_svg_created else None,
        "single_stats_svg": "bench_multi_stats.svg" if len(scenario_results) == 1 else None,
        "single_iqr_svg": "bench_multi_iqr.svg" if len(scenario_results) == 1 else None,
    },
    "skipped_modes": mode_errors,
}

json_path = os.path.join(OUT_DIR, "bench_multi_result.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

print("saved", json_path)
print("saved", result_svg_path)
if scan_svg_created:
    print("saved", scan_svg_path)
if len(scenario_results) == 1:
    print("saved", os.path.join(OUT_DIR, "bench_multi_stats.svg"))
    print("saved", os.path.join(OUT_DIR, "bench_multi_iqr.svg"))

for sc in scenario_results:
    print(f"scenario={sc['scenario']} devices={sc['devices']} records_per_device={sc['records_per_device']}")
    for mode in active_modes:
        s = sc["summary"][mode]
        print(
            f"  {mode}: "
            f"mean={s['elapsed_ms']['mean']:.3f}ms "
            f"median={s['elapsed_ms']['median']:.3f}ms "
            f"p95={s['elapsed_ms']['p95']:.3f}ms "
            f"std={s['elapsed_ms']['std']:.3f} "
            f"ci95=[{s['elapsed_ms']['ci95_low']:.3f},{s['elapsed_ms']['ci95_high']:.3f}] "
            f"thr={s['throughput_rps']['mean']:.1f}rps "
            f"total_bytes={s['total_bytes']['mean']:.1f} "
            f"kept={s['elapsed_ms']['n']}"
        )

    cmpv = sc["summary"]["comparison"]
    for mode in active_modes:
        if mode == BASELINE_MODE:
            continue
        c = cmpv["by_mode"][mode]
        print(
            f"  vs {BASELINE_MODE} -> {mode}: "
            f"elapsed={c['elapsed_improve_pct']:.2f}% "
            f"size={c['size_save_pct']:.2f}% "
            f"throughput={c['throughput_gain_pct']:.2f}%"
        )

if mode_errors:
    print("skipped:")
    for m, err in mode_errors.items():
        print(f"  {m}: {err}")
