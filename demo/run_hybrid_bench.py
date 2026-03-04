import json
import os
import subprocess


ROOT = os.path.dirname(__file__)
RUN_BENCH = os.path.join(ROOT, "run_bench.py")
PROJECT = os.path.abspath(os.path.join(ROOT, ".."))

LOCAL_BIN = os.path.join(ROOT, "optbinlog_bench_macos")
LINUX_BIN = os.path.join(ROOT, "optbinlog_bench_linux")

HYBRID_OUT = os.environ.get("OPTBINLOG_HYBRID_OUT_DIR", os.path.join(ROOT, "bench", "hybrid"))
LOCAL_OUT = os.path.join(HYBRID_OUT, "local")
LINUX_OUT = os.path.join(HYBRID_OUT, "linux")
MERGED_JSON = os.path.join(HYBRID_OUT, "bench_result_merged.json")
MERGED_RESULT_SVG = os.path.join(HYBRID_OUT, "bench_result_merged.svg")
MERGED_STATS_SVG = os.path.join(HYBRID_OUT, "bench_stats_merged.svg")
DUAL_RELATIVE_SVG = os.path.join(HYBRID_OUT, "bench_dual_relative.svg")

LOCAL_MODES = os.environ.get("OPTBINLOG_HYBRID_LOCAL_MODES", "text,binary,syslog")
LINUX_MODES = os.environ.get("OPTBINLOG_HYBRID_LINUX_MODES", "ftrace")
BASELINE_MODE = os.environ.get("OPTBINLOG_BENCH_BASELINE", "text")
LINUX_BASELINE_MODE = os.environ.get("OPTBINLOG_HYBRID_LINUX_BASELINE", "text")
FORCE_LOCAL_BASELINE = os.environ.get("OPTBINLOG_HYBRID_FORCE_LOCAL_BASELINE", "1") != "0"
FORCE_LINUX_BASELINE = os.environ.get("OPTBINLOG_HYBRID_FORCE_LINUX_BASELINE", "1") != "0"

LINUX_INSTANCE = os.environ.get("OPTBINLOG_LINUX_INSTANCE", "thesis-linux")
TRACE_MARKER = os.environ.get("OPTBINLOG_TRACE_MARKER", "/sys/kernel/tracing/trace_marker")


def run(cmd, env=None, cwd=None):
    proc = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed\ncmd: {}\nstdout:\n{}\nstderr:\n{}".format(
                " ".join(cmd), proc.stdout, proc.stderr
            )
        )
    return proc.stdout


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


def ensure_baseline(modes, baseline, force_add):
    out = list(modes)
    if force_add and baseline and baseline not in out:
        out.append(baseline)
    return out


def mean_of(summary_by_mode, mode, metric):
    return float(summary_by_mode[mode][metric]["mean"])


def metric_improvement(base, cur, higher_better):
    if base == 0:
        return 0.0
    if higher_better:
        return (cur - base) / base * 100.0
    return (base - cur) / base * 100.0


def compute_by_mode_comparison(summary, baseline_mode, modes):
    base_write = mean_of(summary, baseline_mode, "write_only_ms")
    base_e2e = mean_of(summary, baseline_mode, "end_to_end_ms")
    base_total = mean_of(summary, baseline_mode, "total_bytes")
    base_thr_w = mean_of(summary, baseline_mode, "throughput_write_rps")
    base_thr_e = mean_of(summary, baseline_mode, "throughput_e2e_rps")

    by_mode = {}
    for mode in modes:
        if mode == baseline_mode:
            continue
        cur_write = mean_of(summary, mode, "write_only_ms")
        cur_e2e = mean_of(summary, mode, "end_to_end_ms")
        cur_total = mean_of(summary, mode, "total_bytes")
        cur_thr_w = mean_of(summary, mode, "throughput_write_rps")
        cur_thr_e = mean_of(summary, mode, "throughput_e2e_rps")
        by_mode[mode] = {
            "write_only_improve_pct": metric_improvement(base_write, cur_write, higher_better=False),
            "end_to_end_improve_pct": metric_improvement(base_e2e, cur_e2e, higher_better=False),
            "size_save_pct": metric_improvement(base_total, cur_total, higher_better=False),
            "throughput_write_gain_pct": metric_improvement(base_thr_w, cur_thr_w, higher_better=True),
            "throughput_e2e_gain_pct": metric_improvement(base_thr_e, cur_thr_e, higher_better=True),
        }
    return by_mode


def source_palette(source):
    if source == "local":
        return ["#2b8cbe", "#f03b20", "#31a354", "#756bb1", "#e6550d", "#636363"]
    return ["#1f78b4", "#33a02c", "#fb9a99", "#cab2d6", "#ff7f00", "#6a3d9a"]


def build_result_svg(summary, modes, baseline_mode, out_path):
    width = 1660
    height = 560
    margin = 70
    gap = 40
    panel_w = (width - margin * 2 - gap * 2) / 3.0
    panel_h = 320
    panel_y = 120
    panel_x = [margin + i * (panel_w + gap) for i in range(3)]

    panels = [
        ("end_to_end_ms", "Time (end_to_end_ms)", "milliseconds", False),
        ("total_bytes", "Space (total_bytes)", "bytes", False),
        ("throughput_e2e_rps", "Throughput (e2e)", "records/s", True),
    ]

    palette = ["#2b8cbe", "#f03b20", "#31a354", "#756bb1", "#e6550d", "#636363"]
    mode_color = {m: palette[i % len(palette)] for i, m in enumerate(modes)}

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width/2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">Hybrid benchmark merged result</text>')
    lines.append(f'<text x="{width/2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">global baseline={baseline_mode}</text>')

    for pi, (key, title, unit, higher_better) in enumerate(panels):
        x0 = panel_x[pi]
        vals = [summary[m][key]["mean"] for m in modes]
        vmax = max(vals + [1.0])
        base = summary[baseline_mode][key]["mean"]

        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#ffffff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y - 12}" text-anchor="middle" font-family="Arial" font-size="15">{title}</text>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y + panel_h + 52}" text-anchor="middle" font-family="Arial" font-size="12">{unit}</text>')

        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            val = vmax * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f0f0f0"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="11">{val:.2f}</text>')

        group_step = panel_w / max(1, len(modes))
        bar_w = min(56, group_step * 0.58)
        for i, mode in enumerate(modes):
            v = summary[mode][key]["mean"]
            h = 0 if vmax <= 0 else (v / vmax) * (panel_h * 0.9)
            x = x0 + i * group_step + (group_step - bar_w) / 2
            y = panel_y + panel_h - h
            color = mode_color[mode]
            lines.append(f'<rect x="{x}" y="{y}" width="{bar_w}" height="{h}" fill="{color}"/>')
            lines.append(f'<text x="{x + bar_w/2}" y="{y - 8}" text-anchor="middle" font-family="Arial" font-size="10">{v:.3f}</text>')
            lines.append(f'<text x="{x + bar_w/2}" y="{panel_y + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="11">{mode}</text>')
            if mode == baseline_mode:
                lines.append(f'<text x="{x + bar_w/2}" y="{y - 22}" text-anchor="middle" font-family="Arial" font-size="10" fill="#666">baseline</text>')
            elif base > 0:
                change = metric_improvement(base, v, higher_better=higher_better)
                sign = "+" if change >= 0 else "-"
                color2 = "#1b7837" if change >= 0 else "#b2182b"
                lines.append(
                    f'<text x="{x + bar_w/2}" y="{y - 22}" text-anchor="middle" font-family="Arial" font-size="10" fill="{color2}">{sign}{abs(change):.1f}%</text>'
                )

    lines.append(f'<text x="{margin}" y="{height - 24}" font-family="Arial" font-size="12">labels above bars: relative change vs global baseline</text>')
    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_stats_svg(summary, comparison, modes, baseline_mode, out_path):
    width = 1680
    height = 640
    panel_gap = 35
    panel_w = (width - 150 - panel_gap * 2) / 3.0
    panel_h = 360
    panel_y = 130
    panel_x0 = 75
    palette = ["#2b8cbe", "#f03b20", "#31a354", "#756bb1", "#e6550d", "#636363"]
    mode_color = {m: palette[i % len(palette)] for i, m in enumerate(modes)}

    panels = [
        ("end_to_end_ms", "end_to_end_ms", "milliseconds"),
        ("total_bytes", "total_bytes", "bytes"),
        ("throughput_e2e_rps", "throughput_e2e_rps", "records/s"),
    ]

    def value_range(metric):
        vals = []
        for mode in modes:
            m = summary[mode][metric]
            vals.extend(
                [m["mean"], m["median"], m["p95"], m["ci95_low"], m["ci95_high"], m["mean"] - m["std"], m["mean"] + m["std"]]
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
        cmp_lines.append(f"{mode}: e2e={c['end_to_end_improve_pct']:.1f}% size={c['size_save_pct']:.1f}% thr_e2e={c['throughput_e2e_gain_pct']:.1f}%")

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">Hybrid benchmark stats summary</text>')
    lines.append(f'<text x="{width / 2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">global baseline={baseline_mode}; metrics=mean/median/p95/std/95%CI</text>')
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
            f'<text x="{x0 - 36}" y="{panel_y + panel_h / 2}" transform="rotate(-90,{x0 - 36},{panel_y + panel_h / 2})" text-anchor="middle" font-family="Arial" font-size="12">{unit}</text>'
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


def build_dual_relative_svg(summary, source_modes, source_comp, out_path):
    width = 1660
    height = 660
    margin = 70
    panel_gap = 40
    panel_w = (width - margin * 2 - panel_gap) / 2.0
    panel_h = 400
    panel_top = 150
    panels = [("local", margin), ("linux", margin + panel_w + panel_gap)]

    metric_defs = [
        ("end_to_end_improve_pct", "time", "#2b8cbe"),
        ("size_save_pct", "size", "#31a354"),
        ("throughput_e2e_gain_pct", "throughput", "#f03b20"),
    ]

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">Dual-baseline relative improvements</text>')
    lines.append(
        f'<text x="{width / 2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">positive means better (time/size lower is better, throughput higher is better)</text>'
    )

    for source, x0 in panels:
        modes = source_modes.get(source, [])
        comp = source_comp.get(source, {})
        baseline = comp.get("baseline_mode")
        by_mode = comp.get("by_mode", {})

        lines.append(f'<rect x="{x0}" y="{panel_top}" width="{panel_w}" height="{panel_h}" fill="#ffffff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{panel_top - 14}" text-anchor="middle" font-family="Arial" font-size="16">{source}</text>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{panel_top + 20}" text-anchor="middle" font-family="Arial" font-size="12">baseline={baseline}</text>')

        compare_modes = [m for m in modes if m != baseline and m in by_mode]
        if not compare_modes:
            lines.append(
                f'<text x="{x0 + panel_w / 2}" y="{panel_top + panel_h / 2}" text-anchor="middle" font-family="Arial" font-size="13" fill="#b2182b">no comparable modes in this source</text>'
            )
            continue

        vals = []
        for mode in compare_modes:
            row = by_mode[mode]
            vals.extend([row["end_to_end_improve_pct"], row["size_save_pct"], row["throughput_e2e_gain_pct"]])

        max_abs = max(10.0, max(abs(v) for v in vals))
        max_abs = min(250.0, max_abs + 8.0)
        lo = -max_abs
        hi = max_abs

        def y_map(v):
            return panel_top + 45 + (hi - v) / (hi - lo) * (panel_h - 85)

        y_zero = y_map(0.0)
        lines.append(f'<line x1="{x0 + 44}" y1="{y_zero}" x2="{x0 + panel_w - 16}" y2="{y_zero}" stroke="#666"/>')
        for i in range(5):
            frac = i / 4.0
            val = lo + (hi - lo) * frac
            y = y_map(val)
            lines.append(f'<line x1="{x0 + 44}" y1="{y}" x2="{x0 + panel_w - 16}" y2="{y}" stroke="#f0f0f0"/>')
            lines.append(f'<text x="{x0 + 38}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="11">{val:.0f}%</text>')

        group_w = (panel_w - 70) / max(1, len(compare_modes))
        bar_w = min(20.0, group_w / 4.0)
        for i, mode in enumerate(compare_modes):
            cx = x0 + 50 + (i + 0.5) * group_w
            row = by_mode[mode]
            metric_values = [row["end_to_end_improve_pct"], row["size_save_pct"], row["throughput_e2e_gain_pct"]]
            for j, (metric_key, _label, color) in enumerate(metric_defs):
                _ = metric_key
                v = metric_values[j]
                bx = cx + (j - 1) * (bar_w + 4) - bar_w / 2
                by = y_map(max(0.0, v))
                bh = abs(y_map(v) - y_zero)
                lines.append(f'<rect x="{bx}" y="{by}" width="{bar_w}" height="{bh}" fill="{color}"/>')
                ty = by - 6 if v >= 0 else by + bh + 12
                lines.append(f'<text x="{bx + bar_w / 2}" y="{ty}" text-anchor="middle" font-family="Arial" font-size="10">{v:.1f}%</text>')
            lines.append(f'<text x="{cx}" y="{panel_top + panel_h - 12}" text-anchor="middle" font-family="Arial" font-size="11">{mode}</text>')

    legend_y = height - 34
    lx = 110
    for _key, label, color in metric_defs:
        lines.append(f'<rect x="{lx}" y="{legend_y - 11}" width="16" height="10" fill="{color}"/>')
        lines.append(f'<text x="{lx + 22}" y="{legend_y - 2}" font-family="Arial" font-size="12">{label}</text>')
        lx += 140
    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def clone_rows_with_mode_map(rows, mode_map, source):
    out = []
    for r in rows:
        old_mode = r["mode"]
        if old_mode not in mode_map:
            continue
        row = dict(r)
        row["mode"] = mode_map[old_mode]
        row["source"] = source
        out.append(row)
    return out


def merge_results(local_data, linux_data):
    summary = {}
    runs = []
    source_modes = {"local": [], "linux": []}
    source_mode_map = {"local": {}, "linux": {}}

    def add_mode(source, mode, mode_summary):
        new_name = mode
        if new_name in summary:
            new_name = f"{mode}@{source}"
            idx = 2
            while new_name in summary:
                new_name = f"{mode}@{source}{idx}"
                idx += 1
        summary[new_name] = mode_summary
        source_modes[source].append(new_name)
        source_mode_map[source][mode] = new_name

    for mode in local_data["config"]["active_modes"]:
        add_mode("local", mode, local_data["summary"][mode])
    for mode in linux_data["config"]["active_modes"]:
        add_mode("linux", mode, linux_data["summary"][mode])

    runs.extend(clone_rows_with_mode_map(local_data["runs"], source_mode_map["local"], "local"))
    runs.extend(clone_rows_with_mode_map(linux_data["runs"], source_mode_map["linux"], "linux"))

    local_baseline = source_mode_map["local"].get(BASELINE_MODE, source_modes["local"][0])
    linux_baseline = source_mode_map["linux"].get(LINUX_BASELINE_MODE)
    if linux_baseline is None:
        linux_baseline = source_mode_map["linux"].get(BASELINE_MODE, source_modes["linux"][0])

    global_baseline = local_baseline
    global_comparison = {
        "baseline_mode": global_baseline,
        "by_mode": compute_by_mode_comparison(summary, global_baseline, list(summary.keys())),
    }

    source_comparison = {
        "local": {
            "baseline_mode": local_baseline,
            "by_mode": compute_by_mode_comparison(summary, local_baseline, source_modes["local"]),
        },
        "linux": {
            "baseline_mode": linux_baseline,
            "by_mode": compute_by_mode_comparison(summary, linux_baseline, source_modes["linux"]),
        },
    }

    merged = {
        "config": {
            "strategy": "local_non_ftrace + linux_ftrace(+linux_baseline)",
            "records": local_data["config"]["records"],
            "repeats": local_data["config"]["repeats"],
            "warmup": local_data["config"]["warmup"],
            "iqr_mult": local_data["config"]["iqr_mult"],
            "filter_field": local_data["config"]["filter_field"],
            "local_modes_requested": parse_modes(LOCAL_MODES),
            "linux_modes_requested": parse_modes(LINUX_MODES),
            "local_modes_active": source_modes["local"],
            "linux_modes_active": source_modes["linux"],
            "active_modes": list(summary.keys()),
            "baseline_mode": global_baseline,
            "linux_instance": LINUX_INSTANCE,
            "trace_marker": TRACE_MARKER,
            "linux_baseline_mode": linux_baseline,
        },
        "runs": runs,
        "summary": summary,
        "comparison": global_comparison,
        "source_comparison": source_comparison,
        "source_modes": source_modes,
        "sources": {
            "local_result": os.path.join(LOCAL_OUT, "bench_result.json"),
            "linux_result": os.path.join(LINUX_OUT, "bench_result.json"),
        },
        "skipped_modes": {
            "local": local_data.get("skipped_modes", {}),
            "linux": linux_data.get("skipped_modes", {}),
        },
    }
    return merged


def propagate_common_env(env):
    keys = [
        "OPTBINLOG_BENCH_RECORDS",
        "OPTBINLOG_BENCH_REPEATS",
        "OPTBINLOG_BENCH_WARMUP",
        "OPTBINLOG_BENCH_IQR_MULT",
        "OPTBINLOG_BENCH_FILTER_FIELD",
        "OPTBINLOG_BENCH_BASELINE",
    ]
    for k in keys:
        if k in os.environ:
            env[k] = os.environ[k]


def build_local_binary():
    cmd = [
        "clang",
        "-O2",
        "-Wall",
        "-Wextra",
        "-std=c11",
        "-Iinclude",
        "-o",
        LOCAL_BIN,
        "optbinlog_bench.c",
        "src/optbinlog_shared.c",
        "src/optbinlog_eventlog.c",
        "src/optbinlog_binlog.c",
    ]
    run(cmd, cwd=ROOT)


def build_linux_binary():
    script = (
        "set -euo pipefail; "
        f"cd {json.dumps(ROOT)}; "
        "gcc -O2 -Wall -Wextra -std=c11 -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L "
        "-Iinclude -o optbinlog_bench_linux optbinlog_bench.c "
        "src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c"
    )
    run(["limactl", "shell", LINUX_INSTANCE, "--", "bash", "-lc", script], cwd=PROJECT)


def ensure_linux_trace_marker_access():
    script = (
        "set -e; "
        f"if sudo test -e {json.dumps(TRACE_MARKER)}; then "
        f"  sudo chgrp sky {json.dumps(TRACE_MARKER)} || true; "
        f"  sudo chmod g+w {json.dumps(TRACE_MARKER)} || true; "
        "fi"
    )
    run(["limactl", "shell", LINUX_INSTANCE, "--", "bash", "-lc", script], cwd=PROJECT)


def run_local(modes):
    env = os.environ.copy()
    env["OPTBINLOG_BENCH_MODES"] = ",".join(modes)
    env["OPTBINLOG_BENCH_BASELINE"] = BASELINE_MODE
    env["OPTBINLOG_BENCH_BIN"] = LOCAL_BIN
    env["OPTBINLOG_BENCH_OUT_DIR"] = LOCAL_OUT
    propagate_common_env(env)
    run(["python3", RUN_BENCH], env=env, cwd=ROOT)


def run_linux(modes):
    linux_env = {
        "OPTBINLOG_BENCH_MODES": ",".join(modes),
        "OPTBINLOG_BENCH_BASELINE": LINUX_BASELINE_MODE if LINUX_BASELINE_MODE in modes else modes[0],
        "OPTBINLOG_BENCH_BIN": LINUX_BIN,
        "OPTBINLOG_BENCH_OUT_DIR": LINUX_OUT,
        "OPTBINLOG_TRACE_MARKER": TRACE_MARKER,
    }
    for k in [
        "OPTBINLOG_BENCH_RECORDS",
        "OPTBINLOG_BENCH_REPEATS",
        "OPTBINLOG_BENCH_WARMUP",
        "OPTBINLOG_BENCH_IQR_MULT",
        "OPTBINLOG_BENCH_FILTER_FIELD",
    ]:
        if k in os.environ:
            linux_env[k] = os.environ[k]

    exports = " ".join([f"{k}={json.dumps(v)}" for k, v in linux_env.items()])
    script = "set -euo pipefail; " + f"cd {json.dumps(ROOT)}; " + f"export {exports}; " + "python3 run_bench.py"
    run(["limactl", "shell", LINUX_INSTANCE, "--", "bash", "-lc", script], cwd=PROJECT)


def main():
    os.makedirs(LOCAL_OUT, exist_ok=True)
    os.makedirs(LINUX_OUT, exist_ok=True)
    os.makedirs(HYBRID_OUT, exist_ok=True)

    local_modes = ensure_baseline(parse_modes(LOCAL_MODES), BASELINE_MODE, FORCE_LOCAL_BASELINE)
    linux_modes = ensure_baseline(parse_modes(LINUX_MODES), LINUX_BASELINE_MODE, FORCE_LINUX_BASELINE)

    build_local_binary()
    run_local(local_modes)
    build_linux_binary()
    ensure_linux_trace_marker_access()
    run_linux(linux_modes)

    local_json = os.path.join(LOCAL_OUT, "bench_result.json")
    linux_json = os.path.join(LINUX_OUT, "bench_result.json")
    with open(local_json, "r", encoding="utf-8") as f:
        local_data = json.load(f)
    with open(linux_json, "r", encoding="utf-8") as f:
        linux_data = json.load(f)

    merged = merge_results(local_data, linux_data)
    with open(MERGED_JSON, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2)

    modes = merged["config"]["active_modes"]
    baseline = merged["config"]["baseline_mode"]
    summary = merged["summary"]
    comparison = merged["comparison"]
    build_result_svg(summary, modes, baseline, MERGED_RESULT_SVG)
    build_stats_svg(summary, comparison, modes, baseline, MERGED_STATS_SVG)
    build_dual_relative_svg(summary, merged["source_modes"], merged["source_comparison"], DUAL_RELATIVE_SVG)

    print("saved", MERGED_JSON)
    print("saved", MERGED_RESULT_SVG)
    print("saved", MERGED_STATS_SVG)
    print("saved", DUAL_RELATIVE_SVG)
    print("local baseline:", merged["source_comparison"]["local"]["baseline_mode"])
    print("linux baseline:", merged["source_comparison"]["linux"]["baseline_mode"])
    for mode in modes:
        s = summary[mode]
        print(
            f"{mode}: e2e_mean={s['end_to_end_ms']['mean']:.3f}ms "
            f"e2e_p95={s['end_to_end_ms']['p95']:.3f}ms "
            f"total_bytes={s['total_bytes']['mean']:.1f} "
            f"thr_e2e={s['throughput_e2e_rps']['mean']:.1f}rps "
            f"kept={s['filter'].get('kept', 0)}"
        )


if __name__ == "__main__":
    main()
