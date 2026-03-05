#!/usr/bin/env python3
import argparse
import json
import math
import os
from typing import Dict, List, Tuple


def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    pos = (len(sorted_vals) - 1) * p
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_vals[lo])
    frac = pos - lo
    return float(sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac)


def iqr_filter(vals: List[float], mult: float = 1.5) -> Tuple[List[float], int]:
    if len(vals) < 4:
        return list(vals), 0
    s = sorted(vals)
    q1 = percentile(s, 0.25)
    q3 = percentile(s, 0.75)
    iqr = q3 - q1
    lo = q1 - mult * iqr
    hi = q3 + mult * iqr
    kept = [x for x in vals if lo <= x <= hi]
    if not kept:
        return list(vals), 0
    return kept, len(vals) - len(kept)


def stats(vals: List[float]) -> Dict[str, float]:
    if not vals:
        return {
            "count": 0,
            "mean": 0.0,
            "median": 0.0,
            "p95": 0.0,
            "std": 0.0,
            "ci95_low": 0.0,
            "ci95_high": 0.0,
            "min": 0.0,
            "max": 0.0,
        }
    s = sorted(vals)
    n = len(s)
    mean = sum(s) / n
    median = percentile(s, 0.5)
    p95 = percentile(s, 0.95)
    if n > 1:
        var = sum((x - mean) ** 2 for x in s) / (n - 1)
        std = math.sqrt(var)
    else:
        std = 0.0
    ci = 1.96 * std / math.sqrt(n) if n > 1 else 0.0
    return {
        "count": n,
        "mean": mean,
        "median": median,
        "p95": p95,
        "std": std,
        "ci95_low": mean - ci,
        "ci95_high": mean + ci,
        "min": s[0],
        "max": s[-1],
    }


def gain_pct(base: float, cur: float, higher_better: bool) -> float:
    if base == 0:
        return 0.0
    if higher_better:
        return (cur - base) / base * 100.0
    return (base - cur) / base * 100.0


def load_modes(path: str) -> List[str]:
    data = json.load(open(path, "r", encoding="utf-8"))
    for node in data.get("nodes", []):
        sm = node.get("summary", {})
        modes = sm.get("modes", [])
        if modes:
            return modes
    return []


def collect_mode_metric(data: dict, mode: str, metric_key: str) -> List[float]:
    out: List[float] = []
    for node in data.get("nodes", []):
        sm = node.get("summary", {})
        bm = sm.get("by_mode", {}).get(mode, {})
        if metric_key in bm:
            out.append(float(bm[metric_key]))
    return out


def summarize_group(data: dict, modes: List[str], iqr_mult: float) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for mode in modes:
        t_raw = collect_mode_metric(data, mode, "end_to_end_ms_mean")
        s_raw = collect_mode_metric(data, mode, "total_bytes_mean")
        th_raw = collect_mode_metric(data, mode, "throughput_e2e_rps_mean")
        t_kept, t_removed = iqr_filter(t_raw, iqr_mult)
        s_kept, s_removed = iqr_filter(s_raw, iqr_mult)
        th_kept, th_removed = iqr_filter(th_raw, iqr_mult)
        out[mode] = {
            "time_ms": stats(t_kept),
            "bytes": stats(s_kept),
            "throughput_rps": stats(th_kept),
            "raw_counts": {
                "time_ms": len(t_raw),
                "bytes": len(s_raw),
                "throughput_rps": len(th_raw),
            },
            "iqr_removed": {
                "time_ms": t_removed,
                "bytes": s_removed,
                "throughput_rps": th_removed,
            },
        }
    return out


def build_svg(out_path: str, summary: dict, labels: List[str], modes: List[str]):
    width = 1700
    height = 620
    margin = 70
    gap = 28
    panel_w = (width - margin * 2 - gap * 2) / 3.0
    panel_h = 360
    panel_y = 150
    colors = ["#2b8cbe", "#f03b20", "#31a354", "#756bb1", "#e6550d", "#636363", "#1f78b4", "#33a02c"]
    mode_color = {m: colors[i % len(colors)] for i, m in enumerate(modes)}

    metrics = [
        ("time_ms", "Time (end_to_end, ms)", False),
        ("bytes", "Space (bytes)", False),
        ("throughput_rps", "Throughput (records/s)", True),
    ]

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width/2}" y="34" text-anchor="middle" font-family="Arial" font-size="22">L1 5 vs 10 Real Devices</text>')
    lines.append(f'<text x="{width/2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">IQR-filtered node-level means + 95%CI</text>')

    for pi, (metric, title, higher_better) in enumerate(metrics):
        x0 = margin + pi * (panel_w + gap)
        vals = []
        for lb in labels:
            for m in modes:
                vals.append(summary[lb]["by_mode"][m][metric]["mean"])
        vmax = max(vals + [1.0])
        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y - 14}" text-anchor="middle" font-family="Arial" font-size="15">{title}</text>')
        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            v = vmax * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f0f0f0"/>')
            lines.append(f'<text x="{x0 - 6}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.2f}</text>')

        groups = len(modes)
        cluster_w = panel_w / max(1, groups)
        bw = min(24.0, cluster_w * 0.28)
        for i, mode in enumerate(modes):
            gx = x0 + i * cluster_w + cluster_w * 0.1
            left = gx
            right = gx + bw + 8
            for j, lb in enumerate(labels):
                x = left if j == 0 else right
                st = summary[lb]["by_mode"][mode][metric]
                mean = st["mean"]
                h = 0 if vmax <= 0 else mean / vmax * panel_h * 0.9
                y = panel_y + panel_h - h
                lines.append(f'<rect x="{x}" y="{y}" width="{bw}" height="{h}" fill="{mode_color[mode]}" opacity="{0.75 if j == 0 else 1.0}"/>')
                lo = st["ci95_low"]
                hi = st["ci95_high"]
                y_lo = panel_y + panel_h - (lo / vmax * panel_h * 0.9 if vmax > 0 else 0)
                y_hi = panel_y + panel_h - (hi / vmax * panel_h * 0.9 if vmax > 0 else 0)
                cx = x + bw / 2
                lines.append(f'<line x1="{cx}" y1="{y_lo}" x2="{cx}" y2="{y_hi}" stroke="#111" stroke-width="1"/>')
                lines.append(f'<line x1="{cx-3}" y1="{y_lo}" x2="{cx+3}" y2="{y_lo}" stroke="#111" stroke-width="1"/>')
                lines.append(f'<line x1="{cx-3}" y1="{y_hi}" x2="{cx+3}" y2="{y_hi}" stroke="#111" stroke-width="1"/>')
                lines.append(f'<text x="{x + bw/2}" y="{y - 6}" text-anchor="middle" font-family="Arial" font-size="9">{mean:.2f}</text>')

            v_a = summary[labels[0]]["by_mode"][mode][metric]["mean"]
            v_b = summary[labels[1]]["by_mode"][mode][metric]["mean"]
            gp = gain_pct(v_a, v_b, higher_better=higher_better)
            c = "#1b7837" if gp >= 0 else "#b2182b"
            sign = "+" if gp >= 0 else "-"
            lines.append(f'<text x="{gx + bw + 4}" y="{panel_y + panel_h + 14}" text-anchor="middle" font-family="Arial" font-size="9" fill="{c}">{sign}{abs(gp):.1f}%</text>')
            lines.append(f'<text x="{gx + bw + 4}" y="{panel_y + panel_h + 28}" text-anchor="middle" font-family="Arial" font-size="9">{mode}</text>')

    legend_y = 95
    lines.append(f'<rect x="{margin}" y="{legend_y-12}" width="14" height="10" fill="#666" opacity="0.75"/>')
    lines.append(f'<text x="{margin+20}" y="{legend_y-3}" font-family="Arial" font-size="11">{labels[0]}</text>')
    lines.append(f'<rect x="{margin+120}" y="{legend_y-12}" width="14" height="10" fill="#666" opacity="1.0"/>')
    lines.append(f'<text x="{margin+140}" y="{legend_y-3}" font-family="Arial" font-size="11">{labels[1]}</text>')
    lines.append(f'<text x="{margin}" y="{height-24}" font-family="Arial" font-size="11">Bottom %: change from {labels[0]} to {labels[1]} for each mode</text>')
    lines.append("</svg>")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    ap = argparse.ArgumentParser(description="Compare two L1 suite summaries")
    ap.add_argument("--a", required=True, help="Path to first l1_summary.json (e.g., 5 nodes)")
    ap.add_argument("--b", required=True, help="Path to second l1_summary.json (e.g., 10 nodes)")
    ap.add_argument("--label-a", default="5 nodes")
    ap.add_argument("--label-b", default="10 nodes")
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-svg", required=True)
    ap.add_argument("--iqr-mult", type=float, default=1.5)
    args = ap.parse_args()

    data_a = json.load(open(args.a, "r", encoding="utf-8"))
    data_b = json.load(open(args.b, "r", encoding="utf-8"))

    modes_a = set(load_modes(args.a))
    modes_b = set(load_modes(args.b))
    modes = [m for m in load_modes(args.a) if m in modes_b]
    if not modes:
        modes = sorted(modes_a & modes_b)

    sum_a = summarize_group(data_a, modes, args.iqr_mult)
    sum_b = summarize_group(data_b, modes, args.iqr_mult)

    out = {
        "generated_from": {"a": args.a, "b": args.b},
        "labels": {"a": args.label_a, "b": args.label_b},
        "iqr_mult": args.iqr_mult,
        "modes": modes,
        "groups": {
            args.label_a: {"nodes": len(data_a.get("nodes", [])), "by_mode": sum_a},
            args.label_b: {"nodes": len(data_b.get("nodes", [])), "by_mode": sum_b},
        },
        "delta_pct_b_vs_a": {},
    }

    for mode in modes:
        out["delta_pct_b_vs_a"][mode] = {
            "time_ms": gain_pct(sum_a[mode]["time_ms"]["mean"], sum_b[mode]["time_ms"]["mean"], higher_better=False),
            "bytes": gain_pct(sum_a[mode]["bytes"]["mean"], sum_b[mode]["bytes"]["mean"], higher_better=False),
            "throughput_rps": gain_pct(sum_a[mode]["throughput_rps"]["mean"], sum_b[mode]["throughput_rps"]["mean"], higher_better=True),
        }

    os.makedirs(os.path.dirname(os.path.abspath(args.out_json)), exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    build_svg(
        args.out_svg,
        {args.label_a: out["groups"][args.label_a], args.label_b: out["groups"][args.label_b]},
        [args.label_a, args.label_b],
        modes,
    )
    print("saved", args.out_json)
    print("saved", args.out_svg)


if __name__ == "__main__":
    main()

