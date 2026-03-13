#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import math
import os
import shutil
import subprocess
from typing import Dict, List, Tuple


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
    p = argparse.ArgumentParser(description="Scan record volume to estimate binary/peer space crossover.")
    p.add_argument("--out-dir", default="", help="Output directory")
    p.add_argument("--bench-bin", default=os.path.join(ROOT, "optbinlog_bench_macos"))
    p.add_argument("--records", default="1,2,5,10,20,50,100,200,500,1000,5000,20000,100000")
    p.add_argument("--repeats", type=int, default=2)
    p.add_argument("--warmup", type=int, default=0)
    return p.parse_args()


def parse_records(raw: str) -> List[int]:
    out: List[int] = []
    seen = set()
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        n = int(text)
        if n <= 0:
            raise ValueError(f"invalid record count: {n}")
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    if not out:
        raise ValueError("record list is empty")
    return out


def run_cmd(cmd: List[str], cwd: str = ROOT, env: Dict[str, str] = None) -> None:
    proc = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed\ncmd: {}\nstdout:\n{}\nstderr:\n{}".format(" ".join(cmd), proc.stdout, proc.stderr)
        )


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def fit_line(xs: List[float], ys: List[float]) -> Tuple[float, float]:
    if len(xs) != len(ys) or not xs:
        return 0.0, 0.0
    if len(xs) == 1:
        return 0.0, float(ys[0])
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    sxx = sum((x - mx) * (x - mx) for x in xs)
    if sxx == 0:
        return 0.0, my
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    k = sxy / sxx
    b = my - k * mx
    return k, b


def estimate_crossover(records: List[int], binary_vals: List[float], peer_vals: List[float]) -> dict:
    xs = [float(x) for x in records]
    kb, bb = fit_line(xs, binary_vals)
    kp, bp = fit_line(xs, peer_vals)
    slope_diff = kb - kp
    intercept_diff = bb - bp
    model_cross = None
    if abs(slope_diff) > 1e-12:
        model_cross = (bp - bb) / slope_diff

    empirical_first = None
    for n, b, p in zip(records, binary_vals, peer_vals):
        if b <= p:
            empirical_first = n
            break

    max_n = max(records)
    min_n = min(records)
    status = ""
    if empirical_first is not None:
        status = f"empirical crossover observed at N={empirical_first}"
    else:
        if model_cross is None:
            if intercept_diff < 0:
                status = "binary is always smaller in fitted model (parallel lines)"
            elif intercept_diff > 0:
                status = "binary is always larger in fitted model (parallel lines)"
            else:
                status = "binary and peer overlap in fitted model"
        elif model_cross < min_n:
            status = f"model crossover before scan range (N≈{model_cross:.1f})"
        elif model_cross > max_n:
            status = f"model crossover after scan range (N≈{model_cross:.1f})"
        else:
            status = f"model crossover inside range (N≈{model_cross:.1f}), but no direct observed point"

    return {
        "binary_fit": {"k": kb, "b": bb},
        "peer_fit": {"k": kp, "b": bp},
        "model_cross_records": model_cross,
        "empirical_cross_records": empirical_first,
        "status": status,
    }


def build_space_scan_svg(rows: List[dict], out_path: str) -> None:
    width = 1600
    height = 980
    cols = 2
    rows_n = 2
    margin = 72
    gap_x = 64
    gap_y = 92
    panel_w = (width - margin * 2 - gap_x) / cols
    panel_h = (height - margin * 2 - gap_y) / rows_n

    all_records = []
    all_bytes = []
    for row in rows:
        for p in row["points"]:
            all_records.append(float(p["records"]))
            all_bytes.extend([float(p["text_bytes"]), float(p["binary_bytes"]), float(p["peer_bytes"])])
    min_r = min(all_records)
    max_r = max(all_records)
    min_b = min(all_bytes)
    max_b = max(all_bytes)
    if max_b <= min_b:
        max_b = min_b + 1.0
    y_pad = max((max_b - min_b) * 0.08, 1.0)
    y_lo = min_b - y_pad
    y_hi = max_b + y_pad

    def x_map(x: float, x0: float) -> float:
        if max_r == min_r:
            return x0 + panel_w / 2
        lx = math.log10(x)
        l0 = math.log10(min_r)
        l1 = math.log10(max_r)
        return x0 + ((lx - l0) / (l1 - l0)) * panel_w

    def y_map(v: float, y0: float) -> float:
        return y0 + panel_h - ((v - y_lo) / (y_hi - y_lo)) * panel_h

    colors = {
        "text": "#7f7f7f",
        "binary": "#1f78b4",
        "peer": "#e31a1c",
    }

    lines: List[str] = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="22">Space Trend vs Record Volume (Strict Aligned)</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="13">x-axis uses log10(N); y-axis is total_bytes mean per run</text>')

    for i, row in enumerate(rows):
        col = i % cols
        rr = i // cols
        x0 = margin + col * (panel_w + gap_x)
        y0 = margin + rr * (panel_h + gap_y) + 16
        lines.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{y0 - 10}" text-anchor="middle" font-family="Arial" font-size="16">{row["profile"]}</text>')

        for t in range(6):
            frac = t / 5.0
            y = y0 + panel_h * (1.0 - frac)
            v = y_lo + (y_hi - y_lo) * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f2f2f2"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.0f}</text>')

        for n in sorted(set(p["records"] for p in row["points"])):
            x = x_map(float(n), x0)
            lines.append(f'<line x1="{x}" y1="{y0}" x2="{x}" y2="{y0 + panel_h}" stroke="#f8f8f8"/>')
            lines.append(f'<text x="{x}" y="{y0 + panel_h + 16}" text-anchor="middle" font-family="Arial" font-size="10">{n}</text>')

        peer_mode = row["peer_mode"]
        series = [
            ("text", "text_bytes", "text"),
            ("binary", "binary_bytes", "binary"),
            (peer_mode, "peer_bytes", "peer"),
        ]
        for label, key, color_key in series:
            pts = " ".join(
                f'{x_map(float(p["records"]), x0)},{y_map(float(p[key]), y0)}' for p in row["points"]
            )
            lines.append(
                f'<polyline fill="none" stroke="{colors[color_key]}" stroke-width="2.2" points="{pts}"/>'
            )
            for p in row["points"]:
                lines.append(
                    f'<circle cx="{x_map(float(p["records"]), x0)}" cy="{y_map(float(p[key]), y0)}" r="3.2" fill="{colors[color_key]}"/>'
                )

        legend_x = x0 + panel_w - 170
        legend_y = y0 + 16
        items = [("text", colors["text"]), ("binary", colors["binary"]), (peer_mode, colors["peer"])]
        for li, (label, color) in enumerate(items):
            yy = legend_y + li * 16
            lines.append(f'<line x1="{legend_x}" y1="{yy}" x2="{legend_x + 16}" y2="{yy}" stroke="{color}" stroke-width="2.2"/>')
            lines.append(f'<text x="{legend_x + 22}" y="{yy + 4}" font-family="Arial" font-size="10">{label}</text>')

    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_delta_svg(rows: List[dict], out_path: str) -> None:
    width = 1600
    height = 980
    cols = 2
    rows_n = 2
    margin = 72
    gap_x = 64
    gap_y = 92
    panel_w = (width - margin * 2 - gap_x) / cols
    panel_h = (height - margin * 2 - gap_y) / rows_n

    all_records = []
    all_delta = []
    for row in rows:
        for p in row["points"]:
            all_records.append(float(p["records"]))
            all_delta.append(float(p["binary_minus_peer"]))
    min_r = min(all_records)
    max_r = max(all_records)
    min_d = min(all_delta)
    max_d = max(all_delta)
    lo = min(min_d, 0.0)
    hi = max(max_d, 0.0)
    if hi <= lo:
        hi = lo + 1.0
    pad = max((hi - lo) * 0.08, 1.0)
    lo -= pad
    hi += pad

    def x_map(x: float, x0: float) -> float:
        if max_r == min_r:
            return x0 + panel_w / 2
        lx = math.log10(x)
        l0 = math.log10(min_r)
        l1 = math.log10(max_r)
        return x0 + ((lx - l0) / (l1 - l0)) * panel_w

    def y_map(v: float, y0: float) -> float:
        return y0 + panel_h - ((v - lo) / (hi - lo)) * panel_h

    lines: List[str] = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="22">Binary - Peer Space Delta vs Record Volume</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="13">delta = binary_bytes - peer_bytes (below 0 means binary is smaller)</text>')

    for i, row in enumerate(rows):
        col = i % cols
        rr = i // cols
        x0 = margin + col * (panel_w + gap_x)
        y0 = margin + rr * (panel_h + gap_y) + 16
        lines.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{y0 - 10}" text-anchor="middle" font-family="Arial" font-size="16">{row["profile"]}</text>')

        for t in range(6):
            frac = t / 5.0
            y = y0 + panel_h * (1.0 - frac)
            v = lo + (hi - lo) * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f2f2f2"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.0f}</text>')

        y_zero = y_map(0.0, y0)
        lines.append(f'<line x1="{x0}" y1="{y_zero}" x2="{x0 + panel_w}" y2="{y_zero}" stroke="#999" stroke-width="1.4"/>')

        for n in sorted(set(p["records"] for p in row["points"])):
            x = x_map(float(n), x0)
            lines.append(f'<line x1="{x}" y1="{y0}" x2="{x}" y2="{y0 + panel_h}" stroke="#f8f8f8"/>')
            lines.append(f'<text x="{x}" y="{y0 + panel_h + 16}" text-anchor="middle" font-family="Arial" font-size="10">{n}</text>')

        pts = " ".join(
            f'{x_map(float(p["records"]), x0)},{y_map(float(p["binary_minus_peer"]), y0)}' for p in row["points"]
        )
        lines.append(f'<polyline fill="none" stroke="#1f78b4" stroke-width="2.2" points="{pts}"/>')
        for p in row["points"]:
            v = float(p["binary_minus_peer"])
            color = "#1b7837" if v <= 0 else "#b2182b"
            lines.append(
                f'<circle cx="{x_map(float(p["records"]), x0)}" cy="{y_map(v, y0)}" r="3.2" fill="{color}"/>'
            )

        cross = row["crossover"].get("empirical_cross_records")
        if cross is not None:
            lines.append(
                f'<text x="{x0 + 8}" y="{y0 + 18}" font-family="Arial" font-size="10" fill="#1b7837">'
                f'empirical cross @ N={cross}</text>'
            )
        else:
            model = row["crossover"].get("model_cross_records")
            if model is not None:
                lines.append(
                    f'<text x="{x0 + 8}" y="{y0 + 18}" font-family="Arial" font-size="10" fill="#444">'
                    f'model cross ≈ N={model:.1f}</text>'
                )
            else:
                lines.append(
                    f'<text x="{x0 + 8}" y="{y0 + 18}" font-family="Arial" font-size="10" fill="#444">'
                    f'no model cross</text>'
                )

    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def build_report(rows: List[dict], records: List[int], out_path: str) -> None:
    lines: List[str] = []
    lines.append("# Space Crossover Scan Report")
    lines.append("")
    lines.append("## Design")
    lines.append("")
    lines.append("- Modes: `text_semantic_like`, `binary`, `peer semantic_like`")
    lines.append(f"- Record scan: `{','.join(str(x) for x in records)}`")
    lines.append("- Metric: `total_bytes` mean per run")
    lines.append("- Crossover definition: earliest `N` where `binary_bytes <= peer_bytes`")
    lines.append("")
    lines.append("## Profile Summary")
    lines.append("")
    lines.append("| profile | peer mode | binary@Nmin bytes | peer@Nmin bytes | binary@Nmax bytes | peer@Nmax bytes | empirical cross | model cross | interpretation |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---|")
    for row in rows:
        p0 = row["points"][0]
        p1 = row["points"][-1]
        cross = row["crossover"]
        empirical = cross["empirical_cross_records"]
        model = cross["model_cross_records"]
        lines.append(
            "| {} | {} | {:.1f} | {:.1f} | {:.1f} | {:.1f} | {} | {} | {} |".format(
                row["profile"],
                row["peer_mode"],
                p0["binary_bytes"],
                p0["peer_bytes"],
                p1["binary_bytes"],
                p1["peer_bytes"],
                empirical if empirical is not None else "-",
                f"{model:.1f}" if model is not None else "-",
                cross["status"],
            )
        )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- `binary_minus_peer = binary_bytes - peer_bytes` ; negative means binary has better space efficiency.")
    lines.append("- For groups with no crossover in scan range, model-cross is reported only as trend indication.")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    args = parse_args()
    records = parse_records(args.records)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = args.out_dir.strip() or os.path.join(RESULTS_ROOT, f"space_crossover_scan_{ts}")
    if os.path.exists(out_root):
        shutil.rmtree(out_root)
    os.makedirs(out_root, exist_ok=True)

    rows = []
    for profile in PROFILES:
        profile_out = os.path.join(out_root, profile["name"])
        os.makedirs(profile_out, exist_ok=True)
        peer = profile["peer_mode"]
        points = []
        for n in records:
            run_out = os.path.join(profile_out, f"r{n}")
            env = os.environ.copy()
            env["OPTBINLOG_BENCH_OUT_DIR"] = run_out
            env["OPTBINLOG_BENCH_BIN"] = args.bench_bin
            env["OPTBINLOG_EVENTLOG_DIR"] = os.path.join(ROOT, profile["eventlog_dir"])
            env["OPTBINLOG_BENCH_MODES"] = ",".join(MAIN_MODES + [peer])
            env["OPTBINLOG_BENCH_BASELINE"] = "text_semantic_like"
            env["OPTBINLOG_BENCH_RECORDS"] = str(n)
            env["OPTBINLOG_BENCH_REPEATS"] = str(args.repeats)
            env["OPTBINLOG_BENCH_WARMUP"] = str(args.warmup)
            run_cmd(["python3", RUN_BENCH], cwd=ROOT, env=env)
            bench = load_json(os.path.join(run_out, "bench_result.json"))
            summ = bench["summary"]
            text_b = float(summ["text_semantic_like"]["total_bytes"]["mean"])
            bin_b = float(summ["binary"]["total_bytes"]["mean"])
            peer_b = float(summ[peer]["total_bytes"]["mean"])
            points.append(
                {
                    "records": n,
                    "text_bytes": text_b,
                    "binary_bytes": bin_b,
                    "peer_bytes": peer_b,
                    "binary_minus_peer": bin_b - peer_b,
                }
            )

        points.sort(key=lambda x: x["records"])
        crossover = estimate_crossover(
            [int(p["records"]) for p in points],
            [float(p["binary_bytes"]) for p in points],
            [float(p["peer_bytes"]) for p in points],
        )
        rows.append(
            {
                "profile": profile["name"],
                "peer_mode": peer,
                "points": points,
                "crossover": crossover,
            }
        )

    merged = {
        "generated_at": ts,
        "records": records,
        "repeats": int(args.repeats),
        "warmup": int(args.warmup),
        "bench_bin": args.bench_bin,
        "rows": rows,
    }
    json_path = os.path.join(out_root, "space_crossover_scan.json")
    save_json(json_path, merged)

    trend_svg = os.path.join(out_root, "space_records_scan.svg")
    delta_svg = os.path.join(out_root, "space_binary_minus_peer_scan.svg")
    build_space_scan_svg(rows, trend_svg)
    build_delta_svg(rows, delta_svg)

    report_md = os.path.join(out_root, "space_crossover_report.md")
    build_report(rows, records, report_md)

    print("saved", json_path)
    print("saved", trend_svg)
    print("saved", delta_svg)
    print("saved", report_md)


if __name__ == "__main__":
    main()
