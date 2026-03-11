#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import subprocess
from typing import Dict, List


ROOT = os.path.dirname(__file__)
RUN_BENCH = os.path.join(ROOT, "run_bench.py")


PROFILES = [
    {"name": "nanolog", "eventlog_dir": "eventlogst_semantic_nanolog", "peer_mode": "nanolog_like"},
    {"name": "zephyr", "eventlog_dir": "eventlogst_semantic_zephyr", "peer_mode": "zephyr_like"},
    {"name": "ulog", "eventlog_dir": "eventlogst_semantic_ulog", "peer_mode": "ulog_async_like"},
    {"name": "hilog", "eventlog_dir": "eventlogst_semantic_hilog", "peer_mode": "hilog_lite_like"},
    {"name": "ftrace", "eventlog_dir": "eventlogst_semantic_ftrace", "peer_mode": "ftrace"},
    {"name": "mixed", "eventlog_dir": "eventlogst_semantic_mixed", "peer_mode": ""},
]


def run_cmd(cmd: List[str], env: Dict[str, str]) -> None:
    proc = subprocess.run(cmd, cwd=ROOT, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed\ncmd: {}\nstdout:\n{}\nstderr:\n{}".format(" ".join(cmd), proc.stdout, proc.stderr)
        )


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def pct_delta(old: float, new: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / old * 100.0


def make_svg(rows: List[dict], out_svg: str) -> None:
    width = 1460
    height = 640
    margin = 90
    panel_gap = 40
    panel_w = (width - margin * 2 - panel_gap) / 2.0
    panel_h = 380
    panel_y = 150
    left_x = margin
    right_x = margin + panel_w + panel_gap

    def value_range(key: str):
        vals = [float(r[key]) for r in rows]
        lo = min(vals + [0.0])
        hi = max(vals + [0.0])
        if hi <= lo:
            hi = lo + 1.0
        pad = (hi - lo) * 0.1
        return lo - pad, hi + pad

    def color(v: float) -> str:
        if v >= 0:
            return "#b2182b"
        return "#1b7837"

    def draw_panel(lines: List[str], x0: float, key: str, title: str):
        lo, hi = value_range(key)
        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#fff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w / 2}" y="{panel_y - 14}" text-anchor="middle" font-family="Arial" font-size="15">{title}</text>')
        zero_y = panel_y + panel_h - ((0.0 - lo) / (hi - lo)) * panel_h
        lines.append(f'<line x1="{x0}" y1="{zero_y}" x2="{x0 + panel_w}" y2="{zero_y}" stroke="#777" stroke-dasharray="4,3"/>')

        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            v = lo + (hi - lo) * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f2f2f2"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{v:.1f}%</text>')

        col_w = panel_w / len(rows)
        bar_w = min(72, col_w * 0.66)
        for i, r in enumerate(rows):
            v = float(r[key])
            x = x0 + i * col_w + (col_w - bar_w) / 2.0
            y = panel_y + panel_h - ((v - lo) / (hi - lo)) * panel_h
            h = abs(zero_y - y)
            y_bar = min(y, zero_y)
            lines.append(f'<rect x="{x}" y="{y_bar}" width="{bar_w}" height="{max(1.0, h)}" fill="{color(v)}"/>')
            lines.append(f'<text x="{x + bar_w/2}" y="{y_bar - 7}" text-anchor="middle" font-family="Arial" font-size="10">{v:+.1f}%</text>')
            lines.append(f'<text x="{x + bar_w/2}" y="{panel_y + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="11">{r["profile"]}</text>')

    lines: List[str] = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="20">Binary/Text semantic alignment profile impact</text>')
    lines.append('<text x="50%" y="58" text-anchor="middle" font-family="Arial" font-size="12">delta = aligned(profile) vs unaligned(eventlogst); green means aligned reduced, red means increased</text>')

    draw_panel(lines, left_x, "binary_time_delta_pct", "binary time delta %")
    draw_panel(lines, right_x, "binary_bytes_delta_pct", "binary size delta %")

    legend_y = height - 34
    lines.append(f'<rect x="{margin}" y="{legend_y - 10}" width="14" height="10" fill="#1b7837"/>')
    lines.append(f'<text x="{margin + 20}" y="{legend_y - 1}" font-family="Arial" font-size="12">negative delta (aligned smaller/faster)</text>')
    lines.append(f'<rect x="{margin + 245}" y="{legend_y - 10}" width="14" height="10" fill="#b2182b"/>')
    lines.append(f'<text x="{margin + 265}" y="{legend_y - 1}" font-family="Arial" font-size="12">positive delta (aligned larger/slower)</text>')
    lines.append("</svg>")

    with open(out_svg, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run semantic aligned-vs-unaligned benchmark by profile (text/binary centered).")
    p.add_argument("--out-dir", default="", help="Output root dir")
    p.add_argument("--records", type=int, default=60000)
    p.add_argument("--repeats", type=int, default=4)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--bin", default="./optbinlog_bench_macos")
    p.add_argument("--include-ftrace", action="store_true", help="Try ftrace profile on local platform")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = args.out_dir.strip() or os.path.join(ROOT, "results", f"semantic_profile_bench_{ts}")
    os.makedirs(out_root, exist_ok=True)

    rows: List[dict] = []
    for pf in PROFILES:
        if pf["peer_mode"] == "ftrace" and not args.include_ftrace:
            continue

        profile_name = pf["name"]
        peer_mode = pf["peer_mode"].strip()
        modes = ["text", "binary"]
        if peer_mode:
            modes.append(peer_mode)
        modes_str = ",".join(modes)

        unaligned_out = os.path.join(out_root, profile_name, "unaligned")
        aligned_out = os.path.join(out_root, profile_name, "aligned")
        os.makedirs(unaligned_out, exist_ok=True)
        os.makedirs(aligned_out, exist_ok=True)

        env_u = os.environ.copy()
        env_u["OPTBINLOG_BENCH_OUT_DIR"] = unaligned_out
        env_u["OPTBINLOG_BENCH_BIN"] = args.bin
        env_u["OPTBINLOG_EVENTLOG_DIR"] = os.path.join(ROOT, "eventlogst")
        env_u["OPTBINLOG_BENCH_MODES"] = modes_str
        env_u["OPTBINLOG_BENCH_BASELINE"] = "text"
        env_u["OPTBINLOG_BENCH_RECORDS"] = str(args.records)
        env_u["OPTBINLOG_BENCH_REPEATS"] = str(args.repeats)
        env_u["OPTBINLOG_BENCH_WARMUP"] = str(args.warmup)
        run_cmd(["python3", RUN_BENCH], env_u)

        env_a = os.environ.copy()
        env_a["OPTBINLOG_BENCH_OUT_DIR"] = aligned_out
        env_a["OPTBINLOG_BENCH_BIN"] = args.bin
        env_a["OPTBINLOG_EVENTLOG_DIR"] = os.path.join(ROOT, pf["eventlog_dir"])
        env_a["OPTBINLOG_TEXT_PROFILE"] = "semantic"
        env_a["OPTBINLOG_BENCH_MODES"] = modes_str
        env_a["OPTBINLOG_BENCH_BASELINE"] = "text"
        env_a["OPTBINLOG_BENCH_RECORDS"] = str(args.records)
        env_a["OPTBINLOG_BENCH_REPEATS"] = str(args.repeats)
        env_a["OPTBINLOG_BENCH_WARMUP"] = str(args.warmup)
        run_cmd(["python3", RUN_BENCH], env_a)

        ju = load_json(os.path.join(unaligned_out, "bench_result.json"))
        ja = load_json(os.path.join(aligned_out, "bench_result.json"))
        su = ju["summary"]
        sa = ja["summary"]

        row = {
            "profile": profile_name,
            "unaligned_eventlog_dir": "eventlogst",
            "aligned_eventlog_dir": pf["eventlog_dir"],
            "modes": modes,
            "binary_time_ms_unaligned": float(su["binary"]["end_to_end_ms"]["mean"]),
            "binary_time_ms_aligned": float(sa["binary"]["end_to_end_ms"]["mean"]),
            "binary_bytes_unaligned": float(su["binary"]["total_bytes"]["mean"]),
            "binary_bytes_aligned": float(sa["binary"]["total_bytes"]["mean"]),
            "text_time_ms_unaligned": float(su["text"]["end_to_end_ms"]["mean"]),
            "text_time_ms_aligned": float(sa["text"]["end_to_end_ms"]["mean"]),
            "text_bytes_unaligned": float(su["text"]["total_bytes"]["mean"]),
            "text_bytes_aligned": float(sa["text"]["total_bytes"]["mean"]),
        }
        row["binary_time_delta_pct"] = pct_delta(row["binary_time_ms_unaligned"], row["binary_time_ms_aligned"])
        row["binary_bytes_delta_pct"] = pct_delta(row["binary_bytes_unaligned"], row["binary_bytes_aligned"])
        row["text_time_delta_pct"] = pct_delta(row["text_time_ms_unaligned"], row["text_time_ms_aligned"])
        row["text_bytes_delta_pct"] = pct_delta(row["text_bytes_unaligned"], row["text_bytes_aligned"])
        rows.append(row)

    rows.sort(key=lambda x: x["profile"])
    out_json = os.path.join(out_root, "semantic_profile_summary.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"generated_at": ts, "rows": rows}, f, indent=2, ensure_ascii=False)

    out_svg = os.path.join(out_root, "semantic_profile_overview.svg")
    if rows:
        make_svg(rows, out_svg)

    latest = os.path.join(ROOT, "results", "semantic_profile_bench_latest")
    if os.path.islink(latest) or os.path.exists(latest):
        if os.path.islink(latest):
            os.unlink(latest)
        elif os.path.isdir(latest):
            import shutil
            shutil.rmtree(latest)
        else:
            os.remove(latest)
    os.symlink(out_root, latest)

    print("saved", out_json)
    if os.path.exists(out_svg):
        print("saved", out_svg)
    print("saved", latest)


if __name__ == "__main__":
    main()
