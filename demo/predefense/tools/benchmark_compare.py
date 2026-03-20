#!/usr/bin/env python3
import argparse
import csv
import json
import os
import statistics
import subprocess
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run repeated text/binary benchmark and summarize median metrics")
    p.add_argument("--bench-bin", required=True, help="path to optbinlog_bench binary")
    p.add_argument("--eventlog-dir", required=True, help="eventlog schema dir")
    p.add_argument("--out-dir", required=True, help="output directory")
    p.add_argument("--records", type=int, default=80000, help="records per run")
    p.add_argument("--runs", type=int, default=3, help="repeat runs and report median")
    p.add_argument("--shared-path", default="", help="shared path for binary mode")
    return p.parse_args()


def parse_line(line: str) -> Dict[str, float]:
    parts = [p.strip() for p in line.strip().split(",") if p.strip() != ""]
    out: Dict[str, float] = {}
    for i in range(0, len(parts) - 1, 2):
        k = parts[i]
        v = parts[i + 1]
        try:
            out[k] = float(v)
        except ValueError:
            out[k] = float("nan")
    return out


def run_once(cmd: List[str]) -> str:
    cp = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if cp.returncode != 0:
        raise RuntimeError(f"command failed ({cp.returncode}): {' '.join(cmd)}\n{cp.stderr}")
    out = cp.stdout.strip().splitlines()
    if not out:
        raise RuntimeError(f"no output from {' '.join(cmd)}")
    return out[-1].strip()


def ascii_compare(text_val: float, bin_val: float, width: int = 36) -> str:
    m = max(text_val, bin_val, 1e-9)
    t = int(round((text_val / m) * width))
    b = int(round((bin_val / m) * width))
    return "\n".join(
        [
            f"text   [{'#' * t}{'-' * (width - t)}] {text_val:.3f}",
            f"binary [{'#' * b}{'-' * (width - b)}] {bin_val:.3f}",
        ]
    )


def main() -> int:
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    raw_csv = os.path.join(args.out_dir, "benchmark_raw.csv")
    summary_json = os.path.join(args.out_dir, "benchmark_summary.json")
    summary_md = os.path.join(args.out_dir, "benchmark_summary.md")
    chart_txt = os.path.join(args.out_dir, "benchmark_chart.txt")
    text_out = os.path.join(args.out_dir, "text_semantic.log")
    bin_out = os.path.join(args.out_dir, "optbinlog_binary.bin")
    shared_path = args.shared_path or os.path.join(args.out_dir, "shared_eventtag.bin")

    rows: List[Dict[str, float]] = []
    text_write: List[float] = []
    text_bytes: List[float] = []
    text_peak: List[float] = []
    bin_write: List[float] = []
    bin_bytes: List[float] = []
    bin_peak: List[float] = []

    for i in range(args.runs):
        text_line = run_once(
            [
                args.bench_bin,
                "--mode",
                "text_semantic_like",
                "--eventlog-dir",
                args.eventlog_dir,
                "--out",
                text_out,
                "--records",
                str(args.records),
            ]
        )
        text_m = parse_line(text_line)
        text_write.append(text_m.get("write_only_ms", float("nan")))
        text_bytes.append(text_m.get("total_bytes", float("nan")))
        text_peak.append(text_m.get("peak_kb", float("nan")))
        rows.append(
            {
                "mode": "text_semantic_like",
                "run_idx": i + 1,
                "write_only_ms": text_m.get("write_only_ms", float("nan")),
                "total_bytes": text_m.get("total_bytes", float("nan")),
                "peak_kb": text_m.get("peak_kb", float("nan")),
            }
        )

        bin_line = run_once(
            [
                args.bench_bin,
                "--mode",
                "binary",
                "--eventlog-dir",
                args.eventlog_dir,
                "--shared",
                shared_path,
                "--out",
                bin_out,
                "--records",
                str(args.records),
            ]
        )
        bin_m = parse_line(bin_line)
        bin_write.append(bin_m.get("write_only_ms", float("nan")))
        bin_bytes.append(bin_m.get("total_bytes", float("nan")))
        bin_peak.append(bin_m.get("peak_kb", float("nan")))
        rows.append(
            {
                "mode": "binary",
                "run_idx": i + 1,
                "write_only_ms": bin_m.get("write_only_ms", float("nan")),
                "total_bytes": bin_m.get("total_bytes", float("nan")),
                "peak_kb": bin_m.get("peak_kb", float("nan")),
            }
        )

    with open(raw_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["mode", "run_idx", "write_only_ms", "total_bytes", "peak_kb"])
        w.writeheader()
        for row in rows:
            w.writerow(row)

    med_text_write = statistics.median(text_write)
    med_text_bytes = statistics.median(text_bytes)
    med_text_peak = statistics.median(text_peak)
    med_bin_write = statistics.median(bin_write)
    med_bin_bytes = statistics.median(bin_bytes)
    med_bin_peak = statistics.median(bin_peak)

    summary = {
        "runs": args.runs,
        "records": args.records,
        "text": {
            "write_only_ms_median": med_text_write,
            "total_bytes_median": med_text_bytes,
            "peak_kb_median": med_text_peak,
        },
        "binary": {
            "write_only_ms_median": med_bin_write,
            "total_bytes_median": med_bin_bytes,
            "peak_kb_median": med_bin_peak,
        },
        "ratios": {
            "space_text_div_binary": (med_text_bytes / med_bin_bytes) if med_bin_bytes > 0 else float("inf"),
            "write_time_text_div_binary": (med_text_write / med_bin_write) if med_bin_write > 0 else float("inf"),
        },
    }

    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    with open(summary_md, "w", encoding="utf-8") as f:
        f.write("# Benchmark Summary (Median)\n\n")
        f.write(f"- runs: `{args.runs}`\n")
        f.write(f"- records/run: `{args.records}`\n\n")
        f.write("| metric | text_semantic_like | binary |\n")
        f.write("|---|---:|---:|\n")
        f.write(f"| write_only_ms | {med_text_write:.3f} | {med_bin_write:.3f} |\n")
        f.write(f"| total_bytes | {med_text_bytes:.0f} | {med_bin_bytes:.0f} |\n")
        f.write(f"| peak_kb | {med_text_peak:.0f} | {med_bin_peak:.0f} |\n\n")
        f.write(f"- space ratio (text/binary): `{summary['ratios']['space_text_div_binary']:.2f}x`\n")
        f.write(f"- write ratio (text/binary): `{summary['ratios']['write_time_text_div_binary']:.2f}x`\n")

    with open(chart_txt, "w", encoding="utf-8") as f:
        f.write("WRITE TIME (ms)\n")
        f.write(ascii_compare(med_text_write, med_bin_write) + "\n\n")
        f.write("TOTAL BYTES\n")
        f.write(ascii_compare(med_text_bytes, med_bin_bytes) + "\n")

    print(f"summary_json={summary_json}")
    print(f"summary_md={summary_md}")
    print(f"chart_txt={chart_txt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
