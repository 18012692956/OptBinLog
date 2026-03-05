#!/usr/bin/env python3
import argparse
import json
import os
from typing import Dict, List, Tuple


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def mean(vals: List[float]) -> float:
    if not vals:
        return 0.0
    return sum(vals) / float(len(vals))


def improve_pct(old: float, new: float, higher_better: bool) -> float:
    if old == 0:
        return 0.0
    if higher_better:
        return (new - old) / old * 100.0
    return (old - new) / old * 100.0


def mode_display(mode: str) -> str:
    return mode.replace("@linux", "")


def collect_single(root: str) -> Dict[str, dict]:
    u = load_json(os.path.join(root, "single_unaligned", "bench_result_merged.json"))
    a = load_json(os.path.join(root, "single_aligned", "bench_result_merged.json"))

    records_u = int(u["config"]["records"])
    records_a = int(a["config"]["records"])
    out: Dict[str, dict] = {}

    groups = {
        "single_local": lambda m: "@linux" not in m and m != "ftrace",
        "single_linux": lambda m: "@linux" in m or m == "ftrace",
    }

    for group_name, pred in groups.items():
        modes_u = [m for m in u["summary"].keys() if pred(m)]
        rows = {}
        for m in modes_u:
            m2 = mode_display(m)
            a_mode = m
            if m2 in a["summary"] and m not in a["summary"]:
                a_mode = m2
            if a_mode not in a["summary"]:
                continue
            su = u["summary"][m]
            sa = a["summary"][a_mode]
            bu = float(su["total_bytes"]["mean"])
            ba = float(sa["total_bytes"]["mean"])
            tu = float(su["end_to_end_ms"]["mean"])
            ta = float(sa["end_to_end_ms"]["mean"])
            thu = float(su["throughput_e2e_rps"]["mean"])
            tha = float(sa["throughput_e2e_rps"]["mean"])
            bpr_u = bu / records_u if records_u else 0.0
            bpr_a = ba / records_a if records_a else 0.0
            rows[m2] = {
                "time_ms_unaligned": tu,
                "time_ms_aligned": ta,
                "throughput_unaligned": thu,
                "throughput_aligned": tha,
                "bytes_unaligned": bu,
                "bytes_aligned": ba,
                "bytes_per_record_unaligned": bpr_u,
                "bytes_per_record_aligned": bpr_a,
                "semantic_bpr_inflation_ratio": (bpr_u / bpr_a) if bpr_a > 0 else 0.0,
                "time_improve_pct": improve_pct(tu, ta, higher_better=False),
                "throughput_gain_pct": improve_pct(thu, tha, higher_better=True),
                "bytes_reduce_pct": improve_pct(bu, ba, higher_better=False),
            }
        out[group_name] = rows
    return out


def collect_multi_source(result_json: str) -> Tuple[Dict[str, dict], float]:
    d = load_json(result_json)
    scenarios = d.get("scenarios", [])
    by_mode: Dict[str, dict] = {}
    total_records = 0.0
    total_bytes = 0.0
    rec_acc = 0.0

    for sc in scenarios:
        devices = int(sc["devices"])
        rpd = int(sc["records_per_device"])
        records = float(devices * rpd)
        summary = sc["summary"]
        for mode, row in summary.items():
            if mode == "comparison":
                continue
            by_mode.setdefault(
                mode,
                {
                    "time_ms": [],
                    "throughput_rps": [],
                    "bytes": [],
                    "bytes_per_record_weighted_sum": 0.0,
                    "records_weight": 0.0,
                },
            )
            t = float(row["elapsed_ms"]["mean"])
            thr = float(row["throughput_rps"]["mean"])
            b = float(row["total_bytes"]["mean"])
            by_mode[mode]["time_ms"].append(t)
            by_mode[mode]["throughput_rps"].append(thr)
            by_mode[mode]["bytes"].append(b)
            by_mode[mode]["bytes_per_record_weighted_sum"] += (b / records) * records
            by_mode[mode]["records_weight"] += records

    out: Dict[str, dict] = {}
    for mode, acc in by_mode.items():
        rw = float(acc["records_weight"])
        out[mode] = {
            "time_ms": mean(acc["time_ms"]),
            "throughput_rps": mean(acc["throughput_rps"]),
            "bytes": mean(acc["bytes"]),
            "bytes_per_record": (acc["bytes_per_record_weighted_sum"] / rw) if rw > 0 else 0.0,
        }
        total_records += rw
        total_bytes += out[mode]["bytes"] * rw
        rec_acc += rw
    global_bpr = (total_bytes / rec_acc) if rec_acc > 0 else 0.0
    return out, global_bpr


def collect_multi(root: str) -> Dict[str, dict]:
    um = load_json(os.path.join(root, "multi_unaligned_fast", "bench_multi_merged.json"))
    am = load_json(os.path.join(root, "multi_aligned_fast", "bench_multi_merged.json"))

    u_local_path = um["sources"]["local_result"]
    u_linux_path = um["sources"]["linux_result"]
    a_local_path = am["sources"]["local_result"]
    a_linux_path = am["sources"]["linux_result"]

    u_local, _ = collect_multi_source(u_local_path)
    a_local, _ = collect_multi_source(a_local_path)
    u_linux, _ = collect_multi_source(u_linux_path)
    a_linux, _ = collect_multi_source(a_linux_path)

    out: Dict[str, dict] = {}
    for group, u_src, a_src in [
        ("multi_local", u_local, a_local),
        ("multi_linux", u_linux, a_linux),
    ]:
        rows = {}
        for mode, urow in u_src.items():
            if mode not in a_src:
                continue
            arow = a_src[mode]
            rows[mode] = {
                "time_ms_unaligned": urow["time_ms"],
                "time_ms_aligned": arow["time_ms"],
                "throughput_unaligned": urow["throughput_rps"],
                "throughput_aligned": arow["throughput_rps"],
                "bytes_unaligned": urow["bytes"],
                "bytes_aligned": arow["bytes"],
                "bytes_per_record_unaligned": urow["bytes_per_record"],
                "bytes_per_record_aligned": arow["bytes_per_record"],
                "semantic_bpr_inflation_ratio": (urow["bytes_per_record"] / arow["bytes_per_record"]) if arow["bytes_per_record"] > 0 else 0.0,
                "time_improve_pct": improve_pct(urow["time_ms"], arow["time_ms"], higher_better=False),
                "throughput_gain_pct": improve_pct(urow["throughput_rps"], arow["throughput_rps"], higher_better=True),
                "bytes_reduce_pct": improve_pct(urow["bytes"], arow["bytes"], higher_better=False),
            }
        out[group] = rows
    return out


def collect_l1(root: str) -> Dict[str, dict]:
    c = load_json(os.path.join(root, "l1_10_semantic_compare.json"))
    ga = c["groups"]["unaligned"]["by_mode"]
    gb = c["groups"]["aligned"]["by_mode"]
    out = {}
    for mode, ua in ga.items():
        if mode not in gb:
            continue
        ab = gb[mode]
        tu = float(ua["time_ms"]["mean"])
        ta = float(ab["time_ms"]["mean"])
        thu = float(ua["throughput_rps"]["mean"])
        tha = float(ab["throughput_rps"]["mean"])
        bu = float(ua["bytes"]["mean"])
        ba = float(ab["bytes"]["mean"])
        # L1 config uses 80000 records per node run.
        bpr_u = bu / 80000.0
        bpr_a = ba / 80000.0
        out[mode] = {
            "time_ms_unaligned": tu,
            "time_ms_aligned": ta,
            "throughput_unaligned": thu,
            "throughput_aligned": tha,
            "bytes_unaligned": bu,
            "bytes_aligned": ba,
            "bytes_per_record_unaligned": bpr_u,
            "bytes_per_record_aligned": bpr_a,
            "semantic_bpr_inflation_ratio": (bpr_u / bpr_a) if bpr_a > 0 else 0.0,
            "time_improve_pct": improve_pct(tu, ta, higher_better=False),
            "throughput_gain_pct": improve_pct(thu, tha, higher_better=True),
            "bytes_reduce_pct": improve_pct(bu, ba, higher_better=False),
        }
    return {"l1_linux_10": out}


def build_svg(groups: Dict[str, Dict[str, dict]], out_path: str) -> None:
    names = list(groups.keys())
    modes = sorted({m for g in groups.values() for m in g.keys()})
    if not names or not modes:
        return

    metrics = [
        ("time_improve_pct", "Time improve % (aligned vs unaligned)"),
        ("bytes_reduce_pct", "Bytes reduce % (semantic gap)"),
        ("throughput_gain_pct", "Throughput gain %"),
    ]

    cell_w = 95
    cell_h = 28
    left = 220
    top = 90
    gap_group = 18
    group_w = len(metrics) * cell_w + gap_group
    width = left + len(names) * group_w + 60
    height = top + len(modes) * cell_h + 100

    def color(v: float) -> str:
        # green for positive, red for negative
        v = max(-60.0, min(60.0, v))
        if v >= 0:
            g = int(120 + (v / 60.0) * 115)
            r = int(240 - (v / 60.0) * 180)
            b = int(120 - (v / 60.0) * 70)
        else:
            a = abs(v)
            r = int(120 + (a / 60.0) * 125)
            g = int(235 - (a / 60.0) * 170)
            b = int(130 - (a / 60.0) * 80)
        return f"rgb({r},{g},{b})"

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="20">Semantic Alignment Gap Overview</text>')
    lines.append('<text x="50%" y="56" text-anchor="middle" font-family="Arial" font-size="12">positive value means aligned version is better; values are percentage change</text>')

    for r, mode in enumerate(modes):
        y = top + r * cell_h
        lines.append(f'<text x="{left - 10}" y="{y + 19}" text-anchor="end" font-family="Arial" font-size="12">{mode}</text>')

    for gi, gname in enumerate(names):
        gx = left + gi * group_w
        lines.append(f'<text x="{gx + (len(metrics) * cell_w) / 2}" y="{top - 18}" text-anchor="middle" font-family="Arial" font-size="13">{gname}</text>')
        for mi, (mk, mt) in enumerate(metrics):
            x = gx + mi * cell_w
            lines.append(f'<text x="{x + cell_w/2}" y="{top - 2}" text-anchor="middle" font-family="Arial" font-size="10">{mk}</text>')
            for r, mode in enumerate(modes):
                y = top + r * cell_h
                row = groups[gname].get(mode)
                v = float(row.get(mk, 0.0)) if row else 0.0
                fill = color(v) if row else "#f4f4f4"
                lines.append(f'<rect x="{x}" y="{y}" width="{cell_w-2}" height="{cell_h-2}" fill="{fill}" stroke="#ffffff"/>')
                if row:
                    lines.append(f'<text x="{x + (cell_w-2)/2}" y="{y + 18}" text-anchor="middle" font-family="Arial" font-size="10">{v:.1f}</text>')

    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def write_report(groups: Dict[str, Dict[str, dict]], out_path: str) -> None:
    lines = []
    lines.append("# 语义未对齐 vs 对齐 差距汇总")
    lines.append("")
    lines.append("定义：")
    lines.append("- `bytes_per_record = total_bytes / records`")
    lines.append("- `semantic_bpr_inflation_ratio = bytes_per_record_unaligned / bytes_per_record_aligned`")
    lines.append("- `time_improve_pct = (T_unaligned - T_aligned) / T_unaligned * 100%`")
    lines.append("- `throughput_gain_pct = (Q_aligned - Q_unaligned) / Q_unaligned * 100%`")
    lines.append("")
    for gname, rows in groups.items():
        lines.append(f"## {gname}")
        lines.append("")
        lines.append("| mode | bpr_unaligned | bpr_aligned | inflation | time_improve% | bytes_reduce% | throughput_gain% |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for mode in sorted(rows.keys()):
            r = rows[mode]
            lines.append(
                "| {} | {:.3f} | {:.3f} | {:.3f}x | {:.2f} | {:.2f} | {:.2f} |".format(
                    mode,
                    r["bytes_per_record_unaligned"],
                    r["bytes_per_record_aligned"],
                    r["semantic_bpr_inflation_ratio"],
                    r["time_improve_pct"],
                    r["bytes_reduce_pct"],
                    r["throughput_gain_pct"],
                )
            )
        lines.append("")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze semantic misalignment gap across single/multi/L1 runs")
    p.add_argument("--root", required=True, help="results/full_matrix_xxx root")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    root = os.path.abspath(args.root)
    groups: Dict[str, Dict[str, dict]] = {}
    groups.update(collect_single(root))
    groups.update(collect_multi(root))
    groups.update(collect_l1(root))

    out_json = os.path.join(root, "semantic_gap_analysis.json")
    out_svg = os.path.join(root, "semantic_gap_overview.svg")
    out_md = os.path.join(root, "semantic_gap_report.md")

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"root": root, "groups": groups}, f, indent=2, ensure_ascii=False)
    build_svg(groups, out_svg)
    write_report(groups, out_md)

    print("saved", out_json)
    print("saved", out_svg)
    print("saved", out_md)


if __name__ == "__main__":
    main()
