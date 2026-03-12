#!/usr/bin/env python3
import argparse
import os

import run_final_aligned_suite as suite


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge externally collected L1 results into final aligned summary.")
    p.add_argument("--summary-json", required=True)
    p.add_argument("--l1-root", required=True)
    return p.parse_args()


def extract_from_dir(profile: str, peer_mode: str, eventlog_dir: str, profile_dir: str) -> dict:
    modes = suite.MAIN_MODES + [peer_mode]
    node_dirs = []
    if os.path.isdir(os.path.join(profile_dir, "nodes")):
        for name in sorted(os.listdir(os.path.join(profile_dir, "nodes"))):
            node_path = os.path.join(profile_dir, "nodes", name, "bench_out", "bench_result.json")
            if os.path.exists(node_path):
                node_dirs.append((name, node_path))

    if not node_dirs:
        raise RuntimeError(f"no bench_result.json under {profile_dir}")

    by_mode = {}
    for mode in modes:
        tvals = []
        svals = []
        thvals = []
        for _, path in node_dirs:
            data = suite.load_json(path)
            summ = data.get("summary", {}).get(mode, {})
            if not summ:
                continue
            tvals.append(float(summ.get("end_to_end_ms", {}).get("mean", 0.0)))
            svals.append(float(summ.get("total_bytes", {}).get("mean", 0.0)))
            thvals.append(float(summ.get("throughput_e2e_rps", {}).get("mean", 0.0)))
        by_mode[mode] = {
            "time_ms": suite.metric_stats(suite.iqr_filter_values(tvals)),
            "bytes": suite.metric_stats(suite.iqr_filter_values(svals)),
            "throughput_rps": suite.metric_stats(suite.iqr_filter_values(thvals)),
        }

    return {
        "profile": profile,
        "peer_mode": peer_mode,
        "eventlog_dir": eventlog_dir,
        "nodes_ok": len(node_dirs),
        "nodes_used": len(node_dirs),
        "nodes_total": 10,
        "modes": by_mode,
        "source_dir": profile_dir,
    }


def main() -> None:
    args = parse_args()
    summary = suite.load_json(args.summary_json)
    rows = summary.get("rows", [])
    if not rows:
        raise SystemExit("summary rows empty")
    profile_eventlog = {p["name"]: p["eventlog_dir"] for p in suite.PROFILES}

    for row in rows:
        profile = row["profile"]
        profile_dir = os.path.join(args.l1_root, profile)
        row["l1"] = extract_from_dir(profile, row["peer_mode"], profile_eventlog.get(profile, ""), profile_dir)
        suite.save_json(os.path.join(profile_dir, "l1_extracted.json"), row["l1"])

    suite.save_json(args.summary_json, summary)

    merged_root = os.path.dirname(args.summary_json)
    suite.build_single_overview_svg(rows, os.path.join(merged_root, "single_aligned_overview.svg"))
    suite.build_direct_delta_svg(rows, "single", os.path.join(merged_root, "single_binary_vs_peer.svg"), "Strict Aligned Single: Final Binary vs Peer")
    suite.build_multi_svg(rows, os.path.join(merged_root, "multi_time_scan.svg"), "elapsed_ms", "Strict Aligned Multi-Device Time Scan", "ms")
    suite.build_multi_svg(rows, os.path.join(merged_root, "multi_throughput_scan.svg"), "throughput_rps", "Strict Aligned Multi-Device Throughput Scan", "records/s")
    suite.build_multi_svg(rows, os.path.join(merged_root, "multi_space_scan.svg"), "total_bytes", "Strict Aligned Multi-Device Space Scan", "bytes")
    suite.build_l1_overview_svg([row["l1"] for row in rows], os.path.join(merged_root, "l1_aligned_overview.svg"))
    suite.build_direct_delta_svg(rows, "l1", os.path.join(merged_root, "l1_binary_vs_peer.svg"), "Strict Aligned Real-Device: Final Binary vs Peer")
    suite.build_report(rows, os.path.join(merged_root, "final_aligned_report.md"), include_l1=True)

    print("saved", args.summary_json)
    print("saved", os.path.join(merged_root, "l1_aligned_overview.svg"))
    print("saved", os.path.join(merged_root, "l1_binary_vs_peer.svg"))
    print("saved", os.path.join(merged_root, "final_aligned_report.md"))


if __name__ == "__main__":
    main()
