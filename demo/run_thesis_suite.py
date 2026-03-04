#!/usr/bin/env python3
import datetime as dt
import json
import os
import shutil
import subprocess
import run_platform_suite


ROOT = os.path.dirname(__file__)
RESULTS_ROOT = os.path.join(ROOT, "results")


def run_cmd(cmd, env=None):
    proc = subprocess.run(cmd, cwd=ROOT, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed\ncmd: {}\nstdout:\n{}\nstderr:\n{}".format(
                " ".join(cmd), proc.stdout, proc.stderr
            )
        )
    return proc.stdout


def build_local_init_race():
    cmd = [
        "clang",
        "-O2",
        "-Wall",
        "-Wextra",
        "-std=c11",
        "-Iinclude",
        "-o",
        os.path.join(ROOT, "optbinlog_init_race"),
        "optbinlog_init_race.c",
        "src/optbinlog_shared.c",
        "src/optbinlog_eventlog.c",
    ]
    run_cmd(cmd)


def build_local_multi_binary():
    cmd = [
        "clang",
        "-O2",
        "-Wall",
        "-Wextra",
        "-std=c11",
        "-Iinclude",
        "-o",
        os.path.join(ROOT, "optbinlog_multi_bench_macos"),
        "optbinlog_multi_bench.c",
        "src/optbinlog_shared.c",
        "src/optbinlog_eventlog.c",
        "src/optbinlog_binlog.c",
    ]
    run_cmd(cmd)


def to_num(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def safe_copy(src, dst_dir):
    if not os.path.exists(src):
        return None
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, os.path.basename(src))
    shutil.copy2(src, dst)
    return dst


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def summarize_single(path):
    data = load_json(path)
    local = data["source_comparison"]["local"]
    linux = data["source_comparison"]["linux"]
    return {
        "local_baseline": local["baseline_mode"],
        "linux_baseline": linux["baseline_mode"],
        "local_by_mode": local["by_mode"],
        "linux_by_mode": linux["by_mode"],
    }


def summarize_multi(path):
    data = load_json(path)
    out = {}
    for src in data["sources_data"]:
        src_name = src["source"]
        out[src_name] = {
            "baseline": src["baseline_mode"],
            "scenarios": {},
        }
        for sc in src["scenarios"]:
            out[src_name]["scenarios"][sc["scenario"]] = sc["comparison"]["by_mode"]
    return out


def summarize_init(path):
    data = load_json(path)
    agg = data["aggregate"]
    return {
        "elapsed_mean_ms": agg["elapsed_ms"]["mean"],
        "elapsed_p95_ms": agg["elapsed_ms"]["p95"],
        "wait_events_mean": agg["wait_events"]["mean"],
        "wait_total_mean_ms": agg["wait_total_ms"]["mean"],
        "wait_p95_mean_ms": agg["wait_p95_ms"]["mean"],
        "create_success_mean": agg["create_success"]["mean"],
        "open_existing_ok_mean": agg["open_existing_ok"]["mean"],
        "init_done_mean": agg["init_done"]["mean"],
    }


def summarize_binary_contention(path):
    data = load_json(path)
    rows = []
    for sc in data.get("scenarios", []):
        b = sc.get("summary", {}).get("binary", {})
        rows.append(
            {
                "scenario": sc.get("scenario"),
                "devices": sc.get("devices"),
                "records_per_device": sc.get("records_per_device"),
                "elapsed_mean_ms": b.get("elapsed_ms", {}).get("mean"),
                "throughput_mean_rps": b.get("throughput_rps", {}).get("mean"),
                "total_bytes_mean": b.get("total_bytes", {}).get("mean"),
            }
        )
    rows.sort(key=lambda x: (int(x.get("devices") or 0), int(x.get("records_per_device") or 0)))
    return {
        "baseline": data.get("config", {}).get("baseline_mode"),
        "rows": rows,
    }


def collect_linux_platform_info(target_path):
    cmd = [
        "limactl",
        "shell",
        os.environ.get("OPTBINLOG_LINUX_INSTANCE", "thesis-linux"),
        "--",
        "bash",
        "-lc",
        "cd "
        + json.dumps(ROOT)
        + "; python3 -c "
        + json.dumps(
            "import json,run_platform_suite as r; "
            + f"print(json.dumps(r.collect_platform_info({json.dumps(target_path)}), ensure_ascii=False))"
        ),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    out = (proc.stdout or "").strip()
    if not out:
        return {
            "error": "empty output from linux platform collection",
            "stderr": proc.stderr,
            "returncode": proc.returncode,
        }
    line = out.splitlines()[-1]
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return {
            "error": "invalid json from linux platform collection",
            "stdout_tail": line,
            "stderr": proc.stderr,
            "returncode": proc.returncode,
        }


def write_summary(report_path, tag, cfg, single_sum, multi_sum, init_sum, bin_cont):
    lines = []
    lines.append(f"# Thesis Suite Report ({tag})")
    lines.append("")
    lines.append("## 配置")
    lines.append("")
    lines.append(f"- 单机高负载：records={cfg['single_records']}, repeats={cfg['single_repeats']}, warmup={cfg['single_warmup']}")
    lines.append(f"- 多设备：devices={cfg['multi_scan_devices']}, rpd={cfg['multi_scan_rpd']}, repeats={cfg['multi_repeats']}, warmup={cfg['multi_warmup']}")
    lines.append(f"- 竞争模拟：procs={cfg['init_procs']}, repeats={cfg['init_repeats']}, warmup={cfg['init_warmup']}")
    if "platform_meta" in cfg:
        pm = cfg["platform_meta"]
        lines.append(f"- 平台(local): arch={pm['local'].get('host', {}).get('arch')} kernel={pm['local'].get('host', {}).get('kernel_release')} fs={pm['local'].get('storage', {}).get('filesystem')} medium={pm['local'].get('storage', {}).get('medium')}")
        lines.append(f"- 平台(linux): arch={pm['linux'].get('host', {}).get('arch')} kernel={pm['linux'].get('host', {}).get('kernel_release')} fs={pm['linux'].get('storage', {}).get('filesystem')} medium={pm['linux'].get('storage', {}).get('medium')}")
    lines.append("")
    lines.append("## 单机高负载（双基线）")
    lines.append("")
    lines.append(f"- local baseline: `{single_sum['local_baseline']}`")
    for mode, c in single_sum["local_by_mode"].items():
        lines.append(
            f"  - local {mode}: e2e={c['end_to_end_improve_pct']:.2f}%, size={c['size_save_pct']:.2f}%, thr={c['throughput_e2e_gain_pct']:.2f}%"
        )
    lines.append(f"- linux baseline: `{single_sum['linux_baseline']}`")
    for mode, c in single_sum["linux_by_mode"].items():
        lines.append(
            f"  - linux {mode}: e2e={c['end_to_end_improve_pct']:.2f}%, size={c['size_save_pct']:.2f}%, thr={c['throughput_e2e_gain_pct']:.2f}%"
        )
    lines.append("")
    lines.append("## 多设备模拟（双平台）")
    lines.append("")
    for src_name, src in multi_sum.items():
        lines.append(f"- {src_name} baseline: `{src['baseline']}`")
        for sc_name, by_mode in sorted(src["scenarios"].items()):
            best_mode = None
            best_thr = None
            for mode, row in by_mode.items():
                v = to_num(row.get("throughput_gain_pct", 0.0))
                if best_thr is None or v > best_thr:
                    best_thr = v
                    best_mode = mode
            if best_mode is None:
                lines.append(f"  - {sc_name}: no comparison mode")
            else:
                lines.append(f"  - {sc_name}: best_throughput={best_mode} ({best_thr:.2f}%)")
    lines.append("")
    lines.append("## 多设备竞争模拟（仅本地）")
    lines.append("")
    lines.append(f"- elapsed_mean: {init_sum['elapsed_mean_ms']:.3f} ms")
    lines.append(f"- elapsed_p95: {init_sum['elapsed_p95_ms']:.3f} ms")
    lines.append(f"- wait_events_mean: {init_sum['wait_events_mean']:.3f}")
    lines.append(f"- wait_total_mean: {init_sum['wait_total_mean_ms']:.3f} ms")
    lines.append(f"- wait_p95_mean: {init_sum['wait_p95_mean_ms']:.3f} ms")
    lines.append(f"- create_success_mean: {init_sum['create_success_mean']:.3f}")
    lines.append(f"- open_existing_ok_mean: {init_sum['open_existing_ok_mean']:.3f}")
    lines.append(f"- init_done_mean: {init_sum['init_done_mean']:.3f}")
    lines.append("")
    lines.append("## Binary 多设备竞争（仅本地）")
    lines.append("")
    lines.append(f"- baseline: `{bin_cont['baseline']}`")
    for row in bin_cont["rows"]:
        lines.append(
            f"- {row['scenario']}: d={row['devices']}, rpd={row['records_per_device']}, elapsed_mean={to_num(row['elapsed_mean_ms']):.3f} ms, throughput={to_num(row['throughput_mean_rps']):.1f} rps, total_bytes={to_num(row['total_bytes_mean']):.1f}"
        )
    lines.append("")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = os.environ.get("OPTBINLOG_SUITE_TAG", f"thesis_suite_{ts}")
    out = os.path.join(RESULTS_ROOT, tag)
    os.makedirs(out, exist_ok=True)

    cfg = {
        "single_records": int(os.environ.get("OPTBINLOG_SUITE_SINGLE_RECORDS", "120000")),
        "single_repeats": int(os.environ.get("OPTBINLOG_SUITE_SINGLE_REPEATS", "12")),
        "single_warmup": int(os.environ.get("OPTBINLOG_SUITE_SINGLE_WARMUP", "2")),
        "multi_scan_devices": os.environ.get("OPTBINLOG_SUITE_MULTI_SCAN_DEVICES", "1,2,4,8,12,16"),
        "multi_scan_rpd": os.environ.get("OPTBINLOG_SUITE_MULTI_SCAN_RPD", "1000,2000"),
        "multi_repeats": int(os.environ.get("OPTBINLOG_SUITE_MULTI_REPEATS", "8")),
        "multi_warmup": int(os.environ.get("OPTBINLOG_SUITE_MULTI_WARMUP", "2")),
        "binary_scan_devices": os.environ.get("OPTBINLOG_SUITE_BINARY_SCAN_DEVICES", "1,2,4,8,12,16,24"),
        "binary_scan_rpd": os.environ.get("OPTBINLOG_SUITE_BINARY_SCAN_RPD", "2000"),
        "binary_repeats": int(os.environ.get("OPTBINLOG_SUITE_BINARY_REPEATS", "10")),
        "binary_warmup": int(os.environ.get("OPTBINLOG_SUITE_BINARY_WARMUP", "2")),
        "init_procs": int(os.environ.get("OPTBINLOG_SUITE_INIT_PROCS", "32")),
        "init_repeats": int(os.environ.get("OPTBINLOG_SUITE_INIT_REPEATS", "20")),
        "init_warmup": int(os.environ.get("OPTBINLOG_SUITE_INIT_WARMUP", "3")),
    }
    linux_workdir = os.environ.get("OPTBINLOG_HYBRID_LINUX_WORKDIR", ROOT)

    # 1) Single high-load dual-platform
    single_out = os.path.join(out, "single_highload")
    env_single = os.environ.copy()
    env_single.update(
        {
            "OPTBINLOG_HYBRID_OUT_DIR": single_out,
            "OPTBINLOG_HYBRID_LOCAL_MODES": "text,binary,syslog",
            "OPTBINLOG_HYBRID_LINUX_MODES": "text,binary,ftrace",
            "OPTBINLOG_BENCH_BASELINE": "text",
            "OPTBINLOG_HYBRID_LINUX_BASELINE": "binary",
            "OPTBINLOG_BENCH_RECORDS": str(cfg["single_records"]),
            "OPTBINLOG_BENCH_REPEATS": str(cfg["single_repeats"]),
            "OPTBINLOG_BENCH_WARMUP": str(cfg["single_warmup"]),
        }
    )
    run_cmd(["python3", os.path.join(ROOT, "run_hybrid_bench.py")], env=env_single)

    # 2) Multi-device dual-platform
    multi_out = os.path.join(out, "multi_device")
    env_multi = os.environ.copy()
    env_multi.update(
        {
            "OPTBINLOG_HYBRID_MULTI_OUT_DIR": multi_out,
            "OPTBINLOG_HYBRID_MULTI_LOCAL_MODES": "text,binary,syslog",
            "OPTBINLOG_HYBRID_MULTI_LINUX_MODES": "text,binary,ftrace",
            "OPTBINLOG_MULTI_BASELINE": "text",
            "OPTBINLOG_HYBRID_MULTI_LINUX_BASELINE": "binary",
            "OPTBINLOG_MULTI_REPEATS": str(cfg["multi_repeats"]),
            "OPTBINLOG_MULTI_WARMUP": str(cfg["multi_warmup"]),
            "OPTBINLOG_SCAN_DEVICES": cfg["multi_scan_devices"],
            "OPTBINLOG_SCAN_RECORDS_PER_DEVICE": cfg["multi_scan_rpd"],
        }
    )
    run_cmd(["python3", os.path.join(ROOT, "run_hybrid_multi_bench.py")], env=env_multi)

    # 3) Binary contention local only
    bin_cont_out = os.path.join(out, "binary_contention")
    build_local_multi_binary()
    env_bin = os.environ.copy()
    env_bin.update(
        {
            "OPTBINLOG_MULTI_OUT_DIR": bin_cont_out,
            "OPTBINLOG_MULTI_BIN": os.path.join(ROOT, "optbinlog_multi_bench_macos"),
            "OPTBINLOG_MULTI_MODES": "binary",
            "OPTBINLOG_MULTI_BASELINE": "binary",
            "OPTBINLOG_MULTI_REPEATS": str(cfg["binary_repeats"]),
            "OPTBINLOG_MULTI_WARMUP": str(cfg["binary_warmup"]),
            "OPTBINLOG_SCAN_DEVICES": cfg["binary_scan_devices"],
            "OPTBINLOG_SCAN_RECORDS_PER_DEVICE": cfg["binary_scan_rpd"],
        }
    )
    run_cmd(["python3", os.path.join(ROOT, "run_multi_bench.py")], env=env_bin)

    # 4) Init-race local only
    init_out = os.path.join(out, "init_race")
    build_local_init_race()
    env_init = os.environ.copy()
    env_init.update(
        {
            "OPTBINLOG_INIT_OUT_DIR": init_out,
            "OPTBINLOG_INIT_PROCS": str(cfg["init_procs"]),
            "OPTBINLOG_INIT_REPEATS": str(cfg["init_repeats"]),
            "OPTBINLOG_INIT_WARMUP": str(cfg["init_warmup"]),
        }
    )
    run_cmd(["python3", os.path.join(ROOT, "run_init_race.py")], env=env_init)

    platform_meta = {
        "local": run_platform_suite.collect_platform_info(out),
        "linux": collect_linux_platform_info(linux_workdir),
    }

    summary = {
        "tag": tag,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "config": cfg,
        "platform_meta": platform_meta,
        "artifacts": {
            "single_merged_json": os.path.join(single_out, "bench_result_merged.json"),
            "single_dual_svg": os.path.join(single_out, "bench_dual_relative.svg"),
            "multi_merged_json": os.path.join(multi_out, "bench_multi_merged.json"),
            "multi_dual_svg": os.path.join(multi_out, "bench_multi_dual_relative.svg"),
            "binary_contention_json": os.path.join(bin_cont_out, "bench_multi_result.json"),
            "binary_contention_svg": os.path.join(bin_cont_out, "bench_multi_scan.svg"),
            "init_result_json": os.path.join(init_out, "init_race_result.json"),
            "init_timeline_svg": os.path.join(init_out, "init_race_result.svg"),
        },
    }
    summary_path = os.path.join(out, "suite_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    single_sum = summarize_single(summary["artifacts"]["single_merged_json"])
    multi_sum = summarize_multi(summary["artifacts"]["multi_merged_json"])
    init_sum = summarize_init(summary["artifacts"]["init_result_json"])
    bin_cont = summarize_binary_contention(summary["artifacts"]["binary_contention_json"])
    cfg["platform_meta"] = platform_meta
    report_path = os.path.join(out, "suite_report.md")
    write_summary(report_path, tag, cfg, single_sum, multi_sum, init_sum, bin_cont)

    latest = os.path.join(RESULTS_ROOT, "latest")
    if os.path.islink(latest) or os.path.exists(latest):
        if os.path.islink(latest):
            os.unlink(latest)
        elif os.path.isdir(latest):
            shutil.rmtree(latest)
        else:
            os.remove(latest)
    os.symlink(out, latest)

    # keep quick entry copies
    export_dir = os.path.join(out, "key_svgs")
    safe_copy(summary["artifacts"]["single_dual_svg"], export_dir)
    safe_copy(summary["artifacts"]["multi_dual_svg"], export_dir)
    safe_copy(summary["artifacts"]["binary_contention_svg"], export_dir)
    safe_copy(summary["artifacts"]["init_timeline_svg"], export_dir)

    print("saved", summary_path)
    print("saved", report_path)
    print("saved", latest)
    print("single", summary["artifacts"]["single_dual_svg"])
    print("multi", summary["artifacts"]["multi_dual_svg"])
    print("init", summary["artifacts"]["init_timeline_svg"])


if __name__ == "__main__":
    main()
