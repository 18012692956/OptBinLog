import json
import os
import subprocess


ROOT = os.path.dirname(__file__)
RUN_MULTI = os.path.join(ROOT, "run_multi_bench.py")
PROJECT = os.path.abspath(os.path.join(ROOT, ".."))

LOCAL_BIN = os.path.join(ROOT, "optbinlog_multi_bench_macos")
LINUX_BIN = os.path.join(ROOT, "optbinlog_multi_bench_linux")

HYBRID_OUT = os.environ.get("OPTBINLOG_HYBRID_MULTI_OUT_DIR", os.path.join(ROOT, "bench", "hybrid_multi"))
LOCAL_OUT = os.path.join(HYBRID_OUT, "local")
LINUX_OUT = os.path.join(HYBRID_OUT, "linux")
MERGED_JSON = os.path.join(HYBRID_OUT, "bench_multi_merged.json")
DUAL_HEATMAP_SVG = os.path.join(HYBRID_OUT, "bench_multi_dual_relative.svg")

LOCAL_MODES = os.environ.get("OPTBINLOG_HYBRID_MULTI_LOCAL_MODES", "text,binary,syslog")
LINUX_MODES = os.environ.get("OPTBINLOG_HYBRID_MULTI_LINUX_MODES", "text,binary,syslog,ftrace")
LOCAL_BASELINE = os.environ.get("OPTBINLOG_MULTI_BASELINE", "text")
LINUX_BASELINE = os.environ.get("OPTBINLOG_HYBRID_MULTI_LINUX_BASELINE", "text")
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


def ensure_mode(modes, mode):
    out = list(modes)
    if mode and mode not in out:
        out.append(mode)
    return out


def propagate_env(src_env, dst_env, keys):
    for k in keys:
        if k in src_env:
            dst_env[k] = src_env[k]


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
        "optbinlog_multi_bench.c",
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
        "-Iinclude -o optbinlog_multi_bench_linux optbinlog_multi_bench.c "
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


def run_local(local_modes):
    env = os.environ.copy()
    env["OPTBINLOG_MULTI_OUT_DIR"] = LOCAL_OUT
    env["OPTBINLOG_MULTI_BIN"] = LOCAL_BIN
    env["OPTBINLOG_MULTI_MODES"] = ",".join(local_modes)
    env["OPTBINLOG_MULTI_BASELINE"] = LOCAL_BASELINE if LOCAL_BASELINE in local_modes else local_modes[0]
    propagate_env(
        os.environ,
        env,
        [
            "OPTBINLOG_DEVICES",
            "OPTBINLOG_RECORDS_PER_DEVICE",
            "OPTBINLOG_MULTI_REPEATS",
            "OPTBINLOG_MULTI_WARMUP",
            "OPTBINLOG_MULTI_IQR_MULT",
            "OPTBINLOG_MULTI_FILTER_FIELD",
            "OPTBINLOG_SCAN_DEVICES",
            "OPTBINLOG_SCAN_RECORDS_PER_DEVICE",
        ],
    )
    run(["python3", RUN_MULTI], env=env, cwd=ROOT)


def run_linux(linux_modes):
    linux_env = {
        "OPTBINLOG_MULTI_OUT_DIR": LINUX_OUT,
        "OPTBINLOG_MULTI_BIN": LINUX_BIN,
        "OPTBINLOG_MULTI_MODES": ",".join(linux_modes),
        "OPTBINLOG_MULTI_BASELINE": LINUX_BASELINE if LINUX_BASELINE in linux_modes else linux_modes[0],
        "OPTBINLOG_TRACE_MARKER": TRACE_MARKER,
    }
    propagate_env(
        os.environ,
        linux_env,
        [
            "OPTBINLOG_DEVICES",
            "OPTBINLOG_RECORDS_PER_DEVICE",
            "OPTBINLOG_MULTI_REPEATS",
            "OPTBINLOG_MULTI_WARMUP",
            "OPTBINLOG_MULTI_IQR_MULT",
            "OPTBINLOG_MULTI_FILTER_FIELD",
            "OPTBINLOG_SCAN_DEVICES",
            "OPTBINLOG_SCAN_RECORDS_PER_DEVICE",
        ],
    )
    exports = " ".join([f"{k}={json.dumps(v)}" for k, v in linux_env.items()])
    script = "set -euo pipefail; " + f"cd {json.dumps(ROOT)}; " + f"export {exports}; " + "python3 run_multi_bench.py"
    run(["limactl", "shell", LINUX_INSTANCE, "--", "bash", "-lc", script], cwd=PROJECT)


def collect_source(source_name, data):
    out = {
        "source": source_name,
        "baseline_mode": data["config"]["baseline_mode"],
        "active_modes": data["config"]["active_modes"],
        "scenarios": [],
    }
    for sc in data["scenarios"]:
        comp = sc["summary"]["comparison"]
        out["scenarios"].append(
            {
                "scenario": sc["scenario"],
                "devices": sc["devices"],
                "records_per_device": sc["records_per_device"],
                "comparison": comp,
            }
        )
    return out


def color_for_value(v, vmax):
    vmax = max(1.0, float(vmax))
    x = max(-1.0, min(1.0, v / vmax))
    if x >= 0:
        r0, g0, b0 = 237, 248, 233
        r1, g1, b1 = 35, 139, 69
        t = x
    else:
        r0, g0, b0 = 254, 224, 210
        r1, g1, b1 = 165, 15, 21
        t = -x
    r = int(r0 + (r1 - r0) * t)
    g = int(g0 + (g1 - g0) * t)
    b = int(b0 + (b1 - b0) * t)
    return f"#{r:02x}{g:02x}{b:02x}"


def build_dual_heatmap_svg(merged, out_path):
    metric_defs = [
        ("elapsed_improve_pct", "time%"),
        ("size_save_pct", "size%"),
        ("throughput_gain_pct", "throughput%"),
    ]
    sources = merged["sources_data"]

    panel_gap = 36
    row_h = 24
    col_w = 86
    metric_gap = 18
    panel_pad = 20

    max_rows = 1
    max_cols = 1
    for src in sources:
        rows = len(src["scenarios"])
        cols = max([len(sc["comparison"]["by_mode"]) for sc in src["scenarios"]] + [1])
        max_rows = max(max_rows, rows)
        max_cols = max(max_cols, cols)

    block_h = 38 + max_rows * row_h
    panel_h = panel_pad * 2 + len(metric_defs) * block_h + (len(metric_defs) - 1) * metric_gap
    panel_w = 230 + max_cols * col_w

    width = panel_w * 2 + panel_gap + 40
    height = panel_h + 80

    all_values = []
    for src in sources:
        for sc in src["scenarios"]:
            for row in sc["comparison"]["by_mode"].values():
                all_values.extend([row["elapsed_improve_pct"], row["size_save_pct"], row["throughput_gain_pct"]])
    vmax = max([10.0] + [abs(v) for v in all_values])

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width/2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">multi-device dual-platform relative heatmap</text>')
    lines.append(f'<text x="{width/2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">value>0 is better; baseline per platform</text>')

    for si, src in enumerate(sources):
        x0 = 20 + si * (panel_w + panel_gap)
        y0 = 74
        lines.append(f'<rect x="{x0}" y="{y0}" width="{panel_w}" height="{panel_h}" fill="#ffffff" stroke="#d9d9d9"/>')
        lines.append(
            f'<text x="{x0 + panel_w/2}" y="{y0 + 24}" text-anchor="middle" font-family="Arial" font-size="16">{src["source"]} (baseline={src["baseline_mode"]})</text>'
        )

        y_metric = y0 + panel_pad + 18
        for metric_key, metric_title in metric_defs:
            lines.append(f'<text x="{x0 + panel_w/2}" y="{y_metric}" text-anchor="middle" font-family="Arial" font-size="13">{metric_title}</text>')
            y_table = y_metric + 10

            mode_order = []
            for sc in src["scenarios"]:
                for m in sc["comparison"]["by_mode"].keys():
                    if m not in mode_order:
                        mode_order.append(m)
            if not mode_order:
                mode_order = ["(none)"]

            for ci, mode in enumerate(mode_order):
                cx = x0 + 220 + ci * col_w
                lines.append(f'<text x="{cx + col_w/2}" y="{y_table + 14}" text-anchor="middle" font-family="Arial" font-size="11">{mode}</text>')

            for ri, sc in enumerate(src["scenarios"]):
                row_y = y_table + 22 + ri * row_h
                sc_label = f'{sc["scenario"]}'
                lines.append(f'<text x="{x0 + 10}" y="{row_y + 16}" font-family="Arial" font-size="11">{sc_label}</text>')
                lines.append(
                    f'<text x="{x0 + 150}" y="{row_y + 16}" font-family="Arial" font-size="10" fill="#666">d={sc["devices"]}, rpd={sc["records_per_device"]}</text>'
                )
                by_mode = sc["comparison"]["by_mode"]
                for ci, mode in enumerate(mode_order):
                    cx = x0 + 220 + ci * col_w
                    if mode in by_mode:
                        v = by_mode[mode][metric_key]
                        color = color_for_value(v, vmax)
                        text = f"{v:.1f}%"
                    else:
                        v = 0.0
                        color = "#f7f7f7"
                        text = "-"
                    lines.append(f'<rect x="{cx}" y="{row_y}" width="{col_w-4}" height="{row_h-4}" fill="{color}" stroke="#ffffff"/>')
                    lines.append(f'<text x="{cx + (col_w-4)/2}" y="{row_y + 16}" text-anchor="middle" font-family="Arial" font-size="10">{text}</text>')

            y_metric = y_table + 22 + max_rows * row_h + metric_gap

    lx = width - 250
    ly = height - 26
    lines.append(f'<text x="{lx}" y="{ly-6}" font-family="Arial" font-size="10">negative</text>')
    for i in range(11):
        frac = i / 10.0
        v = -vmax + 2 * vmax * frac
        c = color_for_value(v, vmax)
        lines.append(f'<rect x="{lx + 52 + i*14}" y="{ly - 16}" width="14" height="10" fill="{c}"/>')
    lines.append(f'<text x="{lx + 52 + 11*14 + 8}" y="{ly-6}" font-family="Arial" font-size="10">positive</text>')
    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    os.makedirs(HYBRID_OUT, exist_ok=True)
    os.makedirs(LOCAL_OUT, exist_ok=True)
    os.makedirs(LINUX_OUT, exist_ok=True)

    local_modes = ensure_mode(parse_modes(LOCAL_MODES), LOCAL_BASELINE)
    linux_modes = ensure_mode(parse_modes(LINUX_MODES), LINUX_BASELINE)

    build_local_binary()
    run_local(local_modes)
    build_linux_binary()
    ensure_linux_trace_marker_access()
    run_linux(linux_modes)

    with open(os.path.join(LOCAL_OUT, "bench_multi_result.json"), "r", encoding="utf-8") as f:
        local = json.load(f)
    with open(os.path.join(LINUX_OUT, "bench_multi_result.json"), "r", encoding="utf-8") as f:
        linux = json.load(f)

    merged = {
        "config": {
            "strategy": "local + linux multi-device simulation",
            "local_modes_requested": parse_modes(LOCAL_MODES),
            "linux_modes_requested": parse_modes(LINUX_MODES),
            "local_baseline": local["config"]["baseline_mode"],
            "linux_baseline": linux["config"]["baseline_mode"],
            "linux_instance": LINUX_INSTANCE,
        },
        "sources_data": [
            collect_source("local", local),
            collect_source("linux", linux),
        ],
        "sources": {
            "local_result": os.path.join(LOCAL_OUT, "bench_multi_result.json"),
            "linux_result": os.path.join(LINUX_OUT, "bench_multi_result.json"),
        },
    }

    with open(MERGED_JSON, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2)
    build_dual_heatmap_svg(merged, DUAL_HEATMAP_SVG)

    print("saved", MERGED_JSON)
    print("saved", DUAL_HEATMAP_SVG)
    print("local_result_svg", os.path.join(LOCAL_OUT, "bench_multi_result.svg"))
    print("linux_result_svg", os.path.join(LINUX_OUT, "bench_multi_result.svg"))


if __name__ == "__main__":
    main()
