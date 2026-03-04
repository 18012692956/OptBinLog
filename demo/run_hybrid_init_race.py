import json
import os
import subprocess


ROOT = os.path.dirname(__file__)
RUN_INIT = os.path.join(ROOT, "run_init_race.py")
PROJECT = os.path.abspath(os.path.join(ROOT, ".."))

LOCAL_BIN = os.path.join(ROOT, "optbinlog_init_race_macos")
LINUX_BIN = os.path.join(ROOT, "optbinlog_init_race_linux")

HYBRID_OUT = os.environ.get("OPTBINLOG_HYBRID_INIT_OUT_DIR", os.path.join(ROOT, "bench", "hybrid_init_race"))
LOCAL_OUT = os.path.join(HYBRID_OUT, "local")
LINUX_OUT = os.path.join(HYBRID_OUT, "linux")
MERGED_JSON = os.path.join(HYBRID_OUT, "init_race_merged.json")
COMPARE_SVG = os.path.join(HYBRID_OUT, "init_race_compare.svg")

LINUX_INSTANCE = os.environ.get("OPTBINLOG_LINUX_INSTANCE", "thesis-linux")


def run(cmd, env=None, cwd=None):
    proc = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed\ncmd: {}\nstdout:\n{}\nstderr:\n{}".format(
                " ".join(cmd), proc.stdout, proc.stderr
            )
        )
    return proc.stdout


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
        "optbinlog_init_race.c",
        "src/optbinlog_shared.c",
        "src/optbinlog_eventlog.c",
    ]
    run(cmd, cwd=ROOT)


def build_linux_binary():
    script = (
        "set -euo pipefail; "
        f"cd {json.dumps(ROOT)}; "
        "gcc -O2 -Wall -Wextra -std=c11 -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L "
        "-Iinclude -o optbinlog_init_race_linux optbinlog_init_race.c "
        "src/optbinlog_shared.c src/optbinlog_eventlog.c"
    )
    run(["limactl", "shell", LINUX_INSTANCE, "--", "bash", "-lc", script], cwd=PROJECT)


def run_local():
    env = os.environ.copy()
    env["OPTBINLOG_INIT_OUT_DIR"] = LOCAL_OUT
    env["OPTBINLOG_INIT_BIN"] = LOCAL_BIN
    propagate_env(
        os.environ,
        env,
        [
            "OPTBINLOG_INIT_PROCS",
            "OPTBINLOG_INIT_REPEATS",
            "OPTBINLOG_INIT_WARMUP",
            "OPTBINLOG_INIT_IQR_MULT",
            "OPTBINLOG_INIT_CMD_RETRIES",
        ],
    )
    run(["python3", RUN_INIT], env=env, cwd=ROOT)


def run_linux():
    linux_env = {
        "OPTBINLOG_INIT_OUT_DIR": LINUX_OUT,
        "OPTBINLOG_INIT_BIN": LINUX_BIN,
    }
    propagate_env(
        os.environ,
        linux_env,
        [
            "OPTBINLOG_INIT_PROCS",
            "OPTBINLOG_INIT_REPEATS",
            "OPTBINLOG_INIT_WARMUP",
            "OPTBINLOG_INIT_IQR_MULT",
            "OPTBINLOG_INIT_CMD_RETRIES",
        ],
    )
    exports = " ".join([f"{k}={json.dumps(v)}" for k, v in linux_env.items()])
    script = "set -euo pipefail; " + f"cd {json.dumps(ROOT)}; " + f"export {exports}; " + "python3 run_init_race.py"
    run(["limactl", "shell", LINUX_INSTANCE, "--", "bash", "-lc", script], cwd=PROJECT)


def pct_change(base, cur, lower_better=True):
    if base == 0:
        return 0.0
    if lower_better:
        return (base - cur) / base * 100.0
    return (cur - base) / base * 100.0


def build_compare_svg(merged, out_path):
    local = merged["sources"]["local"]["aggregate"]
    linux = merged["sources"]["linux"]["aggregate"]

    metrics = [
        ("elapsed_ms", "elapsed mean (ms)", True),
        ("wait_total_ms", "wait_total mean (ms)", True),
        ("wait_p95_ms", "wait_p95 mean (ms)", True),
        ("wait_events", "wait_events mean", True),
    ]

    width = 1280
    height = 560
    margin = 80
    panel_w = (width - margin * 2 - 50) / 2.0
    panel_h = 340
    panel_y = 130
    local_x = margin
    linux_x = margin + panel_w + 50

    vals = []
    for key, _, _ in metrics:
        vals.extend([local[key]["mean"], linux[key]["mean"]])
    vmax = max(vals + [1.0])

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    lines.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    lines.append(f'<text x="{width/2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">init-race dual-platform comparison</text>')
    lines.append(f'<text x="{width/2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">lower is better for all shown metrics</text>')

    for x0, label, color in [(local_x, "local", "#2b8cbe"), (linux_x, "linux", "#f03b20")]:
        lines.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#ffffff" stroke="#d9d9d9"/>')
        lines.append(f'<text x="{x0 + panel_w/2}" y="{panel_y - 12}" text-anchor="middle" font-family="Arial" font-size="15">{label}</text>')
        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            val = vmax * frac
            lines.append(f'<line x1="{x0}" y1="{y}" x2="{x0 + panel_w}" y2="{y}" stroke="#f0f0f0"/>')
            lines.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="10">{val:.2f}</text>')

        step = panel_w / max(1, len(metrics))
        bar_w = min(60, step * 0.6)
        src = local if label == "local" else linux
        for i, (key, mlabel, _) in enumerate(metrics):
            v = src[key]["mean"]
            h = (v / vmax) * (panel_h * 0.9) if vmax > 0 else 0
            bx = x0 + i * step + (step - bar_w) / 2
            by = panel_y + panel_h - h
            lines.append(f'<rect x="{bx}" y="{by}" width="{bar_w}" height="{h}" fill="{color}"/>')
            lines.append(f'<text x="{bx + bar_w/2}" y="{by - 8}" text-anchor="middle" font-family="Arial" font-size="10">{v:.3f}</text>')
            lines.append(f'<text x="{bx + bar_w/2}" y="{panel_y + panel_h + 18}" text-anchor="middle" font-family="Arial" font-size="10">{mlabel}</text>')

    comp = merged["comparison_local_vs_linux"]
    lines.append(
        f'<text x="{width/2}" y="{height - 24}" text-anchor="middle" font-family="Arial" font-size="12">'
        f'local vs linux: elapsed={comp["elapsed_ms_improve_pct"]:.1f}% wait_total={comp["wait_total_ms_improve_pct"]:.1f}% wait_p95={comp["wait_p95_ms_improve_pct"]:.1f}% wait_events={comp["wait_events_improve_pct"]:.1f}%</text>'
    )

    lines.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    os.makedirs(HYBRID_OUT, exist_ok=True)
    os.makedirs(LOCAL_OUT, exist_ok=True)
    os.makedirs(LINUX_OUT, exist_ok=True)

    build_local_binary()
    run_local()
    build_linux_binary()
    run_linux()

    local_json = os.path.join(LOCAL_OUT, "init_race_result.json")
    linux_json = os.path.join(LINUX_OUT, "init_race_result.json")

    with open(local_json, "r", encoding="utf-8") as f:
        local = json.load(f)
    with open(linux_json, "r", encoding="utf-8") as f:
        linux = json.load(f)

    l = local["aggregate"]
    r = linux["aggregate"]
    comparison = {
        "elapsed_ms_improve_pct": pct_change(r["elapsed_ms"]["mean"], l["elapsed_ms"]["mean"], lower_better=True),
        "wait_total_ms_improve_pct": pct_change(r["wait_total_ms"]["mean"], l["wait_total_ms"]["mean"], lower_better=True),
        "wait_p95_ms_improve_pct": pct_change(r["wait_p95_ms"]["mean"], l["wait_p95_ms"]["mean"], lower_better=True),
        "wait_events_improve_pct": pct_change(r["wait_events"]["mean"], l["wait_events"]["mean"], lower_better=True),
    }

    merged = {
        "config": {
            "strategy": "local + linux init-race simulation",
            "linux_instance": LINUX_INSTANCE,
            "procs": local["config"]["procs"],
            "repeats": local["config"]["repeats"],
            "warmup": local["config"]["warmup"],
        },
        "sources": {
            "local": local,
            "linux": linux,
        },
        "comparison_local_vs_linux": comparison,
        "artifacts": {
            "local_timeline": os.path.join(LOCAL_OUT, "init_race_result.svg"),
            "linux_timeline": os.path.join(LINUX_OUT, "init_race_result.svg"),
        },
    }
    with open(MERGED_JSON, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2)
    build_compare_svg(merged, COMPARE_SVG)

    print("saved", MERGED_JSON)
    print("saved", COMPARE_SVG)
    print("saved", os.path.join(LOCAL_OUT, "init_race_result.svg"))
    print("saved", os.path.join(LINUX_OUT, "init_race_result.svg"))


if __name__ == "__main__":
    main()
