#!/usr/bin/env python3
import argparse
import concurrent.futures
import datetime as dt
import json
import math
import os
import shutil
import time
from typing import Any, Dict, List, Tuple

import run_l1_suite as l1


ROOT = os.path.dirname(__file__)
RESULTS_ROOT = os.path.join(ROOT, "results")


def percentile(vals: List[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    if len(s) == 1:
        return float(s[0])
    pos = (len(s) - 1) * (p / 100.0)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(s[lo])
    f = pos - lo
    return float(s[lo] * (1.0 - f) + s[hi] * f)


def stat(vals: List[float]) -> Dict[str, float]:
    if not vals:
        return {"n": 0, "mean": 0.0, "median": 0.0, "p95": 0.0, "min": 0.0, "max": 0.0}
    s = sorted(vals)
    n = len(s)
    return {
        "n": n,
        "mean": sum(s) / n,
        "median": percentile(s, 50),
        "p95": percentile(s, 95),
        "min": s[0],
        "max": s[-1],
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="L1 distributed init-race suite (binary only)")
    p.add_argument("--config", required=True, help="Path to L1 config JSON (nodes transport/workdir)")
    p.add_argument("--tag", default="", help="Override output tag")
    p.add_argument("--procs", type=int, default=32)
    p.add_argument("--repeats", type=int, default=10)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--max-workers", type=int, default=10)
    p.add_argument("--start-sync-delay-s", type=float, default=8.0)
    return p.parse_args()


def run_one_node(
    raw_node: Dict[str, Any],
    *,
    idx: int,
    out_root: str,
    tag: str,
    procs: int,
    repeats: int,
    warmup: int,
    start_at_epoch: float,
) -> Tuple[int, Dict[str, Any]]:
    node = l1.NodeExecutor(raw_node)
    rec: Dict[str, Any] = {
        "name": node.name,
        "transport": node.transport,
        "status": "running",
        "started_at": l1.utc_now(),
        "scheduled_start_at_epoch": start_at_epoch,
    }
    local_node_out = os.path.join(out_root, "nodes", node.name)
    l1.ensure_dir(local_node_out)
    remote_out = os.path.join(node.workdir, "bench_l1_init", tag, node.name)
    rec["remote_out_dir"] = remote_out

    try:
        build_cmd = raw_node.get("init_build_cmd")
        if not build_cmd:
            build_cmd = (
                "gcc -O2 -Wall -Wextra -std=c11 -Iinclude "
                "-o optbinlog_init_race optbinlog_init_race.c "
                "src/optbinlog_shared.c src/optbinlog_eventlog.c"
            )
        node.run_in_workdir(build_cmd, check=True)

        node.run_in_workdir(f"rm -rf {json.dumps(remote_out)} || true", check=False)
        node.run_in_workdir(f"mkdir -p {json.dumps(remote_out)}", check=True)

        eventlog_dir = str(raw_node.get("eventlog_dir", "./eventlogst"))
        text_profile = str(raw_node.get("text_profile", "")).strip()
        py = str(raw_node.get("python", "python3"))
        gate_py = f"import time; t={start_at_epoch:.6f}; d=t-time.time(); time.sleep(d if d>0 else 0.0)"
        cmd = (
            f"{py} -c {json.dumps(gate_py)}; "
            f"export OPTBINLOG_INIT_OUT_DIR={json.dumps(remote_out)}; "
            f"export OPTBINLOG_INIT_BIN=./optbinlog_init_race; "
            f"export OPTBINLOG_EVENTLOG_DIR={json.dumps(eventlog_dir)}; "
            f"export OPTBINLOG_INIT_PROCS={procs}; "
            f"export OPTBINLOG_INIT_REPEATS={repeats}; "
            f"export OPTBINLOG_INIT_WARMUP={warmup}; "
            + (f"export OPTBINLOG_TEXT_PROFILE={json.dumps(text_profile)}; " if text_profile else "")
            + f"{py} run_init_race.py"
        )
        proc = node.run_in_workdir(cmd, check=True)
        with open(os.path.join(local_node_out, "runner.stdout.log"), "w", encoding="utf-8") as f:
            f.write(proc.stdout or "")
        with open(os.path.join(local_node_out, "runner.stderr.log"), "w", encoding="utf-8") as f:
            f.write(proc.stderr or "")

        pulled = os.path.join(local_node_out, "init_out")
        node.pull_dir(remote_out, pulled)
        result_json = os.path.join(pulled, "init_race_result.json")
        if not os.path.exists(result_json):
            raise RuntimeError("missing init_race_result.json")
        data = json.load(open(result_json, "r", encoding="utf-8"))
        agg = data.get("aggregate", {})
        rec["summary"] = {
            "elapsed_ms_mean": float(agg.get("elapsed_ms", {}).get("mean", 0.0)),
            "elapsed_ms_p95": float(agg.get("elapsed_ms", {}).get("p95", 0.0)),
            "wait_events_mean": float(agg.get("wait_events", {}).get("mean", 0.0)),
            "wait_total_ms_mean": float(agg.get("wait_total_ms", {}).get("mean", 0.0)),
            "create_success_mean": float(agg.get("create_success", {}).get("mean", 0.0)),
            "open_existing_ok_mean": float(agg.get("open_existing_ok", {}).get("mean", 0.0)),
            "init_done_mean": float(agg.get("init_done", {}).get("mean", 0.0)),
        }
        rec["status"] = "ok"
    except Exception as e:
        rec["status"] = "failed"
        rec["error"] = str(e)
    finally:
        rec["finished_at"] = l1.utc_now()
    return idx, rec


def write_svg(path: str, nodes: List[Dict[str, Any]]) -> None:
    rows = [n for n in nodes if n.get("status") == "ok"]
    if not rows:
        return
    labels = [r["name"] for r in rows]
    vals = [r["summary"]["elapsed_ms_mean"] for r in rows]
    vmax = max(vals + [1.0])
    w, h = 1280, 480
    m = 70
    pw = w - 2 * m
    ph = 280
    y0 = 110
    bw = pw / max(1, len(rows)) * 0.6
    step = pw / max(1, len(rows))
    out = []
    out.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}">')
    out.append('<rect width="100%" height="100%" fill="#fff"/>')
    out.append(f'<text x="{w/2}" y="34" text-anchor="middle" font-family="Arial" font-size="20">L1 Init Race (binary only)</text>')
    out.append(f'<text x="{w/2}" y="56" text-anchor="middle" font-family="Arial" font-size="12">Per-node mean elapsed_ms after IQR filtering</text>')
    out.append(f'<rect x="{m}" y="{y0}" width="{pw}" height="{ph}" fill="#fff" stroke="#ddd"/>')
    for i in range(6):
        f = i / 5.0
        y = y0 + ph * (1.0 - f)
        v = vmax * f
        out.append(f'<line x1="{m}" y1="{y}" x2="{m+pw}" y2="{y}" stroke="#f0f0f0"/>')
        out.append(f'<text x="{m-6}" y="{y+4}" text-anchor="end" font-family="Arial" font-size="10">{v:.2f}</text>')
    for i, r in enumerate(rows):
        v = r["summary"]["elapsed_ms_mean"]
        bh = (v / vmax) * ph * 0.9
        x = m + i * step + (step - bw) / 2
        y = y0 + ph - bh
        out.append(f'<rect x="{x}" y="{y}" width="{bw}" height="{bh}" fill="#2b8cbe"/>')
        out.append(f'<text x="{x+bw/2}" y="{y-6}" text-anchor="middle" font-family="Arial" font-size="10">{v:.3f}</text>')
        out.append(f'<text x="{x+bw/2}" y="{y0+ph+16}" text-anchor="middle" font-family="Arial" font-size="10">{labels[i]}</text>')
    out.append("</svg>")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(out))


def main() -> None:
    args = parse_args()
    cfg = l1.load_config(args.config)
    nodes = cfg.get("nodes", [])
    if not isinstance(nodes, list) or not nodes:
        raise SystemExit("config.nodes must be non-empty")

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = args.tag or f"l1_init_{ts}"
    out_root = os.path.join(RESULTS_ROOT, tag)
    l1.ensure_dir(out_root)
    l1.ensure_dir(os.path.join(out_root, "nodes"))

    start_at = time.time() + max(0.0, args.start_sync_delay_s)
    run_nodes: List[Dict[str, Any]] = [{} for _ in nodes]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(args.max_workers, len(nodes)))) as ex:
        futs = [
            ex.submit(
                run_one_node,
                n,
                idx=i,
                out_root=out_root,
                tag=tag,
                procs=args.procs,
                repeats=args.repeats,
                warmup=args.warmup,
                start_at_epoch=start_at,
            )
            for i, n in enumerate(nodes)
        ]
        for f in concurrent.futures.as_completed(futs):
            idx, rec = f.result()
            run_nodes[idx] = rec

    ok = [n for n in run_nodes if n.get("status") == "ok"]
    elapsed = [n["summary"]["elapsed_ms_mean"] for n in ok]
    waits = [n["summary"]["wait_total_ms_mean"] for n in ok]
    summary = {
        "tag": tag,
        "generated_at": l1.utc_now(),
        "config_path": os.path.abspath(args.config),
        "procs": args.procs,
        "repeats": args.repeats,
        "warmup": args.warmup,
        "scheduled_start_at_epoch": start_at,
        "nodes": run_nodes,
        "aggregate": {
            "elapsed_ms_mean_by_node": stat(elapsed),
            "wait_total_ms_mean_by_node": stat(waits),
            "ok_nodes": len(ok),
            "total_nodes": len(run_nodes),
        },
    }
    out_json = os.path.join(out_root, "l1_init_race_summary.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    out_svg = os.path.join(out_root, "l1_init_race_overview.svg")
    write_svg(out_svg, run_nodes)

    latest = os.path.join(RESULTS_ROOT, "l1_init_latest")
    if os.path.islink(latest) or os.path.exists(latest):
        if os.path.islink(latest):
            os.unlink(latest)
        elif os.path.isdir(latest):
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

