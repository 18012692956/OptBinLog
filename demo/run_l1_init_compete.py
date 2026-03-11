#!/usr/bin/env python3
import argparse
import concurrent.futures
import datetime as dt
import json
import math
import os
import shlex
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
    x = (len(s) - 1) * (p / 100.0)
    lo = int(math.floor(x))
    hi = int(math.ceil(x))
    if lo == hi:
        return float(s[lo])
    f = x - lo
    return float(s[lo] * (1.0 - f) + s[hi] * f)


def metric_stats(vals: List[float]) -> Dict[str, float]:
    if not vals:
        return {
            "n": 0,
            "mean": 0.0,
            "median": 0.0,
            "p95": 0.0,
            "min": 0.0,
            "max": 0.0,
            "std": 0.0,
            "ci95_low": 0.0,
            "ci95_high": 0.0,
        }
    n = len(vals)
    m = sum(vals) / n
    if n > 1:
        var = sum((x - m) ** 2 for x in vals) / (n - 1)
        std = math.sqrt(var)
        half = 1.96 * std / math.sqrt(n)
    else:
        std = 0.0
        half = 0.0
    return {
        "n": n,
        "mean": m,
        "median": percentile(vals, 50),
        "p95": percentile(vals, 95),
        "min": min(vals),
        "max": max(vals),
        "std": std,
        "ci95_low": m - half,
        "ci95_high": m + half,
    }


def iqr_filter(rows: List[Dict[str, Any]], field: str, mult: float = 1.5) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    vals = [float(r[field]) for r in rows]
    if len(vals) < 4:
        return rows, {"field": field, "method": "iqr", "kept": len(rows), "removed": 0}
    q1 = percentile(vals, 25)
    q3 = percentile(vals, 75)
    iqr = q3 - q1
    lo = q1 - mult * iqr
    hi = q3 + mult * iqr
    kept = [r for r in rows if lo <= float(r[field]) <= hi]
    if len(kept) < max(3, len(rows) // 2):
        return rows, {
            "field": field,
            "method": "iqr_fallback_to_all",
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower": lo,
            "upper": hi,
            "kept": len(rows),
            "removed": 0,
        }
    return kept, {
        "field": field,
        "method": "iqr",
        "q1": q1,
        "q3": q3,
        "iqr": iqr,
        "lower": lo,
        "upper": hi,
        "kept": len(kept),
        "removed": len(rows) - len(kept),
    }


def parse_trace(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "rb") as f:
        for raw in f:
            line = raw.decode("utf-8", errors="ignore").replace("\x00", "").strip()
            if not line:
                continue
            parts = line.split(" ", 2)
            if len(parts) < 3:
                continue
            try:
                ts_ns = int(parts[0])
                pid = int(parts[1].split("=")[1])
            except Exception:
                continue
            out.append({"ts_ns": ts_ns, "pid": pid, "event": parts[2]})
    out.sort(key=lambda x: (x["pid"], x["ts_ns"]))
    return out


def summarize_trace(events: List[Dict[str, Any]]) -> Dict[str, float]:
    if not events:
        return {
            "create_success": 0,
            "open_existing_ok": 0,
            "wait_initializing": 0,
            "init_done": 0,
            "elapsed_ms": 0.0,
            "start_ns": 0,
            "end_ns": 0,
        }
    start = min(e["ts_ns"] for e in events)
    end = max(e["ts_ns"] for e in events)
    return {
        "create_success": sum(1 for e in events if e["event"] == "create_success"),
        "open_existing_ok": sum(1 for e in events if e["event"] == "open_existing_ok"),
        "wait_initializing": sum(1 for e in events if e["event"] == "wait_initializing"),
        "init_done": sum(1 for e in events if e["event"] == "init_done"),
        "elapsed_ms": (end - start) / 1e6,
        "start_ns": start,
        "end_ns": end,
    }


def run_one_node_once(
    raw_node: Dict[str, Any],
    *,
    round_idx: int,
    round_kind: str,
    start_at: float,
    shared_path: str,
    trace_path: str,
    eventlog_dir: str,
    lock_mode: str,
    lock_timeout_ms: int,
) -> Dict[str, Any]:
    node = l1.NodeExecutor(raw_node)
    py = str(node.node.get("python", "python3"))
    gate = f"import time; t={start_at:.6f}; d=t-time.time(); time.sleep(d if d>0 else 0.0)"
    lock_mode = (lock_mode or "").strip()
    lock_timeout_ms = int(max(100, lock_timeout_ms))
    env_parts = []
    if lock_mode:
        env_parts.append(f"OPTBINLOG_INIT_LOCK_MODE={json.dumps(lock_mode)}")
    env_parts.append(f"OPTBINLOG_INIT_LOCK_TIMEOUT_MS={lock_timeout_ms}")
    env_prefix = (" ".join(env_parts) + " ") if env_parts else ""
    cmd = (
        f"{py} -c {json.dumps(gate)}; "
        f"{env_prefix}"
        f"./optbinlog_init_race --eventlog-dir {json.dumps(eventlog_dir)} "
        f"--shared {json.dumps(shared_path)} --trace {json.dumps(trace_path)} --procs 1"
    )
    wall_start_ns = time.time_ns()
    proc = node.run_in_workdir(cmd, check=False)
    wall_end_ns = time.time_ns()
    rec = {
        "name": node.name,
        "round": round_idx,
        "kind": round_kind,
        "returncode": int(proc.returncode),
        "trace_path": trace_path,
        "wall_start_ns": wall_start_ns,
        "wall_end_ns": wall_end_ns,
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
    }
    evs = parse_trace(trace_path)
    rec["trace_summary"] = summarize_trace(evs)
    return rec


def ensure_build(raw_node: Dict[str, Any]) -> None:
    node = l1.NodeExecutor(raw_node)
    build_cmd = raw_node.get("init_build_cmd")
    if not build_cmd:
        build_cmd = (
            "gcc -O2 -Wall -Wextra -std=c11 -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L -Iinclude "
            "-o optbinlog_init_race optbinlog_init_race.c src/optbinlog_shared.c src/optbinlog_eventlog.c"
        )
    node.run_in_workdir(build_cmd, check=True)


def cleanup_shared_on_node(raw_node: Dict[str, Any], shared_path: str) -> None:
    node = l1.NodeExecutor(raw_node)
    d = os.path.dirname(shared_path) or "."
    dq = shlex.quote(d)
    p = shlex.quote(shared_path)
    pl = shlex.quote(shared_path + ".lock")
    node.run_in_workdir(f"mkdir -p {dq}; rm -f {p} {pl} || true", check=False)


def build_svg(out_svg: str, kept_runs: List[Dict[str, Any]]) -> None:
    if not kept_runs:
        return
    w, h = 1280, 560
    m = 80
    pw = w - 2 * m
    ph = 320
    y0 = 140
    vals = [float(r["global_elapsed_ms"]) for r in kept_runs]
    vmax = max(vals + [1.0])
    step = pw / max(1, len(kept_runs))
    bw = step * 0.62

    lines = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}">')
    lines.append('<rect width="100%" height="100%" fill="#fff"/>')
    lines.append('<text x="50%" y="34" text-anchor="middle" font-family="Arial" font-size="20">Real-Device Simulation Init Competition (1 contender per node)</text>')
    lines.append('<text x="50%" y="56" text-anchor="middle" font-family="Arial" font-size="12">bar=global elapsed(ms), blue dot=nodes with create_success>0</text>')
    lines.append(f'<rect x="{m}" y="{y0}" width="{pw}" height="{ph}" fill="#fff" stroke="#ddd"/>')

    for i in range(6):
        f = i / 5
        y = y0 + ph * (1 - f)
        v = vmax * f
        lines.append(f'<line x1="{m}" y1="{y}" x2="{m+pw}" y2="{y}" stroke="#f0f0f0"/>')
        lines.append(f'<text x="{m-8}" y="{y+4}" text-anchor="end" font-family="Arial" font-size="10">{v:.1f}</text>')

    for i, r in enumerate(kept_runs):
        x = m + i * step + (step - bw) / 2
        v = float(r["global_elapsed_ms"])
        bh = (v / vmax) * ph * 0.9
        y = y0 + ph - bh
        kind = r.get("kind", "run")
        fill = "#2b8cbe" if kind == "run" else "#9ecae1"
        lines.append(f'<rect x="{x}" y="{y}" width="{bw}" height="{bh}" fill="{fill}"/>')
        lines.append(f'<text x="{x+bw/2}" y="{y-6}" text-anchor="middle" font-family="Arial" font-size="10">{v:.2f}</text>')
        cs = int(r.get("nodes_created", 0))
        lines.append(f'<circle cx="{x+bw/2}" cy="{y0+ph+24}" r="4" fill="#08519c"/>')
        lines.append(f'<text x="{x+bw/2}" y="{y0+ph+40}" text-anchor="middle" font-family="Arial" font-size="10">{cs}</text>')
        lines.append(f'<text x="{x+bw/2}" y="{y0+ph+56}" text-anchor="middle" font-family="Arial" font-size="9">{kind[0]}{r["round"]:02d}</text>')

    lines.append('</svg>')
    with open(out_svg, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="L1 init race competition: one contender per node, same shared file")
    p.add_argument("--config", required=True)
    p.add_argument("--tag", default="")
    p.add_argument("--repeats", type=int, default=8)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--max-workers", type=int, default=10)
    p.add_argument("--start-sync-delay-s", type=float, default=6.0)
    p.add_argument("--eventlog-dir", default="")
    p.add_argument("--shared-path", default="")
    p.add_argument("--lock-mode", default="create_excl", help="Lock mode passed to init race (e.g., create_excl or flock)")
    p.add_argument("--lock-timeout-ms", type=int, default=15000)
    p.add_argument("--iqr-mult", type=float, default=1.5)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = l1.load_config(args.config)
    nodes = cfg.get("nodes", [])
    if not isinstance(nodes, list) or not nodes:
        raise SystemExit("config.nodes must be non-empty")

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = args.tag or f"l1_init_compete_{ts}"
    out_root = os.path.join(RESULTS_ROOT, tag)
    l1.ensure_dir(out_root)
    l1.ensure_dir(os.path.join(out_root, "runs"))

    cfg_shared = str(cfg.get("init_shared_path", "")).strip()
    shared_path = args.shared_path.strip() or cfg_shared or os.path.join(out_root, "shared_eventtag.bin")
    os.makedirs(os.path.dirname(shared_path), exist_ok=True)
    cfg_eventlog_dir = str(cfg.get("init_eventlog_dir", "")).strip()

    # Build once per node.
    for n in nodes:
        ensure_build(n)

    all_rounds: List[Dict[str, Any]] = []
    all_node_records: List[Dict[str, Any]] = []

    total_rounds = [("warmup", i) for i in range(args.warmup)] + [("run", i) for i in range(args.repeats)]
    for kind, ridx in total_rounds:
        for p in [shared_path, shared_path + ".lock"]:
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass
        for n in nodes:
            cleanup_shared_on_node(n, shared_path)

        start_at = time.time() + max(0.0, args.start_sync_delay_s)
        round_dir = os.path.join(out_root, "runs", f"{kind}_{ridx:03d}")
        l1.ensure_dir(round_dir)

        def submit_one(nraw: Dict[str, Any]) -> Dict[str, Any]:
            node = l1.NodeExecutor(nraw)
            evdir = (
                args.eventlog_dir.strip()
                or str(node.node.get("init_eventlog_dir", "")).strip()
                or cfg_eventlog_dir
                or str(node.node.get("eventlog_dir", "./eventlogst"))
            )
            trace_path = os.path.join(round_dir, f"{node.name}.trace.log")
            return run_one_node_once(
                nraw,
                round_idx=ridx,
                round_kind=kind,
                start_at=start_at,
                shared_path=shared_path,
                trace_path=trace_path,
                eventlog_dir=evdir,
                lock_mode=args.lock_mode,
                lock_timeout_ms=args.lock_timeout_ms,
            )

        per_node: List[Dict[str, Any]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(args.max_workers, len(nodes)))) as ex:
            futs = [ex.submit(submit_one, n) for n in nodes]
            for f in concurrent.futures.as_completed(futs):
                per_node.append(f.result())

        per_node.sort(key=lambda x: x["name"])
        all_node_records.extend(per_node)

        wall_start_ns = []
        wall_end_ns = []
        cs_total = 0
        open_total = 0
        wait_total = 0
        nodes_created = 0
        nodes_opened = 0
        failed_nodes = 0

        for r in per_node:
            s = r["trace_summary"]
            cs = int(s["create_success"])
            op = int(s["open_existing_ok"])
            wt = int(s["wait_initializing"])
            cs_total += cs
            open_total += op
            wait_total += wt
            if cs > 0:
                nodes_created += 1
            if op > 0:
                nodes_opened += 1
            if int(r["returncode"]) != 0:
                failed_nodes += 1
            ws = int(r.get("wall_start_ns", 0))
            we = int(r.get("wall_end_ns", 0))
            if ws > 0 and we > 0 and we >= ws:
                wall_start_ns.append(ws)
                wall_end_ns.append(we)

        global_elapsed_ms = 0.0
        if wall_start_ns and wall_end_ns:
            global_elapsed_ms = (max(wall_end_ns) - min(wall_start_ns)) / 1e6

        all_rounds.append(
            {
                "kind": kind,
                "round": ridx,
                "nodes": len(per_node),
                "failed_nodes": failed_nodes,
                "create_success_total": cs_total,
                "open_existing_total": open_total,
                "wait_initializing_total": wait_total,
                "single_creator": 1 if cs_total == 1 else 0,
                "nodes_created": nodes_created,
                "nodes_opened": nodes_opened,
                "global_elapsed_ms": global_elapsed_ms,
            }
        )

    run_rows_all = [r for r in all_rounds if r["kind"] == "run"]
    run_rows_ok = [r for r in run_rows_all if int(r.get("failed_nodes", 0)) == 0]
    run_rows = run_rows_ok if run_rows_ok else run_rows_all
    kept, filt = iqr_filter(run_rows, "global_elapsed_ms", mult=args.iqr_mult)

    summary = {
        "tag": tag,
        "generated_at": l1.utc_now(),
        "config_path": os.path.abspath(args.config),
        "shared_path": shared_path,
        "nodes": len(nodes),
        "repeats": args.repeats,
        "warmup": args.warmup,
        "iqr_mult": args.iqr_mult,
        "lock_mode": args.lock_mode,
        "lock_timeout_ms": args.lock_timeout_ms,
        "filter": filt,
        "run_rounds_total": len(run_rows_all),
        "run_rounds_no_failed_nodes": len(run_rows_ok),
        "aggregate": {
            "global_elapsed_ms": metric_stats([float(r["global_elapsed_ms"]) for r in kept]),
            "create_success_total": metric_stats([float(r["create_success_total"]) for r in kept]),
            "open_existing_total": metric_stats([float(r["open_existing_total"]) for r in kept]),
            "single_creator": metric_stats([float(r["single_creator"]) for r in kept]),
            "nodes_created": metric_stats([float(r["nodes_created"]) for r in kept]),
            "failed_nodes": metric_stats([float(r["failed_nodes"]) for r in kept]),
        },
        "runs": all_rounds,
        "kept_runs": kept,
        "node_records": all_node_records,
    }

    out_json = os.path.join(out_root, "l1_init_compete_summary.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    out_svg = os.path.join(out_root, "l1_init_compete_overview.svg")
    build_svg(out_svg, kept)

    latest = os.path.join(RESULTS_ROOT, "l1_init_compete_latest")
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
