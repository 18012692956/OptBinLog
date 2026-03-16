import json
import math
import os
import statistics
import subprocess
import time

ROOT = os.path.dirname(__file__)
EVENTLOG_DIR = os.environ.get("OPTBINLOG_EVENTLOG_DIR", os.path.join(ROOT, "eventlogst"))
OUT_DIR = os.environ.get("OPTBINLOG_INIT_OUT_DIR", os.path.join(ROOT, "init_race"))
RUN_DIR = os.path.join(OUT_DIR, "runs")
SHARED = os.path.join(OUT_DIR, "shared_eventtag.bin")
PROCS = int(os.environ.get("OPTBINLOG_INIT_PROCS", "10"))
REPEATS = int(os.environ.get("OPTBINLOG_INIT_REPEATS", "20"))
WARMUP = int(os.environ.get("OPTBINLOG_INIT_WARMUP", "3"))
IQR_MULT = float(os.environ.get("OPTBINLOG_INIT_IQR_MULT", "1.5"))
CMD_RETRIES = int(os.environ.get("OPTBINLOG_INIT_CMD_RETRIES", "5"))

os.makedirs(RUN_DIR, exist_ok=True)
race = os.environ.get("OPTBINLOG_INIT_BIN", os.path.join(ROOT, "optbinlog_init_race"))


def percentile(values, p):
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    x = sorted(values)
    idx = (len(x) - 1) * (p / 100.0)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return float(x[lo])
    ratio = idx - lo
    return float(x[lo] + (x[hi] - x[lo]) * ratio)


def metric_stats(values):
    if not values:
        return {
            "n": 0,
            "mean": 0.0,
            "median": 0.0,
            "p95": 0.0,
            "std": 0.0,
            "ci95_low": 0.0,
            "ci95_high": 0.0,
            "min": 0.0,
            "max": 0.0,
        }
    n = len(values)
    mean = statistics.fmean(values)
    std = statistics.stdev(values) if n > 1 else 0.0
    half_ci = 1.96 * std / math.sqrt(n) if n > 1 else 0.0
    return {
        "n": n,
        "mean": mean,
        "median": statistics.median(values),
        "p95": percentile(values, 95),
        "std": std,
        "ci95_low": mean - half_ci,
        "ci95_high": mean + half_ci,
        "min": min(values),
        "max": max(values),
    }


def iqr_filter(rows, field):
    values = [r[field] for r in rows]
    if len(values) < 4:
        return rows, {"field": field, "method": "iqr", "kept": len(rows), "removed": 0}
    q1 = percentile(values, 25)
    q3 = percentile(values, 75)
    iqr = q3 - q1
    lo = q1 - IQR_MULT * iqr
    hi = q3 + IQR_MULT * iqr
    kept = [r for r in rows if lo <= r[field] <= hi]
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


def parse_trace(trace_path):
    records = []
    with open(trace_path, "rb") as f:
        for raw in f:
            line = raw.decode("utf-8", errors="ignore").replace("\x00", "").strip()
            if not line:
                continue
            parts = line.split(" ", 2)
            if len(parts) < 3:
                continue
            try:
                ts_ns = int(parts[0])
            except ValueError:
                continue
            try:
                pid = int(parts[1].split("=")[1])
            except (ValueError, IndexError):
                continue
            evt = parts[2]
            records.append({"ts_ns": ts_ns, "pid": pid, "event": evt})
    records.sort(key=lambda x: (x["pid"], x["ts_ns"]))
    return records


def analyze_records(records):
    by_pid = {}
    for r in records:
        by_pid.setdefault(r["pid"], []).append(r)

    for pid in by_pid:
        by_pid[pid].sort(key=lambda x: x["ts_ns"])

    create_success = sum(1 for r in records if r["event"] == "create_success")
    init_done = sum(1 for r in records if r["event"] == "init_done")
    open_ok = sum(1 for r in records if r["event"] == "open_existing_ok")
    wait_events = sum(1 for r in records if r["event"] == "wait_initializing")

    wait_durations_ms = []
    per_pid = []
    for pid, evs in sorted(by_pid.items()):
        waits = 0
        for i, ev in enumerate(evs):
            if ev["event"] != "wait_initializing":
                continue
            waits += 1
            if i + 1 < len(evs):
                dt_ns = max(0, evs[i + 1]["ts_ns"] - ev["ts_ns"])
                wait_durations_ms.append(dt_ns / 1e6)

        start_ns = evs[0]["ts_ns"]
        end_ns = evs[-1]["ts_ns"]
        per_pid.append(
            {
                "pid": pid,
                "event_count": len(evs),
                "wait_count": waits,
                "lifecycle_ms": (end_ns - start_ns) / 1e6,
            }
        )

    if records:
        ts_min = min(r["ts_ns"] for r in records)
        ts_max = max(r["ts_ns"] for r in records)
        elapsed_ms = (ts_max - ts_min) / 1e6
    else:
        elapsed_ms = 0.0

    wait_total_ms = sum(wait_durations_ms)
    wait_p95_ms = percentile(wait_durations_ms, 95) if wait_durations_ms else 0.0

    return {
        "procs": PROCS,
        "create_success": create_success,
        "init_done": init_done,
        "open_existing_ok": open_ok,
        "wait_events": wait_events,
        "waiters": sum(1 for x in per_pid if x["wait_count"] > 0),
        "wait_total_ms": wait_total_ms,
        "wait_p95_ms": wait_p95_ms,
        "wait_max_ms": max(wait_durations_ms) if wait_durations_ms else 0.0,
        "elapsed_ms": elapsed_ms,
        "per_pid": per_pid,
    }


def run_once(idx, warmup=False):
    kind = "warmup" if warmup else "run"
    trace_path = os.path.join(RUN_DIR, f"trace_{kind}_{idx:03d}.log")
    cmd = [
        race,
        "--eventlog-dir",
        EVENTLOG_DIR,
        "--shared",
        SHARED,
        "--trace",
        trace_path,
        "--procs",
        str(PROCS),
        "--clean",
    ]
    last = None
    for attempt in range(CMD_RETRIES):
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            last = None
            break
        last = proc
        retryable = "File exists" in (proc.stderr or "")
        if retryable and attempt + 1 < CMD_RETRIES:
            time.sleep(0.05 * (attempt + 1))
            continue
        raise subprocess.CalledProcessError(proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr)
    if last is not None:
        raise subprocess.CalledProcessError(last.returncode, cmd, output=last.stdout, stderr=last.stderr)
    events = parse_trace(trace_path)
    summary = analyze_records(events)
    return {
        "iteration": idx,
        "trace": trace_path,
        "events": events,
        "summary": summary,
        "elapsed_ms": summary["elapsed_ms"],
        "wait_events": summary["wait_events"],
        "wait_total_ms": summary["wait_total_ms"],
        "wait_p95_ms": summary["wait_p95_ms"],
    }


def build_timeline_svg(events, out_path):
    if not events:
        return

    by_pid = {}
    for r in events:
        by_pid.setdefault(r["pid"], []).append(r)
    for pid in by_pid:
        by_pid[pid].sort(key=lambda x: x["ts_ns"])

    pids = sorted(by_pid.keys())
    min_ts = min(r["ts_ns"] for r in events)
    max_ts = max(r["ts_ns"] for r in events)
    span = max(1, max_ts - min_ts)

    width = 980
    row_h = 20
    margin = 60
    height = margin * 2 + row_h * len(pids)

    svg = []
    svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    svg.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    svg.append(f'<text x="{width/2}" y="30" text-anchor="middle" font-family="Arial" font-size="16">shared init race ({PROCS} procs)</text>')

    svg.append('<rect x="60" y="40" width="12" height="12" fill="#2b8cbe"/>')
    svg.append('<text x="78" y="50" font-family="Arial" font-size="12">init window</text>')
    svg.append('<circle cx="200" cy="46" r="4" fill="#31a354"/>')
    svg.append('<text x="210" y="50" font-family="Arial" font-size="12">create_success</text>')
    svg.append('<circle cx="340" cy="46" r="4" fill="#756bb1"/>')
    svg.append('<text x="350" y="50" font-family="Arial" font-size="12">open_existing_ok</text>')
    svg.append('<circle cx="500" cy="46" r="4" fill="#de2d26"/>')
    svg.append('<text x="510" y="50" font-family="Arial" font-size="12">wait_initializing</text>')
    svg.append('<circle cx="660" cy="46" r="4" fill="#636363"/>')
    svg.append('<text x="670" y="50" font-family="Arial" font-size="12">other</text>')

    for i, pid in enumerate(pids):
        y = margin + i * row_h
        evs = by_pid[pid]
        start = evs[0]["ts_ns"]
        end = evs[-1]["ts_ns"]
        x1 = 60 + int((start - min_ts) / span * (width - 120))
        x2 = 60 + int((end - min_ts) / span * (width - 120))
        svg.append(f'<rect x="{x1}" y="{y-8}" width="{max(2, x2-x1)}" height="8" fill="#2b8cbe" opacity="0.6"/>')
        svg.append(f'<text x="10" y="{y-2}" font-family="Arial" font-size="10">pid {pid}</text>')

        for ev in evs:
            x = 60 + int((ev["ts_ns"] - min_ts) / span * (width - 120))
            if ev["event"] == "create_success":
                color = "#31a354"
            elif ev["event"] == "open_existing_ok":
                color = "#756bb1"
            elif ev["event"] == "wait_initializing":
                color = "#de2d26"
            else:
                color = "#636363"
            svg.append(f'<circle cx="{x}" cy="{y-4}" r="3" fill="{color}"/>')

    svg.append('</svg>')

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(svg))


for i in range(WARMUP):
    run_once(i, warmup=True)

runs = []
for i in range(REPEATS):
    runs.append(run_once(i, warmup=False))

filtered_runs, filter_info = iqr_filter(runs, "elapsed_ms")

agg = {
    "filter": filter_info,
    "elapsed_ms": metric_stats([r["elapsed_ms"] for r in filtered_runs]),
    "wait_events": metric_stats([float(r["wait_events"]) for r in filtered_runs]),
    "wait_total_ms": metric_stats([r["wait_total_ms"] for r in filtered_runs]),
    "wait_p95_ms": metric_stats([r["wait_p95_ms"] for r in filtered_runs]),
    "create_success": metric_stats([float(r["summary"]["create_success"]) for r in filtered_runs]),
    "open_existing_ok": metric_stats([float(r["summary"]["open_existing_ok"]) for r in filtered_runs]),
    "init_done": metric_stats([float(r["summary"]["init_done"]) for r in filtered_runs]),
    "kept_iterations": [r["iteration"] for r in filtered_runs],
}

representative = None
if filtered_runs:
    rep_sorted = sorted(filtered_runs, key=lambda x: x["elapsed_ms"])
    representative = rep_sorted[len(rep_sorted) // 2]

json_path = os.path.join(OUT_DIR, "init_race_result.json")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(
        {
            "config": {
                "procs": PROCS,
                "repeats": REPEATS,
                "warmup": WARMUP,
                "iqr_mult": IQR_MULT,
            },
            "aggregate": agg,
            "runs": runs,
            "representative_iteration": representative["iteration"] if representative else None,
        },
        f,
        indent=2,
    )

print("saved", json_path)
print(
    "summary: "
    f"elapsed_mean={agg['elapsed_ms']['mean']:.3f}ms "
    f"elapsed_p95={agg['elapsed_ms']['p95']:.3f}ms "
    f"wait_events_mean={agg['wait_events']['mean']:.2f} "
    f"wait_total_mean={agg['wait_total_ms']['mean']:.3f}ms"
)

if representative:
    svg_path = os.path.join(OUT_DIR, "init_race_result.svg")
    build_timeline_svg(representative["events"], svg_path)
    print("saved", svg_path)
