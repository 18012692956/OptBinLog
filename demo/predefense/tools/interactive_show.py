#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from collections import Counter, deque
from typing import Any, Deque, Dict, List, Tuple


BOOT_STAGE_MAP = {
    0: "ROM",
    1: "BOOTLOADER",
    2: "KERNEL",
    3: "DRIVERS",
    4: "SERVICES",
    5: "APP_READY",
}

NET_STATE_MAP = {
    0: "DOWN",
    1: "SCANNING",
    2: "ASSOCIATING",
    3: "ONLINE",
}

ALERT_LEVEL_MAP = {
    1: "INFO",
    2: "WARN",
    3: "CRITICAL",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Interactive terminal dashboard for embedded optbinlog playback")
    p.add_argument("--jsonl", required=True, help="input jsonl decoded by optbinlog_read --format jsonl")
    p.add_argument("--mode", choices=["auto", "step", "off"], default="auto", help="auto: continuous, step: manual")
    p.add_argument("--speed", type=float, default=1.0, help="playback speed multiplier in auto mode")
    p.add_argument("--summary-out", default="", help="write summary json to this path")
    p.add_argument("--snapshot-out", default="", help="write stage snapshots (markdown) for slide backup")
    return p.parse_args()


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def bar(value: float, lo: float, hi: float, width: int = 22) -> str:
    if hi <= lo:
        return "[" + ("#" * width) + "]"
    ratio = clamp((value - lo) / (hi - lo), 0.0, 1.0)
    filled = int(round(ratio * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def colorize(s: str, code: str, enabled: bool) -> str:
    if not enabled:
        return s
    return f"\033[{code}m{s}\033[0m"


def fmt_uptime(ms: int) -> str:
    sec = ms // 1000
    rem = ms % 1000
    m = sec // 60
    s = sec % 60
    return f"{m:02d}:{s:02d}.{rem:03d}"


def load_events(path: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or not s.startswith("{"):
                continue
            try:
                events.append(json.loads(s))
            except json.JSONDecodeError:
                continue
    return events


def fields_map(event: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for field in event.get("fields", []):
        out[field.get("name", "unknown")] = field.get("value")
    return out


def phase_from_state(state: Dict[str, Any]) -> str:
    if state["boot_stage"] != "APP_READY":
        return "Phase 1/5: Boot"
    if state["net_state"] != "ONLINE":
        return "Phase 2/5: Network Bring-up"
    if state["alert_active"]:
        return "Phase 4/5: Fault Handling"
    if state["seen_alert"] and not state["alert_active"]:
        return "Phase 5/5: Recovery"
    return "Phase 3/5: Steady Runtime"


def brief_event(tag: str, f: Dict[str, Any]) -> str:
    at = fmt_uptime(int(f.get("uptime_ms", 0)))
    if tag == "boot_stage":
        stage = BOOT_STAGE_MAP.get(int(f.get("stage", 0)), "UNKNOWN")
        return f"{at} BOOT stage={stage} code={int(f.get('code', 0))}"
    if tag == "sensor_sample":
        return f"{at} SENSOR id={int(f.get('sensor_id', 0))} value={int(f.get('value_x100', 0))/100.0:.2f}"
    if tag == "control_loop":
        return f"{at} CTRL latency={int(f.get('latency_us', 0))}us pwm={int(f.get('pwm', 0))}"
    if tag == "net_state":
        ns = NET_STATE_MAP.get(int(f.get("state", 0)), "UNKNOWN")
        return f"{at} NET state={ns} rssi={int(f.get('rssi_dbm', 0))} retry={int(f.get('retry', 0))}"
    if tag == "power_state":
        return f"{at} PWR soc={int(f.get('soc', 0))}% i={int(f.get('current_ma', 0))}mA"
    if tag == "alert_event":
        lv = ALERT_LEVEL_MAP.get(int(f.get("level", 0)), "UNKNOWN")
        return f"{at} ALERT level={lv} fault={int(f.get('fault', 0))}"
    if tag == "note_event":
        return f"{at} NOTE {f.get('msg', '')}"
    return f"{at} {tag}"


def default_state() -> Dict[str, Any]:
    return {
        "uptime_ms": 0,
        "boot_stage": "ROM",
        "net_state": "DOWN",
        "soc": 100.0,
        "voltage_mv": 4100,
        "current_ma": 0,
        "rssi_dbm": 0,
        "latency_us": 0,
        "pwm": 0,
        "sensor_1": 0.0,
        "sensor_2": 0.0,
        "sensor_3": 0.0,
        "alert_count": 0,
        "alert_active": False,
        "seen_alert": False,
    }


def default_summary() -> Dict[str, Any]:
    return {
        "record_count": 0,
        "alert_count": 0,
        "note_count": 0,
        "max_latency_us": 0,
        "latency_sum": 0.0,
        "latency_count": 0,
        "min_soc": 100.0,
        "max_uptime_ms": 0,
        "phases": [],
    }


def update_state(state: Dict[str, Any], summary: Dict[str, Any], tag: str, f: Dict[str, Any]) -> None:
    uptime = int(f.get("uptime_ms", state["uptime_ms"]))
    state["uptime_ms"] = uptime
    summary["max_uptime_ms"] = max(summary["max_uptime_ms"], uptime)

    if tag == "boot_stage":
        state["boot_stage"] = BOOT_STAGE_MAP.get(int(f.get("stage", 0)), "UNKNOWN")
    elif tag == "sensor_sample":
        sid = int(f.get("sensor_id", 0))
        val = int(f.get("value_x100", 0)) / 100.0
        if sid == 1:
            state["sensor_1"] = val
        elif sid == 2:
            state["sensor_2"] = val
        elif sid == 3:
            state["sensor_3"] = val
    elif tag == "control_loop":
        latency = int(f.get("latency_us", 0))
        state["latency_us"] = latency
        state["pwm"] = int(f.get("pwm", 0))
        summary["max_latency_us"] = max(summary["max_latency_us"], latency)
        summary["latency_sum"] += latency
        summary["latency_count"] += 1
    elif tag == "net_state":
        state["net_state"] = NET_STATE_MAP.get(int(f.get("state", 0)), "UNKNOWN")
        state["rssi_dbm"] = int(f.get("rssi_dbm", 0))
    elif tag == "power_state":
        state["soc"] = float(int(f.get("soc", 0)))
        state["voltage_mv"] = int(f.get("voltage_mv", 0))
        state["current_ma"] = int(f.get("current_ma", 0))
        summary["min_soc"] = min(summary["min_soc"], state["soc"])
    elif tag == "alert_event":
        level = int(f.get("level", 0))
        state["alert_count"] += 1
        summary["alert_count"] += 1
        state["seen_alert"] = True
        state["alert_active"] = level >= 2
    elif tag == "note_event":
        summary["note_count"] += 1
        msg = str(f.get("msg", ""))
        if "RECOVER" in msg:
            state["alert_active"] = False


def render(
    state: Dict[str, Any],
    recent: Deque[str],
    counters: Counter,
    total: int,
    idx: int,
    color: bool,
) -> str:
    phase = phase_from_state(state)
    progress = (idx + 1) / max(total, 1)
    phase_colored = phase
    if "Fault" in phase:
        phase_colored = colorize(phase, "31", color)
    elif "Recovery" in phase:
        phase_colored = colorize(phase, "36", color)
    elif "Steady" in phase:
        phase_colored = colorize(phase, "32", color)

    lines: List[str] = []
    lines.append("OPTBINLOG PREDEFENSE SHOWCASE | Embedded Runtime Story")
    lines.append("=" * 86)
    lines.append(f"Progress {bar(progress, 0, 1, 42)} {idx + 1}/{total} | {phase_colored}")
    lines.append(
        f"Uptime {fmt_uptime(state['uptime_ms'])} | Boot={state['boot_stage']} | "
        f"Network={state['net_state']} | Alerts={state['alert_count']}"
    )
    lines.append("-" * 86)
    lines.append(
        f"Battery {bar(state['soc'], 0, 100)} {state['soc']:>5.1f}%   "
        f"Voltage {state['voltage_mv']:>4d} mV   Current {state['current_ma']:>4d} mA"
    )
    lines.append(
        f"Control {bar(state['latency_us'], 250, 1200)} {state['latency_us']:>4d} us   "
        f"PWM {state['pwm']:>3d}   RSSI {state['rssi_dbm']:>3d}"
    )
    lines.append(
        f"Sensors s1={state['sensor_1']:>5.2f}  s2={state['sensor_2']:>5.2f}  s3={state['sensor_3']:>5.2f}"
    )
    lines.append("-" * 86)
    lines.append("Timeline (latest 10):")
    for item in recent:
        lines.append(f"  {item}")
    lines.append("-" * 86)
    lines.append(
        "Counters: "
        f"boot={counters['boot_stage']} "
        f"sensor={counters['sensor_sample']} "
        f"control={counters['control_loop']} "
        f"net={counters['net_state']} "
        f"power={counters['power_state']} "
        f"alert={counters['alert_event']} "
        f"note={counters['note_event']}"
    )
    return "\n".join(lines)


def write_snapshots(path: str, frames: List[Tuple[str, str]]) -> None:
    lines: List[str] = ["# Playback Snapshots", ""]
    for title, content in frames:
        lines.append(f"## {title}")
        lines.append("")
        lines.append("```text")
        lines.append(content)
        lines.append("```")
        lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> int:
    args = parse_args()
    events = load_events(args.jsonl)
    if not events:
        print(f"no events parsed from {args.jsonl}", file=sys.stderr)
        return 1

    state = default_state()
    summary = default_summary()
    counters: Counter = Counter()
    recent: Deque[str] = deque(maxlen=10)
    use_color = sys.stdout.isatty() and os.environ.get("TERM", "") != "dumb"
    snapshots: List[Tuple[str, str]] = []
    phase_last = ""

    if args.mode != "off":
        print("Controls: auto mode runs continuously; step mode accepts Enter=next, j<number>=jump, q=quit.")
        if args.mode == "auto":
            time.sleep(0.7)

    idx = 0
    while idx < len(events):
        event = events[idx]
        tag = event.get("tag", "unknown")
        f = fields_map(event)
        counters[tag] += 1
        summary["record_count"] += 1
        update_state(state, summary, tag, f)
        recent.append(brief_event(tag, f))
        phase = phase_from_state(state)

        frame = render(state, recent, counters, len(events), idx, use_color)
        if args.mode != "off":
            if use_color:
                sys.stdout.write("\033[2J\033[H")
            sys.stdout.write(frame + "\n")
            sys.stdout.flush()

        # Capture one snapshot per phase to support slide backup.
        if phase != phase_last:
            snapshots.append((phase, frame))
            summary["phases"].append({"phase": phase, "uptime_ms": state["uptime_ms"], "index": idx + 1})
            phase_last = phase

        if args.mode == "step":
            cmd = input("[step] Enter=next, j<number>=jump, q=quit > ").strip().lower()
            if cmd == "q":
                break
            if cmd.startswith("j"):
                try:
                    target = int(cmd[1:].strip())
                    target = max(1, min(target, len(events)))
                    idx = target - 1
                    continue
                except ValueError:
                    pass
        elif args.mode == "auto":
            speed = args.speed if args.speed > 0 else 1.0
            time.sleep(0.085 / speed)

        idx += 1

    avg_latency = (
        float(summary["latency_sum"]) / float(summary["latency_count"])
        if summary["latency_count"] > 0
        else 0.0
    )
    summary["avg_latency_us"] = avg_latency
    summary["min_soc"] = float(summary["min_soc"])

    print("")
    print("Playback Summary")
    print("-" * 86)
    print(f"records      : {summary['record_count']}")
    print(f"alerts       : {summary['alert_count']}")
    print(f"notes        : {summary['note_count']}")
    print(f"max latency  : {summary['max_latency_us']} us")
    print(f"avg latency  : {summary['avg_latency_us']:.1f} us")
    print(f"min battery  : {summary['min_soc']:.1f}%")
    print(f"max uptime   : {fmt_uptime(int(summary['max_uptime_ms']))}")
    print(f"source jsonl : {os.path.abspath(args.jsonl)}")

    if args.summary_out:
        with open(args.summary_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"summary json : {os.path.abspath(args.summary_out)}")

    if args.snapshot_out:
        write_snapshots(args.snapshot_out, snapshots)
        print(f"snapshots md : {os.path.abspath(args.snapshot_out)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
