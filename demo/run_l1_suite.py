#!/usr/bin/env python3
import argparse
import concurrent.futures
import datetime as dt
import io
import json
import os
import shlex
import shutil
import subprocess
import tarfile
import time
from typing import Any, Dict, List, Optional, Tuple


ROOT = os.path.dirname(__file__)
RESULTS_ROOT = os.path.join(ROOT, "results")


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def run_subprocess(
    cmd: List[str],
    *,
    capture: bool = True,
    text: bool = True,
    check: bool = False,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=text,
    )


def quote_cmd(parts: List[str]) -> str:
    return " ".join(shlex.quote(x) for x in parts)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="L1 distributed benchmark suite (multi-node + netem)")
    p.add_argument("--config", required=True, help="Path to L1 config JSON")
    p.add_argument("--tag", default="", help="Override run tag")
    p.add_argument("--keep-remote-out", action="store_true", help="Do not remove remote output after pulling")
    p.add_argument("--no-parallel", action="store_true", help="Run nodes sequentially")
    return p.parse_args()


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def node_defaults(node: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(node)
    out.setdefault("transport", "local")
    out.setdefault("workdir", ROOT)
    out.setdefault("shell", "bash")
    out.setdefault("python", "python3")
    out.setdefault("bench_script", "run_bench.py")
    out.setdefault("bench_bin", "optbinlog_bench")
    out.setdefault("eventlog_dir", "eventlogst")
    out.setdefault("records", 80000)
    out.setdefault("repeats", 5)
    out.setdefault("warmup", 1)
    out.setdefault("modes", "text,binary,syslog")
    out.setdefault("baseline", "text")
    out.setdefault("sudo_prefix", "sudo -n")
    out.setdefault("bench_prefix", "")
    out.setdefault("strict_perm", False)
    return out


class NodeExecutor:
    def __init__(self, node: Dict[str, Any]) -> None:
        self.node = node_defaults(node)
        self.name = self.node["name"]
        self.transport = self.node["transport"]
        self.shell = self.node["shell"]
        self.workdir = self.node["workdir"]

    def _build_outer_cmd(self, inner_cmd: str) -> List[str]:
        transport = self.transport
        if transport == "local":
            return [self.shell, "-lc", inner_cmd]
        if transport == "ssh":
            target = self.node.get("ssh_target")
            if not target:
                raise RuntimeError(f"node {self.name}: ssh transport requires ssh_target")
            remote_cmd = f"{self.shell} -lc {shlex.quote(inner_cmd)}"
            return ["ssh", "-o", "BatchMode=yes", target, remote_cmd]
        if transport == "prefix":
            prefix = self.node.get("prefix")
            if not isinstance(prefix, list) or not prefix:
                raise RuntimeError(f"node {self.name}: prefix transport requires prefix list")
            if str(prefix[0]) == "ssh":
                remote_cmd = f"{self.shell} -lc {shlex.quote(inner_cmd)}"
                return [str(x) for x in prefix] + [remote_cmd]
            return [str(x) for x in prefix] + [self.shell, "-lc", inner_cmd]
        raise RuntimeError(f"node {self.name}: unknown transport {transport}")

    def run(self, inner_cmd: str, *, check: bool = True, text: bool = True) -> subprocess.CompletedProcess:
        cmd = self._build_outer_cmd(inner_cmd)
        proc = None
        is_lima_prefix = (
            self.transport == "prefix"
            and isinstance(self.node.get("prefix"), list)
            and len(self.node.get("prefix")) > 0
            and str(self.node.get("prefix")[0]) == "limactl"
        )
        max_retry = 8 if is_lima_prefix else 0
        probe_cmd = None
        if is_lima_prefix:
            prefix = [str(x) for x in self.node.get("prefix", [])]
            probe_cmd = prefix + [self.shell, "-lc", "true"]
        for attempt in range(max_retry + 1):
            proc = run_subprocess(cmd, capture=True, text=text, check=False)
            if proc.returncode == 0:
                break
            stderr = proc.stderr or ""
            # limactl occasionally reports "Bad port '0'" under short bursts.
            retryable = is_lima_prefix and "Bad port '0'" in stderr
            if not retryable or attempt >= max_retry:
                break
            if probe_cmd is not None:
                run_subprocess(probe_cmd, capture=True, text=text, check=False)
            time.sleep(1.2 * (attempt + 1))
        assert proc is not None
        if check and proc.returncode != 0:
            raise RuntimeError(
                "node command failed\n"
                f"node={self.name}\n"
                f"cmd={quote_cmd(cmd)}\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}\n"
            )
        return proc

    def run_in_workdir(self, cmd: str, *, check: bool = True) -> subprocess.CompletedProcess:
        inner = f"cd {shlex.quote(self.workdir)}; {cmd}"
        return self.run(inner, check=check, text=True)

    def pull_dir(self, remote_dir: str, local_dir: str) -> None:
        ensure_dir(local_dir)
        if self.transport == "local":
            if os.path.exists(local_dir):
                shutil.rmtree(local_dir)
            shutil.copytree(remote_dir, local_dir)
            return

        # For remote transports, stream tar.
        inner = f"cd {shlex.quote(remote_dir)}; tar -cf - ."
        cmd = self._build_outer_cmd(inner)
        proc = run_subprocess(cmd, capture=True, text=False, check=False)
        stderr = (proc.stderr or b"").decode("utf-8", errors="replace")
        recoverable_warn = "file changed as we read it" in stderr.lower()
        if proc.returncode != 0 and not (proc.returncode == 1 and recoverable_warn):
            raise RuntimeError(
                "pull_dir failed\n"
                f"node={self.name}\n"
                f"cmd={quote_cmd(cmd)}\n"
                f"stderr:\n{stderr}\n"
            )
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)
        ensure_dir(local_dir)
        bio = io.BytesIO(proc.stdout or b"")
        with tarfile.open(fileobj=bio, mode="r:") as tf:
            tf.extractall(local_dir)


def build_netem_cmd(netem: Dict[str, Any]) -> str:
    iface = str(netem.get("iface", "")).strip()
    if not iface:
        raise RuntimeError("netem.iface is required")
    parts = ["tc", "qdisc", "replace", "dev", iface, "root", "netem"]
    if netem.get("delay_ms") is not None:
        delay_ms = float(netem["delay_ms"])
        parts += ["delay", f"{delay_ms}ms"]
        if netem.get("jitter_ms") is not None:
            parts += [f"{float(netem['jitter_ms'])}ms", "distribution", "normal"]
    if netem.get("loss_pct") is not None:
        parts += ["loss", f"{float(netem['loss_pct'])}%"]
    if netem.get("rate_mbit") is not None:
        parts += ["rate", f"{float(netem['rate_mbit'])}mbit"]
    if netem.get("limit") is not None:
        parts += ["limit", str(int(netem["limit"]))]
    return quote_cmd(parts)


def netem_apply(node: NodeExecutor) -> Optional[Dict[str, Any]]:
    netem = node.node.get("netem")
    if not isinstance(netem, dict):
        return None
    cmd = build_netem_cmd(netem)
    sudo_prefix = str(node.node.get("sudo_prefix", "sudo -n")).strip()
    run_cmd = f"{sudo_prefix} {cmd}".strip()
    node.run_in_workdir(run_cmd, check=True)
    return {"applied": True, "cmd": run_cmd, "config": netem}


def netem_clear(node: NodeExecutor) -> None:
    netem = node.node.get("netem")
    if not isinstance(netem, dict):
        return
    iface = str(netem.get("iface", "")).strip()
    if not iface:
        return
    sudo_prefix = str(node.node.get("sudo_prefix", "sudo -n")).strip()
    clear_cmd = f"{sudo_prefix} tc qdisc del dev {shlex.quote(iface)} root || true"
    node.run_in_workdir(clear_cmd, check=False)


def build_bench_env(node: NodeExecutor, remote_out: str) -> Dict[str, str]:
    cfg = node.node
    env = {
        "OPTBINLOG_BENCH_OUT_DIR": remote_out,
        "OPTBINLOG_BENCH_BIN": cfg["bench_bin"],
        "OPTBINLOG_EVENTLOG_DIR": str(cfg.get("eventlog_dir", "eventlogst")),
        "OPTBINLOG_BENCH_RECORDS": str(cfg["records"]),
        "OPTBINLOG_BENCH_REPEATS": str(cfg["repeats"]),
        "OPTBINLOG_BENCH_WARMUP": str(cfg["warmup"]),
        "OPTBINLOG_BENCH_MODES": str(cfg["modes"]),
        "OPTBINLOG_BENCH_BASELINE": str(cfg["baseline"]),
        "OPTBINLOG_MULTI_BENCH": "0",
    }
    if cfg.get("strict_perm"):
        env["OPTBINLOG_STRICT_PERM"] = "1"
    if cfg.get("trace_marker"):
        env["OPTBINLOG_TRACE_MARKER"] = str(cfg["trace_marker"])
    if cfg.get("syslog_source"):
        env["OPTBINLOG_SYSLOG_SOURCE"] = str(cfg["syslog_source"])
    if cfg.get("text_profile"):
        env["OPTBINLOG_TEXT_PROFILE"] = str(cfg["text_profile"])
    if cfg.get("shared_tag_path"):
        env["OPTBINLOG_SHARED_TAG_PATH"] = str(cfg["shared_tag_path"])
    if cfg.get("native_align_required"):
        env["OPTBINLOG_NATIVE_ALIGN_REQUIRED"] = "1"
    return env


def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def summarize_node_bench(bench_json: Dict[str, Any]) -> Dict[str, Any]:
    cfg = bench_json.get("config", {})
    summary = bench_json.get("summary", {})
    baseline = cfg.get("baseline_mode")
    compare = bench_json.get("comparison", {}).get("by_mode", {})
    modes = cfg.get("active_modes", [])
    out = {
        "baseline": baseline,
        "modes": modes,
        "by_mode": {},
        "improvements": compare,
    }
    for m in modes:
        s = summary.get(m, {})
        out["by_mode"][m] = {
            "end_to_end_ms_mean": s.get("end_to_end_ms", {}).get("mean"),
            "bytes_mean": s.get("bytes", {}).get("mean"),
            "shared_bytes_mean": s.get("shared_bytes", {}).get("mean"),
            "total_bytes_mean": s.get("total_bytes", {}).get("mean"),
            "throughput_e2e_rps_mean": s.get("throughput_e2e_rps", {}).get("mean"),
        }
    return out


def collect_platform_meta(node: NodeExecutor) -> Dict[str, Any]:
    # Reuse collect_platform_info() on each node.
    snippet = (
        "python3 - <<'PY'\n"
        "import json, run_platform_suite as r\n"
        "print(json.dumps(r.collect_platform_info('.'), ensure_ascii=False))\n"
        "PY"
    )
    proc = node.run_in_workdir(snippet, check=True)
    line = ""
    for ln in (proc.stdout or "").splitlines():
        ln = ln.strip()
        if ln.startswith("{") and ln.endswith("}"):
            line = ln
    if not line:
        return {"error": "platform meta parse failed", "stdout": proc.stdout, "stderr": proc.stderr}
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return {"error": "platform meta invalid json", "line": line}


def bench_uses_sudo(node: NodeExecutor) -> bool:
    bench_prefix = str(node.node.get("bench_prefix", "")).strip()
    return bench_prefix.startswith("sudo ")


def remote_out_prepare_cmd(node: NodeExecutor, remote_out: str) -> str:
    q = shlex.quote(remote_out)
    if bench_uses_sudo(node):
        sudo_prefix = str(node.node.get("sudo_prefix", "sudo -n")).strip()
        return f"{sudo_prefix} mkdir -p {q}"
    return f"mkdir -p {q}"


def remote_out_cleanup_cmd(node: NodeExecutor, remote_out: str) -> str:
    q = shlex.quote(remote_out)
    if bench_uses_sudo(node):
        sudo_prefix = str(node.node.get("sudo_prefix", "sudo -n")).strip()
        return f"{sudo_prefix} rm -rf {q} || true"
    return f"rm -rf {q} || true"


def build_start_gate_cmd(node: NodeExecutor, start_at_epoch: Optional[float]) -> str:
    if start_at_epoch is None:
        return ""
    gate_py = f"import time; t={start_at_epoch:.6f}; n=time.time(); d=t-n; time.sleep(d if d>0 else 0.0)"
    py = shlex.quote(str(node.node["python"]))
    return f"{py} -c {shlex.quote(gate_py)}"


def run_one_node(
    raw_node: Dict[str, Any],
    *,
    index: int,
    out_root: str,
    tag: str,
    keep_remote_out: bool,
    start_at_epoch: Optional[float],
) -> Tuple[int, Dict[str, Any]]:
    node = NodeExecutor(raw_node)
    node_rec: Dict[str, Any] = {
        "name": node.name,
        "transport": node.transport,
        "status": "running",
        "started_at": utc_now(),
    }
    if start_at_epoch is not None:
        node_rec["scheduled_start_at_epoch"] = start_at_epoch

    local_node_out = os.path.join(out_root, "nodes", node.name)
    ensure_dir(local_node_out)
    remote_out = node.node.get("remote_out_dir") or os.path.join(node.workdir, "bench_l1", tag, node.name)
    node_rec["remote_out_dir"] = remote_out

    try:
        build_cmd = raw_node.get("build_cmd")
        if isinstance(build_cmd, str) and build_cmd.strip():
            node.run_in_workdir(build_cmd, check=True)

        node.run_in_workdir(remote_out_cleanup_cmd(node, remote_out), check=False)
        node.run_in_workdir(remote_out_prepare_cmd(node, remote_out), check=True)
        shared_tag_path = node.node.get("shared_tag_path")
        if isinstance(shared_tag_path, str) and shared_tag_path.strip():
            shared_parent = os.path.dirname(shared_tag_path)
            if shared_parent:
                node.run_in_workdir(f"mkdir -p {shlex.quote(shared_parent)}", check=True)

        applied = netem_apply(node)
        if applied:
            node_rec["netem"] = applied

        bench_env = build_bench_env(node, remote_out)
        env_inline = " ".join(f"{k}={shlex.quote(v)}" for k, v in bench_env.items())
        bench_script = str(node.node["bench_script"])
        bench_prefix = str(node.node.get("bench_prefix", "")).strip()
        py_cmd = f"{shlex.quote(str(node.node['python']))} {shlex.quote(bench_script)}"
        if bench_prefix:
            run_cmd_main = f"{bench_prefix} env {env_inline} {py_cmd}"
        else:
            run_cmd_main = f"export {env_inline}; {py_cmd}"

        gate_cmd = build_start_gate_cmd(node, start_at_epoch)
        if gate_cmd:
            run_cmd = f"{gate_cmd}; {run_cmd_main}"
        else:
            run_cmd = run_cmd_main

        proc = node.run_in_workdir(run_cmd, check=True)
        with open(os.path.join(local_node_out, "runner.stdout.log"), "w", encoding="utf-8") as f:
            f.write(proc.stdout or "")
        with open(os.path.join(local_node_out, "runner.stderr.log"), "w", encoding="utf-8") as f:
            f.write(proc.stderr or "")

        pulled_out = os.path.join(local_node_out, "bench_out")
        node.pull_dir(remote_out, pulled_out)
        bench_json_path = os.path.join(pulled_out, "bench_result.json")
        if not os.path.exists(bench_json_path):
            raise RuntimeError(f"missing bench_result.json for node {node.name}")

        bench_json = read_json(bench_json_path)
        node_rec["summary"] = summarize_node_bench(bench_json)
        try:
            node_rec["platform_meta"] = collect_platform_meta(node)
        except Exception as meta_err:
            node_rec["platform_meta"] = {"error": str(meta_err)}
            node_rec["platform_meta_warning"] = "platform meta collection skipped"
        node_rec["status"] = "ok"
    except Exception as e:
        node_rec["status"] = "failed"
        node_rec["error"] = str(e)
    finally:
        try:
            netem_clear(node)
        except Exception:
            pass
        if not keep_remote_out:
            try:
                node.run_in_workdir(remote_out_cleanup_cmd(node, remote_out), check=False)
            except Exception:
                pass
        node_rec["finished_at"] = utc_now()

    return index, node_rec


def write_markdown_report(path: str, tag: str, run_nodes: List[Dict[str, Any]]) -> None:
    lines: List[str] = []
    lines.append(f"# L1 Distributed Report ({tag})")
    lines.append("")
    lines.append("## Nodes")
    lines.append("")
    for rn in run_nodes:
        lines.append(f"- {rn['name']}: status={rn['status']}, transport={rn.get('transport')}")
        if rn.get("netem"):
            n = rn["netem"].get("config", {})
            lines.append(
                f"  netem: iface={n.get('iface')} delay={n.get('delay_ms')}ms jitter={n.get('jitter_ms')}ms loss={n.get('loss_pct')}% rate={n.get('rate_mbit')}mbit"
            )
    lines.append("")
    lines.append("## Per-Node Summary")
    lines.append("")
    for rn in run_nodes:
        if rn.get("status") != "ok":
            continue
        s = rn.get("summary", {})
        lines.append(f"### {rn['name']}")
        lines.append("")
        lines.append(f"- baseline: `{s.get('baseline')}`")
        for mode, row in s.get("by_mode", {}).items():
            lines.append(
                f"- {mode}: e2e={row.get('end_to_end_ms_mean')}, bytes={row.get('total_bytes_mean')}, thr={row.get('throughput_e2e_rps_mean')}"
            )
        for mode, imp in s.get("improvements", {}).items():
            lines.append(
                f"- vs baseline {mode}: e2e={imp.get('end_to_end_improve_pct'):.2f}% size={imp.get('size_save_pct'):.2f}% thr={imp.get('throughput_e2e_gain_pct'):.2f}%"
            )
        lines.append("")

    lines.append("## Visualizations")
    lines.append("")
    lines.append("- Node raw benchmark charts: `results/<tag>/nodes/<node_name>/bench_out/bench_result.svg`")
    lines.append("- Node distribution charts: `results/<tag>/nodes/<node_name>/bench_out/bench_stats.svg`")
    lines.append("- Node IQR charts: `results/<tag>/nodes/<node_name>/bench_out/bench_iqr.svg`")
    lines.append("- L1 merged overview: `results/<tag>/l1_overview.svg`")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def write_l1_overview_svg(path: str, run_nodes: List[Dict[str, Any]]) -> None:
    rows: List[Dict[str, Any]] = []
    for rn in run_nodes:
        if rn.get("status") != "ok":
            continue
        node_name = rn.get("name", "node")
        summ = rn.get("summary", {})
        by_mode = summ.get("by_mode", {})
        for mode, item in by_mode.items():
            rows.append(
                {
                    "label": f"{node_name}/{mode}",
                    "e2e_ms": float(item.get("end_to_end_ms_mean") or 0.0),
                    "bytes": float(item.get("total_bytes_mean") or 0.0),
                }
            )

    if not rows:
        return

    width = 1800
    height = 760
    panel_gap = 60
    panel_w = (width - 160 - panel_gap) / 2.0
    panel_h = 470
    panel_y = 120
    panel_x0 = 80

    palette = ["#1f77b4", "#2ca02c", "#d62728", "#ff7f0e", "#9467bd", "#8c564b", "#17becf", "#7f7f7f"]
    bar_gap = 10

    def draw_panel(metric_key: str, title: str, unit: str, x0: float) -> List[str]:
        vals = [r[metric_key] for r in rows]
        vmax = max(vals) if vals else 1.0
        if vmax <= 0:
            vmax = 1.0
        lines_local: List[str] = []
        x1 = x0 + panel_w
        lines_local.append(f'<rect x="{x0}" y="{panel_y}" width="{panel_w}" height="{panel_h}" fill="#ffffff" stroke="#d9d9d9"/>')
        lines_local.append(f'<text x="{x0 + panel_w/2}" y="{panel_y - 16}" text-anchor="middle" font-family="Arial" font-size="16">{title}</text>')
        for i in range(6):
            frac = i / 5.0
            y = panel_y + panel_h * (1.0 - frac)
            v = vmax * frac
            lines_local.append(f'<line x1="{x0}" y1="{y}" x2="{x1}" y2="{y}" stroke="#f1f1f1"/>')
            lines_local.append(f'<text x="{x0 - 8}" y="{y + 4}" text-anchor="end" font-family="Arial" font-size="11">{v:.0f}</text>')
        lines_local.append(
            f'<text x="{x0 - 40}" y="{panel_y + panel_h/2}" transform="rotate(-90,{x0 - 40},{panel_y + panel_h/2})" text-anchor="middle" font-family="Arial" font-size="12">{unit}</text>'
        )

        bars = len(rows)
        avail_w = panel_w - 30
        bw = max(16.0, (avail_w - (bars - 1) * bar_gap) / max(1, bars))
        bw = min(bw, 80.0)
        used_w = bars * bw + (bars - 1) * bar_gap
        start_x = x0 + (panel_w - used_w) / 2.0

        for i, row in enumerate(rows):
            v = row[metric_key]
            h = (v / vmax) * (panel_h - 18)
            bx = start_x + i * (bw + bar_gap)
            by = panel_y + panel_h - h
            color = palette[i % len(palette)]
            lines_local.append(f'<rect x="{bx}" y="{by}" width="{bw}" height="{h}" fill="{color}" opacity="0.86"/>')
            lines_local.append(f'<text x="{bx + bw/2}" y="{by - 6}" text-anchor="middle" font-family="Arial" font-size="10">{v:.1f}</text>')
            lines_local.append(f'<text x="{bx + bw/2}" y="{panel_y + panel_h + 14}" text-anchor="middle" font-family="Arial" font-size="10" transform="rotate(30,{bx + bw/2},{panel_y + panel_h + 14})">{row["label"]}</text>')
        return lines_local

    svg: List[str] = []
    svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
    svg.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    svg.append(f'<text x="{width/2}" y="34" text-anchor="middle" font-family="Arial" font-size="22">L1 Distributed Benchmark Overview</text>')
    svg.append(f'<text x="{width/2}" y="58" text-anchor="middle" font-family="Arial" font-size="13">Per-node/per-mode mean comparison (end_to_end_ms, total_bytes)</text>')
    svg.extend(draw_panel("e2e_ms", "end_to_end_ms (mean)", "milliseconds", panel_x0))
    svg.extend(draw_panel("bytes", "total_bytes (mean)", "bytes", panel_x0 + panel_w + panel_gap))
    svg.append('</svg>')

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(svg))


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    nodes_cfg = cfg.get("nodes")
    if not isinstance(nodes_cfg, list) or not nodes_cfg:
        raise SystemExit("config.nodes must be a non-empty array")

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = args.tag or cfg.get("tag") or f"l1_suite_{ts}"
    out_root = os.path.join(RESULTS_ROOT, tag)
    ensure_dir(out_root)
    ensure_dir(os.path.join(out_root, "nodes"))

    parallel_enabled = bool(cfg.get("parallel", True)) and not args.no_parallel
    max_workers_cfg = int(cfg.get("max_workers", len(nodes_cfg)))
    max_workers = max(1, min(len(nodes_cfg), max_workers_cfg))
    start_sync_delay_s = float(cfg.get("start_sync_delay_s", 3.0))
    start_at_epoch = None
    if parallel_enabled:
        start_at_epoch = time.time() + max(0.0, start_sync_delay_s)

    run_nodes: List[Dict[str, Any]] = [{} for _ in nodes_cfg]
    if parallel_enabled:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [
                ex.submit(
                    run_one_node,
                    raw_node,
                    index=i,
                    out_root=out_root,
                    tag=tag,
                    keep_remote_out=args.keep_remote_out,
                    start_at_epoch=start_at_epoch,
                )
                for i, raw_node in enumerate(nodes_cfg)
            ]
            for fut in concurrent.futures.as_completed(futures):
                idx, rec = fut.result()
                run_nodes[idx] = rec
    else:
        for i, raw_node in enumerate(nodes_cfg):
            _, rec = run_one_node(
                raw_node,
                index=i,
                out_root=out_root,
                tag=tag,
                keep_remote_out=args.keep_remote_out,
                start_at_epoch=None,
            )
            run_nodes[i] = rec

    summary = {
        "tag": tag,
        "generated_at": utc_now(),
        "config_path": os.path.abspath(args.config),
        "parallel": parallel_enabled,
        "max_workers": max_workers if parallel_enabled else 1,
        "start_sync_delay_s": start_sync_delay_s if parallel_enabled else 0.0,
        "scheduled_start_at_epoch": start_at_epoch,
        "nodes": run_nodes,
    }
    summary_json = os.path.join(out_root, "l1_summary.json")
    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    report_md = os.path.join(out_root, "l1_report.md")
    write_markdown_report(report_md, tag, run_nodes)
    l1_overview_svg = os.path.join(out_root, "l1_overview.svg")
    write_l1_overview_svg(l1_overview_svg, run_nodes)

    latest = os.path.join(RESULTS_ROOT, "l1_latest")
    if os.path.islink(latest) or os.path.exists(latest):
        if os.path.islink(latest):
            os.unlink(latest)
        elif os.path.isdir(latest):
            shutil.rmtree(latest)
        else:
            os.remove(latest)
    os.symlink(out_root, latest)

    print("saved", summary_json)
    print("saved", report_md)
    if os.path.exists(l1_overview_svg):
        print("saved", l1_overview_svg)
    print("saved", latest)


if __name__ == "__main__":
    main()
