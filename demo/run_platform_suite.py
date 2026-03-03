#!/usr/bin/env python3
import argparse
import datetime as dt
import glob
import json
import os
import platform
import re
import shutil
import socket
import subprocess
from typing import Dict, List, Optional, Tuple


ROOT = os.path.dirname(__file__)
BENCH_DIR = os.path.join(ROOT, "bench")
BENCH_MULTI_DIR = os.path.join(ROOT, "bench_multi")
OUT_ROOT = os.path.join(ROOT, "bench_platform")


def run_cmd(cmd: List[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        out = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
        return out.strip(), None
    except Exception as e:
        return None, str(e)


def parse_os_release() -> Dict[str, str]:
    out = {}
    path = "/etc/os-release"
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k] = v.strip().strip('"')
    return out


def detect_cpu_model() -> str:
    cpuinfo = "/proc/cpuinfo"
    if os.path.exists(cpuinfo):
        with open(cpuinfo, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
        for key in ("model name", "Hardware", "Processor"):
            m = re.search(rf"^{re.escape(key)}\s*:\s*(.+)$", text, flags=re.M)
            if m:
                return m.group(1).strip()
    out, _ = run_cmd(["sysctl", "-n", "machdep.cpu.brand_string"])
    return out or "unknown"


def detect_mem_total_kb() -> Optional[int]:
    meminfo = "/proc/meminfo"
    if os.path.exists(meminfo):
        with open(meminfo, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
        m = re.search(r"^MemTotal:\s+(\d+)\s+kB$", text, flags=re.M)
        if m:
            return int(m.group(1))
    return None


def mount_info_for_path(path: str) -> Dict[str, Optional[str]]:
    info = {
        "path": path,
        "mount_point": None,
        "source": None,
        "fs_type": None,
    }

    df_out, _ = run_cmd(["df", "-P", path])
    if df_out:
        lines = [ln for ln in df_out.splitlines() if ln.strip()]
        if len(lines) >= 2:
            cols = lines[-1].split()
            if len(cols) >= 6:
                info["source"] = cols[0]
                info["mount_point"] = cols[-1]

    findmnt_out, _ = run_cmd(["findmnt", "-no", "FSTYPE,SOURCE,TARGET", "--target", path])
    if findmnt_out:
        cols = findmnt_out.split()
        if len(cols) >= 3:
            info["fs_type"] = cols[0]
            info["source"] = cols[1]
            info["mount_point"] = cols[2]
            return info

    stat_out, _ = run_cmd(["stat", "-f", "-c", "%T", path])
    if not stat_out:
        stat_out, _ = run_cmd(["stat", "-f", "%T", path])
    if stat_out and "/" in stat_out:
        stat_out = None
    if stat_out and not info["fs_type"]:
        info["fs_type"] = stat_out

    if not info["fs_type"] and info["mount_point"]:
        mount_out, _ = run_cmd(["mount"])
        if mount_out:
            mp = re.escape(info["mount_point"])
            m = re.search(rf" on {mp} \(([^,\)]+)", mount_out)
            if m:
                info["fs_type"] = m.group(1).strip()

    return info


def parse_lsblk() -> List[Dict[str, object]]:
    out, _ = run_cmd(
        [
            "lsblk",
            "-J",
            "-o",
            "NAME,PATH,TYPE,PKNAME,ROTA,RM,TRAN,MODEL,FSTYPE,MOUNTPOINT",
        ]
    )
    if not out:
        return []
    try:
        obj = json.loads(out)
    except json.JSONDecodeError:
        return []

    flat: List[Dict[str, object]] = []

    def walk(nodes: List[Dict[str, object]]) -> None:
        for n in nodes:
            flat.append(n)
            children = n.get("children") or []
            if isinstance(children, list):
                walk(children)

    devices = obj.get("blockdevices") or []
    if isinstance(devices, list):
        walk(devices)
    return flat


def classify_storage_medium(name: Optional[str], tran: Optional[str], rota: Optional[object]) -> str:
    if name:
        if name.startswith("nvme"):
            return "NVMe"
        if name.startswith("mmcblk"):
            return "MMC (SD/eMMC)"
    if tran:
        t = tran.lower()
        if t in ("sata", "ata"):
            return "SATA"
        if t == "usb":
            return "USB"
        if t == "virtio":
            return "virtio"
    if str(rota) == "0":
        return "SSD/Flash (non-rotational)"
    if str(rota) == "1":
        return "HDD (rotational)"
    return "unknown"


def detect_macos_storage(source: Optional[str]) -> Dict[str, object]:
    if not source or not source.startswith("/dev/disk"):
        return {}
    info_out, _ = run_cmd(["diskutil", "info", source])
    if not info_out:
        return {}

    protocol = None
    model = None
    solid = None
    for line in info_out.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        k = k.strip().lower()
        v = v.strip()
        if k == "protocol":
            protocol = v
        elif k in ("device / media name", "media name"):
            model = v
        elif k == "solid state":
            solid = v

    medium = "unknown"
    if protocol:
        pl = protocol.lower()
        if pl == "nvme":
            medium = "NVMe"
        elif pl in ("sata", "ata"):
            medium = "SATA"
        elif pl == "usb":
            medium = "USB"
    if medium == "unknown" and solid:
        sl = solid.lower()
        if sl == "yes":
            medium = "SSD/Flash (non-rotational)"
        elif sl == "no":
            medium = "HDD (rotational)"

    return {
        "medium": medium,
        "device_name": os.path.basename(source),
        "device_path": source,
        "transport": protocol,
        "rotational": None if solid is None else (0 if solid.lower() == "yes" else 1),
        "model": model,
    }


def detect_storage_for_mount(mount: Dict[str, Optional[str]]) -> Dict[str, object]:
    nodes = parse_lsblk()
    source = mount.get("source") or ""
    target = mount.get("mount_point") or ""

    chosen = None
    for n in nodes:
        mp = n.get("mountpoint")
        path = n.get("path")
        if isinstance(mp, str) and mp == target:
            chosen = n
            break
        if isinstance(path, str) and source and path == source:
            chosen = n
            break

    if chosen is None:
        mac = detect_macos_storage(source)
        if mac:
            return mac
        return {
            "medium": "unknown",
            "device_name": None,
            "device_path": None,
            "transport": None,
            "rotational": None,
            "model": None,
        }

    name = chosen.get("name")
    path = chosen.get("path")
    tran = chosen.get("tran")
    rota = chosen.get("rota")
    model = chosen.get("model")

    return {
        "medium": classify_storage_medium(str(name) if name else None, str(tran) if tran else None, rota),
        "device_name": name,
        "device_path": path,
        "transport": tran,
        "rotational": rota,
        "model": model,
    }


def collect_platform_info(target_path: str) -> Dict[str, object]:
    osr = parse_os_release()
    mount = mount_info_for_path(target_path)
    storage = detect_storage_for_mount(mount)
    uname_all, _ = run_cmd(["uname", "-a"])
    uname_m, _ = run_cmd(["uname", "-m"])
    uname_r, _ = run_cmd(["uname", "-r"])

    return {
        "captured_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "host": {
            "hostname": socket.gethostname(),
            "arch": uname_m or platform.machine(),
            "kernel_release": uname_r or platform.release(),
            "kernel_full": uname_all,
            "os_name": osr.get("NAME"),
            "os_version": osr.get("VERSION"),
            "cpu_model": detect_cpu_model(),
            "cpu_logical_cores": os.cpu_count(),
            "mem_total_kb": detect_mem_total_kb(),
        },
        "storage": {
            "result_path": target_path,
            "filesystem": mount.get("fs_type"),
            "mount_point": mount.get("mount_point"),
            "source": mount.get("source"),
            **storage,
        },
    }


def run_script(script_name: str, env: Dict[str, str], log_path: str) -> Dict[str, object]:
    cmd = ["python3", os.path.join(ROOT, script_name)]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("CMD: " + " ".join(cmd) + "\n\n")
        f.write("STDOUT:\n")
        f.write(proc.stdout or "")
        f.write("\n\nSTDERR:\n")
        f.write(proc.stderr or "")
    return {
        "script": script_name,
        "returncode": proc.returncode,
        "log_file": os.path.basename(log_path),
    }


def copy_if_exists(src: str, dst_dir: str) -> Optional[str]:
    if not os.path.exists(src):
        return None
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, os.path.basename(src))
    shutil.copy2(src, dst)
    return dst


def snapshot_outputs(snapshot_dir: str) -> Dict[str, List[str]]:
    copied = {"bench": [], "bench_multi": []}

    bench_files = [
        "bench_result.json",
        "bench_result.svg",
        "bench_stats.svg",
        "bench_iqr.svg",
    ]
    bench_dst = os.path.join(snapshot_dir, "bench")
    for fn in bench_files:
        p = copy_if_exists(os.path.join(BENCH_DIR, fn), bench_dst)
        if p:
            copied["bench"].append(os.path.basename(p))

    multi_patterns = [
        "bench_multi_result.json",
        "bench_multi_result.svg",
        "bench_multi_scan.svg",
        "*_stats.svg",
        "*_iqr.svg",
    ]
    multi_dst = os.path.join(snapshot_dir, "bench_multi")
    for pat in multi_patterns:
        for src in sorted(glob.glob(os.path.join(BENCH_MULTI_DIR, pat))):
            p = copy_if_exists(src, multi_dst)
            if p:
                copied["bench_multi"].append(os.path.basename(p))
    return copied


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run benchmark suite and capture platform metadata for cross-platform comparison."
    )
    parser.add_argument("--tag", default="", help="Result tag, e.g. x86_ubuntu_nvme or rpi5_sd")
    parser.add_argument("--skip-single", action="store_true", help="Skip run_bench.py")
    parser.add_argument("--skip-multi", action="store_true", help="Skip run_multi_bench.py")
    parser.add_argument("--out-root", default=OUT_ROOT, help="Output root directory")
    args = parser.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    default_tag = f"{platform.machine()}_{socket.gethostname()}_{ts}"
    tag = args.tag.strip() or default_tag
    snapshot_dir = os.path.join(args.out_root, tag)
    os.makedirs(snapshot_dir, exist_ok=True)

    env = os.environ.copy()
    optbinlog_env = {k: v for k, v in env.items() if k.startswith("OPTBINLOG_")}

    run_reports = []
    if not args.skip_single:
        run_reports.append(
            run_script("run_bench.py", env, os.path.join(snapshot_dir, "run_bench.log"))
        )
    if not args.skip_multi:
        run_reports.append(
            run_script("run_multi_bench.py", env, os.path.join(snapshot_dir, "run_multi_bench.log"))
        )

    copied = snapshot_outputs(snapshot_dir)
    platform_info = collect_platform_info(snapshot_dir)

    report = {
        "tag": tag,
        "captured_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "platform": platform_info,
        "env": optbinlog_env,
        "runs": run_reports,
        "artifacts": copied,
    }

    report_path = os.path.join(snapshot_dir, "platform_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("saved", report_path)
    print("snapshot", snapshot_dir)
    print(
        "platform:",
        f"arch={platform_info['host']['arch']}",
        f"kernel={platform_info['host']['kernel_release']}",
        f"fs={platform_info['storage']['filesystem']}",
        f"medium={platform_info['storage']['medium']}",
    )
    for rr in run_reports:
        print(f"{rr['script']}: returncode={rr['returncode']} log={rr['log_file']}")


if __name__ == "__main__":
    main()
