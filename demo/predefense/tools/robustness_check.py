#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import struct
import subprocess
from typing import Dict


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inject corruption into binlog and verify detection behavior")
    p.add_argument("--read-bin", required=True, help="path to optbinlog_read")
    p.add_argument("--shared", required=True, help="shared_eventtag.bin path")
    p.add_argument("--log", required=True, help="valid runtime binlog path")
    p.add_argument("--out-dir", required=True, help="output directory")
    return p.parse_args()


def run_read(read_bin: str, shared: str, log: str, out_path: str) -> int:
    cp = subprocess.run(
        [read_bin, "--shared", shared, "--log", log, "--format", "table", "--limit", "40", "--summary"],
        capture_output=True,
        text=True,
        check=False,
    )
    with open(out_path, "w", encoding="utf-8") as f:
        if cp.stdout:
            f.write(cp.stdout)
        if cp.stderr:
            f.write(cp.stderr)
    return cp.returncode


def mutate_first_tag(path: str) -> None:
    with open(path, "r+b") as f:
        data = bytearray(f.read())
        # frame: [len:4][timestamp:8][tag_id:2][...]
        if len(data) >= 14:
            data[12] = 0xFF
            data[13] = 0xFF
            f.seek(0)
            f.write(data)


def truncate_to_break_first_frame(path: str) -> None:
    size = os.path.getsize(path)
    with open(path, "r+b") as f:
        raw = f.read(4)
        if len(raw) < 4:
            f.truncate(max(0, size - 1))
            return
        frame_hdr = struct.unpack("<I", raw)[0]
        frame_len = frame_hdr & 0x1FFFFFFF
        first_frame_total = 4 + frame_len
        if first_frame_total > 1 and first_frame_total <= size:
            # 截断到首帧内部，确保解码器命中“帧不完整”路径。
            f.truncate(first_frame_total - 1)
        else:
            f.truncate(max(0, size - 8))


def main() -> int:
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    valid_out = os.path.join(args.out_dir, "valid_read.txt")
    bad_tag_path = os.path.join(args.out_dir, "corrupt_bad_tag.bin")
    trunc_path = os.path.join(args.out_dir, "corrupt_truncated.bin")
    bad_tag_out = os.path.join(args.out_dir, "bad_tag_read.txt")
    trunc_out = os.path.join(args.out_dir, "trunc_read.txt")
    summary_json = os.path.join(args.out_dir, "robustness_summary.json")
    summary_md = os.path.join(args.out_dir, "robustness_summary.md")

    valid_rc = run_read(args.read_bin, args.shared, args.log, valid_out)

    shutil.copy2(args.log, bad_tag_path)
    mutate_first_tag(bad_tag_path)
    bad_tag_rc = run_read(args.read_bin, args.shared, bad_tag_path, bad_tag_out)

    shutil.copy2(args.log, trunc_path)
    truncate_to_break_first_frame(trunc_path)
    trunc_rc = run_read(args.read_bin, args.shared, trunc_path, trunc_out)

    summary: Dict[str, object] = {
        "valid_ok": valid_rc == 0,
        "bad_tag_detected": bad_tag_rc != 0,
        "truncated_detected": trunc_rc != 0,
        "return_codes": {
            "valid": valid_rc,
            "bad_tag": bad_tag_rc,
            "truncated": trunc_rc,
        },
        "artifacts": {
            "valid_read": valid_out,
            "bad_tag_read": bad_tag_out,
            "trunc_read": trunc_out,
        },
    }

    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    with open(summary_md, "w", encoding="utf-8") as f:
        f.write("# Robustness Summary\n\n")
        f.write(f"- valid_ok: `{summary['valid_ok']}`\n")
        f.write(f"- bad_tag_detected: `{summary['bad_tag_detected']}`\n")
        f.write(f"- truncated_detected: `{summary['truncated_detected']}`\n\n")
        f.write("## Return Codes\n\n")
        f.write(f"- valid: `{valid_rc}`\n")
        f.write(f"- bad_tag: `{bad_tag_rc}`\n")
        f.write(f"- truncated: `{trunc_rc}`\n\n")
        f.write("## Evidence Files\n\n")
        f.write(f"- `valid_read.txt`\n")
        f.write(f"- `bad_tag_read.txt`\n")
        f.write(f"- `trunc_read.txt`\n")

    print(f"summary_json={summary_json}")
    print(f"summary_md={summary_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
