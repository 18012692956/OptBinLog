import json
import os
import subprocess

ROOT = os.path.dirname(__file__)
EVENTLOG_DIR = os.path.join(ROOT, "eventlogst")
OUT_DIR = os.path.join(ROOT, "roundtrip")
os.makedirs(OUT_DIR, exist_ok=True)

SRC = os.path.join(ROOT, "optbinlog_roundtrip.c")
BIN = os.path.join(ROOT, "optbinlog_roundtrip")
SHARED = os.path.join(OUT_DIR, "shared_eventtag.bin")
LOG = os.path.join(OUT_DIR, "roundtrip_ok.bin")
BAD_LOG = os.path.join(OUT_DIR, "roundtrip_bad_tag.bin")
TRUNC_LOG = os.path.join(OUT_DIR, "roundtrip_truncated.bin")
RESULT_JSON = os.path.join(OUT_DIR, "roundtrip_result.json")


def parse_kv_csv(line):
    parts = line.strip().split(",")
    out = {}
    for i in range(0, len(parts) - 1, 2):
        out[parts[i]] = parts[i + 1]
    return out


compile_cmd = [
    "cc",
    "-O2",
    "-I",
    os.path.join(ROOT, "include"),
    "-o",
    BIN,
    SRC,
    os.path.join(ROOT, "src", "optbinlog_shared.c"),
    os.path.join(ROOT, "src", "optbinlog_eventlog.c"),
    os.path.join(ROOT, "src", "optbinlog_binlog.c"),
]
compile_proc = subprocess.run(compile_cmd, capture_output=True, text=True)

run_proc = None
parsed = {}
if compile_proc.returncode == 0:
    run_cmd = [
        BIN,
        "--eventlog-dir",
        EVENTLOG_DIR,
        "--shared",
        SHARED,
        "--log",
        LOG,
        "--bad-log",
        BAD_LOG,
        "--trunc-log",
        TRUNC_LOG,
    ]
    run_proc = subprocess.run(run_cmd, capture_output=True, text=True)
    parsed = parse_kv_csv(run_proc.stdout.strip()) if run_proc.stdout.strip() else {}

result = {
    "compile": {
        "cmd": compile_cmd,
        "returncode": compile_proc.returncode,
        "stdout": compile_proc.stdout,
        "stderr": compile_proc.stderr,
    },
    "run": None,
}

if run_proc is not None:
    result["run"] = {
        "returncode": run_proc.returncode,
        "stdout": run_proc.stdout,
        "stderr": run_proc.stderr,
        "parsed": parsed,
    }

with open(RESULT_JSON, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2)

print("saved", RESULT_JSON)
if run_proc is not None and run_proc.stdout.strip():
    print(run_proc.stdout.strip())

if compile_proc.returncode != 0:
    raise SystemExit(1)
if run_proc is None or run_proc.returncode != 0:
    raise SystemExit(1)
