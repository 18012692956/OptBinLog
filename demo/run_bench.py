import json
import os
import subprocess
import sys

ROOT = os.path.dirname(__file__)
EVENTLOG_DIR = os.path.join(ROOT, "eventlogst")
OUT_DIR = os.path.join(ROOT, "bench")
SHARED = os.path.join(OUT_DIR, "shared_eventtag.bin")
TEXT_LOG = os.path.join(OUT_DIR, "text.log")
BIN_LOG = os.path.join(OUT_DIR, "binary.bin")
RECORDS = int(os.environ.get("OPTBINLOG_BENCH_RECORDS", "20000"))

os.makedirs(OUT_DIR, exist_ok=True)

bench = os.path.join(ROOT, "optbinlog_bench")

def run(mode, out_path):
    cmd = [bench, "--mode", mode, "--eventlog-dir", EVENTLOG_DIR, "--out", out_path, "--records", str(RECORDS), "--shared", SHARED]
    out = subprocess.check_output(cmd, text=True).strip()
    return out

text_line = run("text", TEXT_LOG)
bin_line = run("binary", BIN_LOG)

print(text_line)
print(bin_line)

# parse

def parse(line):
    parts = line.split(',')
    out = {}
    for i in range(0, len(parts) - 1, 2):
        out[parts[i]] = parts[i + 1]
    return out

text = parse(text_line)
binr = parse(bin_line)

result = {
    "records": int(text.get("records", 0)),
    "text": {
        "elapsed_ms": float(text.get("elapsed_ms", 0)),
        "bytes": int(text.get("bytes", 0)),
        "shared_bytes": int(text.get("shared_bytes", 0)),
        "total_bytes": int(text.get("total_bytes", text.get("bytes", 0))),
        "peak_kb": int(text.get("peak_kb", 0)),
    },
    "binary": {
        "elapsed_ms": float(binr.get("elapsed_ms", 0)),
        "bytes": int(binr.get("bytes", 0)),
        "shared_bytes": int(binr.get("shared_bytes", 0)),
        "total_bytes": int(binr.get("total_bytes", binr.get("bytes", 0))),
        "peak_kb": int(binr.get("peak_kb", 0)),
    },
}

json_path = os.path.join(OUT_DIR, "bench_result.json")
with open(json_path, "w") as f:
    json.dump(result, f, indent=2)

print("saved", json_path)

# Try matplotlib
try:
    import matplotlib.pyplot as plt

    labels = ["time(ms)", "size(bytes)", "rss(kb)"]
    text_vals = [result["text"]["elapsed_ms"], result["text"]["bytes"], result["text"]["peak_kb"]]
    bin_vals = [result["binary"]["elapsed_ms"], result["binary"]["bytes"], result["binary"]["peak_kb"]]

    x = range(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar([i - width/2 for i in x], text_vals, width, label="text")
    ax.bar([i + width/2 for i in x], bin_vals, width, label="binary")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_title(f"optbinlog vs text ({result['records']} records)")
    ax.legend()
    fig.tight_layout()

    png_path = os.path.join(OUT_DIR, "bench_result.png")
    fig.savefig(png_path, dpi=150)
    print("saved", png_path)
except Exception as e:
    print("matplotlib unavailable, skip plot:", e)
