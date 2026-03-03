import json
import os
import subprocess

ROOT = os.path.dirname(__file__)
EVENTLOG_DIR = os.path.join(ROOT, "eventlogst")
OUT_DIR = os.path.join(ROOT, "bench_multi")
SHARED = os.path.join(OUT_DIR, "shared_eventtag.bin")
DEVICES = int(os.environ.get("OPTBINLOG_DEVICES", "10"))
RECORDS = int(os.environ.get("OPTBINLOG_RECORDS_PER_DEVICE", "2000"))

os.makedirs(OUT_DIR, exist_ok=True)

bench = os.path.join(ROOT, "optbinlog_multi_bench")

def run(mode):
    cmd = [bench, "--mode", mode, "--eventlog-dir", EVENTLOG_DIR, "--out-dir", OUT_DIR,
           "--devices", str(DEVICES), "--records-per-device", str(RECORDS), "--shared", SHARED]
    out = subprocess.check_output(cmd, text=True).strip()
    return out

text_line = run("text")
bin_line = run("binary")

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
    "devices": int(text.get("devices", DEVICES)),
    "records_per_device": int(text.get("records_per_device", RECORDS)),
    "text": {
        "elapsed_ms": float(text.get("elapsed_ms", 0)),
        "bytes": int(text.get("bytes", 0)),
        "shared_bytes": int(text.get("shared_bytes", 0)),
        "total_bytes": int(text.get("total_bytes", text.get("bytes", 0))),
    },
    "binary": {
        "elapsed_ms": float(binr.get("elapsed_ms", 0)),
        "bytes": int(binr.get("bytes", 0)),
        "shared_bytes": int(binr.get("shared_bytes", 0)),
        "total_bytes": int(binr.get("total_bytes", binr.get("bytes", 0))),
    },
}

json_path = os.path.join(OUT_DIR, "bench_multi_result.json")
with open(json_path, "w") as f:
    json.dump(result, f, indent=2)

print("saved", json_path)

# svg
svg_path = os.path.join(OUT_DIR, "bench_multi_result.svg")
labels = ["time(ms)", "total_bytes"]
text_vals = [result["text"]["elapsed_ms"], result["text"]["total_bytes"]]
bin_vals = [result["binary"]["elapsed_ms"], result["binary"]["total_bytes"]]

width = 800
height = 300
margin = 60
plot_w = width - margin * 2
plot_h = height - margin * 2

max_per_metric = [max(tv, bv) or 1 for tv, bv in zip(text_vals, bin_vals)]
bar_w = plot_w / (len(labels) * 3)
colors = {"text": "#2b8cbe", "binary": "#f03b20"}

svg = []
svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
svg.append('<rect width="100%" height="100%" fill="#ffffff"/>')
svg.append(f'<text x="{width/2}" y="30" text-anchor="middle" font-family="Arial" font-size="16">multi-device bench ({result["devices"]} devices, {result["records_per_device"]} records/device)</text>')
svg.append(f'<line x1="{margin}" y1="{height-margin}" x2="{width-margin}" y2="{height-margin}" stroke="#333"/>')
svg.append(f'<line x1="{margin}" y1="{margin}" x2="{margin}" y2="{height-margin}" stroke="#333"/>')

for i, label in enumerate(labels):
    x_group = margin + (i + 0.5) * (plot_w / len(labels))
    tv = text_vals[i]
    bv = bin_vals[i]
    maxm = max_per_metric[i]
    t_h = (tv / maxm) * (plot_h * 0.9)
    b_h = (bv / maxm) * (plot_h * 0.9)
    tx = x_group - bar_w * 1.2
    ty = height - margin - t_h
    bx = x_group + bar_w * 0.2
    by = height - margin - b_h
    svg.append(f'<rect x="{tx}" y="{ty}" width="{bar_w}" height="{t_h}" fill="{colors["text"]}"/>')
    svg.append(f'<rect x="{bx}" y="{by}" width="{bar_w}" height="{b_h}" fill="{colors["binary"]}"/>')
    svg.append(f'<text x="{x_group}" y="{height-margin+20}" text-anchor="middle" font-family="Arial" font-size="12">{label}</text>')
    svg.append(f'<text x="{tx+bar_w/2}" y="{ty-6}" text-anchor="middle" font-family="Arial" font-size="10">{tv:.2f}</text>')
    svg.append(f'<text x="{bx+bar_w/2}" y="{by-6}" text-anchor="middle" font-family="Arial" font-size="10">{bv:.2f}</text>')
    if tv > 0:
        improve = (tv - bv) / tv * 100.0
        sign = '+' if improve >= 0 else '-'
        svg.append(f'<text x="{x_group}" y="{ty-22}" text-anchor="middle" font-family="Arial" font-size="10" fill="#2ca25f">{sign}{abs(improve):.1f}%</text>')

svg.append(f'<rect x="{width-200}" y="{margin}" width="12" height="12" fill="{colors["text"]}"/>')
svg.append(f'<text x="{width-180}" y="{margin+11}" font-family="Arial" font-size="12">text</text>')
svg.append(f'<rect x="{width-200}" y="{margin+20}" width="12" height="12" fill="{colors["binary"]}"/>')
svg.append(f'<text x="{width-180}" y="{margin+31}" font-family="Arial" font-size="12">binary</text>')

svg.append('</svg>')

with open(svg_path, 'w') as f:
    f.write('\n'.join(svg))

print("saved", svg_path)
