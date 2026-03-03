import os
import subprocess
import json

ROOT = os.path.dirname(__file__)
EVENTLOG_DIR = os.path.join(ROOT, 'eventlogst')
OUT_DIR = os.path.join(ROOT, 'init_race')
SHARED = os.path.join(OUT_DIR, 'shared_eventtag.bin')
TRACE = os.path.join(OUT_DIR, 'trace.log')
PROCS = int(os.environ.get('OPTBINLOG_INIT_PROCS', '10'))

os.makedirs(OUT_DIR, exist_ok=True)

race = os.path.join(ROOT, 'optbinlog_init_race')
cmd = [race, '--eventlog-dir', EVENTLOG_DIR, '--shared', SHARED, '--trace', TRACE, '--procs', str(PROCS), '--clean']
subprocess.check_call(cmd)

# parse trace
records = []
with open(TRACE, 'r') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        parts = line.split(' ', 2)
        if len(parts) < 3:
            continue
        ts_ns = int(parts[0])
        pid_part = parts[1]
        evt = parts[2]
        pid = int(pid_part.split('=')[1])
        records.append({'ts_ns': ts_ns, 'pid': pid, 'event': evt})

# group by pid
by_pid = {}
for r in records:
    by_pid.setdefault(r['pid'], []).append(r)

for pid in by_pid:
    by_pid[pid].sort(key=lambda x: x['ts_ns'])

# compute summary
create_success = sum(1 for r in records if r['event'] == 'create_success')
init_done = sum(1 for r in records if r['event'] == 'init_done')
open_ok = sum(1 for r in records if r['event'] == 'open_existing_ok')

summary = {
    'procs': PROCS,
    'create_success': create_success,
    'init_done': init_done,
    'open_existing_ok': open_ok,
    'pids': list(by_pid.keys()),
}

json_path = os.path.join(OUT_DIR, 'init_race_result.json')
with open(json_path, 'w') as f:
    json.dump({'summary': summary, 'events': records}, f, indent=2)

# build svg timeline
svg_path = os.path.join(OUT_DIR, 'init_race_result.svg')

pids = sorted(by_pid.keys())
if not pids:
    raise SystemExit('no trace records')

min_ts = min(r['ts_ns'] for r in records)
max_ts = max(r['ts_ns'] for r in records)
span = max(1, max_ts - min_ts)

width = 900
row_h = 18
margin = 60
height = margin * 2 + row_h * len(pids)

svg = []
svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">')
svg.append('<rect width="100%" height="100%" fill="#ffffff"/>')
svg.append(f'<text x="{width/2}" y="30" text-anchor="middle" font-family="Arial" font-size="16">shared init race ({PROCS} procs)</text>')

# legend
svg.append('<rect x="60" y="40" width="12" height="12" fill="#2b8cbe"/>')
svg.append('<text x="78" y="50" font-family="Arial" font-size="12">init window</text>')
svg.append('<circle cx="200" cy="46" r="4" fill="#31a354"/>')
svg.append('<text x="210" y="50" font-family="Arial" font-size="12">create_success</text>')
svg.append('<circle cx="340" cy="46" r="4" fill="#756bb1"/>')
svg.append('<text x="350" y="50" font-family="Arial" font-size="12">open_existing_ok</text>')
svg.append('<circle cx="500" cy="46" r="4" fill="#de2d26"/>')
svg.append('<text x="510" y="50" font-family="Arial" font-size="12">wait_initializing</text>')

for i, pid in enumerate(pids):
    y = margin + i * row_h
    events = by_pid[pid]
    start = events[0]['ts_ns']
    end = events[-1]['ts_ns']
    x1 = 60 + int((start - min_ts) / span * (width - 120))
    x2 = 60 + int((end - min_ts) / span * (width - 120))
    svg.append(f'<rect x="{x1}" y="{y-8}" width="{max(2, x2-x1)}" height="8" fill="#2b8cbe" opacity="0.6"/>')
    svg.append(f'<text x="10" y="{y-2}" font-family="Arial" font-size="10">pid {pid}</text>')

    for ev in events:
        x = 60 + int((ev['ts_ns'] - min_ts) / span * (width - 120))
        if ev['event'] == 'create_success':
            color = '#31a354'
        elif ev['event'] == 'open_existing_ok':
            color = '#756bb1'
        elif ev['event'] == 'wait_initializing':
            color = '#de2d26'
        else:
            color = '#636363'
        svg.append(f'<circle cx="{x}" cy="{y-4}" r="3" fill="{color}"/>')

svg.append(f'<text x="60" y="{height-10}" font-family="Arial" font-size="12">create_success={create_success}, open_existing_ok={open_ok}, init_done={init_done}</text>')
svg.append('</svg>')

with open(svg_path, 'w') as f:
    f.write('\n'.join(svg))

print('saved', json_path)
print('saved', svg_path)
