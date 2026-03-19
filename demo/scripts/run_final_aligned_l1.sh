#!/bin/zsh
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "usage: $0 <profile> <eventlog_dir> <peer_mode> <out_dir> [records] [repeats] [warmup] [start_delay_s] [node_count]" >&2
  exit 2
fi

PROFILE="$1"
EVENTLOG_DIR="$2"
PEER_MODE="$3"
OUT_DIR="$4"
RECORDS="${5:-20000}"
REPEATS="${6:-3}"
WARMUP="${7:-1}"
START_DELAY_S="${8:-10}"
NODE_COUNT="${9:-10}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKDIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MODES="text_semantic_like,binary,${PEER_MODE}"
BUILD_CMD="cd '$WORKDIR'; mkdir -p build/bin; gcc -O2 -Wall -Wextra -std=c11 -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L -Iinclude -o build/bin/optbinlog_bench_linux '$WORKDIR/optbinlog_bench.c' src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR/nodes"
SHARED_TAG_PATH="$OUT_DIR/shared/shared_eventtag.bin"
mkdir -p "$(dirname "$SHARED_TAG_PATH")"
rm -f "$SHARED_TAG_PATH"

for i in $(seq -w 1 "$NODE_COUNT"); do
  node="thesis-dev-$i"
  name="dev-$i"
  mkdir -p "$OUT_DIR/nodes/$name"
  limactl shell "$node" -- bash -lc "$BUILD_CMD" \
    >"$OUT_DIR/nodes/$name/build.stdout.log" \
    2>"$OUT_DIR/nodes/$name/build.stderr.log" &
done
wait

START_AT="$(python3 -c "import time; print(f'{time.time() + float($START_DELAY_S):.6f}')")"
for i in $(seq -w 1 "$NODE_COUNT"); do
  node="thesis-dev-$i"
  name="dev-$i"
  node_out="$OUT_DIR/nodes/$name/bench_out"
  runner_log="$OUT_DIR/nodes/$name/runner"
  rm -rf "$node_out"
  mkdir -p "$node_out"
  limactl shell "$node" -- bash -lc "cd '$WORKDIR'; \
python3 -c 'import time; t=float(\"$START_AT\"); d=t-time.time(); time.sleep(d if d>0 else 0.0)'; \
export OPTBINLOG_BENCH_OUT_DIR='$node_out' \
OPTBINLOG_BENCH_BIN=./build/bin/optbinlog_bench_linux \
OPTBINLOG_EVENTLOG_DIR='$EVENTLOG_DIR' \
OPTBINLOG_SHARED_TAG_PATH='$SHARED_TAG_PATH' \
OPTBINLOG_BENCH_RECORDS='$RECORDS' \
OPTBINLOG_BENCH_REPEATS='$REPEATS' \
OPTBINLOG_BENCH_WARMUP='$WARMUP' \
OPTBINLOG_BENCH_MODES='$MODES' \
OPTBINLOG_BENCH_BASELINE=text_semantic_like \
OPTBINLOG_MULTI_BENCH=0 \
OPTBINLOG_TEXT_PROFILE=semantic; \
python3 scripts/run_bench.py" \
    >"${runner_log}.stdout.log" \
    2>"${runner_log}.stderr.log" &
done
wait

echo "profile=$PROFILE"
echo "out_dir=$OUT_DIR"
