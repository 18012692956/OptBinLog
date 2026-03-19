#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PREDEFENSE_DIR="${DEMO_DIR}/predefense"
cd "${DEMO_DIR}"

usage() {
  cat <<'EOF'
Usage:
  bash predefense/run_demo.sh [--eventlog-dir DIR] [--records N] [--tag TAG]

Options:
  --eventlog-dir DIR   Eventlog schema directory (default: eventlogst_semantic_min)
  --records N          Record count for space/speed comparison (default: 50000)
  --tag TAG            Output tag under predefense/results/ (default: predefense_YYYYmmdd_HHMMSS)
EOF
}

EVENTLOG_DIR_RAW="eventlogst_semantic_min"
RECORDS=50000
TAG="predefense_$(date +%Y%m%d_%H%M%S)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --eventlog-dir)
      EVENTLOG_DIR_RAW="${2:-}"
      shift 2
      ;;
    --records)
      RECORDS="${2:-}"
      shift 2
      ;;
    --tag)
      TAG="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${EVENTLOG_DIR_RAW}" = /* ]]; then
  EVENTLOG_DIR="${EVENTLOG_DIR_RAW}"
else
  EVENTLOG_DIR="${DEMO_DIR}/${EVENTLOG_DIR_RAW}"
fi

if [[ ! -d "${EVENTLOG_DIR}" ]]; then
  echo "eventlog dir not found: ${EVENTLOG_DIR}" >&2
  exit 1
fi

if ! [[ "${RECORDS}" =~ ^[0-9]+$ ]] || [[ "${RECORDS}" -le 0 ]]; then
  echo "--records must be a positive integer" >&2
  exit 1
fi

choose_cc() {
  if [[ -n "${CC:-}" ]] && command -v "${CC}" >/dev/null 2>&1; then
    echo "${CC}"
    return 0
  fi
  if [[ "$(uname -s)" == "Darwin" ]] && command -v clang >/dev/null 2>&1; then
    echo "clang"
    return 0
  fi
  if command -v gcc >/dev/null 2>&1; then
    echo "gcc"
    return 0
  fi
  if command -v clang >/dev/null 2>&1; then
    echo "clang"
    return 0
  fi
  return 1
}

extract_metric() {
  local line="$1"
  local key="$2"
  awk -F',' -v key="${key}" '
    {
      for (i = 1; i < NF; i += 2) {
        if ($i == key) {
          print $(i + 1);
          exit;
        }
      }
    }
  ' <<< "${line}"
}

CC_BIN="$(choose_cc || true)"
if [[ -z "${CC_BIN}" ]]; then
  echo "no compiler found (need gcc or clang)" >&2
  exit 1
fi

BIN_DIR="${PREDEFENSE_DIR}/build/bin"
OUT_ROOT="${PREDEFENSE_DIR}/results"
COMMON_FLAGS=(-O2 -Wall -Wextra -std=c11 -Iinclude)
COMMON_SRCS=(src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c)

mkdir -p "${BIN_DIR}"

echo "[1/4] building demo binaries with ${CC_BIN}"
"${CC_BIN}" "${COMMON_FLAGS[@]}" -o "${BIN_DIR}/optbinlog_read" optbinlog_read.c "${COMMON_SRCS[@]}"
"${CC_BIN}" "${COMMON_FLAGS[@]}" -o "${BIN_DIR}/optbinlog_roundtrip" optbinlog_roundtrip.c "${COMMON_SRCS[@]}"
"${CC_BIN}" "${COMMON_FLAGS[@]}" -o "${BIN_DIR}/optbinlog_bench" optbinlog_bench.c "${COMMON_SRCS[@]}"

OUT_DIR="${OUT_ROOT}/${TAG}"
ROUND_DIR="${OUT_DIR}/01_roundtrip"
BENCH_DIR="${OUT_DIR}/02_space_speed"
DECODE_DIR="${OUT_DIR}/03_decode_showcase"
mkdir -p "${ROUND_DIR}" "${BENCH_DIR}" "${DECODE_DIR}"

ROUND_SHARED="${ROUND_DIR}/shared_eventtag.bin"
ROUND_OK_LOG="${ROUND_DIR}/binary_ok.bin"
ROUND_BAD_LOG="${ROUND_DIR}/binary_bad.bin"
ROUND_TRUNC_LOG="${ROUND_DIR}/binary_trunc.bin"
ROUND_STDOUT="${ROUND_DIR}/roundtrip_stdout.txt"

echo "[2/4] running correctness and corruption-detection check"
"${BIN_DIR}/optbinlog_roundtrip" \
  --eventlog-dir "${EVENTLOG_DIR}" \
  --shared "${ROUND_SHARED}" \
  --log "${ROUND_OK_LOG}" \
  --bad-log "${ROUND_BAD_LOG}" \
  --trunc-log "${ROUND_TRUNC_LOG}" \
  > "${ROUND_STDOUT}" 2>&1

ROUNDTRIP_LINE="$(grep -E '^roundtrip_ok,' "${ROUND_STDOUT}" | tail -n 1 || true)"
if [[ -z "${ROUNDTRIP_LINE}" ]]; then
  echo "failed to parse roundtrip result from ${ROUND_STDOUT}" >&2
  exit 1
fi

IFS=',' read -r _ ROUNDTRIP_OK _ BAD_TAG_DETECTED _ TRUNC_DETECTED _ RECORDS_CHECKED <<< "${ROUNDTRIP_LINE}"

"${BIN_DIR}/optbinlog_read" \
  --shared "${ROUND_SHARED}" \
  --log "${ROUND_OK_LOG}" \
  --format table \
  --limit 5 \
  --summary \
  > "${DECODE_DIR}/decoded_roundtrip_table.txt"

"${BIN_DIR}/optbinlog_read" \
  --shared "${ROUND_SHARED}" \
  --log "${ROUND_OK_LOG}" \
  --format jsonl \
  --limit 5 \
  --summary \
  > "${DECODE_DIR}/decoded_roundtrip.jsonl"

echo "[3/4] running text vs binary comparison (records=${RECORDS})"
TEXT_LOG="${BENCH_DIR}/text_semantic.log"
BINARY_LOG="${BENCH_DIR}/optbinlog_binary.bin"
BINARY_SHARED="${BENCH_DIR}/shared_eventtag.bin"

TEXT_LINE="$(
  "${BIN_DIR}/optbinlog_bench" \
    --mode text_semantic_like \
    --eventlog-dir "${EVENTLOG_DIR}" \
    --out "${TEXT_LOG}" \
    --records "${RECORDS}"
)"
printf '%s\n' "${TEXT_LINE}" > "${BENCH_DIR}/text_bench.csvline"

BINARY_LINE="$(
  "${BIN_DIR}/optbinlog_bench" \
    --mode binary \
    --eventlog-dir "${EVENTLOG_DIR}" \
    --shared "${BINARY_SHARED}" \
    --out "${BINARY_LOG}" \
    --records "${RECORDS}"
)"
printf '%s\n' "${BINARY_LINE}" > "${BENCH_DIR}/binary_bench.csvline"

"${BIN_DIR}/optbinlog_read" \
  --shared "${BINARY_SHARED}" \
  --log "${BINARY_LOG}" \
  --format table \
  --limit 8 \
  --summary \
  > "${DECODE_DIR}/decoded_bench_table.txt"

"${BIN_DIR}/optbinlog_read" \
  --shared "${BINARY_SHARED}" \
  --log "${BINARY_LOG}" \
  --format jsonl \
  --limit 8 \
  --summary \
  > "${DECODE_DIR}/decoded_bench.jsonl"

TEXT_WRITE_MS="$(extract_metric "${TEXT_LINE}" "write_only_ms")"
TEXT_TOTAL_BYTES="$(extract_metric "${TEXT_LINE}" "total_bytes")"
BINARY_WRITE_MS="$(extract_metric "${BINARY_LINE}" "write_only_ms")"
BINARY_TOTAL_BYTES="$(extract_metric "${BINARY_LINE}" "total_bytes")"

if [[ -z "${TEXT_TOTAL_BYTES}" ]]; then
  TEXT_TOTAL_BYTES="$(wc -c < "${TEXT_LOG}" | tr -d '[:space:]')"
fi
if [[ -z "${BINARY_TOTAL_BYTES}" ]]; then
  BINARY_TOTAL_BYTES="$(( $(wc -c < "${BINARY_LOG}" | tr -d '[:space:]') + $(wc -c < "${BINARY_SHARED}" | tr -d '[:space:]') ))"
fi

SPACE_RATIO="$(awk -v t="${TEXT_TOTAL_BYTES}" -v b="${BINARY_TOTAL_BYTES}" 'BEGIN { if (b <= 0) print "inf"; else printf "%.2f", t / b }')"
SPEED_RATIO="$(awk -v t="${TEXT_WRITE_MS}" -v b="${BINARY_WRITE_MS}" 'BEGIN { if (b <= 0) print "inf"; else printf "%.2f", t / b }')"

echo "[4/4] generating markdown report"
REPORT_PATH="${OUT_DIR}/demo_report.md"
cat > "${REPORT_PATH}" <<EOF
# Optbinlog 预答辩演示报告

- 生成时间：$(date '+%Y-%m-%d %H:%M:%S %Z')
- 演示脚本：\`predefense/run_demo.sh\`
- eventlog 目录：\`${EVENTLOG_DIR}\`
- records：\`${RECORDS}\`

## 1) 正确性与鲁棒性

- roundtrip_ok：\`${ROUNDTRIP_OK}\`
- bad_tag_detected：\`${BAD_TAG_DETECTED}\`
- truncated_detected：\`${TRUNC_DETECTED}\`
- records_checked：\`${RECORDS_CHECKED}\`

原始输出：\`01_roundtrip/roundtrip_stdout.txt\`

## 2) 二进制可读回放

- 表格输出（建议现场展示）：\`03_decode_showcase/decoded_bench_table.txt\`
- JSONL 输出：\`03_decode_showcase/decoded_bench.jsonl\`
- roundtrip 样本回放：\`03_decode_showcase/decoded_roundtrip_table.txt\`

## 3) 文本日志 vs Optbinlog（二进制）

| 指标 | text_semantic_like | optbinlog binary |
|---|---:|---:|
| write_only_ms | ${TEXT_WRITE_MS} | ${BINARY_WRITE_MS} |
| total_bytes | ${TEXT_TOTAL_BYTES} | ${BINARY_TOTAL_BYTES} |

- 空间压缩比（text / binary）：\`${SPACE_RATIO}x\`
- 写入时间加速比（text / binary）：\`${SPEED_RATIO}x\`

原始行输出：
- \`02_space_speed/text_bench.csvline\`
- \`02_space_speed/binary_bench.csvline\`

## 4) 建议现场展示顺序（约 5 分钟）

1. 打开本文件快速报结论（正确性 + 空间/速度指标）
2. 展示 \`03_decode_showcase/decoded_bench_table.txt\`（可读字段名与数值）
3. 展示 \`01_roundtrip/roundtrip_stdout.txt\`（异常数据可被检测）
4. 若老师追问指标来源，再展示两条 bench 原始输出
EOF

ln -sfn "${TAG}" "${OUT_ROOT}/latest"

echo
echo "predefense demo completed"
echo "output dir: ${OUT_DIR}"
echo "latest link: ${OUT_ROOT}/latest"
echo "report: ${REPORT_PATH}"
