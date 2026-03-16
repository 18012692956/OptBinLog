#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ ! -x "${DEMO_DIR}/optbinlog_read" ]]; then
  "${SCRIPT_DIR}/build_optbinlog_read.sh"
fi

cd "${DEMO_DIR}"
./optbinlog_read \
  --shared ./results/paper_dataset/samples/optbinlog_read/shared_eventtag.bin \
  --log ./results/paper_dataset/samples/optbinlog_read/binary_run_000.bin \
  --format table --limit 20 --summary
