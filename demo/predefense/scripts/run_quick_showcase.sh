#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PREDEFENSE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Quick mode: no live playback to reduce现场等待时间.
exec python3 "${PREDEFENSE_DIR}/scripts/run_full_showcase.py" --scenario normal --mode off --benchmark-runs 1 --benchmark-records 40000 "$@"
