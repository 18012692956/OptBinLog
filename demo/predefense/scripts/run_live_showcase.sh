#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PREDEFENSE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

exec python3 "${PREDEFENSE_DIR}/scripts/run_full_showcase.py" --mode live "$@"
