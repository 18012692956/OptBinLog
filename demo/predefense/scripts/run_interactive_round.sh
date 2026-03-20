#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PREDEFENSE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Select scenario:"
echo "  1) normal (Recommended)"
echo "  2) stress"
read -r -p "choice [1/2]: " choice

SCENARIO="normal"
if [[ "${choice}" == "2" ]]; then
  SCENARIO="stress"
fi

echo "Select display mode:"
echo "  1) live dashboard + terminal stream (Recommended)"
echo "  2) terminal step playback only"
read -r -p "choice [1/2]: " show_choice

MODE="live"
if [[ "${show_choice}" == "2" ]]; then
  MODE="step"
fi

read -r -p "cycles (Enter for default): " cycles
read -r -p "fault-at-cycle (Enter for default): " fault
read -r -p "recover-at-cycle (Enter for default): " recover
read -r -p "stream-interval-ms (live mode only, Enter for default): " stream_interval

ARGS=(--scenario "${SCENARIO}" --mode "${MODE}")
if [[ -n "${cycles}" ]]; then
  ARGS+=(--cycles "${cycles}")
fi
if [[ -n "${fault}" ]]; then
  ARGS+=(--fault-at-cycle "${fault}")
fi
if [[ -n "${recover}" ]]; then
  ARGS+=(--recover-at-cycle "${recover}")
fi
if [[ "${MODE}" == "live" && -n "${stream_interval}" ]]; then
  ARGS+=(--stream-interval-ms "${stream_interval}")
fi

exec python3 "${PREDEFENSE_DIR}/scripts/run_full_showcase.py" "${ARGS[@]}"
