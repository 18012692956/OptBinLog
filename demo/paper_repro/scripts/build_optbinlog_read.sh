#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${DEMO_DIR}"
clang -O2 -Wall -Wextra -std=c11 -Iinclude \
  -o optbinlog_read optbinlog_read.c \
  src/optbinlog_shared.c src/optbinlog_eventlog.c src/optbinlog_binlog.c

echo "built: ${DEMO_DIR}/optbinlog_read"
