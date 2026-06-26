#!/usr/bin/env bash
# Thorough tests against ITU E.164 + generated rate deck.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export REPO_ROOT
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROUTING="${ROUTING_URL:-http://localhost:8081/route}"
INGESTION="${INGESTION_URL:-http://localhost:8080}"
TELEMETRY="${TELEMETRY_URL:-http://localhost:8082}"
COMPOSE="$REPO_ROOT/scripts/compose.sh"
OUT="${1:-/tmp/lcr-data-driven.md}"

exec 3>&1
exec 1> >(tee /tmp/data-driven-test.log)
exec 2>&1

echo "=== Data-Driven LCR Test Suite ==="
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo

python3 "$SCRIPT_DIR/data_driven_test.py" --report "$OUT"

exec 1>&3 3>&-
echo "Done."
