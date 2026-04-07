#!/usr/bin/env bash
set -euo pipefail

INGESTION_URL="${INGESTION_URL:-http://localhost:8080}"
ROUTING_URL="${ROUTING_URL:-http://localhost:8081}"
MAX_ATTEMPTS="${WAIT_MAX_ATTEMPTS:-60}"
SLEEP_SEC="${WAIT_SLEEP_SEC:-2}"

wait_url() {
  local name=$1 url=$2
  for i in $(seq 1 "$MAX_ATTEMPTS"); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo "$name is ready."
      return 0
    fi
    echo "Waiting for $name... ($i/$MAX_ATTEMPTS)"
    sleep "$SLEEP_SEC"
  done
  echo "ERROR: Timed out waiting for $name at $url" >&2
  return 1
}

wait_url "ingestion" "$INGESTION_URL/health"
wait_url "routing-engine" "$ROUTING_URL/health"
