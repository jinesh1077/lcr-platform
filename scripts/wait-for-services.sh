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

wait_docker_healthy() {
  local name=$1
  local max="${2:-60}"
  for i in $(seq 1 "$max"); do
    local status
    status="$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$name" 2>/dev/null || echo missing)"
    if [ "$status" = "healthy" ] || [ "$status" = "no-healthcheck" ]; then
      echo "$name is healthy."
      return 0
    fi
    echo "Waiting for $name ($status)... ($i/$max)"
    sleep 2
  done
  echo "ERROR: Timed out waiting for $name to be healthy" >&2
  return 1
}

wait_kafka_broker() {
  local name="${KAFKA_CONTAINER:-communicationproject-kafka-1}"
  local max="${1:-60}"
  for i in $(seq 1 "$max"); do
    if docker exec "$name" /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
      echo "kafka broker is ready."
      return 0
    fi
    echo "Waiting for kafka broker... ($i/$max)"
    sleep 2
  done
  echo "ERROR: Timed out waiting for kafka broker" >&2
  return 1
}

wait_url "ingestion" "$INGESTION_URL/health"
wait_url "routing-engine" "$ROUTING_URL/health"
wait_kafka_broker 30 || echo "WARN: kafka broker probe slow; simulations will retry if needed." >&2
wait_docker_healthy "${CH_CONTAINER:-communicationproject-clickhouse-1}" 30 || echo "WARN: ClickHouse health slow; ledger metrics may be telemetry-only." >&2
