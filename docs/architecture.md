# Architecture

## Flow

1. Rate sheets uploaded via ingestion, normalized, stored in Postgres.
2. Trie builder loads active rates into Redis (double-buffered A/B).
3. Routing engine matches dialed number by longest prefix, returns ranked carriers.
4. CDRs on Kafka consumed by telemetry (quality + blocklist) and written to ClickHouse.
5. Invoice auditor compares carrier bills against ClickHouse aggregates.

## Redis keys

| Key | Purpose |
|-----|---------|
| `trie:active` | Active buffer pointer (A or B) |
| `trie:{A\|B}:{prefix}` | Carrier rates for prefix |
| `blocklist:{carrier_id}` | Circuit breaker |
| `health:{carrier_id}` | Routing penalty score |

## Kafka topics

- `rates.activated`
- `cdr.events`
- `cdr.events.dlq`

## Ops

```bash
# Upload rates
curl -X POST "http://localhost:8080/rates/upload?vendor=vendor-default" \
  -H "X-API-Key: local-upload-key" \
  --data-binary @scripts/seed/rates-default.csv

# Rebuild trie
curl -X POST http://localhost:8080/admin/trie/rebuild -H "X-API-Key: local-upload-key"

# Clear blocklist
curl -X DELETE http://localhost:8080/admin/blocklist/nexatel -H "X-API-Key: local-upload-key"

# LPM example: London number should match prefix 442
curl -X POST http://localhost:8081/route \
  -H 'Content-Type: application/json' \
  -d '{"dialedNumber":"44207123456","defaultRegion":"GB"}'
```

## Memory (Minikube)

Postgres 256MB, Redis 128MB, Kafka 512MB, ClickHouse 512MB. Steady-state ~3.5GB of 8GB.
