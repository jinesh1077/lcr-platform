#!/usr/bin/env bash
set -euo pipefail
ROUTING_URL="${ROUTING_URL:-http://localhost:8081}"

echo "Testing routing failover for 447700900123..."
RESPONSE=$(curl -sf -X POST "$ROUTING_URL/route" \
  -H 'Content-Type: application/json' \
  -d '{"dialedNumber":"447700900123","defaultRegion":"GB"}')

echo "$RESPONSE" | python3 -m json.tool

CANDIDATES=$(echo "$RESPONSE" | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('candidates',[])))")
if [ "$CANDIDATES" -ge 1 ]; then
  echo "PASS: Got $CANDIDATES routing candidate(s)"
  exit 0
else
  echo "FAIL: No routing candidates returned"
  exit 1
fi
