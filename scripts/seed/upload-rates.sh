#!/usr/bin/env bash
set -euo pipefail

API_KEY="${API_KEY:-local-upload-key}"
BASE="${INGESTION_URL:-http://localhost:8080}"

upload() {
  local vendor=$1 file=$2
  echo "Uploading $file for vendor $vendor..."
  curl -sf -X POST "$BASE/rates/upload?vendor=$vendor" \
    -H "X-API-Key: $API_KEY" \
    -H "Content-Type: text/csv" \
    --data-binary "@$file"
  echo
}

upload "vendor-default" "$(dirname "$0")/rates-default.csv"
upload "vendor-a" "$(dirname "$0")/rates-vendor-a.csv"
upload "vendor-lpm-demo" "$(dirname "$0")/rates-lpm-demo.csv"

echo "Uploading vendor-b JSON..."
curl -sf -X POST "$BASE/rates/upload?vendor=vendor-b" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  --data-binary "@$(dirname "$0")/rates-vendor-b.json"
echo

echo "Triggering trie rebuild..."
curl -sf -X POST "$BASE/admin/trie/rebuild" -H "X-API-Key: $API_KEY"
echo
echo "Seed complete."
