#!/usr/bin/env bash
set -euo pipefail

API_KEY="${API_KEY:-local-upload-key}"
BASE="${INGESTION_URL:-http://localhost:8080}"
DIR="$(dirname "$0")"
ROOT="$(cd "$DIR/../.." && pwd)"

echo "Building dataset and generating rate decks ..."
python3 "$ROOT/scripts/data/build-dataset.py"
python3 "$DIR/generate-rates.py"
python3 "$DIR/generate-traffic-profile.py"

upload() {
  local vendor=$1 file=$2
  echo "Uploading $file for vendor $vendor..."
  curl -sf -X POST "$BASE/rates/upload?vendor=$vendor" \
    -H "X-API-Key: $API_KEY" \
    -H "Content-Type: text/csv" \
    --data-binary "@$file"
  echo
}

upload "vendor-global" "$DIR/generated/rates-global.csv"
upload "vendor-competitive" "$DIR/generated/rates-competitive.csv"

# LPM reference routes (nested UK prefixes)
upload "vendor-lpm-demo" "$DIR/rates-lpm-demo.csv"

echo "Triggering trie rebuild..."
curl -sf -X POST "$BASE/admin/trie/rebuild" -H "X-API-Key: $API_KEY"
echo
echo "Seed complete."
echo "Rate deck stats:"
cat "$DIR/generated/rate-deck-stats.json"
