#!/usr/bin/env bash
set -euo pipefail
NAMESPACE="${NAMESPACE:-carrier-opt}"
echo "Flushing all blocklist keys from Redis..."
kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli KEYS 'blocklist:*' | \
  xargs -r kubectl exec -n "$NAMESPACE" deploy/redis -- redis-cli DEL
echo "Blocklist cleared."
