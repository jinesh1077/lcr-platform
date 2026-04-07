#!/usr/bin/env bash
set -euo pipefail
NAMESPACE="${NAMESPACE:-carrier-opt}"
echo "Scaling mock-carrier to 0..."
kubectl scale deployment mock-carrier -n "$NAMESPACE" --replicas=0
echo "Mock carrier offline."
