#!/usr/bin/env bash
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-carrier-opt}"
MEMORY="${MINIKUBE_MEMORY:-8192}"
CPUS="${MINIKUBE_CPUS:-4}"

echo "Starting Minikube profile: $PROFILE (${MEMORY}MB, ${CPUS} CPUs)"

if ! minikube status -p "$PROFILE" &>/dev/null; then
  minikube start -p "$PROFILE" --memory="$MEMORY" --cpus="$CPUS" \
    --addons=ingress,metrics-server
else
  minikube start -p "$PROFILE"
fi

kubectl config use-context "$PROFILE"
eval "$(minikube -p "$PROFILE" docker-env)"

echo "Minikube ready. Run 'make deploy' to deploy the stack."
