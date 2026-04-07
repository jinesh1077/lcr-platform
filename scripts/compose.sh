#!/usr/bin/env bash
# Wrapper: supports "docker compose" (plugin) and "docker-compose" (standalone).
set -euo pipefail

# Colima/Homebrew setups often keep credsStore=desktop after Docker Desktop is removed.
fix_missing_credential_helper() {
  local config="${DOCKER_CONFIG:-$HOME/.docker}/config.json"
  [[ -f "$config" ]] || return 0

  local store
  store=$(python3 -c "import json; print(json.load(open('$config')).get('credsStore',''))" 2>/dev/null || true)
  [[ -n "$store" ]] || return 0
  command -v "docker-credential-${store}" &>/dev/null && return 0

  export DOCKER_CONFIG
  DOCKER_CONFIG=$(mktemp -d)
  python3 - "$config" "$DOCKER_CONFIG/config.json" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
cfg = json.load(open(src))
cfg.pop("credsStore", None)
cfg.pop("credHelpers", None)
with open(dst, "w") as f:
    json.dump(cfg, f, indent="\t")
    f.write("\n")
PY
  echo "NOTE: credential helper 'docker-credential-${store}' not found; using config without credsStore." >&2
  echo "      To fix permanently, remove \"credsStore\" from ~/.docker/config.json" >&2
}

fix_missing_credential_helper

if docker compose version &>/dev/null 2>&1; then
  exec docker compose "$@"
fi

if command -v docker-compose &>/dev/null; then
  exec docker-compose "$@"
fi

cat >&2 <<'EOF'
ERROR: Docker Compose is not installed.

Your Docker CLI is present but the Compose plugin is missing.
Install one of the following:

  Option A — Standalone Compose (Homebrew CLI setup):
    brew install docker-compose

  Option B — Docker Desktop (includes Compose + daemon):
    https://docs.docker.com/desktop/install/mac-install/

You also need a running Docker daemon. With Homebrew docker alone, use Colima:
    brew install colima
    colima start

Then retry: make up
EOF
exit 1
