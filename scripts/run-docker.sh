#!/usr/bin/env bash
# Build and run the Defender Advanced Hunting MCP server in Docker.
#
# Steps:
#   1. (Optional) az login to the desired tenant so DefaultAzureCredential / bearer
#      tokens work for the MCP client.
#   2. docker build -t defender-ah-mcp .
#   3. docker run --rm -p 3000:3000 --env-file .env defender-ah-mcp
#
# Usage:
#   ./scripts/run-docker.sh                          # build + run, use existing az session
#   ./scripts/run-docker.sh --tenant <tenant-id>     # ensure az login against this tenant first
#   ./scripts/run-docker.sh --local-token            # acquire a Defender AH token via `az` and pass
#                                                    # it to the container (mimics bearer pass-through).
#                                                    # Default --token-resource is
#                                                    # https://securitycenter.microsoft.com/mtp, which
#                                                    # matches DEFENDER_GRAPH_TOKEN_SCOPE for the
#                                                    # Advanced Hunting endpoint. Override with
#                                                    # --token-resource if hitting a different audience.
#   ./scripts/run-docker.sh --no-build               # skip docker build
#   ./scripts/run-docker.sh --port 8080              # bind to a different host port
#   ./scripts/run-docker.sh --env-file path/to/.env  # use a non-default env file

set -euo pipefail

# --- defaults -----------------------------------------------------------------
IMAGE_NAME="defender-ah-mcp"
HOST_PORT="3000"
CONTAINER_PORT="3000"
ENV_FILE=".env"
TENANT_ID=""
DO_BUILD="true"
LOCAL_TOKEN="false"
TOKEN_RESOURCE="https://securitycenter.microsoft.com/mtp"

# --- locate repo root ---------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# --- parse args ---------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tenant)         TENANT_ID="$2"; shift 2 ;;
    --port)           HOST_PORT="$2"; shift 2 ;;
    --env-file)       ENV_FILE="$2"; shift 2 ;;
    --image)          IMAGE_NAME="$2"; shift 2 ;;
    --no-build)       DO_BUILD="false"; shift ;;
    --local-token)    LOCAL_TOKEN="true"; shift ;;
    --token-resource) TOKEN_RESOURCE="$2"; shift 2 ;;
    -h|--help)
      grep -E "^# " "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

# --- prerequisites ------------------------------------------------------------
command -v docker >/dev/null 2>&1 || {
  echo "✗ docker is not installed or not on PATH." >&2
  exit 1
}

if [[ ! -f "$ENV_FILE" ]]; then
  cat >&2 <<EOF
✗ Env file not found: $ENV_FILE

Create one based on the README's "⚙️ Configuration" section. Minimum example:

  AH_MODE=VibeHunting
  DEFENDER_GRAPH_TENANT_ID=<tenant>
  DEFENDER_GRAPH_CLIENT_ID=<client-id>
  DEFENDER_GRAPH_CLIENT_SECRET=<client-secret>

Or for bearer pass-through, just:

  AH_MODE=VibeHunting

EOF
  exit 1
fi

# --- (optional) az login ------------------------------------------------------
ensure_az_login() {
  local tenant="$1"
  command -v az >/dev/null 2>&1 || {
    echo "✗ az CLI requested (--tenant) but not installed." >&2
    exit 1
  }

  local current_tenant
  current_tenant="$(az account show --query tenantId -o tsv 2>/dev/null || true)"
  if [[ "$current_tenant" == "$tenant" ]]; then
    echo "✓ az already logged in to tenant $tenant"
  else
    echo "→ Logging in to tenant $tenant via az login..."
    az login --tenant "$tenant" >/dev/null
  fi

  # Acquire a Graph token to verify access (cached for DefaultAzureCredential).
  echo "→ Acquiring Microsoft Graph access token..."
  az account get-access-token \
      --resource "https://graph.microsoft.com" \
      --query 'expiresOn' -o tsv >/dev/null
  echo "✓ Token acquired"
}

if [[ -n "$TENANT_ID" ]]; then
  ensure_az_login "$TENANT_ID"
fi

# --- build --------------------------------------------------------------------
if [[ "$DO_BUILD" == "true" ]]; then
  echo "→ Building image $IMAGE_NAME..."
  docker build -t "$IMAGE_NAME" .
else
  echo "↷ Skipping build (--no-build)"
fi

# --- run ----------------------------------------------------------------------
EXTRA_ENV=()

if [[ "$LOCAL_TOKEN" == "true" ]]; then
  command -v az >/dev/null 2>&1 || {
    echo "✗ --local-token requires the az CLI." >&2
    exit 1
  }
  echo "→ Acquiring access token from host az session (resource: $TOKEN_RESOURCE)..."
  TOKEN_JSON="$(az account get-access-token --resource "$TOKEN_RESOURCE" -o json)"
  ACCESS_TOKEN="$(echo "$TOKEN_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["accessToken"])')"
  EXPIRES_ON="$(echo "$TOKEN_JSON" | python3 -c 'import json,sys,datetime; t=json.load(sys.stdin); print(t.get("expires_on") or int(datetime.datetime.fromisoformat(t["expiresOn"]).timestamp()))')"
  echo "  ✓ Token acquired (expires_on=$EXPIRES_ON)"
  echo "  ↪ Passing through to container as DEFENDER_GRAPH_ACCESS_TOKEN."
  echo "    This mimics the production bearer pass-through scenario where the"
  echo "    container does NOT acquire its own token."
  EXTRA_ENV+=(-e "DEFENDER_GRAPH_ACCESS_TOKEN=$ACCESS_TOKEN")
fi

echo "→ Running $IMAGE_NAME on http://localhost:${HOST_PORT}/mcp"
echo "  (Ctrl+C to stop)"
exec docker run --rm \
  -p "${HOST_PORT}:${CONTAINER_PORT}" \
  --env-file "$ENV_FILE" \
  ${EXTRA_ENV[@]+"${EXTRA_ENV[@]}"} \
  "$IMAGE_NAME"
