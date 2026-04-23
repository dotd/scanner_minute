#!/usr/bin/env bash
# Run scanner_minute locally in Docker.
#
# Requirements before running:
#   1. api_keys/polygon_api_key.txt        (Polygon API key)
#   2. api_keys/allowed_emails.txt         (one Gmail per line)
#   3. api_keys/oauth.env                  (copy from docker/templates/oauth.env.example
#                                           and fill in client id/secret)
#      — OR set SKIP_AUTH=1 in the env to bypass auth for a smoke test.
#
# Visit http://localhost:3000/ after it starts.
set -euo pipefail

cd "$(dirname "$0")/../.."

IMAGE_NAME="${IMAGE_NAME:-scanner-minute}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CONTAINER_NAME="${CONTAINER_NAME:-scanner-minute-local}"
HOST_PORT="${HOST_PORT:-3000}"

ENV_FILE="api_keys/oauth.env"

if [[ ! -f api_keys/polygon_api_key.txt ]]; then
    echo "ERROR: api_keys/polygon_api_key.txt is missing." >&2
    exit 1
fi

if [[ ! -f api_keys/allowed_emails.txt && "${SKIP_AUTH:-}" != "1" ]]; then
    echo "ERROR: api_keys/allowed_emails.txt is missing (or set SKIP_AUTH=1)." >&2
    exit 1
fi

DOCKER_ENV_ARGS=()
if [[ "${SKIP_AUTH:-}" == "1" ]]; then
    echo "WARNING: SKIP_AUTH=1 — no authentication enforced."
    DOCKER_ENV_ARGS+=(-e AUTH_ENABLED=0)
elif [[ -f "${ENV_FILE}" ]]; then
    DOCKER_ENV_ARGS+=(--env-file "${ENV_FILE}")
else
    echo "ERROR: ${ENV_FILE} not found. Copy docker/templates/oauth.env.example to api_keys/oauth.env, fill it in, or run with SKIP_AUTH=1." >&2
    exit 1
fi

# Remove any previous container with the same name
docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

echo "Starting ${CONTAINER_NAME} on http://localhost:${HOST_PORT}/ ..."

docker run --rm -it \
    --name "${CONTAINER_NAME}" \
    -p "${HOST_PORT}:3000" \
    -v "$(pwd)/api_keys:/app/api_keys:ro" \
    -v "$(pwd)/data:/app/data" \
    -v "$(pwd)/logs:/app/logs" \
    -v "$(pwd)/ScannerMinute:/app/ScannerMinute:ro" \
    -v "$(pwd)/tst:/app/tst:ro" \
    -v "$(pwd)/node_server:/app/node_server:ro" \
    "${DOCKER_ENV_ARGS[@]}" \
    "${IMAGE_NAME}:${IMAGE_TAG}"
