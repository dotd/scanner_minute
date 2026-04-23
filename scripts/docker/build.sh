#!/usr/bin/env bash
# Build the scanner_minute image locally.
# Run from the repo root:
#   ./scripts/docker/build.sh
set -euo pipefail

cd "$(dirname "$0")/../.."

IMAGE_NAME="${IMAGE_NAME:-scanner-minute}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

echo "Building ${IMAGE_NAME}:${IMAGE_TAG}..."
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .
echo "Built ${IMAGE_NAME}:${IMAGE_TAG}."
