#!/usr/bin/env bash
# 构建后端镜像 new-api-server:<tag>。用法：deploy/build-server-image.sh [tag]
set -euo pipefail
cd "$(dirname "$0")/.."
TAG="$(deploy/resolve-tag.sh "${1:-}")"
docker build -f deploy/server.Dockerfile --build-arg "VERSION=${TAG}" -t "new-api-server:${TAG}" .
echo "built new-api-server:${TAG}"
