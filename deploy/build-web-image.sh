#!/usr/bin/env bash
# 构建前端镜像 new-api-web:<tag>。用法：deploy/build-web-image.sh [tag]
set -euo pipefail
cd "$(dirname "$0")/.."
TAG="$(deploy/resolve-tag.sh "${1:-}")"
docker build -f deploy/web.Dockerfile --build-arg "VERSION=${TAG}" -t "new-api-web:${TAG}" .
echo "built new-api-web:${TAG}"
