#!/usr/bin/env bash
# 用本地构建的镜像启动 docker-compose.local.yml。用法：deploy/local-up.sh [tag]
# 停止：docker compose -f docker-compose.local.yml down
set -euo pipefail
cd "$(dirname "$0")/.."
TAG="$(deploy/resolve-tag.sh "${1:-}")"
for img in new-api-server new-api-web; do
  docker image inspect "${img}:${TAG}" >/dev/null 2>&1 || { echo "error: 镜像 ${img}:${TAG} 不存在，先运行 deploy/build-server-image.sh / deploy/build-web-image.sh" >&2; exit 1; }
done
IMAGE_TAG="$TAG" docker compose -f docker-compose.local.yml up -d
echo "waiting for server healthcheck..."
for _ in $(seq 1 30); do
  if curl -fs localhost:8080/api/status | grep -q '"success":\s*true'; then
    echo "ok: http://localhost:8080"; exit 0
  fi
  sleep 2
done
echo "error: 启动超时，查看日志：docker compose -f docker-compose.local.yml logs" >&2; exit 1
