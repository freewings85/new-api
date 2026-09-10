#!/usr/bin/env bash
# 解析镜像 tag 并打印到标准输出，供 build-*-image.sh 和 local-up.sh 共用。
# 规则：优先取仓库根目录 VERSION 文件；为空则用第一个参数；参数也没有则提示输入；
#       VERSION 和参数都有但不一致则报错。
set -euo pipefail
cd "$(dirname "$0")/.."
INPUT_TAG="${1:-}"
FILE_TAG="$(tr -d '[:space:]' < VERSION 2>/dev/null || true)"
if [[ -n "$FILE_TAG" && -n "$INPUT_TAG" && "$FILE_TAG" != "$INPUT_TAG" ]]; then
  echo "error: VERSION 文件里的 tag ($FILE_TAG) 和传入的 tag ($INPUT_TAG) 不一致" >&2; exit 1
fi
TAG="${FILE_TAG:-$INPUT_TAG}"
if [[ -z "$TAG" ]]; then
  read -rp "VERSION 文件为空，请输入镜像 tag: " TAG </dev/tty
  [[ -n "$TAG" ]] || { echo "error: tag 不能为空" >&2; exit 1; }
fi
echo "$TAG"
