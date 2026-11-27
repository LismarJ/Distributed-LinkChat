#!/usr/bin/env bash
# What the cluster looks like right now, from Ray's own point of view.
#
# Ray's state CLI is the stable interface for this; its Python SDK is a
# developer API, so application code does not use it (ADR-0004).
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

if ! container_running "${HEAD_NAME}"; then
  echo "${HEAD_NAME} is not running" >&2
  exit 1
fi

echo "=== containers ==="
docker ps --filter "network=${NETWORK}" \
  --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

echo
echo "=== ray status ==="
docker exec "${HEAD_NAME}" ray status || true

echo
echo "=== nodes ==="
docker exec "${HEAD_NAME}" ray list nodes || true

echo
echo "=== actors ==="
docker exec "${HEAD_NAME}" ray list actors || true
