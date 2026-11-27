#!/usr/bin/env bash
# Start the head node. Publishes only the Ray dashboard to the host.
#
# The head holds the cluster directory and nothing of ours: no relay, no
# client, no mailbox, no key. It is never killed in phase 1 (ADR-0012).
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker
require_network
require_image

if container_running "${HEAD_NAME}"; then
  echo "${HEAD_NAME} is already running"
  exit 0
fi
docker rm -f "${HEAD_NAME}" >/dev/null 2>&1 || true

mapfile -t common < <(docker_run_common)
docker run "${common[@]}" \
  --name "${HEAD_NAME}" \
  --hostname "${HEAD_NAME}" \
  --publish "${DASHBOARD_PORT}:8265" \
  "${IMAGE}" head >/dev/null

echo "started ${HEAD_NAME}; dashboard on http://localhost:${DASHBOARD_PORT}"
echo "relay and client nodes may be started in any order, before or after this."
