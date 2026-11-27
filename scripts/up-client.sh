#!/usr/bin/env bash
# Start a client node.  scripts/up-client.sh <index> [more...]
#
# Each client is one user: its own container, its own keys, its own history on
# its own volume. The volume is what makes "kill the client and restart it"
# lose nothing, which is part of the two-failure demonstration.
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker
require_network
require_image

if [ $# -eq 0 ]; then
  echo "usage: $(basename "$0") <index> [index...]" >&2
  exit 64
fi

for index in "$@"; do
  name="${CLIENT_PREFIX}-${index}"
  port=$((CLIENT_BASE_PORT + index))

  if container_running "${name}"; then
    echo "${name} already running on http://localhost:${port}"
    continue
  fi
  docker rm -f "${name}" >/dev/null 2>&1 || true

  mapfile -t common < <(docker_run_common)
  docker run "${common[@]}" \
    --name "${name}" \
    --hostname "${name}" \
    --label linkchat.role=client \
    --publish "${port}:8000" \
    --volume "${name}-data:/data" \
    --env "LINKCHAT_DATA_DIR=/data" \
    "${IMAGE}" client >/dev/null

  echo "started ${name}; UI on http://localhost:${port}"
done
