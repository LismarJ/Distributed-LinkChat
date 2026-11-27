#!/usr/bin/env bash
# Start relay nodes. With no arguments, starts all of them.
#
#   scripts/up-relay-node.sh          every node, 0 to RELAY_NODES-1
#   scripts/up-relay-node.sh 3 5      just those two
#
# A relay node is capacity, not a relay: it offers one relay_slot and runs no
# code of ours. Which relay actor lands on it is Ray's decision.
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker
require_network
require_image

indices=("$@")
if [ ${#indices[@]} -eq 0 ]; then
  mapfile -t indices < <(seq 0 $((RELAY_NODES - 1)))
fi

for index in "${indices[@]}"; do
  name="${RELAY_PREFIX}-${index}"
  if container_running "${name}"; then
    echo "${name} already running"
    continue
  fi
  docker rm -f "${name}" >/dev/null 2>&1 || true

  mapfile -t common < <(docker_run_common)
  docker run "${common[@]}" \
    --name "${name}" \
    --hostname "${name}" \
    --label linkchat.role=relay \
    "${IMAGE}" relay >/dev/null

  echo "started ${name}"
done
