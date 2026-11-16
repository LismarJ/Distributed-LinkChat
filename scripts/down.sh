#!/usr/bin/env bash
# Remove every container on our network. Leaves the network and client volumes.
#
#   scripts/down.sh            containers only
#   scripts/down.sh --volumes  also delete client data, which is their history
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

mapfile -t containers < <(docker ps -aq --filter "network=${NETWORK}")
if [ ${#containers[@]} -gt 0 ]; then
  docker rm -f "${containers[@]}" >/dev/null
  echo "removed ${#containers[@]} container(s)"
else
  echo "no containers on ${NETWORK}"
fi

if [ "${1:-}" = "--volumes" ]; then
  mapfile -t volumes < <(docker volume ls -q --filter "name=${CLIENT_PREFIX}-")
  if [ ${#volumes[@]} -gt 0 ]; then
    echo "about to delete client history: ${volumes[*]}"
    read -r -p "type yes to confirm: " answer
    if [ "${answer}" = "yes" ]; then
      docker volume rm "${volumes[@]}" >/dev/null
      echo "removed ${#volumes[@]} volume(s)"
    else
      echo "kept the volumes"
    fi
  fi
fi
