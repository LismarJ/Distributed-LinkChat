#!/usr/bin/env bash
# Kill a relay actor's worker process without killing its container.
#
#   scripts/kill-relay-process.sh linkchat-relay-0 [more...]
#
# This is the other half of the two-failure requirement. Killing a container
# tests "the node died"; killing the process inside tests "the process died",
# which Ray handles differently: it restarts the actor on the same node instead
# of placing it elsewhere. Both have to work, and the examiners named processes
# and nodes separately.
#
# Ray renames an actor's worker process to ray::<ClassName>..., which is what
# we match on.
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

PATTERN="${LINKCHAT_ACTOR_PATTERN:-ray::RelayActor}"

if [ $# -eq 0 ]; then
  echo "usage: $(basename "$0") <container> [container...]" >&2
  exit 64
fi

missing=0
for name in "$@"; do
  if ! container_running "${name}"; then
    echo "${name} is not running" >&2
    exit 1
  fi
  if ! docker exec "${name}" pgrep -f "${PATTERN}" >/dev/null 2>&1; then
    echo "${name}: no process matching '${PATTERN}'" >&2
    missing=1
  fi
done

if [ "${missing}" -eq 1 ]; then
  echo >&2
  echo "No relay actor is running yet. The actor class is task R3, and the" >&2
  echo "bootstrap that creates the actors is scripts/bootstrap.sh." >&2
  exit 1
fi

for name in "$@"; do
  docker exec "${name}" pkill -9 -f "${PATTERN}"
  echo "killed ${PATTERN} in ${name}"
done
echo
echo "Ray should restart each actor in place. It comes back empty and must"
echo "copy its share from its peers before serving (status SYNCING, then READY)."
