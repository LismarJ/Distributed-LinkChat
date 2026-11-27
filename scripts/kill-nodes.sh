#!/usr/bin/env bash
# Kill containers simultaneously. This is the two-failure test.
#
#   scripts/kill-nodes.sh linkchat-relay-1 linkchat-relay-3
#
# One docker call, so the two containers go down together rather than one after
# the other. Killing them one at a time would let the system repair between the
# two and would not test what the examiners asked about.
#
# It refuses to kill the head: that is out of scope for phase 1 (ADR-0012), and
# doing it by accident mid-demonstration would end the cluster.
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

if [ $# -eq 0 ]; then
  echo "usage: $(basename "$0") <container> [container...]" >&2
  echo "hint: scripts/status.sh lists the running containers" >&2
  exit 64
fi

for name in "$@"; do
  if [ "${name}" = "${HEAD_NAME}" ]; then
    echo "refusing to kill the head node: out of scope in phase 1 (ADR-0012)." >&2
    echo "If you really mean to, use: docker kill ${HEAD_NAME}" >&2
    exit 1
  fi
  if ! container_running "${name}"; then
    echo "${name} is not running" >&2
    exit 1
  fi
done

echo "killing together: $*"
docker kill "$@" >/dev/null
echo "killed at $(date -u +%H:%M:%S.%3N) UTC"
echo
echo "Ray should now notice, then restart each lost relay actor on a node with"
echo "a free relay_slot. Watch it with: scripts/status.sh"
