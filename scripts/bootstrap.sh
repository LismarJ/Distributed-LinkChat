#!/usr/bin/env bash
# Create the five relay actors. Idempotent: safe to run again at any time.
#
# Submitted as a Ray job from the head container, so the actors do not depend
# on this script's process staying alive. They are detached, which is what lets
# the job finish while they keep running.
#
# Relay nodes may be started before or after this. An actor created when no
# node has a free relay_slot waits for one rather than failing.
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

if ! container_running "${HEAD_NAME}"; then
  echo "${HEAD_NAME} is not running. Run scripts/up-head.sh first." >&2
  exit 1
fi

if ! docker exec "${HEAD_NAME}" python -c "import linkchat.ray_runtime.bootstrap" 2>/dev/null; then
  cat >&2 <<'TEXT'
linkchat.ray_runtime.bootstrap does not exist yet.

It is task R3 in docs/planning/phase-1-work-plan.md, and it is blocked on the
spike (gate G1): until we know whether Ray restarts a relay onto a spare node
the way its documentation reads, the actor options this script would pass are
not settled.

To run the spike instead:  scripts/spike.sh
TEXT
  exit 1
fi

docker exec "${HEAD_NAME}" ray job submit --no-wait -- \
  python -m linkchat.ray_runtime.bootstrap

echo "bootstrap submitted; watch it with scripts/status.sh"
