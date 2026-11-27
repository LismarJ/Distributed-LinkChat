#!/usr/bin/env bash
# Bring up a cluster and run the automated half of the spike.
#
# The kill-based measurements need two terminals and a human watching the
# timings, so they are not in here. spike/README.md has the procedure.
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

"$(dirname "$0")/net-up.sh"
require_image
"$(dirname "$0")/up-head.sh"

echo "waiting for the head to accept connections..."
for _ in $(seq 1 30); do
  if docker exec "${HEAD_NAME}" ray status >/dev/null 2>&1; then break; fi
  sleep 1
done

"$(dirname "$0")/up-relay-node.sh"

echo "waiting for relay nodes to join..."
sleep 5
docker exec "${HEAD_NAME}" ray status || true

echo
echo "=== object store before ==="
docker exec "${HEAD_NAME}" ray memory --stats-only || true

echo
docker exec "${HEAD_NAME}" python -m spike.run create
echo
docker exec "${HEAD_NAME}" python -m spike.run checks

echo
echo "=== object store after (item 9: no new objects is the answer we want) ==="
docker exec "${HEAD_NAME}" ray memory --stats-only || true

echo
echo "=== nodes (item 7) ==="
docker exec "${HEAD_NAME}" python -m spike.run nodes
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}' || true

cat <<'TEXT'

Still to do by hand, items 1 and 2, which are the ones that can change the
design. See spike/README.md:

  terminal A: docker exec linkchat-head python -m spike.run watch 0
  terminal B: scripts/kill-nodes.sh <the container holding probe-0>

Write every answer into docs/research/07-ray.md section 12.
TEXT
