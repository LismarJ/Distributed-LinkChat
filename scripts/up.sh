#!/usr/bin/env bash
# Bring the whole cluster up, in order, waiting at each step. Task S6.
#
# Every piece of this exists as its own script, because the course rule is one
# `docker run` per container and no Compose. What was missing was the ordering
# and the waiting, and both matter more than they look: a rehearsal that starts
# against a half-built cluster fails for a reason that has nothing to do with
# the system, and that is the worst way to lose a demonstration.
#
# So each step polls for a condition rather than sleeping for a guess. The
# conditions are the honest ones:
#
#   head      `ray status` answers inside the container
#   nodes     the head has seen as many nodes as we started
#   relays    all five relay actors report READY through a client's /api/relays
#   clients   each client answers GET /api/me on its published port
#
# The relay wait is the one that is easy to get wrong. A relay actor exists as
# soon as the bootstrap returns, but it comes up SYNCING and refuses client work
# until it has pulled its share from its peers (R6). Waiting for "the actor
# exists" would hand the demonstration a cluster that refuses the first message.
#
# Idempotent: every step is a no-op if it is already done, so this is also the
# right thing to run after a scenario has killed half the cluster.
#
#   scripts/up.sh                 bring everything up
#   scripts/up.sh --build         rebuild the image first
#   scripts/up.sh --clients 3     three clients instead of two
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

HERE="$(dirname "$0")"

CLIENTS="${LINKCHAT_CLIENTS:-2}"
FORCE_BUILD=0

HEAD_TIMEOUT_S="${LINKCHAT_HEAD_TIMEOUT_S:-120}"
NODE_TIMEOUT_S="${LINKCHAT_NODE_TIMEOUT_S:-120}"
RELAY_TIMEOUT_S="${LINKCHAT_RELAY_TIMEOUT_S:-180}"
CLIENT_TIMEOUT_S="${LINKCHAT_CLIENT_TIMEOUT_S:-120}"
POLL_INTERVAL_S="${LINKCHAT_POLL_INTERVAL_S:-2}"

usage() {
  cat >&2 <<TEXT
usage: $(basename "$0") [--build] [--clients N]

  --build       rebuild the image even if it already exists
  --clients N   how many client containers to start (default ${CLIENTS})
  --help        this text

Brings up, in order: network, image, head, ${RELAY_NODES} relay nodes, the five
relay actors, and N clients. Waits for each to be ready before the next.
Safe to run again at any time.
TEXT
}

while [ $# -gt 0 ]; do
  case "$1" in
    --build) FORCE_BUILD=1; shift ;;
    --clients) CLIENTS="${2:?--clients needs a number}"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 64 ;;
  esac
done

step() { echo; echo "=== $* ==="; }
note() { echo "  - $*"; }

# wait_for <description> <timeout_s> <command...>
wait_for() {
  local what="$1" timeout="$2"; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      echo >&2
      echo "timed out after ${timeout}s waiting for: ${what}" >&2
      echo "Look at scripts/status.sh to see where it stopped." >&2
      exit 1
    fi
    sleep "${POLL_INTERVAL_S}"
  done
  note "ready: ${what}"
}

# JSON parsed by the Python inside the head container, so this script needs
# nothing on the host but curl and docker (the host's `python` is often the
# Windows Store stub). Same approach as scripts/scenario.sh.
pyjson() { docker exec -i "${HEAD_NAME}" python -c "
import json, sys
data = json.load(sys.stdin)
$1
"; }

client_port() { echo $((CLIENT_BASE_PORT + $1)); }

# -- 1. network -------------------------------------------------------------

step "network"
"${HERE}/net-up.sh"

# -- 2. image ---------------------------------------------------------------

step "image"
if [ "${FORCE_BUILD}" -eq 1 ]; then
  "${HERE}/build.sh"
elif docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  note "${IMAGE} already exists (pass --build to rebuild)"
else
  "${HERE}/build.sh"
fi

# -- 3. head ----------------------------------------------------------------

step "head node"
"${HERE}/up-head.sh"
head_answers() { docker exec "${HEAD_NAME}" ray status; }
wait_for "the head's Ray instance" "${HEAD_TIMEOUT_S}" head_answers

# -- 4. relay nodes ---------------------------------------------------------

step "relay nodes (${RELAY_NODES})"
"${HERE}/up-relay-node.sh"

# Count ALIVE nodes the head knows about: the head itself plus the relay nodes.
# `ray list nodes` is the stable state CLI (ADR-0004), so it is what we parse.
nodes_joined() {
  local alive
  alive="$(docker exec "${HEAD_NAME}" ray list nodes --format json 2>/dev/null |
    pyjson "print(sum(1 for n in data if n.get('state') == 'ALIVE'))")"
  [ -n "${alive}" ] && [ "${alive}" -ge "$((RELAY_NODES + 1))" ]
}
wait_for "${RELAY_NODES} relay nodes to join" "${NODE_TIMEOUT_S}" nodes_joined

# -- 5. the relay actors ----------------------------------------------------

step "relay actors"
"${HERE}/bootstrap.sh"

# -- 6. clients -------------------------------------------------------------

step "clients (${CLIENTS})"
indices=()
for ((i = 0; i < CLIENTS; i++)); do indices+=("${i}"); done
"${HERE}/up-client.sh" "${indices[@]}"

for index in "${indices[@]}"; do
  port="$(client_port "${index}")"
  client_answers() { curl --silent --fail --connect-timeout 2 --max-time 10 \
    "http://localhost:${port}/api/me"; }
  wait_for "$(echo "${CLIENT_PREFIX}-${index}") on port ${port}" \
    "${CLIENT_TIMEOUT_S}" client_answers
done

# -- 7. all five relays READY ----------------------------------------------
#
# Asked through a client rather than the head, because what matters is that a
# *client* can see them serving. A relay that exists but is still SYNCING would
# refuse the demonstration's first message.

step "waiting for every relay to serve"
first_port="$(client_port "${indices[0]}")"
relays_ready() {
  curl --silent --fail --connect-timeout 2 --max-time 10 \
    "http://localhost:${first_port}/api/relays" |
    pyjson "
states = [r['state'] for r in data]
sys.exit(0 if states and all(s == 'ready' for s in states) else 1)
"
}
wait_for "all relays READY" "${RELAY_TIMEOUT_S}" relays_ready

# -- summary ----------------------------------------------------------------

echo
echo "=== the cluster is up ==="
docker ps --filter "network=${NETWORK}" --format 'table {{.Names}}\t{{.Status}}'
echo
echo "  Ray dashboard   http://localhost:${DASHBOARD_PORT}"
for index in "${indices[@]}"; do
  echo "  client ${index} UI      http://localhost:$(client_port "${index}")"
done
echo
echo "Next: open two client UIs, add each other as contacts (each UI shows its"
echo "own user id at the top), then run a failure scenario:"
echo "  scripts/scenario.sh --list"
echo "  scripts/scenario.sh two-nodes"
