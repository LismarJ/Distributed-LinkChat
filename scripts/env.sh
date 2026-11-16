#!/usr/bin/env bash
# Shared settings, sourced by every other script. Not meant to be run.

# Git Bash on Windows rewrites arguments that look like Unix paths into
# C:/... before docker sees them, which breaks container paths and Ray's
# resource JSON. This turns that off for the whole script.
export MSYS_NO_PATHCONV=1

IMAGE="${LINKCHAT_IMAGE:-linkchat:dev}"
NETWORK="${LINKCHAT_NETWORK:-linkchat-net}"

HEAD_NAME="${LINKCHAT_HEAD_NAME:-linkchat-head}"
RELAY_PREFIX="${LINKCHAT_RELAY_PREFIX:-linkchat-relay}"
CLIENT_PREFIX="${LINKCHAT_CLIENT_PREFIX:-linkchat-client}"

# Seven relay nodes for five relay actors: two spares, so that two killed
# containers can both be replaced without anyone starting anything (ADR-0011).
RELAY_NODES="${LINKCHAT_RELAY_NODES:-7}"

DASHBOARD_PORT="${LINKCHAT_DASHBOARD_PORT:-8265}"
CLIENT_BASE_PORT="${LINKCHAT_CLIENT_BASE_PORT:-8100}"

# Ray's object store lives in /dev/shm, and Docker's default 64 MB is too small
# for it. Both numbers are provisional: items 7 and 8 of the spike measure what
# is actually needed, and envelopes travel inline so it should be able to shrink.
SHM_SIZE="${LINKCHAT_SHM_SIZE:-1g}"
OBJECT_STORE_MEMORY="${LINKCHAT_OBJECT_STORE_MEMORY:-134217728}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is not on PATH." >&2
    echo "On Windows, start Docker Desktop and make sure its CLI is exposed to" >&2
    echo "this shell, then try again." >&2
    exit 127
  fi
}

require_network() {
  if ! docker network inspect "${NETWORK}" >/dev/null 2>&1; then
    echo "network ${NETWORK} does not exist. Run scripts/net-up.sh first." >&2
    exit 1
  fi
}

require_image() {
  if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
    echo "image ${IMAGE} does not exist. Run scripts/build.sh first." >&2
    exit 1
  fi
}

container_running() {
  [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

# Shared run options. Each container is its own `docker run`, by the course rule.
docker_run_common() {
  printf '%s\n' \
    --detach \
    --network "${NETWORK}" \
    --shm-size "${SHM_SIZE}" \
    --env "LINKCHAT_HEAD_HOST=${HEAD_NAME}" \
    --env "LINKCHAT_OBJECT_STORE_MEMORY=${OBJECT_STORE_MEMORY}"
}
