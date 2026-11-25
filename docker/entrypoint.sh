#!/usr/bin/env bash
# One entrypoint, one role per container. No Docker Compose anywhere.
#
# Every node joins the cluster by being given the head's container name, which
# Docker's own DNS resolves on a user-defined network. That name is the only
# address configured anywhere in this project.
set -euo pipefail

HEAD_HOST="${LINKCHAT_HEAD_HOST:-linkchat-head}"
HEAD_PORT="${LINKCHAT_HEAD_PORT:-6379}"
HEAD_ADDRESS="${HEAD_HOST}:${HEAD_PORT}"

# Envelopes travel inline, so the object store can be small. The real minimum
# is item 8 of the spike.
OBJECT_STORE_MEMORY="${LINKCHAT_OBJECT_STORE_MEMORY:-134217728}"

role="${1:-help}"
shift || true

case "${role}" in

  head)
    # --num-cpus=0 and no relay_slot, so Ray places nothing of ours on the head.
    # The examiners have put head failure out of scope (ADR-0012), so there is
    # no Redis and no restart supervision here.
    exec ray start --head \
      --port="${HEAD_PORT}" \
      --num-cpus=0 \
      --dashboard-host=0.0.0.0 \
      --object-store-memory="${OBJECT_STORE_MEMORY}" \
      --disable-usage-stats \
      --block
    ;;

  relay)
    # A relay node is capacity, not a relay. It offers exactly one relay_slot,
    # which is how at most one relay actor ever lands on it (ADR-0011). It runs
    # no application code at start: the actors are created once, by bootstrap.
    exec ray start \
      --address="${HEAD_ADDRESS}" \
      --num-cpus=1 \
      --resources='{"relay_slot": 1}' \
      --object-store-memory="${OBJECT_STORE_MEMORY}" \
      --disable-usage-stats \
      --block
    ;;

  client)
    # No CPUs and no relay_slot, so Ray schedules nothing here. The client runs
    # as a driver on this node, which is why it is not a Ray Client connection:
    # that mode is discouraged for long-running programs and drops the session
    # after 30 seconds offline.
    ray start \
      --address="${HEAD_ADDRESS}" \
      --num-cpus=0 \
      --object-store-memory="${OBJECT_STORE_MEMORY}" \
      --disable-usage-stats
    # find_spec rather than an import: importing a __main__ module would run it,
    # and then `python -m` would run it a second time.
    if python -c "import importlib.util,sys; sys.exit(0 if importlib.util.find_spec('linkchat.client_app.__main__') else 1)" 2>/dev/null; then
      exec python -m linkchat.client_app "$@"
    fi
    echo "linkchat: node joined the cluster, but linkchat.client_app is missing" >&2
    echo "          from this image, so this container is only a Ray node." >&2
    exec sleep infinity
    ;;

  spike)
    # Joins as a relay-capable node and hands over to the spike harness.
    ray start \
      --address="${HEAD_ADDRESS}" \
      --num-cpus=1 \
      --resources='{"relay_slot": 1}' \
      --object-store-memory="${OBJECT_STORE_MEMORY}" \
      --disable-usage-stats
    exec python -m spike.run "$@"
    ;;

  shell)
    exec bash
    ;;

  help | *)
    cat <<'TEXT'
linkchat <role>

  head     Ray head node: cluster directory and dashboard. Never killed.
  relay    A relay node: one relay_slot of capacity for a relay actor.
  client   A client node, plus the client program and its browser UI.
  spike    A relay node, then the spike harness (docs/research/07-ray.md s.12).
  shell    An interactive shell in the image.

The head's address comes from LINKCHAT_HEAD_HOST (default linkchat-head).
TEXT
    [ "${role}" = "help" ] && exit 0 || exit 64
    ;;
esac
