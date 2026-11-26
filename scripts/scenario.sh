#!/usr/bin/env bash
# The two-failure test, as one command per scenario. Task R11.
#
# The examiners added the requirement this script exists to check: any two
# elements can be killed at the same instant without the system being affected.
# "At the same instant" is the part that needs care. Killing two relays one
# after the other lets the system repair in between, so it tests the one-failure
# case twice and proves nothing about the two-failure case. Every scenario here
# therefore takes its two elements out in a single `docker kill`, or with one
# `pkill` per container issued back to back with nothing awaited between them.
#
# Each scenario sends a uniquely tagged message, injures the cluster, and then
# waits for the recipient to hold exactly one copy of it. Three things are
# asserted, because all three are in the delivery checklist:
#
#   * the message arrives at all (nothing confirmed is lost);
#   * it arrives exactly once (no duplicates);
#   * the messages the recipient already had are still in the same order.
#
# Then it heals and waits until all five relays are READY again, so the next run
# starts from a clean cluster. That is what makes these repeatable rather than a
# sequence that only works the first time.
#
# Polling, never a fixed sleep: Ray's restart time is the thing we are measuring
# and a sleep long enough to be safe would make the demonstration tedious, while
# a short one would make it flaky.
#
#   scripts/scenario.sh --list
#   scripts/scenario.sh two-nodes
#   scripts/scenario.sh all
set -euo pipefail
source "$(dirname "$0")/env.sh"
require_docker

# `wipe-all-relays` is last on purpose: it is the only one that takes every
# relay out at once, so it is both the slowest to heal and the one that leaves
# the cluster furthest from where it started.
SCENARIOS=(
  two-nodes
  two-processes
  one-of-each
  send-while-down
  client-mid-send
  recipient-offline
  wipe-all-relays
)

SENDER_INDEX="${LINKCHAT_SENDER_INDEX:-0}"
RECIPIENT_INDEX="${LINKCHAT_RECIPIENT_INDEX:-1}"

DELIVER_TIMEOUT_S="${LINKCHAT_DELIVER_TIMEOUT_S:-90}"
HEAL_TIMEOUT_S="${LINKCHAT_HEAL_TIMEOUT_S:-150}"
POLL_INTERVAL_S="${LINKCHAT_POLL_INTERVAL_S:-2}"

ACTOR_PATTERN="${LINKCHAT_ACTOR_PATTERN:-ray::RelayActor}"

# ---------------------------------------------------------------------------
# Plumbing
# ---------------------------------------------------------------------------

usage() {
  cat >&2 <<TEXT
usage: $(basename "$0") <scenario>

  two-nodes        kill two relay containers at the same instant
  two-processes    kill two relay worker processes at the same instant
  one-of-each      kill one relay container and one relay process together
  send-while-down  kill two relays first, then send through the stand-ins
  client-mid-send  kill the sending client and a relay while a send is in flight
  recipient-offline
                   stop the recipient's container, send to it while it is down,
                   then bring it back and check it collects its mail
  wipe-all-relays  take every relay out at once and check both clients still
                   have their full history
  all              every scenario above, in order, with a summary

  --list           print the scenario names, one per line

The cluster must already be up:
  scripts/net-up.sh && scripts/build.sh && scripts/up-head.sh
  scripts/up-relay-node.sh && scripts/bootstrap.sh
  scripts/up-client.sh ${SENDER_INDEX} ${RECIPIENT_INDEX}
TEXT
}

client_name() { echo "${CLIENT_PREFIX}-$1"; }
client_port() { echo $((CLIENT_BASE_PORT + $1)); }

note() { echo "  - $*"; }
fail() { echo "FAIL ${SCENARIO}: $*" >&2; return 1; }

# How many lines a history string holds. An empty string is zero messages, not
# one, which `wc -l` alone would get wrong for a single unterminated line and
# `grep -c ''` gets right for both.
count_lines() { [ -z "$1" ] && echo 0 || printf '%s\n' "$1" | grep -c ''; }

# One HTTP call to a client's API. Short timeouts: a client we just killed must
# report as unreachable quickly rather than hanging the scenario.
http() {
  local method="$1" port="$2" path="$3" body="${4:-}"
  if [ -n "${body}" ]; then
    curl --silent --show-error --fail-with-body \
      --connect-timeout 3 --max-time 15 \
      --request "${method}" \
      --header 'content-type: application/json' \
      --data "${body}" \
      "http://localhost:${port}${path}"
  else
    curl --silent --show-error --fail-with-body \
      --connect-timeout 3 --max-time 15 \
      --request "${method}" "http://localhost:${port}${path}"
  fi
}

# JSON is parsed by the Python already inside the head container, so this script
# needs nothing on the host but curl and docker. The head is the right container
# for it because it is the one element phase 1 guarantees stays alive
# (ADR-0012): a parser running in a relay would be killed by the scenarios.
pyjson() {
  docker exec -i "${HEAD_NAME}" python -c "
import json, sys
data = json.load(sys.stdin)
$1
"
}

# How many messages in this conversation have exactly this text.
count_text() {
  http GET "$(client_port "$1")" "/conversations/$2" |
    pyjson "print(sum(1 for m in data['messages'] if m['text'] == ${3@Q}))"
}

# The texts of a conversation, in display order, one per line.
texts_of() {
  http GET "$(client_port "$1")" "/conversations/$2" |
    pyjson "[print(m['text']) for m in data['messages']]"
}

# The delivery state the sender recorded for its copy.
state_of() {
  http GET "$(client_port "$1")" "/conversations/$2" |
    pyjson "print(next((m['state'] for m in data['messages'] if m['text'] == ${3@Q}), 'absent'))"
}

user_id_of() {
  http GET "$(client_port "$1")" /me | pyjson "print(data['user_id'])"
}

# relay name and state, space separated, one per line.
relay_states() {
  http GET "$(client_port "${SENDER_INDEX}")" /relays |
    pyjson "[print(r['name'], r['state']) for r in data]"
}

# Which relay containers are currently running a relay actor. Used so that
# "kill two relays" takes out two actual relays rather than two spare nodes,
# which would injure nothing and pass for the wrong reason.
containers_with_actors() {
  local index name
  for index in $(seq 0 $((RELAY_NODES - 1))); do
    name="${RELAY_PREFIX}-${index}"
    container_running "${name}" || continue
    if docker exec "${name}" pgrep -f "${ACTOR_PATTERN}" >/dev/null 2>&1; then
      echo "${name}"
    fi
  done
}

relay_index_of() { echo "${1##*-}"; }

# ---------------------------------------------------------------------------
# Preconditions
# ---------------------------------------------------------------------------

preflight() {
  container_running "${HEAD_NAME}" ||
    { echo "${HEAD_NAME} is not running. Run scripts/up-head.sh." >&2; exit 1; }

  local -a hosts
  mapfile -t hosts < <(containers_with_actors)
  if [ "${#hosts[@]}" -lt 3 ]; then
    echo "only ${#hosts[@]} relay actor(s) found; need at least 3 to injure two." >&2
    echo "Run scripts/up-relay-node.sh and then scripts/bootstrap.sh." >&2
    exit 1
  fi

  local index
  for index in "${SENDER_INDEX}" "${RECIPIENT_INDEX}"; do
    container_running "$(client_name "${index}")" ||
      { echo "$(client_name "${index}") is not running. Run scripts/up-client.sh ${index}." >&2; exit 1; }
    http GET "$(client_port "${index}")" /me >/dev/null ||
      { echo "$(client_name "${index}") is not answering on port $(client_port "${index}")." >&2; exit 1; }
  done

  SENDER_ID="$(user_id_of "${SENDER_INDEX}")"
  RECIPIENT_ID="$(user_id_of "${RECIPIENT_INDEX}")"

  # Idempotent: adding a contact that exists is harmless, so this can run again.
  http POST "$(client_port "${SENDER_INDEX}")" /contacts \
    "{\"user_id\":\"${RECIPIENT_ID}\"}" >/dev/null 2>&1 || true
  http POST "$(client_port "${RECIPIENT_INDEX}")" /contacts \
    "{\"user_id\":\"${SENDER_ID}\"}" >/dev/null 2>&1 || true

  # The first message to a contact needs their prekey bundle from a relay, which
  # the recipient republishes on a timer. Wait for it rather than failing a
  # scenario over a cold start.
  wait_until "both clients know each other" 60 bundles_ready
}

bundles_ready() {
  http GET "$(client_port "${SENDER_INDEX}")" /contacts |
    pyjson "sys.exit(0 if ${RECIPIENT_ID@Q} in data['contacts'] else 1)"
}

# wait_until <what> <timeout_s> <command...>
wait_until() {
  local what="$1" timeout="$2"; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    if [ "${SECONDS}" -ge "${deadline}" ]; then
      return 1
    fi
    sleep "${POLL_INTERVAL_S}"
  done
  return 0
}

all_relays_ready() {
  # The endpoint reports every relay on the ring, so "all of them are ready" is
  # "every line says ready, and there was at least one line". No count is
  # hardcoded here: the ring size lives in core/constants.py and nowhere else.
  local line count=0
  while read -r line; do
    [ -z "${line}" ] && continue
    case "${line}" in
      *" ready") count=$((count + 1)) ;;
      *) return 1 ;;
    esac
  done < <(relay_states)
  [ "${count}" -gt 0 ]
}

# ---------------------------------------------------------------------------
# The scenarios
# ---------------------------------------------------------------------------

# Kill two relay containers in ONE docker call, so they die together.
#
# Every injure_* function returns non-zero if it did not actually injure the
# cluster, and every caller treats that as a failed scenario. That is not
# defensive habit, it is a bug that happened: `kill-nodes.sh` refuses to kill a
# container that is already down and exits 1, and because its status was
# ignored the scenario carried on against an uninjured cluster. For a
# send-and-assert scenario that merely produced a confusing failure later, but
# for `wipe-all-relays` — whose assertion is "both histories are unchanged" —
# wiping nothing would have satisfied the assertion and printed PASS. A
# scenario that cannot break the cluster must never report success.
injure_two_nodes() {
  local -a hosts
  mapfile -t hosts < <(containers_with_actors)
  if [ "${#hosts[@]}" -lt 2 ]; then
    echo "need two relay containers with actors, found ${#hosts[@]}" >&2
    return 1
  fi
  KILLED_CONTAINERS=("${hosts[0]}" "${hosts[1]}")
  note "killing containers together: ${KILLED_CONTAINERS[*]}"
  "$(dirname "$0")/kill-nodes.sh" "${KILLED_CONTAINERS[@]}" >/dev/null || return 1
}

# Kill two relay worker processes, leaving their containers up. Ray restarts the
# actor on the same node rather than placing it elsewhere, which is a different
# recovery path from a dead container and has to work too.
injure_two_processes() {
  local -a hosts
  mapfile -t hosts < <(containers_with_actors)
  if [ "${#hosts[@]}" -lt 2 ]; then
    echo "need two relays with actors, found ${#hosts[@]}" >&2
    return 1
  fi
  note "killing actor processes together: ${hosts[0]} ${hosts[1]}"
  # Issued back to back with nothing awaited between them: as simultaneous as
  # two `docker exec` calls can be, and both land well inside one Ray health
  # check interval.
  #
  # Each job is waited on by pid rather than with a bare `wait`, which reports
  # only the last job's status. `pkill` exits non-zero when it matched nothing,
  # and "there was no actor process to kill" is exactly the case that must fail
  # the scenario rather than pass quietly.
  local pid_a pid_b rc=0
  docker exec "${hosts[0]}" pkill -9 -f "${ACTOR_PATTERN}" & pid_a=$!
  docker exec "${hosts[1]}" pkill -9 -f "${ACTOR_PATTERN}" & pid_b=$!
  wait "${pid_a}" || rc=1
  wait "${pid_b}" || rc=1
  return "${rc}"
}

injure_one_of_each() {
  local -a hosts
  mapfile -t hosts < <(containers_with_actors)
  if [ "${#hosts[@]}" -lt 2 ]; then
    echo "need two relays with actors, found ${#hosts[@]}" >&2
    return 1
  fi
  KILLED_CONTAINERS=("${hosts[0]}")
  note "killing container ${hosts[0]} and actor process in ${hosts[1]}"
  local pid_a pid_b rc=0
  docker kill "${hosts[0]}" >/dev/null & pid_a=$!
  docker exec "${hosts[1]}" pkill -9 -f "${ACTOR_PATTERN}" & pid_b=$!
  wait "${pid_a}" || rc=1
  wait "${pid_b}" || rc=1
  return "${rc}"
}

# Bring back whatever was killed. Processes need nothing: Ray restarts the actor
# itself, which is the behaviour under test. Containers have to be started again,
# because nothing in Docker does that for us and Compose is not allowed.
heal() {
  local name index
  for name in "${KILLED_CONTAINERS[@]:-}"; do
    [ -z "${name}" ] && continue
    index="$(relay_index_of "${name}")"
    note "restarting ${name}"
    "$(dirname "$0")/up-relay-node.sh" "${index}" >/dev/null
  done
  KILLED_CONTAINERS=()

  for name in "${KILLED_CLIENTS[@]:-}"; do
    [ -z "${name}" ] && continue
    index="$(relay_index_of "${name}")"
    note "restarting ${name}"
    "$(dirname "$0")/up-client.sh" "${index}" >/dev/null
    wait_until "${name} answering" 60 http GET "$(client_port "${index}")" /me ||
      note "warning: ${name} did not come back within 60s"
  done
  KILLED_CLIENTS=()

  if wait_until "all relays ready" "${HEAL_TIMEOUT_S}" all_relays_ready; then
    note "all five relays are READY again"
  else
    note "warning: relays did not all reach READY within ${HEAL_TIMEOUT_S}s"
    relay_states | sed 's/^/      /'
  fi
}

# ---------------------------------------------------------------------------
# Running one scenario
# ---------------------------------------------------------------------------

# Delivery checklist item 6: every relay can be wiped and both clients still
# have their full history.
#
# This has a different shape from every other scenario, which is why it gets its
# own function rather than a branch. The others send a message and assert it
# survives; here, wiping *all five* relays is outside the fault model, so a
# message still sitting on the relays would legitimately be lost and asserting
# otherwise would be wrong. What is being demonstrated is the opposite and
# stronger claim: history does not live on the relays at all. It is each
# client's own SQLite, and the relay interface has no history operation to ask
# for old mail even if someone wanted to.
#
# The assertion compares the two histories as whole strings, which catches loss,
# duplication and reordering in one comparison. Duplication is the interesting
# one: wiping the relays destroys their tombstones too, so anything that came
# back would be re-offered to the client -- and the client's persisted seen-set
# is what refuses it. That makes this a test of duplicate suppression as well.
run_wipe_all_relays() {
  local recv_before send_before recv_after send_after
  recv_before="$(texts_of "${RECIPIENT_INDEX}" "${SENDER_ID}")"
  send_before="$(texts_of "${SENDER_INDEX}" "${RECIPIENT_ID}")"
  note "recipient holds $(count_lines "${recv_before}") message(s), sender holds $(count_lines "${send_before}")"

  local -a hosts
  mapfile -t hosts < <(containers_with_actors)
  if [ "${#hosts[@]}" -eq 0 ]; then
    fail "no relay actors found to wipe"
    return 1
  fi
  note "wiping every relay at once: ${hosts[*]}"
  KILLED_CONTAINERS=("${hosts[@]}")
  # Checked, because this scenario's assertion is that the histories did *not*
  # change: a wipe that silently did nothing would satisfy it and print PASS.
  if ! "$(dirname "$0")/kill-nodes.sh" "${hosts[@]}" >/dev/null; then
    heal
    fail "could not wipe the relays; nothing was tested"
    return 1
  fi

  heal  # restarts every container and waits for all five relays to be READY

  recv_after="$(texts_of "${RECIPIENT_INDEX}" "${SENDER_ID}")"
  send_after="$(texts_of "${SENDER_INDEX}" "${RECIPIENT_ID}")"

  if [ "${recv_before}" != "${recv_after}" ]; then
    fail "the recipient's history changed: $(count_lines "${recv_before}") message(s) became $(count_lines "${recv_after}")"
    return 1
  fi
  if [ "${send_before}" != "${send_after}" ]; then
    fail "the sender's history changed: $(count_lines "${send_before}") message(s) became $(count_lines "${send_after}")"
    return 1
  fi
  note "both histories are identical after every relay was wiped"

  echo "PASS ${SCENARIO}"
  return 0
}

run_scenario() {
  SCENARIO="$1"
  KILLED_CONTAINERS=()
  KILLED_CLIENTS=()

  echo
  echo "=== ${SCENARIO} ==="

  if [ "${SCENARIO}" = "wipe-all-relays" ]; then
    run_wipe_all_relays
    return $?
  fi

  # A nonce in the text so a repeat run is distinguishable from the last one,
  # and so "exactly one copy" is a question about this run only.
  local tag="${SCENARIO}-$(date -u +%H%M%S)-$$"
  local text="scenario ${tag}"

  local -a before
  mapfile -t before < <(texts_of "${RECIPIENT_INDEX}" "${SENDER_ID}")
  note "recipient holds ${#before[@]} message(s) before this run"

  case "${SCENARIO}" in
    client-mid-send)
      local sender_container
      sender_container="$(client_name "${SENDER_INDEX}")"
      local -a hosts
      mapfile -t hosts < <(containers_with_actors)
      note "queueing durably, then killing ${sender_container} and ${hosts[0]} together"
      # Wait for the POST to return before killing anything. The response is
      # only sent after `queue_outgoing` has committed the message and its
      # outbox row in one SQLite transaction, so the client genuinely owes the
      # send when it dies.
      #
      # This was first written as a backgrounded POST killed immediately, which
      # raced the kill against the request even reaching the API. When the kill
      # won, nothing had been queued and the run proved nothing.
      #
      # **What this scenario does and does not establish.** It is a true
      # simultaneous two-element kill -- the sending client and a relay, in one
      # `docker kill` -- and it checks that the message still arrives exactly
      # once and reaches `sent`. It does NOT reliably catch the client *during*
      # its relay call. Inside the fault model a send always has three healthy
      # relays and finishes in milliseconds, while `docker kill` takes far
      # longer to land, so the outbox has usually completed by then; staging a
      # genuine mid-call crash from outside the process is not possible without
      # a test hook in the client, which is not worth putting in shipped code.
      # The mid-write durability claim is proven where it can be proven
      # deterministically: `tests/test_history.py` SIGKILLs a writer at twenty
      # random points and asserts the reopened database lost and duplicated
      # nothing, and `tests/test_outbox.py` covers finishing a send after a
      # restart.
      http POST "$(client_port "${SENDER_INDEX}")" \
        "/conversations/${RECIPIENT_ID}/messages" "{\"text\":\"${text}\"}" >/dev/null
      docker kill "${sender_container}" "${hosts[0]}" >/dev/null
      KILLED_CLIENTS=("${sender_container}")
      KILLED_CONTAINERS=("${hosts[0]}")
      note "restarting the sender so its outbox can finish the send"
      "$(dirname "$0")/up-client.sh" "${SENDER_INDEX}" >/dev/null
      KILLED_CLIENTS=()
      wait_until "sender answering" 90 http GET "$(client_port "${SENDER_INDEX}")" /me ||
        { heal; fail "the sender never came back"; return 1; }
      ;;
    recipient-offline)
      # Delivery checklist item 2, which no other scenario covers: every one of
      # them has the recipient online and polling. This is the feature the whole
      # relay tier exists for -- a message reaching someone whose container is
      # not running at the time it is sent -- so it is worth a scenario of its
      # own rather than being assumed from the unit tests.
      #
      # Note what is *not* being tested: the sender still reaches `sent` here,
      # because `sent` means three relays confirmed, which has nothing to do
      # with the recipient being up. That is the point of the state and the
      # assertion below checks it.
      local recipient_container
      recipient_container="$(client_name "${RECIPIENT_INDEX}")"
      note "stopping ${recipient_container}, then sending while it is down"
      docker kill "${recipient_container}" >/dev/null
      KILLED_CLIENTS=("${recipient_container}")
      http POST "$(client_port "${SENDER_INDEX}")" \
        "/conversations/${RECIPIENT_ID}/messages" "{\"text\":\"${text}\"}" >/dev/null
      note "sent while the recipient was down: ${text}"
      note "restarting ${recipient_container} so it can collect its mail"
      "$(dirname "$0")/up-client.sh" "${RECIPIENT_INDEX}" >/dev/null
      KILLED_CLIENTS=()
      wait_until "the recipient answering" 90 \
        http GET "$(client_port "${RECIPIENT_INDEX}")" /me ||
        { heal; fail "the recipient never came back"; return 1; }
      ;;
    send-while-down)
      # The other scenarios kill relays *after* a message is confirmed, which
      # tests that a confirmed record survives. This one is the opposite and
      # tests the write path: with two relays already gone, a send has to fall
      # through the preference list to stand-ins and still collect three
      # confirmations (task R5). Nothing is waited for between the kill and the
      # send, so the send lands inside the window where the two replacements are
      # still restarting and resyncing.
      injure_two_nodes ||
        { heal; fail "could not take two relays down; nothing was tested"; return 1; }
      note "sending with two relays down: ${text}"
      http POST "$(client_port "${SENDER_INDEX}")" \
        "/conversations/${RECIPIENT_ID}/messages" "{\"text\":\"${text}\"}" >/dev/null
      ;;
    *)
      note "sending: ${text}"
      http POST "$(client_port "${SENDER_INDEX}")" \
        "/conversations/${RECIPIENT_ID}/messages" "{\"text\":\"${text}\"}" >/dev/null
      local injured=0
      case "${SCENARIO}" in
        two-nodes)     injure_two_nodes     || injured=1 ;;
        two-processes) injure_two_processes  || injured=1 ;;
        one-of-each)   injure_one_of_each    || injured=1 ;;
      esac
      if [ "${injured}" -ne 0 ]; then
        heal
        fail "could not injure the cluster; nothing was tested"
        return 1
      fi
      ;;
  esac

  # -- the assertions ------------------------------------------------------

  arrived_once() { [ "$(count_text "${RECIPIENT_INDEX}" "${SENDER_ID}" "${text}")" = "1" ]; }

  if ! wait_until "delivery" "${DELIVER_TIMEOUT_S}" arrived_once; then
    local seen
    seen="$(count_text "${RECIPIENT_INDEX}" "${SENDER_ID}" "${text}" 2>/dev/null || echo "?")"
    heal
    fail "the message was not delivered exactly once within ${DELIVER_TIMEOUT_S}s (copies: ${seen})"
    return 1
  fi
  note "delivered exactly once"

  # The sender must also have recorded three confirmations. `sent` is the state
  # that means the distributed property held; `pending` would mean the send is
  # still being retried and the delivery above came from an earlier attempt.
  local sender_state
  sender_state="$(state_of "${SENDER_INDEX}" "${RECIPIENT_ID}" "${text}")"
  if [ "${sender_state}" != "sent" ]; then
    # Give the outbox a moment: three confirmations can land just after the
    # recipient has already read the record from the relays.
    sender_confirmed() { [ "$(state_of "${SENDER_INDEX}" "${RECIPIENT_ID}" "${text}")" = "sent" ]; }
    if ! wait_until "confirmation" 45 sender_confirmed; then
      sender_state="$(state_of "${SENDER_INDEX}" "${RECIPIENT_ID}" "${text}")"
      heal
      fail "the sender still reports '${sender_state}', not 'sent'"
      return 1
    fi
  fi
  note "the sender reports 'sent' (three relays confirmed)"

  # Nothing the recipient already had may have moved or vanished: the messages
  # from before must still be a prefix of what it holds now.
  local -a after
  mapfile -t after < <(texts_of "${RECIPIENT_INDEX}" "${SENDER_ID}")
  local i
  for i in "${!before[@]}"; do
    if [ "${before[$i]}" != "${after[$i]:-}" ]; then
      heal
      fail "the history was reordered at position ${i}: '${before[$i]}' became '${after[$i]:-}'"
      return 1
    fi
  done
  note "the earlier ${#before[@]} message(s) are unchanged and in order"

  heal
  echo "PASS ${SCENARIO}"
  return 0
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

case "${1:-}" in
  --list) printf '%s\n' "${SCENARIOS[@]}"; exit 0 ;;
  -h | --help) usage; exit 0 ;;
  "") usage; exit 64 ;;
esac

requested="$1"
if [ "${requested}" != "all" ] &&
   ! printf '%s\n' "${SCENARIOS[@]}" | grep -qx -- "${requested}"; then
  echo "unknown scenario: ${requested}" >&2
  usage
  exit 64
fi

preflight

if [ "${requested}" != "all" ]; then
  run_scenario "${requested}"
  exit $?
fi

declare -a results=()
status=0
for name in "${SCENARIOS[@]}"; do
  if run_scenario "${name}"; then
    results+=("PASS ${name}")
  else
    results+=("FAIL ${name}")
    status=1
  fi
done

echo
echo "=== summary ==="
printf '%s\n' "${results[@]}"
exit "${status}"
