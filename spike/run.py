"""The spike harness. Answers the questions in docs/research/07-ray.md s.12.

Run from inside the cluster, normally through scripts/spike.sh:

    create     make the probe actors
    info       print every probe's current incarnation and node
    checks     the measurements that need no container kill
    watch N    poll probe N and print every change, with timings
    nodes      memory and process counts per node
    teardown   remove the probes

The kill-based measurements need two terminals, or scripts/spike.sh, because
one side kills a container while the other watches. Each check prints what it
observed and what the design assumed, so a disagreement is visible rather than
buried.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any

import ray

from .probe import NAMESPACE, create, probe_name

PROBE_COUNT = 5
ENVELOPE_SIZE = 4096
"""Matches linkchat.core.envelope.ENVELOPE_BUDGET, deliberately duplicated so
the spike does not import the application."""


def connect() -> None:
    ray.init(address="auto", namespace=NAMESPACE, log_to_driver=False)


def stamp() -> str:
    ms = int(time.time() * 1000) % 1000
    return time.strftime("%H:%M:%S", time.gmtime()) + f".{ms:03d}"


def say(text: str = "") -> None:
    print(text, flush=True)


# ---------------------------------------------------------------------------


def cmd_create() -> None:
    handles = [create(i) for i in range(PROBE_COUNT)]
    say(f"created or fetched {len(handles)} probes in namespace {NAMESPACE}")
    for index, handle in enumerate(handles):
        try:
            info = ray.get(handle.info.remote(), timeout=30)
        except Exception as exc:
            say(f"  {probe_name(index)}: not ready ({type(exc).__name__}: {exc})")
            say("     If this stays pending, no node has a free relay_slot.")
            say("     That is itself a spike answer: write it down.")
            continue
        say(f"  {probe_name(index)}: node {info['node_id'][:12]} pid {info['pid']}")


def cmd_info() -> None:
    for index in range(PROBE_COUNT):
        try:
            handle = ray.get_actor(probe_name(index), namespace=NAMESPACE)
            info = ray.get(handle.info.remote(), timeout=10)
        except Exception as exc:
            say(f"{probe_name(index)}: {type(exc).__name__}: {exc}")
            continue
        say(f"{probe_name(index)}: {json.dumps(info)}")


def cmd_nodes() -> None:
    """Spike item 7: how heavy is a Ray node, and how many fit here."""
    say("=== nodes, from ray.nodes() ===")
    alive = 0
    for node in ray.nodes():
        if not node.get("Alive"):
            continue
        alive += 1
        resources = node.get("Resources", {})
        object_store_mib = int(resources.get("object_store_memory", 0)) // (1024 * 1024)
        say(
            f"  {node['NodeID'][:12]}  {node.get('NodeManagerAddress', '?'):<16}"
            f" cpus={resources.get('CPU', 0):<4}"
            f" relay_slot={resources.get('relay_slot', 0):<4}"
            f" object_store={object_store_mib}MiB"
        )
    say(f"alive nodes: {alive}")
    say()
    say("For memory per node, run on the host (the figure the plan needs):")
    say("  docker stats --no-stream --format 'table {{.Name}}\\t{{.MemUsage}}'")

# ---------------------------------------------------------------------------


def check_handle_survives_restart() -> None:
    """Spike item 3, the process-death half.

    The client holds one gateway per relay name. If a handle stops working
    after a restart, the gateway has to re-resolve the name on every error,
    which changes the adapter in task R4.
    """
    say("--- item 3: does a handle survive a restart? ---")
    handle = ray.get_actor(probe_name(0), namespace=NAMESPACE)
    before = ray.get(handle.info.remote(), timeout=10)

    ray.kill(handle, no_restart=False)
    say(f"{stamp()} killed the actor, keeping the same handle")

    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            after = ray.get(handle.info.remote(), timeout=5)
        except Exception as exc:
            say(f"{stamp()}   {type(exc).__name__}")
            time.sleep(0.5)
            continue
        elapsed = time.time() - (deadline - 60)
        say(f"{stamp()} the old handle works again after {elapsed:.1f}s")
        say(f"  incarnation {before['incarnation'][:8]} -> {after['incarnation'][:8]}")
        say(f"  node        {before['node_id'][:12]} -> {after['node_id'][:12]}")
        say(f"  same node:  {before['node_id'] == after['node_id']}")
        say("  ASSUMED: it restarts and the handle keeps working.")
        return
    say("the old handle never worked again within 60s. Record this: it would")
    say("mean the gateway must re-resolve the name after every failure.")


def check_what_a_caller_sees() -> None:
    """Spike item 4. This number sets the client's per-call timeout."""
    say()
    say("--- item 4: what does a caller see, and how fast? ---")
    handle = ray.get_actor(probe_name(1), namespace=NAMESPACE)
    ray.get(handle.ping.remote(), timeout=10)

    ray.kill(handle, no_restart=False)
    started = time.time()
    first_error: str | None = None
    errors: list[str] = []

    while time.time() - started < 30:
        try:
            ray.get(handle.ping.remote(), timeout=2)
        except Exception as exc:
            name = type(exc).__name__
            if first_error is None:
                first_error = name
                say(f"  first error after {time.time() - started:.2f}s: {name}")
            if name not in errors:
                errors.append(name)
            time.sleep(0.25)
            continue
        say(f"  recovered after {time.time() - started:.2f}s")
        break

    say(f"  exceptions seen, in order: {errors}")
    say("  ASSUMED: ActorUnavailableError or ActorDiedError, within a few seconds.")
    say("  NOTE: whichever it is, the call may still have run. Idempotence is")
    say("        what makes that safe (fault-tolerance.md case 1).")


def check_background_loop() -> None:
    """Spike item 5. The TTL sweeper depends on this working."""
    say()
    say("--- item 5: does a background loop run inside the actor? ---")
    handle = ray.get_actor(probe_name(2), namespace=NAMESPACE)
    first = ray.get(handle.info.remote(), timeout=10)
    say(f"  started from the constructor: {first['loop_started_in_init']}")
    if not first["loop_started_in_init"]:
        say(f"  the constructor could not: {first['loop_init_error']}")
        started = ray.get(handle.ensure_loop.remote(), timeout=10)
        say(f"  started from a method instead: {started}")
        say("  CONSEQUENCE: the relay needs a kick after creation, so bootstrap")
        say("               must call a start method rather than only create.")

    time.sleep(3)
    later = ray.get(handle.info.remote(), timeout=10)
    advanced = later["loop_rounds"] - first["loop_rounds"]
    say(f"  rounds in ~3s: {advanced} (expect about 6 at 0.5s per round)")
    say(f"  loop error: {later['loop_error']}")
    say("  ASSUMED: it runs unattended, with no traffic to drive it.")


def check_concurrency_groups() -> None:
    """Spike item 6. Parked waiters must not starve client calls."""
    say()
    say("--- item 6: do parked waiters block client calls? ---")
    handle = ray.get_actor(probe_name(3), namespace=NAMESPACE)

    parked = [handle.wait_for_mail.remote(5.0) for _ in range(40)]
    time.sleep(0.5)

    started = time.time()
    try:
        ray.get(handle.ping.remote(), timeout=5)
        say(f"  a client call went through in {time.time() - started:.3f}s with "
            f"{len(parked)} callers parked")
        say("  ASSUMED: yes, because they are in different concurrency groups.")
    except Exception as exc:
        say(f"  the client call failed: {type(exc).__name__}: {exc}")
        say("  CONSEQUENCE: the group limits are wrong, or groups do not work")
        say("               the way the design assumes. This blocks R3.")

    ray.get(handle.store.remote("wake", b"x" * 16), timeout=5)
    done, _pending = ray.wait(parked, num_returns=len(parked), timeout=8)
    say(f"  parked calls that returned after a store: {len(done)}/{len(parked)}")


def check_inline_envelopes() -> None:
    """Spike item 9. An envelope must never become an object-store entry."""
    say()
    say("--- item 9: do envelope-sized arguments travel inline? ---")
    handle = ray.get_actor(probe_name(4), namespace=NAMESPACE)
    blob = b"x" * ENVELOPE_SIZE
    for index in range(200):
        ray.get(handle.store.remote(f"k{index}", blob), timeout=10)
    say(f"  sent 200 arguments of {ENVELOPE_SIZE} bytes")
    say("  Now check the object store on the host:")
    say("    docker exec linkchat-head ray memory --stats-only")
    say("  ASSUMED: no new objects. Ray inlines arguments under about 100 KB,")
    say("           and an object in the store dies with the sender, which is")
    say("           why envelopes must not go there.")


def cmd_checks() -> None:
    check_handle_survives_restart()
    check_what_a_caller_sees()
    check_background_loop()
    check_concurrency_groups()
    check_inline_envelopes()
    say()
    say("Items 1, 2, 7, 8, 10, 11 and 12 need container kills or host commands.")
    say("See spike/README.md, and write every answer into docs/research/07-ray.md.")


# ---------------------------------------------------------------------------


def cmd_watch(index: int) -> None:
    """Spike items 1 and 2: watch one probe across a container or process kill.

    Start this, then kill the container in another terminal. Every transition
    is printed with a timestamp, which is where the restart time comes from.
    """
    name = probe_name(index)
    say(f"watching {name}. Kill its container or its process now.")
    say("Ctrl-C to stop.")
    say()

    last: str | None = None
    lost_at: float | None = None

    while True:
        try:
            handle = ray.get_actor(name, namespace=NAMESPACE)
            info = ray.get(handle.info.remote(), timeout=3)
            state = f"up inc={info['incarnation'][:8]} node={info['node_id'][:12]}"
            if lost_at is not None:
                say(f"{stamp()} back after {time.time() - lost_at:.1f}s")
                lost_at = None
        except Exception as exc:
            state = f"down {type(exc).__name__}"
            if lost_at is None:
                lost_at = time.time()

        if state != last:
            say(f"{stamp()} {state}")
            last = state
        time.sleep(0.25)


def cmd_teardown() -> None:
    for index in range(PROBE_COUNT):
        try:
            ray.kill(ray.get_actor(probe_name(index), namespace=NAMESPACE))
            say(f"removed {probe_name(index)}")
        except Exception:
            pass


COMMANDS: dict[str, Any] = {
    "create": cmd_create,
    "info": cmd_info,
    "checks": cmd_checks,
    "nodes": cmd_nodes,
    "teardown": cmd_teardown,
}


def main(argv: list[str]) -> int:
    if not argv or argv[0] in {"-h", "--help", "help"}:
        say(__doc__ or "")
        return 0

    command = argv[0]
    connect()

    if command == "watch":
        index = int(argv[1]) if len(argv) > 1 else 0
        try:
            cmd_watch(index)
        except KeyboardInterrupt:
            say("\nstopped")
        return 0

    action = COMMANDS.get(command)
    if action is None:
        say(f"unknown command: {command}")
        say(__doc__ or "")
        return 64
    action()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
