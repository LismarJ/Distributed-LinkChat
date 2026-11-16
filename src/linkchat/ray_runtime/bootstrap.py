"""Create the fixed set of relay actors, once and idempotently. Task R3.

Submitted after the head is up, with `ray job submit`, or run inside any driver.
`get_if_exists=True` makes it safe to run again at any time: an actor that is
already there is returned rather than recreated, so re-running changes nothing.

Relay nodes may be started before or after this, in any order. An actor created
before a node with a free `relay_slot` exists simply waits for one (that waiting
is a spike item, G1).

**Status: pre-spike (2026-11-15).** Not run against Ray.
"""

from __future__ import annotations

import ray

from linkchat.core.constants import RELAY_COUNT, RELAY_NAMES
from linkchat.core.types import RelayName

NAMESPACE = "linkchat"
"""The one namespace every relay and every client agrees on, so that
`ray.get_actor(name, namespace=NAMESPACE)` resolves the same actor everywhere."""


def bootstrap(ring_size: int = RELAY_COUNT) -> dict[RelayName, "ray.actor.ActorHandle"]:
    """Ensure `relay-0 .. relay-(ring_size-1)` exist, and return their handles.

    Idempotent. Creating an actor that already exists returns the existing one
    (`get_if_exists=True`), so a second call is a no-op that just hands back the
    same five handles.

    Done when (R3): five relays appear in `ray list actors` on five different
    nodes, and running this again changes nothing.
    """
    if ring_size != RELAY_COUNT:
        raise ValueError(
            f"ring_size {ring_size} != RELAY_COUNT {RELAY_COUNT}; the relay set is "
            "fixed in phase 1 (constants.py)"
        )

    # Imported here, not at module top, so this module can be read and its
    # docstring checked without Ray resolving the decorated class.
    from linkchat.ray_runtime.relay_actor import RelayActor

    handles: dict[RelayName, ray.actor.ActorHandle] = {}
    for index in range(ring_size):
        name = RELAY_NAMES[index]
        handles[name] = RelayActor.options(
            name=name,
            namespace=NAMESPACE,
            lifetime="detached",
            get_if_exists=True,
        ).remote(index, ring_size)
    return handles


def main() -> None:
    """Entry point for `ray job submit -- python -m linkchat.ray_runtime.bootstrap`."""
    ray.init(address="auto", namespace=NAMESPACE)
    handles = bootstrap()
    print(f"bootstrap: ensured {len(handles)} relays: {', '.join(handles)}")


if __name__ == "__main__":
    main()
