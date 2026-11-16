"""A stand-in for the relay actor, carrying the options the design chose.

It holds no mailboxes and implements no protocol. The point is that it is
decorated exactly as `RelayActor` will be, so that what Ray does to it is what
Ray will do to a relay.

It reports an `incarnation`: a fresh random value generated every time the
constructor runs. That is how the harness tells a restart from a survival
without depending on
`ray.get_runtime_context().was_current_actor_reconstructed`, which we are not
certain exists in the version we will pin.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any
from uuid import uuid4

import ray

NAMESPACE = "linkchat-spike"

LOOP_INTERVAL_S = 0.5


@ray.remote(
    num_cpus=0,
    resources={"relay_slot": 1},
    max_restarts=-1,
    max_task_retries=2,
    concurrency_groups={"client": 8, "waiters": 100},
)
class Probe:
    """Same decorator options as the planned relay, minus the behaviour."""

    def __init__(self) -> None:
        self.incarnation = uuid4().hex
        self.created_at = time.time()
        self.node_id = ray.get_runtime_context().get_node_id()
        self.pid = os.getpid()

        self.loop_rounds = 0
        self.loop_last_ok = 0.0
        self.loop_error: str | None = None

        self._mail = asyncio.Event()
        self._stored: dict[str, int] = {}
        self._loop_task: asyncio.Task[None] | None = None

        # Spike item 5. The design needs a TTL sweeper and a handoff loop
        # running inside the actor with no traffic to drive them. Ray's
        # out-of-band page starts such a task from an `async def __init__`, but
        # does not say whether a *sync* constructor of an async actor has a
        # running loop. Rather than assume, try it and record what happened.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            self.loop_started_in_init = False
            self.loop_init_error = f"{type(exc).__name__}: {exc}"
        else:
            self._loop_task = loop.create_task(self._background_loop())
            self.loop_started_in_init = True
            self.loop_init_error = None

    async def _background_loop(self) -> None:
        """Stands in for the expiry and handoff loops."""
        while True:
            try:
                await asyncio.sleep(LOOP_INTERVAL_S)
                self.loop_rounds += 1
                self.loop_last_ok = time.time()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - spike code
                self.loop_error = f"{type(exc).__name__}: {exc}"

    @ray.method(concurrency_group="client")
    async def info(self) -> dict[str, Any]:
        """Everything the harness needs to know about this incarnation."""
        return {
            "incarnation": self.incarnation,
            "created_at": self.created_at,
            "node_id": self.node_id,
            "pid": self.pid,
            "loop_started_in_init": self.loop_started_in_init,
            "loop_init_error": self.loop_init_error,
            "loop_rounds": self.loop_rounds,
            "loop_last_ok": self.loop_last_ok,
            "loop_error": self.loop_error,
            "stored": len(self._stored),
        }

    @ray.method(concurrency_group="client")
    async def ensure_loop(self) -> bool:
        """Start the background loop if the constructor could not.

        Returns True if a loop is running by the time this returns.
        """
        if self._loop_task is None or self._loop_task.done():
            self._loop_task = asyncio.get_running_loop().create_task(
                self._background_loop()
            )
        return not self._loop_task.done()

    @ray.method(concurrency_group="client")
    async def store(self, key: str, blob: bytes) -> int:
        """Take a fixed-size value by value, as a relay takes an envelope.

        Spike item 9 watches the object store while this is called, to confirm
        an envelope of our size travels inline and never becomes an object.
        """
        self._stored[key] = len(blob)
        self._mail.set()
        return len(blob)

    @ray.method(concurrency_group="waiters")
    async def wait_for_mail(self, timeout_s: float) -> bool:
        """Park on an event, as the relay will for a connected recipient.

        In its own concurrency group, so that spike item 6 can check that many
        parked callers do not use up the capacity `store` needs.
        """
        self._mail.clear()
        try:
            await asyncio.wait_for(self._mail.wait(), timeout=timeout_s)
        except asyncio.TimeoutError:
            return False
        return True

    @ray.method(concurrency_group="client")
    async def ping(self) -> float:
        return time.time()


def probe_name(index: int) -> str:
    return f"probe-{index}"


def create(index: int) -> Any:
    """Create or fetch probe `index`, the way the bootstrap will."""
    return Probe.options(  # type: ignore[attr-defined]
        name=probe_name(index),
        namespace=NAMESPACE,
        lifetime="detached",
        get_if_exists=True,
    ).remote()
