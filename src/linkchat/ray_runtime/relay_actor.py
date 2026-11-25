"""The Ray actor that gives a `MailboxStore` a network and a supervisor. Task R3.

This is the thin Ray wrapper described in `docs/design/ray-architecture.md`
section 3. Everything about *what* is kept and for how long lives in
`linkchat.core.mailbox` (R2); this file only exposes that store as actor methods,
runs the background loops, and tracks the SYNCING/READY lifecycle.

**Status: pre-spike (2026-11-15).** The decorator numbers and the assumption
that Ray restarts a dead actor onto a spare node are exactly what gate G1 checks
(the S4 spike). Nothing here has run against Ray. If the spike disproves the
restart assumption, ADR-0011 changes and so does this file. Signatures are still
subject to ADR-0007 (the `credential`) and ADR-0008 (the actor interface).

**Wired in 2026-11-24.** The repair *orchestration* now runs here, over the
Ray-free mechanisms in `linkchat.core.repair`:

- **R6, sync on restart.** `_resync` calls `core.repair.resync` against the peer
  view, then flips to READY. Until this was wired the actor went READY straight
  away, which made a restarted relay serve an empty mailbox — exactly the
  "readers miss mail" failure `fault-tolerance.md` section 6 case 3 says we pay
  the sync time to avoid.
- **R7, hinted handoff.** `_handoff_loop` calls `core.repair.handoff_round` on a
  timer, returning covered records to primaries that came back.
- **R8, loop supervision.** Every background loop runs under `_supervise`, which
  catches an exception, counts it, restarts the round, and records the last
  successful round in `status`. An exception in an asyncio task does not stop the
  actor, so a loop that died silently would otherwise be invisible
  (`fault-tolerance.md` case 15).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable

import ray

from linkchat.core.clock import SystemClock
from linkchat.core.constants import RELAY_COUNT, RELAY_NAMES
from linkchat.core.mailbox import MailboxStore, StoreOutcome
from linkchat.core.ports import (
    MailboxFull,
    PullResult,
    Record,
    RelayPeers,
    RelayState,
    RelayStatus,
    RelaySyncing,
    Tombstone,
)
from linkchat.core.repair import handoff_round, resync
from linkchat.core.types import Credential, EnvelopeId, MailboxId, RelayName, UserId

log = logging.getLogger(__name__)

TTL_SWEEP_INTERVAL_S = 1.0
"""How often the TTL sweeper runs. A starting value for the spike; R8 tunes it
against how quickly expiry needs to show in the demonstration."""

HANDOFF_INTERVAL_S = 2.0
"""How often a stand-in tries to return hinted records to their primary (R7).

Short enough that the demonstration shows a record going home within a few
seconds of a relay returning, long enough that five relays retrying an
unreachable primary is not a busy loop.
"""

RESYNC_RETRY_INTERVAL_S = 2.0
"""How long to wait before trying a failed or empty-handed sync again.

Short, because the relay refuses every client call until the sync completes, so
this interval is pure unavailability.
"""

MAX_RESYNC_ATTEMPTS = 15
"""How many sync attempts before the relay gives up on its own process.

Fifteen attempts at `RESYNC_RETRY_INTERVAL_S` is about thirty seconds, which has
to cover the one legitimately slow case: at bootstrap this actor may exist before
its peers do, so the first attempts find nothing to talk to and `ray.get_actor`
raises for every name. Thirty seconds is far longer than the measured bring-up
and still short enough that a genuinely broken relay is restarted rather than
left refusing calls.
"""

MAX_LOOP_FAILURES = 10
"""Consecutive failed rounds before a loop gives up on its own process.

`fault-tolerance.md` section 3 says a loop that fails repeatedly should take the
actor down so Ray restarts it clean, rather than leave a relay that looks alive
while one of its duties is dead. Ten rounds is enough to ride out a transient
peer failure and few enough to surface a real bug during a demonstration.
"""


@ray.remote(
    num_cpus=0,
    resources={"relay_slot": 1},
    max_restarts=-1,
    max_task_retries=2,
    concurrency_groups={"client": 256, "waiters": 1000, "repair": 16},
)
class RelayActor:
    """One logical relay: `relay-0` .. `relay-(R-1)`.

    The name is the relay's identity on the ring and never changes; which
    container runs it is Ray's business. Created detached by `bootstrap.py` so it
    outlives any driver.

    Memory only. A restart runs this constructor again with nothing restored, so
    the relay comes up `SYNCING`, refills from peers (R6), and only then serves
    clients. Durability is the three copies, not this process.
    """

    async def __init__(  # type: ignore[misc]  # async __init__ is a G1 spike item
        self, index: int, ring_size: int = RELAY_COUNT
    ) -> None:
        # Whether Ray awaits an `async def __init__` on an async actor is itself
        # a spike item; if it does not, this moves to a `start()` method the
        # bootstrap calls once. Kept as the design wrote it for now.
        if ring_size != RELAY_COUNT:
            raise ValueError(
                f"ring_size {ring_size} != RELAY_COUNT {RELAY_COUNT}; the ring is "
                "fixed in phase 1 (constants.py)"
            )
        if not 0 <= index < ring_size:
            raise IndexError(f"relay index {index} outside 0..{ring_size - 1}")

        self._name: RelayName = RELAY_NAMES[index]
        self._clock = SystemClock()
        self._store = MailboxStore(self._name, self._clock)
        self._prekeys: dict[UserId, bytes] = {}

        # One event per mailbox with a parked reader. `store`/`handoff` set it to
        # wake the parked `wait_for_mail` without polling (the "waiters" group).
        self._waiters: dict[MailboxId, asyncio.Event] = {}

        # Loop name -> clock time it last completed a round. A loop that dies
        # silently (an unhandled exception in a task does not stop the actor)
        # would otherwise be invisible; `status` exposes this. R8 hardens it.
        self._loops_last_ok: dict[str, float] = {}
        self._loop_failures: dict[str, int] = {}

        # Resolved lazily: building the peer view needs `ray.get_actor`, and at
        # bootstrap the other relays may not exist yet.
        self._peers_view: RelayPeers | None = None

        self._state = RelayState.SYNCING
        self._tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(self._resync()),
            asyncio.create_task(
                self._supervise("ttl_sweeper", self._sweep_once, TTL_SWEEP_INTERVAL_S)
            ),
            asyncio.create_task(
                self._supervise("handoff", self._handoff_once, HANDOFF_INTERVAL_S)
            ),
        ]

    # -- client-facing (concurrency group "client") -------------------------

    @ray.method(concurrency_group="client")
    async def store(self, record: Record) -> None:
        """Store one record, idempotently. See `ports.RelayGateway.store`.

        Raises `MailboxFull` (definite) or `RelaySyncing` (retry elsewhere).
        Storing something tombstoned or expired is a harmless no-op success:
        the caller must never resurrect it, and has nothing left to do.
        """
        self._require_ready()
        outcome = self._store.store(record)
        if outcome is StoreOutcome.REFUSED_FULL:
            raise MailboxFull(f"{self._name} is at capacity for {record.mailbox_id!r}")
        if outcome is StoreOutcome.STORED:
            self._wake(record.mailbox_id)

    @ray.method(concurrency_group="client")
    async def fetch(self, mailbox_id: MailboxId, credential: Credential) -> list[Record]:
        """Everything held for a mailbox right now. No history, by design."""
        self._require_ready()
        # TODO(ADR-0007): check `credential` before returning. Carried, not yet
        # checked, so the ports can be frozen before that decision.
        return self._store.fetch(mailbox_id)

    @ray.method(concurrency_group="client")
    async def ack(
        self,
        mailbox_id: MailboxId,
        credential: Credential,
        envelope_ids: list[EnvelopeId],
    ) -> None:
        """Replace these records with tombstones. Idempotent."""
        self._require_ready()
        self._store.ack(mailbox_id, envelope_ids)

    @ray.method(concurrency_group="client")
    async def publish_prekeys(self, user_id: UserId, bundle: bytes) -> None:
        """Store a signed prekey bundle. The owner republishes on reconnect."""
        self._require_ready()
        self._prekeys[user_id] = bundle

    @ray.method(concurrency_group="client")
    async def get_prekeys(self, user_id: UserId) -> bytes | None:
        self._require_ready()
        return self._prekeys.get(user_id)

    # -- parked calls (concurrency group "waiters") -------------------------

    @ray.method(concurrency_group="waiters")
    async def wait_for_mail(
        self, mailbox_id: MailboxId, credential: Credential, timeout_s: float
    ) -> bool:
        """Block until mail arrives for `mailbox_id` or the timeout passes.

        Returns True if mail is (or became) available, False on timeout. The
        caller fetches afterwards either way; this only avoids polling. Lives in
        its own concurrency group so thousands of parked readers cannot starve
        `store`/`fetch`.
        """
        self._require_ready()
        if self._store.fetch(mailbox_id):
            return True
        event = self._waiters.setdefault(mailbox_id, asyncio.Event())
        try:
            await asyncio.wait_for(event.wait(), timeout_s)
            return True
        except TimeoutError:
            return False
        finally:
            # Drop the event if nobody else is still waiting on it.
            if mailbox_id in self._waiters and self._waiters[mailbox_id].is_set():
                self._waiters.pop(mailbox_id, None)

    # -- relay-to-relay (concurrency group "repair") ------------------------

    @ray.method(concurrency_group="repair")
    async def pull(self, for_relay: RelayName) -> PullResult:
        """Give `for_relay` everything we hold that it should hold too.

        Answered even while this relay is itself SYNCING: it returns what it has.
        Since every acknowledged record is on three relays and at most two are
        down, at least one holder answers. Placement is computed from the fixed
        relay set, so no range has to be described in the call.
        """
        records, tombstones = self._store.share_for(for_relay)
        return PullResult(records=records, tombstones=tombstones)

    @ray.method(concurrency_group="repair")
    async def handoff(self, records: list[Record], tombstones: list[Tombstone]) -> None:
        """Absorb records a stand-in is returning, and tombstones we may lack."""
        self._store.absorb(records, tombstones)
        for mailbox_id in {r.mailbox_id for r in records}:
            self._wake(mailbox_id)

    @ray.method(concurrency_group="repair")
    async def digest(self, for_relay: RelayName) -> None:
        """Periodic digest comparison. Deferred from phase 1 (ADR-0014, R10)."""
        raise NotImplementedError("digest comparison is deferred from phase 1 (R10)")

    # -- reporting (default group) ------------------------------------------

    @ray.method(concurrency_group="repair")
    async def status(self) -> RelayStatus:
        """This relay's state and counters. Never raises `RelaySyncing`.

        Counts only: a relay must put no mailbox id or envelope content in
        anything that leaves it, because Ray forwards worker logs to the head.
        """
        counts = self._store.counts()
        return RelayStatus(
            name=self._name,
            state=self._state,
            records=counts.records,
            tombstones=counts.tombstones,
            mailboxes=counts.mailboxes,
            hinted_records=counts.hinted_records,
            loops_last_ok=dict(self._loops_last_ok),
        )

    # -- internals ----------------------------------------------------------

    def _require_ready(self) -> None:
        """Refuse client work until this relay has caught up after a restart."""
        if self._state is not RelayState.READY:
            raise RelaySyncing(f"{self._name} is still syncing")

    def _wake(self, mailbox_id: MailboxId) -> None:
        event = self._waiters.get(mailbox_id)
        if event is not None:
            event.set()

    def _peers(self) -> RelayPeers:
        """This relay's view of the others, built on first use.

        Imported here rather than at module scope so that importing the actor
        never pulls in the gateway, and resolved lazily because at bootstrap the
        peers may not exist yet.
        """
        if self._peers_view is None:
            from linkchat.ray_runtime.gateway import RayRelayDirectory

            self._peers_view = RayRelayDirectory().peers(self._name)
        return self._peers_view

    async def _resync(self) -> None:
        """Refill this relay's share from its peers, then go READY. R6.

        The mechanism itself is Ray-free (`core.repair.resync`): pull from every
        peer everything this relay should hold, and absorb it. Only then does the
        relay serve clients, because serving while empty would make a reader
        believe a mailbox is empty when it is not (`fault-tolerance.md` case 3).

        **READY requires a sync that actually happened**, which means at least one
        peer answered. That is stricter than it first looks, and the reason is the
        read-correctness argument in `fault-tolerance.md` section 5. That argument
        permits a READY relay not to hold a record in exactly two cases: it was
        unreachable when the record was written, which counts against the limit of
        two bad relays, or "it synced before becoming READY". A relay that went
        READY *without* syncing belongs to neither: it is short of records, looks
        perfectly healthy, and is not counted as one of the two failures. It would
        quietly invalidate the argument rather than break it visibly.

        So a failed or empty-handed sync is retried instead of waved through, and a
        relay that still cannot sync after `MAX_RESYNC_ATTEMPTS` takes its own
        process down so Ray restarts it clean -- the escalation
        `fault-tolerance.md` section 3 already prescribes for a background loop
        that keeps failing. Refusing clients while SYNCING costs availability;
        serving an unsynced mailbox costs correctness, and only one of those is
        recoverable.

        Note that "no peer answered" is not "the peers had nothing". A fresh
        cluster syncs successfully and stores nothing, because all four peers
        answer with empty results: that is a completed sync and goes READY.
        """
        peers = self._peers()
        expected = len(peers.peer_names)
        for attempt in range(1, MAX_RESYNC_ATTEMPTS + 1):
            try:
                answered = await resync(self._name, self._store, peers, clock=self._clock)
                if answered > 0 or expected == 0:
                    self._loops_last_ok["resync"] = self._clock.now()
                    self._state = RelayState.READY
                    return
                log.warning(
                    "%s: sync attempt %d/%d reached none of its %d peers; retrying",
                    self._name,
                    attempt,
                    MAX_RESYNC_ATTEMPTS,
                    expected,
                )
            except Exception:
                # `resync` swallows per-peer RelayErrors, so reaching here means a
                # bug or a Ray-level surprise. Either way it is not a reason to
                # start serving: log it and fall through to the retry. The
                # logging is the point -- the other end of this loop is
                # `os._exit`, and exiting without a recorded cause would make a
                # relay that never syncs impossible to diagnose.
                log.exception(
                    "%s: sync attempt %d/%d raised; retrying",
                    self._name,
                    attempt,
                    MAX_RESYNC_ATTEMPTS,
                )
            await asyncio.sleep(RESYNC_RETRY_INTERVAL_S)

        # Still unsynced after every attempt. Staying SYNCING for ever would
        # strand this relay as a healthy-looking actor that refuses every call,
        # so hand the problem to the supervisor that can actually fix it by
        # starting us over.
        log.error(
            "%s: giving up after %d sync attempts; exiting so Ray restarts this "
            "actor clean. The attempt logs above say why each one failed.",
            self._name,
            MAX_RESYNC_ATTEMPTS,
        )
        os._exit(1)

    async def _sweep_once(self) -> None:
        """One TTL round: drop everything past its time."""
        self._store.expire()

    async def _handoff_once(self) -> None:
        """One handoff round: return covered records to primaries that are back. R7."""
        await handoff_round(self._name, self._store, self._peers(), clock=self._clock)

    async def _supervise(
        self, name: str, round_fn: Callable[[], Awaitable[None]], interval_s: float
    ) -> None:
        """Run one background loop forever, restarting the round on failure. R8.

        An unhandled exception in an asyncio task does not stop a Ray actor, so a
        loop that died would leave a relay that answers calls while silently
        doing none of its upkeep. Every round is therefore guarded: a failure is
        counted and the loop continues, and `status` reports the last round that
        actually completed so a dead loop is visible rather than invisible.

        After `MAX_LOOP_FAILURES` consecutive failures the actor takes its own
        process down so Ray restarts it clean (`fault-tolerance.md` section 3). A
        relay that comes back SYNCING and refills is a better state than one that
        looks healthy with a broken duty.
        """
        while True:
            try:
                await asyncio.sleep(interval_s)
                await round_fn()
            except asyncio.CancelledError:
                raise
            except Exception:
                failures = self._loop_failures.get(name, 0) + 1
                self._loop_failures[name] = failures
                if failures >= MAX_LOOP_FAILURES:
                    # Abnormal exit on purpose: Ray's supervision is the
                    # recovery path, and a clean return would just end the loop.
                    os._exit(1)
                continue
            self._loops_last_ok[name] = self._clock.now()
            self._loop_failures[name] = 0
