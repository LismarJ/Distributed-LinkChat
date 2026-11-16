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

**What is deliberately NOT here**, because it is a separate, graded task:

- the *orchestration* of sync-on-restart (R6) — this file has the SYNCING/READY
  state machine and a `_resync` hook, but the actual pull-from-peers loop is R6;
- hinted-handoff *orchestration* (R7) — the `handoff`/`pull` methods a peer calls
  are here (they are one-liners over the store), but the background loop that
  decides when to hand records back is R7;
- metrics and loop supervision hardening (R8, R9).

Those are marked `TODO(Rn)` below.
"""

from __future__ import annotations

import asyncio

import ray

from linkchat.core.clock import SystemClock
from linkchat.core.constants import RELAY_COUNT, RELAY_NAMES
from linkchat.core.mailbox import MailboxStore, StoreOutcome
from linkchat.core.ports import (
    MailboxFull,
    PullResult,
    Record,
    RelayState,
    RelaySyncing,
    RelayStatus,
    Tombstone,
)
from linkchat.core.types import Credential, EnvelopeId, MailboxId, RelayName, UserId

TTL_SWEEP_INTERVAL_S = 1.0
"""How often the TTL sweeper runs. A starting value for the spike; R8 tunes it
against how quickly expiry needs to show in the demonstration."""


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

    async def __init__(self, index: int, ring_size: int = RELAY_COUNT) -> None:
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

        self._state = RelayState.SYNCING
        self._tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(self._ttl_sweeper()),
            asyncio.create_task(self._resync()),
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
    async def fetch(
        self, mailbox_id: MailboxId, credential: Credential
    ) -> list[Record]:
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
        except asyncio.TimeoutError:
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
    async def handoff(
        self, records: list[Record], tombstones: list[Tombstone]
    ) -> None:
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

    async def _resync(self) -> None:
        """Refill this relay's share from its peers, then go READY (R6 hook).

        TODO(R6): pull from every peer for the mailboxes this relay is a primary
        of, merging with `store.absorb`, and only then flip to READY. That loop
        needs `RayRelayPeers` (R4) and is the essential-repair task R6, so it is
        left as a hook here rather than written ahead of the spike. Until R6
        lands this flips straight to READY, which is correct ONLY for a relay
        that is genuinely the first and has nothing to pull.
        """
        self._loops_last_ok["resync"] = self._clock.now()
        self._state = RelayState.READY

    async def _ttl_sweeper(self) -> None:
        """Drop expired records and tombstones on a timer.

        A background loop so retention does not depend on anyone calling. R8
        hardens this (restart-on-exception supervision, counters); here it is the
        minimal version the actor needs to honour the TTL guarantee, with a broad
        guard so one bad round does not kill the loop silently.
        """
        while True:
            try:
                await asyncio.sleep(TTL_SWEEP_INTERVAL_S)
                self._store.expire()
                self._loops_last_ok["ttl_sweeper"] = self._clock.now()
            except asyncio.CancelledError:
                raise
            except Exception:
                # TODO(R8): count this and expose it; for now keep sweeping.
                continue
