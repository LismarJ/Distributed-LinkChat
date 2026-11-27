"""An in-process implementation of the relay ports that can be told to fail. C2.

This is the harness the replication and repair mechanisms (R5-R7) are written
against. It is far easier to script than killing containers: a test can say
"kill relays 1 and 3 after the second call" and watch the core behave exactly as
it would on the real cluster.

It is deliberately *real where it matters*: every relay is backed by a genuine
`linkchat.core.mailbox.MailboxStore`, so the store logic under test is the
production one. Only the transport around it is fake, and only the transport is
where faults are injected:

- **kill / revive** a relay — calls to it raise `RelayUnreachable` (or
  `RelayGone`), as a client would see for a dead or unreachable actor;
- **restart** a relay — a new empty store and `SYNCING` state, exactly what Ray
  does when it reruns an actor's constructor;
- **drop / delay / duplicate** individual calls — for the randomized fuzzing
  (S5). Reordering is achieved with per-relay delays plus the manual clock:
  completion order, not call order, is what the sender reacts to.

Pure Python, no Ray. Runs under plain `pytest`.
"""

from __future__ import annotations

from collections.abc import Sequence

from linkchat.core.constants import RELAY_NAMES
from linkchat.core.crypto.bundle import check_published_bundle
from linkchat.core.mailbox import MailboxStore, StoreOutcome
from linkchat.core.ports import (
    Clock,
    MailboxFull,
    PullResult,
    Record,
    RelayDirectory,
    RelayGateway,
    RelayGone,
    RelayPeers,
    RelayState,
    RelayStatus,
    RelaySyncing,
    RelayUnreachable,
    Tombstone,
)
from linkchat.core.types import Credential, EnvelopeId, MailboxId, RelayName, UserId


class ManualClock:
    """A `Clock` that only moves when a test moves it (or when `sleep` is used).

    Expiry and delays can then be tested in microseconds. Satisfies the `Clock`
    port, including `monotonic`.
    """

    def __init__(self, now: float = 1_000_000.0) -> None:
        self._now = now

    def now(self) -> float:
        return self._now

    def monotonic(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        self._now += seconds

    def advance(self, seconds: float) -> None:
        self._now += seconds


class FaultController:
    """What is currently wrong with the cluster. A test mutates this directly.

    Everything defaults to healthy. Faults are per relay name so a test can be
    precise about which relays misbehave and when.
    """

    def __init__(self) -> None:
        self.killed: set[RelayName] = set()
        """Relays that do not answer. Calls raise `RelayUnreachable`."""
        self.gone: set[RelayName] = set()
        """Relays known dead. Calls raise `RelayGone` instead of unreachable."""
        self.drop_next: dict[RelayName, int] = {}
        """Drop (raise on) the next N calls to a relay, then recover."""
        self.latency: dict[RelayName, float] = {}
        """Seconds a call to a relay takes, advancing the manual clock."""
        self.duplicate: set[RelayName] = set()
        """Relays that apply every write twice (tests idempotence)."""


class FakeCluster:
    """Five relays in one process, with a fault controller and a manual clock.

    Hand a test the `directory()` to drive the client side (R5), and
    `peers(name)` to drive a relay's view of the others (R6, R7). Mutate
    `faults` and call `kill`/`revive`/`restart` to script a scenario.
    """

    def __init__(self, clock: Clock | None = None) -> None:
        self.clock: Clock = clock or ManualClock()
        self.faults = FaultController()
        self.stores: dict[RelayName, MailboxStore] = {
            name: MailboxStore(name, self.clock) for name in RELAY_NAMES
        }
        self.state: dict[RelayName, RelayState] = {
            name: RelayState.READY for name in RELAY_NAMES
        }
        self.prekeys: dict[RelayName, dict[UserId, bytes]] = {
            name: {} for name in RELAY_NAMES
        }

    # -- scripting a scenario ----------------------------------------------

    def kill(self, *names: RelayName) -> None:
        """Make these relays unreachable (a client sees `RelayUnreachable`)."""
        self.faults.killed.update(names)

    def revive(self, *names: RelayName) -> None:
        self.faults.killed.difference_update(names)
        self.faults.gone.difference_update(names)

    def restart(self, name: RelayName) -> None:
        """Simulate Ray restarting the actor: empty store, `SYNCING`, reachable.

        The relay must refill from peers (R6) and flip to READY before it serves
        clients again.
        """
        self.stores[name] = MailboxStore(name, self.clock)
        self.prekeys[name] = {}
        self.state[name] = RelayState.SYNCING
        self.faults.killed.discard(name)
        self.faults.gone.discard(name)

    def set_ready(self, name: RelayName) -> None:
        self.state[name] = RelayState.READY

    # -- the ports ----------------------------------------------------------

    def directory(self) -> RelayDirectory:
        return _FakeDirectory(self)

    def peers(self, this_relay: RelayName) -> RelayPeers:
        return _FakePeers(self, this_relay)

    # -- internals used by the fake gateway/peers ---------------------------

    async def _transport(self, relay: RelayName) -> None:
        """Apply transport-level faults before a call reaches a relay's store."""
        if relay in self.faults.gone:
            raise RelayGone(f"{relay} is gone")
        if relay in self.faults.killed:
            raise RelayUnreachable(f"{relay} is unreachable")
        remaining = self.faults.drop_next.get(relay, 0)
        if remaining > 0:
            self.faults.drop_next[relay] = remaining - 1
            raise RelayUnreachable(f"{relay} dropped the call")
        latency = self.faults.latency.get(relay, 0.0)
        if latency:
            await self.clock.sleep(latency)


class _FakeDirectory(RelayDirectory):
    def __init__(self, cluster: FakeCluster) -> None:
        self._cluster = cluster

    @property
    def relay_names(self) -> Sequence[RelayName]:
        return RELAY_NAMES

    def gateway(self, name: RelayName) -> RelayGateway:
        if name not in RELAY_NAMES:
            raise ValueError(f"{name!r} is not one of {RELAY_NAMES}")
        return _FakeGateway(self._cluster, name)


class _FakeGateway(RelayGateway):
    """One relay's client-facing surface, backed by its real store."""

    def __init__(self, cluster: FakeCluster, name: RelayName) -> None:
        self._cluster = cluster
        self._name = name

    def _store(self) -> MailboxStore:
        return self._cluster.stores[self._name]

    def _require_ready(self) -> None:
        if self._cluster.state[self._name] is not RelayState.READY:
            raise RelaySyncing(f"{self._name} is still syncing")

    async def store(self, record: Record) -> None:
        await self._cluster._transport(self._name)
        self._require_ready()
        outcome = self._store().store(record)
        if self._name in self._cluster.faults.duplicate:
            self._store().store(record)  # idempotent: must change nothing
        if outcome is StoreOutcome.REFUSED_FULL:
            raise MailboxFull(f"{self._name} is full")

    async def fetch(
        self, mailbox_id: MailboxId, credential: Credential
    ) -> Sequence[Record]:
        await self._cluster._transport(self._name)
        self._require_ready()
        return self._store().fetch(mailbox_id)

    async def ack(
        self,
        mailbox_id: MailboxId,
        credential: Credential,
        envelope_ids: Sequence[EnvelopeId],
    ) -> None:
        await self._cluster._transport(self._name)
        self._require_ready()
        self._store().ack(mailbox_id, list(envelope_ids))

    async def wait_for_mail(
        self, mailbox_id: MailboxId, credential: Credential, timeout_s: float
    ) -> bool:
        await self._cluster._transport(self._name)
        self._require_ready()
        # No real parking in the fake: report whether mail is already here, and
        # otherwise advance the clock by the timeout so tests stay deterministic.
        if self._store().fetch(mailbox_id):
            return True
        await self._cluster.clock.sleep(timeout_s)
        return bool(self._store().fetch(mailbox_id))

    async def publish_prekeys(self, user_id: UserId, bundle: bytes) -> None:
        await self._cluster._transport(self._name)
        self._require_ready()
        check_published_bundle(user_id, bundle)  # exactly what the real relay runs
        self._cluster.prekeys[self._name][user_id] = bundle

    async def get_prekeys(self, user_id: UserId) -> bytes | None:
        await self._cluster._transport(self._name)
        self._require_ready()
        return self._cluster.prekeys[self._name].get(user_id)

    async def status(self) -> RelayStatus:
        # status never raises RelaySyncing, and is reachable even mid-sync.
        await self._cluster._transport(self._name)
        counts = self._store().counts()
        return RelayStatus(
            name=self._name,
            state=self._cluster.state[self._name],
            records=counts.records,
            tombstones=counts.tombstones,
            mailboxes=counts.mailboxes,
            hinted_records=counts.hinted_records,
            loops_last_ok={},
        )


class _FakePeers(RelayPeers):
    """What relay `this_relay` can ask of the others."""

    def __init__(self, cluster: FakeCluster, this_relay: RelayName) -> None:
        self._cluster = cluster
        self._this = this_relay

    @property
    def peer_names(self) -> Sequence[RelayName]:
        return tuple(name for name in RELAY_NAMES if name != self._this)

    async def pull(self, peer: RelayName, for_relay: RelayName) -> PullResult:
        await self._cluster._transport(peer)
        records, tombstones = self._cluster.stores[peer].share_for(for_relay)
        return PullResult(records=records, tombstones=tombstones)

    async def handoff(
        self,
        peer: RelayName,
        records: Sequence[Record],
        tombstones: Sequence[Tombstone],
    ) -> None:
        await self._cluster._transport(peer)
        self._cluster.stores[peer].absorb(list(records), list(tombstones))
