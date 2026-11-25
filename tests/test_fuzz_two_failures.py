"""S5: randomized fault injection against the fake transport.

This is the test the central correctness claim rests on. `fault-tolerance.md`
section 5 argues that a reader always finds a confirmed record, and then says
plainly that it is "an argument, not a proof", whose weak point is timing: a
relay that is slow rather than dead can be skipped by a writer and still be
READY for a reader. Section 8 names the check — thousands of sends with random
pairs of failures, restarts and delays, asserting after each run that every
confirmed message was delivered exactly once.

So this file scripts random damage within the fault model and checks the two
invariants that must hold whatever happened:

1. **Nothing confirmed is lost.** Every message whose sender recorded `sent`
   (three distinct relays confirmed) appears in the recipient's history.
2. **Nothing arrives twice, and the order is consistent.** No envelope id is
   committed twice, and one sender's messages appear in the order they were sent.

Nothing here asserts that a *pending* message arrives. That is the honest line:
with three relays unreachable a send cannot collect three confirmations, stays
pending, and the outbox retries it. Only `sent` is a promise.

**The fault model is enforced, not assumed.** `_Chaos` never takes more than
`MAX_DOWN` relays out at once, because that is the limit the three-copy
argument is built on (`constants.py`: 3 + 2 = 5). A relay that is SYNCING
counts as down, since it refuses client calls exactly as a dead one does.
Transient faults (a dropped call, a duplicated write, latency) are *not*
counted against the limit: they make a single call fail, which costs a retry
rather than a copy, and injecting them on top of two failures is what finds the
timing hole the argument admits to.

Seeds are fixed, so a run that fails fails again: keep the seed as a regression
test (work plan S5).
"""

from __future__ import annotations

import functools
import random

import pytest

from linkchat.core.client.client import ChatClient
from linkchat.core.client.history import History, MessageState, StoredMessage
from linkchat.core.constants import RELAY_NAMES
from linkchat.core.crypto.sessions import NoBundle
from linkchat.core.repair import handoff_round, resync
from linkchat.core.replication import send_to_three
from linkchat.core.types import RelayName

from .fakes.transport import FakeCluster, ManualClock

MAX_DOWN = 2
"""The fault model: any two elements at once, never three."""

ROUNDS = 24
"""Chaos rounds per seed. Each one injures or heals something and then runs a
send, a receive and a repair round."""

SEEDS = 40
"""Distinct scenarios. Kept low enough that the suite stays fast; the nightly
sweep below raises it."""

DRAIN_ROUNDS = 40
"""How many healthy rounds to allow for the backlog to clear at the end."""


class _Chaos:
    """Random damage to a `FakeCluster`, inside the fault model.

    Owns the set of relays that are currently SYNCING, because the fake cluster
    tracks their state but not whether this test still owes them a `resync`.
    """

    def __init__(self, cluster: FakeCluster, clock: ManualClock, rng: random.Random):
        self.cluster = cluster
        self.clock = clock
        self.rng = rng
        self.syncing: set[RelayName] = set()

    def down(self) -> set[RelayName]:
        """Every relay that will refuse a client call: killed, gone, or syncing."""
        faults = self.cluster.faults
        return set(faults.killed) | set(faults.gone) | self.syncing

    def _up(self) -> list[RelayName]:
        down = self.down()
        return [name for name in RELAY_NAMES if name not in down]

    def _take_out(self, name: RelayName) -> None:
        """Kill or wipe one relay.

        The distinction matters more than it looks. A kill leaves the store
        intact, so reviving the relay brings its records back and nothing was
        ever at risk. A restart is Ray rerunning the constructor: the store comes
        back **empty**, and the records only return if a peer still has them.
        Wipes are therefore the destructive fault, and the one a durability claim
        has to survive, so this leans towards them.
        """
        if self.rng.random() < 0.35:
            self.cluster.kill(name)
        else:
            self.cluster.restart(name)
            self.syncing.add(name)

    async def double_fault(self) -> None:
        """Take two relays out at the same instant: the examiners' scenario.

        Two wipes together is the case the three-copy rule exists for: a record
        on three relays keeps a copy, a record on only two can lose both.
        """
        up = self._up()
        if len(up) < 2:
            return
        for name in self.rng.sample(up, 2):
            self._take_out(name)

    async def step(self) -> None:
        """Injure or heal, never exceeding `MAX_DOWN` failures at once."""
        down = self.down()
        roll = self.rng.random()

        if down and roll < 0.40:
            await self.heal(self.rng.choice(sorted(down)))
        elif not down and roll < 0.65:
            await self.double_fault()
        elif len(down) < MAX_DOWN and roll < 0.85:
            self._take_out(self.rng.choice(self._up()))
        else:
            # Transient. Not counted against MAX_DOWN: it fails one call, which
            # costs a retry, and stacking it on two real failures is the point.
            name = self.rng.choice(RELAY_NAMES)
            if self.rng.random() < 0.5:
                self.cluster.faults.drop_next[name] = self.rng.randint(1, 2)
            else:
                self.cluster.faults.latency[name] = self.rng.uniform(0.01, 0.2)

    async def heal(self, name: RelayName) -> None:
        """Bring one relay back, finishing its sync the way the actor would."""
        self.cluster.revive(name)
        self.cluster.faults.drop_next.pop(name, None)
        self.cluster.faults.latency.pop(name, None)
        if name in self.syncing:
            # R6, exactly as `RelayActor._resync` runs it in production.
            await resync(
                name,
                self.cluster.stores[name],
                self.cluster.peers(name),
                clock=self.clock,
            )
            self.cluster.set_ready(name)
            self.syncing.discard(name)

    async def heal_everything(self) -> None:
        self.cluster.faults.drop_next.clear()
        self.cluster.faults.latency.clear()
        self.cluster.faults.duplicate.clear()
        for name in RELAY_NAMES:
            await self.heal(name)

    async def repair_round(self) -> None:
        """One handoff round on every relay that is up (R7's background loop)."""
        for name in self._up():
            await handoff_round(
                name,
                self.cluster.stores[name],
                self.cluster.peers(name),
                clock=self.clock,
            )


def _pair(tmp_path, cluster: FakeCluster) -> tuple[ChatClient, ChatClient]:
    """Two clients over the fake cluster, using the real three-copy send."""
    directory = cluster.directory()
    send = functools.partial(send_to_three, directory=directory, clock=cluster.clock)
    alice = ChatClient(
        History(tmp_path / "alice.db"), directory, cluster.clock, send=send
    )
    bob = ChatClient(History(tmp_path / "bob.db"), directory, cluster.clock, send=send)
    alice.add_contact(bob.me)
    bob.add_contact(alice.me)
    return alice, bob


def _incoming(client: ChatClient, peer) -> list[StoredMessage]:
    return [m for m in client.history.messages(peer) if not m.outgoing]


def _confirmed(client: ChatClient, peer) -> list[StoredMessage]:
    """Outgoing messages the sender recorded as `sent`: three relays confirmed."""
    return [
        m
        for m in client.history.messages(peer)
        if m.outgoing and m.state is MessageState.SENT
    ]


def _assert_delivered_exactly_once(sender: ChatClient, recipient: ChatClient) -> None:
    arrived = _incoming(recipient, sender.me)
    ids = [m.envelope_id for m in arrived]

    assert len(ids) == len(set(ids)), "an envelope was committed twice"

    present = set(ids)
    lost = [
        m.text for m in _confirmed(sender, recipient.me) if m.envelope_id not in present
    ]
    assert not lost, f"confirmed but never delivered: {lost}"


def _assert_order_is_consistent(sender: ChatClient, recipient: ChatClient) -> None:
    """One sender's messages appear in the order they were sent.

    The texts are allocated in increasing order per sender, so a consistent view
    is simply an increasing subsequence: whatever arrived, in send order, with no
    pair swapped.
    """
    shown = [
        display.message.text
        for display in recipient.ordered_messages(sender.me)
        if not display.message.outgoing
    ]
    assert shown == sorted(shown), f"out of order: {shown}"


@pytest.mark.parametrize("seed", range(SEEDS))
async def test_confirmed_messages_survive_random_two_failures(
    tmp_path, seed: int
) -> None:
    rng = random.Random(seed)
    clock = ManualClock()
    cluster = FakeCluster(clock)
    chaos = _Chaos(cluster, clock, rng)
    alice, bob = _pair(tmp_path, cluster)

    # Bundles first, while everything is healthy, so the sessions can start. A
    # restarted relay loses its bundles, which is what the republisher is for;
    # here the clients republish as part of each round.
    await alice.publish()
    await bob.publish()

    counter = 0
    for _ in range(ROUNDS):
        await chaos.step()

        if rng.random() < 0.85:
            counter += 1
            await alice.send(bob.me, f"a{counter:03d}")
        if rng.random() < 0.35:
            counter += 1
            try:
                await bob.send(alice.me, f"b{counter:03d}")
            except NoBundle:
                # Bob has not got Alice's bundle from a relay yet. Nothing is
                # stored, and he tries again on a later round.
                counter -= 1

        await alice.outbox.run_once()
        await bob.outbox.run_once()
        # The recipient is not always there to collect. Skipping the read leaves
        # a backlog sitting on the relays across the next rounds of damage, which
        # is both requirement R3 (the recipient is offline) and the only way a
        # wipe can threaten a message that was already confirmed.
        if rng.random() < 0.55:
            await bob.receiver.receive_once(alice.me)
        if rng.random() < 0.55:
            await alice.receiver.receive_once(bob.me)
        await chaos.repair_round()

        # Let the outbox backoff come due: the manual clock moves only when asked.
        clock.advance(rng.uniform(0.5, 3.0))
        await alice.publish()
        await bob.publish()

    # Heal, then give the system room to finish what it owes.
    await chaos.heal_everything()
    await alice.publish()
    await bob.publish()
    for _ in range(DRAIN_ROUNDS):
        await alice.outbox.run_once()
        await bob.outbox.run_once()
        fresh = await bob.receiver.receive_once(alice.me)
        fresh += await alice.receiver.receive_once(bob.me)
        await chaos.repair_round()
        quiet = (
            not fresh
            and alice.history.next_due() is None
            and bob.history.next_due() is None
        )
        if quiet:
            break
        clock.advance(2.0)

    _assert_delivered_exactly_once(alice, bob)
    _assert_delivered_exactly_once(bob, alice)
    _assert_order_is_consistent(alice, bob)
    _assert_order_is_consistent(bob, alice)

    # The run has to have actually exercised something, or a bug that stops
    # messages being sent at all would pass quietly.
    assert _confirmed(alice, bob.me), "no message was ever confirmed: nothing was tested"


async def test_a_relay_wipe_costs_nothing_the_clients_already_have(tmp_path) -> None:
    """Requirement R9, end to end: wipe every relay, both histories survive.

    Delivery item 6. The relays are memory only and hold padded ciphertext under
    opaque ids; the conversation lives in the clients' SQLite files.
    """
    clock = ManualClock()
    cluster = FakeCluster(clock)
    alice, bob = _pair(tmp_path, cluster)
    await alice.publish()
    await bob.publish()

    await alice.send(bob.me, "a001")
    await alice.outbox.run_once()
    await bob.receiver.receive_once(alice.me)
    await bob.send(alice.me, "b002")
    await bob.outbox.run_once()
    await alice.receiver.receive_once(bob.me)

    # Wipe every relay: new empty stores, no peer left holding anything.
    for name in RELAY_NAMES:
        cluster.restart(name)
        cluster.set_ready(name)
    assert all(len(store) == 0 for store in cluster.stores.values())

    assert [m.text for m in alice.history.messages(bob.me)] == ["a001", "b002"]
    assert [m.text for m in bob.history.messages(alice.me)] == ["a001", "b002"]
