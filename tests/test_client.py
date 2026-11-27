"""Two clients exchange end-to-end encrypted messages through the (fake) relays."""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from linkchat.core.client.client import ChatClient
from linkchat.core.client.history import History, MessageState
from linkchat.core.client.mailbox_ids import mailbox_for
from linkchat.core.clock import SystemClock
from linkchat.core.constants import RELAY_NAMES
from linkchat.core.crypto.acks import identity_signer, sign_ack
from linkchat.core.crypto.sessions import NoBundle, RelaysUnavailable
from linkchat.core.envelope import ENVELOPE_SIZE, envelope_id
from linkchat.core.payload import Clock2, Payload
from linkchat.core.ports import CredentialRejected, Record
from linkchat.core.replication import SendResult, send_to_three
from linkchat.core.ring import primaries
from linkchat.core.types import Credential, RelayName

from .fakes.transport import FakeCluster


class Relays:
    """The fake cluster, plus a log of every record a client stored on it."""

    def __init__(self) -> None:
        self.cluster = FakeCluster()
        self.stored: list[Record] = []

    async def naive_send(self, record: Record) -> SendResult:
        """Stores on the three primaries only. Enough until R5 exists."""
        self.stored.append(record)
        directory = self.cluster.directory()
        names = primaries(record.mailbox_id)
        await asyncio.gather(*(directory.gateway(n).store(record) for n in names))
        return SendResult(confirmed_relays=tuple(names))

    async def hold_only(self, record: Record) -> SendResult:
        """Confirms without storing: the test places the record itself."""
        self.stored.append(record)
        return SendResult(confirmed_relays=tuple(primaries(record.mailbox_id)))


def make_client(path: Path, relays: Relays, send=None) -> ChatClient:
    return ChatClient(
        History(path),
        relays.cluster.directory(),
        relays.cluster.clock,
        send=send or relays.naive_send,
    )


async def two_friends(tmp_path: Path, relays: Relays) -> tuple[ChatClient, ChatClient]:
    alice = make_client(tmp_path / "alice.db", relays)
    bob = make_client(tmp_path / "bob.db", relays)
    alice.add_contact(bob.me)
    bob.add_contact(alice.me)
    await alice.publish()
    await bob.publish()
    return alice, bob


async def until(condition: Callable[[], bool], limit_s: float = 5.0) -> None:
    deadline = time.monotonic() + limit_s
    while not condition():
        assert time.monotonic() < deadline, "timed out waiting"
        await asyncio.sleep(0.01)


def texts(client: ChatClient, peer) -> list[str]:
    return [m.text for m in client.history.messages(peer)]


# -- the ordinary path --------------------------------------------------------------


async def test_contacts_use_one_mailbox_per_direction(tmp_path) -> None:
    relays = Relays()
    alice, bob = await two_friends(tmp_path, relays)

    eid = await alice.send(bob.me, "hola")

    [item] = alice.history.due_outbox(now=1e12)
    assert item.envelope_id == eid
    assert item.mailbox_id == mailbox_for(alice.me, bob.me)
    assert alice.history.recv_mailbox(bob.me) == mailbox_for(bob.me, alice.me)


async def test_full_encrypted_exchange_through_relays(tmp_path) -> None:
    relays = Relays()
    alice, bob = await two_friends(tmp_path, relays)
    await alice.start()
    await bob.start()

    await alice.send(bob.me, "hola bob")
    await until(lambda: texts(bob, alice.me) == ["hola bob"])
    await bob.send(alice.me, "hola alice")
    await until(lambda: texts(alice, bob.me) == ["hola bob", "hola alice"])
    await until(lambda: alice.history.next_due() is None)
    await alice.stop()
    await bob.stop()

    states = {m.text: m.state for m in alice.history.messages(bob.me)}
    assert states == {"hola bob": MessageState.SENT, "hola alice": MessageState.RECEIVED}


async def test_relays_only_ever_see_fixed_size_ciphertext(tmp_path) -> None:
    relays = Relays()
    alice, bob = await two_friends(tmp_path, relays)
    secrets = ["short", "a much longer message " * 20, "hola bob"]

    for text in secrets:
        await alice.send(bob.me, text)
    await alice.outbox.run_once()

    assert len(relays.stored) == len(secrets)
    for record in relays.stored:
        assert len(record.envelope) == ENVELOPE_SIZE
        assert not any(text.encode()[:8] in record.envelope for text in secrets)


async def test_the_first_message_needs_the_peers_bundle(tmp_path) -> None:
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays)
    bob = make_client(tmp_path / "bob.db", relays)  # has not published yet
    alice.add_contact(bob.me)

    with pytest.raises(NoBundle):
        await alice.send(bob.me, "anyone?")
    assert texts(alice, bob.me) == []

    await bob.publish()
    await alice.send(bob.me, "now")
    assert texts(alice, bob.me) == ["now"]


async def test_no_relay_answering_is_not_mistaken_for_no_bundle(tmp_path) -> None:
    """Every relay down: still a `NoBundle` to old callers, but a distinct one."""
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays)
    bob = make_client(tmp_path / "bob.db", relays)
    alice.add_contact(bob.me)
    await bob.publish()
    relays.cluster.kill(*RELAY_NAMES)

    with pytest.raises(RelaysUnavailable) as raised:
        await alice.send(bob.me, "anyone?")
    assert isinstance(raised.value, NoBundle)
    assert texts(alice, bob.me) == []

    relays.cluster.revive(*RELAY_NAMES)
    await alice.send(bob.me, "now")
    assert texts(alice, bob.me) == ["now"]


# -- restarts and backlogs ------------------------------------------------------------


async def test_a_restart_mid_conversation(tmp_path) -> None:
    relays = Relays()
    alice, bob = await two_friends(tmp_path, relays)
    await alice.send(bob.me, "before")
    await alice.outbox.run_once()
    await bob.receiver.receive_once(alice.me)
    await bob.send(alice.me, "reply")
    await bob.outbox.run_once()
    await alice.receiver.receive_once(bob.me)

    alice.history.close()
    alice = make_client(tmp_path / "alice.db", relays)  # same file, new process

    await alice.send(bob.me, "after")
    await alice.outbox.run_once()
    await bob.receiver.receive_once(alice.me)
    assert texts(bob, alice.me) == ["before", "reply", "after"]
    assert len(alice.channel.sessions.sessions_with(bob.me)) == 1


async def test_a_backlog_is_read_in_ratchet_order(tmp_path) -> None:
    """Bob was offline for 30 messages, and the relays hand them back newest first."""
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays, send=relays.hold_only)
    bob = make_client(tmp_path / "bob.db", relays)
    alice.add_contact(bob.me)
    bob.add_contact(alice.me)
    await bob.publish()
    for n in range(30):
        await alice.send(bob.me, f"m{n:02d}")
    await alice.outbox.run_once()

    directory = relays.cluster.directory()
    for record in reversed(relays.stored):
        for name in primaries(record.mailbox_id):
            await directory.gateway(name).store(record)

    assert await bob.receiver.receive_once(alice.me) == 30
    assert texts(bob, alice.me) == [f"m{n:02d}" for n in range(30)]


async def test_a_held_message_is_announced_again_when_released(tmp_path) -> None:
    """A message held behind a lost one shows by itself later: the UI must hear of it."""
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays, send=relays.hold_only)
    # A real clock: the manual one would jump the hold the moment anyone sleeps.
    bob = ChatClient(
        History(tmp_path / "bob.db"),
        relays.cluster.directory(),
        SystemClock(),
        send=relays.naive_send,
        hold_timeout_s=0.3,
    )
    alice.add_contact(bob.me)
    bob.add_contact(alice.me)
    await bob.publish()
    await alice.send(bob.me, "lost")
    await alice.send(bob.me, "held")
    await alice.outbox.run_once()
    held = relays.stored[1]  # "lost" never reaches a relay
    for name in primaries(held.mailbox_id):
        await relays.cluster.directory().gateway(name).store(held)
    events = bob.events.subscribe()

    assert await bob.receiver.receive_once(alice.me) == 1
    assert bob.ordered_messages(alice.me) == []  # held, waiting for "lost"
    first = events.get_nowait()

    second = await asyncio.wait_for(events.get(), 2.0)
    shown = bob.ordered_messages(alice.me)
    assert (first.envelope_id, second.envelope_id) == (held.envelope_id,) * 2
    assert [(d.message.text, d.after_gap) for d in shown] == [("held", True)]


# -- someone else in the mailbox ---------------------------------------------------------


async def test_mallory_cannot_write_as_alice(tmp_path) -> None:
    """Mallory puts a valid envelope, sealed for Bob, in Alice's mailbox to him."""
    relays = Relays()
    alice, bob = await two_friends(tmp_path, relays)
    mallory = make_client(tmp_path / "mallory.db", relays)
    mallory.add_contact(bob.me)
    forged = await mallory.channel.sessions.seal(
        bob.me, Payload(text="it's alice", clock=Clock2(1, 0), prev=None, sent_at=0.0)
    )
    mailbox = mailbox_for(alice.me, bob.me)
    record = Record(mailbox, envelope_id(forged), forged, relays.cluster.clock.now() + 60)
    for name in primaries(mailbox):
        await relays.cluster.directory().gateway(name).store(record)

    assert await bob.receiver.receive_once(alice.me) == 0
    assert texts(bob, alice.me) == []
    first = primaries(mailbox)[0]
    assert relays.cluster.stores[first].has(mailbox, record.envelope_id)  # not acked


async def test_mallory_knows_both_ids_but_cannot_delete_bobs_mail(tmp_path) -> None:
    """The attack ADR-0007 left open: compute the mailbox, then ack it early."""
    relays = Relays()
    alice, bob = await two_friends(tmp_path, relays)
    mallory = make_client(tmp_path / "mallory.db", relays)
    await alice.send(bob.me, "for Bob only")
    await alice.outbox.run_once()
    mailbox = mailbox_for(alice.me, bob.me)  # public: both ids are enough
    eids = [record.envelope_id for record in relays.stored]
    forged = sign_ack(identity_signer(mallory.identity), alice.me, mailbox, eids)

    for name in primaries(mailbox):
        with pytest.raises(CredentialRejected):
            await relays.cluster.directory().gateway(name).ack(mailbox, forged, eids)

    assert await bob.receiver.receive_once(alice.me) == 1
    assert texts(bob, alice.me) == ["for Bob only"]
    for name in primaries(mailbox):  # and Bob's own, signed ack did clear it
        assert (
            await relays.cluster.directory().gateway(name).fetch(mailbox, Credential(b""))
            == []
        )


# -- lifecycle ---------------------------------------------------------------


async def test_stop_cancels_every_task(tmp_path) -> None:
    relays = Relays()
    alice, _bob = await two_friends(tmp_path, relays)
    await alice.start()
    carol = make_client(tmp_path / "carol.db", relays)
    alice.add_contact(carol.me)  # added while running: listened to at once
    tasks = list(alice._tasks.values())
    assert len(tasks) == 4  # outbox, republisher, two receivers

    await alice.stop()
    assert all(t.done() for t in tasks)


async def test_exchange_with_two_relays_down(tmp_path) -> None:
    """With the real three-copy send: two relays down, still delivered and `sent`."""
    relays = Relays()
    send = functools.partial(
        send_to_three, directory=relays.cluster.directory(), clock=relays.cluster.clock
    )
    alice = make_client(tmp_path / "alice.db", relays, send=send)
    bob = make_client(tmp_path / "bob.db", relays, send=send)
    alice.add_contact(bob.me)
    bob.add_contact(alice.me)
    await bob.publish()
    relays.cluster.kill(RelayName("relay-0"), RelayName("relay-3"))

    await alice.send(bob.me, "hola")
    await alice.outbox.run_once()
    await bob.receiver.receive_once(alice.me)

    assert texts(bob, alice.me) == ["hola"]
    assert alice.history.messages(bob.me)[0].state is MessageState.SENT
