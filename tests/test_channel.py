"""Encrypted messages that survive a killed client: atomicity, rollback, lock.

Red until the persistence contract is implemented (`SecureChannel`,
`Sessions.export`/`restore`, and the four new `History` methods). Each client
is a real SQLite file; a restart is closing it and building a new client on it.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from linkchat.core.client.history import History, MessageState
from linkchat.core.client.mailbox_ids import mailbox_for
from linkchat.core.crypto import sessions as sessions_module
from linkchat.core.crypto.channel import SecureChannel
from linkchat.core.crypto.identity import load_or_create_identity
from linkchat.core.crypto.sessions import CannotOpen, Sessions
from linkchat.core.crypto.wire import FLAG_X3DH
from linkchat.core.envelope import envelope_id
from linkchat.core.types import EnvelopeId, UserId

from .test_sessions import Bundles

NOW = 1_000.0
TTL = 3_600.0


def start(path: Path, bundles: Bundles) -> SecureChannel:
    """What a client does on start: identity, publish, load sessions."""
    history = History(path)
    identity = load_or_create_identity(history)
    bundles.published[history.me] = identity.bundle
    channel = SecureChannel(history, Sessions(history.me, identity, bundles.fetch))
    channel.restore()
    return channel


def restart(channel: SecureChannel, bundles: Bundles) -> SecureChannel:
    """The process dies; a new one starts on the same file."""
    path = Path(channel.history._conn.execute("PRAGMA database_list").fetchone()[2])
    channel.history.close()
    return start(path, bundles)


def me(channel: SecureChannel) -> UserId:
    return channel.history.me


def befriend(a: SecureChannel, b: SecureChannel) -> None:
    for x, y in ((a, b), (b, a)):
        x.history.open_conversation(
            me(y),
            send_mailbox=mailbox_for(me(x), me(y)),
            recv_mailbox=mailbox_for(me(y), me(x)),
        )


@pytest.fixture
def world(tmp_path: Path) -> tuple[SecureChannel, SecureChannel, Bundles]:
    bundles = Bundles()
    alice, bob = (
        start(tmp_path / "alice.db", bundles),
        start(tmp_path / "bob.db", bundles),
    )
    befriend(alice, bob)
    return alice, bob, bundles


async def send(sender: SecureChannel, receiver: SecureChannel, text: str) -> bytes:
    """Send one message and return its envelope, as the relays would hold it."""
    eid = await sender.send(me(receiver), text, sent_at=NOW, expires_at=NOW + TTL)
    [item] = [i for i in sender.history.due_outbox(1e12) if i.envelope_id == eid]
    return item.envelope


async def deliver(sender: SecureChannel, receiver: SecureChannel) -> list[str]:
    """Hand every queued envelope to the receiver, as relays and loops would."""
    for item in sender.history.due_outbox(1e12):
        await receiver.receive(
            me(sender), item.envelope_id, item.envelope, received_at=NOW
        )
        sender.history.confirm_sent(item.envelope_id)
    return texts(receiver, sender)


def texts(owner: SecureChannel, peer: SecureChannel) -> list[str]:
    return [m.text for m in owner.history.messages(me(peer))]


def envelope_id_of(envelope: bytes) -> EnvelopeId:
    return envelope_id(envelope)


# -- the ordinary path -------------------------------------------------------------


async def test_send_and_receive_through_history(world) -> None:
    alice, bob, _ = world

    await alice.send(me(bob), "hola", sent_at=NOW, expires_at=NOW + TTL)

    assert alice.history.messages(me(bob))[0].state is MessageState.PENDING
    assert await deliver(alice, bob) == ["hola"]
    assert len(bob.history.load_sessions(me(alice))) == 1
    assert len(alice.history.load_sessions(me(bob))) == 1


async def test_the_payload_carries_the_history_clock(world) -> None:
    alice, bob, _ = world
    for text in ("one", "two"):
        await alice.send(me(bob), text, sent_at=NOW, expires_at=NOW + TTL)
    await deliver(alice, bob)

    first, second = bob.history.messages(me(alice))
    assert second.prev == first.envelope_id
    assert first.clock != second.clock


async def test_a_duplicate_is_dropped_before_decrypting(world) -> None:
    alice, bob, _ = world
    envelope = await send(alice, bob, "once")
    eid = envelope_id_of(envelope)

    assert await bob.receive(me(alice), eid, envelope, received_at=NOW) is True
    assert await bob.receive(me(alice), eid, envelope, received_at=NOW) is False
    assert texts(bob, alice) == ["once"]


# -- restarts ---------------------------------------------------------------------------


async def test_the_conversation_survives_both_restarting(world) -> None:
    alice, bob, bundles = world
    await send(alice, bob, "before")
    await deliver(alice, bob)
    await send(bob, alice, "reply")
    await deliver(bob, alice)

    alice, bob = restart(alice, bundles), restart(bob, bundles)

    await send(alice, bob, "after")
    assert (await deliver(alice, bob))[-1] == "after"
    await send(bob, alice, "after too")
    assert (await deliver(bob, alice))[-1] == "after too"
    assert len(alice.sessions.sessions_with(me(bob))) == 1


async def test_the_header_survives_a_restart_until_the_first_reply(world) -> None:
    """Alice writes twice, restarts, writes again: all three must open a session."""
    alice, bob, bundles = world
    first = await send(alice, bob, "1")
    second = await send(alice, bob, "2")
    alice = restart(alice, bundles)
    third = await send(alice, bob, "3")

    assert third[1] == FLAG_X3DH
    for envelope in (third, first, second):
        await bob.receive(me(alice), envelope_id_of(envelope), envelope, received_at=NOW)
    assert sorted(texts(bob, alice)) == ["1", "2", "3"]


async def test_a_seen_envelope_stays_seen_after_a_restart(world) -> None:
    """Committed, then killed before the ack: it comes back, and is dropped."""
    alice, bob, bundles = world
    envelope = await send(alice, bob, "hola")
    eid = envelope_id_of(envelope)
    await bob.receive(me(alice), eid, envelope, received_at=NOW)

    bob = restart(bob, bundles)

    assert await bob.receive(me(alice), eid, envelope, received_at=NOW) is False
    assert texts(bob, alice) == ["hola"]


# -- the transaction fails: nothing stored, nothing changed ------------------------------


async def test_sender_killed_before_the_commit(world, monkeypatch) -> None:
    alice, bob, _ = world
    await send(alice, bob, "kept")
    await deliver(alice, bob)
    before = alice.sessions.export(me(bob))

    def crash(*args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(alice.history, "queue_sealed", crash)
    with pytest.raises(sqlite3.OperationalError):
        await alice.send(me(bob), "lost", sent_at=NOW, expires_at=NOW + TTL)
    monkeypatch.undo()

    assert alice.sessions.export(me(bob)) == before  # memory rolled back
    assert [m.text for m in alice.history.messages(me(bob))] == ["kept"]
    await send(alice, bob, "next")
    assert (await deliver(alice, bob))[-1] == "next"


async def test_first_message_killed_before_the_commit_leaves_no_session(
    world, monkeypatch
) -> None:
    alice, bob, _ = world

    def crash(*args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(alice.history, "queue_sealed", crash)
    with pytest.raises(sqlite3.OperationalError):
        await alice.send(me(bob), "lost", sent_at=NOW, expires_at=NOW + TTL)
    monkeypatch.undo()

    assert alice.sessions.sessions_with(me(bob)) == []
    assert alice.history.load_sessions(me(bob)) == []


async def test_receiver_killed_before_the_commit_can_read_it_again(
    world, monkeypatch
) -> None:
    """Without the rollback the message key is spent and the message is lost."""
    alice, bob, _ = world
    envelope = await send(alice, bob, "precious")
    eid = envelope_id_of(envelope)

    def crash(*args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(bob.history, "commit_opened", crash)
    with pytest.raises(sqlite3.OperationalError):
        await bob.receive(me(alice), eid, envelope, received_at=NOW)
    monkeypatch.undo()

    assert not bob.history.is_seen(eid)
    assert bob.sessions.sessions_with(me(alice)) == []
    assert await bob.receive(me(alice), eid, envelope, received_at=NOW) is True
    assert texts(bob, alice) == ["precious"]


async def test_an_unreadable_payload_leaves_no_session(world, monkeypatch) -> None:
    """Authentic ciphertext, broken payload: CannotOpen, and no trace of it."""
    alice, bob, _ = world
    monkeypatch.setattr(sessions_module, "encode", lambda payload: b"\xc1 not msgpack")
    envelope = await send(alice, bob, "broken")
    monkeypatch.undo()

    with pytest.raises(CannotOpen):
        await bob.receive(me(alice), envelope_id_of(envelope), envelope, received_at=NOW)

    assert bob.sessions.sessions_with(me(alice)) == []
    assert bob.history.load_sessions(me(alice)) == []
    assert not bob.history.is_seen(envelope_id_of(envelope))


async def test_garbage_changes_nothing(world) -> None:
    alice, bob, _ = world
    await send(alice, bob, "real")
    await deliver(alice, bob)
    before = bob.sessions.export(me(alice))

    with pytest.raises(CannotOpen):
        await bob.receive(
            me(alice), envelope_id_of(b"x" * 10), b"x" * 10, received_at=NOW
        )

    assert bob.sessions.export(me(alice)) == before
    assert bob.history.load_sessions(me(alice)) == before


# -- one operation per peer at a time -------------------------------------------------


async def test_two_first_messages_at_once_start_one_session(world) -> None:
    """Without the lock, both would fetch the bundle and start a session each."""
    alice, bob, bundles = world
    real_fetch = bundles.fetch

    async def slow_fetch(user_id: UserId):
        await asyncio.sleep(0.01)  # an await in the middle, as with real relays
        return await real_fetch(user_id)

    alice.sessions.fetch = slow_fetch
    await asyncio.gather(
        alice.send(me(bob), "a", sent_at=NOW, expires_at=NOW + TTL),
        alice.send(me(bob), "b", sent_at=NOW, expires_at=NOW + TTL),
    )

    assert len(alice.sessions.sessions_with(me(bob))) == 1
    assert sorted(await deliver(alice, bob)) == ["a", "b"]


async def test_sending_and_receiving_at_once_keep_the_ratchet_whole(world) -> None:
    alice, bob, _ = world
    await send(alice, bob, "start")
    await deliver(alice, bob)
    incoming = [await send(bob, alice, f"in{n}") for n in range(5)]

    await asyncio.gather(
        *(
            alice.send(me(bob), f"out{n}", sent_at=NOW, expires_at=NOW + TTL)
            for n in range(5)
        ),
        *(
            alice.receive(me(bob), envelope_id_of(e), e, received_at=NOW)
            for e in incoming
        ),
    )

    assert sorted(texts(alice, bob)) == sorted(
        ["start", *(f"in{n}" for n in range(5)), *(f"out{n}" for n in range(5))]
    )
    delivered = await deliver(alice, bob)
    assert {f"out{n}" for n in range(5)} <= set(delivered)
