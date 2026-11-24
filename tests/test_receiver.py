"""C5: the receive loop finds every message with two relays down, exactly once."""

from __future__ import annotations

import asyncio
import dataclasses
from pathlib import Path

import pytest

from linkchat.core.client.history import History
from linkchat.core.client.mailbox_ids import mailbox_for
from linkchat.core.client.receiver import Receiver
from linkchat.core.envelope import envelope_id, pad_payload, unpad_payload
from linkchat.core.payload import Clock2, Payload, decode, encode
from linkchat.core.ports import Record
from linkchat.core.ring import preference_list, primaries
from linkchat.core.types import RelayName, UserId

from .fakes.transport import FakeCluster

ME = UserId("aaaa")
PEER = UserId("bbbb")
INBOX = mailbox_for(PEER, ME)


def open_plain(peer: UserId, envelope: bytes) -> Payload:
    """These tests are about reading the relays, so envelopes are in the clear."""
    return decode(unpad_payload(envelope))


def plain_accept(history: History, open_envelope=open_plain):
    """What `SecureChannel.receive` does, minus the cryptography."""

    async def accept(peer: UserId, eid, envelope: bytes, received_at: float) -> bool:
        if history.is_seen(eid):
            return False
        return history.commit_incoming(
            peer, eid, open_envelope(peer, envelope), received_at
        )

    return accept


def message(cluster: FakeCluster, n: int, text: str = "hola") -> Record:
    payload = Payload(text=f"{text}-{n}", clock=Clock2(0, n), prev=None, sent_at=0.0)
    envelope = pad_payload(encode(payload))
    return Record(
        mailbox_id=INBOX,
        envelope_id=envelope_id(envelope),
        envelope=envelope,
        expires_at=cluster.clock.now() + 3600,
    )


def put(cluster: FakeCluster, record: Record, *relays: RelayName) -> None:
    for name in relays:
        cluster.stores[name].store(record)


@pytest.fixture
def cluster() -> FakeCluster:
    return FakeCluster()


@pytest.fixture
def history(tmp_path: Path):
    h = History(tmp_path / "client.db", ME)
    h.open_conversation(PEER, send_mailbox=mailbox_for(ME, PEER), recv_mailbox=INBOX)
    yield h
    h.close()


@pytest.fixture
def receiver(history, cluster) -> Receiver:
    return Receiver(history, cluster.directory(), cluster.clock, plain_accept(history))


def texts(history: History) -> list[str]:
    return [m.text for m in history.messages(PEER)]


async def test_receives_and_acks_on_every_relay_read(cluster, history, receiver) -> None:
    record = message(cluster, 1)
    put(cluster, record, *primaries(INBOX))

    assert await receiver.receive_once(PEER) == 1

    assert texts(history) == ["hola-1"]
    for name in primaries(INBOX):
        assert cluster.stores[name].is_tombstoned(INBOX, record.envelope_id)


async def test_two_relays_down_still_receives(cluster, history, receiver) -> None:
    record = message(cluster, 1)
    first, second, third = primaries(INBOX)
    put(cluster, record, first, second, third)
    cluster.kill(first, second)

    assert await receiver.receive_once(PEER) == 1
    assert texts(history) == ["hola-1"]


async def test_finds_a_copy_held_only_by_a_stand_in(cluster, history, receiver) -> None:
    """Written while two primaries were down; now one of them is down again."""
    record = message(cluster, 1)
    order = preference_list(INBOX)
    standin = order[3]
    put(cluster, dataclasses.replace(record, hint=order[0]), standin)
    put(cluster, record, order[1], order[2])
    cluster.kill(order[1], order[2])  # only the stand-in still holds it

    assert await receiver.receive_once(PEER) == 1
    assert texts(history) == ["hola-1"]


async def test_the_same_envelope_on_three_relays_is_one_message(
    cluster, history, receiver
) -> None:
    put(cluster, message(cluster, 1), *primaries(INBOX))
    put(cluster, message(cluster, 2), *primaries(INBOX))

    assert await receiver.receive_once(PEER) == 2
    assert await receiver.receive_once(PEER) == 0
    assert sorted(texts(history)) == ["hola-1", "hola-2"]


async def test_killed_between_commit_and_ack(cluster, history, receiver) -> None:
    """fault-tolerance.md: committed locally, the ack never left. No duplicate."""
    record = message(cluster, 1)
    put(cluster, record, *primaries(INBOX))
    payload = open_plain(PEER, record.envelope)
    history.commit_incoming(PEER, record.envelope_id, payload, received_at=0.0)

    assert await receiver.receive_once(PEER) == 0  # the seen-set drops it
    assert texts(history) == ["hola-1"]
    for name in primaries(INBOX):  # and this time it is acknowledged
        assert cluster.stores[name].is_tombstoned(INBOX, record.envelope_id)


async def test_unreadable_envelope_is_not_acked_and_does_not_block(
    cluster, history
) -> None:
    good, bad = message(cluster, 1), message(cluster, 2, text="broken")

    def picky_open(peer: UserId, envelope: bytes) -> Payload:
        payload = open_plain(peer, envelope)
        if payload.text.startswith("broken"):
            raise ValueError("cannot decrypt")
        return payload

    put(cluster, good, *primaries(INBOX))
    put(cluster, bad, *primaries(INBOX))
    receiver = Receiver(
        history, cluster.directory(), cluster.clock, plain_accept(history, picky_open)
    )

    assert await receiver.receive_once(PEER) == 1
    assert texts(history) == ["hola-1"]
    first = primaries(INBOX)[0]
    assert cluster.stores[first].has(INBOX, bad.envelope_id)  # left for its TTL


async def test_forged_envelope_id_is_ignored(cluster, history, receiver) -> None:
    real = message(cluster, 1)
    forged = dataclasses.replace(message(cluster, 2), envelope_id=real.envelope_id)
    put(cluster, forged, *primaries(INBOX))

    assert await receiver.receive_once(PEER) == 0
    assert not history.is_seen(real.envelope_id)


async def test_syncing_relay_is_skipped(cluster, history, receiver) -> None:
    record = message(cluster, 1)
    order = preference_list(INBOX)
    put(cluster, record, *order)
    cluster.restart(order[0])  # empty and SYNCING

    assert await receiver.receive_once(PEER) == 1
    assert cluster.stores[order[3]].is_tombstoned(INBOX, record.envelope_id)


async def test_run_keeps_receiving(cluster, history, receiver) -> None:
    task = asyncio.create_task(receiver.run(PEER))
    await asyncio.sleep(0)
    put(cluster, message(cluster, 1), *primaries(INBOX))
    for _ in range(20):
        await asyncio.sleep(0)
    task.cancel()

    assert texts(history) == ["hola-1"]


def test_mailboxes_are_per_direction() -> None:
    assert mailbox_for(ME, PEER) != mailbox_for(PEER, ME)
    assert mailbox_for(ME, PEER) == mailbox_for(ME, PEER)
    assert mailbox_for(UserId("ab"), UserId("c")) != mailbox_for(
        UserId("a"), UserId("bc")
    )
    with pytest.raises(ValueError):
        mailbox_for(ME, ME)
