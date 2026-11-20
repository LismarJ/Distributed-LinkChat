"""C5: two clients exchange messages through the (fake) relays."""

from __future__ import annotations

import asyncio
import functools
from pathlib import Path

import pytest

from linkchat.core.client.client import ChatClient
from linkchat.core.client.history import History, MessageState
from linkchat.core.client.mailbox_ids import mailbox_for
from linkchat.core.envelope import pad_payload
from linkchat.core.payload import Payload, encode
from linkchat.core.ports import Record
from linkchat.core.replication import SendResult, send_to_three
from linkchat.core.ring import primaries
from linkchat.core.types import RelayName, UserId

from .fakes.transport import FakeCluster
from .test_receiver import open_plain

ALICE = UserId("aaaa")
BOB = UserId("bbbb")


def seal_plain(peer: UserId, payload: Payload) -> bytes:
    """Stand-in for encryption until C6."""
    return pad_payload(encode(payload))


def naive_send(cluster: FakeCluster):
    """Stores on the three primaries only. Enough until R5 exists."""

    async def send(record: Record) -> SendResult:
        directory = cluster.directory()
        names = primaries(record.mailbox_id)
        await asyncio.gather(*(directory.gateway(n).store(record) for n in names))
        return SendResult(confirmed_relays=tuple(names))

    return send


def make_client(
    tmp_path: Path, cluster: FakeCluster, me: UserId, send=None
) -> ChatClient:
    history = History(tmp_path / f"{me}.db", me)
    return ChatClient(
        history,
        cluster.directory(),
        cluster.clock,
        seal=seal_plain,
        open_envelope=open_plain,
        send=send,
    )


async def settle(rounds: int = 50) -> None:
    for _ in range(rounds):
        await asyncio.sleep(0)


def texts(client: ChatClient, peer: UserId) -> list[str]:
    return [m.text for m in client.history.messages(peer)]


def test_contacts_use_one_mailbox_per_direction(tmp_path) -> None:
    alice = make_client(tmp_path, FakeCluster(), ALICE)
    alice.add_contact(BOB)
    eid = alice.send(BOB, "hola")

    [item] = alice.history.due_outbox(now=1e12)
    assert item.envelope_id == eid
    assert item.mailbox_id == mailbox_for(ALICE, BOB)
    assert alice.history.recv_mailbox(BOB) == mailbox_for(BOB, ALICE)


async def test_full_exchange_through_relays(tmp_path) -> None:
    cluster = FakeCluster()
    alice = make_client(tmp_path, cluster, ALICE, send=naive_send(cluster))
    bob = make_client(tmp_path, cluster, BOB, send=naive_send(cluster))
    alice.add_contact(BOB)
    bob.add_contact(ALICE)
    await alice.start()
    await bob.start()

    alice.send(BOB, "hola bob")
    await settle()
    bob.send(ALICE, "hola alice")
    await settle()
    await alice.stop()
    await bob.stop()

    assert texts(bob, ALICE) == ["hola bob", "hola alice"]
    assert texts(alice, BOB) == ["hola bob", "hola alice"]
    states = {m.text: m.state for m in alice.history.messages(BOB)}
    assert states == {"hola bob": MessageState.SENT, "hola alice": MessageState.RECEIVED}


async def test_stop_cancels_every_task(tmp_path) -> None:
    alice = make_client(tmp_path, FakeCluster(), ALICE)
    alice.add_contact(BOB)
    await alice.start()
    alice.add_contact(UserId("cccc"))  # added while running: listened to at once
    tasks = list(alice._tasks.values())
    assert len(tasks) == 3

    await alice.stop()
    assert all(t.done() for t in tasks)


@pytest.mark.xfail(reason="R5 not implemented yet; remove once it is", strict=True)
async def test_exchange_with_two_relays_down(tmp_path) -> None:
    """The plan's done-when for C5, with the real three-copy send."""
    cluster = FakeCluster()
    send = functools.partial(
        send_to_three, directory=cluster.directory(), clock=cluster.clock
    )
    alice = make_client(tmp_path, cluster, ALICE, send=send)
    bob = make_client(tmp_path, cluster, BOB, send=send)
    alice.add_contact(BOB)
    bob.add_contact(ALICE)
    cluster.kill(RelayName("relay-0"), RelayName("relay-3"))
    await alice.start()
    await bob.start()

    alice.send(BOB, "hola")
    await settle()
    await alice.stop()
    await bob.stop()

    assert texts(bob, ALICE) == ["hola"]
    assert alice.history.messages(BOB)[0].state is MessageState.SENT
