"""C4: the outbox retries until three relays confirm, and never sends twice."""

from __future__ import annotations

import asyncio
import functools
import random
import sqlite3
from pathlib import Path

import pytest

from linkchat.core.client import outbox as outbox_module
from linkchat.core.client.history import History, MessageState
from linkchat.core.client.outbox import CAP_S, Outbox
from linkchat.core.ports import Record
from linkchat.core.replication import SendResult, send_to_three
from linkchat.core.types import EnvelopeId, MailboxId, RelayName

from .fakes.transport import FakeCluster, ManualClock
from .test_history import INBOX, MAILBOX, ME, PEER, seal

TTL = 3600.0
OK = SendResult(confirmed_relays=tuple(RelayName(f"relay-{i}") for i in range(3)))
SHORT = SendResult(confirmed_relays=(RelayName("relay-0"),))


class FakeSend:
    """Records every attempt. Fails the first `fail` calls, then confirms."""

    def __init__(self, fail: int = 0, raises: bool = False) -> None:
        self.fail = fail
        self.raises = raises
        self.calls: list[Record] = []
        self.relay: dict[EnvelopeId, bytes] = {}  # what a relay would hold

    async def __call__(self, record: Record) -> SendResult:
        self.calls.append(record)
        self.relay[record.envelope_id] = record.envelope
        if self.fail > 0:
            self.fail -= 1
            if self.raises:
                raise RuntimeError("transport exploded")
            return SHORT
        return OK


def make_history(path: Path) -> History:
    h = History(path, ME)
    h.open_conversation(PEER, send_mailbox=MAILBOX, recv_mailbox=INBOX)
    return h


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def history(tmp_path: Path):
    h = make_history(tmp_path / "client.db")
    yield h
    h.close()


def state(history: History) -> MessageState:
    return history.messages(PEER)[-1].state


async def test_confirmed_send_leaves_the_outbox(history, clock) -> None:
    send = FakeSend()
    outbox = Outbox(history, send, clock)

    outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)
    assert state(history) is MessageState.PENDING
    await outbox.run_once()

    assert state(history) is MessageState.SENT
    assert history.next_due() is None
    assert len(send.calls) == 1


async def test_retries_until_three_relays_confirm(history, clock) -> None:
    send = FakeSend(fail=2)
    outbox = Outbox(history, send, clock, rng=random.Random(0))
    outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)

    for _ in range(3):
        await outbox.run_once()
        due = history.next_due()
        if due is not None:
            clock.advance(due - clock.now())

    assert state(history) is MessageState.SENT
    assert len(send.calls) == 3


async def test_nothing_is_resent_before_its_backoff(history, clock) -> None:
    send = FakeSend(fail=1)
    outbox = Outbox(history, send, clock, rng=random.Random(1))
    outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)

    await outbox.run_once()
    due = history.next_due()
    assert due is not None and due > clock.now()
    await outbox.run_once()  # not due yet

    assert len(send.calls) == 1


async def test_an_exception_counts_as_a_failed_attempt(history, clock) -> None:
    send = FakeSend(fail=1, raises=True)
    outbox = Outbox(history, send, clock, rng=random.Random(0))
    outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)

    await outbox.run_once()
    assert state(history) is MessageState.PENDING

    clock.advance(CAP_S)
    await outbox.run_once()
    assert state(history) is MessageState.SENT


async def test_expired_message_is_given_up(history, clock) -> None:
    send = FakeSend(fail=99)
    outbox = Outbox(history, send, clock)
    outbox.submit(PEER, "hola", ttl_s=10.0, seal=seal)
    await outbox.run_once()
    calls = len(send.calls)

    clock.advance(10.0)
    await outbox.run_once()

    assert state(history) is MessageState.NOT_DELIVERED
    assert history.next_due() is None
    assert len(send.calls) == calls


def test_backoff_grows_and_is_capped(clock) -> None:
    outbox = Outbox(None, FakeSend(), clock, rng=random.Random(2))  # type: ignore[arg-type]
    ceilings = [max(outbox.backoff(n) for _ in range(200)) for n in range(12)]

    assert ceilings[0] <= 0.5
    assert ceilings[3] > ceilings[0]
    assert all(c <= CAP_S for c in ceilings)


async def test_retry_sends_the_same_bytes(history, clock) -> None:
    send = FakeSend(fail=1)
    outbox = Outbox(history, send, clock, rng=random.Random(0))
    outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)

    await outbox.run_once()
    clock.advance(CAP_S)
    await outbox.run_once()

    first, second = send.calls
    assert first.envelope == second.envelope
    assert first.envelope_id == second.envelope_id


async def test_killed_mid_send_finishes_after_restart(tmp_path: Path, clock) -> None:
    """The plan's done-when: one copy at the relays, and the message ends `sent`."""
    path = tmp_path / "client.db"
    send = FakeSend()

    class Killed(BaseException):
        pass

    async def store_then_die(record: Record) -> SendResult:
        await send(record)  # the relays got it...
        raise Killed  # ...and the client died before confirm_sent

    before = make_history(path)
    outbox = Outbox(before, store_then_die, clock)
    outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)
    with pytest.raises(Killed):
        await outbox.run_once()
    before.close()

    after = make_history(path)  # restart on the same volume
    await Outbox(after, send, clock).run_once()

    assert state(after) is MessageState.SENT
    assert len(send.relay) == 1
    assert len(after.messages(PEER)) == 1
    after.close()


async def test_run_wakes_up_on_submit(history) -> None:
    send = FakeSend()
    outbox = Outbox(history, send, ManualClock())
    task = asyncio.create_task(outbox.run())
    await asyncio.sleep(0)  # the loop is now waiting with an empty outbox

    outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)
    for _ in range(10):
        await asyncio.sleep(0)
    task.cancel()

    assert state(history) is MessageState.SENT


async def test_backoff_counts_from_the_end_of_a_slow_attempt(history, clock) -> None:
    async def slow_failure(record: Record) -> SendResult:
        clock.advance(10.0)  # e.g. two relay timeouts
        return SHORT

    outbox = Outbox(history, slow_failure, clock, rng=random.Random(0))
    outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)
    await outbox.run_once()

    due = history.next_due()
    assert due is not None and due > clock.now()  # not immediately due again


async def test_one_item_failing_does_not_stop_the_others(
    history, clock, monkeypatch
) -> None:
    outbox = Outbox(history, FakeSend(), clock)
    broken = outbox.submit(PEER, "a", ttl_s=TTL, seal=seal)
    outbox.submit(PEER, "b", ttl_s=TTL, seal=seal)
    real_confirm = history.confirm_sent

    def confirm(eid: EnvelopeId) -> None:
        if eid == broken:
            raise sqlite3.OperationalError("disk I/O error")
        real_confirm(eid)

    monkeypatch.setattr(history, "confirm_sent", confirm)
    await outbox.run_once()  # does not raise

    states = {m.text: m.state for m in history.messages(PEER)}
    assert states == {"a": MessageState.PENDING, "b": MessageState.SENT}


async def test_run_survives_a_database_error(history, monkeypatch) -> None:
    monkeypatch.setattr(outbox_module, "ERROR_PAUSE_S", 0.0)
    outbox = Outbox(history, FakeSend(), ManualClock())
    real_due = history.due_outbox
    failures = [sqlite3.OperationalError("database is locked")]

    def due(now: float):
        if failures:
            raise failures.pop()
        return real_due(now)

    monkeypatch.setattr(history, "due_outbox", due)
    outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)
    task = asyncio.create_task(outbox.run())
    for _ in range(20):
        await asyncio.sleep(0)
    task.cancel()

    assert not failures  # the error did happen
    assert state(history) is MessageState.SENT


@pytest.mark.xfail(reason="R5 not implemented yet; remove once it is", strict=True)
async def test_with_real_three_copy_send(history) -> None:
    cluster = FakeCluster()
    send = functools.partial(
        send_to_three, directory=cluster.directory(), clock=cluster.clock
    )
    outbox = Outbox(history, send, cluster.clock)
    cluster.kill(RelayName("relay-0"), RelayName("relay-1"))

    eid = outbox.submit(PEER, "hola", ttl_s=TTL, seal=seal)
    await outbox.run_once()

    assert state(history) is MessageState.SENT
    holders = [n for n, s in cluster.stores.items() if s.has(MailboxId(MAILBOX), eid)]
    assert len(holders) == 3
