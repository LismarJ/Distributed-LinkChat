"""C3: the client's history survives being killed at any point."""

from __future__ import annotations

import multiprocessing
import os
import random
import signal
import sqlite3
import time
from pathlib import Path

import pytest

from linkchat.core.client.history import History, MessageState, UnknownConversation
from linkchat.core.envelope import pad_payload
from linkchat.core.payload import Clock2, Payload, encode
from linkchat.core.types import EnvelopeId, MailboxId, UserId

ME = UserId("aaaa")
PEER = UserId("bbbb")
MAILBOX = MailboxId("mb-1")


def seal(payload: Payload) -> bytes:
    """Stand-in for encryption until C6."""
    return pad_payload(encode(payload))


@pytest.fixture
def history(tmp_path: Path):
    h = History(tmp_path / "client.db", ME)
    h.open_conversation(PEER, MAILBOX)
    yield h
    h.close()


def incoming(n: int) -> tuple[EnvelopeId, Payload]:
    payload = Payload(text=f"in-{n}", clock=Clock2(low=0, high=n), prev=None, sent_at=0.0)
    return EnvelopeId(f"in-{n}"), payload


# -- sending ----------------------------------------------------------------


def test_queue_writes_message_and_outbox_together(history: History) -> None:
    eid = history.queue_outgoing(PEER, "hola", sent_at=1.0, expires_at=99.0, seal=seal)

    [msg] = history.messages(PEER)
    assert msg.envelope_id == eid and msg.state is MessageState.PENDING
    assert [item.envelope_id for item in history.due_outbox(now=1.0)] == [eid]


def test_clock_and_prev_advance_per_message(history: History) -> None:
    first = history.queue_outgoing(PEER, "1", sent_at=1.0, expires_at=99.0, seal=seal)
    history.queue_outgoing(PEER, "2", sent_at=2.0, expires_at=99.0, seal=seal)

    a, b = history.messages(PEER)
    assert (a.clock, b.clock) == (Clock2(1, 0), Clock2(2, 0))  # ME is the "low" slot
    assert (a.prev, b.prev) == (None, first)


def test_a_failing_seal_leaves_nothing_behind(history: History) -> None:
    def broken(_: Payload) -> bytes:
        raise RuntimeError("encryption failed")

    with pytest.raises(RuntimeError):
        history.queue_outgoing(PEER, "x", sent_at=1.0, expires_at=99.0, seal=broken)

    assert history.messages(PEER) == []
    assert history.due_outbox(now=1e9) == []
    history.queue_outgoing(PEER, "y", sent_at=1.0, expires_at=99.0, seal=seal)
    assert history.messages(PEER)[0].clock == Clock2(1, 0)  # the counter did not move


def test_confirm_clears_outbox_and_is_idempotent(history: History) -> None:
    eid = history.queue_outgoing(PEER, "hola", sent_at=1.0, expires_at=99.0, seal=seal)

    history.confirm_sent(eid)
    history.confirm_sent(eid)

    assert history.due_outbox(now=1e9) == []
    assert history.messages(PEER)[0].state is MessageState.SENT


def test_retry_resends_the_same_bytes(history: History) -> None:
    eid = history.queue_outgoing(PEER, "hola", sent_at=1.0, expires_at=99.0, seal=seal)
    [before] = history.due_outbox(now=1.0)

    history.mark_attempt(eid, next_attempt_at=5.0)

    assert history.due_outbox(now=4.0) == []
    [after] = history.due_outbox(now=5.0)
    assert after.envelope == before.envelope and after.attempts == 1


def test_unknown_peer_is_refused(history: History) -> None:
    with pytest.raises(UnknownConversation):
        history.queue_outgoing(UserId("zzzz"), "?", sent_at=1, expires_at=9, seal=seal)


# -- receiving --------------------------------------------------------------


def test_duplicate_incoming_is_stored_once(history: History) -> None:
    eid, payload = incoming(1)

    assert not history.is_seen(eid)
    assert history.commit_incoming(PEER, eid, payload, received_at=1.0)
    assert history.is_seen(eid)
    assert not history.commit_incoming(PEER, eid, payload, received_at=2.0)

    assert len(history.messages(PEER)) == 1


def test_incoming_advances_the_peer_slot_of_the_next_send(history: History) -> None:
    eid, payload = incoming(3)
    history.commit_incoming(PEER, eid, payload, received_at=1.0)

    history.queue_outgoing(PEER, "reply", sent_at=2.0, expires_at=99.0, seal=seal)

    assert history.messages(PEER)[-1].clock == Clock2(low=1, high=3)


def test_trim_seen_only_drops_old_ids(history: History) -> None:
    for n, at in ((1, 10.0), (2, 50.0)):
        eid, payload = incoming(n)
        history.commit_incoming(PEER, eid, payload, received_at=at)

    assert history.trim_seen(older_than=20.0) == 1
    assert not history.is_seen(EnvelopeId("in-1"))
    assert history.is_seen(EnvelopeId("in-2"))


def test_reopening_keeps_everything(tmp_path: Path) -> None:
    path = tmp_path / "client.db"
    h = History(path, ME)
    h.open_conversation(PEER, MAILBOX)
    h.queue_outgoing(PEER, "hola", sent_at=1.0, expires_at=99.0, seal=seal)
    h.close()

    reopened = History(path, ME)
    assert [m.text for m in reopened.messages(PEER)] == ["hola"]
    reopened.close()


# -- killed at a random point -----------------------------------------------


def _worker(path: str, seed: int, done, ready) -> None:
    """Writes forever. done[0] / done[1] count sends / receipts whose commit returned."""
    rng = random.Random(seed)
    h = History(path, ME)
    h.open_conversation(PEER, MAILBOX)
    ready.set()
    n = 0
    while True:
        roll = rng.random()
        if roll < 0.5:
            h.queue_outgoing(PEER, "m", sent_at=time.time(), expires_at=1e12, seal=seal)
            done[0] += 1
        elif roll < 0.7:
            for item in h.due_outbox(now=1e12)[:1]:
                h.confirm_sent(item.envelope_id)
        else:
            n += 1
            eid, payload = incoming(n)
            h.commit_incoming(PEER, eid, payload, received_at=time.time())
            h.commit_incoming(PEER, eid, payload, received_at=time.time())  # replay
            done[1] += 1


def _check_invariants(path: Path) -> None:
    db = sqlite3.connect(path)
    q = lambda sql: db.execute(sql).fetchone()[0]  # noqa: E731

    assert q("PRAGMA integrity_check") == "ok"
    assert (
        q(
            "SELECT COUNT(*) FROM outbox o LEFT JOIN messages m USING (envelope_id)"
            " WHERE m.state IS NOT 'pending'"
        )
        == 0
    ), "outbox row without a pending message"
    assert (
        q(
            "SELECT COUNT(*) FROM messages m LEFT JOIN outbox o USING (envelope_id)"
            " WHERE m.state = 'pending' AND o.envelope_id IS NULL"
        )
        == 0
    ), "pending lost"
    assert q("SELECT my_count FROM conversations") == q(
        "SELECT COUNT(*) FROM messages WHERE direction = 'out'"
    ), "clock drifted"
    assert q("SELECT COUNT(*) FROM seen") == q(
        "SELECT COUNT(*) FROM messages WHERE direction = 'in'"
    ), "seen-set out of step"
    db.close()


@pytest.mark.parametrize("seed", range(20))
def test_killed_at_any_point_reopens_consistent(tmp_path: Path, seed: int) -> None:
    path = tmp_path / "client.db"
    ctx = multiprocessing.get_context("fork")
    done = ctx.Array("i", 2, lock=False)  # only the child writes
    ready = ctx.Event()
    child = ctx.Process(target=_worker, args=(str(path), seed, done, ready))
    child.start()
    assert ready.wait(timeout=10), "worker never started"

    time.sleep(random.Random(seed).uniform(0.05, 0.3))
    assert child.pid is not None
    os.kill(child.pid, signal.SIGKILL)
    child.join()

    _check_invariants(path)
    h = History(path, ME)  # and it still opens through the normal path
    stored = h.messages(PEER)
    h.close()
    outgoing = sum(m.outgoing for m in stored)
    # Nothing whose commit returned is lost; at most the one in flight is extra.
    assert done[0] <= outgoing <= done[0] + 1
    assert done[1] <= len(stored) - outgoing <= done[1] + 1
