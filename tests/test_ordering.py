"""Causal ordering, gap detection and deterministic tie-break.

These tests build StoredMessage rows by hand, so they exercise the ordering
algorithm without SQLite, without relays, without encryption.
"""

from __future__ import annotations

import pytest

from linkchat.core.client.history import MessageState, StoredMessage
from linkchat.core.client.ordering import DisplayMessage, order_messages
from linkchat.core.payload import Clock2
from linkchat.core.types import EnvelopeId, UserId

ALICE = UserId("aaaa")  # low slot
BOB = UserId("bbbb")  # high slot

ME, PEER = ALICE, BOB

NOW = 1_000.0
TIMEOUT = 30.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def msg(
    eid: str,
    *,
    outgoing: bool,
    low: int,
    high: int,
    prev: str | None = None,
    received_at: float | None = None,
) -> StoredMessage:
    """Build a StoredMessage.

    Incoming messages default to arriving one second ago,
    so they are not timed out.
    """

    if received_at is None and not outgoing:
        received_at = NOW - 1.0

    return StoredMessage(
        envelope_id=EnvelopeId(eid),
        peer=PEER,
        outgoing=outgoing,
        text=eid,
        clock=Clock2(low=low, high=high),
        prev=EnvelopeId(prev) if prev else None,
        sent_at=0.0,
        state=MessageState.SENT if outgoing else MessageState.RECEIVED,
        received_at=received_at,
    )


def order(
    messages,
    *,
    now=NOW,
    timeout=TIMEOUT,
) -> list[DisplayMessage]:
    """Run causal ordering with the test participants."""

    return order_messages(
        messages,
        me=ME,
        peer=PEER,
        now=now,
        hold_timeout_s=timeout,
    )


def ids(shown: list[DisplayMessage]) -> list[str]:
    """Extract envelope IDs from displayed messages."""

    return [d.message.envelope_id for d in shown]


# ---------------------------------------------------------------------------
# Causal readiness
# ---------------------------------------------------------------------------


def test_in_order_messages_are_all_shown() -> None:
    """Messages with valid causal dependencies are displayed in order."""

    a = msg(
        "a",
        outgoing=True,
        low=1,
        high=0,
    )

    b = msg(
        "b",
        outgoing=False,
        low=1,
        high=1,
    )

    c = msg(
        "c",
        outgoing=True,
        low=2,
        high=1,
    )

    shown = order([a, b, c])

    assert ids(shown) == ["a", "b", "c"]
    assert all(not d.after_gap for d in shown)


def test_out_of_order_incoming_is_held_until_predecessor_arrives() -> None:
    """A later message is held until its causal predecessor is available."""

    first = msg(
        "a",
        outgoing=False,
        low=0,
        high=1,
    )

    second = msg(
        "b",
        outgoing=False,
        low=0,
        high=2,
        prev="a",
    )

    # Only the second message is stored.
    # Its sender counter indicates a missing predecessor.
    assert order([second]) == []

    # When the first arrives, both become deliverable.
    shown = order([first, second])

    assert ids(shown) == ["a", "b"]
    assert all(not d.after_gap for d in shown)


def test_missing_prev_is_held_even_if_clock_looks_ready() -> None:
    """A message with an unavailable predecessor cannot be normally delivered."""

    delivered = msg(
        "x",
        outgoing=False,
        low=0,
        high=1,
    )

    orphan = msg(
        "b",
        outgoing=False,
        low=0,
        high=2,
        prev="a",
    )

    shown = order([delivered, orphan])

    assert ids(shown) == ["x"]


def test_own_messages_use_the_other_slot() -> None:
    """Outgoing and incoming messages use the correct vector-clock slots."""

    mine1 = msg(
        "m1",
        outgoing=True,
        low=1,
        high=0,
    )

    theirs = msg(
        "t1",
        outgoing=False,
        low=1,
        high=1,
    )

    mine2 = msg(
        "m2",
        outgoing=True,
        low=2,
        high=1,
        prev="m1",
    )

    shown = order([mine1, theirs, mine2])

    assert ids(shown) == ["m1", "t1", "m2"]


def test_invalid_sender_counter_is_not_delivered() -> None:
    """Counter zero is invalid as the first sender message."""

    invalid = msg(
        "invalid",
        outgoing=False,
        low=0,
        high=0,
    )

    shown = order([invalid])

    assert shown == []


def test_same_sender_same_counter_is_not_causally_delivered() -> None:
    """Two distinct messages cannot share the same sender counter."""

    first = msg(
        "a",
        outgoing=False,
        low=0,
        high=1,
    )

    conflict = msg(
        "b",
        outgoing=False,
        low=0,
        high=1,
    )

    shown = order([first, conflict])

    # Both messages conflict because they share the same sender counter.
    # Neither should be accepted through normal causal delivery.
    assert shown == []


# ---------------------------------------------------------------------------
# Timeout and gap marker
# ---------------------------------------------------------------------------


def test_held_message_is_released_with_gap_after_timeout() -> None:
    """A missing predecessor eventually causes an explicit gap."""

    orphan = msg(
        "b",
        outgoing=False,
        low=0,
        high=2,
        prev="a",
        received_at=NOW - TIMEOUT + 1,
    )

    # Not yet timed out.
    assert order([orphan]) == []

    # Once timed out, it may be displayed with a gap marker.
    shown = order([orphan], now=NOW + 1)

    assert len(shown) == 1
    assert shown[0].message.envelope_id == "b"
    assert shown[0].after_gap is True


def test_release_after_gap_advances_the_clock() -> None:
    """Only the first message after the gap is marked; its successor flows normally."""

    orphan = msg(
        "b", outgoing=False, low=0, high=2, prev="a", received_at=NOW - TIMEOUT - 5
    )
    after = msg(
        "c", outgoing=False, low=0, high=3, prev="b", received_at=NOW - TIMEOUT - 5
    )

    shown = order([orphan, after], now=NOW)

    assert ids(shown) == ["b", "c"]
    assert [d.after_gap for d in shown] == [True, False]


def test_after_a_gap_new_messages_flow_without_waiting() -> None:
    """Bob's #1 is lost: #2 is marked, and #5, just arrived, shows at once."""

    old = NOW - TIMEOUT - 5
    later = [
        msg(f"b{n}", outgoing=False, low=0, high=n, prev=f"b{n - 1}", received_at=old)
        for n in (2, 3, 4)
    ]
    fresh = msg("b5", outgoing=False, low=0, high=5, prev="b4", received_at=NOW - 1)

    shown = order([*later, fresh])

    assert ids(shown) == ["b2", "b3", "b4", "b5"]
    assert [d.after_gap for d in shown] == [True, False, False, False]


def test_own_message_after_a_gap_is_shown() -> None:
    """Alice replied to Bob's #4 while #1 was missing: her message must appear."""

    old = NOW - TIMEOUT - 5
    theirs = [
        msg(f"b{n}", outgoing=False, low=0, high=n, prev=f"b{n - 1}", received_at=old)
        for n in (2, 3, 4)
    ]
    mine = msg("m1", outgoing=True, low=1, high=4)

    shown = order([*theirs, mine])

    assert ids(shown) == ["b2", "b3", "b4", "m1"]
    assert shown[-1].after_gap is False


def test_late_predecessor_closes_the_gap() -> None:
    """Recomputed from scratch: once #1 arrives, nothing is marked any more."""

    old = NOW - TIMEOUT - 5
    first = msg("b1", outgoing=False, low=0, high=1, received_at=NOW - 1)
    second = msg("b2", outgoing=False, low=0, high=2, prev="b1", received_at=old)

    assert [d.after_gap for d in order([second])] == [True]

    shown = order([first, second])
    assert ids(shown) == ["b1", "b2"]
    assert not any(d.after_gap for d in shown)


def test_early_arrival_does_not_leapfrog_a_late_predecessor() -> None:
    """A higher counter cannot leapfrog a lower pending counter."""

    early = msg(
        "c",
        outgoing=False,
        low=0,
        high=3,
        prev="b",
        received_at=NOW - TIMEOUT - 10,
    )

    late = msg(
        "b",
        outgoing=False,
        low=0,
        high=2,
        prev="a",
        received_at=NOW - 5,
    )

    # c has already timed out, but b has not.
    # c must remain blocked.
    assert order([early, late], now=NOW) == []

    # Once both have timed out, they can be released in counter order.
    shown = order(
        [early, late],
        now=NOW + TIMEOUT,
    )

    assert ids(shown) == ["b", "c"]
    assert [d.after_gap for d in shown] == [True, False]  # c only needed b


def test_outgoing_messages_never_time_out() -> None:
    """Outgoing messages are not timeout-released as gaps."""

    mine = msg(
        "m1",
        outgoing=True,
        low=1,
        high=0,
    )

    shown = order(
        [mine],
        now=NOW + 10_000,
    )

    assert ids(shown) == ["m1"]
    assert shown[0].after_gap is False


def test_negative_timeout_is_rejected() -> None:
    """A negative hold timeout is invalid configuration."""

    message = msg(
        "a",
        outgoing=False,
        low=0,
        high=1,
    )

    with pytest.raises(ValueError, match="hold_timeout_s"):
        order(
            [message],
            timeout=-1,
        )


# ---------------------------------------------------------------------------
# Deterministic tie-break
# ---------------------------------------------------------------------------


def test_concurrent_messages_have_a_deterministic_order() -> None:
    """Concurrent messages are sorted by envelope_id, regardless of arrival."""

    alice_msg = msg(
        "alice",
        outgoing=True,
        low=1,
        high=0,
    )

    bob_msg = msg(
        "bob",
        outgoing=False,
        low=0,
        high=1,
    )

    first = order([alice_msg, bob_msg])
    second = order([bob_msg, alice_msg])

    # Both arrival orders must produce the same display order.
    assert ids(first) == ids(second)

    # Tie-break is exactly envelope_id.
    assert ids(first) == ["alice", "bob"]

    assert all(not d.after_gap for d in first)
    assert all(not d.after_gap for d in second)


def test_both_clients_show_the_same_order() -> None:
    """The plan's done-when: concurrent messages, the same order on both sides."""

    def view(me: UserId, peer: UserId, arrival: list[tuple[str, UserId, int, int]]):
        rows = [
            StoredMessage(
                envelope_id=EnvelopeId(eid),
                peer=peer,
                outgoing=sender == me,
                text=eid,
                clock=Clock2(low=low, high=high),
                prev=None,
                sent_at=0.0,
                state=MessageState.SENT if sender == me else MessageState.RECEIVED,
                received_at=None if sender == me else NOW - 1.0,
            )
            for eid, sender, low, high in arrival
        ]
        return ids(order_messages(rows, me=me, peer=peer, now=NOW))

    # Each wrote twice before seeing the other: two concurrent chains, then a reply.
    exchange = [
        ("x1", ALICE, 1, 0),
        ("x2", ALICE, 2, 0),
        ("y1", BOB, 0, 1),
        ("y2", BOB, 0, 2),
        ("z", ALICE, 3, 2),
    ]

    alice_view = view(ALICE, BOB, exchange)
    bob_view = view(BOB, ALICE, list(reversed(exchange)))

    assert alice_view == bob_view
    assert alice_view[-1] == "z"


# ---------------------------------------------------------------------------
# End-to-end with History
# ---------------------------------------------------------------------------


def test_order_matches_history_after_a_full_exchange(tmp_path) -> None:
    """Ordering works over rows produced by History queue/commit."""

    from linkchat.core.client.history import History
    from linkchat.core.envelope import pad_payload
    from linkchat.core.payload import Payload, encode
    from linkchat.core.types import MailboxId

    def seal(payload: Payload) -> bytes:
        return pad_payload(encode(payload))

    alice = History(
        tmp_path / "alice.db",
        ALICE,
    )

    alice.open_conversation(
        PEER,
        send_mailbox=MailboxId("mb"),
        recv_mailbox=MailboxId("in"),
    )

    try:
        alice.queue_outgoing(
            PEER,
            "hola",
            sent_at=1.0,
            expires_at=99.0,
            seal=seal,
        )

        payload = Payload(
            text="hey",
            clock=Clock2(low=1, high=1),
            prev=None,
            sent_at=2.0,
        )

        alice.commit_incoming(
            PEER,
            EnvelopeId("in-1"),
            payload,
            received_at=NOW - 1.0,
        )

        alice.queue_outgoing(
            PEER,
            "bien",
            sent_at=4.0,
            expires_at=99.0,
            seal=seal,
        )

        shown = order(alice.messages(PEER))

        assert [d.message.text for d in shown] == [
            "hola",
            "hey",
            "bien",
        ]

    finally:
        alice.close()
