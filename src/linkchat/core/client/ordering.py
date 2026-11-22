"""Causal ordering for display (C7).

Pure function over StoredMessage rows.

Responsibilities:
- Respect vector clocks.
- Respect the predecessor (prev) chain.
- Deterministically order concurrent messages.
- Hold messages that cannot yet be delivered.
- Release expired messages with an explicit gap marker, once per gap.
- Detect conflicting messages with the same sender counter.

This module does not perform network I/O, ACKs, decryption,
database writes, or outbox operations.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from linkchat.core.client.history import StoredMessage
from linkchat.core.payload import slot_of
from linkchat.core.types import EnvelopeId, UserId

DEFAULT_HOLD_TIMEOUT_S = 30.0
"""Time before a blocked message may be displayed with a gap marker."""


@dataclass(frozen=True, slots=True)
class DisplayMessage:
    message: StoredMessage
    after_gap: bool = False
    """True when displayed despite an unresolved causal gap."""


def order_messages(
    messages: Sequence[StoredMessage],
    *,
    me: UserId,
    peer: UserId,
    now: float,
    hold_timeout_s: float = DEFAULT_HOLD_TIMEOUT_S,
) -> list[DisplayMessage]:
    """Return stored messages that can be displayed in causal order.

    A message is normally ready when:

    1. Its sender counter is exactly the next expected counter.
    2. Its other vector-clock counter has already been delivered.
    3. Its predecessor (prev), if any, has already been delivered.

    Concurrent messages are deterministically ordered by envelope_id.

    A message blocked by a missing dependency is held. Once its timeout
    expires it is displayed with after_gap=True, and that release counts as
    delivering it: the clock advances, so what follows it flows normally and
    the gap is marked once, not on every later message.
    """

    if hold_timeout_s < 0:
        raise ValueError("hold_timeout_s cannot be negative")

    my_slot = slot_of(me, peer)
    their_slot = slot_of(peer, me)

    delivered_upto = {"low": 0, "high": 0}
    delivered: set[EnvelopeId] = set()
    output: list[DisplayMessage] = []

    # Remove duplicated envelope IDs before ordering.
    unique_messages: dict[EnvelopeId, StoredMessage] = {}
    for message in messages:
        unique_messages.setdefault(message.envelope_id, message)
    pending = list(unique_messages.values())

    # Messages sharing a sender counter cannot go through the normal path.
    conflicting_ids = _find_counter_conflicts(pending, my_slot, their_slot)

    def deliver(message: StoredMessage, *, after_gap: bool) -> None:
        output.append(DisplayMessage(message=message, after_gap=after_gap))
        delivered.add(message.envelope_id)
        slot = _sender_slot(message, my_slot, their_slot)
        counter = _counter_for_slot(message, slot)
        delivered_upto[slot] = max(delivered_upto[slot], counter)

    while True:
        # Normal causal delivery, in rounds, until nothing more is ready.
        while True:
            ready = [
                message
                for message in pending
                if message.envelope_id not in conflicting_ids
                and _is_ready(
                    message,
                    delivered,
                    delivered_upto["low"],
                    delivered_upto["high"],
                    my_slot,
                    their_slot,
                )
            ]
            if not ready:
                break
            # Stable tie-break for concurrent messages.
            for message in sorted(ready, key=lambda m: m.envelope_id):
                deliver(message, after_gap=False)
            pending = [m for m in pending if m.envelope_id not in delivered]

        # Stuck: release the earliest message whose timeout has expired.
        released = _next_timed_out(pending, now, hold_timeout_s, my_slot, their_slot)
        if released is None:
            return output
        deliver(released, after_gap=True)
        pending.remove(released)


# =============================================================
# Internal helpers
# =============================================================


def _sender_slot(
    message: StoredMessage,
    my_slot: str,
    their_slot: str,
) -> str:
    """Return the vector-clock slot belonging to this message's sender."""

    return my_slot if message.outgoing else their_slot


def _counter_for_slot(
    message: StoredMessage,
    slot: str,
) -> int:
    """Return the counter belonging to the selected vector-clock slot."""

    return message.clock.low if slot == "low" else message.clock.high


def _find_counter_conflicts(
    messages: Sequence[StoredMessage],
    my_slot: str,
    their_slot: str,
) -> set[EnvelopeId]:
    """Detect messages sharing a sender slot and counter.

    Two different messages from the same sender must not have the
    same vector-clock counter.

    Conflicting messages are excluded from normal causal delivery.
    They may eventually be released with after_gap=True after timeout.
    """

    counters: dict[tuple[str, int], list[StoredMessage]] = {}

    for message in messages:
        # History gives outgoing messages unique counters; only a peer can
        # send a conflict, and only incoming messages can time out of one.
        if message.outgoing:
            continue
        slot = _sender_slot(
            message,
            my_slot,
            their_slot,
        )

        counter = _counter_for_slot(
            message,
            slot,
        )

        key = (slot, counter)

        counters.setdefault(key, []).append(message)

    conflicts: set[EnvelopeId] = set()

    for group in counters.values():
        # Multiple messages with the same sender counter
        # constitute a conflict.
        if len(group) > 1:
            for message in group:
                conflicts.add(message.envelope_id)

    return conflicts


def _is_ready(
    msg: StoredMessage,
    delivered: set[EnvelopeId],
    delivered_low: int,
    delivered_high: int,
    my_slot: str,
    their_slot: str,
) -> bool:
    """Check whether a message can be delivered without a causal gap."""

    sender_slot = _sender_slot(
        msg,
        my_slot,
        their_slot,
    )

    counter = _counter_for_slot(
        msg,
        sender_slot,
    )

    other_counter = msg.clock.high if sender_slot == "low" else msg.clock.low

    delivered_this = delivered_low if sender_slot == "low" else delivered_high

    delivered_other = delivered_high if sender_slot == "low" else delivered_low

    # Sender must be the next expected message.
    if counter != delivered_this + 1:
        return False

    # The other participant's causal history must already be known.
    if other_counter > delivered_other:
        return False

    # The predecessor must already have been displayed.
    return msg.prev is None or msg.prev in delivered


def _order_key(
    msg: StoredMessage,
    my_slot: str,
    their_slot: str,
) -> tuple[str, int, str]:
    """Ordering key for timeout releases."""

    sender_slot = _sender_slot(
        msg,
        my_slot,
        their_slot,
    )

    counter = _counter_for_slot(
        msg,
        sender_slot,
    )

    return (
        sender_slot,
        counter,
        msg.envelope_id,
    )


def _next_timed_out(
    pending: list[StoredMessage],
    now: float,
    hold_timeout_s: float,
    my_slot: str,
    their_slot: str,
) -> StoredMessage | None:
    """The held message to release next with a gap marker, if any.

    Only incoming messages time out: an outgoing message depends only on
    messages this client already has, which are released in turn.
    """
    candidates = sorted(
        (
            m
            for m in pending
            if m.received_at is not None and now - m.received_at >= hold_timeout_s
        ),
        key=lambda m: _order_key(m, my_slot, their_slot),
    )
    for message in candidates:
        if not _blocked_by_lower_same_sender(message, pending, my_slot, their_slot):
            return message
    return None


def _blocked_by_lower_same_sender(
    msg: StoredMessage,
    pending: list[StoredMessage],
    my_slot: str,
    their_slot: str,
) -> bool:
    """Do not release a higher sender counter before a lower held one."""
    sender_slot = _sender_slot(msg, my_slot, their_slot)
    counter = _counter_for_slot(msg, sender_slot)
    return any(
        other is not msg
        and _sender_slot(other, my_slot, their_slot) == sender_slot
        and _counter_for_slot(other, sender_slot) < counter
        for other in pending
    )
