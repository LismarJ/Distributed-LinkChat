"""Causal ordering for display (C7).

Pure function over StoredMessage rows.

Responsibilities:
- Respect vector clocks.
- Respect the predecessor (prev) chain.
- Deterministically order concurrent messages.
- Hold messages that cannot yet be delivered.
- Release expired messages with an explicit gap marker.
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

    Messages blocked by missing causal dependencies are held. When their
    timeout expires, they may be displayed with after_gap=True.

    Timeout releases do not advance the delivered vector clock.
    """

    if hold_timeout_s < 0:
        raise ValueError("hold_timeout_s cannot be negative")

    my_slot = slot_of(me, peer)
    their_slot = slot_of(peer, me)

    delivered_low = 0
    delivered_high = 0

    delivered: set[EnvelopeId] = set()
    output: list[DisplayMessage] = []

    # Remove duplicated envelope IDs before ordering.
    unique_messages: dict[EnvelopeId, StoredMessage] = {}

    for message in messages:
        unique_messages.setdefault(message.envelope_id, message)

    pending = list(unique_messages.values())

    # Identify messages that share the same sender and vector-clock counter.
    # These are conflicting messages and cannot be safely delivered
    # through the normal causal path.
    conflicting_ids = _find_counter_conflicts(
        pending,
        my_slot,
        their_slot,
    )

    # ---------------------------------------------------------
    # Normal causal delivery
    # ---------------------------------------------------------

    while True:
        newly_ready: list[StoredMessage] = []
        still_pending: list[StoredMessage] = []

        for message in pending:
            if message.envelope_id in conflicting_ids:
                still_pending.append(message)
                continue

            if _is_ready(
                message,
                delivered,
                delivered_low,
                delivered_high,
                my_slot,
                their_slot,
            ):
                newly_ready.append(message)
            else:
                still_pending.append(message)

        pending = still_pending

        if not newly_ready:
            break

        # Stable tie-break for concurrent messages.
        newly_ready.sort(key=lambda message: message.envelope_id)

        for message in newly_ready:
            output.append(
                DisplayMessage(
                    message=message,
                    after_gap=False,
                )
            )

            delivered.add(message.envelope_id)

            sender_slot = (
                my_slot if message.outgoing else their_slot
            )

            counter = (
                message.clock.low
                if sender_slot == "low"
                else message.clock.high
            )

            if sender_slot == "low":
                delivered_low = max(delivered_low, counter)
            else:
                delivered_high = max(delivered_high, counter)

    # ---------------------------------------------------------
    # Timeout delivery
    # ---------------------------------------------------------

    pending_sorted = sorted(
        pending,
        key=lambda message: _order_key(
            message,
            my_slot,
            their_slot,
        ),
    )

    still_held = set(pending)

    for message in pending_sorted:

        # Outgoing messages do not have a receive timestamp.
        if message.received_at is None:
            continue

        elapsed = now - message.received_at

        if elapsed < hold_timeout_s:
            continue

        # Prevent releasing a later message before an earlier
        # pending message from the same sender.
        if _blocked_by_lower_same_sender(
            message,
            pending_sorted,
            still_held,
            my_slot,
            their_slot,
        ):
            continue

        output.append(
            DisplayMessage(
                message=message,
                after_gap=True,
            )
        )

        still_held.discard(message)

    return output


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

    return (
        message.clock.low
        if slot == "low"
        else message.clock.high
    )


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

    other_counter = (
        msg.clock.high
        if sender_slot == "low"
        else msg.clock.low
    )

    delivered_this = (
        delivered_low
        if sender_slot == "low"
        else delivered_high
    )

    delivered_other = (
        delivered_high
        if sender_slot == "low"
        else delivered_low
    )

    # Sender must be the next expected message.
    if counter != delivered_this + 1:
        return False

    # The other participant's causal history must already be known.
    if other_counter > delivered_other:
        return False

    # The predecessor must already have been displayed.
    if msg.prev is not None and msg.prev not in delivered:
        return False

    return True


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


def _blocked_by_lower_same_sender(
    msg: StoredMessage,
    all_pending: list[StoredMessage],
    still_held: set[StoredMessage],
    my_slot: str,
    their_slot: str,
) -> bool:
    """Do not release a higher sender counter before a lower one."""

    sender_slot = _sender_slot(
        msg,
        my_slot,
        their_slot,
    )

    counter = _counter_for_slot(
        msg,
        sender_slot,
    )

    for other in all_pending:

        if other is msg:
            continue

        if other not in still_held:
            continue

        other_slot = _sender_slot(
            other,
            my_slot,
            their_slot,
        )

        if other_slot != sender_slot:
            continue

        other_counter = _counter_for_slot(
            other,
            sender_slot,
        )

        if other_counter < counter:
            return True

    return False