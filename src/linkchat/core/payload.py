"""What is inside an envelope, before encryption. Frozen 2026-11-12 (gate G0).

A relay never sees any of this. It exists so that the recipient can put
messages in a sensible order and drop duplicates, which requirements R5 and R6
ask for and which the runtime gives us nothing towards: Ray does not order
calls to an async actor, and its retries can repeat them.

This module freezes the field names and what they mean. The ordering algorithm
that reads them (comparison, the hold-and-release buffer, gap markers) is task
C7 and lives in the client.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, cast

import msgpack

from .envelope import MAX_PLAINTEXT, PayloadTooLarge
from .types import EnvelopeId, UserId

FORMAT_VERSION: Final = 1
"""Bumped when a field changes meaning. A reader refuses a version it does not
know rather than guessing, because guessing would corrupt someone's history."""

KIND_MESSAGE: Final = "msg"
"""The only kind in phase 1. Read receipts were cut (ADR-0014), and they would
have been a second kind here."""


class MalformedPayload(ValueError):
    """A decoded payload is not something this version can read."""


@dataclass(frozen=True, slots=True)
class Clock2:
    """A two-element vector clock for one conversation.

    One counter per participant, counting messages that participant has sent.

    The slots are **not** "mine" and "theirs", because then the same clock
    would mean different things on the two sides and neither could compare
    them. They are assigned by the participants' user ids: `low` belongs to the
    lexicographically smaller id, `high` to the other. Both clients compute the
    same assignment from the same two ids without agreeing on anything.
    """

    low: int
    high: int

    def as_list(self) -> list[int]:
        return [self.low, self.high]

    @staticmethod
    def from_list(values: Any) -> Clock2:
        if (
            not isinstance(values, (list, tuple))
            or len(values) != 2
            or not all(isinstance(v, int) and v >= 0 for v in values)
        ):
            raise MalformedPayload(f"clock must be two non-negative integers: {values!r}")
        return Clock2(low=int(values[0]), high=int(values[1]))


def slot_of(user: UserId, other: UserId) -> str:
    """Which slot of the clock belongs to `user` in a conversation with `other`.

    Returns "low" or "high". Raises if the two ids are equal, which would be a
    conversation with oneself and is not a case phase 1 has.
    """
    if user == other:
        raise MalformedPayload("a conversation needs two different participants")
    return "low" if user < other else "high"


@dataclass(frozen=True, slots=True)
class Payload:
    """The decrypted contents of one message."""

    text: str
    clock: Clock2
    prev: EnvelopeId | None
    """The envelope id of the sender's previous message in this conversation.

    Gives the recipient a second, independent ordering signal: the clock says
    what is concurrent, this says what came directly before. `None` for the
    first message a participant sends.
    """

    sent_at: float
    """The sender's wall clock, seconds since the epoch.

    **For display only. Never for ordering.** Two clients' clocks disagree, and
    nothing stops a client from lying about this. Ordering uses `clock` and
    `prev`, which are causal and cannot be faked into a wrong order without the
    recipient noticing a gap.
    """

    kind: str = KIND_MESSAGE


def encode(payload: Payload) -> bytes:
    """Encode a payload, ready to be padded and encrypted.

    Raises:
        PayloadTooLarge: if the encoded form cannot fit in one envelope. Raised
            here rather than at padding time so the caller learns which message
            was too long.
    """
    document = {
        "version": FORMAT_VERSION,
        "kind": payload.kind,
        "text": payload.text,
        "clock": payload.clock.as_list(),
        "prev": payload.prev,
        "sent_at": payload.sent_at,
    }
    encoded = cast(bytes, msgpack.packb(document, use_bin_type=True))
    if encoded is None:  # pragma: no cover - msgpack returns bytes for a dict
        raise MalformedPayload("msgpack produced nothing")
    if len(encoded) > MAX_PLAINTEXT:
        raise PayloadTooLarge(
            f"encoded payload is {len(encoded)} bytes, limit is {MAX_PLAINTEXT}"
        )
    return encoded


def decode(encoded: bytes) -> Payload:
    """Decode a payload that came out of an envelope.

    Raises:
        MalformedPayload: on an unknown version, a missing field or a wrong
            type. The caller treats this as a message it cannot display rather
            than as a reason to stop, because one bad payload must not stall a
            conversation.
    """
    try:
        document = msgpack.unpackb(encoded, raw=False, strict_map_key=True)
    except Exception as exc:
        raise MalformedPayload(f"not valid msgpack: {exc}") from exc

    if not isinstance(document, dict):
        raise MalformedPayload(f"expected a map, got {type(document).__name__}")

    version = document.get("version")
    if version != FORMAT_VERSION:
        raise MalformedPayload(
            f"payload format version {version!r}, this build reads {FORMAT_VERSION}"
        )

    text = document.get("text")
    if not isinstance(text, str):
        raise MalformedPayload("text must be a string")

    kind = document.get("kind")
    if not isinstance(kind, str):
        raise MalformedPayload("kind must be a string")

    prev = document.get("prev")
    if prev is not None and not isinstance(prev, str):
        raise MalformedPayload("prev must be a string or absent")

    sent_at = document.get("sent_at")
    if not isinstance(sent_at, (int, float)):
        raise MalformedPayload("sent_at must be a number")

    return Payload(
        text=text,
        clock=Clock2.from_list(document.get("clock")),
        prev=EnvelopeId(prev) if prev is not None else None,
        sent_at=float(sent_at),
        kind=kind,
    )
