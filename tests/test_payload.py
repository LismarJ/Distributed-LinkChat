"""The payload format, including the part that is easy to get wrong.

The clock's two slots are assigned by user id, not by "mine" and "theirs". If
that were ever changed to a per-sender perspective, both sides would compute
different orders from the same data and nobody would notice until a
conversation looked scrambled.
"""

from __future__ import annotations

import pytest

# Before importing the module under test, which imports msgpack itself.
msgpack = pytest.importorskip(
    "msgpack",
    reason="msgpack comes from the project venv: `uv venv --python 3.12 && uv sync`",
)

from linkchat.core.envelope import MAX_PLAINTEXT, PayloadTooLarge  # noqa: E402
from linkchat.core.payload import (  # noqa: E402
    FORMAT_VERSION,
    Clock2,
    MalformedPayload,
    Payload,
    decode,
    encode,
    slot_of,
)
from linkchat.core.types import EnvelopeId, UserId  # noqa: E402


def a_payload(**overrides: object) -> Payload:
    base = {
        "text": "hello",
        "clock": Clock2(low=1, high=0),
        "prev": None,
        "sent_at": 1763000000.0,
    }
    base.update(overrides)
    return Payload(**base)  # type: ignore[arg-type]


def test_round_trips() -> None:
    original = a_payload(prev=EnvelopeId("ab" * 32))
    assert decode(encode(original)) == original


def test_round_trips_with_no_predecessor() -> None:
    assert decode(encode(a_payload(prev=None))).prev is None


def test_round_trips_unicode() -> None:
    original = a_payload(text="hola, ¿qué tal? 🙂")
    assert decode(encode(original)).text == original.text


def test_a_long_message_is_refused_rather_than_truncated() -> None:
    with pytest.raises(PayloadTooLarge):
        encode(a_payload(text="x" * (MAX_PLAINTEXT + 10)))


def test_an_unknown_version_is_refused() -> None:
    document = {
        "version": FORMAT_VERSION + 1,
        "kind": "msg",
        "text": "hi",
        "clock": [1, 0],
        "prev": None,
        "sent_at": 1.0,
    }
    with pytest.raises(MalformedPayload, match="version"):
        decode(msgpack.packb(document, use_bin_type=True))


def test_garbage_is_refused() -> None:
    with pytest.raises(MalformedPayload):
        decode(b"\xc1not msgpack at all")


def test_a_missing_field_is_refused() -> None:
    with pytest.raises(MalformedPayload):
        decode(msgpack.packb({"version": FORMAT_VERSION}, use_bin_type=True))


@pytest.mark.parametrize("clock", [[1], [1, 2, 3], ["a", "b"], [1, -1], "nope"])
def test_a_malformed_clock_is_refused(clock: object) -> None:
    document = {
        "version": FORMAT_VERSION,
        "kind": "msg",
        "text": "hi",
        "clock": clock,
        "prev": None,
        "sent_at": 1.0,
    }
    with pytest.raises(MalformedPayload):
        decode(msgpack.packb(document, use_bin_type=True))


def test_both_participants_agree_which_slot_is_whose() -> None:
    alice = UserId("aaaa")
    bob = UserId("bbbb")
    assert slot_of(alice, bob) == "low"
    assert slot_of(bob, alice) == "high"


def test_slot_assignment_does_not_depend_on_who_is_asking() -> None:
    """The point of assigning slots by id rather than by perspective."""
    one = UserId("f00d")
    two = UserId("0ff1")
    assert slot_of(one, two) != slot_of(two, one)
    assert {slot_of(one, two), slot_of(two, one)} == {"low", "high"}


def test_a_conversation_with_oneself_is_rejected() -> None:
    me = UserId("abcd")
    with pytest.raises(MalformedPayload):
        slot_of(me, me)
