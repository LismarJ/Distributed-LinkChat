"""The envelope format: fixed length, and identity from content.

These two properties are load-bearing. Fixed length is why a relay cannot tell
a one-word message from a long one. Identity from content is why every relay
operation is idempotent, which is the only reason we can safely retry a call
whose outcome the runtime could not report.
"""

from __future__ import annotations

import pytest

from linkchat.core.envelope import (
    ENVELOPE_BUDGET,
    MAX_PLAINTEXT,
    PAYLOAD_SIZE,
    MalformedEnvelope,
    PayloadTooLarge,
    check_envelope_size,
    envelope_id,
    pad_payload,
    unpad_payload,
)


@pytest.mark.parametrize(
    "payload",
    [b"", b"h", b"hello", b"x" * 100, b"x" * (MAX_PLAINTEXT - 1), b"x" * MAX_PLAINTEXT],
)
def test_padding_round_trips(payload: bytes) -> None:
    assert unpad_payload(pad_payload(payload)) == payload


@pytest.mark.parametrize("length", [0, 1, 7, 500, MAX_PLAINTEXT])
def test_every_padded_block_is_the_same_length(length: int) -> None:
    """The property the whole padding scheme exists for."""
    assert len(pad_payload(b"x" * length)) == PAYLOAD_SIZE


def test_padded_blocks_of_different_messages_are_indistinguishable_by_length() -> None:
    short = pad_payload(b"ok")
    long = pad_payload(b"a considerably longer message than the other one")
    assert len(short) == len(long) == PAYLOAD_SIZE


def test_payload_longer_than_the_limit_is_refused() -> None:
    with pytest.raises(PayloadTooLarge):
        pad_payload(b"x" * (MAX_PLAINTEXT + 1))


def test_unpadding_rejects_a_wrong_length_block() -> None:
    with pytest.raises(MalformedEnvelope):
        unpad_payload(b"\x00\x02ok")


def test_unpadding_rejects_an_impossible_claimed_length() -> None:
    block = (MAX_PLAINTEXT + 1).to_bytes(2, "big") + b"\x00" * MAX_PLAINTEXT
    with pytest.raises(MalformedEnvelope):
        unpad_payload(block)


def test_unpadding_rejects_dirty_padding() -> None:
    """Nothing should ever write in the padding, so a byte there means our bug."""
    block = bytearray(pad_payload(b"ok"))
    block[-1] = 0x41
    with pytest.raises(MalformedEnvelope):
        unpad_payload(bytes(block))


def test_envelope_id_is_deterministic() -> None:
    assert envelope_id(b"same bytes") == envelope_id(b"same bytes")


def test_envelope_id_differs_for_different_content() -> None:
    assert envelope_id(b"one") != envelope_id(b"two")


def test_envelope_id_is_hex_of_the_expected_width() -> None:
    value = envelope_id(b"anything")
    assert len(value) == 64
    int(value, 16)


def test_envelope_id_rejects_an_empty_envelope() -> None:
    with pytest.raises(MalformedEnvelope):
        envelope_id(b"")


def test_size_check_accepts_a_realistic_envelope() -> None:
    """A padded payload plus a generous allowance for header, nonce and tag."""
    check_envelope_size(b"x" * (PAYLOAD_SIZE + 100))


def test_size_check_refuses_an_envelope_over_budget() -> None:
    with pytest.raises(PayloadTooLarge):
        check_envelope_size(b"x" * (ENVELOPE_BUDGET + 1))


def test_the_budget_leaves_room_for_crypto_overhead() -> None:
    """If this ever fails, PAYLOAD_SIZE was raised without raising the budget."""
    assert ENVELOPE_BUDGET - PAYLOAD_SIZE >= 64
