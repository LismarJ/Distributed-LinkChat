"""The fixed envelope layout: exact size, round trip, rejection and tampering."""

from __future__ import annotations

import pytest
from doubleratchet import AuthenticationFailedException, EncryptedMessage
from x3dh import Header as X3DHHeader

from linkchat.core.crypto.suite import (
    LinkChatDoubleRatchet,
    accept_ratchet,
    new_x3dh_state,
    start_ratchet,
)
from linkchat.core.crypto.wire import (
    AUTHENTICATED_PREFIX_SIZE,
    FLAG_X3DH,
    authenticated_prefix,
    pack,
    unpack,
)
from linkchat.core.envelope import (
    ENVELOPE_BUDGET,
    ENVELOPE_SIZE,
    MAX_PLAINTEXT,
    MalformedEnvelope,
    pad_payload,
    unpad_payload,
)


async def a_session(
    first_text: bytes = b"hola",
) -> tuple[LinkChatDoubleRatchet, LinkChatDoubleRatchet, bytes, bytes]:
    """Alice opens a session with Bob through a packed first envelope.

    Returns both ratchets, Alice's first envelope and the X3DH associated data.
    """
    bob, alice = new_x3dh_state(), new_x3dh_state()
    sk, ad, header = await alice.get_shared_secret_active(
        bob.bundle, require_pre_key=False
    )
    prefix = authenticated_prefix(header)
    dr_a, first = await start_ratchet(
        sk, bob.bundle.signed_pre_key, pad_payload(first_text), ad + prefix
    )
    envelope = pack(header, first)

    got = unpack(envelope)
    assert got.x3dh_header is not None
    sk_b, ad_b, spk = await bob.get_shared_secret_passive(
        got.x3dh_header, require_pre_key=False
    )
    dr_b, plain = await accept_ratchet(
        sk_b, spk.priv, got.message, ad_b + got.authenticated_prefix
    )
    assert unpad_payload(plain) == first_text
    return dr_a, dr_b, envelope, ad


async def a_normal_envelope(dr: LinkChatDoubleRatchet, ad: bytes, text: bytes) -> bytes:
    message = await dr.encrypt_message(pad_payload(text), ad + authenticated_prefix(None))
    return pack(None, message)


async def read(dr: LinkChatDoubleRatchet, ad: bytes, envelope: bytes) -> bytes:
    got = unpack(envelope)
    return unpad_payload(
        await dr.decrypt_message(got.message, ad + got.authenticated_prefix)
    )


# -- size ---------------------------------------------------------------------


def test_the_layout_fits_the_budget() -> None:
    assert ENVELOPE_SIZE == 4090
    assert ENVELOPE_SIZE <= ENVELOPE_BUDGET


async def test_every_envelope_has_the_same_size() -> None:
    dr_a, _, first, ad = await a_session(b"")
    short = await a_normal_envelope(dr_a, ad, b"")
    longest = await a_normal_envelope(dr_a, ad, b"x" * MAX_PLAINTEXT)

    assert len(first) == len(short) == len(longest) == ENVELOPE_SIZE


# -- round trip ---------------------------------------------------------------


async def test_first_envelope_carries_the_x3dh_header() -> None:
    _, _, first, _ = await a_session()

    assert first[1] == FLAG_X3DH
    assert unpack(first).x3dh_header is not None


async def test_later_envelopes_round_trip_without_it() -> None:
    dr_a, dr_b, _, ad = await a_session()
    envelope = await a_normal_envelope(dr_a, ad, b"second")

    assert unpack(envelope).x3dh_header is None
    assert await read(dr_b, ad, envelope) == b"second"


async def test_counters_are_readable_without_decrypting() -> None:
    """The receiver can sort a backlog by the ratchet counter before decrypting."""
    dr_a, _, _, ad = await a_session()
    envelopes = [await a_normal_envelope(dr_a, ad, b"m") for _ in range(3)]

    counters = [unpack(e).message.header.sending_chain_length for e in envelopes]
    assert counters == sorted(counters) and len(set(counters)) == 3


# -- tampering ----------------------------------------------------------------


async def test_an_altered_prefix_fails_authentication() -> None:
    """A relay sets the X3DH flag and fills the slot: Bob's decryption must fail."""
    dr_a, dr_b, _, ad = await a_session()
    envelope = bytearray(await a_normal_envelope(dr_a, ad, b"secret"))
    envelope[1] = FLAG_X3DH
    envelope[2:AUTHENTICATED_PREFIX_SIZE] = b"\x07" * (AUTHENTICATED_PREFIX_SIZE - 2)

    with pytest.raises(AuthenticationFailedException):
        await read(dr_b, ad, bytes(envelope))


async def test_an_altered_counter_fails_authentication() -> None:
    dr_a, dr_b, _, ad = await a_session()
    envelope = bytearray(await a_normal_envelope(dr_a, ad, b"secret"))
    envelope[137] ^= 0x01  # last byte of the sending chain length

    with pytest.raises(Exception):  # noqa: B017 - the library's choice of error
        await read(dr_b, ad, bytes(envelope))


# -- rejection ----------------------------------------------------------------


@pytest.fixture
async def normal_envelope() -> bytes:
    dr_a, _, _, ad = await a_session()
    return await a_normal_envelope(dr_a, ad, b"m")


@pytest.mark.parametrize("length", [0, ENVELOPE_SIZE - 1, ENVELOPE_SIZE + 1])
def test_wrong_length_is_rejected(length: int) -> None:
    with pytest.raises(MalformedEnvelope, match="bytes"):
        unpack(b"\x01" * length)


@pytest.mark.parametrize(
    ("offset", "value", "why"),
    [
        (0, 2, "version"),
        (1, 0x02, "unknown flags"),
        (1, FLAG_X3DH, "flag is set but the header is empty"),
        (40, 0xFF, "without its flag"),
    ],
)
def test_malformed_prefix_is_rejected(
    normal_envelope: bytes, offset: int, value: int, why: str
) -> None:
    envelope = bytearray(normal_envelope)
    envelope[offset] = value

    with pytest.raises(MalformedEnvelope, match=why):
        unpack(bytes(envelope))


def test_one_time_prekeys_are_refused() -> None:
    header = X3DHHeader(b"i" * 32, b"e" * 32, b"s" * 32, b"o" * 32)

    with pytest.raises(ValueError, match="one-time"):
        authenticated_prefix(header)


async def test_unpadded_plaintext_cannot_be_packed() -> None:
    dr_a, _, _, ad = await a_session()
    message: EncryptedMessage = await dr_a.encrypt_message(b"not padded", ad)

    with pytest.raises(ValueError, match="PAYLOAD_SIZE"):
        pack(None, message)
