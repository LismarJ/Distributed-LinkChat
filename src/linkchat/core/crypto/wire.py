"""The envelope on the wire: a fixed binary layout.

Every envelope is exactly `ENVELOPE_SIZE` bytes, whether or not it starts a
session, so its length tells a relay nothing:

    offset  size  field
         0     1  version
         1     1  flags (bit 0: the X3DH header slot is in use)
         2    32  X3DH identity key      \\
        34    32  X3DH ephemeral key      > zeros when the flag is off
        66    32  X3DH signed prekey     /
        98    32  ratchet public key
       130     4  previous sending chain length (big-endian)
       134     4  sending chain length (big-endian)
       138  3952  ciphertext

The first 98 bytes are the *authenticated prefix*: the sender adds them to the
associated data, so a relay that flips a flag or edits the X3DH slot makes
decryption fail. The ratchet header is authenticated by `suite.py`.

What a relay can still read: the ratchet header is in the clear, as in the
standard Double Ratchet, so it can tell envelopes of one session apart from
another and see message numbers. The flag adds nothing to that: a first message
already shows counters of zero. Signal hides this with header encryption, which
the library does not offer.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

from doubleratchet import EncryptedMessage
from doubleratchet import Header as RatchetHeader
from x3dh import Header as X3DHHeader

from linkchat.core.envelope import (
    CIPHERTEXT_SIZE,
    ENVELOPE_HEADER_SIZE,
    ENVELOPE_SIZE,
    MalformedEnvelope,
)

WIRE_VERSION = 1
FLAG_X3DH = 0x01

KEY_SIZE = 32
_PREFIX = struct.Struct(">BB32s32s32s")  # version, flags, IK, EK, SPK
_RATCHET = struct.Struct(">32sII")  # ratchet pub, previous length, length
_EMPTY_SLOT = bytes(3 * KEY_SIZE)
_MAX_COUNTER = 2**32 - 1

AUTHENTICATED_PREFIX_SIZE = _PREFIX.size
assert _PREFIX.size + _RATCHET.size == ENVELOPE_HEADER_SIZE


@dataclass(frozen=True, slots=True)
class Unpacked:
    x3dh_header: X3DHHeader | None
    """Present on the messages that may start a session."""

    message: EncryptedMessage
    authenticated_prefix: bytes
    """Add to the associated data before decrypting, exactly as the sender did."""


def authenticated_prefix(x3dh_header: X3DHHeader | None) -> bytes:
    """The 98 bytes the sender must authenticate. Needed before encrypting."""
    if x3dh_header is None:
        return _PREFIX.pack(WIRE_VERSION, 0, *(bytes(KEY_SIZE),) * 3)
    if x3dh_header.pre_key is not None:
        raise ValueError("phase 1 uses no one-time prekeys")
    keys = (
        x3dh_header.identity_key,
        x3dh_header.ephemeral_key,
        x3dh_header.signed_pre_key,
    )
    if any(len(k) != KEY_SIZE for k in keys):
        raise ValueError("an X3DH key is not 32 bytes")
    return _PREFIX.pack(WIRE_VERSION, FLAG_X3DH, *keys)


def pack(x3dh_header: X3DHHeader | None, message: EncryptedMessage) -> bytes:
    """Lay out an encrypted message. Raises ValueError if it cannot be exact."""
    ratchet = message.header
    if len(ratchet.ratchet_pub) != KEY_SIZE:
        raise ValueError("the ratchet key is not 32 bytes")
    counters = (ratchet.previous_sending_chain_length, ratchet.sending_chain_length)
    if not all(0 <= c <= _MAX_COUNTER for c in counters):
        raise ValueError("a ratchet counter does not fit in 4 bytes")
    if len(message.ciphertext) != CIPHERTEXT_SIZE:
        raise ValueError(
            f"ciphertext is {len(message.ciphertext)} bytes, expected {CIPHERTEXT_SIZE}:"
            " was the plaintext padded to PAYLOAD_SIZE?"
        )
    envelope = b"".join(
        [
            authenticated_prefix(x3dh_header),
            _RATCHET.pack(ratchet.ratchet_pub, *counters),
            message.ciphertext,
        ]
    )
    assert len(envelope) == ENVELOPE_SIZE
    return envelope


def unpack(envelope: bytes) -> Unpacked:
    """Read an envelope. Raises MalformedEnvelope if it is not one of ours."""
    if len(envelope) != ENVELOPE_SIZE:
        raise MalformedEnvelope(f"envelope is {len(envelope)} bytes, not {ENVELOPE_SIZE}")

    version, flags, ik, ek, spk = _PREFIX.unpack_from(envelope, 0)
    if version != WIRE_VERSION:
        raise MalformedEnvelope(
            f"wire version {version}, this build reads {WIRE_VERSION}"
        )
    if flags & ~FLAG_X3DH:
        raise MalformedEnvelope(f"unknown flags {flags:#04x}")

    slot = ik + ek + spk
    if flags & FLAG_X3DH:
        if slot == _EMPTY_SLOT:
            raise MalformedEnvelope("the X3DH flag is set but the header is empty")
        x3dh_header: X3DHHeader | None = X3DHHeader(ik, ek, spk, None)
    else:
        if slot != _EMPTY_SLOT:
            raise MalformedEnvelope("an X3DH header is present without its flag")
        x3dh_header = None

    ratchet_pub, previous, current = _RATCHET.unpack_from(envelope, _PREFIX.size)
    ciphertext = envelope[ENVELOPE_HEADER_SIZE:]
    return Unpacked(
        x3dh_header=x3dh_header,
        message=EncryptedMessage(
            RatchetHeader(ratchet_pub, previous, current), ciphertext
        ),
        authenticated_prefix=envelope[:AUTHENTICATED_PREFIX_SIZE],
    )
