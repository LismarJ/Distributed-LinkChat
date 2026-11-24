"""The envelope format, frozen 2026-11-12 (gate G0).

An envelope is what a relay stores: opaque, authenticated, always the same
number of bytes. A relay learns three things from it and no more: that it
exists, which mailbox it is addressed to, and when it expires.

Two properties this file is responsible for:

**Length carries no information.** The plaintext payload is padded to exactly
`PAYLOAD_SIZE` *before* encryption, so every ciphertext comes out the same
length. Padding the ciphertext instead would leak the true length through the
padding offset, which is the mistake this layout avoids. SimpleX pads for the
same reason.

**Identity is content.** `envelope_id` is a hash of the envelope bytes. That is
what makes every relay operation idempotent, which is what lets us retry a call
whose outcome Ray could not tell us (fault-tolerance.md case 1).

Stdlib only, on purpose: this module is the one both work tracks depend on, so
it must not wait on the cryptography library decision or on msgpack.
"""

from __future__ import annotations

import hashlib

from .types import EnvelopeId

LENGTH_PREFIX_BYTES = 2
"""Bytes at the front of a padded block holding the true payload length.

Inside the encrypted region, so a relay cannot read it. Two bytes is enough
because `PAYLOAD_SIZE` is well under 65536.
"""

PAYLOAD_SIZE = 3904
"""Every plaintext payload is padded to exactly this before encryption.

The largest multiple of 16 for which the finished envelope fits in
`ENVELOPE_BUDGET`, measured with the cryptography of ADR-0009 (see
`ENVELOPE_SIZE`). Was 3968 until the library was chosen; that left 128 bytes
for an overhead that turned out to be 186.
"""

AEAD_BLOCK = 16
"""AES-CBC pads to a multiple of this, always adding at least one byte."""

AEAD_TAG = 32
"""HMAC-SHA-256 appended to the ciphertext."""

CIPHERTEXT_SIZE = PAYLOAD_SIZE + AEAD_BLOCK + AEAD_TAG
"""3952. `PAYLOAD_SIZE` is a multiple of 16, so the padding is one full block."""

ENVELOPE_HEADER_SIZE = 138
"""Everything in front of the ciphertext: version and flags (2), the X3DH header
slot (3 x 32) and the ratchet header (32 + 4 + 4). Layout in `crypto/wire.py`.
"""

ENVELOPE_SIZE = ENVELOPE_HEADER_SIZE + CIPHERTEXT_SIZE
"""4090: the exact length of every envelope, first message or not."""

MAX_PLAINTEXT = PAYLOAD_SIZE - LENGTH_PREFIX_BYTES
"""The largest payload that fits in one envelope, 3966 bytes.

Longer messages are refused rather than split. Phase 1 has no multi-envelope
message, and the UI enforces this limit before encrypting.
"""

ENVELOPE_BUDGET = 4096
"""An envelope must never exceed this many bytes.

Two reasons for 4 KiB. Ray passes small arguments inline and only puts larger
ones in its shared object store, with the threshold documented at about 100 KB;
staying far below it keeps envelopes out of the object store entirely, which
matters because an object there dies with the process that created it, and that
process is the sender (ray-architecture.md section 12). And it bounds relay
memory: ten thousand undelivered envelopes is about 40 MB.

The exact size is now `ENVELOPE_SIZE`; this stays as the ceiling that
`check_envelope_size` enforces.
"""

if PAYLOAD_SIZE % AEAD_BLOCK or ENVELOPE_SIZE > ENVELOPE_BUDGET:
    raise AssertionError("envelope constants are inconsistent")  # pragma: no cover

ENVELOPE_ID_BYTES = 32
"""Digest length of an envelope id, before hex encoding."""


class PayloadTooLarge(ValueError):
    """The plaintext payload does not fit in one envelope."""


class MalformedEnvelope(ValueError):
    """An envelope or padded block is not the shape this module produces.

    From our own padding code, it means a bug. From `crypto/wire.py`, it can
    also mean a relay handed back bytes that are not one of our envelopes,
    which the receiver drops without acknowledging.
    """


def pad_payload(payload: bytes) -> bytes:
    """Pad `payload` to exactly `PAYLOAD_SIZE` bytes, ready to be encrypted.

    Layout: a two-byte big-endian length, the payload, then zeros.

    Raises:
        PayloadTooLarge: if `payload` is longer than `MAX_PLAINTEXT`.
    """
    if len(payload) > MAX_PLAINTEXT:
        raise PayloadTooLarge(
            f"payload is {len(payload)} bytes, limit is {MAX_PLAINTEXT}"
        )
    prefix = len(payload).to_bytes(LENGTH_PREFIX_BYTES, "big")
    filler = b"\x00" * (MAX_PLAINTEXT - len(payload))
    return prefix + payload + filler


def unpad_payload(block: bytes) -> bytes:
    """Recover the payload from a decrypted padded block.

    Raises:
        MalformedEnvelope: if the block is the wrong length, claims a length
            that cannot fit, or has anything other than zeros in its padding.
    """
    if len(block) != PAYLOAD_SIZE:
        raise MalformedEnvelope(
            f"padded block is {len(block)} bytes, expected {PAYLOAD_SIZE}"
        )
    length = int.from_bytes(block[:LENGTH_PREFIX_BYTES], "big")
    if length > MAX_PLAINTEXT:
        raise MalformedEnvelope(
            f"block claims a {length}-byte payload, limit is {MAX_PLAINTEXT}"
        )
    start = LENGTH_PREFIX_BYTES
    end = start + length
    if block[end:] != b"\x00" * (PAYLOAD_SIZE - end):
        raise MalformedEnvelope("padding is not zeroed, which means a bug in ours")
    return block[start:end]


def envelope_id(envelope: bytes) -> EnvelopeId:
    """The content hash of `envelope`, as hex.

    BLAKE2b truncated to 32 bytes: in the standard library, fast, and also
    exposed by libsodium, so the same identifier can be computed elsewhere if
    we ever need to.

    Two senders cannot collide by accident, and a relay cannot forge an id for
    content it does not have, which is what the storage audit relies on.
    """
    if not envelope:
        raise MalformedEnvelope("an envelope cannot be empty")
    return EnvelopeId(
        hashlib.blake2b(envelope, digest_size=ENVELOPE_ID_BYTES).hexdigest()
    )


def check_envelope_size(envelope: bytes) -> None:
    """Raise if `envelope` would not travel inline through the runtime.

    Called by the sender before storing, so that a library whose header is
    larger than we assumed fails here rather than quietly turning every message
    into an object-store entry.

    Raises:
        PayloadTooLarge: if the envelope exceeds `ENVELOPE_BUDGET`.
    """
    if len(envelope) > ENVELOPE_BUDGET:
        raise PayloadTooLarge(
            f"envelope is {len(envelope)} bytes, budget is {ENVELOPE_BUDGET}; "
            "the crypto overhead is larger than PAYLOAD_SIZE assumed"
        )
