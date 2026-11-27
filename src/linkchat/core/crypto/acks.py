"""Signed acknowledgements: only a mailbox's recipient may delete from it.

An `ack` makes a relay forget records, so it is the one relay call whose misuse
loses mail. A mailbox id is computable by anyone who knows both user ids of a
conversation, so knowing it proves nothing (ADR-0007). The recipient therefore
signs every ack with their Ed25519 identity key, and the relay checks three
things, all from the credential itself:

1. the public key in it hashes to a user id: ids are self-certifying, so this is
   who signed;
2. the mailbox is the one the named sender writes to *that* user,
   `mailbox_for(sender, signer)`: only the reader of a mailbox may empty it;
3. the signature covers this mailbox and exactly these envelope ids.

Nothing the relay holds is consulted, so a relay restarted a moment ago, with no
bundles in memory, checks an ack as well as one that has run for days.

What this leaves open, on purpose: `fetch` and `wait_for_mail` take no proof,
because they return only fixed-size ciphertext. A replayed ack acknowledges the
same records again, which is idempotent and therefore harmless.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import xeddsa
from x3dh.identity_key_pair import IdentityKeyPair

from linkchat.core.client.mailbox_ids import mailbox_for
from linkchat.core.crypto.identity import user_id_of
from linkchat.core.crypto.suite import LinkChatX3DH
from linkchat.core.ports import CredentialRejected
from linkchat.core.types import Credential, EnvelopeId, MailboxId, UserId

ACK_VERSION = 1
_ID_BYTES = 32
_KEY_BYTES = 32
_SIGNATURE_BYTES = 64
ACK_CREDENTIAL_SIZE = 1 + _ID_BYTES + _KEY_BYTES + _SIGNATURE_BYTES
"""Version, sender's user id, signer's public key, signature: 129 bytes."""

_DOMAIN = b"linkchat-ack\x00"
"""Keeps an ack signature from ever being valid as any other signed message."""


@dataclass(frozen=True, slots=True)
class AckSigner:
    """What a client needs to sign its acks: its identity key pair."""

    seed: bytes = field(repr=False)
    public_key: bytes
    user_id: UserId


def identity_signer(state: LinkChatX3DH) -> AckSigner:
    """The signer for this client's identity, read through the library's public API."""
    serialized = state.json["identity_key"]
    if not isinstance(serialized, Mapping):
        raise TypeError("unexpected identity key serialization")
    pair = IdentityKeyPair.from_json(serialized)
    seed = getattr(pair, "seed", None)
    if not isinstance(seed, bytes):
        raise TypeError("the identity key pair is not seed-based; cannot sign acks")
    public_key = state.bundle.identity_key
    return AckSigner(seed=seed, public_key=public_key, user_id=user_id_of(public_key))


def _signed_bytes(mailbox_id: MailboxId, envelope_ids: Sequence[EnvelopeId]) -> bytes:
    """What is signed. The ids are sorted, so their order does not matter."""
    ids = b"\x00".join(sorted(eid.encode() for eid in envelope_ids))
    return _DOMAIN + mailbox_id.encode() + b"\x00" + ids


def sign_ack(
    signer: AckSigner,
    sender: UserId,
    mailbox_id: MailboxId,
    envelope_ids: Sequence[EnvelopeId],
) -> Credential:
    """The credential to acknowledge `envelope_ids` in the mailbox `sender` writes to."""
    message = _signed_bytes(mailbox_id, envelope_ids)
    signature = xeddsa.ed25519_seed_sign(signer.seed, message)
    return Credential(
        bytes([ACK_VERSION]) + bytes.fromhex(sender) + signer.public_key + signature
    )


def check_ack(
    credential: Credential, mailbox_id: MailboxId, envelope_ids: Sequence[EnvelopeId]
) -> None:
    """What a relay runs before acknowledging. Raises `CredentialRejected`."""
    if len(credential) != ACK_CREDENTIAL_SIZE or credential[0] != ACK_VERSION:
        raise CredentialRejected("not a signed ack credential")
    sender = UserId(credential[1 : 1 + _ID_BYTES].hex())
    key_end = 1 + _ID_BYTES + _KEY_BYTES
    public_key = bytes(credential[1 + _ID_BYTES : key_end])
    signature = bytes(credential[key_end:])

    signer = user_id_of(public_key)
    try:
        expected = mailbox_for(sender, signer)
    except ValueError as exc:  # sender and signer are the same user
        raise CredentialRejected("a user has no mailbox to themselves") from exc
    if expected != mailbox_id:
        raise CredentialRejected("only this mailbox's recipient may acknowledge it")

    try:
        valid = xeddsa.ed25519_verify(
            signature, public_key, _signed_bytes(mailbox_id, envelope_ids)
        )
    except ValueError as exc:  # a key that is not a valid Ed25519 point
        raise CredentialRejected("unusable public key") from exc
    if not valid:
        raise CredentialRejected("the signature does not cover these envelopes")
