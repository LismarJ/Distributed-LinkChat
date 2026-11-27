"""Signed acknowledgements: only a mailbox's recipient may delete from it (ADR-0007)."""

from __future__ import annotations

import pytest

from linkchat.core.client.mailbox_ids import mailbox_for
from linkchat.core.crypto.acks import (
    ACK_CREDENTIAL_SIZE,
    check_ack,
    identity_signer,
    sign_ack,
)
from linkchat.core.crypto.identity import user_id_of
from linkchat.core.crypto.suite import new_x3dh_state
from linkchat.core.ports import CredentialRejected
from linkchat.core.types import Credential, EnvelopeId, MailboxId


def person():
    state = new_x3dh_state()
    return identity_signer(state), user_id_of(state.bundle.identity_key)


EIDS = [EnvelopeId("b" * 64), EnvelopeId("a" * 64)]


def test_the_recipient_can_acknowledge_their_own_mail() -> None:
    bob, bob_id = person()
    _, alice_id = person()
    mailbox = mailbox_for(alice_id, bob_id)  # Alice writes, Bob reads

    credential = sign_ack(bob, alice_id, mailbox, EIDS)

    assert len(credential) == ACK_CREDENTIAL_SIZE
    check_ack(credential, mailbox, EIDS)  # does not raise


def test_the_order_of_the_envelope_ids_does_not_matter() -> None:
    bob, bob_id = person()
    _, alice_id = person()
    mailbox = mailbox_for(alice_id, bob_id)

    check_ack(sign_ack(bob, alice_id, mailbox, EIDS), mailbox, list(reversed(EIDS)))


def test_the_sender_cannot_acknowledge_what_they_sent() -> None:
    """Alice knows both ids and the mailbox; she still is not its reader."""
    _, bob_id = person()
    alice, alice_id = person()
    mailbox = mailbox_for(alice_id, bob_id)

    with pytest.raises(CredentialRejected):
        check_ack(sign_ack(alice, bob_id, mailbox, EIDS), mailbox, EIDS)


def test_a_third_party_who_knows_both_ids_cannot_acknowledge() -> None:
    """Mallory computes the mailbox from the two public ids and signs as herself."""
    _, bob_id = person()
    _, alice_id = person()
    mallory, _ = person()
    mailbox = mailbox_for(alice_id, bob_id)

    with pytest.raises(CredentialRejected):
        check_ack(sign_ack(mallory, alice_id, mailbox, EIDS), mailbox, EIDS)


def test_a_signature_does_not_carry_over_to_other_envelopes() -> None:
    bob, bob_id = person()
    _, alice_id = person()
    mailbox = mailbox_for(alice_id, bob_id)
    credential = sign_ack(bob, alice_id, mailbox, EIDS[:1])

    with pytest.raises(CredentialRejected):
        check_ack(credential, mailbox, EIDS)


def test_a_signature_does_not_carry_over_to_another_mailbox() -> None:
    bob, bob_id = person()
    _, alice_id = person()
    _, carol_id = person()
    credential = sign_ack(bob, alice_id, mailbox_for(alice_id, bob_id), EIDS)

    with pytest.raises(CredentialRejected):
        check_ack(credential, mailbox_for(carol_id, bob_id), EIDS)


def test_a_swapped_public_key_is_refused() -> None:
    """Mallory signs, then pastes Bob's public key in: the signature fails."""
    bob, bob_id = person()
    _, alice_id = person()
    mallory, _ = person()
    mailbox = mailbox_for(alice_id, bob_id)
    forged = bytearray(sign_ack(mallory, alice_id, mailbox, EIDS))
    honest = sign_ack(bob, alice_id, mailbox, EIDS)
    forged[33:65] = honest[33:65]  # Bob's key, so the id check passes

    with pytest.raises(CredentialRejected):
        check_ack(Credential(bytes(forged)), mailbox, EIDS)


@pytest.mark.parametrize(
    "raw",
    [b"", b"test-credential", b"\x01" + b"\x00" * 10, b"\x09" + b"\x00" * 128],
    ids=["empty", "text", "short", "unknown-version"],
)
def test_malformed_credentials_are_refused(raw: bytes) -> None:
    _, bob_id = person()
    _, alice_id = person()

    with pytest.raises(CredentialRejected):
        check_ack(Credential(raw), mailbox_for(alice_id, bob_id), EIDS)


def test_a_mailbox_that_is_not_derived_from_two_ids_is_refused() -> None:
    bob, _ = person()
    _, alice_id = person()

    with pytest.raises(CredentialRejected):
        check_ack(
            sign_ack(bob, alice_id, MailboxId("mailbox-roundtrip"), EIDS),
            MailboxId("mailbox-roundtrip"),
            EIDS,
        )
