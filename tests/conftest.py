"""Shared test fixtures.

The fake clock is here rather than in `linkchat.core` because production code
has no use for it, and because expiry is the one mechanism that would otherwise
need a test to actually wait.
"""

from __future__ import annotations

import pytest

from linkchat.core.client.mailbox_ids import mailbox_for
from linkchat.core.crypto.acks import AckSigner, identity_signer, sign_ack
from linkchat.core.crypto.suite import new_x3dh_state
from linkchat.core.envelope import envelope_id
from linkchat.core.ports import Record
from linkchat.core.types import Credential, EnvelopeId, MailboxId, UserId


class FakeClock:
    """A `Clock` that only moves when a test moves it.

    `sleep` returns at once and advances the clock instead of waiting, so a
    seven-day tombstone lifetime can be tested in microseconds.
    """

    def __init__(self, now: float = 1_000_000.0) -> None:
        self._now = now

    def now(self) -> float:
        return self._now

    def monotonic(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        self.advance(seconds)

    def advance(self, seconds: float) -> None:
        self._now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def make_record(
    *,
    mailbox: str = "mailbox-1",
    body: bytes = b"ciphertext",
    expires_at: float = 1_000_060.0,
    hint: str | None = None,
) -> Record:
    """A record whose id really is the hash of its content.

    Deriving it properly rather than making one up keeps the tests honest about
    the property the whole store depends on: two records with the same content
    are the same record.
    """
    return Record(
        mailbox_id=MailboxId(mailbox),
        envelope_id=envelope_id(body),
        envelope=body,
        expires_at=expires_at,
        hint=hint,  # type: ignore[arg-type]
    )


# -- signed acks (ADR-0007) ----------------------------------------------------

SENDER = UserId("c" * 64)
"""Any well-formed user id: an ack check only needs the sender to derive the mailbox."""


def make_reader() -> AckSigner:
    """A mailbox's reader, holding a real identity key so it can sign its acks."""
    return identity_signer(new_x3dh_state())


def inbox(reader: AckSigner, sender: UserId = SENDER) -> MailboxId:
    """The mailbox `sender` writes to `reader`: the only one `reader` may ack."""
    return mailbox_for(sender, reader.user_id)


def ack_credential(
    reader: AckSigner, mailbox: MailboxId, eids: list[EnvelopeId], sender: UserId = SENDER
) -> Credential:
    """What a client sends with `ack`, and what every relay now requires."""
    return sign_ack(reader, sender, mailbox, eids)
