"""Mailbox identifiers: one per direction of each conversation (ADR-0007)."""

from __future__ import annotations

import hashlib

from linkchat.core.types import MailboxId, UserId

_DOMAIN = b"linkchat-mailbox\x00"


def mailbox_for(sender: UserId, recipient: UserId) -> MailboxId:
    """Where `sender` leaves mail for `recipient`.

    Both sides compute it from the two ids alone, so no exchange is needed.
    The separator stops ("ab", "c") and ("a", "bc") from colliding.
    """
    if sender == recipient:
        raise ValueError("a mailbox needs two different users")
    material = _DOMAIN + sender.encode() + b"\x00" + recipient.encode()
    return MailboxId(hashlib.blake2b(material, digest_size=32).hexdigest())
