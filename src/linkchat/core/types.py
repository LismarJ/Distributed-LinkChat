"""Identifier aliases shared across the core.

These are `NewType` rather than bare `str` so that `mypy --strict` catches a
mailbox id passed where an envelope id belongs. They are all opaque to relays.
"""

from __future__ import annotations

from typing import NewType

MailboxId = NewType("MailboxId", str)
"""Opaque identifier of a mailbox. A relay must learn nothing from it.

One per direction of each conversation, derived from the two user ids
(ADR-0007, `core/client/mailbox_ids.py`).
"""

EnvelopeId = NewType("EnvelopeId", str)
"""Content hash of an envelope, hex. See `envelope.envelope_id`.

Being derived from the content is what makes every relay operation idempotent:
storing the same envelope twice is indistinguishable from storing it once.
"""

RelayName = NewType("RelayName", str)
"""A relay's identity, `relay-0` to `relay-4`.

It is a name, not a machine. Which container runs `relay-2` at a given moment
is Ray's business in phase 1, and the name survives a restart.
"""

UserId = NewType("UserId", str)
"""Hash of a user's long-term identity public key, hex. Self-generated."""

Credential = NewType("Credential", bytes)
"""Proof that the caller may read from or delete from a mailbox.

Its contents are still open: ADR-0007 settled the mailbox ids, not this. It is
declared here so that the ports can be frozen before that decision is taken:
today it is carried through the calls and not yet checked.
"""
