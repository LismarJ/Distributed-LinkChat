"""Shared test fixtures.

The fake clock is here rather than in `linkchat.core` because production code
has no use for it, and because expiry is the one mechanism that would otherwise
need a test to actually wait.
"""

from __future__ import annotations

import pytest

from linkchat.core.envelope import envelope_id
from linkchat.core.ports import Record
from linkchat.core.types import MailboxId


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
