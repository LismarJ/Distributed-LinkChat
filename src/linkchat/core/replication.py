"""Three-copy send with stand-ins. Task R5.

**This is a graded mechanism — write the body yourself (work plan section 7).**
At the delivery you have to defend why two simultaneous failures cannot lose a
confirmed message, and reading an implementation is not the same as having built
it. This file is only the *contract*; the behaviour is pinned by failing tests
in `tests/test_three_copy_send.py`. Make them pass.

The shape of the mechanism, from `ray-architecture.md` section 5:

1. The three primaries of the record's mailbox are `ring.primaries(mailbox_id)`.
2. Call `store` on the three primaries at once, each with its own timeout.
3. For every primary that fails or times out, call `store` on the next relay on
   `ring.preference_list(...)`, passing a record whose `hint` names the primary
   it is standing in for.
4. When THREE DISTINCT relays have confirmed, the message is `sent`.
5. If three confirmations cannot be collected (more than two relays down), it
   stays `pending` and the caller's outbox retries.

`store` is idempotent, so a retry, a Ray retry, and a duplicate after an
ambiguous error are all harmless.
"""

from __future__ import annotations

from dataclasses import dataclass

from .constants import WRITE_COPIES
from .ports import Clock, Record, RelayDirectory
from .types import RelayName

DEFAULT_SEND_TIMEOUT_S = 5.0
"""Per-relay timeout for a store during a send. Tune with the spike (G1)."""


@dataclass(frozen=True, slots=True)
class SendResult:
    """The outcome of trying to store one record on three distinct relays."""

    confirmed_relays: tuple[RelayName, ...]
    """The distinct relays that confirmed the store, in confirmation order."""

    @property
    def sent(self) -> bool:
        """True once `WRITE_COPIES` distinct relays have confirmed."""
        return len(set(self.confirmed_relays)) >= WRITE_COPIES


async def send_to_three(
    record: Record,
    directory: RelayDirectory,
    *,
    clock: Clock,
    timeout_s: float = DEFAULT_SEND_TIMEOUT_S,
) -> SendResult:
    """Store `record` on three distinct relays, falling through stand-ins. R5.

    Returns a `SendResult`; `result.sent` says whether three confirmations were
    collected. Must be safe to call again with the same record (idempotent).

    Raises nothing for a normal partial failure — fewer than three confirmations
    is reported through `SendResult.sent`, not an exception.
    """
    raise NotImplementedError(
        "R5: three-copy send with stand-ins. This is a graded mechanism — "
        "implement it by hand against tests/test_three_copy_send.py."
    )
