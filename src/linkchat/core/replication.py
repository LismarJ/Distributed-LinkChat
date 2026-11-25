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

import asyncio
from dataclasses import dataclass, replace

from .constants import WRITE_COPIES
from .ports import Clock, Record, RelayDirectory, RelayError
from .ring import preference_list
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

    The mechanism, slot by slot. Every mailbox has `WRITE_COPIES` slots to fill,
    one per primary. A slot starts on its own primary; if that store fails, the
    slot *falls through* to the next unused relay on the preference list, which
    stores a copy carrying `hint=<the primary it stands in for>`. Each round
    tries all still-open slots at once (`asyncio.gather`), each call bounded by
    `timeout_s` so one slow relay cannot hold up the send. When `WRITE_COPIES`
    distinct relays have confirmed, the message is `sent`; if the preference list
    runs out first, it stays `pending` and the outbox retries. `store` is
    idempotent on the envelope id, so a retry and a stand-in copy of a message a
    primary also received both collapse to one record.
    """
    prefs = preference_list(record.mailbox_id)
    primaries = prefs[:WRITE_COPIES]

    confirmed: list[RelayName] = []
    # Each open slot is the primary whose copy it is responsible for placing.
    slots: list[tuple[RelayName, RelayName]] = [(p, p) for p in primaries]
    next_fallback = WRITE_COPIES  # index into `prefs` of the next stand-in to try

    async def _store_on(primary: RelayName, relay: RelayName) -> bool:
        """Try to place this slot's copy on `relay`. True if the relay confirmed.

        A stand-in (`relay != primary`) carries a hint naming the primary, so it
        knows to hand the record back later (R7). A failure of any kind — the
        relay unreachable, gone, syncing, full, or simply slow past the timeout —
        is a `False`, which the caller turns into a fall-through to the next relay.
        """
        to_store = record if relay == primary else replace(record, hint=primary)
        try:
            await asyncio.wait_for(directory.gateway(relay).store(to_store), timeout_s)
        except (RelayError, TimeoutError):
            return False
        return True

    while slots and len(confirmed) < WRITE_COPIES:
        outcomes = await asyncio.gather(
            *(_store_on(primary, relay) for primary, relay in slots)
        )
        taken = set(confirmed)
        next_slots: list[tuple[RelayName, RelayName]] = []
        for (primary, relay), ok in zip(slots, outcomes, strict=True):
            if ok:
                confirmed.append(relay)
                taken.add(relay)
                continue
            # Fall through to the next relay on the preference list that is not
            # already holding a copy or reserved by another slot this round.
            while next_fallback < len(prefs):
                candidate = prefs[next_fallback]
                next_fallback += 1
                if candidate not in taken:
                    next_slots.append((primary, candidate))
                    taken.add(candidate)  # reserve it for this slot
                    break
        slots = next_slots

    return SendResult(confirmed_relays=tuple(confirmed))
