"""The reduced overlay view. Task C10.

What the browser shows about the relay tier, and nothing more. ADR-0014 cut this
down from a full overlay map: Ray's own dashboard already shows nodes and where
each actor runs, so the only thing missing — and the only thing that makes the
replication *visible* during the demonstration — is which relays are up and
which three hold the mailbox being written to right now.

Deliberately built from the same calls the client already makes. It asks each
relay for its `status`, which is the one method that never raises `RelaySyncing`,
so a relay that is mid-sync reports itself as syncing rather than vanishing. A
relay that does not answer within the timeout is `unreachable`, which is exactly
what a sender would conclude about it.

Ray-free, like everything else under `core`: it reaches relays only through
`RelayDirectory`, so the same view works over the fake transport in tests.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from linkchat.core.ports import RelayDirectory, RelayError, RelayState
from linkchat.core.ring import preference_list, primaries
from linkchat.core.types import MailboxId, RelayName

OVERLAY_TIMEOUT_S = 2.0
"""Per-relay timeout for a status call.

Short on purpose: this drives a view that has to change within the failover
timeout when a relay container is killed (C10's "done when"). A relay that needs
longer than this to say hello is, for display purposes, not there.
"""

UNREACHABLE = "unreachable"
"""Not a `RelayState`: that enum describes a relay that answered. This is what
the client concludes when nothing answered at all."""


@dataclass(frozen=True, slots=True)
class RelayView:
    """One relay, as the browser shows it."""

    name: RelayName
    state: str
    """`ready`, `syncing`, or `unreachable`."""

    records: int
    tombstones: int
    hinted_records: int
    """How many records this relay is holding for an unreachable primary (R7)."""

    holds_mailbox: bool
    """Whether this relay is one of the three primaries of the asked-about mailbox."""

    rank: int
    """Position on that mailbox's preference list, 0-based. The first three are
    the primaries and the rest are the stand-in order, which is what makes a
    fall-through visible when a primary is killed."""

    @property
    def reachable(self) -> bool:
        return self.state != UNREACHABLE


async def overlay(
    directory: RelayDirectory,
    *,
    mailbox: MailboxId | None = None,
    timeout_s: float = OVERLAY_TIMEOUT_S,
) -> list[RelayView]:
    """Ask every relay how it is, in one round.

    Returns one `RelayView` per relay. With `mailbox`, the list comes back in
    that mailbox's preference order and the three primaries are marked; without
    it, in the directory's own order with nothing marked.

    Never raises: a relay that fails is reported as `unreachable`, because the
    point of this view is to show failures rather than to propagate them.
    """
    names = list(directory.relay_names)
    holders: tuple[RelayName, ...] = ()
    order: dict[RelayName, int] = {}
    if mailbox is not None:
        holders = primaries(mailbox)
        ranked = preference_list(mailbox)
        order = {name: index for index, name in enumerate(ranked)}
        names = [name for name in ranked if name in set(names)]

    views = await asyncio.gather(
        *(
            _one(directory, name, timeout_s, name in holders, order.get(name, index))
            for index, name in enumerate(names)
        )
    )
    return list(views)


async def _one(
    directory: RelayDirectory,
    name: RelayName,
    timeout_s: float,
    holds_mailbox: bool,
    rank: int,
) -> RelayView:
    """One relay's view. Any failure becomes `unreachable` with zero counts."""
    try:
        status = await asyncio.wait_for(directory.gateway(name).status(), timeout_s)
    except (RelayError, TimeoutError):
        return RelayView(
            name=name,
            state=UNREACHABLE,
            records=0,
            tombstones=0,
            hinted_records=0,
            holds_mailbox=holds_mailbox,
            rank=rank,
        )
    state = (
        status.state.value if isinstance(status.state, RelayState) else str(status.state)
    )
    return RelayView(
        name=name,
        state=state,
        records=status.records,
        tombstones=status.tombstones,
        hinted_records=status.hinted_records,
        holds_mailbox=holds_mailbox,
        rank=rank,
    )
