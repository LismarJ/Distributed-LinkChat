"""Sync on restart (R6) and hinted handoff (R7). Repair over the RelayPeers port.

**These are graded mechanisms — write the bodies yourself (work plan section 7).**
They are the heart of "two failures cannot lose a confirmed message", so you
have to be able to explain them. This file is the *contract*; the behaviour is
pinned by failing tests in `tests/test_repair.py`.

Both are Ray-free: they work through `RelayPeers` and a `MailboxStore`, so they
run under the fake transport in tests and under the real relay actor in
production without changing.

The shapes, from `ray-architecture.md` section 6:

- **R6, sync on restart.** A relay that starts empty asks every reachable peer
  for the records and tombstones of every mailbox it is a primary of
  (`RelayPeers.pull`), and `absorb`s the answers. Since every acknowledged record
  is on three relays and at most two are down, at least one holder answers. The
  relay goes READY once every reachable peer has answered.
- **R7, hinted handoff.** A stand-in holding a record for an unreachable primary
  (`record.hint == primary`) tries to deliver it (`RelayPeers.handoff`). When the
  primary confirms, the stand-in drops its copy (`store.forget_record`) — never a
  tombstone, because the record has moved, not been acknowledged.
"""

from __future__ import annotations

from .mailbox import MailboxStore
from .ports import Clock, RelayPeers
from .types import RelayName


async def resync(
    this_relay: RelayName,
    store: MailboxStore,
    peers: RelayPeers,
    *,
    clock: Clock,
) -> None:
    """Refill `store` from the peers, for every mailbox `this_relay` owns. R6.

    Pulls from each reachable peer and `absorb`s the result into `store`. A peer
    that is itself down or syncing is skipped; the guarantee holds as long as one
    holder of each record answers. Returns once every reachable peer has been
    asked; the caller (the relay actor) then flips to READY.
    """
    raise NotImplementedError(
        "R6: sync on restart. Graded mechanism — implement against "
        "tests/test_repair.py."
    )


async def handoff_round(
    this_relay: RelayName,
    store: MailboxStore,
    peers: RelayPeers,
    *,
    clock: Clock,
) -> int:
    """Return hinted records to their primaries; drop our copy on success. R7.

    For each primary this relay is covering for (`store.hinted_primaries()`),
    hand the hinted records to that primary (`peers.handoff`) and, only if that
    succeeds, `store.forget_record` each one locally. Returns how many records
    were handed off and dropped. A primary still unreachable is left for the next
    round; the stand-in keeps serving the record meanwhile.
    """
    raise NotImplementedError(
        "R7: hinted handoff. Graded mechanism — implement against "
        "tests/test_repair.py."
    )
