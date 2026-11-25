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

from dataclasses import replace

from .mailbox import MailboxStore
from .ports import Clock, RelayError, RelayPeers
from .types import RelayName


async def resync(
    this_relay: RelayName,
    store: MailboxStore,
    peers: RelayPeers,
    *,
    clock: Clock,
) -> int:
    """Refill `store` from the peers, for every mailbox `this_relay` owns. R6.

    Pulls from each reachable peer and `absorb`s the result into `store`. A peer
    that is itself down or syncing is skipped; the guarantee holds as long as one
    holder of each record answers.

    **Returns how many peers actually answered**, which the caller needs in order
    to tell whether this was a sync at all. "Every peer answered and had nothing
    for us" and "no peer answered" both leave the store empty, so they cannot be
    told apart from the store alone — yet they mean opposite things. The first is
    the normal state of a fresh cluster; the second means this relay learned
    nothing and must not serve clients yet. Only the caller knows how many peers
    it expected, so the count goes back rather than a verdict.

    Why this is enough to survive two failures. Every acknowledged record sits on
    three relays, and at most two are down at once, so for any mailbox this relay
    owns at least one holder is reachable and answers the pull. `pull` asks the
    peer for exactly what `this_relay` should hold (the peer computes it from the
    same fixed ring), and `absorb` is idempotent and tombstone-aware, so pulling
    the same record from two surviving peers stores it once and an acknowledgement
    that reached a peer as a tombstone suppresses the record rather than resurrecting
    it. A peer that is itself still syncing answers with whatever it already has,
    which only ever helps.
    """
    answered = 0
    for peer in peers.peer_names:
        try:
            result = await peers.pull(peer, this_relay)
        except RelayError:
            # Down, gone, or otherwise unreachable: skip it. Another holder of
            # each record is reachable, so skipping one loses nothing.
            continue
        store.absorb(list(result.records), list(result.tombstones))
        answered += 1
    return answered


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

    Two details the fault-tolerance argument rests on. The hint is cleared before
    handing a record over (`replace(..., hint=None)`): the record is the primary's
    own now, so leaving the hint set would make the primary think it is standing
    in for itself and hand the record straight back. And the local copy is dropped
    with `forget_record`, never a tombstone: the record has *moved*, not been
    acknowledged, and a tombstone here would stop the primary from ever handing it
    back if this relay later became a holder. The order — confirm the primary has
    it, *then* drop ours — means a failure between the two leaves a harmless
    duplicate, never a gap.
    """
    handed = 0
    for primary in store.hinted_primaries():
        records = store.hinted_for(primary)
        if not records:
            continue
        payload = [replace(record, hint=None) for record in records]
        try:
            await peers.handoff(primary, payload, [])
        except RelayError:
            # Primary still unreachable: keep our copies and serve them; the next
            # round tries again.
            continue
        for record in records:
            if store.forget_record(record.mailbox_id, record.envelope_id):
                handed += 1
    return handed
