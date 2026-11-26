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
from typing import NamedTuple

from .mailbox import MailboxStore
from .ports import Clock, RelayError, RelayPeers
from .types import RelayName


class SyncOutcome(NamedTuple):
    """What one pass of `resync` achieved.

    Two numbers rather than one, because "nothing came back" has two opposite
    meanings and only the caller can tell them apart (see `resync`).
    """

    peers_answered: int
    """How many peers replied. Zero means this relay learned nothing at all."""

    records_refused: int
    """Records a peer offered that the store would not take, i.e. it is full.

    Non-zero means the sync was incomplete: the relay is missing records it is
    a primary of and must not start serving clients.
    """

    @property
    def complete(self) -> bool:
        """Whether this counts as a sync that actually happened."""
        return self.peers_answered > 0 and self.records_refused == 0


async def resync(
    this_relay: RelayName,
    store: MailboxStore,
    peers: RelayPeers,
    *,
    clock: Clock,
) -> SyncOutcome:
    """Refill `store` from the peers, for every mailbox `this_relay` owns. R6.

    Pulls from each reachable peer and `absorb`s the result into `store`. A peer
    that is itself down or syncing is skipped; the guarantee holds as long as one
    holder of each record answers.

    **Returns how many peers actually answered, and how many records the store
    refused**, which is what the caller needs in order to tell whether this was
    a sync at all. "Every peer answered and had nothing
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
    refused = 0
    for peer in peers.peer_names:
        try:
            result = await peers.pull(peer, this_relay)
        except RelayError:
            # Down, gone, or otherwise unreachable: skip it. Another holder of
            # each record is reachable, so skipping one loses nothing.
            continue
        records = list(result.records)
        store.absorb(records, list(result.tombstones))
        answered += 1
        # A record the peer sent that we are now neither holding nor
        # suppressing was refused, and the only remaining reason is a full
        # store. `absorb` reports "newly held", which cannot tell that apart
        # from "already had it", so the store is asked directly. Counting it
        # matters because the caller must not call an incomplete sync a sync:
        # a relay that goes READY short of records looks healthy, is not
        # counted as one of the two tolerated failures, and so silently voids
        # the read-correctness argument instead of visibly breaking it.
        refused += sum(
            1
            for record in records
            if not store.has(record.mailbox_id, record.envelope_id)
            and not store.is_tombstoned(record.mailbox_id, record.envelope_id)
            and record.expires_at > clock.now()
        )
    return SyncOutcome(peers_answered=answered, records_refused=refused)


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
        if primary == this_relay:
            # Never hand a record to ourselves. `forget_record` is only safe
            # once a *different* relay has confirmed it holds the record; a
            # self-handoff confirms nothing, because the absorb and the forget
            # hit the same store under the same `envelope_id`, so the forget
            # deletes exactly what the absorb stored. If the stand-in we
            # absorbed from was the last other holder, the record is gone. A
            # hint naming us is a record we are a primary of and should simply
            # keep, so clear the hint rather than act on it.
            for record in store.hinted_for(primary):
                store.clear_hint(record.mailbox_id, record.envelope_id)
            continue
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
