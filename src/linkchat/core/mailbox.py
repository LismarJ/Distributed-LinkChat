"""What a relay actually holds. Task R2.

Plain Python, no Ray, no disk. A relay actor wraps one of these; the actor
supplies the transport and the supervision, and everything about what is kept
and for how long is here.

Three rules shape the whole thing, and each exists because of a failure we
cannot otherwise survive.

**Storing is idempotent.** A record is named by the hash of its content, so
storing it twice is indistinguishable from storing it once. This is what makes
it safe to retry a call whose outcome the runtime could not report: Ray's
documentation is explicit that a failed actor call may in fact have run, and
offers no way to tell (fault-tolerance.md case 1).

**Deleting leaves a tombstone.** Acknowledgement removes the record and
remembers that it is gone, because a copy can still arrive afterwards from a
stand-in that was holding it for an unreachable primary. Without the memory the
relay would accept that copy and offer it again.

**Everything expires on its own.** A record is dropped at its `expires_at`
whether or not anyone acknowledged it. Acknowledgement is best effort;
expiry is the guarantee, and it is the one requirement R9 rests on. No other
process has to be alive or reachable for it to happen.

There is deliberately no operation that returns anything already acknowledged
or expired. A relay cannot reconstruct a conversation because it cannot be
asked to.
"""

from __future__ import annotations

import heapq
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import Enum

from .constants import RELAY_NAMES
from .ports import Clock, Record, Tombstone
from .ring import is_primary
from .types import EnvelopeId, MailboxId, RelayName

DEFAULT_MAX_RECORDS_PER_MAILBOX = 1_000
"""Provisional. Caps how much one recipient's absence can cost a relay.

A sender hitting this keeps the message in its outbox and retries, so the
message is delayed rather than lost. The real number belongs with the TTL in
ADR-0006, which is pending: the two trade against each other, since a longer
TTL means more records per mailbox for the same traffic.
"""

DEFAULT_MAX_RECORDS_TOTAL = 50_000
"""Provisional. At the 4 KiB envelope budget this is about 200 MB of payload.

Exists so that a relay refuses work before Ray's memory monitor kills it. Being
refused is a delay; being killed loses every mailbox the relay held and forces
a full resync.
"""

DEFAULT_TOMBSTONE_TTL_S = 7 * 24 * 60 * 60
"""Provisional, and the number here matters more than it looks.

A tombstone has to outlive every record it could have to suppress. When a
client acknowledges an envelope on the two primaries it can reach, the third
never learns, and the tombstone on the others must still be there if a late
copy shows up. Since periodic digest comparison is deferred (ADR-0014), nothing
actively spreads tombstones, so they are kept comfortably longer than the
longest record TTL. Settle it with ADR-0006.
"""


class StoreOutcome(Enum):
    """What happened to a `store`. The actor turns these into its reply.

    Separating "already present" from "stored" is not pedantry: it is how the
    relay reports a retry as a success without counting the record twice, and
    how the demonstration can show that duplicate traffic is absorbed rather
    than accumulating.
    """

    STORED = "stored"
    ALREADY_PRESENT = "already_present"
    """A record with this id is here. The caller should treat this as success."""

    REFUSED_TOMBSTONED = "refused_tombstoned"
    """Already acknowledged. Refusing is the point: it must not come back."""

    REFUSED_EXPIRED = "refused_expired"
    """Its `expires_at` has already passed. Storing it would be pointless."""

    REFUSED_FULL = "refused_full"
    """The mailbox or the relay is at its cap. The sender keeps and retries."""

    @property
    def accepted(self) -> bool:
        """Whether the caller may count this relay as holding the record."""
        return self in (StoreOutcome.STORED, StoreOutcome.ALREADY_PRESENT)


@dataclass(frozen=True, slots=True)
class ExpiryReport:
    """What one sweep removed. Counts only, for metrics and the UI."""

    records: int = 0
    tombstones: int = 0

    @property
    def total(self) -> int:
        return self.records + self.tombstones


@dataclass(frozen=True, slots=True)
class StoreCounts:
    """A snapshot for `RelayStatus`. Counts only: never an id, never content."""

    records: int = 0
    tombstones: int = 0
    mailboxes: int = 0
    hinted_records: int = 0


@dataclass(slots=True)
class _Mailbox:
    records: dict[EnvelopeId, Record] = field(default_factory=dict)
    tombstones: dict[EnvelopeId, Tombstone] = field(default_factory=dict)

    def empty(self) -> bool:
        return not self.records and not self.tombstones


class MailboxStore:
    """One relay's memory of what it is holding, and of what it has forgotten.

    Not thread safe, and does not need to be: it lives inside one async actor,
    which Ray runs on a single event loop. Every method here is synchronous and
    returns without awaiting, so no other coroutine can observe it half way
    through a change.
    """

    def __init__(
        self,
        relay: RelayName,
        clock: Clock,
        *,
        max_records_per_mailbox: int = DEFAULT_MAX_RECORDS_PER_MAILBOX,
        max_records_total: int = DEFAULT_MAX_RECORDS_TOTAL,
        tombstone_ttl_s: float = DEFAULT_TOMBSTONE_TTL_S,
    ) -> None:
        if relay not in RELAY_NAMES:
            raise ValueError(f"{relay!r} is not one of {RELAY_NAMES}")
        self.relay = relay
        self._clock = clock
        self._max_per_mailbox = max_records_per_mailbox
        self._max_total = max_records_total
        self._tombstone_ttl_s = tombstone_ttl_s

        self._mailboxes: dict[MailboxId, _Mailbox] = {}
        self._record_count = 0

        # Expiry by heap rather than by scanning every mailbox: the sweeper runs
        # on a timer and must not get slower as the relay fills up. Entries can
        # outlive the thing they point at, when a record is acknowledged before
        # it expires, so the sweeper checks before deleting.
        self._record_expiry: list[tuple[float, MailboxId, EnvelopeId]] = []
        self._tombstone_expiry: list[tuple[float, MailboxId, EnvelopeId]] = []

    # -- writing ------------------------------------------------------------

    def store(self, record: Record) -> StoreOutcome:
        """Take a record, or say why not. Never raises on a normal refusal.

        Order of checks matters. A tombstone beats everything, because
        resurrecting an acknowledged message is worse than refusing a valid
        one. Expiry is checked before capacity, so that a relay at its cap
        still refuses junk for the honest reason.
        """
        mailbox = self._mailboxes.get(record.mailbox_id)

        if mailbox is not None and record.envelope_id in mailbox.tombstones:
            return StoreOutcome.REFUSED_TOMBSTONED

        if record.expires_at <= self._clock.now():
            return StoreOutcome.REFUSED_EXPIRED

        if mailbox is not None and record.envelope_id in mailbox.records:
            return StoreOutcome.ALREADY_PRESENT

        if self._record_count >= self._max_total:
            return StoreOutcome.REFUSED_FULL
        if mailbox is not None and len(mailbox.records) >= self._max_per_mailbox:
            return StoreOutcome.REFUSED_FULL

        if mailbox is None:
            mailbox = self._mailboxes.setdefault(record.mailbox_id, _Mailbox())

        mailbox.records[record.envelope_id] = record
        self._record_count += 1
        heapq.heappush(
            self._record_expiry,
            (record.expires_at, record.mailbox_id, record.envelope_id),
        )
        return StoreOutcome.STORED

    def ack(self, mailbox_id: MailboxId, envelope_ids: Sequence[EnvelopeId]) -> int:
        """Forget these records, and remember that they are forgotten.

        Returns how many records were actually removed. Idempotent: a repeated
        acknowledgement removes nothing and returns 0, which is a success.

        **A tombstone is written even for an id this relay never held.** That
        looks wasteful and is the point. A relay that missed the original write
        and then receives the acknowledgement would otherwise accept the record
        later from a stand-in handing it back, and offer it to a reader who had
        already seen it. The tombstone is the only thing standing between a
        correct acknowledgement and a message that reappears.
        """
        mailbox = self._mailboxes.setdefault(mailbox_id, _Mailbox())
        expires_at = self._clock.now() + self._tombstone_ttl_s
        removed = 0

        for envelope_id in envelope_ids:
            if mailbox.records.pop(envelope_id, None) is not None:
                self._record_count -= 1
                removed += 1
            if envelope_id not in mailbox.tombstones:
                mailbox.tombstones[envelope_id] = Tombstone(
                    mailbox_id=mailbox_id,
                    envelope_id=envelope_id,
                    expires_at=expires_at,
                )
                heapq.heappush(
                    self._tombstone_expiry, (expires_at, mailbox_id, envelope_id)
                )

        self._drop_if_empty(mailbox_id)
        return removed

    def absorb(
        self,
        records: Sequence[Record] = (),
        tombstones: Sequence[Tombstone] = (),
    ) -> tuple[int, int]:
        """Take what a peer sent us, during a resync or a handoff.

        Tombstones are applied first, so that a batch containing both a record
        and its tombstone cannot leave the record behind depending on order.
        Returns how many records and tombstones were newly held.
        """
        tombstones_added = 0
        for tombstone in tombstones:
            mailbox = self._mailboxes.setdefault(tombstone.mailbox_id, _Mailbox())
            if tombstone.expires_at <= self._clock.now():
                continue
            if tombstone.envelope_id in mailbox.tombstones:
                continue
            if mailbox.records.pop(tombstone.envelope_id, None) is not None:
                self._record_count -= 1
            mailbox.tombstones[tombstone.envelope_id] = tombstone
            heapq.heappush(
                self._tombstone_expiry,
                (tombstone.expires_at, tombstone.mailbox_id, tombstone.envelope_id),
            )
            tombstones_added += 1

        records_added = sum(
            1 for record in records if self.store(record) is StoreOutcome.STORED
        )

        for mailbox_id in {t.mailbox_id for t in tombstones}:
            self._drop_if_empty(mailbox_id)
        return records_added, tombstones_added

    def forget_record(self, mailbox_id: MailboxId, envelope_id: EnvelopeId) -> bool:
        """Drop a record without a tombstone, after handing it to its primary.

        Only for a stand-in that has confirmed the primary holds the record. No
        tombstone, because the record is not acknowledged: it has simply moved,
        and a tombstone here would stop the primary from ever handing it back.
        """
        mailbox = self._mailboxes.get(mailbox_id)
        if mailbox is None or mailbox.records.pop(envelope_id, None) is None:
            return False
        self._record_count -= 1
        self._drop_if_empty(mailbox_id)
        return True

    # -- reading ------------------------------------------------------------

    def fetch(self, mailbox_id: MailboxId) -> list[Record]:
        """Everything this relay currently holds for a mailbox.

        Expired records are filtered here as well as swept on a timer, so a
        reader never sees something past its time just because the sweeper has
        not run yet.
        """
        mailbox = self._mailboxes.get(mailbox_id)
        if mailbox is None:
            return []
        now = self._clock.now()
        return [record for record in mailbox.records.values() if record.expires_at > now]

    def has(self, mailbox_id: MailboxId, envelope_id: EnvelopeId) -> bool:
        mailbox = self._mailboxes.get(mailbox_id)
        return mailbox is not None and envelope_id in mailbox.records

    def is_tombstoned(self, mailbox_id: MailboxId, envelope_id: EnvelopeId) -> bool:
        mailbox = self._mailboxes.get(mailbox_id)
        return mailbox is not None and envelope_id in mailbox.tombstones

    def share_for(self, relay: RelayName) -> tuple[list[Record], list[Tombstone]]:
        """Everything we hold that `relay` ought to hold too.

        This is what a relay coming back from a restart asks its peers for. The
        asking relay does not describe what it wants: both sides compute
        placement from the same fixed relay set, so the peer works out the
        answer itself.

        Includes records this relay is holding as a stand-in, because a
        stand-in is often the only copy a restarted primary can get.
        """
        now = self._clock.now()
        records: list[Record] = []
        tombstones: list[Tombstone] = []

        for mailbox_id, mailbox in self._mailboxes.items():
            if not is_primary(relay, mailbox_id):
                continue
            records.extend(r for r in mailbox.records.values() if r.expires_at > now)
            tombstones.extend(
                t for t in mailbox.tombstones.values() if t.expires_at > now
            )
        return records, tombstones

    def hinted_for(self, primary: RelayName) -> list[Record]:
        """Records we are holding on behalf of `primary`, to hand back."""
        now = self._clock.now()
        return [
            record
            for mailbox in self._mailboxes.values()
            for record in mailbox.records.values()
            if record.hint == primary and record.expires_at > now
        ]

    def hinted_primaries(self) -> set[RelayName]:
        """Which primaries we are currently covering for.

        The handoff loop walks these rather than every mailbox, so a relay with
        nothing hinted does no work.
        """
        return {
            record.hint
            for mailbox in self._mailboxes.values()
            for record in mailbox.records.values()
            if record.hint is not None
        }

    def mailboxes_not_ours(self) -> list[MailboxId]:
        """Mailboxes we hold without being a primary and without a hint.

        Should normally be empty. It fills if a sender wrote to a stand-in
        without a hint, which is a bug worth seeing rather than a state worth
        tolerating.
        """
        return [
            mailbox_id
            for mailbox_id, mailbox in self._mailboxes.items()
            if not is_primary(self.relay, mailbox_id)
            and any(record.hint is None for record in mailbox.records.values())
        ]

    # -- forgetting ---------------------------------------------------------

    def expire(self) -> ExpiryReport:
        """Drop everything past its time. Called on a timer by the relay.

        Nothing else has to be alive for this to work, which is what makes the
        retention promise structural rather than cooperative.
        """
        now = self._clock.now()
        records = self._sweep(self._record_expiry, now, self._remove_record)
        tombstones = self._sweep(self._tombstone_expiry, now, self._remove_tombstone)
        return ExpiryReport(records=records, tombstones=tombstones)

    def _sweep(
        self,
        heap: list[tuple[float, MailboxId, EnvelopeId]],
        now: float,
        remove: Callable[[MailboxId, EnvelopeId, float], bool],
    ) -> int:
        """Pop everything due and let `remove` decide whether it is still real.

        One clock reading for the whole sweep, so that a long sweep cannot
        remove something that only became due part way through it.
        """
        removed = 0
        while heap and heap[0][0] <= now:
            _, mailbox_id, envelope_id = heapq.heappop(heap)
            if remove(mailbox_id, envelope_id, now):
                removed += 1
        return removed

    def _remove_record(
        self, mailbox_id: MailboxId, envelope_id: EnvelopeId, now: float
    ) -> bool:
        mailbox = self._mailboxes.get(mailbox_id)
        if mailbox is None:
            return False
        record = mailbox.records.get(envelope_id)
        # A heap entry can be stale: the record may have been acknowledged, or
        # stored again with a later expiry. Only drop it if it is really due.
        if record is None or record.expires_at > now:
            return False
        del mailbox.records[envelope_id]
        self._record_count -= 1
        self._drop_if_empty(mailbox_id)
        return True

    def _remove_tombstone(
        self, mailbox_id: MailboxId, envelope_id: EnvelopeId, now: float
    ) -> bool:
        mailbox = self._mailboxes.get(mailbox_id)
        if mailbox is None:
            return False
        tombstone = mailbox.tombstones.get(envelope_id)
        if tombstone is None or tombstone.expires_at > now:
            return False
        del mailbox.tombstones[envelope_id]
        self._drop_if_empty(mailbox_id)
        return True

    def _drop_if_empty(self, mailbox_id: MailboxId) -> None:
        mailbox = self._mailboxes.get(mailbox_id)
        if mailbox is not None and mailbox.empty():
            del self._mailboxes[mailbox_id]

    # -- reporting ----------------------------------------------------------

    def counts(self) -> StoreCounts:
        return StoreCounts(
            records=self._record_count,
            tombstones=sum(len(m.tombstones) for m in self._mailboxes.values()),
            mailboxes=len(self._mailboxes),
            hinted_records=sum(
                1
                for mailbox in self._mailboxes.values()
                for record in mailbox.records.values()
                if record.hint is not None
            ),
        )

    def __len__(self) -> int:
        return self._record_count
