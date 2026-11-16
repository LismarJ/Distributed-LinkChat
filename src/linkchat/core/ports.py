"""The ports: the only way `linkchat.core` reaches anything outside itself.

Nothing in `linkchat.core` may import Ray. Phase 1 implements these ports with
Ray actor calls in `linkchat.ray_runtime`; phase 2 replaces that implementation
and leaves this file alone (ADR-0013). A fault-injecting fake implements them
for tests.

**Frozen 2026-11-12, gate G0.** Both work tracks are written against these
signatures, so changing one blocks the other. Propose a change before making
it, and record it here.

Deliberately absent from this file: Ray handles, `ObjectRef`, Ray's exception
types, actor names, namespaces, and anything about how a relay is addressed.
If one of those appears here, the phase 2 migration has already started leaking.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol, Sequence, runtime_checkable

from .types import Credential, EnvelopeId, MailboxId, RelayName, UserId

# ---------------------------------------------------------------------------
# Failures
# ---------------------------------------------------------------------------


class RelayError(Exception):
    """Base of every failure of a relay operation."""


class RelayUnreachable(RelayError):
    """The relay did not answer in time, or the runtime called it unavailable.

    **The operation may already have run.** Ray's documentation is explicit
    that an actor call can raise even though the call did execute, and offers
    no way to tell which happened. Never treat this as "it did not happen".

    The correct response is to retry, or to use the next relay on the
    preference list, which is safe because every relay operation is idempotent
    on the envelope id. See docs/design/fault-tolerance.md case 1.
    """


class RelayGone(RelayError):
    """The runtime confirmed this relay is dead.

    Unlike `RelayUnreachable` this is not ambiguous about liveness, but it is
    still ambiguous about whether the call ran. Treat the call as unknown and
    the relay as unusable until a later probe succeeds.
    """


class RelaySyncing(RelayError):
    """The relay is alive but refusing client work until it has caught up.

    A relay that Ray restarted comes back with no data, so it must copy its
    share from its peers before serving anyone. Serving while empty would make
    a reader believe a mailbox is empty when it is not. Callers treat this
    exactly like `RelayUnreachable` and move to the next relay.
    """


class MailboxFull(RelayError):
    """The mailbox is at its quota, or the relay is at its total cap.

    Unlike the errors above this one is definite: the record was not stored.
    The sender keeps it in the outbox.
    """


class CredentialRejected(RelayError):
    """The caller may not read from or delete from this mailbox.

    Definite, like `MailboxFull`. Not retryable.
    """


# ---------------------------------------------------------------------------
# What crosses the boundary
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Record:
    """One stored envelope, as it travels between processes.

    `envelope` is opaque bytes of a fixed size. A relay never learns anything
    from it, and `envelope_id` is its content hash, which is what makes `store`
    idempotent.
    """

    mailbox_id: MailboxId
    envelope_id: EnvelopeId
    envelope: bytes
    expires_at: float
    """Absolute time, seconds since the epoch, set by the sender.

    A relay drops the record at this time whether or not an acknowledgement
    ever arrives. This, not acknowledgement, is the guarantee requirement R9
    rests on.
    """

    hint: RelayName | None = None
    """Set when this relay is standing in for an unreachable primary.

    The stand-in keeps the record, serves it to readers, and hands it back once
    the named primary answers again. `None` means this relay is a primary for
    the mailbox and the record is simply its own.
    """


@dataclass(frozen=True, slots=True)
class Tombstone:
    """A record that was acknowledged, remembered so it cannot come back.

    Needed because a copy can still arrive from a stand-in or a peer after the
    recipient acknowledged. Without the tombstone the relay would accept it and
    offer it again. Tombstones expire when the record itself would have.
    """

    mailbox_id: MailboxId
    envelope_id: EnvelopeId
    expires_at: float


class RelayState(Enum):
    """Whether a relay will serve clients."""

    SYNCING = "syncing"
    """Started or restarted, still copying its share from peers."""

    READY = "ready"
    """Serving."""


@dataclass(frozen=True, slots=True)
class RelayStatus:
    """What a relay reports about itself, for the UI and the demonstration.

    Counts only. A relay must never put a mailbox id or any envelope content in
    anything that leaves it, because Ray forwards worker logs to the head.
    """

    name: RelayName
    state: RelayState
    records: int
    tombstones: int
    mailboxes: int
    hinted_records: int
    """How many records this relay holds on behalf of an unreachable primary."""

    loops_last_ok: dict[str, float]
    """Per background loop, when it last completed a round.

    A loop that died silently would otherwise be invisible: an exception in an
    asyncio task does not stop the actor. See docs/design/fault-tolerance.md
    case 15.
    """


@dataclass(frozen=True, slots=True)
class PullResult:
    """What a peer sends to a relay that is catching up."""

    records: Sequence[Record]
    tombstones: Sequence[Tombstone]


# ---------------------------------------------------------------------------
# Ports
# ---------------------------------------------------------------------------


@runtime_checkable
class RelayGateway(Protocol):
    """What a client needs from **one** relay.

    Every method may raise any of the errors above. A caller must give each
    call its own timeout rather than waiting for the runtime to decide a relay
    is dead, because that verdict can take tens of seconds.
    """

    async def store(self, record: Record) -> None:
        """Store `record`, or do nothing if this relay already has it.

        Idempotent on `record.envelope_id`. Storing an id the relay holds, or
        has a tombstone for, succeeds and changes nothing.

        Raises:
            MailboxFull: definite, the record was not stored.
            RelayUnreachable, RelayGone, RelaySyncing: unknown, retry elsewhere.
        """
        ...

    async def fetch(
        self, mailbox_id: MailboxId, credential: Credential
    ) -> Sequence[Record]:
        """Return every record this relay holds for `mailbox_id`.

        There is deliberately no way to ask for history: a relay can only
        return what it is currently holding, which is what makes requirement R9
        structural rather than a promise.
        """
        ...

    async def ack(
        self,
        mailbox_id: MailboxId,
        credential: Credential,
        envelope_ids: Sequence[EnvelopeId],
    ) -> None:
        """Replace these records with tombstones.

        Idempotent. Acknowledging something already gone succeeds. The caller
        must have committed the messages to its own storage first, so that a
        crash between the two loses nothing.
        """
        ...

    async def wait_for_mail(
        self, mailbox_id: MailboxId, credential: Credential, timeout_s: float
    ) -> bool:
        """Block until a record arrives for `mailbox_id`, or the timeout passes.

        Returns True if mail arrived, False on timeout. The caller fetches
        afterwards either way; this only avoids polling. Implemented on the
        relay with an `asyncio.Event`, which is Ray's documented actor-sync
        pattern.
        """
        ...

    async def publish_prekeys(self, user_id: UserId, bundle: bytes) -> None:
        """Store this user's signed prekey bundle.

        The bundle is signed by its owner, so a relay cannot substitute one.
        Republished by the owner whenever it comes online.
        """
        ...

    async def get_prekeys(self, user_id: UserId) -> bytes | None:
        """Return a prekey bundle, or None if this relay has none for the user."""
        ...

    async def status(self) -> RelayStatus:
        """Report this relay's state and counters. Never raises `RelaySyncing`."""
        ...


@runtime_checkable
class RelayDirectory(Protocol):
    """Turns a relay name into something callable.

    The only object that knows how relays are addressed. In phase 1 that is a
    Ray named actor lookup; in phase 2 it is an address from the membership
    view. Nothing in the core may learn the difference.

    Not in the original design sketch, which listed three ports. It was added
    on 2026-11-12 because `RelayGateway` is per-relay and the client needs some
    way to get one, and leaving that to the caller would have put the runtime
    detail back into the core.
    """

    @property
    def relay_names(self) -> Sequence[RelayName]:
        """Every relay name. Fixed in phase 1; see constants.RELAY_NAMES."""
        ...

    def gateway(self, name: RelayName) -> RelayGateway:
        """A gateway to `name`.

        Cheap and synchronous: it resolves a name, it does not call the relay.
        Whether the returned object survives that relay being restarted is a
        runtime concern, and the implementation must handle re-resolution
        itself rather than making callers think about it.
        """
        ...


@runtime_checkable
class RelayPeers(Protocol):
    """What a relay needs from the **other** relays, to repair itself and them.

    Deliberately missing: `digest`, the periodic comparison of what two relays
    hold. ADR-0014 defers it from phase 1, so shipping the method would be dead
    interface. When it arrives it belongs here, as
    `async def digest(self, peer: RelayName, for_relay: RelayName) -> Sequence[tuple[EnvelopeId, bool]]`
    returning each id and whether it is a tombstone.
    """

    @property
    def peer_names(self) -> Sequence[RelayName]:
        """Every relay name except this one."""
        ...

    async def pull(self, peer: RelayName, for_relay: RelayName) -> PullResult:
        """Ask `peer` for everything it holds that `for_relay` should hold.

        Used by a relay that has just started or been restarted. `peer`
        computes the answer itself from the ring, which both sides know, so no
        range has to be described in the call.

        Safe to call on a peer that is itself syncing: it answers with what it
        has. Since every acknowledged record is on three relays and at most two
        can be down, at least one holder answers.
        """
        ...

    async def handoff(
        self,
        peer: RelayName,
        records: Sequence[Record],
        tombstones: Sequence[Tombstone],
    ) -> None:
        """Give `peer` records it should hold, and tombstones it may have missed.

        Used by a stand-in returning records to a primary that is back, and by
        a primary pushing to a relay that joined its set. The sender deletes its
        own copy of a handed-off record only once this call has succeeded; if it
        raises, the copy stays and the attempt is repeated.
        """
        ...


@runtime_checkable
class Clock(Protocol):
    """Time, as a port, so that expiry can be tested without waiting for it."""

    def now(self) -> float:
        """Seconds since the epoch, UTC.

        Each relay reads its own clock and compares it against the absolute
        `expires_at` the sender set, so a relay whose clock runs fast expires
        records early. Accepted for phase 1; see fault-tolerance.md case 10.
        """
        ...

    def monotonic(self) -> float:
        """A steadily increasing counter, for measuring durations.

        Unlike `now`, this is not comparable across processes and bears no
        relation to the wall clock; it only ever moves forward. Used where code
        times how long something took (a spike measurement, a backoff) and must
        not be fooled by the wall clock being adjusted. Folded in from the other
        interface draft during the 2026-11-15 reconciliation.
        """
        ...

    async def sleep(self, seconds: float) -> None:
        """Yield for `seconds`. A fake clock returns at once and advances `now`."""
        ...
