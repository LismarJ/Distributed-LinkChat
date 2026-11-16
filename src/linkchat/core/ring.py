"""Where a mailbox lives. Task R1.

Every process computes this independently and they all have to agree, because
nothing coordinates them: a sender picks three relays, a reader picks three
relays, and a relay decides what it is responsible for, all from this function
alone. If two processes disagreed, a message would be stored where nobody looks
for it.

Agreement rests on two things.

**The relay set is fixed.** Five names from `constants.py`, and Ray keeps those
five alive rather than us tracking which are up. That is the largest
simplification Ray's supervision buys phase 1, and it is exactly what phase 2
takes away, where membership changes as nodes come and go.

**The hash is stable.** BLAKE2b, never Python's `hash()`, which is randomized
per process and would make every container place mailboxes differently.

## Why rendezvous hashing rather than a hash ring

The obvious choice is consistent hashing: place each relay at points on a ring,
then walk clockwise from the mailbox's point. We wrote that first, with 64
virtual nodes per relay, and measured it: relays ended up first primary for
between 15.8% and 24.8% of mailboxes against an ideal 20%, and primary for
between 54% and 66% against an ideal 60%. The imbalance is structural, not
sampling noise, because five relays carve the ring into arcs of accidental
sizes, and virtual nodes only shrink that as the square root of their count.

Rendezvous hashing (highest random weight) gives each relay a score per
mailbox and sorts by it. The ordering is then a uniform random permutation, so
balance is as good as the hash function, with no parameter to tune. Measured on
the same 20000 mailboxes: 19.61% to 20.27% first primary, and 59.62% to 60.42%
primary (`tests/test_ring.py` keeps measuring it).

It is also less code, needs no ring built at import, and costs one hash per
relay per lookup, which is five hashes. And it keeps consistent hashing's one
real advantage for phase 2: when the relay set changes, only the mailboxes a
departing relay actually won have to move.
"""

from __future__ import annotations

import hashlib

from .constants import PRIMARY_COUNT, RELAY_NAMES
from .types import MailboxId, RelayName

SCORE_BYTES = 8
"""Width of a score, 64 bits. Ties are vanishingly unlikely and broken by name
anyway, so a tie would only reorder one mailbox's preference list."""


def _score(mailbox_id: str, relay: str) -> int:
    """How strong `relay`'s claim to `mailbox_id` is. Higher wins.

    The null byte between the two keeps the inputs unambiguous: without it,
    mailbox "ab" with relay "c" and mailbox "a" with relay "bc" would hash the
    same string.
    """
    material = f"{mailbox_id}\x00{relay}".encode()
    digest = hashlib.blake2b(material, digest_size=SCORE_BYTES).digest()
    return int.from_bytes(digest, "big")


def preference_list(mailbox_id: MailboxId) -> tuple[RelayName, ...]:
    """Every relay, ordered by how responsible it is for this mailbox.

    The first `PRIMARY_COUNT` hold the record. The rest are the order in which a
    sender falls through when a primary does not answer, so that two senders
    writing the same mailbox during the same failure choose the same stand-in
    and the copies land together instead of scattering.

    Always a permutation of every relay name, so a sender can keep walking it
    until three relays have confirmed. Sorted by score descending, with the
    name as a tie-break so the result does not depend on the order
    `RELAY_NAMES` happens to be in.
    """
    return tuple(
        sorted(RELAY_NAMES, key=lambda relay: (-_score(mailbox_id, relay), relay))
    )


def primaries(mailbox_id: MailboxId) -> tuple[RelayName, ...]:
    """The relays that are supposed to hold this mailbox's records."""
    return preference_list(mailbox_id)[:PRIMARY_COUNT]


def stand_ins(mailbox_id: MailboxId) -> tuple[RelayName, ...]:
    """The relays that cover for an unreachable primary, in fallback order."""
    return preference_list(mailbox_id)[PRIMARY_COUNT:]


def is_primary(relay: RelayName, mailbox_id: MailboxId) -> bool:
    """Whether `relay` should hold this mailbox as one of its own.

    A relay uses this to decide what to ask its peers for when it restarts
    empty, and what to keep rather than hand back.
    """
    return relay in primaries(mailbox_id)


PLACEMENT_PROBES: tuple[str, ...] = tuple(f"probe-{index}" for index in range(64))
"""A fixed sample of mailbox ids, used only by `placement_fingerprint`."""


def placement_fingerprint() -> str:
    """A digest of where a fixed sample of mailboxes would be placed.

    Two processes that disagree about this disagree about where mailboxes live,
    which is the one disagreement this module exists to prevent, so it is worth
    being able to compare at a glance. It changes if the hash, the relay names,
    the relay count or the primary count change, and the test that pins it has
    to be updated deliberately.
    """
    digest = hashlib.blake2b(digest_size=16)
    for probe in PLACEMENT_PROBES:
        digest.update(probe.encode("utf-8"))
        for relay in preference_list(MailboxId(probe)):
            digest.update(b"\x00")
            digest.update(relay.encode("utf-8"))
    return digest.hexdigest()
