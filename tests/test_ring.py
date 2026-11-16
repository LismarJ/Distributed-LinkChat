"""Placement: the one thing every process must compute identically.

Nothing coordinates the senders, the readers and the relays. They agree only
because they run the same function over the same fixed relay set, so these
tests are mostly about agreement and shape rather than about any single answer.
"""

from __future__ import annotations

import collections

import pytest

from linkchat.core.constants import PRIMARY_COUNT, RELAY_COUNT, RELAY_NAMES
from linkchat.core.ring import (
    is_primary,
    placement_fingerprint,
    preference_list,
    primaries,
    stand_ins,
)
from linkchat.core.types import MailboxId, RelayName

SAMPLE = tuple(MailboxId(f"mailbox-{index}") for index in range(2000))

PINNED_FINGERPRINT = "b743807167efbf6e59151d9c7a8b0fc7"
"""Pinned 2026-11-12.

If this fails, placement changed. That is only ever correct on purpose: a
cluster running two different placement functions stores messages where the
other half does not look. Changing the hash, the relay names, `RELAY_COUNT` or
`PRIMARY_COUNT` all move it. Update it in the same commit as the change, and
say in the message why.
"""


def test_placement_is_pinned() -> None:
    assert placement_fingerprint() == PINNED_FINGERPRINT, (
        "placement changed. If that was deliberate, update PINNED_FINGERPRINT "
        "in this test and explain it; if not, something broke agreement "
        "between processes."
    )


@pytest.mark.parametrize("mailbox_id", SAMPLE[:50])
def test_preference_list_is_a_permutation_of_every_relay(mailbox_id: MailboxId) -> None:
    """A sender must be able to keep falling through until three confirm."""
    listed = preference_list(mailbox_id)
    assert len(listed) == RELAY_COUNT
    assert set(listed) == set(RELAY_NAMES)
    assert len(set(listed)) == len(listed)


@pytest.mark.parametrize("mailbox_id", SAMPLE[:50])
def test_primaries_and_stand_ins_partition_the_list(mailbox_id: MailboxId) -> None:
    listed = preference_list(mailbox_id)
    assert primaries(mailbox_id) == listed[:PRIMARY_COUNT]
    assert stand_ins(mailbox_id) == listed[PRIMARY_COUNT:]
    assert len(primaries(mailbox_id)) == PRIMARY_COUNT


def test_the_same_mailbox_always_lands_in_the_same_place() -> None:
    for mailbox_id in SAMPLE[:200]:
        assert preference_list(mailbox_id) == preference_list(mailbox_id)


def test_different_mailboxes_do_not_all_land_in_the_same_place() -> None:
    """Guards against a hash that ignores its input, which would pass the
    determinism test above while putting every mailbox on the same relays."""
    distinct = {primaries(mailbox_id) for mailbox_id in SAMPLE}
    assert len(distinct) > 10


@pytest.mark.parametrize("mailbox_id", SAMPLE[:50])
def test_is_primary_agrees_with_primaries(mailbox_id: MailboxId) -> None:
    for relay in primaries(mailbox_id):
        assert is_primary(relay, mailbox_id)
    for relay in stand_ins(mailbox_id):
        assert not is_primary(relay, mailbox_id)


def test_the_list_is_ordered_by_descending_score() -> None:
    """The ordering rule itself, checked against the scores it claims to use.

    Reaches into `_score` on purpose: the ordering is the contract, and a test
    that only checked the output was a permutation would pass on a list in any
    order at all.
    """
    from linkchat.core.ring import _score

    for mailbox_id in SAMPLE[:200]:
        listed = preference_list(mailbox_id)
        scores = [_score(mailbox_id, relay) for relay in listed]
        assert scores == sorted(scores, reverse=True), (
            f"{mailbox_id} is not ordered by score: {list(zip(listed, scores))}"
        )


def test_ties_are_broken_by_name_not_by_listing_order() -> None:
    """So the result cannot depend on how RELAY_NAMES happens to be ordered."""
    from linkchat.core.ring import _score

    for mailbox_id in SAMPLE[:500]:
        listed = preference_list(mailbox_id)
        for earlier, later in zip(listed, listed[1:]):
            first_score = _score(mailbox_id, earlier)
            second_score = _score(mailbox_id, later)
            if first_score == second_score:
                assert earlier < later


def test_every_relay_gets_a_fair_share_of_first_place() -> None:
    """Balance decides how evenly memory is used across relays.

    Loose bounds on purpose: this is a property of the hash, and tightening it
    would make the test fail on a sample size change rather than on a bug.
    """
    counts: collections.Counter[RelayName] = collections.Counter(
        preference_list(mailbox_id)[0] for mailbox_id in SAMPLE
    )
    assert set(counts) == set(RELAY_NAMES), "a relay is never first primary"
    ideal = len(SAMPLE) / RELAY_COUNT
    for relay, count in counts.items():
        assert 0.8 * ideal < count < 1.2 * ideal, f"{relay} holds {count}, ideal {ideal}"


def test_every_relay_gets_a_fair_share_of_primary_duty() -> None:
    counts: collections.Counter[RelayName] = collections.Counter()
    for mailbox_id in SAMPLE:
        counts.update(primaries(mailbox_id))
    ideal = len(SAMPLE) * PRIMARY_COUNT / RELAY_COUNT
    for relay, count in counts.items():
        assert 0.9 * ideal < count < 1.1 * ideal, f"{relay} holds {count}, ideal {ideal}"


def test_losing_a_relay_only_moves_the_mailboxes_it_won() -> None:
    """The property that makes this scheme worth keeping for phase 2.

    In phase 2 the relay set changes. A placement function that reshuffled
    everything when one relay left would make every membership change a
    cluster-wide data movement.
    """
    departing = RELAY_NAMES[0]
    unaffected = [m for m in SAMPLE if departing not in primaries(m)]
    assert unaffected, "the sample should contain mailboxes relay-0 does not hold"
    for mailbox_id in unaffected:
        before = list(primaries(mailbox_id))
        after = [r for r in preference_list(mailbox_id) if r != departing][
            :PRIMARY_COUNT
        ]
        assert before == after, (
            f"{mailbox_id} was not held by {departing} yet its primaries moved"
        )
