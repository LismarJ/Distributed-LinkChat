"""The relay's memory: idempotence, tombstones, expiry, quotas, resync.

These are the four behaviours the fault-tolerance argument leans on, so most of
these tests are written as the failure they protect against rather than as the
feature they exercise.
"""

from __future__ import annotations

import pytest

from linkchat.core.mailbox import MailboxStore, StoreOutcome
from linkchat.core.ports import Tombstone
from linkchat.core.ring import primaries, stand_ins
from linkchat.core.types import MailboxId
from tests.conftest import FakeClock, make_record

MAILBOX = MailboxId("mailbox-1")


def a_store(clock: FakeClock, **kwargs: object) -> MailboxStore:
    """A store on whichever relay is actually a primary for MAILBOX.

    Placement is a real function, so a test that picked a relay by hand would
    be testing a relay that is not responsible for the mailbox and would get
    confusing answers from `share_for`.
    """
    return MailboxStore(primaries(MAILBOX)[0], clock, **kwargs)  # type: ignore[arg-type]


# -- idempotence -------------------------------------------------------------


def test_storing_the_same_record_twice_holds_one_copy(clock: FakeClock) -> None:
    """The property that makes retrying a failed call safe."""
    store = a_store(clock)
    record = make_record()

    assert store.store(record) is StoreOutcome.STORED
    assert store.store(record) is StoreOutcome.ALREADY_PRESENT
    assert len(store) == 1
    assert len(store.fetch(MAILBOX)) == 1


def test_already_present_counts_as_accepted(clock: FakeClock) -> None:
    """A sender retrying after an ambiguous failure must be able to count it.

    Ray cannot tell a caller whether a failed call ran. If a retry reported
    anything the sender read as a refusal, a message already safely stored
    three times would never reach `sent`.
    """
    store = a_store(clock)
    record = make_record()
    store.store(record)
    assert store.store(record).accepted


def test_two_records_with_the_same_content_are_the_same_record(clock: FakeClock) -> None:
    store = a_store(clock)
    store.store(make_record(body=b"identical"))
    store.store(make_record(body=b"identical"))
    assert len(store) == 1


# -- tombstones --------------------------------------------------------------


def test_acknowledging_removes_the_record_and_remembers_it(clock: FakeClock) -> None:
    store = a_store(clock)
    record = make_record()
    store.store(record)

    assert store.ack(MAILBOX, [record.envelope_id]) == 1
    assert store.fetch(MAILBOX) == []
    assert store.is_tombstoned(MAILBOX, record.envelope_id)


def test_a_late_copy_cannot_resurrect_an_acknowledged_record(clock: FakeClock) -> None:
    """The case tombstones exist for.

    A stand-in that held this record for an unreachable primary hands it back
    after the recipient has already acknowledged it. Without the tombstone the
    relay would take it and offer it again.
    """
    store = a_store(clock)
    record = make_record()
    store.store(record)
    store.ack(MAILBOX, [record.envelope_id])

    assert store.store(record) is StoreOutcome.REFUSED_TOMBSTONED
    assert store.fetch(MAILBOX) == []


def test_acknowledging_twice_is_harmless(clock: FakeClock) -> None:
    store = a_store(clock)
    record = make_record()
    store.store(record)

    assert store.ack(MAILBOX, [record.envelope_id]) == 1
    assert store.ack(MAILBOX, [record.envelope_id]) == 0


def test_acknowledging_something_never_held_still_leaves_a_tombstone(
    clock: FakeClock,
) -> None:
    """The subtle one, and the reason `ack` does not simply ignore unknown ids.

    This relay missed the original write. The recipient acknowledges on every
    relay it can reach, including this one. Later a stand-in hands the record
    over. If acknowledging an unknown id did nothing, this relay would accept
    that record and serve a message the reader has already seen.
    """
    store = a_store(clock)
    record = make_record()

    assert store.ack(MAILBOX, [record.envelope_id]) == 0
    assert store.is_tombstoned(MAILBOX, record.envelope_id)
    assert store.store(record) is StoreOutcome.REFUSED_TOMBSTONED


def test_a_tombstone_beats_a_record_arriving_in_the_same_batch(
    clock: FakeClock,
) -> None:
    """Order inside one `absorb` must not decide the outcome."""
    store = a_store(clock)
    record = make_record()
    tombstone = Tombstone(
        mailbox_id=MAILBOX,
        envelope_id=record.envelope_id,
        expires_at=clock.now() + 3600,
    )

    store.absorb(records=[record], tombstones=[tombstone])
    assert store.fetch(MAILBOX) == []
    assert store.is_tombstoned(MAILBOX, record.envelope_id)


def test_a_tombstone_removes_a_record_we_already_held(clock: FakeClock) -> None:
    store = a_store(clock)
    record = make_record()
    store.store(record)

    store.absorb(
        tombstones=[
            Tombstone(
                mailbox_id=MAILBOX,
                envelope_id=record.envelope_id,
                expires_at=clock.now() + 3600,
            )
        ]
    )
    assert store.fetch(MAILBOX) == []
    assert len(store) == 0


# -- expiry ------------------------------------------------------------------


def test_records_expire_without_anyone_acknowledging(clock: FakeClock) -> None:
    """Expiry is the retention guarantee; acknowledgement is only an optimisation."""
    store = a_store(clock)
    store.store(make_record(expires_at=clock.now() + 60))

    clock.advance(61)
    report = store.expire()

    assert report.records == 1
    assert store.fetch(MAILBOX) == []
    assert len(store) == 0


def test_an_expired_record_is_not_served_before_the_sweep_runs(
    clock: FakeClock,
) -> None:
    """The sweeper is on a timer, so reads must filter as well."""
    store = a_store(clock)
    store.store(make_record(expires_at=clock.now() + 60))

    clock.advance(61)
    assert store.fetch(MAILBOX) == []


def test_a_record_already_past_its_time_is_refused(clock: FakeClock) -> None:
    store = a_store(clock)
    assert store.store(make_record(expires_at=clock.now() - 1)) is (
        StoreOutcome.REFUSED_EXPIRED
    )
    assert len(store) == 0


def test_tombstones_expire_too(clock: FakeClock) -> None:
    """Otherwise a relay would accumulate deletion markers for ever, which is
    a slower way of becoming the permanent database requirement R9 forbids."""
    store = a_store(clock, tombstone_ttl_s=100.0)
    record = make_record()
    store.store(record)
    store.ack(MAILBOX, [record.envelope_id])

    clock.advance(101)
    report = store.expire()

    assert report.tombstones == 1
    assert not store.is_tombstoned(MAILBOX, record.envelope_id)


def test_a_tombstone_outlives_the_record_it_suppresses(clock: FakeClock) -> None:
    """With no digest comparison to spread tombstones (ADR-0014), the only
    protection against a late copy is that the tombstone is still there."""
    store = a_store(clock, tombstone_ttl_s=3600.0)
    record = make_record(expires_at=clock.now() + 60)
    store.store(record)
    store.ack(MAILBOX, [record.envelope_id])

    clock.advance(120)
    store.expire()

    # Refused as tombstoned rather than as expired, because the tombstone is
    # checked first. That order is deliberate: "a late copy was suppressed" is
    # a real event worth counting, while "something arrived past its time" is
    # junk, and the record here is both.
    assert store.store(record) is StoreOutcome.REFUSED_TOMBSTONED
    assert store.is_tombstoned(MAILBOX, record.envelope_id)


def test_sweeping_is_safe_when_a_record_was_acknowledged_first(
    clock: FakeClock,
) -> None:
    """The expiry heap keeps an entry for a record that is already gone."""
    store = a_store(clock)
    record = make_record(expires_at=clock.now() + 60)
    store.store(record)
    store.ack(MAILBOX, [record.envelope_id])

    clock.advance(61)
    report = store.expire()

    assert report.records == 0
    assert len(store) == 0


def test_an_empty_mailbox_is_dropped(clock: FakeClock) -> None:
    """A relay should not keep a per-mailbox entry for every conversation it
    has ever seen: that alone would be a record of who talks to whom."""
    store = a_store(clock, tombstone_ttl_s=10.0)
    record = make_record(expires_at=clock.now() + 5)
    store.store(record)
    store.ack(MAILBOX, [record.envelope_id])

    clock.advance(11)
    store.expire()

    assert store.counts().mailboxes == 0


# -- quotas ------------------------------------------------------------------


def test_a_mailbox_at_its_quota_refuses_more(clock: FakeClock) -> None:
    store = a_store(clock, max_records_per_mailbox=2)
    for index in range(2):
        assert store.store(make_record(body=f"m{index}".encode())) is StoreOutcome.STORED
    assert store.store(make_record(body=b"m2")) is StoreOutcome.REFUSED_FULL


def test_a_full_refusal_is_not_accepted(clock: FakeClock) -> None:
    """The sender must keep the message rather than count the relay."""
    store = a_store(clock, max_records_per_mailbox=1)
    store.store(make_record(body=b"first"))
    assert not store.store(make_record(body=b"second")).accepted


def test_the_relay_total_cap_applies_across_mailboxes(clock: FakeClock) -> None:
    store = a_store(clock, max_records_total=2)
    assert store.store(make_record(mailbox="a", body=b"1")) is StoreOutcome.STORED
    assert store.store(make_record(mailbox="b", body=b"2")) is StoreOutcome.STORED
    assert store.store(make_record(mailbox="c", body=b"3")) is StoreOutcome.REFUSED_FULL


def test_capacity_frees_up_after_expiry(clock: FakeClock) -> None:
    store = a_store(clock, max_records_total=1)
    store.store(make_record(body=b"first", expires_at=clock.now() + 60))
    clock.advance(61)
    store.expire()
    assert store.store(make_record(body=b"second", expires_at=clock.now() + 60)) is (
        StoreOutcome.STORED
    )


def test_a_tombstone_still_wins_over_a_full_mailbox(clock: FakeClock) -> None:
    """Refusing for the right reason matters: a full relay must not report a
    resurrection attempt as a capacity problem."""
    store = a_store(clock, max_records_per_mailbox=1)
    record = make_record(body=b"gone")
    store.store(record)
    store.ack(MAILBOX, [record.envelope_id])
    store.store(make_record(body=b"filler"))

    assert store.store(record) is StoreOutcome.REFUSED_TOMBSTONED


# -- resync and handoff ------------------------------------------------------


def test_share_for_gives_a_peer_what_it_should_hold(clock: FakeClock) -> None:
    """What a relay coming back from a restart receives."""
    store = a_store(clock)
    record = make_record()
    store.store(record)

    other_primary = primaries(MAILBOX)[1]
    records, _ = store.share_for(other_primary)
    assert [r.envelope_id for r in records] == [record.envelope_id]


def test_share_for_gives_nothing_to_a_relay_that_should_not_hold_it(
    clock: FakeClock,
) -> None:
    store = a_store(clock)
    store.store(make_record())

    outsider = stand_ins(MAILBOX)[-1]
    records, tombstones = store.share_for(outsider)
    assert records == [] and tombstones == []


def test_share_for_includes_tombstones(clock: FakeClock) -> None:
    """A restarted relay must learn what was deleted, not only what exists.

    Otherwise it would come back, be handed a record by a stand-in, and serve
    a message the reader already acknowledged.
    """
    store = a_store(clock)
    record = make_record()
    store.store(record)
    store.ack(MAILBOX, [record.envelope_id])

    _, tombstones = store.share_for(primaries(MAILBOX)[1])
    assert [t.envelope_id for t in tombstones] == [record.envelope_id]


def test_share_for_leaves_out_what_has_expired(clock: FakeClock) -> None:
    store = a_store(clock)
    store.store(make_record(expires_at=clock.now() + 60))
    clock.advance(61)

    records, _ = store.share_for(primaries(MAILBOX)[1])
    assert records == []


def test_a_resyncing_relay_can_absorb_what_a_peer_sends(clock: FakeClock) -> None:
    source = a_store(clock)
    record = make_record()
    source.store(record)

    replacement = MailboxStore(primaries(MAILBOX)[1], clock)
    records, tombstones = source.share_for(replacement.relay)
    added, _ = replacement.absorb(records=records, tombstones=tombstones)

    assert added == 1
    assert [r.envelope_id for r in replacement.fetch(MAILBOX)] == [record.envelope_id]


def test_absorbing_the_same_batch_twice_changes_nothing(clock: FakeClock) -> None:
    """Resync may be retried after a failure that may or may not have landed."""
    store = MailboxStore(primaries(MAILBOX)[1], clock)
    record = make_record()

    assert store.absorb(records=[record]) == (1, 0)
    assert store.absorb(records=[record]) == (0, 0)
    assert len(store) == 1


def test_hinted_records_are_tracked_by_the_primary_they_cover_for(
    clock: FakeClock,
) -> None:
    covering = stand_ins(MAILBOX)[0]
    absent = primaries(MAILBOX)[0]
    store = MailboxStore(covering, clock)

    record = make_record(hint=absent)
    assert store.store(record) is StoreOutcome.STORED
    assert store.hinted_primaries() == {absent}
    assert [r.envelope_id for r in store.hinted_for(absent)] == [record.envelope_id]
    assert store.counts().hinted_records == 1


def test_a_stand_in_drops_its_copy_once_the_primary_has_it(clock: FakeClock) -> None:
    """No tombstone here: the record moved, it was not acknowledged.

    A tombstone would be wrong and would stop the primary from ever handing it
    back if the roles reversed.
    """
    covering = stand_ins(MAILBOX)[0]
    store = MailboxStore(covering, clock)
    record = make_record(hint=primaries(MAILBOX)[0])
    store.store(record)

    assert store.forget_record(MAILBOX, record.envelope_id) is True
    assert store.fetch(MAILBOX) == []
    assert not store.is_tombstoned(MAILBOX, record.envelope_id)
    assert store.hinted_primaries() == set()


def test_forgetting_something_we_do_not_have_is_a_no_op(clock: FakeClock) -> None:
    store = a_store(clock)
    assert store.forget_record(MAILBOX, make_record().envelope_id) is False


def test_a_stand_in_serves_the_record_while_it_holds_it(clock: FakeClock) -> None:
    """A reader must find a record even when a primary was unreachable."""
    covering = stand_ins(MAILBOX)[0]
    store = MailboxStore(covering, clock)
    record = make_record(hint=primaries(MAILBOX)[0])
    store.store(record)

    assert [r.envelope_id for r in store.fetch(MAILBOX)] == [record.envelope_id]


# -- reporting ---------------------------------------------------------------


def test_holding_a_mailbox_we_should_not_is_visible(clock: FakeClock) -> None:
    """A diagnostic, not a normal state.

    A record on a relay that is neither a primary nor standing in for one means
    a sender wrote without a hint, which is a bug. It should be visible on the
    status rather than sit there looking like ordinary data.
    """
    outsider = stand_ins(MAILBOX)[-1]
    store = MailboxStore(outsider, clock)
    store.store(make_record(hint=None))

    assert store.mailboxes_not_ours() == [MAILBOX]


def test_a_stand_in_with_a_hint_is_not_flagged(clock: FakeClock) -> None:
    covering = stand_ins(MAILBOX)[0]
    store = MailboxStore(covering, clock)
    store.store(make_record(hint=primaries(MAILBOX)[0]))

    assert store.mailboxes_not_ours() == []


def test_a_primary_is_never_flagged(clock: FakeClock) -> None:
    store = a_store(clock)
    store.store(make_record())
    assert store.mailboxes_not_ours() == []


def test_an_expiry_report_totals_both_kinds(clock: FakeClock) -> None:
    store = a_store(clock, tombstone_ttl_s=50.0)
    acknowledged = make_record(body=b"acked", expires_at=clock.now() + 500)
    store.store(acknowledged)
    store.ack(MAILBOX, [acknowledged.envelope_id])
    store.store(make_record(body=b"left", expires_at=clock.now() + 10))

    clock.advance(60)
    report = store.expire()

    assert report.records == 1
    assert report.tombstones == 1
    assert report.total == 2


def test_counts_describe_the_store(clock: FakeClock) -> None:
    store = a_store(clock)
    first = make_record(body=b"one")
    store.store(first)
    store.store(make_record(body=b"two"))
    store.ack(MAILBOX, [first.envelope_id])

    counts = store.counts()
    assert counts.records == 1
    assert counts.tombstones == 1
    assert counts.mailboxes == 1


def test_a_store_must_belong_to_a_real_relay(clock: FakeClock) -> None:
    with pytest.raises(ValueError):
        MailboxStore("relay-99", clock)  # type: ignore[arg-type]
