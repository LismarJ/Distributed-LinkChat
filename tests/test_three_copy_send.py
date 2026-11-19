"""Behaviour of three-copy send with stand-ins (R5). These are RED until R5 is
implemented in `linkchat.core.replication.send_to_three`.

They are the specification: make them pass by writing the mechanism. They say
nothing about *how* stand-ins are chosen beyond the invariants the
fault-tolerance argument needs, so they do not prescribe your algorithm, only its
guarantees.
"""

from __future__ import annotations

from linkchat.core.replication import send_to_three
from linkchat.core.ring import primaries
from linkchat.core.types import MailboxId, RelayName

from .conftest import make_record
from .fakes.transport import FakeCluster

MAILBOX = "mb-r5"


def _holders(cluster: FakeCluster, mailbox: str, envelope_id) -> list[RelayName]:
    mb = MailboxId(mailbox)
    return [name for name, store in cluster.stores.items() if store.has(mb, envelope_id)]


def _stored_record(cluster: FakeCluster, name: RelayName, mailbox: str, envelope_id):
    for record in cluster.stores[name].fetch(MailboxId(mailbox)):
        if record.envelope_id == envelope_id:
            return record
    return None


async def test_happy_path_confirms_the_three_primaries():
    cluster = FakeCluster()
    record = make_record(mailbox=MAILBOX, body=b"hello")

    result = await send_to_three(record, cluster.directory(), clock=cluster.clock)

    assert result.sent
    assert set(result.confirmed_relays) == set(primaries(MailboxId(MAILBOX)))
    assert set(_holders(cluster, MAILBOX, record.envelope_id)) == set(
        primaries(MailboxId(MAILBOX))
    )


async def test_two_primaries_down_still_sends_through_standins():
    cluster = FakeCluster()
    prim = primaries(MailboxId(MAILBOX))
    cluster.kill(prim[0], prim[1])
    record = make_record(mailbox=MAILBOX, body=b"resilient")

    result = await send_to_three(record, cluster.directory(), clock=cluster.clock)

    assert result.sent
    assert len(set(result.confirmed_relays)) == 3

    holders = _holders(cluster, MAILBOX, record.envelope_id)
    assert len(holders) == 3
    # Any holder that is not a primary is a stand-in and must carry a hint that
    # names one of the primaries it is covering for.
    for name in holders:
        if name not in prim:
            stored = _stored_record(cluster, name, MAILBOX, record.envelope_id)
            assert stored is not None and stored.hint in (prim[0], prim[1])


async def test_three_relays_down_stays_pending():
    cluster = FakeCluster()
    prim = primaries(MailboxId(MAILBOX))
    cluster.kill(prim[0], prim[1], prim[2])  # only the two stand-ins remain
    record = make_record(mailbox=MAILBOX, body=b"stuck")

    result = await send_to_three(record, cluster.directory(), clock=cluster.clock)

    assert not result.sent
    assert len(set(result.confirmed_relays)) < 3


async def test_resend_is_idempotent():
    cluster = FakeCluster()
    record = make_record(mailbox=MAILBOX, body=b"again")

    first = await send_to_three(record, cluster.directory(), clock=cluster.clock)
    second = await send_to_three(record, cluster.directory(), clock=cluster.clock)

    assert first.sent and second.sent
    # No relay holds two copies: every store of the same id is one record.
    for name in _holders(cluster, MAILBOX, record.envelope_id):
        held = [
            r
            for r in cluster.stores[name].fetch(MailboxId(MAILBOX))
            if r.envelope_id == record.envelope_id
        ]
        assert len(held) == 1
