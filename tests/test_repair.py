"""Behaviour of repair: sync on restart (R6) and hinted handoff (R7). These are
RED until `linkchat.core.repair.resync` and `handoff_round` are implemented.

They use the real `MailboxStore` behind the fake transport, so when you make them
pass you have exercised the same store the relay actor wraps in production.
"""

from __future__ import annotations

from linkchat.core.repair import handoff_round, resync
from linkchat.core.ring import primaries, stand_ins
from linkchat.core.types import MailboxId

from .conftest import make_record
from .fakes.transport import FakeCluster

# -- R6: sync on restart ----------------------------------------------------

R6_MAILBOX = "mb-r6"


async def test_resync_refills_a_restarted_relay_from_peers():
    cluster = FakeCluster()
    prim = primaries(MailboxId(R6_MAILBOX))
    record = make_record(mailbox=R6_MAILBOX, body=b"durable")
    for p in prim:
        await cluster.directory().gateway(p).store(record)

    cluster.restart(prim[0])  # empty, SYNCING
    assert not cluster.stores[prim[0]].has(MailboxId(R6_MAILBOX), record.envelope_id)

    await resync(prim[0], cluster.stores[prim[0]], cluster.peers(prim[0]), clock=cluster.clock)

    assert cluster.stores[prim[0]].has(MailboxId(R6_MAILBOX), record.envelope_id)


async def test_resync_survives_a_down_peer():
    cluster = FakeCluster()
    prim = primaries(MailboxId(R6_MAILBOX))
    record = make_record(mailbox=R6_MAILBOX, body=b"from-the-survivor")
    for p in prim:
        await cluster.directory().gateway(p).store(record)

    cluster.restart(prim[0])
    cluster.kill(prim[1])  # one holder down; prim[2] still answers

    await resync(prim[0], cluster.stores[prim[0]], cluster.peers(prim[0]), clock=cluster.clock)

    assert cluster.stores[prim[0]].has(MailboxId(R6_MAILBOX), record.envelope_id)


# -- R7: hinted handoff -----------------------------------------------------

R7_MAILBOX = "mb-r7"


async def test_handoff_returns_record_to_primary_and_drops_copy():
    cluster = FakeCluster()
    primary = primaries(MailboxId(R7_MAILBOX))[0]
    standin = stand_ins(MailboxId(R7_MAILBOX))[0]
    record = make_record(mailbox=R7_MAILBOX, body=b"covered", hint=str(primary))
    cluster.stores[standin].absorb([record], [])

    handed = await handoff_round(
        standin, cluster.stores[standin], cluster.peers(standin), clock=cluster.clock
    )

    assert handed == 1
    assert cluster.stores[primary].has(MailboxId(R7_MAILBOX), record.envelope_id)
    assert not cluster.stores[standin].has(MailboxId(R7_MAILBOX), record.envelope_id)


async def test_handoff_keeps_copy_while_primary_is_down():
    cluster = FakeCluster()
    primary = primaries(MailboxId(R7_MAILBOX))[0]
    standin = stand_ins(MailboxId(R7_MAILBOX))[0]
    record = make_record(mailbox=R7_MAILBOX, body=b"still-covered", hint=str(primary))
    cluster.stores[standin].absorb([record], [])
    cluster.kill(primary)

    handed = await handoff_round(
        standin, cluster.stores[standin], cluster.peers(standin), clock=cluster.clock
    )

    assert handed == 0
    assert cluster.stores[standin].has(MailboxId(R7_MAILBOX), record.envelope_id)
