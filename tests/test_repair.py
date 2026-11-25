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

    await resync(
        prim[0], cluster.stores[prim[0]], cluster.peers(prim[0]), clock=cluster.clock
    )

    assert cluster.stores[prim[0]].has(MailboxId(R6_MAILBOX), record.envelope_id)


async def test_resync_survives_a_down_peer():
    cluster = FakeCluster()
    prim = primaries(MailboxId(R6_MAILBOX))
    record = make_record(mailbox=R6_MAILBOX, body=b"from-the-survivor")
    for p in prim:
        await cluster.directory().gateway(p).store(record)

    cluster.restart(prim[0])
    cluster.kill(prim[1])  # one holder down; prim[2] still answers

    await resync(
        prim[0], cluster.stores[prim[0]], cluster.peers(prim[0]), clock=cluster.clock
    )

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


# -- Durability across repair: the two bugs these pin -----------------------
#
# Both are about the `hint` field surviving where it should not. A hint means "I
# am holding this on behalf of someone else, and I should hand it back". A
# *primary* must never hold its own mailbox's record under a hint, because the
# handoff loop would then hand the record away and forget it. `handoff_round`
# already clears the hint on the copy it sends, and says why in its docstring;
# the resync path (`share_for` -> `absorb`) had the same hazard and did not.

LOSS_MAILBOX = "mb-durability"


async def test_resync_does_not_leave_a_primary_standing_in_for_itself():
    """A record pulled by a primary is its own, so it arrives with no hint.

    `share_for` deliberately includes stand-in copies, because a stand-in is
    often the only copy a restarted primary can get. It must strip the hint
    while doing so: the destination is a primary of that mailbox, so the record
    is the destination's own, and a hint pointing at the destination would make
    it stand in for itself.
    """
    cluster = FakeCluster()
    mailbox = MailboxId(LOSS_MAILBOX)
    primary = primaries(mailbox)[0]
    standin = stand_ins(mailbox)[0]

    # A send that happened while `primary` was down: the stand-in holds it for it.
    record = make_record(mailbox=LOSS_MAILBOX, body=b"hinted", hint=str(primary))
    cluster.stores[standin].absorb([record], [])

    # Only the stand-in can answer, so its hinted copy is what lands.
    cluster.restart(primary)
    for other in primaries(mailbox)[1:]:
        cluster.kill(other)

    await resync(
        primary, cluster.stores[primary], cluster.peers(primary), clock=cluster.clock
    )

    held = cluster.stores[primary].fetch(mailbox)
    assert [r.envelope_id for r in held] == [record.envelope_id]
    assert held[0].hint is None, "a primary must not hold its own record under a hint"
    assert primary not in cluster.stores[primary].hinted_primaries()


async def test_handoff_never_hands_a_record_to_this_relay_itself():
    """Handing a record to yourself and then forgetting it destroys it.

    `forget_record` is only safe after a *different* relay has confirmed it
    holds the record. A self-handoff confirms nothing: the absorb and the forget
    hit the same store and the same `envelope_id`, so the forget deletes exactly
    what the absorb stored.
    """
    cluster = FakeCluster()
    mailbox = MailboxId(LOSS_MAILBOX)
    primary = primaries(mailbox)[0]

    # However it got here, this relay holds its own mailbox's record under a
    # hint naming itself. It must not act on that hint.
    record = make_record(mailbox=LOSS_MAILBOX, body=b"self-hinted", hint=str(primary))
    cluster.stores[primary].absorb([record], [])

    await handoff_round(
        primary, cluster.stores[primary], cluster.peers(primary), clock=cluster.clock
    )

    assert cluster.stores[primary].has(mailbox, record.envelope_id), (
        "the relay handed its only copy to itself and forgot it"
    )


async def test_two_failures_cannot_lose_a_confirmed_message_across_repair():
    """The guarantee phase 1 is graded on, driven through a full repair cycle.

    The sequence is the demonstration's worst case, in order: a message is
    confirmed on three relays while one primary is down, then the two other
    primaries are wiped (the two simultaneous failures), then the down primary
    restarts and the cluster repairs itself. The stand-in's hinted copy is the
    only survivor at that point, so every step after it has to preserve it.
    """
    cluster = FakeCluster()
    mailbox = MailboxId(LOSS_MAILBOX)
    prim = primaries(mailbox)
    standin = stand_ins(mailbox)[0]
    record = make_record(mailbox=LOSS_MAILBOX, body=b"confirmed-and-must-survive")

    # Confirmed on three relays: two primaries plus a stand-in covering prim[0].
    for holder in prim[1:]:
        cluster.stores[holder].absorb([record], [])
    cluster.stores[standin].absorb(
        [make_record(mailbox=LOSS_MAILBOX, body=record.envelope, hint=str(prim[0]))], []
    )

    # The two simultaneous failures: both other primaries lose everything.
    for holder in prim[1:]:
        cluster.restart(holder)
        cluster.kill(holder)

    # prim[0] comes back and the cluster repairs: resync, then handoff rounds.
    cluster.restart(prim[0])
    await resync(
        prim[0], cluster.stores[prim[0]], cluster.peers(prim[0]), clock=cluster.clock
    )
    cluster.set_ready(prim[0])
    # Several rounds over both relays, because these are background loops that
    # run repeatedly and in no fixed order relative to each other. Asserting
    # after a single round in one order lets this pass for the wrong reason.
    for _ in range(3):
        for relay in (standin, prim[0]):
            await handoff_round(
                relay, cluster.stores[relay], cluster.peers(relay), clock=cluster.clock
            )

    holders = [
        name
        for name, store in cluster.stores.items()
        if store.has(mailbox, record.envelope_id)
    ]
    assert holders, "a confirmed message was lost by repair after two failures"
    assert prim[0] in holders, "the primary should end up holding its own record"
