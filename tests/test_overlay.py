"""C10: the reduced overlay view the browser shows."""

from __future__ import annotations

from linkchat.core.client.overlay import UNREACHABLE, overlay
from linkchat.core.constants import RELAY_COUNT
from linkchat.core.ports import RelayState
from linkchat.core.ring import preference_list, primaries
from linkchat.core.types import MailboxId

from .conftest import make_record
from .fakes.transport import FakeCluster

MAILBOX = MailboxId("mb-overlay")


async def test_every_relay_appears_with_its_counts() -> None:
    cluster = FakeCluster()
    record = make_record(mailbox=str(MAILBOX), body=b"one")
    for name in primaries(MAILBOX):
        await cluster.directory().gateway(name).store(record)

    views = await overlay(cluster.directory(), mailbox=MAILBOX)

    assert len(views) == RELAY_COUNT
    holding = {v.name: v for v in views if v.holds_mailbox}
    assert set(holding) == set(primaries(MAILBOX))
    assert all(v.records == 1 for v in holding.values())
    assert all(v.state == RelayState.READY.value for v in views)


async def test_the_view_is_in_preference_order_so_fallthrough_is_visible() -> None:
    cluster = FakeCluster()

    views = await overlay(cluster.directory(), mailbox=MAILBOX)

    assert [v.name for v in views] == list(preference_list(MAILBOX))
    assert [v.holds_mailbox for v in views] == [True, True, True, False, False]
    assert [v.rank for v in views] == [0, 1, 2, 3, 4]


async def test_a_killed_relay_shows_as_unreachable_not_an_error() -> None:
    cluster = FakeCluster()
    victim = primaries(MAILBOX)[0]
    cluster.kill(victim)

    views = {v.name: v for v in await overlay(cluster.directory(), mailbox=MAILBOX)}

    assert views[victim].state == UNREACHABLE
    assert not views[victim].reachable
    assert views[victim].holds_mailbox  # still one of its primaries, just absent
    # The others are unaffected: one relay failing must not blank the view.
    assert all(v.reachable for name, v in views.items() if name != victim)


async def test_a_syncing_relay_reports_syncing_rather_than_vanishing() -> None:
    cluster = FakeCluster()
    victim = primaries(MAILBOX)[1]
    cluster.restart(victim)  # empty store, SYNCING, but reachable

    views = {v.name: v for v in await overlay(cluster.directory(), mailbox=MAILBOX)}

    assert views[victim].state == RelayState.SYNCING.value
    assert views[victim].reachable


async def test_a_stand_in_copy_is_counted_as_hinted() -> None:
    cluster = FakeCluster()
    prim = primaries(MAILBOX)
    standin = preference_list(MAILBOX)[3]
    hinted = make_record(mailbox=str(MAILBOX), body=b"covered", hint=str(prim[0]))
    cluster.stores[standin].absorb([hinted], [])

    views = {v.name: v for v in await overlay(cluster.directory(), mailbox=MAILBOX)}

    assert views[standin].hinted_records == 1
    assert not views[standin].holds_mailbox  # a stand-in, not a primary


async def test_without_a_mailbox_nothing_is_marked_as_holding() -> None:
    cluster = FakeCluster()

    views = await overlay(cluster.directory())

    assert len(views) == RELAY_COUNT
    assert not any(v.holds_mailbox for v in views)
