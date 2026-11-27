"""Tests for the fake transport itself (C2). These PASS: they prove the harness.

If these ever fail, the harness is wrong and every R5-R7 test built on it is
suspect, so they are the foundation the rest stands on.
"""

from __future__ import annotations

import pytest

from linkchat.core.ports import (
    CredentialRejected,
    RelayGone,
    RelayState,
    RelaySyncing,
    RelayUnreachable,
)
from linkchat.core.ring import primaries
from linkchat.core.types import Credential, MailboxId

from .conftest import ack_credential, inbox, make_reader, make_record
from .fakes.transport import FakeCluster

CRED = Credential(b"cred")


def _primary(mailbox: str):
    return primaries(MailboxId(mailbox))[0]


async def test_store_fetch_ack_roundtrip():
    cluster = FakeCluster()
    reader = make_reader()
    mailbox = inbox(reader)
    gw = cluster.directory().gateway(_primary(mailbox))
    record = make_record(mailbox=mailbox, body=b"hi")

    await gw.store(record)
    held = await gw.fetch(mailbox, CRED)
    assert [r.envelope_id for r in held] == [record.envelope_id]

    eids = [record.envelope_id]
    await gw.ack(mailbox, ack_credential(reader, mailbox, eids), eids)
    assert await gw.fetch(mailbox, CRED) == []


async def test_an_unsigned_ack_is_refused_and_deletes_nothing():
    """Knowing the mailbox id is not enough to empty it (ADR-0007)."""
    cluster = FakeCluster()
    mailbox = inbox(make_reader())
    gw = cluster.directory().gateway(_primary(mailbox))
    record = make_record(mailbox=mailbox, body=b"keep me")
    await gw.store(record)

    with pytest.raises(CredentialRejected):
        await gw.ack(mailbox, CRED, [record.envelope_id])

    assert [r.envelope_id for r in await gw.fetch(mailbox, CRED)] == [record.envelope_id]


async def test_killed_relay_is_unreachable():
    cluster = FakeCluster()
    name = _primary("mb")
    cluster.kill(name)
    with pytest.raises(RelayUnreachable):
        await cluster.directory().gateway(name).store(make_record(mailbox="mb"))


async def test_gone_relay_raises_relaygone():
    cluster = FakeCluster()
    name = _primary("mb")
    cluster.faults.gone.add(name)
    with pytest.raises(RelayGone):
        await cluster.directory().gateway(name).status()


async def test_restarted_relay_is_syncing_but_status_works():
    cluster = FakeCluster()
    mailbox = "mb-sync"
    name = _primary(mailbox)
    gw = cluster.directory().gateway(name)
    await gw.store(make_record(mailbox=mailbox))

    cluster.restart(name)
    # Client calls refuse while syncing...
    with pytest.raises(RelaySyncing):
        await gw.fetch(MailboxId(mailbox), CRED)
    # ...but status is reachable and reports SYNCING and an empty store.
    status = await gw.status()
    assert status.state is RelayState.SYNCING
    assert status.records == 0


async def test_drop_next_then_recovers():
    cluster = FakeCluster()
    name = _primary("mb-drop")
    cluster.faults.drop_next[name] = 1
    gw = cluster.directory().gateway(name)
    with pytest.raises(RelayUnreachable):
        await gw.store(make_record(mailbox="mb-drop", body=b"a"))
    # The next call goes through.
    await gw.store(make_record(mailbox="mb-drop", body=b"b"))


async def test_duplicate_fault_is_idempotent():
    cluster = FakeCluster()
    mailbox = "mb-dup"
    name = _primary(mailbox)
    cluster.faults.duplicate.add(name)
    gw = cluster.directory().gateway(name)
    record = make_record(mailbox=mailbox, body=b"once")
    await gw.store(record)
    held = await gw.fetch(MailboxId(mailbox), CRED)
    assert len(held) == 1  # applied twice, stored once


async def test_pull_and_handoff_between_peers():
    cluster = FakeCluster()
    mailbox = "mb-peer"
    prim = primaries(MailboxId(mailbox))
    holder, other = prim[0], prim[1]

    record = make_record(mailbox=mailbox, body=b"shared")
    await cluster.directory().gateway(holder).store(record)

    # `other` pulls what it should hold; the record's mailbox has it as a primary.
    pulled = await cluster.peers(other).pull(holder, other)
    assert record.envelope_id in {r.envelope_id for r in pulled.records}

    # Hand it to `other` and it holds it.
    await cluster.peers(holder).handoff(other, [record], [])
    assert cluster.stores[other].has(MailboxId(mailbox), record.envelope_id)
