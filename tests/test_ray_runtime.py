"""Integration tests for the Ray runtime (R3/R4). Pre-spike, Ray-only.

Skipped where Ray is not installed (the dev machine runs Python 3.14, which Ray
does not support). On CI (Python 3.12 with Ray) they exercise the relay actor and
the gateway against a local Ray instance.

These are deliberately where a wrong assumption about Ray surfaces first: a
failure here is a finding for the spike (gate G1), not automatically a bug in the
intent. They have never run yet.
"""

from __future__ import annotations

import asyncio
import time

import pytest

ray = pytest.importorskip("ray")

from linkchat.core.constants import RELAY_COUNT, relay_name
from linkchat.core.ports import RelayError, RelayState
from linkchat.core.ring import primaries, stand_ins
from linkchat.core.types import Credential, MailboxId
from linkchat.ray_runtime.bootstrap import NAMESPACE, bootstrap
from linkchat.ray_runtime.gateway import RayRelayDirectory

from .conftest import make_record

CREDENTIAL = Credential(b"test-credential")  # ADR-0007 pending; carried, not checked


@pytest.fixture(scope="module")
def ray_cluster():
    """A single local node offering every relay_slot, enough for all R relays."""
    ray.init(
        namespace=NAMESPACE,
        num_cpus=RELAY_COUNT,
        resources={"relay_slot": RELAY_COUNT},
        include_dashboard=False,
        ignore_reinit_error=True,
    )
    bootstrap()
    yield
    ray.shutdown()


async def _wait_ready(gateway, tries: int = 400) -> None:
    """Poll `status` (allowed while SYNCING) until the relay reports READY.

    The budget is 20 s rather than the 2.5 s it started with, because READY
    now requires a sync in which at least one peer answered. At bootstrap an
    actor can exist before its peers do, so the first attempts find nothing
    to talk to and are retried on `RESYNC_RETRY_INTERVAL_S`.
    """
    for _ in range(tries):
        status = await gateway.status()
        if status.state is RelayState.READY:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("relay never reached READY")


async def test_bootstrap_is_idempotent(ray_cluster):
    """Running the bootstrap again returns the same actors, creates none."""
    first = bootstrap()
    second = bootstrap()
    assert set(first) == set(second)
    for name in first:
        assert first[name]._actor_id == second[name]._actor_id


async def test_store_fetch_ack_roundtrip(ray_cluster):
    """A record stored through the gateway is fetched back, then acknowledged."""
    directory = RayRelayDirectory()
    mailbox = MailboxId("mailbox-roundtrip")
    primary = relay_name(0)
    gateway = directory.gateway(primary)
    await _wait_ready(gateway)

    # Real cluster, real SystemClock: expiry is wall-clock, so set it in the
    # future rather than the FakeClock-era default make_record carries.
    record = make_record(
        mailbox=str(mailbox), body=b"hello", expires_at=time.time() + 3600
    )
    await gateway.store(record)

    held = await gateway.fetch(mailbox, CREDENTIAL)
    assert [r.envelope_id for r in held] == [record.envelope_id]

    await gateway.ack(mailbox, CREDENTIAL, [record.envelope_id])
    assert await gateway.fetch(mailbox, CREDENTIAL) == []


async def test_pull_and_handoff_between_relays(ray_cluster):
    """A record handed to one relay can be pulled by the primary it is for."""
    directory = RayRelayDirectory()
    standin = relay_name(1)
    primary = relay_name(2)
    peers = directory.peers(primary)

    # A stand-in holds a record hinted for `primary`.
    record = make_record(
        mailbox="mailbox-handoff",
        body=b"via-standin",
        hint=str(primary),
        expires_at=time.time() + 3600,
    )
    await _wait_ready(directory.gateway(standin))
    await peers.handoff(standin, [record], [])

    pulled = await peers.pull(standin, primary)
    assert record.envelope_id in {r.envelope_id for r in pulled.records}


async def test_status_counts_and_ready(ray_cluster):
    directory = RayRelayDirectory()
    gateway = directory.gateway(relay_name(3))
    await _wait_ready(gateway)
    status = await gateway.status()
    assert status.name == relay_name(3)
    assert status.state is RelayState.READY
    assert status.records >= 0


async def test_restarted_relay_refills_from_peers(ray_cluster):
    """R6 end to end: Ray restarts a relay and it comes back holding the record.

    This is the mechanism `fault-tolerance.md` case 3 describes, against the real
    thing rather than the fake transport: the restarted actor runs its
    constructor with an empty store, reports SYNCING, pulls its share from the
    peers that still hold the record, and only then serves.
    """
    directory = RayRelayDirectory()
    mailbox = MailboxId("mailbox-resync")
    holders = primaries(mailbox)
    record = make_record(
        mailbox=str(mailbox), body=b"survives-a-restart", expires_at=time.time() + 3600
    )
    for name in holders:
        gateway = directory.gateway(name)
        await _wait_ready(gateway)
        await gateway.store(record)

    victim = holders[0]
    ray.kill(ray.get_actor(victim, namespace=NAMESPACE), no_restart=False)

    # Ray reruns the constructor; the new actor starts empty and syncs. Poll
    # through the gateway, which re-resolves the name after the restart.
    gateway = directory.gateway(victim)
    for _ in range(200):
        try:
            held = await gateway.fetch(mailbox, CREDENTIAL)
        except RelayError:
            # Mid-restart: unreachable, or SYNCING and refusing client calls.
            await asyncio.sleep(0.1)
            continue
        if record.envelope_id in {r.envelope_id for r in held}:
            break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError(f"{victim} never refilled the record after its restart")


async def test_handoff_loop_returns_a_hinted_record_to_its_primary(ray_cluster):
    """R7 end to end: a stand-in's background loop gives the record back.

    The stand-in is handed a record hinted for a primary that is up, so its next
    handoff round should deliver it and drop the local copy without anyone asking.
    """
    directory = RayRelayDirectory()
    mailbox = MailboxId("mailbox-handoff-loop")
    primary = primaries(mailbox)[0]
    standin = stand_ins(mailbox)[0]
    record = make_record(
        mailbox=str(mailbox),
        body=b"goes-home-by-itself",
        hint=str(primary),
        expires_at=time.time() + 3600,
    )

    await _wait_ready(directory.gateway(primary))
    await _wait_ready(directory.gateway(standin))
    # Plant the hinted copy on the stand-in, as a three-copy send would have.
    await directory.peers(primary).handoff(standin, [record], [])

    for _ in range(200):
        at_primary = await directory.gateway(primary).fetch(mailbox, CREDENTIAL)
        at_standin = await directory.gateway(standin).fetch(mailbox, CREDENTIAL)
        moved = record.envelope_id in {r.envelope_id for r in at_primary}
        dropped = record.envelope_id not in {r.envelope_id for r in at_standin}
        if moved and dropped:
            break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("the hinted record never reached its primary")


async def test_dead_relay_becomes_a_relay_error(ray_cluster):
    """Killing a relay makes the next call raise one of ours, not a Ray type."""
    directory = RayRelayDirectory()
    name = relay_name(4)
    gateway = directory.gateway(name)
    await _wait_ready(gateway)

    handle = ray.get_actor(name, namespace=NAMESPACE)
    ray.kill(handle, no_restart=True)

    with pytest.raises(RelayError):
        # Retry a few times: the death may take a moment to observe.
        for _ in range(20):
            await gateway.status()
            await asyncio.sleep(0.05)
