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

import pytest

ray = pytest.importorskip("ray")

from linkchat.core.constants import RELAY_COUNT, relay_name
from linkchat.core.ports import RelayError, RelayState
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


async def _wait_ready(gateway, tries: int = 50) -> None:
    """Poll `status` (allowed while SYNCING) until the relay reports READY."""
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

    record = make_record(mailbox=str(mailbox), body=b"hello", expires_at=2_000_000.0)
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
        mailbox="mailbox-handoff", body=b"via-standin", hint=str(primary)
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
