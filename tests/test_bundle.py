"""Prekey bundles: encoding, the identity check, and the relays failing.

Red until `linkchat/core/crypto/bundle.py` is implemented. The tests assert
guarantees, not steps: how the relays are asked, and in what order, is yours.
"""

from __future__ import annotations

import asyncio
import time

import msgpack
import pytest
from x3dh import Bundle, KeyAgreementException

from linkchat.core.constants import RELAY_NAMES
from linkchat.core.crypto.bundle import (
    InvalidBundle,
    NoRelayAnswered,
    decode_bundle,
    encode_bundle,
    fetch_bundle,
    publish_bundle,
)
from linkchat.core.crypto.identity import user_id_of
from linkchat.core.crypto.suite import (
    LinkChatX3DH,
    accept_ratchet,
    new_x3dh_state,
    start_ratchet,
)
from linkchat.core.ports import BundleRejected
from linkchat.core.types import UserId

from .fakes.transport import FakeCluster


def identity() -> tuple[LinkChatX3DH, UserId]:
    state = new_x3dh_state()
    return state, user_id_of(state.bundle.identity_key)


def without_prekeys(bundle: Bundle) -> Bundle:
    return Bundle(
        bundle.identity_key, bundle.signed_pre_key, bundle.signed_pre_key_sig, frozenset()
    )


def raw(**fields: object) -> bytes:
    """A hand-built document, for the malformed cases."""
    packed: bytes = msgpack.packb(fields, use_bin_type=True)
    return packed


# -- encoding -----------------------------------------------------------------


def test_round_trip() -> None:
    bob, bob_id = identity()

    decoded = decode_bundle(encode_bundle(bob.bundle), bob_id)

    assert decoded == without_prekeys(bob.bundle)


def test_one_time_prekeys_are_never_published() -> None:
    bob, bob_id = identity()
    with_prekeys = Bundle(
        bob.bundle.identity_key,
        bob.bundle.signed_pre_key,
        bob.bundle.signed_pre_key_sig,
        frozenset({b"o" * 32}),
    )

    assert decode_bundle(encode_bundle(with_prekeys), bob_id).pre_keys == frozenset()


def test_a_bundle_of_another_identity_is_rejected() -> None:
    """The relay's trick: a valid, correctly signed bundle that is not Bob's."""
    _, bob_id = identity()
    mallory, _ = identity()

    with pytest.raises(InvalidBundle):
        decode_bundle(encode_bundle(mallory.bundle), bob_id)


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda b: b"not msgpack at all \xc1", id="garbage"),
        pytest.param(lambda b: msgpack.packb([1, 2, 3]), id="not-a-map"),
        pytest.param(lambda b: raw(v=1, ik=b["ik"], spk=b["spk"]), id="no-signature"),
        pytest.param(lambda b: raw(**{**b, "v": 2}), id="unknown-version"),
        pytest.param(lambda b: raw(**{**b, "v": True}), id="version-is-a-bool"),
        pytest.param(lambda b: raw(**{**b, "spk": b["spk"][:31]}), id="short-prekey"),
        pytest.param(lambda b: raw(**{**b, "sig": b["sig"][:63]}), id="short-signature"),
        pytest.param(lambda b: raw(**{**b, "ik": "a string"}), id="key-not-bytes"),
    ],
)
def test_malformed_bundles_are_rejected(mutate) -> None:
    bob, bob_id = identity()
    fields = {
        "v": 1,
        "ik": bob.bundle.identity_key,
        "spk": bob.bundle.signed_pre_key,
        "sig": bob.bundle.signed_pre_key_sig,
    }

    with pytest.raises(InvalidBundle):
        decode_bundle(mutate(fields), bob_id)


async def test_a_tampered_signature_fails_key_agreement() -> None:
    """Not ours to check: the library verifies it when the bundle is used."""
    bob, _ = identity()
    alice, _ = identity()
    sig = bytearray(bob.bundle.signed_pre_key_sig)
    sig[0] ^= 0x01
    forged = Bundle(
        bob.bundle.identity_key, bob.bundle.signed_pre_key, bytes(sig), frozenset()
    )

    with pytest.raises(KeyAgreementException):
        await alice.get_shared_secret_active(forged, require_pre_key=False)


# -- on the relays --------------------------------------------------------------


async def test_publish_reaches_every_relay() -> None:
    cluster = FakeCluster()
    bob, bob_id = identity()

    confirmed = await publish_bundle(
        cluster.directory(), bob_id, encode_bundle(bob.bundle)
    )

    assert confirmed == len(RELAY_NAMES)
    assert all(bob_id in cluster.prekeys[name] for name in RELAY_NAMES)


async def test_two_relays_down_while_publishing() -> None:
    cluster = FakeCluster()
    bob, bob_id = identity()
    cluster.kill(RELAY_NAMES[0], RELAY_NAMES[3])

    confirmed = await publish_bundle(
        cluster.directory(), bob_id, encode_bundle(bob.bundle)
    )

    assert confirmed == len(RELAY_NAMES) - 2
    fetched = await fetch_bundle(cluster.directory(), bob_id)
    assert fetched == without_prekeys(bob.bundle)


async def test_relays_that_lost_it_or_are_down_are_skipped() -> None:
    """Two relays restarted empty, two are down: the fifth still answers."""
    cluster = FakeCluster()
    bob, bob_id = identity()
    await publish_bundle(cluster.directory(), bob_id, encode_bundle(bob.bundle))
    for name in RELAY_NAMES[:2]:
        cluster.restart(name)  # comes back with no bundles at all
        cluster.set_ready(name)
    cluster.kill(RELAY_NAMES[2], RELAY_NAMES[3])

    fetched = await fetch_bundle(cluster.directory(), bob_id)

    assert fetched == without_prekeys(bob.bundle)


async def test_forged_bundles_are_skipped_whatever_the_order() -> None:
    """Four relays serve Mallory's bundle under Bob's id; only one is honest."""
    cluster = FakeCluster()
    bob, bob_id = identity()
    mallory, _ = identity()
    honest = RELAY_NAMES[-1]
    for name in RELAY_NAMES:
        source = bob if name == honest else mallory
        cluster.prekeys[name][bob_id] = encode_bundle(source.bundle)

    fetched = await fetch_bundle(cluster.directory(), bob_id)

    assert fetched is not None
    assert fetched.identity_key == bob.bundle.identity_key


async def test_garbage_on_a_relay_is_skipped() -> None:
    cluster = FakeCluster()
    bob, bob_id = identity()
    await publish_bundle(cluster.directory(), bob_id, encode_bundle(bob.bundle))
    for name in RELAY_NAMES[:-1]:
        cluster.prekeys[name][bob_id] = b"\x00garbage"

    assert await fetch_bundle(cluster.directory(), bob_id) == without_prekeys(bob.bundle)


async def test_nobody_has_it() -> None:
    _, bob_id = identity()

    assert await fetch_bundle(FakeCluster().directory(), bob_id) is None


async def test_every_relay_down_says_so_rather_than_nobody_has_it() -> None:
    """No answer is not the same as "no bundle": the caller must be able to tell."""
    cluster = FakeCluster()
    bob, bob_id = identity()
    cluster.kill(*RELAY_NAMES)

    assert (
        await publish_bundle(cluster.directory(), bob_id, encode_bundle(bob.bundle)) == 0
    )
    with pytest.raises(NoRelayAnswered):
        await fetch_bundle(cluster.directory(), bob_id)


async def test_one_relay_answering_is_enough_to_say_nobody_has_it() -> None:
    _, bob_id = identity()
    cluster = FakeCluster()
    cluster.kill(*RELAY_NAMES[1:])

    assert await fetch_bundle(cluster.directory(), bob_id) is None


async def test_a_relay_refuses_someone_elses_bundle_and_keeps_the_owners() -> None:
    """Mallory cannot overwrite Bob's keys: the relay checks the binding first."""
    cluster = FakeCluster()
    bob, bob_id = identity()
    mallory, _ = identity()
    await publish_bundle(cluster.directory(), bob_id, encode_bundle(bob.bundle))
    gateway = cluster.directory().gateway(RELAY_NAMES[0])

    with pytest.raises(BundleRejected):
        await gateway.publish_prekeys(bob_id, encode_bundle(mallory.bundle))
    with pytest.raises(BundleRejected):
        await gateway.publish_prekeys(bob_id, b"\x00garbage")

    assert await gateway.get_prekeys(bob_id) == encode_bundle(bob.bundle)


async def test_publishing_a_forged_bundle_confirms_nowhere() -> None:
    cluster = FakeCluster()
    _, bob_id = identity()
    mallory, _ = identity()

    forged = encode_bundle(mallory.bundle)
    assert await publish_bundle(cluster.directory(), bob_id, forged) == 0
    assert await fetch_bundle(cluster.directory(), bob_id) is None


# -- end to end -----------------------------------------------------------------


async def test_alice_opens_a_session_from_a_fetched_bundle() -> None:
    """Bob publishes and leaves; Alice, knowing only his UserId, reaches him."""
    cluster = FakeCluster()
    bob, bob_id = identity()
    await publish_bundle(cluster.directory(), bob_id, encode_bundle(bob.bundle))

    alice, _ = identity()
    bundle = await fetch_bundle(cluster.directory(), bob_id)
    assert bundle is not None
    sk_a, ad_a, header = await alice.get_shared_secret_active(
        bundle, require_pre_key=False
    )
    _, first = await start_ratchet(sk_a, bundle.signed_pre_key, b"hola", ad_a)

    sk_b, ad_b, spk = await bob.get_shared_secret_passive(header, require_pre_key=False)
    _, plaintext = await accept_ratchet(sk_b, spk.priv, first, ad_b)

    assert plaintext == b"hola"


# -- latency --------------------------------------------------------------------


class _HangingGateway:
    """A relay that never answers, like one Ray is still restarting."""

    def __init__(self, inner, hangs: bool) -> None:
        self._inner, self._hangs = inner, hangs

    async def get_prekeys(self, user_id):
        if self._hangs:
            await asyncio.sleep(60)
        return await self._inner.get_prekeys(user_id)


class _HangingDirectory:
    relay_names = RELAY_NAMES

    def __init__(self, inner, hanging: set[str]) -> None:
        self._inner, self._hanging = inner, hanging

    def gateway(self, name):
        return _HangingGateway(self._inner.gateway(name), name in self._hanging)


async def test_hanging_relays_do_not_add_up(monkeypatch) -> None:
    """Four relays hang: the fifth answers at once, without waiting four timeouts."""
    from linkchat.core.crypto import bundle as bundle_module

    monkeypatch.setattr(bundle_module, "BUNDLE_TIMEOUT_S", 1.0)
    cluster = FakeCluster()
    bob, bob_id = identity()
    await publish_bundle(cluster.directory(), bob_id, encode_bundle(bob.bundle))
    directory = _HangingDirectory(cluster.directory(), set(RELAY_NAMES[:4]))

    started = time.monotonic()
    fetched = await fetch_bundle(directory, bob_id)

    assert fetched == without_prekeys(bob.bundle)
    assert time.monotonic() - started < 0.5  # sequential would take about 4 s


async def test_every_relay_hanging_is_no_answer(monkeypatch) -> None:
    """Relays that time out did not answer, exactly like relays that are down."""
    from linkchat.core.crypto import bundle as bundle_module

    monkeypatch.setattr(bundle_module, "BUNDLE_TIMEOUT_S", 0.1)
    _, bob_id = identity()
    directory = _HangingDirectory(FakeCluster().directory(), set(RELAY_NAMES))

    with pytest.raises(NoRelayAnswered):
        await fetch_bundle(directory, bob_id)
