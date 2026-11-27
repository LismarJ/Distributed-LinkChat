"""The prekey bundle as it travels through relays.

A relay stores an opaque blob. It must never be able to substitute it: the
signature on the signed prekey proves the bundle was made by the holder of
the identity key, but that is not enough. A relay can generate its own
identity key and sign its own prekey perfectly. What ties a bundle to a
person is the UserId, which the recipient learned through the invitation,
outside the relays. `decode_bundle` enforces that link.

Format: msgpack with four fields, versioned.
    {"v": 1, "ik": bytes32, "spk": bytes32, "sig": bytes64}
No one-time prekeys in phase 1.
"""

from __future__ import annotations

import asyncio
import logging

import msgpack
from x3dh import Bundle

from linkchat.core.crypto.identity import user_id_of
from linkchat.core.ports import BundleRejected, RelayDirectory, RelayError
from linkchat.core.types import RelayName, UserId

log = logging.getLogger(__name__)

BUNDLE_VERSION = 1
KEY_SIZE = 32
"""Identity key and signed prekey, both 32 bytes."""
SIGNATURE_SIZE = 64

BUNDLE_TIMEOUT_S = 5.0
"""Per-relay timeout for publishing and for fetching a bundle."""


class InvalidBundle(ValueError):
    """A bundle is not something this build can trust."""


class NoRelayAnswered(RelayError):
    """Not one relay answered a bundle fetch: down, unreachable or timed out.

    Distinct from "every relay answered and none has it", which `fetch_bundle`
    reports as None. The first means "try again soon"; the second means the
    peer has never been online, and retrying will not help until they are.
    """


def encode_bundle(bundle: Bundle) -> bytes:
    """Serialize a bundle for `publish_prekeys`. Raises on bad lengths."""
    if len(bundle.identity_key) != KEY_SIZE:
        raise InvalidBundle(
            f"identity key is {len(bundle.identity_key)} bytes, expected {KEY_SIZE}"
        )
    if len(bundle.signed_pre_key) != KEY_SIZE:
        raise InvalidBundle(
            f"signed prekey is {len(bundle.signed_pre_key)} bytes, expected {KEY_SIZE}"
        )
    if len(bundle.signed_pre_key_sig) != SIGNATURE_SIZE:
        raise InvalidBundle(
            f"signature is {len(bundle.signed_pre_key_sig)} bytes,"
            f" expected {SIGNATURE_SIZE}"
        )

    document = {
        "v": BUNDLE_VERSION,
        "ik": bundle.identity_key,
        "spk": bundle.signed_pre_key,
        "sig": bundle.signed_pre_key_sig,
    }
    packed: bytes = msgpack.packb(document, use_bin_type=True)
    return packed


def decode_bundle(data: bytes, expected: UserId) -> Bundle:
    """Parse a bundle and check that it belongs to `expected`.

    Raises:
        InvalidBundle: on malformed bytes, an unknown version, bad key
            lengths, or an identity key whose hash is not `expected`.
    """
    try:
        document = msgpack.unpackb(data, raw=False, strict_map_key=True)
    except Exception as exc:
        raise InvalidBundle(f"not valid msgpack: {exc}") from exc

    if not isinstance(document, dict):
        raise InvalidBundle(f"expected a map, got {type(document).__name__}")

    version = document.get("v")
    # `type(...) is int`, not `==`: in Python True == 1, and a bool is no version.
    if type(version) is not int or version != BUNDLE_VERSION:
        raise InvalidBundle(
            f"bundle version {version!r}, this build reads {BUNDLE_VERSION}"
        )

    identity_key = document.get("ik")
    signed_pre_key = document.get("spk")
    signature = document.get("sig")
    if not isinstance(identity_key, bytes) or len(identity_key) != KEY_SIZE:
        raise InvalidBundle("identity key is missing or not 32 bytes")
    if not isinstance(signed_pre_key, bytes) or len(signed_pre_key) != KEY_SIZE:
        raise InvalidBundle("signed prekey is missing or not 32 bytes")
    if not isinstance(signature, bytes) or len(signature) != SIGNATURE_SIZE:
        raise InvalidBundle("signature is missing or not 64 bytes")

    # The control that makes the relay story safe: the identity key must hash
    # to the UserId Alice already knows from the invitation. A relay can sign a
    # prekey of its own perfectly well, but it cannot make its key hash to
    # Bob's UserId.
    if user_id_of(identity_key) != expected:
        raise InvalidBundle(f"bundle identity key does not belong to {expected}")

    return Bundle(
        identity_key=identity_key,
        signed_pre_key=signed_pre_key,
        signed_pre_key_sig=signature,
        pre_keys=frozenset(),
    )


async def publish_bundle(
    directory: RelayDirectory, user_id: UserId, encoded: bytes
) -> int:
    """Publish to every relay at once. Returns how many confirmed.

    All five on purpose: relays keep bundles in memory and do not replicate
    them, so one restarted relay is one relay that has forgotten. Publishing
    everywhere means any single relay that answers is enough to read.
    """

    async def one(name: RelayName) -> bool:
        gateway = directory.gateway(name)
        try:
            await asyncio.wait_for(
                gateway.publish_prekeys(user_id, encoded), BUNDLE_TIMEOUT_S
            )
        except (RelayError, TimeoutError) as exc:
            log.info("publish to %s failed: %r", name, exc)
            return False
        return True

    # return_exceptions: an unexpected error on one relay counts as a miss,
    # it does not cancel the publish on the other four.
    results = await asyncio.gather(
        *(one(name) for name in directory.relay_names), return_exceptions=True
    )
    for name, result in zip(directory.relay_names, results, strict=True):
        if isinstance(result, BaseException):
            log.warning("publish to %s raised: %r", name, result)
    confirmed = sum(1 for result in results if result is True)
    log.debug("bundle for %s confirmed by %d relays", user_id, confirmed)
    return confirmed


def check_published_bundle(user_id: UserId, raw: bytes) -> None:
    """What a relay runs before storing a bundle. Raises `BundleRejected`.

    The same binding check a reader applies (`decode_bundle` with `expected`),
    moved forward to the relay so that a forged or garbage bundle is never
    stored in the first place. A relay cannot verify more than that, and does
    not need to: the signature on the signed prekey is checked by the reader.
    """
    try:
        decode_bundle(raw, expected=user_id)
    except InvalidBundle as exc:
        raise BundleRejected(f"bundle refused for {user_id}: {exc}") from exc


async def fetch_bundle(directory: RelayDirectory, user_id: UserId) -> Bundle | None:
    """The first bundle that passes `decode_bundle`, or None if nobody has one.

    Asks every relay at once and keeps the first valid answer, so relays that
    hang (a relay being restarted) cost at most one timeout, not one each.
    Skips relays that are down, that have nothing, and that serve a bundle for
    the wrong identity. Logs the skips; does not stop on them.

    Returns None only when at least one relay answered. When none did, there is
    no telling whether the peer has a bundle, so it raises instead.

    Raises:
        NoRelayAnswered: every relay was down, unreachable or timed out.
    """

    async def one(name: RelayName) -> tuple[bool, Bundle | None]:
        """(whether the relay answered, the valid bundle it served if any)."""
        gateway = directory.gateway(name)
        try:
            raw = await asyncio.wait_for(gateway.get_prekeys(user_id), BUNDLE_TIMEOUT_S)
        except (RelayError, TimeoutError) as exc:
            log.info("fetch from %s failed: %r", name, exc)
            return False, None
        if raw is None:
            return True, None
        try:
            return True, decode_bundle(raw, expected=user_id)
        except InvalidBundle as exc:
            log.warning(
                "relay %s served an invalid bundle for %s: %s", name, user_id, exc
            )
            return True, None

    tasks = [asyncio.create_task(one(name)) for name in directory.relay_names]
    answered = 0
    try:
        for finished in asyncio.as_completed(tasks):
            try:
                did_answer, bundle = await finished
            except Exception as exc:  # one relay misbehaving is never fatal
                log.warning("fetch of %s's bundle raised: %r", user_id, exc)
                continue
            if bundle is not None:
                return bundle
            answered += did_answer
        if answered == 0:
            raise NoRelayAnswered(f"no relay answered for {user_id}'s bundle")
        return None
    finally:
        for task in tasks:
            task.cancel()  # the answers we no longer need
