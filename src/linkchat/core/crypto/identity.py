"""The client's user identity: UserId = blake2b(identity public key).

The identity is created once, on first start, and never rotated in
phase 1. `user_id_of` is also how a client checks that a bundle fetched from
a relay really belongs to the UserId it expected: the signature on the signed
prekey is not enough on its own, since a relay can sign a bundle of its own.
"""

from __future__ import annotations

import hashlib

from linkchat.core.client.history import History, IdentityConflict
from linkchat.core.crypto.suite import LinkChatX3DH, new_x3dh_state, restore_x3dh_state
from linkchat.core.types import UserId

IDENTITY_KEY_SIZE = 32
"""Ed25519 public key size in bytes."""


def user_id_of(identity_key: bytes) -> UserId:
    """The UserId for an identity public key.

    Deterministic: the same key always yields the same UserId, on every
    machine.

    Raises:
        ValueError: if the key is not 32 bytes. For our own key that is a bug;
            for a key in a bundle fetched from a relay it is bad input
            from the network, and the caller must treat the bundle as invalid.
    """
    if len(identity_key) != IDENTITY_KEY_SIZE:
        raise ValueError(
            f"an Ed25519 identity key is {IDENTITY_KEY_SIZE} bytes,"
            f" got {len(identity_key)}"
        )
    return UserId(hashlib.blake2b(identity_key, digest_size=32).hexdigest())


def load_or_create_identity(history: History) -> LinkChatX3DH:
    """The client's X3DH state: created and saved on first start, reloaded after.

    Called once when the client starts, before any conversation is opened.
    """
    loaded = history.load_identity()
    if loaded is None:
        state = new_x3dh_state()
        history.save_identity(user_id_of(state.bundle.identity_key), dict(state.json))
        return state

    stored_id, serialized = loaded
    state, _needs_publish = restore_x3dh_state(serialized)
    # The client republishes its bundle on every start anyway, so the
    # pre-stable "needs publishing" flag adds nothing here.
    if user_id_of(state.bundle.identity_key) != stored_id:
        raise IdentityConflict("the stored UserId does not match the stored key")
    return state
