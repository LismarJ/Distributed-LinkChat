"""LinkChat's cryptographic parameters (task C6).

Only the choices the libraries ask us to make: the subclasses they leave
abstract, and thin wrappers that always pass the same parameters, so no caller
can pick a different KDF by accident. No sessions, bundles or storage here.

The choices follow Signal's recommended algorithms:
https://signal.org/docs/specifications/doubleratchet/#recommended-cryptographic-algorithms
"""

from __future__ import annotations

from doubleratchet import DoubleRatchet, EncryptedMessage, Header, JSONObject
from doubleratchet.recommended import (
    HashFunction,
    aead_aes_hmac,
    diffie_hellman_ratchet_curve25519,
    kdf_hkdf,
    kdf_separate_hmacs,
)
from x3dh import BaseState, IdentityKeyFormat
from x3dh import HashFunction as X3DHHashFunction

# -- X3DH ---------------------------------------------------------------------

X3DH_IDENTITY_KEY_FORMAT = IdentityKeyFormat.ED_25519
"""Ed25519, so the identity key can sign the signed prekey."""

X3DH_HASH_FUNCTION = X3DHHashFunction.SHA_256
X3DH_INFO = b"LinkChat X3DH v1"


class LinkChatX3DH(BaseState):
    """`BaseState`, not `State`: we publish the bundle ourselves, asynchronously."""

    @staticmethod
    def _encode_public_key(key_format: IdentityKeyFormat, pub: bytes) -> bytes:
        # Unique per (format, key), as the X3DH associated data requires.
        return key_format.value.encode("ascii") + b"\x00" + pub


def new_x3dh_state() -> LinkChatX3DH:
    """A fresh identity key and signed prekey. No one-time prekeys (phase 1)."""
    return LinkChatX3DH.create(
        identity_key_format=X3DH_IDENTITY_KEY_FORMAT,
        hash_function=X3DH_HASH_FUNCTION,
        info=X3DH_INFO,
    )


# -- Double Ratchet building blocks -------------------------------------------


class _RootKDF(kdf_hkdf.KDF):
    """Root chain: HKDF."""

    @staticmethod
    def _get_hash_function() -> HashFunction:
        return HashFunction.SHA_256

    @staticmethod
    def _get_info() -> bytes:
        return b"LinkChat root chain"


class _MessageKDF(kdf_separate_hmacs.KDF):
    """Sending and receiving chains: one HMAC per constant byte."""

    @staticmethod
    def _get_hash_function() -> HashFunction:
        return HashFunction.SHA_256


class _AEAD(aead_aes_hmac.AEAD):
    """AES-256-CBC with HMAC-SHA-256."""

    @staticmethod
    def _get_hash_function() -> HashFunction:
        return HashFunction.SHA_256

    @staticmethod
    def _get_info() -> bytes:
        return b"LinkChat message keys"


MESSAGE_CHAIN_CONSTANT = b"\x01\x02"
"""HMAC with 0x01 gives the message key, with 0x02 the next chain key."""

MAX_SKIPPED_MESSAGE_KEYS = 1000
"""Keys kept for messages that arrive out of order.

The DoS threshold is the same number: relays return a backlog in no
particular order, so the first message decrypted may be far ahead.
"""


class LinkChatDoubleRatchet(DoubleRatchet):
    """Authenticates the ratchet header together with the X3DH associated data."""

    @staticmethod
    def _build_associated_data(associated_data: bytes, header: Header) -> bytes:
        # Length prefix, so the boundary between the two parts is unambiguous.
        return b"".join(
            [
                len(associated_data).to_bytes(4, "big"),
                associated_data,
                header.ratchet_pub,
                header.previous_sending_chain_length.to_bytes(4, "big"),
                header.sending_chain_length.to_bytes(4, "big"),
            ]
        )


# -- Wrappers: the only way the rest of the code touches the ratchet ----------


async def start_ratchet(
    shared_secret: bytes,
    recipient_ratchet_pub: bytes,
    message: bytes,
    associated_data: bytes,
) -> tuple[LinkChatDoubleRatchet, EncryptedMessage]:
    """Initiator side. `recipient_ratchet_pub` is the recipient's signed prekey."""
    return await LinkChatDoubleRatchet.encrypt_initial_message(
        diffie_hellman_ratchet_class=diffie_hellman_ratchet_curve25519.DiffieHellmanRatchet,
        root_chain_kdf=_RootKDF,
        message_chain_kdf=_MessageKDF,
        message_chain_constant=MESSAGE_CHAIN_CONSTANT,
        dos_protection_threshold=MAX_SKIPPED_MESSAGE_KEYS,
        max_num_skipped_message_keys=MAX_SKIPPED_MESSAGE_KEYS,
        aead=_AEAD,
        shared_secret=shared_secret,
        recipient_ratchet_pub=recipient_ratchet_pub,
        message=message,
        associated_data=associated_data,
    )


async def accept_ratchet(
    shared_secret: bytes,
    own_ratchet_priv: bytes,
    message: EncryptedMessage,
    associated_data: bytes,
) -> tuple[LinkChatDoubleRatchet, bytes]:
    """Recipient side. `own_ratchet_priv` is our signed prekey's private half."""
    return await LinkChatDoubleRatchet.decrypt_initial_message(
        diffie_hellman_ratchet_class=diffie_hellman_ratchet_curve25519.DiffieHellmanRatchet,
        root_chain_kdf=_RootKDF,
        message_chain_kdf=_MessageKDF,
        message_chain_constant=MESSAGE_CHAIN_CONSTANT,
        dos_protection_threshold=MAX_SKIPPED_MESSAGE_KEYS,
        max_num_skipped_message_keys=MAX_SKIPPED_MESSAGE_KEYS,
        aead=_AEAD,
        shared_secret=shared_secret,
        own_ratchet_priv=own_ratchet_priv,
        message=message,
        associated_data=associated_data,
    )


def restore_ratchet(serialized: JSONObject) -> LinkChatDoubleRatchet:
    """Rebuild a ratchet saved with `.json`, e.g. after a restart."""
    return LinkChatDoubleRatchet.from_json(
        serialized,
        diffie_hellman_ratchet_class=diffie_hellman_ratchet_curve25519.DiffieHellmanRatchet,
        root_chain_kdf=_RootKDF,
        message_chain_kdf=_MessageKDF,
        message_chain_constant=MESSAGE_CHAIN_CONSTANT,
        dos_protection_threshold=MAX_SKIPPED_MESSAGE_KEYS,
        max_num_skipped_message_keys=MAX_SKIPPED_MESSAGE_KEYS,
        aead=_AEAD,
    )


def restore_x3dh_state(serialized: JSONObject) -> tuple[LinkChatX3DH, bool]:
    """Rebuild an X3DH state saved with `.json`, e.g. after a restart.

    Returns the state and a migration flag: True means the saved format was
    older and was upgraded on load. The caller should republish the bundle
    in that case, so relays stop serving the stale one. False on a fresh
    install or an up-to-date file.
    """
    return LinkChatX3DH.from_json(
        serialized,
        identity_key_format=X3DH_IDENTITY_KEY_FORMAT,
        hash_function=X3DH_HASH_FUNCTION,
        info=X3DH_INFO,
    )
