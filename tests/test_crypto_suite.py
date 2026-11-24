"""C6: the first X3DH + Double Ratchet exchange, and its sizes.

No relays, no SQLite: only that the libraries, configured as in `suite.py`, do
what we need. A session with the recipient offline, a reply, a restart, and
messages out of order.
"""

from __future__ import annotations

import os

from linkchat.core.crypto.suite import (
    accept_ratchet,
    new_x3dh_state,
    restore_ratchet,
    start_ratchet,
)
from linkchat.core.envelope import (
    CIPHERTEXT_SIZE,
    ENVELOPE_BUDGET,
    ENVELOPE_SIZE,
    PAYLOAD_SIZE,
)


async def test_first_session_while_bob_is_offline() -> None:
    """Requirement R3: Bob is not connected when Alice writes to him."""
    bob = new_x3dh_state()
    bundle = bob.bundle  # what Bob publishes on the relays before leaving

    # Alice, with no prior contact, runs X3DH against the bundle alone.
    alice = new_x3dh_state()
    sk_a, ad_a, x3dh_header = await alice.get_shared_secret_active(
        bundle, require_pre_key=False
    )
    assert x3dh_header.pre_key is None  # phase 1 uses no one-time prekeys

    # Bob's signed prekey is the first ratchet key.
    dr_a, first = await start_ratchet(sk_a, bundle.signed_pre_key, b"hola Bob", ad_a)

    # Bob comes back and finds the X3DH header and the first message.
    sk_b, ad_b, spk_pair = await bob.get_shared_secret_passive(
        x3dh_header, require_pre_key=False
    )
    assert (sk_b, ad_b) == (sk_a, ad_a)

    dr_b, plaintext = await accept_ratchet(sk_b, spk_pair.priv, first, ad_b)
    assert plaintext == b"hola Bob"

    # Bob replies: the first Diffie-Hellman ratchet step.
    reply = await dr_b.encrypt_message(b"hola Alice", ad_b)
    assert await dr_a.decrypt_message(reply, ad_a) == b"hola Alice"

    # Alice restarts: her ratchet comes back from its JSON form. The X3DH
    # associated data has to be stored with it, since every message needs it.
    dr_a = restore_ratchet(dr_a.json)
    later = await dr_b.encrypt_message(b"after the restart", ad_b)
    assert await dr_a.decrypt_message(later, ad_a) == b"after the restart"

    # Out of order: Alice sends three, Bob reads them as 3, 1, 2.
    sent = [await dr_a.encrypt_message(text, ad_a) for text in (b"one", b"two", b"three")]
    assert await dr_b.decrypt_message(sent[2], ad_b) == b"three"
    assert await dr_b.decrypt_message(sent[0], ad_b) == b"one"
    assert await dr_b.decrypt_message(sent[1], ad_b) == b"two"


async def test_a_large_backlog_read_newest_first() -> None:
    """Bob was offline for 200 messages and the relays return the newest first."""
    bob, alice = new_x3dh_state(), new_x3dh_state()
    sk, ad, header = await alice.get_shared_secret_active(
        bob.bundle, require_pre_key=False
    )
    dr_a, first = await start_ratchet(sk, bob.bundle.signed_pre_key, b"0", ad)
    sk_b, ad_b, spk = await bob.get_shared_secret_passive(header, require_pre_key=False)
    dr_b, _ = await accept_ratchet(sk_b, spk.priv, first, ad_b)

    backlog = [await dr_a.encrypt_message(str(n).encode(), ad) for n in range(1, 201)]

    assert await dr_b.decrypt_message(backlog[-1], ad_b) == b"200"
    assert await dr_b.decrypt_message(backlog[0], ad_b) == b"1"


async def test_measure_envelope_overhead() -> None:
    """The numbers that decide PAYLOAD_SIZE. Run with -s to see them."""
    bob, alice = new_x3dh_state(), new_x3dh_state()
    sk, ad, header = await alice.get_shared_secret_active(
        bob.bundle, require_pre_key=False
    )

    x3dh_bytes = len(header.identity_key) + len(header.ephemeral_key)
    x3dh_bytes += len(header.signed_pre_key)

    lengths = set()
    for _ in range(3):  # different content, same size: length must not leak
        _dr, message = await start_ratchet(
            sk, bob.bundle.signed_pre_key, os.urandom(PAYLOAD_SIZE), ad
        )
        lengths.add(len(message.ciphertext))
    [ciphertext] = lengths

    ratchet = message.header
    dr_header_bytes = len(ratchet.ratchet_pub) + 4 + 4  # two counters, 4 bytes each

    total = x3dh_bytes + dr_header_bytes + ciphertext
    print(
        f"\n  X3DH header (IK + EK + SPK, raw): {x3dh_bytes} B"
        f"\n  DR header (ratchet pub + 2 counters): {dr_header_bytes} B"
        f"\n  ciphertext for {PAYLOAD_SIZE} B of plaintext: {ciphertext} B"
        f"\n  overhead: {total - PAYLOAD_SIZE} B, room in the budget:"
        f" {ENVELOPE_BUDGET - PAYLOAD_SIZE} B, envelope would be {total} B"
        f" of {ENVELOPE_BUDGET}"
    )
    # The measured sizes must be exactly what envelope.py assumes.
    assert ciphertext == CIPHERTEXT_SIZE
    assert 2 + total == ENVELOPE_SIZE  # plus the version and flags bytes
