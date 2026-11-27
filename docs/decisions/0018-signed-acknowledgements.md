# ADR-0018: Only a mailbox's recipient may acknowledge it, by signature

**Status:** accepted · **Date:** 2026-11-27 · **Deciders:** project team
**Applies to:** phase 1 and phase 2 · **Settles:** the access credential [ADR-0007](0007-mailbox-identifiers.md) left open

## Context

Every relay call carries a `credential`, and until now no relay checked it. A mailbox id is `mailbox_for(sender, recipient)`, a hash of two user ids, so anyone who knows both ids of a conversation can compute it. With that alone they could:

- **`fetch`** the mailbox, and get fixed-size ciphertext they cannot open;
- **`ack`** its records, which turns them into tombstones: the recipient never receives them. An attack on availability, not on confidentiality or authenticity, but the only relay call whose misuse loses mail.

## Decision

**`ack` requires the recipient's signature; `fetch` and `wait_for_mail` stay open.**

The credential for an ack is 129 bytes: a version byte, the sender's user id, the signer's Ed25519 identity public key, and a signature by the identity key over `"linkchat-ack" ‖ mailbox id ‖ sorted envelope ids`. The relay accepts it only if:

1. `user_id_of(public key)` is the signer: user ids are self-certifying, so no lookup is needed;
2. `mailbox_for(sender, signer)` is the mailbox being acknowledged: only its reader may empty it;
3. the signature is valid for exactly this mailbox and these ids.

Otherwise it raises `CredentialRejected` and deletes nothing. The check (`linkchat.core.crypto.acks`) is Ray-free, and the same function runs in the real relay actor and in the fake used by the tests.

## Why this shape

- **Nothing on the relay is consulted.** An earlier draft verified against the bundle the relay had stored. A relay restarted by Ray comes back with no bundles until clients republish (up to 60 s), and during that window it would have refused every ack; the receive loop would then find its mail still there and spin. Carrying the public key, and checking it against the self-certifying id, removes that window and that state.
- **The identity key is the right key.** It is already Ed25519, it already signs the signed prekey, and the user id is its hash. No new key, no new exchange, no change to the relay interface: the `credential` parameter was there from the start.
- **Replay is harmless.** A captured credential only acknowledges the same records again, and `ack` is idempotent. It covers no other mailbox and no other ids.
- **Why `fetch` stays open.** It returns only ciphertext of one fixed size, which reveals nothing the relay itself does not already see. Signing every read would add a signature per poll for no confidentiality gained. This is a decision, not an omission.

## Consequences

- An unsigned or wrongly signed ack is refused on every relay. A client acknowledges its own mail exactly as before; the signing is inside `Receiver`.
- `ChatClient` builds the signer from its identity through the X3DH library's public API (`IdentityKeyPair.from_json(...).seed`).
- Relay-to-relay repair (resync, handoff) carries tombstones between relays and is unaffected: it does not go through `ack`.
- Tests that called `ack` with a placeholder credential on made-up mailbox ids now sign a real one (`tests/conftest.py`: `make_reader`, `inbox`, `ack_credential`).
- What remains open from ADR-0007 is the invitation format, nothing else.
