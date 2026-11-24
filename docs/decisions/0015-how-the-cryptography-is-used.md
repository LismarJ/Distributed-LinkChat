# ADR-0015: How the end-to-end cryptography is used

**Status:** accepted for phase 1, pending team review · **Date:** 2026-11-24
**Applies to:** both phases · **Follows from:** [ADR-0009](0009-adopt-a-cryptography-library.md) (adopt a library), [ADR-0007](0007-mailbox-identifiers.md) (mailboxes) · **Settles:** [`fault-tolerance.md`](../design/fault-tolerance.md) case 14

## Context

ADR-0009 chose to take X3DH and the Double Ratchet from a library rather than write them. A library gives the two protocols; it does not decide how they meet the rest of this system: relays that reorder and duplicate, keep bundles only in memory and may be malicious, and a client that may be killed at any moment and must lose nothing. Those choices are recorded here, with what each one costs.

The code is in `src/linkchat/core/crypto/`: `suite.py` (parameters), `identity.py`, `bundle.py`, `wire.py` (envelope layout), `sessions.py`, `channel.py` (storage and locking).

## Decisions

### 1. Library and parameters

`X3DH` 1.3.0 and `DoubleRatchet` 1.3.0 by Syndace, pinned exactly in `pyproject.toml` so that a cryptographic dependency never changes silently. Their own test suites pass against the installed versions (9/9 and 8/8; `XEdDSA` 2/2). Signal publishes no official test vectors for either protocol, so these are the libraries' own suites, not independent vectors.

| Choice | Value | Why |
|---|---|---|
| Identity key | Ed25519 | It signs the signed prekey; the library converts it for Diffie-Hellman (XEdDSA) |
| X3DH hash and info | SHA-256, `LinkChat X3DH v1` | |
| Ratchet curve | Curve25519 | |
| Root chain KDF | HKDF-SHA-256 | Signal's recommendation |
| Message chain KDF | HMAC-SHA-256, constants `0x01` (message key) and `0x02` (next chain key) | Signal's recommendation |
| AEAD | AES-256-CBC with HMAC-SHA-256 | The library's recommended recipe |
| Skipped message keys kept, and DoS threshold | 1000 and 1000 | Relays return a backlog in no order, so the first message decrypted may be far ahead of the last one read |

### 2. No one-time prekeys

Bundles carry the identity key, the signed prekey and its signature, nothing more. Both sides call X3DH with `require_pre_key=False`, and the envelope refuses to carry one.

This settles fault-tolerance case 14. Relays do not coordinate reads, so two senders could be handed the same one-time prekey; and if the recipient deleted it on first use, the second sender's first message could never be decrypted. Without one-time prekeys the case cannot arise.

### 3. No signed-prekey rotation in phase 1

A rotated prekey would break a sender holding a bundle from a relay that has not yet seen the new one. Keeping one signed prekey for the life of the identity removes that case.

### 4. Identity and bundles

A user's id is `BLAKE2b-256(identity public key)`, created once and stored in SQLite; a second, different identity is refused (`IdentityConflict`).

A bundle is accepted only if its identity key hashes to the UserId the reader expected. The signature is not enough: a relay can sign a prekey of its own perfectly well; it cannot make its key hash to someone else's id. The UserId arrives through the invitation, outside the relays. The signature itself is checked by the library when the bundle is used.

Relays keep bundles in memory without replicating them, so a client publishes to all five relays at once and republishes every 60 seconds while online. Reading asks all five at once and keeps the first valid answer, so a relay that hangs costs one timeout, not one each.

### 5. The X3DH header travels until the first reply

A session is identified by the initiator's ephemeral key. The initiator puts its X3DH header in every envelope of that session until it has read something the peer sent in it. Relays reorder, so the recipient may read message 2 before message 1; with the header on both, either can open the session, and a header with a known ephemeral key reuses the session instead of opening another.

Every envelope's header must carry an identity key that hashes to the mailbox's sender; otherwise someone else is writing into that mailbox, and the envelope is refused.

### 6. Both sides starting at once

If each side initiates before seeing the other, each holds two sessions. Both send with the one initiated by the smaller UserId, which both compute alike, and keep the other so that what was already encrypted in it can still be read. The choice is deterministic for any number of sessions, so a misbehaving peer cannot make sending fail.

### 7. A fixed-size envelope

Every envelope is exactly 4090 bytes, inside the 4096-byte budget: version and flags (2), an X3DH header slot always present (96), the ratchet header (40) and the ciphertext (3952). The plaintext is padded to 3904 bytes before encryption, so length carries no information. Version, flags and the X3DH slot are added to the associated data, so a relay that edits them makes decryption fail.

### 8. Ratchet state stored with the message

The ratchet advances on every encryption and decryption. Its new state is stored in the same SQLite transaction as the message: with the message and its outbox entry when sending, with the message and the seen-set when receiving. If that transaction fails, the state in memory is rolled back to what it was. A per-peer lock keeps the outbox and the receive loop from interleaving on one ratchet. The seen-set is checked before decrypting, because a ratchet cannot decrypt the same message twice.

So a client killed at any point loses nothing: no ciphertext leaves the process unless the state that made it is stored, and no message is marked seen unless the state that read it is stored.

## Consequences

**What holds.** Relays store only fixed-size ciphertext and cannot read it, forge it, or substitute a bundle. Two failures of any kind, and a killed client, lose no confirmed message and decrypt nothing twice.

**What a relay can still see, stated plainly.**

- The ratchet header is in the clear, as in the standard Double Ratchet: a relay can tell envelopes of one session apart and read their message numbers. Signal hides this with header encryption, which the library does not provide.
- The first messages of a session carry the sender's identity public key in the clear, and the UserId is its hash. A relay can therefore learn who started a session. Combined with ADR-0007 (mailbox ids are derived from the two UserIds), the pair behind a mailbox is not hidden from a relay that knows both ids. Deriving mailboxes from the X3DH secret would close most of this, and is left for later.
- The flag marking "carries an X3DH header" adds nothing to that, since message numbers of zero already mark a first message.

**What it costs.**

- Without one-time prekeys, the first message of a session has weaker forward secrecy: it depends on the signed prekey, which does not change.
- Without rotation, a stolen signed prekey exposes the first messages of every session started against it.
- An identity cannot be recovered: losing a client's volume is losing the user. There is one device per user.

## Licences

Every library here is permissive: none requires our code to be open or under any particular licence. The obligation they share is to keep their copyright and licence notices with any copy we redistribute. Installing them with pip places each notice in the package's `dist-info`, inside our Docker image, so the image already complies; we do not copy, modify or vendor their code. If that ever changes, the notice must travel with the copied code.

| Library | Version | Licence | Copyright, as in its licence file |
|---|---|---|---|
| X3DH | 1.3.0 | MIT | © 2026 Tim Henkes (Syndace) |
| DoubleRatchet | 1.3.0 | MIT | © 2026 Tim Henkes (Syndace) |
| XEdDSA (installed by X3DH) | 1.2.0 | MIT | © 2026 Tim Henkes (Syndace) |
| cryptography (used by both) | 50.0.2 | Apache-2.0 or BSD-3-Clause | © Individual contributors |
| pydantic (used by both) | 2.14.3 | MIT | © 2017 to present Pydantic Services Inc. and individual contributors |
| msgpack | 1.2.4 | Apache-2.0 | © 2008-2011 INADA Naoki |

The written report should credit the cryptographic libraries by name, author and licence, and describe them as ADR-0009 does: the maintained Python implementation used by the OMEMO ecosystem.

PyNaCl and `aiosqlite` are still declared in `pyproject.toml` but no code uses them: the cryptography goes through `cryptography`, and the history uses the standard library's `sqlite3`. Removing them is a follow-up.
