# 03 · Signal — Identity, Asynchronous Key Agreement, and the Double Ratchet

**Status:** research note · **Date:** 2026-11-10
**Why the brief names it:** *"comparación orientada a identidad y conceptos de cifrado de extremo a extremo."*

Signal matters to us for one structural reason above all: **the prekey mechanism is the cryptographic solution to offline delivery.** It lets A establish an encrypted session with B while B's device is switched off. That is requirement R3, solved at the key-agreement layer, and no amount of clever networking substitutes for it.

---

## 1. Identity

- A user's identity is a long-term **identity key pair**. Everything else hangs off it.
- Human-readable identity (a phone number, in Signal's case) is a *discovery* convenience layered on top, not the identity itself. Signal has publicly moved toward usernames to reduce phone-number exposure.
- **Safety numbers** let two users compare a fingerprint of both identity keys out of band, detecting a man-in-the-middle at session setup.
- Signal's own deployment keeps a central server for registration, prekey distribution and routing. Private contact discovery has been attempted with hashing and with SGX enclaves, and remains one of the hardest problems in the space.

**For us:** identity is `UserID = H(identity_public_key)`, self-generated, no registration authority. Contact discovery is out-of-band (an invitation string containing the key), which sidesteps the hardest problem entirely — the same move SimpleX makes, see [`04-decentralized-alternatives.md`](04-decentralized-alternatives.md). Safety-number comparison is cheap to implement and a very good demo of "authenticated communication" for R8.

---

## 2. Asynchronous key agreement: X3DH → PQXDH

**The problem.** Diffie-Hellman needs both parties online. Messaging does not have both parties online.

**The solution.** B uploads, ahead of time, a **prekey bundle**:
- the long-term **identity key** `IK_B`,
- a **signed prekey** `SPK_B` plus its signature under `IK_B`, rotated periodically,
- a batch of **one-time prekeys** `OPK_B`.

A fetches a bundle, verifies the signed prekey's signature (**aborting if verification fails**), generates an ephemeral key, and performs several DH operations combining identity, signed-prekey, one-time-prekey and ephemeral keys. The outputs are concatenated and run through a KDF to produce the initial shared secret. A can now send an encrypted first message immediately; B derives the same secret when it comes online.

Two forward-secrecy details that matter operationally:
- The server **deletes each one-time prekey as it hands it out**, so it is used once.
- B **deletes the one-time prekey private key** after use.
- If one-time prekeys run out, the protocol still works using only the signed prekey, at reduced forward secrecy — so the bundle store needs replenishment, and running dry must degrade rather than fail.

**Post-quantum.** X3DH has been replaced by **PQXDH**, which keeps the elliptic-curve steps and adds a KEM encapsulation (Kyber/ML-KEM) against the recipient's static post-quantum key, giving harvest-now-decrypt-later protection. Note that in PQXDH *authentication* still rests on classical discrete-log hardness; only confidentiality gains PQ protection. In 2025 Signal added the **Triple Ratchet**, combining the classical Double Ratchet with a Sparse Post-Quantum Ratchet (SPQR).

**For us:** implement X3DH (classical). PQXDH is a *stretch goal and an excellent "future work" slide* — we should be able to explain precisely what it adds and what it does not. The prekey bundle must be published somewhere reachable while B is offline; in our architecture that is the relay overlay, stored as a signed, self-verifying record rather than trusted server state.

---

## 3. The Double Ratchet

After the handshake, every message advances two ratchets:

- **The DH ratchet.** Each party attaches a fresh ephemeral DH public key to its messages. When you receive a new one, you perform a DH with it and mix the result into the **root key**, producing a new sending or receiving chain key. This is what provides **post-compromise security** (self-healing): an attacker who steals your keys loses access once a fresh DH exchange they did not observe completes.
- **The symmetric-key ratchet.** Within a chain, each message key is derived by a KDF step from the chain key, which is then advanced and the old value discarded. This provides **forward secrecy**: past message keys cannot be recovered from the current state.

**Out-of-order handling** is a genuinely distributed-systems-flavoured part of the design, and is exactly where our R5/R6 work meets the crypto: each message header carries the sending chain's ephemeral public key, the message number `N`, and the previous chain length `PN`. A receiver that gets message 5 before message 3 derives and **stores skipped message keys** so 3 can still be decrypted when it arrives. Implementations must bound that skipped-key store, or an attacker sends `N = 2^31` and exhausts memory.

**For us:** this is the single most delicate component to implement. Decision (see §6): do **not** write our own. Use an established open-source implementation.

---

## 4. Sealed sender and metadata

Signal's content encryption is strong, but as Wikipedia bluntly notes, the protocol *does not by itself* prevent an operator retaining who talked to whom and when. **Sealed sender** narrows that: the sender's identity is placed inside an encrypted envelope, and the sender authenticates to the server with a short-lived delivery certificate rather than by identifying itself per message, so the server learns the recipient but not the sender.

**For us:** we can go further than Signal precisely because we have no accounts. If messages are addressed to an **opaque per-conversation mailbox identifier** rather than to a user identity, a relay never learns either endpoint — it sees "something deposited into mailbox `0xA3F...`". That is the SimpleX insight, and it is a strong, defensible claim to make at the defense: *our relays see strictly less metadata than Signal's servers do.*

---

## 5. Signal's architecture, and where it is not what we need

Signal is **centralized by design**. The server performs registration, prekey storage, message queuing for offline recipients, and routing. It is a deliberate tradeoff — centralisation buys consistent prekey availability, spam control, and the ability to ship protocol upgrades to everyone at once, at the cost of being a single point of failure and control. Signal's defence is that the server is minimally trusted, not that it is absent.

The brief forbids us that choice. So we take Signal's **cryptographic core** and reject its **topology** — the mirror image of what we do with Matrix, where we take the topology ideas and reject the permanent per-server history.

---

## 6. Implementation decision: do not roll our own crypto

Rolling our own Double Ratchet in three months would consume the time we need for the distributed systems work — which is the part actually being graded — and would almost certainly be subtly wrong. Candidate free/open-source libraries:

| Option | Language | Licence | Notes |
|---|---|---|---|
| `libsignal` (Signal's own) | Rust core, bindings | AGPL-3.0 | Authoritative; licence is heavy for a course project but fine for non-distributed academic work. Bindings can be awkward. |
| `@privacyresearch/libsignal-protocol-typescript` | TypeScript | GPL-3.0 | Pure-TS port of the Signal protocol. Easiest path if we go Node/TS. |
| **libsodium** (via `libsodium-wrappers`, `golang.org/x/crypto/nacl`) | any | ISC | Not a ratchet — gives us X25519, Ed25519, XChaCha20-Poly1305, BLAKE2b, HKDF. We implement X3DH + the ratchet *on top*, which is ~300 lines against a well-specified document. |
| `libolm` / `vodozemac` (Matrix) | C / Rust | Apache-2.0 | Olm is the Double Ratchet; vodozemac is the maintained Rust rewrite. Permissive licence. |
| `openmls` | Rust | MIT | MLS — group-oriented, overkill for 1:1. |

**Recommendation:** build on **libsodium primitives** and implement **X3DH + Double Ratchet from the published specifications ourselves**, against the official test vectors. Rationale: the specifications are short and precise, the primitives are the dangerous part and libsodium provides them, and *having implemented the ratchet is itself a defensible piece of project work* that we can explain at the defense — whereas calling a library is not. If we fall behind schedule, swapping in vodozemac or the TS port is a contained, pre-planned fallback. This is a deliberate exception to "never roll your own crypto", justified by the academic context and by using audited primitives throughout; it should be stated as such in the report.

---

## 7. Verdict for our project

| Signal idea | Verdict |
|---|---|
| Public-key identity as the root of everything | **Adopt** |
| Prekey bundles for asynchronous session setup | **Adopt — this is how R3 is solved cryptographically** |
| Signed prekey + verification-or-abort | **Adopt** |
| One-time prekeys, deleted on issue and on use | **Adopt**, with explicit depletion handling |
| Double Ratchet (FS + PCS) | **Adopt**, implemented over libsodium |
| Skipped-message-key store with a hard bound | **Adopt** — ties directly to our out-of-order delivery work |
| Safety numbers / fingerprint comparison | **Adopt** — cheap, and demonstrates R8 |
| Sealed sender | **Adopt the goal, improve on it** via opaque mailbox IDs |
| PQXDH / Triple Ratchet | **Future work** — explain, don't build |
| Centralized prekey + routing server | **Reject** — replaced by the relay overlay |
| Phone-number identity and server-side contact discovery | **Reject** — out-of-band invitations instead |

---

## Sources

- [Signal Protocol — Wikipedia](https://en.wikipedia.org/wiki/Signal_Protocol)
- [A Comprehensive Study of the Signal Handshake Protocol — NIST PQC Standardization Conference, 2025](https://csrc.nist.gov/csrc/media/events/2025/sixth-pqc-standardization-conference/a%20comprehensive%20study%20of%20the%20signal%20handshake%20protocol%20(2).pdf)
- [Diving into Signal's new PQ protocol — PQShield](https://pqshield.com/diving-into-signals-new-pq-protocol/)
- [Study of Post Quantum status of Widely Used Protocols (arXiv)](https://arxiv.org/pdf/2603.28728)
- [The Snowden Phone: A Comparative Survey of Secure Instant Messaging Mobile Applications (arXiv)](https://arxiv.org/pdf/1807.07952)
- [From Specs to Apps: Verifying and Monitoring Models of Signal and WhatsApp (arXiv)](https://arxiv.org/pdf/2609.11882)
- [Adapting the Signal Protocol for P2P Messaging](https://positive-intentions.com/blog/p2p-signal-protocol/)
