# 05 · The Three Reference Platforms — Study and Deep Comparison

**Status:** research note · **Date:** 2026-11-10
**Scope:** WhatsApp, Matrix and Signal — the three systems the brief names for architectural comparison. Studied individually in [`01-whatsapp.md`](01-whatsapp.md), [`02-matrix.md`](02-matrix.md) and [`03-signal.md`](03-signal.md); compared here dimension by dimension.

> **A note on evidence.** The three are not equally knowable. **Matrix** has a public, versioned specification we read directly — its claims here are spec-backed. **Signal** publishes its protocol specifications and all of its source code — its claims are strong. **WhatsApp** publishes an encryption white paper and almost nothing else; its architecture is reconstructed from secondary sources that contradict each other, so every WhatsApp row below should be read as *"commonly reported"*. We will state this distinction explicitly in the defense; it is itself a finding about the field.

---

## 1. The three in one sentence each

- **WhatsApp** — a *centralized router* with end-to-end encryption bolted on: one operator, a transient per-user queue, history archived only on the client, and a user-experience contract so refined that it has become the industry's definition of "working".
- **Matrix** — a *federation of persistent servers* replicating a signed event DAG per conversation, with eventual consistency and a deterministic conflict-resolution algorithm; the only one of the three that is genuinely decentralized, and the only one that keeps full history server-side forever.
- **Signal** — a *centralized server deliberately kept minimal*, whose real contribution is cryptographic: public-key identity, prekey bundles that make asynchronous session setup possible, and the Double Ratchet.

The brief's framing matches this exactly: WhatsApp for **UX**, Matrix for **decentralized mechanism**, Signal for **identity and crypto**. Each is best in class on one axis and weak on the other two.

---

## 2. Architecture and trust model

| | **WhatsApp** | **Matrix** | **Signal** |
|---|---|---|---|
| **Topology** | Single operator, logically one router | Federation of independent homeservers | Single operator |
| **Decentralized?** | No | **Yes** — any server can join the federation | No |
| **Unit of participation** | Account on the operator | Homeserver hosting users | Account on the operator |
| **Who must be trusted for availability** | The operator | Your homeserver, plus the peers' homeservers | The operator |
| **Who must be trusted for confidentiality** | Nobody (E2EE) | Nobody (E2EE, if enabled) | Nobody (E2EE) |
| **Who must be trusted for metadata** | The operator | Your homeserver + every participating server | The operator (narrowed by sealed sender) |
| **Server knows the social graph** | Yes | Yes (your homeserver does) | Narrowed by sealed sender + private contact discovery |
| **Failure of one server** | Total outage | Degraded — that server's users are cut off, the rest continue | Total outage |
| **Partition behaviour** | Clients queue locally | **Forks the DAG, heals by backfill + state resolution** | Clients queue locally |
| **Governance** | Corporate | Open specification, versioned room versions | Non-profit, single implementation |

**Reading.** Only Matrix has anything to say about partitions, because only Matrix has more than one authority. That is why the brief points us there for "estado replicado". Its price is paid in §4.

---

## 3. Identity and contact discovery

| | **WhatsApp** | **Matrix** | **Signal** |
|---|---|---|---|
| **Identifier** | Phone number | `@user:homeserver` | Phone number, moving toward usernames |
| **Rooted in** | A telecom operator | A DNS domain | An account on the operator |
| **Self-authenticating?** | No | No (the server vouches) | **Partly** — the identity key is the real identity |
| **Survives the server dying?** | No | **No** — this is Matrix's deepest structural flaw | No |
| **Discovery mechanism** | Upload your address book | Server-side user directory + room aliases | Private contact discovery (hashing, SGX enclaves) |
| **MITM detection** | Security code comparison | Device verification + cross-signing | **Safety numbers** |
| **Key distribution** | Operator | `GET /_matrix/key/v2/server`, self-signed, `valid_until_ts`, plus **notary servers** that can serve cached keys when the origin is offline | Operator distributes prekey bundles |

**Three findings we will reuse.**

1. **All three bind identity to an authority** — a phone network, a DNS name, an account. All three therefore fail the test "can your identity survive the infrastructure?". Our design will not: `UserID = H(public_key)`, generated locally, owned by nobody.
2. **Contact discovery is unsolved even by Signal.** Private contact discovery needed SGX enclaves and is still debated. The cheap, correct escape is the one SimpleX and Briar take — **out-of-band invitations**. We take it too, and we say in the defense that we are sidestepping an open research problem deliberately rather than failing to notice it.
3. **Matrix's notary-server trick is quietly brilliant** and directly transferable: a third party can vouch for a signed key *while its owner is offline*, because the key record is self-verifying. Our relays will cache and serve each other's identity records the same way.

---

## 4. Message storage, offline delivery and retention — the decisive dimension

This is requirement R3/R4/R9, and it splits the three cleanly.

| | **WhatsApp** | **Matrix** | **Signal** |
|---|---|---|---|
| **Who holds a message for an offline user** | The operator's queue | The recipient's **homeserver**, which also holds the whole room | The operator's queue |
| **Retention** | **Until delivered, then deleted** | **Forever** (full room history, replicated per server) | **Until delivered, then deleted** |
| **Is the server an archive?** | **No — a queue** | **Yes, by design** | **No — a queue** |
| **Where history lives permanently** | Client (SQLite) | Every participating homeserver, *and* clients | Client |
| **Replication of the queued message** | Internal to the operator | Replicated to every server with a member in the room | Internal to the operator |
| **Delivery retry when the peer is down** | Operator-internal | **Per-destination transaction queue, `txnId` reused until 200 OK**; no backoff schedule, queue bound or drop policy is specified | Operator-internal |
| **Catch-up after being offline** | Drain the queue | **Walk the DAG**: `/backfill`, `/get_missing_events`, `/state_ids` | Drain the queue |

**The central tension of this project, stated precisely.**

> WhatsApp and Signal have the **retention policy** our brief demands (queue, not archive) but the **topology** it forbids (a centre).
> Matrix has the **topology** our brief demands (no centre) but the **retention policy** it forbids (permanent full history on servers).

Requirement R9 — *"relés temporales sin convertir la arquitectura en una base de datos central permanente con todo el historial"* — is therefore asking for something **none of the three named references provides**. That is not an oversight in the brief; it is the point of the exercise. The combination exists only in the fourth family ([`04-decentralized-alternatives.md`](04-decentralized-alternatives.md)): Session, SimpleX and Briar all pair a decentralized topology with bounded retention.

Our architecture must therefore be: **Matrix's decentralization + WhatsApp/Signal's amnesia.**

---

## 5. Ordering, consistency and duplicate suppression

| | **WhatsApp** | **Matrix** | **Signal** |
|---|---|---|---|
| **Ordering mechanism** | Server-assigned order per conversation; client sorts | **`prev_events` DAG** — a partial causal order, plus `depth` as a hint | Double Ratchet header: `N` (message number) and `PN` (previous chain length) |
| **Consistency model** | Effectively linear per conversation (one authority) | **Eventual**, with deterministic merge | Linear per session |
| **Conflict resolution** | Not needed | **State resolution v2** (v2.1 in room v12, after CVE-2025-49090) | Not needed |
| **Dedup key** | Server message ID | **Event ID = content hash** — dedup is free | Message number within a chain |
| **Out-of-order handling** | Buffer and sort | DAG naturally tolerates it; gaps repaired by backfill | **Skipped message keys stored** so a late message still decrypts |
| **Hardest part** | — | State resolution: widely regarded as the most bug-prone component; large branching rooms are a known scaling problem | Bounding the skipped-key store (DoS if unbounded) |

**Three transferable mechanisms.**

- **Content-hash event IDs (Matrix).** If a message's ID *is* the hash of its content, duplicate suppression (R6) costs one index lookup and forgery is impossible. We adopt this.
- **`prev_events` (Matrix).** Each message naming its predecessors makes causal order (R5) intrinsic, and makes a reconnecting client able to *detect precisely what it is missing*. We adopt a simplified version — for a 1:1 conversation the DAG is nearly a chain.
- **`N` / `PN` and the skipped-key store (Signal).** The crypto layer has its own ordering problem, and it must be solved even if the network layer delivers perfectly in order. We adopt it with a hard bound on stored skipped keys.

**And one we avoid by construction.** State resolution exists because rooms have *mutable shared state* — membership, power levels, topic — that several servers change concurrently. **A 1:1 conversation between two fixed parties has none.** The brief restricts us to 1:1, so we inherit the DAG's benefits while structurally eliminating Matrix's hardest problem. This is the single best scoping insight in the whole study and belongs on a defense slide.

---

## 6. Cryptography

| | **WhatsApp** | **Matrix** | **Signal** |
|---|---|---|---|
| **E2EE by default** | Yes | **No** — opt-in per room historically; a long-standing criticism | Yes |
| **Protocol** | Signal Protocol (adopted) | **Olm** (pairwise Double Ratchet) + **Megolm** (group ratchet) | **X3DH → PQXDH**, Double Ratchet, 2025 **Triple Ratchet** (adds Sparse PQ Ratchet) |
| **Async session setup** | Prekeys | Olm one-time keys published to the homeserver | **Prekey bundle**: identity key + signed prekey (verify-or-abort) + one-time prekeys, deleted on issue and on use |
| **Forward secrecy** | Yes | Yes | Yes |
| **Post-compromise security** | Yes | Yes | Yes (DH ratchet) |
| **Sender anonymity from the server** | No | No | **Sealed sender** |
| **Formal analysis** | Modelled in recent work | **Mechanized formal analysis of the Matrix cryptographic suite** exists | Extensively analysed, incl. NIST PQC venue work on the handshake |
| **Multi-device approach** | Per-device keys, encrypt per device (up to 4 companions) | Per-device keys + cross-signing + device verification | Per-device keys |

**Finding.** All three converge on the *same* cryptographic core — Signal's. Matrix reimplemented it as Olm; WhatsApp adopted it directly. For 1:1 messaging, **the Double Ratchet is the settled answer**, and there is no interesting design choice left to make. Our contribution must be in the distributed systems layer, not the crypto. We implement the standard construction over audited primitives and spend our ingenuity elsewhere.

**The one thing nobody else does, that we can:** all three bind the stored, offline-reachable key material to a *trusted* server. A Matrix homeserver is trusted to serve honest one-time keys; Signal's server is trusted to serve honest prekey bundles. In our design, a prekey bundle is **self-verifying** — signed under the identity key, stored on untrusted replicated relays — so a relay cannot substitute one. Worth a slide.

---

## 7. Technology stacks

Verified from project repositories and ecosystem pages where possible; unverified items are marked.

| | **WhatsApp** | **Matrix** | **Signal** |
|---|---|---|---|
| **Specification** | Closed (encryption white paper only) | **Open, versioned**: Client-Server, Server-Server, room versions | **Open protocol specs**, open source, single implementation |
| **Server language** | **Erlang/OTP**, originally a heavily modified **ejabberd** fork | **Synapse** — Python/Twisted, AGPL-3.0, Element-maintained, most complete and battle-tested · **Dendrite** — Go, Apache-2.0, beta / maintenance mode as of 2026 · **Conduit** — Rust, Apache-2.0; actively developed fork **Continuwuity** (earlier fork conduwuit marked obsolete) | **Signal-Server** — **Java**, AGPL-3.0 |
| **Crypto library** | Signal Protocol implementation (closed) | **libolm** (C/C++, deprecated, reviewed by NCC Group) → **vodozemac** (Rust, current reference implementation) | **libsignal** — **Rust** core, AGPL-3.0, with **JNI** bindings for Java/Android, **C FFI** for Swift/iOS, and **TypeScript** bindings; used by all clients *and* server-side |
| **Server storage** | **Mnesia** reported for routing table and offline queues; secondary sources disagree on specifics | PostgreSQL (production) / SQLite (small deployments) | Relational + caching tiers (not documented in detail publicly) |
| **Wire protocol** | Custom binary protocol, historically XMPP-derived | **HTTPS + JSON**, canonical-JSON signing, **Ed25519**, `X-Matrix` auth header | Custom, over HTTPS/WebSocket |
| **Clients** | Native Android/iOS; multi-device companions | **Element Web** (TypeScript/React) and a large third-party ecosystem | **Signal-Android** (Kotlin, AGPL-3.0) · **Signal-iOS** (Swift, AGPL-3.0) · **Signal-Desktop** (**Electron + TypeScript**, AGPL-3.0) |
| **Client storage** | SQLite | Browser storage / IndexedDB; SQLite in native clients | Local encrypted DB (SQLCipher commonly cited — **unverified**) |
| **Concurrency model** | **One lightweight Erlang process per connected user**, supervision trees | Python async (Twisted) / Go goroutines / Rust async | JVM thread pools |
| **Licence** | Proprietary | AGPL-3.0 (Synapse), **Apache-2.0** (Dendrite, Conduit, vodozemac) | **AGPL-3.0 throughout** |

**Four stack lessons for our own choices.**

1. **Licence matters for our library choice.** `libsignal` is **AGPL-3.0**; Matrix's **vodozemac is Apache-2.0** and is a maintained Rust implementation of the same Double Ratchet. If we want a ready-made ratchet under a permissive licence, **vodozemac is the better pick than libsignal** — a non-obvious finding worth recording as ADR material.
2. **Erlang's "one process per user" is the right mental model** even if we do not use Erlang. Every language we are considering has an equivalent — goroutines, async tasks, actors — and the supervision-tree habit (assume it crashes, restart it) is exactly the posture a churn-tested project needs.
3. **Matrix proves a protocol can outlive its implementation.** Three independent homeservers in three languages interoperate because the specification is the contract. **Our project should be specified before it is implemented**: a written wire-protocol document in `docs/design/`, versioned, so our two node types are written against a spec rather than against each other.
4. **Rust core + language bindings (libsignal) is the industrial pattern** for sharing one crypto implementation across platforms. Out of scope for us, but the right answer to "how would you ship this on mobile?" at the defense.

---

## 8. Scorecard against our nine requirements

Legend: ● fully satisfies · ◐ partially / with caveats · ○ does not satisfy or conflicts.

| | R1 identity | R2 1:1 | R3 offline | R4 store-fwd | R5 order | R6 dedup | R7 local history | R8 auth+crypto | **R9 no permanent central DB** |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| **WhatsApp** | ◐ phone, central | ● | ● | ● | ● | ● | ● | ● | ◐ *policy* yes, *topology* no |
| **Matrix** | ◐ server-bound | ● | ● | ● | ● | ● | ◐ also server-side | ◐ opt-in E2EE | **○ permanent server history** |
| **Signal** | ● key-based | ● | ● | ● | ● | ● | ● | ● | ◐ *policy* yes, *topology* no |
| **Session** | ● | ● | ● | ● | ◐ | ● | ● | ◐ no FS | ● |
| **SimpleX** | ● none at all | ● | ● | ◐ no replication | ◐ | ● | ● | ● | ● |
| **Target: ours** | ● | ● | ● | ● | ● | ● | ● | ● | ● |

The bottom row is the thesis. No existing system in the top three fills it; our job is to assemble one that does.

---

## 9. Synthesis — what we take from each, and what we refuse

**From WhatsApp**
Take: the delivery-state UX (sent → delivered → read, plus a `pending` state for "no relay reachable"); "the server is a queue, not an archive"; deletion on acknowledgement; local SQLite as the only permanent home of history; supervision-tree thinking.
Refuse: the centralized topology; phone-number identity; address-book upload; anything branded (the brief says so explicitly).

**From Matrix**
Take: content-hash event IDs; `prev_events` causal references; the receipt-validation pipeline (drop → redact → reject → soft-fail); per-destination transaction queues with idempotent `txnId`; gap repair by DAG walk; canonical-JSON request signing that includes the intended destination; self-published signing keys with notary caching; "try another server" error semantics.
Refuse: homeserver-bound identity; permanent per-server history; the full federation surface.
Avoid by construction: state resolution — 1:1 conversations have no mutable shared state.
Fill in ourselves: the backoff schedule, queue bound and drop policy the spec deliberately leaves undefined.

**From Signal**
Take: public-key identity; prekey bundles as the cryptographic solution to offline session setup; signed prekey with verify-or-abort; one-time prekeys deleted on issue and on use, with graceful depletion; the Double Ratchet; the `N`/`PN` skipped-key store with a hard bound; safety-number verification; sealed sender's *goal*.
Refuse: the centralized prekey and routing server; phone-number identity and server-side contact discovery.
Improve on: metadata — address messages to **opaque mailbox identifiers** so a relay learns neither endpoint, which is strictly less than Signal's server learns.

**The one-line statement of our architecture, derived from this comparison:**

> Matrix's decentralization and causal event model, with WhatsApp's and Signal's amnesiac queue semantics, Signal's cryptography, and Session's replicated-swarm mailboxes — addressed the SimpleX way, so relays hold padded ciphertext for opaque mailboxes under a TTL and can reconstruct nothing.

Architecture candidates built on this synthesis, with their tradeoffs, are in [`../design/architecture-options.md`](../design/architecture-options.md).

---

## Sources

**Matrix** — [Server-Server API spec (v1.19)](https://spec.matrix.org/latest/server-server-api/) · [Room Version 2 / state resolution](https://spec.matrix.org/v1.19/rooms/v2/) · [Matrix ecosystem: servers](https://matrix.org/ecosystem/servers/) · [Synapse vs Dendrite vs Continuwuity (2026)](https://www.pistack.xyz/posts/2026-05-02-synapse-vs-dendrite-vs-continuwuity-self-hosted-matrix-server-guide/) · [Matrix Homeservers Overview](https://matrixdocs.github.io/docs/servers/overview) · [Matrix Decomposition: independent analysis of state resolution](https://matrix.org/blog/2020/06/16/matrix-decomposition-an-independent-academic-analysis-of-matrix-state-resolution/) · [The Matrix Reloaded: mechanized formal analysis (arXiv)](https://arxiv.org/pdf/2408.12743) · [A Glimpse of the Matrix: scalability issues (arXiv)](https://arxiv.org/pdf/1910.06295)

**Signal** — [signalapp/Signal-Server](https://github.com/signalapp/Signal-Server) · [signalapp/libsignal](https://github.com/signalapp/libsignal) · [Signal org repositories](https://github.com/orgs/signalapp/repositories) · [Signal (software) — Wikipedia](https://en.wikipedia.org/wiki/Signal_(software)) · [Signal Protocol — Wikipedia](https://en.wikipedia.org/wiki/Signal_Protocol) · [A Comprehensive Study of the Signal Handshake Protocol (NIST PQC 2025)](https://csrc.nist.gov/csrc/media/events/2025/sixth-pqc-standardization-conference/a%20comprehensive%20study%20of%20the%20signal%20handshake%20protocol%20(2).pdf) · [Diving into Signal's new PQ protocol — PQShield](https://pqshield.com/diving-into-signals-new-pq-protocol/)

**WhatsApp** — [WhatsApp Architecture & System Design — CometChat](https://www.cometchat.com/blog/whatsapps-architecture-and-system-design) · [How WhatsApp Works — GetStream](https://getstream.io/blog/whatsapp-works/) · [WhatsApp scaling](https://singhajit.com/whatsapp-scaling-secrets/) · [WhatsApp Architecture — uptime.engineer](https://www.uptime.engineer/p/whatsapp-architecture) · [From Specs to Apps: Verifying and Monitoring Models of Signal and WhatsApp (arXiv)](https://arxiv.org/pdf/2609.11882)

**Cross-cutting** — [The Snowden Phone: comparative survey of secure messaging apps (arXiv)](https://arxiv.org/pdf/1807.07952)
