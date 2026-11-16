# Report 1: State of the Art

**Project:** P08 · Mensajería segura descentralizada, Sistemas Distribuidos 2026, Universidad de La Habana, Facultad de Matemática y Computación
**For:** the development team · **Date:** 2026-11-11

> **Revised 2026-11-11.** Sections 1 to 8 were written from the brief alone. The examiners have since split the project into two phases: phase 1 on Ray, tolerating two simultaneous failures (due 2026-12-01), and phase 2 without Ray, tolerating partitions (due 2027-01-01). **Section 9** covers Ray and says which statements in sections 7 and 8 change. Where an earlier section mentions a three-month timeline, gossip discovery or a write quorum of two, section 9 has the current position.

---

## Glossary 

> **DHT** = distributed hash table. **TTL** = time-to-live, how long something is kept before automatic deletion. **ADR** = Architecture Decision Record, our format for recording a decision in `docs/decisions/`. **SMP** = the SimpleX Messaging Protocol. **SWIM** = a gossip-based failure-detection protocol (the name is the paper's, not an acronym worth expanding). **MLS** = Messaging Layer Security, the IETF standard for group end-to-end encryption. **Sybil attack** = flooding a system with fake identities to gain disproportionate influence over it. **KDF** = key derivation function. **KEM** = key encapsulation mechanism, the post-quantum equivalent of a Diffie-Hellman exchange. **X3DH** = Extended Triple Diffie-Hellman, Signal's original asynchronous handshake; **PQXDH** is its post-quantum successor. **SGX** = Intel's hardware enclave technology, used in some private-contact-discovery designs. **HLC** = hybrid logical clock.

---

## 1. The brief

Build decentralized messaging with user identity, contact discovery, one-to-one conversations, delivery to offline recipients, store-and-forward, message ordering, duplicate suppression, local history, and authenticated encrypted communication. Temporary relays are allowed, but the architecture must not become a permanent central database holding all history.

That last sentence is the most important.Every other clause is a well-understood problem with a well-understood solution. Ordering has logical clocks. Duplicates have content hashes. Encryption has the Double Ratchet. But "deliver to someone whose device is switched off, without anyone permanently holding the conversation" is the genuine tension.

### Requirements

We decomposed the brief into nine numbered requirements, and we use these numbers throughout the project:

| # | Requirement | What it really asks for | The failure it tests |
|---|---|---|---|
| R1 | Identity and contact discovery | Self-authenticating identity, no registration authority | Can two nodes agree who someone is with no central registry? |
| R2 | One-to-one conversations | Per-pair session state | — |
| R3 | Delivery to offline recipients | Delivery succeeds with no process running at the other end | The recipient is *down*, not slow |
| R4 | Deferred delivery / store-and-forward | Someone holds the message, durably | The *holder* crashes before delivering |
| R5 | Message ordering | Reconstruct sensible order from out-of-order arrivals | The network reorders |
| R6 | Duplicate suppression | At-least-once transport, exactly-once display | Retry after a lost acknowledgement |
| R7 | Local persistence of history | The archive lives at the edge | The relay loses everything, history survives |
| R8 | Authentication and encryption | Nobody in the middle reads or forges | A malicious relay |
| R9 | **Temporary relays, no permanent central database** | Relays buffer, never archive | "Did you just rebuild a server?" |

R9 is the discriminating constraint: any architecture we propose has to answer, in one sentence, why the relay tier *cannot* reconstruct a conversation, not merely *won't*.

### The second constraint: Docker without Compose

The only general rule the course gave us is that we use Docker but not Docker Compose. Compose gives you a declarative topology, DNS names for every service, and a guaranteed startup order; removing it removes all three. What's left is a set of containers that must find each other at runtime, start in any order, and survive being killed and replaced. In effect, the rule is a disguised requirement for dynamic discovery and churn tolerance: during the defense, someone will `docker kill` a container and expect the system to heal.

Three consequences we designed around:

- **Discovery must be dynamic.** Seed addresses passed as environment variables plus gossip, with UDP broadcast as a zero-configuration convenience. We verify broadcast behaviour on Docker Desktop/WSL2 before depending on it, and keep a path that needs no multicast at all.
- **Startup must be order-independent.** Any container must be able to start first, alone, and converge when others appear.
- **Identity must not be the container.** A node is its keypair, not its IP or hostname, since both change on restart.

### What the brief does *not* ask for

One-to-one conversations only. No groups. That spares us the hardest part of modern secure messaging (group key agreement, MLS, sender keys), and, as we'll see in section 5, it also spares us Matrix's single most difficult component. Also absent: voice and video, file transfer, mobile clients, federation with real networks.

---

## 2. WhatsApp: the expectations users arrive with

The brief asks us to compare against WhatsApp for *user experience*, explicitly not to reuse its brand. That framing is correct, and we should resist the temptation to study its architecture too closely for a practical reason: there isn't much reliable material. WhatsApp publishes an encryption white paper and little else about its own infrastructure. Everything in 2.2 below is reconstructed from secondary blogs, which disagree on implementation specifics (storage engine, sharding, process model) but converge on the one behavioural claim we actually rely on: the server queues and discards rather than archiving. Treat the behavioural claim as solid and the implementation colour as illustrative.

### 2.1 The real contribution: a UX contract

What WhatsApp genuinely gives us is a set of user expectations so entrenched that violating any of them makes a messenger feel broken. Each one is a distributed-systems guarantee wearing a costume:

| What the user sees | What it actually guarantees | Our requirement |
|---|---|---|
| One grey tick, "sent" | Something outside my device durably accepted this | R4 |
| Two grey ticks, "delivered" | The recipient's device has it | R4, acknowledgement routed back |
| Two blue ticks, "read" | Their client displayed it | Application-level receipt |
| Messages never appear twice | Exactly-once display over at-least-once transport | R6 |
| Messages never appear out of order | Causal order preserved | R5 |
| Messages sent while I was away are waiting | Asynchronous handoff with retention | R3 |
| My history survives server problems | The archive is at the edge | R7 |

The ticks are the best idea here, and we're adopting them for a reason that goes beyond familiarity: they make our internal acknowledgement chain visible on screen. With a write quorum of two out of three replicas (section 7.4), killing a *single* relay after the quorum is already met should change nothing visible, which is itself worth demonstrating: it shows the quorum did its job. What should visibly matter is killing enough relays to break the quorum, or killing them before it's reached: the tick should stall at `pending` and recover to the double tick once the quorum is met again. Making the UI honestly distinguish "quorum met" from "quorum broken" is not yet designed; it's one of the open items in section 8.5.

We're adding a fourth state, which WhatsApp shows as a clock icon: `pending`, accepted by the client but not yet accepted by any relay, because none is reachable. It's the honest representation of a partition.

### 2.2 The one server-side idea worth stealing

WhatsApp started as a heavily modified fork of ejabberd, an Erlang XMPP server, and reports consistently describe one lightweight Erlang process per connected user with supervision trees underneath. The protocol was XMPP-derived before being replaced by something custom and binary.

But the architectural idea that matters to us is simpler, and the sources agree on it even where they disagree on everything else:

> The server routes if the recipient is online, queues if offline, drains in order on reconnect, and **deletes after the client acknowledges.** It is a transient relay, not a message archive.

Permanent history lives in the client's SQLite database. That is exactly the posture R9 demands. WhatsApp achieved it with a single trusted operator; our job is to achieve it with untrusted, replaceable relays.

### 2.3 What we reject

The centralized topology, obviously: one operator with one logical router is precisely what the brief forbids. Phone numbers as identity, which require a registration authority and leak the social graph. Server-side contact discovery by uploading an address book. And anything branded: no green, no name resemblance, no cloned iconography.

Our system will be far worse than WhatsApp at scale, latency, polish and battery life, and we should say so plainly at the defense. We compete on one axis only: decentralisation under failure.

---

## 3. Matrix: decentralization done thoroughly

Matrix is the most useful technical reference we have, because it's the only one of the three that is a published, versioned specification for replicating conversation state across servers that don't trust each other. We read the Server-Server API specification (v1.19) directly rather than relying on summaries, so this section is spec-backed.

### 3.1 The model

Users belong to homeservers (`@alice:example.org`). A conversation is a *room*, and a room is an append-only directed acyclic graph of signed events, replicated at every homeserver that has a participating user. Servers exchange events over the federation API. There's no central server and no global ordering. Consistency is eventual, and when concurrent branches merge, a deterministic state resolution algorithm picks a winner.

The structural fact that matters to us is that a user's history lives on their homeserver, permanently. Matrix is federated, not amnesiac, and that's exactly where it collides with R9.

### 3.2 How servers find and trust each other

This is directly relevant to our no-Compose problem, because it's the same question: how does one node find and authenticate another with no central registry?

Matrix resolves a server name through a chain: IP literal, then `.well-known` delegation over HTTPS, then an SRV record, then A/AAAA with port 8448. One detail is a nice piece of paranoia: the SRV lookup uses the *original* hostname rather than the delegated one, because DNS isn't trusted.

Each server publishes its signing keys at a well-known endpoint: a map of `ed25519:<id>` to public key, plus expired keys that can still verify historical events, a validity timestamp, and a self-signature. Every federation request carries an `X-Matrix` authorization header signing the canonical JSON of the method, URI, origin, destination and content. Including the destination means a request can't be replayed at a different server.

Then there's the detail we're stealing outright: **notary servers.** A third party can serve a cached, counter-signed copy of another server's keys *while that server is offline*. Key availability doesn't depend on the key owner being up, because the key record is self-verifying. Our relays will cache and serve each other's identity records the same way.

### 3.3 Transactions and what happens when a destination is down

Federation delivery is a `PUT` to a send endpoint carrying up to 50 PDUs (persistent events, part of the DAG) and 100 EDUs (ephemeral things: typing notices, read receipts, presence). A rejected PDU doesn't fail the transaction; the response is a 200 with per-event errors.

The spec states exactly one retry rule: the sending server must wait for a 200 before sending a transaction with a different transaction ID. So delivery is a per-destination *serialized* queue with ID reuse: the next transaction to a given destination doesn't go out until the current one is acknowledged, which is stop-and-wait, not pipelining.

What's striking is what the spec *doesn't* say: no backoff schedule, no queue bound, no drop policy, all left entirely to implementations. This is good news for us. We need exactly this component, and because the reference specification leaves the hard parts open, specifying them well (bounded outbox, exponential backoff with full jitter, hard TTL, explicit drop policy with visible consequences) counts as original work rather than copying.

### 3.4 The event DAG, which is the best idea in Matrix

Every event carries `prev_events`, defined as "all of the events in the room for which it has not yet seen a child", meaning the current frontier of the graph. It also carries `auth_events` (the specific events proving the sender was allowed to do this), a depth, a content hash, and signatures.

Incoming events run a validation pipeline that's worth copying as a discipline in its own right: invalid for the room version → drop; bad signature → drop; bad content hash → redact and continue; fails auth against its own `auth_events` → reject; fails against the state before it → reject; fails against *current* state → soft fail.

That last one is the interesting case. A soft-failed event isn't shown to clients and isn't referenced by new events, but it still participates in state resolution. It exists to stop someone evading a ban by branching from an old point in the graph.

When a server has been partitioned, it repairs the gap by walking the DAG: backfill backwards from known events, or a breadth-first walk over `prev_events` for specifically missing ones, or fetch the resolved state before a given event.

This is, we think, the single most transferable idea in Matrix. A conversation whose events reference their predecessors by hash is self-describing. A client that reconnects can see exactly what it's missing and ask for it. The hashes make duplicate suppression cheap, one index lookup rather than nothing (R6), and causal gaps detectable, though section 7's actual implementation still needs a persisted seen-set and a vector clock on top; the DAG gives us the raw material, not a finished mechanism. We adopt a simplified version: for a two-party conversation it's nearly a chain.

### 3.5 State resolution, and the scoping win

State is a map from `(event_type, state_key)` to an event. When partitioned branches merge, the room version's algorithm decides the result. Versions 2 through 11 use state resolution v2: split state into unconflicted and conflicted sets, add the auth difference, run iterative auth checks over a reverse-topological ordering. Room version 12 uses v2.1, which changed where iterative auth checks start in order to fix a state-reset vulnerability tracked as CVE-2025-49090.

This is widely regarded as the hardest and most bug-prone part of Matrix: an independent academic analysis was needed to pin down its behaviour, and rooms with large branching histories are a known scaling problem. The good part is that we don't need it.

State resolution exists because rooms have *mutable shared state* (membership, power levels, topic) that several servers can change concurrently. A one-to-one conversation between two fixed parties has essentially none. Our "room" has two members, decided at creation, and never changes.

The brief restricts us to one-to-one conversations, so we inherit the DAG's benefits (causal ordering, cheap deduplication, gap detection) while structurally eliminating Matrix's hardest component. It's the most valuable scoping insight in the whole study.

---

## 4. Signal: identity and the cryptography

Signal matters for one structural reason above all others: the prekey mechanism is the cryptographic solution to offline delivery. It lets A establish an encrypted session with B while B's device is switched off. That's R3 solved at the key-agreement layer, and no amount of clever networking substitutes for it.

### 4.1 Identity

A user's identity is a long-term key pair. Everything hangs off it. The phone number is a discovery convenience layered on top, not the identity itself, and Signal has been moving toward usernames to reduce phone-number exposure. Safety numbers let two users compare a fingerprint of both identity keys out of band, catching a man-in-the-middle at setup.

For us: identity is the hash of an Ed25519 public key, self-generated, with no authority involved. Contact discovery happens out of band through an invitation string. That sidesteps a problem Signal has *not* solved: private contact discovery has needed hashing schemes and SGX enclaves and remains contested. We should say clearly at the defense that we're stepping around an open research problem deliberately, not failing to notice it.

Safety-number comparison is cheap to implement and demonstrates R8 nicely.

### 4.2 Prekeys: how you encrypt to someone who is offline

Diffie-Hellman needs both parties online. Messaging doesn't have both parties online. The resolution is that B uploads a bundle ahead of time: a long-term identity key, a signed prekey (rotated periodically) with its signature, and a batch of one-time prekeys.

A fetches the bundle, verifies the signed prekey's signature and aborts if it fails, generates an ephemeral key, and performs several Diffie-Hellman operations combining identity, signed-prekey, one-time-prekey and ephemeral keys. The results are concatenated through a KDF into the initial shared secret. A can send immediately; B derives the same secret whenever it comes online.

Two operational details that matter more than they look:

- The server deletes each one-time prekey as it hands it out, and B deletes the private key after use. That protects the very first message (the one sent before any ratchet has had a chance to run) from later compromise of B's long-term state. The ratchet, covered next, is what extends forward secrecy to every message after that.
- If one-time prekeys run out, the protocol still works using only the signed prekey, at reduced forward secrecy. So the bundle store needs replenishing, and depletion must degrade rather than fail.

X3DH has since been replaced by PQXDH, which keeps the elliptic-curve steps and adds a KEM encapsulation against a static post-quantum key, protecting against harvest-now-decrypt-later attacks. Note the asymmetry: only confidentiality gains post-quantum protection, authentication still rests on discrete-log hardness. In 2025 Signal added the Triple Ratchet, pairing the classical Double Ratchet with a sparse post-quantum ratchet.

We implement classical X3DH. PQXDH is a future-work slide where we explain precisely what it adds and what it doesn't.

The part we improve on: in Signal, the prekey bundle lives on a *trusted* server. In our design it's a *self-verifying signed record* stored on untrusted relays, so a relay can't substitute one. If a bundle is unavailable the session simply can't start, which is a safe failure.

### 4.3 The Double Ratchet

After the handshake, two ratchets turn simultaneously.

The **DH ratchet**: each party attaches a fresh ephemeral public key to its messages. Receiving a new one triggers a Diffie-Hellman whose result is mixed into the root key, producing new chain keys. This gives post-compromise security: an attacker who steals your keys loses access once a fresh exchange they didn't observe completes, and the system heals itself.

The **symmetric ratchet**: within a chain, each message key comes from a KDF step that advances the chain key and discards the old one. This gives forward secrecy: past keys can't be recovered from current state.

The part that overlaps with our networking work: each message header carries the sending chain's ephemeral key, the message number, and the previous chain's length. A receiver that gets message 5 before message 3 stores the skipped message keys so 3 still decrypts when it arrives. Implementations must bound that store, or an attacker sends a message numbered two billion and exhausts memory.

So the crypto layer has its own out-of-order problem, and it must be solved even if the network delivers everything in perfect order.

### 4.4 Metadata, and where Signal stops

Signal's content encryption is strong, but the protocol by itself doesn't stop an operator recording who talked to whom and when. **Sealed sender** narrows this: the sender's identity goes inside the encrypted envelope and the sender authenticates with a short-lived delivery certificate, so the server learns the recipient but not the sender.

We can go further than sealed sender, precisely because we have no accounts, but exactly how far depends on a choice we haven't made yet. SimpleX's answer (section 6.2) is two separate, single-direction queues per conversation, each with its own identifier, specifically so a relay cannot tell that the same person operates both ends. A single shared per-conversation mailbox, the simpler design and the one we sketch in section 7, does **not** give that property: a relay that watches both parties deposit into, and poll, the same identifier can correlate them as conversation partners, which is exactly the linkage SimpleX's two-queue scheme exists to prevent. We haven't picked between the two yet; it's an open decision in section 8.5. Either way, there's no account for a relay to learn in the first place, which is already ahead of Signal's model.

---

## 5. The three, side by side

### 5.1 One sentence each

**WhatsApp** is a centralized router with end-to-end encryption added: one operator, a transient per-user queue, history archived only on the client, and a UX contract so refined it has become the industry's definition of "working".

**Matrix** is a federation of persistent servers replicating a signed event DAG per conversation, with eventual consistency and deterministic conflict resolution. It's the only genuinely decentralized one of the three, and the only one that keeps full history server-side forever.

**Signal** is a centralized server deliberately kept minimal, whose real contribution is cryptographic: public-key identity, prekey bundles, and the Double Ratchet.

Each is best in class on exactly one axis and weak on the other two. The brief's framing, WhatsApp for UX, Matrix for decentralized mechanism, Signal for identity and crypto, matches this precisely.

### 5.2 Architecture and trust

| | WhatsApp | Matrix | Signal |
|---|---|---|---|
| Topology | Single operator | Federation of homeservers | Single operator |
| Decentralized | No | **Yes** | No |
| Trusted for availability | The operator | Your homeserver and your peers' | The operator |
| Trusted for confidentiality | Nobody | Nobody | Nobody |
| Trusted for metadata | The operator | Every participating server | The operator, narrowed by sealed sender |
| One server/node fails | Operator absorbs it internally (redundant infra), but it's still one operator's call | Degraded: that server's users are cut off, the rest continue | Operator absorbs it internally, same caveat as WhatsApp |
| The operator itself fails | Total outage | N/A, no single operator | Total outage |
| Partition behaviour | Clients queue locally | **DAG forks, heals by backfill and state resolution** | Clients queue locally |

Only Matrix has anything to say about partitions, because only Matrix has more than one authority. That's why the brief points us there.

### 5.3 Identity

| | WhatsApp | Matrix | Signal |
|---|---|---|---|
| Identifier | Phone number | `@user:homeserver` | Phone number → usernames |
| Rooted in | A telecom operator | A DNS domain | An account |
| Self-authenticating | No | No | **Partly** |
| Survives the server dying | No | **No** | No |
| MITM detection | Security code | Device verification, cross-signing | **Safety numbers** |

A few things follow from this:

1. All three bind identity to an authority, and all three therefore fail the test "can your identity survive the infrastructure?". Ours won't: a user ID is the hash of a locally generated public key, owned by nobody.
2. Contact discovery is unsolved even by Signal. The cheap, correct escape is out-of-band invitations, which SimpleX and Briar both take. We take it too.
3. Matrix's notary trick transfers directly: a self-verifying key record means a third party can vouch for a key while its owner is offline.

### 5.4 The decisive dimension: who holds a message, and for how long

| | WhatsApp | Matrix | Signal |
|---|---|---|---|
| Holder for an offline user | The operator's queue | The recipient's homeserver | The operator's queue |
| Retention | **Until delivered, then deleted** | **Forever** | **Until delivered, then deleted** |
| Server is an archive? | **No, a queue** | **Yes, by design** | **No, a queue** |
| Permanent history lives | Client | Every participating server, plus clients | Client |
| Catch-up after being offline | Drain the queue | **Walk the DAG** | Drain the queue |

Here is the central tension of this project, stated precisely:

> WhatsApp and Signal have the **retention policy** the brief demands but the **topology** it forbids.
> Matrix has the **topology** the brief demands but the **retention policy** it forbids.

R9 asks for something **none of the three named references provides.** We read that as the point of the exercise, not an oversight in the brief. Our architecture has to be Matrix's decentralization with WhatsApp and Signal's amnesia.

### 5.5 Ordering and duplicates

| | WhatsApp | Matrix | Signal |
|---|---|---|---|
| Ordering | Server-assigned per conversation | **`prev_events` DAG**, partial causal order | Message number and previous-chain-length in the header |
| Consistency | Linear per conversation | **Eventual**, deterministic merge | Linear per session |
| Dedup key | Server message ID | **Event ID = content hash** | Message number within a chain |
| Out-of-order | Buffer and sort | DAG tolerates it; gaps repaired by backfill | **Skipped message keys stored** |
| Hardest part | — | State resolution | Bounding the skipped-key store |

A few mechanisms transfer directly: content-hash event IDs make deduplication cheap and forgery impossible; `prev_events` make causal gaps detectable; the message-number scheme with a bounded skipped-key store handles reordering at the crypto layer. And one problem, state resolution, we avoid by construction, as section 3.5 covers.

### 5.6 Cryptography

All three converge on the same core. Matrix reimplemented Signal's design as Olm (pairwise) and Megolm (groups); WhatsApp adopted it directly. For one-to-one messaging the Double Ratchet is the settled answer, and there's no interesting design choice left. That tells us our contribution has to be in the distributed systems layer, not the cryptography; we implement the standard construction over audited primitives and spend our ingenuity elsewhere.

One real difference remains. All three bind the offline-reachable key material to a *trusted* server. Making prekey bundles self-verifying so they can sit on untrusted replicated storage is, as far as our research found, a genuine point of difference, with the open replay caveat noted in section 6.1.

Matrix's end-to-end encryption is also not on by default everywhere; it has historically been opt-in per room, a long-standing criticism and a contrast with WhatsApp and Signal.

### 5.7 Technology stacks

| | WhatsApp | Matrix | Signal |
|---|---|---|---|
| Specification | Closed | **Open, versioned** | **Open protocol, open source** |
| Server | Erlang/OTP, originally a modified ejabberd | **Synapse** (Python/Twisted, AGPL-3.0, most complete) · **Dendrite** (Go, Apache-2.0, beta/maintenance) · **Conduit** (Rust, Apache-2.0; active fork Continuwuity) | **Signal-Server** (Java, AGPL-3.0) |
| Crypto library | Closed implementation of the Signal protocol | **libolm** (C/C++, deprecated) → **vodozemac** (Rust, current) | **libsignal** (Rust core, AGPL-3.0, with JNI, C FFI and TypeScript bindings) |
| Wire protocol | Custom binary, historically XMPP-derived | **HTTPS + JSON**, canonical-JSON signing, Ed25519 | Custom over HTTPS/WebSocket |
| Clients | Native | **Element Web** (TypeScript/React) plus a large ecosystem | Android (Kotlin), iOS (Swift), Desktop (Electron/TypeScript), all AGPL-3.0 |
| Concurrency | **One Erlang process per connected user** | Python async / goroutines / Rust async | JVM thread pools |
| Licence | Proprietary | AGPL-3.0 (Synapse), **Apache-2.0** (Dendrite, Conduit, vodozemac) | **AGPL-3.0 throughout** |

A few lessons stand out:

1. **Licence matters for library selection.** `libsignal` is AGPL-3.0; Matrix's `vodozemac` is Apache-2.0 and implements the same Double Ratchet. For a permissively licensed ready-made ratchet, vodozemac beats libsignal. Moot for us now that we're on Python (see section 8), but worth having checked before we committed.
2. **The process-per-user mental model is worth borrowing** in another language too, however solid the reports of WhatsApp's actual internals are (section 2's evidence note). Every runtime we considered has an equivalent (goroutines, async tasks, Python's `asyncio` tasks), and the supervision-tree habit, assume it crashes and restart it, is the posture a churn-tested project needs regardless of where we first read about it.
3. **Matrix proves a protocol can outlive its implementation.** Three independent homeservers in three languages interoperate because the specification is the contract. We should write our wire protocol down before implementing it, so our two node types are built against a spec rather than against each other.
4. **Rust core plus language bindings** is the industrial pattern for sharing one crypto implementation across platforms. Out of scope, but the right answer if anyone asks how we'd ship this on mobile.

### 5.8 Scorecard

● satisfies · ◐ partial · ○ fails or conflicts. Session and SimpleX are introduced properly in section 6; they're included here so the full comparison lives in one table.

| | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 | **R9** |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| WhatsApp | ◐ | ● | ● | ● | ● | ● | ● | ● | ◐ policy yes, topology no |
| Matrix | ◐ | ● | ● | ● | ● | ● | ◐ | ◐ | **○ permanent server history** |
| Signal | ◐ phone-number registration | ● | ● | ● | ● | ● | ● | ● | ◐ policy yes, topology no |
| Session | ● | ● | ● | ◐ no durability guarantee by design | ◐ | ● | ● | ◐ no forward secrecy | ● |
| SimpleX | ● | ● | ● | ◐ no replication, in-memory only | ◐ | ● | ● | ● | ● |
| **Target: ours** | ● | ● | ● | ● | ● | ● | ● | ● | ● |

Signal's R1 is marked partial rather than full because, as section 5.3 lays out, its identifier is still a phone number issued by an authority: the identity key underneath is self-authenticating, the account isn't. Session's R4 is marked partial because section 6.1 quotes the whitepaper's own admission that swarms provide "neither ordering nor durability guarantees"; SimpleX's spec goes further and recommends in-memory-only storage, so "no replication" undersold the gap in an earlier draft.

Nothing in the top three fills the bottom row. That's the thesis.

---

## 6. The systems nobody told us to read

If none of the three named platforms solves our problem, something must. We looked at five systems built for exactly this: Session and SimpleX solve it outright, each a different way; Briar solves a related but easier version of it; and Jami and Tox are here because they *don't* solve it, and the reason why turns out to be the most important lesson in this section. This is where our architecture actually comes from.

### 6.1 Session: swarms as replicated mailboxes

Session is the closest existing thing to what we need to build. We read the whitepaper directly. One caveat: it's the 2020 version, and its own addendum notes renamings and that parts of the token design weren't live, so the parameters are design intent rather than current fact.

**The swarm mechanism.** Every swarm has a deterministic 64-bit identifier. A user's account ID is reduced to a 64-bit integer and assigned to the swarm whose ID is numerically closest. Any client computes this locally from the node list; there is no resolver to ask. Placement costs zero network round trips, because it's a pure function of membership.

Swarm sizing uses three parameters: minimum 5, target 7, maximum 10. A swarm that drops below the minimum is "starving" and steals a node from a larger one; if every swarm is at minimum, the starving swarm is **dissolved** and its nodes redistributed. When every swarm is at maximum, a *new* swarm forms from seven randomly chosen excess nodes: new swarms are created, existing ones are never split.

Churn is handled by two rules. When a node joins a swarm, existing members **push** the swarm's records to it. When a node leaves a dissolving swarm, it must work out which swarms are now responsible for its records and distribute them there.

A node's swarm is assigned at registration, and the node has deliberately minimal influence over which one it joins, so an attacker can't concentrate malicious nodes in a single swarm.

**Storage and retrieval.** The sender asks a random node for the swarm mapping, then sends the message, carrying recipient ID, timestamp, expiry and a namespace, to one swarm member, which propagates to the others and returns a signed success. If a node is no longer responsible, it replies with the current membership, so mapping repair is driven by errors rather than polling. Recipients poll their own swarm with requests signed over type, namespace and timestamp, checked against a replay window.

No proof-of-work is required for storage; it's listed only as one of several unimplemented anti-spam candidates.

**The tradeoff we're taking the opposite side of.** Session deliberately abandoned ratcheting. Its stated reasoning is directly relevant: Signal-style protocols make strong assumptions about synchronising shared state and need durable storage for prekey bundles, and swarms provide neither ordering nor durability guarantees, and delete messages at expiry. So Session uses a stateless construction and accepts the cost: no forward secrecy for one-to-one chats, weakened deniability (messages are signed with the long-term key), and multi-device implemented by literally sharing the long-term private key.

We can keep the ratchet because our prekey bundles are *self-verifying*: a relay can't forge one, and an unavailable bundle means the session doesn't start, which is safer than a forged or stale one, though it does leave the bundle open to a replay of an *old but validly signed* version, which we haven't solved yet (section 8.5). Keeping the Double Ratchet *and* DHT-based mailboxes is, as far as our research turned up, a genuine point of difference, and it's our answer to "what did you do that the references don't?"

**What Session admits it doesn't do.** Nodes are tested for reachability (a node silent for over two hours is presumed offline, with testing quorums of ten reporting on sets of fifty), but not for whether they actually store anything. A record-hash challenge is proposed and unimplemented. Traffic analysis against a global passive adversary is described as trivial, since onion requests are low-latency and unbatched. Free account creation makes Sybil spam easy. Polling is "inefficient and slow".

The storage audit is cheap for us to implement (challenge a node to return a hash of a stored record plus a nonce), and it's a real improvement over the prior art.

### 6.2 SimpleX: no user identifiers at all

We read the SMP specification (v20) in full. It contains the most radical idea in the field: there are no user identities in the protocol. Not phone numbers, not usernames, not public keys, not random IDs. Only queues.

**The queue model.** A unidirectional queue lives on a relay and has *two different* random IDs, 16-24 bytes each: one for the recipient, one for the sender. The relay never learns they belong to the same participant. Clients must use a unique ephemeral key per queue, specifically so a compromised relay can't aggregate a user's queues by key. A two-way conversation is two queues, and the spec is explicit that pairing them is the application's problem.

Establishment happens through an **out-of-band invitation** carrying a queue URI. There are no global identifiers because there's nothing to identify, exactly the move that lets us skip contact discovery.

**Delivery semantics, which map straight onto R5 and R6.** The relay sends one message at a time and the next only after acknowledgement. It must delete on acknowledgement. And then the sentence we'll cite directly:

> A message can be re-delivered if the connection drops before ACK, so clients must deduplicate by message ID.

That's the at-least-once-transport / exactly-once-display split stated plainly in a production specification.

Relays must not keep logs, delivery history, or database snapshots; in-memory persistence is recommended. R9, written as a protocol requirement.

Everything is fixed-size padded (a 16,384-byte transport block, payloads padded to constant sizes), which removes message length as a metadata channel. The relay sees queue IDs, command types, timing and constant sizes. It cannot see message bodies, sender identity, or which other queues belong to the same participant.

**Where SimpleX is weak, and where we beat it.** The specification is candid: reliability is deprioritised, and redundancy is explicitly out of scope ("clients must manage redundancy by using more than one relay"). There is no failover, no queue replication, no migration. Relays never talk to each other.

So: Session defines replication but gives up the ratchet. SimpleX has opaque addressing but no replication. Our design takes SimpleX's opaque, padded, acknowledgement-deleted queues and puts them on Session-style replicated storage. We didn't find that combination in anything we surveyed.

### 6.3 Briar: delay tolerance as a design centre

The Bramble Transport Protocol is explicitly built for delay-tolerant networks. One detail we like: where a transport imposes a maximum connection length, BTP pushes that restriction *up* to higher layers by limiting stream length, rather than silently fragmenting across connections.

Offline delivery has three paths: a separate **Mailbox** application acting as a private encrypted intermediary (with transfers chunked into equal parts so an interrupted upload resumes), relaying through mutual contacts, and physical transport: USB sticks, SD cards, Bluetooth. Briar works with no internet at all.

Chunked resumable transfer is directly useful. "A mailbox is a dumb encrypted box owned by one user" is a good mental model. Mutual-contact relaying is elegant but needs a social graph we won't have.

A 2023 ETH Zurich semester report identified three design weaknesses in Briar, including a lack of forward secrecy in the handshake, and we should cite that honestly. We should check whether those were fixed before holding Briar up as a security model.

### 6.4 Jami and Tox: the honest failure of pure peer-to-peer

Jami uses OpenDHT for peer discovery with no account server. It's the cleanest example of the architecture we're tempted by, and its own documentation states the trap:

> The DHT only stores your message briefly. You miss your messages if you don't have clients logged into your account.

Jami's documentation further concedes that OpenDHT must run continuously to receive notifications, which mobile apps can't do, so Jami added a DHT proxy. A real deployment involves a bootstrap server, a proxy, a push server, a TURN server and a name server. Tox has the same gap; offline messaging has been a long-standing unresolved request.

**The lesson, and it's the central one for our architecture choice:** pure peer-to-peer where every peer is a user device *cannot* satisfy R3. Users' devices are offline exactly when their users are offline, that's the definition. Something that stays up must hold the message.

So an always-on tier isn't a failure of decentralization; it's a requirement of asynchronous messaging. The design question is not whether it exists, but how little it's allowed to know and how briefly it's allowed to remember. Jami answered by quietly adding servers back. We answer by making the always-on tier explicit, replicated, untrusted and amnesiac.

### 6.5 The family at a glance

| | Holder for an offline user | Retention | Identity | Replication | Forward secrecy |
|---|---|---|---|---|---|
| **Session** | Swarm of 5-10 nodes closest to the account ID | Per-message TTL | Ed25519 long-term key | Full, inside the swarm | **No**, deliberately dropped |
| **SimpleX** | One relay, opaque unidirectional queue | Until ACK or storage limit | **None**, only queue IDs | **None**, client's problem | Yes |
| **Briar** | A user-owned Mailbox, mutual contacts, or a USB stick | Until collected | Public key, exchanged out of band | None | ◐ disputed, see 6.3 |
| **Jami / Tox** | Nobody, in principle | Very short | Public key in a DHT | DHT-level only | Yes |

---

## 7. The toolbox

The mechanisms we'll actually assemble, and the evidence for choosing between them.

### 7.1 Where does a mailbox live? Chord, Kademlia, or neither

We need to answer "which nodes are responsible for user X's mailbox?" with no coordinator, and have everyone compute the same answer. (We'll call the per-mailbox replica count `k` from here on, to keep it visually distinct from the R1-R9 requirement numbers.)

| | Chord | Kademlia |
|---|---|---|
| Keyspace | Ring, successor-based | XOR metric |
| Routing state | O(log N) finger table | O(k · log N) k-buckets |
| Upkeep | Periodic stabilization required for correctness | **Lookup traffic doubles as neighbour discovery** |
| Under churn | More fragile | **Measurably better** |

Simulation studies in communication-oriented peer-to-peer settings find Kademlia wins on lookup path length (roughly 0.76 to 2.02 fewer hops on average, widening under skewed workloads) while Chord scales better on messaging cost at large N. Churn-focused evaluations favour Kademlia. Not unanimous: one comparison found Pastry lowest in hop count, and a hierarchical study favoured Chord-based hierarchies under very high churn. Most results are simulations with differing churn models, so treat the margins as indicative.

But at our scale there's a better answer than either. For ten to thirty containers, asymptotic differences are irrelevant and robustness under `docker kill` is everything. With tens of nodes, every node can simply know every other node. Then `placement(mailbox_id)` is the `k` nodes closest to a hash of the mailbox ID, computed locally by everyone, with zero lookup hops. No routing table, no iterative lookup, no bootstrap complexity.

This is consistent hashing with full membership, which is the same idea behind Session's swarm assignment, though not Session's swarm *sizing and lifecycle* mechanics, which we don't adopt (section 8.2 explains why). It's far simpler to implement correctly in three months, and it's honest: we note that it's O(N) state per node, and we can explain precisely how we'd swap in Kademlia routing if N grew. That explanation is itself the distributed-systems content; implementing Kademlia badly would be worth less.

### 7.2 Membership and failure detection

Our discovery mechanism, in order of how much we rely on it:

1. **Seed addresses plus gossip (primary).** A node receives zero or more seeds via environment variables, gossips with any it reaches, and learns the full membership. No multicast required. A node started with no seeds forms a cluster of one and is joined later when someone seeds *to* it.
2. **UDP broadcast on the Docker bridge (convenience).** Enables a zero-configuration demo. Must be verified on Docker Desktop/WSL2 before we depend on it.
3. **Subnet scan (last resort).** Ugly, always works, makes a decent fallback story.

For liveness we use **SWIM**: ping a random peer; on no reply, ask k others to ping it indirectly, ruling out a single bad link; then mark suspect, then dead after a timeout, disseminating the verdict by piggybacking on normal gossip.

Two cautions from the literature. SWIM is a failure detector, not a consensus protocol: it converges views, it doesn't guarantee agreement at any instant. And gossip membership allows views to diverge; the Rapid paper documents communication failures between live processes causing repeated accusation and refutation. Detection latency is tunable; one implementation reports around 5.5 seconds with default settings, and for our demo it should be a few seconds so a killed relay visibly heals within the examiner's attention span.

The industrial pattern is SWIM for liveness plus Raft for decisions that must be agreed; Consul does exactly this. We're using SWIM and no Raft, for message storage. We don't need agreement on membership for that: we need convergence, plus a replication scheme that tolerates disagreement at the edges, and temporary divergence costs at worst a write to a placement set that anti-entropy later repairs. That argument does *not* automatically extend to every mechanism we borrow from Session; see the flag in section 8.2 about swarm sizing and dissolution, which *are* allocation decisions and would need the agreement we're deliberately avoiding if we adopted them as Session defines them.

### 7.3 Ordering

| | Lamport | Vector clock | Hybrid logical clock |
|---|---|---|---|
| State | One integer | One per participant | Physical time + counter |
| Guarantee | a→b implies L(a)<L(b); converse false | **Full causality** | Causality-preserving, close to real time |
| Detects concurrency | No | **Yes** | Approximates |

Causal ordering is the right level for chat (a reply mustn't appear before the message it answers), and total order would need consensus latency for no benefit. The standard objection to vector clocks is that they grow with the number of participants.

A one-to-one conversation has exactly two participants, though, so a vector clock is just two integers, which is the second scoping win in this report. We use a two-element vector clock per conversation for causal delivery, plus hash references to predecessor messages so gaps are detectable, plus a wall-clock timestamp for display only, never for ordering.

Causal buffering rule: hold a message until its predecessors have been delivered. Bound the buffer, and after a timeout surface a visible gap rather than blocking the conversation forever. That's a deliberate availability-over-consistency choice.

### 7.4 Replication and anti-entropy

**Placement:** the `k` nodes closest to the mailbox hash. Start at `k` = 3.

**Write:** send to all `k`, acknowledge the sender once a write quorum of `w` = 2 confirms. One-failure durability at modest latency. The user's first tick appears exactly when the quorum is met, which makes the quorum visible during the demo.

**Read:** poll any relay in the placement set, or several and union the results. We're choosing not to require a read quorum, but that's a deliberate availability trade-off with a real cost, not a free asymmetry: a client that happens to poll the one replica that missed the write sees a gap until anti-entropy catches it up. For ordinary messages that's acceptable, because the sender's own acknowledgement timeout or the client's retry surfaces the problem either way. It does *not* hold for one-time prekeys, which are meant to be consumed exactly once: reading one from each of several replicas independently can hand the same prekey to two different senders, exactly what Signal's single trusted server exists to prevent (section 4.2). We don't have an answer for that yet; see section 8.5.

**Anti-entropy:** the relays in a placement set periodically exchange a Merkle digest of message IDs per mailbox and pull what's missing, so the common case, where nothing differs, costs one hash comparison. Plus push-on-join and hand-off on departure, following Session's rules (with the caveat in section 8.2 about which of Session's rules those are).

**Deletion** happens on two independent paths, and both matter for R9: acknowledgement-driven, propagated as a tombstone so a stale replica can't resurrect the message; and TTL-driven, independent of acknowledgement. The second is the structural guarantee: even a relay that never sees an acknowledgement forgets.

**Storage audit:** challenge a node to return a hash of a stored record plus a nonce. Closes the gap Session admits.

### 7.5 Duplicate suppression

At-least-once transport is unavoidable, so the split is at-least-once delivery, exactly-once display.

Message IDs are deterministic content hashes, so a retransmission produces the *same* ID. Clients keep a persisted seen-set with a unique index, so inserting a duplicate simply fails. Relays deduplicate on store too. Every operation is idempotent: storing a message already present succeeds, acknowledging twice is harmless, deleting an absent message succeeds. A Bloom filter over seen IDs is a cheap optimisation for the anti-entropy exchange.

### 7.6 Retry and the outbox

Matrix specifies the mechanism but deliberately omits the policy, so we define it: exponential backoff with full jitter, base around 500 ms, cap around 30 s; a bounded per-destination outbox that fails the oldest entry on overflow and surfaces it in the UI; a hard message TTL; and idempotent transaction IDs so a retry after an unseen success is harmless.

### 7.7 What we are deliberately not building

We'd rather name these clearly than half-build any of them.

- **Consensus (Raft/Paxos).** We need convergence, not agreement.
- **State resolution.** Avoided by construction: no mutable shared state in a two-party conversation.
- **Group messaging / MLS.** Out of scope per the brief.
- **Sybil resistance, who may join the overlay.** This is a different question from whether a relay is trusted with content: our cryptography and addressing are built to assume any individual relay could be malicious and still leak nothing useful. What we're not solving is *open* membership; Session needs staking and a blockchain to stop anyone spinning up arbitrary relays. For the demo we run a known, fixed set of relay processes we start ourselves; we explain in the report what an open deployment would need on top of that.
- **Onion routing.** Interesting, orthogonal, a stretch goal at best.
- **NAT traversal, STUN, TURN.** Irrelevant inside a Docker network, and worth saying, because it's a large part of why Jami and Tox are complicated.
- **Multi-device.** Multiplies session state. Explain the design, don't build it.
- **Post-quantum key agreement.** Explain, don't build.

---

## 8. Where this leaves us

### 8.1 The finding

Restating the conclusion from section 5.4: no named reference satisfies R9. The combination exists only in the family the brief doesn't name.

### 8.2 What we take from each

**From WhatsApp:** the delivery-state UX, including a `pending` state for "no relay reachable"; "the server is a queue, not an archive"; deletion on acknowledgement; local SQLite as the only permanent home of history; supervision-tree thinking.
*Refused:* centralized topology, phone-number identity, address-book upload, anything branded.

**From Matrix:** content-hash event IDs; predecessor references for causal ordering; the drop/redact/reject/soft-fail validation pipeline; per-destination transaction queues with idempotent IDs; gap repair by graph walk; canonical-JSON request signing including the destination; self-published signing keys with notary caching; "try another server" error semantics.
*Refused:* server-bound identity, permanent per-server history, the full federation surface.
*Avoided by construction:* state resolution.
*Filled in ourselves:* the backoff, queue bound and drop policy the spec leaves open.

**From Signal:** public-key identity; prekey bundles as the cryptographic answer to offline delivery; signed prekeys with verify-or-abort; one-time prekeys with graceful depletion; the Double Ratchet; the bounded skipped-key store; safety-number verification; sealed sender's goal.
*Refused:* the centralized prekey and routing server, phone-number identity, server-side contact discovery.
*Improved on:* metadata, via opaque mailbox addressing.

**From Session:** deterministic placement computed locally from the full membership view, with no resolver to ask; push-on-join sync; error-driven mapping repair; signed, replay-windowed retrieval; TTL-bounded storage.
*Not* adopted, despite living in the same research note: Session's minimum/target/maximum swarm sizing with starvation-stealing and dissolution. Those mechanics exist because Session assigns *nodes* to persistent groups, a different placement scheme from our flat consistent hashing (section 7.1). We keep them in mind only as the path to follow if the node count outgrows full membership.
*Refused:* abandoning the ratchet; token staking.
*Filled in ourselves:* write quorum, partial-propagation consistency, storage auditing.

**From SimpleX:** opaque queue addressing; fixed-size padding; delete-on-acknowledgement; mandatory client-side deduplication; relays that keep no logs or snapshots.
*Added:* replication, which SimpleX explicitly leaves out of scope.

**From Briar:** chunked resumable transfer; delay tolerance as a design centre.

**From Jami and Tox:** the proof that pure peer-to-peer cannot meet R3.

### 8.3 The architecture this points to

Two tiers. Relay nodes form a gossip-maintained overlay and hold consistently-hashed, replicated, TTL-bounded mailboxes keyed by opaque identifiers. Client nodes hold all plaintext, all identity, and all permanent history.

The five candidate architectures this was chosen from, with their tradeoffs and a comparison matrix, are in [`../design/architecture-options.md`](../design/architecture-options.md). The decision is recorded in [ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md); the Python and React stack in [ADR-0002](../decisions/0002-python-backend-react-frontend.md).

### 8.4 Two things we claim

1. **We keep forward secrecy where Session gave it up.** Session dropped ratcheting because swarms offer no durable prekey storage. Self-verifying signed prekey bundles let us keep the Double Ratchet on untrusted replicated storage.
2. **We add replication where SimpleX explicitly declines to.** The SMP specification pushes redundancy onto the client and defines no failover. We put opaque queues on replicated storage.

### 8.5 Known gaps and open decisions

This report settles the architecture and the technology stack. It deliberately does not settle everything below: these are implementation decisions for the whole team, not conclusions a reading of the literature can produce by itself. They surfaced during review of this report, which is exactly the point of writing it down now rather than discovering them mid-implementation.

**Protocol parameters, none of these has a value yet:**

- **Message TTL**, and critically, what happens when it expires before delivery. R3 (deliver while offline) and R9 (don't become an archive) collide at exactly this point, and the collision needs an explicit, user-visible answer, whether the sender finds out, the tick reverts, or the message is just gone, not an implicit one.
- Anti-entropy interval between replicas.
- Outbox bound per destination, and the skipped-message-key store bound (section 4.3 notes that an unbounded store is a denial-of-service vector but doesn't give it a number).
- Causal reorder buffer size and timeout (section 7.3).
- SWIM ping interval and suspicion timeout, tuned so a killed relay heals within a demo-length attention span; the shape is right, the numbers aren't picked.
- Polling interval, and prekey batch size / replenishment threshold.

These belong in one parameter table, which is the next concrete deliverable after this report.

**Design questions this report raises but doesn't close:**

- **One mailbox per conversation, or two (one per direction), as SimpleX does?** Section 4.4 flags this directly: it decides how much a relay can correlate, which is the report's central privacy claim. → ADR-0007.
- **How does a client, not just a relay, learn the overlay membership it needs to compute placement?** Section 7.2's gossip mechanism is specified for relay-to-relay discovery only.
- **One-time prekey consumption without a read quorum** (section 7.4): needs an actual mechanism, not just a flag.
- **Replica count `k`.** We argue for 3 on scale grounds (section 7.1), not yet weighed against Session's 5-10, which solves a different problem (long-lived node groups, not a per-mailbox hash).
- **Does a relay persist to disk at all**, or is replication the only durability mechanism? SimpleX recommends in-memory-only; R4 (durable store-and-forward) needs an explicit answer either way.
- **A threat model, stated once**, naming the adversary: honest-but-curious relay? Active? How many of the `k` colluding? Every "the relay learns nothing" claim in this report implicitly assumes one.

**Things to verify before building:**

- **Does UDP broadcast work on our Docker setup?** Determines whether zero-configuration discovery is possible. An afternoon's work; shapes ADR-0004.
- **Does the course require a written report in Spanish?** Cheap to ask now, expensive to discover in month three.
- **Were the Briar weaknesses in the 2023 ETH Zurich report fixed?** Check before citing Briar as a security model.
- **Confirm the licences** of PyNaCl, `cryptography`, and any Double Ratchet package before the written report cites them.

**Not attempted here, and belonging to the implementation plan instead:** the wire protocol and message schemas for both node types; a local storage schema (tables and indices; the seen-set in section 7.5 needs a unique index); a failure-injection-to-requirement test matrix (a way to demonstrate each of R1-R9 failing and recovering); and a work breakdown across the three of us.

---

## 9. Ray, and the rules that arrived after this report

Sections 1 to 8 were written from the brief. On 2026-11-11 the examiners added rules that the brief does not contain, and one of them names a technology none of our reading had covered. This section brings the report up to date. The full study of Ray is [`../research/07-ray.md`](../research/07-ray.md).

### 9.1 What the examiners added

- The project has two deliveries: **2026-12-01** and **2027-01-01**.
- The first is built on **Ray** and Python. We must use every Ray feature that applies to the project naturally, and must not reimplement what Ray offers.
- The first must survive **any two elements failing at the same time**. The head node is the exception: it will not be killed. Network partitions do not have to be survived.
- The second delivery **removes Ray completely**. Everything is redesigned and implemented by us, and partitions must be survived.
- Throughout, we are expected to think through cases and edge cases and to say what each decision gains and loses, because distributed systems have problems that nothing solves completely.

Two requirements therefore join R1 to R9: **R10**, tolerance of two simultaneous failures, and **R11**, tolerance of partitions (phase 2 only).

### 9.2 What Ray is

Ray is an open-source framework for running Python across a cluster of machines. It was built at UC Berkeley for machine-learning workloads, which is why it did not appear in a study of messaging systems. Underneath the machine-learning libraries is a small general-purpose core with three ideas:

- A **task** is a function that runs in another process.
- An **actor** is an object that lives in its own process and keeps state between calls. Other processes call its methods remotely. Actors can be given names, and any process in the cluster can look one up by name.
- An **object** is an immutable value in a store shared across the cluster.

A Ray cluster has one **head node**, which keeps the directory of the cluster (which nodes are alive, where each actor is), and any number of worker nodes. A node joins by being given the head's address.

### 9.3 What Ray does when things fail

This is the part that matters most to us.

- Ray notices when a node or a process dies.
- If told to (`max_restarts`), Ray **restarts a dead actor**, on another node if its own node is gone.
- The restarted actor is **empty**. Ray runs its constructor again and restores nothing. The documentation is direct about it: for actors with important state, "the application is responsible for recovering the state".
- A call that fails may or may not have run; Ray says there is "no guarantee on whether the actor executed the task or not". Ray can retry calls automatically, and then a call may run twice.
- Ray gives no ordering guarantee for calls to the kind of actor we need.
- If the head node fails, "the entire Ray cluster fails".

### 9.4 How Ray lines up with the toolbox in section 7

| Section 7 topic | With Ray in phase 1 |
|---|---|
| 7.1 Where a mailbox lives | Still ours, and simpler: Ray keeps a fixed set of named relays alive, so the hash ring never changes |
| 7.2 Membership and failure detection | Ray's. We build none, and are told not to |
| 7.3 Ordering | Still ours. Ray orders nothing for us |
| 7.4 Replication and repair | Still ours, and the heart of phase 1. Ray restarts relays empty; we refill them |
| 7.5 Duplicate suppression | Still ours, and needed more: Ray's retries can repeat a call |
| 7.6 Retry and the outbox | Still ours; Ray's retries sit underneath |

So Ray takes over the plumbing we had planned to write by hand and leaves the distributed-data problems where they were. One line summarises the phase 1 design: Ray keeps the relays alive, and we keep the data alive.

### 9.5 What Ray costs

- **A single head node.** It holds no messages, so R9 is unaffected, but it is a single point of failure for the cluster. The examiners have put it out of scope.
- **One trust domain.** Ray's documentation says it "expects to run in a safe network environment and to act upon trusted code". Inside one Ray cluster we cannot claim relays distrust each other. Section 8.4's claims about cryptography stand; the claim about untrusted relays waits for phase 2.
- **Heavier nodes.** A Ray node runs several system processes besides ours.
- **Features that look useful and are not.** Ray's object store ties an object's life to the process that created it, which would make a stored message vanish when its sender went offline. Ray's recipe for restoring actor state uses a checkpoint on shared storage, which would be the central database R9 forbids. Using everything that applies naturally also means recognising what does not.

### 9.6 What two simultaneous failures change

Section 7.4 worked with three replicas and confirmation at two. That is not enough: the two that confirmed could be the two that fail. To survive two crashes a message must be on **three** relays before the sender is told it was sent.

A majority quorum, which section 7.4 also discusses, is not needed in phase 1. Majorities exist to tell a crashed node from an unreachable one, and phase 1 has no partitions. Phase 2 does, and there the question reopens. Our direction for phase 2 is to stay available on both sides of a partition and merge afterwards, which is safe for us because envelopes never change and are named by their content.

### 9.7 What this does to section 8

- **8.3, the architecture:** unchanged in shape. The words "gossip-maintained overlay" describe phase 2. In phase 1 the relays are Ray actors and Ray maintains the overlay.
- **8.5, parameters:** the SWIM intervals belong to phase 2. Replica count and write rule are now proposed in [ADR-0005](../decisions/0005-three-copies-before-sent.md): three copies, all confirmed, five relays.
- **8.5, "does a relay persist to disk at all":** no. Memory only, with three copies as the durability mechanism.
- **8.5, "how does a client learn the overlay membership":** in phase 1 it does not need to; relays are found by name. The question returns in phase 2.
- **8.5, "a threat model, stated once":** for phase 1, relays that follow the protocol but would read what they store; one operator; a private network.
- **8.5, UDP broadcast on Docker:** not needed in phase 1. Relevant again in phase 2.
- **8.5, "a failure-injection-to-requirement test matrix":** now exists in outline in [`../design/fault-tolerance.md`](../design/fault-tolerance.md), together with eighteen edge cases and the tradeoff of each decision.

The design that follows from all this is in Report 2, sections 8 to 15.

---

## Appendix: Sources

**Read directly, in full or in substantial part**

- [Matrix Server-Server API specification, v1.19](https://spec.matrix.org/latest/server-server-api/)
- [SimpleX Messaging Protocol specification, v20](https://github.com/simplex-chat/simplexmq/blob/stable/protocol/simplex-messaging.md)
- [Session whitepaper, arXiv 2002.04609](https://arxiv.org/abs/2002.04609) (via [ar5iv HTML](https://ar5iv.labs.arxiv.org/html/2002.04609))
- [X3DH specification, signal.org](https://www.signal.org/docs/specifications/x3dh/) · [Double Ratchet specification, signal.org](https://www.signal.org/docs/specifications/doubleratchet/): the primary sources for section 4, not yet cross-checked line by line against our account, flagged for verification

**Matrix:** [Room Version 2 / state resolution](https://spec.matrix.org/v1.19/rooms/v2/) · [Matrix ecosystem: servers](https://matrix.org/ecosystem/servers/) · [Synapse vs Dendrite vs Continuwuity (2026)](https://www.pistack.xyz/posts/2026-05-02-synapse-vs-dendrite-vs-continuwuity-self-hosted-matrix-server-guide/) · [Matrix Decomposition: independent analysis of state resolution](https://matrix.org/blog/2020/06/16/matrix-decomposition-an-independent-academic-analysis-of-matrix-state-resolution/) · [The Matrix Reloaded: mechanized formal analysis (arXiv)](https://arxiv.org/pdf/2408.12743) · [A Glimpse of the Matrix: scalability issues (arXiv)](https://arxiv.org/pdf/1910.06295)

**Signal:** [signalapp/Signal-Server](https://github.com/signalapp/Signal-Server) · [signalapp/libsignal](https://github.com/signalapp/libsignal) · [Signal Protocol, Wikipedia](https://en.wikipedia.org/wiki/Signal_Protocol) · [Signal (software), Wikipedia](https://en.wikipedia.org/wiki/Signal_(software)) · [A Comprehensive Study of the Signal Handshake Protocol (NIST PQC 2025)](https://csrc.nist.gov/csrc/media/events/2025/sixth-pqc-standardization-conference/a%20comprehensive%20study%20of%20the%20signal%20handshake%20protocol%20(2).pdf) · [Diving into Signal's new PQ protocol, PQShield](https://pqshield.com/diving-into-signals-new-pq-protocol/)

**WhatsApp:** [CometChat architecture overview](https://www.cometchat.com/blog/whatsapps-architecture-and-system-design) · [GetStream deep dive](https://getstream.io/blog/whatsapp-works/) · [WhatsApp scaling](https://singhajit.com/whatsapp-scaling-secrets/) · [uptime.engineer](https://www.uptime.engineer/p/whatsapp-architecture) · [From Specs to Apps: Verifying and Monitoring Models of Signal and WhatsApp (arXiv)](https://arxiv.org/pdf/2609.11882)

**Decentralized alternatives:** [SimpleX platform overview](https://simplex.chat/docs/simplex.html) · [Bramble Transport Protocol v4](https://code.briarproject.org/briar/briar-spec/blob/master/protocols/BTP.md) · [Briar Mailbox chunking](https://code.briarproject.org/goapunk/briar-repeater/-/wikis/Mailbox-Chunking) · [Cryptography in the Wild: Briar (ETH Zurich, 2023)](https://ethz.ch/content/dam/ethz/special-interest/infk/inst-infsec/appliedcrypto/education/theses/report_YuanmingSong.pdf) · [How does Jami work on mobile without a server?](https://jami.net/jami-and-proxys/) · [Session overview, nym.com](https://nym.com/blog/what-is-session)

**Distributed systems:** [Chord vs Kademlia in high churn (CAIDA catalog)](https://catalog.caida.org/paper/2014_a_medrano_chavez_ppna_8_807) · [Comparing two popular DHTs in interpersonal communication](https://www.researchgate.net/publication/224239543_Comparing_the_performance_and_efficiency_of_two_popular_DHTs_in_interpersonal_communication) · [A performance vs. cost framework for DHT design tradeoffs (MIT, INFOCOM'05)](https://pdos.csail.mit.edu/~strib/docs/dhtcomparison/dhtcomparison-infocom05.pdf) · [Stable and Consistent Membership at Scale with Rapid (arXiv)](https://arxiv.org/pdf/1803.03620) · [Vector Clocks and Distributed Snapshots, Princeton COS 418](https://www.cs.princeton.edu/courses/archive/fall18/cos418/docs/L4-vc.pdf) · [Logical clocks, UMass CS677](https://lass.cs.umass.edu/~shenoy/courses/spring22/lectures/Lec13_notes.pdf)
