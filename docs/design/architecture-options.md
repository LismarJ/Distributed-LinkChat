# Architectural and Design Options — A Comparative Study

**Status:** design study; decision taken in ADR-0001 · **Date:** 2026-11-10

> **Update 2026-11-11: how to read this document now.** The project has two phases ([`phase-plan.md`](../planification/phase-plan.md)). The comparison of options A to E and the choice of option C stand for both.
>
> - **Phase 1 (Ray, due 2026-12-01)** is designed in [`ray-architecture.md`](ray-architecture.md). For phase 1, ignore what this document says about gossip discovery, SWIM, the wire protocol, the option D fallback, "mutually distrusting" relays, the write quorum of two, and the technology options in section 9.
> - **Phase 2 (no Ray, partition-tolerant, due 2027-01-01)** starts from this document. Those same parts are its first draft. Two things in it need revising for phase 2: the replication rule must be argued for partitions, and the timeline references ("three months", "week-1 spike") are out of date.
**Inputs:** [`../research/00-problem-analysis.md`](../research/00-problem-analysis.md) · [`../research/05-reference-platforms-comparison.md`](../research/05-reference-platforms-comparison.md) · [`../research/04-decentralized-alternatives.md`](../research/04-decentralized-alternatives.md) · [`../research/06-distributed-primitives.md`](../research/06-distributed-primitives.md)

---

## 1. The design space

Every architecture for this problem is an answer to one question:

> **The recipient's device is switched off. Who holds the message, for how long, and how do we find them?**

The answer determines everything else. There are only four fundamentally different answers, and a fifth that is a combination:

| Answer | Architecture |
|---|---|
| "The recipient's own server holds it, forever" | **A — Federated homeservers** |
| "Nobody; both parties must be online" | **B — Pure peer-to-peer** |
| "A computed subset of an always-on relay tier holds it, briefly" | **C — Two-tier relay overlay with replicated mailboxes** |
| "Everybody holds a bit of everything, briefly" | **D — Epidemic / gossip flooding** |
| "A strongly-consistent replica group holds it" | **E — Partitioned cluster with consensus** |

Two hard constraints prune this space before we score anything:

- **R9** (relays must not become a permanent central archive) eliminates A in its natural form.
- **R3** (delivery to offline recipients) eliminates B in its natural form.

That is not a reason to skip them. The defense will ask why we did not do the obvious thing, and "we evaluated it and here is the specific requirement it fails" is the answer that earns marks.

---

## 2. Option A — Federated homeservers (Matrix-lite)

```
  Client A ──┐                           ┌── Client C
             ├─► Homeserver 1 ◄════════► Homeserver 2 ◄──┤
  Client B ──┘   (owns A, B,             (owns C, D)     └── Client D
                  full history)           full history
                       ▲                        ▲
                       └── federation: signed transactions,
                           per-destination queue, retry, backfill
```

Users belong to a homeserver. The homeserver holds their identity, their queue, and their history. Servers federate by exchanging signed events.

**Mechanisms:** signed event DAG · per-destination transaction queue with idempotent `txnId` · backfill for gap repair · self-published signing keys · server discovery.

**Advantages**
- Offline delivery is trivially solved: your homeserver is always up and holds your messages.
- Best-documented prior art in the entire study; we could follow a real specification.
- Clean, legible architecture that maps directly onto the brief's phrase *"estado replicado"*.
- Modest implementation risk — essentially two well-understood components.

**Disadvantages and tradeoffs**
- **Conflicts with R9.** A homeserver is a permanent database of its users' full history. We could TTL-delete after delivery, but then the homeserver is no longer a Matrix-style homeserver and we lose the thing that made this option attractive.
- **Identity dies with the server.** `@alice:server1` is meaningless once `server1` is gone — and in our demo the grader *will* kill `server1`. We would need to replicate each homeserver, which turns this into option E.
- **Thin on distributed-systems content.** Discovery is trivial, placement is trivial, there is no churn problem to solve. We would be building a federated web service.
- The examiners know Matrix is the reference; copying its shape invites "what did you contribute?"

**Verdict:** rejected as the primary architecture. Its *mechanisms* (transaction queues, DAG, signing) are adopted wholesale inside option C.

---

## 3. Option B — Pure peer-to-peer, every client a node

```
   Client A ◄──────► Client C
      ▲  ╲          ╱  ▲
      │   ╲        ╱   │        Kademlia DHT over user devices.
      ▼    ╲      ╱    ▼        Direct encrypted connections.
   Client B ◄──────► Client D
```

Every user device joins a DHT, finds peers by public key, and connects directly.

**Mechanisms:** Kademlia routing · public-key identity · direct encrypted transport.

**Advantages**
- Maximally decentralized — no infrastructure at all. The purest answer to R9: nothing can be a central database because nothing is permanent.
- Rich in classic distributed-systems content (DHT routing, k-buckets, churn).
- NAT traversal, normally the dominant difficulty, is **free inside a Docker network** — a real and unusual advantage in our setting.

**Disadvantages and tradeoffs**
- **Fails R3, which is the point of the project.** Users' devices are offline exactly when their users are offline. Jami documents this plainly — *"you miss your messages if you don't have clients logged into your account"* — and Tox has the same long-standing gap.
- Every real system that started here added an always-on tier back in disguise: Jami needs a bootstrap server, a DHT proxy, a push server, a TURN server and a name server.
- Storing mailboxes *on the DHT itself* fixes R3 — but a DHT of user devices has catastrophic churn, and the moment we add stable nodes to hold data, we have built option C.

**Verdict:** rejected, and **the reason is the most instructive finding in the whole study.** Lesson for the defense: *an always-on tier is not a failure of decentralization; it is a requirement of asynchronous messaging. The design question is not whether it exists, but how little it is allowed to know and how briefly it is allowed to remember.*

---

## 4. Option C — Two-tier relay overlay with swarm-replicated amnesiac mailboxes ★

The synthesis of Session (replicated swarms), SimpleX (opaque queues, padded ciphertext, ACK-deletion), Signal (prekeys + ratchet) and Matrix (causal DAG, retry discipline).

```
                      RELAY OVERLAY  (gossip membership, consistent hashing)
        ┌───────────────────────────────────────────────────────────────┐
        │   R1      R2      R3      R4      R5      R6   ...            │
        │    └───────┴───────┘                                          │
        │   swarm(mailbox_AB) = 3 nodes closest to H(mailbox_AB)        │
        │   each holds: {opaque mailbox id → padded ciphertext + TTL}   │
        └───────────────────────────────────────────────────────────────┘
              ▲  store (W=2 of R=3)              ▲  poll / subscribe, ACK
              │                                  │
         ┌────┴─────┐                      ┌─────┴────┐
         │ Client A │                      │ Client B │
         │ SQLite:  │                      │ SQLite:  │
         │ the ONLY │                      │ the ONLY │
         │ history  │                      │ history  │
         └──────────┘                      └──────────┘
```

**Two node types, both containers.** *Relay nodes* form the overlay and hold nothing but ciphertext. *Client nodes* hold identity, history and all plaintext, and serve a small web UI to the examiner's browser.

**Core mechanisms**

1. **Identity.** `UserID = H(Ed25519 identity public key)`. Self-generated, no authority. Contact discovery is out-of-band: an invitation string carrying the public key and a mailbox seed.
2. **Membership.** Relays discover each other by seed address + SWIM-style gossip, with UDP broadcast as a zero-config convenience. Every relay converges on the full node list.
3. **Placement.** `swarm(mailbox_id)` = the **R = 3** relays whose node IDs are closest to `H(mailbox_id)`. Computed locally by anyone holding the membership view — **no lookup hops, no resolver** (Session's design).
4. **Mailboxes are opaque.** A conversation's mailbox IDs are derived from a shared secret established at introduction, not from either user's identity. A relay sees deposits into `0xA3F…` and cannot tell who sends or who collects (SimpleX's insight, pushed further than Signal's sealed sender).
5. **Store.** Sender writes the padded ciphertext envelope + TTL to all R replicas, acknowledges to the user at **W = 2**. One grey tick appears exactly when the write quorum is met — *the quorum is visible on screen*.
6. **Retrieve.** Recipient polls or subscribes to any swarm member, pulls, decrypts, persists locally, then **ACKs**, and the swarm deletes (tombstoned, so anti-entropy does not resurrect it).
7. **Forget.** Hard TTL expiry independent of ACK. Relay storage is in-memory, no logs, no snapshots, **no history-fetch operation exists in the protocol**. R9 becomes structural rather than a promise.
8. **Crypto.** X3DH + Double Ratchet over libsodium primitives. Prekey bundles are **self-verifying signed records** stored on the overlay, so an untrusted relay cannot forge one.
9. **Ordering and dedup.** 2-element vector clock per conversation + `prev_message_id` hash references; `message_id` = content hash; persisted seen-set with a unique index.
10. **Churn.** Push-on-join record sync; hand-off on departure; periodic Merkle-digest anti-entropy between swarm members; storage audit challenges (`H(record ‖ nonce)`) — closing a gap Session itself admits.
11. **Fast path (optional).** Direct client-to-client transfer when both are online, falling back to the overlay. Nice demo, strictly optional.

**Advantages**
- **Satisfies all nine requirements**, including R9 structurally rather than by policy.
- Densest distributed-systems content of any option: gossip membership, failure detection, consistent hashing, replication with write quorum, anti-entropy, churn rebalancing, causal ordering, idempotence, TTL garbage collection.
- **Demos beautifully under the no-Compose rule.** Start relays in any order; kill the one holding a message and it still arrives; kill all relays, restart them empty, and both clients still show full history.
- A genuine, statable contribution: *Session gave up forward secrecy because swarms offer no durable prekey storage; we keep the ratchet by making prekey bundles self-verifying. SimpleX has no replication; we add swarms.*
- Clean separation of concerns makes it parallelisable across three workers.

**Disadvantages and tradeoffs**
- **The most to build.** Highest implementation risk of the five. Mitigation: strict milestone ordering, with the relay overlay and the crypto developed in parallel against a written wire protocol, and a working end-to-end slice by week 4.
- Two node types means two deployment stories and more Docker plumbing.
- Eventual consistency of the membership view means a message can occasionally be written to a stale swarm. Mitigated by error-driven mapping repair (Session's rule: a relay that is no longer responsible replies with the current swarm) plus anti-entropy.
- Polling is, as Session's authors concede, "inefficient and slow". Mitigated with long-lived subscriptions when the client is connected, polling only as fallback.
- Sybil resistance is genuinely unsolved without the economic layer we are not building. We declare it out of scope and explain what an open deployment would need.

**Verdict: recommended.**

---

## 5. Option D — Epidemic / gossip flooding (Briar / Scuttlebutt flavour)

```
   N1 ◄──► N2 ◄──► N3
    ▲ ╲   ╱ ▲  ╲  ╱ ▲      Every node gossips every envelope it holds.
    │  ╳    │   ╳   │      Envelopes flood until collected or TTL.
    ▼ ╱ ╲   ▼  ╱ ╲  ▼      Dedup by bloom filter; no routing at all.
   N4 ◄──► N5 ◄──► N6
```

No placement function, no lookup. Encrypted envelopes spread epidemically; recipients recognise their own by trial decryption or by an opaque tag; TTL and bloom filters bound the flood.

**Advantages**
- Extremely robust: survives arbitrary partitions, works with intermittent connectivity, no routing state to corrupt, no swarm to go stale. Genuinely delay-tolerant in the Briar sense.
- The simplest thing that could possibly work — a plausible 3-week implementation.
- Strong anti-entropy content (Merkle/bloom reconciliation) and a nice theoretical story (epidemic dissemination).
- Excellent privacy by default: nobody knows who a message is for.

**Disadvantages and tradeoffs**
- **O(N) storage and bandwidth amplification** — every node carries a share of every conversation. It does not scale, and the examiners will say so.
- No notion of responsibility: nobody is accountable for durability, so "is this message stored?" has no answer.
- Trial decryption is expensive and leaks timing.
- Very little *placement* content — the intellectually interesting half of the problem is simply skipped.

**Verdict:** rejected as the primary architecture, **adopted as a degraded mode inside option C.** When a client can reach *no* member of the target swarm (partition), it hands the envelope to any reachable relay, which gossips it toward the swarm opportunistically. This is a small amount of code that buys a dramatic demo — *partition the network, send anyway, heal, message arrives* — and it is exactly the Briar insight applied where it fits.

---

## 6. Option E — Partitioned cluster with Raft replica groups

Consistent hashing over partitions, each partition a Raft group of 3 replicas with strong consistency.

**Advantages**
- Strongest guarantees: linearizable writes, unambiguous durability, a leader that definitively owns each partition.
- Textbook-respectable; Raft is the canonical course topic.

**Disadvantages and tradeoffs**
- **Raft is expensive to implement correctly** (leader election, log replication, membership changes, snapshotting). It would consume the whole three months and leave no time for the messaging system.
- **Strong consistency is the wrong guarantee for messaging.** We need causal order and availability under partition; a quorum-unavailable Raft group refuses writes, which for a messenger means "cannot send" — worse than storing the message somewhere slightly stale. This is a CAP choice and we should make the AP choice deliberately and say so.
- Membership changes need quorum writes, a known bottleneck, and the pattern the literature recommends is precisely SWIM-for-membership + Raft-only-for-durable-decisions.
- Feels like a distributed database with a chat UI bolted on, rather than a messenger.

**Verdict:** rejected. **Our deliberate non-use of consensus is a result to present, not a gap to apologise for** — explained via the CAP tradeoff and the observation that none of WhatsApp, Matrix, Signal, Session or SimpleX uses consensus on the message path either.

---

## 7. Comparison matrix

Legend: ● strong · ◐ partial · ○ weak/absent. "DS depth" = density of distributed-systems content, i.e. what is actually being graded.

| Criterion | **A** Federated | **B** Pure P2P | **C** Relay overlay ★ | **D** Gossip flood | **E** Raft cluster |
|---|:--:|:--:|:--:|:--:|:--:|
| R1 identity survives infrastructure loss | ○ | ● | ● | ● | ◐ |
| R3 delivery to offline recipient | ● | **○** | ● | ● | ● |
| R4 store-and-forward survives holder crash | ◐ | ○ | ● | ● | ● |
| R5 ordering | ● | ◐ | ● | ◐ | ● |
| R6 duplicate suppression | ● | ● | ● | ● | ● |
| R7 local history | ● | ● | ● | ● | ● |
| R8 authenticated + encrypted | ● | ● | ● | ● | ● |
| **R9 no permanent central archive** | **○** | ● | ● | ● | ◐ |
| Decentralization | ◐ | ● | ● | ● | ◐ |
| Metadata exposure (lower is better) | ○ | ● | ● | ● | ○ |
| Churn / fault tolerance | ◐ | ○ | ● | ● | ◐ |
| Partition tolerance | ◐ | ○ | ◐ | ● | ○ |
| Scalability story | ● | ● | ● | ○ | ● |
| **DS depth (grading value)** | ◐ | ◐ | **●** | ◐ | ● |
| **Demo quality under Docker, no Compose** | ◐ | ◐ | **●** | ● | ◐ |
| **Implementation risk in 3 months** (lower is better) | low | medium | **high** | low | **very high** |
| Originality vs. the named references | ○ | ◐ | ● | ◐ | ◐ |

**Reading the matrix.** C is the only column with no ○ in the requirements block. Its single weakness is implementation risk, which is a *schedule* problem — addressable by milestones and by the fact that there are three of us — whereas A's and B's weaknesses are *requirement* failures, which no amount of schedule fixes. D is the cheapest credible fallback and the right degraded mode. E trades the whole timeline for a guarantee the problem does not need.

---

## 8. Recommendation

**Build option C, with option D as its partition-mode fallback.**

In one sentence for the report:

> A two-tier architecture in which self-organizing, mutually distrusting relay nodes form a gossip-maintained overlay and hold padded ciphertext in consistently-hashed, quorum-replicated, TTL-bounded mailboxes addressed by opaque identifiers — while all plaintext, all identity and all permanent history live exclusively on client nodes.

**Why this is defensible against each reference.** Against **WhatsApp**: same amnesiac queue semantics and the same delivery-state UX, with no operator. Against **Matrix**: the same causal event model and retry discipline, without permanent server-side history and without needing state resolution. Against **Signal**: the same cryptography, with prekey bundles made self-verifying so they can live on untrusted infrastructure, and strictly less metadata exposed than sealed sender achieves.

**Phasing, to retire the risk early.** A thin vertical slice first — two clients, one relay, unreplicated, no ratchet, plaintext envelopes — proving store-and-forward end to end. Then replication and gossip. Then the ratchet. Then churn, anti-entropy and the demo script. Detailed milestones belong in the implementation plan, not here.

---

## 9. Technology options

Free and open source throughout, as required. These are the choices still genuinely open; the team should decide §9.1 before anything else.

### 9.1 Implementation language

| | **Go** | **TypeScript / Node** | **Rust** | **Elixir / OTP** | **Python** |
|---|---|---|---|---|---|
| Fit for this problem | **Excellent** — goroutines, `net`, stdlib crypto | Good — async, but CPU-bound crypto is awkward | Excellent, and `vodozemac` is native | **Thematically perfect** (WhatsApp's lineage), supervision trees free | Fair |
| Team familiarity (full-stack devs) | Medium | **Highest** | Low | Low | High |
| Container size / startup | **~15 MB static binary, instant** | ~150 MB, slower | ~10 MB, instant | Large, BEAM startup | Large |
| Crypto story | libsodium bindings, `x/crypto` | `libsodium-wrappers`, TS libsignal port (GPL-3.0) | **`vodozemac` (Apache-2.0)** | libsodium NIFs | `pynacl` |
| Local DB | `modernc.org/sqlite` (pure Go) | `better-sqlite3` | `rusqlite` | `exqlite` | `sqlite3` |
| Risk in 3 months | Low | Low | **Medium-high** (borrow checker + async) | Medium-high (new paradigm) | Medium (packaging, perf) |

**Study's recommendation was Go**, on container size, startup time and concurrency.

> **DECIDED — see [ADR-0002](../decisions/0002-python-backend-react-frontend.md): Python backend, React frontend.** The team chose team familiarity and velocity against a three-month deadline, which is a sound trade given that the graded content is the distributed-systems design rather than the runtime. The ADR records concrete mitigations for the risks flagged in the table above — in particular that image size is paid once per *image*, not per container, so twenty containers cost roughly 40–70 MB of RAM each rather than 150 MB of disk each; and that PyNaCl and `cryptography` release the GIL, so the crypto is not GIL-bound. Full component stack: [`technology-stack.md`](technology-stack.md).

### 9.2 Everything else

| Concern | Choice | Alternatives considered |
|---|---|---|
| Crypto primitives | **libsodium** (X25519, Ed25519, XChaCha20-Poly1305, BLAKE2b, HKDF) — ISC licence | — |
| Double Ratchet | Implement X3DH + Double Ratchet from spec over libsodium, against official test vectors | `vodozemac` (Rust, **Apache-2.0** — the permissive ready-made option); `libsignal` (**AGPL-3.0**); TS libsignal port (GPL-3.0) |
| Relay ↔ relay transport | TCP + length-prefixed binary frames, or gRPC | Matrix-style HTTPS+JSON — simpler to debug, heavier |
| Client ↔ relay transport | WebSocket (subscribe) + HTTP (poll fallback) | Raw TCP |
| Message encoding | Protobuf or CBOR, **fixed-size padded** | JSON — leaks length, easier to inspect |
| Membership / failure detection | SWIM-style gossip, hand-rolled | `hashicorp/memberlist` (MPL-2.0) — but hand-rolling *is* the graded content |
| Local persistence | SQLite | — |
| Relay storage | In-memory with TTL index, optional disk spill | — |
| Client UI | React + Vite, served by the client node, opened in the host browser | TUI — faster to build, much worse to demo |
| Orchestration | Shell scripts over `docker network create` + `docker run` | *Compose is forbidden* |
| Tests | Unit per module + a scripted churn/partition harness driving the Docker CLI | — |

### 9.3 Decisions to record as ADRs

1. Implementation language and split between node types.
2. Discovery mechanism, after the week-1 multicast spike on Docker Desktop/WSL2.
3. Replication factor R and write quorum W.
4. Default TTL, and the retry/backoff/drop policy Matrix leaves undefined.
5. Implement the ratchet ourselves vs. adopt vodozemac.
6. Mailbox identifier derivation (the opaque-addressing scheme).
7. Wire protocol: encoding and framing.

---

## 10. Open questions for the team

**Resolved**

1. ~~Language~~ → **Python backend, React frontend** ([ADR-0002](../decisions/0002-python-backend-react-frontend.md)).
2. ~~Architecture direction~~ → **Option C** ([ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md)).
3. ~~Documentation language~~ → **English throughout** ([ADR-0003](../decisions/0003-english-for-all-documentation.md)).

**Still open**

4. **How many containers will run on one laptop at the defense?** Bounds R, swarm sizes, and the test topology. Memory budget from ADR-0002: roughly 40–70 MB per Python container.
5. **Appetite for risk:** implement X3DH + Double Ratchet ourselves (more defensible at the defense, more time) or adopt `python-x3dh` + `DoubleRatchet` (protects the schedule)? → ADR-0009, after the week-1 spike.
6. **Is the direct client-to-client fast path worth the extra code,** or do all messages go through the overlay for uniformity?
7. **Is the Option D gossip fallback (§5) in scope for v1?** Small code, dramatic partition demo.
8. **Does the course require a written report in Spanish?** Cheap to confirm now, expensive to discover in month three ([ADR-0003](../decisions/0003-english-for-all-documentation.md)).
