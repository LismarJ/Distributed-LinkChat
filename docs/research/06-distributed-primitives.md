# 06 · Distributed Systems Primitives — The Toolbox

**Status:** research note · **Date:** 2026-11-10

The mechanisms we will actually assemble, with the evidence for choosing between them. This is the reference we consult while writing [`../design/architecture-options.md`](../design/architecture-options.md).

> **Update 2026-11-11: which parts apply to which phase.** The project now has two phases ([`../planning/phase-plan.md`](../planning/phase-plan.md)).
>
> | Section | Phase 1 (on Ray, due 2026-12-01) | Phase 2 (no Ray, partitions, due 2027-01-01) |
> |---|---|---|
> | 1. Key placement | Applies, simplified: the ring is a fixed set of five relay names, since Ray keeps the relays alive | Applies as written: placement over the live membership |
> | 2. Membership, discovery, failure detection | **Not built.** Ray provides all three, and the examiners forbid reimplementing them | Applies as written |
> | 3. Ordering | Applies | Applies |
> | 4. Replication, quorum, anti-entropy | Changed. Two simultaneous failures require three confirmed copies, not two of three. Without partitions no majority is needed. Repair is sync on restart plus hinted handoff; digest comparison is deferred from phase 1 (ADR-0014). See section 9 below | To be redesigned for partitions; anti-entropy becomes essential |
> | 5 to 7. Duplicates, local persistence, retry | Apply. Ray's call retries are added underneath ours | Apply |
> | 8. What we are not building | Still not built | Still not built |
>
> References to a three-month timeline in this note are out of date.

---

## 1. Overlay and key placement: which DHT, or none at all

We need to answer "which nodes are responsible for user X's mailbox?" with no coordinator, and have every participant compute the same answer.

### Chord vs Kademlia

| | **Chord** | **Kademlia** |
|---|---|---|
| Keyspace | Ring, successor-based | XOR metric over 160/256-bit IDs |
| Routing state | O(log N) finger table | O(k · log N) — k-buckets |
| Lookup | O(log N) hops | O(log N) hops, **fewer in practice** |
| Upkeep | Periodic **stabilization** required for correctness | **Lookup traffic doubles as neighbour discovery** — far less dedicated maintenance |
| Under churn | More fragile; ring correctness depends on stabilization | **Measurably better** |
| Parallel lookups | Awkward | Natural (α concurrent queries) |

Evidence: simulation studies comparing the two in communication-oriented P2P find **Kademlia wins on lookup path length (roughly 0.76–2.02 fewer hops on average, widening under skewed workloads) while Chord scales better on messaging cost** at large N. Churn-focused evaluations report Kademlia outperforming Chord, and the MIT performance-vs-cost framework notes Kademlia needs less dedicated stabilization precisely because lookups maintain the routing table. Not unanimous — one study found Pastry lowest in hop count, and a hierarchical study found Chord-based hierarchies best under very high churn — and most results are simulations with differing churn models, so treat the margins as indicative rather than decisive.

**Our verdict.** For a demo of **10–30 containers**, asymptotic differences are irrelevant; *robustness under `docker kill` is everything*. Kademlia's self-maintaining routing table is the right property. **But** at our scale a third option is better still:

### Consistent hashing over a gossip-maintained full membership view

With tens of nodes, **every node can simply know every other node**. Then:
- `swarm(userID)` = the R nodes whose IDs are closest to `H(userID)` in the keyspace — computed **locally, by everyone, with zero lookup hops**.
- No routing table, no finger table, no iterative lookup, no bootstrap complexity.
- Membership is maintained by gossip (§2); placement is a pure function of the membership view.

This is **exactly Session's design** — swarm assignment is computed from the node list, with no resolver to ask. It is dramatically simpler to implement correctly in three months, and it is honest: we present it as *consistent hashing with full membership*, note that it is O(N) state per node, and explain precisely how we would swap in Kademlia routing if N grew. That explanation *is* the distributed-systems content; implementing Kademlia badly would be worth less.

**Recommendation: consistent hashing + full membership via gossip, with Kademlia documented as the scale-out path.**

---

## 2. Membership, discovery and failure detection

### The no-Compose discovery problem

A node must find peers with no configuration. Three mechanisms, in order of how much we should rely on them:

1. **Seed addresses + gossip (primary).** A node is given zero or more seed addresses via `--env`. It gossips with any seed it reaches, learns the full membership, and from then on needs no seeds. Works everywhere, no multicast required. A node started with no seeds forms a cluster of one and is joined later when someone else seeds *to* it.
2. **UDP broadcast/multicast on the Docker bridge (convenience).** Announce on a well-known port; anyone listening learns the new node. Makes a zero-configuration demo possible. **Must be verified early on Docker Desktop/WSL2** — behaviour differs by backend. Treat as an optimisation layered on (1), never as the only path.
3. **Subnet scan (last resort).** On a known `/24` created with `docker network create --subnet`, probe the range. Ugly, but it always works and makes a good fallback story.

### SWIM vs heartbeats vs consensus

**SWIM** is the right model: each node periodically pings a random peer; on no reply it asks k other nodes to ping that peer indirectly (ruling out a single bad link); failing that, the peer is marked **suspect**, then **dead** after a timeout, with the verdict disseminated by piggybacking on normal gossip traffic.

Key properties and cautions from the literature:
- SWIM is **a failure detector, not a consensus protocol**. It converges views; it does not guarantee that all nodes agree at any instant.
- Gossip membership **allows views to diverge**, and communication failures between live processes can cause **repeated accusation/refutation oscillation** (the Rapid paper's critique).
- Detection latency is a tunable: one implementation reports `PingTimeout + SuspectTimeout` ≈ 5.5 s with default settings. **For our demo this must be a few seconds**, so a killed relay visibly heals within the examiner's attention span.
- The standard industrial pattern is **SWIM for liveness + Raft for decisions that must be agreed** (Consul does exactly this: Raft for the KV store, gossip for membership). The counter-argument to using Raft for membership is that every change needs a quorum write.

**Our verdict.** SWIM-style gossip for membership and failure detection; **no Raft**. We do not need agreement on membership — we need *convergence*, plus a replication scheme that tolerates disagreement at the edges (§4). Temporary view divergence causes at worst a message written to a slightly stale swarm, which anti-entropy repairs. We will say this explicitly, because "we deliberately chose not to use consensus, and here is why" is a stronger answer than using Raft reflexively.

---

## 3. Ordering: Lamport vs vector clocks vs HLC

| | **Lamport** | **Vector clock** | **Hybrid logical clock** |
|---|---|---|---|
| State | one integer | one integer per participant | physical time + counter |
| Guarantee | a→b ⟹ L(a)<L(b); **converse false** | **full causality**: detects concurrency | causality-preserving and close to real time |
| Size | O(1) | O(N) participants | O(1) |
| Detects concurrency | No | **Yes** | No (approximates) |
| Good for | total tie-break | **causal delivery** | timestamps users will read |

Causal ordering is the correct level for chat: a reply must not appear before the message it answers; total order would need consensus-latency and buys nothing. Vector clocks' known cost is that they grow with the number of participants.

**Our verdict — and the scoping win:** a **1:1 conversation has exactly two participants**, so a vector clock is **two integers**. The usual objection to vector clocks evaporates. We use a 2-element vector clock per conversation for causal delivery, plus `prev_message_id` hash references (Matrix's `prev_events`, simplified) so gaps are detectable, plus a wall-clock timestamp **for display only, never for ordering**. HLC is documented as what we would use if conversations became multi-party.

**Causal buffering rule:** hold a message until its causal predecessors have been delivered; release in order. Bound the buffer, and after a timeout surface a visible "gap" rather than blocking the conversation forever — a deliberate availability-over-consistency choice we should defend.

---

## 4. Replication, quorum and anti-entropy

**Placement.** `swarm(mailboxID)` = the R closest nodes by `H(mailboxID)`. Start with **R = 3**, `N_min`/`N_target`/`N_max` following Session's shape (5/7/10) if we grow the relay tier.

**Write.** Send to all R; **acknowledge to the sender after W replicas confirm.** With R=3, **W=2** gives one-failure durability at modest latency. The sender's UI shows "sent" (one grey tick) only when W is met — making the quorum visible in the demo.

**Read.** The recipient polls **any** swarm member and may union results from several; duplicates are harmless because dedup is by message ID (§5). So **no read quorum is needed** — a nice asymmetry to point out.

**Anti-entropy.** Swarm members periodically reconcile:
- Exchange a **Merkle tree** (or a simple sorted digest of message IDs per mailbox) and pull what is missing. Merkle trees make the common case — nothing differs — cost one hash comparison.
- **Push on join**: when a node enters a swarm, existing members push the swarm's records to it (Session's rule).
- **Hand-off on departure/dissolution**: a node leaving a dissolving swarm must determine which swarms are now responsible for its records and forward them there.

**Deletion.** Two independent paths, and both matter for R9:
1. **ACK-driven** — recipient acknowledges, the swarm deletes. Propagated to replicas by the same anti-entropy mechanism (as a tombstone, so a stale replica does not resurrect the message).
2. **TTL-driven** — hard expiry regardless of acknowledgement. This is the structural guarantee: even a relay that never sees an ACK forgets.

**The gap Session admits and we should close:** nodes are tested for *reachability*, not for *whether they actually stored anything*. A **storage audit** — challenge a node to return `H(stored_record ‖ nonce)` — is cheap to implement and is a genuine contribution over the reference systems.

---

## 5. Duplicate suppression

At-least-once transport is unavoidable: SimpleX's spec states plainly that a message may be re-delivered if the connection drops before `ACK`, so **clients must deduplicate by message ID**. The split is: **at-least-once delivery, exactly-once display.**

Mechanism:
- `message_id = H(ciphertext ‖ sender_pk ‖ conversation_id ‖ counter)` — deterministic, so a retransmission produces the *same* ID (Matrix's content-hash event IDs).
- Client keeps a **seen-set** keyed by message ID, persisted in the local database, with a unique index so insertion of a duplicate simply fails.
- Relays deduplicate too, on store, so a retried write does not multiply.
- Make every operation **idempotent**: storing a message already present returns success; acknowledging twice is harmless; deleting an absent message returns success.

A **Bloom filter** over seen IDs is a cheap optimisation for the anti-entropy exchange and an easy thing to explain at the defense.

---

## 6. Local persistence

SQLite in every client (`better-sqlite3` for Node, `modernc.org/sqlite` or `mattn/go-sqlite3` for Go). Schema sketch: `identities`, `conversations`, `messages` (with `UNIQUE(message_id)`), `ratchet_state`, `skipped_message_keys` (bounded), `outbox` (pending sends with retry metadata), `seen_ids`.

Encryption at rest is a stretch goal; SQLCipher is the usual answer. Mention it, scope it as optional.

The relay's store should be **in-memory with optional disk spill** — SimpleX's spec recommends in-memory persistence and forbids logs, delivery history and DB snapshots, precisely so that seizing a relay yields nothing. Adopting that posture turns R9 from a policy claim into a structural one.

---

## 7. Retry, backoff and the outbox

Matrix specifies the mechanism (per-destination queue, reuse `txnId` until 200 OK) but **deliberately specifies no backoff schedule, no queue bound, and no drop policy**. We must define them — and because the reference spec leaves them open, defining them well is original work:

- Exponential backoff with **full jitter**, base ~500 ms, cap ~30 s.
- Bounded outbox per destination; on overflow, fail the oldest and surface it in the UI.
- Hard message TTL (default 7 days, following Session's refresh cadence as a precedent); on expiry, mark failed locally and tell the user.
- Idempotent transaction IDs so a retry after an unseen success is harmless.

---

## 8. What we are deliberately not building

Saying this clearly at the defense is worth more than half-building any of it:

- **Consensus (Raft/Paxos).** Not needed — we need convergence, not agreement. §2.
- **State resolution.** Avoided by construction — 1:1 conversations have no mutable shared state. [`05-reference-platforms-comparison.md`](05-reference-platforms-comparison.md) §5.
- **Group messaging / MLS.** Out of scope per the brief.
- **Sybil resistance.** Session needs staking and a blockchain; we declare a trusted-enough relay set for the demo and explain what an open deployment would require.
- **Onion routing.** Interesting, orthogonal, a stretch goal at best.
- **NAT traversal / STUN / TURN.** Irrelevant inside a Docker network — and worth saying, because it is a large part of why Jami and Tox are complicated.
- **Multi-device.** Multiplies session state; explain the design, do not build it.
- **Post-quantum (PQXDH / Triple Ratchet).** Explain, do not build.

---

## 9. Added 2026-11-11: how many copies, and when a majority is needed

The examiners require phase 1 to survive any two simultaneous failures and do not require it to survive partitions. Phase 2 requires partitions. The two cases call for different replication rules, and the difference is worth having in the toolbox.

**Crash failures with a reliable way to know who is down.** To survive `f` crashes, keep `f + 1` copies. One survives, and since a node that does not answer really is dead, whoever is left can be trusted to be the only one acting. For `f = 2`: three copies, all written before the write is confirmed. This is what primary-backup systems and chain replication do, and they rely on an external service to say who is alive. In phase 1 that service is Ray.

**Crash failures where silence is ambiguous.** In a partition, a node that does not answer may be running and accepting writes on the other side. If the system must give one consistent answer (who is the leader, what is the value), it needs majorities: `2f + 1` nodes, with every decision confirmed by `f + 1`, so that any two decisions share a node. That is what Paxos and Raft do, and it stops the minority side from working.

**The third way, for data that cannot conflict.** If the data is a set of immutable items, two sides of a partition can both keep accepting items and merge by union afterwards. No majority is needed, both sides stay available, and the cost is that readers may see an incomplete set until the merge. Dynamo's sloppy quorums with hinted handoff and anti-entropy are the standard form. Our envelopes are immutable and named by their content hash, so they fit. Deletion is the one operation that needs care, and tombstones cover it.

| | Copies | Confirm at | Needs | Under partition |
|---|---|---|---|---|
| Phase 1: `f + 1`, no majority | 3 | 3, with stand-ins | A trusted source of liveness (Ray) | Not safe |
| Majority quorum | 5 for `f = 2` | 3 | Nothing external | Safe; minority side stops |
| Union-merge with anti-entropy | 3 or more | Tunable | Immutable, content-named data | Safe and available; temporarily incomplete reads |

Phase 1 uses the first row with Dynamo-style stand-ins. Phase 2 is expected to use the third.

Reading for this section: Dynamo (DeCandia et al., SOSP 2007) for sloppy quorum and hinted handoff; chain replication (van Renesse and Schneider, OSDI 2004) for `f + 1` with an external membership service; the Raft paper (Ongaro and Ousterhout, 2014) for majorities. These are cited from our knowledge of the papers; they were not re-read for this note.

## Sources

- [A performance comparison of Chord and Kademlia DHTs in high churn scenarios (CAIDA catalog)](https://catalog.caida.org/paper/2014_a_medrano_chavez_ppna_8_807)
- [Comparing the performance and efficiency of two popular DHTs in interpersonal communication](https://www.researchgate.net/publication/224239543_Comparing_the_performance_and_efficiency_of_two_popular_DHTs_in_interpersonal_communication)
- [A performance vs. cost framework for evaluating DHT design tradeoffs (MIT, INFOCOM'05)](https://pdos.csail.mit.edu/~strib/docs/dhtcomparison/dhtcomparison-infocom05.pdf)
- [Chord vs Kademlia — Princeton systems course](https://medium.com/princeton-systems-course/chord-vs-kademlia-039b0e3c99ec)
- [Comparative study of CAN, Pastry, Kademlia (IJP2P)](https://airccse.org/ijp2p/papers/12321ijp2p01.pdf)
- [Stable and Consistent Membership at Scale with Rapid (arXiv)](https://arxiv.org/pdf/1803.03620) — the critique of gossip membership
- [SWIM failure detection — implementation notes](https://kahunakv.github.io/kommander.github.io/docs/guides/swim-failure-detection/)
- [Logical clocks — UMass CS677 lecture notes](https://lass.cs.umass.edu/~shenoy/courses/spring22/lectures/Lec13_notes.pdf)
- [Vector Clocks and Distributed Snapshots — Princeton COS 418](https://www.cs.princeton.edu/courses/archive/fall18/cos418/docs/L4-vc.pdf)
- [Message Ordering Guarantees — FIFO, Partition Keys, and Causal Consistency](https://codelit.io/blog/message-ordering-guarantees)
- [SimpleX Messaging Protocol specification](https://github.com/simplex-chat/simplexmq/blob/stable/protocol/simplex-messaging.md) — dedup and retention requirements
- [Matrix Server-Server API](https://spec.matrix.org/latest/server-server-api/) — transaction retry semantics
