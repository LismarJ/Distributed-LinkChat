# ADR-0001 — Two-tier relay overlay with swarm-replicated, TTL-bounded mailboxes

**Status:** accepted · **Date:** 2026-11-10 · **Deciders:** project team
**Amended 2026-11-11:** the project now has two phases ([ADR-0010](0010-build-phase-1-on-ray.md)). The two-tier design stands in both.
- **Phase 1 (Ray, due 2026-12-01).** Membership, transport and supervision come from Ray ([ADR-0004](0004-discovery-through-ray.md), [ADR-0011](0011-relays-as-ray-supervised-actors.md)). "W = 2" below is replaced by three confirmed copies, to tolerate two simultaneous failures ([ADR-0005](0005-three-copies-before-sent.md)). Relays are not "mutually distrusting" inside one Ray cluster. The option D fallback is not built, since phase 1 does not tolerate partitions.
- **Phase 2 (no Ray, due 2027-01-01).** The mechanisms listed below (gossip membership, failure detection, placement over a live membership view, our own transport) are the starting point, to be revised for partition tolerance ([`../planning/phase-plan.md`](../planning/phase-plan.md)).

## Context

The brief (P08) requires delivery to offline recipients and store-and-forward (R3, R4) while forbidding the relay tier from becoming a permanent central database holding all history (R9). Full requirement decomposition in [`../research/00-problem-analysis.md`](../research/00-problem-analysis.md).

Research established that **none of the three named reference platforms satisfies this combination** ([`../research/05-reference-platforms-comparison.md`](../research/05-reference-platforms-comparison.md)):

- WhatsApp and Signal have the right retention policy (server as transient queue, delete on delivery) but a centralized topology.
- Matrix has the right topology (federated, no centre) but keeps permanent full history on every participating homeserver.

The combination exists only in a fourth family the brief does not name — Session, SimpleX, Briar ([`../research/04-decentralized-alternatives.md`](../research/04-decentralized-alternatives.md)).

## Options considered

Five architectures were evaluated in full in [`../design/architecture-options.md`](../design/architecture-options.md):

| Option | Outcome |
|---|---|
| **A** — Federated homeservers (Matrix-lite) | Rejected: a homeserver is a permanent history store (conflicts with R9); identity dies with the container; thin on distributed-systems content |
| **B** — Pure peer-to-peer, every client a node | Rejected: **fails R3**. User devices are offline exactly when their users are. Jami and Tox document this gap; every such system adds an always-on tier back in disguise |
| **C** — Two-tier relay overlay with replicated mailboxes | **Accepted** |
| **D** — Epidemic / gossip flooding | Rejected as primary: O(N) storage amplification, no durability accountability. Retained as an **optional** partition-mode fallback |
| **E** — Partitioned cluster with Raft replica groups | Rejected: Raft would consume the timeline, and strong consistency is the wrong guarantee for messaging — we want AP, not CP |

## Decision

Build **Option C**: a two-tier architecture in which self-organizing, mutually distrusting **relay nodes** form a gossip-maintained overlay and hold padded ciphertext in consistently-hashed, quorum-replicated, TTL-bounded mailboxes addressed by **opaque identifiers** — while all plaintext, all identity and all permanent history live exclusively on **client nodes**.

Defining mechanisms:

- Identity: `UserID = H(Ed25519 identity public key)`, self-generated, no registration authority. Contact discovery out-of-band.
- Membership: seed addresses + SWIM-style gossip; every relay converges on the full node list. No consensus.
- Placement: `swarm(mailbox_id)` = the **R = 3** relays closest to `H(mailbox_id)`, computed locally by anyone holding the membership view — no lookup hops, no resolver (Session's design).
- Store: write to all R, acknowledge the user at **W = 2**.
- Retrieve: poll or subscribe to any swarm member, then ACK; the swarm deletes with a tombstone.
- Forget: hard TTL independent of ACK; in-memory relay storage; **no history-fetch operation exists in the protocol**.
- Crypto: X3DH + Double Ratchet, with **self-verifying signed prekey bundles** stored on untrusted relays.
- Ordering and dedup: 2-element vector clock per conversation, `prev_message_id` hash references, content-hash `message_id`, persisted seen-set.

## Consequences

**Positive**

- Satisfies all nine requirements, with R9 guaranteed **structurally** (relays hold only opaque padded ciphertext, under TTL, with no history API) rather than by policy.
- Densest distributed-systems content of the five options: gossip membership, failure detection, consistent hashing, write quorums, anti-entropy, churn rebalancing, causal ordering, idempotence, TTL garbage collection.
- Demos well under the no-Compose rule: relays start in any order; killing the relay holding a message still delivers it; killing every relay and restarting them empty leaves both clients' history intact.
- A statable contribution over the prior art: Session abandoned forward secrecy because swarms offer no durable prekey storage — we keep the Double Ratchet by making prekey bundles self-verifying. SimpleX has no replication — we add swarms.

**Negative / risks**

- Highest implementation cost of the five options. Mitigation: a thin vertical slice first (two clients, one relay, unreplicated, no ratchet), then replication and gossip, then the ratchet, then churn and anti-entropy.
- Two node types means two deployment stories and more Docker plumbing.
- Eventual consistency of the membership view allows a write to a stale swarm. Mitigated by error-driven mapping repair (a relay no longer responsible replies with the current swarm) plus periodic anti-entropy.
- Polling is inefficient, as Session's authors concede. Mitigated with long-lived subscriptions while the client is connected.

**Explicitly out of scope** — to be stated at the defense rather than half-built: consensus (Raft/Paxos), Matrix-style state resolution (avoided by construction, since 1:1 conversations have no mutable shared state), group messaging / MLS, Sybil resistance, onion routing, NAT traversal, multi-device, post-quantum key agreement.

## Follow-ups

- ADR-0002 — implementation stack
- ADR-0004 — discovery mechanism, after the week-1 multicast spike on Docker Desktop / WSL2
- ADR-0005 — replication factor R and write quorum W (provisionally 3 and 2)
- ADR-0006 — default TTL and the retry/backoff/drop policy the Matrix spec leaves undefined
- ADR-0007 — mailbox identifier derivation (the opaque-addressing scheme)
- ADR-0008 — wire protocol encoding and framing
- Decide whether the Option D gossip fallback is in scope for v1
