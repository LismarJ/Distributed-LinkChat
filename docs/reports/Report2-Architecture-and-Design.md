# Report 2: Architecture and Design

**Project:** P08 · Mensajería segura descentralizada, Sistemas Distribuidos 2026, Universidad de La Habana, Facultad de Matemática y Computación
**For:** the development team · **Date:** 2026-11-11

## 1. The one question every architecture has to answer

Every design for this problem is an answer to the same question: **the recipient's device is switched off; who holds the message, for how long, and how do we find them?**

There are four fundamentally different answers, and a fifth that combines two of them:

| Answer | Architecture |
|---|---|
| "The recipient's own server holds it, forever" | A: Federated homeservers |
| "Nobody; both parties must be online" | B: Pure peer-to-peer |
| "A computed subset of an always-on relay tier holds it, briefly" | C: Two-tier relay overlay with replicated mailboxes |
| "Everybody holds a bit of everything, briefly" | D: Epidemic / gossip flooding |
| "A strongly-consistent replica group holds it" | E: Partitioned cluster with consensus |

Two of the brief's own requirements prune this space before we score anything: R9 (relays must not become a permanent archive) rules out A in its natural form, and R3 (delivery to offline recipients) rules out B in its natural form. That's not a reason to skip them. A grader will ask why we didn't do the obvious thing, and "we evaluated it and here's the specific requirement it fails" is a stronger answer than not having looked.

---

## 2. Option A: federated homeservers (Matrix-lite)

```
  Client A ──┐                           ┌── Client C
             ├─► Homeserver 1 ◄════════► Homeserver 2 ◄──┤
  Client B ──┘   (owns A, B,             (owns C, D)     └── Client D
                  full history)           full history
                       ▲                        ▲
                       └── federation: signed transactions,
                           per-destination queue, retry, backfill
```

Users belong to a homeserver, which holds their identity, their queue, and their history. Servers federate by exchanging signed events: a signed event DAG, a per-destination transaction queue with idempotent transaction IDs, backfill for gap repair, self-published signing keys, server discovery.

It's tempting precisely because offline delivery is trivially solved (your homeserver is always up), it's the best-documented prior art in the whole study, and the architecture maps directly onto the brief's own phrase *"estado replicado."* Implementation risk is modest too: essentially two well-understood components.

But it fails the one requirement that matters most. A homeserver is a permanent database of its users' full history, which conflicts with R9 directly. We could TTL-delete after delivery, but then it stops being a Matrix-style homeserver and loses the property that made it attractive in the first place. There's a second, more immediate problem for our Docker setting: identity dies with the server. `@alice:server1` means nothing once `server1` is gone, and in our demo a grader *will* kill `server1`. Fixing that means replicating each homeserver, which is just option E wearing Matrix's clothes. And underneath both of those: discovery is trivial, placement is trivial, there's no churn problem to solve, so we'd end up building a federated web service with very little distributed-systems content to show for it.

**Verdict: rejected as the primary architecture.** Its mechanisms, transaction queues, the DAG, signing, survive wholesale inside option C.

---

## 3. Option B: pure peer-to-peer, every client a node

```
   Client A ◄──────► Client C
      ▲  ╲          ╱  ▲
      │   ╲        ╱   │        Kademlia DHT over user devices.
      ▼    ╲      ╱    ▼        Direct encrypted connections.
   Client B ◄──────► Client D
```

Every user device joins a DHT, finds peers by public key, and connects directly: Kademlia routing, public-key identity, direct encrypted transport. It's maximally decentralized, arguably the purest possible answer to R9 since nothing is permanent because nothing is infrastructure, and it's rich in classic distributed-systems content. Running inside a Docker network even removes the usual dominant headache, NAT traversal, which is normally what makes this architecture painful in practice.

It fails on exactly the requirement the project is testing, though. Users' devices are offline precisely when their users are offline. Jami says this about itself in plain language: *"you miss your messages if you don't have clients logged into your account."* Tox has the identical long-standing gap. Every real system that started from this design ended up adding an always-on tier back in disguise: Jami needs a bootstrap server, a DHT proxy, a push server, a TURN server, and a name server. You could store mailboxes directly on the DHT to patch this, but a DHT made of user devices has catastrophic churn, and the moment you stabilize it with dedicated nodes, you've built option C.

**Verdict: rejected**, and this is the most instructive finding in the whole study: an always-on tier is not a failure of decentralization, it is a requirement of asynchronous messaging. The design question was never whether it exists, only how little it's allowed to know and how briefly it's allowed to remember.

---

## 4. Option C: two-tier relay overlay with swarm-replicated amnesiac mailboxes ★

This is the synthesis the state-of-the-art report pointed to: Session's replicated swarms, SimpleX's opaque padded queues with ACK-deletion, Signal's prekeys and ratchet, Matrix's causal DAG and retry discipline.

```
                      RELAY OVERLAY  (gossip membership, consistent hashing)
        ┌───────────────────────────────────────────────────────────────┐
        │   R1      R2      R3      R4      R5      R6   ...            │
        │    └───────┴───────┘                                          │
        │   placement(mailbox_AB) = k nodes closest to H(mailbox_AB)    │
        │   each holds: {opaque mailbox id → padded ciphertext + TTL}   │
        └───────────────────────────────────────────────────────────────┘
              ▲  store (w=2 of k=3)             ▲  poll / subscribe, ACK
              │                                  │
         ┌────┴─────┐                      ┌─────┴────┐
         │ Client A │                      │ Client B │
         │ SQLite:  │                      │ SQLite:  │
         │ the ONLY │                      │ the ONLY │
         │ history  │                      │ history  │
         └──────────┘                      └──────────┘
```

(We use `k` for the per-mailbox replica count and `w` for the write quorum here, rather than reusing R, to keep this visually distinct from the R1-R9 requirement numbers.)

There are two node types, both plain containers. **Relay nodes** form the overlay and hold nothing but ciphertext. **Client nodes** hold identity, history, and all plaintext, and serve a small web UI to whoever is watching, which in practice means the examiner's browser during the defense.

The design rests on eleven mechanisms, each one borrowed or adapted from a specific system Report 1 studied:

1. **Identity.** `UserID = H(Ed25519 identity public key)`, self-generated, no authority. Contact discovery is out-of-band: an invitation string carrying the public key and a mailbox seed.
2. **Membership.** Relays discover each other by seed address plus SWIM-style gossip, with UDP broadcast as a zero-config convenience. Every relay converges on the full node list.
3. **Placement.** `placement(mailbox_id)` is the `k = 3` relays whose node IDs are closest to `H(mailbox_id)`, computed locally by anyone holding the membership view. No lookup hops, no resolver; this is Session's trick.
4. **Mailboxes are opaque.** A conversation's mailbox IDs come from a shared secret established at introduction, not from either user's identity. A relay sees deposits into `0xA3F…` and can't tell who sent them or who's collecting, which is SimpleX's insight, pushed further than Signal's sealed sender (whether it's one mailbox per conversation or two, one per direction as SimpleX does, is still an open decision; see section 10).
5. **Store.** The sender writes the padded ciphertext envelope plus TTL to every reachable relay in the placement set and is acknowledged once `w = 2` of the `k = 3` confirm; that's when the first grey tick fires, so the quorum is visible on screen during a demo. The important difference from option E's Raft groups: when fewer than `w` relays are reachable, the write is never refused. Whatever's reachable, even a single relay, still accepts and stores the envelope, the UI shows `pending` rather than `sent`, and the same anti-entropy mechanism that repairs ordinary replica staleness (mechanism 10) carries the envelope to the remaining replicas once they're reachable again, upgrading the tick once the quorum is finally met. A send only fails outright when the sender can't reach *any* relay in the placement set, which is exactly where the option D fallback (section 5) takes over.
6. **Retrieve.** The recipient polls or subscribes to any relay in the placement set, decrypts, persists locally, then ACKs, and the relay deletes the record (tombstoned, so anti-entropy doesn't resurrect it).
7. **Forget.** A hard TTL expires independently of any ACK. Relay storage is in-memory only, with no logs, no snapshots, and no history-fetch operation anywhere in the protocol. Two different guarantees ride on this, and they aren't equally strong: that a relay *cannot read* a message is cryptographic and absolute, since it never holds a decryption key. That a relay *will actually delete* ciphertext on schedule is a property of the protocol having no API to ask for old data, plus relay-operator honesty, and it is not a proof against an operator who logs bytes off the wire before their own node deletes its copy. The first is what makes R9 structural rather than a promise. The second is a real limitation worth stating plainly rather than glossing over; mechanism 10's storage audit checks whether a relay still has what it's supposed to have, not whether it secretly kept more than it should have.
8. **Crypto.** X3DH plus the Double Ratchet over libsodium primitives. Prekey bundles are self-verifying signed records stored on the overlay, so an untrusted relay can't forge one.
9. **Ordering and dedup.** A two-element vector clock per conversation, plus `prev_message_id` hash references, plus a content-hash `message_id`, plus a persisted seen-set with a unique index.
10. **Churn.** Push-on-join record sync, hand-off on departure, periodic Merkle-digest anti-entropy between relays in a placement set, and storage-audit challenges (`H(record ‖ nonce)`) that close a gap Session itself admits it never solved.
11. **Fast path (optional).** Direct client-to-client transfer when both happen to be online, falling back to the overlay otherwise. A nice demo, and strictly optional.

What makes this the recommended option is that it's the only one that satisfies all nine requirements (R2, one-to-one conversations, isn't in the comparison table in section 7 because every option here satisfies it by construction; it's a scoping choice, not something that discriminates between architectures), including R9 structurally rather than as a stated policy. It also carries by far the densest distributed-systems content of the five: gossip membership, failure detection, consistent hashing, replication with a write quorum, anti-entropy, churn rebalancing, causal ordering, idempotence, TTL garbage collection. It also demos well under the no-Compose rule specifically: start relays in any order, kill the one holding a message and it still arrives, kill every relay and restart them empty and both clients still show their full history. And it lets us make a specific, checkable claim about what we did differently from the two closest prior systems: Session gave up forward secrecy because its swarms offer no durable prekey storage, we keep the ratchet by making prekey bundles self-verifying instead; SimpleX has no replication at all, we add it.

The honest cost is that it's the most to build, and carries the second-highest implementation risk of the five, behind only option E's Raft groups. We're managing that by splitting the work across people rather than across time: with three of us, one can build the crypto module in isolation against a written wire-protocol spec while the others build the overlay and replication, so the ratchet is ready to integrate exactly when the milestone plan (section 8) calls for it, instead of becoming a second project competing for the same weeks. Two other costs are smaller but real. Having two node types means two deployment stories and more Docker plumbing than a single-binary design would need. And the membership view is only eventually consistent, so a message can occasionally land on a relay that no longer believes it's responsible; Session's own fix, where a relay that's no longer responsible replies with the current membership, plus anti-entropy, repairs this. Polling itself is, as Session's own authors concede, "inefficient and slow," which we mitigate with long-lived subscriptions whenever the client is actually connected, falling back to polling only when it isn't. Sybil resistance (stopping anyone from flooding the overlay with fake relay nodes) is genuinely unsolved without the economic layer we're deliberately not building; we scope it out for the demo, where we run a known fixed set of relays ourselves, and explain what an open deployment would need on top.

**Verdict: recommended**, and the basis for [ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md).

---

## 5. Option D: epidemic gossip flooding (the Briar/Scuttlebutt flavour)

```
   N1 ◄──► N2 ◄──► N3
    ▲ ╲   ╱ ▲  ╲  ╱ ▲      Every node gossips every envelope it holds.
    │  ╳    │   ╳   │      Envelopes flood until collected or TTL.
    ▼ ╱ ╲   ▼  ╱ ╲  ▼      Dedup by bloom filter; no routing at all.
   N4 ◄──► N5 ◄──► N6
```

No placement function and no lookup at all. Encrypted envelopes spread epidemically; recipients recognise their own either by trial decryption or by an opaque tag; a TTL and bloom filters keep the flood bounded.

It's extremely robust: it survives arbitrary partitions, tolerates intermittent connectivity, and has no routing state to corrupt and no placement to go stale, genuinely delay-tolerant in the way Briar is. It's also the simplest thing that could plausibly work, roughly a three-week build, with strong anti-entropy content (Merkle and bloom-filter reconciliation) and real privacy by default, since nobody in the system knows who a message is actually for.

Its failure mode is scale: every node ends up carrying a share of every conversation, which is O(N) storage and bandwidth amplification that simply doesn't grow, and an examiner will say so. There's also no notion of responsibility, so "is this message actually stored anywhere?" has no good answer, trial decryption is expensive and leaks timing, and the intellectually interesting half of the problem, placement, is skipped entirely rather than solved.

**Verdict: rejected as the primary architecture, adopted as a degraded mode inside option C.** When a client can't reach any member of a mailbox's placement set (a partition), it hands the envelope to any relay it *can* reach, which gossips it toward the right placement set opportunistically. That's a small amount of code buying a dramatic demo moment: partition the network, send anyway, let it heal, watch the message arrive. It's the Briar insight applied exactly where it fits, and nowhere else.

---

## 6. Option E: a partitioned cluster with Raft replica groups

Consistent hashing over partitions, each partition backed by a Raft group of three replicas with strong consistency. It gives the strongest guarantees of any option here (linearizable writes, unambiguous durability, a leader that definitively owns each partition), and it's textbook-respectable since Raft is the canonical course topic.

It costs too much to build, though: Raft implemented correctly (leader election, log replication, membership changes, snapshotting) would eat the whole three months and leave nothing for the actual messaging system. Worse, strong consistency is the wrong guarantee for messaging in the first place. We need causal order and availability under partition, and a Raft group that loses quorum simply refuses writes, which for a messenger means "cannot send," a worse failure mode than storing the message somewhere slightly stale. That's a CAP tradeoff, and we'd rather make the AP choice deliberately and say so than back into CP by reflex. Membership changes in a Raft design also need quorum writes, a known bottleneck, which is exactly why the pattern the literature actually recommends is SWIM for membership with Raft reserved only for the decisions that truly need durability. Built this way, the system ends up feeling like a distributed database with a chat UI bolted on rather than a messenger.

**Verdict: rejected.** Our deliberate non-use of consensus is a result worth presenting, not a gap to apologise for: it follows from the CAP tradeoff directly, and none of WhatsApp, Matrix, Signal, Session or SimpleX uses consensus on the message path either.

---

## 7. Comparing all five

● strong · ◐ partial · ○ weak or absent. "DS depth" means density of distributed-systems content, which is to say, roughly, what's actually being graded.

| Criterion | A Federated | B Pure P2P | C Relay overlay ★ | D Gossip flood | E Raft cluster |
|---|:--:|:--:|:--:|:--:|:--:|
| R1 identity survives infrastructure loss | ○ | ● | ● | ● | ◐ |
| R3 delivery to offline recipient | ● | **○** | ● | ● | ● |
| R4 store-and-forward survives holder crash | ◐ | ○ | ● | ● | ● |
| R5 ordering | ● | ◐ | ● | ◐ | ● |
| R6 duplicate suppression | ● | ● | ● | ● | ● |
| R7 local history | ● | ● | ● | ● | ● |
| R8 authenticated and encrypted | ● | ● | ● | ● | ● |
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

C is the only column with no ○ anywhere in the requirements block. Its one real weakness, implementation risk, is a schedule problem: addressable with milestones, and there are three of us to share it. A and B's weaknesses, by contrast, are requirement failures, which no amount of schedule discipline fixes. D is the cheapest credible fallback and exactly the right degraded mode to borrow from. E trades away the entire timeline for a guarantee the problem doesn't actually ask for.

A few of these cells rest on reasoning that sections 2 through 6 don't spell out directly: D scores only partial on ordering because flooding gives no native sequencing at all, causal order would have to be rebuilt entirely client-side from timestamps or vector clocks, which works but is strictly weaker than a design with native causal structure (option C's, via the DAG-derived mechanism in Report 1). E scores only partial on identity surviving infrastructure loss because a Raft group survives losing any single node, which beats option A's one homeserver, but it's still tied to the cluster as a whole staying up. The metadata-exposure and originality columns are scored relative to option C's own properties, described in sections 4 and 8, since none of the rejected options were designed with either property as a goal.

---

## 8. The decision

**Build option C, with option D folded in as its partition-mode fallback.** This is [ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md).

In one sentence: a two-tier architecture in which self-organizing, mutually distrusting relay nodes form a gossip-maintained overlay and hold padded ciphertext in consistently-hashed, quorum-replicated, TTL-bounded mailboxes addressed by opaque identifiers, while all plaintext, all identity, and all permanent history live exclusively on client nodes.

It holds up against each of the three named references for a specific reason. Against WhatsApp: the same amnesiac queue semantics and the same delivery-state UX, just with no operator behind them. Against Matrix: the same causal event model and retry discipline, minus permanent server-side history and minus the need for state resolution. Against Signal: the same cryptography on infrastructure we don't have to trust the way Signal's server is trusted (mechanism 8 and section 4's closing caveat on what that trust actually buys), and less metadata exposed than even sealed sender achieves.

To retire the implementation risk early rather than discover it in month three, we're building a thin vertical slice first: two clients, one relay, no replication, no ratchet yet, plaintext envelopes, just proving store-and-forward end to end. Replication and gossip come next, then the ratchet, then churn, anti-entropy, and the actual demo script. The detailed milestones belong in the implementation plan, not here.

---

## 9. Technology stack

Option C is an architecture; this section is what we're actually going to type. [ADR-0002](../decisions/0002-python-backend-react-frontend.md) records the language decision itself, with the reasoning and the mitigations for the risks it carries. This section is the resulting component-by-component stack, all free and open source, as the project requires.

### 9.1 Why Python, briefly

The state-of-the-art study's own comparison (Go, TypeScript/Node, Rust, Elixir/OTP, Python) rated Go ahead on container size, startup time, and concurrency, and Elixir as thematically perfect given WhatsApp's own lineage. The team chose Python and React anyway, on team familiarity and velocity against a three-month deadline, which is a reasonable trade given that the distributed-systems design is what's actually graded, not the runtime it's written in.

Two risks from that comparison are worth naming because they're easy to overstate. The 150 MB per-container figure in that original comparison was a rough estimate for an unoptimized image; our actual build target is a multi-stage build on `python:3.12-slim` (section 9.5) at under 120 MB, and that cost is paid once per *image*, not once per container. Separately, and this is a different resource entirely, twenty relay containers sharing that one image cost roughly 40 to 70 MB of RAM each at runtime. What's actually slower in practice is build and pull time during iteration, which `uv` and a committed lockfile keep manageable. And the expensive cryptographic operations run inside PyNaCl and `cryptography`, both C or Rust extensions that release the GIL, so the ratchet isn't GIL-bound at the message rates this project will see.

### 9.2 Component map

```
┌─ CLIENT NODE (container) ──────────────────────────────┐
│  React + Vite + TS  ──WebSocket──►  FastAPI / Uvicorn  │
│  (browser on host)                       │             │
│                                   asyncio core:        │
│                                   · identity & keys    │
│                                   · X3DH + ratchet     │
│                                   · outbox + retry     │
│                                   · causal reorder     │
│                                   · dedup (seen-set)   │
│                                   · aiosqlite ← HISTORY│
└───────────────────────────┬────────────────────────────┘
                            │ WS (subscribe) + HTTP (poll)
┌─ RELAY NODE (container) ──▼────────────────────────────┐
│  asyncio TCP server (length-prefixed binary frames)    │
│  asyncio DatagramProtocol (gossip / UDP discovery)     │
│  · SWIM-style membership + failure detection           │
│  · consistent hashing → placement(mailbox_id)          │
│  · in-memory mailbox store + TTL expiry heap           │
│  · anti-entropy (Merkle digest) with placement peers   │
│  NO plaintext · NO history API · NO logs of content    │
└────────────────────────────────────────────────────────┘
```

### 9.3 Backend

| Concern | Choice | Licence | Notes |
|---|---|---|---|
| Runtime | **Python 3.12+** | PSF | `asyncio` throughout; no threads for I/O |
| Relay-to-relay transport | **`asyncio` streams**, length-prefixed binary frames over TCP | stdlib | Hand-rolled; keeps the wire format fully ours and inspectable |
| Gossip / discovery | **`asyncio.DatagramProtocol`** over UDP | stdlib | Seed addresses primary; broadcast as a zero-config convenience |
| Client-to-relay | WebSocket (subscribe) + HTTP (poll fallback) | n/a | `websockets` or Starlette's own WS support |
| Client HTTP/WS server | **FastAPI** + **Uvicorn** | MIT / BSD | Serves the React build and the API |
| Crypto primitives | **PyNaCl** (libsodium: X25519, Ed25519, XChaCha20-Poly1305, BLAKE2b) | Apache-2.0 *(confirm)* | C extension, releases the GIL |
| KDF / extras | **`cryptography`** (HKDF, AEAD) | Apache-2.0 / BSD *(confirm)* | Rust-backed |
| X3DH + Double Ratchet | Implemented over the above, against the published specs and official test vectors | ours | Fallback: `python-x3dh` (MIT) plus `DoubleRatchet` (python-omemo family); see ADR-0009 |
| Message encoding | **msgpack** (`msgpack` / `ormsgpack`) or **protobuf**, fixed-size padded | n/a | Padding removes length as a metadata channel, SimpleX's rule |
| Client persistence | **`aiosqlite`** over stdlib `sqlite3` | MIT / PSF | The only permanent home of history |
| Relay persistence | In-memory dict plus a `heapq` TTL index; no disk persistence | stdlib | Durability comes from replication across the placement set, not from local storage. Keeping this in-memory-only is what makes the "no logs" claim in section 4 true rather than aspirational |
| Packaging | **`uv`** with a committed `uv.lock` | MIT/Apache-2.0 | Fast, deterministic Docker builds |
| Lint / types | **`ruff`** plus **`mypy --strict`** on the protocol and state-machine modules | MIT | A type error in the ratchet is a correctness bug, not a style one |
| Tests | **`pytest`** plus **`pytest-asyncio`**, plus a scripted churn harness driving the Docker CLI | MIT | The harness is itself a project deliverable |

### 9.4 Frontend

| Concern | Choice | Notes |
|---|---|---|
| Framework | React 18 + TypeScript | |
| Build | Vite | Built assets served by FastAPI from the client container |
| Transport | WebSocket to the client node | Live message and delivery-state updates |
| Key UI element | A delivery-state indicator per message: `pending` (no relay reachable) → `sent` (write quorum met) → `delivered` (recipient ACKed) → `read` | This is our main demo instrument: it makes the quorum and the ACK chain visible on screen the moment a grader kills a relay |
| Secondary view | A relay-overlay view: live membership, placement assignment, per-mailbox replication depth | Turns the gossip layer into something a grader can actually watch during the defense |

### 9.5 Containers and orchestration

The base image is `python:3.12-slim`, built multi-stage and pinned by digest, targeting under 120 MB. There are exactly two images, `linkchat-relay` and `linkchat-client`. Networking is `docker network create --subnet=… linkchat-net`, followed by individual `docker run` calls; orchestration is shell scripts only, since Compose is forbidden by the course rules, living in `scripts/`: `up-relay.sh`, `up-client.sh`, `chaos-kill.sh`, `partition.sh`, `demo.sh`. Nodes announce their own readiness on the gossip channel, and the scripts wait on that signal rather than on a `sleep`. The first node up has nobody to announce to yet; per Report 1's discovery design, a node started with no reachable seeds simply forms a cluster of one and is joined later when a second node seeds *to* it, so there's no actual deadlock, just a one-node cluster until company arrives.

---

## 10. Decisions still open

Every number and mechanism in sections 2 through 9 that reads as settled, the `k = 3` / `w = 2` quorum, the TTL, the discovery path, the ratchet's build-vs-adopt choice, is the team's current working assumption, not a closed decision. Each becomes real only once its ADR below is written, and this list is the one to trust if anything above reads more settled than it actually is.

**To record as ADRs:**

1. Discovery mechanism, confirmed after the week-1 multicast spike on Docker Desktop/WSL2, including where a relay's seed-address list actually comes from at container start (environment variable, mounted file, or CLI flag). → ADR-0004.
2. Replication factor `k` and write quorum `w` (provisionally 3 and 2), plus the conflict-resolution rule for when two relays' anti-entropy digests disagree about a mailbox's contents. → ADR-0005.
3. Default message TTL, and the retry/backoff/drop policy that the Matrix spec leaves undefined. → ADR-0006.
4. Mailbox identifier derivation: one shared mailbox per conversation, or two unidirectional ones as SimpleX does; the format of the out-of-band invitation string that seeds it; and what, if anything, proves a client is entitled to write, read, or delete a given mailbox, since an opaque ID known to anyone is writable by anyone. → ADR-0007. The first part is the privacy-bearing decision Report 1 flagged and hasn't resolved either; the authorization question is new and at least as important.
5. Wire protocol: encoding (msgpack vs. protobuf), framing, and the actual message-type schema (store, retrieve, ack, gossip/membership update, Merkle-digest exchange, storage-audit challenge and response). Encoding alone isn't enough for anyone to start coding against; the message types are. → ADR-0008.
6. Implement the ratchet ourselves versus adopting `python-x3dh` + `DoubleRatchet`, decided after the week-1 spike. → ADR-0009.

**Open questions that aren't yet ADR-shaped:**

- How many containers will realistically run on one laptop at the defense? This bounds `k`, the placement-set size, and the whole test topology. The memory budget from ADR-0002 (roughly 40 to 70 MB per Python container) makes a reasonably large topology feasible, but it should be tested, not assumed.
- Is the direct client-to-client fast path (mechanism 11 in section 4) worth the extra code, or should every message go through the overlay for uniformity?
- Is the option D gossip fallback in scope for v1, given it's a small amount of code for a disproportionately good demo moment?
- Does the course require the written report itself in Spanish? Cheap to confirm now, expensive to discover in month three; already flagged in [ADR-0003](../decisions/0003-english-for-all-documentation.md).

