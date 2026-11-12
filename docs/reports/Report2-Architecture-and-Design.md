# Report 2: Architecture and Design

**Project:** P08 · Mensajería segura descentralizada, Sistemas Distribuidos 2026, Universidad de La Habana, Facultad de Matemática y Computación
**For:** the development team · **Date:** 2026-11-11

> **How this report is organised (revised 2026-11-11).** The project has two phases. Phase 1 (due 2026-12-01) is built on Ray and must survive any two simultaneous failures. Phase 2 (due 2027-01-01) removes Ray and must also survive network partitions.
>
> - **Sections 1 to 7** compare five architectures and were written before the examiners set those rules. The comparison and its conclusion stand. Where section 4 describes option C's mechanisms (gossip membership, a write quorum of two, mutually distrusting relays, the option D fallback), read it as the form option C takes in phase 2.
> - **Sections 8 to 15** were rewritten on 2026-11-11: the decision, the two phases, the phase 1 design on Ray, the failure analysis, the tradeoffs, the outline of phase 2, and the stack.

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

**Build option C.** This is [ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md), and it holds for both phases of the project.

In one sentence: a two-tier architecture in which relay nodes hold padded ciphertext in replicated, TTL-bounded mailboxes addressed by opaque identifiers, while all plaintext, all identity, and all permanent history live exclusively on client nodes.

It holds up against each of the three named references for a specific reason. Against WhatsApp: the same amnesiac queue semantics and the same delivery-state UX, with no operator behind them. Against Matrix: the same causal event model and retry discipline, minus permanent server-side history and minus the need for state resolution. Against Signal: the same cryptography, and less metadata exposed than even sealed sender achieves.

What sections 2 to 7 could not know is that the examiners would split the project in two and prescribe the platform for the first half. The rest of this report is about that.

---

## 9. Two phases, two sets of rules

On 2026-11-11 the examiners clarified what is expected. The full list is in [`../research/00-problem-analysis.md`](../research/00-problem-analysis.md) section 6.

| | Phase 1 | Phase 2 |
|---|---|---|
| Delivery | 2026-12-01 | 2027-01-01 |
| Platform | Ray and Python. Use every Ray feature that applies naturally; do not reimplement what Ray offers | No Ray. Everything redesigned and implemented by us |
| Failures | Any two elements at the same time must not affect the system | Not yet stated |
| Partitions | Not required | Required |
| Head node | Never killed | Does not exist |

They also said what they are looking for beyond working software: that we think through the cases and edge cases, accept that distributed systems have problems nothing solves completely, and can say what each decision gains and what it costs.

Option C survives all of this, but it is built twice. In phase 1 Ray supplies the machinery around the relays, and our work is the data. In phase 2 we supply the machinery too. Section 4's description of option C, with gossip membership, a failure detector and our own wire protocol, is the phase 2 form. Sections 10 to 12 describe the phase 1 form.

---

## 10. Phase 1: option C on Ray

The detailed design is [`../design/ray-architecture.md`](../design/ray-architecture.md), and what we learned about Ray is in [`../research/07-ray.md`](../research/07-ray.md). This section is the summary.

### 10.1 Ray in five sentences

Ray runs Python across a cluster. An **actor** is a Python object living in its own process on some node; other processes call its methods remotely. One **head node** keeps the cluster's directory: which nodes are alive and where each actor is. If an actor's process or node dies, Ray can **restart** it, on another node if necessary, but it comes back empty, because Ray does not keep application state. Ray's own documentation says that for state, "the application is responsible".

That last point is the whole division of labour: **Ray keeps the relays alive; we keep the data alive.**

### 10.2 The cluster

```
                 HEAD (never killed): cluster directory, dashboard
                                   │
   ┌───────────┬───────────┬───────┴───┬───────────┬───────────┬──────────────┐
 relay node  relay node  relay node  relay node  relay node  spare      CLIENT nodes
 relay-0     relay-1     relay-2     relay-3     relay-4     nodes      keys, ratchet,
                                                                         SQLite, UI
```

- **Five relays**, `relay-0` to `relay-4`. They are named Ray actors, and the name is the relay's identity. Which container runs `relay-2` at a given moment is Ray's business.
- **Relay nodes** are identical containers that each offer one slot for a relay. If a relay's process crashes, Ray restarts it where it was. If its container is killed, Ray restarts it on a node with a free slot. Two spare nodes mean two killed containers can both be replaced without anyone starting anything.
- **Each client** is its user's own program, in its own container, joined to the cluster so that it can call relays. It holds the keys, the conversation state and the history.
- **The head** runs Ray's control plane and nothing of ours.

Each container is started with its own `docker run`; there is no Compose. The only address configured anywhere is the head's container name.

### 10.3 What Ray does and what we do

| Ray does | We do |
|---|---|
| Nodes finding each other; knowing who is alive | Deciding which three relays hold a mailbox |
| Remote calls between processes | Writing three copies before telling the sender `sent` |
| The directory of relays, by name | Refilling a restarted, empty relay from the others |
| Restarting a failed relay, moving it off a dead node | Deleting on acknowledgement and on expiry |
| Retrying a call that arrived during a restart | Ordering messages and removing duplicates |
| At most one relay per node | Identity, encryption, local history |
| Access control for the cluster; dashboard and metrics | |

Nothing in the right-hand column exists in Ray, so building it is not reimplementing Ray. We went through Ray's features one by one to check this in both directions. Several that look relevant turn out to be wrong for us. The clearest example is Ray's shared object store: an object in it dies with the process that created it, which would be the sender, so a message stored that way would vanish when the sender went offline. The same goes for Ray's documented recipe for restoring an actor's state from a checkpoint on shared storage: that checkpoint would be a durable store of every mailbox, which is what R9 forbids. The full table is in the research note, section 10.

### 10.4 A message's path

1. A's client encrypts the message, pads it to a fixed size, names it by the hash of the ciphertext, and writes it to its outbox.
2. It computes the mailbox's **preference list**: the five relays in ring order starting from the mailbox's position. The first three are the mailbox's primaries.
3. It stores the envelope on the three primaries at once. If one does not answer in time, the next relay on the list stands in for it and remembers which primary it is covering for.
4. When three different relays have confirmed, the message shows `sent`.
5. B's client, when it comes online, asks the first three ready relays on the same list, merges what they return, decrypts, puts the messages in order, saves them, and only then acknowledges.
6. Acknowledged envelopes are deleted, and a marker is kept so a late copy cannot bring them back. Everything else is deleted when its time-to-live runs out.

A relay that has just been restarted is empty. It answers "syncing" until it has copied its share from the other relays, and only then serves clients. A stand-in hands its covered envelopes back to the primary once the primary is ready.

---

## 11. Two simultaneous failures

The analysis is [`../design/fault-tolerance.md`](../design/fault-tolerance.md). The reasoning in short:

**Why three copies, confirmed before `sent`.** To survive two crashes a record must be on three nodes. Our earlier working numbers were "write to three, confirm at two". With those, the two relays that confirmed could be the two that die. So the confirmation now waits for all three ([ADR-0005](../decisions/0005-three-copies-before-sent.md)).

**Why five relays.** With two down, three are left, which is what a write needs. The stand-in rule is what lets those three be any three.

**Why not a majority quorum.** Majorities exist to tell a crashed node from a node on the far side of a partition. Phase 1 has no partitions and Ray tells us who is alive, so three copies are enough. This is the largest shortcut in the phase 1 design, and it is exactly what phase 2 takes away.

| What fails | What recovers it | What users notice |
|---|---|---|
| A relay's process | Ray restarts it in place; it refills from peers | Nothing |
| A relay's container | Ray restarts the relay on a spare node; it refills from peers | Nothing |
| Two primaries of the same mailbox | The third holds every confirmed message; stand-ins take new ones | Nothing |
| A background job inside a relay | A wrapper restarts it; if it keeps failing the relay exits and Ray restarts it | Nothing |
| A client | The user restarts it; the outbox resumes and unacknowledged mail is fetched again | That user is offline meanwhile |
| The head | Out of scope; not recovered | The cluster stops; local history survives |
| Three holders of one message at once | Nothing | That undelivered message is lost |

The last two rows are the limits, and we state them without softening.

---

## 12. Edge cases and what each decision costs

The examiners asked for this kind of reasoning explicitly, so it has its own document with eighteen cases. A selection:

- **A call fails and we cannot tell whether it ran.** Ray says so itself. We never try to find out: every relay operation can be repeated safely because envelopes are named by their content.
- **The acknowledgement is lost.** The relay keeps the envelope until it expires and offers it again; the client recognises it and discards it. So "deleted on acknowledgement" is a best effort, and "deleted at expiry" is the guarantee.
- **A relay is slow, not dead.** The client's timeout treats it as dead and uses a stand-in. A short timeout means fast failover and more false alarms; a long one means the opposite. There is no right value, only a chosen one.
- **A message expires before the recipient comes back.** "Deliver to offline users" and "do not become an archive" collide here, and no mechanism satisfies both without limit. We pick a time-to-live, and the sender is told the message was not delivered.
- **Two senders get the same one-time prekey.** Preventing it would need relays to agree on every read, which is the consensus we chose not to build. We accept a slightly weaker first message in that case.

| Decision | Gains | Loses |
|---|---|---|
| Three copies, no majority | Survives two crashes with five relays | Not safe under partitions |
| Stand-ins and handoff | Sending continues with two relays down | More states to reason about |
| A fixed ring of named relays | No membership tracking, no rebalancing | The relay count is fixed; depends on Ray for recovery |
| Ray restarts relays | No supervisor of ours | Recovery is as fast as Ray, which may be tens of seconds |
| No disk and no checkpoint on relays | R9 holds by construction | Every restart is a full refill; three lost holders lose data |
| Idempotent operations with retries | Ambiguous failures stop mattering | Duplicate traffic; bookkeeping |
| Expiry on everything | Bounded retention with nobody's cooperation | A message can expire undelivered |
| One head (given) | Nothing to build | A single point of failure for the control plane |
| One trust domain (given by Ray) | Nothing to build | No claim against a hostile relay in phase 1 |

---

## 13. Phase 2 in outline

Phase 2 removes Ray, so every row in the left-hand column of section 10.3 becomes ours: discovery, failure detection, remote calls, bringing relays back, access control between nodes. That is the design described in section 4, written before the Ray requirement existed, and it is where phase 2 starts.

It also removes the assumption that made phase 1 simple. With partitions, a relay that does not answer may be running on the other side, and "three copies, no majority" is no longer safe as stated. Our direction is to stay available on both sides and merge afterwards. That is safe for this system because an envelope never changes and is named by its content, so two sides can only ever hold different sets of envelopes, never different versions of one, and merging two sets is a union. Deletions are the one thing that needs care. This argument has to be worked out in full at the start of phase 2.

To make the move cheap, all application logic is written in a package that does not import Ray and reaches relays only through a small set of interfaces ([ADR-0013](../decisions/0013-ray-free-core-for-phase-2.md)). Cryptography, the client, the mailbox store, the web UI and the tests carry over. The plan and schedule for both phases are in [`../planning/phase-plan.md`](../planning/phase-plan.md).

---

## 14. Technology stack

[ADR-0002](../decisions/0002-python-backend-react-frontend.md) records the language decision. The component-by-component stack, with licences, is [`../design/technology-stack.md`](../design/technology-stack.md). Everything is free and open source.

### 14.1 Why Python

The comparison in the design study rated Go ahead on container size and startup, and Elixir as a natural fit given WhatsApp's lineage. The team chose Python and React on familiarity and speed. For phase 1 the question is closed from outside: the examiners require Ray and Python.

### 14.2 Phase 1

| Concern | Choice |
|---|---|
| Distributed runtime | Ray (`ray[default]`), one pinned 2.6x release |
| Relay | Async Ray actor: named, detached, restarted by Ray without limit, one per node through a custom resource |
| Discovery, liveness, supervision, calls | Ray |
| Replication, repair, expiry | Ours |
| Client server | FastAPI and Uvicorn in the client process |
| Cryptography | PyNaCl and `cryptography`; X3DH and Double Ratchet either implemented by us or adopted from a library (ADR-0009, to decide in the first week) |
| Client storage | SQLite on a Docker volume |
| Relay storage | Memory only |
| Cluster access | Ray token authentication |
| Observability | Ray dashboard, state CLI, application metrics |
| Frontend | React 18, TypeScript, Vite |
| Tooling | `uv`, `ruff`, `mypy --strict` on the core, `pytest` |
| Containers | One image, three roles (head, relay node, client); one Docker network; one `docker run` per container; shell scripts |

A Ray node is much heavier than a bare Python process, so the memory figures in ADR-0002 (40 to 70 MB per container) do not apply to phase 1. The target of about ten Ray nodes on one laptop is the first thing we measure.

### 14.3 Phase 2

Ray is replaced by plain asyncio processes: TCP with our own framing between nodes, gossip for membership, a SWIM-style failure detector, signed requests. The head container disappears. Everything else in the table above stays.

---

## 15. Decisions still open

Recorded so far: [ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md) architecture, [0002](../decisions/0002-python-backend-react-frontend.md) stack, [0003](../decisions/0003-english-for-all-documentation.md) language of the documents, [0010](../decisions/0010-build-phase-1-on-ray.md) Ray for phase 1, [0012](../decisions/0012-head-node-policy.md) head node, [0013](../decisions/0013-ray-free-core-for-phase-2.md) Ray-free core.

**Proposed, waiting for the team and the spike:** [0004](../decisions/0004-discovery-through-ray.md) discovery through Ray, [0005](../decisions/0005-three-copies-before-sent.md) three copies before `sent`, [0011](../decisions/0011-relays-as-ray-supervised-actors.md) relays as Ray-supervised actors.

**Not yet written:**

1. Message time-to-live, and what the sender sees when a message expires undelivered. → ADR-0006.
2. Mailbox identifiers: one per conversation or one per direction; the invitation format; what proves a client may read or delete from a mailbox. → ADR-0007.
3. The relay's interface and the envelope format. → ADR-0008.
4. Implement the ratchet or adopt a library. With 20 days to the first delivery this is the largest schedule risk. → ADR-0009.

**The design rests on Ray behaviour we have read about and not yet run.** The first three days of phase 1 are a spike: does a relay restart on a spare node as documented, how long does it take, what does a caller see meanwhile, and how many Ray nodes fit on the laptop. If the answers differ from the documentation, ADR-0011 is revisited before anything else is built on it.

**Questions for the examiners:** whether phase 2 keeps the two-failure requirement; whether anything in phase 2 is guaranteed not to fail; whether the written deliverable must be in Spanish.
