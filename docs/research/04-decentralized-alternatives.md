# 04 · The Fourth Family — Systems That Actually Solve Our Problem

**Status:** research note · **Date:** 2026-11-10

The brief names WhatsApp, Matrix and Signal. None of them solves our exact problem: *deliver to an offline recipient, with no permanent central database of history.* WhatsApp has a centre. Matrix keeps permanent per-server history. Signal has a centre.

The systems below do solve it, each differently. **This is where our architecture comes from.** The two most important are **Session** (DHT swarms with replicated, TTL-bounded mailboxes) and **SimpleX** (opaque unidirectional queues with no user identifiers at all).

---

## 1. Session — DHT swarms as replicated mailboxes

**Primary source:** the Session whitepaper, *"End-To-End Encrypted Conversations With Minimal Metadata Leakage"* (arXiv 2002.04609), read via its HTML rendering. **Caveat:** this is the early-2020 version; its own addendum notes renamings ("Account ID" for "Session ID", "Session Node" for "Service Node") and that parts of the token/staking design were not live. Treat parameters as design intent, not current fact.

### The swarm mechanism — the core idea for us

- Every swarm has a deterministically generated **64-bit identifier**. A user's Account ID is reduced to a 64-bit integer and assigned to the swarm whose ID is **numerically closest**. Any client can compute this locally from the node list; **there is no resolver to ask**.
- Swarm size parameters: **N_min = 5, N_target = 7, N_max = 10**.
- A node's swarm is assigned at registration, and the node has deliberately **minimal influence** over which swarm it joins — so an attacker cannot concentrate malicious nodes in one swarm.
- **Starvation:** a swarm below `N_min` steals a node from a larger swarm. If every swarm is at `N_min`, the starving swarm is **dissolved** and its nodes redistributed.
- **Growth:** when every swarm is at `N_max`, a *new* swarm is created from `N_target = 7` randomly chosen excess nodes. Note: a new swarm is created, existing swarms are not split.
- **Churn sync:** when a node joins a swarm, existing members **push** the swarm's records to it. When a node leaves, its records may be erased — except a node migrating from a dissolving swarm, which must work out which swarms are now responsible for its records and distribute them there.
- All members of a swarm are expected to store the same records. The whitepaper does not state a replication factor beyond swarm size, nor the consistency rule when a store only partly propagates — **a gap we must fill ourselves.**

### Offline storage and retrieval

- The sender asks a **random node** for the swarm mapping for the recipient's Account ID. If a node replies that it is no longer in that swarm, it returns the current members, and the client refreshes its mapping. Mapping repair is therefore driven by errors, not by polling.
- The stored message carries the recipient Account ID, a timestamp, an **expiry time**, and a **namespace** (e.g. configuration vs. visible messages, so clients can fetch selectively). It is sent via an onion request to one random swarm member, which **propagates to the other members** and returns a signed success once propagation succeeds.
- Nodes store each message **until its expiry**; the paper gives no default TTL. Configuration messages are refreshed at least every 7 days to extend expiry.
- Recipients **poll their own swarm**. Each request is signed by the Account ID private key over request type, namespace and timestamp; the node checks the type, checks the namespace requires auth, checks the timestamp is "within an acceptable range" to limit replay, then checks the signer owns the mailbox. The paper calls stateless poll/response **"inefficient and slow"** and proposes streaming as the fix.
- **No proof-of-work is required for storage.** PoW is listed only as one of several *unimplemented* anti-spam candidates alongside CAPTCHA, per-IP first-hop limits and moderation.

### Identity and the deliberate retreat from the Double Ratchet

- Account ID = a **66-character hex** long-term public key. Ed25519 is the identity key; the derived X25519 key is the Account ID.
- The Ed25519 seed is surfaced to the user as a mnemonic "Recovery Password".
- 1:1 encryption: pad plaintext to the next **160-byte multiple**, sign with Ed25519, append public key and signature, encrypt with **XSalsa20-Poly1305** under a shared secret from an ephemeral X25519 key and the recipient's X25519 key.
- **Session deliberately abandoned ratcheting.** Its stated reason is directly relevant to us: Signal-style protocols "make strong assumptions about establishing and synchronising shared states" and need "permanent storage available for large pre-key bundles" — and **swarms provide neither ordering nor durability guarantees**, and delete messages at expiry.
- What that costs: **no forward secrecy for 1:1** ("the Session Protocol is stateless and requires no ratcheting of key material"); **weakened deniability**, since messages are signed with the long-term key; and multi-device done by literally **sharing the long-term private key** across devices, with sent messages also deposited in the sender's own swarm so other devices can read them.

> **This is the most important tradeoff in the whole research set, and we should take the opposite side of it.** Session gave up forward secrecy because it did not want to rely on durable prekey storage. We *can* rely on it, because our relay overlay stores **self-verifying signed prekey bundles** — a relay cannot forge one, and if a bundle is unavailable the session simply cannot start, which is a safe failure. Keeping the Double Ratchet while keeping DHT-based mailboxes is a genuine design contribution for our report, and a strong answer to "what did you do that the references don't?"

### Onion requests, Sybil resistance, acknowledged weaknesses

- **Three hops** (A, B, C), chosen at random from the node list at startup; the path is "merely a logical abstraction", nodes are reused, no persistent connections. Each layer is encrypted to a hop's Ed25519 key; each hop learns only its neighbours. Requests are capped at **10 MB**.
- Bootstrap: an initial node list from **hardcoded seed nodes** over clearnet; later updates via onion requests, and a client accepts an update only if **all queried nodes agree**.
- Sybil resistance is economic — operators **stake tokens** bound to the node's public key, with slashing for misbehaviour.
- **Reachability testing:** nodes shuffle the node list and test peers; uptime proofs are published peer-to-peer; a node silent for **more than 2 hours** is presumed offline. A RANDAO-style commit-reveal produces randomness that sorts nodes into **testing quorums of 10**, each reporting on a deterministic set of **50 nodes**; if **more than 7 of 10** report a node failing, a jointly signed transaction decommissions it.
- Acknowledged weaknesses, usefully candid: traffic analysis against a global passive adversary is "trivial" because onion requests are low-latency and unbatched; **nodes are tested for reachability but not for whether they actually store messages** (a record-hash challenge is proposed, not implemented); free account creation makes Sybil spam easy; and polling is inefficient.

### What we take from Session

**Adopt:** deterministic swarm assignment computed locally from a node list; `N_min`/`N_target`/`N_max` with starvation-stealing and dissolution; push-on-join record sync; error-driven mapping repair; signed, timestamped, replay-windowed retrieval requests; TTL-bounded storage; namespaces.
**Reject:** abandoning the ratchet; token staking (no blockchain in this project — Sybil resistance will be out of scope and declared as such); onion routing as a requirement (interesting stretch goal, not core).
**Fill in ourselves:** replication factor and write quorum, partial-propagation consistency, storage auditing, default TTLs.

---

## 2. SimpleX — no user identifiers at all

**Primary source:** the SMP specification v20 (2026-05-25), read in full.

The most radical idea in the field, and the cleanest answer to metadata: **there are no user identities in the protocol.** Not phone numbers, not usernames, not public keys, not random IDs. There are only **queues**.

### The queue model

- A **simplex (unidirectional) queue** lives on a relay and has **two different random IDs**, 16–24 bytes each: a **recipient ID (RID)** and a **sender ID (SID)**, both from a CSPRNG, unique per relay. The relay never learns they belong to the same conversation participant.
- Keys: **RK** (recipient auth key) authorises recipient commands; **RDHK** (recipient DH key) lets the relay encrypt message bodies to the recipient; **SK** (sender auth key), installed later, authorises sends; **EK**, the end-to-end key, is **never sent to the relay**.
- Clients must use **a unique ephemeral key per queue**, specifically so a compromised relay cannot aggregate a user's queues by key.
- **A duplex conversation is two simplex queues**, one per direction, and the spec is explicit that pairing them is an *application-level* concern. Bob's ability to send to Alice does not imply Alice can send to Bob.

### Establishment, and why no global IDs are needed

Alice sends `NEW` (with RK and RDHK) → relay replies `IDS recipientId senderId srvDhPublicKey`. Alice passes an **out-of-band invitation** containing a queue URI:

```
smp://<routerIdentity>@<hosts>[:port]/<queueId>#/?v=<range>&dh=<recipientDhPublicKey>[&k=s]
```

(default port 5223; `routerIdentity` is a hash of the relay's offline certificate SPKI). Bob sends a confirmation containing **SK**; the queue is then secured with `KEY` (legacy) or `SKEY` (v9+, sender-driven), after which only authorised sends are accepted. Alice then deletes SID and SK.

Identity is established **entirely out of band** — exactly the move that lets us skip the contact-discovery problem that defeats even Signal.

### Command set (abridged)

`PING`/`PONG` · `NEW` → `IDS` · `SUB` → `MSG`/`SOK` · `SUBS` → `SOKS` · `KEY` / `SKEY` / `RKEY` · `ACK` → next `MSG` or `OK` · `OFF` (suspend, irreversible) · `DEL` · `GET` (fetch one without subscribing) · `QUE` → `INFO` · `LSET`/`LDEL`/`LKEY`/`LGET` (short links) · `NKEY` → `NID`, `NDEL`, `NSUB` (notifications on a *separate* notifier ID) · `PRXY`/`PFWD`/`RFWD` (proxying to hide sender IP from the recipient's relay) · relay-initiated `MSG`, `NMSG`, `END`, `DELD`. Errors include `AUTH`, `QUOTA`, `NO_MSG`, `EXPIRED`, `LARGE_MSG`, `BLOCK`, `CRYPTO`.

### Delivery semantics — directly applicable to our R5/R6

- The relay sends **one message at a time**; the next is sent only after `ACK`. The spec does not explicitly promise FIFO.
- `ACK msgId` is required, and the relay **must delete the message on ACK**.
- **A message can be re-delivered if the connection drops before ACK, so clients must deduplicate by `msgId`** (24 random bytes). This is a textbook at-least-once-transport / exactly-once-display split, stated plainly in a production spec. We will cite it.
- Retention: deleted on ACK, or when the relay's storage limit expires. The spec says relays "should limit the time the message is stored" but **gives no duration**. `DEL` drops undelivered messages immediately.
- Capacity: on reaching the queue limit the relay rejects sends with `ERR QUOTA` and, after the last message is delivered, sends a special `QUOTA` marker.
- Relays **must not keep logs, delivery history, or DB snapshots**; in-memory persistence is recommended. This is R9 written as a protocol requirement.
- **Everything is fixed-size padded**: transport block 16,384 bytes; max `SEND` payload 16,048; client E2E message padded to 16,000; confirmation body 15,904; notification metadata 128 + 16-byte tag. Padding to a constant size removes length as a metadata channel.

### What the relay can and cannot see

Sees: queue IDs, command types, authorisation, session ID, timing, padded (constant) sizes, and the plaintext notification flag.
Cannot see: message bodies, sender identity, or which other queues belong to the same participant — the spec states that even a compromised relay cannot determine the list of a user's queues.

### Honest limitations the spec itself states

- Unidirectional by design: **no delivery proof, no receipts** at protocol level — intentional, for deniability. (We need ticks, so we will carry receipts as application-level messages on the reverse queue.)
- A **race before securing**: until `KEY`/`SKEY`, anyone who knows SID can send to the queue.
- **Reliability is deprioritised, and redundancy is explicitly out of scope** — "clients must manage redundancy by using more than one SMP relay". There is **no failover, no queue replication, and no migration procedure** in the spec. Relays never talk to each other.

> **This is SimpleX's great weakness and precisely where our project can beat it.** SimpleX pushes redundancy onto the client and defines no replication. Session defines replication (swarms) but gives up the ratchet. **Our design can take SimpleX's opaque-queue addressing and Session's replicated swarms and combine them** — which is the core thesis of [`../design/architecture-options.md`](../design/architecture-options.md).

---

## 3. Briar — delay-tolerant, mutual-contact relaying

**Bramble Transport Protocol (BTP)** is a transport-layer security protocol explicitly for **delay-tolerant networks**, providing a secure channel with forward secrecy. Where a transport imposes a maximum connection length, BTP **pushes that restriction up to higher layers** by limiting stream length rather than fragmenting a stream across connections — a clean separation worth imitating.

Offline delivery has three mechanisms:
1. **Briar Mailbox** — a separate always-on helper app acting as a private intermediary. Contacts leave encrypted messages for the owner while the owner is offline, and vice versa. The mailbox stores only encrypted messages for its owner; the operator cannot read them. Transfers are **chunked into equal-sized parts** so an interrupted upload or download can resume.
2. **Relaying through mutual contacts** — your messages travel via people you both know.
3. **Physical transport** — USB sticks, SD cards, Bluetooth, local Wi-Fi. Briar works with no internet at all.

Academic caveat worth citing: an ETH Zurich semester report (2023) identified three design weaknesses including a lack of forward secrecy in the handshake and DoS exposure in the sync protocol. We should check whether these were fixed before citing Briar as a security model.

**For us:** chunking with resume is directly useful. The "mailbox is a dumb encrypted box owned by one user" framing is a good mental model for our mailbox abstraction. Mutual-contact relaying is a beautiful idea but needs a social graph we will not have.

---

## 4. Jami — the honest failure mode of pure P2P

Jami uses **OpenDHT** for peer discovery and routing by unique ID, with no account server. It is the cleanest example of the architecture we are *tempted* by, and its documented limitation is exactly the trap:

> The DHT only stores your message briefly. You miss your messages if you don't have clients logged into your account.

Jami's own documentation concedes that OpenDHT must run **continuously in the background** to receive notifications, which mobile apps cannot do — so Jami added an **OpenDHT proxy**. In practice a Jami deployment also involves a **bootstrap server, a DHT proxy, a push-notification server, a TURN server and a name server** (`dhtproxy.jami.net`, `ns.jami.net`).

**Lesson, and it is the central one for our architecture choice:** *pure peer-to-peer where every peer is a user device cannot satisfy R3.* Users' devices are not online when the user is offline — that is the definition of offline. Something that stays up must hold the message. The only questions are what that something is, how many of them hold it, and for how long. Jami answered by quietly adding servers back. We should answer by making the always-on tier **explicit, replicated, untrusted and amnesiac**.

---

## 5. Tox — DHT identity, and the same gap

Tox gives each user a public-key identity and uses a Kademlia-style DHT for peer discovery and NAT traversal, with direct encrypted peer connections. It has the same structural gap as Jami: both parties must be online simultaneously. Offline messaging has been a long-standing unsolved request. We cite it as corroboration of the Jami lesson rather than as a model.

---

## 6. Summary table

| System | Who stores for an offline user | Retention | Identity | Replication | Ratchet / FS | Lesson for us |
|---|---|---|---|---|---|---|
| **Session** | DHT **swarm** of 5–10 nodes closest to the Account ID | Per-message expiry (TTL) | Ed25519 long-term key, 66-hex Account ID | Full replication inside the swarm | **No** — deliberately dropped | Swarms are the right shape for a decentralized mailbox |
| **SimpleX** | A single relay holding an **opaque unidirectional queue** | Until ACK, or relay storage limit | **None** — only queue IDs | **None** — client's problem | Yes (agent layer) | Opaque addressing beats anonymised identity; dedup by `msgId` is mandatory |
| **Briar** | A user-owned **Mailbox**, or mutual contacts, or a USB stick | Until collected | Public key, exchanged out of band | None (mailbox is singular) | Yes (BTP) | Chunked, resumable transfer; delay tolerance as a design centre |
| **Jami** | Nobody, in principle — DHT holds briefly; in practice a proxy | Very short | Public key in OpenDHT | DHT-level only | Yes | Pure P2P cannot meet R3; the always-on tier returns in disguise |
| **Tox** | Nobody | n/a | Public key | n/a | Yes | Same as Jami |
| *(for contrast)* **Matrix** | The recipient's homeserver | **Permanent** | `@user:server` | Per-server, permanent | Yes (Olm) | Violates R9 |
| *(for contrast)* **WhatsApp/Signal** | The operator | Until delivered | Phone number / account | Operator's internal | Yes | Right retention policy, wrong topology |

**The synthesis our architecture will make:** Session's *replicated swarm* + SimpleX's *opaque, padded, ACK-deleted queue* + Signal's *ratchet and prekeys* + Matrix's *signed causal DAG and retry discipline* + WhatsApp's *delivery-state UX*.

---

## Sources

**Session** — [whitepaper, arXiv 2002.04609](https://arxiv.org/abs/2002.04609) (read via [ar5iv HTML](https://ar5iv.labs.arxiv.org/html/2002.04609)) · [nym.com overview](https://nym.com/blog/what-is-session) · [CyberInsider review](https://cyberinsider.com/secure-encrypted-messaging-apps/session/) · [ProtectStar analysis](https://www.protectstar.com/en/blog/the-security-of-the-session-messenger-a-guide)

**SimpleX** — [SMP specification](https://github.com/simplex-chat/simplexmq/blob/stable/protocol/simplex-messaging.md) (primary, read in full) · [agent protocol](https://github.com/simplex-chat/simplexmq/blob/master/protocol/agent-protocol.md) · [SimpleX platform overview](https://simplex.chat/docs/simplex.html) · [Channels overview](https://simplex.chat/docs/protocol/channels-overview.html)

**Briar** — [Bramble Transport Protocol v4 spec](https://code.briarproject.org/briar/briar-spec/blob/master/protocols/BTP.md) · [Mailbox chunking](https://code.briarproject.org/goapunk/briar-repeater/-/wikis/Mailbox-Chunking) · [Briar — Wikipedia](https://en.wikipedia.org/wiki/Briar_(software)) · [Cryptography in the Wild: Briar, ETH Zurich, 2023](https://ethz.ch/content/dam/ethz/special-interest/infk/inst-infsec/appliedcrypto/education/theses/report_YuanmingSong.pdf)

**Jami / Tox** — [How does Jami work on mobile without a server?](https://jami.net/jami-and-proxys/) · [Not missing messages with Jami](https://clehaxze.tw/gemlog/2021/12-01-runing-the-jami-daemon.gmi) · [The Jami messenger — Linux Kamarada](https://linuxkamarada.com/en/2021/05/25/the-jami-messenger/) · [Distributed hash table — Wikipedia](https://en.wikipedia.org/wiki/Distributed_hash_table)
