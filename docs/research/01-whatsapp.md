# 01 · WhatsApp — The UX Contract and the "Queue, Not Archive" Principle

**Status:** research note · **Date:** 2026-11-10
**Why the brief names it:** *"comparación histórica de experiencia de usuario, sin reutilizar su marca."* We study the user-visible contract and the one server-side idea worth stealing. We do not reuse the brand, the look, or the name.

> **Source-quality warning.** WhatsApp publishes almost nothing authoritative about its current internals. Most available material is secondary — system-design blogs and interview guides that reconstruct a plausible architecture rather than document the real one, and they contradict each other on where queues live. Everything in §2 should be presented in the defense as *"the commonly reported design"*, not as fact. The two genuinely authoritative primary sources are the Signal Protocol specification (which WhatsApp adopted) and WhatsApp's own encryption white paper.

---

## 1. The UX contract (this is the part that matters to us)

WhatsApp's real contribution to our project is a set of user expectations so entrenched that violating them makes a messenger feel broken. Each one is a distributed-systems requirement wearing a costume.

| User-visible behaviour | Underlying guarantee | Our requirement |
|---|---|---|
| One grey tick — "sent" | The message is durably accepted by *something* outside my device | R4 — store-and-forward accepted and persisted |
| Two grey ticks — "delivered" | The recipient's device has it | R4 — end-to-end delivery acknowledgement, routed back |
| Two blue ticks — "read" | The recipient's client displayed it | Application-level receipt, ephemeral |
| Messages never appear twice | Exactly-once *display* over at-least-once transport | R6 — dedup by message ID |
| Messages never appear out of order | Causal order preserved per conversation | R5 — logical clocks + reorder buffer |
| Messages sent while I was offline are waiting for me | Asynchronous handoff with retention | R3 — offline delivery |
| My history is on my phone and survives server problems | The archive is at the edge | R7 — local persistence |
| It works on a terrible connection | Retry, resume, idempotence | R6 + backoff |

**Design lesson for us:** the tick model is the cleanest available vocabulary for exposing distributed-systems state to a user. We should implement a three-state delivery lifecycle — `accepted-by-relay` → `delivered-to-device` → `read` — because it makes our internal acknowledgement chain *visible on screen during the defense*. When a grader kills a relay and the tick stays grey then turns double after recovery, the mechanism demonstrates itself.

A fourth state, unique to us and worth showing: **`pending-local`** — accepted by the client, not yet accepted by any relay, because no relay is reachable. WhatsApp shows this as a clock icon. It is the honest representation of a partition.

---

## 2. The commonly reported server architecture

**Origins — Erlang and ejabberd.** WhatsApp began as a heavily modified fork of ejabberd, an Erlang XMPP server, and the protocol was an XMPP derivative before being replaced by a custom binary protocol. Erlang was chosen for cheap processes and supervision trees: reports describe one lightweight Erlang process per connected user, and famously a very small engineering team serving enormous scale.

**The queue, not the archive.** This is the single most important idea for us, and the sources agree on it even where they disagree on details:

> The server routes if the recipient is online, queues if the recipient is offline, drains in order on reconnect, and **deletes after the client acknowledges**. It is a transient relay, not a message archive.

Undelivered messages sit in a server-side queue — various accounts attribute this to Mnesia or to a sharded offline store — and are discarded once delivered. Long-term history lives in the client's local SQLite database (and in whatever backup the user opts into). This is *exactly* the posture R9 demands of our relays. WhatsApp solved R9 with a single trusted operator; we have to solve it with untrusted, replaceable relays.

**Multi-device (since 2021).** Each linked device has its own identity key and its own encryption session; a message is encrypted separately per device rather than being forwarded between devices. Up to four companion devices operate without the phone online. We are explicitly **not** doing multi-device — it multiplies session state and fan-out — but we should be able to explain the design and why we excluded it.

---

## 3. What we take, and what we deliberately reject

**Take**

- The ticks. A visible delivery lifecycle is the best demo instrument we have.
- "Server is a queue, not an archive," with deletion on acknowledgement.
- Per-conversation ordering that the user can trust.
- Local SQLite as the only permanent home of history.
- Supervision-tree thinking: assume every component crashes and is restarted.

**Reject**

- The centralized topology itself. One operator with one logical message router is precisely the architecture the brief forbids.
- Phone numbers as identity. They require a registration authority and leak the social graph; we use public keys (see [`03-signal.md`](03-signal.md)).
- Server-side contact discovery by uploading an address book.
- Anything branded. No green, no name resemblance, no cloned iconography — the brief says so explicitly.

---

## 4. Where WhatsApp is weakest, and what our project can claim

Honest comparison points for the defense:

- **Metadata.** The operator sees who talks to whom and when, even though it cannot read content. Our design can do better by addressing messages to opaque mailbox identifiers rather than to user identities.
- **Availability is the operator's.** If the operator is down or blocks you, there is no system. A relay-overlay design degrades instead of stopping.
- **Trust is unverifiable.** Users cannot check that the server deletes delivered messages. In our design the claim is structural: relays never hold plaintext and expose no history API.

Our system will be dramatically worse than WhatsApp at scale, latency, polish and mobile battery life. We should concede that up front and compete only on the axis the brief cares about: decentralisation under failure.

---

## Sources

- [Understanding WhatsApp's Architecture & System Design — CometChat](https://www.cometchat.com/blog/whatsapps-architecture-and-system-design)
- [How WhatsApp Works: Architecture Deep Dive — GetStream](https://getstream.io/blog/whatsapp-works/)
- [How WhatsApp Scaled to Billions of Users with Just 50 Engineers](https://singhajit.com/whatsapp-scaling-secrets/)
- [WhatsApp Architecture — uptime.engineer](https://www.uptime.engineer/p/whatsapp-architecture)
- [Design WhatsApp: System Design Interview Guide](https://www.systemdesign.academy/interview/design-whatsapp)
- [From Specs to Apps: Verifying and Monitoring Models of Signal and WhatsApp (arXiv)](https://arxiv.org/pdf/2609.11882)
