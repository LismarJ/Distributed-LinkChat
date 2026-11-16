# 00 · Problem Analysis — What P08 Actually Demands

**Status:** research note · **Date:** 2026-11-10 · **Audience:** the project team

Source of truth: `Orinetacion.pdf` — *Sistemas Distribuidos 2026, P08 · Mensajería segura descentralizada*, Universidad de La Habana, Facultad de Matemática y Computación.

---

## 1. The statement, decomposed

The brief is short, but every clause is a distributed-systems primitive in disguise. Here is each requirement mapped to the mechanism it forces us to build and the failure it is testing.

| # | Requirement (original) | What it really asks for | Primitive we must implement | The failure it tests |
|---|---|---|---|---|
| R1 | Identidad de usuarios y descubrimiento de contactos | Globally unique, self-authenticating identity with no registration authority | Public-key identity (`UserID = H(pk)`), plus lookup mapping identity → current location | Can two nodes agree who someone is with no central registry? |
| R2 | Conversaciones uno a uno | Per-pair session with its own state | Session establishment + per-session keys | — |
| R3 | Entrega a destinatarios sin conexión | Delivery must succeed when the recipient has no process running at all | Asynchronous handoff to a third party | The recipient is *down*, not slow |
| R4 | Entrega diferida y store-and-forward | Someone holds the message in the meantime, durably | Replicated mailbox, ACK-driven and TTL-driven deletion | The *holder* crashes before delivering |
| R5 | Ordenación de mensajes | Recipient reconstructs a sensible order from out-of-order arrivals | Logical clocks (Lamport / vector / HLC) + causal buffering | Network reorders; relays deliver out of order |
| R6 | Supresión de duplicados | At-least-once transport, exactly-once *display* | Deterministic message IDs + seen-set index | Retries after a lost ACK |
| R7 | Persistencia local del historial | The archive lives on the edge, not the middle | Local embedded DB per client | Relay loses everything → history survives |
| R8 | Autenticación y cifrado | Nobody in the middle reads or forges | E2E encryption + authenticated transport | A malicious relay |
| R9 | Relés temporales, **sin** convertirse en una BD central permanente con todo el historial | Relays are a *buffer*, never an *archive* | Bounded retention: delete on ACK, hard TTL, no history-replay API | "Did you just rebuild a server?" |

**R9 is the discriminating constraint.** It is what makes this a distributed-systems project rather than a web project. Any design we propose must answer in one sentence *why the relay tier cannot reconstruct a conversation*. Our answer: relays hold opaque, size-padded ciphertext addressed to a mailbox identifier, for a bounded TTL, deleted on acknowledgement — and they expose no "fetch history" operation at all.

### Explicit non-requirements

The brief asks for **one-to-one conversations only**. Group messaging is out of scope, which spares us the hardest part of modern secure messaging (group key agreement, MLS, sender keys). We should say this out loud in the defense and show we know what we skipped and why.

Also absent: voice/video, file transfer, federation with real-world networks, mobile clients, push notifications.

---

## 2. The operating constraint: Docker, no Docker Compose

This is the only general rule we were given, and it is not an arbitrary annoyance — it is a test.

**What it removes**

- No `docker-compose.yml`, so no declarative topology.
- No Compose-provided DNS names (`relay1`, `relay2`) to hardcode.
- No guaranteed startup order, no `depends_on`.
- No single command that brings the system up in a known-good state.

**What it therefore demands**

1. **Dynamic peer discovery.** A node finds its peers at runtime, not from configuration. Options: UDP broadcast/multicast on a user-defined bridge network, a small set of seed addresses passed via `--env`, or a bootstrap node whose address is the only thing a new node is told.
2. **Order-independent startup.** Any container must be able to start first, alone, and converge once others appear.
3. **Churn tolerance as a first-class feature.** The graders will `docker kill` a container mid-demo and `docker run` a new one. The system must notice within seconds and heal.
4. **Identity must not be the container.** A node's identity is its keypair, not its IP or hostname — both change on restart.

**Practical risk to spike in week 1.** We will create our own network (`docker network create --subnet=... linkchat-net`) and attach containers with `--network linkchat-net`. Broadcast and multicast behave differently across Docker backends: on a Linux bridge they generally work, but under Docker Desktop on Windows (WSL2) this needs verifying before we depend on it. Keep a fallback that needs no multicast at all — discovery via one or two seed addresses plus gossip — and treat multicast as an optimisation, never as the only path.

---

## 3. What the three reference systems are for

The brief names three systems and, importantly, says what each comparison is *for*. That framing tells us what the examiners expect us to have read.

- **WhatsApp — "comparación histórica de experiencia de usuario, sin reutilizar su marca."**
  Not an architecture to copy. It is the *UX contract* users already expect: delivery ticks, read receipts, ordering that never looks wrong, messages that arrive while you were away. Its server-side lesson is one powerful sentence: **the server is a queue, not an archive** — hold ciphertext until delivery, then discard. That is R9 already solved by a centralized system; our job is to do it without the centre.

- **Matrix — "comparación técnica para mensajería descentralizada o federada y estado replicado."**
  The *mechanism* reference. The best-documented open specification for replicated conversation state across mutually distrusting servers: a signed event DAG, `prev_events` / `auth_events`, transactional federation with retry, backfill for gap repair, state resolution for merged forks. Even where we reject its architecture we should steal its vocabulary and its solutions to ordering and gap repair.

- **Signal — "comparación orientada a identidad y conceptos de cifrado de extremo a extremo."**
  The *identity and crypto* reference. Prekey bundles exist precisely so you can start an encrypted session with someone who is offline — R3 at the cryptographic layer. X3DH/PQXDH plus the Double Ratchet give forward secrecy and post-compromise security; sealed sender shows how far metadata minimisation can be pushed.

Beyond the three named systems, the closest prior art to *our exact problem* is a fourth family the brief does not mention: **Session, SimpleX, Briar, Jami and Tox** — systems that must deliver to offline users with no permanent central database. Those are studied in [`04-decentralized-alternatives.md`](04-decentralized-alternatives.md), and they are where our architecture will actually come from.

---

## 4. Success criteria for the defense

Working backwards from the deadline and a live defense, the system must survive this demo script:

1. Start one relay container. Start a second and third; they discover each other with no configuration.
2. Start client A and client B. A discovers B by identity, with no directory server.
3. A messages B. B is **not running**. The message is accepted anyway.
4. `docker kill` the relay that holds the message. Start B. B receives the message.
5. Send 500 messages while partitioning the network; heal; verify B's history is complete, correctly ordered, with no duplicates.
6. Show that no relay can produce the plaintext, and that nothing is retained after ACK + TTL.
7. Kill every relay, restart them empty, and show both clients still have their full history locally.

Step 7 proves R9. Step 4 proves R4. Step 5 proves R5 and R6.

---

## 5. Open questions for the team

1. Primary implementation language — see [`../design/architecture-options.md`](../design/architecture-options.md) §7.
2. Are the written deliverable and the defense in Spanish? These working notes are in English and would need translating.
3. How many containers will realistically run on one laptop during the defense? This bounds our replication factor and our test topology.

---

## 6. Examiner feedback, 2026-11-11: two phases, Ray first

Sections 1 to 5 were written on 2026-11-10 from the brief alone. On 2026-11-11 the examiners gave the following answers and clarifications. They are constraints of the same weight as the brief.

### 6.1 What the examiners said

| # | Statement | Applies to |
|---|---|---|
| E1 | First delivery on **2026-12-01**, second on **2027-01-01** | Both |
| E2 | The first delivery is built on **Ray** and Python. Use every Ray tool and feature that applies to the project naturally, without forcing any. Do not reimplement what Ray already offers | Phase 1 |
| E3 | Tolerate **level-two failures**: any two elements (two nodes, two processes, two threads, two machines, any two things) failing simultaneously must not affect the system | Phase 1 |
| E4 | The head node will not be killed; that is out of scope | Phase 1 |
| E5 | Partitions do not have to be tolerated | Phase 1 |
| E6 | The developer is expected to consider most cases and edge cases and how to solve them, knowing that distributed systems have inherent problems that no "magic" solves, and to weigh what is gained and lost: the tradeoffs of the design and of each decision | Both |
| E7 | **Remove Ray completely** and redesign and implement everything ourselves | Phase 2 |
| E8 | Partitions **must** be tolerated | Phase 2 |

### 6.2 What each one does to the analysis above

- **E1 replaces the timeline.** Earlier notes speak of "three months" and a "week-1 spike". Phase 1 has 20 days from today and phase 2 has 31 more. The schedule is in [`../planning/phase-plan.md`](../planning/phase-plan.md).
- **E2 moves section 2's work to Ray in phase 1.** Containers are still started one by one without Compose, in any order, and are still killed during the demonstration. But dynamic discovery, liveness and restarting failed components are Ray's in phase 1, and writing our own would break the rule. Ray is studied in [`07-ray.md`](07-ray.md), including a feature-by-feature account of what applies to us.
- **E3 is a new requirement, stricter than anything in sections 1 to 4.** The demo script in section 4 killed one relay. Two simultaneous failures change the replication rule: a message must be on three relays before it is reported as sent ([ADR-0005](../decisions/0005-three-copies-before-sent.md)). The element-by-element analysis is [`../design/fault-tolerance.md`](../design/fault-tolerance.md).
- **E4 settles the head-node question** we had raised: one head, no recovery ([ADR-0012](../decisions/0012-head-node-policy.md)). The head holds cluster metadata and no messages, so R9 is unaffected.
- **E5 removes step 5 of section 4** (partition, send 500 messages, heal) from phase 1. It returns in phase 2 under E8.
- **E6 makes the reasoning a deliverable.** Edge cases and tradeoffs get their own document and are kept current.
- **E7 and E8 define phase 2.** The design we had before Ray (gossip membership, our own failure detector and transport, in [`../design/architecture-options.md`](../design/architecture-options.md)) becomes the starting point for phase 2, and has to be revised for partitions. Application logic is kept free of Ray from the first day so that it carries over ([ADR-0013](../decisions/0013-ray-free-core-for-phase-2.md)).

### 6.3 The requirements, extended

R1 to R9 stand in both phases. Two requirements are added:

| # | Requirement | Phase 1 | Phase 2 |
|---|---|---|---|
| R10 | Fault tolerance | Any two elements failing simultaneously, the head excluded | Not yet stated; assumed at least the same |
| R11 | Partition tolerance | Not required | Required |

### 6.4 The demonstration, revised for phase 1

The script in section 4 is replaced by the one in [`../design/ray-architecture.md`](../design/ray-architecture.md) section 13. Its core steps: kill two relay containers in one command while a client is sending and show that sending continues and nothing is lost; kill two relay processes; kill a client in the middle of a burst; and, as before, wipe every relay and show both clients' histories intact.

### 6.5 Section 5's open questions, updated

1. Language: Python, now also an examiner requirement for phase 1.
2. Spanish or English for the written deliverable: still not confirmed.
3. How many containers fit on one laptop: now a sharper question, because a Ray node is much heavier than a Python process and the target topology is about ten Ray nodes. First item of the spike.

Still to ask the examiners: whether phase 2 keeps the two-failure requirement; whether anything in phase 2 is guaranteed not to fail; whether each delivery is a live defense.
