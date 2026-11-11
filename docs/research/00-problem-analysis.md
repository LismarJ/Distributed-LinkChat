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
