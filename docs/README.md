# Distributed-LinkChat — Documentation

Project: **P08 · Mensajería segura descentralizada** — Sistemas Distribuidos 2026, Universidad de La Habana, Facultad de Matemática y Computación. Brief: [`../Orinetacion.pdf`](../Orinetacion.pdf).

Every conclusion, insight and decision in this project is written down here rather than left in conversation.

## Current state (2026-11-11)

The examiners have split the project into two phases:

| | Phase 1 | Phase 2 |
|---|---|---|
| Delivery | **2026-12-01** | **2027-01-01** |
| Platform | Ray and Python; use what Ray offers, do not reimplement it | No Ray; everything ours |
| Must survive | Any two elements failing at once (the head node is never killed) | Network partitions as well |

No code exists yet. The phase 1 design is written and rests on Ray behaviour we have read about and not yet run; the first task is the spike listed in [`research/07-ray.md`](research/07-ray.md) section 12.

**Where to start reading:** [`planning/phase-1-work-plan.md`](planning/phase-1-work-plan.md) for what to do next, [`design/ray-architecture.md`](design/ray-architecture.md) for the phase 1 design, [`design/fault-tolerance.md`](design/fault-tolerance.md) for failures, edge cases and tradeoffs, [`planning/phase-plan.md`](planning/phase-plan.md) for how the two phases relate.

## Reports

| Doc | What it covers |
|---|---|
| [`reports/Report1-State-of-the-Art.md`](reports/Report1-State-of-the-Art.md) | The research set merged into one argument: the brief and why it is hard, the three reference platforms studied and compared, the systems the brief does not name, the mechanism toolbox, what we take from each, and (section 9) Ray and the examiners' rules |
| [`reports/Report2-Architecture-and-Design.md`](reports/Report2-Architecture-and-Design.md) | Five candidate architectures compared and option C chosen (sections 1 to 8); the two phases, the phase 1 design on Ray, the two-failure analysis, edge cases and tradeoffs, phase 2 in outline, the stack (sections 9 to 15) |

The notes below are the sources the reports are built from; they keep the full detail and citations.

## Research

| Doc | What it covers |
|---|---|
| [`research/00-problem-analysis.md`](research/00-problem-analysis.md) | The brief decomposed into nine requirements (R1–R9), the Docker/no-Compose constraint, and in section 6 the examiners' clarifications, which add R10 (two failures) and R11 (partitions, phase 2) |
| [`research/01-whatsapp.md`](research/01-whatsapp.md) | The UX contract (ticks as visible distributed state) and the "server is a queue, not an archive" principle |
| [`research/02-matrix.md`](research/02-matrix.md) | Federation mechanics from the spec: server discovery, signed transactions, the event DAG, backfill, state resolution, and why we avoid needing it |
| [`research/03-signal.md`](research/03-signal.md) | Identity, prekey bundles as the cryptographic answer to offline delivery, the Double Ratchet, sealed sender |
| [`research/04-decentralized-alternatives.md`](research/04-decentralized-alternatives.md) | Session, SimpleX, Briar, Jami, Tox: the systems that solve our problem, and where our architecture comes from |
| [`research/05-reference-platforms-comparison.md`](research/05-reference-platforms-comparison.md) | The three reference platforms compared dimension by dimension |
| [`research/06-distributed-primitives.md`](research/06-distributed-primitives.md) | The toolbox: placement, membership, clocks, replication, dedup, retry. A table at the top says which parts apply to which phase; section 9 covers how many copies and when a majority is needed |
| [`research/07-ray.md`](research/07-ray.md) | Ray: primitives, cluster layout, actors in depth, fault tolerance, deployment in Docker, security, **every Ray feature and whether it applies to us**, Ray's patterns and anti-patterns, the spike list |

## Planning

| Doc | What it covers |
|---|---|
| [`planning/phase-1-work-plan.md`](planning/phase-1-work-plan.md) | **What to do next.** The execution plan for phase 1: definition of done, the split between the two developers, six gates starting with the Ray spike, every task with owner, dependency and done-when check, the person-day budget, how to use Claude Code, the cut list and the risks |
| [`planning/phase-plan.md`](planning/phase-plan.md) | The two phases: what each requires, what phase 2 must replace, what carries over, the milestone schedule and the scope priorities |

## Design

| Doc | What it covers |
|---|---|
| [`design/ray-architecture.md`](design/ray-architecture.md) | **The phase 1 design.** Five named relay actors supervised by Ray, the fixed ring, send/receive/forget, repair, the client, security claims, design patterns, code structure, the demonstration |
| [`design/fault-tolerance.md`](design/fault-tolerance.md) | The fault model, why three copies and no majority, element-by-element and pair analysis, eighteen edge cases, what each decision gains and loses, how each claim is tested |
| [`design/technology-stack.md`](design/technology-stack.md) | Component-by-component stack for phase 1, with licences, and what changes in phase 2 |
| [`design/architecture-options.md`](design/architecture-options.md) | The study that compared five architectures and chose option C. Its mechanism details are the starting point for phase 2 |

## Decisions

ADRs live in [`decisions/`](decisions/), numbered, in the form *context → options → decision → consequences*.

| ADR | Decision | Status |
|---|---|---|
| [0001](decisions/0001-two-tier-relay-overlay-architecture.md) | Two-tier relay architecture with replicated, expiring, opaquely addressed mailboxes (option C) | Accepted, amended for the two phases |
| [0002](decisions/0002-python-backend-react-frontend.md) | Python backend, React frontend | Accepted, amended |
| [0003](decisions/0003-english-for-all-documentation.md) | English for all documentation | Accepted |
| [0004](decisions/0004-discovery-through-ray.md) | Phase 1: discovery and liveness come from Ray; clients keep no membership view | Proposed |
| [0005](decisions/0005-three-copies-before-sent.md) | Phase 1: three copies on three relays before a message counts as sent; five relays | Proposed |
| [0010](decisions/0010-build-phase-1-on-ray.md) | Phase 1 is built on Ray, using what Ray offers | Accepted (examiners) |
| [0011](decisions/0011-relays-as-ray-supervised-actors.md) | Phase 1: relays are a fixed set of named actors supervised by Ray; clients are Ray drivers | Proposed |
| [0012](decisions/0012-head-node-policy.md) | Phase 1: one head node, not made fault tolerant | Accepted (examiners' scope) |
| [0013](decisions/0013-ray-free-core-for-phase-2.md) | Application logic lives in a core that does not import Ray | Accepted |

Pending: 0006 TTL and expiry behaviour · 0007 mailbox identifiers and access credential · 0008 relay interface and envelope format · 0009 implement the ratchet or adopt a library (urgent, see the phase plan).

## Specs

The agreed design document and the interface specification go in [`superpowers/specs/`](superpowers/specs/).

---

## The finding in one paragraph

WhatsApp and Signal have the **retention policy** the brief demands (the server is a transient queue that deletes on delivery) but the **centralized topology** it forbids. Matrix has the **decentralized topology** but keeps **permanent full history** on every participating server. No named reference satisfies requirement R9. The combination exists only in a fourth family the brief does not name (Session, SimpleX, Briar), and our architecture is a synthesis of it: Matrix's causal event model, WhatsApp's and Signal's amnesiac queue semantics, Signal's cryptography, Session's replicated swarms, and SimpleX's opaque addressing.

## The phase 1 design in one paragraph

Ray keeps the relays alive; we keep the data alive. Five named relay actors hold padded ciphertext in memory. Ray restarts a relay whose process or container dies, on a spare node if needed, and it comes back empty. Every message is stored on three relays before its sender sees `sent`, so any two can fail; an unreachable relay is covered by the next one on a fixed ring, and a restarted relay refills from the others before serving. Clients hold the keys, the order, the duplicates filter and the only permanent copy of the history.
