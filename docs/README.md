# Distributed-LinkChat — Documentation

Project: **P08 · Mensajería segura descentralizada** — Sistemas Distribuidos 2026, Universidad de La Habana, Facultad de Matemática y Computación. Brief: [`../Orinetacion.pdf`](../Orinetacion.pdf).

Every conclusion, insight and decision in this project is written down here rather than left in conversation.

## Current state (2026-11-27)

The examiners have split the project into three graded phases (15% / 50% / 35%); the weights and the full requirement-by-requirement audit are in [`planning/methodology-compliance.md`](planning/methodology-compliance.md):

| | Phase 1 — 15% | Phase 2 — 50% | Phase 3 — 35% |
|---|---|---|---|
| Date | **2026-12-01** (submission) | **2027-01-01** (architecture report) | Final examination (oral, individual) |
| Platform | Ray and Python; use what Ray offers, do not reimplement it | No Ray; everything ours. **"Normally Go"** — unresolved | Both, compared |
| Must survive | Any two elements failing at once (the head node is never killed) | Any two, with no exempt component, and network partitions as well | Demonstrated under a fault campaign |

Phase 1 is built, reviewed and verified. `scripts/check.sh` is green (507
passed, 20 skipped), and `scripts/scenario.sh all` passes all seven scenarios
against real Docker containers — the two-simultaneous-failure demonstration,
sending to an offline recipient, and a full relay wipe that both clients'
history survives untouched. All five of the methodology's required phase 1
artifacts exist and its exit gate is answerable from measurement rather than
assumption. Gates G0 through G3 are answered; what remains for 1 December is
G4 (the full demonstration script end to end) and G5 (three clean rehearsals
and the finished written deliverable). See
[`guides/2026-11-25-review-and-durability-fixes.md`](guides/2026-11-25-review-and-durability-fixes.md)
for the review that found and fixed three defects in the graded mechanisms, and
[`experiments/2026-11-26-phase-1-initial-experiments.md`](experiments/2026-11-26-phase-1-initial-experiments.md)
for the measured numbers.

**The three things that are not code.** Ranked in
[`planning/methodology-compliance.md`](planning/methodology-compliance.md) §9:
the submission is a **GitHub issue** that must exist for the project to count
as submitted; the methodology says phase 2 "should normally use **Go**", which
our phase 2 strategy had assumed would be Python, and that question belongs
with the instructors rather than with us; and **phase 3 is 35% of the mark and
was unplanned** until this week.

**Where to start reading:** [`planning/phase-1-work-plan.md`](planning/phase-1-work-plan.md) for what to do next, [`design/ray-architecture.md`](design/ray-architecture.md) for the phase 1 design, [`design/fault-tolerance.md`](design/fault-tolerance.md) for failures, edge cases and tradeoffs, [`planning/phase-plan.md`](planning/phase-plan.md) for how the three phases relate, and [`design/behavioral-contract.md`](design/behavioral-contract.md) for the frozen contract both implementations are measured against.

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
| [`planning/phase-plan.md`](planning/phase-plan.md) | The three phases: what each requires and delivers, what phase 2 must replace, what carries over, the phase 3 outline, the milestone schedule and the scope priorities |
| [`planning/methodology-compliance.md`](planning/methodology-compliance.md) | **Audit against the examiners' methodology.** The three phases and their weights, the five required phase 1 artifacts, the exit gate, submission mechanics, the F0–F21 scenario mapping, the common baseline item by item, and the ranked actions. Re-run before each submission |

## Design

| Doc | What it covers |
|---|---|
| [`design/behavioral-contract.md`](design/behavioral-contract.md) | **The frozen contract.** Every externally visible operation and outcome, the four delivery states, the seven guarantees and the six things deliberately not promised. Names no Ray concept, so it should survive the redesign unchanged |
| [`design/middleware-dependency-map.md`](design/middleware-dependency-map.md) | **What Ray does for us, and what we already do.** 13 requirements Ray provides and the question each poses for the native redesign, 14 that are already ours, and the five failure semantics we measured rather than assumed |
| [`design/ray-architecture.md`](design/ray-architecture.md) | **The phase 1 design.** Five named relay actors supervised by Ray, the fixed ring, send/receive/forget, repair, the client, security claims, design patterns, code structure, the demonstration |
| [`design/fault-tolerance.md`](design/fault-tolerance.md) | The fault model, why three copies and no majority, element-by-element and pair analysis, eighteen edge cases, what each decision gains and loses, how each claim is tested |
| [`design/technology-stack.md`](design/technology-stack.md) | Component-by-component stack for phase 1, with licences, and what changes in phase 2 |
| [`design/architecture-options.md`](design/architecture-options.md) | The study that compared five architectures and chose option C. Its mechanism details are the starting point for phase 2 |

## Experiments

| Doc | What it covers |
|---|---|
| [`experiments/2026-11-26-phase-1-initial-experiments.md`](experiments/2026-11-26-phase-1-initial-experiments.md) | The methodology's required initial experiment, as four records in its format: two relay containers killed together; failover timing (container ~31 s versus worker process ~1.3 s); every relay wiped at once; and the concurrency experiment that found `wait_for_mail` silently inert because each Ray concurrency group is its own event loop |

## Decisions

ADRs live in [`decisions/`](decisions/), numbered, in the form *context → options → decision → consequences*.

| ADR | Decision | Status |
|---|---|---|
| [0001](decisions/0001-two-tier-relay-overlay-architecture.md) | Two-tier relay architecture with replicated, expiring, opaquely addressed mailboxes (option C) | Accepted, amended for the two phases |
| [0002](decisions/0002-python-backend-react-frontend.md) | Python backend, React frontend | Accepted, amended |
| [0003](decisions/0003-english-for-all-documentation.md) | English for all documentation | Accepted |
| [0004](decisions/0004-discovery-through-ray.md) | Phase 1: discovery and liveness come from Ray; clients keep no membership view | Proposed |
| [0005](decisions/0005-three-copies-before-sent.md) | Phase 1: three copies on three relays before a message counts as sent; five relays | Proposed |
| [0007](decisions/0007-mailbox-identifiers.md) | One mailbox per direction of each conversation, derived from the two user ids | Accepted for phase 1 |
| [0009](decisions/0009-adopt-a-cryptography-library.md) | Adopt an existing library for X3DH and the Double Ratchet; do not write them ourselves | Accepted |
| [0010](decisions/0010-build-phase-1-on-ray.md) | Phase 1 is built on Ray, using what Ray offers | Accepted (examiners) |
| [0011](decisions/0011-relays-as-ray-supervised-actors.md) | Phase 1: relays are a fixed set of named actors supervised by Ray; clients are Ray drivers | Proposed |
| [0012](decisions/0012-head-node-policy.md) | Phase 1: one head node, not made fault tolerant | Accepted (examiners' scope) |
| [0013](decisions/0013-ray-free-core-for-phase-2.md) | Application logic lives in a core that does not import Ray | Accepted |

| [0014](decisions/0014-phase-1-scope-cuts.md) | Phase 1 scope: optional features deferred, and no direct client-to-client path | Accepted |
| [0015](decisions/0015-how-the-cryptography-is-used.md) | How X3DH and the Double Ratchet are used: no one-time prekeys, bundles bound to the UserId, a fixed 4090-byte envelope, ratchet state stored with each message | Accepted for phase 1 |
| [0016](decisions/0016-plain-browser-ui-without-a-build-step.md) | A plain HTML/JS browser UI with no build step | Superseded by 0017 |
| [0017](decisions/0017-react-ui-with-a-committed-build.md) | The browser UI is React + TypeScript, built in development and committed; the API lives under `/api` | Accepted |
| [0018](decisions/0018-signed-acknowledgements.md) | Only a mailbox's recipient may acknowledge it: acks carry an Ed25519 signature; reads stay open | Accepted |

Pending: 0006 TTL and expiry behaviour · 0008 relay interface and envelope format.

## Specs

The agreed design document and the interface specification go in [`superpowers/specs/`](superpowers/specs/).

---

## The finding in one paragraph

WhatsApp and Signal have the **retention policy** the brief demands (the server is a transient queue that deletes on delivery) but the **centralized topology** it forbids. Matrix has the **decentralized topology** but keeps **permanent full history** on every participating server. No named reference satisfies requirement R9. The combination exists only in a fourth family the brief does not name (Session, SimpleX, Briar), and our architecture is a synthesis of it: Matrix's causal event model, WhatsApp's and Signal's amnesiac queue semantics, Signal's cryptography, Session's replicated swarms, and SimpleX's opaque addressing.

## The phase 1 design in one paragraph

Ray keeps the relays alive; we keep the data alive. Five named relay actors hold padded ciphertext in memory. Ray restarts a relay whose process or container dies, on a spare node if needed, and it comes back empty. Every message is stored on three relays before its sender sees `sent`, so any two can fail; an unreachable relay is covered by the next one on a fixed ring, and a restarted relay refills from the others before serving. Clients hold the keys, the order, the duplicates filter and the only permanent copy of the history.
