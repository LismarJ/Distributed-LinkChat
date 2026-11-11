# Distributed-LinkChat — Documentation

Project: **P08 · Mensajería segura descentralizada** — Sistemas Distribuidos 2026, Universidad de La Habana, Facultad de Matemática y Computación. Brief: [`../Orinetacion.pdf`](../Orinetacion.pdf).

Every conclusion, insight and decision in this project is written down here rather than left in conversation.

## Reports

| Doc | What it covers |
|---|---|
| [`reports/Report1-State-of-the-Art.md`](reports/Report1-State-of-the-Art.md) | **Start here.** The whole research set merged into one argument: the brief and why it's hard, the three reference platforms studied and compared, the systems the brief doesn't name, the mechanism toolbox, and what we take from each |
| [`reports/Report2-Architecture-and-Design.md`](reports/Report2-Architecture-and-Design.md) | The five candidate architectures compared, the chosen design (Option C) explained mechanism by mechanism, and the resulting Python/React technology stack |

The research notes below are the sources this report is built from; they keep the full detail and citations.

## Research

| Doc | What it covers |
|---|---|
| [`research/00-problem-analysis.md`](research/00-problem-analysis.md) | The brief decomposed into nine requirements (R1–R9), the Docker/no-Compose constraint and what it forces, the defense demo script we design backwards from |
| [`research/01-whatsapp.md`](research/01-whatsapp.md) | The UX contract (ticks as visible distributed state) and the "server is a queue, not an archive" principle |
| [`research/02-matrix.md`](research/02-matrix.md) | Federation mechanics from the spec: server discovery, signed transactions, the event DAG, backfill, state resolution — and why we avoid needing it |
| [`research/03-signal.md`](research/03-signal.md) | Identity, prekey bundles as the cryptographic answer to offline delivery, the Double Ratchet, sealed sender |
| [`research/04-decentralized-alternatives.md`](research/04-decentralized-alternatives.md) | Session, SimpleX, Briar, Jami, Tox — the systems that actually solve our problem, and where our architecture comes from |
| [`research/05-reference-platforms-comparison.md`](research/05-reference-platforms-comparison.md) | **The three reference platforms compared dimension by dimension**: architecture, identity, retention, ordering, crypto, stacks, scorecard |
| [`research/06-distributed-primitives.md`](research/06-distributed-primitives.md) | The toolbox: DHT choice, gossip/SWIM, logical clocks, replication and anti-entropy, dedup, retry — and what we deliberately won't build |

## Design

| Doc | What it covers |
|---|---|
| [`design/architecture-options.md`](design/architecture-options.md) | **Five candidate architectures with advantages, disadvantages and tradeoffs**, a comparison matrix, the recommendation, and the technology options |
| [`design/technology-stack.md`](design/technology-stack.md) | Component-by-component stack for the chosen Python/React implementation, with licences |

## Decisions

ADRs live in [`decisions/`](decisions/), numbered, in the form *context → options → decision → consequences*.

| ADR | Decision |
|---|---|
| [0001](decisions/0001-two-tier-relay-overlay-architecture.md) | **Two-tier relay overlay** with swarm-replicated, TTL-bounded, opaquely-addressed mailboxes (Option C) |
| [0002](decisions/0002-python-backend-react-frontend.md) | **Python backend, React frontend** — with mitigations for the container-size and GIL concerns |
| [0003](decisions/0003-english-for-all-documentation.md) | **English** for all documentation and the defense |

Pending: 0004 discovery mechanism · 0005 replication factor R and write quorum W · 0006 TTL and retry/backoff/drop policy · 0007 mailbox identifier derivation · 0008 wire protocol · 0009 implement the ratchet vs. adopt a library.

## Specs

The agreed design document and the wire-protocol specification go in [`superpowers/specs/`](superpowers/specs/).

---

## The finding in one paragraph

WhatsApp and Signal have the **retention policy** the brief demands — the server is a transient queue that deletes on delivery — but the **centralized topology** it forbids. Matrix has the **decentralized topology** the brief demands but keeps **permanent full history** on every participating server. No named reference satisfies requirement R9, which is the point of the exercise. The combination exists only in a fourth family the brief does not name — Session, SimpleX, Briar — and our architecture is a synthesis of it: Matrix's decentralization and causal event model, WhatsApp's and Signal's amnesiac queue semantics, Signal's cryptography, Session's replicated swarms, and SimpleX's opaque addressing.
