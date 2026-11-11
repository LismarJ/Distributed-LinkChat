# ADR-0002 — Python backend, React frontend

**Status:** accepted · **Date:** 2026-11-10 · **Deciders:** project team

## Context

[ADR-0001](0001-two-tier-relay-overlay-architecture.md) commits us to two node types — relay nodes and client nodes — both shipped as Docker containers, orchestrated by shell scripts (Compose is forbidden), and subject to deliberate churn during the defense.

The comparative study in [`../design/architecture-options.md`](../design/architecture-options.md) §9.1 evaluated Go, TypeScript/Node, Rust, Elixir/OTP and Python. It recommended Go on container size and startup, and rated Python a "fair" fit with medium risk.

The team chose **Python for the backend with a React frontend**, on team familiarity and development velocity against a three-month deadline. That trade is reasonable: the graded content of this project is the distributed-systems design, and velocity on familiar ground protects the schedule for exactly that work. This ADR records the decision and, more importantly, the concrete mitigations for the risks the study flagged.

## Decision

- **Both node types in Python 3.12+**, `asyncio` throughout. No threads for I/O, no multiprocessing.
- **React + Vite + TypeScript** for the client UI, served by the client node and opened in the examiner's host browser.
- Full component-level stack in [`../design/technology-stack.md`](../design/technology-stack.md).

## The flagged risks, and what we actually do about them

**1. "Python containers are large (~150 MB) and twenty of them won't fit on a laptop."**

Partly a misreading, and worth getting right because it affects our test topology. Image size is paid **once per image**, not per container — twenty containers from the same image share the same read-only layers on disk. What is paid per container is resident memory, roughly **40–70 MB** for an `asyncio` Python process. Twenty containers is therefore on the order of 1–1.4 GB of RAM, which is fine on a development laptop.

What image size does cost is **build and pull time**, which we feel on every iteration. Mitigations: `python:3.12-slim` as the base, a multi-stage build, `uv` with a committed lockfile for fast deterministic installs, and `.dockerignore` discipline. Target under 120 MB per image.

**2. "The GIL will bottleneck the crypto."**

Largely not true for our workload. The expensive operations — X25519, Ed25519, XChaCha20-Poly1305, BLAKE2b — run inside PyNaCl and `cryptography`, which are C and Rust extensions that **release the GIL**. Our message rates are measured in tens per second, not tens of thousands. If profiling later shows a hot spot, the mitigation is to move that one operation into a thread executor, not to change language.

**3. "Startup time."**

A Python process starts in roughly 100–300 ms including imports. The demo involves killing and restarting containers, so this is visible but acceptable. Mitigation: keep module-level imports lean in the relay, and make the readiness signal explicit (a node announces itself on the gossip channel as soon as it is listening) so the demo script can wait on readiness rather than on a sleep.

**4. Determinism.**

`uv` with a committed `uv.lock`, pinned base image digests, and no network access at container runtime beyond the overlay itself.

## Consequences

**Positive**

- Highest team velocity, which is the scarcest resource against a three-month deadline.
- `asyncio` is a good fit for the actual workload: many idle connections, periodic gossip timers, occasional bursts.
- Excellent libraries for every component we need, all free and open source (see [`../design/technology-stack.md`](../design/technology-stack.md)).
- Readable code makes the defense easier — examiners can follow the gossip loop and the quorum write on screen.

**Negative**

- Larger images and slower iteration than Go or Rust. Accepted, mitigated above.
- We lose `vodozemac` (the Apache-2.0 Rust Double Ratchet) as a drop-in. Python alternatives exist — `python-x3dh` (MIT) and `DoubleRatchet` from the python-omemo family — and are the fallback if our own implementation slips. Licences to be confirmed before use. See ADR-0009 (pending).
- Type safety depends on discipline rather than the compiler. Mitigation: `mypy --strict` on the protocol and state-machine modules, where a type error becomes a correctness bug, and `ruff` in CI.

## Re-examination — 2026-11-11

The language decision was revisited from scratch on request, not because new information had appeared, but to pressure-test it seriously rather than let it stand by default. No code exists yet, so switching cost was zero and the only real question was fit.

**Candidates re-weighed:** Go, Rust, Elixir/OTP, TypeScript/Node, Python, and a split stack (Go for relay nodes, Python for client nodes).

- **Rust and Elixir, ruled out on team risk, not technical merit.** Both are individually excellent fits for this problem (Rust for `vodozemac`, Elixir for its supervision trees and natural match to the gossip/failure-detector domain), but the team rates "Low" familiarity with both, and the comparative study already rates their 3-month risk "Medium-high". Option C — the architecture already chosen — is itself the **highest-implementation-risk** of the five options evaluated in [`architecture-options.md`](../design/architecture-options.md) §7. Stacking a second source of schedule risk (an unfamiliar language) on top of the highest-risk architecture is not a trade this team should take.
- **TypeScript/Node, dominated by Python for this project.** Its one advantage over Python — team familiarity ("Highest" vs "High") — is marginal and doesn't change anything that's actually hard here: the Double Ratchet still has to be implemented from spec in either language (the one ready-made TS libsignal port is GPL-3.0, which the team would likely avoid for licensing reasons regardless), and Node's single-threaded event loop has the same synchronous-crypto-blocks-everything exposure that Python's `asyncio` does — minus the one escape hatch Python has (PyNaCl releases the GIL, so a thread pool can make progress; Node has no equivalent without `worker_threads`). No dimension favors Node enough to justify discarding the Python design work already done.
- **Go — the real alternative, seriously considered.** Go's fit for the *relay* node specifically is hard to beat: goroutines map directly onto the gossip/SWIM/anti-entropy concurrency pattern, a ~15 MB static binary starts instantly (useful when the demo script repeatedly kills and restarts relays), and the compiler enforces the type safety that the Python stack only gets from `mypy --strict` discipline. If this project were graded on relay-node engineering in isolation, Go would win outright.
  What keeps Python ahead anyway: language choice doesn't move the needle on what is actually graded. The DS depth — gossip membership, SWIM failure detection, consistent hashing, quorum writes, anti-entropy, causal ordering, TTL garbage collection — is produced by the *protocol design*, not by the implementation language; Go would make the system faster and the type-checking stronger, not the design more correct. Against that wash, Python keeps the single biggest, most concrete lever this team has against a three-month deadline on an already-high-risk architecture: "High" team familiarity versus Go's "Medium". And the risks the original study raised against Python — image size, the GIL, startup latency, determinism — are not hand-waved in this ADR; each has a specific, checkable mitigation (digest-pinned `python:3.12-slim` multi-stage builds targeted under 120 MB, GIL-releasing C/Rust crypto extensions against a workload measured in tens of messages per second, readiness-announce-over-`sleep` for startup, `uv.lock` for determinism). A serious re-examination has to weigh documented, checkable mitigations against a generically "cleaner" alternative, and the mitigations hold up.
- **Split stack — Go for relay nodes, Python for client nodes — considered and rejected.** This is the one alternative that isn't dominated: relays touch zero plaintext and zero cryptography (only opaque padded ciphertext), so none of Python's crypto-ecosystem advantage applies to them, and Go's concurrency model fits the relay's job better than `asyncio` does. But [ADR-0001](0001-two-tier-relay-overlay-architecture.md) already flags "two node types means two deployment stories and more Docker plumbing" as a cost of the chosen architecture; a second language doubles that again — a second toolchain, lint config, test runner and CI path, for a small team, plus an extra question to field at the defense ("why does half the system use a different language?"). That is real engineering sophistication, but of the kind that pays off in a production system, not in a three-month graded project whose hard problem is the protocol, not the runtime. Rejected on schedule and complexity grounds, not technical ones.

**Conclusion: ADR-0002 stands — Python backend, React frontend, unchanged.**

**One addition.** Rather than defend the Python choice by argument indefinitely, it is tied to evidence: the week-1 discovery spike already planned for ADR-0004 (UDP broadcast/multicast behavior on Docker Desktop/WSL2) is also the designated checkpoint for the two load-bearing Python claims in this ADR — the sub-120 MB image budget and `asyncio` gossip-timer behavior under real container churn. If either fails concretely at that checkpoint, that is the moment to reopen this ADR with **Go as the named fallback** — not before, on speculation, and not after, once more has been built on top of it.

## Follow-ups

- ADR-0009 — implement X3DH + Double Ratchet ourselves over PyNaCl, or adopt `python-x3dh` + `DoubleRatchet`. Decide after the week-1 spike; confirm licences either way.
- Confirm `PyNaCl` is Apache-2.0 and `cryptography` is Apache-2.0/BSD against their project pages before the report cites them.
