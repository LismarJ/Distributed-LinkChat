# Commit series walkthrough — 2026-11-27: Methodology compliance artifacts

**Commits:** `36ce7ad` → `be08a0c`  
**Branch:** main  
**Author:** Jocdan Lismar López Mantecón  

---

## Why this series exists

On 2026-11-26 the examiners' written *Final Project Methodology* was read in full for the first time. Until that day no document in this project referenced it — the plan had been built from the original brief and verbal clarifications from 2026-11-11. Reading the methodology revealed three surprises:

1. **The project has three graded phases, not two.** Weights are 15 / 50 / 35. Phase 1, due in five days at the time, is the smallest. Phase 3 (35% of the final mark) had not been planned at all.
2. **Phase 2 "should normally use Go."** Our entire phase 2 strategy assumed Python. This is an open risk that needs an answer from the instructors.
3. **Phase 1 requires five specific artifacts**, four of which we had either not written or had written under different names.

This series produces all five required artifacts, adds the AI-use disclosure the methodology demands, and updates every document that referred to "two phases" or the old test counts.

---

## Commit 1 of 7 — `36ce7ad`
### `docs(design): add behavioural contract — methodology artifact 3`
**File added:** [`docs/design/behavioral-contract.md`](../design/behavioral-contract.md)

**What it is.** A contract document that defines the system entirely from the outside. It lists every operation a client can perform ("send a message", "read a conversation", etc.), the four delivery states a sent message can be in, the seven things the system guarantees, and the six things it deliberately does not promise.

**Why it matters.** The methodology says this document must "survive the redesign" — when Ray is removed in phase 2 and replaced with native Go or Python code, the external behaviour should not change. By writing the contract in a way that names no Ray concept (no actors, no ObjectRefs, no cluster) we make it possible to hold both implementations to the same standard. The contract is frozen at the phase 1 exit gate; a change to it after that date requires an Architecture Decision Record explaining why.

**For a junior reader:** Imagine you are testing the system but you are not allowed to look at the source code. This document is what tells you whether a given observation is a bug or expected behaviour.

---

## Commit 2 of 7 — `5425689`
### `docs(design): add middleware dependency map — methodology artifact 4`
**File added:** [`docs/design/middleware-dependency-map.md`](../design/middleware-dependency-map.md)

**What it is.** A table-driven audit of everything Ray does for the system, with a matching table of everything we already do ourselves.

**Why it matters.** The methodology requires answering two questions: "Which behaviour comes from my code?" and "Which behaviour comes from the middleware?" Without a written answer, phase 2 will discover dependencies one by one as things break, instead of planning for them in advance. The map has three sections:

- **13 requirements Ray provides today** — each row names the mechanism, asks the question the native redesign must answer, and gives our provisional answer (seed addresses + gossip, SWIM failure detector, our own framing, etc.).
- **14 requirements already ours** — the core library, the outbox, the cryptography, the relay surface, the browser UI.
- **5 failure semantics measured rather than assumed** — timing numbers from the experiments (commit 3), not estimates.

**For a junior reader:** Think of this as a "what do I owe the library?" checklist. Before you can delete a dependency you need to know exactly what it was doing for you. This document is that checklist.

---

## Commit 3 of 7 — `905795a`
### `docs(experiments): record phase 1 initial experiments — methodology artifact 5`
**File added:** [`docs/experiments/2026-11-26-phase-1-initial-experiments.md`](../experiments/2026-11-26-phase-1-initial-experiments.md)

**What it is.** Four controlled experiments written in the methodology's exact format: scenario, workload, claim under test, prediction, configuration, fault trigger, collected evidence, result, conclusion, remaining uncertainty.

**The four experiments:**

- **E1 — Two relay containers killed at once.** This is the core level-two fault tolerance claim: any two components failing simultaneously must not lose a confirmed message. The experiment kills two relay node containers with a single `docker kill` command (not one after the other, which would let the system repair in between) and checks that the message arrives exactly once and the history is unchanged.
- **E2 — Failover timing.** How long does the system take to notice a dead component? A dead *container* (node-level failure) takes ~31 seconds for Ray's Global Control Store to notice. A dead *process inside a living container* (worker-level failure) takes ~1.3 seconds. These numbers feed directly into the middleware dependency map (commit 2) and into the phase 2 failure detector design.
- **E3 — Entire relay tier wiped.** All five relay actors are killed simultaneously. No relay survives. The experiment confirms that both clients can recover their full history from their local SQLite databases, since the relays are designed to be stateless archives rather than the source of truth.
- **E4 — `wait_for_mail` across Ray concurrency groups.** This one found a defect. The long-poll endpoint that tells a waiting browser "you have new mail" was silently doing nothing. The root cause: each Ray concurrency group runs in its own OS thread with its own asyncio event loop. Setting an event in one thread does not wake a waiter in another thread. The fix was to use a thread-safe primitive. This experiment exists because measuring the mechanism (checking thread IDs and event loop IDs directly) found something that testing the outcome alone would have missed.

**Why the format matters.** The methodology maps experiments to a "Fault and Workload Scenario Library" (F0–F21). Writing experiments in this format means phase 3 can compare the Ray and native systems against the same vocabulary, using the same scenario IDs.

---

## Commit 4 of 7 — `9a3618b`
### `docs(planning): audit compliance with the examiners' methodology`
**File added:** [`docs/planning/methodology-compliance.md`](../planning/methodology-compliance.md)

**What it is.** A full item-by-item audit of the repository against the methodology, written to be re-run before each of the three submissions.

**What it covers:**

- The three surprises (§1): three phases not two, phase 1 is 15%, Go requirement in phase 2.
- The five required phase 1 artifacts and their locations (§2). All five now exist.
- The exit gate questions and how each is answered from measurement rather than assumption (§3).
- Submission mechanics (§4): the submission is a **GitHub issue**, not a file upload. The issue must exist for the project to count as submitted.
- The F0–F21 scenario mapping (§5): how the course's standard fault and workload scenarios map to `scripts/scenario.sh`.
- The common baseline item by item (§6–§8): security, ordering, local history, all checked.
- Ranked actions for the five days remaining before 2026-12-01 (§9).

**For a junior reader:** This is the "did we actually do what the examiners asked?" checklist. It is the document you open when you are preparing a submission and want to be sure you have not missed a required artifact or misread a requirement.

---

## Commit 5 of 7 — `02aac26`
### `chore: add AI-use disclosure (methodology requirement)`
**File added:** [`ai_use.yml`](../../ai_use.yml)

**What it is.** A YAML file at the repository root declaring how AI was used in the project.

**Why it is required.** The methodology has a section titled "AI-assisted engineering and disclosure". Its governing principle: "You may delegate work to AI, but you may not delegate responsibility for correctness." Any assessed repository must carry a machine-readable disclosure.

**What it records:**

- Tools (Claude Code) and models (Opus 4.8, Opus 5, Sonnet 5, Fable 5.2).
- Activities covered: architecture discussion, documentation, implementation, test generation, debugging, code review.
- Two areas **deliberately not delegated**: the replication and repair mechanisms (tasks R5–R7) and the cryptographic integration (task C6). The reasoning is recorded in `docs/planning/phase-1-work-plan.md` §7: at the oral defence each author must explain independently why two simultaneous failures cannot lose a confirmed message.
- Verification methods: public test suite (507 passing), fault/workload campaign, fuzz testing, manual review, cross-review between the two authors.
- Three notable corrections where AI output was rejected or rewritten — kept because the methodology's defence may ask "explain an AI-generated suggestion you accepted or rejected".
- Verifiable commit counts (41 of 111 commits carry a `Co-Authored-By: Claude` trailer — a lower bound on AI involvement, not an exact measure).

---

## Commit 6 of 7 — `bc0b96a`
### `docs(planning): revise phase plan to three phases`
**File modified:** [`docs/planning/phase-plan.md`](../planning/phase-plan.md)

**What changed.** The document previously described two deliveries. It now describes three graded phases with their weights, what each delivers, and what changes between them.

**New content:**

- Three-phase table (15 / 50 / 35) replacing the old two-phase one.
- Phase 3 outline: a fault/workload campaign, a guarantee analysis, a side-by-side comparison of the Ray and native systems, and an individual oral defence where each author must explain the system without help.
- The Go risk in phase 2, flagged explicitly and marked as a question for the instructors rather than a decision we may take quietly.
- Milestone schedule extended to the final examination.

**For a junior reader:** The phase plan is the "big picture" document. Before this commit it was missing an entire third of the project. After this commit anyone who reads it gets the same picture the examiners have.

---

## Commit 7 of 7 — `be08a0c`
### `docs: update READMEs and guides for methodology-driven changes`
**Files modified:** `README.md`, `docs/README.md`, `docs/guides/2026-11-24-demonstration-runbook.md`, `docs/guides/2026-11-25-commit-series-walkthrough.md`

**What changed and why:**

- **README.md:** Test count corrected from 481 to 507 (and from 501 to 527 on Linux) — the signed-acknowledgements series added 26 tests. Adds `methodology-compliance.md` to the key documents table. Reflects ADR-0017: `client_app/static/` is now a committed React build, not hand-written JavaScript. Adds `ai_use.yml` to the directory tree description.

- **docs/README.md:** Current-state date updated. Three-phase table with weights. The "three things that are not code" summary: the submission is a GitHub issue (not a file); the Go question for the instructors; and phase 3 existing and being 35% of the mark. New rows in the Planning, Design, and Experiments tables for the new documents.

- **docs/guides/2026-11-24-demonstration-runbook.md:** The pre-flight checklist numbers corrected. If a grader runs the check script and counts 507 tests when the runbook says 481, they may think something is broken. Now it says 507 (Windows) and 527 (Linux).

- **docs/guides/2026-11-25-commit-series-walkthrough.md:** One note added: `app.js` no longer exists in the repository. ADR-0017, adopted the day after that guide was written, replaced the hand-written UI with a React + Vite build. The same fix described in that commit lives in the React component now. The original commit is still the record of the defect and the reasoning.

---

## Summary

| # | Commit | Purpose | Files |
|---|--------|---------|-------|
| 1 | `36ce7ad` | Methodology artifact 3: frozen external contract | `docs/design/behavioral-contract.md` |
| 2 | `5425689` | Methodology artifact 4: middleware dependency map | `docs/design/middleware-dependency-map.md` |
| 3 | `905795a` | Methodology artifact 5: initial experiments | `docs/experiments/2026-11-26-phase-1-initial-experiments.md` |
| 4 | `9a3618b` | Methodology compliance audit | `docs/planning/methodology-compliance.md` |
| 5 | `02aac26` | AI-use disclosure | `ai_use.yml` |
| 6 | `bc0b96a` | Phase plan revised to three phases | `docs/planning/phase-plan.md` |
| 7 | `be08a0c` | READMEs and guides corrected | `README.md`, `docs/README.md`, two guides |

After this series: all five required phase 1 artifacts exist, the AI-use disclosure is in place, every document that said "two phases" now says three, and the test counts are accurate. The two things this series does not resolve — the Go language question and opening the submission GitHub issue — are recorded as the ranked actions in `docs/planning/methodology-compliance.md` §9.
