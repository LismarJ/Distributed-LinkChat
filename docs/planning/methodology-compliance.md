# Compliance with the examiners' methodology

**Status:** first audit · **Date:** 2026-11-26 · **Five days to the phase 1 submission**
**Source:** *Final Project Methodology — Distributed Systems 2026*, MatCom
**Re-run:** before each of the three submissions

The methodology was read in full on 2026-11-26 and audited against the
repository. Until that day **no document in this project referenced it**: the
plan was built from the brief and the examiners' verbal clarifications of
2026-11-11. Most of what we built is unaffected by that, and three things are
not. Those three are in section 1.

---

## 1. What the methodology changes

### 1.1 The project is three phases, not two, and phase 1 is 15%

Everything we have written assumes two deliveries. The methodology defines
three graded phases:

| Phase | What | Weight | Date |
|---|---|---|---|
| 1 | Ray prototype + architecture/middleware analysis + initial experiment | **15%** | 2026-12-01 (submission) |
| 2 | Native system, no Ray, "normally Go" | **50%** | 2027-01-01 (architecture report) |
| 3 | Fault campaign, guarantee analysis, Ray/native comparison, **individual defence** | **35%** | Final examination |

Two consequences worth internalising:

- **85% of the mark comes after 2026-12-01.** We have been treating 1 December
  as the project's centre of gravity. It is the smallest of the three.
- **Phase 3 does not exist in our planning at all.** `phase-plan.md` ends at
  phase 2. A tenth of the whole mark is a fault/workload campaign and another
  tenth is each author defending the system alone.

### 1.2 Phase 2 "should normally use Go"

> "The core implementation should normally use Go for native services. Another
> language may be justified when the project domain has a strong reason, but
> changing language cannot be used to hide the required distributed mechanisms
> behind another framework."

Our entire phase 2 strategy assumes Python.
[ADR-0013](../decisions/0013-ray-free-core-for-phase-2.md) exists precisely so
that `linkchat.core` — about 2 000 statements of crypto, replication, repair,
ordering and history — carries over untouched. If phase 2 must be Go, that
advantage is gone and roughly half the system is rewritten in a language the
project has not used.

**This is the single largest open risk in the project**, and it is a question
for the instructors, not a decision we may take quietly. The methodology is
explicit about how to handle doubt: *ask before using it, and if you receive no
reply, the answer is no.*

The case for Python, if we make it, is a genuine domain argument and not a
convenience one:

1. **The cryptography.** [ADR-0009](../decisions/0009-adopt-a-cryptography-library.md)
   adopted maintained X3DH and Double Ratchet libraries rather than writing a
   ratchet from the specification in three weeks. Those libraries are Python.
   Rewriting in Go means either finding equivalent audited Go libraries or
   writing the ratchet ourselves — the exact risk ADR-0009 retired.
2. **It hides nothing.** The rule's purpose is that a language change must not
   conceal the distributed mechanisms behind a framework. Ours would be plain
   `asyncio` TCP with our own framing, our own membership, our own failure
   detector — no framework at all. Every mechanism stays ours and inspectable.
3. **The Ray-free boundary is already enforced in CI**, so the "native
   mechanisms" layer is genuinely separable and genuinely ours.

**Action:** ask the instructors in writing, this week, and record the answer as
an ADR. Until there is a reply, plan for Go as the default and treat the Python
continuation as the thing we are requesting.

### 1.3 The Ray implementation must survive

Phase 3 requires a **Ray/native comparison** across dimensions including
startup behaviour, latency percentiles, recovery time, implementation
complexity and "guarantees present in one implementation but absent in the
other". That is 5% of the mark, and it is impossible if phase 1 is deleted or
left unrunnable.

It also means **the measurements must be taken while the Ray system is alive**.
Recovery time we have (~31 s node, ~1.3 s process). Startup time, latency
percentiles, throughput and mechanism counts we do not. Those should be
captured before attention moves to phase 2, because reconstructing a working
Ray cluster in January to measure it will be far more expensive than measuring
it this week.

---

## 2. Phase 1 required artifacts

| # | Artifact | Status | Where |
|---|---|---|---|
| 1 | Working Ray prototype | **Done.** Genuine distributed execution: 10 containers, five relay actors across seven nodes, two clients | `src/`, `scripts/up.sh` |
| 2 | Architecture diagram | **Done**, as an ASCII cluster diagram showing our components and the Ray components they depend on | [`ray-architecture.md`](../design/ray-architecture.md) §2 |
| 3 | Behavioural/API contract | **Done 2026-11-26.** Did not exist before; now written and frozen | [`behavioral-contract.md`](../design/behavioral-contract.md) |
| 4 | Middleware dependency map | **Done 2026-11-26.** Existed only as two partial tables in two documents, in neither's required shape | [`middleware-dependency-map.md`](../design/middleware-dependency-map.md) |
| 5 | Initial experiment | **Done 2026-11-26.** Four experiments in the required record format; the data existed as narrative, not as records | [`../experiments/2026-11-26-phase-1-initial-experiments.md`](../experiments/2026-11-26-phase-1-initial-experiments.md) |

## 3. Phase 1 exit gate

The methodology freezes the contract once these are answerable.

| Question | Answer |
|---|---|
| What problem does my application solve? | Messages reach people whose device is off, and no component becomes a permanent archive. [`behavioral-contract.md`](../design/behavioral-contract.md) §1 |
| What externally visible behaviour must survive the redesign? | [`behavioral-contract.md`](../design/behavioral-contract.md) §§2–5, frozen 2026-11-26 |
| Which behaviour comes from my code? | 14 requirements. [`middleware-dependency-map.md`](../design/middleware-dependency-map.md) §2 |
| Which behaviour comes from Ray? | 13 requirements. [`middleware-dependency-map.md`](../design/middleware-dependency-map.md) §1 |
| Which failure semantics did I observe rather than assume? | Five, two of which contradicted what we assumed. [`middleware-dependency-map.md`](../design/middleware-dependency-map.md) §4 |

**The gate is passable.** All five have written answers grounded in measurement.

## 4. Submission mechanics

| Requirement | Status |
|---|---|
| GitHub issue with topic, every student's full name **and class group**, and a link to the solution | **Unverified — check this first.** The project counts as submitted only once the issue exists. Nothing in the repository records it |
| Every member contributes and can defend the whole solution individually | On track: 111 commits across two authors, with a cross-review practice recorded in the work plan |
| Independent naming and branding; third-party marks preserved as references | **Compliant.** "LinkChat" is ours; Ray, WhatsApp, Signal, Matrix, Session, SimpleX, Briar, Jami and Tox appear only as descriptive citations in the research notes and reports |
| Local, reproducible execution; public deterministic tests | **Compliant.** `scripts/check.sh`, `scripts/up.sh`, `scripts/scenario.sh all`, CI running the same checks |

## 5. Level-two fault tolerance

> "For both oral defenses, the system must maintain client-side functionality
> and recover when any two components fail simultaneously. This includes two
> logical processes, two entire nodes, or a process and a separate node."

All three shapes are demonstrated, each in a single kill:

| Shape | Scenario | Result |
|---|---|---|
| Two logical processes | `scenario.sh two-processes` | PASS |
| Two entire nodes | `scenario.sh two-nodes` | PASS |
| One process and one node | `scenario.sh one-of-each` | PASS |

Plus four beyond the minimum: `send-while-down`, `client-mid-send`,
`recipient-offline`, `wipe-all-relays`. Seven of seven, verified on two
independent clean runs against real containers.

**The caveat to state out loud at the defence.** Phase 1 has one component the
examiners exempted: the head node
([ADR-0012](../decisions/0012-head-node-policy.md)). It is a genuine single
point of failure, and the methodology's defence explicitly asks candidates to
"identify a single point of failure". The honest answer is: the head is it, it
is Ray's GCS, the exemption is theirs and not ours, and phase 2 has no head at
all. Say that before being asked.

## 6. Fault and Workload Scenario Library mapping

The course keeps canonical scenario IDs "so experiments are comparable". Ours
are named after what they do; this is the mapping. Phase 3's campaign should
cite the F-IDs.

| ID | Library scenario | Our coverage | Where |
|---|---|---|---|
| F0 | Baseline | Beat 1 of the runbook; `test_three_copy_send.py` | containers + unit |
| F1 | Single process crash | `kill-relay-process.sh`; `two-processes` | containers |
| F2 | Crash and restart | Relay restart then resync before serving (R6) | containers + unit |
| F3 | Slow node | Fake-transport latency only | **unit only** |
| F4 | Packet loss | Fake-transport drop only | **unit only** |
| F5 | Minority partition | Excluded from phase 1; **required in phase 2** | — |
| F6 | Symmetric partition | Excluded from phase 1; **required in phase 2** | — |
| F7 | Heal and reconcile | Resync + hinted handoff after every scenario heals | containers |
| F8 | Lost response after successful effect | Outbox retry over an idempotent store; the message is not duplicated | unit |
| F9 | Duplicate request delivery | Content-hash ids, idempotent store, persistent seen-set; the fake transport duplicates every write | unit |
| F10 | Stale discovery | `ray.get_actor` raising mid-restart, mapped to `RelayUnreachable`, cached handle dropped | containers + unit |
| F11 | Stale replica read | A reader asks three relays and merges, so a relay that missed a write is covered | unit |
| F12 | Concurrent conflicting writes | Two-element vector clock with a deterministic tie-break; both clients show the same order | unit |
| F13/F14 | 2PC coordinator/participant | **Not applicable.** No transactions and no two-phase commit; a write is a quorum of idempotent stores |
| F15 | Leader failure | **Not applicable.** No leader and no election; placement is a hash ring every node computes identically |
| F16 | Overload | Per-mailbox and total quotas, `MailboxFull` | **unit only** |
| F17 | Retry amplification | Outbox backoff with jitter | **unit only** |
| F18 | Churn | Fixed relay set in phase 1; **a phase 2 concern** | — |
| F19 | Disconnected CRDT updates | **Not applicable** as stated, though the phase 2 merge is a set union of immutable records, which is the same argument |
| F20 | Unauthorized operation | `ack` refused unless signed by the mailbox's reader ([ADR-0018](../decisions/0018-signed-acknowledgements.md)) | **unit only** |
| F21 | Replay of authenticated mutation | A captured credential re-acknowledges the same records; `ack` is idempotent, so replay is harmless by construction | unit |

Against the methodology's **common minimum campaign** of eight classes: we are
complete on baseline, single crash, crash/restart, duplicate/lost-response
semantics and concurrency; thin on delay/loss and overload (unit only); and
absent on partitions, which phase 1 excludes by agreement and phase 2 requires.

## 7. Common distributed-systems baseline

Audited item by item. Phase 1 is a Ray prototype, so some rows are properly
phase 2's, and are marked.

| Requirement | Status |
|---|---|
| Multiple independent processes/nodes | Compliant: 10 containers |
| No implicit shared memory | Compliant: separate containers, no shared volumes between logical nodes |
| Explicit network communication | Compliant, via Ray in phase 1 |
| Concurrent clients/operations | Partial: concurrency groups and the parked-reader path are exercised; **no multi-client concurrent workload has been run** |
| Discovery / bootstrap / membership | Ray's in phase 1, by design; ours in phase 2 |
| Durable acknowledged state | Compliant and precisely stated: `sent` = three relays in memory; client history on disk with WAL and `synchronous=FULL` |
| Single-node failure behaviour | Compliant, measured |
| Latency / loss / partition behaviour | **Gap:** latency and loss are unit-only; partitions are phase 2 |
| Recovery and reconciliation | Compliant: resync before serving, hinted handoff |
| Retry and duplicate semantics | Compliant |
| Configurable deployment parameters | Compliant: every topology value is an environment variable in `scripts/env.sh` |
| **Structured logs and stable identifiers** | **Gap.** Logging is printf-style (`log.warning("%s: ...")`). Stable identifiers exist (relay names, envelope ids) but there is no structured format a grader could filter. Phase 2/3 work |
| Explicit trust/security model | Compliant since [ADR-0018](../decisions/0018-signed-acknowledgements.md): self-certifying ids, signed acks, relays trusted only to store and forget |
| Local, reproducible execution | Compliant |
| Public deterministic tests | Compliant: 507 passing, seeded fuzz, CI |

> "A partition does not imply that both sides must remain writable. You must
> state the consistency/availability trade-off selected by the architecture."

Stated: phase 1 does not claim partition tolerance. Phase 2 chooses **AP**, and
the reason it is safe is specific rather than hand-waved — envelopes are
immutable and content-addressed, so two sides can never hold conflicting
versions of one record, only different sets, and merging sets is a union. The
only state that genuinely conflicts is deletion, which tombstones carry
([`phase-plan.md`](phase-plan.md) §2).

## 8. The four questions phase-plan.md left open

`phase-plan.md` §6 listed four questions for the examiners. The methodology
answers three.

| Question | Answer |
|---|---|
| Does phase 2 keep the two-failure requirement? | **Yes.** "For both oral defenses" — it applies to the native system |
| Is any element guaranteed not to fail in phase 2? | **No exemption is given.** Phase 1's head exemption does not carry over, and phase 2 has no head by design |
| Is each delivery a live defence, a submission, or both? | Parts 1 and 2 are **submissions**; part 3 is the **oral defence**, individually, at the official exam timetable |
| Is the written deliverable expected in Spanish? | **Still open.** The methodology does not say. [ADR-0003](../decisions/0003-english-for-all-documentation.md) records English; the course site is bilingual, which is weak evidence either way. Worth asking alongside the Go question |

## 9. Ranked actions

**Before 2026-12-01 (five days)**

1. **Confirm the GitHub submission issue exists** with topic, both authors'
   full names and class groups, and the repository link. Without it the project
   is not submitted, whatever the code does. *Highest value per minute of
   anything in this document.*
2. **Ask the instructors the Go question** (§1.2), in writing, and the language
   question (§8) with it. Reply time is outside our control, so the clock
   starts when we send it, not when we decide.
3. **Capture the phase 1 numbers for the phase 3 comparison** (§1.3) while the
   Ray cluster is alive: startup time, send latency percentiles under a
   concurrent workload, and mechanism/dependency counts.
4. Three clean rehearsals and the written deliverable — the existing G5.

**Phase 2 planning (from 2026-12-02)**

5. Add a **phase 3** to `phase-plan.md`: the campaign, the comparison and
   individual-defence preparation are 35% and currently unplanned.
6. Adopt the methodology's **architecture report template** (five sections) as
   the shape of the January submission, and its **ADR decision table**
   (requirement / Ray phase 1 / native candidates / selected / trade-off) as
   the shape of the phase 2 decisions.
7. Close the baseline gaps: **structured logging**, container-level delay and
   loss (F3/F4), overload (F16/F17), and a concurrent multi-client workload.
8. Commit experiment evidence under `docs/experiments/runs/` rather than
   leaving it in scratch directories.

**Standing**

9. Keep the Ray implementation runnable and tagged. Phase 3 needs it.
10. Re-run this audit before each submission.

## 10. Verdict

**Phase 1 complies.** The prototype works and is genuinely distributed, all
five required artifacts now exist, the exit gate is answerable from measurement
rather than assumption, and level-two fault tolerance is demonstrated in all
three shapes the methodology names. Against the 15% rubric — prototype 7%,
architecture and middleware analysis 5%, initial experiment 3% — the evidence
is in place for each component.

Three caveats, in order of how much they should worry us:

1. **The Go question is unresolved** and it governs half the remaining mark.
2. **Phase 3 is unplanned**, and it is 35%.
3. **The submission issue is unverified**, and it is binary.

None of the three is a code problem. The code is in better shape than the plan
around it.
