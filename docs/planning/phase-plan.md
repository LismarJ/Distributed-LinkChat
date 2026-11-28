# The three phases

**Status:** working plan · **Date:** 2026-11-11 · **Revised:** 2026-11-26
**Source:** the examiners' clarifications of 2026-11-11, recorded in [`../research/00-problem-analysis.md`](../research/00-problem-analysis.md) section 6 and in [ADR-0010](../decisions/0010-build-phase-1-on-ray.md) and [ADR-0013](../decisions/0013-ray-free-core-for-phase-2.md); and, from 2026-11-26, the examiners' written *Final Project Methodology*.

**Revision of 2026-11-26.** The methodology was read in full and it changes
three things this document had wrong. It defines **three** graded phases, not
two. It weights them **15 / 50 / 35**, so phase 1 is the smallest. And it says
the native implementation "should normally use Go", which this plan had assumed
would be Python. The audit is
[`methodology-compliance.md`](methodology-compliance.md); the Go question is
section 1.2 there and is the project's largest open risk.

---

## 1. What each phase requires

| | Phase 1 | Phase 2 | Phase 3 |
|---|---|---|---|
| Weight | **15%** | **50%** | **35%** |
| Date | **2026-12-01** (submission) | **2027-01-01** (architecture report) | Final examination (oral, individual) |
| Platform | Ray and Python. Use every Ray feature that applies naturally; do not reimplement what Ray offers | No Ray. Everything redesigned and implemented by us. **"Normally Go"** — unresolved, see the audit | Both, side by side |
| Delivers | Working prototype, architecture diagram, behavioural contract, middleware dependency map, initial experiment | The native system and a five-section architecture report | Fault/workload campaign, guarantee analysis, Ray/native comparison, individual defence |
| Failures to tolerate | Any two elements at once | Any two, with **no exempt component** | Demonstrated under the campaign |
| Partitions | Not required | **Required** | Tested |
| Head node | Exists, is never killed | Does not exist | — |
| Constant in all three | Docker without Compose; the brief's requirements R1 to R9; **the externally visible behavioural contract**; reasoning about edge cases and tradeoffs |

The behavioural contract is the invariant the methodology cares most about: the
internal architecture is *expected* to change across the Ray boundary, and the
contract is what both implementations are measured against. It is written and
frozen in [`../design/behavioral-contract.md`](../design/behavioral-contract.md).

**Phase 3 has a consequence for phase 2's housekeeping:** the Ray/native
comparison is worth 5% and cannot be done if the phase 1 system is deleted or
left unrunnable. Keep it working and tagged, and take its measurements while it
is alive.

## 2. What phase 2 has to replace

Phase 1 hands a list of jobs to Ray. In phase 2 each one becomes ours.

| Job | Phase 1 (Ray) | Phase 2 (ours) | Where we already studied it |
|---|---|---|---|
| Nodes finding each other | Join through the head's address | Seed addresses and gossip | `research/06`, `architecture-options.md` section 4 |
| Knowing who is alive | GCS health checks | A SWIM-style failure detector | `research/06` |
| Remote calls | Actor method calls | asyncio TCP with our own framing and message types | `architecture-options.md` section 9 |
| Finding a relay | Named actors | The membership view | |
| Bringing a failed relay back | `max_restarts` | Nobody does. A dead relay is gone and a new container is a new relay. Placement must follow membership | Session's swarms, `research/04` |
| Retrying calls | `max_task_retries` | Our own retry with backoff | Matrix's transaction queues, `research/02` |
| Concurrency inside a relay | Async actor, concurrency groups | Plain asyncio with our own limits | |
| Access control between nodes | Token authentication | Signed requests between nodes | Matrix request signing, `research/02` |
| Observability | Dashboard, state CLI, metrics | Our own status endpoint and overlay view | |

And the assumptions phase 1 is allowed to make stop holding:

| Phase 1 assumption | Phase 2 reality | Consequence |
|---|---|---|
| No partitions; a silent relay is dead | A silent relay may be alive on the other side | "Three copies, no majority" is no longer safe as stated. The replication rule must be redesigned for partitions |
| The ring is fixed; Ray restores its members | Membership changes and views can disagree | Placement over a changing membership, rebalancing, and repair of writes that went to a stale set |
| A restarted relay syncs once and is correct | Two sides accept writes separately and must merge afterwards | Continuous anti-entropy with digests becomes essential, not optional |
| One trust domain | Relays can be independent | The stronger threat model returns |

This is the design that existed before the Ray requirement: [`architecture-options.md`](../design/architecture-options.md), option C with option D (gossip flooding) as a partition-mode fallback. It is not thrown away; it is the starting point for phase 2, to be revised once phase 1 has taught us what was hard.

**Choosing availability under partition.** For messaging, a user on either side of a partition should still be able to send to anyone reachable, and messages should merge when the partition heals. That is an AP choice, and it is safe for us because envelopes are immutable and identified by content: two sides can never hold conflicting versions of the same record, only different sets of records, and merging sets is a union. The one piece of state that does conflict is deletion, which tombstones handle. This argument is the core of the phase 2 design and has not been written up in full yet.

## 3. What carries over unchanged

Everything in `linkchat.core` ([`ray-architecture.md`](../design/ray-architecture.md) section 11):

- identity, X3DH, Double Ratchet;
- envelope format, padding, content-hash IDs;
- the in-memory mailbox store with TTL and tombstones;
- the client: outbox, seen-set, ordering, SQLite history;
- the repair logic, written against an interface;
- the web UI and the client's HTTP layer;
- the fake transport and the fault-injection tests built on it.

The hash ring carries over as code but changes its input: a fixed list of names in phase 1, a live membership view in phase 2.

**The rule that makes this work:** no module under `linkchat.core` imports Ray, and it reaches relays only through the interfaces in `core/ports.py`. A check in CI fails the build if `import ray` appears under `core/`. This is [ADR-0013](../decisions/0013-ray-free-core-for-phase-2.md).

There is a tension here and we should name it. Phase 1 says not to reimplement what Ray offers; phase 2 says to implement everything. Writing phase 2's networking early, behind the interfaces, would be the efficient thing to do and is exactly what phase 1 forbids in the delivered system. Our line: phase 1's delivered code uses Ray for all of it, and no phase 2 networking ships in phase 1. Designing phase 2 on paper during phase 1 is allowed.

## 4. Phase 1 schedule

| Dates | Goal | Done when |
|---|---|---|
| Nov 12 to 14 | **Spike** (research note 07, section 12, items 1 to 9). In parallel: repository skeleton, images, `up-*.sh` scripts, CI | We know restart times, the exceptions callers see, memory per node, and how many nodes the laptop holds |
| Nov 15 to 18 | **Vertical slice.** One client sends to another through relays on Ray: `store`, `fetch`, `ack`, three copies, plaintext envelopes, SQLite history, minimal UI | A message survives the recipient being offline |
| Nov 19 to 23 | **Fault tolerance.** Stand-ins and hints, sync on restart, handoff, TTL and tombstones, supervised loops. Fake transport with randomized fault injection. In parallel: encryption | The two-failure tests in `fault-tolerance.md` section 8 pass |
| Nov 24 to 27 | **Encryption integrated**, ordering and gap handling, delivery states in the UI, metrics, the overlay view | The demonstration script runs end to end |
| Nov 28 to 30 | **Hardening and the written deliverable.** Rehearse the demonstration, fix what breaks, regenerate the reports | Three clean rehearsals in a row |

**Scope, in priority order**

Settled on 2026-11-12 by [ADR-0014](../decisions/0014-phase-1-scope-cuts.md). The task-level version is [`phase-1-work-plan.md`](phase-1-work-plan.md) section 8.

1. *In scope:* everything in the vertical slice; three copies before `sent`; stand-ins; sync on restart; hinted handoff; TTL and tombstones; duplicate suppression; ordering; local history; authenticated encryption between clients; `wait_for_mail`; metrics; a reduced overlay view; the two-failure demonstration.
2. *Out of phase 1:* periodic digest comparison; streaming delivery; padding to several size classes; read receipts; any direct client-to-client path.

**The ratchet is no longer the largest schedule risk.** Implementing X3DH and the Double Ratchet from the specifications would have been realistic over several weeks and unrealistic inside this one, so the team decided to adopt a maintained library ([ADR-0009](../decisions/0009-adopt-a-cryptography-library.md)). What remains is to confirm that library's licence and maintenance before anything depends on it, and to be able to explain the protocol at the defense despite not having written it. The same decision holds for phase 2.

**The deferred items are candidates for phase 2**, where anti-entropy stops being optional because partitions make divergence normal rather than exceptional.

## 5. Phase 2 outline

| Dates | Goal |
|---|---|
| Dec 2 to 6 | Phase 2 design document: replication rule under partitions, membership, failure detection, transport, message types. Decisions recorded as ADRs |
| Dec 7 to 16 | Transport, membership and failure detector behind the existing interfaces; relays as plain processes |
| Dec 17 to 24 | Placement over live membership, rebalancing, anti-entropy, partition tests with Docker networks |
| Dec 25 to 31 | Hardening, demonstration, written deliverable |

## 6. Phase 3 outline

Added 2026-11-26. It is 35% of the mark and was previously unplanned.

| Goal | What it means |
|---|---|
| **Fault/workload campaign** (10%) | The eight scenario classes of the methodology's common minimum campaign, plus the project-specific ones, each written as an experiment record. The mapping from our scenario names to the course's F0–F21 vocabulary is in [`methodology-compliance.md`](methodology-compliance.md) §6 |
| **Correctness/guarantee analysis** (5%) | What the system guarantees under those faults, as observable histories, and the evidence for each claim |
| **Ray/native comparison** (5%) | Phase 1 against phase 2: implementation complexity, explicit mechanism count, dependencies, startup, latency percentiles, recovery time, failure behaviour, and guarantees present in one and absent in the other. **No requirement that the native version wins** — the likely honest conclusion is that it is smaller because it omits general-purpose scheduling and object management we never needed |
| **Individual technical defence** (10%) | Each author alone: predict a fault before it is injected, locate where a guarantee is implemented, name an invariant, explain why an operation is idempotent, state the consistency model, identify a single point of failure, justify a timeout, modify a small part live, and explain an AI suggestion accepted or rejected |
| **Reproducibility, report, disclosure** (5%) | One command to run it, the report, and [`../../ai_use.yml`](../../ai_use.yml) |

Two preparations that have to start before January:

- **Measure phase 1 while it runs.** Recovery time is measured (~31 s for a
  node, ~1.3 s for a worker process). Startup time, latency percentiles under a
  concurrent workload, and mechanism counts are not, and they are inputs to the
  comparison.
- **Rehearse the individual defence by swapping tracks.** The work plan already
  says each author presents the other's work once; phase 3 turns that from good
  practice into a graded requirement.

## 7. Questions with the examiners

The methodology of 2026-11-26 answered three of the four we had open.

| Question | Answer |
|---|---|
| Does phase 2 keep the two-failure requirement? | **Yes**, and for the oral defence of the native system |
| Is any element guaranteed not to fail in phase 2, as the head is in phase 1? | **No.** No exemption is given, and phase 2 has no head by design |
| Is each delivery a live defence, a submission, or both? | Parts 1 and 2 are **submissions**; part 3 is the **individual oral defence** |
| Is the written deliverable expected in Spanish? | **Still open.** [ADR-0003](../decisions/0003-english-for-all-documentation.md) records English as our working choice |

Still to ask, and the more urgent of the two:

- **May phase 2 be Python rather than Go?** The methodology says Go is the
  normal choice and that another language needs a strong domain reason. Ours is
  the cryptography ([ADR-0009](../decisions/0009-adopt-a-cryptography-library.md)):
  the X3DH and Double Ratchet libraries we adopted are Python, and rewriting in
  Go reinstates the exact schedule risk that decision retired. The methodology's
  own rule is to ask first and that silence means no, so this needs sending,
  not deciding. [`methodology-compliance.md`](methodology-compliance.md) §1.2.
- Spanish or English for the written deliverable, as above.
