# The two phases

**Status:** working plan · **Date:** 2026-11-11
**Source:** the examiners' clarifications of 2026-11-11, recorded in [`../research/00-problem-analysis.md`](../research/00-problem-analysis.md) section 6 and in [ADR-0010](../decisions/0010-build-phase-1-on-ray.md) and [ADR-0013](../decisions/0013-ray-free-core-for-phase-2.md).

---

## 1. What each phase requires

| | Phase 1 | Phase 2 |
|---|---|---|
| Delivery | **2026-12-01** | **2027-01-01** |
| Platform | Ray and Python. Use every Ray feature that applies naturally; do not reimplement what Ray offers | No Ray at all. Everything redesigned and implemented by us |
| Failures to tolerate | Any two elements at once | Not yet stated; we assume at least the same |
| Partitions | Not required | **Required** |
| Head node | Exists, is never killed | Does not exist |
| Constant in both | Docker without Compose; the brief's requirements R1 to R9; reasoning about edge cases and tradeoffs |

Today is 2026-11-11. Phase 1 has 20 days. Phase 2 has 31 days after that.

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

Twenty days is short for this design. The order below puts the things that can sink the delivery first. The task-level plan, with owners, dependencies, gates and the person-day budget, is [`phase-1-work-plan.md`](phase-1-work-plan.md).

| Dates | Goal | Done when |
|---|---|---|
| Nov 12 to 14 | **Spike** (research note 07, section 12, items 1 to 9). In parallel: repository skeleton, images, `up-*.sh` scripts, CI | We know restart times, the exceptions callers see, memory per node, and how many nodes the laptop holds |
| Nov 15 to 18 | **Vertical slice.** One client sends to another through relays on Ray: `store`, `fetch`, `ack`, three copies, plaintext envelopes, SQLite history, minimal UI | A message survives the recipient being offline |
| Nov 19 to 23 | **Fault tolerance.** Stand-ins and hints, sync on restart, handoff, TTL and tombstones, supervised loops. Fake transport with randomized fault injection. In parallel: encryption | The two-failure tests in `fault-tolerance.md` section 8 pass |
| Nov 24 to 27 | **Encryption integrated**, ordering and gap handling, delivery states in the UI, metrics, the overlay view | The demonstration script runs end to end |
| Nov 28 to 30 | **Hardening and the written deliverable.** Rehearse the demonstration, fix what breaks, regenerate the reports | Three clean rehearsals in a row |

**Scope, in priority order**

1. *Must:* everything in the vertical slice; three copies before `sent`; stand-ins; sync on restart; TTL; duplicate suppression; ordering; local history; authenticated encryption between clients; the two-failure demonstration.
2. *Should:* hinted handoff back to primaries; tombstone propagation; `wait_for_mail`; metrics; the overlay view.
3. *Could:* periodic digest comparison; streaming delivery; padding to several size classes.

**The largest schedule risk is the ratchet.** Implementing X3DH and the Double Ratchet from the specifications, with tests, is realistic over several weeks and tight inside this one. The pending decision (ADR-0009) should be taken in the first week. Our recommendation is to adopt an existing Python implementation for phase 1 if the spike shows one that works and has an acceptable licence, and otherwise to ship X3DH with a simple symmetric ratchet in phase 1 and complete the Double Ratchet in phase 2. Either way the choice and what it costs in security properties gets written down.

## 5. Phase 2 outline

| Dates | Goal |
|---|---|
| Dec 2 to 6 | Phase 2 design document: replication rule under partitions, membership, failure detection, transport, message types. Decisions recorded as ADRs |
| Dec 7 to 16 | Transport, membership and failure detector behind the existing interfaces; relays as plain processes |
| Dec 17 to 24 | Placement over live membership, rebalancing, anti-entropy, partition tests with Docker networks |
| Dec 25 to 31 | Hardening, demonstration, written deliverable |

## 6. Questions still open with the examiners

- Does phase 2 keep the two-failure requirement, raise it, or leave it to us?
- In phase 2, is there any element that is guaranteed not to fail, as the head is in phase 1?
- Is the written deliverable expected in Spanish? ([ADR-0003](../decisions/0003-english-for-all-documentation.md) records English as our working choice.)
- Is each delivery a live defense, a submission, or both?
