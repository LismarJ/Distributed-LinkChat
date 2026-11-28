# Phase 1 initial experiments

**Status:** complete for the phase 1 submission · **Date:** 2026-11-26
**Required by:** the examiners' methodology, *Required Phase 1 artifacts* item 5
**Graded as:** "Initial experiment", 3% of the final mark
**Format:** the methodology's experiment record — scenario, workload, claim, prediction, configuration, trigger, evidence, result, conclusion, remaining uncertainty

The methodology asks for *at least one* controlled failure, concurrency or
workload experiment, "to reveal runtime behavior before you remove the
runtime". Four are recorded here. The first three are the failure experiments
the delivery checklist turns on; the fourth is a concurrency experiment that
exists because it caught a defect nothing else did, and it is the one that most
directly answers "what did Ray have to do for this code to work?"

Scenario IDs in parentheses are from the course's Fault and Workload Scenario
Library, so phase 3 can be compared against the same vocabulary. The mapping
for the whole library is in
[`../planning/methodology-compliance.md`](../planning/methodology-compliance.md).

**Reproducing all of this:** `scripts/up.sh` then `scripts/scenario.sh all`.
Every experiment below is one command. The raw logs from the runs on 2026-11-25
and 2026-11-26 were written to a scratch directory and are *not* committed —
see "remaining uncertainty" in E1 and the recommendation in §5.

---

## E1 — Two relay containers killed at the same instant

**Scenario.** F1/F2 (crash, and crash-with-restart) applied to *two* components
simultaneously, which is the examiners' level-two fault tolerance requirement.
`scripts/scenario.sh two-nodes`.

**Workload.** Sequential sanity: one conversation with existing history, one
uniquely tagged message sent immediately before the fault.

**Claim under test.** Once a message reads `sent`, no two relays or relay nodes
failing at the same instant can lose it, duplicate it, or reorder the history
around it.

**Prediction.** The message arrives exactly once. The sender reads `sent`
(three relays confirmed). The recipient's earlier history is unchanged and in
the same order. Two of the five relay actors are re-placed by Ray and return to
`ready`.

**Configuration.** One head, seven relay node containers (five relay actors,
two spare nodes), two client containers, one user-defined Docker network, no
Compose. `R = 5`, `WRITE_COPIES = 3`. Built from `main` at the merged tip.

**Fault trigger.** A single `docker kill` naming both containers, so they die
together. This matters: killing them one after the other lets the system repair
in between and tests the one-failure case twice. The script selects two
containers that actually *host* relay actors, and since 2026-11-25 it fails the
scenario if the kill did not take effect rather than continuing against an
uninjured cluster.

**Collected evidence.** The script's own assertions: a count of messages with
the unique tag (must be exactly 1), the sender's recorded delivery state (must
be `sent`), and an element-wise comparison of the recipient's prior history
before and after. Plus `GET /api/relays` polled until all five report `ready`.

**Observed result.** PASS. Delivered exactly once; sender `sent`; the 31 prior
messages unchanged and in order; both relays back to `ready`.

**Conclusion.** The claim holds for two whole nodes. Combined with E2's
observation that a dead node is detected in ~31 s, the guarantee is "no loss",
not "no delay" — and the delay is bounded by Ray's health check, not by our
replication.

**Remaining uncertainty.** The run proves the *outcome*, not that the two kills
were truly simultaneous at the kernel level; they are one `docker kill`
invocation, which is as close as the API allows. The logs are not committed, so
the evidence for this record is the reproducible command rather than an
archived trace.

---

## E2 — How long failover actually takes, container versus process

**Scenario.** F1 (single process crash) against F2-at-node-scale, measured
rather than asserted. This is the experiment that "reveals runtime behaviour".

**Workload.** None. An idle, healthy cluster, so the measurement is of Ray's
detection and placement rather than of our traffic.

**Claim under test.** Ray restarts a lost relay actor, and does so onto a spare
node when the original node is gone. Implicitly: fast enough to demonstrate
live.

**Prediction.** Before measuring, we had no number — the design documents said
"restart time unmeasured" and the demonstration runbook said "record the
recovery time during rehearsal". We expected seconds, and expected a container
kill and a process kill to behave similarly. The second half of that
expectation was wrong.

**Configuration.** As E1. Five relay actors across seven relay nodes, leaving
two spare `relay_slot`s.

**Fault trigger.** Two separate triggers, each repeated: (a) `docker kill` of a
container hosting a relay actor, **without** restarting it, so Ray must find a
spare node; (b) `pkill -9` of the actor's worker process inside its container,
leaving the node alive.

**Collected evidence.** Wall-clock milliseconds from the trigger until
`GET /api/relays` reports all five relays `ready`, measured through a client —
so the number includes our own resync (R6), not just Ray's placement. Also
which container hosts each actor, before and after.

**Observed result.**

| Trigger | Runs | Time to all five `ready` |
|---|---|---|
| Relay **container** killed | 3 | 30 682 ms · 31 102 ms · 31 094 ms (~31 s) |
| Relay **worker process** killed | 2 | 1 353 ms · 1 256 ms (~1.3 s) |

Placement onto a **spare node** confirmed three times — e.g. `relay-0` killed,
its actor observed afterwards on `relay-4`.

**Conclusion.** Two findings, one of them load-bearing.

1. [ADR-0011](../decisions/0011-relays-as-ray-supervised-actors.md) holds: Ray
   does re-place a relay onto a spare node, so the fallback design of pinned
   relays is not needed. This was the question gate G1 existed to answer.
2. **A dead node costs ~24× a dead process.** The difference is Ray noticing: a
   worker death is immediate, a node death waits on the GCS health check. Thirty
   seconds is a long silence in a live demonstration, so the runbook now says to
   start explaining the moment the kill lands rather than waiting for green.

**Remaining uncertainty.** Five measurements on one Windows laptop running
Docker Desktop; no variance analysis, and the demonstration machine may differ.
The ~31 s is Ray's default health-check interval and is presumably tunable — we
chose not to tune cluster configuration days before a delivery, and to narrate
the pause instead.

---

## E3 — Every relay wiped at once

**Scenario.** Beyond the fault model on purpose: all five relays, not two.
Closest library entries are F2 (crash and restart) and F7 (heal and reconcile),
applied to the entire tier. `scripts/scenario.sh wipe-all-relays`.

**Workload.** Two clients with 38 messages of existing history each.

**Claim under test.** The relay tier is a transport, never a store of record.
Conversation history lives only in the clients, so destroying every relay
simultaneously costs nothing a participant already holds — and nothing comes
back as a duplicate.

**Prediction.** Both clients' histories are *byte-identical* before and after.
Note what is deliberately **not** predicted: a message still in flight on the
relays at the moment of the wipe would legitimately be lost, because five
simultaneous failures is outside the declared fault model. Asserting delivery
here would be claiming a guarantee we do not make.

**Configuration.** As E1.

**Fault trigger.** One `docker kill` naming every container that hosts a relay
actor. The containers are then restarted and the scenario waits for all five
relays to reach `ready` again.

**Collected evidence.** Each client's full conversation as an ordered list of
message texts, captured before the wipe and after the heal, compared as whole
strings — which catches loss, duplication *and* reordering in one comparison.

**Observed result.** PASS, in ~43 s. Both histories identical.

**Conclusion.** Requirement R9 ("do not become an archive") is structural here,
not a promise: a relay cannot reconstruct a conversation because there is no
operation in the interface that would let anyone ask it to. The wipe also
exercises duplicate suppression, because destroying the relays destroys their
**tombstones** too, so anything that came back would be re-offered to the
client — and the client's persistent seen-set is the only thing refusing it.

**Remaining uncertainty.** This tests history already *delivered*. It does not
characterise how much in-flight mail is lost as a function of wipe timing,
which would be a useful phase 3 experiment with a measured workload.

---

## E4 — Concurrency: do Ray's concurrency groups share an event loop?

**Scenario.** A concurrency experiment, not a fault one. No library ID: it tests
the middleware's execution model rather than the application's failure
behaviour.

**Workload.** One reader parked on an empty mailbox via `wait_for_mail`, then a
single `store` to that mailbox from a different concurrency group.

**Claim under test.** `wait_for_mail` lets a client park on a relay and be woken
the moment mail arrives, instead of polling. It is named in the work plan as
"one of the clearest natural uses of a Ray async actor".

**Prediction.** The parked call returns `True` promptly — well under a second —
when the `store` lands.

**Configuration.** A local Ray instance with the real `RelayActor`, plus a
separate minimal probe actor declaring two concurrency groups, used to inspect
the execution model directly.

**Fault trigger.** None. This is the point: no fault was needed to expose the
defect.

**Collected evidence.** (a) Time from the `store` until the parked call
returns. (b) From the probe actor, `threading.get_ident()` and
`id(asyncio.get_running_loop())` as reported from two different concurrency
groups.

**Observed result.** The parked reader was **never woken**. It sat until its own
15-second timeout expired and returned `False`, after which the client fetched
the message by the ordinary path — so the user-visible behaviour was correct and
the mechanism was entirely inert. The probe showed why: the two groups reported
**different thread ids and different event-loop ids**. A reader parks on the
`waiters` loop; the `store` that should release it runs on the `client` loop;
and `asyncio.Event.set()` is neither thread-safe nor cross-loop, so it queued
the waiter's callback on a loop it never woke. After switching the wake to
`loop.call_soon_threadsafe`, the same experiment returned `True` in 0.0 s.

**Conclusion.** **Each Ray concurrency group is its own asyncio event loop in
its own thread.** This is the most expensive thing phase 1 taught us about the
middleware, and it is exactly the kind of hidden mechanism the methodology's
phase 1 exists to surface: the application code was correct Python, the tests
passed, and the distributed behaviour it claimed was not happening. Phase 2
inherits the lesson as a requirement — our own relay will run one loop with
explicit per-class limits, and waking a waiter will be a local operation with no
cross-thread hazard.

A second lesson, about method: an earlier probe appeared to show cross-group
waking *working*, because the target loop wakes on its own timers often enough
to pick the callback up sometimes. Measuring the outcome would have confirmed
the wrong conclusion; measuring the mechanism — the loop ids — settled it.

**Remaining uncertainty.** The group-per-loop behaviour is observed on Ray
2.60.1 and is not, as far as we found, documented as a guarantee. It could
change between versions. Nothing in our phase 2 design depends on it; it is
recorded as a hazard, not as an assumption.

---

## 5. What these four do not cover

Honest gaps, so phase 3's campaign has a starting list rather than a blank page.
The methodology's common minimum campaign expects eight scenario classes; these
four experiments cover three of them end to end.

| Not yet run in containers | Why it matters | Library ID |
|---|---|---|
| Delay and packet-loss degradation | We can inject both in the fake transport, never between containers | F3, F4 |
| Network partition, then heal | Excluded from phase 1 by the examiners; **required in phase 2** | F5, F6, F7 |
| Sustained overload / saturation | The per-mailbox quota and `MailboxFull` path have unit tests only | F16, F17 |
| Concurrent multi-client workload | Every container run so far is two clients, one conversation | — |
| Unauthorized operation and replay | Now implemented and unit-tested ([ADR-0018](../decisions/0018-signed-acknowledgements.md)); not yet exercised against containers | F20, F21 |

**One process recommendation for phase 3.** None of the evidence above is
committed — the logs went to a scratch directory. For a campaign worth 10% of
the mark, each run should write a timestamped log into a committed
`docs/experiments/runs/` directory, so a record cites an archived artefact
rather than a command the grader has to re-run. Cheap to add, and it is the
difference between "we observed" and "here is the observation".
