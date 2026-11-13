# Phase 1 work plan

**Status:** working plan, to be adjusted after the spike · **Written:** 2026-11-11 · **Revised:** 2026-11-12
**For:** the two developers on the team, working with Claude Code
**Delivery:** 2026-12-01 (19 calendar days from today)
**Revision of 2026-11-12:** the team settled the cryptography ([ADR-0009](../decisions/0009-adopt-a-cryptography-library.md): adopt a library) and the scope ([ADR-0014](../decisions/0014-phase-1-scope-cuts.md): optional features deferred, no direct client-to-client path). Sections 4, 6, 8 and 9 are updated accordingly.
**Design this executes:** [`ray-architecture.md`](../design/ray-architecture.md) · [`fault-tolerance.md`](../design/fault-tolerance.md) · [`phase-plan.md`](phase-plan.md) (the two phases) · [`../research/07-ray.md`](../research/07-ray.md) (Ray, and the spike list)

---

## 1. What has to exist on 2026-12-01

The delivery is done when all of this is true, and not before:

1. A running system: one head container, five or more relay node containers, two or more client containers, all started one by one with `docker run`, in any order, with
 no Compose.
2. Two users exchange encrypted one-to-one messages through a browser UI, with the recipient's container stopped at the time of sending.
3. **Any two elements can be killed simultaneously** without the system being affected. Demonstrated for: two relay containers, two relay processes, one of each, and a client killed mid-send.
4. No duplicates and no wrong order in either client's history after any of those scenarios.
5. Relays hold only padded ciphertext under opaque identifiers, in memory, deleted on acknowledgement or expiry, with no history operation in the interface.
6. Every relay can be wiped and both clients still have their full history.
7. The reasoning is written down: the failure analysis, the edge cases, and what each decision gains and loses.
8. The demonstration runs start to finish without improvisation, three times in a row.

Items 3, 4 and 7 are what the examiners added to the brief. They are the ones to protect when time runs short.

## 2. Two facts that shape this plan

**The design rests on Ray behaviour we have read about and never run.** Whether a relay restarts on a spare node, how long that takes, what a caller sees meanwhile, and how many Ray nodes fit on the demonstration laptop are all unverified. If the first of those is wrong, [ADR-0011](../decisions/0011-relays-as-ray-supervised-actors.md) changes and so does a week of work. So the spike comes first, before anything is built on top of it.

**The interface between the two tracks is the critical path.** One person can own the relay side and the other the client side, in parallel, only if both agree on `core/ports.py` and the envelope format first. That agreement is the first task of the project and it is a shared one.

## 3. The split

| | Dev A, relay track | Dev B, client track |
|---|---|---|
| Owns | `linkchat.core` relay side (ring, mailbox store, repair), `linkchat.ray_runtime`, Docker images, cluster scripts, fault-injection scripts | `linkchat.core` client side (envelope, crypto, outbox, ordering, history), `linkchat.client_app`, the React UI, the fake transport |
| Leads | The spike, the replication and repair argument, the two-failure tests | The cryptography decision, the ordering rule, the demonstration UI |
| Defends at the delivery | Why two failures cannot lose a confirmed message | Why the history is correct, ordered and free of duplicates |

Both own the interfaces, the fuzz tests and the written deliverable. Neither should be the only person who understands a mechanism: whoever did not write a piece reviews it, and the review is where the explanation gets rehearsed.

## 4. Budget check

**Revised 2026-11-12,** after the team settled the cryptography and the scope ([ADR-0009](../decisions/0009-adopt-a-cryptography-library.md), [ADR-0014](../decisions/0014-phase-1-scope-cuts.md)).

13 weekday days remain (Nov 12, 13, 16 to 20, 23 to 27, 30), plus 6 weekend days (Nov 14, 15, 21, 22, 28, 29).

| | Person-days available |
|---|---|
| Weekdays only, two people | 26 |
| Every day, two people | 38 |

The task list in section 6 now adds up to about **32 person-days**: the original 33.5, minus the deferred digest comparison (1) and the reduced overlay view (0.75), with the cryptography fixed at 3 rather than 6 because the library decision is taken.

Three things follow:

1. **The two decisions bought less than they appear to.** Deferring the optional features returned under 2 person-days. The one that mattered was the cryptography: writing it ourselves would have put the total near 35 against 38, leaving nothing for the spike invalidating something. That risk is now retired.
2. **Weekdays alone are still not enough.** 32 against 26 leaves a gap of 6 person-days, which is three weekend days with both people working, out of the six available. The pressure in this project comes from the core mechanisms, not the extras, and there is nothing optional left whose removal would change that.
3. **Protect the last two days.** Rehearsal and the written deliverable are in the budget, not squeezed out of it.

These are estimates by two people who have not built this before. Expect them to be wrong individually and treat the total as the thing to watch. If the spike costs more than the two days allowed for it, the gap comes out of the weekend rather than out of section 1.

## 5. Milestones and gates

A gate is a question with a yes or no answer. If the answer is no, the plan changes that day rather than at the end.

| Gate | Date | Question |
|---|---|---|
| **G0** | Nov 12, end of day | Are `core/ports.py` and the envelope format frozen, and does the repository build and test on both machines? Until this is done nothing else can run in parallel, so it is today's only priority |
| **G1** | Nov 14 | Does Ray behave as the design assumes? Specifically: does a relay actor restart on a spare node when its container is killed, how long does it take, what exception does a caller see first, and do about ten Ray nodes fit on the laptop? |
| **G2** | Nov 18 | Does one client send a message through real relays on a real Ray cluster to a client whose container is stopped, and does that client get it on return? |
| **G3** | Nov 23 | Do the two-failure scenarios pass: two relay containers, two relay processes, one of each, and a client killed mid-send? |
| **G4** | Nov 26 | Does the whole demonstration script run end to end, with encryption, ordering and the UI? |
| **G5** | Nov 30 | Three clean rehearsals in a row, and the written deliverable finished? |

### G1 is the one that can change the plan

If the spike shows that Ray does not restart a relay onto a spare node the way the documentation reads, the fallback is the design we wrote first: relays pinned to their container, `max_restarts=0`, and placement recomputed over the live relays. That is more of our own code, and it needs the examiners to agree that letting Ray restart actors is not available to us rather than that we declined to use it. Decide this on Nov 14, not later.

## 6. Tasks

Half-day granularity. "Done when" is the check, not an opinion. Dependencies are the only ordering that matters; anything else can move.

### Shared, Nov 11 to 14

| # | Task | Owner | Days | Depends | Done when |
|---|---|---|---|---|---|
| S1 | Freeze `core/ports.py` (`RelayGateway`, `RelayPeers`, `Clock`) and the envelope format: fixed size, field layout, how `envelope_id` is derived | Both | 0.5 each | | Both tracks can be written against it without talking. No Ray type appears in it |
| S2 | Repository skeleton: `uv` with a lockfile, `ruff`, `mypy --strict` on `core`, `pytest`, and the CI check that fails if `core` imports Ray | B | 0.5 | | `uv run pytest` and the no-Ray check run green on both machines |
| S3 | One Docker image, three entrypoints. `up-head.sh`, `up-relay-node.sh`, `up-client.sh`, `kill-nodes.sh`, `kill-relay-process.sh` | A | 1 | | A head and three relay nodes join one cluster, started in any order, and `ray list nodes` shows them |
| S4 | **The spike.** Items 1 to 9 of [`../research/07-ray.md`](../research/07-ray.md) section 12 | A | 2 | S3 | Every item has a measured answer written into the research note, and G1 is answered |

### Relay track, Dev A

| # | Task | Days | Depends | Done when |
|---|---|---|---|---|
| R1 | `core/ring.py`: hash ring over relay names, preference list of a mailbox | 0.5 | S1 | Unit tests: the list is the same from any process, and its first three are the primaries |
| R2 | `core/mailbox.py`: in-memory store, TTL index, tombstones, idempotent store keyed by `envelope_id`, per-mailbox quota | 1 | S1 | Unit tests: storing twice is one record; acknowledging leaves a tombstone; a late store after a tombstone is refused; expiry removes both |
| R3 | `ray_runtime/relay_actor.py` and `bootstrap.py`: the async actor wrapping R2, named and detached, `relay_slot`, three concurrency groups; bootstrap is idempotent | 1 | S4, R2 | Five relays appear in `ray list actors`, on five different nodes, and running the bootstrap again changes nothing |
| R4 | `ray_runtime/gateway.py`: `RelayGateway` and `RelayPeers` over actor calls, turning Ray's exceptions into ours, with a per-call timeout | 1 | S1, R3 | The client track can use it without importing Ray. A call to a killed relay raises our own error within the timeout |
| R5 | Three-copy store with stand-ins: write to the three primaries at once, fall through the preference list on failure, confirm at three distinct relays, carry the hint | 1.5 | R1, R4 | With two relays killed, a send still reaches three relays and reports `sent`. With three killed it stays `pending` |
| R6 | Sync on restart: `SYNCING` and `READY`, `pull` from peers, refuse client calls while syncing | 1.5 | R3, R5 | A killed and restarted relay reaches `READY` with the same record count as before, and serves nothing while syncing |
| R7 | Hinted handoff: the background loop that returns covered records to their primary and drops its own copy on confirmation | 1 | R5, R6 | After a relay returns, the stand-in's copy is gone and the primary has the record |
| R8 | TTL sweeper, supervised background loops (restart on exception, counters, last-success time), `status` | 1 | R2, R3 | Killing a loop's coroutine is visible in `status` and the loop comes back. Expiry removes records with no acknowledgement |
| R9 | `ray.util.metrics` counters and gauges per relay; tune the concurrency groups | 0.5 | R3, R8 | Stored, expired, repaired and parked-waiter counts are visible, and parked waiters do not block `store` |
| R10 | Periodic digest comparison between primaries | **deferred** | | Not built in phase 1 ([ADR-0014](../decisions/0014-phase-1-scope-cuts.md)). The reasoning, and what it costs us, are recorded there |
| R11 | Fault-injection scripts and the container-level test runs: kill two nodes in one command, kill two processes, one of each | 1 | R5 to R8 | Each scenario is one command, repeatable, with a pass or fail printed at the end |

### Client track, Dev B

| # | Task | Days | Depends | Done when |
|---|---|---|---|---|
| C1 | `core/envelope.py`: padding to a fixed size, `envelope_id` from the ciphertext, msgpack payload | 0.5 | S1 | Round-trip tests; every envelope is the same number of bytes whatever the message |
| C2 | The fake transport: an in-process `RelayGateway` and `RelayPeers` that can drop, delay, duplicate, reorder and kill relays on command | 1 | S1 | A test can script "kill relays 1 and 3 after the second call" and the core behaves as in the real thing |
| C3 | SQLite schema and the history store: identity, sessions, messages, outbox, seen-set, with a unique index on the envelope ID; WAL mode | 1 | S1 | A client killed at any point reopens with no lost or duplicated row |
| C4 | Outbox with retry and backoff: written before the first attempt, cleared on confirmation | 1 | C3 | A client killed mid-send finishes the send after restart, and the recipient sees one copy |
| C5 | Send and receive loops: three-copy send through the gateway, fetch-merge-decrypt-commit-acknowledge, one parked `wait_for_mail` | 1 | C1 to C4, R4 | Against the fake transport, a full exchange works with relays failing underneath |
| C6 | **Cryptography:** integrate the adopted library ([ADR-0009](../decisions/0009-adopt-a-cryptography-library.md)); identity keys, prekey bundles published through relays, session and ratchet state in SQLite. Confirm the library's licence and maintenance **before** any code depends on it | 3 | C3 | The library's own test vectors pass. Two clients with no prior contact establish a session while one of them is offline, and both survive being killed and restarted mid-conversation |
| C7 | Ordering: two-element vector clock per conversation, predecessor reference, hold-and-release buffer, gap marker on timeout, tie-break on concurrent messages | 1.5 | C3, C5 | Both clients show the same order for concurrent messages. A missing predecessor releases after the timeout with a marker |
| C8 | FastAPI and the WebSocket to the browser, served from the client process with one worker | 1 | C5 | The UI updates without polling, and no process is forked after Ray starts |
| C9 | React UI: conversations, composer, per-message delivery state (`pending`, `sent`, `delivered`, `read`, `not delivered`), contact invitation | 2 | C8 | A grader can watch a tick change from `pending` to `sent` as relays are killed |
| C10 | Overlay view, reduced: the five relays with the status this client already learns from its own calls, and which three hold the current mailbox. Ray's dashboard covers nodes and actor placement | 0.25 | C9 | Killing a relay container changes the view within the failover timeout |

### Shared, Nov 24 to 30

| # | Task | Owner | Days | Depends | Done when |
|---|---|---|---|---|---|
| S5 | Randomized fault injection on the fake transport: thousands of runs with random pairs of failures, restarts and delays, asserting after each that every confirmed message arrived exactly once and in a consistent order | Both, 0.75 each | 1.5 | C2, R5 to R7 | A seeded run that found a bug is kept as a regression test. The suite runs in CI |
| S6 | The demonstration: script it, automate what can be automated, rehearse | Both, 0.75 each | 1.5 | G4 | Three consecutive clean runs, each under the time allowed |
| S7 | The written deliverable: regenerate the reports, finish the failure analysis, write the known limits | Both, 1 each | 2 | G4 | Someone outside the team can read it and say what the system does when two relays die |

## 7. Working with Claude Code

Treat it as a fast pair who has read the design and has no judgement about what matters. Three rules make the difference between it saving days and costing them.

**Delegate the mechanical work.** Dockerfiles and shell scripts (S3), the SQLite schema and migrations (C3), the fake transport (C2), React components (C9, C10), test scaffolding from a written spec, and the randomized fuzz harness (S5). These are well-specified, easy to check, and boring to type.

**Do not delegate the two things that are graded.** The replication and repair argument (R5 to R7) and the cryptography (C6). Not because it writes them badly, but because at the delivery you have to explain why two failures cannot lose a confirmed message, and reading an explanation is not the same as having built the thing. Use it to review your reasoning, to look for the case you missed, and to write the tests that would catch you being wrong. Write the mechanism yourselves.

**Never ship cryptography it wrote without test vectors passing.** This holds whoever writes it, but it is the one area where plausible and correct look identical.

A few practical habits:

- Point it at the design documents instead of re-explaining. `docs/design/ray-architecture.md` section 5 is a better prompt than a paragraph of description.
- Ask for the test first, then the implementation. Then you review a test you understand rather than code you do not.
- When Ray produces an error you have not seen, give it the full traceback and the relevant part of `docs/research/07-ray.md`. It is good at mapping Ray's errors onto what the documentation says they mean.
- Have it write the edge cases it thinks you missed into `fault-tolerance.md` as questions, and then answer them yourselves.

On cost, since this ran high during the research:

- One session per track, closed when you stop for the day. Long-lived sessions are the largest single cost.
- `/clear` when you switch task. The context from the last task is being resent with every message otherwise.
- Do not ask it to spawn subagents unless you want many of them; one session reading one file is far cheaper than five agents reading the web.
- Prefer small edits to whole-file rewrites.

## 8. Scope

Settled on 2026-11-12 by [ADR-0014](../decisions/0014-phase-1-scope-cuts.md). The optional work is out of phase 1 rather than attempted and abandoned half-built.

**In scope, and not negotiable.** The vertical slice; three copies before `sent`; stand-ins; sync on restart; hinted handoff (R7); TTL and tombstones; duplicate suppression; ordering; local history; authenticated encryption between clients; the parked `wait_for_mail`; metrics (R9); the reduced overlay view (C10); the two-failure demonstration; the written reasoning.

**Out of phase 1.** Periodic digest comparison (R10); several padding size classes; streaming delivery; read receipts; any direct client-to-client path. Each one's reason, and what it costs us, is in ADR-0014.

Two of those deserve a word here because they look like they touch correctness and do not. Dropping the digest comparison means an acknowledged envelope can sit on one relay until it expires, where the client's seen-set absorbs it: deletion is less prompt, nothing is lost or duplicated, and expiry remains the guarantee R9 rests on. Dropping the direct client-to-client path means every message goes through relays without exception, which is a property worth claiming rather than an omission to apologise for.

**If the plan still slips** after this, the next things to go are the reduced overlay view (0.25) and `wait_for_mail`, in that order. Without `wait_for_mail` the client polls, which is slower and still correct, but it also removes one of the clearest natural uses of a Ray async actor, so it goes last. Decide at G3 (Nov 23), not in the final week.

## 9. Risks

| Risk | Signal | What we do |
|---|---|---|
| Ray does not restart relays as documented | G1 on Nov 14 | Fall back to the pinned-relay design and tell the examiners why. Costs about three days |
| About ten Ray nodes do not fit on the laptop | G1 | Fewer relay nodes, no spares, and say what that costs in the analysis. Decide the topology that night |
| ~~The ratchet eats the schedule~~ | Retired 2026-11-12 | A library is adopted ([ADR-0009](../decisions/0009-adopt-a-cryptography-library.md)). What remains is smaller: the library may fail licence or maintenance verification, in which case we ship X3DH from a library with a single symmetric ratchet of our own and declare the reduced forward secrecy |
| We cannot explain cryptography we did not write | The first rehearsal | Whoever integrates the library reads the X3DH and Double Ratchet specifications while doing it, not the week before the defense |
| The two tracks drift apart | The first integration at G2 | S1 on day one, and integrate on the real cluster at G2 and then every two days, never only at the end |
| A two-failure test fails for a reason we do not understand | G3 | This is the one to stop for. An unexplained failure in the mechanism we are graded on is worse than a missing feature. Use the fake transport to reproduce it deterministically before changing anything |
| Everything works and nobody can explain it | Rehearsals at G5 | Each person presents the other's track once during rehearsal. Whatever cannot be explained gets explained or removed |

## 10. Cadence

- **Every morning, 15 minutes.** What each person finished, what they are on, what is blocked. Move the gate dates if they are wrong rather than hoping.
- **Every two days from G2.** Integrate on a real cluster. Not on the fake transport, which is for correctness, not for Ray.
- **At each gate.** Answer the question, write the answer into the relevant document, and adjust what follows the same day.
- **Every evening.** Push. Any work that exists only on one laptop is one failure away from gone, which would be an ironic way to lose this project.

## 11. What this plan assumes

Written down so that the wrong ones are visible later: that the spike takes two days and not four; that both developers are available most days of the next three weeks; that the Ray documentation describes the Ray we get; that adopting a ratchet library is possible with an acceptable licence; that the demonstration laptop holds the topology; and that estimates made by people who have not built this before are wrong by less than the slack in section 4.
