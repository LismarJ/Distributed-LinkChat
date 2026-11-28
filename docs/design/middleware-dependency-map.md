# Middleware dependency map

**Status:** current for phase 1 · **Date:** 2026-11-26
**Required by:** the examiners' methodology, *Required Phase 1 artifacts* item 4
**Answers the exit-gate questions:** "Which behaviour currently comes from my code?" and "Which behaviour currently comes from Ray?"

Consolidates, in the shape the methodology asks for, what was previously split
between [`ray-architecture.md`](ray-architecture.md) §1 (who does what now) and
[`phase-plan.md`](../planning/phase-plan.md) §2 (what phase 2 must replace).
Those two stay as the design narrative; this is the artifact.

For every distributed requirement: what provides it today, and the question the
native redesign has to answer. The fourth column is our provisional answer —
the methodology only asks for the question, but we have studied most of them
already and hiding that would be false modesty. A provisional answer is not a
commitment; the phase 2 ADRs are.

---

## 1. Requirements Ray currently provides

These are the rows where **Ray is load-bearing**. Every one of them is work
that phase 2 must do itself, and together they are the honest answer to "what
did Ray have to do for this apparently simple application code to work?"

| Requirement | Phase 1 mechanism | Question for the native redesign | Our provisional answer |
|---|---|---|---|
| **Nodes finding each other** | Every container runs `ray start --address=linkchat-head:6379`; the GCS on the head is the rendezvous | What replaces a single well-known rendezvous, and what happens when it is the thing that fails? | Seed addresses plus gossip. No head, so no single rendezvous to lose (`research/06`, `architecture-options.md` §4) |
| **Knowing who is alive** | GCS health checks. Measured: a dead *node* is noticed in ~30 s, a dead *worker* in ~1.3 s | How is suspicion produced, how fast, and what does a false positive cost? | A SWIM-style failure detector with explicit suspicion, tuned against the ~31 s we measured here |
| **Remote calls** | Actor method calls (`handle.store.remote(...)`) | What protocol, framing and message types? | asyncio TCP with our own length-prefixed framing (`architecture-options.md` §9) |
| **Serialization** | Ray serializes our dataclasses across the boundary automatically | What is on the wire, exactly, and who versions it? | Explicit codec for control messages. The *envelope* already has a frozen binary layout (`core/crypto/wire.py`), so only the control plane is new |
| **Finding a relay by name** | Named, detached actors in a namespace; `ray.get_actor("relay-2")` | Without a name service, how does a client address a relay? | The membership view: names resolve to addresses locally, from gossip |
| **Restarting a dead relay** | `max_restarts=-1`. Ray reruns the constructor, on a **spare node** if the original is gone (measured, three times) | Who notices, who decides where it restarts, and does a replacement even make sense? | Nobody restarts it. A dead relay is gone; a new container is a *new* relay, and placement follows membership. This is a genuine design change, not a port |
| **One relay per node** | Custom resource `relay_slot: 1`, `num_cpus=0` | Does this application need scheduling at all? | No. Placement is ours already (the ring); relays are started by hand, one per container |
| **Retrying a call across a restart** | `max_task_retries=2` | Which operations are safe to repeat, and how are duplicates detected? | All of them: `store` is idempotent on `envelope_id`, `ack` is idempotent and signed. Retry with backoff is already ours in the outbox |
| **Concurrency inside a relay** | Async actor with four concurrency groups (`client` 256, `waiters` 1000, `repair` 16, `report` 16) | What prevents one class of work starving another? | Plain asyncio with our own per-class limits. **Note:** each Ray group is its own event loop in its own thread — a fact we learned the hard way (see §4) |
| **Cluster access control** | Ray token authentication between nodes | Who is trusted to do what, and how is it enforced? | Signed requests between nodes (Matrix-style). Client→relay authorization is already ours ([ADR-0018](../decisions/0018-signed-acknowledgements.md)) |
| **Seeing node and actor placement** | Ray dashboard and `ray list nodes` / `ray list actors` | What evidence must the native design expose? | Our own status surface, extended. Already partly ours — see §2 |
| **Bootstrapping the relay set** | `ray job submit` runs an idempotent script using `get_if_exists=True` | How does the system come up, and is it still idempotent? | A plain script per container. Idempotence is ours already (the ring is fixed) |
| **The head node existing at all** | One head, never killed ([ADR-0012](../decisions/0012-head-node-policy.md)) | **This is phase 1's single point of failure.** What replaces it? | Nothing replaces it: phase 2 has no head. This is the row the methodology's "identify a single point of failure" question is about |

## 2. Requirements that are already ours

The methodology's exit gate asks which behaviour comes from our code. This is
that list, and it is deliberately the longer one.

Nothing below changes when Ray is removed, because no module under
`linkchat.core` imports Ray and the rule is enforced by a test that fails the
build ([ADR-0013](../decisions/0013-ray-free-core-for-phase-2.md),
`tests/test_core_is_ray_free.py`).

| Requirement | Mechanism, today and in phase 2 |
|---|---|
| Which relays hold a mailbox | Rendezvous hashing over relay names (`core/ring.py`). Phase 2 changes its *input* — a live membership view instead of a fixed list — not its logic |
| Durable acknowledgement | Three distinct relays confirm before `sent` (`core/replication.py`, [ADR-0005](../decisions/0005-three-copies-before-sent.md)) |
| Covering an unreachable relay | Stand-ins down a fixed preference list, carrying a hint |
| Refilling a restarted relay | Pull from peers before serving (`core/repair.py`, R6) |
| Returning covered records | Hinted handoff, dropping our copy only after the primary confirms (R7) |
| Forgetting | Acknowledgement → tombstone; TTL sweeper; tombstones outlive records (`core/mailbox.py`) |
| Duplicate suppression | Content-hash `envelope_id`, idempotent store, persistent client seen-set |
| Ordering | Two-element vector clock, predecessor reference, hold-and-release with a gap marker (`core/client/ordering.py`) |
| Local history | SQLite, WAL, `synchronous=FULL`, unique index on `envelope_id` |
| Retry and backoff | The outbox, written before the first attempt and cleared on confirmation |
| Identity, sessions, encryption | X3DH + Double Ratchet ([ADR-0009](../decisions/0009-adopt-a-cryptography-library.md), [ADR-0015](../decisions/0015-how-the-cryptography-is-used.md)) |
| Authorization of `ack` | Ed25519 signature by the mailbox's reader ([ADR-0018](../decisions/0018-signed-acknowledgements.md)) |
| Application observability | `status()` counts (records, tombstones, mailboxes, hinted records, per-loop last-success time) surfaced at `GET /api/relays` |
| Background-loop supervision | Our own supervisor: counts failures, records the last good round, takes the process down after `MAX_LOOP_FAILURES` |

**A correction worth recording.** Task R9 was planned as `ray.util.metrics`
counters and gauges. It was not built that way: the counts are exposed through
our own `status()` and the overlay view instead. The planned *outcome* ("stored,
expired, repaired counts are visible") is met, by a mechanism that survives
phase 2 — which is better than planned — but the parked-waiter count
specifically is **not** exposed, and `phase-1-work-plan.md` §6 still describes
the Ray-metrics version. Flagged rather than quietly reworded.

## 3. What the map says about the redesign

Counting the rows is itself a result:

- **13 requirements** are Ray's today. All 13 must be built in phase 2.
- **14 requirements** are already ours and are untouched by the boundary.

So the Ray removal is not a rewrite: it is a **transport, membership and
supervision** project bolted under an interface that already exists
(`core/ports.py`). That is the payoff of ADR-0013, and it is the single most
useful thing phase 1 did for phase 2.

The three rows that are genuine *design* changes rather than ports:

1. **Restarting a dead relay.** Ray makes a replacement with the same name on
   a spare node. Nothing in phase 2 does that, so placement must follow a
   changing membership instead of a fixed ring — and records must move.
2. **The head node.** Phase 1 is allowed one unkillable component. Phase 2 is
   not, and the methodology's two-failure requirement applies to the defence
   of the *native* system.
3. **Partitions.** Phase 1 treats a silent relay as dead, which is only safe
   because partitions are excluded. "Three copies, no majority" has to be
   re-argued under partitions; the provisional answer is AP with
   set-union merge, which is sound because envelopes are immutable and
   content-addressed, with tombstones as the only conflicting state
   ([`phase-plan.md`](../planning/phase-plan.md) §2).

## 4. Failure semantics we observed rather than assumed

The methodology's exit gate asks this one separately, and it is the question
most easily answered with wishful thinking. Each of these was measured on a
real cluster; none was taken from Ray's documentation.

| What we assumed | What we observed |
|---|---|
| A killed relay's actor restarts somewhere with a free slot | **True**, and onto a *spare node* specifically — observed three times |
| Restart is fast | **False, for a node.** ~31 s for a killed container (30.7 / 31.1 / 31.1), versus ~1.3 s for a killed worker process. The gap is Ray's node health check, and it sets the demonstration's pacing |
| A caller sees a Ray exception when a relay is gone | **Not always.** Between the kill and the restart being registered, `ray.get_actor` raises a plain `ValueError`. Resolution had to move inside the `try` so it maps to `RelayUnreachable` |
| Concurrency groups are just scheduling limits | **False, and it cost us a silent bug.** Each group is its own asyncio event loop in its own thread (different thread *and* loop ids, measured with a probe actor). `asyncio.Event.set()` is neither thread-safe nor cross-loop, so a reader parked in `waiters` was never woken by a `store` running in `client`; `wait_for_mail` degraded silently into a timeout-then-poll. Fixed with `loop.call_soon_threadsafe` |
| An exception in a background task stops the actor | **False.** The actor keeps serving, which is why every loop runs under our own supervisor |

The last two are the most valuable entries in this document: both are cases
where the application code looked correct, the tests passed, and the mechanism
was not doing what it claimed. Full detail in
[`../guides/2026-11-25-review-and-durability-fixes.md`](../guides/2026-11-25-review-and-durability-fixes.md)
and [`../research/07-ray.md`](../research/07-ray.md) §12.
