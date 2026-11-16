# Guide: the Ray runtime — relay actor, bootstrap and gateway (R3 + R4)

**Audience:** a developer new to this repository who wants to understand the
`ray_runtime` layer added in this batch. No prior Ray knowledge assumed.

---

## 1. What this batch is

It adds the **Ray layer** of the relay track: the code that takes the pure,
Ray-free relay logic we already had and makes it run as real actors on a Ray
cluster. Two planned tasks:

- **R3** — the **relay actor** (`relay_actor.py`) and the **bootstrap**
  (`bootstrap.py`) that creates the five of them.
- **R4** — the **gateway** (`gateway.py`): the adapter that lets the rest of the
  system call a relay *without knowing Ray exists*.

**Important:** this was written **ahead of the plan on purpose.** The plan says
the spike (running Ray for the first time, gate G1) comes before R3+, because the
whole design assumes Ray restarts a dead relay onto a spare machine, and we
haven't confirmed that. So everything here is marked **"pre-spike"**: it's
written against the documented behaviour and may need rework once the spike runs.
Nothing here has executed against Ray.

---

## 2. The one idea behind the whole layer

> **Ray keeps the relays alive; we keep the data alive.**

- **Ray's job:** start the relay processes, find them by name, restart one that
  crashes (or move it to another machine), serialize the network calls.
- **Our job:** decide which relays store a mailbox, keep three copies, refill a
  restarted (empty) relay from its peers, expire old messages.

The `core` package is all "our job" and is **forbidden from importing Ray**
(a test enforces it). `ray_runtime` is all "Ray's job" and is the *only* place
`import ray` appears. Phase 2 deletes `ray_runtime` and writes a replacement; the
`core` package doesn't change. That split is why these two layers are separate.

---

## 3. The relay actor (`relay_actor.py`, R3)

A **Ray actor** is just a Python object that lives in its own process somewhere
on the cluster, which you call remotely. `RelayActor` is one relay
(`relay-0` … `relay-4`). Inside, it holds a `MailboxStore` (the pure R2 logic)
and exposes its operations as methods Ray can call over the network.

**The decorator** (`@ray.remote(...)`) sets the rules Ray follows:

| Setting | Meaning in plain words |
|---|---|
| `resources={"relay_slot": 1}` | "This actor needs one relay slot." Only relay-node machines offer slots, and each offers one, so **no two relays share a machine** — important for surviving two failures. |
| `max_restarts=-1` | "If this relay dies, restart it forever" — on the same machine, or another with a free slot. |
| `max_task_retries=2` | "If a call arrives mid-restart, retry it." Safe because every operation is idempotent. |
| `concurrency_groups={...}` | Separate lanes so that thousands of parked readers (`waiters`) or repair traffic (`repair`) can't starve normal `store`/`fetch` (`client`). A "bulkhead". |

**The lifecycle.** When a relay starts (or restarts), Ray runs its constructor
with *nothing restored* — the memory is empty. So it comes up `SYNCING`, is
supposed to refill from its peers, and only then flips to `READY`. While
`SYNCING` it refuses client calls (raising `RelaySyncing`), so a reader never
sees an empty mailbox and wrongly thinks "no messages." **The actual refill
(pulling from peers) is task R6 and is left as a marked hook here** — this batch
has the state machine, not the refill loop.

**The methods**, grouped by lane:

- *client:* `store`, `fetch`, `ack`, `publish_prekeys`, `get_prekeys`.
- *waiters:* `wait_for_mail` — parks on an event and returns the moment a message
  arrives or the timeout passes, so clients don't poll.
- *repair:* `pull` (give a peer the records it should hold) and `handoff` (absorb
  records a stand-in is returning). These are one-liners over the store; the
  *logic that decides when to call them* is R5–R7 and is **not** here.

## 4. The bootstrap (`bootstrap.py`, R3)

A tiny script that creates `relay-0 … relay-4`. The key property is
**idempotent**: thanks to `get_if_exists=True`, running it a second time returns
the relays that already exist and creates nothing. So it's safe to run any time,
and relay machines can be started before or after it, in any order. The actors
are **detached**, meaning they keep running even after the script that made them
exits.

## 5. The gateway (`gateway.py`, R4)

The `core` code wants to say "store this on relay-3" without touching Ray. The
gateway is the translator. It implements three interfaces from `core/ports.py`:

- **`RelayDirectory`** — turns a relay *name* into something callable (via Ray's
  "find an actor by name"), caching handles and re-resolving after a failure.
- **`RelayGateway`** — the calls to *one* relay (`store`, `fetch`, …).
- **`RelayPeers`** — what a relay needs from the *other* relays (`pull`,
  `handoff`).

It does two things beyond just forwarding:

1. **A timeout on every call.** A relay that Ray is still placing on a machine
   can hang; the sender must be able to give up quickly and try the next relay.
2. **Turning Ray's errors into ours.** Callers only know our `RelayError` family,
   never a Ray exception (that would leak Ray across the boundary phase 2
   depends on). The mapping:
   - a timeout, or "actor temporarily unavailable" → **`RelayUnreachable`**. This
     means *the call might still have run* — which is only safe because every
     operation is idempotent, so a retry can't do harm.
     - a dead actor → **`RelayGone`**.
   - an error the relay itself raised (`RelaySyncing`, `MailboxFull`) → passed
     through unchanged, because it's already one of ours.

## 6. What is deliberately NOT in this batch

| Not here | Why |
|---|---|
| Three-copy send with stand-ins (R5) | **Graded mechanism** — the team writes it by hand, not via the assistant. Also gated on the spike. |
| Sync-on-restart loop (R6) | Essential repair, graded, gated on the spike. The actor has the `SYNCING→READY` hook; the pull loop is R6. |
| Hinted-handoff loop (R7) | Same. The `handoff`/`pull` methods exist; the loop that drives them is R7. |
| TTL/loop supervision + metrics (R8, R9) | The actor has a basic sweep loop; hardening and counters are R8/R9. |

These are marked `TODO(Rn)` in the code.

## 7. Verification status — read before trusting this

**None of it has run against Ray** — this machine has no Ray and runs Python 3.14
(Ray needs ≤ 3.13). What *was* checked here: the files parse, `core` still has
zero Ray imports, and `core` still imports cleanly.

The real test is `tests/test_ray_runtime.py`, which runs on **CI (Python 3.12 +
Ray)**. It's written to skip automatically where Ray isn't installed. Those tests
are **where a wrong assumption shows up first** — for example, whether an
`async def __init__` on an actor works, whether a handle survives a restart, and
which exception a caller actually sees when a relay is being replaced. A failure
there is a *finding for the spike*, not necessarily a bug in intent.

## 8. How to take this forward

1. Run the spike (gate G1) on a real machine to confirm the restart assumption.
   If it fails, revisit the actor decorator (`max_restarts`, `relay_slot`) and
   possibly fall back to the pinned-relay design.
2. Run `tests/test_ray_runtime.py` on CI; fix whatever the first real Ray run
   reveals.
3. Then build R5 → R6 → R7 **by hand** (they're graded), using the `pull`/
   `handoff`/`store` methods this batch exposes.
