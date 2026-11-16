# Guide: the fake transport and the R5–R7 test-first setup

**Audience:** a developer new to this repository — most of all whoever is about
to implement R5, R6 and R7. No prior context assumed.

---

## 1. What this batch is, and why it's shaped this way

R5–R7 are the **graded** mechanisms — three-copy send, sync-on-restart, hinted
handoff. The work plan says to **write them by hand**, because at the delivery you
have to *explain why two failures can't lose a confirmed message*, and reading an
implementation isn't the same as having built it.

So this batch does **not** implement them. Instead it gives you a **test-first
setup**:

1. **The fake transport (C2)** — a controllable, in-process stand-in for a Ray
   cluster of relays, so you can test failure scenarios without Docker or Ray.
2. **Stub contracts** for R5–R7 (`send_to_three`, `resync`, `handoff_round`) that
   currently just `raise NotImplementedError`.
3. **Failing ("red") tests** that pin exactly what correct behaviour looks like.

Your job: **make the red tests green** by writing the bodies of the three stubs.
That's the whole workflow — the tests tell you when you're done and catch you
when you're wrong.

## 2. How to run the tests

The dev machine has no virtualenv, so set one up once (the plan's command, but
these particular tests need neither Ray nor msgpack, so plain Python works too):

```bash
uv venv --python 3.12 && uv sync --extra dev   # the real project way
# then:
pytest tests/test_fake_transport.py     # the harness itself — PASSES (7)
pytest tests/test_three_copy_send.py    # R5 — RED until you implement it
pytest tests/test_repair.py             # R6, R7 — RED until you implement them
```

These five test/transport files were verified on the author's Windows machine in
a throwaway Python-3.14 venv: `test_fake_transport.py` passes 7/7, and the R5–R7
suites fail 8/8 on `NotImplementedError`, which is the correct starting point.

## 3. The fake transport (`tests/fakes/transport.py`, C2)

Think of `FakeCluster` as "five relays living in your test process." The clever
part: each relay is backed by the **real** `MailboxStore` (the production R2
code), so only the *network* around it is fake. That means a test exercises the
real storage logic, just without real processes.

You script failures by mutating the cluster:

| Call | What it simulates |
|---|---|
| `cluster.kill(name)` | the relay is unreachable → calls raise `RelayUnreachable` |
| `cluster.revive(name)` | it answers again |
| `cluster.restart(name)` | Ray restarted the actor: **empty store, `SYNCING`** |
| `cluster.faults.drop_next[name] = 2` | the next 2 calls to it fail, then recover |
| `cluster.faults.latency[name] = 0.5` | calls take time (advances the manual clock) |
| `cluster.faults.duplicate.add(name)` | it applies every write twice (tests idempotence) |

You get the ports from it: `cluster.directory()` (the client's view, for R5) and
`cluster.peers(name)` (a relay's view of the others, for R6/R7). Time is a
`ManualClock` that only moves when you (or a delay) move it, so expiry and
timeouts are deterministic.

## 4. What you have to implement

### R5 — `core/replication.py: send_to_three(record, directory, *, clock)`
Store the record on **three distinct relays**. Steps (design §5):
1. The three primaries are `ring.primaries(record.mailbox_id)`.
2. Call `store` on all three at once.
3. For each that fails, fall through `ring.preference_list(...)` to the next
   relay, storing a copy whose `hint` names the primary it's covering for.
4. Return a `SendResult`; it's `.sent` once three distinct relays confirmed.

The red tests (`test_three_copy_send.py`) check: all-up confirms the three
primaries; two primaries down still sends via stand-ins (and those copies carry a
`hint`); three down stays unsent; a resend stores no duplicates.

### R6 — `core/repair.py: resync(this_relay, store, peers, *, clock)`
A restarted relay comes up empty. Pull from every reachable peer the records and
tombstones for the mailboxes this relay is a primary of (`peers.pull`), and
`store.absorb` them. Skip peers that are down.

The red tests (`test_repair.py`) check: a restarted relay regains the record from
its peers; and it still works when one peer is down (another holder answers).

### R7 — `core/repair.py: handoff_round(this_relay, store, peers, *, clock)`
A stand-in holding records hinted for a primary tries to hand them back. For each
primary in `store.hinted_primaries()`, `peers.handoff(...)` the hinted records
and, **only on success**, `store.forget_record(...)` each (a move, not an
acknowledgement — so no tombstone). Return how many were handed off.

The red tests check: when the primary is up, the record moves to it and the
stand-in drops its copy; when the primary is down, the stand-in keeps the copy
and nothing is handed off.

## 5. Why the tests don't prescribe your algorithm

They assert **guarantees**, not steps: "three distinct relays hold it," "a
non-primary holder carries a hint," "a down primary means the copy stays." How
you choose stand-ins, order the calls, or structure the code is yours. That's
deliberate — the mechanism is what you defend, so it should be yours.

## 6. A note on ownership

The fake transport is nominally task **C2 (client track)**, but it's shared
infrastructure the relay-track tests (R5–R7) need, so it was built here to unblock
them. The randomized fuzzing (S5) will build on the same harness.
