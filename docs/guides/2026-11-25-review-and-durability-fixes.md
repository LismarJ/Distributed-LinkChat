# Review and durability fixes, 2026-11-25

**Written:** 2026-11-25 · **Six days to delivery (2026-12-01)**
**For:** the two developers on the team
**Status:** all fixes landed, `scripts/check.sh` green (481 passed, 20 skipped)

A full review of `src/linkchat` against the phase-1 delivery checklist, the fixes
it produced, and what is still unverified. Read section 2 first: three of the
defects were in the mechanisms the delivery is graded on, and one of them made a
claim in the runbook false.

---

## 1. What was checked, and what the suite says

| Check | Before | After |
|---|---|---|
| `ruff check` | clean | clean |
| `ruff format --check` | clean | clean |
| `mypy --strict` | clean (63 files) | clean |
| `pytest` | 469 passed, 20 skipped | **481 passed, 20 skipped** |
| Coverage (`src/linkchat`) | 90% | 90% |
| Ray integration tests | 7 passed | **10 passed** |

The 20 skips are the `test_history.py` crash-consistency cases. They need
`fork` and `SIGKILL`, which Windows has not; they run in CI and in the
containers, which are Linux. That is a real gap on the development machine and
not one in the delivered system.

**Coverage, honestly.** 90% overall, but the per-file numbers for
`ray_runtime/` understate reality: `relay_actor.py` reads 38% because the actor
runs in a Ray *worker process* that the main process's `coverage.py` cannot
trace. The 10 Ray integration tests do exercise those methods. The one real
coverage gap found was `core/clock.py` at 70%, now 100% — see section 3.

---

## 2. The three defects in the graded mechanisms

### 2.1 Repair could destroy a confirmed message under two failures

**This is the guarantee the delivery is graded on, and it was breakable.**

Two bugs that compose:

1. `MailboxStore.share_for` handed records to a resyncing peer **with the
   `hint` field intact.** A hint means "I am holding this on behalf of someone
   else, and I should hand it back". Every record `share_for` returns belongs
   to a mailbox the destination is a primary of, so it is the destination's own
   record and must carry no hint.
2. `repair.handoff_round` never skipped `primary == this_relay`, so a relay
   could `handoff` a record **to itself** and then `forget_record` it. The
   absorb and the forget hit the same store under the same `envelope_id`, so
   the forget deleted exactly what the absorb had just stored.

Together: a stand-in's hinted copy is pulled by the restarting primary, which
now thinks it is standing in for itself, hands the record to itself, and
forgets it. If the stand-in was the last holder — which is precisely the state
after two failures — **the record is gone.**

`handoff_round`'s own docstring already explained this hazard and cleared the
hint on the copy *it* sends. The resync path had the identical hazard and did
not. That is worth remembering: the reasoning was written down correctly and
only one of the two paths that needed it applied it.

**Fixed:** `share_for` strips the hint on the way out; `handoff_round` skips
itself and clears the self-hint instead (new `MailboxStore.clear_hint`, so the
loop stops rescanning the record every round).

**Pinned by** three tests in `tests/test_repair.py`, including
`test_two_failures_cannot_lose_a_confirmed_message_across_repair`, which drives
the demonstration's worst case in order: confirmed on three relays while one
primary is down, the two other primaries wiped, then the down primary restarts
and the cluster repairs. Before the fix it ended with `holders == []`.

> **The test caught itself passing for the wrong reason.** The first version ran
> one handoff round in one order and passed, because the stand-in happened to
> hand the record back before the primary destroyed it. It now runs several
> rounds over both relays, which is what the background loops really do. This is
> the same lesson the fuzz harness taught on Nov 24, learned a second time: a
> fault-injection test that has never failed is evidence of nothing.

### 2.2 `wait_for_mail` never woke, so the parked call was silently a poll

`wait_for_mail` is in scope and is named in the work plan as "one of the
clearest natural uses of a Ray async actor" — the last thing to be cut if the
plan slipped. **It did not work.** A reader parked on an empty mailbox was never
released by a later `store`; it waited out its full timeout and returned
`False`, after which the client fetched anyway and found the message. Correct
output, entirely wrong mechanism, and no test covered it.

**Cause, measured rather than assumed.** Ray gives each concurrency group its
own asyncio event loop in its own thread. A probe confirmed it: the `client` and
`waiters` groups report different thread ids *and* different loop ids. A reader
parks on the `waiters` loop; the `store` that should release it runs on the
`client` loop. `asyncio.Event.set()` is neither thread-safe nor cross-loop — it
queues the waiter's callback on the parked loop **without waking it**.

A first probe appeared to show cross-group waking working, which is the trap:
the target loop wakes on its own timers often enough that the queued callback
gets picked up sometimes. It is a race, not a feature.

**Fixed:** a waiter now records the loop it parked on, and `_wake` releases it
through `loop.call_soon_threadsafe(event.set)`, which wakes the loop as well as
queueing the work. Measured after the fix: the parked reader returns in 0.0 s
instead of never.

**Pinned by** three new Ray tests, including `test_a_store_wakes_a_parked_reader`.

### 2.3 Parked readers leaked, and could be told mail had arrived

The actor dropped its `asyncio.Event` only when the event was *set*. A reader
that timed out left the event unset, so its entry — and its mailbox id — stayed
in `_waiters` for the life of the relay, for every mailbox ever waited on. The
next reader of that mailbox then inherited a stale, already-set event and was
told `True` with no mail behind it.

**Fixed:** waiters are counted, and the last reader to leave removes the entry
whether it was woken or timed out. `_wake` removes the entry as it wakes it, so
a reader arriving afterwards never finds a set event.

---

## 3. The other five

| Where | Defect | Fix |
|---|---|---|
| `client/history.py` | The v2 migration adds `recv_mailbox` with `DEFAULT ''` and never backfills it, so a conversation carried over from schema v1 **silently never receives again**. `INSERT OR IGNORE` cannot repair it. | Derived and written back on first read. It cannot be done in the migration: the value depends on this client's own id, and the identity is loaded *after* `_migrate` (a v1 file has no identity table at all). |
| `repair.py` / `relay_actor.py` | `resync` discarded `absorb`'s counts, and `absorb` cannot distinguish "refused because full" from "already had it". A relay whose store was full could therefore go **READY while short of records** — the one state `_resync`'s own docstring rules out, because such a relay looks healthy and is not counted as one of the two tolerated failures. | `resync` now returns a `SyncOutcome` (peers answered, records refused) with a `complete` property; `_resync` retries an incomplete sync instead of serving. Unreachable at the delivery's traffic (quotas are 1,000/mailbox and 50,000 total) but it closes a hole in the argument. |
| `relay_actor.py` | `status` sat in the 16-slot `repair` concurrency group while its section comment claimed the default group. `repair` is exactly what saturates while relays resync and hand records back, and the overlay paints a relay that misses its 2 s timeout as `unreachable` — so a busy but healthy relay could show as dead **during the two-failure demonstration**. | Its own `report` group. |
| `core/envelope.py` | `MAX_PLAINTEXT`'s docstring said 3966; the value is 3902 (stale from when `PAYLOAD_SIZE` was 3968). Every consumer uses the symbol, so only the prose was wrong. | Corrected. |
| `static/app.js` | Double-clicking the id chip captured "copied" as the original text, hiding the user's own id until reload. | Restore from `state.me`, and clear the pending timer. |
| `core/clock.py` | 70% covered; `SystemClock` was only ever exercised inside Ray workers. It backs every relay and the client app. | `tests/test_clock.py`, now 100%. Pins the distinction the design rests on: wall-clock for `expires_at`, monotonic for durations. |

**A latent fragility in the Ray test file, fixed while there.**
`test_dead_relay_becomes_a_relay_error` kills relay-4 with `no_restart=True`
under a *module-scoped* fixture, so every test added after it ran against a
cluster permanently short one relay and passed or failed on where its mailbox
happened to hash. It is now last in the file, with a comment saying so.

---

## 4. Checked and found correct

Worth recording so nobody re-checks them:

- `x3dh` does verify `signed_pre_key_sig` inside `get_shared_secret_active`, so
  `decode_bundle` not verifying it separately is fine.
- `doubleratchet.decrypt_message` works on a clone and leaves state untouched on
  failure, so the multi-session trial loop in `sessions._decrypt_without_header`
  is safe.
- `pad_payload` / `unpad_payload` arithmetic is exact.
- `send_to_three`'s slot fall-through never reuses or double-counts a relay.
- `core` is still Ray-free, and the guard still catches evasions.

---

## 5. Still unverified, and what it would take

Honest list. Nothing here is fixed by code.

*Items 1 to 3 of this list were closed later the same day — see section 7.*

1. **`client-mid-send` does not do what its name says.** It waits for the send
   request to commit before killing the client, so it tests a durably queued send
   under a simultaneous failure, not a crash *during* the relay call. The outbox
   and history tests cover retry-after-restart; this scenario does not complete
   the checklist's mid-call crash demonstration by itself.
2. **The credential on every relay call is still not checked** (ADR-0007 open).
   A relay trusts any caller who knows a mailbox id. This is why
   `ports.CredentialRejected` is never raised. Deliberately deferred and already
   written down in the runbook; left alone here rather than designing
   authentication six days out.
3. **`core/models.py` is dead.** Nothing imports it. It is the compatibility
   shim from the 2026-11-15 interface reconciliation, and its `StoreReceipt` has
   been superseded by `replication.SendResult`, which is what the outbox
   actually uses. Left in place deliberately: it exists to keep a second
   developer's imports resolving and that branch is not visible from here.
   **Decide together whether to delete it** — two names for the send result is
   the kind of thing that costs a minute at the defense.

---

## 6. What to do next

1. Decide on `core/models.py` (section 5.3).
2. Rehearse three times, per G5, without editing `pyproject.toml`.
3. Consider whether the ~31 s container-failover time should be tuned down by
   shortening Ray's node health-check interval, or simply narrated. Narrating it
   is the honest option and costs nothing; tuning it is a change to cluster
   configuration days before a delivery.

---

## 7. The container run — gate G3, closed

Everything in this section happened on 2026-11-25, after the fixes above, on a
real cluster: one head, seven relay nodes, two clients, every container started
by its own `docker run` with no Compose.

### 7.1 The cluster comes up, and the scenarios pass

`scripts/up.sh` brought the whole cluster up end to end for the first time, and
then **every fault-injection scenario passed against real containers**, which is
gate **G3** and was the largest remaining delivery risk:

```
PASS two-nodes          PASS client-mid-send
PASS two-processes      PASS recipient-offline      (new, see 7.3)
PASS one-of-each        PASS wipe-all-relays        (new, see 7.3)
PASS send-while-down
```

Each one sends a uniquely tagged message, injures the cluster, and then asserts
the message arrived **exactly once**, that the sender recorded `sent` (three
relays confirmed), and that the recipient's earlier history is unchanged and in
order. The fixes in section 2 were confirmed present in the deployed image
before the run, not assumed.

### 7.2 Two measurements the demonstration needs

| What dies | Time until all five relays are READY again |
|---|---|
| A relay **container** | **~31 s** — 30682, 31102, 31094 ms over three runs |
| A relay **worker process** | **~1.3 s** — 1353, 1256 ms over two runs |

The gap between them is Ray's failure detection: a dead *worker* is noticed
almost at once, a dead *node* only through the health check. **Thirty seconds is
a long silence in a demonstration**, so beat 3 of the runbook now says to start
explaining the moment the kill lands instead of waiting for green.

**And the half of spike item 1 that needed containers is now answered.** Killing
a relay container and *not* restarting it re-places its actor onto a **spare
node** — observed three times, for example relay-0 killed and its actor
appearing on relay-4. [ADR-0011](../decisions/0011-relays-as-ray-supervised-actors.md)
rests on exactly this, and it holds. Still unverified: whether an actor stays
*pending* rather than failing when no `relay_slot` is free anywhere.

Also measured: the image is **498,384,886 bytes (475 MiB)**, which is spike item
7. And one operational finding worth more than it looks — the image's cached
dependency layer is keyed on `pyproject.toml`, so **editing that file forces a
full re-download of Ray**, about 13 minutes on a slow connection. Nothing else
in the repository does that. Do not touch it on the demonstration day.

### 7.3 Two delivery-checklist items had no scenario

Both are now scripted, repeatable and self-asserting:

- **`recipient-offline`** — checklist item 2. Every existing scenario had the
  recipient *online and polling*, so the one requirement the whole relay tier
  exists for was never actually exercised in containers. It now kills the
  recipient's container, sends while it is down, brings it back, and asserts the
  message is there exactly once with the sender reading `sent`.
- **`wipe-all-relays`** — checklist item 6. This one has a different shape from
  the others and so has its own assertion path: wiping *all five* relays is
  outside the fault model, so a message still sitting on them would legitimately
  be lost and asserting otherwise would be wrong. What it demonstrates is the
  stronger claim — history does not live on the relays at all. It compares both
  clients' histories before and after as whole strings, which catches loss,
  duplication and reordering together. Passed in about 43 seconds with 38
  messages on each side.

  The duplication half is not redundant: wiping the relays destroys their
  **tombstones** too, so anything that came back would be re-offered to the
  client, and the client's persisted seen-set is the only thing refusing it. So
  this is a test of duplicate suppression as much as of durability.

Both beats already existed in the runbook as *manual* steps. What was missing
was a command that checks its own outcome, which is what makes them usable in a
rehearsal rather than something a person has to eyeball.

### 7.4 A scenario that could not injure the cluster used to report PASS

Found while adding the two scenarios above, and it is the most important thing
in this section, because it is about whether the evidence can be trusted at all.

`kill-nodes.sh` **refuses to kill a container that is already down, and exits
1.** That status was ignored by `injure_two_nodes`, `injure_two_processes` and
`injure_one_of_each`. It happened for real during this session: a scenario ran
against a cluster it had failed to injure, printing

```
  - killing containers together: linkchat-relay-0 linkchat-relay-1
linkchat-relay-0 is not running
```

and then carrying on as though two relays were down.

For the send-and-assert scenarios that produced a confusing failure 90 seconds
later, which is survivable. For **`wipe-all-relays` it would have been a false
PASS**, and quietly: its assertion is that both histories are *unchanged*, so
wiping nothing satisfies it perfectly. The scenario added to prove relays hold
no history would have proved it by not touching the relays.

**Fixed:** every `injure_*` function now returns non-zero when it did not
actually injure the cluster — including the case where `pkill` matched no
process, and the case where fewer than two relays carry actors — and every
caller turns that into `FAIL … nothing was tested`. Verified both halves
separately: `kill-nodes.sh` does exit 1 for a stopped container, and both new
call-site constructs convert that into a failed scenario rather than a pass.

The general lesson is the one this project keeps relearning, written down a
third time: **an assertion that something did not change is only as good as the
proof that something was done to it.** The fuzz harness taught it on Nov 24, the
repair regression test taught it again in section 2.1, and here it is in the
shell.

> **A note on how this was nearly mis-diagnosed.** The failure first appeared in
> a log that also contained a contradictory `PASS` for the same scenario, which
> looked like a reporting bug in the summary. It was not: two suites had been
> launched concurrently against one cluster and one log file, because a
> background launch had backgrounded an entire `&&` chain rather than its last
> command, so `scripts/up.sh` never ran and a second suite kept running
> unnoticed. **Run one suite at a time, to its own log file, and check the log
> has exactly one summary block before believing it.** The clean re-run is
> 7 PASS, zero `FAIL` lines, exit 0.
