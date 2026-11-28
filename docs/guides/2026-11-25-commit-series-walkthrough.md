# Walking through the 2026-11-25 durability-review commit series

**Written:** 2026-11-25 · **For:** anyone new to the codebase reading this
branch for the first time — a teammate picking it up, a reviewer, or future
you in three weeks
**Branch:** `fix/phase1-durability-review-2026-11-25` (8 commits on top of
`main`) · **Companion reading:**
[`2026-11-25-review-and-durability-fixes.md`](2026-11-25-review-and-durability-fixes.md)
has the full technical writeup; this document is the plain-language walk
through the commits themselves, in the order they land.

Nothing here is a new feature. Every commit either fixes a bug that was
already shipping, adds a test that was missing, or removes something that had
stopped being used. `scripts/check.sh` is green at every commit in this
series, not only at the tip — each one is a complete, working step.

---

## How to read this

Each section below covers one commit. For each one: **what broke** (in plain
terms, no jargon assumed), **why it broke**, **what the fix does**, and **how
you'd notice** if it broke again. If you only read one section, make it the
first two — they're the ones that would have failed the actual delivery.

---

## 1. `fix(ray_runtime)`: the "wait for mail" feature was quietly broken

**Commit:** `22685a3`
**Files:** [`relay_actor.py`](../../src/linkchat/ray_runtime/relay_actor.py),
[`test_ray_runtime.py`](../../tests/test_ray_runtime.py)

### What this part of the system does

A relay is a small server that temporarily holds messages until the recipient
picks them up. Instead of the client asking "do I have mail yet?" over and
over (polling), it can call `wait_for_mail` and the relay call just... waits,
until either mail shows up or a timeout passes. This is nicer for everyone: no
wasted requests, and the client finds out about new mail immediately.

### What was actually happening

It never worked. A client would call `wait_for_mail`, and no matter how fast
a message arrived, the call always sat there until its own timeout expired,
then the client would go fetch the message the normal way. The *feature*
looked fine from the outside — mail did arrive eventually — but the
"instant wake-up" part, the whole point of building it, silently did nothing.

### Why

This took real detective work to find (see the companion doc for how it was
tracked down), and the short version is: the library this project is built
on — Ray — runs each *group* of relay operations on its own separate thread,
each with its own event loop (an event loop is the thing that lets one thread
juggle many `async` tasks). A client parked in the "waiters" group was
sleeping on a *different* loop than the "client" group where a new message
gets stored. The one line of code meant to wake the sleeper
(`event.set()`) only works reliably when the waker and the sleeper share a
loop. Across two different loops, it's not safe and doesn't actually wake
anything up.

### The fix

Each waiting client now remembers *which* event loop it's sleeping on, and
when a new message arrives, the code wakes it using a special cross-thread-safe
call (`loop.call_soon_threadsafe`) instead of the plain one. That's the entire
fix, in one method (`_wake`) — the kind of bug where the solution is three
lines once you know exactly what's wrong, and weeks without a test if you
don't.

A second, smaller leak got fixed at the same time: if a client's wait timed
out (nobody sent it mail), the bookkeeping entry for that wait never got
cleaned up. Over the life of a relay this would slowly pile up unused entries,
one per mailbox someone had ever waited on.

### How you'd notice if this regressed

There's now a test
(`test_a_store_wakes_a_parked_reader`) that parks a reader, then stores a
message, and checks the reader wakes up in well under a second. Before the
fix, that test would hang until its own 10-second timeout and fail.

---

## 2. `fix(ray_runtime)` (same commit): one relay's status could get buried

**Still commit `22685a3`**

### What this part of the system does

Each relay reports its own health (`status`) so that the client's dashboard
can show which relays are up, down, or recovering. The dashboard treats a
status check that takes more than 2 seconds as "this relay must be
unreachable."

### What was actually happening

The status check shared a lane (Ray calls these "concurrency groups" — think
of them as separate checkout lines at a store) with the much busier work of
repairing the cluster after a failure. When a relay was busy catching up after
a crash — exactly the moment you most want to *see* that it's alive and
working — its status check could get stuck behind that repair traffic long
enough to blow past the 2-second cutoff. The dashboard would then show a
perfectly healthy, hard-working relay as "dead."

### The fix

`status` now has its own dedicated lane, separate from repair traffic. It can
never get stuck behind someone else's work.

---

## 3. `fix(repair)`: a confirmed message could be lost after two failures

**Commit:** `258d1f4`
**Files:** [`mailbox.py`](../../src/linkchat/core/mailbox.py),
[`repair.py`](../../src/linkchat/core/repair.py),
[`relay_actor.py`](../../src/linkchat/ray_runtime/relay_actor.py),
[`test_repair.py`](../../tests/test_repair.py)

**Read this one carefully — it's the most important commit in the series.**
The system's entire reason for existing is the promise that losing any two
relays at once never loses a message. This bug could break that promise.

### What this part of the system does

Every message is stored on three relays at once (so two can fail and one
still has it). Sometimes a relay is temporarily unreachable when a message
arrives; a *stand-in* relay holds the message on its behalf, with a little
note attached — a "hint" — saying "this really belongs to relay X, hand it
back when X is available again." When a relay comes back online, it asks its
peers to resync it and catches it up.

### What was actually happening

When a relay resynced, the records it pulled back from a stand-in **kept that
stand-in's hint attached** — still saying "this belongs to someone else,"
even though the someone-else *was the relay that just received it*. Later, a
separate piece of code (the "hand records back to whoever they belong to"
loop) would see that hint, think "oh, this needs to go to relay X," and —
because the relay already *was* relay X — hand the record to itself and then
immediately delete its own copy, thinking it had successfully passed it
along.

Put together: a message that survived being bounced between relays during a
failure could end up getting deleted by the very relay that was supposed to
be its new permanent home. If that stand-in copy had been the *last* copy
left (which is exactly the situation after two simultaneous failures), the
message would be gone. Not delayed — gone.

A new test proves this used to happen:
`test_two_failures_cannot_lose_a_confirmed_message_across_repair` recreates
that exact sequence (confirm a message on three relays, knock out two of them,
let the third resync) and checks a copy of the message survives. Before this
fix, it didn't; the test ended with zero relays holding the message.

### The fix

Two small, related changes:

1. When a relay picks up records from a resync, the hint gets stripped off —
   because if it's a primary's own resync, the record is now unambiguously
   *its own*, not a loan from somewhere else.
2. The "hand records back" loop now explicitly refuses to hand a record to
   *itself*. If it notices a hint pointing at its own name, it just clears the
   hint and keeps the record, instead of performing a pointless (and
   destructive) hand-off to itself.

### A smaller related fix, same commit

A relay catching up after a restart is supposed to refuse to serve clients
until it's *fully* caught up — otherwise it might tell a client "no mail
here" when really it just hasn't fetched the mail yet. The code that tracked
"did I fully catch up?" couldn't actually tell the difference between "my
peers had nothing for me" (fine, totally caught up) and "my peers had records
but my storage was full and I had to refuse some of them" (not fine — it's
lying about being caught up). This fix makes that distinction explicit, so a
full-storage relay keeps retrying instead of pretending it's ready.

### How you'd notice if this regressed

Run `tests/test_repair.py` — three tests specifically target this scenario,
including the full two-failure simulation above.

---

## 4. `fix(client)`: an old conversation could silently stop receiving

**Commit:** `475076e`
**Files:** [`history.py`](../../src/linkchat/core/client/history.py),
[`test_history.py`](../../tests/test_history.py)

### What this part of the system does

Each client keeps a local database (SQLite) of its conversations. The
database format has changed over time (schema "versions"), and old
databases get automatically upgraded ("migrated") when the client opens them.

### What was actually happening

An early version of the database format only tracked one mailbox per
conversation. A later version split that into two — one for sending, one for
receiving — because that's a cleaner design. The upgrade code for old
databases correctly carried over the *sending* mailbox, but left the new
*receiving* mailbox column blank. Nothing ever filled it in afterward. A user
whose conversation database predated this change would never notice anything
was wrong in the UI — they just would never, ever receive a new message in
that conversation again, with no error, no warning.

### Why it couldn't just be fixed in the upgrade step

The correct receiving-mailbox value can be *calculated* (it's a function of
the client's own identity and who they're talking to), but at the exact
moment the database upgrade runs, the client doesn't know its own identity
yet — that gets loaded afterward. So the upgrade step genuinely cannot
compute the right answer; something later has to do it.

### The fix

The code that *reads* the receiving mailbox now checks: is this blank? If so,
calculate the right value on the spot, save it back to the database so it
never has to be recalculated, and return it. The first time an old
conversation is touched after this fix ships, it quietly heals itself.

### How you'd notice if this regressed

`test_a_version_1_file_migrates_to_two_mailboxes` opens a deliberately
old-format database and checks the receiving mailbox resolves correctly. A
second test, `test_a_derived_receive_mailbox_is_written_back`, checks the
calculated value actually gets saved, not just computed every time.

---

## 5. `fix(client)`: clicking "copy my ID" twice could hide your own ID

**Commit:** `d082b3f`
**File:** `src/linkchat/client_app/static/app.js`, as it was on 2026-11-25.
That file no longer exists: [ADR-0017](../decisions/0017-react-ui-with-a-committed-build.md)
replaced the hand-written UI with a React build the next day, and the same
fix lives in the React component. The commit below is still the record of
the defect and the reasoning.

### What was actually happening

The little chip in the UI that shows your user ID can be clicked to copy it.
Clicking it briefly changes the label to "copied," then changes it back after
a second. The bug: it changed it back to *whatever the label said a moment
ago* — and if you clicked twice quickly, the second click would "remember"
the label as "copied" (from the first click) and restore *that* instead of
your actual ID. Your own ID would then stay hidden behind the word "copied"
until you reloaded the page. Not dangerous, just confusing — and exactly the
kind of thing a grader clicks twice during a demo.

### The fix

Restore the label from the one source of truth that never changes —
`state.me`, the actual stored user ID — instead of from whatever text happens
to be on screen. Also cancels any pending "restore" timer before starting a
new one, so rapid clicks can't race each other.

---

## 6. `refactor`: removed one unused method, fixed one wrong number in a comment

**Commit:** `7634397`
**Files:** [`transport.py`](../../tests/fakes/transport.py),
[`envelope.py`](../../src/linkchat/core/envelope.py)

Nothing behavioral here — pure tidy-up, found by running an automated
dead-code scanner (`vulture`) over the codebase and checking each hit by hand.

- A test-only helper method (`FaultController.is_down`) had no caller anywhere
  and was deleted. (Everything else the scanner flagged turned out to be used
  — Ray calls some methods by name rather than through normal Python calls,
  so a naive "is this ever called" check gets those wrong; each one was
  checked individually before deciding to leave it.)
- A docstring said a size limit was "3966 bytes." The actual number, computed
  from the real constants, is 3902. Nothing in the code used the wrong
  number — only the comment was stale — but a wrong comment is worse than no
  comment, so it's corrected.

---

## 7. `test(core)`: gave the system clock its own direct test

**Commit:** `692d8c9`
**File:** [`test_clock.py`](../../tests/test_clock.py) *(new file)*

### Why this needed a test

`SystemClock` is the one piece of code every relay and every client actually
asks "what time is it?" through. It had never been tested directly — it was
only ever exercised as a side effect of other tests that happened to run
inside a Ray worker process, which meant code-coverage tooling couldn't even
see it being used. Coverage on this one small file read 70%, which was really
"we're trusting this by accident."

### What the new test actually checks

The design here depends on a subtle distinction: `now()` returns the *real*
wall-clock time (needed because an expiration time is set by one computer and
checked by another — they need to agree on what time it actually is), while
`monotonic()` returns a *different* kind of clock meant only for measuring
"how much time has passed," which must never be confused with wall-clock
time. The new tests pin down that the two really are different, that
`monotonic()` never runs backward, and that actually calling `sleep()` really
does wait. (One test deliberately uses a generous time margin rather than a
tight one — Windows' internal timer isn't perfectly precise, and a test that
times out of pure measurement noise, not a real bug, is worse than no test.)

---

## 8. `test(scripts)`: filled in two missing real-world test scenarios, and caught a bug doing it

**Commit:** `f93e60d`
**File:** [`scenario.sh`](../../scripts/scenario.sh)

### What this part of the system does

`scripts/scenario.sh` runs the project's fault-injection drills against real
Docker containers — actually killing relay containers and processes and
checking the system survives, rather than simulating it in a unit test. This
is the closest thing to a dress rehearsal for the live demonstration.

### Two scenarios the suite didn't have

- **`recipient-offline`:** every *existing* scenario had the message
  recipient's container running the whole time. Nobody had actually tested
  the headline feature — "send a message to someone whose client isn't even
  running, and have it waiting for them when they come back" — against real
  containers. This scenario does exactly that.
- **`wipe-all-relays`:** kills *every single relay at once* and checks both
  people's conversation histories are completely unaffected afterward. This
  is the strongest possible demonstration that conversation history doesn't
  live on the relays at all — it lives entirely on each person's own device.

### The bug found while writing these

While building `wipe-all-relays`, a real bug turned up in the test
*infrastructure* itself: the helper script that actually kills containers
(`kill-nodes.sh`) correctly refuses to "kill" a container that's already
stopped — but the scenario code wasn't checking whether that refusal
happened. If a scenario tried to break the cluster and *failed* to break
anything, it would just carry on as if nothing was wrong.

For most scenarios that would eventually cause a confusing failure later on.
But for `wipe-all-relays` specifically, it would have been *silently wrong in
the dangerous direction*: that scenario's whole check is "did the history stay
the same?" — and if nothing actually got killed, the history obviously stays
the same, and the script would print `PASS` for a test that tested nothing at
all. A green checkmark that means "we didn't even try" is worse than a red
one.

### The fix

Every function that's supposed to break something now explicitly checks that
it succeeded, and if it didn't, the whole scenario is reported as a failure
with a clear message ("could not injure the cluster; nothing was tested")
instead of quietly passing.

### Proof this all actually works

Two separate clean runs of `scripts/scenario.sh all` against real Docker
containers — not simulated, the actual thing — came back **7 for 7**, with
the measured recovery times written into the companion document.

---

## 9. `docs(delivery)`: wrote all of the above down properly

**Commit:** `b72659b`

Added a detailed technical writeup
([`2026-11-25-review-and-durability-fixes.md`](2026-11-25-review-and-durability-fixes.md))
covering everything above in more depth, with the measured numbers (how long
a relay takes to recover, how big the Docker image is, etc.), and updated the
project's planning and research notes so the record stays current for anyone
reading them later. No code changed in this commit — only documentation.

---

## If you're reviewing this branch, here's the fast path

1. Read commit 3 (`258d1f4`) first — it's the one that matters most for the
   grade.
2. Read commits 1–2 (`22685a3`) second — subtle, instructive, and the kind of
   bug worth remembering the shape of.
3. The rest are smaller and mostly speak for themselves from their commit
   messages.
4. Run `scripts/check.sh` — it's green at the tip, and was green at every
   commit along the way.
