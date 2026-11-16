# Guide: publishing phase 1 and reconciling the core interface (2026-11-15)

**Audience:** a developer new to this repository who wants to understand what
landed in the `feat/phase-1-core-and-scaffolding` batch and *why*. No prior
context assumed.

---

## 1. The situation this batch resolves

Two people were building phase 1 in parallel:

- **Dev A** wrote a large amount of code locally (the message store, the relay
  placement logic, tests, the Docker setup, the scripts, a Ray experiment) but
  had only ever pushed **documentation**. The code sat uncommitted.
- **Dev B** wrote a smaller "contracts" version of the same `core` interface and
  **merged it to `main`** while Dev A's code was still unpublished.

So when Dev A went to publish, the two versions of `core` **overlapped and
disagreed**. This batch publishes Dev A's work *reconciled* with Dev B's, as a
pull request, so nothing on `main` is silently overwritten.

If you remember one sentence: **this batch makes one coherent `core` out of two
drafts, and adds everything else that was only on Dev A's machine.**

---

## 2. Background you need (30 seconds)

LinkChat is a decentralized secure messenger. Messages must reach people whose
phone is **off**, and **no server may become a permanent store** of anyone's
conversations. Those two goals fight each other, and the design is the answer.

- **Phase 1** (due 2026-12-01) runs on **Ray** (a Python framework for running
  actors across machines).
- **Phase 2** (due 2027-01-01) removes Ray entirely.

To make phase 2 affordable, **all the real logic lives in `linkchat.core`, and
that package is forbidden from importing Ray** (decision ADR-0013). Phase 2 then
replaces one thin layer instead of rewriting everything. A test enforces the
rule so nobody has to remember it.

---

## 3. The two things that genuinely conflicted

Everything Dev A wrote was *new* except two areas, which both drafts had:

1. **The project layout.** Dev B used a `src/` layout (`src/linkchat/...`).
   Dev A used a flat layout (`linkchat/...`). → **We adopted `src/`.**
2. **The core interface** (`ports.py` + the data models). Same ideas, different
   shapes. → **We kept Dev A's fuller version as canonical and kept Dev B's
   names importable.** Details below.

Everything else — the placement ring, the mailbox store, tests, Docker, scripts,
the spike, CI — existed only on Dev A's side, so there was nothing to conflict
with. It was just added.

---

## 4. What the interface reconciliation actually did

The "interface" is the set of promises that the rest of the system is written
against: what a relay can be asked to do, and what a stored message looks like.

Dev A's version was richer and already had working code built on it, so it
became the **canonical superset**. Dev B's version is still importable so their
code doesn't break. Concretely:

- **`ports.py`** (the boundary) is Dev A's: it has named error types, a
  `Record` (one stored message) and a `Tombstone` (a marker that a message was
  deleted so it can't come back), a directory to find relays, prekey methods for
  encryption setup, and a status report type.
- **`models.py`** (new) is a small "front door" that re-exports those types
  under the names Dev B used, so `from linkchat.core.models import Envelope,
  StoreReceipt` keeps working:
  - **`Envelope` is an alias of `Record`.** The stored unit is opaque bytes
    carried inside a `Record`; `Envelope` is just the other draft's name for it.
  - **`StoreReceipt` moved up a layer.** In Dev B's draft, storing returned a
    receipt. But storing happens *per relay*, and a single relay either stored
    the message or raised an error — it has nothing to report back. A *receipt*
    only makes sense once you've stored to several relays and counted how many
    confirmed. So `StoreReceipt` now belongs to the (not-yet-written)
    client-send layer that does that counting.
- **Folded in from Dev B:** `Clock.monotonic()` (a steadily increasing timer for
  measuring durations) and a reserved `PROTOCOL_VERSION` constant.

Three small decisions are flagged in the PR for Dev B to confirm (the `Envelope`
field mapping, deferring the actual version byte, and whether `get_live_peers`
should be an explicit method). Full reasoning is in
`docs/superpowers/specs/2026-11-15-core-interface-reconciliation-design.md`.

---

## 5. The commits, in order, and why each exists

| # | Commit | What it adds | Why it matters |
|---|--------|--------------|----------------|
| 1 | `feat(core): add the src package, reconciled interface, envelope and models` | The `src/linkchat` package tree, the reconciled `ports.py`/`models.py`, the envelope format, payload, ids, constants, clock, and their tests | This is the foundation both work tracks build on. It's the one that reconciles the two interfaces. |
| 2 | `feat(core): add rendezvous-hashing relay placement (R1)` | `ring.py` — decides *which* relays store a given mailbox | Messages are copied to 3 of 5 relays. Something has to pick which 3, fairly and predictably. See §6. |
| 3 | `feat(core): add the mailbox store with tombstones and quotas (R2)` | `mailbox.py` — one relay's in-memory store of messages | This is where a message physically lives on a relay, with the rules that keep deleted messages from reappearing. See §6. |
| 4 | `build: add tooling, CI and the Ray-free guard (S2)` | ignore files, GitHub Actions CI, `scripts/check.sh`, the Ray-free test | Automation: lint, type-check and test on every push, and fail the build if `core` ever imports Ray. |
| 5 | `build(docker): add the image and per-container cluster scripts (S3)` | `Dockerfile`, one shell script per cluster operation | How you actually run a cluster. The course forbids Docker Compose, so each container is started by its own script. |
| 6 | `feat(spike): add the Ray-behaviour spike and runner (S4)` | `spike/` — a throwaway experiment | The design *assumes* things about how Ray restarts a dead actor. The spike checks whether those assumptions are true before we build more on them. |
| 7 | `docs: rewrite the README, log progress, and record the reconciliation` | README, progress log, the reconciliation spec | Keeps the written record honest about what's done vs. verified. |

(Nine earlier documentation-only commits also ride along; they conflict with
nothing.)

---

## 6. The two clever bits, explained plainly

**Rendezvous hashing (commit 2 / `ring.py`).** We need to spread mailboxes evenly
across 5 relays, and when a relay dies, move as few mailboxes as possible. The
trick: for a given mailbox, compute a number from `(mailbox, relay)` for every
relay, and pick the relays with the highest numbers. It needs no tuning and,
when a relay leaves, only the mailboxes *it* was responsible for move. We tried
the more common "consistent hashing with virtual nodes" first; it balanced worse
(15.8%–24.8% per relay vs. an ideal 20%, against rendezvous's 19.6%–20.3%).

**Tombstones (commit 3 / `mailbox.py`).** When you read a message and confirm it,
the relay deletes it — but a *copy* of that message can still arrive later from
another relay that hadn't heard about the deletion. Without protection, the relay
would happily store the copy and show you the message again. So instead of just
deleting, the relay leaves a **tombstone**: a small marker saying "this message
id is done." A late copy hits the tombstone and is refused. Tombstones expire at
the same time the message would have, so they don't pile up forever.

---

## 7. What is NOT verified (important)

The machine this was written on has **no virtual environment**, and Ray can't
even install on its Python (3.14). So:

- **`ruff`, `mypy`, `pytest` have never run.** CI has never run.
- **Nothing has run against Ray or Docker.**

What *was* checked, by hand, against the system Python: the core modules import
correctly under the new `src/` layout, every frozen interface name is present,
`Clock.monotonic` works, the `Envelope`/`Record` alias holds, and the Ray-free
guard finds zero Ray imports in the 10 core files.

**Treat a failure as a bug in this code until you've ruled it out**, not as a bug
in Ray or Docker.

---

## 8. How to pick this up

1. Open the PR (link in the push output) and let Dev B review the interface
   reconciliation — that's the part that supersedes their merged work.
2. Stand up the environment and run the checks for the first time:
   ```bash
   uv venv --python 3.12      # NOT 3.14: Ray can't install on it
   uv sync --extra dev
   scripts/check.sh           # ruff, mypy, pytest
   ```
3. Run the spike (`scripts/spike.sh`) — gate G1 depends on its results, and the
   relay actor work is blocked until it's done.
