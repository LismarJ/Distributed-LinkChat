# Core interface reconciliation and publishing the phase-1 scaffolding

**Status:** proposed, awaiting approval
**Date:** 2026-11-15
**Branch:** `docs/research-and-design` → a new integration branch off `origin/main`

## Why this exists

Two people froze *different* "frozen" core interfaces. Dev A (this author)
wrote the full phase-1 scaffolding on `docs/research-and-design` but never
published the code — only docs were ever committed. Meanwhile the partner
implemented a thinner `core` (`feat/core-contracts`) and it was **merged to
`main`** (commits `8ed2568`, `d71283e`, `e4e7561`). The two now overlap and
conflict.

The goal is to publish this author's work **without clobbering the partner's
merged interface**, reconciled into one coherent superset, and surfaced for the
partner's sign-off through a pull request — never a unilateral overwrite of
`main`.

## Current state (measured 2026-11-15, after `git fetch`)

- `origin/main` = initial + docs (PR #1) + partner's 3 core commits.
- Local `docs/research-and-design` = same base + **9 unpushed docs commits** +
  a large **untracked** working tree (the code).
- Branch vs `origin/main`: **9 ahead / 5 behind** — diverged. Local `main` is
  12 behind `origin/main`. A fast-forward of `main` is impossible.
- Merge-base: `06107a9` (the PR #1 docs index commit).

Conflict map since the merge-base:

| Area | Partner (on `main`) | This author (unpublished) | Verdict |
|---|---|---|---|
| `.gitignore` | adds one (13 lines) | adds one | trivial — union merge |
| core interface | `src/linkchat/core/ports.py` (77 lines) + `models.py` (`Envelope`, `StoreReceipt`) | `linkchat/core/{ports,envelope,payload,types,constants,clock}.py` | **reconcile** |
| layout | `src/` | flat | **adopt `src/`** |
| docs | untouched since merge-base | 9 commits | clean — additive |
| ring, mailbox, tests, Docker, scripts, spike, CI, pyproject | none | all present | additive |

~90% of this author's work (docs, `ring`, `mailbox`, tests, Docker, scripts,
spike) conflicts with nothing. The real collision is the **core interface** and
the **layout**.

## Goals

- One coherent `core` interface on a published branch, as a superset that keeps
  this author's working consumers (`mailbox.py`, `ring.py`, their tests) valid.
- Adopt the partner's `src/` layout (already on `main`).
- Keep the partner's merged names importable so their code/imports do not break
  (`from linkchat.core.models import Envelope, StoreReceipt`).
- Publish everything via a PR to `main` that **explicitly flags the interface
  supersession for the partner to review**.
- Do not overwrite or force `main`.

## Non-goals

- Implementing `ray_runtime` or `client_app` (still empty scaffolds; blocked on
  the spike, gate G1).
- Running anything against Ray or Docker (no Ray/Docker on this machine — the
  docs already say so; nothing here will be claimed as verified).
- Recording a project ADR for the interface decision **now**. That waits until
  the partner signs off on the PR, so the decision log stays honest.

## Anti-goals (would make this a failure even if it "works")

- Silently replacing the partner's interface on `main` with no review.
- Losing the documented semantics this author's design carries (typed errors,
  `Tombstone` expiry, stand-in `hint`, absolute `expires_at`, prekeys,
  `RelayStatus`) in order to match a thinner sketch.
- A green-looking commit set that hides the fact that nothing has been executed.

## Constraints

- `linkchat.core` must never import Ray (ADR-0013), enforced by
  `tests/test_core_is_ray_free.py`.
- No Docker Compose (course rule).
- Deliveries: phase 1 due 2026-12-01 (on Ray), phase 2 due 2027-01-01 (no Ray).
- The partner has already merged their interface; this is collaborative code,
  so the reconciliation is a *proposal* that lands through review.

## Approaches considered

1. **This author's interface as the superset base** *(chosen)*. Keep the richer
   `ports.py`/models as canonical; fold in the partner's additive ideas; keep
   their names importable. Least code loss, keeps working consumers and their
   documented rationale. Cost: supersedes the partner's `ports.py`/`models.py`,
   so it needs their sign-off (handled by the PR).
2. **Partner's interface as base.** Adopt `Envelope(bytes ids)` / `ttl_seconds`
   / `StoreReceipt` / their `RelayPeers`; rewrite `mailbox.py`, `ring.py` and
   their tests onto it. Rejected: high churn, discards documented
   tombstone/hint/expiry semantics, and the thin interface would have to regrow
   to support the mailbox anyway.
3. **Union both name-for-name.** Keep both `store()` variants, both `Envelope`
   and `Record`. Rejected: incoherent, two ways to do everything.

## The reconciled interface (the decided design)

**Layout.** Move everything under `src/`:

```
src/linkchat/
  __init__.py
  core/        __init__, types, constants, clock, envelope, payload,
               ports, ring, mailbox, models  (new compat surface)
  ray_runtime/ __init__  (empty; phase-1 task R3+)
  client_app/  __init__  (empty; task C8+)
```

`pyproject.toml` switches to a `src/` package discovery (`[tool.setuptools]` /
hatch `packages = ["src/linkchat"]` as appropriate); test imports stay
`linkchat.core.*` because the package is installed, not path-hacked.

**Types.** This author's typed-string `NewType`s win
(`MailboxId`, `EnvelopeId`, `RelayName`, `UserId`, `Credential`) — the consumers
use them. Migration note for the partner's `bytes` ids goes in the PR.

**Models surface (`core/models.py`, new).** A small public module re-exporting
the boundary models under the agreed names, so the partner's imports resolve:

- `Record`, `Tombstone` — this author's dataclasses (moved/ re-exported).
- `StoreReceipt` — kept as the partner named it, but documented at the **client
  send layer**: the aggregate result of storing to the quorum of relays
  (`envelope_id`, `acknowledged_relays: tuple[RelayName, ...]`, `confirmed:
  bool`). It is *not* the return of the per-relay `RelayGateway.store`.
- `Envelope` — kept as an alias/thin wrapper for the partner's name so their
  code imports cleanly; the canonical stored unit remains the opaque
  `envelope: bytes` carried inside `Record`.

**Ports (`core/ports.py`).** This author's file, unchanged in substance:
per-relay `RelayGateway.store(record) -> None` (idempotent), `fetch`, `ack`,
`wait_for_mail`, `publish_prekeys`, `get_prekeys`, `status`; `RelayDirectory`;
`RelayPeers.pull`/`handoff`; the `RelayError` hierarchy; `RelayState`,
`RelayStatus`, `PullResult`.

Folded in from the partner:

- `Clock.monotonic() -> float` added alongside `now()`/`sleep()`.
- The partner's `get_live_peers` maps onto liveness, which in phase 1 is Ray's
  concern surfaced via `RelayDirectory`/`RelayStatus`; **not added as a new
  method now** — noted in the PR as a deferred reconciliation item.

**One minor call left open (builder/partner):** the partner's `Envelope.version`
(a relay-visible protocol version). Proposal: add a `PROTOCOL_VERSION` constant
in `constants.py` and a one-byte version at the head of the envelope layout in
`envelope.py`, kept non-content-bearing so the relay still learns only
{exists, mailbox, expiry}. Marked as this author's call unless the partner
objects in review.

## Integration and publishing plan

1. Create an integration branch off `origin/main` (keeps the partner's layout +
   their 3 commits as the base).
2. Replay the 9 docs commits onto it (clean — partner touched no docs).
3. Add this author's code under `src/`, **replacing** the partner's
   `src/linkchat/core/ports.py` and `models.py` with the reconciled versions,
   and resolving `.gitignore` as a union.
4. Commit set (conventional commits, matching repo style) — see below.
5. Push the integration branch; open a **PR to `main`** whose description flags
   the interface supersession, lists the folded-in items and the two open calls
   (`version`, `get_live_peers`), and requests the partner's sign-off.
6. **Do not** fast-forward, merge, or force `main` locally.

Proposed commit set on the integration branch:

1. `build: adopt the src/ layout and union the gitignore`
2. `feat(core): reconcile the frozen ports, models and envelope format`
   (supersedes the partner's `ports.py`/`models.py`; adds `Clock.monotonic`,
   the `models.py` compat surface, `StoreReceipt` at the client-send layer)
3. `feat(core): add rendezvous-hashing relay placement ring` (R1)
4. `feat(core): add the mailbox store with tombstones and quotas` (R2)
5. `build: add packaging, tooling and CI with the Ray-free guard` (S2)
6. `build(docker): add the image and per-container cluster scripts` (S3)
7. `feat(spike): add the Ray-behaviour spike and runner` (S4)
8. `docs: rewrite the README and log phase 1 progress`

## What is left to the builder

- Exact `pyproject.toml` discovery stanza for the chosen build backend.
- Final `models.py` re-export shape and whether `Envelope` is an alias or a thin
  wrapper.
- The `PROTOCOL_VERSION` / version-byte call, if taken now.
- Whether the 9 docs commits are replayed as-is (rebase) or folded — default:
  replayed as-is to preserve history.

## Verification reality

Nothing here has run against Ray, Docker, `ruff`, `mypy` or `pytest` — this
machine has none of them (see the work-plan progress log). The commit messages
and PR will say so. "Done" here means *published and reconciled*, not *tested*.
