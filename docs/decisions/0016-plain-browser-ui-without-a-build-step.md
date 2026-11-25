# ADR-0016: The phase 1 browser UI ships without a build step

**Status:** accepted · **Date:** 2026-11-24 · **Deciders:** project team
**Applies to:** phase 1 (tasks C9, C10) · **Amends:** [ADR-0002](0002-python-backend-react-frontend.md) for phase 1 only
**Relates to:** [ADR-0014](0014-phase-1-scope-cuts.md) (the overlay view is reduced, not cut)

## Context

[ADR-0002](0002-python-backend-react-frontend.md) chose "React + Vite + TypeScript for the client UI, served by the client node and opened in the examiner's host browser". That decision was taken on 2026-11-10, before any code existed, and it was about the *language and stack* question rather than about the frontend build pipeline, which nothing has depended on since.

Seven days before the delivery the situation is concrete in three ways ADR-0002 could not have accounted for:

1. **There is no Node toolchain anywhere in the project.** No `package.json`, no lockfile, and nothing in the `Dockerfile`, which is a single `python:3.12-slim` stage. Adding React + Vite means a second toolchain, a second lockfile, a Node build stage in the image, and `npm install` reaching the network during `docker build`.
2. **The image is explicitly meant not to need the network at runtime.** ADR-0002's own determinism mitigation is "no network access at container runtime beyond the overlay itself". Pulling React from a CDN in the browser would break that in spirit and, worse, make the demonstration depend on the examiners' room having working internet.
3. **The UI is small, and what it must show is fixed.** Four things: the identity and contact invitation, a conversation with a per-message delivery state, a composer, and the reduced overlay view of five relays ([ADR-0014](0014-phase-1-scope-cuts.md)). There is no routing, no shared client-side state worth a store, and no component tree deeper than two levels.

## Decision

**The phase 1 UI is a single directory of static files — `index.html`, `app.css`, `app.js` — served by the client's own FastAPI process from inside the Python package, with no build step, no framework and no third-party asset.** It is plain DOM against the HTTP and WebSocket API that already exists (task C8).

ADR-0002 otherwise stands: Python with `asyncio` for both node types, and the UI still served by the client node and opened in the examiner's host browser.

**What is not being claimed.** This is not an argument that React is the wrong tool for a chat UI in general; it plainly is not. It is an argument that a build toolchain is the wrong thing to add to this repository in its last week, for a view this size, when the thing being graded is the distributed-systems design.

## What this gains

- **One toolchain and one lockfile.** `uv` already gives deterministic installs; nothing else is introduced.
- **The demonstration cannot fail on the network or on a build.** The assets are in the image, served from the package directory. There is no `npm install` in `docker build` and no CDN fetch in the browser, which removes two failure modes from the one event that has to work three times in a row (work plan S6).
- **The image stays inside its budget.** ADR-0002 targets under 120 MB; a Node build stage would be the largest single addition to it.
- **It is reviewable.** Three files, roughly 600 lines in total, that a reader can follow in one sitting — which matters, because ADR-0002's own positive consequence was "readable code makes the defense easier".

## What this costs, honestly

- **No component model.** Rendering is `replaceChildren()` over a fresh node list on every change. That is fine at this size and would become unpleasant past a few more views; it is a real ceiling, just one phase 1 does not reach.
- **No TypeScript on the frontend.** The backend keeps `mypy --strict`; the UI has no static checking at all. Mitigated by keeping it small, keeping all authority in the API, and holding no derived state in the browser: every change refetches, so the worst a bug there can do is show something stale, never commit something wrong.
- **No frontend test framework.** Covered instead from the Python side: `tests/test_client_api.py` asserts the page and both assets are served, that no `http(s)://` or CDN reference appears in them, and that every API route still wins over the static mount at `/`.
- **The deviation has to be explained at the defense** rather than passing unremarked, which is why it is written down here instead of being done quietly.

## Consequences

- `src/linkchat/client_app/static/` is part of the Python package, so `pip install .` ships the UI and the container needs nothing further.
- The static mount is registered **last**, after every route, because it is mounted at `/` with `html=True` and would otherwise shadow the API. A test pins that ordering, since it is the kind of thing a later edit breaks silently.
- The overlay view polls `GET /relays` every two seconds. A relay dying is not a conversation event, so it does not arrive on the WebSocket; polling is the honest way to show it and two seconds is inside the failover timeout C10 asks for.
- **If phase 2 grows the UI** — more views, a membership map, partition state — this decision should be revisited rather than defended. The natural trigger is the first view that needs its own state; at that point React earns its toolchain and ADR-0002's original choice applies unchanged.
