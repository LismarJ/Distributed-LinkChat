# ADR-0017: The browser UI is React, built in development and committed

**Status:** accepted · **Date:** 2026-11-26 · **Deciders:** project team
**Applies to:** phase 1 and phase 2 (tasks C9, C10) · **Supersedes:** [ADR-0016](0016-plain-browser-ui-without-a-build-step.md) · **Restores:** [ADR-0002](0002-python-backend-react-frontend.md)'s frontend choice

## Context

Two UIs were written for the same tasks in the same week. [ADR-0016](0016-plain-browser-ui-without-a-build-step.md) shipped a plain HTML/CSS/JS client with the reduced overlay view (C10). In parallel, the client track built a React + TypeScript + Vite UI in `frontend/`, with the agreed delivery states, gap markers, error messages for 409 and 413, reconnection with backoff, a narrow-screen layout, light and dark schemes, and accessible forms, but no overlay view.

We merged the two: the React UI, with the overlay view ported into it as a component.

ADR-0016's case against a build step rested on three risks. Committing the build output removes all three:

1. **A Node toolchain in the image and `npm install` at `docker build`.** Neither happens. `npm run build` runs on a developer's machine and writes into `src/linkchat/client_app/static/`, which is committed. The `Dockerfile` is unchanged: one `python:3.12-slim` stage, no Node.
2. **Network at runtime, or a CDN in the room.** None. The build bundles React into one local script; `tests/test_client_api.py` asserts that the page and every asset it references load from the client itself and name no third-party URL.
3. **Image size.** The built UI is about 175 kB on disk (53 kB gzipped). The image grows by that much, not by a Node stage.

## Decision

- **The UI is React 18 + TypeScript (strict) + Vite, in `frontend/`.** `npm run build` (which runs `tsc` first) writes `index.html` and `assets/` into `src/linkchat/client_app/static/`. That output is committed and is what the package and the image ship.
- **Every API route is under `/api`**, including the WebSocket (`/api/ws`) and the overlay view (`/api/relays`). The static mount at `/` is registered last. One prefix lets the Vite dev server proxy the API with a single rule, and means no API path can collide with a UI path.
- **The overlay view keeps ADR-0016's design:** it polls `GET /api/relays?peer=` every two seconds, because a relay dying is not a conversation event and never reaches the WebSocket. It renders whatever list the API returns and assumes no relay count or copy count, so it survives phase 2 unchanged.
- **The UI text is in English**, like the rest of the repository ([ADR-0003](0003-english-for-all-documentation.md)).

## Consequences

- **Rebuild before committing a UI change.** If `frontend/src/` changes and `npm run build` is not run, the image ships the previous UI. This is the main new risk. Stage the output with `git add -A src/linkchat/client_app/static/` so deleted assets go too: each build renames its files by content hash.
- **Two toolchains for developers, one for the image.** Working on the UI needs Node; building or running the system does not.
- **Generated files in git.** The minified bundle cannot be reviewed in a pull request; review `frontend/src/` instead.
- **TypeScript checks the UI** against the API types in `frontend/src/api.ts`, which is the gap ADR-0016 listed as its main cost.
- **Scripts follow the prefix.** `scripts/up.sh` and `scripts/scenario.sh` call `/api/...`.
- **Local development without Ray:** `scripts/dev_ui.py` runs several real clients over the in-process fake relays, with a console to take relays down, turn clients off, and lose messages.
