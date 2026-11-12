# ADR-0013: Application logic lives in a core that does not import Ray

**Status:** accepted · **Date:** 2026-11-11

## Context

Phase 1 (2026-12-01) must be built on Ray. Phase 2 (2027-01-01) must remove Ray completely, with everything redesigned and implemented by us, and must tolerate partitions. There are 31 days between the two deliveries.

If Ray calls are spread through the code, phase 2 is a rewrite. Much of the system has nothing to do with Ray: cryptography, the envelope format, the mailbox store, the client's outbox, ordering, history, the UI.

## Decision

Two layers, with a rule that is checked automatically.

- **`linkchat.core`** contains all application logic and never imports Ray. It reaches relays only through interfaces declared in `core/ports.py`:
  - `RelayGateway`: what a client needs from a relay (`store`, `fetch`, `ack`, `wait_for_mail`, prekey operations), with failures reported as ordinary exceptions of ours.
  - `RelayPeers`: what a relay needs from other relays (`pull`, `handoff`, `digest`).
  - `Clock`: time, so that expiry can be tested.
- **`linkchat.ray_runtime`** implements those interfaces with Ray: the actor class that wraps the core mailbox store, the gateway that turns calls into actor calls and Ray's exceptions into ours, the bootstrap script, metrics.
- **CI fails** if `import ray` or `from ray` appears anywhere under `linkchat/core/`.
- A third implementation of the interfaces, an in-process **fake** that can drop, delay, duplicate and reorder calls and kill relays, is used for tests in both phases.

Phase 1's delivered system contains no phase 2 networking code. Phase 2 adds a second runtime package next to `ray_runtime` and then removes `ray_runtime`.

## Consequences

- Phase 2 replaces one package. The cryptography, client, mailbox store, repair logic, UI and tests carry over.
- Most failure cases can be tested without a cluster, in milliseconds, against the fake.
- The interfaces must not leak Ray concepts (handles, `ObjectRef`s, Ray exceptions). Designing them takes some care at the start.
- The interfaces will not fit phase 2 perfectly. Phase 1 assumes a fixed set of relays that someone else keeps alive; phase 2 has changing membership and partitions. We expect to change `core/ring.py` and the repair logic, and say so now.
- A small amount of indirection in phase 1 that a Ray-only project would not need.
