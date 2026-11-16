# ADR-0010: Build phase 1 on Ray, using what Ray offers

**Status:** accepted (external requirement) · **Date:** 2026-11-11, revised the same day after the examiners' clarifications · **Decided by:** the examiners

## Context

On 2026-11-11 the examiners told us the project has two phases and that phase 1 must be a functional project on Ray and Python with Ray as a core pillar. Later the same day they clarified:

- Phase 1 is delivered on **2026-12-01**; phase 2 on **2027-01-01**.
- In phase 1 we must use every Ray tool and feature that applies to the project naturally, without forcing any, and must not reimplement what Ray already offers.
- Phase 1 must tolerate **level-two failures**: any two elements (nodes, processes, threads, machines) failing simultaneously must not affect the system.
- The head node will not be killed; that is out of scope.
- Phase 1 does not have to tolerate partitions.
- One aim of the project is that we consider most cases and edge cases and how to solve them, accepting that distributed systems have inherent problems that nothing solves completely, and that we weigh what each design decision gains and loses.
- In phase 2 we must remove Ray completely and redesign and implement everything ourselves. Phase 2 must tolerate partitions.

Before this feedback the design assumed a hand-written networking layer ([ADR-0001](0001-two-tier-relay-overlay-architecture.md), `docs/design/architecture-options.md`).

## Decision

Phase 1 is built on Ray in Python. Every container is a Ray node, relays are Ray actors, clients are Ray drivers.

**We give to Ray everything Ray offers that the project needs:** cluster membership and discovery; remote calls; the directory of relays (named actors); supervision of relays (restart in place, restart on another node, call retries); placement constraints (a custom resource); concurrency inside relays (async actors and concurrency groups); cluster authentication; observability (dashboard, state CLI, application metrics); job submission for one-off scripts.

**We build what Ray has no feature for:** keeping three copies of each record, refilling a restarted relay from its peers, expiry and deletion, ordering, duplicate suppression, identity, encryption, and local history.

**We leave out Ray features that would have to be forced:** the object store for messages, checkpointing to shared storage, `ray.util.queue`, placement groups, Ray Serve, Ray Client, and the machine-learning libraries. The reason for each is in [`../research/07-ray.md`](../research/07-ray.md) section 10.

## Consequences

- **Python stays** ([ADR-0002](0002-python-backend-react-frontend.md)).
- **The two-tier relay design stays** (ADR-0001), mapped onto Ray by [ADR-0011](0011-relays-as-ray-supervised-actors.md) and `docs/design/ray-architecture.md`.
- **No hand-written discovery, failure detection, transport or supervisor in phase 1** ([ADR-0004](0004-discovery-through-ray.md)).
- **Replication changes to meet two failures** ([ADR-0005](0005-three-copies-before-sent.md)).
- **The head node is fixed infrastructure** ([ADR-0012](0012-head-node-policy.md)).
- **A Ray cluster is one trust domain.** In phase 1 we do not describe relays as mutually distrusting.
- **Phase 2 removes Ray**, so application logic lives in a layer that does not import it ([ADR-0013](0013-ray-free-core-for-phase-2.md)), and the pre-Ray design becomes the starting point for phase 2 (`docs/planning/phase-plan.md`).
- **Edge cases and tradeoffs are a deliverable**, kept in `docs/design/fault-tolerance.md`.
- **Resource estimates made for bare Python processes are void.** A Ray node is much heavier; the spike measures it.

## Still to confirm with the examiners

- Whether phase 2 keeps the two-failure requirement, and whether any element is guaranteed not to fail in phase 2.
- Whether the written deliverable must be in Spanish.
