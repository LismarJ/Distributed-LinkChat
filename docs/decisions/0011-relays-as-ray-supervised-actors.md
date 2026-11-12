# ADR-0011: Relays are a fixed set of named actors supervised by Ray; clients are Ray drivers

**Status:** proposed, pending team review and the spike · **Date:** 2026-11-11 (second version)
**Replaces:** the draft "relays as pinned actors" of the same day, which was never accepted
**Applies to:** phase 1 · **Amends:** [ADR-0001](0001-two-tier-relay-overlay-architecture.md) · **Follows from:** [ADR-0010](0010-build-phase-1-on-ray.md)

## Context

ADR-0001's architecture has relays that hold replicated, expiring mailboxes and clients that hold identity, keys and history. We need to decide how those become Ray processes.

The first draft created one relay actor per relay container, pinned to that container with hard node affinity, with `max_restarts=0`. Killing a container removed that relay for good; our own code recomputed placement over the surviving relays and copied data to them. The reasoning was that a restarted relay is empty, so a restart seemed worse than a clean disappearance.

The examiners then stated that phase 1 must use what Ray offers and must not reimplement it. Restarting a failed actor, on another node if necessary, is something Ray offers. The first draft switched it off and rebuilt node-failure handling by hand.

Relevant facts about Ray (`docs/research/07-ray.md` sections 4 and 5):

- With `max_restarts=-1`, Ray restarts an actor whose process or node dies, on another node if needed, by running its constructor again. Application state is not restored.
- Ray's documented way to restore state is a checkpoint on storage the whole cluster can reach.
- With `max_task_retries` set, calls are at-least-once and may run twice.
- A zero-CPU actor with no other resource is placed on a random node.
- There is no anti-affinity option; a per-node custom resource or a `STRICT_SPREAD` placement group gives one actor per node.

## Options considered

1. **A fixed set of named relay actors, restarted by Ray, at most one per node through a custom resource, refilled from peers after a restart.**
2. One pinned, never-restarted relay per container, with placement over live relays (the first draft).
3. Relays restarted by Ray and restored from a shared checkpoint.
4. Option 1 with a `STRICT_SPREAD` placement group in place of the custom resource.
5. One actor per mailbox.
6. A single mailbox service, or Ray Serve.

## Decision

Option 1.

- **Relays.** `R = 5` async actors named `relay-0` to `relay-4` in namespace `linkchat`, created once by an idempotent bootstrap script (`get_if_exists=True`), with `lifetime="detached"`, `max_restarts=-1`, a small positive `max_task_retries`, `num_cpus=0` and `resources={"relay_slot": 1}`.
- **Relay nodes.** Identical containers started with `--resources='{"relay_slot": 1}'`. They run no application script at start. At least five; seven recommended, so that two node failures leave room for Ray to restart both relays.
- **Identity.** A relay's identity is its name. The hash ring is built from the names and never changes.
- **After a restart** the relay is empty. It reports `SYNCING`, copies its share of the data from the other relays, then reports `READY`.
- **Clients.** Each client container is a Ray node with no CPU and no `relay_slot`, running the client program as a driver.
- **Head.** Control plane only, with no CPU and no `relay_slot`.

## Why not the others

- **Option 2** does by hand what Ray's supervision does, which the examiners ruled out, and it needs every client to track cluster membership.
- **Option 3** is the pattern in Ray's documentation, and we considered it seriously for that reason. The checkpoint store would durably hold every mailbox, reachable from the whole cluster. That is the permanent central database that requirement R9 forbids. Peers already hold the data, so they are the checkpoint.
- **Option 4** works. It adds a placement group that must be created, kept alive and watched, and that can stay partially created when nodes are short. The custom resource gives the same guarantee with nothing extra to manage.
- **Option 5** costs one process per mailbox and lists every mailbox in the head's actor directory.
- **Option 6** is a central store, or a central entry point.

## Consequences

- No supervisor, membership view or rebalancing code of ours in phase 1.
- How fast a relay comes back is decided by Ray's failure detection and restart, which may take tens of seconds. Clients do not wait for it: calls have timeouts and unreachable primaries are covered by stand-ins ([ADR-0005](0005-three-copies-before-sent.md)).
- Because calls can be retried and failures are ambiguous, every relay operation must be idempotent.
- A restarted relay is unavailable until it has synced. Serving while empty would make readers miss mail.
- The number of relays is fixed at bootstrap. Growing it means changing the ring, which phase 1 does not support.
- Every client is a full Ray node, which costs memory. To be measured.
- This design depends on Ray behaviours we have read about and not yet seen: restart on another node with a custom resource, waiting when no node has room, handles across restarts. If the spike contradicts any of them, this ADR is revisited before any more is built on it.
- None of this survives into phase 2 as mechanism. Without Ray nobody restarts a relay, and placement must follow a changing membership ([`../planning/phase-plan.md`](../planning/phase-plan.md)).
