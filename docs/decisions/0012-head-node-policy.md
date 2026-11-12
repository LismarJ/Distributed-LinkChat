# ADR-0012: One head node, not made fault tolerant

**Status:** accepted (follows from the examiners' scope) · **Date:** 2026-11-11
**Applies to:** phase 1

## Context

A Ray cluster has one head node, and "when a head node fails, the entire Ray cluster fails". Ray can persist the head's state in an external Redis so that a restarted head recovers, but supports that officially only under KubeRay (`docs/research/07-ray.md` section 7).

We had left open whether to build head recovery with Redis. The examiners have now stated that the head node will not be killed in phase 1 and that its failure is out of scope.

## Decision

- One head container, started with `ray start --head --num-cpus=0`. No Redis, no restart script.
- The head runs Ray's control plane, the dashboard and the job server. It offers no CPU and no `relay_slot`, so no relay and no client runs on it.
- The two-failure requirement is read as applying to every element except the head.

## Consequences

- No work on head recovery in phase 1.
- The head is a single point of failure for the cluster's control plane, and we say so in the report and at the defense. It holds no mailbox, envelope or key, so it is not a central store of messages and R9 is unaffected.
- If the head is lost anyway, the cluster stops, undelivered messages in relay memory are lost, and each client keeps its full local history.
- Phase 2 has no Ray and so no head. Its replacement for membership must not have a single point of failure.
