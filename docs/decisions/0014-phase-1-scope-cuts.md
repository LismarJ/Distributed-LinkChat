# ADR-0014: Phase 1 scope: optional features deferred, and no direct client-to-client path

**Status:** accepted · **Date:** 2026-11-12 · **Deciders:** project team
**Applies to:** phase 1 · **Relates to:** [ADR-0005](0005-three-copies-before-sent.md), [ADR-0011](0011-relays-as-ray-supervised-actors.md)

## Context

19 days remain to the first delivery. The budget in [`../planning/phase-1-work-plan.md`](../planning/phase-1-work-plan.md) section 4 shows the required work at roughly 31 person-days against 28 available on weekdays, so the optional work has to go rather than be attempted and abandoned half-built.

The examiners also stated that they grade whether we considered the cases and can say what each decision gains and loses. A feature deliberately excluded with a reason is worth more than the same feature half-implemented, and worth more than silence about it.

## Decision A: optional features are deferred

Phase 1 implements the "must" and "should" lists in [`../planning/phase-1-work-plan.md`](../planning/phase-1-work-plan.md) section 8. These are deferred:

| Deferred | What we lose |
|---|---|
| **Periodic digest comparison between relays** (repair mechanism 3, [`../design/ray-architecture.md`](../design/ray-architecture.md) section 6) | Deletion becomes less prompt, not less certain. See the analysis below |
| **Several envelope padding size classes** | One fixed size for every message. Simpler, and it leaks nothing; a long message is split or refused rather than padded to a bigger class |
| **Streaming delivery to clients** (Ray generators) | A parked `wait_for_mail` call plus a fetch does the same with less risk |
| **Read receipts** | Delivery states stop at `delivered`. The state that proves the distributed property is `sent` (three copies confirmed) and `delivered` (the recipient acknowledged), and both stay |

**The digest comparison is the only one of these that touches correctness, so it gets an argument rather than an assertion.** Cutting it does not weaken the two-failure guarantee. Every confirmed record is on three relays; sync on restart restores a replaced relay from its peers, and hinted handoff returns records to a primary that was unreachable when they were written. What digest comparison would have added is catching divergence those two do not: mainly a tombstone that reached only two of the three holders, because the client acknowledged while one relay was unreachable. Without it, that relay keeps the envelope until its TTL and offers it again, and the recipient's seen-set discards it. So nothing is lost and nothing is delivered twice; an acknowledged envelope can simply outlive its acknowledgement on one relay for up to one TTL. Requirement R9 is unaffected, because the guarantee there has always been expiry rather than acknowledgement ([`../design/fault-tolerance.md`](../design/fault-tolerance.md), case 3). Expiry is the backstop for any divergence we did not foresee, which is a weaker position than having both mechanisms and an honest one.

**The overlay view in the UI is reduced, not cut.** The examiners cannot grade what they cannot see, and the view that shows which three relays hold a mailbox is what makes the replication visible during the demonstration. It shrinks to the relay statuses the client already learns from its own calls, and Ray's dashboard covers the rest by showing nodes and where each relay actor is running.

## Decision B: no direct client-to-client path

A team member argued that sending directly between two users who are both online carries implementation complexity we cannot afford, and proposed dropping it. **The argument holds, and there is a stronger version of it specific to phase 1.** The feature is removed from phase 1 scope. It was listed as mechanism 11 of option C in [`../design/architecture-options.md`](../design/architecture-options.md) and was already marked optional there.

Why it holds:

1. **In phase 1 a client has no address other processes can call.** Each client runs as a Ray driver. A driver holds actor handles and calls actors; nothing we read in Ray's documentation offers a way to call into a driver. Ray's own out-of-band communication page reaches a process from outside by running a server **inside an actor**, not inside a driver.
2. **So a direct path needs one of two things, and both are bad here.** Either every client gets its own inbox actor, which is a new component with its own failure semantics (Ray restarts an actor empty, so anything sitting in a client's inbox actor is lost on restart, and the relay path must exist as a fallback anyway), or clients open their own sockets between containers, which is the hand-written transport the examiners told us not to build in phase 1.
3. **"Both online" is not a knowable state.** It is only ever "was reachable a moment ago", and the moment that guess is wrong is exactly the moment of sending. Every direct send therefore needs its own timeout and fallback, which adds a second copy of the ambiguous-failure problem that [`../design/fault-tolerance.md`](../design/fault-tolerance.md) case 1 already deals with once.
4. **Every cross-cutting mechanism would have to work twice.** Ordering, duplicate suppression, the outbox, delivery states, acknowledgement and expiry all assume one delivery path. Two paths do not add one case to the failure analysis; they multiply the interactions.
5. **It earns nothing against the requirements.** R3 and R4 are about the recipient being offline, which is the opposite situation. The two-failure requirement is about relays. Latency is not graded.
6. **It would weaken the demonstration.** The headline is killing two relays while messages keep flowing. A path that bypasses relays invites the question of whether the relay path was exercised at all.

## Consequences

- Roughly 2 person-days returned to the required work, which is less than it sounds: the schedule pressure in this project comes from the core mechanisms, not from the extras. The decision that actually bought time was [ADR-0009](0009-adopt-a-cryptography-library.md).
- Every message in phase 1 goes through the relay tier, with no exceptions and no second path. That uniformity is worth stating as a property, not only as an omission.
- **These exclusions get named at the defense**, each with the reason above, alongside the exclusions already listed in ADR-0001. That is the response to the examiners' point about tradeoffs.
- Nothing here is irreversible. The digest comparison and the direct path are both written up well enough to be built later, and the deferred items are recorded in [`../planning/phase-plan.md`](../planning/phase-plan.md) as candidates for phase 2, where clients have their own transport and the cost of a direct path is a different calculation. The presence problem in point 3 does not go away, so it stays out of phase 2 too unless a reason appears.
