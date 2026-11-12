# ADR-0004: Discovery and liveness come from Ray; clients keep no membership view

**Status:** proposed, pending the spike · **Date:** 2026-11-11 (second version; replaces the draft "membership from Ray" of the same day, which was never accepted)
**Applies to:** phase 1 only · **Follows from:** [ADR-0010](0010-build-phase-1-on-ray.md)

## Context

This number was reserved for the discovery mechanism, which we had planned to write ourselves: seed addresses, gossip, and a SWIM-style failure detector.

Ray provides node discovery and liveness (the GCS), and a directory of actors by name. The examiners' rule for phase 1 is not to reimplement what Ray offers.

The first draft of this ADR had every client and relay keep a local list of live relay nodes, read from Ray, and recompute placement from it. With [ADR-0011](0011-relays-as-ray-supervised-actors.md) the relays are a fixed set of named actors that Ray keeps alive, so that list is no longer needed.

## Decision

- **Joining.** Every container runs `ray start --address=linkchat-head:6379`. The head's container name, resolved by Docker's DNS on our network, is the only address configured anywhere.
- **Finding relays.** By name: `ray.get_actor("relay-<i>", namespace="linkchat")` for `i` in `0 .. R-1`. `R` is a configuration constant shared by clients and relays.
- **Liveness.** Ray is the authority and Ray acts on it (it restarts relays). The application does not track which nodes are alive.
- **Reacting to failure.** A caller learns that a relay is unusable from the call itself: a timeout, `ActorUnavailableError`, `ActorDiedError`, or the relay's own "still syncing" answer. It skips that relay for a short time and tries it again later, as Ray's documentation advises.

We do not call `ray.nodes()` or the `ray.util.state` Python SDK from application code. The overlay view in the UI and the demonstration use the state CLI and the dashboard.

## Consequences

- No discovery or failure-detection code in phase 1, and no membership view that could disagree between processes.
- The UDP broadcast spike planned earlier is cancelled.
- Failure handling in the client is driven by call outcomes and timeouts. The timeout value trades failover speed against false suspicion ([`../design/fault-tolerance.md`](../design/fault-tolerance.md), cases 6 and 7).
- Gossip membership and SWIM are not part of phase 1. They return in phase 2, where Ray is removed ([`../design/phase-plan.md`](../design/phase-plan.md)).

## To confirm in the spike

- Whether a handle obtained once keeps working after the actor restarts, or must be looked up again by name.
- How long a call to a killed relay takes to fail, and with which exception.
