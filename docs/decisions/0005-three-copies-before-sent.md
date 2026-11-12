# ADR-0005: Three copies on three relays before a message counts as sent

**Status:** proposed, pending team review · **Date:** 2026-11-11
**Applies to:** phase 1 · **Amends:** [ADR-0001](0001-two-tier-relay-overlay-architecture.md) (which had "write to 3, acknowledge at 2")

## Context

The examiners require phase 1 to tolerate any two elements failing at the same time, and do not require it to tolerate network partitions.

The working values until now were three replicas and a write quorum of two. With those, a message could be reported as sent while on two relays only, and those two could be the two that fail.

## Options considered

1. **Three copies, all three confirmed before `sent`, with stand-in relays when a primary is unreachable.**
2. Three copies, confirm at two (the previous values).
3. Five copies, confirm at three (a majority quorum).
4. Three copies, all three confirmed, no stand-ins: refuse the write if a primary is down.

## Decision

Option 1.

- Each record (envelope or prekey bundle) is stored on **three relays on three different nodes** before the sender's client reports `sent`.
- The three are the first three reachable relays on the mailbox's preference list. When a primary is unreachable, the next relay on the list stands in and keeps a hint naming the primary.
- A reader asks the first three ready relays on the same list and merges the answers.
- There are **five** relay actors. With two down, three remain, so writes and reads continue.
- Fewer than three reachable relays means more than two failures. Sends then stay `pending` in the client's outbox until relays return.

## Why

- To survive `f` crashes a record needs `f + 1` copies; `f = 2` gives three. They must all exist before the confirmation, or the confirmation can be a lie.
- Option 2 fails the requirement directly.
- Option 3 buys protection against partitions, which phase 1 does not ask for, at the cost of more relays and more copies. A majority is needed to tell "crashed" from "unreachable"; in phase 1 the two are the same by assumption.
- Option 4 is safe but stops sends whenever one primary of a mailbox is down, which is the system being affected by a single failure.

## Consequences

- Sending is slower than before: the confirmation waits for the slowest of three relays, or for a timeout and a stand-in.
- Stand-ins add cases to reason about: records outside their primaries, handoff, and tombstones that must stop late copies. These are analysed in [`../design/fault-tolerance.md`](../design/fault-tolerance.md).
- With exactly five relay nodes and two dead, redundancy is not restored until a node is added. Two spare relay nodes are recommended.
- **This rule is not partition-safe.** In a partition, all three holders of a record can be on the side the recipient cannot reach, and both sides would keep accepting writes with no rule for merging. Phase 2 requires partition tolerance and will replace this ADR.

## Still open

- The client's per-call timeout, from the spike.
- Whether periodic digest comparison between primaries is built in phase 1 or left out ([`../design/ray-architecture.md`](../design/ray-architecture.md) section 6).
