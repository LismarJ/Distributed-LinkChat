# Fault tolerance, edge cases and tradeoffs (phase 1)

**Status:** proposed, pending team review and the spike · **Date:** 2026-11-11
**Reads with:** [`ray-architecture.md`](ray-architecture.md) (the design this analyses) and [`../research/07-ray.md`](../research/07-ray.md) (what Ray does on failure).

The examiners set two things for phase 1. The system must tolerate **level-two failures**: any two elements failing at the same time (two nodes, two processes, two threads, two machines) must not affect it. And the team is expected to have thought through the cases and edge cases, knowing that distributed systems have problems no mechanism removes completely, and to say what each decision gains and loses.

This document is where we do that. Nothing here has been tested yet. Each case names how we intend to test it.

---

## 1. The fault model

**Tolerated in phase 1**

- Crash of any two of the following at the same moment, in any combination: a relay node (container), a relay actor's process, a raylet, a client container, a client process, a background loop inside a relay.
- Any number of further failures, as long as the system has finished repairing before the next pair. Repair time is bounded by Ray's restart time plus the relay's sync time, both to be measured.

**Excluded by the examiners**

- Failure of the head node.
- Network partitions. Nodes are either up and reachable or down.

**Excluded by us, stated openly**

- Three or more simultaneous failures among relays.
- Relays or clients that lie (Byzantine behaviour). A relay can be curious; it is not malicious.
- Loss of a client's Docker volume. That is the user losing their device, and with it their identity and history.
- Failure of the Docker host or the Docker network, which would take everything at once.

**What "must not affect the system" means here.** We read it as two promises that hold with any two elements down:

1. **Nothing confirmed is lost.** A message shown as `sent` is delivered to its recipient, once, as long as the recipient comes online before the TTL.
2. **The system keeps working.** Users whose own client is running can still send, and still receive, without waiting for a repair.

## 2. Why three copies, and why not a majority

To survive `f` crashes, a record must be on `f + 1` nodes, so that one copy is left. With `f = 2` that is **three copies**, and all three must exist **before** the sender is told `sent`. If we confirmed at two copies, as the earlier design did, the two relays holding them could be the two that fail.

Majority quorums (`2f + 1` nodes, confirm at `f + 1`) exist for a different problem: telling a crashed node from a node on the other side of a partition, so that two sides cannot both make progress. In phase 1 there are no partitions and Ray tells us who is alive, so we do not pay for a majority.

| | Phase 1 (this design) | What a partition-tolerant design needs |
|---|---|---|
| Copies of each record | 3 (`f + 1`) | 3 or more |
| Confirm a write at | 3 of 3, using stand-ins | A rule that still works when the sides cannot talk |
| Relays needed to keep writing with 2 down | 5 | Depends on the rule |
| What goes wrong if the assumption fails | A partition could leave a record's three holders on the far side | (designed for it) |

The last row is the price. This design is correct only under the stated assumption, and phase 2 removes the assumption. See [`phase-plan.md`](../planning/phase-plan.md).

## 3. Element by element

| Element that fails | What notices | What recovers it | Effect on users |
|---|---|---|---|
| **Relay actor process** (crash, `kill -9`, out of memory) | Ray | Ray restarts it on the same node; the relay syncs from peers | None. Calls in flight are retried by Ray or go to a stand-in |
| **Relay node** (container killed) | Ray, and before that the client's call timeout | Ray restarts the relay on a node with a free `relay_slot`; if none, the relay waits for one. The relay syncs from peers | None. Writes use stand-ins meanwhile |
| **Raylet on a relay node** | Ray treats it as node death | As above | None |
| **Background loop in a relay** (expiry sweeper, handoff, sync) | The relay itself: every loop runs under a wrapper that catches the exception | The wrapper restarts the loop. If it fails repeatedly, the relay exits its process abnormally so that Ray restarts the whole actor | None |
| **Client process or container** | The user | Restart on the same volume. The outbox resumes; unacknowledged mail is fetched again | That user is offline meanwhile. Other users are unaffected |
| **Bootstrap script** | Nobody needs to | Relays are detached and do not depend on it | None |
| **Spare relay node** | Ray | Nothing to recover | None. Fewer places for Ray to restart a relay |
| **Head node** | Out of scope | Not recovered | The cluster stops. Local history survives |

"Two threads" needs a comment. A relay is an async actor with a single thread, and the client is a single asyncio process, so the units that can fail independently inside a process are coroutines, the background loops above. That is why they are supervised explicitly.

## 4. Pairs that matter

Most pairs are two independent rows of the table above. These are the ones where the two failures interact.

| Pair | Why it is the hard case | Outcome |
|---|---|---|
| Two primaries of the same mailbox | Only one original holder is left | The third primary holds every confirmed record. New writes go to it and two stand-ins. Reads find it |
| A primary and the stand-in that covered for another primary earlier | The hinted copy is gone before handoff | The record was on three relays; one remains |
| A relay while it is `SYNCING`, and one of the peers it is pulling from | The restarted relay cannot finish sync from that peer | It finishes from the others. A record it needs is on three relays, and the syncing relay never counted as a holder |
| Sender's client and one primary, during a send | The write is partial and nobody is retrying | The message was never shown as `sent`. When the client restarts, the outbox sends it again; relays that already have it ignore the repeat |
| Recipient's client and a relay, between fetch and acknowledgement | The message may be committed locally but not acknowledged | After restart the client fetches it again and the seen-set discards it, then acknowledges |
| Two relay nodes when there are exactly five and no spares | Ray has nowhere to restart them | Three relays remain, which is exactly enough for writes and reads. Redundancy is not restored until a relay node is added. A third failure in that state can lose data |

The last row is why the recommended topology has two spare relay nodes.

## 5. Why a reader finds the record

A confirmed record is on three relays. Up to two fail, so at least one holder, call it X, is alive and `READY`. The reader asks the first three `READY` relays on the mailbox's preference list. X is among them unless three relays ahead of X on the list are `READY` and do not hold the record. A relay ahead of X that does not hold the record was either unreachable when the record was written, or was restarted since. In the first case it counts against the limit of two bad relays at that moment, so there are at most two of them, not three. In the second case it synced before becoming `READY`, and sync pulls every record it is a primary for from all reachable peers, which include a holder.

This is an argument, not a proof. Its weak point is timing: a relay that is slow, not dead, can be skipped by a writer and still be `READY` for a reader. The test for it is a randomized run against the in-process fake transport: thousands of sends with random pairs of failures, restarts and delays injected, checking after each run that every confirmed message was delivered once.

## 6. Edge cases

Each case states what happens, what we do, and what it costs.

**1. A call fails and we cannot tell whether it ran.**
Ray says of an unavailable actor that there is "no guarantee on whether the actor executed the task or not." We never try to find out. Every operation is safe to repeat: `store` is keyed by the hash of the ciphertext, `ack` of something already gone succeeds, `fetch` only reads. *Cost:* some duplicate work on the wire.

**2. The same message arrives twice.**
From an outbox retry, a Ray retry, or the same envelope fetched from two relays. The relay ignores a `store` for an ID it holds or has a tombstone for. The client's seen-set, with a unique index in SQLite, discards what gets through. *Cost:* the seen-set grows; it is trimmed by age, bounded by the TTL plus a margin.

**3. The acknowledgement is lost.**
The recipient committed the message and crashed, or the `ack` reached only some relays. Relays that missed it keep the envelope until its TTL and offer it again; the client discards it and acknowledges again. *Cost:* an acknowledged envelope can outlive its acknowledgement on a relay by up to one TTL. It is still ciphertext and still expires, so R9 holds, but "deleted on acknowledgement" is a best effort and "deleted at TTL" is the guarantee.

**4. A late copy brings back a deleted envelope.**
A stand-in hands off a record after the recipient acknowledged it on the primaries. The primaries hold a tombstone and refuse it. *Cost:* tombstones use memory until the envelope's own expiry time.

**5. A restarted relay is empty.**
Ray reruns the constructor and restores nothing. The relay reports `SYNCING` and refuses client calls until it has pulled its share from its peers. *Cost:* the relay is unavailable for the length of the sync on top of Ray's restart time. Serving while empty would make readers miss mail, so we pay it.

**6. A relay is slow, not dead.**
The client's timeout treats it as failed and uses a stand-in. Now four relays may hold the record. Handoff later finds the primary already has it, and the stand-in drops its copy. *Cost:* extra copies for a while. *Tradeoff:* a short timeout keeps sending fast but produces more of these; a long timeout does the opposite. The value comes from the spike.

**7. Ray has not noticed a death yet.**
Between a container being killed and Ray declaring the node dead, calls to the relay hang or fail. The client does not wait for Ray; its own timeout moves it on. *Cost:* the first send after a failure is slower by one timeout.

**8. A relay cannot restart because no node has room.**
The relay stays pending. The other relays carry on, and stand-ins keep its hinted records. When a relay node is added, Ray starts the relay and it syncs. *Cost:* redundancy is reduced for as long as an operator takes to add a node.

**9. The message expires before the recipient returns.**
R3 says deliver to offline users; R9 says do not become an archive. They collide here, and no mechanism satisfies both without limit. We choose a TTL, and after it the message is gone from the relays. The sender's client still has it and knows it was never acknowledged, so it can show "not delivered" and offer to send again. *Cost:* delivery is guaranteed only within the TTL. The value and the sender-side behaviour are ADR-0006.

**10. Clocks differ between relays.**
Expiry uses each relay's own clock against an absolute `expires_at` set by the sender. All containers share the host's clock in the demonstration, so skew is negligible there. In general a relay with a fast clock expires early. *Cost:* accepted for phase 1; a grace margin on expiry is a one-line mitigation.

**11. A relay runs out of memory.**
Ray's memory monitor kills the process and, with `max_restarts`, restarts it. We add a quota per mailbox and a total cap, and refuse `store` beyond them, so the monitor is a last resort. *Cost:* a sender can be refused when a recipient's mailbox is full; the message stays in the outbox.

**12. Messages arrive out of order.**
Ray gives no order for async actors, and envelopes come from several relays. The recipient orders by the vector clock and predecessor reference inside the payload, and holds a message whose predecessor has not arrived. *Cost:* if the predecessor expired or was never sent, the hold must end; after a timeout the client shows the message with a gap marker.

**13. Both users send at the same moment.**
Neither message is before the other. The vector clock reports them as concurrent and both clients break the tie the same way (by envelope ID), so both show the same order. *Cost:* the order of concurrent messages is arbitrary, though identical on both sides.

**14. A one-time prekey is handed out twice.**
Two senders ask different relays for the recipient's bundle and both get the same one-time prekey, because relays do not coordinate reads. Preventing it needs agreement between relays on every read, which is the consensus machinery we chose not to build. We accept it: the signed prekey alone still gives a secure session, and the recipient treats the second use of a one-time prekey as a session without one. *Cost:* slightly weaker forward secrecy for that one first message. To be settled in the ADR on the ratchet.

**15. A background loop dies silently.**
An exception in an asyncio task does not stop the actor, so expiry or handoff could stop without anyone noticing. Every loop runs under a wrapper that logs, counts and restarts it, and `status` reports each loop's last successful run. *Cost:* a little code, and one more thing to show on the dashboard.

**16. Many relays restart together.**
After a large failure several relays sync at once and pull from each other. Repair calls have their own concurrency group, so client traffic is not starved, and a `SYNCING` relay answers `pull` with what it has. *Cost:* sync takes longer under load.

**17. The bootstrap is run twice, or before the nodes exist.**
`get_if_exists=True` makes creation idempotent. Actors created before enough relay nodes exist wait for nodes. *Cost:* none.

**18. More than two failures.**
If all three holders of a record die before repair, the record is lost. If fewer than three relays are reachable, sends stay `pending`. If the head dies, the cluster stops. In every one of these, each client keeps its full local history. We state the limit; we do not hide it.

## 7. What each decision gains and loses

| Decision | Gains | Loses |
|---|---|---|
| Three copies before `sent`, no majority | Survives two crashes with five relays; simple write rule | Correct only without partitions |
| Stand-ins and hinted handoff | Sends keep working with two relays down | Records can sit outside their primaries for a while; more cases to reason about |
| Fixed ring of logical relays | No membership view in clients, no rebalancing | The number of relays is fixed at bootstrap; depends on Ray to bring relays back |
| Ray restarts relays | No supervisor of ours; a crashed relay returns without an operator | Restart speed is Ray's, and its detection time may be tens of seconds |
| Relays keep no disk state and no checkpoint | R9 holds by construction | Every restart is a full refill from peers; losing three holders loses data |
| Idempotent operations plus retries | Ambiguous failures stop mattering | Duplicate traffic; seen-set and tombstones to maintain |
| Short client timeouts | Fast failover | More false suspicions and extra copies |
| TTL on everything | Bounded retention with nobody's cooperation | A message can expire undelivered |
| Order decided by clients | No coordination between relays | Concurrent messages get an arbitrary, though consistent, order |
| One head node (given) | Nothing to build | A single point of failure for the control plane, outside the tested scope |
| One trust domain (given by Ray) | Nothing to build | No claim against a hostile relay in phase 1 |

## 8. How each claim gets tested

| Claim | Test |
|---|---|
| No confirmed message is lost with two failures | Randomized fault injection on the fake transport; then the scripted container tests below |
| Two relay nodes | `docker kill` two relay containers in one command during a send burst |
| Two relay processes | Kill two actor worker processes inside their containers |
| Node and process together | One of each |
| Client mid-send, client mid-receive | Kill the client container during a burst; restart on the same volume; compare both histories |
| Restarted relay refills | Compare a relay's record count before the kill and after `READY` |
| No duplicates, correct order | Compare sender and recipient histories message by message after every scenario |
| Nothing kept after acknowledgement or TTL | Dump relay counters after delivery and after expiry |
| The stated limit | Kill three holders at once and show the loss, and that local history is intact |
