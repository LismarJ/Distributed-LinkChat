# Architecture on Ray (phase 1)

**Status:** proposed, pending team review and the spike · **Date:** 2026-11-11, second revision
**Delivery:** 2026-12-01
**Inputs:** [`../research/07-ray.md`](../research/07-ray.md) for everything said about Ray; [`architecture-options.md`](architecture-options.md) for the comparison that chose the two-tier relay design ([ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md)); [`fault-tolerance.md`](fault-tolerance.md) for the failure analysis; [`phase-plan.md`](../planning/phase-plan.md) for what moves to phase 2.

**What changed in this revision.** The first Ray design (earlier the same day) pinned one relay to each container and told Ray never to restart it, so that we handled every failure ourselves. The examiners have since clarified the rules, and that design broke two of them:

| Examiner rule | Effect on the design |
|---|---|
| Use every Ray feature that applies naturally; do not reimplement what Ray offers | Ray's actor supervision (restart, rescheduling on another node, call retries) is now switched on. Relays become logical actors that Ray keeps alive, in place of per-container actors that we replace ourselves |
| Tolerate any two simultaneous failures | Every stored record is on three relays before the sender is told it was sent. The earlier "write to three, confirm at two" is not enough |
| The head node is not killed | Single head, no recovery work |
| No partition tolerance in phase 1 | Majority quorums are not needed; `f + 1` copies are |
| Phase 2 removes Ray completely | The split between Ray-free logic and Ray-specific code is now a requirement, with named interfaces |

---

## 1. The division of labour

One sentence carries the whole design: **Ray keeps the relays alive; we keep the data alive.**

| Concern | Who does it in phase 1 |
|---|---|
| Nodes finding each other, knowing who is alive | Ray (GCS) |
| Remote calls, serialization | Ray (actor method calls) |
| Finding a relay | Ray (named actors) |
| Restarting a crashed relay; moving it off a dead node | Ray (`max_restarts`) |
| Retrying a call that hit a restart | Ray (`max_task_retries`) |
| One relay per node, none on client nodes | Ray (custom resource) |
| Concurrency inside a relay | Ray (async actor, concurrency groups) |
| Cluster access control | Ray (token authentication) |
| Watching the cluster | Ray (dashboard, state CLI, metrics) |
| Which relays hold a mailbox | Ours: a fixed hash ring over the relay names |
| Keeping three copies of every record | Ours |
| Refilling a restarted (empty) relay | Ours: pull from peers |
| Forgetting: acknowledgement, expiry | Ours |
| Ordering, duplicate suppression | Ours, in the client |
| Identity, encryption, local history | Ours, in the client |

Ray offers nothing for the bottom half (research note, section 10), so building it is not reimplementing Ray.

## 2. The cluster

```
                 ┌──────────────────────────────────────────┐
                 │ HEAD  (linkchat-head)   never killed     │
                 │ GCS: membership, actor directory         │
                 │ dashboard, job server. No relay, no      │
                 │ client, no message data.                 │
                 └────────────────────┬─────────────────────┘
                                      │ every node joins via linkchat-head:6379
   ┌──────────────┬──────────────┬────┴─────────┬──────────────┬──────────────┐
┌──┴────────┐ ┌───┴───────┐ ┌────┴──────┐ ┌─────┴─────┐ ┌──────┴────┐ ┌───────┴───────┐
│ relay node│ │ relay node│ │ relay node│ │ relay node│ │ relay node│ │ CLIENT node   │
│ relay-0   │ │ relay-1   │ │ relay-2   │ │ relay-3   │ │ relay-4   │ │ client driver │
└───────────┘ └───────────┘ └───────────┘ └───────────┘ └───────────┘ │ keys, ratchet │
   + optional spare relay nodes (no actor until Ray needs them)       │ SQLite, UI    │
                                                                      └───────────────┘
```

Three kinds of container on one user-defined Docker network, each started with its own `docker run`:

- **Head.** `ray start --head --num-cpus=0`. Runs Ray's control plane. It offers no `relay_slot` and no CPU, so Ray places nothing of ours on it. The examiners have said it is not killed ([ADR-0012](../decisions/0012-head-node-policy.md)).
- **Relay node.** `ray start --address=linkchat-head:6379 --num-cpus=1 --resources='{"relay_slot": 1}'`. All relay nodes are identical and run no script of ours at start. They are capacity: a place where Ray may run one relay actor.
- **Client node.** `ray start --address=linkchat-head:6379 --num-cpus=0`, then the client program as a Ray driver. One per user.

**Bootstrap.** Once the head is up, a one-off script submitted with `ray job submit` creates the relay actors. It is idempotent (`get_if_exists=True`) and can be run again at any time. Relay nodes may be started before or after it, in any order: an actor created before enough nodes exist waits until a node with a free `relay_slot` appears.

**Sizes.** `R = 5` relay actors on at least five relay nodes. With five nodes and two killed, three relays remain, which is what a write needs. Two spare relay nodes (seven in total) let Ray bring all five relays back after two node failures without anyone starting a container. Whether seven Ray nodes plus the head and clients fit on the demonstration laptop is the first thing the spike measures.

## 3. The relay actor

```python
@ray.remote(
    num_cpus=0,
    resources={"relay_slot": 1},
    max_restarts=-1,
    max_task_retries=2,
    concurrency_groups={"client": 256, "waiters": 1000, "repair": 16},
)
class RelayActor:
    async def __init__(self, index: int, ring_size: int): ...   # starts sync + background loops

    # client-facing (group "client")
    async def store(self, mailbox_id, envelope_id, envelope, expires_at, hint=None): ...
    async def fetch(self, mailbox_id, credential): ...
    async def ack(self, mailbox_id, credential, envelope_ids): ...
    async def publish_prekeys(self, user_id, bundle): ...
    async def get_prekeys(self, user_id): ...

    # parked calls (group "waiters")
    async def wait_for_mail(self, mailbox_id, credential, timeout_s): ...

    # relay-to-relay (group "repair")
    async def pull(self, ring_range, since=None): ...    # a restarted peer asks for its share
    async def handoff(self, records, tombstones): ...    # a stand-in returns hinted records
    async def digest(self, ring_range): ...              # periodic comparison

    async def status(self): ...                          # SYNCING or READY, counters
```

Created by the bootstrap script as:

```python
RelayActor.options(
    name=f"relay-{i}", namespace="linkchat",
    lifetime="detached", get_if_exists=True,
).remote(i, R)
```

Exact signatures and the `credential` are still to be fixed (ADR-0008 and ADR-0007, pending). The numbers in the decorator are starting values for the spike. The choices that are part of the design:

- **A fixed set of logical relays.** `relay-0` to `relay-4` are names, not machines. The name is the relay's identity on the hash ring and it never changes. Which container currently runs `relay-2` is Ray's business.
- **Supervised by Ray.** `max_restarts=-1`. If the relay's process crashes, Ray restarts it on the same node. If its node dies, Ray restarts it on any node with a free `relay_slot`. If there is none, the relay waits and starts as soon as a relay node is added.
- **One per node.** Every relay node offers one `relay_slot` and every relay needs one, so two relays never share a node. This matters for the failure analysis: one container failure takes out at most one relay.
- **Detached.** The relays do not depend on the bootstrap script, or on any driver, staying alive.
- **Retries by Ray.** `max_task_retries=2` lets Ray retry a call that arrived during a restart. This is safe because every relay operation is idempotent (section 5). It is kept small, and every client call also has a timeout, because a relay that is waiting for a node could otherwise hold a call for a long time.
- **Empty after a restart.** Ray restarts an actor by running its constructor again; nothing is restored. The constructor marks the relay `SYNCING`, pulls its share of the data from the other relays (section 6) and then marks it `READY`. A `SYNCING` relay refuses client operations with an application error, which clients treat like an unreachable relay.
- **Memory only.** Mailboxes live in the actor's Python memory with a TTL index. Nothing goes to disk or to Ray's object store. Durability comes from the three copies.
- **Three concurrency groups.** Parked `wait_for_mail` calls and repair traffic each have their own limit, so neither can use up the capacity that `store` and `fetch` need.

## 4. Placement

The ring is fixed and known to everyone from two constants: the namespace and `R`.

- Each relay name is hashed to a point on a ring. A mailbox ID is hashed to a point too.
- The **preference list** of a mailbox is all `R` relays in clockwise order starting from the mailbox's point.
- The first three relays on that list are the mailbox's **primaries**. The remaining ones are **stand-ins**, used only while a primary is unreachable.

Because the ring never changes, clients and relays always compute the same list. There is no membership view to keep in step, no rebalancing when a container dies, and no directory to refresh. This is the largest simplification that Ray's supervision buys us, and it is specific to phase 1: in phase 2 nobody restarts a relay for us and membership will change (see [`phase-plan.md`](../planning/phase-plan.md)).

Clients hold one handle per relay name, obtained once with `ray.get_actor`. Whether a handle keeps working across a restart is a spike item; if it does not, the client re-resolves the name after an error.

## 5. Sending, receiving, forgetting

**Send.**

1. The client encrypts the message, pads it to the fixed envelope size, computes `envelope_id` as a hash of the ciphertext, and writes it to its outbox in SQLite.
2. It calls `store` on the three primaries at once and waits for replies in completion order, each with a timeout.
3. For every primary that fails or times out, it calls `store` on the next stand-in on the preference list, passing a `hint` that names the primary it is standing in for.
4. When **three distinct relays** have confirmed, the message is `sent` (first tick) and leaves the outbox.
5. If three confirmations cannot be collected, the message stays `pending` and the outbox retries with backoff. That happens only when fewer than three relays are reachable, which means more than two have failed.

`store` is idempotent: storing an `envelope_id` a relay already has, or has a tombstone for, changes nothing and returns success. So a retry by Ray, a retry by the outbox, and a duplicate call after an ambiguous error are all harmless.

**Receive.**

1. The recipient's client walks the preference list and calls `fetch` on the first three relays that answer `READY`, then merges the answers by `envelope_id`.
2. For each new envelope it decrypts, checks the ID against its seen-set, applies the ordering rule, and commits the message to SQLite.
3. Only after the commit does it call `ack` on every relay it fetched from.
4. While connected, it keeps one `wait_for_mail` call parked on one of those relays. The call returns when an envelope arrives or the timeout passes, and the client fetches again.

**Forget.**

- `ack` replaces the envelope with a tombstone, so that a copy arriving later from a stand-in or a peer is not stored again. Tombstones are passed along with repair traffic.
- Every relay drops an envelope at its `expires_at` whether or not an acknowledgement came. Tombstones expire at the same time the envelope would have.
- A relay offers no operation that returns anything already acknowledged or expired.

**Order and duplicates.** Ray does not order calls to an async actor, and retries can repeat them. The client does both jobs: a two-element vector clock per conversation and a reference to the previous message, inside the encrypted payload, give the order; the seen-set of envelope IDs removes duplicates.

**Prekey bundles** are stored like envelopes, under an ID derived from the user ID, on the same three-of-preference-list rule, and are republished by their owner whenever it comes online. How one-time prekeys are consumed across three copies is still open (ADR-0009 area; see [`fault-tolerance.md`](fault-tolerance.md), case 14).

## 6. Repair

Three mechanisms, in order of importance.

**Sync on restart (essential).** A relay that starts, for the first time or after a restart, asks every other relay: "send me the records and tombstones of every mailbox for which I am a primary" (`pull`). It becomes `READY` when every reachable peer has answered. Since every acknowledged record is on three relays and at most two can be down, at least one holder is reachable.

**Hinted handoff (essential).** A stand-in that holds a record for an unreachable primary keeps the hint. A background loop tries to deliver hinted records to their primary (`handoff`). When the primary confirms, the stand-in deletes its copy. Until then the stand-in serves the record to anyone who asks.

**Periodic comparison (useful, first to cut).** Primaries of a range exchange a digest of envelope IDs and tombstones now and then and send what the other lacks. Its job is to spread tombstones that missed a relay and to catch anything the first two mechanisms did not. If it is cut, the cost is that an acknowledged envelope may sit on one relay until its TTL and be offered again, where the client's seen-set discards it.

The argument that these are enough, and the cases where they are not, are in [`fault-tolerance.md`](fault-tolerance.md).

## 7. The client

Unchanged in substance from the earlier design, and almost entirely free of Ray:

- Identity is a locally generated key pair; the user ID is the hash of the public key. Contacts are added through an invitation exchanged out of band.
- X3DH to start a session with someone offline; Double Ratchet afterwards.
- SQLite on a Docker volume holds identity, session state, the outbox, the seen-set and the whole history. It is the only permanent home of any message.
- The outbox is written before the first send attempt, and incoming messages are acknowledged to relays only after they are committed. A client killed at any moment loses nothing that it had been told was sent or received.
- FastAPI serves the React UI and a WebSocket to the browser. Uvicorn runs with one worker inside the driver process, because forking after Ray has started is one of Ray's documented anti-patterns.

The only Ray code in the client is the adapter that turns "store this envelope on relay 3" into an actor call.

## 8. Security and what we can claim

Ray "expects to run in a safe network environment and to act upon trusted code". So:

- **What holds.** Encryption and decryption happen only in client processes. A relay never receives plaintext or keys. Mailbox identifiers are opaque. Relays keep nothing after acknowledgement or expiry. Prekey bundles are signed, so a relay cannot substitute one.
- **What we do not claim in phase 1.** That relays distrust each other, or that the design resists an attacker who controls a relay machine. Anyone inside the cluster can run code on any node.
- **Threat model for phase 1.** Relays follow the protocol but would read anything they are given. One party operates the cluster on a private network.
- **Measures.** Token authentication on every node; no Ray port published to the host except the dashboard; relay code never logs mailbox IDs or envelope contents, because Ray forwards worker logs to the head; relay metrics are counts only.

Phase 2 removes the single trust domain along with Ray, and the stronger claim comes back into scope there.

## 9. The requirements, checked

| | Requirement | How it is met in phase 1 |
|---|---|---|
| R1 | Identity and contact discovery | Self-generated key pair; invitations out of band; prekey bundles on relays |
| R2 | One-to-one conversations | One ratchet session and one vector clock per pair |
| R3 | Delivery to offline recipients | The sender stores on three relays; the recipient's container can be stopped and fetches when it returns |
| R4 | Store-and-forward | Three copies on three nodes before `sent`; Ray restarts relays; restarted relays refill from peers |
| R5 | Ordering | Client-side vector clock and predecessor reference |
| R6 | Duplicate suppression | Idempotent relay operations keyed by content hash; client seen-set |
| R7 | Local history | SQLite on the client's volume |
| R8 | Authentication and encryption | X3DH and Double Ratchet between clients; Ray token authentication for the cluster |
| R9 | No permanent central database | Relays hold ciphertext in memory until acknowledgement or TTL, have no history operation, and are never checkpointed; the head holds cluster metadata only |
| | Two simultaneous failures | [`fault-tolerance.md`](fault-tolerance.md) |

## 10. Design patterns in use

| Pattern | Where |
|---|---|
| **Ports and adapters** | `linkchat.core` defines interfaces (`RelayGateway`, `RelayPeers`, `Clock`); `linkchat.ray_runtime` implements them with Ray. Phase 2 writes a second set of adapters |
| **Supervisor**, delegated to the platform | Ray restarts relays. We write no supervisor of our own in phase 1 |
| **Replicated state with sloppy quorum and hinted handoff** (Dynamo) | Sections 4 to 6, with `N = 3`, `W = 3`, reads from three |
| **Recovery by state transfer from peers** | Sync on restart, in place of checkpoint and restore |
| **Idempotent receiver** | Every relay operation is keyed by a content hash |
| **Transactional outbox** | The client records a message before sending and removes it on confirmation |
| **Acknowledge after commit** | The client acknowledges to relays only after its own durable write |
| **Tombstone** | Deletions are recorded so that late copies are not stored again |
| **Lease by TTL** | Every record expires without anyone's cooperation |
| **Quarantine and probe** (Ray's advice for unavailable actors) | A relay that fails a call is skipped for a short time, then tried again |
| **Bulkhead** | Concurrency groups isolate client, waiting and repair traffic |
| **Event-driven wait** (Ray's actor-sync pattern) | `wait_for_mail` parks on an `asyncio.Event` |
| **Fixed-size padding** | Envelope length carries no information |

Ray's own list of patterns and anti-patterns, and how each applies, is in the research note, section 11.

## 11. Code structure

```
linkchat/
  core/            no "import ray" anywhere
    crypto/        identity, X3DH, ratchet
    envelope.py    padding, envelope IDs
    ring.py        hash ring, preference lists
    mailbox.py     in-memory store, TTL index, tombstones
    repair.py      pull, handoff and digest logic over the RelayPeers interface
    client/        outbox, seen-set, ordering, SQLite history
    ports.py       RelayGateway, RelayPeers, Clock
  ray_runtime/     everything that imports ray
    relay_actor.py RelayActor: wraps core.mailbox and core.repair
    gateway.py     RelayGateway and RelayPeers implemented with actor calls
    bootstrap.py   creates relay-0 ... relay-(R-1)
    metrics.py     ray.util.metrics
  client_app/      FastAPI, WebSocket, static React build
```

`linkchat.core` is tested with plain `pytest`, with an in-process fake of the ports that can drop, delay, duplicate and reorder calls. That fake is also where most edge cases in the failure analysis are tested, because it is far easier to script than killing containers. Phase 2 replaces `ray_runtime` and leaves `core` in place.

## 12. Alternatives for mapping the design onto Ray

| Alternative | Why not |
|---|---|
| One relay pinned to each container, never restarted, with placement recomputed over the live relays (our first Ray design) | It switches Ray's supervision off and rebuilds node-failure handling in application code, against the examiners' rule. It also needs a membership view in every client |
| Restart relays and restore them from a checkpoint on shared storage (the pattern in Ray's documentation) | The checkpoint store would durably hold every mailbox, which R9 forbids |
| `STRICT_SPREAD` placement groups to separate replicas | Works, but adds an object that can stay partially created; the `relay_slot` resource is enough |
| One actor per mailbox | One process per mailbox, and every mailbox name in the head's actor directory |
| `ray.util.queue.Queue` as the mailbox | A single unreplicated actor with no acknowledgement or expiry |
| A single mailbox service, or Ray Serve in front of relays | A central store or a central entry point |
| Envelopes in the object store (`ray.put`) | An object dies with its owner, which is the sender |
| Clients outside the cluster through Ray Client | Discouraged for long-running programs |

## 13. The demonstration

1. Start the head, then five relay nodes and two spares in any order, then run the bootstrap. `ray list actors` shows `relay-0` to `relay-4` alive on five different nodes.
2. Start clients A and B. A adds B through an invitation.
3. Stop B's container. A sends messages; each shows `sent`.
4. **Two nodes at once.** Kill two relay containers that are primaries of B's mailbox, in the same command. A keeps sending and messages still reach `sent`, through stand-ins. The dashboard shows Ray restarting both relays on the spare nodes; their status goes from `SYNCING` to `READY`.
5. Start B. B receives everything, once, in order.
6. **Two processes at once.** Kill the worker processes of two relays without killing their containers. Ray restarts them in place; nothing is lost.
7. **A client mid-send.** Kill A's container while it is sending a burst, then start it again on the same volume. The outbox finishes the burst and B shows no duplicates.
8. Show a relay's memory: padded ciphertext under opaque IDs, gone after acknowledgement.
9. Kill all relay containers, start new ones: the relays come back empty, and both clients still show their whole history.

Step 9 is more than two failures, and it is there to show R9 and R7, not fault tolerance: messages that had not been delivered at that moment are lost, and we say so.

## 14. Open items

- The spike list in the research note, section 12. Items 1 to 6 decide whether this design stands as written.
- [ADR-0011](../decisions/0011-relays-as-ray-supervised-actors.md), [ADR-0004](../decisions/0004-discovery-through-ray.md) and [ADR-0005](../decisions/0005-three-copies-before-sent.md) are proposed and need the team's acceptance.
- ADR-0006: TTL, and what the sender sees when a message expires undelivered.
- ADR-0007: mailbox identifiers, the invitation format, and the credential for `fetch` and `ack`.
- ADR-0008: the relay actor interface and the envelope format.
- ADR-0009: write the ratchet ourselves or adopt a library. With delivery on 2026-12-01 this needs deciding in the first week ([`phase-plan.md`](../planning/phase-plan.md)).
- Ray version to pin, and base image.
