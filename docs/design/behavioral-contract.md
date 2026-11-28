# The behavioural contract

**Status:** frozen for phase 1 · **Date:** 2026-11-26
**Required by:** the examiners' methodology, *Required Phase 1 artifacts* item 3
**Frozen at:** the phase 1 exit gate. Changes after that need an explicit justification, recorded here and in an ADR.

This is the externally visible contract of the system: the operations a client
can perform and the outcomes it may observe. It is the one thing the
methodology says must **survive the redesign** — the internal architecture is
expected to change completely when Ray is removed, and this document is what
both implementations are measured against.

It deliberately names no Ray concept. There are no actors, no `ObjectRef`s and
no cluster in anything below. A reader who knows nothing about how the system
is built should be able to tell, from this document alone, whether a given
observed behaviour is a bug.

---

## 1. Clients, operations, and what is promised

The client is **a person and their own device**: one container, one identity,
one local database. There is no account, no server that knows them, and no
administrator. Two people exchange messages; that is the whole service.

| Operation | What the user means by it |
|---|---|
| **Learn my own identity** | "What do I give someone so they can write to me?" |
| **Add a contact** | "Here is the identity of someone I want to talk to." |
| **List contacts** | "Who can I talk to?" |
| **Send a message** | "Deliver this text to this contact, whether or not they are switched on." |
| **Read a conversation** | "Show me what was said, in the order it was said, once each." |
| **Watch for changes** | "Tell me when something arrives, without me asking repeatedly." |
| **Inspect the relay tier** | "Which relays are holding my mail, and which are in trouble?" |

The last one needs a word, because it looks like an implementation detail
leaking into the contract. It is not. The relay tier is part of **our**
problem domain, not of the middleware: [ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md)
chose a replicated relay overlay as the architecture, the brief requires that
no component become a permanent archive, and the examiners require that two
simultaneous failures be *demonstrated*. A user — and a grader — must be able
to see the tier's state to believe either claim. The view names relays and
their states, never Ray nodes or actors, and it assumes neither how many
relays exist nor how many copies a message has, so it is unchanged by the
removal of Ray.

## 2. The operations

All routes live under `/api` ([ADR-0017](../decisions/0017-react-ui-with-a-committed-build.md)).
Paths are stable; this is the contract.

| Operation | Route | Success | Failure outcomes |
|---|---|---|---|
| Learn my identity | `GET /api/me` | `200` `{user_id}` | — |
| List contacts | `GET /api/contacts` | `200` `{contacts: [...]}` | — |
| Add a contact | `POST /api/contacts` | `201` `{user_id}` | `422` not a UserId · `400` that is you |
| Send a message | `POST /api/conversations/{peer}/messages` | `201` `{envelope_id, state: "pending"}` | `404` not a contact · `409` contact's keys not yet published · `503` no relay answered for the keys · `413` message too long |
| Read a conversation | `GET /api/conversations/{peer}` | `200` `{peer, messages: [...]}` | `404` not a contact |
| Watch for changes | `WS /api/ws` | `{kind, peer, envelope_id}` per change | socket closes |
| Inspect the relay tier | `GET /api/relays[?peer=]` | `200` `[{name, state, records, tombstones, hinted_records, holds_mailbox, rank}]` | `404` unknown `peer` |

A **UserId** is 64 lowercase hex digits: BLAKE2b-256 of an Ed25519 identity
public key. It is self-certifying — holding the key proves the id — which is
what lets the system have no account and no registry.

`GET /api/relays` **never fails because a relay is down.** That is the state it
exists to report. This is a promise, not an accident: a view that 500s when
the cluster is damaged is useless at exactly the moment it is needed.

### The two error outcomes worth distinguishing

`409` and `503` both mean "cannot send yet", and a user would reasonably
confuse them, so the API does not:

- **`409`** — no relay has this contact's prekey bundle. The contact has
  never been online, so no session can be started. Waiting will not help
  until they run their client once.
- **`503`** — no relay *answered*. The keys may well exist. This is
  transient; retrying shortly is the correct response.

## 3. Delivery states

Each message carries exactly one state. These four are the contract; the UI
renders them, and a grader reads them.

| State | Meaning | Promise attached |
|---|---|---|
| `pending` | Accepted locally and durably queued | It will be retried across restarts until it is `sent` or it expires |
| `sent` | **Three distinct relays have confirmed holding it** | It survives any two simultaneous failures of relays or their nodes |
| `received` | The recipient's client has it in its own history | It is in the recipient's permanent copy and will be shown exactly once |
| `not_delivered` | It expired on the relays before the recipient collected it | It is gone from the relay tier. The sender is told, and may send again |

`pending → sent` is the transition the demonstration turns on, and `sent` is
the only one that makes a distributed claim. It means three *distinct* relays
confirmed, not three attempts and not two confirmations plus a hope
([ADR-0005](../decisions/0005-three-copies-before-sent.md)).

A message may also be flagged `after_gap`. That means a message causally
before it never arrived and the buffer gave up waiting: the conversation is
shown rather than blocked, with the gap marked. Availability over
completeness, deliberately.

## 4. The guarantees

These are the statements the system is willing to defend.

1. **Delivery to an absent recipient.** A message sent while the recipient's
   container is stopped is delivered when it returns, within the TTL. This is
   the requirement the relay tier exists for.
2. **Any two simultaneous failures.** Once a message reads `sent`, no two
   relays or relay nodes failing at the same instant can lose it. Two
   *logical processes*, two *whole nodes*, or one of each.
3. **Exactly once, in both histories.** No message is ever shown twice, under
   any retry, duplicate delivery, relay restart, or total relay wipe. An
   envelope's identity is the hash of its ciphertext, the relay store is
   idempotent on it, and each client keeps a persistent seen-set.
4. **A consistent order.** Both clients display the same order for the same
   conversation, including for messages sent concurrently. Order comes from a
   two-element vector clock plus a predecessor reference, never from a
   timestamp.
5. **Relays learn nothing.** A relay holds fixed-size (4090-byte) ciphertext
   under an opaque mailbox id, in memory, and has **no operation to ask for
   history**. It cannot read a message because it never holds a key, and it
   cannot serve an old one because nothing can request one.
6. **Only the recipient may empty a mailbox.** `ack` requires an Ed25519
   signature from the mailbox's reader ([ADR-0018](../decisions/0018-signed-acknowledgements.md)).
   Knowing both user ids — which are semi-public — is not enough to delete
   someone's mail.
7. **The history is the clients'.** Every relay can be wiped, simultaneously,
   and both participants still hold their full conversation. The relay tier is
   a transport, never a store of record.

## 5. What is deliberately not promised

Stated plainly, because a contract without limits is marketing.

| Not promised | Why, and what the user sees |
|---|---|
| Delivery beyond the TTL | "Do not become an archive" and "deliver to the offline" collide. We pick a TTL; past it the sender sees `not_delivered` |
| Durability against total power loss | `sent` means three relays hold it **in memory**. That survives two failures, which is the requirement; it does not survive losing every relay at once before collection. The clients' own history *is* on disk (WAL, `synchronous=FULL`) |
| Confidentiality of *traffic patterns* | A relay sees that one mailbox receives mail repeatedly, and can link a pair over time. It cannot tell who they are |
| Partition tolerance | Not required in phase 1 and not claimed. A silent relay is treated as dead. **Phase 2 requires it** and the replication rule is redesigned there |
| A direct client-to-client path | Every message goes through relays, without exception ([ADR-0014](../decisions/0014-phase-1-scope-cuts.md)). A property, not an omission |
| Read receipts, streaming, multi-device | Out of phase 1 scope |

## 6. Why nothing here names Ray

The methodology asks for a contract phrased in the problem's own terms —
"send message", not "call actor named X". Checked against the list above:

- No operation mentions an actor, a node, a task, an `ObjectRef`, a
  namespace, or a resource.
- The only infrastructure word that appears is **relay**, which is ours: it
  predates the Ray requirement ([ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md),
  2026-11-10) and survives it.
- `sent` is defined as "three distinct relays confirmed", a statement about
  our replication rule, not about Ray's delivery semantics.
- The relay view reports `ready` / `syncing` / `unreachable` — three states of
  *our* relay lifecycle. Ray's own actor states, node ids and restart counts
  are not exposed and are not part of the contract.

Consequently this document should need **no change** when Ray is removed. That
is the test of whether it was written at the right altitude, and it is the
reason it is frozen here rather than after phase 2.

## 7. Change control

Frozen at the phase 1 exit gate (2026-11-26). To change it:

1. Record what changed and why, here, with a date.
2. Open an ADR if the change alters a guarantee in section 4 or a limit in
   section 5.
3. Say explicitly whether the change was forced by the Ray removal. The
   methodology expects the contract to be stable across that boundary, so a
   change forced by it is a finding worth reporting, not a detail.

No changes yet.
