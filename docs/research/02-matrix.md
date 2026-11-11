# 02 · Matrix — Federation, the Event DAG, and Replicated State

**Status:** research note · **Date:** 2026-11-10
**Why the brief names it:** *"comparación técnica para mensajería descentralizada o federada y estado replicado."*
**Primary source:** the [Server-Server API specification, v1.19](https://spec.matrix.org/latest/server-server-api/), read directly. Details below are spec-backed unless marked otherwise.

Matrix is the most valuable *technical* reference we have, because it is the only one of the three named systems that is a published, versioned specification for replicating conversation state across servers that do not trust each other.

---

## 1. The model in one paragraph

Users belong to **homeservers** (`@alice:example.org`). A conversation is a **room**, and a room is an append-only **directed acyclic graph of signed events**, replicated at every homeserver with a participating user. Servers exchange events over the federation API; there is no central server and no global ordering. Consistency is eventual, and when concurrent branches merge, a deterministic **state resolution** algorithm decides the winner.

The crucial structural fact for us: **a user's history lives on their homeserver, permanently.** Matrix is federated, not non-persistent. That is the point where Matrix conflicts with our R9, and it is the most important thing to say about it in the defense.

---

## 2. Server discovery and authentication (directly relevant to our no-Compose problem)

Matrix solves "how does one node find and trust another" without any central registry — the same problem we have inside a Docker network.

**Resolution order** for a server name `host[:port]`:
1. IP literal → use directly (default port 8448).
2. `GET https://<hostname>/.well-known/matrix/server` → `{"m.server": "<delegated_host>[:<port>]"}`. Follow redirects, no loops. Cache per HTTP headers, recommended default 24 h, maximum 48 h; errors cached up to 1 h with exponential backoff.
3. If no port given, SRV lookup `_matrix-fed._tcp.<delegated_hostname>` (deprecated fallback `_matrix._tcp`).
4. Otherwise A/AAAA and port 8448.

Note the deliberate detail: SRV lookup uses the *original* hostname, not the delegated one, because DNS is not trusted.

**Key distribution.** Each server publishes `GET /_matrix/key/v2/server` returning `server_name`, `verify_keys` (`ed25519:<id>` → public key), `old_verify_keys` (with `expired_ts`), `valid_until_ts`, and a self-signature. Keys are valid until the lesser of `valid_until_ts` and 7 days out; old keys may still verify historical events. **Notary servers** (`POST /_matrix/key/v2/query`) can serve cached, counter-signed keys for an origin server that is currently offline — an elegant trick: key availability does not depend on the key owner being up.

**Request signing.** Every federation request carries
`Authorization: X-Matrix origin="...",destination="...",key="ed25519:<id>",sig="..."`
over canonical JSON of `{method, uri, origin, destination, content?, signatures}`, where `uri` is path + query only. A `destination` mismatch returns 401. This prevents replaying a request at a different server.

**Lesson for us.** Replace DNS with our discovery mechanism, but keep the shape: **self-published signing keys, canonical-JSON request signing including the intended destination, and third parties able to vouch for a key when its owner is down.** That last idea maps directly onto relays caching each other's identity keys.

---

## 3. Transactions, retries and offline destinations

`PUT /_matrix/federation/v1/send/{txnId}` with body `{origin, origin_server_ts, pdus (max 50), edus (max 100)}`.

- **PDU** = Persistent Data Unit = a room event, durable, part of the DAG.
- **EDU** = Ephemeral Data Unit = typing notices, presence, read receipts, device-list updates, direct-to-device messages. Not persisted, not acknowledged.

The response is `200` with `pdus: {<event_id>: {"error": ...}}`; **a rejected PDU does not fail the transaction**, and a 200 is returned even if individual PDUs failed.

The one retry rule the spec states:

> The sending server must wait and retry for a 200 OK response before sending a transaction with a different `txnId`.

So delivery is a **per-destination ordered pipeline with txnId reuse** — essentially a TCP-like stop-and-wait over HTTP. The spec notably gives **no backoff schedule, no queue bound, and no drop policy**; those are left to implementations. The spec frames PDUs as the originating server's responsibility to deliver, "like email", but signed so third parties can relay them.

**Lesson for us.** We will need exactly this component — a per-destination outbound queue with idempotent transaction IDs — and because the spec leaves the hard parts undefined, we get to specify them ourselves and present that as original work: bounded queue, exponential backoff with jitter and a cap, hard TTL, and an explicit drop policy with user-visible consequences.

---

## 4. The event DAG — ordering without a global clock

Each event carries:
- `prev_events` — "all of the events in the room for which it has not yet seen a child", i.e. the current DAG frontier. This is what creates the partial order.
- `auth_events` — the specific events proving the sender was allowed to do this: the `m.room.create` event, current `m.room.power_levels`, the sender's membership event, and for membership changes the target's membership, `join_rules`, third-party invite token, and the authorising user for restricted rooms.
- `depth`, `hashes.sha256`, `signatures` keyed by server name and key ID.

**Receipt pipeline**, in order — worth copying wholesale as a validation discipline:
1. Valid event for the room version → else **drop**.
2. Signature check → else **drop**.
3. Content hash check → else **redact** and continue.
4. Auth against `auth_events` → else **reject**.
5. Auth against state before the event → else **reject**.
6. Auth against *current* state → else **soft fail**.
7. (v1.18+) policy-server validation → else soft fail.

**Rejected** events are not shown to clients and not used as `prev_events`, but may remain in the DAG. **Soft-failed** events exist to stop ban evasion by branching from an old DAG point: not relayed, not referenced by new events, but still counted in state resolution.

**Gap repair.** A server that was partitioned catches up by walking the DAG:
- `GET /_matrix/federation/v1/backfill/{roomId}?v=<eventIds>&limit=<n>` — walk backwards from known events.
- `POST /_matrix/federation/v1/get_missing_events/{roomId}` with `earliest_events`, `latest_events`, `limit` (default 10), `min_depth` (default 0) — breadth-first walk over `prev_events`.
- `GET /_matrix/federation/v1/state/{roomId}?event_id=` / `state_ids/...` — resolved state before an event.
- `GET /_matrix/federation/v1/event_auth/{roomId}/{eventId}` — the auth chain.

**This is the single most useful idea in Matrix for our project.** A conversation whose events reference their predecessors by hash is self-describing: a client that reconnects can see exactly which events it is missing and ask for them, and the hashes make duplicate suppression free (R6) and ordering intrinsic (R5). We can adopt a simplified version — a per-conversation hash chain or small DAG — without adopting Matrix's federation at all.

---

## 5. Joining a room you have never seen

1. Resolve the alias: `GET /_matrix/federation/v1/query/directory?room_alias=` → `room_id` plus candidate servers.
2. `GET /_matrix/federation/v1/make_join/{roomId}/{userId}?ver=<versions>` → an unsigned event template plus `room_version`. The joiner validates `room_id`, `sender`, `state_key`, `type`, `content.membership == "join"` before signing.
3. `PUT /_matrix/federation/v2/send_join/{roomId}/{eventId}` → `auth_chain`, `event`, `state` (the resolved state before the join), and from v1.6 `members_omitted` / `servers_in_room` for large rooms.
4. The resident server forwards the join event to the other servers in the room.

Errors are instructive: `M_UNABLE_TO_AUTHORISE_JOIN` / `M_UNABLE_TO_GRANT_JOIN` mean *try a different server* — the protocol assumes any single peer may be unable to help and the client retries elsewhere. Good pattern for our relay selection.

---

## 6. State resolution, and why we should avoid needing it

State is a map `(event_type, state_key) → event_id`. When partitioned branches merge, the room version's state-resolution algorithm picks a winner. Room versions 2–11 use **state resolution v2**: split state into unconflicted and conflicted sets, add the *auth difference* (auth events not shared by all branches) to the conflicted set, then run iterative auth checks over a reverse-topological ordering. Room version 12 uses v2.1, which starts iterative auth checks from an empty state map and includes a conflicted state subgraph — a change made to fix state resets (CVE-2025-49090).

Two honest observations:
- State resolution is widely regarded as the hardest and most bug-prone part of Matrix, and an independent academic analysis was needed to pin down its behaviour. Rooms with large, heavily branching histories are a known scaling pain point.
- **We do not need it.** State resolution exists because rooms have *mutable shared state* — membership, power levels, topic — that several servers can change concurrently. A 1:1 conversation between two fixed parties has essentially no mutable shared state. Our "room" has two members, decided at creation, and never changes.

**This is a key scoping decision and a strong defense talking point:** we get Matrix's benefits (causal DAG, gap repair, dedup by hash) while structurally eliminating its hardest problem, because the brief restricts us to 1:1 conversations.

---

## 7. Matrix's E2E layer, briefly

Olm (Double Ratchet, per-device pairwise) and Megolm (group ratchet for rooms), with cross-signing and device verification. A mechanised formal analysis of the Matrix cryptographic suite exists and is worth citing. For 1:1 we need only the Olm-equivalent, i.e. the Signal construction — see [`03-signal.md`](03-signal.md).

---

## 8. Verdict for our project

| Matrix idea | Verdict |
|---|---|
| Signed event DAG with `prev_events` | **Adopt (simplified)** — gives R5 and R6 almost for free |
| Content-hash event IDs | **Adopt** — natural dedup key |
| Canonical-JSON request signing incl. destination | **Adopt** |
| Self-published signing keys + notary/cached keys | **Adopt the idea** for relay identity |
| Per-destination transaction queue with idempotent `txnId` | **Adopt**, and specify the backoff/TTL the spec omits |
| Backfill / get_missing_events gap repair | **Adopt (simplified)** — this is how a reconnecting client catches up |
| "Try another server on failure" error semantics | **Adopt** |
| Homeserver owns user identity (`@user:server`) | **Reject** — identity must not die with a container |
| Permanent per-server history replication | **Reject** — violates R9 |
| State resolution v2 | **Avoid by construction** — no mutable shared state in 1:1 |
| Full federation protocol surface | **Reject** — far too large for three months |

---

## Sources

- [Matrix Server-Server API specification (latest / v1.19)](https://spec.matrix.org/latest/server-server-api/) — primary, read in full
- [Matrix Room Version 2 — state resolution](https://spec.matrix.org/v1.19/rooms/v2/)
- [Matrix Specification index](https://matrix.org/docs/spec/)
- [Matrix Decomposition: an independent academic analysis of Matrix State Resolution](https://matrix.org/blog/2020/06/16/matrix-decomposition-an-independent-academic-analysis-of-matrix-state-resolution/)
- [A Glimpse of the Matrix: Scalability Issues of a New Message-Oriented Data Synchronization Middleware (arXiv)](https://arxiv.org/pdf/1910.06295)
- [The Matrix Reloaded: A Mechanized Formal Analysis of the Matrix Cryptographic Suite (arXiv)](https://arxiv.org/pdf/2408.12743)
