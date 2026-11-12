# Technology Stack

**Status:** proposed, pending the spike and the library decisions noted below · **Date:** 2026-11-11, third version
**Decided by:** [ADR-0010](../decisions/0010-build-phase-1-on-ray.md) (Ray for phase 1, examiner requirement), [ADR-0002](../decisions/0002-python-backend-react-frontend.md) (Python and React), [ADR-0013](../decisions/0013-ray-free-core-for-phase-2.md) (Ray-free core). Architecture: [`ray-architecture.md`](ray-architecture.md).

Everything here is free and open source. Ray is Apache-2.0.

Sections 1 to 4 describe phase 1 (due 2026-12-01). Section 5 lists what changes in phase 2 (due 2027-01-01), where Ray is removed.

---

## 1. Component map (phase 1)

```
┌─ CLIENT NODE (container, Ray node, no CPU, no relay_slot) ─┐
│  React + Vite + TS  ──WebSocket──►  FastAPI / Uvicorn      │
│  (browser on host)                       │                 │
│                          client process = Ray driver       │
│                          linkchat.core:                    │
│                           · identity & keys                │
│                           · X3DH + ratchet                 │
│                           · outbox + retry                 │
│                           · ordering, seen-set             │
│                           · hash ring, preference lists    │
│                           · SQLite ← HISTORY               │
│                          linkchat.ray_runtime:             │
│                           · RelayGateway over actor calls  │
└───────────────────────────┬────────────────────────────────┘
                            │ Ray actor calls: store / fetch / wait / ack
┌─ RELAY NODE (container, Ray node, {"relay_slot": 1}) ──────▼┐
│  hosts at most one RelayActor (relay-0 … relay-4),          │
│  placed and restarted by Ray                                │
│   · in-memory mailbox store + TTL index (linkchat.core)     │
│   · sync on start, hinted handoff, expiry loops             │
│  NO plaintext · NO disk · NO object-store use               │
└───────────────────────────┬─────────────────────────────────┘
                            │ every node joins via linkchat-head:6379
┌─ HEAD NODE (container, never killed) ──▼────────────────────┐
│  ray start --head: GCS (membership, actor directory),       │
│  dashboard, job server. No relay, no client, no messages.   │
└─────────────────────────────────────────────────────────────┘
```

## 2. Backend

| Concern | Choice | Licence | Notes |
|---|---|---|---|
| Distributed runtime | **Ray** (`ray[default]`), one pinned 2.6x release | Apache-2.0 | The `default` extra adds the dashboard, state CLI and job submission. Version pinned after the spike |
| Language | **Python 3.11 or 3.12** | PSF | Ray supports 3.10 to 3.13 |
| Relay | Async Ray actor: named, detached, `max_restarts=-1`, small `max_task_retries`, `num_cpus=0`, `resources={"relay_slot": 1}`, three concurrency groups | n/a | [ADR-0011](../decisions/0011-relays-as-ray-supervised-actors.md) |
| Calls between processes | Ray actor method calls, awaited from asyncio | n/a | |
| Discovery and liveness | Ray: cluster join, named actors, call errors | n/a | [ADR-0004](../decisions/0004-discovery-through-ray.md). No membership code of ours |
| Supervision | Ray: `max_restarts`, `max_task_retries`, memory monitor | n/a | No supervisor of ours |
| Replication | Ours: three copies before `sent`, stand-ins, sync on restart, handoff | ours | [ADR-0005](../decisions/0005-three-copies-before-sent.md) |
| Bootstrap and load scripts | Submitted with `ray job submit` | n/a | The bootstrap is idempotent |
| Metrics | `ray.util.metrics` (Counter, Gauge, Histogram), Prometheus format | n/a | Counts only; never mailbox IDs |
| Cluster view | Ray dashboard and state CLI (`ray list actors`, `ray list nodes`, `ray status`) | n/a | The CLI is stable; the Python state SDK is a developer API and is not used in application code |
| Cluster authentication | Ray token authentication, `RAY_AUTH_MODE=token`, shared token file mounted in every container | n/a | Default from Ray 2.61 |
| Envelope | Fixed-size padded `bytes`, passed by value | n/a | Far below Ray's 100 KB inline limit, so it never enters the object store; to confirm in the spike |
| Payload inside the envelope | **msgpack** | Apache-2.0 | Client-side only, before encryption |
| Client HTTP/WS server | **FastAPI** + **Uvicorn**, one worker, in the driver process | MIT / BSD | No forking after Ray starts (a documented Ray anti-pattern) |
| Crypto primitives | **PyNaCl** (libsodium: X25519, Ed25519, XChaCha20-Poly1305, BLAKE2b) | Apache-2.0 *(confirm)* | |
| KDF and extras | **`cryptography`** (HKDF, AEAD) | Apache-2.0 / BSD *(confirm)* | |
| X3DH and Double Ratchet | Undecided: our implementation over the above, or `python-x3dh` + `DoubleRatchet` | ours / MIT *(confirm)* | ADR-0009, to decide in the first week; see [`phase-plan.md`](phase-plan.md) section 4 |
| Client persistence | **SQLite** (`aiosqlite`), WAL mode, on a Docker volume | MIT / PSF | Identity, sessions, outbox, seen-set, history |
| Relay persistence | Actor memory: dict plus `heapq` TTL index | stdlib | No disk and no checkpoint. Durability comes from three copies |
| Packaging | **`uv`** with a committed `uv.lock` | MIT / Apache-2.0 | |
| Lint and types | **`ruff`**, **`mypy --strict`** on `linkchat.core`, and a CI check that `core` does not import Ray | MIT | |
| Tests | **`pytest`** + **`pytest-asyncio`** on `linkchat.core` with a fault-injecting fake transport; integration tests on a local Ray cluster; scripts that kill containers and processes | MIT | [`fault-tolerance.md`](fault-tolerance.md) section 8 |

## 3. Frontend

| Concern | Choice | Notes |
|---|---|---|
| Framework | React 18 + TypeScript | |
| Build | Vite | Built assets served by FastAPI from the client container |
| Transport | WebSocket to the client process | Live messages and delivery states |
| Main view | Conversations with a delivery state per message: `pending`, `sent` (three copies confirmed), `delivered`, `read`, and `not delivered` after expiry | The states make replication and acknowledgement visible |
| Overlay view | The five relays, their status (`READY`, `SYNCING`, unreachable as seen by this client), and which three hold the current mailbox | From the client's own call outcomes and each relay's `status`. Ray's dashboard shows nodes and where each relay runs |

## 4. Containers and orchestration

- One image, three entrypoints: head, relay node, client. Base is either `rayproject/ray:<version>-py311-cpu` or `python:3.11-slim` with `ray[default]`; decided after measuring size and start time.
- One user-defined network: `docker network create linkchat-net`. Containers find the head by its container name through Docker's DNS.
- One `docker run` per container (Compose is forbidden):
  - head: `ray start --head --port=6379 --num-cpus=0 --dashboard-host=0.0.0.0 --block`
  - relay node: `ray start --address=linkchat-head:6379 --num-cpus=1 --resources='{"relay_slot": 1}' --block`
  - client: `ray start --address=linkchat-head:6379 --num-cpus=0`, then the client program
- Bootstrap, once, from the head container: `ray job submit -- python -m linkchat.ray_runtime.bootstrap --relays 5`.
- Every container gets `--shm-size` and a small `--object-store-memory`; values from the spike.
- Published to the host: the dashboard (8265, head) and each client's web UI port. Nothing else.
- Target topology: one head, five relay nodes plus two spares, two or three clients. Whether that fits on the demonstration laptop is the first measurement of the spike.
- Scripts in `scripts/`: `up-head.sh`, `up-relay-node.sh`, `up-client.sh`, `bootstrap.sh`, `kill-nodes.sh` (takes several names, kills them in one command), `kill-relay-process.sh`, `demo.sh`.

## 5. What changes in phase 2

| Concern | Phase 1 | Phase 2 |
|---|---|---|
| Runtime | Ray | None. Plain Python processes |
| Calls | Actor calls | asyncio TCP, length-prefixed frames, our own message types |
| Discovery, liveness | Ray | Seed addresses, gossip, a SWIM-style failure detector |
| Supervision | Ray | None inside the system; a dead relay is replaced by a new one |
| Placement | Fixed ring of five names | Ring over the live membership view |
| Replication rule | Three copies, no majority | To be redesigned for partitions |
| Authentication between nodes | Ray token | Signed requests |
| Observability | Ray dashboard, CLI, metrics | Our own status endpoint and overlay view |
| Containers | Head, relay nodes, clients | Relays and clients; no head |
| Unchanged | | Python, FastAPI, React, SQLite, the crypto libraries, msgpack, `uv`, `ruff`, `mypy`, `pytest`, and all of `linkchat.core` |

The earlier estimates for bare Python containers (40 to 70 MB of memory each) apply again in phase 2 and do not apply to Ray nodes in phase 1.

## 6. Open items

1. The spike list in [`../research/07-ray.md`](../research/07-ray.md) section 12.
2. ADR-0009: implement the ratchet or adopt a library.
3. Confirm the licences of PyNaCl, `cryptography` and any ratchet package before the written report cites them.

## Sources

- Ray documentation as cited in [`../research/07-ray.md`](../research/07-ray.md).
- [python-x3dh on PyPI](https://libraries.io/pypi/X3DH) · [DoubleRatchet on PyPI](https://pypi.org/project/DoubleRatchet/0.3.1/) · [ratchetkit](https://github.com/parsapoorsh/ratchetkit) · [python-axolotl](https://tgalal.com/publications/python-axolotl/)
