# Technology Stack

**Status:** agreed, pending the library decisions noted below · **Date:** 2026-11-10
**Decided by:** [ADR-0002](../decisions/0002-python-backend-react-frontend.md) — Python backend, React frontend. Architecture: [ADR-0001](../decisions/0001-two-tier-relay-overlay-architecture.md).

Everything here is free and open source, as the project requires.

---

## 1. Component map

```
┌─ CLIENT NODE (container) ──────────────────────────────┐
│  React + Vite + TS  ──WebSocket──►  FastAPI / Uvicorn  │
│  (browser on host)                       │             │
│                                   asyncio core:        │
│                                   · identity & keys    │
│                                   · X3DH + ratchet     │
│                                   · outbox + retry     │
│                                   · causal reorder     │
│                                   · dedup (seen-set)   │
│                                   · aiosqlite ← HISTORY│
└───────────────────────────┬────────────────────────────┘
                            │ WS (subscribe) + HTTP (poll)
┌─ RELAY NODE (container) ──▼────────────────────────────┐
│  asyncio TCP server (length-prefixed binary frames)    │
│  asyncio DatagramProtocol (gossip / UDP discovery)     │
│  · SWIM-style membership + failure detection           │
│  · consistent hashing → swarm(mailbox_id)              │
│  · in-memory mailbox store + TTL expiry heap           │
│  · anti-entropy (Merkle digest) with swarm peers       │
│  NO plaintext · NO history API · NO logs of content    │
└────────────────────────────────────────────────────────┘
```

## 2. Backend

| Concern | Choice | Licence | Notes |
|---|---|---|---|
| Runtime | **Python 3.12+** | PSF | `asyncio` throughout; no threads for I/O |
| Relay ↔ relay transport | **`asyncio` streams**, length-prefixed binary frames over TCP | stdlib | Hand-rolled; keeps the wire format fully ours and inspectable |
| Gossip / discovery | **`asyncio.DatagramProtocol`** over UDP | stdlib | Seed addresses primary; broadcast as a zero-config convenience |
| Client ↔ relay | WebSocket (subscribe) + HTTP (poll fallback) | — | `websockets` or Starlette's WS |
| Client HTTP/WS server | **FastAPI** + **Uvicorn** | MIT / BSD | Serves the React build and the API |
| Crypto primitives | **PyNaCl** (libsodium: X25519, Ed25519, XChaCha20-Poly1305, BLAKE2b) | Apache-2.0 *(confirm)* | C extension, releases the GIL |
| KDF / extras | **`cryptography`** (HKDF, AEAD) | Apache-2.0 / BSD *(confirm)* | Rust-backed |
| X3DH + Double Ratchet | **Implement over the above**, against the published specs and official test vectors | ours | Fallback: `python-x3dh` (MIT) + `DoubleRatchet` (python-omemo family) — see ADR-0009 |
| Message encoding | **msgpack** (`msgpack` / `ormsgpack`) or **protobuf**, **fixed-size padded** | — | Padding removes length as a metadata channel (SimpleX's rule) |
| Client persistence | **`aiosqlite`** over stdlib `sqlite3` | MIT / PSF | The only permanent home of history |
| Relay persistence | **In-memory dict + `heapq` TTL index**, optional disk spill | stdlib | No logs, no snapshots — R9 made structural |
| Packaging | **`uv`** with a committed `uv.lock` | MIT/Apache-2.0 | Fast, deterministic Docker builds |
| Lint / types | **`ruff`** + **`mypy --strict`** on protocol and state-machine modules | MIT | A type error in the ratchet is a correctness bug |
| Tests | **`pytest`** + **`pytest-asyncio`**, plus a churn harness driving the Docker CLI | MIT | The harness is itself a deliverable |

## 3. Frontend

| Concern | Choice | Notes |
|---|---|---|
| Framework | **React 18 + TypeScript** | |
| Build | **Vite** | Built assets served by FastAPI from the client container |
| Transport | WebSocket to the client node | Live message and delivery-state updates |
| Key UI element | **Delivery-state indicator** per message: `pending` (no relay reachable) → `sent` (write quorum W met) → `delivered` (recipient ACKed) → `read` | This is our main demo instrument — it makes the quorum and the ACK chain *visible on screen* when a grader kills a relay |
| Secondary | A relay-overlay view: live membership, swarm assignment, per-mailbox depth | Turns the gossip layer into something watchable during the defense |

## 4. Containers and orchestration

- Base image `python:3.12-slim`, multi-stage build, pinned by digest. Target < 120 MB.
- Two images: `linkchat-relay`, `linkchat-client`.
- `docker network create --subnet=… linkchat-net`, then individual `docker run` calls.
- **Shell scripts only** — Compose is forbidden by the course rules. Scripts live in `scripts/`: `up-relay.sh`, `up-client.sh`, `chaos-kill.sh`, `partition.sh`, `demo.sh`.
- Nodes announce readiness on the gossip channel; scripts wait on readiness, never on `sleep`.

## 5. Open library decisions

1. **ADR-0009** — implement the ratchet ourselves vs. adopt `python-x3dh` + `DoubleRatchet`. Decide after the week-1 spike. Implementing it is more defensible at the defense; adopting it protects the schedule.
2. Confirm licences for PyNaCl, `cryptography`, and the `DoubleRatchet` package before the written report cites them.
3. msgpack vs protobuf for the wire format — decide alongside ADR-0008 (wire protocol).

## Sources

- [python-x3dh on PyPI](https://libraries.io/pypi/X3DH) (MIT, latest release Jan 2026) · [DoubleRatchet on PyPI](https://pypi.org/project/DoubleRatchet/0.3.1/) (python-omemo family) · [ratchetkit](https://github.com/parsapoorsh/ratchetkit) (X3DH/PQXDH + Double/Triple Ratchet, pluggable primitives; licence unconfirmed) · [python-axolotl / Double Ratchet and X3DH for Python](https://tgalal.com/publications/python-axolotl/)
