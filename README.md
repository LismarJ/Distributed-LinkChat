# Distributed-LinkChat

An educational distributed-systems project: a secure, decentralized messenger.
Course project P08, *Mensajería segura descentralizada*, Sistemas Distribuidos
2026, Universidad de La Habana, Facultad de Matemática y Computación.

Messages reach people whose device is switched off, and no part of the system
is allowed to become a permanent store of anyone's conversations. Those two
requirements pull against each other, and the whole design is an answer to
that tension.

## State

Phase 1 is due **2026-12-01** and is built on Ray. Phase 2 is due
**2027-01-01** and removes Ray entirely.

Phase 1 is implemented and verified on real containers: encryption, ordering,
local history, replicated writes, sync-on-restart, hinted handoff, the browser
UI, and the fault-injection scenarios. `scripts/check.sh` is green (481
passed, 20 skipped — the skips need `fork`/`SIGKILL` and run on Linux CI and
in the containers). `scripts/scenario.sh all` passes 7 for 7 against real
Docker containers, including every two-simultaneous-failure case the
examiners added and a full wipe of every relay with both clients keeping
their history. See
[`docs/guides/2026-11-25-review-and-durability-fixes.md`](docs/guides/2026-11-25-review-and-durability-fixes.md)
for the measured recovery times and
[`docs/planning/phase-1-work-plan.md`](docs/planning/phase-1-work-plan.md)
for what is still open before the 2026-12-01 delivery (rehearsal, the written
deliverable, and two small decisions — no implementation work remains).

## Read first

| | |
|---|---|
| [`docs/README.md`](docs/README.md) | Index of everything |
| [`docs/planning/phase-1-work-plan.md`](docs/planning/phase-1-work-plan.md) | What to do next, who does it, and the gates |
| [`docs/design/ray-architecture.md`](docs/design/ray-architecture.md) | The phase 1 design |
| [`docs/design/fault-tolerance.md`](docs/design/fault-tolerance.md) | Failures, edge cases, and what each decision costs |

The design in one line: **Ray keeps the relays alive; we keep the data alive.**

## Layout

```
src/linkchat/core/         all application logic. Never imports Ray (ADR-0013)
  ports.py                 the frozen boundary: RelayGateway, RelayDirectory,
                           RelayPeers, Clock, and the errors that cross it
  mailbox.py               the in-memory store: TTL, tombstones, quotas
  ring.py                  rendezvous hashing: which relays hold a mailbox
  repair.py                sync on restart (R6) and hinted handoff (R7)
  replication.py           three-copy send with stand-ins (R5)
  envelope.py              fixed-size padding, content-hash identifiers
  payload.py               what is inside an envelope: clock, predecessor, text
  constants.py             five relays, three copies, and why
  client/                  outbox, ordering, SQLite history, the overlay view
  crypto/                  X3DH and the Double Ratchet (ADR-0009, ADR-0015)
src/linkchat/ray_runtime/  the Ray implementation of those ports. Phase 2 deletes it
src/linkchat/client_app/   FastAPI, the WebSocket, and the plain browser UI
                           (no build step — ADR-0016)
spike/                 the Ray behaviour the design assumed, now measured
scripts/               one shell script per operation. No Docker Compose
tests/                 including the check that core stays Ray-free
docs/                  research, design, decisions, planning, reports, guides
```

## Getting set up

The system Python on the development machines is 3.14, and **Ray does not
support it**. The virtual environment has to be 3.12:

```bash
uv venv --python 3.12
uv sync --extra dev
scripts/check.sh          # ruff, mypy, pytest
```

## Running a cluster

Every container is started on its own, by the course rule that forbids Docker
Compose. On a user-defined network, containers find each other by name through
Docker's own DNS, so the head's container name is the only address configured
anywhere.

The whole thing in one command, waiting at each step rather than guessing:

```bash
scripts/up.sh                      # network, image, head, 7 relay nodes, 2 clients
scripts/up.sh --build              # rebuild the image first
scripts/status.sh                  # what Ray thinks is alive
```

Or the individual steps `up.sh` runs, useful when something needs to come up
on its own:

```bash
scripts/net-up.sh                  # create the network
scripts/build.sh                   # one image, three roles
scripts/up-head.sh                 # the cluster directory, never killed
scripts/up-relay-node.sh           # seven nodes: five for relays, two spare
scripts/up-client.sh 0 1           # two users
scripts/bootstrap.sh               # create the five relay actors
```

Nodes may start in any order, before or after the head.

To break things on purpose, which is the point — a scripted run of the
delivery checklist's two-simultaneous-failure demonstration:

```bash
scripts/scenario.sh --list                 # the seven scenarios
scripts/scenario.sh two-nodes               # one of them
scripts/scenario.sh all                     # all seven, with a pass/fail summary
```

Or by hand:

```bash
scripts/kill-nodes.sh linkchat-relay-1 linkchat-relay-3   # two at once
scripts/kill-relay-process.sh linkchat-relay-0            # process, not node
scripts/down.sh
```

## The spike, and what it answered

The design rested on Ray behaviour taken from its documentation and not
observed firsthand: whether a lost relay actor restarts on another node with a
free slot, how long that takes, what a caller sees meanwhile, and how many Ray
nodes fit on one laptop. [`spike/README.md`](spike/README.md) has the
procedure, and [`docs/research/07-ray.md`](docs/research/07-ray.md) section 12
has the measured answers: yes, a killed relay container's actor is re-placed
onto a spare node (~31s); a killed worker process restarts on the same node
(~1.3s); the design's load-bearing assumption held, and ADR-0011 stands.

```bash
scripts/spike.sh
```

## Licence and dependencies

Everything used here is free and open source, as the project requires. Ray is
Apache-2.0. The stack is listed with licences in
[`docs/design/technology-stack.md`](docs/design/technology-stack.md).
