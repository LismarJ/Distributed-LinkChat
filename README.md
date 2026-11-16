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

Today the repository holds the documentation, the frozen interfaces, the
envelope format and the cluster scripts. The relay actor and the client do not
exist yet: they are blocked on the spike below.

**Nothing here has been run against Ray or Docker.** The code was written on a
machine with neither, so treat a failure as a bug in this code until you have
ruled that out.

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
  models.py                the same boundary types under agreed importable
                           names (Envelope, StoreReceipt)
  envelope.py              fixed-size padding, content-hash identifiers
  payload.py               what is inside an envelope: clock, predecessor, text
  constants.py             five relays, three copies, and why
src/linkchat/ray_runtime/  the Ray implementation of those ports. Phase 2 deletes it
src/linkchat/client_app/   FastAPI, the WebSocket, the React build
spike/                 throwaway code that answers what the design assumes
scripts/               one shell script per operation. No Docker Compose
tests/                 including the check that core stays Ray-free
docs/                  research, design, decisions, planning, reports
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

```bash
scripts/net-up.sh                  # create the network
scripts/build.sh                   # one image, three roles
scripts/up-head.sh                 # the cluster directory, never killed
scripts/up-relay-node.sh           # seven nodes: five for relays, two spare
scripts/up-client.sh 0 1           # two users
scripts/bootstrap.sh               # create the five relay actors  (needs R3)
scripts/status.sh                  # what Ray thinks is alive
```

Nodes may start in any order, before or after the head.

To break things on purpose, which is the point:

```bash
scripts/kill-nodes.sh linkchat-relay-1 linkchat-relay-3   # two at once
scripts/kill-relay-process.sh linkchat-relay-0            # process, not node
scripts/down.sh
```

## The spike comes first

The design rests on Ray behaviour taken from its documentation and not yet
observed: that a lost relay actor restarts on another node with a free slot,
how long that takes, what a caller sees meanwhile, and how many Ray nodes fit
on one laptop. [`spike/README.md`](spike/README.md) is the procedure.

```bash
scripts/spike.sh
```

If the answers disagree with the documentation, the design changes before any
more is built on it. That is what gate G1 on 2026-11-14 is for.

## Licence and dependencies

Everything used here is free and open source, as the project requires. Ray is
Apache-2.0. The stack is listed with licences in
[`docs/design/technology-stack.md`](docs/design/technology-stack.md).
