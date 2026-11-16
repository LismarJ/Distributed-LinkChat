# The spike: procedure

**Gate G1, due 2026-11-14.** The phase 1 design assumes things about Ray that
we have read and never run. This is where we find out. The questions are listed
in [`../docs/research/07-ray.md`](../docs/research/07-ray.md) section 12.

**None of this code has been run.** It was written on a machine with no Docker
and no Ray, so expect to fix it before it works, and treat a failure here as a
bug in the harness until you have ruled that out.

**Write every answer into `docs/research/07-ray.md` section 12 as you get it.**
An answer in a terminal that nobody wrote down is an answer we will re-measure
in two weeks.

---

## Before you start

The system Python on these machines is 3.14, and Ray does not support it. The
virtual environment has to be 3.12:

```bash
uv venv --python 3.12
uv sync --extra dev
```

Then build and bring up a cluster:

```bash
scripts/net-up.sh
scripts/build.sh
scripts/up-head.sh
scripts/up-relay-node.sh            # seven nodes: five for relays, two spare
scripts/status.sh                   # all eight should be alive
```

## The automated part

```bash
docker exec linkchat-head python -m spike.run create
docker exec linkchat-head python -m spike.run checks
```

That covers items 3, 4, 5, 6 and 9: handles across a restart, what a caller
sees and how fast, whether a background loop runs unattended, whether parked
waiters starve client calls, and whether envelope-sized arguments stay out of
the object store.

The two that can change the design most are 4 and 5. Item 4 sets the client's
per-call timeout, and every failover in the system is paced by it. Item 5
decides whether the bootstrap can simply create an actor or has to call a start
method afterwards.

## Item 1: a relay node is killed (the important one)

This is the measurement gate G1 turns on. ADR-0011 assumes Ray restarts a lost
actor on another node that has a free `relay_slot`.

Two terminals.

```bash
# terminal A
docker exec linkchat-head python -m spike.run watch 0

# terminal B: find which container holds probe-0, then kill it
scripts/status.sh
scripts/kill-nodes.sh linkchat-relay-N
```

Record: how long until the watcher says "down", how long until it says "back",
whether the node id changed, and whether the incarnation changed. The node id
changing is the behaviour the design needs.

Then the harder half. Kill relay nodes until there is no spare, and kill one
more holding a probe:

```bash
scripts/kill-nodes.sh linkchat-relay-5 linkchat-relay-6   # the spares
scripts/kill-nodes.sh linkchat-relay-N                    # one holding a probe
```

The design assumes the actor **waits** for a node rather than failing. Confirm
that, then add a node back and confirm it starts:

```bash
scripts/up-relay-node.sh 5
```

If instead it fails permanently, say so loudly: ADR-0011 is wrong and the
fallback is the pinned-relay design, which costs about three days.

## Item 2: a relay process is killed

```bash
# terminal A
docker exec linkchat-head python -m spike.run watch 1
# terminal B
scripts/kill-relay-process.sh linkchat-relay-N
```

The actor pattern defaults to `ray::RelayActor`, so for the spike pass the
probe's name:

```bash
LINKCHAT_ACTOR_PATTERN=ray::Probe scripts/kill-relay-process.sh linkchat-relay-N
```

Expect a restart on the same node, faster than the node case. Record both.

## Item 7: how heavy is a node, and how many fit

```bash
docker exec linkchat-head python -m spike.run nodes
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}'
docker exec linkchat-relay-0 ps -eo pid,rss,comm --sort=-rss | head -20
```

The target topology is one head, seven relay nodes and two or three clients,
so about eleven Ray nodes. If that does not fit, the plan changes today: fewer
spares first, then fewer relays, and the consequence goes into the
fault-tolerance document rather than being quietly absorbed.

## Item 8: the smallest object store that works

`scripts/env.sh` starts with `--shm-size=1g` and a 128 MiB object store, both
guesses. Since envelopes travel inline, try smaller and find where Ray starts
complaining:

```bash
LINKCHAT_OBJECT_STORE_MEMORY=67108864 LINKCHAT_SHM_SIZE=256m scripts/up-relay-node.sh 0
docker logs linkchat-relay-0 | tail -30
```

## Item 9, the host half

```bash
docker exec linkchat-head ray memory --stats-only
```

Run it before and after `spike.run checks`. No new objects is the answer the
design needs.

## Items 10 to 12

- **10, token authentication.** Set `RAY_AUTH_MODE=token` on every container
  with the same token file mounted, and confirm nodes still join. Then confirm
  a container without the token cannot.
- **11, `ray job submit`.** `scripts/bootstrap.sh` uses it. Confirm that a job
  which creates detached actors ends while the actors keep running.
- **12, metrics.** Confirm `ray.util.metrics` counters from inside an actor
  appear on the metrics endpoint.

## When you are done

```bash
docker exec linkchat-head python -m spike.run teardown
scripts/down.sh
```

Then answer gate G1 in `docs/planning/phase-1-work-plan.md`: does Ray behave as
the design assumes? If any answer is no, fix the design before writing more
code on top of it.
