# 07 · Ray: what it is, how it works, and how we use it

**Status:** research note · **First written:** 2026-11-11 · **Revised:** 2026-11-11, after the examiners' clarifications
**Why this note exists:** phase 1 of the project (due 2026-12-01) must be built on Ray and Python. The examiners added two rules that shape how we read Ray's documentation: use every Ray tool and feature that applies to our project naturally, without forcing any, and do not reimplement what Ray already offers. This note studies Ray with those two rules in mind. The design that follows from it is [`../design/ray-architecture.md`](../design/ray-architecture.md).

**How it was researched.** From the official documentation at docs.ray.io ("latest", read on 2026-11-11; the site banner refers to Ray 2.59 and 2.61, so read this as Ray 2.6x). Statements taken from the documentation are written plainly, with short quotations. Three other labels are used:

- **(inferred)**: our reading of what the documentation implies.
- **(from memory)**: something we believe about Ray that we did not find on a page we read.
- **(to test)**: must be confirmed in the spike in section 12 before the design depends on it.

We have not run Ray yet. Nothing in this note has been tested by us.

---

## 1. What Ray is

Ray is an open-source framework (Apache-2.0) for running Python programs across a cluster of machines. It came out of UC Berkeley for machine-learning workloads and is maintained by Anyscale. It has two layers:

- **Ray Core**: general distributed-computing primitives. Tasks, actors and objects.
- **Ray libraries** built on Core: Data, Train, Tune, RLlib (machine learning) and Serve (model and HTTP serving).

The idea behind Ray Core is that distributed execution should look like ordinary Python. You mark a function or class with `@ray.remote`, call it with `.remote(...)`, and Ray runs it in another process, possibly on another machine, and hands back a reference to the future result.

Our project is not machine learning. What we need from Ray is in Ray Core, plus Ray's operational tooling (dashboard, state CLI, metrics, job submission).

## 2. The three primitives

### 2.1 Tasks

A task is a stateless function executed in a separate worker process. Tasks declare the resources they need and the scheduler places them. A task is retried by default if its worker or node dies (`max_retries`, default 3); application exceptions are not retried unless `retry_exceptions` is set. Async functions cannot be tasks: "We don't support asyncio for remote tasks."

Relays hold state, so they are actors. Tasks have a small role for us: stateless parallel work in the test and demonstration harness (for example, generating load from many simulated senders).

### 2.2 Actors

An actor is a stateful worker: an instance of a Python class that lives in its own dedicated process. Its methods run in that process and can read and change its state.

```python
@ray.remote
class Counter:
    def __init__(self):
        self.value = 0
    def increment(self):
        self.value += 1
        return self.value

counter = Counter.remote()            # creates the actor process
ref = counter.increment.remote()      # returns an ObjectRef immediately
print(ray.get(ref))                   # blocks until the result is ready
```

Properties that matter for our design:

- **Handles can be passed around**, so one relay can hold handles to other relays and call them directly.
- **Named actors and namespaces.** `Relay.options(name="relay-0").remote()` registers a name that is "unique within their namespace", and any process in the cluster retrieves the handle with `ray.get_actor("relay-0", namespace="linkchat")`. With `get_if_exists=True`, "if the actor already exists, a handle to the actor will be returned and the arguments will be ignored", which makes creation idempotent.
- **Lifetime.** A normal actor dies when its creator dies or when no handles remain. An actor created with `lifetime="detached"` has no owner, is "not automatically garbage-collected by Ray", and must be removed with `ray.kill`, after which "the actor's name may be reused".
- **Resource defaults.** "Ray actors use 1 logical CPU for scheduling, and 0 logical CPU for running." If resources are given explicitly, "they are required both at schedule time and at execution time."

### 2.3 Objects

Every value returned by a remote call, or stored with `ray.put()`, is an immutable object referred to by an `ObjectRef`. Each node has a shared-memory object store. The process that created an `ObjectRef` is its **owner**; if the owner dies, the object is lost for everyone (`OwnerDiedError`). The fault-tolerance guide says to "avoid letting an `ObjectRef` outlive its owner task or actor."

Small values are passed inline with the call and never enter the object store. The design-pattern pages put the limit at about 100 KB: Ray inlines small return values up to that size, and an argument larger than that is stored in the object store once per call.

This ownership rule decides one design question for us. If a sender stored an envelope with `ray.put()` and handed the reference to a relay, the envelope would live in the sender's node and vanish when the sender went offline. That is the opposite of store-and-forward. Envelopes therefore travel **by value**, as small inline arguments, and each relay keeps its own copy in its own memory.

## 3. How a Ray cluster is put together

A Ray cluster is one **head node** and any number of **worker nodes**.

| Component | Where it runs | What it does |
|---|---|---|
| **GCS** (Global Control Service) | Head node only | "Centralized metadata server for a Ray cluster." Node membership, the actor directory, placement groups |
| **Raylet** | Every node | "A system process that runs on each Ray node. It's responsible for scheduling and object management" |
| **Object store** | Every node | Shared memory holding that node's objects |
| **Worker processes** | Every node | Python processes that run tasks; each actor gets its own |
| **Driver** | Wherever a program is started | "The process running the main script that starts all other processes" |
| **Dashboard, state API, job server** | Head node | Web UI, `ray list ...`, `ray job submit` |

Consequences:

1. **Membership is Ray's job.** The GCS is the authority on which nodes are alive and where each actor lives. An application built on Ray does not run its own discovery or failure-detection protocol.
2. **There is exactly one head node.** "When a head node fails, the entire Ray cluster fails." The examiners have told us the head will not be killed in phase 1, so we treat it as fixed infrastructure (section 7).
3. **After an actor exists, callers talk to it directly.** A caller holding a handle sends calls to the actor's process, not through the head.

### How a dead node is detected

The node fault-tolerance page gives no timing. What is documented: when a raylet fails "the corresponding node will be marked as dead and is treated the same as a node failure", and a raylet that restarts on the same machine gets a new ID and is "treated as a new raylet/node". The GCS health-checks raylets on a period, with a timeout and a failure threshold; our recollection of the defaults is a 3 s period, 10 s timeout and 5 failures, which would put detection in the range of 30 to 60 seconds **(from memory; to test)**. Because that window may be long, our clients do not wait for Ray's verdict: every call has its own short timeout, and a relay that does not answer is skipped (see the design, section 5).

## 4. Actors in depth

### 4.1 Concurrency

- A plain actor runs one method at a time.
- An actor with at least one `async def` method is an **async actor**: "Ray runs all of the methods inside a single python event loop." There is one thread, and "by default, 1000 tasks can be running concurrently" (`max_concurrency`).
- "Running blocking `ray.get` or `ray.wait` inside async actor method is not allowed"; `ObjectRef`s are awaited instead. On Python 3.11 and later the documented form is `await asyncio.wrap_future(ref.future())`; `asyncio.gather` and `asyncio.wait` work on the results.
- **Concurrency groups** give separate limits to groups of methods and "work with both asyncio and threaded actors":

```python
@ray.remote(concurrency_groups={"io": 2, "compute": 4})
class AsyncIOActor:
    @ray.method(concurrency_group="io")
    async def f1(self): ...
```

  Methods without a group go to the default group (1000 for async actors). A call can override its group with `.options(concurrency_group=...)`.
- **Background loops.** The out-of-band communication page shows an actor with an `async def __init__` that calls `asyncio.get_running_loop().create_task(...)` to start a long-running coroutine. That is the pattern we need for TTL expiry and repair. The page shows it as an example, not as a stated guarantee **(to test)**.
- **Waiting for an event.** The pattern "using an actor to synchronize other tasks and actors" wraps an `asyncio.Event` inside an actor with `num_cpus=0`: callers block in `await event.wait()` until another call sets it. This is how a relay wakes a recipient who is waiting for mail.

### 4.2 Ordering of calls

- Calls from one submitter to a synchronous single-threaded actor execute in submission order.
- Calls from different submitters have no guaranteed order.
- "For async or threaded actors, tasks might be executed out of order."
- With retries enabled, order is not guaranteed across retries for async actors.

Ray does not give us message ordering. Requirement R5 remains application work.

### 4.3 Where an actor runs

Ray filters nodes by the resources the actor requests, then applies a scheduling strategy.

- **Custom resources.** A node declares invented resources at start, `ray start --resources='{"relay_slot": 1}'`, and an actor demands them. Resources are logical ("don't need to have 1-to-1 mapping with physical resources") and are hard requirements. If no node can satisfy a request, the actor "cannot be scheduled until feasible nodes are added to the cluster."
- **Zero-CPU actors.** An actor with `num_cpus=0` and no other resource is placed by "randomly choosing a node in the cluster without considering resource utilization." So a zero-CPU relay actor with no custom resource could land on a client node or on the head. It needs a resource to steer it.
- **`"SPREAD"`** "will try to spread the tasks or actors among available nodes". Best effort only.
- **Node affinity.** `NodeAffinitySchedulingStrategy(node_id, soft=...)` pins an actor to a node. With `soft=False` the actor fails with `ActorUnschedulableError` if the node is gone. The documentation says it "should only be used if other high level scheduling strategies cannot give the desired" placement.
- **Placement groups.** Reserve bundles of resources across nodes. `STRICT_SPREAD`: "Each bundle must be scheduled in a separate node." On node failure, "Ray tries to reschedule the lost bundles on different nodes", and actors that used a lost bundle are rescheduled "based on their fault tolerant policy once Ray recovers the bundle." If there is no room, "the placement group remains in the partially created state indefinitely."
- **Labels** (beta, Ray 2.49 and later). `ray start --labels="role=relay"` and `label_selector={"role": "relay"}` on the actor; operators are equals, `!value`, `in(...)`, `!in(...)`.

The documentation has no dedicated anti-affinity option for "at most one actor of this class per node". The two ways to get it are a `STRICT_SPREAD` placement group, or a custom resource of which every node offers exactly one unit and every actor requires one **(the second is inferred from how resources work)**. We use the second because it needs no extra object to manage (design, section 3).

The fault-tolerance guide also warns: "avoid using custom resource requirements that only particular nodes can satisfy. If that particular node fails, Ray won't retry the running tasks or actors." Our `relay_slot` resource is offered by every relay node, so it does not tie an actor to one machine.

## 5. Fault tolerance for actors

This is the part of Ray the project leans on most.

### 5.1 Restarts

- "The default value of `max_restarts` is 0, meaning that the actor won't be restarted. If set to -1, the actor will be restarted infinitely many times."
- What triggers a restart: the actor process crashing unexpectedly, or its node failing. After a node failure the recovery mechanisms use "other worker nodes", and the actor "can be restarted on a different node".
- A restarting actor may wait: the documentation lists "it's waiting for resources or running the class constructor during the restart" as a reason for an actor being unavailable. So an actor whose node died stays pending until some node has room for it **(inferred; to test that it waits indefinitely)**.
- **State is not restored.** "When an actor is restarted, its state will be recreated by rerunning its constructor", and `max_restarts` "doesn't automatically restore application level state in your actor." Recovering state is the application's job.
- The documented way to recover state is checkpointing, in two variants: the creator keeps a checkpoint and re-creates the actor, or the actor writes a checkpoint and reloads it in its constructor. For the second: "make sure it's accessible to the entire cluster since the actor can be restarted on a different node" (S3 or NFS are the examples).
- **Detached actors** "will still be restarted by Ray even if their original creator dies", until the restart limit is reached, the actor is destroyed, or the cluster is destroyed.
- **Manual kill.** `ray.kill(handle)` makes the actor "immediately exit its process", without running `__ray_shutdown__()` or `atexit` handlers. It restarts afterwards only with a nonzero `max_restarts` and `no_restart=False`. `ray.actor.exit_actor()` is a graceful, intended exit; whether it triggers a restart is not stated, and we assume it does not.
- **Out of memory.** A memory monitor in each raylet kills workers when node memory passes a threshold (`RAY_memory_usage_threshold`, default 0.95). For actors, "it respects max_restarts".

The checkpoint approach does not fit us. A checkpoint store reachable by the whole cluster and holding every mailbox is the permanent central database that requirement R9 forbids. Our relays recover differently: a restarted relay starts empty and copies what it should hold from the other relays that have it. Ray supplies the restart; replication supplies the state.

### 5.2 Call semantics and retries

- Default is at-most-once (`max_task_retries=0`). A failed call raises `RayActorError`, which "may be thrown even though the task did indeed execute successfully."
- With `max_task_retries` set to a positive number or -1, calls become at-least-once: "Retried methods may execute twice, once on the failed actor and a second time on the restarted actor." Only incomplete calls are retried. "If the actor is currently restarting when a task is submitted, this will count for one retry."
- The setting can be given on the class, at actor creation, per method with `@ray.method`, or per call with `.options()`; the most specific wins.
- `retry_exceptions` extends retries to application exceptions, and then the method must be idempotent: "invoking it multiple times should be equivalent to invoking it only once."

Our relay operations are idempotent by construction (an envelope is identified by the hash of its content, and storing it twice has the same effect as storing it once), so Ray's at-least-once retries are safe for us and we enable them.

### 5.3 Errors a caller sees

- `ActorUnavailableError`: "the actor isn't accessible at the moment, but may recover after waiting and retrying." Typical causes are a restart in progress, transient network trouble, or "the actor is dead, but the death hasn't yet been reported to the system." There is "no guarantee on whether the actor executed the task or not."
- `ActorDiedError`: the actor is confirmed dead.
- Recommended reaction: "if the caller gets the `ActorUnavailableError` error, it should 'quarantine' the actor and stop sending traffic to the actor. It can then periodically ping the actor until it raises `ActorDiedError` or returns OK."

## 6. Failure of nodes

| What dies | Documented effect |
|---|---|
| **A worker node** | "all the running tasks and actors will fail and all the objects owned by worker processes of this node will be lost" |
| **A raylet** | Same as the node dying; a restarted raylet is a new node |
| **An actor's worker process** | The actor dies; restarted if `max_restarts` allows |
| **The head node** | "the entire Ray cluster fails" |

## 7. The head node

By default "the GCS isn't fault tolerant because it stores all data in memory." Ray can persist GCS state to an external Redis so that a restarted head recovers, but this "is officially supported only if you are using KubeRay"; elsewhere it is "at your own risk". While the GCS is down, running actors stay alive, and raylets wait up to 60 seconds for it before exiting.

**The examiners have stated that the head node will not be killed in phase 1.** We therefore run a single head with no Redis and do not build head recovery ([ADR-0012](../decisions/0012-head-node-policy.md)). We still state the limitation openly: the head is a single point of failure for the cluster's control plane. It holds no messages, mailboxes or keys.

Phase 2 removes Ray, and with it the head. Whatever replaces Ray's membership service in phase 2 must not have a single point of failure and must tolerate partitions.

## 8. Deploying Ray by hand in Docker

We cannot use Docker Compose, and we do not need it. One command per node:

```bash
# head container
ray start --head --port=6379 --num-cpus=0 --dashboard-host=0.0.0.0 --block

# relay containers (all identical)
ray start --address=linkchat-head:6379 --num-cpus=1 --resources='{"relay_slot": 1}' --block

# client containers
ray start --address=linkchat-head:6379 --num-cpus=0 --block
```

- `--block` keeps the command in the foreground, which a container entrypoint needs.
- A Python program on a node joins the running cluster with `ray.init(address="auto", namespace="linkchat")`.
- **Finding the head.** On a user-defined Docker network, containers resolve each other by container name through Docker's built-in DNS, without Compose. The head's container name is the only address configured anywhere.
- **Ports.** Ray needs two-way communication between all nodes on several ports (6379 for the GCS, 8265 for the dashboard, a node manager port, an object manager port, worker ports from 10002 up). On one Docker network all ports are reachable between containers; only the dashboard and each client's web UI are published to the host.
- **Images.** Official images are `rayproject/ray:<version>-py311-cpu`; the alternative is `python:3.x-slim` with `pip install "ray[default]"`. The `default` extra is needed for the dashboard, the state CLI and job submission.
- **Shared memory.** Each node's object store lives in `/dev/shm`. Since our envelopes are passed inline, the object store can be small; the practical minimum for `--shm-size` and `--object-store-memory` is something to measure.
- **Operating system.** Nodes are Linux containers. Ray on a Windows host is beta and multi-node Windows clusters are untested, so nothing of Ray runs on the host.

**Ray Client is not for us.** `ray.init("ray://head:10001")` connects a script outside the cluster through a proxy. The documentation says it has architectural limitations and ends the workload if the connection drops for 30 seconds. A messaging client runs for hours, so our client program runs inside the cluster as a driver on its own node.

## 9. Security model

- "Ray allows any clients to run arbitrary code."
- "Security and isolation must be enforced outside of the Ray Cluster. Ray expects to run in a safe network environment and to act upon trusted code."
- **Token authentication** (`RAY_AUTH_MODE=token`): a shared secret for all nodes and clients, the default for all clusters from Ray 2.61. It "is not an alternative to deploying Ray clusters in a controlled network environment".
- **TLS** (`RAY_USE_TLS=1` and certificate variables) encrypts traffic between Ray processes.

A Ray cluster is a single administrative domain. In phase 1 we cannot describe relays as machines run by parties who distrust each other. End-to-end encryption still means a relay never holds plaintext or keys, but the threat we can honestly defend against is a relay that follows the protocol and would read what it stores if it could, not an attacker in control of a relay machine.

## 10. Every Ray feature, and whether it applies to us

The examiners' rule is to use what applies naturally and not to force the rest. This table is our answer, feature by feature.

### Used

| Ray feature | What we use it for |
|---|---|
| Cluster membership and joining (GCS, raylets) | All discovery and liveness. No gossip, no seed lists, no failure detector of our own |
| Actors | Each relay is one actor holding mailboxes in memory |
| Async actors | Relays wait on calls and timers; hundreds of recipients can wait for mail on one relay |
| Concurrency groups | Separate limits for client operations, parked "wait for mail" calls, and repair traffic, so repair cannot starve clients |
| Named actors, namespace, `get_if_exists` | The relay directory: `relay-0` ... `relay-4` in namespace `linkchat`. Clients find relays by name |
| Detached lifetime | Relays outlive the bootstrap script that creates them |
| `max_restarts=-1` | Ray supervises relays: a crashed relay process restarts in place, a relay on a dead node restarts on another node |
| `max_task_retries` | Transient failures during a restart are retried by Ray, safely because our operations are idempotent |
| Custom resources (`relay_slot`) | At most one relay per node, and no relays on client nodes or the head |
| `ActorUnavailableError` / `ActorDiedError` and the quarantine advice | The client's rule for skipping a relay and trying the next |
| Actor handles passed between actors | Relay-to-relay repair calls |
| Inline argument passing | Envelopes travel by value and never enter the object store |
| Token authentication | Cluster access control |
| Dashboard, state CLI (`ray list actors`, `ray list nodes`, `ray status`) | Watching relays die, restart and move during the demonstration. The CLI is stable; the Python SDK is a developer API, so we use the CLI |
| Application metrics (`ray.util.metrics` Counter, Gauge, Histogram) | Per-relay counts of stored envelopes, expirations, repairs; exported in Prometheus format |
| Ray Jobs (`ray job submit`) | Running the one-off bootstrap script and the load generator on the cluster |
| Tasks | Load generation and fault-injection helpers in the test harness |
| Memory monitor with `max_restarts` | A relay that exceeds memory is killed and restarted by Ray; we add a mailbox quota so this is a last resort |

### Considered and not used, with the reason

| Ray feature | Why not |
|---|---|
| Object store and `ray.put` for envelopes | Ownership: an object dies with the process that created it, which is the sender. Envelopes are small and go inline |
| Actor checkpointing to shared storage | It would be a durable store of every mailbox, which R9 forbids. Relays recover from their peers |
| `ray.util.queue.Queue` | It is one actor wrapping an `asyncio.Queue` (beta). A mailbox needs replication, acknowledgement and expiry, which it does not have. Using it would mean a single unreplicated holder per mailbox |
| Placement groups (`STRICT_SPREAD`) | Would give one-relay-per-node, but adds an object that can get stuck "partially created". The `relay_slot` resource gives the same guarantee with less machinery |
| Node affinity | Discouraged by the documentation unless nothing else works, and it would stop Ray from moving a relay to a healthy node |
| Labels | Beta. The custom resource already identifies relay nodes |
| Ray Serve | A shared HTTP entry point in front of the relays adds a central component. Each user's client is that user's own program with its own keys; it is not a shared service to scale |
| Ray Client (`ray://`) | Discouraged for long-running programs |
| Streaming generators | A candidate for pushing mail to a waiting client. A parked call with a timeout does the same with less risk; revisit if time allows |
| GCS fault tolerance (Redis) | The head is not killed in phase 1 |
| Autoscaler | Fixed set of containers started by hand |
| Ray Data, Train, Tune, RLlib, Workflows | Machine-learning and batch libraries; nothing in the project matches |

### What Ray does not offer at all

No feature of Ray replicates application data between actors, decides which actors hold a given key, orders messages, removes duplicates, expires data, or encrypts end to end. Those are ours in both phases. Section 5.1 quotes Ray's own statement on state: "the application is responsible".

## 11. Ray's design patterns and anti-patterns

Ray's documentation lists 8 patterns and 13 anti-patterns. The ones that touch our code:

| Pattern or anti-pattern | How it applies |
|---|---|
| **Using asyncio to run actor methods concurrently** | A long-running loop in a synchronous actor blocks every other call. Relays are async actors, so the repair loop, TTL sweeper and client calls interleave |
| **Using an actor to synchronize other tasks and actors** | `asyncio.Event` inside a zero-CPU actor. Our `wait_for_mail` is this pattern |
| **Using a supervisor actor to manage a tree of actors** | A supervisor owns its workers, and "if the supervisor dies, its workers are terminated". We do not use it: relays are detached and supervised by Ray itself, so that no application supervisor becomes a single point of failure |
| **Using `ray.wait` to limit the number of pending tasks** | Backpressure. The load generator caps in-flight calls this way; the client caps its outbox |
| **Pipelining** | Issue the next request before processing the current result. Used in the repair loop when pulling ranges from peers |
| **Anti-pattern: `ray.get` in a loop** | Sending to three relays must be three calls issued together and awaited together, never one after the other |
| **Anti-pattern: processing results in submission order** | Wait for replies in completion order, so one slow relay does not delay the other two |
| **Anti-pattern: returning `ray.put()` references** | Ties the value's life to the worker that created it. Relay methods return values directly |
| **Anti-pattern: passing the same large argument by value repeatedly** | Applies above about 100 KB. Our envelopes are fixed-size and far smaller, so by-value is correct |
| **Anti-pattern: global variables to share state** | Each actor is a separate process. Shared state lives in actors, never in module globals |
| **Anti-pattern: redefining a remote class in a loop** | Define `RelayActor` once at module level |
| **Anti-pattern: forking new processes** | Forked children inherit Ray's sockets. The client must not use `fork`; Uvicorn runs in the driver process with a single worker |
| **Anti-pattern: serializing `ObjectRef`s out of band** | We never send references outside Ray |
| **Tasks too fine-grained; nested tasks; generators for heap memory** | About batch computation; not relevant to relays |

## 12. What we have not verified and must test in a spike

In order of how much of the design depends on each:

1. **Restart after node death.** With `max_restarts=-1` and `resources={"relay_slot": 1}`, kill a relay's container: does the actor restart on a spare relay node, how long does it take, and does it stay pending (not fail) when no spare node exists and start when one is added?
2. **Restart after process death.** `kill -9` the actor's worker process: restart time on the same node.
3. **Handles and names across restarts.** Does an existing handle keep working after a restart, and does `ray.get_actor("relay-0")` return the restarted actor?
4. **What a caller sees and when.** Time until a call to a killed relay raises, and which exception, with `max_task_retries` at 0 and at a small positive value. This sets the client's timeout.
5. **Background loops in an async actor** started from `async def __init__` with `create_task`, including what happens to the actor if such a task raises.
6. **Concurrency groups on an async actor**: that parked `wait_for_mail` calls do not consume the limit of the client-operations group.
7. **Memory and process count of an idle Ray node** in a container, and how many nodes fit on the demonstration laptop. The target topology is one head, five to seven relay nodes and two or three client nodes.
8. **Smallest workable `--object-store-memory` and `--shm-size`.**
9. **Envelopes of our fixed size are passed inline** and never written to the object store.
10. **Token authentication across containers** on the pinned Ray version.
11. **`ray job submit` from the head container** for the bootstrap script, and that the detached actors it creates survive the job's end.
12. **`ray.util.metrics`** from inside a relay actor, and where the Prometheus endpoint is exposed.

---

## Sources

Official Ray documentation, read 2026-11-11:

- [Ray Core key concepts](https://docs.ray.io/en/latest/ray-core/key-concepts.html) · [Cluster key concepts](https://docs.ray.io/en/latest/cluster/key-concepts.html) · [Glossary](https://docs.ray.io/en/latest/ray-references/glossary.html)
- [Actors](https://docs.ray.io/en/latest/ray-core/actors.html) · [Named actors](https://docs.ray.io/en/latest/ray-core/actors/named-actors.html) · [AsyncIO and concurrency for actors](https://docs.ray.io/en/latest/ray-core/actors/async_api.html) · [Concurrency groups](https://docs.ray.io/en/latest/ray-core/actors/concurrency_group_api.html) · [Actor task execution order](https://docs.ray.io/en/latest/ray-core/actors/task-orders.html) · [Terminating actors](https://docs.ray.io/en/latest/ray-core/actors/terminating-actors.html) · [Out-of-band communication](https://docs.ray.io/en/latest/ray-core/actors/out-of-band-communication.html)
- [Objects](https://docs.ray.io/en/latest/ray-core/objects.html) · [Serialization](https://docs.ray.io/en/latest/ray-core/objects/serialization.html) · [Ray generators](https://docs.ray.io/en/latest/ray-core/ray-generator.html) · [ray.wait](https://docs.ray.io/en/latest/ray-core/api/doc/ray.wait.html) · [ray.util.queue.Queue](https://docs.ray.io/en/latest/ray-core/api/doc/ray.util.queue.Queue.html)
- [Scheduling](https://docs.ray.io/en/latest/ray-core/scheduling/index.html) · [Resources](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html) · [Placement groups](https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html) · [Labels](https://docs.ray.io/en/latest/ray-core/scheduling/labels.html) · [Out-of-memory prevention](https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html)
- [Fault tolerance overview](https://docs.ray.io/en/latest/ray-core/fault-tolerance.html) · [Actor fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html) · [Node fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/nodes.html) · [GCS fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/gcs.html) · [Object fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/objects.html) · [Task fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/tasks.html)
- [Design patterns and anti-patterns](https://docs.ray.io/en/latest/ray-core/patterns/index.html) (all 21 pages) · [Tips for first-time users](https://docs.ray.io/en/latest/ray-core/tips-for-first-time.html)
- [Launching an on-premise cluster](https://docs.ray.io/en/latest/cluster/vms/user-guides/launching-clusters/on-premises.html) · [Configuring Ray](https://docs.ray.io/en/latest/ray-core/configure.html) · [Cluster management CLI](https://docs.ray.io/en/latest/cluster/cli.html) · [Installation](https://docs.ray.io/en/latest/ray-overview/installation.html) · [Ray Jobs overview](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/index.html) · [Ray Client](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html)
- [Security](https://docs.ray.io/en/latest/ray-security/index.html) · [Token authentication](https://docs.ray.io/en/latest/ray-security/token-auth.html)
- [Adding application-level metrics](https://docs.ray.io/en/latest/ray-observability/user-guides/add-app-metrics.html) · [State CLI and SDK](https://docs.ray.io/en/latest/ray-observability/user-guides/cli-sdk.html) · [State API reference](https://docs.ray.io/en/latest/ray-observability/reference/api.html) · [ray.nodes](https://docs.ray.io/en/latest/ray-core/api/doc/ray.nodes.html)

**On example projects.** We looked for open-source projects that build a replicated store, a queue or a chat system directly on Ray Core actors and did not find one worth citing; public Ray projects are overwhelmingly machine-learning pipelines. The worked examples we learned from are the ones inside the documentation: the self-checkpointing `ImmortalActor` and the creator-managed `Controller` on the actor fault-tolerance page, the `SignalActor` on the actor-sync pattern page, the detached-actor example that survives its creator being killed, and the HTTP-server-in-an-actor example on the out-of-band page. Studying Ray Serve's controller, which is itself a detached actor that supervises replicas, is a possible follow-up if we need a reference for long-running actor services.

Secondary sources, used only for background on Ray's internals: [Ray v2 architecture notes](https://gist.github.com/cyb3rh4wk/26301765b2cfcf8210559fd8a77fdfe7) · [Ray Core system design: a deep dive](https://nkkarpov.github.io/blog/ray-core-system-design/) · [Setting up Docker as a virtual Ray cluster (Ray forum)](https://discuss.ray.io/t/setting-up-docker-as-a-virtual-ray-cluster/21949)
