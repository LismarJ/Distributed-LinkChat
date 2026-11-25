# The demonstration, start to finish

**Status:** to be rehearsed · **Written:** 2026-11-24 · **For:** the 2026-12-01 delivery
**Task:** S6 in [`../planning/phase-1-work-plan.md`](../planning/phase-1-work-plan.md)
**What it demonstrates:** the eight items of that plan's section 1, in the order they are easiest to show

---

## 0. Why this document is written out in full

The delivery requires the demonstration to run "start to finish without improvisation, three times in a row". Anything decided in the room is a thing that can go wrong in the room, so every command, every thing to point at, and every sentence that explains *why* a result holds is written here in advance. Read it once before the defense; follow it verbatim during.

Two people present. One drives the terminal, the other drives the browsers and does the talking. Swap for the second rehearsal, because the plan's last risk is "everything works and nobody can explain it", and the cure is each person having presented the other's track at least once.

## 1. Pre-flight, the night before

| Check | Command | Expected |
|---|---|---|
| The suite is green | `scripts/check.sh` | `all checks passed`; 481 passed, 20 skipped |
| Docker is running | `docker version` | a server version, not a pipe error |
| The image builds | `scripts/up.sh --build` | ends with `the cluster is up` |
| The scenarios pass | `scripts/scenario.sh all` | seven `PASS` lines in the summary |
| Clean slate again | `scripts/down.sh` | removes the containers, keeps the volumes |

The 20 skipped tests are the client crash-consistency cases. They need `fork` and `SIGKILL`, so they are skipped on a Windows host and run on Linux. If the defense machine is Linux, expect 501 passed and no skips. Say this if asked rather than letting a grader find a skip and wonder.

**Record the resource measurements during rehearsal.** The target setup has ten containers: one head, seven relay nodes and two clients. `--shm-size 1g` per container is already set in `scripts/env.sh` for the Ray object store. Record Docker Desktop peak memory use; do not use estimates as rehearsal results. The **image size is measured: 498,384,886 bytes (475 MiB)**.

**Timing.** A cold `--build` is the slow part. Build the night before and never during the defense. Bring-up from a built image takes about 40 seconds.

> **Never edit `pyproject.toml` on the day.** The image's cached dependency
> layer is keyed on that file, so touching it forces a full re-download of Ray
> — measured at about 13 minutes on a slow connection. Nothing else in the
> repository has that effect.

**The two recovery numbers, measured 2026-11-25.** These are what the pacing of beats 3 and 4 depends on, so they are written here rather than left to be discovered on the day:

| What dies | Time until all five relays are READY again |
|---|---|
| A relay **container** | **~31 s** (three runs: 30.7, 31.1, 31.1) |
| A relay **worker process** | **~1.3 s** (two runs: 1.35, 1.26) |

The difference is the whole story and worth saying out loud: Ray notices a dead *worker* almost at once, but a dead *node* only through its health check. **Thirty seconds is a long silence in a demonstration**, so begin the explanation in beat 3 the moment the kill lands rather than waiting for green — the script is polling and prints `PASS` when it is done. Do not let a grader watch a blank pause and conclude something is stuck.

**Record recovery times during rehearsal:** time a killed relay container from docker kill until all relay statuses report READY, then repeat for a killed worker process. Measure both on the demonstration machine.

## 2. Bring it up

```bash
scripts/up.sh
```

One command, and it waits at each step rather than sleeping: the head's Ray instance answering, seven relay nodes joined, five relay actors READY, both clients answering on their ports. It prints the three URLs at the end.

Say while it runs: **every container is its own `docker run` and they may be started in any order.** No Compose anywhere, which is the course rule; a node needs to be told nothing but the head's container name, because Docker's DNS on a user-defined network resolves it.

Then open, side by side:

- `http://localhost:8100` — client 0, call her Alice
- `http://localhost:8101` — client 1, call him Bob
- `http://localhost:8265` — the Ray dashboard, on a third screen or tab

Point out, before anything else happens: the right-hand panel in each UI lists the five relays in **preference order for this conversation**, with the first three marked `primary`. That panel is where every failure in this demonstration becomes visible.

## 3. Introduce the two users

Each UI shows its own user id at the top; click it to copy. Paste Alice's id into Bob's "Invite a contact" box and Bob's into Alice's.

Say: **a user id is the hash of a long-term identity key, not a name chosen by anyone.** A relay never learns it. What a relay sees is a mailbox identifier, and there are two per conversation, one per direction ([ADR-0007](../decisions/0007-mailbox-identifiers.md)), so a relay cannot even tell that two mailboxes belong to the same pair of people.

## Beat 1 — an ordinary encrypted exchange

**Do.** Alice sends "hola bob". Bob replies.

**Watch.** The message appears in Alice's thread as `pending` and turns to `sent ✓✓` within a second. It arrives in Bob's thread marked `received`, with no page refresh.

**Say.** `sent` is not "the network accepted it". It means **three distinct relays have confirmed holding it**. That is the number the whole fault-tolerance argument rests on: five relays, three copies, so any two can fail and a copy survives ([ADR-0005](../decisions/0005-three-copies-before-sent.md)). The update arrives over a WebSocket that only says "this conversation changed"; the browser then refetches, so a dropped notification costs a stale view and never a wrong one.

**If asked what the relays can see:** run `docker exec linkchat-relay-0 python -c "print('relays hold ciphertext only')"` — or better, just point at the relay panel's counters, which are counts and nothing else. A relay holds a fixed-size padded envelope under an opaque id, in memory, with no operation that returns anything already acknowledged or expired. `tests/test_client.py::test_relays_only_ever_see_fixed_size_ciphertext` is the check: every envelope is the same number of bytes whatever the message, and no fragment of the plaintext appears in it.

## Beat 2 — the recipient is not there (requirements R3 and R4)

**Do.**

```bash
docker stop linkchat-client-1          # Bob's container, gone
```

Alice sends two messages: "while you were out" and "second one".

```bash
scripts/up-client.sh 1                 # Bob comes back
```

**Watch.** Alice's two messages reach `sent` while Bob is stopped. When Bob's UI reloads, both messages are there, in the order Alice sent them.

**Say.** This is the asynchronous requirement, and it is the reason the relay tier exists at all. Alice's send does not need Bob to be anywhere: it needs three relays. Bob's history is on his own volume, so stopping the container loses nothing. Note that there is **no direct client-to-client path in phase 1, by decision** ([ADR-0014](../decisions/0014-phase-1-scope-cuts.md)): every message goes through relays without exception, which is a property worth claiming rather than an omission.

**The scripted version, if a grader wants it checked rather than shown:**

```bash
scripts/scenario.sh recipient-offline
```

It does the same thing and asserts the outcome: the recipient's container is killed, the send happens while it is down, the container comes back, and the message is then there **exactly once** with the sender reading `sent`. Added 2026-11-25, because every other scenario had the recipient online and this is the one requirement the relay tier exists for.

## Beat 3 — two relay containers, killed together

This is the beat the examiners added the requirement for. Do it with a conversation in flight.

**Do.**

```bash
scripts/scenario.sh two-nodes
```

**Watch.** The script prints what it kills, then `delivered exactly once`, `the sender reports 'sent'`, the history unchanged, and `PASS two-nodes`. Meanwhile, in the UIs: two entries in the relay panel go red (`unreachable`), then the Ray dashboard shows the lost actors restarted on spare nodes, then those entries go amber (`syncing`) and finally green (`ready`). Record the recovery time during rehearsal; use the measured value to pace the explanation.

**Say, and this is the central claim of the whole project.** Both relays die in **one `docker kill` call**, so there is no window between them in which the system could repair — killing them one after the other would test the one-failure case twice and prove nothing. A message confirmed before the kill was on three relays, at most two of which just died, so at least one holder is alive. The reader asks three relays and merges, and three plus three is greater than five, so a reader's set and a writer's set always overlap. That is why nothing is lost.

Then the recovery, which is the part worth watching: **Ray restarts each lost actor on a node with a free `relay_slot`** — there are seven relay nodes for five relays, so two spares, exactly so that two killed containers can both be replaced with nobody starting anything ([ADR-0011](../decisions/0011-relays-as-ray-supervised-actors.md)). The replacement runs its constructor again and comes back **empty**, so it reports `SYNCING` and **refuses client work** until it has pulled its share from its peers. Serving while empty would make a reader believe a mailbox is empty when it is not, which is a wrong answer rather than a slow one, so we pay the sync time.

If a grader asks what the amber state costs: the relay is unavailable for the length of its sync on top of Ray's restart time, and callers treat `SYNCING` exactly like unreachable and move to the next relay on the preference list.

## Beat 4 — two relay processes, killed together

**Do.**

```bash
scripts/scenario.sh two-processes
```

**Watch.** `PASS two-processes`. The containers stay up throughout — `docker ps` still lists all seven.

**Say.** The design expects a dead **container** to cause the actor to restart on another node, while a dead **worker process** should restart in place. Verify both paths during rehearsal. The gateway maps relay failures to `RelayUnreachable`, `RelayGone` or `RelaySyncing` so Ray-specific errors do not cross into the core ([ADR-0013](../decisions/0013-ray-free-core-for-phase-2.md)).

**Then, optionally, the mixed case:** `scripts/scenario.sh one-of-each`.

## Beat 4b — sending *while* two relays are already down

Short, and it proves a different claim from beats 3 and 4, so do not skip it if there is time. Those two kill relays *after* a message was confirmed, which tests that a confirmed record survives. This one kills first and sends afterwards, which tests the **write** path.

**Do.**

```bash
scripts/scenario.sh send-while-down
```

**Say.** With two relays gone, the three primaries of this mailbox cannot all answer, so the send **falls through the preference list to stand-ins**. A stand-in stores the record with a hint naming the primary it is covering for, and the send still collects three confirmations and reports `sent`. Everyone computes that preference list independently from the same fixed relay set with the same hash, so two senders writing the same mailbox during the same failure pick the *same* stand-in and the copies land together instead of scattering.

Point at the `standing in` counter in the relay panel: it is non-zero while the primaries are away, and it returns to zero once they are back, because a stand-in hands each record back to its primary and only then drops its own copy. Never the other way round — if it dropped first and the handoff failed, the record would be gone. That ordering is one of the two things the fuzz harness in section 7 was shown to catch when it is reversed.

## Beat 5 — wipe every relay

**Do.**

```bash
docker kill $(docker ps -q --filter "label=linkchat.role=relay")
scripts/up-relay-node.sh
scripts/bootstrap.sh
```

**Watch.** Every relay comes back with zero records. Both UIs still show the entire conversation.

**The scripted version:**

```bash
scripts/scenario.sh wipe-all-relays
```

Every relay hosting an actor goes down in **one `docker kill`**, the containers are brought back, and then both clients' histories are compared against what they held before — as whole strings, so loss, duplication *and* reordering all fail the check. Measured at about 43 seconds end to end. Added 2026-11-25.

Worth knowing why the duplication half is not redundant: wiping the relays destroys their **tombstones** as well as their records, so anything that came back would be offered to the client again, and the only thing refusing it is the client's persisted seen-set. This beat therefore tests duplicate suppression too, not just durability.

**Say.** This is requirement R9 and it is **structural, not a promise**. Relays are memory only and hold a record until it is acknowledged or until its TTL expires, whichever comes first, and expiry needs no other process to be alive. The conversation lives in the two clients' SQLite files. So a relay cannot reconstruct a conversation because it cannot be asked to: there is no history operation in the interface at all. `tests/test_fuzz_two_failures.py::test_a_relay_wipe_costs_nothing_the_clients_already_have` is the same claim as a test.

## Beat 6 — the client and a relay killed after the send is queued

**Do.**

```bash
scripts/scenario.sh client-mid-send
```

**Say.** The scenario first waits for the API to confirm that the message and its outbox row are committed, then kills the client and a relay together. It verifies recovery after a durably queued send, but the send may already have completed before the kill lands. The outbox row and message share a SQLite transaction, and `store` is idempotent on the envelope id, so retries after an ambiguous failure collapse into the copy that may already be there. The separate outbox and history tests cover retry after restart and crash consistency.

This matters because Ray's documentation is explicit that an actor call can raise *although it did execute*, with no way to tell which happened. We never treat a failure as "it did not happen"; we retry, and idempotence makes retrying free.

## 7. The randomized evidence

Worth having on screen if the conversation turns to "how do you know?".

```bash
uv run pytest tests/test_fuzz_two_failures.py -q
```

**Say.** The argument for why a reader always finds a confirmed record is written out in [`../design/fault-tolerance.md`](../design/fault-tolerance.md) section 5, and that document says plainly that it is an argument and not a proof, whose weak point is timing: a relay that is slow rather than dead can be skipped by a writer and still be READY for a reader. So it is tested by injecting random damage — 40 seeded scenarios, two relays down at once, wipes, restarts, dropped calls, latency, and a recipient who goes offline for stretches — and asserting after each run that every confirmed message arrived exactly once and in a consistent order. Run the test during rehearsal and record its output here.

**Be ready to volunteer the embarrassing part**, because it is the most honest thing we can say about our own testing: the first version of that harness passed every seed and so did a deliberately broken replication rule, because it healed everything before asserting. It only became evidence once it was rewritten and shown to **catch** a planted durability bug — dropping a stand-in's copy before the primary confirms the handoff — which it does at seed 21. A fault-injection suite that has never failed is evidence of nothing.

## 8. What we deliberately did not build

Name these rather than wait to be caught by them. Each one's full reasoning is in [ADR-0014](../decisions/0014-phase-1-scope-cuts.md).

| Not built | Why, and what it costs |
|---|---|
| Periodic digest comparison between relays | The only cut that touches correctness, so it gets an argument. Sync-on-restart and hinted handoff already cover a replaced relay and an unreachable primary. What digest comparison would have added is catching a tombstone that reached only two of three holders. Without it that relay keeps the envelope until its TTL and offers it again, and the recipient's seen-set discards it: deletion becomes less prompt, not less certain. Expiry was always the guarantee, not acknowledgement |
| Any direct client-to-client path | In phase 1 a client is a Ray driver and has no address another process can call. A direct path would need a per-client inbox actor (new failure semantics, lost on restart, relay fallback needed anyway) or hand-written sockets, which is what phase 1 forbids. "Both online" is never knowable, only "was reachable a moment ago" |
| Read receipts | Delivery states stop at `sent` and `received`. The state that proves the distributed property is `sent` |
| Several padding size classes | One fixed envelope size for every message. Simpler and leaks nothing |
| Streaming delivery | A parked `wait_for_mail` plus a fetch does the same with less risk |
| A React build step for the UI | [ADR-0016](../decisions/0016-plain-browser-ui-without-a-build-step.md). Three static files, no toolchain and no network at runtime, so the demonstration cannot fail on an `npm install` or a CDN. Cost: no component model and no TypeScript on the frontend, which is a real ceiling this UI does not reach |

Also be honest about the two things we know are weak: a relay whose clock runs fast expires records early, because `expires_at` is an absolute instant compared against each relay's own clock (accepted for phase 1, `fault-tolerance.md` case 10); and the credential carried on every relay call is **not yet checked**, because ADR-0007 is still open. A relay therefore trusts any caller who knows a mailbox id, which is a real gap and is written down as one.

## 9. If something fails mid-demonstration

Do not debug in the room. Each beat is independent, so the recovery is always: reset to a known cluster and skip to the next beat.

| Symptom | Do this |
|---|---|
| A scenario prints `FAIL` | Read the one-line reason aloud — it names which assertion failed — then `scripts/up.sh` and move to the next beat. An honest failure explained beats a silent retry |
| Relays stuck `syncing` | `scripts/up.sh` waits for READY and will tell you if they never get there. Then `scripts/status.sh` and `ray list actors` on the dashboard |
| A client UI shows "reconnecting…" | Its container is down or restarting; `scripts/up-client.sh <n>`. The UI reconnects by itself once it answers |
| "no relay has this contact's keys yet" on a first send | The recipient has never been online, so no relay has their prekey bundle. Start their container and wait for the republisher (60 s), then send again |
| Everything is confused | `scripts/down.sh` then `scripts/up.sh`. Keeps the client volumes, so the histories survive — which is itself worth pointing out |
| Truly stuck | `scripts/down.sh --volumes` for a clean slate, then re-introduce the two users. Costs the conversation history |

## 10. Timing

| Beat | Target |
|---|---|
| Bring-up (image already built) | 2 min |
| Introduce the users, beat 1 | 3 min |
| Beat 2, recipient offline | 3 min |
| Beat 3, two containers | 5 min |
| Beat 4, two processes | 3 min |
| Beat 4b, sending through stand-ins | 2 min |
| Beat 5, wipe the relays | 2 min |
| Beat 6, queued send plus client and relay failure | 3 min |
| Randomized evidence, section 7 | 3 min |
| What we did not build, and questions | 6 min |
| **Total** | **~32 min** |

If the slot is shorter, cut in this order: beat 6, then the mixed `one-of-each`, then beat 4b, then section 7 (keep the sentence about the harness having been shown to catch a planted bug — it is 20 seconds and it is the strongest thing we can say). **Never cut beats 3 and 4**: they are the requirement the examiners added.

## 11. Checks still needed before the defense

This runbook is written before rehearsal. Verify the following and update the research note with measured results:

- **The credential carried on every relay call is not checked**, because ADR-0007 is still open. A relay trusts any caller who knows a mailbox id. Mailbox ids are derived per direction and are not guessable from a user id, so this is not wide open, but it is not access control either.
- **`ray.util.metrics` is not wired up** (R9), so the relay counters are visible through `GET /relays` and the UI but not in Prometheus.
- **Token authentication between containers has not been tested.** The cluster runs on a user-defined Docker network and is not exposed beyond the published client ports and the dashboard.
- **The client-mid-send scenario does not reliably kill the client during its relay call.** It queues the message durably, then kills the client and a relay together; the send may have completed before the kill lands. Present it as a simultaneous two-element failure test. The outbox recovery and crash-consistency claims are covered by the dedicated history and outbox tests.
