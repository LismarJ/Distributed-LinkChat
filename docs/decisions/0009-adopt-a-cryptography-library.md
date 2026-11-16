# ADR-0009: Adopt an existing library for X3DH and the Double Ratchet

**Status:** accepted · **Date:** 2026-11-12 · **Deciders:** project team
**Applies to:** both phases · **Follows from:** [ADR-0002](0002-python-backend-react-frontend.md)

## Context

Requirement R8 needs authenticated end-to-end encryption, and R3 needs a session that can be started with someone who is offline, which is what X3DH exists for. Until now the plan left open whether to implement X3DH and the Double Ratchet from the published specifications or adopt a library.

Two things decided it:

- **The schedule.** The budget in [`../planning/phase-1-work-plan.md`](../planning/phase-1-work-plan.md) section 4 put implementation from the specifications at about 6 person-days against 40 available, with the rest of the system needing about 31. That leaves no slack for the spike invalidating something.
- **The failure mode.** Cryptography written from a specification is the one area of this project where plausible and correct look identical from the outside. A subtly wrong ratchet passes a demonstration and is wrong anyway.

The graded content of this project is the distributed-systems design, not the cryptographic primitives. [ADR-0001](0001-two-tier-relay-overlay-architecture.md) already lists the Double Ratchet as a mechanism we take from Signal rather than invent.

## Decision

**Adopt a maintained Python implementation. Do not write X3DH or the Double Ratchet ourselves, in either phase.**

Candidates, in order of preference:

| Library | What it covers | Notes |
|---|---|---|
| `X3DH` (Syndace, PyPI `X3DH` 1.0.x) | The asynchronous handshake | Depends on the native libraries `libxeddsa` and `libsodium`, which the Docker image must carry. Supports Ed25519 and Curve25519 |
| `DoubleRatchet` (Syndace, PyPI `DoubleRatchet`) | The ratchet, with Curve25519 and Curve448 modules over `cryptography` | Used together with the above |
| `python-omemo` (Syndace) | Both of the above, pre-configured | Only if we want its parameter choices; it is built for OMEMO, not for our envelope |

**First task of the cryptography work, before any code depends on it:** confirm for each library the licence, the last release date and whether it is maintained. Our search on 2026-11-12 found the packages and their documentation but did not confirm licences or current maintenance, so none of that is settled. Record the answers in [`../design/technology-stack.md`](../design/technology-stack.md).

**Do not use** `pypxcryptokit`, `0fosdc` or any other unvetted PyPI package advertising a Double Ratchet implementation. An unverified cryptography package is a supply-chain risk and carries no track record.

**If no candidate passes verification**, the fallback is X3DH from a library with a single symmetric-key ratchet of our own for phase 1, declared in the report as reduced forward secrecy, with the full Double Ratchet in phase 2. We do not fall back to writing the Double Ratchet from the specification inside phase 1.

## What stays ours

A library gives the handshake and the ratchet. It does not give the system around them, and that part is still work:

- Identity keys, and the user ID derived from the identity public key.
- Publishing and retrieving signed prekey bundles through relays, including the one-time prekey problem across three replicas ([`../design/fault-tolerance.md`](../design/fault-tolerance.md), case 14).
- Persisting session and ratchet state in SQLite, and restoring it correctly after a client is killed.
- The envelope: padding to a fixed size, the content-hash identifier, the msgpack payload with the vector clock and the predecessor reference.
- Deciding what the relay can see, which is the whole privacy argument.

## Consequences

**Positive**

- About 3 person-days instead of 6, and the saved time goes to the replication and repair work that is actually graded.
- The cryptographic core has been reviewed by people who do this for a living, which is not true of anything we would write in 19 days.
- Test vectors still have to pass, so the integration is checkable.

**Negative, and stated plainly**

- **We must still be able to explain the protocol at the defense.** Adopting an implementation is not a reason to be unable to describe X3DH's four Diffie-Hellman exchanges or why the ratchet gives post-compromise security. Whoever integrates it reads the specifications.
- **The honest claim is narrow.** Python has no equivalent of Signal's own `libsignal`, whose official bindings are Rust. The Syndace libraries are the real Python option and have a track record through the OMEMO ecosystem, which is a genuine one but narrower than "industry standard". We say "the maintained Python implementation used by the OMEMO ecosystem", not more.
- **Native dependencies in the image.** `libsodium` and `libxeddsa` have to be present in the container, which affects the base image choice and the build.
- We lose the understanding that implementing it would have produced. Mitigated by reading the specifications and by being able to explain the integration.

## Follow-ups

- Confirm the licences of the chosen library, PyNaCl and `cryptography` before the written report cites them.
- Record the chosen library and version in [`../design/technology-stack.md`](../design/technology-stack.md) once verified.
