"""The Ray adapters for the core's ports. Task R4.

`linkchat.core` is written against `RelayGateway`, `RelayPeers` and
`RelayDirectory` and never imports Ray. This module is the phase-1 implementation
of those ports over Ray actor calls. Phase 2 writes a second implementation and
deletes this one; the core does not change.

Two jobs beyond forwarding calls:

- **A timeout on every call.** A relay that Ray is still placing on a node can
  hold a call for a long time, and the three-copy send (R5) must be able to give
  up on one relay and try the next quickly. So every call is wrapped in
  `asyncio.wait_for`, independent of Ray's own `max_task_retries`.
- **Turning Ray's exceptions into ours.** Callers in the core only know the
  `RelayError` family (`ports.py`); they must never see a `ray.exceptions` type,
  or a Ray detail would have leaked across the boundary phase 2 depends on.

**Status: pre-spike (2026-11-15).** Not run against Ray. Whether an actor handle
survives a restart, and which exception a caller sees first when a relay is
being replaced, are spike items (G1); the mapping below encodes the documented
behaviour and is where a wrong guess will show up.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence

import ray

from linkchat.core.constants import RELAY_NAMES
from linkchat.core.ports import (
    PullResult,
    Record,
    RelayDirectory,
    RelayError,
    RelayGateway,
    RelayGone,
    RelayPeers,
    RelayStatus,
    RelayUnreachable,
    Tombstone,
)
from linkchat.core.types import Credential, EnvelopeId, MailboxId, RelayName, UserId
from linkchat.ray_runtime.bootstrap import NAMESPACE

DEFAULT_CALL_TIMEOUT_S = 5.0
"""Per-call timeout. A starting value for the spike: it must be longer than a
healthy call and shorter than the patience of the three-copy send. G1 measures
how long a real call to a restarting relay takes and this is tuned against it."""


async def _invoke(
    directory: RayRelayDirectory,
    name: RelayName,
    make_ref: Callable[[ray.actor.ActorHandle], Awaitable[object]],
    timeout_s: float,
):
    """Call one actor method with a timeout, mapping Ray's failures to ours.

    `make_ref` is given a freshly resolved handle and returns the awaitable from
    `handle.method.remote(...)`. Every failure becomes a `RelayError`:

    - a timeout, or Ray reporting the actor temporarily unavailable, is
      `RelayUnreachable` — **the call may still have run**, which is safe only
      because every relay operation is idempotent;
    - the actor being dead is `RelayGone`;
    - an application error the relay itself raised (`RelaySyncing`, `MailboxFull`)
      is re-raised unchanged, because it already is one of ours.
    """
    handle = directory._resolve(name)
    try:
        return await asyncio.wait_for(make_ref(handle), timeout_s)
    except RelayError:
        # The relay raised one of ours (e.g. RelaySyncing, MailboxFull). Ray
        # re-raises application errors as an instance of their original type, so
        # this catches them before the Ray-infrastructure cases below.
        raise
    except TimeoutError as exc:
        raise RelayUnreachable(f"{name} did not answer within {timeout_s}s") from exc
    except ray.exceptions.GetTimeoutError as exc:
        raise RelayUnreachable(f"{name} did not answer within {timeout_s}s") from exc
    except ray.exceptions.ActorUnavailableError as exc:
        # Being restarted or rescheduled: transient, treat like unreachable.
        raise RelayUnreachable(f"{name} is temporarily unavailable") from exc
    except ray.exceptions.RayActorError as exc:
        # Includes ActorDiedError. The handle may be stale; drop it so the next
        # call re-resolves the name.
        directory._forget(name)
        raise RelayGone(f"{name} is dead") from exc
    except ray.exceptions.RayTaskError as exc:
        cause = getattr(exc, "cause", None)
        if isinstance(cause, RelayError):
            raise cause from exc
        raise RelayError(f"{name} raised an unexpected error: {exc}") from exc


class RayRelayDirectory(RelayDirectory):
    """Turns a relay name into a callable gateway, via Ray named actors.

    The only object that knows how a relay is addressed in phase 1. It caches
    resolved handles and re-resolves a name after a failure, so callers never
    deal with a stale handle themselves.
    """

    def __init__(
        self, *, namespace: str = NAMESPACE, timeout_s: float = DEFAULT_CALL_TIMEOUT_S
    ) -> None:
        self._namespace = namespace
        self._timeout_s = timeout_s
        self._handles: dict[RelayName, ray.actor.ActorHandle] = {}

    @property
    def relay_names(self) -> Sequence[RelayName]:
        return RELAY_NAMES

    def gateway(self, name: RelayName) -> RelayGateway:
        if name not in RELAY_NAMES:
            raise ValueError(f"{name!r} is not one of {RELAY_NAMES}")
        return _ActorRelayGateway(name, self, self._timeout_s)

    def peers(self, this_relay: RelayName) -> RelayPeers:
        """The peer view used by a relay to repair itself and the others."""
        return _RayRelayPeers(this_relay, self, self._timeout_s)

    def _resolve(self, name: RelayName) -> ray.actor.ActorHandle:
        handle = self._handles.get(name)
        if handle is None:
            handle = ray.get_actor(name, namespace=self._namespace)
            self._handles[name] = handle
        return handle

    def _forget(self, name: RelayName) -> None:
        self._handles.pop(name, None)


class _ActorRelayGateway(RelayGateway):
    """`RelayGateway` for one relay, over its actor handle."""

    def __init__(
        self, name: RelayName, directory: RayRelayDirectory, timeout_s: float
    ) -> None:
        self._name = name
        self._directory = directory
        self._timeout_s = timeout_s

    async def store(self, record: Record) -> None:
        await _invoke(
            self._directory, self._name, lambda h: h.store.remote(record), self._timeout_s
        )

    async def fetch(
        self, mailbox_id: MailboxId, credential: Credential
    ) -> Sequence[Record]:
        return await _invoke(
            self._directory,
            self._name,
            lambda h: h.fetch.remote(mailbox_id, credential),
            self._timeout_s,
        )

    async def ack(
        self,
        mailbox_id: MailboxId,
        credential: Credential,
        envelope_ids: Sequence[EnvelopeId],
    ) -> None:
        await _invoke(
            self._directory,
            self._name,
            lambda h: h.ack.remote(mailbox_id, credential, list(envelope_ids)),
            self._timeout_s,
        )

    async def wait_for_mail(
        self, mailbox_id: MailboxId, credential: Credential, timeout_s: float
    ) -> bool:
        # The relay-side park has its own timeout; the transport timeout must
        # outlast it, or the call would be abandoned before the relay returns.
        transport_timeout = timeout_s + self._timeout_s
        return await _invoke(
            self._directory,
            self._name,
            lambda h: h.wait_for_mail.remote(mailbox_id, credential, timeout_s),
            transport_timeout,
        )

    async def publish_prekeys(self, user_id: UserId, bundle: bytes) -> None:
        await _invoke(
            self._directory,
            self._name,
            lambda h: h.publish_prekeys.remote(user_id, bundle),
            self._timeout_s,
        )

    async def get_prekeys(self, user_id: UserId) -> bytes | None:
        return await _invoke(
            self._directory,
            self._name,
            lambda h: h.get_prekeys.remote(user_id),
            self._timeout_s,
        )

    async def status(self) -> RelayStatus:
        return await _invoke(
            self._directory, self._name, lambda h: h.status.remote(), self._timeout_s
        )


class _RayRelayPeers(RelayPeers):
    """`RelayPeers` for one relay: what it needs from the other relays."""

    def __init__(
        self, this_relay: RelayName, directory: RayRelayDirectory, timeout_s: float
    ) -> None:
        self._this = this_relay
        self._directory = directory
        self._timeout_s = timeout_s

    @property
    def peer_names(self) -> Sequence[RelayName]:
        return tuple(name for name in RELAY_NAMES if name != self._this)

    async def pull(self, peer: RelayName, for_relay: RelayName) -> PullResult:
        return await _invoke(
            self._directory,
            peer,
            lambda h: h.pull.remote(for_relay),
            self._timeout_s,
        )

    async def handoff(
        self,
        peer: RelayName,
        records: Sequence[Record],
        tombstones: Sequence[Tombstone],
    ) -> None:
        await _invoke(
            self._directory,
            peer,
            lambda h: h.handoff.remote(list(records), list(tombstones)),
            self._timeout_s,
        )
