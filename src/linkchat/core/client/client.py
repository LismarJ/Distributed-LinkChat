"""The client core: identity, encrypted channel, outbox and receivers.

What the FastAPI app holds. Every message is end-to-end encrypted: `send`
seals it through `SecureChannel` and the outbox only retries the stored bytes;
the receivers hand each envelope to the same channel to open and store.

`start()` creates the background tasks (outbox, one receiver per contact, and
the bundle republisher), `stop()` cancels them.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging

from linkchat.core.client.history import History
from linkchat.core.client.mailbox_ids import mailbox_for
from linkchat.core.client.ordering import (
    DEFAULT_HOLD_TIMEOUT_S,
    DisplayMessage,
    order_messages,
)
from linkchat.core.client.outbox import Outbox, SendFn
from linkchat.core.client.receiver import Receiver
from linkchat.core.crypto.bundle import encode_bundle, fetch_bundle, publish_bundle
from linkchat.core.crypto.channel import SecureChannel
from linkchat.core.crypto.identity import load_or_create_identity
from linkchat.core.crypto.sessions import Sessions
from linkchat.core.mailbox import DEFAULT_TOMBSTONE_TTL_S
from linkchat.core.ports import Clock, RelayDirectory
from linkchat.core.replication import send_to_three
from linkchat.core.types import EnvelopeId, UserId

log = logging.getLogger(__name__)

DEFAULT_TTL_S = DEFAULT_TOMBSTONE_TTL_S
"""How long relays keep a message. Must not exceed the tombstone TTL (ADR-0006)."""

REPUBLISH_S = 60.0
"""How often the bundle is published again. Relays keep it in memory only, so
one restarted while this client is online has it back within this time."""


class ChatClient:
    def __init__(
        self,
        history: History,
        directory: RelayDirectory,
        clock: Clock,
        *,
        send: SendFn | None = None,
        ttl_s: float = DEFAULT_TTL_S,
        hold_timeout_s: float = DEFAULT_HOLD_TIMEOUT_S,
        republish_s: float = REPUBLISH_S,
    ) -> None:
        self.history = history
        self.identity = load_or_create_identity(history)
        fetch = functools.partial(fetch_bundle, directory)
        self.channel = SecureChannel(history, Sessions(history.me, self.identity, fetch))
        self.channel.restore()

        self._directory = directory
        self._clock = clock
        self._ttl_s = ttl_s
        self._hold_timeout_s = hold_timeout_s
        self._republish_s = republish_s

        send = send or functools.partial(send_to_three, directory=directory, clock=clock)
        self.outbox = Outbox(history, send, clock)
        self.receiver = Receiver(history, directory, clock, self._accept)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._started = False

    @property
    def me(self) -> UserId:
        return self.history.me

    def add_contact(self, peer: UserId) -> None:
        """Open the conversation with its two mailboxes, and start listening."""
        self.history.open_conversation(
            peer,
            send_mailbox=mailbox_for(self.me, peer),
            recv_mailbox=mailbox_for(peer, self.me),
        )
        if self._started:
            self._listen(peer)

    async def send(self, peer: UserId, text: str) -> EnvelopeId:
        """Encrypt and queue durably. The message starts `pending`.

        Raises `NoBundle` if this is the first message to `peer` and no relay
        has their bundle yet: nothing is stored, and the caller may retry.
        """
        now = self._clock.now()
        eid = await self.channel.send(
            peer, text, sent_at=now, expires_at=now + self._ttl_s
        )
        self.outbox.notify()
        return eid

    async def publish(self) -> int:
        """Publish this client's bundle on every relay. Returns how many confirmed."""
        bundle = encode_bundle(self.identity.bundle)
        return await publish_bundle(self._directory, self.me, bundle)

    def ordered_messages(
        self, peer: UserId, *, now: float | None = None
    ) -> list[DisplayMessage]:
        """The conversation in display order. A pure function of the history."""
        return order_messages(
            self.history.messages(peer),
            me=self.me,
            peer=peer,
            now=now if now is not None else self._clock.now(),
            hold_timeout_s=self._hold_timeout_s,
        )

    async def start(self) -> None:
        self._started = True
        self._tasks.setdefault("outbox", asyncio.create_task(self.outbox.run()))
        self._tasks.setdefault("republish", asyncio.create_task(self._republish()))
        for peer in self.history.peers():
            self._listen(peer)

    async def stop(self) -> None:
        self._started = False
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks, return_exceptions=True)

    # -- internals ----------------------------------------------------------

    async def _accept(
        self, peer: UserId, eid: EnvelopeId, envelope: bytes, received_at: float
    ) -> bool:
        return await self.channel.receive(peer, eid, envelope, received_at=received_at)

    async def _republish(self) -> None:
        """Forever: publish now, then again every `republish_s`. Never dies."""
        while True:
            try:
                confirmed = await self.publish()
                log.debug("bundle republished on %d relays", confirmed)
            except Exception:
                log.exception("bundle republish failed")
            await asyncio.sleep(self._republish_s)

    def _listen(self, peer: UserId) -> None:
        key = f"recv:{peer}"
        if key not in self._tasks:
            self._tasks[key] = asyncio.create_task(self.receiver.run(peer))
