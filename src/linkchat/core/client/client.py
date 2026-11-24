"""The client core: history, outbox and receivers wired together.

This is what the FastAPI app (C8) holds. It owns the background tasks:
`start()` creates them, `stop()` cancels them.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
from collections.abc import Callable

from linkchat.core.client.history import History
from linkchat.core.client.mailbox_ids import mailbox_for
from linkchat.core.client.ordering import (
    DEFAULT_HOLD_TIMEOUT_S,
    DisplayMessage,
    order_messages,
)
from linkchat.core.client.outbox import Outbox, SendFn
from linkchat.core.client.receiver import OpenFn, Receiver
from linkchat.core.mailbox import DEFAULT_TOMBSTONE_TTL_S
from linkchat.core.payload import Payload
from linkchat.core.ports import Clock, RelayDirectory
from linkchat.core.replication import send_to_three
from linkchat.core.types import EnvelopeId, UserId

DEFAULT_TTL_S = DEFAULT_TOMBSTONE_TTL_S
"""How long relays keep a message. Must not exceed the tombstone TTL (ADR-0006)."""

SealFn = Callable[[UserId, Payload], bytes]
"""Payload for `peer` -> envelope: encode, pad, encrypt. C6 supplies the real one."""


class ChatClient:
    def __init__(
        self,
        history: History,
        directory: RelayDirectory,
        clock: Clock,
        *,
        seal: SealFn,
        open_envelope: OpenFn,
        send: SendFn | None = None,
        ttl_s: float = DEFAULT_TTL_S,
        hold_timeout_s: float = DEFAULT_HOLD_TIMEOUT_S,
    ) -> None:
        self.history = history
        self._seal = seal
        self._ttl_s = ttl_s
        self._clock = clock
        self._hold_timeout_s = hold_timeout_s
        send = send or functools.partial(send_to_three, directory=directory, clock=clock)
        self.outbox = Outbox(history, send, clock)
        self.receiver = Receiver(history, directory, clock, open_envelope)
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._started = False

    def add_contact(self, peer: UserId) -> None:
        """Open the conversation with its two mailboxes, and start listening."""
        me = self.history.me
        self.history.open_conversation(
            peer,
            send_mailbox=mailbox_for(me, peer),
            recv_mailbox=mailbox_for(peer, me),
        )
        if self._started:
            self._listen(peer)

    def send(self, peer: UserId, text: str) -> EnvelopeId:
        """Queue durably and return at once. The message starts `pending`."""
        seal = functools.partial(self._seal, peer)
        return self.outbox.submit(peer, text, ttl_s=self._ttl_s, seal=seal)

    def ordered_messages(
        self, peer: UserId, *, now: float | None = None
    ) -> list[DisplayMessage]:
        """The conversation as it should be displayed. C7.

        Pure function of `history.messages(peer)`: no state, no I/O beyond the
        read. `now` is for tests; production passes the real clock.
        """
        return order_messages(
            self.history.messages(peer),
            me=self.history.me,
            peer=peer,
            now=now if now is not None else self._clock.now(),
            hold_timeout_s=self._hold_timeout_s,
        )

    async def start(self) -> None:
        self._started = True
        self._tasks.setdefault("outbox", asyncio.create_task(self.outbox.run()))
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

    def _listen(self, peer: UserId) -> None:
        key = f"recv:{peer}"
        if key not in self._tasks:
            self._tasks[key] = asyncio.create_task(self.receiver.run(peer))
