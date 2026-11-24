"""Notifications for the UI: something changed in a conversation.

The browser is told *that* a conversation changed, not *what* it now says: on
an event it fetches the conversation again. So an event lost to a full queue
costs at most a stale view until the next one, and nothing here has to be
durable.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import Literal

from linkchat.core.types import EnvelopeId, UserId

QUEUE_SIZE = 256
"""Per subscriber. A browser that stops reading loses the oldest events."""

EventKind = Literal["message", "state"]
"""`message`: one was sent or received. `state`: one changed delivery state."""


@dataclass(frozen=True, slots=True)
class Event:
    kind: EventKind
    peer: UserId
    envelope_id: EnvelopeId


class EventBus:
    """Fan-out to every open subscriber. Publishing never blocks."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[Event]] = set()

    def subscribe(self) -> asyncio.Queue[Event]:
        queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=QUEUE_SIZE)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[Event]) -> None:
        self._subscribers.discard(queue)

    def publish(self, event: Event) -> None:
        for queue in self._subscribers:
            if queue.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()  # drop the oldest, keep the newest
            queue.put_nowait(event)
