"""The outbox loop: retry pending sends until confirmed or expired.

The outbox rows themselves live in `History` and are written before any
network call. This module only decides when to try again.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from collections.abc import Awaitable, Callable

from linkchat.core.client.history import History, OutboxItem, Seal
from linkchat.core.ports import Clock, Record
from linkchat.core.replication import SendResult
from linkchat.core.types import EnvelopeId, UserId

log = logging.getLogger(__name__)

BASE_S = 0.5
CAP_S = 30.0
ERROR_PAUSE_S = 1.0
"""Pause after an unexpected error in the loop, so a broken disk does not spin."""

SendFn = Callable[[Record], Awaitable[SendResult]]
"""In production, `send_to_three` bound to a directory and a clock."""


class Outbox:
    def __init__(
        self,
        history: History,
        send: SendFn,
        clock: Clock,
        rng: random.Random | None = None,
    ) -> None:
        self._history = history
        self._send = send
        self._clock = clock
        self._rng = rng or random.Random()
        self._wake = asyncio.Event()

    def submit(self, peer: UserId, text: str, *, ttl_s: float, seal: Seal) -> EnvelopeId:
        """Queue durably, then nudge the loop. The message starts `pending`."""
        now = self._clock.now()
        eid = self._history.queue_outgoing(
            peer, text, sent_at=now, expires_at=now + ttl_s, seal=seal
        )
        self._wake.set()
        return eid

    async def run_once(self) -> None:
        """One pass over everything due, tried concurrently.

        One item failing (say, SQLite refusing a write) does not stop the others.
        """
        due = self._history.due_outbox(self._clock.now())
        results = await asyncio.gather(
            *(self._attempt(item) for item in due), return_exceptions=True
        )
        for item, result in zip(due, results, strict=True):
            if isinstance(result, Exception):
                log.error("outbox item %s failed", item.envelope_id, exc_info=result)
            elif isinstance(result, BaseException):
                raise result  # cancellation and the like must still propagate

    async def _attempt(self, item: OutboxItem) -> None:
        now = self._clock.now()
        if now >= item.expires_at:
            self._history.give_up(item.envelope_id)
            return
        record = Record(
            mailbox_id=item.mailbox_id,
            envelope_id=item.envelope_id,
            envelope=item.envelope,
            expires_at=item.expires_at,
        )
        try:
            sent = (await self._send(record)).sent
        except Exception:
            # A failed attempt, not a reason to stop the loop.
            log.exception("send of %s failed", item.envelope_id)
            sent = False
        if sent:
            self._history.confirm_sent(item.envelope_id)
        else:
            # From the end of the attempt: a send can take several timeouts.
            retry_at = self._clock.now() + self.backoff(item.attempts)
            self._history.mark_attempt(item.envelope_id, retry_at)

    def backoff(self, attempts: int) -> float:
        """Exponential with full jitter, so clients do not retry in lockstep."""
        return self._rng.uniform(0, min(CAP_S, BASE_S * 2**attempts))

    async def run(self) -> None:
        """Forever: send what is due, sleep until the next retry or a new message.

        Started by the client process as a background task
        (`asyncio.create_task(outbox.run())`) and cancelled on shutdown. It never
        returns on an error: a dead loop would leave messages `pending` silently.
        """
        while True:
            self._wake.clear()
            try:
                await self.run_once()
                next_due = self._history.next_due()
            except Exception:
                log.exception("outbox loop error; retrying in %ss", ERROR_PAUSE_S)
                await asyncio.sleep(ERROR_PAUSE_S)
                continue
            timeout = None if next_due is None else max(0.0, next_due - self._clock.now())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout)
