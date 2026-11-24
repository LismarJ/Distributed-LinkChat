"""Encrypted send and receive, stored atomically and serialized per peer.

Joins `Sessions` (the ratchet, in memory) and `History` (SQLite) under three
rules, which make it safe to kill the client at any moment:

**The ratchet state is stored with the message**, in one transaction. No
ciphertext leaves the process, and no message is marked seen, unless the state
that produced or read it is stored too.

**If that transaction fails, memory goes back.** Otherwise the ratchet would
run ahead of the database, and a message whose key it spent would be lost.

**One operation per peer at a time.** The outbox and the receive loop share a
ratchet; a per-peer lock stops them interleaving at an `await`.
"""

from __future__ import annotations

import asyncio
import logging

from linkchat.core.client.history import History, SessionRow
from linkchat.core.crypto.sessions import Sessions
from linkchat.core.types import EnvelopeId, UserId

log = logging.getLogger(__name__)


class SecureChannel:
    def __init__(self, history: History, sessions: Sessions) -> None:
        self.history = history
        self.sessions = sessions
        self._locks: dict[UserId, asyncio.Lock] = {}

    def _lock(self, peer: UserId) -> asyncio.Lock:
        lock = self._locks.get(peer)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[peer] = lock
        return lock

    def restore(self) -> None:
        """Load every stored session into memory. Called once, on start."""
        for peer in self.history.peers():
            rows = self.history.load_sessions(peer)
            self.sessions.restore(peer, rows)

    async def send(
        self, peer: UserId, text: str, *, sent_at: float, expires_at: float
    ) -> EnvelopeId:
        """Encrypt `text` for `peer` and queue it with the new ratchet state.

        Raises whatever sealing or the transaction raised (`NoBundle`, a SQLite
        error), with nothing stored and no session changed.
        """
        async with self._lock(peer):
            before: list[SessionRow] = self.sessions.export(peer)
            try:
                payload = self.history.next_payload(peer, text, sent_at=sent_at)
                envelope = await self.sessions.seal(peer, payload)
                return self.history.queue_sealed(
                    peer,
                    payload,
                    envelope=envelope,
                    expires_at=expires_at,
                    sessions=self.sessions.export(peer),
                )
            except BaseException:  # a cancellation must roll back too
                self.sessions.restore(peer, before)
                raise

    async def receive(
        self, peer: UserId, eid: EnvelopeId, envelope: bytes, *, received_at: float
    ) -> bool:
        """Open one envelope from `peer` and store it with the new ratchet state.

        True if it was new. False, without decrypting, if already seen. Raises
        `CannotOpen` or the transaction's error with nothing changed; the caller
        leaves the envelope unacknowledged.
        """
        async with self._lock(peer):
            if self.history.is_seen(eid):
                return False
            before: list[SessionRow] = self.sessions.export(peer)
            try:
                payload = await self.sessions.open(peer, envelope)
                committed = self.history.commit_opened(
                    peer,
                    eid,
                    payload,
                    received_at=received_at,
                    sessions=self.sessions.export(peer),
                )
                if not committed:  # seen meanwhile: memory must not run ahead
                    self.sessions.restore(peer, before)
                return committed
            except BaseException:
                self.sessions.restore(peer, before)
                raise
