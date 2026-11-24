"""Encrypted send and receive, durable and serialized per peer.

**Contract only: the bodies are yours to write, against `tests/test_channel.py`.**

`Sessions` knows the cryptography and keeps sessions in memory. `History`
knows SQLite. This joins them under two rules, which are what make a killed
client safe:

**The ratchet state is stored in the same transaction as the message.** When
sending, with the message and its outbox entry (`History.queue_sealed`); when
receiving, with the message and the seen-set (`History.commit_opened`). So no
ciphertext leaves the process unless the state that made it is stored, and no
message is marked seen unless the state that read it is stored.

**If that transaction does not commit, the memory goes back too.** Export the
peer's sessions before the operation and `restore` them if anything after it
fails, so memory never runs ahead of the database. An envelope that cannot be
opened (`CannotOpen`) leaves both untouched.

**One operation per peer at a time.** The outbox and the receive loop share a
ratchet. A per-peer `asyncio.Lock` around the whole cycle (export, crypto,
transaction) stops two of them interleaving at an `await`: for instance two
first messages to a new peer each starting their own session.

Order when receiving: the seen-set first, then decrypt. A ratchet cannot
decrypt a message twice, so an envelope already committed must not reach it.
"""

from __future__ import annotations

from linkchat.core.client.history import History
from linkchat.core.crypto.sessions import Sessions
from linkchat.core.types import EnvelopeId, UserId


class SecureChannel:
    def __init__(self, history: History, sessions: Sessions) -> None:
        self.history = history
        self.sessions = sessions

    def restore(self) -> None:
        """Load every stored session into memory. Called once, on start."""
        raise NotImplementedError("SecureChannel.restore: implement it")

    async def send(
        self, peer: UserId, text: str, *, sent_at: float, expires_at: float
    ) -> EnvelopeId:
        """Seal `text` for `peer` and queue it durably. Returns its envelope id.

        Raises whatever sealing or the transaction raised (`NoBundle`, a SQLite
        error), with no message stored and no session changed.
        """
        raise NotImplementedError("SecureChannel.send: implement it")

    async def receive(
        self, peer: UserId, eid: EnvelopeId, envelope: bytes, *, received_at: float
    ) -> bool:
        """Open and commit one envelope from `peer`. True if it was new.

        False, without decrypting, if `eid` was already seen. Raises
        `CannotOpen`, or the transaction's error, with nothing stored and no
        session changed: the caller leaves the envelope unacknowledged.
        """
        raise NotImplementedError("SecureChannel.receive: implement it")
