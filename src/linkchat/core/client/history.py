"""The client's local history in SQLite (task C3).

The only permanent home of any message. Every public method is one
transaction, so a client killed at any moment reopens at its last commit.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from linkchat.core.envelope import envelope_id as compute_envelope_id
from linkchat.core.payload import Clock2, Payload, slot_of
from linkchat.core.types import EnvelopeId, MailboxId, UserId

SCHEMA_VERSION = 2

_SCHEMA_V1 = """
CREATE TABLE conversations (
    peer          TEXT PRIMARY KEY,
    mailbox_id    TEXT NOT NULL,
    my_count      INTEGER NOT NULL DEFAULT 0,
    peer_count    INTEGER NOT NULL DEFAULT 0,
    last_sent_id  TEXT
);

CREATE TABLE messages (
    envelope_id   TEXT PRIMARY KEY,
    peer          TEXT NOT NULL REFERENCES conversations(peer),
    direction     TEXT NOT NULL CHECK (direction IN ('out', 'in')),
    text          TEXT NOT NULL,
    clock_low     INTEGER NOT NULL,
    clock_high    INTEGER NOT NULL,
    prev          TEXT,
    sent_at       REAL NOT NULL,
    received_at   REAL,
    state         TEXT NOT NULL
);

CREATE TABLE outbox (
    envelope_id      TEXT PRIMARY KEY REFERENCES messages(envelope_id),
    mailbox_id       TEXT NOT NULL,
    envelope         BLOB NOT NULL,
    expires_at       REAL NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  REAL NOT NULL
);

CREATE TABLE seen (
    envelope_id  TEXT PRIMARY KEY,
    seen_at      REAL NOT NULL
);
"""

# ADR-0007: one mailbox per direction. `mailbox_id` was the one we send to.
_SCHEMA_V2 = """
ALTER TABLE conversations RENAME COLUMN mailbox_id TO send_mailbox;
ALTER TABLE conversations ADD COLUMN recv_mailbox TEXT NOT NULL DEFAULT '';
"""

_MIGRATIONS = {1: _SCHEMA_V1, 2: _SCHEMA_V2}


class MessageState(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    RECEIVED = "received"
    NOT_DELIVERED = "not_delivered"


class UnknownConversation(LookupError):
    """A message refers to a peer with no conversation opened yet."""


@dataclass(frozen=True, slots=True)
class StoredMessage:
    envelope_id: EnvelopeId
    peer: UserId
    outgoing: bool
    text: str
    clock: Clock2
    prev: EnvelopeId | None
    sent_at: float
    state: MessageState
    received_at: float | None = None
    """When the local client committed this message. `None` for outgoing.

    Used by the ordering layer (C7) to time out a message whose predecessor
    never arrived. Never used for ordering itself.
    """


@dataclass(frozen=True, slots=True)
class OutboxItem:
    envelope_id: EnvelopeId
    mailbox_id: MailboxId
    envelope: bytes
    expires_at: float
    attempts: int


Seal = Callable[[Payload], bytes]
"""Turns a payload into the finished envelope: encode, pad, encrypt."""


class History:
    def __init__(self, path: str | Path, me: UserId) -> None:
        self._me = me
        # isolation_level=None: we issue BEGIN/COMMIT ourselves.
        self._conn = sqlite3.connect(str(path), isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    def _migrate(self) -> None:
        """Apply every migration newer than the file, all in one transaction."""
        version = self._conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= SCHEMA_VERSION:
            return
        with self._tx() as db:
            for target in range(version + 1, SCHEMA_VERSION + 1):
                for statement in _MIGRATIONS[target].split(";"):
                    if statement.strip():
                        db.execute(statement)
            db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    @property
    def me(self) -> UserId:
        return self._me

    # -- conversations ------------------------------------------------------

    def open_conversation(
        self, peer: UserId, *, send_mailbox: MailboxId, recv_mailbox: MailboxId
    ) -> None:
        """Idempotent: opening an existing conversation changes nothing."""
        slot_of(self._me, peer)  # rejects a conversation with oneself
        with self._tx() as db:
            db.execute(
                "INSERT OR IGNORE INTO conversations (peer, send_mailbox, recv_mailbox)"
                " VALUES (?, ?, ?)",
                (peer, send_mailbox, recv_mailbox),
            )

    def peers(self) -> list[UserId]:
        rows = self._conn.execute("SELECT peer FROM conversations ORDER BY peer")
        return [UserId(r[0]) for r in rows]

    def recv_mailbox(self, peer: UserId) -> MailboxId:
        row = self._conn.execute(
            "SELECT recv_mailbox FROM conversations WHERE peer = ?", (peer,)
        ).fetchone()
        if row is None:
            raise UnknownConversation(peer)
        return MailboxId(row[0])

    def _conversation(
        self, db: sqlite3.Connection, peer: UserId
    ) -> tuple[str, int, int, str | None]:
        row = db.execute(
            "SELECT send_mailbox, my_count, peer_count, last_sent_id"
            " FROM conversations WHERE peer = ?",
            (peer,),
        ).fetchone()
        if row is None:
            raise UnknownConversation(peer)
        return row  # type: ignore[no-any-return]

    def _clock(self, peer: UserId, mine: int, theirs: int) -> Clock2:
        if slot_of(self._me, peer) == "low":
            return Clock2(low=mine, high=theirs)
        return Clock2(low=theirs, high=mine)

    # -- sending ------------------------------------------------------------

    def queue_outgoing(
        self,
        peer: UserId,
        text: str,
        *,
        sent_at: float,
        expires_at: float,
        seal: Seal,
    ) -> EnvelopeId:
        """Record a new message and its outbox entry, before any network call.

        `seal` runs inside the transaction, so the clock it sees is the one
        stored. The sealed bytes are kept: a retry resends them unchanged, and
        the envelope id stays the same.
        """
        with self._tx() as db:
            mailbox_id, my_count, peer_count, last_sent = self._conversation(db, peer)
            clock = self._clock(peer, my_count + 1, peer_count)
            prev = EnvelopeId(last_sent) if last_sent is not None else None
            payload = Payload(text=text, clock=clock, prev=prev, sent_at=sent_at)
            envelope = seal(payload)
            eid = compute_envelope_id(envelope)

            db.execute(
                "INSERT INTO messages (envelope_id, peer, direction, text, clock_low,"
                " clock_high, prev, sent_at, state)"
                " VALUES (?, ?, 'out', ?, ?, ?, ?, ?, ?)",
                (
                    eid,
                    peer,
                    text,
                    clock.low,
                    clock.high,
                    prev,
                    sent_at,
                    MessageState.PENDING.value,
                ),
            )
            db.execute(
                "INSERT INTO outbox (envelope_id, mailbox_id, envelope, expires_at,"
                " next_attempt_at) VALUES (?, ?, ?, ?, ?)",
                (eid, mailbox_id, envelope, expires_at, sent_at),
            )
            db.execute(
                "UPDATE conversations SET my_count = ?, last_sent_id = ? WHERE peer = ?",
                (my_count + 1, eid, peer),
            )
        return eid

    def due_outbox(self, now: float) -> list[OutboxItem]:
        rows = self._conn.execute(
            "SELECT envelope_id, mailbox_id, envelope, expires_at, attempts FROM outbox"
            " WHERE next_attempt_at <= ? ORDER BY next_attempt_at",
            (now,),
        ).fetchall()
        return [
            OutboxItem(EnvelopeId(r[0]), MailboxId(r[1]), bytes(r[2]), r[3], r[4])
            for r in rows
        ]

    def mark_attempt(self, eid: EnvelopeId, next_attempt_at: float) -> None:
        with self._tx() as db:
            db.execute(
                "UPDATE outbox SET attempts = attempts + 1, next_attempt_at = ?"
                " WHERE envelope_id = ?",
                (next_attempt_at, eid),
            )

    def confirm_sent(self, eid: EnvelopeId) -> None:
        """Three relays confirmed. Idempotent."""
        with self._tx() as db:
            db.execute("DELETE FROM outbox WHERE envelope_id = ?", (eid,))
            db.execute(
                "UPDATE messages SET state = ? WHERE envelope_id = ? AND state = ?",
                (MessageState.SENT.value, eid, MessageState.PENDING.value),
            )

    def give_up(self, eid: EnvelopeId) -> None:
        """Past its expiry: relays would drop it anyway. Idempotent."""
        with self._tx() as db:
            db.execute("DELETE FROM outbox WHERE envelope_id = ?", (eid,))
            db.execute(
                "UPDATE messages SET state = ? WHERE envelope_id = ? AND state = ?",
                (MessageState.NOT_DELIVERED.value, eid, MessageState.PENDING.value),
            )

    def next_due(self) -> float | None:
        """When the next retry is due, or None if the outbox is empty."""
        row = self._conn.execute("SELECT MIN(next_attempt_at) FROM outbox").fetchone()
        return row[0]  # type: ignore[no-any-return]

    # -- receiving ----------------------------------------------------------

    def is_seen(self, eid: EnvelopeId) -> bool:
        """Check BEFORE decrypting: a ratchet cannot decrypt the same message twice."""
        row = self._conn.execute(
            "SELECT 1 FROM seen WHERE envelope_id = ?", (eid,)
        ).fetchone()
        return row is not None

    def commit_incoming(
        self, peer: UserId, eid: EnvelopeId, payload: Payload, received_at: float
    ) -> bool:
        """Store a received message. False if it was a duplicate.

        Acknowledge to relays only after this returns.
        """
        with self._tx() as db:
            self._conversation(db, peer)  # raises if unknown
            inserted = db.execute(
                "INSERT OR IGNORE INTO seen (envelope_id, seen_at) VALUES (?, ?)",
                (eid, received_at),
            ).rowcount
            if inserted == 0:
                return False
            db.execute(
                "INSERT INTO messages (envelope_id, peer, direction, text, clock_low,"
                " clock_high, prev, sent_at, received_at, state)"
                " VALUES (?, ?, 'in', ?, ?, ?, ?, ?, ?, ?)",
                (
                    eid,
                    peer,
                    payload.text,
                    payload.clock.low,
                    payload.clock.high,
                    payload.prev,
                    payload.sent_at,
                    received_at,
                    MessageState.RECEIVED.value,
                ),
            )
            their_slot = slot_of(peer, self._me)
            theirs = payload.clock.low if their_slot == "low" else payload.clock.high
            db.execute(
                "UPDATE conversations SET peer_count = MAX(peer_count, ?) WHERE peer = ?",
                (theirs, peer),
            )
        return True

    def trim_seen(self, older_than: float) -> int:
        """Forget ids past TTL plus a margin: relays can no longer offer them."""
        with self._tx() as db:
            return db.execute(
                "DELETE FROM seen WHERE seen_at < ?", (older_than,)
            ).rowcount

    # -- reading ------------------------------------------------------------

    def messages(self, peer: UserId) -> list[StoredMessage]:
        """In storage order. Display order is task C7."""
        rows = self._conn.execute(
            "SELECT envelope_id, peer, direction, text, clock_low, clock_high, prev,"
            " sent_at, state, received_at FROM messages WHERE peer = ? ORDER BY rowid",
            (peer,),
        ).fetchall()
        return [
            StoredMessage(
                envelope_id=EnvelopeId(r[0]),
                peer=UserId(r[1]),
                outgoing=r[2] == "out",
                text=r[3],
                clock=Clock2(low=r[4], high=r[5]),
                prev=EnvelopeId(r[6]) if r[6] is not None else None,
                sent_at=r[7],
                state=MessageState(r[8]),
                received_at=r[9],
            )
            for r in rows
        ]
