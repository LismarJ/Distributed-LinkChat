"""The client's local history in SQLite.

The only permanent home of any message. Every public method is one
transaction, so a client killed at any moment reopens at its last commit.

It also holds the client's identity and the per-peer ratchet state. An
encrypted message and the ratchet state that produced or read it are stored
in one transaction (`queue_sealed`, `commit_opened`).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from linkchat.core.envelope import envelope_id as compute_envelope_id
from linkchat.core.payload import Clock2, Payload, slot_of
from linkchat.core.types import EnvelopeId, MailboxId, UserId

SCHEMA_VERSION = 3

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

# One mailbox per direction (ADR-0007). `mailbox_id` was the one we send to.
_SCHEMA_V2 = """
ALTER TABLE conversations RENAME COLUMN mailbox_id TO send_mailbox;
ALTER TABLE conversations ADD COLUMN recv_mailbox TEXT NOT NULL DEFAULT '';
"""

# A permanent identity, and the per-peer ratchet state.
_SCHEMA_V3 = """
CREATE TABLE identity (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    user_id   TEXT NOT NULL,
    x3dh      TEXT NOT NULL
);

CREATE TABLE sessions (
    peer             TEXT NOT NULL REFERENCES conversations(peer),
    session_id       TEXT NOT NULL,
    ratchet          TEXT NOT NULL,
    associated_data  BLOB NOT NULL,
    x3dh_header      BLOB,
    initiator        TEXT NOT NULL,
    PRIMARY KEY (peer, session_id)
);
"""

_MIGRATIONS = {1: _SCHEMA_V1, 2: _SCHEMA_V2, 3: _SCHEMA_V3}


class MessageState(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    RECEIVED = "received"
    NOT_DELIVERED = "not_delivered"


class UnknownConversation(LookupError):
    """A message refers to a peer with no conversation opened yet."""


class NoIdentity(RuntimeError):
    """Asked for `me` before `save_identity` was ever called."""


class IdentityConflict(RuntimeError):
    """A different identity was written over the one already stored.

    It would orphan every session and make this client a stranger to its
    contacts. There is no legitimate reason, so it is a bug or a tampered file.
    """


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


@dataclass(frozen=True, slots=True)
class SessionRow:
    """One session as stored. Defined here, not in sessions.py, to avoid a cycle.

    `ratchet` is the ratchet's JSON as text. `x3dh_header` is the three public
    keys concatenated (ik + ek + spk), or None once answered or if we responded.
    """

    session_id: str
    initiator: UserId
    ratchet: str
    associated_data: bytes
    x3dh_header: bytes | None


Seal = Callable[[Payload], bytes]
"""Turns a payload into the finished envelope: encode, pad, encrypt."""


class History:
    def __init__(self, path: str | Path, me: UserId | None = None) -> None:
        self._me: UserId | None = me
        # isolation_level=None: we issue BEGIN/COMMIT ourselves.
        # check_same_thread=False: the client may be built in one thread and run
        # by an event loop in another (a server, a test). It is only ever used
        # by one event loop at a time, never by two threads at once.
        self._conn = sqlite3.connect(
            str(path), isolation_level=None, check_same_thread=False
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()
        if self._me is None:
            loaded = self.load_identity()
            if loaded is not None:
                self._me = loaded[0]

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
        if self._me is None:
            raise NoIdentity(
                "no identity yet: call save_identity before opening conversations"
            )
        return self._me

    # -- identity -----------------------------------------------------------

    def save_identity(self, user_id: UserId, x3dh_json: dict[str, Any]) -> None:
        """Store the client's identity, or update the state of the same one.

        Raises:
            IdentityConflict: a different identity is already stored, or this
                History was opened as someone else.
        """
        with self._tx() as db:
            row = db.execute("SELECT user_id FROM identity WHERE id = 1").fetchone()
            if row is not None and UserId(row[0]) != user_id:
                raise IdentityConflict(
                    f"this database belongs to {row[0]}, not {user_id}"
                )
            if self._me is not None and self._me != user_id:
                raise IdentityConflict(
                    f"the History was opened as {self._me}, but the identity is {user_id}"
                )
            db.execute(
                "INSERT INTO identity (id, user_id, x3dh) VALUES (1, ?, ?)"
                " ON CONFLICT(id) DO UPDATE SET x3dh = excluded.x3dh",
                (user_id, json.dumps(x3dh_json)),
            )
        if self._me is None:
            self._me = user_id

    def load_identity(self) -> tuple[UserId, dict[str, Any]] | None:
        """The stored identity, or None on a fresh install."""
        row = self._conn.execute(
            "SELECT user_id, x3dh FROM identity WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        return UserId(row[0]), json.loads(row[1])

    # -- conversations ------------------------------------------------------

    def open_conversation(
        self, peer: UserId, *, send_mailbox: MailboxId, recv_mailbox: MailboxId
    ) -> None:
        """Idempotent: opening an existing conversation changes nothing."""
        slot_of(self.me, peer)  # rejects a conversation with oneself
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

    def send_mailbox(self, peer: UserId) -> MailboxId:
        """The mailbox this client writes to for `peer`.

        Read from the conversation rather than recomputed with `mailbox_for`,
        because a database migrated from schema v1 kept its single pre-ADR-0007
        mailbox as the send mailbox, and that value is not derivable.
        """
        row = self._conn.execute(
            "SELECT send_mailbox FROM conversations WHERE peer = ?", (peer,)
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
        if slot_of(self.me, peer) == "low":
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
        """Record a message and its outbox entry, sealing it synchronously.

        For a `seal` that needs no stored state. Encrypted messages go through
        `next_payload` and `queue_sealed`, which also store the ratchet.
        """
        with self._tx() as db:
            payload = self._payload(db, peer, text, sent_at)
            return self._insert_outgoing(db, peer, payload, seal(payload), expires_at)

    def next_payload(self, peer: UserId, text: str, *, sent_at: float) -> Payload:
        """The payload of the next message to `peer`. Stores nothing.

        The caller encrypts it and passes it to `queue_sealed`, holding the
        per-peer lock in between.
        """
        return self._payload(self._conn, peer, text, sent_at)

    def queue_sealed(
        self,
        peer: UserId,
        payload: Payload,
        *,
        envelope: bytes,
        expires_at: float,
        sessions: Sequence[SessionRow],
    ) -> EnvelopeId:
        """Store the message, its outbox entry and the peer's sessions together.

        A crash before this commits leaves none of them: the ciphertext never
        left the process and the stored ratchet never advanced.

        Raises:
            ValueError: `payload` is stale, its clock is not the next one.
        """
        with self._tx() as db:
            expected = self._payload(db, peer, payload.text, payload.sent_at)
            if payload.clock != expected.clock:
                raise ValueError(
                    f"payload clock {payload.clock} is not the next one {expected.clock}"
                )
            eid = self._insert_outgoing(db, peer, payload, envelope, expires_at)
            _write_sessions(db, peer, sessions)
        return eid

    def _payload(
        self, db: sqlite3.Connection, peer: UserId, text: str, sent_at: float
    ) -> Payload:
        _, my_count, peer_count, last_sent = self._conversation(db, peer)
        clock = self._clock(peer, my_count + 1, peer_count)
        prev = EnvelopeId(last_sent) if last_sent is not None else None
        return Payload(text=text, clock=clock, prev=prev, sent_at=sent_at)

    def _insert_outgoing(
        self,
        db: sqlite3.Connection,
        peer: UserId,
        payload: Payload,
        envelope: bytes,
        expires_at: float,
    ) -> EnvelopeId:
        mailbox_id, my_count, _, _ = self._conversation(db, peer)
        eid = compute_envelope_id(envelope)
        db.execute(
            "INSERT INTO messages (envelope_id, peer, direction, text, clock_low,"
            " clock_high, prev, sent_at, state) VALUES (?, ?, 'out', ?, ?, ?, ?, ?, ?)",
            (
                eid,
                peer,
                payload.text,
                payload.clock.low,
                payload.clock.high,
                payload.prev,
                payload.sent_at,
                MessageState.PENDING.value,
            ),
        )
        db.execute(
            "INSERT INTO outbox (envelope_id, mailbox_id, envelope, expires_at,"
            " next_attempt_at) VALUES (?, ?, ?, ?, ?)",
            (eid, mailbox_id, envelope, expires_at, payload.sent_at),
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

    def peer_of(self, eid: EnvelopeId) -> UserId | None:
        """The conversation a stored message belongs to, or None."""
        row = self._conn.execute(
            "SELECT peer FROM messages WHERE envelope_id = ?", (eid,)
        ).fetchone()
        return UserId(row[0]) if row is not None else None

    def is_seen(self, eid: EnvelopeId) -> bool:
        """Check BEFORE decrypting: a ratchet cannot decrypt the same message twice."""
        row = self._conn.execute(
            "SELECT 1 FROM seen WHERE envelope_id = ?", (eid,)
        ).fetchone()
        return row is not None

    def commit_incoming(
        self, peer: UserId, eid: EnvelopeId, payload: Payload, received_at: float
    ) -> bool:
        """Store a received message. False, storing nothing, if already seen.

        Acknowledge to relays only after this returns. Encrypted messages use
        `commit_opened`, which also stores the ratchet.
        """
        with self._tx() as db:
            return self._insert_incoming(db, peer, eid, payload, received_at)

    def commit_opened(
        self,
        peer: UserId,
        eid: EnvelopeId,
        payload: Payload,
        *,
        received_at: float,
        sessions: Sequence[SessionRow],
    ) -> bool:
        """Store a received message and the peer's sessions together.

        False, storing nothing at all, if `eid` was already seen.
        """
        with self._tx() as db:
            if not self._insert_incoming(db, peer, eid, payload, received_at):
                return False
            _write_sessions(db, peer, sessions)
        return True

    def _insert_incoming(
        self,
        db: sqlite3.Connection,
        peer: UserId,
        eid: EnvelopeId,
        payload: Payload,
        received_at: float,
    ) -> bool:
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
        theirs = (
            payload.clock.low if slot_of(peer, self.me) == "low" else payload.clock.high
        )
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

    # -- sessions -----------------------------------------------------------

    def load_sessions(self, peer: UserId) -> list[SessionRow]:
        """Every session stored for `peer`, in session_id order."""
        rows = self._conn.execute(
            "SELECT session_id, initiator, ratchet, associated_data, x3dh_header"
            " FROM sessions WHERE peer = ? ORDER BY session_id",
            (peer,),
        ).fetchall()
        return [
            SessionRow(
                session_id=r[0],
                initiator=UserId(r[1]),
                ratchet=r[2],
                associated_data=bytes(r[3]),
                x3dh_header=bytes(r[4]) if r[4] is not None else None,
            )
            for r in rows
        ]

    # -- reading ------------------------------------------------------------

    def messages(self, peer: UserId) -> list[StoredMessage]:
        """In storage order. Display order is `ordering.order_messages`."""
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


def _write_sessions(
    db: sqlite3.Connection, peer: UserId, sessions: Sequence[SessionRow]
) -> None:
    """Replace every session for `peer` with `sessions`, in the same transaction.

    DELETE + INSERT, not upsert: a session that was dropped in memory (e.g.
    because it lost a tie-break and there were too many) must vanish from
    disk too. Callers always pass the full set.
    """
    db.execute("DELETE FROM sessions WHERE peer = ?", (peer,))
    for row in sessions:
        db.execute(
            "INSERT INTO sessions"
            " (peer, session_id, initiator, ratchet, associated_data, x3dh_header)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                peer,
                row.session_id,
                row.initiator,
                row.ratchet,
                row.associated_data,
                row.x3dh_header,
            ),
        )
