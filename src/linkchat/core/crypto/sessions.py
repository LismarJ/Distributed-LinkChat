"""Sessions: turning payloads into envelopes for one peer, and back.

Sessions live in memory here. `export` and `restore` let `channel.py` store
them with each message and roll memory back when a transaction fails.

What `seal` and `open` have to get right:

**Starting a session.** The first time this client writes to a peer, it fetches
the peer's bundle, runs X3DH as the initiator, and starts a ratchet whose first
key is the peer's signed prekey. No bundle means no session: `NoBundle`.

**The X3DH header rides along until the first reply.** The initiator puts its
X3DH header in every envelope of that session until it has opened something
the peer sent *in that session*. Relays reorder, so the peer may read message
2 before message 1; with the header on both, either one can open the session.

**Opening an envelope with an X3DH header.** If a session with that ephemeral
key already exists, it is the same session: use it. Otherwise this is a new
session from the peer: X3DH as the responder, then accept the ratchet. The
header's identity key must hash to the peer's UserId; anything else is someone
else writing into this mailbox, and is refused.

**Both sides start at once.** Each may have initiated a session before seeing
the other's. Then each client holds two. Both send with the one initiated by
the smaller UserId, which both compute alike; the other stays, so that what was
already encrypted in it can still be read.

**Failure leaves no trace.** An envelope that cannot be opened, for any reason,
raises `CannotOpen` and changes no session. The receiver leaves it
unacknowledged and moves on.

Associated data, for every message: the X3DH associated data of the session,
followed by the envelope's authenticated prefix (`wire.authenticated_prefix`).
Plaintext: `payload.encode`, then `envelope.pad_payload`.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from doubleratchet import (
    AuthenticationFailedException,
    DecryptionFailedException,
    DoSProtectionException,
    DuplicateMessageException,
)
from x3dh import Bundle, KeyAgreementException
from x3dh import Header as X3DHHeader

from linkchat.core.client.history import SessionRow
from linkchat.core.crypto import wire
from linkchat.core.crypto.bundle import NoRelayAnswered
from linkchat.core.crypto.identity import user_id_of
from linkchat.core.crypto.suite import (
    LinkChatDoubleRatchet,
    LinkChatX3DH,
    accept_ratchet,
    restore_ratchet,
    start_ratchet,
)
from linkchat.core.crypto.wire import pack, unpack
from linkchat.core.envelope import MalformedEnvelope, pad_payload, unpad_payload
from linkchat.core.payload import MalformedPayload, Payload, decode, encode
from linkchat.core.types import UserId

BundleFetcher = Callable[[UserId], Awaitable[Bundle | None]]
"""In production, `bundle.fetch_bundle` bound to the relay directory."""

_X3DH_SLOT_SIZE = 3 * 32
"""ik + ek + spk, as stored in SQLite. Never includes a one-time prekey."""


class NoBundle(LookupError):
    """No relay has a valid bundle for the peer, so no session can start."""


class RelaysUnavailable(NoBundle):
    """No relay answered, so whether the peer has a bundle is unknown.

    A `NoBundle`, so callers that only need "no session can start" are
    unchanged; callers that tell the user why can catch it first, because this
    one passes with time while a missing bundle needs the peer to come online.
    """


class CannotOpen(ValueError):
    """An envelope that this client cannot read. No session was changed.

    Covers a malformed envelope, a header that is not the peer's, an unknown
    signed prekey, a failed authentication, a duplicate and a bad payload.
    """


@dataclass
class SessionRecord:
    """One session with one peer. A peer has at most two (both started at once)."""

    session_id: str
    """The initiator's X3DH ephemeral key, hex. The same on both sides."""

    initiator: UserId
    ratchet: LinkChatDoubleRatchet
    associated_data: bytes
    """X3DH's associated data. Every message of the session needs it."""

    x3dh_header: X3DHHeader | None
    """Set while this client initiated the session and has had no reply in it."""


def _authenticated_data(record: SessionRecord, prefix: bytes) -> bytes:
    """X3DH AD, then the envelope's authenticated prefix (wire.py)."""
    return record.associated_data + prefix


def _encode_x3dh_header(header: X3DHHeader | None) -> bytes | None:
    """ik + ek + spk, or None. Reverses `_decode_x3dh_header`."""
    if header is None:
        return None
    return header.identity_key + header.ephemeral_key + header.signed_pre_key


def _decode_x3dh_header(data: bytes | None) -> X3DHHeader | None:
    """Rebuild an X3DH header from its three public keys, or None."""
    if data is None:
        return None
    if len(data) != _X3DH_SLOT_SIZE:
        raise ValueError(
            f"stored X3DH header is {len(data)} bytes, expected {_X3DH_SLOT_SIZE}"
        )
    return X3DHHeader(
        identity_key=data[:32],
        ephemeral_key=data[32:64],
        signed_pre_key=data[64:],
        pre_key=None,
    )


class Sessions:
    def __init__(self, me: UserId, identity: LinkChatX3DH, fetch: BundleFetcher) -> None:
        self.me = me
        self.identity = identity
        self.fetch = fetch
        # One entry per peer, and at most two sessions per peer.
        self._sessions: dict[UserId, dict[str, SessionRecord]] = {}

    # -- reading sessions ---------------------------------------------------

    def sessions_with(self, peer: UserId) -> list[SessionRecord]:
        """Every session held with `peer`: none, one, or two."""
        return list(self._sessions.get(peer, {}).values())

    def sending_session(self, peer: UserId) -> SessionRecord | None:
        """The session `seal` uses for `peer`: the only one, or the one
        initiated by the smaller UserId.

        Deterministic for any number of sessions, so both sides always agree,
        and a misbehaving peer cannot make `seal` fail.
        """
        held = self.sessions_with(peer)
        if not held:
            return None
        winner_id = min(self.me, peer)
        return min(held, key=lambda r: (r.initiator != winner_id, r.session_id))

    # -- export and restore -------------------------------------------------

    def export(self, peer: UserId) -> list[SessionRow]:
        """Everything in memory for `peer`, ready to commit."""
        return [
            SessionRow(
                session_id=record.session_id,
                initiator=record.initiator,
                ratchet=json.dumps(record.ratchet.json),
                associated_data=record.associated_data,
                x3dh_header=_encode_x3dh_header(record.x3dh_header),
            )
            for record in self.sessions_with(peer)
        ]

    def restore(self, peer: UserId, rows: list[SessionRow]) -> None:
        """Replace `peer`'s in-memory sessions with `rows`.

        Used at startup to load from SQLite, and after a failed transaction
        to put the in-memory state back where it was.
        """
        if not rows:
            self._sessions.pop(peer, None)
            return
        self._sessions[peer] = {
            row.session_id: SessionRecord(
                session_id=row.session_id,
                initiator=row.initiator,
                ratchet=restore_ratchet(json.loads(row.ratchet)),
                associated_data=row.associated_data,
                x3dh_header=_decode_x3dh_header(row.x3dh_header),
            )
            for row in rows
        }

    # -- sealing ------------------------------------------------------------

    async def seal(self, peer: UserId, payload: Payload) -> bytes:
        """`payload` for `peer` as one envelope of exactly `ENVELOPE_SIZE` bytes.

        Raises:
            NoBundle: no session yet and no bundle to start one.
            ValueError: `peer` is this client.
        """
        if peer == self.me:
            raise ValueError("a conversation needs two different participants")

        plaintext = pad_payload(encode(payload))

        record = self.sending_session(peer)
        if record is None:
            return await self._seal_initial(peer, plaintext)

        prefix = wire.authenticated_prefix(record.x3dh_header)
        encrypted = await record.ratchet.encrypt_message(
            plaintext, associated_data=_authenticated_data(record, prefix)
        )
        return pack(record.x3dh_header, encrypted)

    async def _seal_initial(self, peer: UserId, plaintext: bytes) -> bytes:
        """First message to `peer`: X3DH as initiator, then `start_ratchet`.

        The first message goes through `encrypt_initial_message`, not
        `encrypt_message`, so that the peer's `decrypt_initial_message`
        finds the initial chain key.
        """
        try:
            bundle = await self.fetch(peer)
        except NoRelayAnswered as exc:
            raise RelaysUnavailable(f"no relay answered for {peer}") from exc
        if bundle is None:
            raise NoBundle(f"no bundle for {peer}")

        (
            shared_secret,
            associated_data,
            x3dh_header,
        ) = await self.identity.get_shared_secret_active(bundle, require_pre_key=False)
        prefix = wire.authenticated_prefix(x3dh_header)
        ratchet, encrypted = await start_ratchet(
            shared_secret=shared_secret,
            recipient_ratchet_pub=bundle.signed_pre_key,
            message=plaintext,
            associated_data=associated_data + prefix,
        )
        record = SessionRecord(
            session_id=x3dh_header.ephemeral_key.hex(),
            initiator=self.me,
            ratchet=ratchet,
            associated_data=associated_data,
            x3dh_header=x3dh_header,
        )
        self._sessions.setdefault(peer, {})[record.session_id] = record
        return pack(x3dh_header, encrypted)

    # -- opening ------------------------------------------------------------

    async def open(self, peer: UserId, envelope: bytes) -> Payload:
        """The payload `peer` sealed into `envelope`.

        Raises:
            CannotOpen: for any envelope this client cannot read. No session
                is created or changed when it does.
        """
        try:
            return await self._open(peer, envelope)
        except CannotOpen:
            raise
        except Exception as exc:
            raise CannotOpen(f"cannot open envelope from {peer}: {exc}") from exc

    async def _open(self, peer: UserId, envelope: bytes) -> Payload:
        try:
            unpacked = unpack(envelope)
        except MalformedEnvelope as exc:
            raise CannotOpen(f"malformed envelope: {exc}") from exc

        try:
            plaintext = await self._decrypt(peer, unpacked)
        except CannotOpen:
            raise
        except (
            KeyAgreementException,
            AuthenticationFailedException,
            DecryptionFailedException,
            DuplicateMessageException,
            DoSProtectionException,
        ) as exc:
            raise CannotOpen(f"cannot decrypt from {peer}: {exc}") from exc

        try:
            return decode(unpad_payload(plaintext))
        except (MalformedEnvelope, MalformedPayload) as exc:
            raise CannotOpen(f"bad payload from {peer}: {exc}") from exc

    async def _decrypt(self, peer: UserId, unpacked: wire.Unpacked) -> bytes:
        header = unpacked.x3dh_header
        if header is not None:
            return await self._decrypt_with_header(peer, unpacked, header)
        return await self._decrypt_without_header(peer, unpacked)

    async def _decrypt_with_header(
        self, peer: UserId, unpacked: wire.Unpacked, header: X3DHHeader
    ) -> bytes:
        if user_id_of(header.identity_key) != peer:
            raise CannotOpen("the X3DH header is not the peer's")

        session_id = header.ephemeral_key.hex()
        held = self._sessions.get(peer, {})

        if session_id in held:
            record = held[session_id]
            plaintext = await record.ratchet.decrypt_message(
                unpacked.message,
                associated_data=_authenticated_data(
                    record, unpacked.authenticated_prefix
                ),
            )
            _clear_header_after_reply(record)
            return plaintext

        (
            shared_secret,
            associated_data,
            spk_pair,
        ) = await self.identity.get_shared_secret_passive(header, require_pre_key=False)
        ratchet, plaintext = await accept_ratchet(
            shared_secret=shared_secret,
            own_ratchet_priv=spk_pair.priv,
            message=unpacked.message,
            associated_data=associated_data + unpacked.authenticated_prefix,
        )
        record = SessionRecord(
            session_id=session_id,
            initiator=peer,
            ratchet=ratchet,
            associated_data=associated_data,
            x3dh_header=None,
        )
        self._sessions.setdefault(peer, {})[session_id] = record
        return plaintext

    async def _decrypt_without_header(
        self, peer: UserId, unpacked: wire.Unpacked
    ) -> bytes:
        held = self.sessions_with(peer)
        if not held:
            raise CannotOpen(f"no session with {peer} and no header to start one")

        sending = self.sending_session(peer)
        assert sending is not None  # `held` is not empty
        order = [sending] + [r for r in held if r is not sending]
        last_error: Exception | None = None
        for record in order:
            try:
                plaintext = await record.ratchet.decrypt_message(
                    unpacked.message,
                    associated_data=_authenticated_data(
                        record, unpacked.authenticated_prefix
                    ),
                )
                _clear_header_after_reply(record)
                return plaintext
            except (
                AuthenticationFailedException,
                DecryptionFailedException,
                DuplicateMessageException,
                DoSProtectionException,
            ) as exc:
                last_error = exc
                continue
        raise CannotOpen(
            f"envelope from {peer} opens in none of {len(held)} session(s)"
        ) from last_error


def _clear_header_after_reply(record: SessionRecord) -> None:
    """The first reply in a session we initiated ends the header ride.

    The responder never sets one, so only our own sessions need this.
    """
    if record.x3dh_header is not None:
        record.x3dh_header = None
