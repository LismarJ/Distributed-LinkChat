"""Sessions: turning payloads into envelopes for one peer, and back.

Sessions live in memory here. Storing them, and serializing the operations on
one peer, is `channel.py`'s job, through `export` and `restore`.

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

from collections.abc import Awaitable, Callable, Sequence
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
from linkchat.core.crypto.identity import user_id_of
from linkchat.core.crypto.suite import (
    LinkChatDoubleRatchet,
    LinkChatX3DH,
    accept_ratchet,
    start_ratchet,
)
from linkchat.core.crypto.wire import pack, unpack
from linkchat.core.envelope import MalformedEnvelope, pad_payload, unpad_payload
from linkchat.core.payload import MalformedPayload, Payload, decode, encode
from linkchat.core.types import UserId

BundleFetcher = Callable[[UserId], Awaitable[Bundle | None]]
"""In production, `bundle.fetch_bundle` bound to the relay directory."""


class NoBundle(LookupError):
    """No relay has a valid bundle for the peer, so no session can start."""


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

        Deterministic for any number of sessions, so both sides always agree:
        first the ones the smaller UserId initiated, then by session id. A
        normal client holds at most two, with different initiators, but a
        misbehaving peer must not be able to make `seal` fail.
        """
        held = self.sessions_with(peer)
        if not held:
            return None
        winner_id = min(self.me, peer)
        return min(held, key=lambda r: (r.initiator != winner_id, r.session_id))

    # -- persistence: contract, implement against tests/test_channel.py ------

    def export(self, peer: UserId) -> list[SessionRow]:
        """The sessions with `peer` in the form History stores."""
        raise NotImplementedError("Sessions.export: implement it")

    def restore(self, peer: UserId, rows: Sequence[SessionRow]) -> None:
        """Replace every session with `peer` by `rows`. Used on start, and to
        roll back an operation whose transaction did not commit."""
        raise NotImplementedError("Sessions.restore: implement it")

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
        """First message to `peer`: X3DH as initiator, then the ratchet's
        initial encryption. The first message goes through
        `encrypt_initial_message`, not `encrypt_message`, so the peer's
        `decrypt_initial_message` finds the initial chain key.
        """
        bundle = await self.fetch(peer)
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
        # Whoever wrote this envelope claims to be `peer`. Their identity key
        # must hash to the UserId the invitation gave us, or it is someone
        # else writing into this mailbox.
        if user_id_of(header.identity_key) != peer:
            raise CannotOpen("the X3DH header is not the peer's")

        session_id = header.ephemeral_key.hex()
        held = self._sessions.get(peer, {})

        if session_id in held:
            # A reordered message from a session we already have.
            record = held[session_id]
            plaintext = await record.ratchet.decrypt_message(
                unpacked.message,
                associated_data=_authenticated_data(
                    record, unpacked.authenticated_prefix
                ),
            )
            self._clear_header_after_reply(record)
            return plaintext

        # A new session from the peer. X3DH as responder, then accept the
        # ratchet on this very message: it is the first message of the
        # session, wherever it sits in the sending chain.
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
            x3dh_header=None,  # the responder never sends a header
        )
        self._sessions.setdefault(peer, {})[session_id] = record
        return plaintext

    async def _decrypt_without_header(
        self, peer: UserId, unpacked: wire.Unpacked
    ) -> bytes:
        held = self.sessions_with(peer)
        if not held:
            raise CannotOpen(f"no session with {peer} and no header to start one")

        # Try the sending session first, then the other. The library does not
        # mutate state on a failed decrypt, so trying both is safe.
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
                self._clear_header_after_reply(record)
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

    def _clear_header_after_reply(self, record: SessionRecord) -> None:
        """The first reply in a session we initiated ends the header ride.

        The responder never sets one, so only our own sessions need this.
        """
        if record.initiator == self.me:
            record.x3dh_header = None
