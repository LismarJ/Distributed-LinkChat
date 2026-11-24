"""The receive loop: fetch, merge, accept, then acknowledge.

Reading three relays out of five is enough: a `sent` message is on three
distinct relays, and any two sets of three out of five share at least one
(W + R > N). So with two relays down, the read still finds every message.

What to do with each envelope is `accept`'s business (in production,
`SecureChannel.receive`): skip what was seen, decrypt, store. This loop only
decides what to acknowledge: everything accepted or already seen, and nothing
that could not be read, which stays on the relays until its TTL.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence

from linkchat.core.client.history import History
from linkchat.core.constants import RELAY_COUNT, WRITE_COPIES
from linkchat.core.crypto import wire
from linkchat.core.envelope import MalformedEnvelope, envelope_id
from linkchat.core.ports import Clock, Record, RelayDirectory, RelayError
from linkchat.core.ring import preference_list
from linkchat.core.types import Credential, EnvelopeId, MailboxId, RelayName, UserId

log = logging.getLogger(__name__)

READ_COPIES = 3
assert READ_COPIES + WRITE_COPIES > RELAY_COUNT, "reads must overlap writes"

FETCH_TIMEOUT_S = 5.0
WAIT_TIMEOUT_S = 25.0
ERROR_PAUSE_S = 1.0

AcceptFn = Callable[[UserId, EnvelopeId, bytes, float], Awaitable[bool]]
"""(peer, envelope id, envelope, received_at) -> True if new, False if seen.

Raises if the envelope cannot be read; it is then not acknowledged.
"""

NO_CREDENTIAL = Credential(b"")
"""ADR-0007 leaves the credential open: carried, not yet checked by relays."""


class Receiver:
    def __init__(
        self,
        history: History,
        directory: RelayDirectory,
        clock: Clock,
        accept: AcceptFn,
        *,
        credential: Credential = NO_CREDENTIAL,
        timeout_s: float = FETCH_TIMEOUT_S,
    ) -> None:
        self._history = history
        self._directory = directory
        self._clock = clock
        self._accept = accept
        self._credential = credential
        self._timeout_s = timeout_s
        self._unreadable: set[EnvelopeId] = set()

    async def receive_once(self, peer: UserId) -> int:
        """One round for `peer`. Returns how many new messages were committed."""
        mailbox = self._history.recv_mailbox(peer)
        answered = await self._read(mailbox)
        if len(answered) < READ_COPIES:
            log.warning("read %s from %d relays only", mailbox, len(answered))

        merged: dict[EnvelopeId, Record] = {}
        for records in answered.values():
            for record in records:
                merged.setdefault(record.envelope_id, record)

        new = 0
        to_ack: list[EnvelopeId] = []
        for record in sorted(merged.values(), key=_chain_position):
            eid = record.envelope_id
            if envelope_id(record.envelope) != eid:
                self._note_unreadable(eid, "id does not match its content")
                continue
            try:
                fresh = await self._accept(peer, eid, record.envelope, self._clock.now())
            except Exception as exc:  # unreadable: no ack, it stays until its TTL
                self._note_unreadable(eid, repr(exc))
                continue
            new += fresh
            to_ack.append(eid)  # seen ones too: an earlier ack may have been lost

        if to_ack:
            await self._ack(list(answered), mailbox, to_ack)
        return new

    async def run(self, peer: UserId) -> None:
        """Forever: receive, then park until mail arrives. Never dies on an error."""
        while True:
            try:
                await self.receive_once(peer)
                await self._park(self._history.recv_mailbox(peer))
            except Exception:
                log.exception("receive loop for %s failed", peer)
                await asyncio.sleep(ERROR_PAUSE_S)
            await asyncio.sleep(0)  # always yield, even if every call returned at once

    # -- internals ----------------------------------------------------------

    async def _read(self, mailbox: MailboxId) -> dict[RelayName, Sequence[Record]]:
        """Fetch from the first READ_COPIES relays that answer, in preference order."""
        order = list(preference_list(mailbox))
        answered: dict[RelayName, Sequence[Record]] = {}
        while len(answered) < READ_COPIES and order:
            batch, order = (
                order[: READ_COPIES - len(answered)],
                order[READ_COPIES - len(answered) :],
            )
            results = await asyncio.gather(
                *(self._fetch(name, mailbox) for name in batch)
            )
            for name, records in zip(batch, results, strict=True):
                if records is not None:
                    answered[name] = records
        return answered

    async def _fetch(
        self, name: RelayName, mailbox: MailboxId
    ) -> Sequence[Record] | None:
        """None if the relay is down, syncing or too slow: try the next one."""
        gateway = self._directory.gateway(name)
        try:
            return await asyncio.wait_for(
                gateway.fetch(mailbox, self._credential), self._timeout_s
            )
        except (RelayError, TimeoutError):
            return None

    def _note_unreadable(self, eid: EnvelopeId, why: str) -> None:
        if eid not in self._unreadable:  # log once, not every round
            self._unreadable.add(eid)
            log.warning("cannot read envelope %s: %s", eid, why)

    async def _ack(
        self, relays: list[RelayName], mailbox: MailboxId, eids: list[EnvelopeId]
    ) -> None:
        """Best effort: a lost ack means the message comes back and `seen` drops it."""

        async def one(name: RelayName) -> None:
            gateway = self._directory.gateway(name)
            await asyncio.wait_for(
                gateway.ack(mailbox, self._credential, eids), self._timeout_s
            )

        results = await asyncio.gather(*(one(n) for n in relays), return_exceptions=True)
        for name, result in zip(relays, results, strict=True):
            if isinstance(result, Exception):
                log.info("ack to %s failed: %r", name, result)

    async def _park(self, mailbox: MailboxId) -> None:
        """One wait_for_mail on the first relay that takes it."""
        for name in preference_list(mailbox):
            gateway = self._directory.gateway(name)
            try:
                await asyncio.wait_for(
                    gateway.wait_for_mail(mailbox, self._credential, WAIT_TIMEOUT_S),
                    WAIT_TIMEOUT_S + self._timeout_s,
                )
                return
            except (RelayError, TimeoutError):
                continue
        await asyncio.sleep(ERROR_PAUSE_S)  # every relay refused: do not spin


def _chain_position(record: Record) -> int:
    """The ratchet counter in the clear header, to read a backlog in order.

    In order, each message needs no skipped keys; out of order, the first one
    read may be far ahead. Envelopes in another format sort first, unchanged.
    """
    try:
        return wire.unpack(record.envelope).message.header.sending_chain_length
    except MalformedEnvelope:
        return 0
