from typing import Protocol, Sequence

from linkchat.core.models import (
    Envelope,
    StoreReceipt,
)


class RelayGateway(Protocol):

    async def store(
        self,
        envelope: Envelope,
        ttl_seconds: int,
    ) -> StoreReceipt:
        """Store an envelope in the relay network."""

    async def fetch(
        self,
        mailbox_id: bytes,
        limit: int = 100,
    ) -> Sequence[Envelope]:
        """Retrieve pending envelopes for a mailbox."""

    async def acknowledge(
        self,
        mailbox_id: bytes,
        envelope_id: bytes,
    ) -> bool:
        """Acknowledge an envelope after local persistence."""

    async def wait_for_mail(
        self,
        mailbox_id: bytes,
        timeout_seconds: float,
    ) -> None:
        """Wait until mail may be available or timeout expires."""


class RelayPeers(Protocol):

    async def get_live_peers(self) -> Sequence[str]:
        """Return the identifiers of currently reachable peers."""

    async def pull(
        self,
        peer_id: str,
        mailbox_id: bytes,
    ) -> Sequence[Envelope]:
        """Retrieve mailbox records from another relay."""

    async def store_on_peer(
        self,
        peer_id: str,
        envelope: Envelope,
        ttl_seconds: int,
    ) -> bool:
        """Store an envelope on a specific peer."""

    async def acknowledge_on_peer(
        self,
        peer_id: str,
        mailbox_id: bytes,
        envelope_id: bytes,
    ) -> bool:
        """Propagate an acknowledgement to another relay."""

class Clock(Protocol): #for local tests

    def now(self) -> float:
        """Return the current wall-clock time."""

    def monotonic(self) -> float:
        """Return monotonic time for measuring durations."""

    async def sleep(self, seconds: float) -> None:
        """Sleep for the requested duration."""