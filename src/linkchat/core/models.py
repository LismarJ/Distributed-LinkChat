from dataclasses import dataclass


@dataclass(frozen=True)
class Envelope:
    version: int
    mailbox_id: bytes
    envelope_id: bytes
    ciphertext: bytes

@dataclass(frozen=True)
class StoreReceipt:
    envelope_id: bytes
    acknowledged_relays: tuple[str, ...]
    confirmed: bool