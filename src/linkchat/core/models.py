"""The public model surface of `linkchat.core`, under agreed importable names.

The canonical definitions of what crosses a relay boundary live in `ports.py`,
so that one file stays the single frozen description. This module re-exports
them and adds the one name the client-send layer needs, so that
`from linkchat.core.models import Envelope, StoreReceipt` keeps resolving.

The core interface was reconciled on 2026-11-15 from two independent drafts;
this is the compatibility surface that keeps both drafts' imports working. See
docs/superpowers/specs/2026-11-15-core-interface-reconciliation-design.md.
"""

from __future__ import annotations

from dataclasses import dataclass

from .ports import PullResult, Record, RelayStatus, Tombstone
from .types import EnvelopeId, RelayName

__all__ = [
    "Envelope",
    "PullResult",
    "Record",
    "RelayStatus",
    "StoreReceipt",
    "Tombstone",
]

# `Envelope` is the name the other draft gave the stored unit. The reconciled
# design keeps the stored unit as the opaque `envelope: bytes` carried inside a
# `Record` (a relay learns nothing from it), so `Envelope` is an alias of
# `Record`, not a second competing shape. The field mapping (version /
# mailbox_id / envelope_id / ciphertext -> Record) is flagged in the
# reconciliation PR for the interface's other author to confirm.
Envelope = Record


@dataclass(frozen=True, slots=True)
class StoreReceipt:
    """The result of storing one envelope to the relay quorum.

    This belongs to the client-send layer, not to the per-relay `RelayGateway`:
    a single relay's `store` returns nothing (it either stored the record or
    raised), while *sending* a message means storing to `WRITE_COPIES` distinct
    relays and counting how many confirmed. `confirmed` is True once enough have.

    Carried here, under the name the other draft gave it, so the layer that
    orchestrates the fan-out has a typed result to return. Nothing constructs it
    yet: the client-send path is a C-series task, still unwritten.
    """

    envelope_id: EnvelopeId
    acknowledged_relays: tuple[RelayName, ...]
    confirmed: bool
