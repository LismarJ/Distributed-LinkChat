"""The numbers the design rests on, in one place.

Changing any of these changes the fault-tolerance argument, so each one carries
the decision that set it.
"""

from __future__ import annotations

from .types import RelayName

PROTOCOL_VERSION = 1
"""The wire-format version of an envelope. ADR pending.

Reserved during the 2026-11-15 interface reconciliation so the protocol can be
versioned the way the other interface draft's `Envelope.version` field intended.
Wiring a version byte into the envelope layout (`envelope.py`) is deferred until
the cryptography library fixes the header and tag sizes (ADR-0009); until then a
relay still learns only that an envelope exists, its mailbox and its expiry.
"""

RELAY_COUNT = 5
"""How many relay actors exist. ADR-0005.

Five, because a write needs three copies and we must survive two failures at
once: 3 + 2 = 5. The set is fixed at bootstrap; phase 1 does not grow it.
"""

PRIMARY_COUNT = 3
"""How many relays hold each record, and how many a reader asks. ADR-0005.

To survive `f` crashes a record needs `f + 1` copies, and `f = 2` here.
"""

WRITE_COPIES = 3
"""How many distinct relays must confirm before a message counts as sent.

Equal to PRIMARY_COUNT on purpose. Confirming at fewer would mean the relays
that confirmed could be exactly the two that fail, which is the mistake the
earlier "write to three, confirm at two" rule made.
"""

RELAY_NAMES: tuple[RelayName, ...] = tuple(
    RelayName(f"relay-{i}") for i in range(RELAY_COUNT)
)
"""Every relay name, in index order. Known to every process without a lookup."""


def relay_name(index: int) -> RelayName:
    """The name of relay `index`.

    Raises:
        IndexError: if `index` is not a relay that exists.
    """
    if not 0 <= index < RELAY_COUNT:
        raise IndexError(f"relay index {index} outside 0..{RELAY_COUNT - 1}")
    return RELAY_NAMES[index]
