"""The real clock. The fake one arrives with the fault-injecting transport (C2).

Separate from `ports.py` because that file holds declarations only.
"""

from __future__ import annotations

import asyncio
import time


class SystemClock:
    """`Clock` backed by the operating system.

    Uses `time.time`, not a monotonic counter, because `expires_at` is an
    absolute instant set by one process and compared by another. The cost is
    that a relay whose clock runs fast expires records early
    (fault-tolerance.md case 10); the benefit is that expiry needs no
    coordination at all, which is what keeps requirement R9 cheap.
    """

    def now(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
