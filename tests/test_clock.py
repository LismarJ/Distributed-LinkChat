"""The real clock. Small, but every relay and the client app depend on it.

`SystemClock` had no direct test: it was only ever exercised inside Ray worker
processes by `test_ray_runtime.py`, where the main process's coverage cannot see
it. The distinction it encodes — wall-clock for `expires_at`, monotonic for
measuring durations — is load-bearing for expiry (R8) and for the backoff in the
outbox, so it is worth pinning here rather than assuming.
"""

from __future__ import annotations

import time

from linkchat.core.clock import SystemClock
from linkchat.core.ports import Clock


def test_system_clock_satisfies_the_clock_port() -> None:
    assert isinstance(SystemClock(), Clock)


def test_now_is_wall_clock_not_monotonic() -> None:
    """`now` must be comparable across processes, so it is a real instant.

    `expires_at` is set by a sender and compared by a relay in another
    container, so the two have to mean the same thing. A monotonic counter
    would be per-process and silently wrong (fault-tolerance.md case 10).
    """
    clock = SystemClock()
    before = time.time()
    value = clock.now()
    after = time.time()
    assert before <= value <= after


def test_monotonic_never_goes_backwards() -> None:
    clock = SystemClock()
    readings = [clock.monotonic() for _ in range(10)]
    assert readings == sorted(readings)


def test_monotonic_is_not_the_wall_clock() -> None:
    """They measure different things, so they must not be the same number.

    Reading them as interchangeable is the mistake this separation prevents.
    """
    clock = SystemClock()
    assert abs(clock.monotonic() - clock.now()) > 1.0


async def test_sleep_actually_waits() -> None:
    """A generous margin on purpose: this must not be a flaky test.

    Windows' timer granularity is about 15.6 ms, so a 50 ms sleep can measure
    as little as ~35 ms. Asserting close to the requested duration would make
    the suite fail on timing noise rather than on a real defect, and the
    property worth pinning is only that `sleep` yields and does take time.
    """
    clock = SystemClock()
    start = clock.monotonic()
    await clock.sleep(0.05)
    assert clock.monotonic() - start >= 0.03
