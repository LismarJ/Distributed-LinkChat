"""Application logic, with no knowledge of how processes reach each other.

**Nothing in this package may import Ray** (ADR-0013). Phase 1 runs this code
under Ray; phase 2 removes Ray and keeps this package unchanged. The rule is
enforced by `tests/test_core_is_ray_free.py`, which fails the build rather than
trusting anyone to remember.

Everything outside this package is reached through `ports.py`.
"""
