"""The Ray implementation of the core's ports. Phase 1 only.

This is the package phase 2 deletes (ADR-0013). Everything that imports Ray
lives here: the relay actor, the gateway and directory built on actor calls,
the bootstrap script and the metrics.

Holds the scaffolding of tasks R3 (`relay_actor.py`, `bootstrap.py`) and R4
(`gateway.py`), written pre-spike on 2026-11-15: not yet run against Ray, and
subject to rework if the spike (gate G1) disproves the restart assumption. Still
to come: the repair orchestration (R5-R7), the supervised loops and metrics
(R8, R9).
"""
