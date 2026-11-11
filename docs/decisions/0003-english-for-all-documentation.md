# ADR-0003 — English for all documentation and the defense

**Status:** accepted · **Date:** 2026-11-10 · **Deciders:** project team

## Context

The course brief (`Orinetacion.pdf`) is in Spanish, and the project is defended at the Universidad de La Habana. All research and design documentation produced so far is in English. A choice was needed before the document set grew further: keep working notes in English and translate deliverables into Spanish, go English throughout, or convert everything to Spanish now.

## Decision

**English throughout** — research notes, design documents, ADRs, code comments, the written report and the defense materials.

## Consequences

**Positive**

- No translation pass, and no risk of a Spanish and an English version drifting apart as the design changes.
- Technical vocabulary stays consistent with the primary sources we cite (the Matrix specification, the SimpleX SMP specification, the Session whitepaper, and the DHT and membership literature are all in English).
- One canonical document set.

**Negative / mitigations**

- The brief's own terminology is Spanish. Requirements are therefore quoted in Spanish and mapped to English terms in the R1–R9 table in [`../research/00-problem-analysis.md`](../research/00-problem-analysis.md), so the mapping back to the brief stays explicit and checkable.
- If the examiners require a Spanish written report, a translation pass becomes necessary late. **Confirm this with the course staff early** — it is cheap to ask now and expensive to discover in month three.
