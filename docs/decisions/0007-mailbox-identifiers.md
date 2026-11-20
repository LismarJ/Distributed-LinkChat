# ADR-0007: One mailbox per direction of each conversation

**Status:** accepted for phase 1, pending team review · **Date:** 2026-11-19
**Applies to:** phase 1 · **Leaves open:** the access credential for `fetch` and `ack`, and the invitation format

## Context

A relay stores records under a `MailboxId` and must learn nothing from it. The client needs to know which mailbox to send to, which to read, and who sent what it reads. Until now each conversation had a single `mailbox_id`. The receive loop (task C5) cannot work without settling this: if both directions shared one mailbox, a client reading it would also get its own messages back and try to decrypt them.

## Options considered

1. **One mailbox per direction of each conversation**, derived from the two user ids.
2. One mailbox per conversation, shared by both directions.
3. One inbox per user, for all of their conversations.

## Decision

Option 1. Alice leaves mail for Bob at

```
mailbox_for(alice, bob) = BLAKE2b-256("linkchat-mailbox\0" || alice || "\0" || bob)
```

and reads her own mail at `mailbox_for(bob, alice)`. Implemented in `linkchat/core/client/mailbox_ids.py`; the client stores both per conversation (`send_mailbox`, `recv_mailbox`, schema version 2).

## Consequences

**Gains.**

- Both clients compute the mailboxes from the two ids alone, with no exchange beyond knowing each other's id.
- The mailbox identifies the sender, so the envelope does not have to carry it and no sealed-sender scheme is needed.
- A client never reads its own messages.

**Costs.**

- A relay can see that the same mailbox receives mail repeatedly, and link the traffic of one pair over time. It still cannot tell who the pair is: a mailbox id is a hash, and user ids are hashes of public keys. Deriving the mailbox from the X3DH shared secret (task C6) would remove even that linkage, because a relay does not know the secret. That is the natural successor to this decision.

**Rejected.** Option 2 makes a client read its own messages back. Option 3 needs the sender inside the envelope, hidden from the relay, which is a sealed-sender scheme we would have to build; and it makes one mailbox a single target holding all of a user's traffic.

**Effect on the relay track.** None. A relay treats `MailboxId` as opaque, and placement (`ring.py`) hashes it whatever it contains.

**Still open.** What proves a client may read or acknowledge a mailbox. The `credential` is carried through every call and not yet checked (`relay_actor.py`, `fetch`). Until it is, anyone who can compute a mailbox id can read its ciphertext, which stays unreadable without the keys, or acknowledge it, which deletes it early. That is a known limit for phase 1, recorded here so that it is a decision rather than an oversight.
