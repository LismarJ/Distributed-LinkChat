"""Sessions in memory: start, the header until the reply, order, and attacks.

Red until `linkchat/core/crypto/sessions.py` is implemented. No relays and no
SQLite: bundles come from a dictionary, so only the session logic is tested.
"""

from __future__ import annotations

import pytest
from x3dh import Bundle

from linkchat.core.crypto.identity import user_id_of
from linkchat.core.crypto.sessions import CannotOpen, NoBundle, SessionRecord, Sessions
from linkchat.core.crypto.suite import new_x3dh_state
from linkchat.core.crypto.wire import FLAG_X3DH
from linkchat.core.envelope import ENVELOPE_SIZE
from linkchat.core.payload import Clock2, Payload
from linkchat.core.types import UserId


class Bundles:
    """Stands in for the relays: whoever is created publishes here."""

    def __init__(self) -> None:
        self.published: dict[UserId, Bundle] = {}

    async def fetch(self, user_id: UserId) -> Bundle | None:
        return self.published.get(user_id)


def party(bundles: Bundles) -> Sessions:
    state = new_x3dh_state()
    me = user_id_of(state.bundle.identity_key)
    bundles.published[me] = state.bundle
    return Sessions(me, state, bundles.fetch)


def pair() -> tuple[Sessions, Sessions]:
    bundles = Bundles()
    return party(bundles), party(bundles)


def says(text: str) -> Payload:
    return Payload(text=text, clock=Clock2(low=1, high=0), prev=None, sent_at=0.0)


def has_x3dh_header(envelope: bytes) -> bool:
    return envelope[1] == FLAG_X3DH


# -- starting a session -----------------------------------------------------------


async def test_first_message_while_bob_is_offline() -> None:
    alice, bob = pair()

    envelope = await alice.seal(bob.me, says("hola"))

    assert len(envelope) == ENVELOPE_SIZE
    assert has_x3dh_header(envelope)
    assert (await bob.open(alice.me, envelope)).text == "hola"


async def test_a_conversation_both_ways() -> None:
    alice, bob = pair()

    for n in range(3):
        sent = await alice.seal(bob.me, says(f"a{n}"))
        assert (await bob.open(alice.me, sent)).text == f"a{n}"
        reply = await bob.seal(alice.me, says(f"b{n}"))
        assert (await alice.open(bob.me, reply)).text == f"b{n}"

    assert len(alice.sessions_with(bob.me)) == 1
    assert len(bob.sessions_with(alice.me)) == 1


async def test_no_bundle_means_no_session() -> None:
    bundles = Bundles()
    alice = party(bundles)
    stranger = UserId("0" * 64)

    with pytest.raises(NoBundle):
        await alice.seal(stranger, says("anyone there?"))
    assert alice.sessions_with(stranger) == []


async def test_a_conversation_with_oneself_is_refused() -> None:
    alice, _ = pair()

    with pytest.raises(ValueError):
        await alice.seal(alice.me, says("me"))


# -- the X3DH header until the first reply ------------------------------------------


async def test_the_header_rides_along_until_the_first_reply() -> None:
    alice, bob = pair()
    unanswered = [await alice.seal(bob.me, says(f"m{n}")) for n in range(3)]
    assert all(has_x3dh_header(e) for e in unanswered)

    await bob.open(alice.me, unanswered[0])
    reply = await bob.seal(alice.me, says("got it"))
    await alice.open(bob.me, reply)

    after_reply = await alice.seal(bob.me, says("good"))
    assert not has_x3dh_header(after_reply)
    assert alice.sending_session(bob.me).x3dh_header is None  # type: ignore[union-attr]


async def test_the_responder_never_sends_a_header() -> None:
    alice, bob = pair()
    await bob.open(alice.me, await alice.seal(bob.me, says("hola")))

    reply = await bob.seal(alice.me, says("hola"))

    assert not has_x3dh_header(reply)


async def test_initial_messages_read_out_of_order() -> None:
    """The relays reorder: Bob reads the third message first."""
    alice, bob = pair()
    first, second, third = [await alice.seal(bob.me, says(t)) for t in ("1", "2", "3")]

    texts = [(await bob.open(alice.me, e)).text for e in (third, first, second)]

    assert texts == ["3", "1", "2"]
    assert len(bob.sessions_with(alice.me)) == 1  # one session, not three


# -- both start at once ---------------------------------------------------------------


async def test_both_start_at_once_and_agree_on_one_session() -> None:
    alice, bob = pair()
    from_alice = await alice.seal(bob.me, says("hi bob"))
    from_bob = await bob.seal(alice.me, says("hi alice"))  # before seeing Alice's

    assert (await bob.open(alice.me, from_alice)).text == "hi bob"
    assert (await alice.open(bob.me, from_bob)).text == "hi alice"

    winner = min(alice.me, bob.me)
    for side, other in ((alice, bob.me), (bob, alice.me)):
        assert len(side.sessions_with(other)) == 2
        chosen = side.sending_session(other)
        assert chosen is not None and chosen.initiator == winner
    assert (
        alice.sending_session(bob.me).session_id  # type: ignore[union-attr]
        == bob.sending_session(alice.me).session_id  # type: ignore[union-attr]
    )

    # And the conversation carries on in the agreed session, both ways.
    assert (await bob.open(alice.me, await alice.seal(bob.me, says("a")))).text == "a"
    assert (await alice.open(bob.me, await bob.seal(alice.me, says("b")))).text == "b"


async def test_messages_in_the_losing_session_still_open() -> None:
    """Sent in a session before it lost the tie-break, delivered late."""
    alice, bob = pair()
    loser, winner = (alice, bob) if alice.me > bob.me else (bob, alice)
    first = await loser.seal(winner.me, says("first"))
    late = await loser.seal(winner.me, says("late"))  # same, losing session
    await loser.open(winner.me, await winner.seal(loser.me, says("crossing")))

    assert (await winner.open(loser.me, first)).text == "first"
    assert (await winner.open(loser.me, late)).text == "late"


# -- attacks and accidents --------------------------------------------------------------


async def test_someone_else_writing_as_alice_is_refused() -> None:
    """Mallory knows Bob's bundle and Alice's mailbox, not Alice's identity key."""
    bundles = Bundles()
    alice, bob, mallory = party(bundles), party(bundles), party(bundles)
    forged = await mallory.seal(bob.me, says("this is alice, honest"))

    with pytest.raises(CannotOpen):
        await bob.open(alice.me, forged)
    assert bob.sessions_with(alice.me) == []


async def test_a_tampered_envelope_is_refused_and_the_session_survives() -> None:
    alice, bob = pair()
    await bob.open(alice.me, await alice.seal(bob.me, says("hola")))
    real = await alice.seal(bob.me, says("real"))
    tampered = bytearray(real)
    tampered[-1] ^= 0x01

    with pytest.raises(CannotOpen):
        await bob.open(alice.me, bytes(tampered))
    assert (await bob.open(alice.me, real)).text == "real"


async def test_garbage_is_refused() -> None:
    alice, bob = pair()

    with pytest.raises(CannotOpen):
        await bob.open(alice.me, b"not an envelope")
    with pytest.raises(CannotOpen):
        await bob.open(alice.me, bytes(ENVELOPE_SIZE))
    assert bob.sessions_with(alice.me) == []


async def test_opening_the_same_envelope_twice_is_refused() -> None:
    """The seen-set should stop this first; if it does not, it fails cleanly."""
    alice, bob = pair()
    envelope = await alice.seal(bob.me, says("once"))
    await bob.open(alice.me, envelope)

    with pytest.raises(CannotOpen):
        await bob.open(alice.me, envelope)


async def test_an_envelope_for_someone_else_is_refused() -> None:
    """Alice writes to Carol; the envelope ends up in front of Bob."""
    bundles = Bundles()
    alice, bob, carol = party(bundles), party(bundles), party(bundles)
    for_carol = await alice.seal(carol.me, says("for carol"))

    with pytest.raises(CannotOpen):
        await bob.open(alice.me, for_carol)
    assert bob.sessions_with(alice.me) == []


async def test_a_misbehaving_peer_cannot_break_the_choice_of_session() -> None:
    """Three sessions, two started by the peer: `seal` must still pick one, and
    pick the same one every time, instead of failing."""
    alice, bob = pair()
    await bob.open(alice.me, await alice.seal(bob.me, says("hola")))
    [real] = bob.sessions_with(alice.me)
    for fake_id in ("ff" * 32, "00" * 32):
        bob._sessions[alice.me][fake_id] = SessionRecord(
            session_id=fake_id,
            initiator=alice.me,
            ratchet=real.ratchet,
            associated_data=real.associated_data,
            x3dh_header=None,
        )

    chosen = bob.sending_session(alice.me)

    assert chosen is not None
    assert chosen is bob.sending_session(alice.me)
    assert chosen.session_id == min(r.session_id for r in bob.sessions_with(alice.me))
