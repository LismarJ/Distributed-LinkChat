"""C6: a permanent identity, and a UserId derived from it."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from linkchat.core.client.history import (
    _SCHEMA_V1,
    _SCHEMA_V2,
    History,
    IdentityConflict,
    NoIdentity,
)
from linkchat.core.crypto.identity import load_or_create_identity, user_id_of
from linkchat.core.crypto.suite import (
    LinkChatX3DH,
    accept_ratchet,
    new_x3dh_state,
    start_ratchet,
)
from linkchat.core.types import MailboxId, UserId


def create_identity(history: History) -> LinkChatX3DH:
    """First start: nothing stored yet."""
    assert history.load_identity() is None
    return load_or_create_identity(history)


def reload_identity(history: History) -> LinkChatX3DH:
    """Every later start: the stored identity comes back."""
    assert history.load_identity() is not None
    return load_or_create_identity(history)


# -- UserId -------------------------------------------------------------------


def test_user_id_is_the_hash_of_the_identity_key() -> None:
    key = bytes(range(32))

    assert user_id_of(key) == hashlib.blake2b(key, digest_size=32).hexdigest()
    assert user_id_of(key) == user_id_of(key)
    assert len(user_id_of(key)) == 64


@pytest.mark.parametrize("length", [0, 31, 33, 57])
def test_user_id_rejects_a_key_of_the_wrong_size(length: int) -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        user_id_of(b"k" * length)


def test_two_clients_get_different_user_ids(tmp_path: Path) -> None:
    a, b = History(tmp_path / "a.db"), History(tmp_path / "b.db")
    create_identity(a)
    create_identity(b)

    assert a.me != b.me
    a.close()
    b.close()


# -- first start, and every start after ----------------------------------------


def test_a_fresh_database_has_no_identity(tmp_path: Path) -> None:
    history = History(tmp_path / "client.db")

    assert history.load_identity() is None
    with pytest.raises(NoIdentity):
        _ = history.me
    with pytest.raises(NoIdentity):
        history.open_conversation(
            UserId("bbbb"), send_mailbox=MailboxId("s"), recv_mailbox=MailboxId("r")
        )
    history.close()


def test_the_identity_survives_a_restart(tmp_path: Path) -> None:
    path = tmp_path / "client.db"
    first = History(path)
    state = create_identity(first)
    me = first.me
    first.close()

    again = History(path)  # no `me` passed: it is read back
    restored = reload_identity(again)

    assert again.me == me == user_id_of(restored.bundle.identity_key)
    assert restored.bundle == state.bundle
    again.close()


async def test_bob_restarts_between_publishing_and_the_first_message(
    tmp_path: Path,
) -> None:
    """Requirement R3 across a restart.

    Bob publishes his bundle and goes offline; his process restarts. Alice opens
    a session with the bundle she fetched before the restart, and Bob, with his
    identity reloaded from SQLite, must still accept it.
    """
    path = tmp_path / "bob.db"
    bob_db = History(path)
    published = create_identity(bob_db).bundle  # what the relays now hold
    bob_db.close()

    alice = new_x3dh_state()
    sk_a, ad_a, header = await alice.get_shared_secret_active(
        published, require_pre_key=False
    )
    _, first = await start_ratchet(sk_a, published.signed_pre_key, b"hola", ad_a)

    bob_db = History(path)
    bob = reload_identity(bob_db)
    sk_b, ad_b, spk = await bob.get_shared_secret_passive(header, require_pre_key=False)
    _, plaintext = await accept_ratchet(sk_b, spk.priv, first, ad_b)

    assert plaintext == b"hola"
    bob_db.close()


# -- schema -------------------------------------------------------------------


def test_a_version_2_file_migrates_to_version_3(tmp_path: Path) -> None:
    path = tmp_path / "old.db"
    db = sqlite3.connect(path)
    db.executescript(_SCHEMA_V1 + _SCHEMA_V2)
    db.execute(
        "INSERT INTO conversations (peer, send_mailbox, recv_mailbox)"
        " VALUES ('bbbb', 'out', 'in')"
    )
    db.execute("PRAGMA user_version = 2")
    db.commit()
    db.close()

    history = History(path, UserId("aaaa"))

    assert history.peers() == [UserId("bbbb")]
    assert history.load_identity() is None  # the new table exists, empty
    tables = {r[0] for r in history._conn.execute("SELECT name FROM sqlite_master")}
    assert {"identity", "sessions"} <= tables
    history.close()


def test_passing_me_still_works_without_a_stored_identity(tmp_path: Path) -> None:
    """The older tests construct History(path, me): that path must keep working."""
    history = History(tmp_path / "client.db", UserId("aaaa"))

    assert history.me == "aaaa"
    history.close()


# -- one database, one identity ------------------------------------------------


def test_a_second_identity_is_refused(tmp_path: Path) -> None:
    history = History(tmp_path / "client.db")
    first = create_identity(history)
    other = new_x3dh_state()

    with pytest.raises(IdentityConflict):
        history.save_identity(user_id_of(other.bundle.identity_key), dict(other.json))

    assert history.me == user_id_of(first.bundle.identity_key)
    loaded = history.load_identity()
    assert loaded is not None and loaded[0] == history.me  # the row is untouched
    history.close()


def test_saving_the_same_identity_again_is_fine(tmp_path: Path) -> None:
    history = History(tmp_path / "client.db")
    state = create_identity(history)

    history.save_identity(history.me, dict(state.json))  # e.g. after an SPK rotation

    assert reload_identity(history).bundle == state.bundle
    history.close()


def test_an_explicit_me_cannot_be_contradicted(tmp_path: Path) -> None:
    history = History(tmp_path / "client.db", UserId("aaaa"))
    state = new_x3dh_state()

    with pytest.raises(IdentityConflict):
        history.save_identity(user_id_of(state.bundle.identity_key), dict(state.json))
    history.close()


def test_load_or_create_is_stable_across_calls(tmp_path: Path) -> None:
    history = History(tmp_path / "client.db")

    first = load_or_create_identity(history)
    second = load_or_create_identity(history)

    assert first.bundle == second.bundle
    history.close()


def test_a_stored_id_that_does_not_match_the_key_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "client.db"
    history = History(path)
    create_identity(history)
    history._conn.execute("UPDATE identity SET user_id = 'forged' WHERE id = 1")
    history.close()

    with pytest.raises(IdentityConflict, match="does not match"):
        load_or_create_identity(History(path))
