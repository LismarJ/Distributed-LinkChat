"""The browser's API over two real clients and the fake relays."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from linkchat.client_app.api import create_app
from linkchat.core.client.client import ChatClient
from linkchat.core.constants import RELAY_COUNT, RELAY_NAMES
from linkchat.core.envelope import MAX_PLAINTEXT
from linkchat.core.ring import primaries

from .test_client import Relays, make_client, two_friends


def api(client: ChatClient) -> httpx.AsyncClient:
    """HTTP against the app, in this event loop; the test drives the client."""
    transport = httpx.ASGITransport(app=create_app(client, run_client=False))
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture
async def friends(tmp_path: Path) -> AsyncIterator[tuple[ChatClient, ChatClient]]:
    alice, bob = await two_friends(tmp_path, Relays())
    yield alice, bob
    alice.history.close()
    bob.history.close()


async def deliver(sender: ChatClient, receiver: ChatClient) -> None:
    """What the outbox and receive loops do, one round each."""
    await sender.outbox.run_once()
    await receiver.receiver.receive_once(sender.me)


# -- identity and contacts -----------------------------------------------------------


async def test_me_is_the_user_id(friends) -> None:
    alice, _ = friends
    async with api(alice) as http:
        response = await http.get("/api/me")

    assert response.status_code == 200
    assert response.json() == {"user_id": alice.me}


async def test_adding_a_contact(tmp_path) -> None:
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays)
    bob = make_client(tmp_path / "bob.db", relays)

    async with api(alice) as http:
        added = await http.post("/api/contacts", json={"user_id": bob.me})
        listed = await http.get("/api/contacts")

    assert added.status_code == 201
    assert listed.json() == {"contacts": [bob.me]}


@pytest.mark.parametrize(
    ("user_id", "code"),
    [("not hex at all", 422), ("ab" * 31, 422), ("AB" * 32, 422)],
)
async def test_a_malformed_user_id_is_refused(friends, user_id: str, code: int) -> None:
    alice, _ = friends
    async with api(alice) as http:
        response = await http.post("/api/contacts", json={"user_id": user_id})

    assert response.status_code == code


async def test_oneself_is_not_a_contact(friends) -> None:
    alice, _ = friends
    async with api(alice) as http:
        response = await http.post("/api/contacts", json={"user_id": alice.me})

    assert response.status_code == 400


# -- messages -----------------------------------------------------------


async def test_a_message_goes_through_both_apis(friends) -> None:
    alice, bob = friends
    async with api(alice) as alice_http, api(bob) as bob_http:
        sent = await alice_http.post(
            f"/api/conversations/{bob.me}/messages", json={"text": "hola"}
        )
        await deliver(alice, bob)
        theirs = (await bob_http.get(f"/api/conversations/{alice.me}")).json()
        mine = (await alice_http.get(f"/api/conversations/{bob.me}")).json()

    assert sent.status_code == 201
    assert sent.json()["state"] == "pending"
    [received] = theirs["messages"]
    assert received["text"] == "hola"
    assert received["from_me"] is False
    assert received["state"] == "received"
    assert received["after_gap"] is False
    assert mine["messages"][0]["state"] == "sent"


async def test_unknown_peer_is_404(friends) -> None:
    alice, _ = friends
    stranger = "0" * 64
    async with api(alice) as http:
        read = await http.get(f"/api/conversations/{stranger}")
        write = await http.post(
            f"/api/conversations/{stranger}/messages", json={"text": "x"}
        )

    assert (read.status_code, write.status_code) == (404, 404)


async def test_no_bundle_is_a_clear_409(tmp_path) -> None:
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays)
    bob = make_client(tmp_path / "bob.db", relays)  # never published
    alice.add_contact(bob.me)

    async with api(alice) as http:
        response = await http.post(
            f"/api/conversations/{bob.me}/messages", json={"text": "hi"}
        )

    assert response.status_code == 409
    assert "keys" in response.json()["detail"]


async def test_relays_not_answering_is_a_503_not_a_409(tmp_path) -> None:
    """Bob has published, but no relay answers: retrying later will work."""
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays)
    bob = make_client(tmp_path / "bob.db", relays)
    alice.add_contact(bob.me)
    await bob.publish()
    relays.cluster.kill(*RELAY_NAMES)

    async with api(alice) as http:
        response = await http.post(
            f"/api/conversations/{bob.me}/messages", json={"text": "hi"}
        )

    assert response.status_code == 503
    assert "no relay answered" in response.json()["detail"]


async def test_empty_and_oversized_messages_are_refused(friends) -> None:
    alice, bob = friends
    path = f"/api/conversations/{bob.me}/messages"
    async with api(alice) as http:
        empty = await http.post(path, json={"text": ""})
        huge = await http.post(path, json={"text": "x" * (MAX_PLAINTEXT + 1)})
        listed = await http.get(f"/api/conversations/{bob.me}")

    assert empty.status_code == 422
    assert huge.status_code == 413
    assert listed.json()["messages"] == []


async def test_the_api_never_returns_ciphertext(friends) -> None:
    alice, bob = friends
    async with api(alice) as http:
        await http.post(f"/api/conversations/{bob.me}/messages", json={"text": "plain"})
        body = (await http.get(f"/api/conversations/{bob.me}")).text

    [item] = alice.history.due_outbox(now=1e12)
    assert "plain" in body
    assert item.envelope.hex()[:64] not in body


# -- the server's lifecycle and the WebSocket ------------------------------------------


def test_the_server_starts_and_stops_the_client(tmp_path) -> None:
    alice = make_client(tmp_path / "alice.db", Relays())

    def running() -> bool:
        return alice._started and "outbox" in alice._tasks

    with TestClient(create_app(alice)):
        assert running()
    assert not running()
    assert alice._tasks == {}


def test_the_websocket_announces_a_new_message(tmp_path) -> None:
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays)
    bob = make_client(tmp_path / "bob.db", relays)
    alice.add_contact(bob.me)
    asyncio.run(bob.publish())

    path = f"/api/conversations/{bob.me}/messages"
    with TestClient(create_app(alice)) as http, http.websocket_connect("/api/ws") as ws:
        sent = http.post(path, json={"text": "hola"})
        event = ws.receive_json()

    assert event == {
        "kind": "message",
        "peer": bob.me,
        "envelope_id": sent.json()["envelope_id"],
    }


# -- the overlay view and the UI (C9, C10) -------------------------------------------


async def test_relays_endpoint_marks_the_three_primaries(friends) -> None:
    alice, bob = friends
    async with api(alice) as http:
        response = await http.get("/api/relays", params={"peer": bob.me})

    assert response.status_code == 200
    relays = response.json()
    assert len(relays) == RELAY_COUNT
    assert [r["rank"] for r in relays] == list(range(RELAY_COUNT))
    assert [r["holds_mailbox"] for r in relays] == [True, True, True, False, False]
    assert all(r["state"] == "ready" for r in relays)


async def test_relays_endpoint_shows_a_killed_relay_as_unreachable(friends) -> None:
    alice, bob = friends
    mailbox = alice.history.send_mailbox(bob.me)
    victim = primaries(mailbox)[0]
    alice._directory._cluster.kill(victim)  # the fake cluster behind the directory

    async with api(alice) as http:
        relays = (await http.get("/api/relays", params={"peer": bob.me})).json()

    by_name = {r["name"]: r for r in relays}
    assert by_name[victim]["state"] == "unreachable"
    assert by_name[victim]["holds_mailbox"]  # still a primary, just absent
    assert sum(r["state"] == "ready" for r in relays) == RELAY_COUNT - 1


async def test_relays_endpoint_without_a_peer_marks_nothing(friends) -> None:
    alice, _ = friends
    async with api(alice) as http:
        relays = (await http.get("/api/relays")).json()

    assert len(relays) == RELAY_COUNT
    assert not any(r["holds_mailbox"] for r in relays)


async def test_relays_endpoint_refuses_an_unknown_peer(friends) -> None:
    alice, _ = friends
    async with api(alice) as http:
        response = await http.get("/api/relays", params={"peer": "f" * 64})

    assert response.status_code == 404


async def test_the_browser_ui_is_served_and_needs_no_network(friends) -> None:
    """The committed build ships in the package and fetches nothing from outside."""
    alice, _ = friends
    async with api(alice) as http:
        page = await http.get("/")
        assets = [
            await http.get("/" + ref.lstrip("/"))
            for ref in re.findall(r'(?:src|href)="(/assets/[^"]+)"', page.text)
        ]

    assert page.status_code == 200
    assert "LinkChat" in page.text
    assert len(assets) >= 2  # at least the script and the stylesheet
    assert all(asset.status_code == 200 for asset in assets)
    # Nothing may be fetched from a third party at runtime.
    for asset in (page, *assets):
        assert "//cdn" not in asset.text
        assert not re.search(r"""(?:src|href)=["']?https?://""", asset.text)
        assert not re.search(r"""(?:fetch|import)\(["']https?://""", asset.text)


async def test_the_api_still_wins_over_the_static_mount(friends) -> None:
    """The UI is mounted at `/`, so every API route must be matched first."""
    alice, bob = friends
    async with api(alice) as http:
        assert (await http.get("/api/me")).json() == {"user_id": alice.me}
        assert (await http.get("/api/contacts")).status_code == 200
        assert (await http.get("/api/conversations/" + bob.me)).status_code == 200
        assert (await http.get("/api/relays")).status_code == 200


# -- the built UI --------------------------------------------------------------------


async def test_the_ui_is_served_beside_the_api(friends, tmp_path) -> None:
    alice, _ = friends
    static = tmp_path / "static"
    static.mkdir()
    (static / "index.html").write_text("<!doctype html><title>LinkChat</title>")
    app = create_app(alice, run_client=False, static_dir=static)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        page = await http.get("/")
        me = await http.get("/api/me")

    assert page.status_code == 200
    assert "LinkChat" in page.text
    assert me.json() == {"user_id": alice.me}


async def test_without_a_build_only_the_api_answers(friends, tmp_path) -> None:
    alice, _ = friends
    app = create_app(alice, run_client=False, static_dir=tmp_path / "missing")
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        page = await http.get("/")

    assert page.status_code == 404


def test_a_closed_websocket_unsubscribes_at_once(tmp_path) -> None:
    alice = make_client(tmp_path / "alice.db", Relays())

    with TestClient(create_app(alice)) as http:
        with http.websocket_connect("/api/ws"):
            pass
        # No event was published: the server noticed the close by itself.
        for _ in range(100):
            if not alice.events._subscribers:
                break
            time.sleep(0.01)
        assert not alice.events._subscribers
