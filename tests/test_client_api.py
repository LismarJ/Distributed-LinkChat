"""The browser's API over two real clients and the fake relays."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from linkchat.client_app.api import create_app
from linkchat.core.client.client import ChatClient
from linkchat.core.constants import RELAY_COUNT
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
        response = await http.get("/me")

    assert response.status_code == 200
    assert response.json() == {"user_id": alice.me}


async def test_adding_a_contact(tmp_path) -> None:
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays)
    bob = make_client(tmp_path / "bob.db", relays)

    async with api(alice) as http:
        added = await http.post("/contacts", json={"user_id": bob.me})
        listed = await http.get("/contacts")

    assert added.status_code == 201
    assert listed.json() == {"contacts": [bob.me]}


@pytest.mark.parametrize(
    ("user_id", "code"),
    [("not hex at all", 422), ("ab" * 31, 422), ("AB" * 32, 422)],
)
async def test_a_malformed_user_id_is_refused(friends, user_id: str, code: int) -> None:
    alice, _ = friends
    async with api(alice) as http:
        response = await http.post("/contacts", json={"user_id": user_id})

    assert response.status_code == code


async def test_oneself_is_not_a_contact(friends) -> None:
    alice, _ = friends
    async with api(alice) as http:
        response = await http.post("/contacts", json={"user_id": alice.me})

    assert response.status_code == 400


# -- messages -----------------------------------------------------------


async def test_a_message_goes_through_both_apis(friends) -> None:
    alice, bob = friends
    async with api(alice) as alice_http, api(bob) as bob_http:
        sent = await alice_http.post(
            f"/conversations/{bob.me}/messages", json={"text": "hola"}
        )
        await deliver(alice, bob)
        theirs = (await bob_http.get(f"/conversations/{alice.me}")).json()
        mine = (await alice_http.get(f"/conversations/{bob.me}")).json()

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
        read = await http.get(f"/conversations/{stranger}")
        write = await http.post(f"/conversations/{stranger}/messages", json={"text": "x"})

    assert (read.status_code, write.status_code) == (404, 404)


async def test_no_bundle_is_a_clear_409(tmp_path) -> None:
    relays = Relays()
    alice = make_client(tmp_path / "alice.db", relays)
    bob = make_client(tmp_path / "bob.db", relays)  # never published
    alice.add_contact(bob.me)

    async with api(alice) as http:
        response = await http.post(
            f"/conversations/{bob.me}/messages", json={"text": "hi"}
        )

    assert response.status_code == 409
    assert "keys" in response.json()["detail"]


async def test_empty_and_oversized_messages_are_refused(friends) -> None:
    alice, bob = friends
    path = f"/conversations/{bob.me}/messages"
    async with api(alice) as http:
        empty = await http.post(path, json={"text": ""})
        huge = await http.post(path, json={"text": "x" * (MAX_PLAINTEXT + 1)})
        listed = await http.get(f"/conversations/{bob.me}")

    assert empty.status_code == 422
    assert huge.status_code == 413
    assert listed.json()["messages"] == []


async def test_the_api_never_returns_ciphertext(friends) -> None:
    alice, bob = friends
    async with api(alice) as http:
        await http.post(f"/conversations/{bob.me}/messages", json={"text": "plain"})
        body = (await http.get(f"/conversations/{bob.me}")).text

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

    with TestClient(create_app(alice)) as http, http.websocket_connect("/ws") as socket:
        sent = http.post(f"/conversations/{bob.me}/messages", json={"text": "hola"})
        event = socket.receive_json()

    assert event == {
        "kind": "message",
        "peer": bob.me,
        "envelope_id": sent.json()["envelope_id"],
    }


# -- the overlay view and the UI (C9, C10) -------------------------------------------


async def test_relays_endpoint_marks_the_three_primaries(friends) -> None:
    alice, bob = friends
    async with api(alice) as http:
        response = await http.get("/relays", params={"peer": bob.me})

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
        relays = (await http.get("/relays", params={"peer": bob.me})).json()

    by_name = {r["name"]: r for r in relays}
    assert by_name[victim]["state"] == "unreachable"
    assert by_name[victim]["holds_mailbox"]  # still a primary, just absent
    assert sum(r["state"] == "ready" for r in relays) == RELAY_COUNT - 1


async def test_relays_endpoint_without_a_peer_marks_nothing(friends) -> None:
    alice, _ = friends
    async with api(alice) as http:
        relays = (await http.get("/relays")).json()

    assert len(relays) == RELAY_COUNT
    assert not any(r["holds_mailbox"] for r in relays)


async def test_relays_endpoint_refuses_an_unknown_peer(friends) -> None:
    alice, _ = friends
    async with api(alice) as http:
        response = await http.get("/relays", params={"peer": "f" * 64})

    assert response.status_code == 404


async def test_the_browser_ui_is_served_and_needs_no_network(friends) -> None:
    """The UI ships inside the package: no build step, no CDN (ADR-0016)."""
    alice, _ = friends
    async with api(alice) as http:
        page = await http.get("/")
        script = await http.get("/app.js")
        style = await http.get("/app.css")

    assert page.status_code == 200
    assert "LinkChat" in page.text
    assert script.status_code == 200 and style.status_code == 200
    # Nothing may be fetched from a third party at runtime.
    for asset in (page, script, style):
        assert "//cdn" not in asset.text
        assert "http://" not in asset.text.replace("http://test", "")
        assert "https://" not in asset.text


async def test_the_api_still_wins_over_the_static_mount(friends) -> None:
    """The UI is mounted at `/`, so every API route must be matched first."""
    alice, bob = friends
    async with api(alice) as http:
        assert (await http.get("/me")).json() == {"user_id": alice.me}
        assert (await http.get("/contacts")).status_code == 200
        assert (await http.get("/conversations/" + bob.me)).status_code == 200
