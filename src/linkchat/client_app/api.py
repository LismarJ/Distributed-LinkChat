"""The HTTP and WebSocket API the browser talks to.

Built around a `ChatClient` passed in, so it runs the same over real relays or
the fake transport. It never handles ciphertext: everything it returns comes
from the local history, already decrypted, and everything it accepts goes
through `ChatClient`, which encrypts.

The WebSocket only says that a conversation changed; the browser then fetches
it again with `GET /conversations/{peer}`. So a dropped notification costs a
stale view until the next one, never a wrong one.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel, Field

from linkchat.core.client.client import ChatClient
from linkchat.core.crypto.sessions import NoBundle
from linkchat.core.envelope import PayloadTooLarge
from linkchat.core.types import UserId

_USER_ID = re.compile(r"^[0-9a-f]{64}$")
"""A UserId: BLAKE2b-256 of an identity key, in lowercase hex."""


class NewContact(BaseModel):
    user_id: str


class NewMessage(BaseModel):
    text: str = Field(min_length=1)


class MessageView(BaseModel):
    envelope_id: str
    from_me: bool
    text: str
    state: str
    sent_at: float
    after_gap: bool
    """Shown although a message before it never arrived."""


class ConversationView(BaseModel):
    peer: str
    messages: list[MessageView]


def create_app(client: ChatClient, *, run_client: bool = True) -> FastAPI:
    """The API over `client`. With `run_client`, the server starts and stops it."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if run_client:
            await client.start()
        try:
            yield
        finally:
            if run_client:
                await client.stop()

    app = FastAPI(title="LinkChat", lifespan=lifespan)

    def known_peer(peer: str) -> UserId:
        if peer not in client.history.peers():
            raise HTTPException(status.HTTP_404_NOT_FOUND, "not a contact")
        return UserId(peer)

    @app.get("/me")
    async def me() -> dict[str, str]:
        """This client's UserId: what goes in an invitation."""
        return {"user_id": client.me}

    @app.get("/contacts")
    async def contacts() -> dict[str, list[str]]:
        return {"contacts": list(client.history.peers())}

    @app.post("/contacts", status_code=status.HTTP_201_CREATED)
    async def add_contact(body: NewContact) -> dict[str, str]:
        if not _USER_ID.match(body.user_id):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                "a UserId is 64 lowercase hex digits",
            )
        if body.user_id == client.me:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "that is you")
        client.add_contact(UserId(body.user_id))
        return {"user_id": body.user_id}

    @app.post("/conversations/{peer}/messages", status_code=status.HTTP_201_CREATED)
    async def send(peer: str, body: NewMessage) -> dict[str, str]:
        target = known_peer(peer)
        try:
            eid = await client.send(target, body.text)
        except NoBundle as exc:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "no relay has this contact's keys yet; they must come online once",
            ) from exc
        except PayloadTooLarge as exc:
            raise HTTPException(
                status.HTTP_413_CONTENT_TOO_LARGE, "message too long"
            ) from exc
        return {"envelope_id": eid, "state": "pending"}

    @app.get("/conversations/{peer}")
    async def conversation(peer: str) -> ConversationView:
        target = known_peer(peer)
        return ConversationView(
            peer=target,
            messages=[
                MessageView(
                    envelope_id=shown.message.envelope_id,
                    from_me=shown.message.outgoing,
                    text=shown.message.text,
                    state=shown.message.state.value,
                    sent_at=shown.message.sent_at,
                    after_gap=shown.after_gap,
                )
                for shown in client.ordered_messages(target)
            ],
        )

    @app.websocket("/ws")
    async def events(socket: WebSocket) -> None:
        """Pushes `{"kind", "peer", "envelope_id"}` whenever a conversation changes."""
        await socket.accept()
        queue = client.events.subscribe()
        try:
            while True:
                event = await queue.get()
                await socket.send_json(
                    {
                        "kind": event.kind,
                        "peer": event.peer,
                        "envelope_id": event.envelope_id,
                    }
                )
        except WebSocketDisconnect:
            pass
        finally:
            client.events.unsubscribe(queue)

    return app
