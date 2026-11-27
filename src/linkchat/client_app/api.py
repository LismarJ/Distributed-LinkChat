"""The HTTP and WebSocket API the browser talks to.

Built around a `ChatClient` passed in, so it runs the same over real relays or
the fake transport. It never handles ciphertext: everything it returns comes
from the local history, already decrypted, and everything it accepts goes
through `ChatClient`, which encrypts.

Every route lives under `/api`. Everything else is the browser UI: the Vite
build of `frontend/`, committed into `static/` (ADR-0017), so one port carries
both, the image needs no Node, and the Vite dev server proxies a single prefix.

The WebSocket only says that a conversation changed; the browser then fetches
it again with `GET /api/conversations/{peer}`. So a dropped notification costs
a stale view until the next one, never a wrong one.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import (
    APIRouter,
    FastAPI,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from linkchat.core.client.client import ChatClient
from linkchat.core.crypto.sessions import NoBundle
from linkchat.core.envelope import PayloadTooLarge
from linkchat.core.types import UserId

_USER_ID = re.compile(r"^[0-9a-f]{64}$")
"""A UserId: BLAKE2b-256 of an identity key, in lowercase hex."""

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
"""The built browser UI (`npm run build` in `frontend/`), shipped in the package."""


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


class RelayStatusView(BaseModel):
    name: str
    state: str
    """`ready`, `syncing`, or `unreachable`."""

    records: int
    tombstones: int
    hinted_records: int
    holds_mailbox: bool
    """One of the three primaries of this conversation's send mailbox."""

    rank: int


def create_app(
    client: ChatClient,
    *,
    run_client: bool = True,
    static_dir: Path | None = STATIC_DIR,
) -> FastAPI:
    """The API over `client`, plus the UI in `static_dir` when it is there.

    With `run_client`, the server starts and stops the client.
    """

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
    api = APIRouter(prefix="/api")

    def known_peer(peer: str) -> UserId:
        if peer not in client.history.peers():
            raise HTTPException(status.HTTP_404_NOT_FOUND, "not a contact")
        return UserId(peer)

    @api.get("/me")
    async def me() -> dict[str, str]:
        """This client's UserId: what goes in an invitation."""
        return {"user_id": client.me}

    @api.get("/contacts")
    async def contacts() -> dict[str, list[str]]:
        return {"contacts": list(client.history.peers())}

    @api.post("/contacts", status_code=status.HTTP_201_CREATED)
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

    @api.post("/conversations/{peer}/messages", status_code=status.HTTP_201_CREATED)
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

    @api.get("/conversations/{peer}")
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

    @api.get("/relays")
    async def relays(peer: str | None = None) -> list[RelayStatusView]:
        """The overlay view (C10). With `?peer=`, focused on that conversation.

        Never fails because a relay is down: that is the state it exists to show.
        """
        target = known_peer(peer) if peer else None
        return [
            RelayStatusView(
                name=view.name,
                state=view.state,
                records=view.records,
                tombstones=view.tombstones,
                hinted_records=view.hinted_records,
                holds_mailbox=view.holds_mailbox,
                rank=view.rank,
            )
            for view in await client.relays(target)
        ]

    @api.websocket("/ws")
    async def events(socket: WebSocket) -> None:
        """Pushes `{"kind", "peer", "envelope_id"}` whenever a conversation changes.

        Also listens, so a browser that leaves (or a server shutting down) is
        noticed at once, not at the next event.
        """
        await socket.accept()
        queue = client.events.subscribe()

        async def push() -> None:
            while True:
                event = await queue.get()
                await socket.send_json(
                    {
                        "kind": event.kind,
                        "peer": event.peer,
                        "envelope_id": event.envelope_id,
                    }
                )

        pusher = asyncio.create_task(push())
        try:
            while (await socket.receive())["type"] != "websocket.disconnect":
                pass  # the browser sends nothing we act on
        finally:
            pusher.cancel()
            with contextlib.suppress(asyncio.CancelledError, WebSocketDisconnect):
                await pusher
            client.events.unsubscribe(queue)

    app.include_router(api)

    # The browser UI (C9, C10), mounted last so every route above wins over it;
    # `html=True` serves index.html at `/`.
    #
    # The missing-UI case is logged at ERROR rather than passed over, because it
    # is invisible otherwise and the way it happens is a packaging mistake, not a
    # coding one: the directory was once listed in `.gitignore`, which hatchling
    # honours, so `pip install .` produced a client whose API worked perfectly
    # and which served no UI at all. That is worth shouting about at startup
    # instead of discovering it in front of the examiners.
    if static_dir is not None and (static_dir / "index.html").is_file():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="ui")
    elif static_dir is not None:
        log.error(
            "no browser UI at %s: the API will work and / will 404. The UI is "
            "part of the package, so this means it was not built (npm run build "
            "in frontend/) or was excluded from the build (check .gitignore and "
            ".dockerignore).",
            static_dir,
        )
    return app
