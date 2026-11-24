"""The client program: `python -m linkchat.client_app`.

Started by the client container's entrypoint after `ray start` has joined this
node to the cluster. One process, one Uvicorn worker, no reload and no fork:
a child forked after Ray starts would inherit its sockets.

Settings, from the environment:
    LINKCHAT_DATA_DIR  where the SQLite file lives (the container's volume)
    LINKCHAT_PORT      the port the API listens on (default 8000)
    RAY_ADDRESS        the cluster to join (default: the one this node is in)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import ray
import uvicorn

from linkchat.client_app.api import create_app
from linkchat.core.client.client import ChatClient
from linkchat.core.client.history import History
from linkchat.core.clock import SystemClock
from linkchat.ray_runtime.bootstrap import NAMESPACE
from linkchat.ray_runtime.gateway import RayRelayDirectory


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    data_dir = Path(os.environ.get("LINKCHAT_DATA_DIR", "data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    port = int(os.environ.get("LINKCHAT_PORT", "8000"))

    ray.init(address=os.environ.get("RAY_ADDRESS", "auto"), namespace=NAMESPACE)
    client = ChatClient(
        History(data_dir / "client.db"), RayRelayDirectory(), SystemClock()
    )
    logging.getLogger(__name__).info("this client is %s", client.me)

    uvicorn.run(create_app(client), host="0.0.0.0", port=port, workers=1, reload=False)


if __name__ == "__main__":
    main()
