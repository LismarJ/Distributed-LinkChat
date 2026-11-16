"""LinkChat: decentralized secure messaging (P08).

Three packages, and the boundary between the first two is the point:

- `core`: all application logic. Never imports Ray.
- `ray_runtime`: the Ray implementation of the core's ports. Phase 1 only.
- `client_app`: the HTTP and WebSocket layer that serves the web UI.
"""

__version__ = "0.0.0"
