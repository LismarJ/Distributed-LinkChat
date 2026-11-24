"""The client program: the HTTP and WebSocket API, and its entry point.

Runs in the same process as the Ray driver, with a single Uvicorn worker and no
forking: a forked child inherits Ray's sockets without synchronization, which
is one of Ray's documented anti-patterns.
"""
