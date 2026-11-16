"""FastAPI and the WebSocket to the browser, plus the built React assets.

Runs in the same process as the Ray driver, with a single Uvicorn worker and no
forking: a forked child inherits Ray's sockets without synchronization, which
is one of Ray's documented anti-patterns.

Empty today. Its contents are tasks C8 and C9.
"""
