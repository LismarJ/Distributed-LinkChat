"""Several clients on fake relays, for developing the UI without Ray or Docker.

    python scripts/dev_ui.py [--clients alice,bob,carol] [--port 8000]
                             [--data-dir DIR] [--ttl SECONDS] [--hold SECONDS]
                             [--verbose]

Each client is the real `create_app` over a real `ChatClient`, on port 8000,
8001, ... (from `--port`) in the order given, all on one in-process
`FakeCluster`, all already contacts of each other. Open the built UI at
http://localhost:800N, or point a Vite dev server at one:

    cd frontend && LINKCHAT_API_PORT=8001 npm run dev

While it runs, type commands to break things (`help` lists them): take relays
down and up, turn a client off and on, lose a client's next message.

The relays live in memory, so mail in flight is lost on exit. Histories and
identities live in a temporary directory, or in `--data-dir` if given, and then
survive restarts. Development only: it imports the test fakes and never ships.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import shlex
import signal
import sys
import tempfile
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "src")]

import uvicorn  # noqa: E402

from linkchat.client_app.api import create_app  # noqa: E402
from linkchat.core.client.client import DEFAULT_TTL_S, ChatClient  # noqa: E402
from linkchat.core.client.history import History  # noqa: E402
from linkchat.core.client.ordering import DEFAULT_HOLD_TIMEOUT_S  # noqa: E402
from linkchat.core.clock import SystemClock  # noqa: E402
from linkchat.core.constants import RELAY_NAMES  # noqa: E402
from linkchat.core.ports import Record  # noqa: E402
from linkchat.core.replication import SendResult, send_to_three  # noqa: E402
from linkchat.core.ring import primaries  # noqa: E402
from linkchat.core.types import RelayName  # noqa: E402
from tests.fakes.transport import FakeCluster  # noqa: E402

POLL_S = 0.3
"""How often a parked receive loop looks at its mailbox again."""

DEFAULT_PORT = 8000

HELP = f"""commands:
  down [N ...]   take relays down (all, or relay-N); with three or more
                 down, sends stay pending; with fewer, stand-ins cover
  up [N ...]     bring relays back up (all, or relay-N)
  off NAME       turn a client off: it neither sends nor receives
  on NAME        turn it back on: it catches up with what the relays hold
  lose NAME      NAME's next message is confirmed but never stored (a gap)
  status         relays, clients and their UserIds
  help           this text
relays: {", ".join(n.removeprefix("relay-") for n in RELAY_NAMES)}"""


class PollingClock(SystemClock):
    """Real time, but no sleep lasts more than `POLL_S`.

    The fake relays do not park a long poll: `wait_for_mail` sleeps the whole
    timeout and then looks. With this clock it looks every `POLL_S` instead, so
    a message shows up at once rather than at the end of the timeout.
    """

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(min(seconds, POLL_S))


SendFn = Callable[[Record], Awaitable[SendResult]]


class DevCluster:
    """The fake relays, plus the faults the console can switch on."""

    def __init__(self) -> None:
        self.cluster = FakeCluster(clock=PollingClock())
        self.lose_next: set[str] = set()

    def sender(self, name: str) -> SendFn:
        """The real replicated write (R5), unless the console asked to lose one."""
        directory = self.cluster.directory()
        clock = SystemClock()

        async def send(record: Record) -> SendResult:
            if name in self.lose_next:
                self.lose_next.discard(name)
                say(f"lost {name}'s message {record.envelope_id[:8]}")
                return SendResult(confirmed_relays=primaries(record.mailbox_id))
            return await send_to_three(record, directory, clock=clock)

        return send

    def down(self) -> set[RelayName]:
        return self.cluster.faults.killed


def say(text: str) -> None:
    print(f"* {text}", flush=True)


def relay_names(args: list[str]) -> list[RelayName] | None:
    """`[]` means all relays; None means an argument named no relay."""
    if not args:
        return list(RELAY_NAMES)
    wanted = [RelayName(a if a.startswith("relay-") else f"relay-{a}") for a in args]
    return wanted if all(n in RELAY_NAMES for n in wanted) else None


class Console:
    """Reads commands from the terminal without blocking the event loop."""

    def __init__(
        self, dev: DevCluster, clients: dict[str, ChatClient], first_port: int
    ) -> None:
        self.dev = dev
        self.clients = clients
        self.first_port = first_port
        self.off: set[str] = set()
        self._lines: asyncio.Queue[str] = asyncio.Queue()
        self._partial = b""
        self._worker: asyncio.Task[None] | None = None

    def attach(self) -> None:
        try:
            asyncio.get_running_loop().add_reader(sys.stdin.fileno(), self._on_input)
        except (OSError, ValueError):
            say("stdin cannot be watched (a file?): no commands this run")
            return
        self._worker = asyncio.create_task(self._work())

    def detach(self) -> None:
        with contextlib.suppress(OSError, ValueError):
            asyncio.get_running_loop().remove_reader(sys.stdin.fileno())
        if self._worker is not None:
            self._worker.cancel()

    def _on_input(self) -> None:
        # Raw reads: a buffered readline would sit on later lines unannounced.
        chunk = os.read(sys.stdin.fileno(), 4096)
        if not chunk:  # end of input: stop listening, keep serving
            with contextlib.suppress(OSError, ValueError):
                asyncio.get_running_loop().remove_reader(sys.stdin.fileno())
            return
        *lines, self._partial = (self._partial + chunk).split(b"\n")
        for line in lines:
            self._lines.put_nowait(line.decode(errors="replace"))

    async def _work(self) -> None:
        """One command at a time, in the order typed."""
        while True:
            line = await self._lines.get()
            try:
                await self.run(line)
            except Exception as exc:
                say(f"command failed: {exc!r}")

    async def run(self, line: str) -> None:
        try:
            words = shlex.split(line)
        except ValueError:
            words = []
        if not words:
            return
        command, args = words[0].lower(), words[1:]
        cluster = self.dev.cluster

        if command in ("down", "up"):
            names = relay_names(args)
            if names is None:
                say(f"unknown relay in {' '.join(args)}")
                return
            if command == "down":
                cluster.kill(*names)
            else:
                cluster.revive(*names)
                # Retry what is due now; the rest follows its backoff (<= 30 s).
                for client in self.clients.values():
                    client.outbox.notify()
            self.status_relays()
        elif command in ("off", "on", "lose"):
            if len(args) != 1 or args[0] not in self.clients:
                say(f"{command} NAME, NAME one of: {', '.join(self.clients)}")
                return
            name = args[0]
            client = self.clients[name]
            if command == "off" and name not in self.off:
                await client.stop()
                self.off.add(name)
                say(f"{name} is off")
            elif command == "on" and name in self.off:
                await client.start()
                self.off.discard(name)
                say(f"{name} is on")
            elif command == "lose":
                self.dev.lose_next.add(name)
                say(f"{name}'s next message will be lost")
            else:
                say(f"{name} is already {command}")
        elif command == "status":
            self.status_relays()
            for i, (name, client) in enumerate(self.clients.items()):
                state = "off" if name in self.off else "on "
                url = f"http://localhost:{self.first_port + i}"
                print(f"  {name:<8} {state} {url}  {client.me}", flush=True)
        else:
            print(HELP, flush=True)

    def status_relays(self) -> None:
        down = self.dev.down()
        marks = [
            f"{n.removeprefix('relay-')}:{'DOWN' if n in down else 'up'}"
            for n in RELAY_NAMES
        ]
        say("relays " + "  ".join(marks))


class QuietServer(uvicorn.Server):
    """Leaves Ctrl+C to `serve`, which stops every server together.

    A plain `uvicorn.Server` traps the signal itself and raises it again once
    it has stopped, which tears down the other servers mid-shutdown.
    """

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


class ShortTracebacks(logging.Formatter):
    """One line per error: with relays down, the loops log one every second."""

    def formatException(self, ei) -> str:  # type: ignore[no-untyped-def]
        return f"    {ei[0].__name__}: {ei[1]}" if ei[0] else ""


async def serve(
    names: list[str], data_dir: Path, *, port: int, ttl_s: float, hold_s: float
) -> None:
    dev = DevCluster()
    clients = {
        name: ChatClient(
            History(data_dir / f"{name}.db"),
            dev.cluster.directory(),
            SystemClock(),
            send=dev.sender(name),
            ttl_s=ttl_s,
            hold_timeout_s=hold_s,
        )
        for name in names
    }
    for client in clients.values():
        for other in clients.values():
            if other is not client:
                client.add_contact(other.me)

    servers = [
        QuietServer(
            uvicorn.Config(create_app(client), port=port + i, log_level="warning")
        )
        for i, client in enumerate(clients.values())
    ]

    def stop_all() -> None:
        for server in servers:
            server.should_exit = True

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_all)
    console = Console(dev, clients, port)
    await console.run("status")
    print(f"data in {data_dir}. Type `help` for commands, Ctrl+C to stop.", flush=True)

    tasks = [asyncio.create_task(server.serve()) for server in servers]
    console.attach()
    try:
        # If one server stops (its port already taken, say), stop them all.
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        console.detach()
        stop_all()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("stopped.", flush=True)
        for client in clients.values():
            client.history.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--clients",
        default="alice,bob",
        help="comma-separated names, one port each (default: alice,bob)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"the first client's port; the rest follow (default: {DEFAULT_PORT})",
    )
    parser.add_argument("--data-dir", type=Path, help="keep histories here")
    parser.add_argument(
        "--ttl",
        type=float,
        default=DEFAULT_TTL_S,
        help="seconds before an unsent message becomes not_delivered",
    )
    parser.add_argument(
        "--hold",
        type=float,
        default=DEFAULT_HOLD_TIMEOUT_S,
        help="seconds a message waits for a missing earlier one before showing",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="warnings too, with full tracebacks"
    )
    args = parser.parse_args()

    names = [n.strip() for n in args.clients.split(",") if n.strip()]
    if len(names) < 2 or len(set(names)) != len(names):
        parser.error("--clients needs at least two distinct names")

    # Quiet by default: with relays down, the loops warn every second.
    handler = logging.StreamHandler()
    formatter_class = logging.Formatter if args.verbose else ShortTracebacks
    handler.setFormatter(formatter_class("%(asctime)s %(name)s %(message)s"))
    level = logging.WARNING if args.verbose else logging.ERROR
    logging.basicConfig(level=level, handlers=[handler])

    def run(data_dir: Path) -> None:
        asyncio.run(
            serve(names, data_dir, port=args.port, ttl_s=args.ttl, hold_s=args.hold)
        )

    if args.data_dir is not None:
        args.data_dir.mkdir(parents=True, exist_ok=True)
        run(args.data_dir)
    else:
        with tempfile.TemporaryDirectory(prefix="linkchat-dev-") as tmp:
            run(Path(tmp))


if __name__ == "__main__":
    main()
