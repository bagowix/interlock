import socket
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from interlock.outcome import Outcome
from interlock.state import State


class FakeClock:
    """Deterministic clock for tests: time advances only when told to."""

    def __init__(self) -> None:
        self._now = 0.0

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


class RecordingListener:
    """An EventListener that records every event it receives, for assertions."""

    def __init__(self) -> None:
        self.state_changes: list[tuple[State, State]] = []
        self.calls: list[tuple[Outcome, float]] = []
        self.rejected = 0
        self.resets = 0

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        self.state_changes.append((old, new))

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        self.calls.append((outcome, duration))

    def on_rejected(self, *, name: str) -> None:
        self.rejected += 1

    def on_reset(self, *, name: str) -> None:
        self.resets += 1


@pytest.fixture
def listener() -> RecordingListener:
    return RecordingListener()


@dataclass
class Upstream:
    """A thread-safe switch for a fake HTTP upstream's behaviour.

    Tests flip ``status``; the server thread reads it and counts every request
    that actually arrived. The counter proves whether the breaker reached the
    socket or short-circuited before it.
    """

    status: int = 200
    body: bytes = b'ok'
    _received: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self) -> None:
        with self._lock:
            self._received += 1

    @property
    def received(self) -> int:
        with self._lock:
            return self._received


class _UpstreamHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        upstream: Upstream = self.server.upstream  # type: ignore[attr-defined]
        upstream.record()
        self.send_response(upstream.status)
        self.send_header('Content-Length', str(len(upstream.body)))
        self.end_headers()
        self.wfile.write(upstream.body)

    def log_message(self, *args: object) -> None:
        """Silence the default per-request stderr logging."""


@pytest.fixture
def serve() -> Iterator[Callable[..., str]]:
    """Start fake upstreams on loopback; tear them all down at test end.

    Yields a starter that binds a real server to ``127.0.0.1:0`` and returns its
    URL. ``url_host`` lets a test address the same loopback server under a
    different hostname (``localhost`` vs ``127.0.0.1``) to get distinct per-host
    breakers.
    """
    servers: list[tuple[ThreadingHTTPServer, threading.Thread]] = []

    def start(upstream: Upstream, *, url_host: str = '127.0.0.1') -> str:
        server = ThreadingHTTPServer(('127.0.0.1', 0), _UpstreamHandler)
        server.upstream = upstream  # type: ignore[attr-defined]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append((server, thread))
        return f'http://{url_host}:{server.server_address[1]}'

    yield start

    for server, thread in servers:
        server.shutdown()
        server.server_close()
        thread.join()


def closed_port() -> int:
    """Return a loopback port with nothing listening (connections refused)."""
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        return probe.getsockname()[1]
