import socket
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from hypothesis import strategies as st

from interlock.config import Config
from interlock.outcome import Outcome
from interlock.state import State

_LARGE_WINDOW = 500


@st.composite
def configs(draw: st.DrawFn) -> Config:
    """Build a valid ``Config`` honouring the ``max_concurrent_probes`` bound.

    Shared by the hypothesis suites over the state machine: the property tests
    and the model-based one.
    """
    permitted = draw(st.integers(min_value=1, max_value=10))
    return Config(
        failure_rate_threshold=draw(st.floats(min_value=0.01, max_value=1.0)),
        minimum_number_of_calls=draw(st.integers(min_value=1, max_value=20)),
        slow_call_rate_threshold=draw(st.floats(min_value=0.01, max_value=1.0)),
        permitted_calls_in_half_open=permitted,
        max_concurrent_probes=draw(st.integers(min_value=1, max_value=permitted)),
        wait_duration_in_open=draw(st.floats(min_value=1.0, max_value=100.0)),
        window_size=_LARGE_WINDOW,
    )


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


class ExplodingListener:
    """An EventListener that records the hook it received, then raises.

    The failure mode the safe-dispatch policy isolates. Recording *before*
    raising is what makes the assertions possible: the hook fired, and the
    caller survived it.
    """

    def __init__(self) -> None:
        self.events: list[str] = []

    def _fire(self, hook: str) -> None:
        self.events.append(hook)
        msg = f'listener {hook} exploded'
        raise RuntimeError(msg)

    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        self._fire('on_state_change')

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        self._fire('on_call')

    def on_rejected(self, *, name: str) -> None:
        self._fire('on_rejected')

    def on_reset(self, *, name: str) -> None:
        self._fire('on_reset')

    def on_storage_degraded(self, *, name: str, error: BaseException) -> None:
        self._fire('on_storage_degraded')

    def on_storage_recovered(self, *, name: str) -> None:
        self._fire('on_storage_recovered')

    def on_retry(self, *, name: str, attempt: int, delay: float) -> None:
        self._fire('on_retry')

    def on_bulkhead_rejected(self, *, name: str) -> None:
        self._fire('on_bulkhead_rejected')

    def on_fallback(self, *, name: str, error: BaseException) -> None:
        self._fire('on_fallback')


@pytest.fixture
def exploding() -> ExplodingListener:
    return ExplodingListener()


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
