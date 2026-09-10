# Production layout for a service with several httpx clients

This layout has carried production traffic: one process, several `httpx.AsyncClient` instances (a shared REST and JSON-RPC client plus SDK clients that accept an httpx client), every host guarded by one breaker registry, rolled out in shadow mode and observed through Prometheus. Names are placeholders; adapt them to the project's conventions. The snippets build on each other in order.

## 1. Settings: thresholds live in deployment config

Keep every `Config` field and the operating mode in the application settings, so the switch from shadow mode to enforcing is a configuration change and never a release:

```python
import ssl
from typing import Literal

import httpx
from interlock import Config, CoreEventListener, Registry, State, WindowType
from interlock.integrations.httpx import AsyncCircuitBreakerTransport, HttpStatusClassifier
from pydantic import BaseModel, Field

BreakerMode = Literal[State.CLOSED, State.DISABLED, State.METRICS_ONLY]


class BreakerSettings(BaseModel):
    mode: BreakerMode = State.METRICS_ONLY
    failure_rate_threshold: float = Field(default=0.5, gt=0, le=1)
    minimum_number_of_calls: int = Field(default=10, ge=1)
    slow_call_duration_seconds: float = Field(default=5, gt=0)
    slow_call_rate_threshold: float = Field(default=1, gt=0, le=1)
    permitted_calls_in_half_open: int = Field(default=10, ge=1)
    max_concurrent_probes: int = Field(default=1, ge=1)
    wait_duration_in_open_seconds: float = Field(default=60, gt=0)
    wait_duration_backoff_multiplier: float = Field(default=2, ge=1)
    wait_duration_in_open_max_seconds: float = Field(default=300, gt=0)
    window_type: WindowType = WindowType.TIME_BASED
    window_size: int = Field(default=60, ge=1)
    failure_statuses: frozenset[int] = frozenset({429, 500, 502, 503, 504})

    def build_config(self) -> Config:
        return Config(
            failure_rate_threshold=self.failure_rate_threshold,
            minimum_number_of_calls=self.minimum_number_of_calls,
            slow_call_duration_threshold=self.slow_call_duration_seconds,
            slow_call_rate_threshold=self.slow_call_rate_threshold,
            permitted_calls_in_half_open=self.permitted_calls_in_half_open,
            max_concurrent_probes=self.max_concurrent_probes,
            wait_duration_in_open=self.wait_duration_in_open_seconds,
            wait_duration_backoff_multiplier=self.wait_duration_backoff_multiplier,
            wait_duration_in_open_max=self.wait_duration_in_open_max_seconds,
            window_type=self.window_type,
            window_size=self.window_size,
        )
```

Decisions behind it:

- `mode` is a `Literal` over `State` values: one source of truth, no parallel enum. `FORCED_OPEN` is deliberately absent. As the initial state of every breaker it turns a healthy process into one that rejects every outgoing call from startup, a silent failure where a crash would be honest.
- The default mode is shadow, so a fresh environment observes before it enforces. The half-open and open-wait fields are exposed from day one even though shadow mode ignores them: enabling `CLOSED` later touches only configuration.
- Thresholds a process reads at startup must exist in every environment before the first deployment of this code. A missing key fails the rollout; it does not fall back to the model default.
- A time-based window of 60 seconds suits uneven traffic; the slow-call duration sits near the client read timeout; a backoff multiplier of 2 capped at 300 seconds stops a dead dependency from being probed at full rate.
- `failure_statuses` can live in the model, but it is a classifier. Keep it out of the per-environment operational config.

## 2. One registry per process, one transport per client

```python
def breaker_name(request: httpx.Request) -> str:
    # Normalise the host (strip an internal DNS suffix, lower-case it) so the
    # breaker name equals the label the dashboards already use.
    return request.url.host.removesuffix('.internal')


class BreakerTransportFactory:
    def __init__(self, settings: BreakerSettings, listener: CoreEventListener) -> None:
        # One registry per process: a host reached from several clients must be
        # one breaker, otherwise minimum_number_of_calls fills at a fraction of
        # the traffic and tripping is asymmetric between clients.
        self._registry = Registry(
            config=settings.build_config(),
            initial_state=settings.mode,
            classifier=HttpStatusClassifier(failure_statuses=settings.failure_statuses),
            listener=listener,
            # The transport sets this only for a registry it creates itself. A
            # probe that never got a connection says nothing about the
            # dependency; in CLOSED an exhausted pool stays a failure.
            unreachable_exceptions=(httpx.PoolTimeout,),
        )

    @property
    def registry(self) -> Registry:
        return self._registry

    def create(
        self,
        transport: httpx.AsyncBaseTransport | None = None,
        *,
        limits: httpx.Limits | None = None,
        verify: ssl.SSLContext | str | bool = True,
    ) -> AsyncCircuitBreakerTransport:
        # httpx applies limits, verify and proxy settings only to a transport it
        # creates itself. A client given transport= drops them silently, so
        # they are configured here, on the inner transport.
        if transport is None:
            transport = (
                httpx.AsyncHTTPTransport(verify=verify)
                if limits is None
                else httpx.AsyncHTTPTransport(verify=verify, limits=limits)
            )

        return AsyncCircuitBreakerTransport(
            transport,
            registry=self._registry,
            name_resolver=breaker_name,
        )

    async def close(self) -> None:
        # Last, after every client that uses a transport from this factory.
        await self._registry.aclose_all()
```

Wiring: build the factory once at startup and hand each root client `transport=factory.create(...)`. An SDK client that accepts an httpx client gets one built the same way, with the SDK's own default `Limits` passed through (read the SDK constructor; the defaults often match httpx's, and relying on that is a trap). Shutdown order: SDK clients, the shared client, the factory. Use an exit stack, so the registry closes even when a client's close raises.

Facts about httpx that bite, verified against httpx 0.28:

- `allow_env_proxies = trust_env and transport is None`: a custom transport also disables `HTTP_PROXY` and `NO_PROXY` detection. Check the deployment manifests before the first rollout.
- Closing one client's transport leaves a shared registry alone; a transport closes only a registry it created. The owner closes the shared one, once.

What to leave unwrapped: observability exporters (metrics, tracing, logging backends), where a breaker can degrade the process recursively; message brokers and database drivers, which are not HTTP and deserve a call-level decision of their own.

Streaming: the transport observes the time to response headers. A failure while reading a streamed body (server-sent events, long downloads) is invisible to the transport breaker. Document that limitation in the runbook, or add a call-level breaker around the whole stream as a separate change.

## 3. Rejections and retries

`CircuitOpenTransportError` is an `httpx.TransportError`, so it lands in existing `except httpx.TransportError` blocks. Treat it as routine: log at WARNING with `retry_after`, answer `503` with `Retry-After` or whatever the edge policy is, and never log it as an exception.

Retry predicates must not match the rejection. Key them on leaf types (`httpx.ConnectError`, `httpx.ReadTimeout`, a status-error predicate) or use `retry_unless_open`. A predicate on the base class (`httpx.TransportError`, `requests.exceptions.RequestException`, `aiohttp.ClientError`) retries every rejection, and the attempts burn in microseconds against a circuit that stays open.

Retries sit above the transport, so the breaker sees every physical attempt. Its failure rate is per attempt, never per user operation. Say so on the dashboard and in the runbook.

## 4. Metrics without the OpenTelemetry extra

When the process exports Prometheus directly and runs no `MeterProvider`, a short listener does the job:

```python
from collections.abc import Iterator

from interlock import Outcome
from prometheus_client import Counter, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

# Buckets must reach past slow_call_duration_threshold; the prometheus_client
# defaults stop at 10 s and hide the p99 the threshold is tuned against.
BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300)

CALL_DURATION = Histogram(
    'circuit_breaker_call_duration_seconds',
    'Calls observed by circuit breakers.',
    labelnames=('breaker', 'outcome'),
    buckets=BUCKETS,
)
REJECTED = Counter(
    'circuit_breaker_rejected_total',
    'Calls rejected by an open circuit.',
    labelnames=('breaker',),
)
STATE_CHANGES = Counter(
    'circuit_breaker_state_changes_total',
    'Circuit breaker transitions.',
    labelnames=('breaker', 'from', 'to'),
)
RESETS = Counter('circuit_breaker_resets_total', 'Manual resets.', labelnames=('breaker',))


class PrometheusBreakerListener(CoreEventListener):
    def on_state_change(self, *, name: str, old: State, new: State) -> None:
        STATE_CHANGES.labels(breaker=name, **{'from': str(old), 'to': str(new)}).inc()

    def on_call(self, *, name: str, outcome: Outcome, duration: float) -> None:
        CALL_DURATION.labels(breaker=name, outcome=str(outcome)).observe(duration)

    def on_rejected(self, *, name: str) -> None:
        REJECTED.labels(breaker=name).inc()

    def on_reset(self, *, name: str) -> None:
        RESETS.labels(breaker=name).inc()


class BreakerStateCollector(Collector):
    """Current state per breaker at scrape time.

    Transition counters answer "what happened"; during an incident the question
    is "what is open right now", and reconstructing it from deltas takes time
    nobody has. The registry knows; ask it.
    """

    def __init__(self, registry: Registry) -> None:
        self._registry = registry

    def collect(self) -> Iterator[GaugeMetricFamily]:
        gauge = GaugeMetricFamily(
            'circuit_breaker_state',
            'Current circuit breaker state, 1 for the active one.',
            labels=('breaker', 'state'),
        )

        for name, breaker in self._registry.items():
            current = breaker.state
            for state in State:
                gauge.add_metric((name, str(state)), float(state is current))

        yield gauge
```

Register the collector once per process with `prometheus_client.REGISTRY.register(...)`. Labels are the breaker name and the outcome, nothing else: no URL path, user or exception text. Dashboards per breaker: the share of `failure` plus `slow_failure`, the share of `slow_success` plus `slow_failure`, p95 and p99 duration, call volume, and `rejected_total`, which must stay at zero in shadow mode (alert on it).

`DISABLED` stops only the sliding window; listener events keep flowing, so switching a breaker off does not blank the dashboards. That is intended: an empty panel reads as "no traffic".

## 5. Rollout stages that worked

1. Pin the version (`interlock-cb[httpx]~=2.8`).
2. Settings, factory, listener, unit tests (section 6).
3. Wrap the shared client first: one change covers every REST and JSON-RPC client built on it. Prove on a test host that repeated `503` responses are recorded and nothing is rejected, and that request hooks, tracing, per-request timeouts and streaming still work.
4. SDK clients: pass the transport into the SDK constructor, or build the equivalent client by hand when the SDK's factory accepts none, keeping base URL, auth headers, timeouts, limits and TLS settings. Verify close on shutdown and the absence of pool leaks.
5. Observe at least one full business cycle. Treat low-traffic dependencies separately: their window may never reach `minimum_number_of_calls`. Record a baseline per host: volume, failure and slow-call rates, retry amplification, typical degradation windows.
6. A separate, reviewed change enables `CLOSED`, with a canary and a rollback path.

## 6. Tests that caught real mistakes

All run against `httpx.MockTransport`, no network:

- Shadow mode records failures and rejects nothing: `snapshot().failed_calls` grows, no `CircuitOpenTransportError`.
- A custom failure status is recorded; a caller-side exception (`httpx.LocalProtocolError`) propagates and is not counted.
- Two hosts get independent breakers; two transports from the factory share one breaker per host (`first.registry is second.registry`).
- Closing one transport keeps the shared registry usable for the others.
- `limits` and `verify` reach the inner transport (`transport.wrapped`); httpx defaults stay when they are not given.
- Each mode maps to the expected initial state.
- A `PoolTimeout` on a half-open probe does not keep the breaker open.
- A rejection is not retried, while a `ConnectError` still is.
- `/metrics` exposes the series after a success, a retryable status and a transport error.

The first one, as a pattern for the rest:

```python
async def test_shadow_mode_records_failures_without_rejecting() -> None:
    settings = BreakerSettings(minimum_number_of_calls=2, window_size=10)
    factory = BreakerTransportFactory(settings, listener=PrometheusBreakerListener())
    transport = factory.create(httpx.MockTransport(lambda _request: httpx.Response(503)))

    async with httpx.AsyncClient(transport=transport) as client:
        for _ in range(5):
            response = await client.get('https://payments.internal/charge')
            assert response.status_code == 503

    breaker = transport.registry.get_existing('payments')

    assert breaker is not None
    assert breaker.state is State.METRICS_ONLY
    assert breaker.snapshot().failed_calls == 5
```
