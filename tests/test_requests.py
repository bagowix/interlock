"""Tests for the requests integration (``interlock.integrations.requests``)."""

from collections.abc import Callable
from typing import cast

import pytest
import requests
from pytest_mock import MockerFixture
from requests import PreparedRequest, Response
from requests.adapters import HTTPAdapter
from tests.conftest import FakeClock

from interlock import CircuitOpenError, Config, Registry, State
from interlock.integrations.requests import CircuitBreakerAdapter, HttpStatusClassifier

_TRIP_FAST = Config(minimum_number_of_calls=2, failure_rate_threshold=0.5)


def _response(status_code: int) -> Response:
    response = Response()
    response.status_code = status_code
    return response


def _prepared(url: str) -> PreparedRequest:
    return requests.Request(method='GET', url=url).prepare()


def _patch_transport(
    mocker: MockerFixture,
    side_effect: Callable[..., Response] | list[Response | BaseException],
) -> MockerFixture:
    return mocker.patch.object(HTTPAdapter, 'send', autospec=True, side_effect=side_effect)


# --- HttpStatusClassifier -----------------------------------------------------


@pytest.mark.parametrize('status_code', [429, 500, 502, 503, 504])
def test__http_status_classifier__retryable_status__is_failure(status_code: int) -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=_response(status_code), exception=None) is True


@pytest.mark.parametrize('status_code', [200, 201, 301, 400, 404])
def test__http_status_classifier__healthy_or_client_error__is_success(status_code: int) -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=_response(status_code), exception=None) is False


def test__http_status_classifier__exception__is_failure() -> None:
    classifier = HttpStatusClassifier()

    exc = requests.ConnectionError('boom')
    assert classifier.is_failure(result=None, exception=exc) is True


@pytest.mark.parametrize(
    'exception',
    [
        requests.exceptions.InvalidURL('no host'),
        requests.exceptions.InvalidProxyURL('proxy has no host'),
    ],
)
def test__http_status_classifier__caller_side_exception__is_success(
    exception: requests.RequestException,
) -> None:
    classifier = HttpStatusClassifier()

    assert classifier.is_failure(result=None, exception=exception) is False


def test__http_status_classifier__custom_exclusions__override_default_set() -> None:
    classifier = HttpStatusClassifier(excluded_exceptions=(requests.exceptions.InvalidHeader,))

    excluded = requests.exceptions.InvalidHeader('bad Retry-After')
    assert classifier.is_failure(result=None, exception=excluded) is False
    assert classifier.is_failure(result=None, exception=requests.exceptions.InvalidURL()) is True


def test__http_status_classifier__empty_exclusions__counts_every_exception() -> None:
    classifier = HttpStatusClassifier(excluded_exceptions=())

    assert classifier.is_failure(result=None, exception=requests.exceptions.InvalidURL()) is True


# --- CircuitBreakerAdapter ----------------------------------------------------


def test__adapter__failure_statuses__trip_breaker_and_reject(
    mocker: MockerFixture, fake_clock: FakeClock
) -> None:
    transport = _patch_transport(mocker, [_response(503), _response(503)])
    adapter = CircuitBreakerAdapter(config=_TRIP_FAST, clock=fake_clock)

    adapter.send(_prepared('https://api.a/x'))
    adapter.send(_prepared('https://api.a/x'))

    with pytest.raises(CircuitOpenError):
        adapter.send(_prepared('https://api.a/x'))
    assert transport.call_count == 2


def test__adapter__open_host__other_host_unaffected(
    mocker: MockerFixture, fake_clock: FakeClock
) -> None:
    transport = _patch_transport(mocker, [_response(503), _response(503), _response(200)])
    adapter = CircuitBreakerAdapter(config=_TRIP_FAST, clock=fake_clock)

    adapter.send(_prepared('https://api.a/x'))
    adapter.send(_prepared('https://api.a/x'))
    with pytest.raises(CircuitOpenError):
        adapter.send(_prepared('https://api.a/x'))

    response = adapter.send(_prepared('https://api.b/x'))

    assert response.status_code == 200
    assert transport.call_count == 3


def test__adapter__name_resolver__splits_host_by_path(
    mocker: MockerFixture,
    fake_clock: FakeClock,
) -> None:
    transport = _patch_transport(mocker, [_response(503), _response(503), _response(200)])
    adapter = CircuitBreakerAdapter(
        config=_TRIP_FAST,
        clock=fake_clock,
        name_resolver=lambda request: request.path_url.split('/')[1],
    )

    adapter.send(_prepared('https://gateway.example.com/orders/1'))
    adapter.send(_prepared('https://gateway.example.com/orders/2'))

    with pytest.raises(CircuitOpenError):
        adapter.send(_prepared('https://gateway.example.com/orders/3'))
    response = adapter.send(_prepared('https://gateway.example.com/payments/1'))
    orders = adapter.registry.get_existing('orders')
    payments = adapter.registry.get_existing('payments')
    assert response.status_code == 200
    assert orders is not None
    assert payments is not None
    assert orders is not payments
    assert transport.call_count == 3


def test__adapter__empty_resolved_name__raises_before_transport(
    mocker: MockerFixture,
    fake_clock: FakeClock,
) -> None:
    transport = _patch_transport(mocker, [_response(200)])
    adapter = CircuitBreakerAdapter(
        clock=fake_clock,
        name_resolver=lambda _request: '',
    )
    request = _prepared('https://api.example.com/v1')

    with pytest.raises(ValueError, match='empty breaker name') as raised:
        adapter.send(request)

    assert request.url in str(raised.value)
    assert transport.call_count == 0


@pytest.mark.parametrize('resolved_name', [None, b'orders'])
def test__adapter__non_string_resolved_name__raises_before_transport(
    mocker: MockerFixture,
    fake_clock: FakeClock,
    resolved_name: object,
) -> None:
    transport = _patch_transport(mocker, [_response(200)])
    adapter = CircuitBreakerAdapter(
        clock=fake_clock,
        name_resolver=lambda _request: cast('str', resolved_name),
    )
    request = _prepared('https://api.example.com/v1')

    with pytest.raises(ValueError, match='non-string breaker name') as raised:
        adapter.send(request)

    assert request.url in str(raised.value)
    assert transport.call_count == 0


def test__adapter__client_errors__do_not_trip(mocker: MockerFixture, fake_clock: FakeClock) -> None:
    transport = _patch_transport(mocker, [_response(404)] * 5)
    adapter = CircuitBreakerAdapter(config=_TRIP_FAST, clock=fake_clock)

    for _ in range(5):
        assert adapter.send(_prepared('https://api.a/x')).status_code == 404

    assert transport.call_count == 5


def test__adapter__metrics_only_failures__record_without_rejecting(
    mocker: MockerFixture,
    fake_clock: FakeClock,
) -> None:
    transport = _patch_transport(mocker, [_response(503)] * 5)
    adapter = CircuitBreakerAdapter(
        initial_state=State.METRICS_ONLY,
        config=_TRIP_FAST,
        clock=fake_clock,
    )

    for _ in range(5):
        assert adapter.send(_prepared('https://api.a/x')).status_code == 503

    breaker = adapter.registry.get_existing('api.a')
    assert breaker is not None
    assert breaker.state is State.METRICS_ONLY
    assert breaker.snapshot().failed_calls == 5
    assert transport.call_count == 5


def test__adapter__close__releases_pools_and_registry(mocker: MockerFixture) -> None:
    parent_close = mocker.patch.object(HTTPAdapter, 'close', autospec=True)
    adapter = CircuitBreakerAdapter()
    close_all = mocker.spy(adapter.registry, 'close_all')

    adapter.close()

    parent_close.assert_called_once_with(adapter)
    close_all.assert_called_once_with()


def test__adapters__shared_registry__merge_window(
    mocker: MockerFixture,
    fake_clock: FakeClock,
) -> None:
    transport = _patch_transport(mocker, [_response(503), _response(503)])
    registry = Registry(
        config=_TRIP_FAST,
        clock=fake_clock,
        classifier=HttpStatusClassifier(),
    )
    first = CircuitBreakerAdapter(registry=registry)
    second = CircuitBreakerAdapter(registry=registry)

    first.send(_prepared('https://api.a/x'))
    second.send(_prepared('https://api.a/x'))
    breaker = registry.get_existing('api.a')

    assert first.registry is registry
    assert second.registry is registry
    assert breaker is not None
    assert breaker.snapshot().failed_calls == 2
    with pytest.raises(CircuitOpenError):
        second.send(_prepared('https://api.a/x'))
    assert transport.call_count == 2


def test__adapter__injected_registry__closes_only_connection_pools(
    mocker: MockerFixture,
) -> None:
    parent_close = mocker.patch.object(HTTPAdapter, 'close', autospec=True)
    registry = Registry()
    adapter = CircuitBreakerAdapter(registry=registry)
    close_all = mocker.spy(registry, 'close_all')

    adapter.close()

    close_all.assert_not_called()
    parent_close.assert_called_once_with(adapter)


def test__adapter__registry_with_initial_state__raises_conflict() -> None:
    with pytest.raises(ValueError, match='initial_state'):
        CircuitBreakerAdapter(registry=Registry(), initial_state=State.CLOSED)


def test__adapter__transport_exception__counts_as_failure(
    mocker: MockerFixture, fake_clock: FakeClock
) -> None:
    boom = requests.ConnectionError('down')
    transport = _patch_transport(mocker, [boom, boom])
    adapter = CircuitBreakerAdapter(config=_TRIP_FAST, clock=fake_clock)

    for _ in range(2):
        with pytest.raises(requests.ConnectionError):
            adapter.send(_prepared('https://api.a/x'))

    with pytest.raises(CircuitOpenError):
        adapter.send(_prepared('https://api.a/x'))
    assert transport.call_count == 2


def test__adapter__mounted_on_session__guards_session_requests(
    mocker: MockerFixture, fake_clock: FakeClock
) -> None:
    _patch_transport(mocker, [_response(503), _response(503)])
    session = requests.Session()
    session.mount('https://', CircuitBreakerAdapter(config=_TRIP_FAST, clock=fake_clock))

    session.get('https://api.a/x')
    session.get('https://api.a/x')

    with pytest.raises(CircuitOpenError):
        session.get('https://api.a/x')


def test__adapter__url_without_host__raises_value_error(fake_clock: FakeClock) -> None:
    adapter = CircuitBreakerAdapter(config=_TRIP_FAST, clock=fake_clock)
    request = PreparedRequest()
    request.url = '/relative/path'

    with pytest.raises(ValueError, match='no host'):
        adapter.send(request)


def test__adapter__missing_url__raises_before_url_parsing(
    mocker: MockerFixture,
    fake_clock: FakeClock,
) -> None:
    adapter = CircuitBreakerAdapter(config=_TRIP_FAST, clock=fake_clock)
    request = PreparedRequest()
    request.url = None
    urlsplit = mocker.patch(
        'interlock.integrations.requests.urlsplit',
        side_effect=AssertionError('missing URL must not reach urlsplit'),
    )

    with pytest.raises(ValueError, match='no host'):
        adapter.send(request)

    urlsplit.assert_not_called()


def test__adapter__send_kwargs__forwarded_to_transport(
    mocker: MockerFixture, fake_clock: FakeClock
) -> None:
    transport = _patch_transport(mocker, [_response(200)])
    adapter = CircuitBreakerAdapter(config=_TRIP_FAST, clock=fake_clock)

    adapter.send(_prepared('https://api.a/x'), timeout=3.0, verify=False)

    kwargs = transport.call_args.kwargs
    assert kwargs['timeout'] == 3.0
    assert kwargs['verify'] is False


def test__http_status_classifier__custom_statuses__override_default_set() -> None:
    classifier = HttpStatusClassifier(failure_statuses={404, 408})

    assert classifier.is_failure(result=_response(404), exception=None) is True
    assert classifier.is_failure(result=_response(408), exception=None) is True
    assert classifier.is_failure(result=_response(500), exception=None) is False
