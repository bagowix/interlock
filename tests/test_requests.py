"""Tests for the requests integration (``interlock.integrations.requests``)."""

from collections.abc import Callable

import pytest
import requests
from pytest_mock import MockerFixture
from requests import PreparedRequest, Response
from requests.adapters import HTTPAdapter
from tests.conftest import FakeClock

from interlock import CircuitOpenError, Config
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


def test__adapter__client_errors__do_not_trip(mocker: MockerFixture, fake_clock: FakeClock) -> None:
    transport = _patch_transport(mocker, [_response(404)] * 5)
    adapter = CircuitBreakerAdapter(config=_TRIP_FAST, clock=fake_clock)

    for _ in range(5):
        assert adapter.send(_prepared('https://api.a/x')).status_code == 404

    assert transport.call_count == 5


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
