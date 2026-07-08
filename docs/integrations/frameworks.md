# Flask / Django — recipe

When a route's outgoing dependency trips its breaker, the raised
[`CircuitOpenError`](../reference.md) should become a clean
`503 Service Unavailable` with a `Retry-After` header — the same behaviour
the [FastAPI extra](fastapi.md) ships as code. For other frameworks the
handler is a few lines; no extra needed.

## Flask

```python
import math

from flask import Flask, jsonify

from interlock import CircuitOpenError

app = Flask(__name__)


@app.errorhandler(CircuitOpenError)
def on_circuit_open(exc: CircuitOpenError):
    response = jsonify({'detail': str(exc)})
    response.status_code = 503

    if exc.retry_after is not None:
        response.headers['Retry-After'] = str(math.ceil(exc.retry_after))

    return response
```

## Django

```python
# middleware.py
import json
import math

from django.http import HttpResponse

from interlock import CircuitOpenError


class CircuitOpenMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        return self.get_response(request)

    def process_exception(self, request, exception):
        if not isinstance(exception, CircuitOpenError):
            return None

        response = HttpResponse(
            json.dumps({'detail': str(exception)}),
            status=503,
            content_type='application/json',
        )

        if exception.retry_after is not None:
            response['Retry-After'] = str(math.ceil(exception.retry_after))

        return response
```

Add it to `MIDDLEWARE` in `settings.py`.

## Where the breakers live

The handler only translates the rejection. The breakers themselves guard your
*outgoing* calls — share one `Registry` across the app and wrap the
dependencies:

```python
from interlock import Registry

registry = Registry()
payments = registry.get('payments-api')


def charge(amount: int) -> str:
    return payments.call(gateway.charge, amount)
```

`Retry-After` is rounded up to whole seconds (per RFC 7231) and omitted when
the breaker cannot estimate the next probe (for example after
`force_open()`).
