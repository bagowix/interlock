"""The default production clock.

Breakers read time only through a ``Clock``, so tests can inject a fake one.
In production the default is this thin wrapper over ``time.monotonic`` — a
monotonic source unaffected by wall-clock adjustments.
"""

import time

__all__ = ('SystemClock',)


class SystemClock:
    """A ``Clock`` backed by ``time.monotonic``."""

    def monotonic(self) -> float:
        """Return ``time.monotonic()`` in fractional seconds."""
        return time.monotonic()
