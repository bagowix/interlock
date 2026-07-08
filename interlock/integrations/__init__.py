"""Optional integrations — each module requires its matching extra.

Nothing here is imported by ``interlock`` itself: the core stays
zero-dependency. Import the module you need explicitly::

    from interlock.integrations.httpx2 import CircuitBreakerTransport
"""
