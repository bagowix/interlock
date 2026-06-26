"""interlock — a modern circuit breaker for Python."""

from interlock._typing import AsyncCallable, Call, SyncCallable
from interlock.breaker import CircuitBreaker
from interlock.config import Config
from interlock.errors import (
    CallTimeoutError,
    CircuitOpenError,
    InterlockDeprecationWarning,
    InterlockError,
)
from interlock.listeners import LoggingEventListener
from interlock.outcome import Outcome
from interlock.protocols import (
    Clock,
    EventListener,
    FailureClassifier,
    SlidingWindow,
    Storage,
)
from interlock.registry import Registry
from interlock.state import State
from interlock.timeout import timeout
from interlock.version import VERSION
from interlock.window import WindowSnapshot, WindowType

__version__ = VERSION

__all__ = (
    'VERSION',
    'AsyncCallable',
    'Call',
    'CallTimeoutError',
    'CircuitBreaker',
    'CircuitOpenError',
    'Clock',
    'Config',
    'EventListener',
    'FailureClassifier',
    'InterlockDeprecationWarning',
    'InterlockError',
    'LoggingEventListener',
    'Outcome',
    'Registry',
    'SlidingWindow',
    'State',
    'Storage',
    'SyncCallable',
    'WindowSnapshot',
    'WindowType',
    '__version__',
    'timeout',
)
