"""interlock — a modern circuit breaker for Python."""

from interlock._typing import AsyncCallable, Call, SyncCallable
from interlock.breaker import CircuitBreaker
from interlock.config import Config
from interlock.errors import (
    BulkheadFullError,
    CallTimeoutError,
    CircuitOpenError,
    InterlockDeprecationWarning,
    InterlockError,
)
from interlock.listeners import LoggingEventListener
from interlock.outcome import Outcome
from interlock.protocols import (
    AsyncStorage,
    Clock,
    EventListener,
    FailureClassifier,
    SlidingWindow,
    Storage,
)
from interlock.registry import Registry
from interlock.shared import ProbeLease, SharedState
from interlock.state import State
from interlock.timeout import sync_timeout, timeout
from interlock.version import VERSION
from interlock.window import WindowSnapshot, WindowType

__version__ = VERSION

__all__ = (
    'VERSION',
    'AsyncCallable',
    'AsyncStorage',
    'BulkheadFullError',
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
    'ProbeLease',
    'Registry',
    'SharedState',
    'SlidingWindow',
    'State',
    'Storage',
    'SyncCallable',
    'WindowSnapshot',
    'WindowType',
    '__version__',
    'sync_timeout',
    'timeout',
)
