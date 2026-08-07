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
from interlock.pipeline import (
    BulkheadStrategy,
    CircuitBreakerStrategy,
    FallbackStrategy,
    Pipeline,
    PipelineBuilder,
    Strategy,
    TimeoutStrategy,
)
from interlock.protocols import (
    AsyncStorage,
    Clock,
    CoreEventListener,
    EventListener,
    FailureClassifier,
    PipelineEventListener,
    SlidingWindow,
    Storage,
    StorageEventListener,
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
    'BulkheadStrategy',
    'Call',
    'CallTimeoutError',
    'CircuitBreaker',
    'CircuitBreakerStrategy',
    'CircuitOpenError',
    'Clock',
    'Config',
    'CoreEventListener',
    'EventListener',
    'FailureClassifier',
    'FallbackStrategy',
    'InterlockDeprecationWarning',
    'InterlockError',
    'LoggingEventListener',
    'Outcome',
    'Pipeline',
    'PipelineBuilder',
    'PipelineEventListener',
    'ProbeLease',
    'Registry',
    'SharedState',
    'SlidingWindow',
    'State',
    'Storage',
    'StorageEventListener',
    'Strategy',
    'SyncCallable',
    'TimeoutStrategy',
    'WindowSnapshot',
    'WindowType',
    '__version__',
    'sync_timeout',
    'timeout',
)
