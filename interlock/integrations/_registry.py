"""Registry construction and ownership shared by transport integrations."""

from collections.abc import Callable
from functools import wraps
from typing import ParamSpec

from interlock.config import Config
from interlock.protocols import (
    Clock,
    CoreEventListener,
    FailureClassifier,
    StorageEventListener,
)
from interlock.registry import Registry
from interlock.state import State

_P = ParamSpec('_P')
_REGISTRY_OPTIONS = ('config', 'clock', 'initial_state', 'classifier', 'listener')


def reject_registry_options(init: Callable[_P, None]) -> Callable[_P, None]:
    """Reject constructor options explicitly combined with an injected registry."""

    @wraps(init)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> None:
        if kwargs.get('registry') is not None:
            conflicts = [name for name in _REGISTRY_OPTIONS if name in kwargs]
            if conflicts:
                joined = ', '.join(conflicts)
                raise ValueError(
                    f'The injected registry already owns these options: {joined}. '
                    'Configure them on the registry instead.'
                )
        init(*args, **kwargs)

    return wrapped


def resolve_registry(  # noqa: PLR0913 - mirrors the integration constructors
    *,
    registry: Registry | None,
    config: Config | None,
    clock: Clock | None,
    initial_state: State,
    classifier: FailureClassifier | None,
    listener: CoreEventListener | StorageEventListener | None,
    default_classifier: FailureClassifier,
) -> tuple[Registry, bool]:
    """Return the effective registry and whether the integration owns it."""
    if registry is not None:
        return registry, False

    return (
        Registry(
            config=config,
            clock=clock,
            initial_state=initial_state,
            classifier=classifier if classifier is not None else default_classifier,
            listener=listener,
        ),
        True,
    )
