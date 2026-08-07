"""The single safe dispatcher for every ``EventListener`` hook.

Observability is optional; the protected call is not. A listener is
user-supplied code running on the breaker's own paths — the protected call, the
transition bookkeeping, the coordinator lane — so a bug in it would otherwise
replace a successful result with a metrics exception, mask the dependency's
error, or kill the background lane of a coordinated breaker. Every hook
therefore goes through ``notify``, which is the only place a listener is ever
invoked.

The policy (documented in ``docs/guides/observability.md``):

- ordinary ``Exception`` from a hook is logged with context and swallowed;
- ``BaseException`` (``KeyboardInterrupt``, ``asyncio.CancelledError``) is not
  caught — cancellation and shutdown keep propagating, as everywhere else;
- a hook the listener does not define is skipped, so listeners written before a
  hook existed keep working unchanged.

This applies to ``EventListener`` hooks only. User-supplied *policy* callbacks —
a ``FailureClassifier``, a fallback function, a tenacity ``before_sleep`` — are
part of the call's behaviour, not observations of it, and keep raising.
"""

import logging
from typing import Final

from interlock.protocols import CoreEventListener, PipelineEventListener, StorageEventListener

__all__ = ('notify',)

_logger: Final = logging.getLogger('interlock')


def notify(
    listener: CoreEventListener | StorageEventListener | PipelineEventListener | None,
    hook: str,
    /,
    *,
    name: str,
    **kwargs: object,
) -> None:
    """Invoke one listener hook, isolating its failure from the caller.

    Args:
        listener: The configured listener, or ``None`` when there is none.
        hook: Name of the hook to invoke, e.g. ``'on_state_change'``.
        name: Breaker or strategy name; passed to the hook and used as log
            context.
        kwargs: The hook's remaining keyword arguments.
    """
    if listener is None:
        return

    method = getattr(listener, hook, None)
    if not callable(method):
        return

    try:
        method(name=name, **kwargs)
    # The one deliberate exception to "no silent exceptions": a blind catch is
    # the whole point here. It is not silent — the traceback is logged.
    except Exception:
        _logger.exception('listener hook %s raised for %r; ignored', hook, name)
