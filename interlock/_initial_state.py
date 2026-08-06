"""Validation for states that can exist without transition history."""

from interlock.state import State

_SUPPORTED_INITIAL_STATES = (
    State.CLOSED,
    State.FORCED_OPEN,
    State.DISABLED,
    State.METRICS_ONLY,
)


def validate_initial_state(initial_state: object) -> None:
    """Reject states that require transition history before they are valid."""
    if not isinstance(initial_state, State) or initial_state not in _SUPPORTED_INITIAL_STATES:
        supported = ', '.join(state.value for state in _SUPPORTED_INITIAL_STATES)
        raise ValueError(f'initial_state must be one of {{{supported}}}, got {initial_state!r}')
