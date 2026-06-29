import dataclasses

import pytest

from interlock import ProbeLease, SharedState, State


def test__shared_state__closed__is_a_clean_baseline() -> None:
    state = SharedState.closed()

    assert state.state is State.CLOSED
    assert state.opened_at == 0.0
    assert state.version == 0
    assert state.probes_permitted == 0
    assert state.probes_remaining == 0
    assert state.probes_completed == 0
    assert state.probe_failures == 0
    assert state.probe_slows == 0


def test__shared_state__is_frozen() -> None:
    state = SharedState.closed()

    with pytest.raises(dataclasses.FrozenInstanceError):
        state.version = 1  # type: ignore[misc]


def test__shared_state__equality_is_by_value() -> None:
    assert SharedState.closed() == SharedState.closed()
    assert SharedState.closed() != dataclasses.replace(SharedState.closed(), version=1)


def test__probe_lease__carries_grant_and_state() -> None:
    state = SharedState.closed()
    lease = ProbeLease(granted=True, state=state)

    assert lease.granted is True
    assert lease.state is state


def test__probe_lease__is_frozen() -> None:
    lease = ProbeLease(granted=False, state=SharedState.closed())

    with pytest.raises(dataclasses.FrozenInstanceError):
        lease.granted = True  # type: ignore[misc]
