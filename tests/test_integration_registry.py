import pytest

from interlock import Config, Registry, State
from interlock.integrations._registry import reject_registry_options, resolve_registry


class _Classifier:
    def is_failure(self, *, result: object, exception: BaseException | None) -> bool:
        return exception is not None


def test__reject_registry_options__compatible_call__delegates() -> None:
    delegated = False

    def init(*, registry: Registry | None = None) -> None:
        nonlocal delegated
        assert registry is not None
        delegated = True

    wrapped = reject_registry_options(init)

    wrapped(registry=Registry())

    assert delegated


def test__reject_registry_options__explicit_builder_option__raises_conflict() -> None:
    def init(*, registry: Registry | None = None, config: Config | None = None) -> None:
        raise AssertionError(f'conflicting call reached the constructor: {registry!r}, {config!r}')

    wrapped = reject_registry_options(init)

    with pytest.raises(ValueError, match='config'):
        wrapped(registry=Registry(), config=None)


def test__resolve_registry__injected_registry__returns_non_owner() -> None:
    registry = Registry()

    effective, owns_registry = resolve_registry(
        registry=registry,
        config=None,
        clock=None,
        initial_state=State.CLOSED,
        classifier=None,
        listener=None,
        default_classifier=_Classifier(),
    )

    assert effective is registry
    assert not owns_registry
