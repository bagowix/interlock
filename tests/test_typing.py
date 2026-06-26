from interlock import AsyncCallable, Call, SyncCallable


def _sync(x: int) -> int:
    return x


def test__call__protocol__is_satisfied_by_a_plain_callable() -> None:
    assert isinstance(_sync, Call)


def test__call__protocol__is_not_satisfied_by_non_callable() -> None:
    assert not isinstance(object(), Call)


def test__callable_aliases__are_subscriptable() -> None:
    assert SyncCallable[[int], int] is not None
    assert AsyncCallable[[int], int] is not None
