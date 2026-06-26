import functools

from interlock._detect import is_async_callable


def sync_fn(x: int) -> int:
    return x


async def async_fn(x: int) -> int:
    return x


class SyncCallable:
    def __call__(self) -> int:
        return 1


class AsyncCallable:
    async def __call__(self) -> int:
        return 1


class WithMethods:
    def sync_method(self) -> int:
        return 1

    async def async_method(self) -> int:
        return 1


def test__plain_sync_function__is_not_async() -> None:
    assert is_async_callable(sync_fn) is False


def test__coroutine_function__is_async() -> None:
    assert is_async_callable(async_fn) is True


def test__lambda__is_not_async() -> None:
    assert is_async_callable(lambda: 1) is False


def test__partial_of_sync__is_not_async() -> None:
    assert is_async_callable(functools.partial(sync_fn, 1)) is False


def test__partial_of_async__is_async() -> None:
    assert is_async_callable(functools.partial(async_fn, 1)) is True


def test__nested_partial_of_async__is_async() -> None:
    once = functools.partial(async_fn, 1)
    assert is_async_callable(functools.partial(once)) is True


def test__object_with_sync_dunder_call__is_not_async() -> None:
    assert is_async_callable(SyncCallable()) is False


def test__object_with_async_dunder_call__is_async() -> None:
    assert is_async_callable(AsyncCallable()) is True


def test__partial_of_async_callable_object__is_async() -> None:
    assert is_async_callable(functools.partial(AsyncCallable())) is True


def test__bound_sync_method__is_not_async() -> None:
    assert is_async_callable(WithMethods().sync_method) is False


def test__bound_async_method__is_async() -> None:
    assert is_async_callable(WithMethods().async_method) is True
