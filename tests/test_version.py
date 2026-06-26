from interlock import VERSION, __version__


def test__version__public_api__exposes_nonempty_string() -> None:
    assert isinstance(VERSION, str)
    assert VERSION


def test__version__dunder__mirrors_version_constant() -> None:
    assert __version__ == VERSION
