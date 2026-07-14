"""Holds the version information for interlock.

Single source of truth for the package version: bumped manually on release
and read at build time by hatchling (see ``[tool.hatch.version]`` in
``pyproject.toml``).
"""

__all__ = ('VERSION',)

VERSION = '2.1.1'
"""The installed version of interlock.

Guaranteed to comply with PEP 440 version specifiers.
See https://peps.python.org/pep-0440/.
"""
