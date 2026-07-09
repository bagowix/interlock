"""Smoke tests keeping the runnable examples green.

The examples are the one place where real wall-clock time is allowed: they are
user-facing scripts (docs/demo.md walks through their output), so each runs in
a subprocess exactly as a user would run it (~1.2s of real sleep each).
"""

import subprocess
import sys
from pathlib import Path

import pytest

_EXAMPLES = Path(__file__).resolve().parent.parent / 'examples'


def _run(script: str) -> str:
    result = subprocess.run(  # noqa: S603 - fixed argv: our own interpreter + example path
        [sys.executable, str(_EXAMPLES / script)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ''
    return result.stdout


@pytest.mark.parametrize(
    ('script', 'markers'),
    [
        (
            'lifecycle.py',
            [
                'state CLOSED -> OPEN',
                'call rejected — circuit is open',
                'REJECTED in ~0ms',
                'state OPEN -> HALF_OPEN',
                'state HALF_OPEN -> CLOSED',
                'final state: CLOSED',
            ],
        ),
        (
            'two_clients.py',
            [
                '[listener] recommendations: state CLOSED -> OPEN',
                'rejected instantly -> fallback: cached picks',
                'charged erin $25',  # payments keeps serving during the outage
                '[listener] recommendations: state HALF_OPEN -> CLOSED',
                'final state of payments: CLOSED',
                'final state of recommendations: CLOSED',
            ],
        ),
    ],
)
def test__example__runs__prints_the_documented_story(script: str, markers: list[str]) -> None:
    stdout = _run(script)
    for marker in markers:
        assert marker in stdout, f'{script}: missing {marker!r} in output:\n{stdout}'


def test__two_clients__payments_breaker__never_transitions() -> None:
    stdout = _run('two_clients.py')
    assert '[listener] payments: state' not in stdout
