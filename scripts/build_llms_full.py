"""Generate ``docs/llms-full.txt`` by inlining every documentation page.

``llms.txt`` is the short index (links only); ``llms-full.txt`` is the same
content expanded inline so an agent can ingest the whole manual in one request.
Regenerate after editing any page::

    uv run python scripts/build_llms_full.py

The page order mirrors the ``## Docs`` section of ``docs/llms.txt`` so the two
files never disagree on structure.
"""

import re
from pathlib import Path

_DOCS = Path(__file__).resolve().parent.parent / 'docs'

# Same order as the index in docs/llms.txt.
_PAGES = (
    'getting-started.md',
    'demo.md',
    'guides/configuration.md',
    'guides/states.md',
    'guides/failure-classification.md',
    'guides/observability.md',
    'guides/timeout.md',
    'guides/retries.md',
    'integrations/index.md',
    'integrations/httpx2.md',
    'integrations/aiohttp.md',
    'integrations/requests.md',
    'integrations/tenacity.md',
    'integrations/fastapi.md',
    'integrations/redis.md',
    'integrations/llm.md',
    'integrations/frameworks.md',
    'comparison.md',
    'reference.md',
)

_HEADER = """# interlock — full documentation

> A modern circuit breaker for Python: sync and async in a single class,
> sliding-window failure-rate and slow-call detection, a type-safe decorator
> API, and transparent per-host integrations for httpx2, aiohttp and requests.
> Zero-dependency core (standard library only); integrations ship as optional
> extras.

This file inlines every documentation page in reading order. It is generated
from the Markdown sources by ``scripts/build_llms_full.py`` — edit the pages in
``docs/``, not this file.
"""


_SNIPPET = re.compile(r'^(?P<indent>[ \t]*)--8<-- "(?P<path>[^"]+)"$', re.MULTILINE)
_ROOT = _DOCS.parent


def _resolve_snippets(body: str) -> str:
    """Expand ``--8<-- "path"`` include directives the way pymdownx.snippets does."""

    def _include(match: re.Match[str]) -> str:
        indent = match.group('indent')
        content = (_ROOT / match.group('path')).read_text(encoding='utf-8').rstrip()
        return '\n'.join(f'{indent}{line}' if line else '' for line in content.splitlines())

    return _SNIPPET.sub(_include, body)


def build() -> str:
    """Return the full inlined documentation as a single string."""
    parts = [_HEADER]
    for page in _PAGES:
        body = (_DOCS / page).read_text(encoding='utf-8').strip()
        parts.append(f'\n\n---\n\n<!-- source: docs/{page} -->\n\n{_resolve_snippets(body)}')
    return ''.join(parts) + '\n'


def main() -> None:
    """Write the assembled documentation to ``docs/llms-full.txt``."""
    (_DOCS / 'llms-full.txt').write_text(build(), encoding='utf-8')


if __name__ == '__main__':
    main()
