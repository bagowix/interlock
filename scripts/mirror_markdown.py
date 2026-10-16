"""Publish every ``llms.txt`` page as Markdown next to its HTML in the built site.

Zensical renders ``guides/timeout.md`` only as ``guides/timeout/index.html``, so
the ``.md`` links in ``llms.txt`` 404 on GitHub Pages unless the sources are
copied over. Run after ``zensical build``::

    uv run python scripts/mirror_markdown.py

Each page is written with its snippet includes expanded, the same way
``llms-full.txt`` inlines it. The script then fails if any relative link in
``site/llms.txt`` still has no file behind it.
"""

import re
import sys

from build_llms_full import _DOCS, _PAGES, _resolve_snippets

_SITE = _DOCS.parent / 'site'
_LINK_TARGET = re.compile(r'\]\(([^)#\s]+)')


def mirror() -> None:
    """Write each documentation page into ``site/`` at its source path."""
    for page in _PAGES:
        body = (_DOCS / page).read_text(encoding='utf-8')
        (_SITE / page).write_text(_resolve_snippets(body), encoding='utf-8')


def broken_links() -> list[str]:
    """Return the relative links in ``site/llms.txt`` that resolve to no file."""
    index = (_SITE / 'llms.txt').read_text(encoding='utf-8')
    return [
        link
        for link in _LINK_TARGET.findall(index)
        if '://' not in link and not (_SITE / link).is_file()
    ]


def main() -> None:
    """Mirror the pages, then fail on any ``llms.txt`` link left dangling."""
    mirror()
    if broken := broken_links():
        sys.exit(f'llms.txt links with no file in site/: {broken}')


if __name__ == '__main__':
    main()
