"""Keeps the README usable away from GitHub.

The README is also the PyPI long description, and PyPI resolves a relative link
against ``pypi.org`` rather than the repository — every ``docs/...`` link a
reader clicks there is a 404. Absolute urls are the fix; these tests keep them
absolute and keep each one pointing at something that exists, since nothing
else in CI follows a link the README makes up.
"""

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_README = (_ROOT / 'README.md').read_text(encoding='utf-8')

_DOCS_SITE = 'https://bagowix.github.io/interlock/'
_REPO_URLS = re.compile(
    r'https://(?:github\.com/bagowix/interlock/(?:blob|tree)|'
    r'raw\.githubusercontent\.com/bagowix/interlock)/main/(?P<path>[^)\s]+)'
)
_MARKDOWN_LINK = re.compile(r'!?\[[^\]]*\]\((?P<target>[^)\s]+)\)')
_HEADING = re.compile(r'^#{1,6} (?P<text>.+)$', re.MULTILINE)


def _targets() -> tuple[str, ...]:
    return tuple(match['target'] for match in _MARKDOWN_LINK.finditer(_README))


def _slug(heading: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', heading.lower()).strip('-')


def _headings(markdown: str) -> set[str]:
    return {_slug(match['text']) for match in _HEADING.finditer(markdown)}


def _docs_candidates(url: str) -> tuple[Path, ...]:
    """Return the sources a documentation url could have been built from."""
    page = url.removeprefix(_DOCS_SITE).split('#')[0].strip('/')
    if not page:
        return (_ROOT / 'docs' / 'index.md',)
    if '.' in Path(page).name:
        return (_ROOT / 'docs' / page,)

    return (_ROOT / 'docs' / f'{page}.md', _ROOT / 'docs' / page / 'index.md')


def test__readme_links__every_target__is_absolute_or_an_anchor() -> None:
    relative = [target for target in _targets() if not target.startswith(('https://', '#'))]

    assert relative == [], f'relative links 404 on PyPI: {relative}'


def test__readme_links__every_documentation_url__matches_a_docs_page() -> None:
    missing = [
        url
        for url in _targets()
        if url.startswith(_DOCS_SITE)
        and not any(candidate.exists() for candidate in _docs_candidates(url))
    ]

    assert missing == [], f'documentation urls without a page in docs/: {missing}'


def test__readme_links__every_repository_url__matches_a_repo_path() -> None:
    missing = [
        match['path']
        for match in _REPO_URLS.finditer(_README)
        if not (_ROOT / match['path']).exists()
    ]

    assert missing == [], f'repository urls without a file on main: {missing}'


def test__readme_links__every_anchor__matches_a_heading() -> None:
    broken: list[str] = []
    for target in _targets():
        url, _, anchor = target.partition('#')
        if not anchor:
            continue
        pages = (
            [_ROOT / 'README.md']
            if not url
            else [page for page in _docs_candidates(url) if page.exists()]
        )
        if not any(anchor in _headings(page.read_text(encoding='utf-8')) for page in pages):
            broken.append(target)

    assert broken == [], f'anchors without a matching heading: {broken}'
