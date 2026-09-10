"""The lane's harness for the current pair: the suite's fixtures, re-provided
over the lane's settings, and the expectations file that makes the lane a gate.

The upstream conftest parametrises every test over twelve settings modules —
sentinel, unix socket, sharding, compressors — and imports pytest-xdist to
schedule them, one module per worker process. That is what makes its own
`cache` fixture work: it re-points `DJANGO_SETTINGS_MODULE` and calls
`django.setup()`, which is idempotent, so the switch takes effect only in a
process that has not been set up yet. A cache behind one server on a port
exercises none of the twelve, so this file provides the same fixture names
(`settings`, `cache`, and the `cache_settings` parameter) over the three
modules under `settings/` — and switches between them with
`override_settings(CACHES=...)` in one process, since a second `setup()` in
the same process would silently keep the first module's serializer.

`caches["default"]` is dropped and rebuilt per test, which is also what the
suite's own `patch_itersize_setting` fixture does to pick up an overridden
setting; the autouse fixture below makes sure the entry that fixture deletes
is there to delete.

A test listed in `expectations.txt` is expected to fail, strictly: if it
starts passing and nobody removed its line, this lane goes red.
"""

import importlib
import os
import pathlib
import re
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).absolute().parent))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings.lane")

import django  # noqa: E402

django.setup()

_EXPECTATIONS = pathlib.Path(__file__).parent / "expectations.txt"
_SETTINGS = ["lane", "lane_msgpack", "lane_json"]


def _caches_for(name):
    """The `CACHES` of one of the lane's settings modules."""
    return importlib.import_module("settings.%s" % name).CACHES


def _drop_default():
    """Forget the built `default` cache, so the next access rebuilds it."""
    from django.core.cache import caches

    try:
        del caches["default"]
    except AttributeError:
        pass


class _SettingsWrapper:
    """The suite's SettingsWrapper: overrides that are undone at teardown."""

    def __init__(self):
        object.__setattr__(self, "_to_restore", [])

    def __setattr__(self, attr, value):
        from django.test import override_settings

        override = override_settings(**{attr: value})
        override.enable()
        self._to_restore.append(override)

    def __delattr__(self, attr):
        from django.conf import settings
        from django.test import override_settings

        override = override_settings()
        override.enable()
        delattr(settings, attr)
        self._to_restore.append(override)

    def __getattr__(self, attr):
        from django.conf import settings

        return getattr(settings, attr)

    def finalize(self):
        for override in reversed(self._to_restore):
            override.disable()
        del self._to_restore[:]


@pytest.fixture()
def settings():
    wrapper = _SettingsWrapper()
    yield wrapper
    wrapper.finalize()


@pytest.fixture(autouse=True)
def _lane_settings(request):
    """Point `CACHES` at the parameter's settings module for the whole test.

    Autouse, so it is in place before any fixture the test asks for — the
    suite's `patch_itersize_setting` deletes `caches["default"]` at setup and
    at teardown, and both need the entry to exist.
    """
    if "cache_settings" not in request.fixturenames:
        yield
        return

    from django.core.cache import caches
    from django.test import override_settings

    override = override_settings(CACHES=_caches_for(request.getfixturevalue("cache_settings")))
    override.enable()
    _drop_default()
    caches["default"]
    try:
        yield
    finally:
        _drop_default()
        override.disable()


@pytest.fixture()
def cache(cache_settings):
    """A cache built after every override the test has enabled is in place.

    Its client is built here rather than on first use. Upstream hands every
    test one process-wide cache that earlier tests have already connected, and
    two of its tests read `_client` directly — `close()` is a no-op on a cache
    that never built one — so a cache handed over cold fails them for a reason
    that has nothing to do with the server.
    """
    from django.core.cache import caches

    _drop_default()
    built = caches["default"]
    built.client  # noqa: B018 — build the client, as upstream's shared cache has
    yield built
    built.clear()


def pytest_generate_tests(metafunc):
    if "cache" in metafunc.fixturenames:
        metafunc.parametrize("cache_settings", _SETTINGS)


def _pattern(text):
    """A row's test field, as a regex.

    `*` is the only metacharacter and everything else is literal — notably
    `[` and `]`, which every parametrised node id carries and which `fnmatch`
    would read as a character class. So `…::test_pexpire[*]` matches that test
    under all three settings and does not match `test_pexpire_at`.
    """
    return re.compile("".join(".*" if ch == "*" else re.escape(ch) for ch in text))


def _expectations():
    """Parse `expectations.txt` into [(regex, source, category, reason)]."""
    rows = []
    for raw in _EXPECTATIONS.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        test, category, reason = (part.strip() for part in line.split("|", 2))
        if category not in ("out-of-rule", "not-yet", "not-this-server"):
            raise ValueError("unknown category %r for %s" % (category, test))
        rows.append((_pattern(test), test, category, reason))
    return rows


def pytest_collection_modifyitems(config, items):
    rows = _expectations()
    matched = {source: [] for _, source, _, _ in rows}
    owner = {}
    for item in items:
        for regex, source, category, reason in rows:
            if not regex.fullmatch(item.nodeid):
                continue
            if item.nodeid in owner:
                raise pytest.UsageError(
                    "expectations.txt: %s is claimed by two rows, %r and %r"
                    % (item.nodeid, owner[item.nodeid], source)
                )
            owner[item.nodeid] = source
            matched[source].append(item.nodeid)
            item.add_marker(
                pytest.mark.xfail(strict=True, reason="%s: %s" % (category, reason))
            )
    stale = sorted(source for source, hits in matched.items() if not hits)
    if stale:
        raise pytest.UsageError(
            "expectations.txt has rows that matched no collected test: %s"
            % ", ".join(stale)
        )
