"""Django bootstrap, and the expectations file that makes this lane a gate.

A test listed in `expectations.txt` is expected to fail, strictly: if it
starts passing and nobody removed its line, this lane goes red. That is the
whole point — a list of known failures that cannot quietly go stale.
"""

import os
import pathlib

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

_EXPECTATIONS = pathlib.Path(__file__).parent / "expectations.txt"


def _expectations():
    """Parse `expectations.txt` into {test id: (category, reason)}."""
    known = {}
    for raw in _EXPECTATIONS.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        test, category, reason = (part.strip() for part in line.split("|", 2))
        if category not in ("out-of-rule", "not-yet"):
            raise ValueError(
                "unknown category %r for %s: expected out-of-rule or not-yet"
                % (category, test)
            )
        known[test] = (category, reason)
    return known


def pytest_collection_modifyitems(config, items):
    known = _expectations()
    seen = set()
    for item in items:
        entry = known.get(item.nodeid)
        if entry is None:
            continue
        seen.add(item.nodeid)
        category, reason = entry
        item.add_marker(
            pytest.mark.xfail(strict=True, reason="%s: %s" % (category, reason))
        )
    missing = sorted(set(known) - seen)
    if missing:
        raise pytest.UsageError(
            "expectations.txt names tests that were not collected: %s"
            % ", ".join(missing)
        )
