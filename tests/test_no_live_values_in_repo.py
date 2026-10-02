"""No value copied from the live database may sit in committed source or tests.

The repo is public. A real corp wallet balance was once pasted into test
fixtures as a "realistic" string. Only its SHA-256 is kept here, so this guard
does not itself republish the value. Synthetic stand-ins (e.g.
"123456789.1234") keep the string shape balances arrive in.
"""
from __future__ import annotations

import hashlib
import os
import re

_ROOT = os.path.join(os.path.dirname(__file__), "..")
_SCANNED = ("src", "tests")

# sha256 of each live value that must never reappear.
_FORBIDDEN_SHA256 = {
    "232a958049f2b8da5253648c2749ee7b815477ec417fbf05fd71532bac69f717",  # corp wallet balance
}

_NUMBER = re.compile(r"\d+(?:\.\d+)?")


def _python_files():
    for top in _SCANNED:
        for dirpath, dirnames, filenames in os.walk(os.path.join(_ROOT, top)):
            dirnames[:] = [d for d in dirnames if d != "__pycache__"]
            for name in filenames:
                if name.endswith(".py"):
                    yield os.path.join(dirpath, name)


def test_no_live_database_value_is_committed():
    hits = []
    for path in _python_files():
        with open(path, encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                for match in _NUMBER.findall(line):
                    if hashlib.sha256(match.encode()).hexdigest() in _FORBIDDEN_SHA256:
                        hits.append(f"{os.path.relpath(path, _ROOT)}:{lineno}")
    assert not hits, f"live database value found at {hits}"
