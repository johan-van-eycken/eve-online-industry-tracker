from __future__ import annotations

import sys
from pathlib import Path


def pytest_configure() -> None:
    # Allow importing the app without requiring an editable install.
    repo_root = Path(__file__).resolve().parents[1]
    src = repo_root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
