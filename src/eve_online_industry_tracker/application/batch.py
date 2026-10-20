"""The result of a read over several characters or corporations."""
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional


class PartialResult(list):
    """The data of every owner (character or corporation) that worked, in
    order, plus `errors`: {owner name: error message} of each that failed;
    empty when none did. A list, so callers that only iterate keep working;
    the routes put `errors` in the reply (flask_app.http.ok_batch) and the
    pages show one warning per failed owner. When every owner fails the
    getter raises instead."""

    def __init__(self, items: Iterable[Any] = (), errors: Optional[Dict[str, str]] = None):
        super().__init__(items)
        self.errors: Dict[str, str] = dict(errors or {})


def batch_errors(value: Any) -> Dict[str, str]:
    """The per-owner errors of a PartialResult; {} for anything else."""
    return dict(value.errors) if isinstance(value, PartialResult) else {}
