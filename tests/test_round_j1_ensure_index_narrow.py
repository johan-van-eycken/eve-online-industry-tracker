"""Round J1.3: schema_migrations._ensure_index logs a DB error as a WARNING
and lets a programming error propagate. Fake values only.
"""
from __future__ import annotations

import logging
import sqlite3

import pytest
from sqlalchemy.exc import OperationalError as SAOperationalError

from eve_online_industry_tracker.infrastructure import schema_migrations as sm


class _Db:
    def __init__(self, exc):
        self.exc = exc

    def execute(self, query, params=None):
        raise self.exc


@pytest.mark.parametrize("exc", [
    sqlite3.OperationalError("database is locked"),
    sqlite3.IntegrityError("UNIQUE constraint failed"),
    SAOperationalError("CREATE INDEX", {}, Exception("database is locked")),
])
def test_a_db_error_on_an_index_is_a_warning(exc, caplog):
    with caplog.at_level(logging.WARNING):
        sm._ensure_index(_Db(exc), ddl="CREATE INDEX IF NOT EXISTS ix ON t(a)", name="ix_test")
    assert any(r.levelno == logging.WARNING and "ix_test" in r.getMessage() for r in caplog.records)


def test_a_programming_error_on_an_index_propagates():
    with pytest.raises(TypeError):
        sm._ensure_index(_Db(TypeError("execute() got an unexpected keyword")), ddl="CREATE INDEX x", name="ix_test")
