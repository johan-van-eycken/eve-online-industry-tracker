"""A failed ADD COLUMN or ensure_table fails startup with a clear
message (SchemaMigrationError), instead of a WARNING after which the ORM
fails obscurely ("no such column") wherever the model is read.

The choice: fail startup. The ORM maps every one of these columns and
tables, so the app cannot read or write the model without them; running on
would turn one clear startup error into errors on every page. A failed
startup is shown on /health (not_ready, with the error) and in the log.

A column of a table that does not exist yet is skipped (create_all makes the
table with all its columns). Fake values only.
"""
from __future__ import annotations

import sqlite3
import logging

import pytest
from sqlalchemy.exc import OperationalError as SAOperationalError

from eve_online_industry_tracker.infrastructure import schema_migrations as sm


class _Db:
    """A sqlite3-backed stand-in for DatabaseManager; fail_on: SQL prefixes that raise."""

    def __init__(self, fail_on=()):
        self.conn = sqlite3.connect(":memory:")
        self.fail_on = tuple(fail_on)

    def execute(self, query, params=None):
        if any(query.startswith(prefix) for prefix in self.fail_on):
            raise sqlite3.OperationalError("attempt to write a readonly database")
        self.conn.execute(query, params or {})
        self.conn.commit()

    def query(self, query, params=None):
        if any(query.startswith(prefix) for prefix in self.fail_on):
            raise sqlite3.OperationalError("database disk image is malformed")
        return self.conn.execute(query, params or {}).fetchall()


def test_a_failed_add_column_fails_with_a_clear_message():
    db = _Db(fail_on=("ALTER TABLE",))
    db.conn.execute("CREATE TABLE character_assets (id INTEGER PRIMARY KEY)")
    with pytest.raises(sm.SchemaMigrationError, match=r"character_assets\.acquisition_source.*readonly"):
        sm.ensure_app_schema(db)


def test_a_failed_ensure_table_fails_with_a_clear_message():
    db = _Db(fail_on=("CREATE TABLE",))
    with pytest.raises(sm.SchemaMigrationError, match="readonly"):
        sm._ensure_table(db, ddl="CREATE TABLE IF NOT EXISTS x (id INTEGER)", table="x")


def test_an_unreadable_column_list_fails_with_a_clear_message():
    db = _Db(fail_on=("PRAGMA table_info",))
    with pytest.raises(sm.SchemaMigrationError, match="malformed"):
        sm._ensure_column(db, table="character_assets", column="c", ddl_type="TEXT")


def test_a_column_of_a_missing_table_is_skipped():
    db = _Db()
    sm._ensure_column(db, table="not_there_yet", column="c", ddl_type="TEXT")
    assert db.query("SELECT name FROM sqlite_master WHERE name='not_there_yet'") == []


def test_the_message_names_the_fix():
    db = _Db(fail_on=("CREATE TABLE",))
    with pytest.raises(sm.SchemaMigrationError) as info:
        sm._ensure_table(db, ddl="CREATE TABLE IF NOT EXISTS x (id INTEGER)", table="x")
    assert "restart" in str(info.value)


def test_a_full_migration_on_a_fresh_db_still_succeeds_twice():
    db = _Db()
    sm.ensure_app_schema(db)
    sm.ensure_app_schema(db)


# --- _ensure_index -----------------------------------------------------------------------------
#
# schema_migrations._ensure_index logs a DB error as a WARNING
# and lets a programming error propagate. Fake values only.

class _IndexDb:
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
        sm._ensure_index(_IndexDb(exc), ddl="CREATE INDEX IF NOT EXISTS ix ON t(a)", name="ix_test")
    assert any(r.levelno == logging.WARNING and "ix_test" in r.getMessage() for r in caplog.records)


def test_a_programming_error_on_an_index_propagates():
    with pytest.raises(TypeError):
        sm._ensure_index(_IndexDb(TypeError("execute() got an unexpected keyword")), ddl="CREATE INDEX x", name="ix_test")
