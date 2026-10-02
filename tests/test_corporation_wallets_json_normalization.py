from __future__ import annotations

import json
import os
import tempfile

import pytest

from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.schema_migrations import (
    normalize_double_encoded_json_column,
)

# Mirrors real corporations.wallets rows: division/balance are strings, not numbers.
_WALLETS = [
    {"division": "1", "division_name": "Master Wallet", "balance": "123456789.1234"},
    {"division": "2", "division_name": "Division 2", "balance": "0.0"},
]


@pytest.fixture()
def db() -> DatabaseManager:
    """A DatabaseManager backed by a real temp sqlite file.

    A bare `sqlite://` (in-memory) engine is avoided here: DatabaseManager's
    QueuePool settings (pool_size/max_overflow) can hand out separate
    connections that each see their own private :memory: database, so a row
    inserted on one connection may not be visible to a query on another. A
    temp file sidesteps that entirely and is closer to how the app actually
    opens eve_app.db.
    """
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    manager = DatabaseManager(f"sqlite:///{path}")
    manager.execute("CREATE TABLE corporations (id INTEGER PRIMARY KEY, wallets TEXT, standings TEXT)")
    try:
        yield manager
    finally:
        manager.engine.dispose()
        os.remove(path)


def _insert(db: DatabaseManager, row_id: int, wallets) -> None:
    db.execute(
        "INSERT INTO corporations (id, wallets, standings) VALUES (:id, :wallets, NULL)",
        {"id": row_id, "wallets": wallets},
    )


def _read_wallets(db: DatabaseManager, row_id: int):
    rows = db.query("SELECT wallets FROM corporations WHERE id = :id", {"id": row_id})
    return rows[0][0]


def test_normalizes_a_double_encoded_row_to_single_encoded(db):
    double_encoded = json.dumps(json.dumps(_WALLETS))
    _insert(db, 1, double_encoded)

    fixed = normalize_double_encoded_json_column(db, table="corporations", column="wallets")

    assert fixed == 1
    stored = _read_wallets(db, 1)
    assert json.loads(stored) == _WALLETS
    # Exactly one more decode should be needed after normalization.
    assert not isinstance(json.loads(stored), str)


def test_is_a_noop_on_an_already_single_encoded_row(db):
    single_encoded = json.dumps(_WALLETS)
    _insert(db, 1, single_encoded)

    fixed = normalize_double_encoded_json_column(db, table="corporations", column="wallets")

    assert fixed == 0
    assert _read_wallets(db, 1) == single_encoded


def test_is_a_noop_on_a_null_row(db):
    _insert(db, 1, None)

    fixed = normalize_double_encoded_json_column(db, table="corporations", column="wallets")

    assert fixed == 0
    assert _read_wallets(db, 1) is None


def test_is_idempotent_across_two_runs(db):
    double_encoded = json.dumps(json.dumps(_WALLETS))
    _insert(db, 1, double_encoded)

    first = normalize_double_encoded_json_column(db, table="corporations", column="wallets")
    second = normalize_double_encoded_json_column(db, table="corporations", column="wallets")

    assert first == 1
    assert second == 0
    assert json.loads(_read_wallets(db, 1)) == _WALLETS


def test_fixes_multiple_rows_and_columns_independently(db):
    db.execute("INSERT INTO corporations (id, wallets, standings) VALUES (1, :w, :s)", {
        "w": json.dumps(json.dumps(_WALLETS)),
        "s": json.dumps([{"from_id": "1", "standing": "0.1"}]),
    })
    db.execute("INSERT INTO corporations (id, wallets, standings) VALUES (2, NULL, :s)", {
        "s": json.dumps(json.dumps([{"from_id": "2", "standing": "-0.2"}])),
    })

    fixed_wallets = normalize_double_encoded_json_column(db, table="corporations", column="wallets")
    fixed_standings = normalize_double_encoded_json_column(db, table="corporations", column="standings")

    assert fixed_wallets == 1  # only row 1's wallets was double-encoded
    assert fixed_standings == 1  # only row 2's standings was double-encoded

    row1 = db.query("SELECT wallets, standings FROM corporations WHERE id = 1")[0]
    row2 = db.query("SELECT wallets, standings FROM corporations WHERE id = 2")[0]
    assert json.loads(row1[0]) == _WALLETS
    assert not isinstance(json.loads(row1[1]), str)  # already correct, untouched
    assert json.loads(row2[1]) == [{"from_id": "2", "standing": "-0.2"}]
