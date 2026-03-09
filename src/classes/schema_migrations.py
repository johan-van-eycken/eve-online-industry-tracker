from __future__ import annotations

import logging

from classes.database_manager import DatabaseManager


def _ensure_table(db: DatabaseManager, *, ddl: str, table: str) -> None:
    try:
        db.execute(ddl)
        logging.info("Ensured table %s", table)
    except Exception as e:
        logging.warning("Failed ensuring table %s: %s", table, str(e))


def _ensure_index(db: DatabaseManager, *, ddl: str, name: str) -> None:
    try:
        db.execute(ddl)
        logging.info("Ensured index %s", name)
    except Exception as e:
        logging.warning("Failed ensuring index %s: %s", name, str(e))


def _table_columns(db: DatabaseManager, table: str) -> set[str]:
    try:
        rows = db.query(f"PRAGMA table_info({table});")
    except Exception:
        return set()

    cols: set[str] = set()
    for r in rows:
        # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
        try:
            cols.add(str(r[1]))
        except Exception:
            continue
    return cols


def _ensure_column(db: DatabaseManager, *, table: str, column: str, ddl_type: str) -> None:
    cols = _table_columns(db, table)
    if column in cols:
        return

    try:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")
        logging.info("Added column %s.%s", table, column)
    except Exception as e:
        # SQLite only supports limited ALTER TABLE; this should be safe for ADD COLUMN.
        logging.warning("Failed adding column %s.%s: %s", table, column, str(e))


def ensure_app_schema(db_app: DatabaseManager) -> None:
    """Best-effort forward migrations for the app DB.

    SQLAlchemy's create_all does not alter existing tables; this adds new columns
    used by newer app versions.
    """

    # Character assets cost-basis/provenance columns
    for table in ("character_assets", "corporation_assets"):
        _ensure_column(db_app, table=table, column="acquisition_source", ddl_type="TEXT")
        _ensure_column(db_app, table=table, column="acquisition_unit_cost", ddl_type="REAL")
        _ensure_column(db_app, table=table, column="acquisition_total_cost", ddl_type="REAL")
        _ensure_column(db_app, table=table, column="acquisition_reference_type", ddl_type="TEXT")
        _ensure_column(db_app, table=table, column="acquisition_reference_id", ddl_type="INTEGER")
        _ensure_column(db_app, table=table, column="acquisition_date", ddl_type="TEXT")
        _ensure_column(db_app, table=table, column="acquisition_updated_at", ddl_type="TEXT")

    # Character market fee metadata (best-effort JSON blob)
    _ensure_column(db_app, table="characters", column="market_fees", ddl_type="TEXT")

    # Market orderbook view cache (persistent hub pricing aggregates)
    _ensure_table(
        db_app,
        table="market_orderbook_view_cache",
        ddl=(
            "CREATE TABLE IF NOT EXISTS market_orderbook_view_cache ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "hub TEXT NOT NULL,"
            "region_id INTEGER NOT NULL,"
            "station_id INTEGER NOT NULL,"
            "side TEXT NOT NULL,"
            "type_id INTEGER NOT NULL,"
            "at_hub INTEGER NOT NULL,"
            "depth INTEGER NOT NULL DEFAULT 200,"
            "levels TEXT NULL,"
            "fetched_at REAL NOT NULL,"
            "version INTEGER NOT NULL DEFAULT 1,"
            "UNIQUE(hub, region_id, station_id, side, type_id, at_hub)"
            ")"
        ),
    )
    _ensure_index(
        db_app,
        name="idx_market_orderbook_view_cache_lookup",
        ddl=(
            "CREATE INDEX IF NOT EXISTS idx_market_orderbook_view_cache_lookup "
            "ON market_orderbook_view_cache(hub, region_id, station_id, side, at_hub, type_id)"
        ),
    )
