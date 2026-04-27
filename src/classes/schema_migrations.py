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


def _ensure_market_orderbook_view_cache_unique_key(db: DatabaseManager) -> None:
    try:
        indexes = db.query("PRAGMA index_list(market_orderbook_view_cache);")
    except Exception as e:
        logging.warning("Failed reading market_orderbook_view_cache indexes: %s", str(e))
        return

    for row in indexes:
        try:
            if int(row[2] or 0) == 1:
                cols = db.query(f"PRAGMA index_info({row[1]!r});")
                names = [str(col[2]) for col in cols]
                if names == ["hub", "region_id", "station_id", "side", "type_id", "at_hub"]:
                    return
        except Exception:
            continue

    try:
        db.execute(
            "DELETE FROM market_orderbook_view_cache "
            "WHERE id NOT IN ("
            "SELECT MAX(id) FROM market_orderbook_view_cache "
            "GROUP BY hub, region_id, station_id, side, type_id, at_hub"
            ")"
        )
    except Exception as e:
        logging.warning("Failed deduplicating market_orderbook_view_cache: %s", str(e))

    _ensure_index(
        db,
        name="uq_market_orderbook_view_cache_key",
        ddl=(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_market_orderbook_view_cache_key "
            "ON market_orderbook_view_cache(hub, region_id, station_id, side, type_id, at_hub)"
        ),
    )


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

    for table in ("character_industry_jobs", "corporation_industry_jobs"):
        _ensure_column(db_app, table=table, column="output_quantity", ddl_type="INTEGER")
        _ensure_column(db_app, table=table, column="materials_cost", ddl_type="REAL")
        _ensure_column(db_app, table=table, column="copy_cost", ddl_type="REAL")
        _ensure_column(db_app, table=table, column="invention_cost", ddl_type="REAL")
        _ensure_column(db_app, table=table, column="total_build_cost", ddl_type="REAL")
        _ensure_column(db_app, table=table, column="unit_build_cost", ddl_type="REAL")
        _ensure_column(db_app, table=table, column="build_cost_source", ddl_type="TEXT")

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
            "total_volume INTEGER NOT NULL DEFAULT 0,"
            "order_count INTEGER NOT NULL DEFAULT 0,"
            "fetched_at REAL NOT NULL,"
            "version INTEGER NOT NULL DEFAULT 1,"
            "UNIQUE(hub, region_id, station_id, side, type_id, at_hub)"
            ")"
        ),
    )
    _ensure_column(db_app, table="market_orderbook_view_cache", column="total_volume", ddl_type="INTEGER NOT NULL DEFAULT 0")
    _ensure_column(db_app, table="market_orderbook_view_cache", column="order_count", ddl_type="INTEGER NOT NULL DEFAULT 0")
    _ensure_index(
        db_app,
        name="idx_market_orderbook_view_cache_lookup",
        ddl=(
            "CREATE INDEX IF NOT EXISTS idx_market_orderbook_view_cache_lookup "
            "ON market_orderbook_view_cache(hub, region_id, station_id, side, at_hub, type_id)"
        ),
    )
    _ensure_market_orderbook_view_cache_unique_key(db_app)

    _ensure_table(
        db_app,
        table="character_realized_sales_ledger",
        ddl=(
            "CREATE TABLE IF NOT EXISTS character_realized_sales_ledger ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "character_id INTEGER NOT NULL,"
            "transaction_id INTEGER NOT NULL,"
            "journal_ref_id INTEGER NULL,"
            "date TEXT NULL,"
            "type_id INTEGER NULL,"
            "type_name TEXT NULL,"
            "type_group_name TEXT NULL,"
            "type_category_name TEXT NULL,"
            "quantity INTEGER NOT NULL DEFAULT 0,"
            "unit_price REAL NULL,"
            "gross_revenue REAL NULL,"
            "sales_tax_amount REAL NULL,"
            "other_fees_amount REAL NULL,"
            "total_fees_amount REAL NULL,"
            "net_revenue REAL NULL,"
            "allocated_cost REAL NULL,"
            "realized_profit REAL NULL,"
            "realized_margin_fraction REAL NULL,"
            "priced_quantity INTEGER NOT NULL DEFAULT 0,"
            "unpriced_quantity INTEGER NOT NULL DEFAULT 0,"
            "source_mix JSON NULL,"
            "allocation_details JSON NULL,"
            "fee_capture_mode TEXT NULL,"
            "confidence TEXT NULL,"
            "notes JSON NULL,"
            "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
            "UNIQUE(character_id, transaction_id)"
            ")"
        ),
    )
    _ensure_index(
        db_app,
        name="idx_character_realized_sales_ledger_character_date",
        ddl=(
            "CREATE INDEX IF NOT EXISTS idx_character_realized_sales_ledger_character_date "
            "ON character_realized_sales_ledger(character_id, date)"
        ),
    )

    _ensure_table(
        db_app,
        table="corporation_realized_sales_ledger",
        ddl=(
            "CREATE TABLE IF NOT EXISTS corporation_realized_sales_ledger ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "corporation_id INTEGER NOT NULL,"
            "transaction_id INTEGER NOT NULL,"
            "journal_ref_id INTEGER NULL,"
            "date TEXT NULL,"
            "type_id INTEGER NULL,"
            "type_name TEXT NULL,"
            "type_group_name TEXT NULL,"
            "type_category_name TEXT NULL,"
            "quantity INTEGER NOT NULL DEFAULT 0,"
            "unit_price REAL NULL,"
            "gross_revenue REAL NULL,"
            "sales_tax_amount REAL NULL,"
            "other_fees_amount REAL NULL,"
            "total_fees_amount REAL NULL,"
            "net_revenue REAL NULL,"
            "allocated_cost REAL NULL,"
            "realized_profit REAL NULL,"
            "realized_margin_fraction REAL NULL,"
            "priced_quantity INTEGER NOT NULL DEFAULT 0,"
            "unpriced_quantity INTEGER NOT NULL DEFAULT 0,"
            "source_mix JSON NULL,"
            "allocation_details JSON NULL,"
            "fee_capture_mode TEXT NULL,"
            "confidence TEXT NULL,"
            "notes JSON NULL,"
            "updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
            "UNIQUE(corporation_id, transaction_id)"
            ")"
        ),
    )
    _ensure_index(
        db_app,
        name="idx_corporation_realized_sales_ledger_corporation_date",
        ddl=(
            "CREATE INDEX IF NOT EXISTS idx_corporation_realized_sales_ledger_corporation_date "
            "ON corporation_realized_sales_ledger(corporation_id, date)"
        ),
    )
