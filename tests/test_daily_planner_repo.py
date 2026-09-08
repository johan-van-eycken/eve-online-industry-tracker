from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.infrastructure.models import BaseApp  # noqa: E402
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_app_schema  # noqa: E402
from eve_online_industry_tracker.infrastructure.persistence.daily_planner_repo import DailyPlannerRepository  # noqa: E402
from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    BuildPlanModel,
    BuildPlanItemModel,
    DailyActionLogModel,
    MarketDepthCacheModel,
    InventionOutcomeLogModel,
    PlanLearningWeightsModel,
)


# ---------------------------------------------------------------------------
# Lightweight stubs so we can call ensure_app_schema without a full
# DatabaseManager (which has engine kwargs incompatible with :memory: in tests)
# ---------------------------------------------------------------------------

class _FakeDb:
    """Minimal DatabaseManager stand-in backed by sqlite3 directly."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, query: str, params=None) -> None:
        if params:
            self._conn.execute(query, params)
        else:
            self._conn.execute(query)
        self._conn.commit()

    def query(self, query: str, params=None) -> list:
        cur = self._conn.execute(query) if not params else self._conn.execute(query, params)
        return cur.fetchall()


_PLANNER_TABLES = [
    "build_plan",
    "build_plan_item",
    "daily_action_log",
    "plan_item_outcome",
    "plan_learning_weights",
    "market_depth_cache",
    "invention_outcome_log",
    "margin_correlation_cache",
]


class _SessionProvider:
    """Minimal SessionProvider backed by a SQLAlchemy sessionmaker."""

    def __init__(self, factory) -> None:
        self._factory = factory

    def app_session(self) -> Session:
        return self._factory()

    def sde_session(self) -> Session:
        raise NotImplementedError

    def oauth_session(self) -> Session:
        raise NotImplementedError


def _make_engine():
    return create_engine("sqlite:///:memory:")


def _make_repo() -> tuple[DailyPlannerRepository, sqlite3.Connection]:
    """
    Create a repo backed by an in-memory SQLite DB.

    We use two separate connections to the same :memory: DB is not possible
    across connections, so we run ensure_app_schema via sqlite3, then create
    the SQLAlchemy engine attached to the same file path (temp file approach),
    or just create everything via BaseApp.metadata + raw sqlite3 DDL.
    """
    # Use SQLAlchemy engine directly - create tables via BaseApp.metadata
    # and run schema migration DDL against the same engine.
    engine = _make_engine()

    # Create all existing ORM tables
    BaseApp.metadata.create_all(bind=engine)

    # Also run ensure_app_schema to create the 8 planner tables via the migration path.
    # We use a thin wrapper so the schema migration DDL runs against our engine.
    class _EngineDb:
        def __init__(self, eng):
            self._eng = eng

        def execute(self, query: str, params=None) -> None:
            from sqlalchemy import text
            with self._eng.begin() as conn:
                conn.execute(text(query), params or {})

        def query(self, query: str, params=None) -> list:
            from sqlalchemy import text
            with self._eng.begin() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchall()

    ensure_app_schema(_EngineDb(engine))

    factory = sessionmaker(bind=engine)
    provider = _SessionProvider(factory)
    repo = DailyPlannerRepository(provider)

    # Return repo and an sqlite3 connection for low-level schema checks
    raw_conn = sqlite3.connect(":memory:")  # separate for pragma checks only
    return repo, engine


def _now() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


# ---------------------------------------------------------------------------
# Schema verification
# ---------------------------------------------------------------------------

def test_ensure_app_schema_creates_all_planner_tables() -> None:
    """ensure_app_schema must create all 8 planner tables."""
    conn = sqlite3.connect(":memory:")
    db = _FakeDb(conn)

    # The schema migration references market_history which doesn't exist yet
    # in a bare DB, so we create it first (the index depends on it).
    conn.execute(
        "CREATE TABLE IF NOT EXISTS market_history ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "type_id INTEGER NOT NULL,"
        "region_id INTEGER NOT NULL,"
        "date TEXT NOT NULL,"
        "close REAL NOT NULL,"
        "high REAL,"
        "low REAL,"
        "volume INTEGER NOT NULL,"
        "order_count INTEGER NOT NULL,"
        "fetched_at DATETIME,"
        "updated_at DATETIME,"
        "UNIQUE(type_id, region_id, date)"
        ")"
    )
    conn.commit()

    ensure_app_schema(db)

    for table_name in _PLANNER_TABLES:
        rows = db.query(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        assert rows, f"Table '{table_name}' was not created by ensure_app_schema"


# ---------------------------------------------------------------------------
# build_plan round-trip
# ---------------------------------------------------------------------------

def test_insert_and_get_active_plan() -> None:
    repo, _ = _make_repo()

    now = _now()
    plan = BuildPlanModel(
        created_at=now,
        updated_at=now,
        status="active",
        corp_wallet_snapshot=1_000_000_000.0,
        freshness_score=1.0,
    )
    plan_id = repo.insert_plan(plan)
    assert isinstance(plan_id, int)
    assert plan_id > 0

    fetched = repo.get_active_plan()
    assert fetched is not None
    assert fetched.id == plan_id
    assert fetched.status == "active"
    assert fetched.corp_wallet_snapshot == 1_000_000_000.0


def test_archive_active_plan() -> None:
    repo, _ = _make_repo()

    now = _now()
    plan = BuildPlanModel(created_at=now, updated_at=now, status="active")
    repo.insert_plan(plan)

    repo.archive_active_plan()
    assert repo.get_active_plan() is None


def test_update_freshness_score() -> None:
    repo, _ = _make_repo()

    now = _now()
    plan = BuildPlanModel(created_at=now, updated_at=now, status="active", freshness_score=1.0)
    plan_id = repo.insert_plan(plan)

    repo.update_freshness_score(plan_id, 0.75)
    fetched = repo.get_active_plan()
    assert fetched is not None
    assert fetched.freshness_score == 0.75


# ---------------------------------------------------------------------------
# build_plan_item round-trip
# ---------------------------------------------------------------------------

def test_insert_and_get_plan_items() -> None:
    repo, _ = _make_repo()

    now = _now()
    plan = BuildPlanModel(created_at=now, updated_at=now, status="active")
    plan_id = repo.insert_plan(plan)

    items = [
        BuildPlanItemModel(
            plan_id=plan_id,
            type_id=590,
            type_name="Rifter",
            decision="build",
            priority_score=18.5,
            isk_per_hour=18_500_000.0,
        ),
        BuildPlanItemModel(
            plan_id=plan_id,
            type_id=24700,
            type_name="Cerberus",
            decision="watch",
            priority_score=6.0,
            isk_per_hour=6_000_000.0,
        ),
    ]
    repo.insert_plan_items(items)

    fetched = repo.get_plan_items(plan_id)
    assert len(fetched) == 2
    type_ids = {r.type_id for r in fetched}
    assert 590 in type_ids
    assert 24700 in type_ids


# ---------------------------------------------------------------------------
# daily_action_log round-trip
# ---------------------------------------------------------------------------

def test_insert_and_get_actions() -> None:
    repo, _ = _make_repo()

    now = _now()
    plan = BuildPlanModel(created_at=now, updated_at=now, status="active")
    plan_id = repo.insert_plan(plan)

    actions = [
        DailyActionLogModel(
            plan_id=plan_id,
            generated_at=now,
            character_id=93_000_001,
            character_name="Aldara Voss",
            action_type="manufacture",
            type_id=590,
            type_name="Rifter",
            quantity=5,
            runs=5,
            status="pending",
            processed_for_feedback=False,
        ),
        DailyActionLogModel(
            plan_id=plan_id,
            generated_at=now,
            action_type="buy_materials",
            type_id=34,
            type_name="Tritanium",
            quantity=1_000_000,
            status="pending",
            processed_for_feedback=False,
        ),
    ]
    repo.insert_actions(actions)

    fetched = repo.get_actions(plan_id)
    assert len(fetched) == 2
    action_types = {r.action_type for r in fetched}
    assert "manufacture" in action_types
    assert "buy_materials" in action_types


def test_mark_action_done() -> None:
    repo, _ = _make_repo()

    now = _now()
    plan = BuildPlanModel(created_at=now, updated_at=now, status="active")
    plan_id = repo.insert_plan(plan)

    action = DailyActionLogModel(
        plan_id=plan_id,
        generated_at=now,
        action_type="deliver",
        type_id=590,
        type_name="Rifter",
        status="pending",
        processed_for_feedback=False,
    )
    repo.insert_actions([action])

    actions = repo.get_actions(plan_id)
    action_id = actions[0].id
    repo.mark_action_done(action_id)

    updated = repo.get_actions(plan_id)
    assert updated[0].status == "done"


def test_get_unprocessed_done_actions() -> None:
    repo, _ = _make_repo()

    now = _now()
    plan = BuildPlanModel(created_at=now, updated_at=now, status="active")
    plan_id = repo.insert_plan(plan)

    actions = [
        DailyActionLogModel(
            plan_id=plan_id, generated_at=now, action_type="manufacture",
            type_id=590, status="done", processed_for_feedback=False,
        ),
        DailyActionLogModel(
            plan_id=plan_id, generated_at=now, action_type="deliver",
            type_id=590, status="done", processed_for_feedback=True,
        ),
        DailyActionLogModel(
            plan_id=plan_id, generated_at=now, action_type="buy_materials",
            type_id=34, status="pending", processed_for_feedback=False,
        ),
    ]
    repo.insert_actions(actions)

    unprocessed = repo.get_unprocessed_done_actions()
    assert len(unprocessed) == 1
    assert unprocessed[0].action_type == "manufacture"


# ---------------------------------------------------------------------------
# market_depth_cache upsert (INSERT OR REPLACE)
# ---------------------------------------------------------------------------

def test_upsert_market_depth_and_get() -> None:
    repo, _ = _make_repo()

    now = _now()
    rows = [
        MarketDepthCacheModel(
            type_id=590, hub="jita",
            competitor_units=10_000, vwap_5d=5_100_000.0,
            spot_sell_price=5_200_000.0, competition_index=1.2, snapshot_at=now,
        ),
    ]
    repo.upsert_market_depth(rows)

    result = repo.get_market_depth([590], "jita")
    assert 590 in result
    assert result[590].vwap_5d == 5_100_000.0

    # Overwrite with new data (INSERT OR REPLACE semantics)
    rows2 = [
        MarketDepthCacheModel(
            type_id=590, hub="jita",
            competitor_units=15_000, vwap_5d=5_050_000.0,
            spot_sell_price=5_100_000.0, competition_index=1.5, snapshot_at=now,
        ),
    ]
    repo.upsert_market_depth(rows2)

    result2 = repo.get_market_depth([590], "jita")
    assert result2[590].vwap_5d == 5_050_000.0
    assert result2[590].competitor_units == 15_000


# ---------------------------------------------------------------------------
# invention_outcome_log INSERT OR IGNORE
# ---------------------------------------------------------------------------

def test_log_invention_outcome_idempotent() -> None:
    repo, _ = _make_repo()

    now = _now()
    outcome = InventionOutcomeLogModel(
        type_id=12003, blueprint_type_id=12019,
        theoretical_success_pct=0.42, was_success=True,
        character_id=93_000_001, completed_at=now,
    )
    repo.log_invention_outcome(outcome)
    # second call with same key should be silently ignored
    repo.log_invention_outcome(outcome)

    rates = repo.get_invention_success_rates([12003], min_attempts=1)
    assert 12003 in rates
    assert rates[12003] == 1.0


# ---------------------------------------------------------------------------
# plan_learning_weights upsert
# ---------------------------------------------------------------------------

def test_upsert_weights_and_get() -> None:
    repo, _ = _make_repo()

    now = _now()
    weights = PlanLearningWeightsModel(
        type_id=590,
        accuracy_ema=0.95,
        velocity_multiplier=1.08,
        cost_multiplier=0.97,
        sample_count=8,
        last_updated=now,
        confidence_tier="medium",
    )
    repo.upsert_weights(weights)

    result = repo.get_weights([590])
    assert 590 in result
    assert result[590].accuracy_ema == 0.95
    assert result[590].confidence_tier == "medium"

    # Update via upsert
    weights2 = PlanLearningWeightsModel(
        type_id=590,
        accuracy_ema=0.90,
        velocity_multiplier=1.10,
        cost_multiplier=0.95,
        sample_count=12,
        last_updated=now,
        confidence_tier="medium",
    )
    repo.upsert_weights(weights2)

    result2 = repo.get_weights([590])
    assert result2[590].accuracy_ema == 0.90
    assert result2[590].sample_count == 12
