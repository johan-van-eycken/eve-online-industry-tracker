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
from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.models import ItemDecision  # noqa: E402


def _service(session_provider, admin_settings=None) -> DailyPlannerService:
    """Build a DailyPlannerService against the real repo/session_provider fixtures.

    Only `repo` and `session_provider` are exercised by get_active_plan(),
    get_analytics() and _persist_plan_items() -- the other collaborators are
    only touched by the compute-pipeline phases, which these tests don't run,
    so plain `None` stubs are enough to satisfy __init__ without raising.
    """
    repo = DailyPlannerRepository(session_provider=session_provider)
    return DailyPlannerService(
        industry_service=None,
        corporations_service=None,
        characters_service=None,
        sales_history_service=None,
        market_pricing_service=None,
        realized_profit_service=None,
        repo=repo,
        admin_settings=admin_settings,
        session_provider=session_provider,
    )


class _AdminSettings:
    """Minimal admin_settings stub matching `_adm`'s `.get(section, key)` shape."""

    def __init__(self, **values):
        self._values = values

    def get(self, section, key):
        return self._values.get(key)


def _decision(meta_group_id: int | None = None) -> ItemDecision:
    """A minimal ItemDecision, standing in for Phase 4 output."""
    return ItemDecision(
        type_id=590,
        type_name="Rifter",
        decision="build",
        decision_reason="test",
        adjusted_score=10_000_000.0,
        absolute_profit_per_batch=50_000_000.0,
        isk_per_hour=10_000_000.0,
        margin_pct=0.15,
        days_of_supply_current=1.0,
        effective_velocity=10.0,
        meta_group_id=meta_group_id,
        pipeline_stage="manufacturing",
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


# ---------------------------------------------------------------------------
# DailyPlannerService.get_analytics() -- plan_history (finding 13)
# ---------------------------------------------------------------------------

def test_plan_history_returns_recent_plans(app_session, session_provider):
    from datetime import datetime, timedelta

    from eve_online_industry_tracker.infrastructure.models import BuildPlanModel

    now = datetime.utcnow()
    app_session.add(BuildPlanModel(
        created_at=now, updated_at=now, status="active", freshness_score=1.0,
    ))
    # 45 days ago: inside the *old* code's hardcoded 90-day cutoff (which
    # ignored planner_history_days entirely), but outside the 30-day window
    # configured below -- a boundary the old code gets wrong two ways at
    # once: it never consulted planner_history_days, and it compared
    # `created_at >= cutoff.isoformat()` (a string) against a DateTime
    # column instead of a real datetime.
    app_session.add(BuildPlanModel(
        created_at=now - timedelta(days=45), updated_at=now, status="superseded",
        freshness_score=0.8,
    ))
    app_session.add(BuildPlanModel(
        created_at=now - timedelta(days=200), updated_at=now, status="superseded",
        freshness_score=0.5,
    ))
    app_session.commit()

    service = _service(session_provider, admin_settings=_AdminSettings(planner_history_days=30))
    history = service.get_analytics()["plan_history"]

    # Only the "now" plan is inside the configured 30-day window. Asserting
    # the exact boundary (not merely "non-empty") is what actually catches
    # finding 13: the old code's hardcoded 90-day cutoff would let the
    # 45-day-old plan back in, producing 2 rows instead of 1.
    assert len(history) == 1
    assert history[0]["status"] == "active"

    # Second boundary: a row cap, not a day cutoff. Configure a window well
    # above 90 days and put more than 90 plans inside it -- the exact same
    # class of bug as the cutoff above (a hardcoded limit silently
    # truncating the configured retention window), this time by row count.
    # The 3 plans already committed above land inside this 180-day window
    # too, except the 200-day-old one, so the running total below accounts
    # for those.
    for days_ago in range(1, 96):
        app_session.add(BuildPlanModel(
            created_at=now - timedelta(days=days_ago), updated_at=now,
            status="superseded", freshness_score=0.9,
        ))
    app_session.commit()

    service_180 = _service(session_provider, admin_settings=_AdminSettings(planner_history_days=180))
    history_180 = service_180.get_analytics()["plan_history"]

    # Inside the 180-day window: the "now" plan, the "-45 days" plan from
    # above, and the 95 new ones just added (1..95 days ago) = 97 rows.
    # The 200-day-old plan stays excluded. A reinstated `.limit(90)` would
    # clamp this to 90 -- asserting the exact count (not "> 90") pins the
    # behaviour rather than merely gesturing at it.
    assert len(history_180) == 97
    assert history_180[0]["status"] == "active"


def test_plan_history_keeps_an_unknown_freshness_unknown(app_session, session_provider):
    """F5: `freshness_score or 1.0` showed a NULL score as 100% fresh and a
    real 0.0 (everything drifted) as 100% too."""
    from datetime import datetime

    from eve_online_industry_tracker.infrastructure.models import BuildPlanModel

    now = datetime.utcnow()
    app_session.add(BuildPlanModel(created_at=now, updated_at=now, status="active",
                                   freshness_score=None))
    app_session.add(BuildPlanModel(created_at=now, updated_at=now, status="superseded",
                                   freshness_score=0.0))
    app_session.commit()

    history = _service(session_provider, admin_settings=_AdminSettings(planner_history_days=30)
                       ).get_analytics()["plan_history"]

    assert sorted(h["freshness_score"] is None for h in history) == [False, True]
    assert [h["freshness_score"] for h in history if h["freshness_score"] is not None] == [0.0]


# ---------------------------------------------------------------------------
# DailyPlannerService._persist_plan_items() -- meta_group_id persistence (D6)
# ---------------------------------------------------------------------------

def test_plan_items_persist_the_resolved_meta_group_id(app_session, session_provider):
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    service = _service(session_provider)
    service._persist_plan_items(plan_id=1, decisions=[_decision(meta_group_id=2)])

    item = app_session.query(BuildPlanItemModel).one()
    assert item.meta_group_id == 2


# ---------------------------------------------------------------------------
# DailyPlannerService._build_plan_items() / _persist_plan_items() --
# snapshot_sell_price write (finding 15 fix round 1)
#
# This pins the one line that *is* the finding-15 fix:
# `snapshot_sell_price=_spot_sell_price(market_depth_cache.get(d.type_id))`.
# Without a test on it, a future refactor could drop the market_depth_cache
# argument (or read the wrong entry) and every plan item would persist
# snapshot_sell_price=None -- freshness would then silently freeze at a
# confident-looking 1.0 forever, with the rest of the suite green. The price
# below is distinctive (not 0 or 1) so a wrong source or a dropped argument
# shows up as a mismatch rather than a coincidental match.
# ---------------------------------------------------------------------------

def test_persisted_plan_item_snapshot_price_matches_the_market_depth_entry(
    app_session, session_provider
):
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    service = _service(session_provider)
    # _decision() defaults to type_id=590 (Rifter) -- see _decision() above.
    market_depth_cache = {590: {"spot_sell_price": 123456.78}}

    built = service._build_plan_items(
        plan_id=1, decisions=[_decision()], market_depth_cache=market_depth_cache
    )
    assert built[0].snapshot_sell_price == 123456.78

    service._persist_plan_items(
        plan_id=1, decisions=[_decision()], market_depth_cache=market_depth_cache
    )

    # Round trip through a separate session -- confirms the value was
    # actually written to the DB, not just present on the in-memory object.
    item = app_session.query(BuildPlanItemModel).one()
    assert item.snapshot_sell_price == 123456.78


def test_persisted_plan_item_snapshot_price_is_none_when_type_id_has_no_market_depth_entry(
    app_session, session_provider
):
    """A decision whose type_id isn't in market_depth_cache must not silently
    borrow another item's price -- it stays NULL (excluded from freshness'
    denominator), not some wrong or stale value."""
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    service = _service(session_provider)
    service._persist_plan_items(
        plan_id=1,
        decisions=[_decision()],
        market_depth_cache={999: {"spot_sell_price": 42.0}},  # unrelated type_id
    )

    item = app_session.query(BuildPlanItemModel).one()
    assert item.snapshot_sell_price is None


# ---------------------------------------------------------------------------
# An unknown sell velocity persists as NULL, never as the analyzer's 0.01 floor
# ---------------------------------------------------------------------------

def _unknown_velocity_decision() -> ItemDecision:
    import dataclasses

    return dataclasses.replace(
        _decision(),
        effective_velocity=0.01,
        velocity_unknown_reason="no corp sales in 30 days and no days-of-supply estimate",
    )


def test_an_unknown_velocity_persists_as_null_not_the_floor(app_session, session_provider):
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    service = _service(session_provider)
    service._persist_plan_items(
        plan_id=1, decisions=[_unknown_velocity_decision()], market_depth_cache={}
    )
    assert app_session.query(BuildPlanItemModel).one().effective_velocity is None


def test_a_measured_velocity_still_persists_as_is(app_session, session_provider):
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    service = _service(session_provider)
    service._persist_plan_items(plan_id=1, decisions=[_decision()], market_depth_cache={})
    assert app_session.query(BuildPlanItemModel).one().effective_velocity == 10.0


def test_readers_do_not_treat_a_null_velocity_as_measured(app_session, session_provider):
    from datetime import datetime
    from types import SimpleNamespace

    from eve_online_industry_tracker.application.daily_planner.feedback_processor import (
        _estimate_predicted_sell_days,
    )
    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import (
        MarketDepthCollector,
    )
    from eve_online_industry_tracker.infrastructure.models import (
        BuildPlanItemModel,
        BuildPlanModel,
    )

    now = datetime(2026, 1, 1)
    app_session.add(BuildPlanModel(id=1, created_at=now, updated_at=now, status="active"))
    app_session.commit()
    _service(session_provider)._persist_plan_items(
        plan_id=1, decisions=[_unknown_velocity_decision()], market_depth_cache={}
    )
    item = app_session.query(BuildPlanItemModel).one()

    # feedback_processor: NULL takes the 7.0 default, not the 30-day clamp of 1/0.01.
    assert _estimate_predicted_sell_days(item) == 7.0
    assert _estimate_predicted_sell_days(SimpleNamespace(effective_velocity=0.01)) == 30.0

    # market_depth_collector: the NULL row is skipped, so with no corp to fall
    # back on the velocity is unknown (None) and competition_index stays None.
    collector = object.__new__(MarketDepthCollector)
    collector._sessions = SimpleNamespace(app_session=lambda: app_session)
    assert collector._get_effective_velocity(item.type_id, None) is None


def test_a_bpo_analysis_skip_reason_is_persisted(app_session, session_provider):
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    decision = _decision()
    decision.bpo_analysis_skip_reason = "no BPO market price for bp_type_id=999"
    _service(session_provider)._persist_plan_items(
        plan_id=1, decisions=[decision], market_depth_cache={}
    )
    item = app_session.query(BuildPlanItemModel).one()
    assert item.bpo_analysis_skip_reason == "no BPO market price for bp_type_id=999"


def test_ensure_app_schema_adds_the_skip_reason_column_to_a_legacy_table() -> None:
    conn = sqlite3.connect(":memory:")
    db = _FakeDb(conn)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS market_history (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "type_id INTEGER NOT NULL, region_id INTEGER NOT NULL, date TEXT NOT NULL,"
        "close REAL NOT NULL, high REAL, low REAL, volume INTEGER NOT NULL,"
        "order_count INTEGER NOT NULL, fetched_at DATETIME, updated_at DATETIME,"
        "UNIQUE(type_id, region_id, date))"
    )
    conn.execute(
        "CREATE TABLE build_plan_item (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "plan_id INTEGER NOT NULL, type_id INTEGER NOT NULL, decision TEXT NOT NULL)"
    )
    conn.commit()

    ensure_app_schema(db)
    ensure_app_schema(db)  # idempotent

    columns = {row[1] for row in db.query("PRAGMA table_info(build_plan_item)")}
    assert "bpo_analysis_skip_reason" in columns
