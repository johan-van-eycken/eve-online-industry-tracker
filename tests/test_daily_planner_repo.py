from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from eve_online_industry_tracker.infrastructure.models import BaseApp  # noqa: E402
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_app_schema  # noqa: E402
from eve_online_industry_tracker.infrastructure.persistence.daily_planner_repo import DailyPlannerRepository  # noqa: E402
from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    BuildPlanModel,
    BuildPlanItemModel,
    DailyActionLogModel,
    MarketDepthCacheModel,
    InventionOutcomeLogModel,
    PlanItemOutcomeModel,
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
        repo=repo,
        admin_settings=admin_settings,
        session_provider=session_provider,
    )


class _AdminSettings:
    """Minimal admin_settings stub matching `_adm`'s `.get(section, key)` shape."""

    def __init__(self, **values):
        self._values = values

    def get(self, section, key):
        if key in self._values:
            return self._values[key]
        from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA
        return ADMIN_SETTINGS_SCHEMA[section]["settings"][key]["default"]   # an unset setting: its default


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
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


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


def _feedback_rows(now):
    outcome = PlanItemOutcomeModel(
        plan_item_id=0, type_id=590, completed_at=now, predicted_isk_per_hour=1.0,
        actual_isk_per_hour=1.0, accuracy_ratio=1.0, predicted_sell_days=7.0,
        actual_sell_days=7.0, slow_mover=False,
    )
    weights = PlanLearningWeightsModel(
        type_id=590, accuracy_ema=1.0, velocity_multiplier=1.0, cost_multiplier=1.0,
        sample_count=1, last_updated=now, confidence_tier="low",
    )
    return outcome, weights


def test_record_feedback_commits_nothing_when_marking_processed_fails(monkeypatch) -> None:
    """A locked database on the last step must not leave the outcome and
    weights behind, or the next compute counts the action a second time."""
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.orm import Query

    repo, _ = _make_repo()
    now = _now()
    plan_id = repo.insert_plan(BuildPlanModel(created_at=now, updated_at=now, status="active"))
    repo.insert_actions([DailyActionLogModel(
        plan_id=plan_id, generated_at=now, action_type="manufacture", type_id=590,
        type_name="Rifter", quantity=5, status="done", processed_for_feedback=False,
    )])
    action_id = repo.get_actions(plan_id)[0].id

    def _locked(*_args, **_kwargs):
        raise OperationalError("UPDATE", {}, Exception("database is locked"))

    with monkeypatch.context() as m:
        m.setattr(Query, "update", _locked)
        with pytest.raises(OperationalError):
            repo.record_feedback(action_id, *_feedback_rows(now))

    assert repo.get_outcomes(590) == []
    assert repo.get_weights([590]) == {}
    assert [a.id for a in repo.get_unprocessed_done_actions()] == [action_id]

    repo.record_feedback(action_id, *_feedback_rows(now))
    assert len(repo.get_outcomes(590)) == 1
    assert repo.get_unprocessed_done_actions() == []


# ---------------------------------------------------------------------------
# DailyPlannerService.get_analytics() -- plan_history (finding 13)
# ---------------------------------------------------------------------------

def test_plan_history_returns_recent_plans(app_session, session_provider):
    from datetime import datetime, timedelta

    from eve_online_industry_tracker.infrastructure.models import BuildPlanModel

    now = datetime.now(timezone.utc).replace(tzinfo=None)
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

    now = datetime.now(timezone.utc).replace(tzinfo=None)
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

    # feedback_processor: NULL is no prediction at all, not the 30-day clamp of 1/0.01.
    assert _estimate_predicted_sell_days(item) is None
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


def test_a_decision_without_a_skip_reason_stores_null(app_session, session_provider):
    from sqlalchemy import text

    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    decision = _decision()
    assert decision.bpo_analysis_skip_reason is None
    _service(session_provider)._persist_plan_items(
        plan_id=1, decisions=[decision], market_depth_cache={}
    )
    assert app_session.query(BuildPlanItemModel).one().bpo_analysis_skip_reason is None
    stored = app_session.execute(
        text("SELECT bpo_analysis_skip_reason FROM build_plan_item")
    ).scalar_one()
    assert stored is None  # SQL NULL, not the string 'None' or ''


def test_a_legacy_row_reads_back_with_no_skip_reason_after_the_column_is_added() -> None:
    from sqlalchemy.pool import StaticPool

    engine = create_engine("sqlite://", poolclass=StaticPool)
    BaseApp.metadata.create_all(engine)
    raw = engine.raw_connection().driver_connection
    # Make the table look like it did before the column existed, with a row in it.
    raw.execute("ALTER TABLE build_plan_item DROP COLUMN bpo_analysis_skip_reason")
    raw.execute(
        "INSERT INTO build_plan_item (plan_id, type_id, decision) VALUES (1, 590, 'build')"
    )
    raw.commit()
    db = _FakeDb(raw)
    assert "bpo_analysis_skip_reason" not in {
        r[1] for r in db.query("PRAGMA table_info(build_plan_item)")
    }

    ensure_app_schema(db)

    with sessionmaker(bind=engine, future=True)() as session:
        item = session.query(BuildPlanItemModel).one()
        assert item.type_id == 590
        assert item.bpo_analysis_skip_reason is None


_INVENT_COLUMNS = (
    "invention_probability", "invention_best_probability", "invention_expected_attempts",
    "decryptor_type_id", "decryptor_type_name",
)


def test_an_invent_actions_chance_and_decryptor_survive_a_reload_on_a_legacy_table() -> None:
    """A4 fix round 1, minor 5: the assigned chance, the best inventor's, the
    expected attempts and Task 3's decryptor are persisted. ensure_app_schema
    adds the columns to a pre-existing daily_action_log (idempotent: run twice)."""
    from sqlalchemy import text

    from eve_online_industry_tracker.application.daily_planner.action_plan_builder import ActionPlanBuilder
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction

    repo, engine = _make_repo()
    legacy_cols = [c for c in DailyActionLogModel.__table__.columns if c.name not in _INVENT_COLUMNS]
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE daily_action_log"))
        conn.execute(text(
            "CREATE TABLE daily_action_log (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            + ", ".join(f"{c.name} {c.type.compile(engine.dialect)}" for c in legacy_cols if c.name != "id")
            + ")"
        ))

    class _EngineDb:
        def execute(self, query, params=None):
            with engine.begin() as conn:
                conn.execute(text(query), params or {})

        def query(self, query, params=None):
            with engine.begin() as conn:
                return conn.execute(text(query), params or {}).fetchall()

    ensure_app_schema(_EngineDb())
    ensure_app_schema(_EngineDb())

    now = _now()
    plan_id = repo.insert_plan(BuildPlanModel(created_at=now, updated_at=now, status="active"))
    invent = AssignedAction(
        type_id=12345, type_name="Widget II", action_type="invent", character_id=2, character_name="Weaker",
        quantity=None, runs=5, estimated_cost_isk=5317.56, estimated_profit_isk=None, estimated_completion=None,
        notes="chance 40.8% (assigned pilot), best inventor 52.5%",
        decryptor_type_id=34201, decryptor_type_name="Fake Decryptor",
        invention_probability=0.408, invention_best_probability=0.525, invention_expected_attempts=4.902,
    )
    repo.insert_actions(ActionPlanBuilder()._build_job_actions(plan_id, [invent], now))

    (row,) = repo.get_actions(plan_id)
    assert (row.runs, row.decryptor_type_id, row.decryptor_type_name) == (5, 34201, "Fake Decryptor")
    assert (row.invention_probability, row.invention_best_probability, row.invention_expected_attempts) == (
        0.408, 0.525, 4.902,
    )


def test_a_job_actions_install_fee_survives_a_reload_on_a_legacy_table() -> None:
    """D4 (Task 8): estimated_job_fee_isk is persisted; ensure_app_schema adds
    the column to a pre-existing daily_action_log (idempotent: run twice).
    A legacy row reads back NULL (unknown), never 0."""
    from sqlalchemy import text

    from eve_online_industry_tracker.application.daily_planner.action_plan_builder import ActionPlanBuilder
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction

    repo, engine = _make_repo()
    legacy_cols = [c for c in DailyActionLogModel.__table__.columns if c.name != "estimated_job_fee_isk"]
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE daily_action_log"))
        conn.execute(text(
            "CREATE TABLE daily_action_log (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            + ", ".join(f"{c.name} {c.type.compile(engine.dialect)}" for c in legacy_cols if c.name != "id")
            + ")"
        ))
        conn.execute(text(
            "INSERT INTO daily_action_log (plan_id, generated_at, action_type, type_id, status, processed_for_feedback) "
            "VALUES (0, '2026-01-01 00:00:00', 'manufacture', 1, 'pending', 0)"
        ))

    class _EngineDb:
        def execute(self, query, params=None):
            with engine.begin() as conn:
                conn.execute(text(query), params or {})

        def query(self, query, params=None):
            with engine.begin() as conn:
                return conn.execute(text(query), params or {}).fetchall()

    ensure_app_schema(_EngineDb())
    ensure_app_schema(_EngineDb())

    (legacy,) = repo.get_actions(0)
    assert legacy.estimated_job_fee_isk is None

    now = _now()
    plan_id = repo.insert_plan(BuildPlanModel(created_at=now, updated_at=now, status="active"))
    action = AssignedAction(
        type_id=12345, type_name="Widget", action_type="manufacture", character_id=1, character_name="Pilot",
        quantity=10, runs=10, estimated_cost_isk=500.0, estimated_profit_isk=None, estimated_completion=None,
        notes=None, estimated_job_fee_isk=25.5,
    )
    repo.insert_actions(ActionPlanBuilder()._build_job_actions(plan_id, [action], now))
    (row,) = repo.get_actions(plan_id)
    assert row.estimated_job_fee_isk == 25.5


def test_a_job_actions_unknown_fee_reason_survives_a_reload() -> None:
    """Task 8b fix round 1: the cause of an unknown fee is persisted, so
    Capital Reserved can name it (and show the refresh hint only without one)."""
    from eve_online_industry_tracker.application.daily_planner.action_plan_builder import ActionPlanBuilder
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction

    repo, _engine = _make_repo()
    now = _now()
    plan_id = repo.insert_plan(BuildPlanModel(created_at=now, updated_at=now, status="active"))
    action = AssignedAction(
        type_id=12345, type_name="Widget", action_type="manufacture", character_id=1, character_name="Pilot",
        quantity=10, runs=10, estimated_cost_isk=500.0, estimated_profit_isk=None, estimated_completion=None,
        notes=None, estimated_job_fee_isk=None, estimated_job_fee_unknown_reason="no manufacturing cost index",
    )
    repo.insert_actions(ActionPlanBuilder()._build_job_actions(plan_id, [action], now))
    (row,) = repo.get_actions(plan_id)
    assert (row.estimated_job_fee_isk, row.estimated_job_fee_unknown_reason) == (None, "no manufacturing cost index")


def test_get_analytics_reads_out_of_band_weights_through_the_band(app_session, session_provider):
    """FU-03: get_analytics reads plan_learning_weights through read_weight,
    so a pre-clamp or hand-edited row is shown clamped, not raw."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    plan = BuildPlanModel(created_at=now, updated_at=now, status="active", freshness_score=1.0)
    app_session.add(plan)
    app_session.commit()
    app_session.add(BuildPlanItemModel(plan_id=plan.id, type_id=4242, decision="build"))
    app_session.add(PlanLearningWeightsModel(
        type_id=4242, accuracy_ema=9.0, velocity_multiplier=1.0, cost_multiplier=0.1,
        sample_count=3, last_updated=now, confidence_tier="low",
    ))
    app_session.commit()

    (item,) = _service(session_provider).get_analytics()["items"]
    assert item["accuracy_ema"] == 4.0       # LEARNING_WEIGHT_MAX
    assert item["cost_multiplier"] == 0.25   # LEARNING_WEIGHT_MIN
    assert item["velocity_multiplier"] == 1.0


def test_a_compute_warns_once_for_a_corrupt_weight_row(session_provider, caplog):
    """FU-04: _run_compute is one compute for the learned-weight warnings, so
    two readers of the same corrupt row produce one WARNING."""
    import logging
    from types import SimpleNamespace

    from eve_online_industry_tracker.application.daily_planner.learning_weights import read_weight

    corrupt = SimpleNamespace(type_id=4242, velocity_multiplier=float("nan"))
    svc = _service(session_provider)

    def phase_1():
        read_weight(corrupt, "velocity_multiplier")   # e.g. the pipeline analyzer
        read_weight(corrupt, "velocity_multiplier")   # e.g. the scorer
        return {"overview_rows": []}

    svc._phase_1_collect = phase_1
    with caplog.at_level(logging.WARNING):
        svc._run_compute()
    assert sum("not a finite number" in r.getMessage() for r in caplog.records) == 1


def test_the_copy_flag_of_busy_blueprints_comes_from_the_asset_history(app_session, session_provider):
    """Fix round 1, minor 3: many live job rows have a NULL
    blueprint_is_blueprint_copy; the asset history says what the item is."""
    from eve_online_industry_tracker.infrastructure.models import (
        CharacterAssetHistoryModel,
        CorporationAssetHistoryModel,
    )

    app_session.add(CorporationAssetHistoryModel(
        corporation_id=1, item_id=501, observed_at="2026-01-01T00:00:00Z", type_id=999, is_blueprint_copy=False,
    ))
    app_session.add(CharacterAssetHistoryModel(
        character_id=2, item_id=502, observed_at="2026-01-01T00:00:00Z", type_id=999, is_blueprint_copy=True,
    ))
    app_session.add(CorporationAssetHistoryModel(
        corporation_id=1, item_id=503, observed_at="2026-01-01T00:00:00Z", type_id=999, is_blueprint_copy=None,
    ))
    app_session.commit()
    flags = _service(session_provider)._blueprint_copy_flags({501, 502, 503, 504})
    assert flags == {501: False, 502: True}


def test_a_job_with_a_null_status_holds_its_slot_and_blueprint(session_provider):
    """Fix round 3, item 3: NOT IN drops NULL rows; a NULL status is active."""
    from eve_online_industry_tracker.infrastructure.models import CorporationIndustryJobsModel

    session = session_provider.app_session()
    try:
        session.add(CorporationIndustryJobsModel(corporation_id=1, job_id=11, blueprint_item_id=601, status=None))
        session.add(CorporationIndustryJobsModel(corporation_id=1, job_id=12, blueprint_item_id=602, status="delivered"))
        session.commit()
    finally:
        session.close()
    jobs = _service(session_provider)._get_industry_jobs()
    assert sorted(int(j.blueprint_item_id) for j in jobs) == [601]
