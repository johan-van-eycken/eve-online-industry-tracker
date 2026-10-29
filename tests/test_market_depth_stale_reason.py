"""market_depth_cache.stale_reason is a model column and a schema
migration (no longer an ALTER inside the collector), and a stale row carries
its reason wherever market depth is read: the daily planner's decision reason
and plan-level unavailable inputs, and the Analytics tab. Fake values."""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.service import (  # noqa: E402
    _market_depth_stale_inputs,
    _market_depth_stale_note,
)
from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import (  # noqa: E402
    MarketDepthCollector,
)
from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    BuildPlanItemModel,
    BuildPlanModel,
    MarketDepthCacheModel,
)
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_app_schema  # noqa: E402
from streamlit_ui.components.daily_planner import tab_analytics  # noqa: E402

TYPE_ID = 12345
SNAPSHOT = datetime(2026, 10, 1, 12, 0, 0)
REASON = "ESI sell order book unavailable: fake 503"


class _FakeDb:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, query: str, params=None) -> None:
        self._conn.execute(query, params) if params else self._conn.execute(query)
        self._conn.commit()

    def query(self, query: str, params=None) -> list:
        return (self._conn.execute(query, params) if params else self._conn.execute(query)).fetchall()


def _legacy_db() -> tuple[sqlite3.Connection, _FakeDb]:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE market_history (id INTEGER PRIMARY KEY AUTOINCREMENT, type_id INTEGER NOT NULL,"
        "region_id INTEGER NOT NULL, date TEXT NOT NULL, close REAL NOT NULL, high REAL, low REAL,"
        "volume INTEGER NOT NULL, order_count INTEGER NOT NULL, fetched_at DATETIME, updated_at DATETIME,"
        "UNIQUE(type_id, region_id, date))"
    )
    return conn, _FakeDb(conn)


# --- the column ------------------------------------------------------------------------------

def test_the_model_has_a_stale_reason_column():
    column = MarketDepthCacheModel.__table__.columns["stale_reason"]
    assert column.nullable


def test_the_schema_migration_adds_stale_reason_to_a_legacy_table():
    conn, db = _legacy_db()
    conn.execute(
        "CREATE TABLE market_depth_cache (id INTEGER PRIMARY KEY AUTOINCREMENT, type_id INTEGER NOT NULL,"
        "hub TEXT NOT NULL, competitor_units INTEGER NULL, vwap_5d REAL NULL, spot_sell_price REAL NULL,"
        "competition_index REAL NULL, snapshot_at DATETIME NOT NULL, UNIQUE(type_id, hub))"
    )
    conn.commit()
    ensure_app_schema(db)
    ensure_app_schema(db)  # idempotent
    assert "stale_reason" in {row[1] for row in db.query("PRAGMA table_info(market_depth_cache)")}


def test_a_new_market_depth_table_has_stale_reason():
    _conn, db = _legacy_db()
    ensure_app_schema(db)
    assert "stale_reason" in {row[1] for row in db.query("PRAGMA table_info(market_depth_cache)")}


def test_the_collector_no_longer_alters_the_table_itself():
    assert not hasattr(MarketDepthCollector, "_ensure_stale_reason_column")


def test_the_repo_reads_the_stale_reason(planner_repo, app_session):
    app_session.add(MarketDepthCacheModel(type_id=TYPE_ID, hub="jita", competitor_units=5, spot_sell_price=10.0,
                                          snapshot_at=SNAPSHOT, stale_reason=REASON))
    app_session.commit()
    assert planner_repo.get_market_depth([TYPE_ID], "jita")[TYPE_ID].stale_reason == REASON


def test_the_repo_upsert_keeps_a_given_stale_reason(planner_repo):
    planner_repo.upsert_market_depth([MarketDepthCacheModel(
        type_id=TYPE_ID, hub="jita", competitor_units=5, snapshot_at=SNAPSHOT, stale_reason=REASON)])
    assert planner_repo.get_market_depth([TYPE_ID], "jita")[TYPE_ID].stale_reason == REASON


# --- the daily planner -------------------------------------------------------------------------

def _entry(stale_reason=REASON, **values):
    base = dict(type_id=TYPE_ID, competitor_units=5, vwap_5d=10.0, spot_sell_price=10.0,
                competition_index=None, snapshot_at=SNAPSHOT, stale_reason=stale_reason)
    base.update(values)
    return SimpleNamespace(**base)


def test_a_fresh_market_depth_row_has_no_note():
    assert _market_depth_stale_note(_entry(stale_reason=None)) is None
    assert _market_depth_stale_note(None) is None
    assert _market_depth_stale_note({"spot_sell_price": 1.0}) is None


def test_a_stale_market_depth_row_names_its_reason_and_snapshot():
    note = _market_depth_stale_note(_entry())
    assert REASON in note and "2026-10-01" in note and "stale" in note


def test_a_stale_dict_entry_is_read_too():
    note = _market_depth_stale_note({"stale_reason": REASON, "snapshot_at": "2026-10-01T12:00:00"})
    assert REASON in note


def test_the_plan_level_stale_input_lists_the_stale_types():
    found = _market_depth_stale_inputs({TYPE_ID: _entry(), 7: _entry(stale_reason=None)})
    assert set(found) == {"market_depth_stale"}
    assert str(TYPE_ID) in found["market_depth_stale"] and REASON in found["market_depth_stale"]
    assert "7" not in found["market_depth_stale"].replace(str(TYPE_ID), "")
    assert _market_depth_stale_inputs({7: _entry(stale_reason=None)}) == {}


def test_a_stale_row_carries_its_reason_into_the_decision_reason(session_provider):
    from tests.test_daily_planner_unavailable_inputs import GOOD_ROW, _pipeline, _service

    svc = _service(session_provider)
    phase1 = {
        "input_rows": svc._build_input_rows([GOOD_ROW]),
        "overview_rows": [GOOD_ROW],
        "weights": {},
        "market_depth_cache": {TYPE_ID: _entry()},
        "margin_correlations": {},
        "trit_trend_7d": None,
        "unavailable_inputs": {},
    }
    pipeline = _pipeline()
    (scored,) = svc._phase_3_score([pipeline], phase1)
    (decision,) = svc._phase_4_decide([scored], [pipeline], phase1)
    assert REASON in decision.decision_reason


def test_a_fresh_row_adds_nothing_to_the_decision_reason(session_provider):
    from tests.test_daily_planner_unavailable_inputs import GOOD_ROW, _pipeline, _service

    svc = _service(session_provider)
    phase1 = {
        "input_rows": svc._build_input_rows([GOOD_ROW]),
        "overview_rows": [GOOD_ROW],
        "weights": {},
        "market_depth_cache": {TYPE_ID: _entry(stale_reason=None)},
        "margin_correlations": {},
        "trit_trend_7d": None,
        "unavailable_inputs": {},
    }
    pipeline = _pipeline()
    (scored,) = svc._phase_3_score([pipeline], phase1)
    (decision,) = svc._phase_4_decide([scored], [pipeline], phase1)
    assert "stale" not in (decision.decision_reason or "")


# --- the Analytics tab ------------------------------------------------------------------------

def _analytics(planner_repo, session_provider, app_session, **depth):
    from tests.test_daily_planner_unavailable_inputs import _service

    plan_id = planner_repo.insert_plan(BuildPlanModel(
        created_at=datetime(2026, 1, 1), updated_at=datetime(2026, 1, 1), status="active", freshness_score=1.0,
    ))
    planner_repo.insert_plan_items([BuildPlanItemModel(
        plan_id=plan_id, type_id=TYPE_ID, type_name="Hobgoblin II", decision="build",
    )])
    app_session.add(MarketDepthCacheModel(type_id=TYPE_ID, hub="jita", snapshot_at=SNAPSHOT, **depth))
    app_session.commit()
    return _service(session_provider, repo=planner_repo).get_analytics()


def test_analytics_items_carry_the_stale_reason(planner_repo, session_provider, app_session):
    result = _analytics(planner_repo, session_provider, app_session, competitor_units=5,
                        competition_index=0.5, stale_reason=REASON)
    (item,) = result["items"]
    assert item["market_stale_reason"] == REASON


def test_analytics_keeps_an_unknown_competition_index_unknown(planner_repo, session_provider, app_session):
    result = _analytics(planner_repo, session_provider, app_session, competitor_units=5, competition_index=None)
    (item,) = result["items"]
    assert item["competition_index"] is None   # not a silent 0.0
    assert item["competitor_units"] == 5
    assert item["market_stale_reason"] is None


def test_the_analytics_stale_caption_names_each_stale_item():
    caption = tab_analytics.market_depth_stale_caption([
        {"type_name": "Hobgoblin II", "market_stale_reason": REASON, "market_snapshot_at": "2026-10-01 12:00:00"},
        {"type_name": "Fresh", "market_stale_reason": None},
    ])
    assert "Hobgoblin II" in caption and REASON in caption and "Fresh" not in caption


def test_no_stale_caption_without_a_stale_item():
    assert tab_analytics.market_depth_stale_caption([{"type_name": "Fresh", "market_stale_reason": None}]) is None
    assert tab_analytics.market_depth_stale_caption([]) is None


def test_the_competition_rows_show_an_unknown_index_as_unknown():
    rows = tab_analytics.competition_rows([
        {"type_name": "A", "competition_index": None, "competitor_units": 5, "market_snapshot_at": "x"},
        {"type_name": "B", "competition_index": 3.0, "competitor_units": 9, "market_snapshot_at": "y",
         "market_stale_reason": REASON},
        {"type_name": "C", "competition_index": None, "competitor_units": None},   # no market depth row
    ])
    assert [r["Item"] for r in rows] == ["A", "B"]
    assert "unknown" in rows[0]["Competition Index"] and rows[0]["Supply Days (est.)"] == "unknown"
    assert rows[1]["Competition Index"].endswith("3.00")
    assert REASON in rows[1]["Market Last Updated"]


def test_logging_is_quiet_for_fresh_rows(caplog):
    with caplog.at_level(logging.WARNING):
        _market_depth_stale_inputs({TYPE_ID: _entry(stale_reason=None)})
    assert not caplog.records
