"""A scoring or analytics read that fails is visibly
"unavailable", never a silent empty result.

Each reader in scope (margin correlation, the Tritanium 7-day trend, and the
three Analytics-tab reads) used to swallow a DB error into {} or None, so the
plan scored without that modifier and nothing said so. A failed read now logs
a WARNING with the reader name and the exception class, records a reason on
the plan (plan_summary_json -> get_active_plan's "unavailable_inputs") and on
the affected rows' decision_reason, and the UI shows a caption. A read that
succeeds with no rows is not an error and is not marked.

All numbers are fake.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application.daily_planner.service import (  # noqa: E402
    DailyPlannerService,
)
from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    BuildPlanItemModel,
    BuildPlanModel,
    MarginCorrelationCacheModel,
    MarketHistoryModel,
)
from streamlit_ui.components.daily_planner import status_bar  # noqa: E402
from streamlit_ui.components.daily_planner import tab_analytics  # noqa: E402

TYPE_ID = 12345

GOOD_ROW = {
    "type_id": TYPE_ID,
    "type_name": "Hobgoblin II",
    "quantity": 10,
    "blueprint_type_id": 999,
    "isk_per_hour": 4_000_000.0,
    "profit_amount": 1_000_000.0,
    "profit_margin_fraction": 0.2,
    "days_of_supply": 3.0,
    "price_trend_7d_pct": 0.0,
    "price_trend_30d_pct": 0.0,
    "manufacturing_job": {"runs": 20, "material_cost": 50_000_000.0},
}

MARGIN_NOTE = "margin correlation unavailable (DB error: OperationalError): scored without it"
TREND_NOTE = "Tritanium 7-day price trend unavailable (DB error: OperationalError): scored without it"


def _db_error() -> OperationalError:
    return OperationalError("SELECT 1", {}, Exception("fake db failure"))


class _FailingSession:
    """A real session whose query()/execute() raise for the listed targets."""

    def __init__(self, real, fail_models=(), fail_execute=False):
        self._real = real
        self._fail_models = tuple(fail_models)
        self._fail_execute = fail_execute

    def query(self, model, *a, **k):
        if model in self._fail_models:
            raise _db_error()
        return self._real.query(model, *a, **k)

    def execute(self, *a, **k):
        if self._fail_execute:
            raise _db_error()
        return self._real.execute(*a, **k)

    def close(self):
        self._real.close()


class _FailingProvider:
    def __init__(self, real, **fail):
        self._real = real
        self._fail = fail

    def app_session(self):
        return _FailingSession(self._real.app_session(), **self._fail)

    def sde_session(self):
        return self._real.sde_session()


def _no_settings():
    def get(section, key):
        raise KeyError(key)
    return SimpleNamespace(get=get)


def _service(session_provider, repo=None):
    return DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: [GOOD_ROW]),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        repo=repo if repo is not None else SimpleNamespace(),
        admin_settings=_no_settings(),
        session_provider=session_provider,
    )


def _pipeline():
    # Neutral on every other factor: trend 0 -> timing 1.0, 3 days -> saturation
    # 1.0, no competition data -> 1.0. The score is isk/hour x squeeze penalty.
    return SimpleNamespace(
        type_id=TYPE_ID, price_trend_7d_pct=0.0, momentum_signal=0.0,
        days_of_supply_current=3.0, competition_index=None, total_pipeline_days=0.0,
        has_active_manufacturing_jobs=False, effective_velocity=1.0,
        velocity_unknown_reason=None,
    )


def _score_and_decide(svc, margin_correlations, trit_trend, unavailable):
    phase1 = {
        "input_rows": svc._build_input_rows([GOOD_ROW]),
        "overview_rows": [GOOD_ROW],
        "weights": {},
        "market_depth_cache": {},
        "margin_correlations": margin_correlations,
        "trit_trend_7d": trit_trend,
        "unavailable_inputs": unavailable,
    }
    pipeline = _pipeline()
    (scored,) = svc._phase_3_score([pipeline], phase1)
    (decision,) = svc._phase_4_decide([scored], [pipeline], phase1)
    return scored, decision


def _sensitive():
    return SimpleNamespace(type_id=TYPE_ID, is_squeeze_sensitive=True)


def _warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


# --- margin correlation --------------------------------------------------------

def test_margin_correlation_db_error_is_unavailable_and_scored_without_it(session_provider, caplog):
    svc = _service(_FailingProvider(session_provider, fail_models=(MarginCorrelationCacheModel,)))
    with caplog.at_level(logging.WARNING):
        correlations, reason = svc._get_margin_correlations([TYPE_ID])
    assert correlations == {}
    assert reason == MARGIN_NOTE
    assert any("_get_margin_correlations" in m and "OperationalError" in m for m in _warnings(caplog))

    # With the cache readable this item is squeeze-sensitive and Tritanium is
    # rising, so the penalty would be 0.85 (3_400_000). Unavailable: no penalty.
    scored, decision = _score_and_decide(svc, correlations, 10.0, {"margin_correlation": reason})
    assert scored.mineral_squeeze_penalty == 1.0
    assert scored.adjusted_score == 4_000_000.0
    assert MARGIN_NOTE in decision.decision_reason


def test_the_squeeze_penalty_still_applies_when_margin_correlation_is_read(session_provider):
    """Control for the test above: the modifier is real when the read works."""
    svc = _service(session_provider)
    scored, decision = _score_and_decide(svc, {TYPE_ID: _sensitive()}, 10.0, {})
    assert scored.adjusted_score == pytest.approx(3_400_000.0)
    assert "unavailable" not in decision.decision_reason


def test_an_empty_margin_correlation_read_is_not_unavailable(session_provider, caplog):
    svc = _service(session_provider)
    with caplog.at_level(logging.WARNING):
        correlations, reason = svc._get_margin_correlations([TYPE_ID])
    assert correlations == {}
    assert reason is None
    assert not any("_get_margin_correlations" in m for m in _warnings(caplog))
    _, decision = _score_and_decide(svc, correlations, 10.0, {})
    assert "unavailable" not in decision.decision_reason


# --- Tritanium 7-day trend -----------------------------------------------------

def test_tritanium_trend_db_error_is_unavailable_and_scored_without_it(session_provider, caplog):
    svc = _service(_FailingProvider(session_provider, fail_models=(MarketHistoryModel,)))
    with caplog.at_level(logging.WARNING):
        trend, reason = svc._get_trit_trend_7d()
    assert trend is None
    assert reason == TREND_NOTE
    assert any("_get_trit_trend_7d" in m and "OperationalError" in m for m in _warnings(caplog))

    scored, decision = _score_and_decide(
        svc, {TYPE_ID: _sensitive()}, trend, {"tritanium_trend": reason},
    )
    assert scored.mineral_squeeze_penalty == 1.0
    assert scored.adjusted_score == 4_000_000.0
    assert TREND_NOTE in decision.decision_reason


def test_a_failed_trend_does_not_mark_a_row_it_cannot_affect(session_provider):
    """Only a squeeze-sensitive row reads the trend; the plan still records it."""
    svc = _service(session_provider)
    insensitive = SimpleNamespace(type_id=TYPE_ID, is_squeeze_sensitive=False)
    _, decision = _score_and_decide(svc, {TYPE_ID: insensitive}, None, {"tritanium_trend": TREND_NOTE})
    assert "unavailable" not in decision.decision_reason


def test_an_empty_tritanium_history_read_is_not_unavailable(session_provider, caplog):
    svc = _service(session_provider)
    with caplog.at_level(logging.WARNING):
        trend, reason = svc._get_trit_trend_7d()
    assert trend is None
    assert reason is None
    assert not any("_get_trit_trend_7d" in m for m in _warnings(caplog))


# --- the plan records it, get_active_plan returns it ---------------------------

def test_phase_9_records_the_unavailable_inputs_on_the_plan(planner_repo, session_provider):
    svc = _service(session_provider, repo=planner_repo)
    svc._phase_9_persist(
        phase1_data={
            "market_depth_cache": {}, "overview_rows": [], "corp_wallet": None,
            "unavailable_inputs": {"margin_correlation": MARGIN_NOTE},
        },
        chain_plan=SimpleNamespace(decisions=[]),
        action_log_rows=[],
    )
    plan = planner_repo.get_active_plan()
    assert json.loads(plan.plan_summary_json) == {"unavailable_inputs": {"margin_correlation": MARGIN_NOTE}}
    assert svc.get_active_plan()["unavailable_inputs"] == {"margin_correlation": MARGIN_NOTE}


def test_a_plan_with_every_input_read_has_no_unavailable_inputs(planner_repo, session_provider):
    svc = _service(session_provider, repo=planner_repo)
    svc._phase_9_persist(
        phase1_data={"market_depth_cache": {}, "overview_rows": [], "corp_wallet": None,
                     "unavailable_inputs": {}},
        chain_plan=SimpleNamespace(decisions=[]),
        action_log_rows=[],
    )
    assert planner_repo.get_active_plan().plan_summary_json is None
    assert svc.get_active_plan()["unavailable_inputs"] == {}


def test_a_full_compute_records_both_failed_reads_on_the_plan_and_the_row(
    session_provider, planner_repo, tmp_path,
):
    from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager

    provider = _FailingProvider(
        session_provider, fail_models=(MarginCorrelationCacheModel, MarketHistoryModel),
    )
    svc = _service(provider, repo=planner_repo)
    svc._admin = AdminSettingsManager(file_path=str(tmp_path / "admin.json"))
    svc._run_compute()
    assert svc._status == "done", svc._error

    plan = planner_repo.get_active_plan()
    assert json.loads(plan.plan_summary_json)["unavailable_inputs"] == {
        "margin_correlation": MARGIN_NOTE, "tritanium_trend": TREND_NOTE,
    }
    (item,) = [i for i in planner_repo.get_plan_items(int(plan.id)) if i.type_id == TYPE_ID]
    # Sensitivity is unknown without the correlation cache, so the trend is
    # missing for this row too.
    assert MARGIN_NOTE in item.decision_reason
    assert TREND_NOTE in item.decision_reason


def test_an_unreadable_plan_summary_is_shown_as_unknown_not_as_none(caplog):
    from eve_online_industry_tracker.application.daily_planner.service import _plan_unavailable_inputs
    with caplog.at_level(logging.WARNING):
        found = _plan_unavailable_inputs(SimpleNamespace(id=1, plan_summary_json="{not json"))
    assert found == {"plan_summary": "plan summary unreadable: its unavailable inputs are unknown"}
    assert any("plan_summary_json" in m for m in _warnings(caplog))


# --- analytics -----------------------------------------------------------------

def _analytics_service(planner_repo, session_provider, provider):
    plan_id = planner_repo.insert_plan(BuildPlanModel(
        created_at=datetime(2026, 1, 1), updated_at=datetime(2026, 1, 1),
        status="active", freshness_score=1.0,
    ))
    planner_repo.insert_plan_items([BuildPlanItemModel(
        plan_id=plan_id, type_id=TYPE_ID, type_name="Hobgoblin II", decision="build",
    )])
    return _service(provider, repo=planner_repo)


def test_analytics_db_errors_are_reported_unavailable(planner_repo, session_provider, caplog, monkeypatch):
    provider = _FailingProvider(
        session_provider, fail_models=(MarginCorrelationCacheModel,), fail_execute=True,
    )
    svc = _analytics_service(planner_repo, session_provider, provider)

    def _fail(*a, **k):
        raise _db_error()
    monkeypatch.setattr(planner_repo, "get_market_depth", _fail)

    with caplog.at_level(logging.WARNING):
        result = svc.get_analytics()
    assert result["unavailable"] == {
        "market_depth": "market depth unavailable (DB error: OperationalError): competition data not shown",
        "margin_correlation": (
            "margin correlation unavailable (DB error: OperationalError): correlation data not shown"
        ),
        "invention_details": (
            "invention details unavailable (DB error: OperationalError): invention data not shown"
        ),
    }
    messages = _warnings(caplog)
    for reader in ("market_depth", "margin_correlation", "invention_details"):
        assert any(f"get_analytics/{reader}" in m and "OperationalError" in m for m in messages)
    assert len(result["items"]) == 1


def test_analytics_empty_reads_are_not_unavailable(planner_repo, session_provider):
    svc = _analytics_service(planner_repo, session_provider, session_provider)
    result = svc.get_analytics()
    assert result["unavailable"] == {}


# --- UI captions (pure helpers) ------------------------------------------------

def test_status_bar_caption_lists_the_unavailable_inputs():
    caption = status_bar.unavailable_inputs_caption(
        {"unavailable_inputs": {"margin_correlation": MARGIN_NOTE, "tritanium_trend": TREND_NOTE}},
    )
    assert caption is not None
    assert MARGIN_NOTE in caption and TREND_NOTE in caption


@pytest.mark.parametrize("plan_data", [None, {}, {"unavailable_inputs": {}}, {"unavailable_inputs": None}])
def test_status_bar_caption_is_absent_without_a_status(plan_data):
    assert status_bar.unavailable_inputs_caption(plan_data) is None


def test_analytics_caption_lists_the_unavailable_reads():
    caption = tab_analytics.analytics_unavailable_caption(
        {"unavailable": {"market_depth": "market depth unavailable (DB error: X): competition data not shown"}},
    )
    assert caption is not None
    assert "market depth unavailable (DB error: X)" in caption


@pytest.mark.parametrize("analytics", [None, {}, {"unavailable": {}}])
def test_analytics_caption_is_absent_without_a_status(analytics):
    assert tab_analytics.analytics_unavailable_caption(analytics) is None



_SECTION_KEYS = ("market_depth", "margin_correlation", "invention_details")


@pytest.mark.parametrize("key", _SECTION_KEYS)
def test_an_empty_section_shows_the_failed_read_not_a_false_cause(key):
    """A failed read must not show "no data yet, run X first" as its cause."""
    reason = f"{key} unavailable (DB error: X): data not shown"
    message, unavailable = tab_analytics.section_empty_message(
        {"unavailable": {key: reason}}, key, "No data yet.",
    )
    assert (message, unavailable) == (reason, True)


@pytest.mark.parametrize("key", _SECTION_KEYS)
@pytest.mark.parametrize("analytics", [None, {}, {"unavailable": {}}, {"unavailable": {"other": "x"}}])
def test_an_empty_section_without_a_failed_read_shows_the_default(key, analytics):
    assert tab_analytics.section_empty_message(analytics, key, "No data yet.") == ("No data yet.", False)


def test_the_status_bar_caption_says_it_is_a_compute_time_snapshot():
    """The status is the plan's, recorded when it was computed."""
    caption = status_bar.unavailable_inputs_caption({"unavailable_inputs": {"margin_correlation": MARGIN_NOTE}})
    assert caption == f"When this plan was computed, some scoring inputs could not be read: {MARGIN_NOTE}"


def test_an_empty_decision_reason_gets_no_leading_separator(session_provider, monkeypatch):
    """The notes are joined without an empty first part."""
    svc = _service(session_provider)
    real_decide = svc._decision_engine.decide

    def decide(**kwargs):
        decision = real_decide(**kwargs)
        decision.decision_reason = ""
        return decision
    monkeypatch.setattr(svc._decision_engine, "decide", decide)
    _, decision = _score_and_decide(svc, {}, None, {"margin_correlation": MARGIN_NOTE})
    assert decision.decision_reason == MARGIN_NOTE
