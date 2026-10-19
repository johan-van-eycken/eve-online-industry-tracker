"""MarketIntelligenceJob: the market depth collector and the margin
correlator run in their own try, so a collector failure (e.g. an invalid
daily_planner/planner_market_hub) does not skip the correlator; each
failure is logged and named in last_error. /planner/market_intel/status
answers in the API envelope the planner page reads, and the Daily Planner
status bar shows last_error. Fake values throughout."""
from __future__ import annotations

import threading
from types import SimpleNamespace

from eve_online_industry_tracker.application.market_intelligence.job import MarketIntelligenceJob


def _job():
    job = MarketIntelligenceJob.__new__(MarketIntelligenceJob)
    job._state = SimpleNamespace(admin_settings=None)
    job._lock = threading.Lock()
    job._running = False
    job._last_completed_at = None
    job._last_error = None
    return job


def _patch(monkeypatch, *, collector_error=None, correlator_error=None):
    ran = []
    from eve_online_industry_tracker.application.market_intelligence import market_depth_collector, margin_correlator

    class _Collector:
        def __init__(self, **kwargs):
            pass

        def run(self):
            ran.append("collector")
            if collector_error:
                raise collector_error

    class _Correlator:
        def __init__(self, **kwargs):
            pass

        def run_if_due(self):
            ran.append("correlator")
            if correlator_error:
                raise correlator_error

    monkeypatch.setattr(market_depth_collector, "MarketDepthCollector", _Collector)
    monkeypatch.setattr(margin_correlator, "MarginCorrelator", _Correlator)
    return ran


def test_a_collector_failure_does_not_skip_the_correlator(monkeypatch):
    ran = _patch(monkeypatch, collector_error=ValueError("daily_planner/planner_market_hub='amarr' is invalid"))
    job = _job()
    job._run_cycle_safe()
    assert ran == ["collector", "correlator"]
    status = job.get_status()
    assert "market depth" in status["last_error"] and "planner_market_hub" in status["last_error"]
    assert status["last_completed_at"] is None                    # the market data was not refreshed


def test_a_correlator_failure_is_named_and_the_market_data_counts_as_refreshed(monkeypatch):
    _patch(monkeypatch, correlator_error=RuntimeError("fake correlator down"))
    job = _job()
    job._run_cycle_safe()
    status = job.get_status()
    assert "margin correlation" in status["last_error"] and "fake correlator down" in status["last_error"]
    assert status["last_completed_at"] is not None


def test_both_failures_are_named_and_a_clean_cycle_clears_them(monkeypatch):
    _patch(monkeypatch, collector_error=ValueError("a"), correlator_error=RuntimeError("b"))
    job = _job()
    job._run_cycle_safe()
    assert "market depth" in job.get_status()["last_error"] and "margin correlation" in job.get_status()["last_error"]
    _patch(monkeypatch)
    job._run_cycle_safe()
    assert job.get_status()["last_error"] is None and job.get_status()["last_completed_at"] is not None


def test_the_status_route_answers_in_the_api_envelope():
    from flask_app.app import create_app

    app = create_app()
    job = _job()
    job._last_error = "market depth collector: fake failure"
    app.extensions["app_state"] = SimpleNamespace(_market_intelligence_job=job, admin_settings=None, db_app=None,
                                                  db_sde=None, db_oauth=None)
    body = app.test_client().get("/planner/market_intel/status").get_json()
    assert body["status"] == "success"
    assert body["data"]["last_error"] == "market depth collector: fake failure"
    app.extensions["app_state"] = SimpleNamespace(admin_settings=None, db_app=None, db_sde=None, db_oauth=None)
    body = app.test_client().get("/planner/market_intel/status").get_json()
    assert body["status"] == "success" and body["data"]["last_error"] is None


def test_the_status_bar_shows_the_last_error(monkeypatch):
    from streamlit_ui.components.daily_planner import status_bar

    assert status_bar.market_intel_warning({}) is None
    assert status_bar.market_intel_warning({"last_error": None}) is None
    warning = status_bar.market_intel_warning({"last_error": "market depth collector: fake failure"})
    assert "fake failure" in warning and "Market data refresh failed" in warning


def test_the_rendered_status_bar_warns(monkeypatch):
    import tests.test_daily_planner_status_bar_freshness as freshness
    from streamlit_ui.components.daily_planner import status_bar
    from streamlit_ui.state.daily_planner_page import DailyPlannerPageState

    fake = freshness._FakeSt()
    plan = freshness._plan(1.0)
    monkeypatch.setattr(status_bar, "st", fake)
    monkeypatch.setattr(status_bar, "get_status", lambda: {"status": "idle"})
    monkeypatch.setattr(status_bar, "get_plan", lambda: plan)
    monkeypatch.setattr(status_bar, "get_market_intel_status",
                        lambda: {"last_completed_at": None, "last_error": "market depth collector: fake failure"})
    status_bar.render_status_bar(DailyPlannerPageState(plan=plan))
    assert any("fake failure" in w for w in fake.warnings)



def test_the_status_bar_warns_without_a_plan_too(monkeypatch):
    import tests.test_daily_planner_status_bar_freshness as freshness
    from streamlit_ui.components.daily_planner import status_bar
    from streamlit_ui.state.daily_planner_page import DailyPlannerPageState

    fake = freshness._FakeSt()
    monkeypatch.setattr(status_bar, "st", fake)
    monkeypatch.setattr(status_bar, "get_status", lambda: {"status": "idle"})
    monkeypatch.setattr(status_bar, "get_plan", lambda: None)
    monkeypatch.setattr(status_bar, "get_market_intel_status", lambda: {"last_error": "fake failure"})
    status_bar.render_status_bar(DailyPlannerPageState(plan=None))
    assert any("fake failure" in w for w in fake.warnings)
