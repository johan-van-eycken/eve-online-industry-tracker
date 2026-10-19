"""Final round F3, Flask side: strict numeric request values, the request
session teardown, and the industry profile context route."""
from __future__ import annotations

import math

import pytest

from eve_online_industry_tracker.application.errors import ServiceError


# --- _number_param edge cases ----------------------------------------------------------------


@pytest.mark.parametrize("raw, cast, expected", [
    ("3", int, 3),
    (" 3 ", int, 3),
    (3, int, 3),
    (3.0, int, 3),
    ("2.5", float, 2.5),
    (" 2.5\t", float, 2.5),
    (0, float, 0.0),
    (0, int, 0),
])
def test_number_param_parses_valid_values(raw, cast, expected):
    from flask_app.routes.industry import _number_param

    assert _number_param({"x": raw}, "x", 7, cast) == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "\t"])
def test_number_param_uses_the_default_for_absent_or_blank_values(raw):
    from flask_app.routes.industry import _number_param

    assert _number_param({"x": raw}, "x", 7, int) == 7
    assert _number_param({}, "x", 24.0, float) == 24.0


@pytest.mark.parametrize("raw, cast", [
    ("inf", float),
    ("-inf", float),
    ("nan", float),
    (math.inf, float),
    (math.inf, int),          # int(inf) raised OverflowError, which was not caught
    ("1e400", float),
    ("2.5", int),
    (2.5, int),               # silently truncated to 2 before
    ("abc", int),
    ([], int),
])
def test_number_param_rejects_invalid_values_with_a_400(raw, cast):
    from flask_app.routes.industry import _number_param

    with pytest.raises(ServiceError) as excinfo:
        _number_param({"x": raw}, "x", 0, cast)
    assert excinfo.value.status_code == 400
    assert "x" in excinfo.value.message


def test_an_explicit_zero_is_kept_not_replaced_by_the_default():
    from flask_app.routes.industry import _number_param

    # `raw or default` turned an explicit 0 into the default.
    assert _number_param({"x": 0}, "x", 24.0, float) == 0.0
    assert _number_param({"x": "0"}, "x", 5, int) == 0


def test_candidate_override_rejects_infinity_with_a_400():
    from flask_app.routes.industry import _candidate_directives_payload

    with pytest.raises(ServiceError) as excinfo:
        _candidate_directives_payload([{"overview_row_id": "r1", "target_units_override": math.inf}])
    assert excinfo.value.status_code == 400
    assert "target_units_override" in excinfo.value.message


def test_candidate_override_rejects_a_fractional_value():
    from flask_app.routes.industry import _candidate_directives_payload

    with pytest.raises(ServiceError) as excinfo:
        _candidate_directives_payload([{"overview_row_id": "r1", "max_batches_override": 2.5}])
    assert excinfo.value.status_code == 400


# --- planning_horizon_hours=0 ----------------------------------------------------------------


@pytest.mark.parametrize("value", [0, "0", -5, 0.0])
def test_portfolio_plan_request_rejects_a_non_positive_planning_horizon(value):
    from flask_app.routes.industry import _portfolio_plan_request_from_payload

    with pytest.raises(ServiceError) as excinfo:
        _portfolio_plan_request_from_payload({"planning_horizon_hours": value})
    assert excinfo.value.status_code == 400
    assert "planning_horizon_hours" in excinfo.value.message


def test_portfolio_plan_request_keeps_the_default_horizon_when_absent():
    from flask_app.routes.industry import _portfolio_plan_request_from_payload

    assert _portfolio_plan_request_from_payload({}).planning_horizon_hours == 24.0
    assert _portfolio_plan_request_from_payload({"planning_horizon_hours": 48}).planning_horizon_hours == 48.0


@pytest.fixture
def industry_client(monkeypatch):
    from flask_app.app import create_app
    import flask_app.routes.industry as routes

    monkeypatch.setattr(routes, "require_ready", lambda s: None)
    monkeypatch.setattr(routes, "require_sde_ready", lambda s: None)
    return create_app().test_client()


def test_portfolio_candidates_route_returns_400_for_a_zero_planning_horizon(industry_client):
    resp = industry_client.get("/industry_products/1/portfolio_candidates?planning_horizon_hours=0")
    assert resp.status_code == 400
    assert "planning_horizon_hours" in resp.get_json()["message"]


def test_portfolio_candidates_start_returns_400_for_a_zero_planning_horizon(industry_client):
    resp = industry_client.post("/industry_products/1/portfolio_candidates/start", json={"planning_horizon_hours": 0})
    assert resp.status_code == 400
    assert "planning_horizon_hours" in resp.get_json()["message"]


def test_portfolio_plan_route_returns_400_for_a_zero_planning_horizon(industry_client):
    resp = industry_client.post("/industry_products/1/portfolio_plan", json={"planning_horizon_hours": 0})
    assert resp.status_code == 400
    assert "planning_horizon_hours" in resp.get_json()["message"]


def test_portfolio_candidates_route_returns_400_for_an_infinite_capital_limit(industry_client):
    resp = industry_client.post("/industry_products/1/portfolio_plan",
                                data='{"capital_limit_isk": Infinity}', content_type="application/json")
    assert resp.status_code == 400
    assert "capital_limit_isk" in resp.get_json()["message"]


# --- the industry profile context route ------------------------------------------------------


def test_industry_profile_context_route_returns_the_resolved_context(industry_client, monkeypatch):
    import flask_app.routes.industry as routes

    calls = []

    class _FakeService:
        def __init__(self, state):
            pass

        def _resolve_industry_profile_context(self, *, character_id, industry_profile_id):
            calls.append((character_id, industry_profile_id))
            return {"id": industry_profile_id, "cost_index_unknown_reason": "no manufacturing index",
                    "industry_system": {"lookup_error": "ESI down"}}

    monkeypatch.setattr(routes, "IndustryService", _FakeService)
    body = industry_client.get("/industry_profile_context/7").get_json()
    assert body["status"] == "success"
    assert body["data"]["cost_index_unknown_reason"] == "no manufacturing index"
    assert body["data"]["industry_system"]["lookup_error"] == "ESI down"
    assert calls == [(None, 7)]


def test_industry_profile_context_route_passes_a_service_error_on(industry_client, monkeypatch):
    import flask_app.routes.industry as routes

    class _FakeService:
        def __init__(self, state):
            pass

        def _resolve_industry_profile_context(self, *, character_id, industry_profile_id):
            raise ServiceError("Industry profile ID 7 not found", status_code=400)

    monkeypatch.setattr(routes, "IndustryService", _FakeService)
    resp = industry_client.get("/industry_profile_context/7")
    assert resp.status_code == 400
    assert "not found" in resp.get_json()["message"]


def test_industry_profile_context_route_answers_404_for_no_profile(industry_client, monkeypatch):
    import flask_app.routes.industry as routes

    class _FakeService:
        def __init__(self, state):
            pass

        def _resolve_industry_profile_context(self, *, character_id, industry_profile_id):
            return None

    monkeypatch.setattr(routes, "IndustryService", _FakeService)
    resp = industry_client.get("/industry_profile_context/7")
    assert resp.status_code == 404


# --- flask_app/db.py teardown ----------------------------------------------------------------


def test_teardown_drops_the_session_even_when_close_raises():
    from flask import g

    from flask_app.app import create_app
    from flask_app.db import close_request_sessions

    class _Broken:
        def close(self):
            raise RuntimeError("close blew up")

    class _Fine:
        closed = False

        def close(self):
            _Fine.closed = True

    app = create_app()
    with app.app_context():
        g._db_app_session = _Broken()
        g._db_sde_session = _Fine()
        with pytest.raises(RuntimeError, match="close blew up"):
            close_request_sessions()
        assert getattr(g, "_db_app_session", None) is None
        assert getattr(g, "_db_sde_session", None) is None
        assert _Fine.closed


def test_teardown_logs_a_failing_sqlalchemy_close(caplog):
    import logging

    from flask import g
    from sqlalchemy.exc import OperationalError

    from flask_app.app import create_app
    from flask_app.db import close_request_sessions

    class _Broken:
        def close(self):
            raise OperationalError("close", {}, Exception("db gone"))

    app = create_app()
    with app.app_context(), caplog.at_level(logging.WARNING):
        g._db_oauth_session = _Broken()
        close_request_sessions()
        assert getattr(g, "_db_oauth_session", None) is None
    assert any("session" in r.getMessage().lower() for r in caplog.records)
