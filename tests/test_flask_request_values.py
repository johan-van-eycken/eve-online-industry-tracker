"""Strict numeric request values (the admin and character/corporation id
query values included), the request session teardown, and the industry
profile context route."""
from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.errors import ServiceError
import flask_app.routes.admin as admin
from flask_app.app import create_app
import flask_app.routes.characters as character_routes
import flask_app.routes.corporations as corporation_routes
import flask_app.routes.industry as industry_routes
from fakes import industry_client


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

        def industry_profile_context(self, profile_id):
            calls.append(profile_id)
            return {"id": profile_id, "cost_index_unknown_reason": "no manufacturing index",
                    "industry_system": {"lookup_error": "ESI down"}}

    monkeypatch.setattr(routes, "IndustryService", _FakeService)
    body = industry_client.get("/industry_profile_context/7").get_json()
    assert body["status"] == "success"
    assert body["data"]["cost_index_unknown_reason"] == "no manufacturing index"
    assert body["data"]["industry_system"]["lookup_error"] == "ESI down"
    assert calls == [7]


def test_industry_profile_context_route_passes_a_service_error_on(industry_client, monkeypatch):
    import flask_app.routes.industry as routes

    class _FakeService:
        def __init__(self, state):
            pass

        def industry_profile_context(self, profile_id):
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

        def industry_profile_context(self, profile_id):
            return None

    monkeypatch.setattr(routes, "IndustryService", _FakeService)
    resp = industry_client.get("/industry_profile_context/7")
    assert resp.status_code == 404


def test_the_public_industry_profile_context_wraps_the_resolver():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    svc = IndustryService.__new__(IndustryService)
    calls = []

    def _resolve(*, character_id, industry_profile_id):
        calls.append((character_id, industry_profile_id))
        return {"id": industry_profile_id}

    svc._resolve_industry_profile_context = _resolve
    assert svc.industry_profile_context(7) == {"id": 7}
    assert calls == [(None, 7)]


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


# --- non-numeric admin query values ------------------------------------------------------------
#
# A non-numeric admin query value is a 400 naming the parameter
# and the value, not a 500 from int()/float(). Covers every numeric argument
# of the admin routes (the public structures status, refresh and scan start,
# and the ESI metrics). Fake values only.

@pytest.fixture
def client(monkeypatch):
    class _NoService:
        def __init__(self, *a, **k):
            pass

        def __getattr__(self, name):
            raise AssertionError(f"{name}: the service must not be reached with an invalid argument")

    monkeypatch.setattr(admin, "require_ready", lambda state: None)
    monkeypatch.setattr(admin, "get_state", lambda: SimpleNamespace())
    monkeypatch.setattr(admin, "AdminService", _NoService)
    monkeypatch.setattr(admin, "get_esi_monitor", lambda: _NoService())
    return create_app().test_client()


_CASES = [
    ("get", "/public_structures_status", "system_id", {}),
    ("post", "/refresh_public_structures", "system_id", {}),
    ("post", "/refresh_public_structures", "scan", {"system_id": "30000142"}),
    ("post", "/refresh_public_structures", "workers", {"system_id": "30000142"}),
    ("post", "/refresh_public_structures", "max_results", {"system_id": "30000142"}),
    ("post", "/refresh_public_structures", "time_budget", {"system_id": "30000142"}),
    ("post", "/public_structures_scan/start", "scan", {}),
    ("post", "/public_structures_scan/start", "workers", {}),
    ("post", "/public_structures_scan/start", "batch_size", {}),
    ("post", "/public_structures_scan/start", "time_budget", {}),
    ("post", "/public_structures_scan/start", "pause", {}),
    ("post", "/public_structures_scan/start", "request_timeout_seconds", {}),
    ("get", "/esi_metrics", "window", {}),
    ("get", "/esi_metrics", "bucket", {}),
    ("get", "/esi_metrics", "top", {}),
]


@pytest.mark.parametrize("method,path,param,extra", _CASES)
@pytest.mark.parametrize("value", ["abc", "1e", "nan"])
def test_a_non_numeric_value_is_a_400_naming_the_parameter(client, method, path, param, extra, value):
    response = getattr(client, method)(path, query_string={**extra, param: value})
    assert response.status_code == 400, response.get_json()
    message = response.get_json()["message"]
    assert param in message and repr(value) in message


@pytest.mark.parametrize("raw,default,expected", [(None, 5, 5), ("", 5, 5), (" 7 ", 5, 7)])
def test_the_int_arg(raw, default, expected):
    query = {} if raw is None else {"scan": raw}
    with create_app().test_request_context("/", query_string=query):
        assert admin._int_arg("scan", default) == expected


@pytest.mark.parametrize("raw,expected", [(None, 8.0), ("2.5", 2.5), ("3", 3.0)])
def test_the_float_arg(raw, expected):
    query = {} if raw is None else {"time_budget": raw}
    with create_app().test_request_context("/", query_string=query):
        assert admin._float_arg("time_budget", 8.0) == expected


def test_a_fractional_int_is_a_400():
    from eve_online_industry_tracker.application.errors import ServiceError

    with create_app().test_request_context("/", query_string={"scan": "2.5"}):
        with pytest.raises(ServiceError) as info:
            admin._int_arg("scan", 5)
    assert info.value.status_code == 400 and "scan" in info.value.message


# --- non-integer character_id / corporation_id -------------------------------------------------
#
# A character_id or corporation_id query value that is not a
# whole number is a 400 naming the field and the value, not a silent "all
# characters" / "all corporations". Absent or blank still means all. Fake
# values only.

_ROUTES = [
    ("/characters/assets", "character_id"),
    ("/characters/realized_profit", "character_id"),
    ("/corporations/assets", "corporation_id"),
    ("/corporations/realized_profit", "corporation_id"),
    ("/industry_active_jobs", "character_id"),
]


@pytest.fixture
def id_client(monkeypatch):
    class _no_service:
        def __init__(self, *a, **k):
            pass

        def __getattr__(self, name):
            raise AssertionError(f"{name}: the service must not be reached with an invalid id")

    for module in (character_routes, corporation_routes, industry_routes):
        monkeypatch.setattr(module, "require_ready", lambda state: None)
        monkeypatch.setattr(module, "get_state", lambda: SimpleNamespace())
    monkeypatch.setattr(character_routes, "CharactersService", _no_service)
    monkeypatch.setattr(corporation_routes, "CorporationsService", _no_service)
    monkeypatch.setattr(industry_routes, "IndustryService", _no_service)
    return create_app().test_client()


@pytest.mark.parametrize("path,field", _ROUTES)
@pytest.mark.parametrize("value", ["abc", "12x", "-5", "1.5"])
def test_an_invalid_id_is_a_400_naming_it(id_client, path, field, value):
    response = id_client.get(path, query_string={field: value})
    assert response.status_code == 400
    message = response.get_json()["message"]
    assert field in message and repr(value) in message


@pytest.mark.parametrize("raw,expected", [(None, None), ("", None), ("  ", None), (" 90000001 ", 90000001)])
def test_the_optional_id_param(raw, expected):
    from flask_app.http import optional_id_param

    app = create_app()
    query = {} if raw is None else {"character_id": raw}
    with app.test_request_context("/", query_string=query):
        assert optional_id_param("character_id") == expected
