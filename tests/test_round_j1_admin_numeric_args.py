"""Round J1.9: a non-numeric admin query value is a 400 naming the parameter
and the value, not a 500 from int()/float(). Covers every numeric argument
of the admin routes (the public structures status, refresh and scan start,
and the ESI metrics). Fake values only.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

import flask_app.routes.admin as admin
from flask_app.app import create_app


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
