"""F2b: what ESIClient raises. Every remote, token or storage failure of an
ESI call is one of ESI_REQUEST_ERRORS (the DB problems of the token store and
the response cache come wrapped in RuntimeError), and the ESI monitor is
telemetry that never fails a fetch. Fake values only."""
from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure import esi_service as esi_service_module


def _db_error():
    return OperationalError("SELECT", {}, Exception("database is locked"))


def _html_response(status: int = 200) -> requests.Response:
    """A real requests.Response whose body is an HTML error page."""
    response = requests.Response()
    response.status_code = status
    response._content = b"<html><body>502 Bad Gateway</body></html>"
    response.headers["Content-Type"] = "text/html"
    response.request = requests.Request("GET", "https://esi.example/x/").prepare()
    return response


class _Session:
    def __init__(self, make_response):
        self.calls = 0
        self._make_response = make_response

    def get(self, url, **kwargs):
        self.calls += 1
        return self._make_response()

    post = get


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(esi_client_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(esi_client_module, "get_esi_monitor", None)
    monkeypatch.setattr(esi_client_module, "esi_max_retries", lambda: 2)
    c = esi_client_module.ESIClient.__new__(esi_client_module.ESIClient)
    c.access_token, c.token_expiry, c.refresh_token = "token", None, "refresh"
    c.character_name = "Test Pilot"
    c.esi_base_uri, c.user_agent = "https://esi.example", "test"
    c.esi_header_accept = c.esi_header_acceptlanguage = c.esi_header_xcompatibilitydate = c.esi_header_xtenant = "x"
    c.token_url, c.client_id, c.client_secret = "https://sso.example/token", "id", "secret"
    c.db_oauth = MagicMock()
    c._http_session = _Session(_html_response)
    monkeypatch.setattr(esi_client_module.requests, "post", lambda url, **kwargs: c._http_session.post(url, **kwargs))
    return c


# --- a 200 with an HTML body -------------------------------------------------------------------

@pytest.mark.parametrize("method, paginate", [("esi_get", False), ("esi_get", True),
                                              ("esi_post", False), ("esi_post", True)])
def test_a_200_with_an_html_body_is_a_runtime_error_after_the_retries(client, method, paginate):
    with pytest.raises(RuntimeError, match="after retries"):
        getattr(client, method)("/x/", use_cache=False, paginate=paginate)
    assert client._http_session.calls == 3          # the first try and 2 retries
    assert issubclass(RuntimeError, esi_client_module.ESI_REQUEST_ERRORS)


# --- the SSO token refresh -------------------------------------------------------------------------

class _TokenResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


@pytest.mark.parametrize("payload", [
    {"access_token": "a", "expires_in": 1199},                       # no refresh_token
    {"refresh_token": "r", "expires_in": 1199},                      # no access_token
    {"access_token": "a", "refresh_token": "r"},                     # no expires_in
    {"access_token": "a", "refresh_token": "r", "expires_in": None},  # expires_in not a number
    ["not", "a", "dict"],
])
def test_a_malformed_sso_token_response_is_a_value_error(client, monkeypatch, payload):
    monkeypatch.setattr(esi_client_module.requests, "post", lambda url, **kwargs: _TokenResponse(payload))
    with pytest.raises(ValueError, match="unexpected SSO token response"):
        client.refresh_access_token()
    assert client.access_token == "token"          # nothing half-updated
    assert client.refresh_token == "refresh"
    client.db_oauth.session.commit.assert_not_called()


def test_a_failed_token_commit_is_a_runtime_error_and_rolls_back(client, monkeypatch):
    monkeypatch.setattr(
        esi_client_module.requests, "post",
        lambda url, **kwargs: _TokenResponse({"access_token": "a", "refresh_token": "r", "expires_in": 1199}),
    )
    client.db_oauth.session.commit.side_effect = _db_error()
    with pytest.raises(RuntimeError, match="could not store the refreshed ESI token"):
        client.refresh_access_token()
    client.db_oauth.session.rollback.assert_called_once()


@pytest.mark.parametrize("method", ["esi_get", "esi_post"])
def test_an_unreadable_esi_cache_is_a_runtime_error(client, method):
    client.db_oauth.session.query.side_effect = _db_error()
    with pytest.raises(RuntimeError, match="ESI response cache"):
        getattr(client, method)("/x/", use_cache=True)
    client.db_oauth.session.rollback.assert_called()
    assert client._http_session.calls == 0


def test_the_error_tuple_still_excludes_programming_errors():
    for exc in (NameError, AttributeError, TypeError, KeyError):
        assert not issubclass(exc, esi_client_module.ESI_REQUEST_ERRORS)


# --- the ESI monitor: telemetry never fails a fetch -------------------------------------------------

class _BrokenMonitor:
    def __getattr__(self, name):
        def _broken(**kwargs):
            raise TypeError(f"{name}() got an unexpected keyword argument")
        return _broken


@pytest.fixture()
def reset_monitor_limiter(monkeypatch):
    monkeypatch.setattr(esi_client_module, "_ESI_MONITOR_WARNING_LIMITER",
                        esi_client_module._EsiRequestErrorLogLimiter(window_seconds=300.0, max_logs_per_key=1))


def test_a_broken_esi_monitor_is_a_warning_and_the_gate_still_sleeps(monkeypatch, caplog, reset_monitor_limiter):
    slept = []
    monkeypatch.setattr(esi_client_module, "get_esi_monitor", lambda: _BrokenMonitor())
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "suggested_sleep_seconds", lambda: 0.5)
    monkeypatch.setattr(esi_client_module, "_sleep_seconds", lambda s: slept.append(s))
    with caplog.at_level(logging.WARNING):
        esi_client_module._esi_gate("test")
    assert slept == [0.5]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "ESI monitor" in r.getMessage()]
    assert len(warnings) == 1
    assert "record_sleep" in warnings[0].getMessage()


def test_a_broken_esi_monitor_does_not_fail_an_esi_get_and_warns_once(client, monkeypatch, caplog, reset_monitor_limiter):
    ok = requests.Response()
    ok.status_code, ok._content = 200, b'[{"x": 1}]'
    client._http_session = _Session(lambda: ok)
    monkeypatch.setattr(esi_client_module, "get_esi_monitor", lambda: _BrokenMonitor())
    with caplog.at_level(logging.WARNING):
        assert client.esi_get("/x/", use_cache=False) == [{"x": 1}]
        assert client.esi_get("/x/", use_cache=False) == [{"x": 1}]
    warnings = [r for r in caplog.records if "ESI monitor" in r.getMessage()]
    assert len(warnings) == 1          # rate-limited per monitor call


def test_a_broken_esi_monitor_does_not_fail_a_public_esi_get(monkeypatch, caplog, reset_monitor_limiter):
    ok = requests.Response()
    ok.status_code, ok._content = 200, b'{"name": "Jita IV - Moon 4"}'
    svc = esi_service_module.ESIService.__new__(esi_service_module.ESIService)
    svc._esi_client = SimpleNamespace(esi_base_uri="https://esi.example")
    svc._admin_settings = None
    svc._http_session = SimpleNamespace(get=lambda url, **kwargs: ok)
    monkeypatch.setattr(esi_service_module, "get_esi_monitor", lambda: _BrokenMonitor())
    with caplog.at_level(logging.WARNING):
        assert svc._public_esi_get("/universe/stations/60003760/") == {"name": "Jita IV - Moon 4"}
    assert any("ESI monitor" in r.getMessage() and "record_http_attempt" in r.getMessage() for r in caplog.records)
