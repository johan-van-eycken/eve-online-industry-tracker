"""F2b follow-ups: the remaining ESIService wrappers catch only ESI failures; a
failed order-book or history type is skipped with a WARNING and a reason the
caller can read (never a silent, cached empty list), and a bug propagates; the
ESI monitor's arguments are built inside its guard. Fake values only."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
import requests

from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure.esi_service import ESIService
from eve_online_industry_tracker.infrastructure.esi_client import EsiRequestError
import flask_app.bootstrap as bootstrap
from flask_app.state import AppState
from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters import character as character_module
from fakes import _raiser

ESI_FAILURES = [requests.ConnectionError("reset"), requests.HTTPError("502"),
                EsiRequestError("ESI GET failed after retries"), EsiRequestError("No refresh token")]
BUGS = [AttributeError("'NoneType' object has no attribute 'get'"), NameError("name 'oops' is not defined"),
        TypeError("unsupported operand"), KeyError("name")]


def _svc(esi_client=None) -> ESIService:
    svc = ESIService.__new__(ESIService)
    svc._esi_client = esi_client if esi_client is not None else SimpleNamespace()
    svc._admin_settings = None
    svc._type_orders_cache, svc._market_history_cache = {}, {}
    svc._type_orders_cache_ttl_seconds, svc._market_history_cache_ttl_seconds = 300, 21600
    svc._industry_facilities_cache, svc._industry_facilities_cache_ttl_seconds = None, 3600
    svc._region_id, svc._station_id = 10000002, 60003760
    return svc


# --- get_industry_facilities, get_industry_systems, get_universe_type -----------------------------

CALLS = [
    ("get_industry_facilities", (), "ESI request failed"),
    ("get_industry_systems", (), "ESI request failed"),
    ("get_universe_type", (34,), "/universe/types/34/"),
]


@pytest.mark.parametrize("exc", ESI_FAILURES)
@pytest.mark.parametrize("method, args, message", CALLS)
def test_an_esi_failure_is_a_chained_runtime_error(method, args, message, exc):
    svc = _svc(SimpleNamespace(esi_get=_raiser(exc)))
    with pytest.raises(RuntimeError, match=message) as info:
        getattr(svc, method)(*args)
    assert info.value.__cause__ is exc


@pytest.mark.parametrize("exc", BUGS)
@pytest.mark.parametrize("method, args, message", CALLS)
def test_a_bug_propagates_as_itself(method, args, message, exc):
    svc = _svc(SimpleNamespace(esi_get=_raiser(exc)))
    with pytest.raises(type(exc)):
        getattr(svc, method)(*args)


# --- order books: a failed type is skipped with a reason -----------------------------------------

def _orders_svc(fail_type_id, exc):
    svc = _svc()

    def public_get(endpoint, *, params=None, **kwargs):
        if params["type_id"] == fail_type_id:
            raise exc
        is_buy = params["order_type"] == "buy"
        return [{"type_id": params["type_id"], "price": 5.0, "volume_remain": 1, "is_buy_order": is_buy,
                 "location_id": 60003760, "order_id": 1}]

    svc._public_esi_get = public_get
    return svc


@pytest.mark.parametrize("side", ["sell", "buy"])
@pytest.mark.parametrize("exc", ESI_FAILURES)
def test_a_failed_order_book_type_is_skipped_with_a_warning_and_a_reason(monkeypatch, caplog, side, exc):
    monkeypatch.setattr("eve_online_industry_tracker.infrastructure.esi_service.time.sleep", lambda s: None)
    svc = _orders_svc(35, exc)
    with caplog.at_level(logging.WARNING):
        book = getattr(svc, f"get_{side}_order_book")([34, 35])
    assert set(book) == {34}
    reason = svc.order_book_unavailable_reason(35, side=side)
    assert reason and str(exc) in reason
    assert svc.order_book_unavailable_reason(34, side=side) is None
    assert ((side, 10000002, 35)) not in svc._type_orders_cache       # not cached as an empty book
    assert any("market orders fetch failed" in r.getMessage() and "35" in r.getMessage() for r in caplog.records)


def test_a_later_successful_fetch_clears_the_reason(monkeypatch):
    monkeypatch.setattr("eve_online_industry_tracker.infrastructure.esi_service.time.sleep", lambda s: None)
    svc = _orders_svc(35, requests.HTTPError("502"))
    svc.get_sell_order_book([35])
    assert svc.order_book_unavailable_reason(35) is not None
    svc._public_esi_get = _orders_svc(None, None)._public_esi_get
    assert set(svc.get_sell_order_book([35])) == {35}
    assert svc.order_book_unavailable_reason(35) is None


def test_the_order_book_metadata_lists_the_unavailable_types(monkeypatch):
    monkeypatch.setattr("eve_online_industry_tracker.infrastructure.esi_service.time.sleep", lambda s: None)
    svc = _orders_svc(35, requests.HTTPError("502"))
    svc.get_sell_order_book([34, 35])
    meta = svc.get_sell_order_book_metadata([34, 35])
    assert meta["unavailable_type_ids"] == [35]
    assert "502" in meta["unavailable_reasons"][35]
    assert svc.get_sell_order_book_metadata([34])["unavailable_type_ids"] == []


@pytest.mark.parametrize("exc", BUGS)
def test_a_bug_in_an_order_book_fetch_propagates(monkeypatch, exc):
    monkeypatch.setattr("eve_online_industry_tracker.infrastructure.esi_service.time.sleep", lambda s: None)
    svc = _orders_svc(35, exc)
    with pytest.raises(type(exc)):
        svc.get_sell_order_book([34, 35])


# --- market history: the same ------------------------------------------------------------------------

def _history_svc(fail_type_id, exc):
    svc = _svc()

    def public_get(endpoint, *, params=None, **kwargs):
        if params["type_id"] == fail_type_id:
            raise exc
        return [{"date": "2026-10-18", "average": 5.0}]

    svc._public_esi_get = public_get
    return svc


@pytest.mark.parametrize("exc", ESI_FAILURES)
def test_a_failed_history_type_is_absent_with_a_warning_and_a_reason(caplog, exc):
    svc = _history_svc(35, exc)
    with caplog.at_level(logging.WARNING):
        history = svc.get_market_history([34, 35])
    assert set(history) == {34}                                    # absent, not an empty history
    assert str(exc) in svc.market_history_unavailable_reason(35)
    assert svc.market_history_unavailable_reason(34) is None
    assert (10000002, 35) not in svc._market_history_cache
    assert any("market history fetch failed" in r.getMessage() and "35" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("exc", BUGS)
def test_a_bug_in_a_history_fetch_propagates(exc):
    with pytest.raises(type(exc)):
        _history_svc(35, exc).get_market_history([34, 35])


# --- the ESI monitor's arguments are built inside the guard -------------------------------------

def test_a_failing_monitor_argument_is_a_warning_not_a_failed_request(monkeypatch, caplog):
    monkeypatch.setattr(esi_client_module, "_ESI_MONITOR_WARNING_LIMITER",
                        esi_client_module._EsiRequestErrorLogLimiter(window_seconds=300.0, max_logs_per_key=1))
    recorded = []
    monkeypatch.setattr(esi_client_module, "get_esi_monitor",
                        lambda: SimpleNamespace(record_http_attempt=lambda **kw: recorded.append(kw)))

    def broken_kwargs():
        raise ValueError("invalid literal for int() with base 10: 'abc'")

    with caplog.at_level(logging.WARNING):
        esi_client_module._monitor("record_http_attempt", broken_kwargs)
        esi_client_module._monitor("record_http_attempt", lambda: {"status_code": 200})
    assert recorded == [{"status_code": 200}]
    assert any("ESI monitor record_http_attempt failed" in r.getMessage() for r in caplog.records)


def test_an_odd_status_code_does_not_fail_an_esi_get(monkeypatch, caplog):
    """A response whose status code int() cannot read: the monitor call fails, the request does not."""
    monkeypatch.setattr(esi_client_module, "_ESI_MONITOR_WARNING_LIMITER",
                        esi_client_module._EsiRequestErrorLogLimiter(window_seconds=300.0, max_logs_per_key=1))
    monkeypatch.setattr(esi_client_module, "get_esi_monitor",
                        lambda: SimpleNamespace(record_http_attempt=lambda **kw: None))

    class _OddResponse:
        headers: dict = {}

        @property
        def status_code(self):
            return _OddStatus()

        def raise_for_status(self):
            raise requests.HTTPError("odd")

    class _OddStatus:
        def __int__(self):
            raise ValueError("not a status")

        def __eq__(self, other):
            return other == 404

        def __hash__(self):
            return 404

    c = esi_client_module.ESIClient.__new__(esi_client_module.ESIClient)
    c.access_token, c.token_expiry = "token", None
    c.esi_base_uri, c.user_agent = "https://esi.example", "test"
    c.esi_header_accept = c.esi_header_acceptlanguage = c.esi_header_xcompatibilitydate = c.esi_header_xtenant = "x"
    c._http_session = SimpleNamespace(get=lambda url, **kw: _OddResponse())
    with caplog.at_level(logging.WARNING):
        assert c.esi_get("/x/", use_cache=False, suppress_not_found_log=True) is None   # the 404 path
    assert any("ESI monitor record_http_attempt failed" in r.getMessage() for r in caplog.records)


# --- public ESI calls share the error-budget limiter ----------------------------------------------

def test_a_public_esi_get_waits_on_and_feeds_the_shared_error_limiter(monkeypatch):
    gates, seen = [], []
    monkeypatch.setattr("eve_online_industry_tracker.infrastructure.esi_service._esi_gate", gates.append)
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "update_from_headers", seen.append)
    monkeypatch.setattr(esi_client_module, "get_esi_monitor", None)
    headers = {"X-Esi-Error-Limit-Remain": "7", "X-Esi-Error-Limit-Reset": "30"}
    response = SimpleNamespace(status_code=200, headers=headers, url="u", json=lambda: [1],
                               raise_for_status=lambda: None)
    svc = _svc(SimpleNamespace(esi_base_uri="https://esi.example"))
    svc._http_session = SimpleNamespace(get=lambda url, **kw: response)

    assert svc._public_esi_get("/markets/10000002/history/") == [1]
    assert gates == ["GET /markets/10000002/history/"] and seen == [headers]


# --- the admin ESI settings apply before the startup refresh -----------------------------------
#
# Round H2.2: the admin ESI settings apply before the startup refresh.
#
# - bootstrap wires the ESI error limiter (and an existing state.esi_service)
# to the admin settings right after loading them, before refresh_all.
# - each Character's own ESIService (the corporations use their director's)
# gets the admin settings.
#
# Fake values throughout.

def test_the_limiter_reads_the_admin_settings_before_the_startup_refresh(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMIN_SETTINGS_PATH", str(tmp_path / "admin_settings.json"))
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "_admin_settings", None)
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", None)
    state = AppState()
    existing_service = SimpleNamespace(_admin_settings=None)
    state.esi_service = existing_service
    seen = {}

    class _CharManager:
        def refresh_all(self):
            seen["limiter"] = esi_client_module._ESI_ERROR_LIMITER._admin_settings
            seen["service"] = existing_service._admin_settings
            state.shutdown_event.set()   # stop the initialisation here

    monkeypatch.setattr(bootstrap, "load_config", lambda: object())
    monkeypatch.setattr(bootstrap, "init_db_managers", lambda cfg, refresh_metadata=True: (None, None, None))
    monkeypatch.setattr(bootstrap, "check_sde_industry_data", lambda s: None)
    monkeypatch.setattr(bootstrap, "init_char_manager", lambda *a, **k: _CharManager())

    bootstrap.initialize_application(state)

    assert seen["limiter"] is state.admin_settings
    assert seen["service"] is state.admin_settings


def test_a_character_esi_service_gets_the_admin_settings(monkeypatch):
    admin = object()

    class _FakeClient:
        character_id = 90000001

        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(character_module, "ESIClient", _FakeClient)
    char = character_module.Character.__new__(character_module.Character)
    char._esi_client = None
    char._esi_service = None
    char._admin_settings = admin
    char._cfgManager = None
    char._db_oauth = None
    char.character_name = "Fake Pilot"
    char.is_main = False
    char.is_corp_director = False
    char._refresh_token = None
    char.ensure_esi()
    assert char.esi_service._admin_settings is admin


def test_the_esi_service_takes_admin_settings_in_its_constructor():
    admin = object()
    assert ESIService(SimpleNamespace(), admin_settings=admin)._admin_settings is admin
    assert ESIService(SimpleNamespace())._admin_settings is None
