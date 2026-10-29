"""Every admin setting is read where the value it describes is used.

The settings no backend code read (re-derived by grepping each schema key in
src outside the schema) are wired to the code that used a constant, a
constructor default or a config.json key for the same value, or removed when
they duplicate another source or describe a feature that does not exist.
Each test changes a setting and checks the behaviour changes. Fake values.
"""
from __future__ import annotations

import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock
import logging

import pytest
import requests

from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA, AdminSettingsManager
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure import esi_service as esi_service_module
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService


def _manager(tmp_path, values: dict | None = None) -> AdminSettingsManager:
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    if values:
        mgr.set_bulk(values)
    return mgr


# --- the schema: removed settings ----------------------------------------------------------

@pytest.mark.parametrize("category, key", [
    ("market_defaults", "default_sales_tax"),                     # the sales tax is base rate x Accounting
    ("daily_planner", "planner_industry_data_max_age_hours"),     # no such staleness warning exists
    ("daily_planner", "planner_shopping_future_stock_days"),      # no future_stock shopping category is built
    ("daily_planner", "planner_market_depth_threads"),            # the collector's fetch is market_order_max_workers
])
def test_the_removed_settings_are_gone(category, key):
    assert key not in ADMIN_SETTINGS_SCHEMA[category]["settings"]


def test_a_stored_removed_setting_is_ignored(tmp_path):
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps({"daily_planner": {"planner_market_depth_threads": 4}}), encoding="utf-8")
    assert AdminSettingsManager(path).invalid() == {}


def test_the_config_no_longer_duplicates_admin_settings():
    from config.schemas import CONFIG_SCHEMA

    with open("config/config.json", encoding="utf-8") as fh:
        config_pricing = json.load(fh)["defaults"]["market_pricing"]
    schema_pricing = CONFIG_SCHEMA["defaults"]["market_pricing"]
    for key in ("orderbook_depth", "material_price_cache_ttl_seconds", "sales_tax_fraction", "broker_fee_fraction"):
        assert key not in config_pricing and key not in schema_pricing, key


def test_the_esi_path_settings_are_startup_critical():
    for category, key in [("performance", "esi_auth_pagination_sleep_seconds"),
                          ("esi_resilience", "esi_max_retries"),
                          ("esi_resilience", "esi_request_timeout_seconds")]:
        assert ADMIN_SETTINGS_SCHEMA[category]["settings"][key].get("startup_critical"), key


# --- the authenticated ESI client --------------------------------------------------------------

class _Response:
    def __init__(self, status=200, body=None, pages=1):
        self.status_code = status
        self._body = body if body is not None else [{"x": 1}]
        self.headers = {"X-Pages": str(pages)}
        self.request = SimpleNamespace(headers={}, body=None)
        self.text = json.dumps(self._body)

    def json(self):
        return self._body

    def raise_for_status(self):
        raise requests.HTTPError(f"HTTP {self.status_code}")


class _Session:
    def __init__(self, responses=None, error=None):
        self.calls: list[dict] = []
        self._responses = list(responses or [])
        self._error = error

    def get(self, url, headers=None, timeout=None, params=None, **kwargs):
        self.calls.append({"url": url, "timeout": timeout})
        if self._error:
            raise self._error
        return self._responses.pop(0) if self._responses else _Response()

    post = get


@pytest.fixture()
def client(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(esi_client_module.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(esi_client_module, "get_esi_monitor", None)
    c = esi_client_module.ESIClient.__new__(esi_client_module.ESIClient)
    c.access_token, c.token_expiry = "token", None
    c.esi_base_uri, c.user_agent = "https://esi.example", "test"
    c.esi_header_accept = c.esi_header_acceptlanguage = c.esi_header_xcompatibilitydate = c.esi_header_xtenant = "x"
    c.sleeps = sleeps
    c.db_oauth = MagicMock()   # the ESI response cache: not under test
    # esi_post goes through requests.post, not the session
    monkeypatch.setattr(esi_client_module.requests, "post", lambda url, **kwargs: c._http_session.post(url, **kwargs))
    yield c
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "_admin_settings", None)


def _wire(monkeypatch, mgr):
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "_admin_settings", mgr)


def test_the_auth_pagination_sleep_is_the_setting(client, monkeypatch, tmp_path):
    _wire(monkeypatch, _manager(tmp_path, {"performance": {"esi_auth_pagination_sleep_seconds": 0.35}}))
    client._http_session = _Session([_Response(pages=2), _Response(pages=2)])
    assert client.esi_get("/x/", use_cache=False, paginate=True) == [{"x": 1}, {"x": 1}]
    assert client.sleeps == [0.35]
    client.sleeps.clear()
    client._http_session = _Session([_Response(pages=2), _Response(pages=2)])
    client.esi_post("/x/", json={"ids": [1]}, paginate=True)
    assert client.sleeps == [0.35]


def test_the_request_timeout_is_the_setting_unless_the_caller_gives_one(client, monkeypatch, tmp_path):
    _wire(monkeypatch, _manager(tmp_path, {"esi_resilience": {"esi_request_timeout_seconds": 22}}))
    client._http_session = _Session()
    client.esi_get("/x/", use_cache=False)
    client.esi_get("/x/", use_cache=False, paginate=True)
    client.esi_post("/x/", json=[1])
    client.esi_get("/x/", use_cache=False, timeout_seconds=3)
    assert [c["timeout"] for c in client._http_session.calls] == [22.0, 22.0, 22.0, 3.0]


@pytest.mark.parametrize("paginate", [False, True])
def test_the_retries_are_the_setting(client, monkeypatch, tmp_path, paginate):
    _wire(monkeypatch, _manager(tmp_path, {"esi_resilience": {"esi_max_retries": 1}}))
    client._http_session = _Session(error=requests.ConnectionError("down"))
    with pytest.raises(RuntimeError, match="after retries"):
        client.esi_get("/x/", use_cache=False, paginate=paginate)
    assert len(client._http_session.calls) == 2          # the first try and 1 retry
    assert len(client.sleeps) == 1                       # a wait before the retry, none after the last try
    _wire(monkeypatch, _manager(tmp_path, {"esi_resilience": {"esi_max_retries": 3}}))
    client._http_session = _Session(error=requests.ConnectionError("down"))
    with pytest.raises(RuntimeError):
        client.esi_post("/x/", json={"ids": [1]}, paginate=paginate)
    assert len(client._http_session.calls) == 4


def test_a_paginated_get_stops_retrying_a_server_error_at_the_setting(client, monkeypatch, tmp_path):
    _wire(monkeypatch, _manager(tmp_path, {"esi_resilience": {"esi_max_retries": 2}}))
    client._http_session = _Session([_Response(status=503) for _ in range(10)])
    with pytest.raises(RuntimeError, match="after retries"):
        client.esi_get("/x/", use_cache=False, paginate=True)
    assert len(client._http_session.calls) == 3


def test_without_admin_settings_the_client_uses_the_schema_defaults(client):
    client._http_session = _Session(error=requests.ConnectionError("down"))
    with pytest.raises(RuntimeError):
        client.esi_get("/x/", use_cache=False)
    defaults = ADMIN_SETTINGS_SCHEMA["esi_resilience"]["settings"]
    assert len(client._http_session.calls) == defaults["esi_max_retries"]["default"] + 1
    assert client._http_session.calls[0]["timeout"] == defaults["esi_request_timeout_seconds"]["default"]


# --- the public ESI service ---------------------------------------------------------------------

def _service(mgr, session=None):
    svc = esi_service_module.ESIService.__new__(esi_service_module.ESIService)
    svc._admin_settings = mgr
    svc._esi_client = SimpleNamespace(esi_base_uri="https://esi.example")
    svc._http_session = session or _Session()
    svc._type_orders_cache, svc._market_history_cache = {}, {}
    svc._market_prices_cache = svc._industry_facilities_cache = None
    svc._type_orders_cache_ttl_seconds = 300
    svc._market_prices_cache_ttl_seconds = 3600
    svc._market_history_cache_ttl_seconds = 21600
    svc._industry_facilities_cache_ttl_seconds = 21600
    return svc


def test_the_public_get_timeout_and_retries_are_the_settings(monkeypatch, tmp_path):
    monkeypatch.setattr(esi_service_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(esi_service_module, "get_esi_monitor", None, raising=False)
    mgr = _manager(tmp_path, {"esi_resilience": {"esi_request_timeout_seconds": 21, "esi_max_retries": 2}})
    session = _Session(error=requests.ConnectionError("down"))
    svc = _service(mgr, session)
    with pytest.raises(Exception):
        svc._public_esi_get("/x/")
    assert len(session.calls) == 3 and session.calls[0]["timeout"] == 21.0
    session = _Session(error=requests.ConnectionError("down"))
    svc._http_session = session
    with pytest.raises(Exception):
        svc._public_esi_get("/x/", timeout_seconds=3.0, max_retries=0)   # the caller's own: the station name
    assert len(session.calls) == 1 and session.calls[0]["timeout"] == 3.0


@pytest.mark.parametrize("key, cache_attr, fetch, age, shorter", [
    ("market_prices_cache_ttl_seconds", "_market_prices_cache", lambda svc: svc.get_market_prices(), 2000, 1000),
    ("industry_facilities_cache_ttl_seconds", "_industry_facilities_cache",
     lambda svc: svc.get_industry_facilities(), 5000, 3600),
])
def test_the_esi_cache_ttls_are_the_settings(tmp_path, key, cache_attr, fetch, age, shorter):
    calls = []
    mgr = _manager(tmp_path)
    svc = _service(mgr)
    svc._esi_client = SimpleNamespace(esi_get=lambda *a, **k: calls.append(a) or [{"type_id": 34}])
    setattr(svc, cache_attr, (time.time() - age, [{"cached": True}]))
    fetch(svc)
    assert calls == []                       # within the default TTL: the cached entry
    mgr.set("cache_ttl", key, shorter)
    fetch(svc)
    assert len(calls) == 1                   # older than the setting: fetched again


def test_the_type_orders_and_market_history_ttls_are_the_settings(monkeypatch, tmp_path):
    monkeypatch.setattr(esi_service_module.time, "sleep", lambda s: None)
    mgr = _manager(tmp_path, {"cache_ttl": {"type_orders_cache_ttl_seconds": 600,
                                            "market_history_cache_ttl_seconds": 3600}})
    svc = _service(mgr)
    old = time.time() - 400
    svc._type_orders_cache[("sell", 10000002, 34)] = (old, [{"price": 1.0}])
    svc._market_history_cache[(10000002, 34)] = (old, [{"date": "2026-10-01"}])
    assert svc._fetch_region_orders([34], region_id=10000002, order_type="sell") == [{"price": 1.0}]
    assert svc.get_market_history([34], region_id=10000002) == {34: [{"date": "2026-10-01"}]}
    assert svc.get_sell_order_book_metadata([34], region_id=10000002)["cache_ttl_seconds"] == 600
    mgr.set("cache_ttl", "type_orders_cache_ttl_seconds", 300)   # 400 s old > 300 s: fetched again
    assert svc._fetch_region_orders([34], region_id=10000002, order_type="sell") == []
    assert svc._http_session.calls


# --- market pricing: one source (the admin setting, not config.json) -----------------------------

def _pricing(mgr, cfg=None):
    from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService

    cfg_manager = SimpleNamespace(all=lambda: {"defaults": {"market_pricing": cfg or {}}})
    return MarketPricingService(state=SimpleNamespace(admin_settings=mgr, cfg_manager=cfg_manager,
                                                      db_app=None, db_sde=None, db_oauth=None))


def test_the_orderbook_depth_and_material_price_ttl_are_the_settings_not_config(tmp_path):
    mgr = _manager(tmp_path)
    svc = _pricing(mgr, {"orderbook_depth": 9, "material_price_cache_ttl_seconds": 99})   # an old config: ignored
    assert (svc.orderbook_depth(), svc.material_price_cache_ttl_seconds()) == (5, 3600)
    mgr.set_bulk({"market_defaults": {"orderbook_depth": 3}, "cache_ttl": {"material_price_cache_ttl_seconds": 600}})
    assert (svc.orderbook_depth(), svc.material_price_cache_ttl_seconds()) == (3, 600)


# --- the relist broker fee ------------------------------------------------------------------

def test_the_relist_broker_fee_assumption_is_the_setting(tmp_path):
    from eve_online_industry_tracker.application.industry.service import IndustryService

    mgr = _manager(tmp_path)
    svc = IndustryService(state=SimpleNamespace(admin_settings=mgr))
    def row(broker):
        return {"manufacturing_job": {"market_fee_context": {"broker_fee_applies": True,
                                                             "rates": {"broker_fee_fraction": broker}}}}

    assert svc._relist_broker_fee_fraction(row(0.012)) == (0.012, None)
    assert svc._relist_broker_fee_fraction(row(0.0)) == (0.0, None)
    fraction, assumed = svc._relist_broker_fee_fraction(row(None))
    assert fraction == 0.03 and "market_defaults/default_broker_fee" in assumed
    fraction, assumed = svc._relist_broker_fee_fraction({})
    assert fraction == 0.03 and "market_defaults/default_broker_fee" in assumed
    mgr.set("market_defaults", "default_broker_fee", 0.05)
    assert svc._relist_broker_fee_fraction({})[0] == 0.05


# --- the daily planner ------------------------------------------------------------------------

def test_the_analytics_batch_window_is_the_setting(planner_repo, session_provider, tmp_path, monkeypatch):
    from test_daily_planner_unavailable_inputs import _analytics_service

    svc = _analytics_service(planner_repo, session_provider, session_provider)
    svc._admin = _manager(tmp_path, {"daily_planner": {"planner_analytics_batch_window": 7}})
    limits = []
    original = planner_repo.get_outcomes
    monkeypatch.setattr(planner_repo, "get_outcomes", lambda tid, limit: limits.append(limit) or original(tid, limit=limit))
    svc.get_analytics()
    assert limits == [7]


def test_the_vwap_days_are_the_setting(session_provider, tmp_path):
    from datetime import datetime, timedelta

    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector
    from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel

    session = session_provider.app_session()
    today = datetime.utcnow().date()
    for days_ago, close, volume in [(0, 10.0, 100), (1, 10.0, 100), (2, 10.0, 100), (3, 20.0, 100),
                                    (4, 20.0, 100), (5, 40.0, 1000), (6, 40.0, 1000)]:
        session.add(MarketHistoryModel(type_id=34, region_id=10000002,
                                       date=(today - timedelta(days=days_ago)).isoformat(), close=close,
                                       volume=volume, order_count=1))
    session.commit()
    collector = MarketDepthCollector.__new__(MarketDepthCollector)
    collector._sessions = session_provider
    collector._admin = _manager(tmp_path)
    assert collector._compute_vwap_5d(34, 10000002) == pytest.approx(14.0)        # 5 days (the default)
    collector._admin.set("daily_planner", "planner_vwap_days", 3)
    assert collector._compute_vwap_5d(34, 10000002) == pytest.approx(10.0)
    collector._admin.set("daily_planner", "planner_vwap_days", 7)
    assert collector._compute_vwap_5d(34, 10000002) == pytest.approx((3000 + 4000 + 80000) / 2500)


def test_the_mineral_squeeze_factor_is_the_setting(tmp_path):
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService

    svc = DailyPlannerService.__new__(DailyPlannerService)
    svc._admin = _manager(tmp_path)
    assert svc._mineral_squeeze_factor() == 0.85
    svc._admin.set("daily_planner", "planner_mineral_squeeze_factor", 0.7)
    assert svc._mineral_squeeze_factor() == 0.7


def test_the_scorer_applies_the_squeeze_factor_it_is_given():
    from test_daily_planner_scoring import _input_row, _pipeline_state
    from eve_online_industry_tracker.application.daily_planner.profitability_scorer import ProfitabilityScorer

    sensitive = SimpleNamespace(is_squeeze_sensitive=True)
    scorer = ProfitabilityScorer()
    row = _input_row()
    item = scorer.score(_pipeline_state(row.type_id), row, None, None, sensitive, trit_trend_7d=6.0,
                        mineral_squeeze_factor=0.7)
    assert item.mineral_squeeze_penalty == 0.7
    default = scorer.score(_pipeline_state(row.type_id), row, None, None, sensitive, trit_trend_7d=6.0)
    assert default.mineral_squeeze_penalty == 0.85


# --- the Streamlit planner thresholds ------------------------------------------------------------

def test_the_planner_page_thresholds_are_the_settings(monkeypatch):
    from streamlit_ui.api import admin_settings as api

    payload = {"status": "success", "data": {"values": {"daily_planner": {
        "planner_freshness_warning_level": 0.8, "planner_max_plan_age_hours": 48,
        "planner_market_refresh_interval_hours": 6}}, "invalid": {}}}
    monkeypatch.setattr(api, "api_get_or_error", lambda path: (payload, None))
    values, problems = api.planner_page_settings()
    assert values == {"planner_freshness_warning_level": 0.8, "planner_max_plan_age_hours": 48.0,
                      "planner_market_refresh_interval_hours": 6.0} and problems == []


def test_the_planner_page_names_an_unreadable_or_invalid_threshold(monkeypatch):
    from streamlit_ui.api import admin_settings as api

    payload = {"status": "success", "data": {"values": {"daily_planner": {
        "planner_freshness_warning_level": 3, "planner_max_plan_age_hours": 48,
        "planner_market_refresh_interval_hours": 6}},
        "invalid": {"daily_planner": {"planner_freshness_warning_level": {
            "value": 3, "problem": "3 is outside 0.5 to 1.0", "startup_critical": False}}}}}
    monkeypatch.setattr(api, "api_get_or_error", lambda path: (payload, None))
    values, problems = api.planner_page_settings()
    assert values["planner_freshness_warning_level"] == 0.9                     # the schema default
    assert any("planner_freshness_warning_level" in p and "default" in p for p in problems)
    monkeypatch.setattr(api, "api_get_or_error", lambda path: (None, "HTTP 503"))
    values, problems = api.planner_page_settings()
    assert values["planner_max_plan_age_hours"] == 36.0 and "HTTP 503" in problems[0]


def test_the_industry_profiles_npc_tax_fallback_is_the_schema_default():
    from streamlit_ui.pages import industry_profiles as page

    spec = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["npc_station_facility_tax"]
    assert page._NPC_STATION_FACILITY_TAX == spec["default"]
    assert page._npc_admin_defaults({"npc_station_facility_tax": 0.005})[0] == 0.005


# --- the removed market default region and station ---------------------------------------------
#
# market_defaults/default_region_id and default_station_id had
# no effect (every market hub carries its own region and station, and the
# ESI service's defaults took precedence over them anyway), so they are
# removed with their strictness. A value still stored in the file is ignored
# with an INFO, not reported as invalid. Fake values only.

def test_the_settings_are_gone_from_the_schema():
    settings = ADMIN_SETTINGS_SCHEMA["market_defaults"]["settings"]
    assert "default_region_id" not in settings
    assert "default_station_id" not in settings


def test_a_stored_value_is_ignored_not_invalid(tmp_path, caplog):
    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps({"market_defaults": {"default_region_id": "not a region", "default_station_id": 1}}),
                    encoding="utf-8")
    with caplog.at_level(logging.INFO):
        mgr = AdminSettingsManager(path)
    assert mgr.invalid() == {}
    assert mgr.problems() == []
    assert "default_region_id" in caplog.text


@pytest.mark.parametrize("hub", sorted(MarketPricingService._MARKET_HUBS))
def test_each_hub_is_priced_in_its_own_region_and_station(hub):
    class _Admin:
        def get(self, category, key):
            raise AssertionError(f"read {category}/{key}")

    svc = MarketPricingService(state=SimpleNamespace(admin_settings=_Admin(),
                                                     esi_service=SimpleNamespace(_region_id=1, _station_id=2)))
    context = svc._market_hub_context(hub)
    configured = MarketPricingService._MARKET_HUBS[hub]
    assert (context["region_id"], context["station_id"]) == (configured["region_id"], configured["station_id"])
