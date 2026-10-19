"""Fix round 1 on the strict settings, the rig migration and the station name.

- I1: an invalid strict setting read under a broad except (the market order
  books, the price suggestion, the market history) is re-raised, so the
  market-order pages show it instead of an empty order book.
- M1: a startup-critical setting read as its default logs a WARNING once per
  process naming it and the default; the market refresh loop reads its
  interval visibly.
- M2: a float's step is input granularity, not validity (type, finiteness
  and min/max only); "6.0" reads as 6 for an int.
- M4: the rig migration shows on /health and the settings page; an invalid
  legacy rig does not fail an Athanor.
- M5: the NPC station name comes from ESI /universe/stations/{id} first.
- M6: a rig's ore class needs a LocationGroupModifier on the reprocessing
  service group 1322.

Fake values throughout.
"""
from __future__ import annotations

import copy
import json
import logging
from types import SimpleNamespace

import pytest

import sde_industry_fakes as fakes
from eve_online_industry_tracker.config import admin_settings as admin_settings_module
from eve_online_industry_tracker.config.admin_settings import (
    ADMIN_SETTINGS_SCHEMA,
    AdminSettingInvalid,
    AdminSettingsManager,
)
from eve_online_industry_tracker.infrastructure.sde import ore
from eve_online_industry_tracker.infrastructure.static_data import facility_repo


def _manager(tmp_path, payload=None) -> AdminSettingsManager:
    path = tmp_path / "admin_settings.json"
    if payload is not None:
        path.write_text(json.dumps(payload), encoding="utf-8")
    return AdminSettingsManager(path)


def _invalid_workers() -> AdminSettingInvalid:
    return AdminSettingInvalid("performance", "market_order_max_workers", 99, "99 is outside 1 to 20")


# --- I1: the market-order pages ----------------------------------------------------------------

class _RaisingEsi:
    def __init__(self, *, buy=False, sell=False, history=False):
        self._buy, self._sell, self._history = buy, sell, history

    def get_type_buyprices(self, type_ids):
        if self._buy:
            raise _invalid_workers()
        return {}

    def get_type_sellprices(self, type_ids):
        if self._sell:
            raise _invalid_workers()
        return {}

    def get_market_history(self, type_ids, region_id=None):
        if self._history:
            raise AdminSettingInvalid("performance", "market_history_max_workers", 0, "0 is outside 1 to 20")
        return {}


_ORDERS = [{"character_id": 1, "corporation_id": 2, "market_orders": [
    {"order_id": 1, "type_id": 34, "is_buy_order": True, "price": 4.0, "issued": "2026-10-01T00:00:00Z",
     "duration": 90},
    {"order_id": 2, "type_id": 35, "is_buy_order": False, "price": 9.0, "issued": "2026-10-01T00:00:00Z",
     "duration": 90}]}]


def _character_state(esi):
    return SimpleNamespace(esi_service=esi, char_manager=SimpleNamespace(get_market_orders=lambda refresh: _ORDERS),
                           admin_settings=None, db_app=None, db_sde=None, db_oauth=None)


@pytest.mark.parametrize("esi", [_RaisingEsi(buy=True), _RaisingEsi(sell=True)], ids=["buy", "sell"])
def test_the_character_order_book_does_not_swallow_an_invalid_setting(esi):
    from eve_online_industry_tracker.application.characters.service import CharactersService

    with pytest.raises(AdminSettingInvalid, match="performance/market_order_max_workers"):
        CharactersService(state=_character_state(esi)).get_market_orders_enriched()


def test_the_character_market_history_does_not_swallow_an_invalid_setting(monkeypatch):
    from eve_online_industry_tracker.application.characters import service as characters_service

    def history(self, **kwargs):
        raise AdminSettingInvalid("performance", "market_history_max_workers", 0, "0 is outside 1 to 20")

    monkeypatch.setattr(characters_service.MarketHistoryService, "fetch_and_store_history", history)
    with pytest.raises(AdminSettingInvalid, match="market_history_max_workers"):
        characters_service.CharactersService(state=_character_state(_RaisingEsi())).get_market_orders_enriched()


@pytest.mark.parametrize("esi", [_RaisingEsi(buy=True), _RaisingEsi(sell=True)], ids=["buy", "sell"])
def test_the_corporation_order_book_does_not_swallow_an_invalid_setting(esi):
    from eve_online_industry_tracker.application.corporations.service import CorporationsService

    state = SimpleNamespace(
        esi_service=esi, corp_manager=SimpleNamespace(get_market_orders=lambda corporation_id, refresh: _ORDERS),
        char_manager=SimpleNamespace(get_corp_director=lambda: None, get_main_character=lambda: None),
        admin_settings=None, db_app=None, db_sde=None, db_oauth=None)
    with pytest.raises(AdminSettingInvalid, match="performance/market_order_max_workers"):
        CorporationsService(state=state).get_market_orders_enriched(corporation_id=2)


def test_the_hub_price_does_not_swallow_an_invalid_setting(monkeypatch):
    from eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service import (
        PricingSuggestionService,
    )

    svc = PricingSuggestionService.__new__(PricingSuggestionService)
    svc._market_pricing = SimpleNamespace(get_type_price_map=lambda **kw: (_ for _ in ()).throw(_invalid_workers()))
    with pytest.raises(AdminSettingInvalid):
        svc._get_hub_price(type_id=34)
    with pytest.raises(AdminSettingInvalid):
        svc._get_hub_buy_price(type_id=34)


def test_the_market_orders_page_api_raises_with_the_backends_message(monkeypatch):
    from streamlit_ui.api import market_orders as api

    message = "admin setting performance/market_order_max_workers=99 is invalid (99 is outside 1 to 20)"
    monkeypatch.setattr(api, "api_get_or_error", lambda path, timeout_seconds=None: (None, message))
    for call in (lambda: api.fetch_market_orders.__wrapped__(),
                 lambda: api.fetch_corp_market_orders_enriched.__wrapped__(2),
                 lambda: api.fetch_corp_market_orders.__wrapped__(2),
                 api.refresh_market_orders, lambda: api.refresh_corp_market_orders(2)):
        with pytest.raises(RuntimeError, match="market_order_max_workers"):
            call()
    monkeypatch.setattr(api, "api_get_or_error", lambda path, timeout_seconds=None: (
        {"status": "success", "data": [{"order_id": 1}]}, None))
    assert api.fetch_corp_market_orders_enriched.__wrapped__(2) == [{"order_id": 1}]
    assert api.fetch_market_orders.__wrapped__()["data"] == [{"order_id": 1}]


# --- M1: the startup-critical settings ---------------------------------------------------------

@pytest.fixture()
def fresh_default_warnings(monkeypatch):
    monkeypatch.setattr(admin_settings_module, "_DEFAULT_IN_USE_WARNED", set())


def test_a_startup_critical_default_in_use_is_a_warning_once_per_process(tmp_path, caplog, fresh_default_warnings):
    payload = {"esi_resilience": {"esi_error_budget_max_sleep_seconds": 5}}
    with caplog.at_level(logging.WARNING):
        mgr = _manager(tmp_path, payload)
        caplog.clear()
        assert [mgr.get("esi_resilience", "esi_error_budget_max_sleep_seconds") for _ in range(3)] == [60, 60, 60]
        _manager(tmp_path, payload).get("esi_resilience", "esi_error_budget_max_sleep_seconds")
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "esi_resilience/esi_error_budget_max_sleep_seconds" in warnings[0] and "60" in warnings[0]


def test_the_market_refresh_interval_reader_flags_an_invalid_setting(tmp_path, caplog, fresh_default_warnings):
    from eve_online_industry_tracker.application.market_intelligence.job import MarketIntelligenceJob

    spec = ADMIN_SETTINGS_SCHEMA["daily_planner"]["settings"]["planner_market_refresh_interval_hours"]
    job = MarketIntelligenceJob.__new__(MarketIntelligenceJob)
    job._state = SimpleNamespace(admin_settings=_manager(tmp_path, {"daily_planner": {
        "planner_market_refresh_interval_hours": 99}}))
    with caplog.at_level(logging.WARNING):
        assert job._refresh_interval_hours() == float(spec["default"])
    assert any("planner_market_refresh_interval_hours" in r.getMessage() for r in caplog.records)

    class Broken:
        def get(self, category, key):
            raise RuntimeError("settings store unreadable")

    job._state = SimpleNamespace(admin_settings=Broken())
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        assert job._refresh_interval_hours() == float(spec["default"])
    assert any("settings store unreadable" in r.getMessage() for r in caplog.records)
    job._state = SimpleNamespace(admin_settings=None)
    assert job._refresh_interval_hours() == float(spec["default"])


# --- M2: float steps and integral strings -------------------------------------------------------

def test_a_float_off_its_step_is_accepted_as_entered(tmp_path):
    """A float's step is the settings page's input granularity, not validity."""
    mgr = _manager(tmp_path)
    mgr.set("daily_planner", "planner_price_outlier_sigma", 3.3)          # step 0.5
    assert mgr.get("daily_planner", "planner_price_outlier_sigma") == 3.3
    mgr.set("reprocessing", "tax", 0.0213)                                # 2.13% after standings, step 0.0005
    assert mgr.get("reprocessing", "tax") == 0.0213
    mgr.set("industry", "base_sales_tax_rate", 0.0752)                   # step 0.005
    assert mgr.get("industry", "base_sales_tax_rate") == 0.0752
    with pytest.raises(ValueError, match="daily_planner/planner_price_outlier_sigma.*outside 1.0 to 10.0"):
        mgr.set("daily_planner", "planner_price_outlier_sigma", 10.5)


def test_every_int_default_is_on_its_step_grid():
    for category, cat in ADMIN_SETTINGS_SCHEMA.items():
        for key, spec in cat["settings"].items():
            if spec["type"] == "int" and spec["default"] is not None:
                AdminSettingsManager._coerce(spec["default"], spec)   # no ValueError


def test_an_integral_string_for_an_int_reads_like_the_float(tmp_path):
    mgr = _manager(tmp_path)
    mgr.set("industry", "portfolio_order_count_threshold", "6.0")
    assert mgr.get("industry", "portfolio_order_count_threshold") == 6
    with pytest.raises(ValueError, match="not an integer"):
        mgr.set("industry", "portfolio_order_count_threshold", "6.5")


# --- M4: the rig migration -------------------------------------------------------------------

def _legacy(tmp_path, reprocessing):
    return _manager(tmp_path, {"reprocessing": reprocessing})


def test_the_rig_migration_shows_on_health_and_the_settings_page(tmp_path):
    from flask_app.app import create_app

    mgr = _legacy(tmp_path, {"facility_type": "athanor", "rig": "t2", "tax": 0.02})
    (notice,) = mgr.notices()
    assert "reprocessing/rig" in notice and "athanor_rig_asteroid" in notice and "moon" in notice
    assert notice in mgr.problems()
    app = create_app()
    app.extensions["app_state"] = SimpleNamespace(admin_settings=mgr, db_app=None, db_sde=None, db_oauth=None)
    assert app.test_client().get("/admin_settings").get_json()["data"]["notices"] == [notice]


def test_an_invalid_legacy_rig_does_not_fail_an_athanor(tmp_path):
    mgr = _legacy(tmp_path, {"facility_type": "athanor", "rig": "t9", "athanor_rig_asteroid": "t2",
                             "security_band": "null", "tax": 0.02})
    facility = facility_repo.configured_facility(mgr, None, fakes.fake_ore_data())
    assert facility.yields["asteroid"].rig == "t2"
    assert "reprocessing" in mgr.invalid() and "rig" in mgr.invalid()["reprocessing"]   # still flagged
    (payload,) = facility_repo.get_all_facilities(mgr, None, data=fakes.fake_ore_data())
    assert payload["rig"] is None and "t9" in payload["rig_problem"]
    tatara = _legacy(tmp_path, {"facility_type": "tatara", "rig": "t9", "tax": 0.02})
    with pytest.raises(facility_repo.ReprocessingSettingsInvalid, match="reprocessing/rig='t9'"):
        facility_repo.configured_facility(tatara, None, fakes.fake_ore_data())


# --- M5: the station name from ESI -----------------------------------------------------------

def _npc(station_name):
    return facility_repo.facility_from_choice(
        facility_type="npc_station", rig="none", security_band="high", tax=None,
        npc_station_id=fakes.FAKE_NPC_STATION_ID, npc_station=fakes.fake_npc_station, data=fakes.fake_ore_data(),
        station_name=station_name)


class _StationEsi:
    def __init__(self, names=None, error=None):
        self._names, self._error, self.calls = names or {}, error, []

    def get_station_name(self, station_id, language="en"):
        self.calls.append(station_id)
        if self._error:
            raise RuntimeError(self._error)
        # (name, note): the language note is tested in tests/test_esi_station_name_language.py
        return self._names[station_id], None


def test_the_station_name_prefers_esi(monkeypatch):
    monkeypatch.setattr(facility_repo._locations, "npc_station_name",
                        lambda session, station_id, language: ("Fakeburg VIII - Fake Academy", None))
    esi = _StationEsi({fakes.FAKE_NPC_STATION_ID: "Fakeburg VIII (Fakeplanet) - Fake Academy"})
    assert facility_repo.npc_station_name(object(), fakes.FAKE_NPC_STATION_ID, "en", esi_service=esi) == (
        "Fakeburg VIII (Fakeplanet) - Fake Academy", None, None)


def test_without_esi_the_sde_name_carries_a_note(monkeypatch):
    monkeypatch.setattr(facility_repo._locations, "npc_station_name",
                        lambda session, station_id, language: ("Fakeburg VIII - Fake Academy", None))
    name, problem, note = facility_repo.npc_station_name(object(), fakes.FAKE_NPC_STATION_ID, "en",
                                                         esi_service=_StationEsi(error="ESI request failed: 503"))
    assert name == "Fakeburg VIII - Fake Academy" and problem is None
    assert "503" in note and "unique name" in note
    name, _, note = facility_repo.npc_station_name(object(), fakes.FAKE_NPC_STATION_ID, "en", esi_service=None)
    assert name == "Fakeburg VIII - Fake Academy" and "unique name" in note
    facility = _npc(lambda station_id: (name, None, note))
    assert "Fakeburg VIII - Fake Academy" in facility.name and "unique name" in facility.name


def test_the_esi_station_name_is_cached():
    from eve_online_industry_tracker.infrastructure.esi_service import ESIService

    svc = ESIService.__new__(ESIService)
    svc._station_name_cache = {}
    svc._station_name_failures = {}
    calls = []

    def fake_get(endpoint, *args, **kwargs):
        calls.append(endpoint)
        payload = {"station_id": 60008494, "name": "Amarr VIII (Oris) - Emperor Family Academy"}
        return SimpleNamespace(json=lambda: payload, headers={"Content-Language": "en"})

    svc._public_esi_get = fake_get
    assert svc.get_station_name(60008494) == ("Amarr VIII (Oris) - Emperor Family Academy", None)
    assert svc.get_station_name(60008494) == ("Amarr VIII (Oris) - Emperor Family Academy", None)
    assert len(calls) == 1 and "60008494" in calls[0]
    svc._public_esi_get = lambda endpoint, *a, **k: SimpleNamespace(json=lambda: {}, headers={})
    with pytest.raises(RuntimeError, match="no name"):
        svc.get_station_name(60000001)


# --- M6: the rig's ore classes need the right modifier ------------------------------------------

def test_a_rig_class_needs_a_location_group_modifier_on_the_service_group():
    effects = copy.deepcopy(fakes.DOGMA_EFFECTS)
    effects[6675]["modifierInfo"][0]["groupID"] = 1404            # another group: not the reprocessing service
    effects[6678]["modifierInfo"][0]["func"] = "ItemModifier"      # not a location group modifier
    data = ore.ore_data_from_tables(compressible_types={}, type_dogma=fakes.TYPE_DOGMA, dogma_effects=effects)
    assert data.rig_ore_classes[46639] == frozenset({"asteroid"})
    assert data.rig_ore_classes[46637] == frozenset()
    assert data.rig_ore_classes[46635] == frozenset()


# --- M3: the ore page shows why the fee is unknown -----------------------------------------------

def test_the_ore_page_warns_when_the_fee_is_unknown():
    from streamlit_ui.pages.ore_calculator import _fee_unknown_warning

    assert _fee_unknown_warning({"reprocessing_fee_unknown_reason": "no ESI adjusted price for Tritanium"}) == (
        "Reprocessing fee unknown: no ESI adjusted price for Tritanium. The total cost and the savings are "
        "unknown too.")
    assert _fee_unknown_warning({"reprocessing_fee": 12.0, "reprocessing_fee_unknown_reason": None}) is None


# --- re-review: the station name with ESI down --------------------------------------------------

def _esi_service_with(public_get):
    from eve_online_industry_tracker.infrastructure.esi_service import ESIService

    svc = ESIService.__new__(ESIService)
    svc._station_name_cache = {}
    svc._station_name_failures = {}
    svc._public_esi_get = public_get
    return svc


def test_a_down_esi_costs_one_short_lookup_per_window(monkeypatch):
    from eve_online_industry_tracker.infrastructure import esi_service as esi_module

    calls = []

    def down(endpoint, **kwargs):
        calls.append(kwargs)
        raise ConnectionError("ESI unreachable")

    svc = _esi_service_with(down)
    now = [1000.0]
    monkeypatch.setattr(esi_module.time, "time", lambda: now[0])
    with pytest.raises(RuntimeError, match="unreachable"):
        svc.get_station_name(60008494)
    assert calls == [{"headers": {"Accept-Language": "en"}, "timeout_seconds": 3.0, "max_retries": 0,
                      "return_response": True}]                   # short, no retries
    with pytest.raises(RuntimeError, match="unreachable"):
        svc.get_station_name(60008494)                            # within the window: cached failure
    assert len(calls) == 1
    now[0] += esi_module.ESIService.STATION_NAME_FAILURE_TTL_SECONDS + 1
    with pytest.raises(RuntimeError):
        svc.get_station_name(60008494)                            # the window is over: one new try
    assert len(calls) == 2


def test_with_esi_down_the_facility_shows_the_sde_name_with_one_lookup(monkeypatch, tmp_path):
    monkeypatch.setattr(facility_repo._locations, "npc_station_name",
                        lambda session, station_id, language: ("Fakeburg VIII - Fake Academy", None))
    calls = []

    def down(endpoint, **kwargs):
        calls.append(endpoint)
        raise ConnectionError("ESI unreachable")

    esi = _esi_service_with(down)
    mgr = _manager(tmp_path)
    mgr.set("reprocessing", "npc_station_id", fakes.FAKE_NPC_STATION_ID)
    monkeypatch.setattr(facility_repo, "npc_station_reprocessing", lambda session, station_id: (0.5, 0.05))
    (payload,) = facility_repo.get_all_facilities(mgr, object(), data=fakes.fake_ore_data(), esi_service=esi)
    assert len(calls) == 1                                        # one lookup per facility read
    assert payload["npc_station_name"] == "Fakeburg VIII - Fake Academy"
    assert "unique name" in payload["npc_station_name_note"] and "unreachable" in payload["npc_station_name_note"]
    assert "Fakeburg VIII - Fake Academy" in payload["name"]
    facility_repo.get_all_facilities(mgr, object(), data=fakes.fake_ore_data(), esi_service=esi)
    assert len(calls) == 1                                        # the cached failure: no new ESI call
