"""Review fixes: the Jita owner's standings in the character's market fees,
undefined names, the VWAP days read once per cycle, the per-page retry
budget and one /admin_settings read per planner page render. Fake values."""
from __future__ import annotations

import inspect
import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module

FAKE_OWNER_CORP, FAKE_OWNER_FACTION = 1000999, 500999


class _Query:
    def __init__(self, row):
        self._row = row

    def filter_by(self, **kwargs):
        return self

    def first(self):
        return self._row


def _character(session, standings):
    from eve_online_industry_tracker.application.characters.character import Character

    c = Character.__new__(Character)
    c._admin_settings = None
    c.skills = {"skills": [{"skill_id": 16622, "trained_skill_level": 4}, {"skill_id": 3446, "trained_skill_level": 5}]}
    c.standings = standings
    c.character_name, c.character_id = "Pilot", 1
    c.market_fees = None
    c._db_sde = SimpleNamespace(session=session)
    c.save_character = lambda: None
    return c


def test_the_jita_owners_standings_count_in_the_characters_broker_fee():
    from eve_online_industry_tracker.infrastructure.models import NpcCorporations, NpcStations

    rows = {NpcStations: SimpleNamespace(ownerID=FAKE_OWNER_CORP),
            NpcCorporations: SimpleNamespace(factionID=FAKE_OWNER_FACTION)}
    session = SimpleNamespace(query=lambda model: _Query(rows[model]))
    c = _character(session, [{"from_type": "faction", "from_id": FAKE_OWNER_FACTION, "standing": 5.0},
                             {"from_type": "npc_corp", "from_id": FAKE_OWNER_CORP, "standing": 2.0}])
    c.refresh_market_fees()
    jita = c.market_fees["jita_4_4"]
    assert (jita["owner_corp_id"], jita["owner_faction_id"]) == (FAKE_OWNER_CORP, FAKE_OWNER_FACTION)
    assert jita["standings"] == {"faction_standing": 5.0, "corp_standing": 2.0}
    assert jita["rates"]["broker_fee_fraction"] == pytest.approx(0.03 - 0.015 - 0.0015 - 0.0004)


def test_an_unreadable_jita_owner_is_a_warning_not_silent(caplog):
    def query(model):
        raise OperationalError("SELECT", {}, Exception("no such table"))

    c = _character(SimpleNamespace(query=query), [])
    with caplog.at_level(logging.WARNING):
        c.refresh_market_fees()
    assert c.market_fees["jita_4_4"]["standings"] == {"faction_standing": 0.0, "corp_standing": 0.0}
    assert any("Jita 4-4" in r.getMessage() and "OperationalError" in r.getMessage() for r in caplog.records)


def test_the_public_structure_scan_has_its_concurrency_names():
    """get_public_structures used wait/FIRST_COMPLETED without importing them."""
    from eve_online_industry_tracker.infrastructure.esi_service import ESIService

    svc = ESIService.__new__(ESIService)
    svc._public_structures_cache, svc._public_structures_cache_ttl_seconds = {}, 600
    svc._esi_client = SimpleNamespace(esi_get=lambda *a, **k: [1021, 1022])
    svc.get_universe_structure = lambda sid, **k: {"name": f"S{sid}", "solar_system_id": 30000142, "owner_id": 1,
                                                   "type_id": 35825, "services": []}
    rows = svc.get_public_structures(30000142, max_workers=2)
    assert sorted(r["station_id"] for r in rows) == [1021, 1022]


# --- M1: the VWAP days, read once per cycle --------------------------------------------------

def test_an_invalid_vwap_days_fails_the_cycle_once_not_per_type(tmp_path):
    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector
    from eve_online_industry_tracker.config.admin_settings import AdminSettingInvalid

    path = tmp_path / "admin_settings.json"
    path.write_text(json.dumps({"daily_planner": {"planner_vwap_days": 99}}), encoding="utf-8")
    collector = MarketDepthCollector.__new__(MarketDepthCollector)
    collector._admin = AdminSettingsManager(path)
    collector._state = SimpleNamespace(esi_service=None, corp_manager=None)
    collector._get_type_ids_from_active_plan = lambda: pytest.fail("the type ids are read after the settings")
    with pytest.raises(AdminSettingInvalid, match="planner_vwap_days"):
        collector.run()


def test_the_vwap_takes_its_days_from_the_caller():
    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import MarketDepthCollector

    assert "days" in inspect.signature(MarketDepthCollector._compute_vwap_5d).parameters


# --- M2: the retry budget is per page ----------------------------------------------------------

class _Response:
    def __init__(self, pages):
        self.status_code = 200
        self.headers = {"X-Pages": str(pages)}

    def json(self):
        return [{"x": 1}]


class _FlakySession:
    """Every page fails once with a connection error, then answers."""

    def __init__(self, pages):
        self.pages, self.calls, self._failed = pages, [], set()

    def get(self, url, headers=None, timeout=None, **kwargs):
        self.calls.append(url)
        if url not in self._failed:
            self._failed.add(url)
            raise requests.ConnectionError("blip")
        return _Response(self.pages)


def test_the_retry_budget_applies_per_page(monkeypatch, tmp_path):
    monkeypatch.setattr(esi_client_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(esi_client_module, "get_esi_monitor", None)
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    mgr.set("esi_resilience", "esi_max_retries", 1)
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "_admin_settings", mgr)
    c = esi_client_module.ESIClient.__new__(esi_client_module.ESIClient)
    c.access_token, c.token_expiry = "token", None
    c.esi_base_uri, c.user_agent = "https://esi.example", "test"
    c.esi_header_accept = c.esi_header_acceptlanguage = c.esi_header_xcompatibilitydate = c.esi_header_xtenant = "x"
    c.db_oauth = MagicMock()
    c._http_session = _FlakySession(pages=3)
    assert c.esi_get("/x/", use_cache=False, paginate=True) == [{"x": 1}] * 3   # 1 retry per page is enough
    assert len(c._http_session.calls) == 6


# --- M3: one settings read per planner page render ---------------------------------------------

def test_the_actions_tab_takes_the_planner_settings_from_the_page():
    from streamlit_ui.components.daily_planner import status_bar, tab_actions
    from streamlit_ui.pages import daily_planner

    assert "planner_settings" in inspect.signature(tab_actions.render_tab_actions).parameters
    assert "planner_settings" in inspect.signature(status_bar.render_status_bar).parameters
    assert "planner_page_settings" not in inspect.getsource(tab_actions)
    assert "planner_page_settings()" not in inspect.getsource(status_bar.render_status_bar)
    assert inspect.getsource(daily_planner).count("planner_page_settings()") == 1
