"""One market-fee formula, read from the admin settings industry/*.

    sales tax  = base_sales_tax_rate x (1 - accounting_skill_reduction x Accounting)
    broker fee = clamp(broker_fee_base - broker_relations_reduction_per_level x BrokerRelations
                       - faction_standing_reduction_per_point x faction standing
                       - corp_standing_reduction_per_point x corp standing,
                       broker_fee_min, broker_fee_base)

The producer (IndustryService), the character's stored market fees and the
price suggestion all use application.market_fees. Where a character's skills
are not known the fee says why instead of a silent 7.5% / 3%. Fake values.
"""
from __future__ import annotations

import inspect
import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application import market_fees
from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module


def _manager(tmp_path, industry=None) -> AdminSettingsManager:
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    if industry:
        mgr.set_bulk({"industry": industry})
    return mgr


# --- the formula -------------------------------------------------------------------------------

def test_the_defaults_give_eves_rates():
    s = market_fees.MarketFeeSettings.from_admin(None)
    assert market_fees.sales_tax_fraction(s, accounting_level=5) == pytest.approx(0.03375)
    assert market_fees.sales_tax_fraction(s, accounting_level=0) == pytest.approx(0.075)
    assert market_fees.broker_fee_fraction(s, broker_relations_level=5, faction_standing=10.0,
                                           corp_standing=10.0) == pytest.approx(0.01)
    assert market_fees.broker_fee_fraction(s, broker_relations_level=0, faction_standing=-10.0,
                                           corp_standing=-10.0) == pytest.approx(0.03)   # capped at the base


def test_the_formula_follows_the_settings(tmp_path):
    mgr = _manager(tmp_path, {"base_sales_tax_rate": 0.05, "accounting_skill_reduction": 0.1, "broker_fee_base": 0.04,
                              "broker_fee_min": 0.02, "broker_relations_reduction_per_level": 0.004})
    s = market_fees.MarketFeeSettings.from_admin(mgr)
    assert market_fees.sales_tax_fraction(s, accounting_level=4) == pytest.approx(0.05 * 0.6)
    assert market_fees.broker_fee_fraction(s, broker_relations_level=5, faction_standing=0.0,
                                           corp_standing=0.0) == pytest.approx(0.02)
    assert market_fees.broker_fee_fraction(s, broker_relations_level=2, faction_standing=0.0,
                                           corp_standing=0.0) == pytest.approx(0.032)


def test_the_producer_has_no_second_copy_of_the_formula():
    from eve_online_industry_tracker.application.industry.service import IndustryService
    from eve_online_industry_tracker.application.characters.character import Character

    for source in (inspect.getsource(IndustryService._resolve_npc_market_fee_context),
                   inspect.getsource(Character.refresh_market_fees)):
        assert "market_fees.sales_tax_fraction" in source and "market_fees.broker_fee_fraction" in source
        assert "0.11" not in source and "0.003" not in source and "broker_relations_reduction_per_level" not in source


# --- the character's stored market fees ---------------------------------------------------------

def _character(admin, skills):
    from eve_online_industry_tracker.application.characters.character import Character

    c = Character.__new__(Character)
    c._admin_settings = admin
    c.skills = skills
    c.standings = []
    c.character_name, c.character_id = "Pilot", 1
    c.market_fees = None
    # An SDE without the Jita 4-4 row: no owner, so no standings count.
    c._db_sde = SimpleNamespace(session=SimpleNamespace(
        query=lambda *a: SimpleNamespace(filter_by=lambda **k: SimpleNamespace(first=lambda: None))))
    c.save_character = lambda: None
    return c


def test_the_characters_market_fees_follow_the_settings(tmp_path):
    skills = {"skills": [{"skill_id": 16622, "trained_skill_level": 4}, {"skill_id": 3446, "trained_skill_level": 5}]}
    c = _character(_manager(tmp_path), skills)
    c.refresh_market_fees()
    rates = c.market_fees["jita_4_4"]["rates"]
    assert rates["sales_tax_fraction"] == pytest.approx(0.075 * (1 - 0.44))
    assert rates["broker_fee_fraction"] == pytest.approx(0.015)
    c = _character(_manager(tmp_path, {"base_sales_tax_rate": 0.05}), skills)
    c.refresh_market_fees()
    assert c.market_fees["jita_4_4"]["rates"]["sales_tax_fraction"] == pytest.approx(0.05 * (1 - 0.44))


def test_without_loaded_skills_the_character_fees_are_unknown_with_a_reason(tmp_path, caplog):
    c = _character(_manager(tmp_path), None)
    with caplog.at_level(logging.WARNING):
        c.refresh_market_fees()
    jita = c.market_fees["jita_4_4"]
    assert jita["rates"]["sales_tax_fraction"] is None and jita["rates"]["broker_fee_fraction"] is None
    assert "skills" in jita["unknown_reason"]


# --- the price suggestion ---------------------------------------------------------------------

def _stored(sales_skill=4, broker_skill=5, faction=2.0, corp=1.0):
    return {"jita_4_4": {"skills": {"accounting_level": sales_skill, "broker_relations_level": broker_skill},
                         "standings": {"faction_standing": faction, "corp_standing": corp},
                         "rates": {"sales_tax_fraction": 0.5, "broker_fee_fraction": 0.5}}}


def test_the_price_suggestion_fees_come_from_the_stored_skills_and_the_current_settings(tmp_path):
    s = market_fees.MarketFeeSettings.from_admin(_manager(tmp_path))
    tax, broker, reason = market_fees.fees_from_stored_market_fees(json.dumps(_stored()), s)
    assert reason is None
    assert tax == pytest.approx(0.075 * (1 - 0.44))
    assert broker == pytest.approx(0.03 - 0.015 - 0.0006 - 0.0002)


@pytest.mark.parametrize("stored, why", [
    (None, "no stored market fees"),
    ("{not json", "unreadable"),
    ({"jita_4_4": {"unknown_reason": "skills not loaded"}}, "skills not loaded"),
])
def test_unknown_price_suggestion_fees_are_assumed_with_a_reason(tmp_path, stored, why):
    s = market_fees.MarketFeeSettings.from_admin(_manager(tmp_path))
    tax, broker, reason = market_fees.fees_from_stored_market_fees(stored, s)
    assert (tax, broker) == (pytest.approx(0.075), pytest.approx(0.03))   # untrained, no standings: the highest
    assert why in reason and "assum" in reason


def test_the_price_suggestion_says_when_its_fees_are_assumed(monkeypatch, tmp_path):
    from eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service import (
        PricingSuggestionService,
    )

    svc = PricingSuggestionService.__new__(PricingSuggestionService)
    svc._state = SimpleNamespace(admin_settings=_manager(tmp_path))
    monkeypatch.setattr(svc, "_stored_market_fees", lambda *, character_id: None, raising=False)
    tax, broker, reason = svc._get_fees(character_id=1)
    assert (tax, broker) == (pytest.approx(0.075), pytest.approx(0.03)) and "no stored market fees" in reason
    assert "fees assumed" in svc._with_fee_note("Some reasoning.", reason)
    assert svc._with_fee_note("Some reasoning.", None) == "Some reasoning."


# --- the relist note on the Industry Builder ----------------------------------------------------

def test_the_relist_note_is_shown_next_to_the_relist_cost():
    from streamlit_ui.state.industry_builder_ui import relist_note

    note = "relist cost as if the broker fee is 3.00% (its own is unknown; admin setting market_defaults/default_broker_fee)"
    assert relist_note({"relist_broker_fee_assumed": note}) == note
    assert relist_note({"relist_broker_fee_assumed": None}) == ""
    source = inspect.getsource(__import__("streamlit_ui.state.industry_builder_ui", fromlist=["x"]))
    cost_at, note_at = source.index('"Relist Cost/Unit"'), source.index('"Relist Note"')
    assert 0 < note_at - cost_at < 200


def test_the_producer_words_the_relist_note_like_the_other_assumed_notes(tmp_path):
    from eve_online_industry_tracker.application.industry.service import IndustryService

    svc = IndustryService.__new__(IndustryService)
    svc._state = SimpleNamespace(admin_settings=AdminSettingsManager(tmp_path / "a.json"))
    _, note = svc._relist_broker_fee_fraction({})
    assert note == ("relist cost as if the broker fee is 3.00% (its own is unknown; admin setting "
                    "market_defaults/default_broker_fee)")


# --- the shopping tab ---------------------------------------------------------------------------

def test_the_shopping_tab_has_no_future_build_stock_section():
    from streamlit_ui.components.daily_planner import tab_shopping

    source = inspect.getsource(tab_shopping)
    assert "Future Build Stock" not in source and "future_stock" not in source


# --- standings, VWAP days, retry budget and one settings read per page -------------------------
#
# Review fixes: the Jita owner's standings in the character's market fees,
# undefined names, the VWAP days read once per cycle, the per-page retry
# budget and one /admin_settings read per planner page render. Fake values.

FAKE_OWNER_CORP, FAKE_OWNER_FACTION = 1000999, 500999


class _Query:
    def __init__(self, row):
        self._row = row

    def filter_by(self, **kwargs):
        return self

    def first(self):
        return self._row


def _jita_character(session, standings):
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
    c = _jita_character(session, [{"from_type": "faction", "from_id": FAKE_OWNER_FACTION, "standing": 5.0},
                             {"from_type": "npc_corp", "from_id": FAKE_OWNER_CORP, "standing": 2.0}])
    c.refresh_market_fees()
    jita = c.market_fees["jita_4_4"]
    assert (jita["owner_corp_id"], jita["owner_faction_id"]) == (FAKE_OWNER_CORP, FAKE_OWNER_FACTION)
    assert jita["standings"] == {"faction_standing": 5.0, "corp_standing": 2.0}
    assert jita["rates"]["broker_fee_fraction"] == pytest.approx(0.03 - 0.015 - 0.0015 - 0.0004)


def test_an_unreadable_jita_owner_is_a_warning_not_silent(caplog):
    def query(model):
        raise OperationalError("SELECT", {}, Exception("no such table"))

    c = _jita_character(SimpleNamespace(query=query), [])
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
