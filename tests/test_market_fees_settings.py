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

import pytest

from eve_online_industry_tracker.application import market_fees
from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager


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
    c._db_sde = SimpleNamespace(session=SimpleNamespace(query=lambda *a: (_ for _ in ()).throw(RuntimeError("no SDE"))))
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
