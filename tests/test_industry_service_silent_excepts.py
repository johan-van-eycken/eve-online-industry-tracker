"""IndustryService no longer swallows every exception. A failed read
or an unparsable value that feeds a cost, fee, bonus, skill or price logs a
WARNING with the reason (and names it in the payload where the data
matters); a programming error (AttributeError, KeyError, an unexpected
exception type) propagates. Fake values throughout."""
from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.application.industry.service import IndustryService
from industry_service_fakes import _profile_service


def _service(**state):
    svc = IndustryService(state=SimpleNamespace(**state))
    return svc


# --- structure bonuses (SDE typeBonus JSON) ----------------------------------------------------

def test_unparsable_structure_role_bonuses_are_logged(caplog):
    with caplog.at_level(logging.WARNING):
        bonuses = IndustryService._extract_structure_industry_bonuses_from_type_bonus(
            {"id": 35825, "roleBonuses": "{not json"}, language="en")
    assert bonuses["material_reduction"] == 0.0
    assert "roleBonuses" in caplog.text and "35825" in caplog.text


def test_a_non_numeric_structure_bonus_is_logged(caplog):
    role = json.dumps([{"bonus": "lots", "bonusText": {"en": "reduction in manufacturing material"}}])
    with caplog.at_level(logging.WARNING):
        IndustryService._extract_structure_industry_bonuses_from_type_bonus(
            {"id": 35825, "roleBonuses": role}, language="en")
    assert "lots" in caplog.text


# --- job fee inputs ------------------------------------------------------------------------------

def test_a_non_numeric_system_cost_index_is_logged(caplog):
    profile = {"system_id": 30000001,
               "system_cost_indices": [{"activity": "manufacturing", "cost_index": "n/a"}]}
    with caplog.at_level(logging.WARNING):
        assert IndustryService._system_cost_index(profile_payload=profile, activity="manufacturing") is None
    assert "cost index" in caplog.text and "n/a" in caplog.text and "30000001" in caplog.text


def test_a_non_numeric_eiv_quantity_is_logged(caplog):
    entries = [{"type_id": 34, "quantity": "many"}]
    prices = {34: {"adjusted_price": 5.0}}
    with caplog.at_level(logging.WARNING):
        IndustryService._sum_estimated_item_value_counted(entries, quantity_key="quantity",
                                                          adjusted_price_map=prices)
    assert "many" in caplog.text and "type_id=34" in caplog.text


def test_a_non_numeric_preferred_value_quantity_is_logged(caplog):
    entries = [{"type_id": 34, "quantity": "many", "unit_price": 5.0}]
    with caplog.at_level(logging.WARNING):
        IndustryService._sum_preferred_item_value(entries, quantity_key="quantity", sell_price_map=None,
                                                  adjusted_price_map={})
    assert "many" in caplog.text and "type_id=34" in caplog.text


# --- the profile context ---------------------------------------------------------------------------

def test_an_unavailable_cost_index_is_logged_and_named_in_the_profile(app_session, caplog):
    svc, profile_id = _profile_service(app_session, system_id=30000001)

    def _raise(**kw):
        raise ServiceError("ESI service not initialized.", status_code=503)

    svc.industry_system_cost_index = _raise
    with caplog.at_level(logging.WARNING):
        payload = svc._resolve_industry_profile_context(character_id=None, industry_profile_id=profile_id)
    assert payload["system_cost_indices"] == []
    assert "ESI service not initialized" in payload["industry_system"]["lookup_error"]
    assert "30000001" in caplog.text and "ESI service not initialized" in caplog.text


def test_a_programming_error_in_the_cost_index_lookup_propagates(app_session):
    svc, profile_id = _profile_service(app_session, system_id=30000001)

    def _raise(**kw):
        raise AttributeError("fake bug")

    svc.industry_system_cost_index = _raise
    with pytest.raises(AttributeError):
        svc._resolve_industry_profile_context(character_id=None, industry_profile_id=profile_id)


def test_an_unavailable_facility_is_logged_and_named_in_the_profile(app_session, caplog):
    svc, profile_id = _profile_service(app_session, facility_id=60000001, facility_tax=0.1)

    def _raise(**kw):
        raise RuntimeError("ESI GET failed after retries: fake")

    svc.industry_facility = _raise
    with caplog.at_level(logging.WARNING):
        payload = svc._resolve_industry_profile_context(character_id=None, industry_profile_id=profile_id)
    assert payload["facility"]["tax"] == 0.1
    assert "fake" in payload["facility"]["lookup_error"]
    assert "60000001" in caplog.text


# --- skills and slots ------------------------------------------------------------------------------


def test_slot_capacities_without_characters_are_empty():
    def _none():
        raise ValueError("No characters available in the manager.")

    assert _service(char_manager=SimpleNamespace(get_characters=_none))._character_slot_capacities() == {}


def test_slot_capacities_propagate_a_character_read_failure():
    from eve_online_industry_tracker.application.characters.character_manager import CharacterDataError

    def _boom():
        raise CharacterDataError("Error retrieving characters: fake")   # what get_characters raises

    with pytest.raises(CharacterDataError):
        _service(char_manager=SimpleNamespace(get_characters=_boom))._character_slot_capacities()


# --- market history -------------------------------------------------------------------------------

class _FailingHistory:
    def __init__(self, *, fetch_error=None, stats_error=None):
        self._fetch_error, self._stats_error = fetch_error, stats_error

    def get_price_stats(self, **kw):
        if self._stats_error:
            raise self._stats_error
        return {"has_data": False}

    def fetch_and_store_history(self, **kw):
        raise self._fetch_error


def _db_error():
    return OperationalError("SELECT fake", {}, Exception("fake db down"))


def test_unreadable_price_stats_for_trends_are_logged(app_session, caplog):
    svc = IndustryService(state=SimpleNamespace(esi_service=None, admin_settings=None),
                          sessions=SimpleNamespace(app_session=lambda: app_session))
    rows = [{"type_id": 34, "manufacturing_job": {}}]
    with caplog.at_level(logging.WARNING):
        svc._enrich_product_rows_with_market_trends(rows, market_history_svc=_FailingHistory(stats_error=_db_error()))
    assert "fake db down" in caplog.text and "type_id=34" in caplog.text


def _anomaly_service(monkeypatch, history):
    from eve_online_industry_tracker.application.industry import market_enrichment

    class _Pricing:
        def __init__(self, **kw):
            pass

        def _market_hub_context(self, hub):
            return {"region_id": 10000002}

    monkeypatch.setattr(market_enrichment, "MarketPricingService", _Pricing)
    monkeypatch.setattr(market_enrichment, "MarketHistoryService", lambda **kw: history)
    return _service(esi_service=None, admin_settings=None)


def test_a_failed_history_fetch_for_the_price_anomaly_is_logged(monkeypatch, caplog):
    svc = _anomaly_service(monkeypatch, _FailingHistory(fetch_error=RuntimeError("ESI GET failed after retries")))
    svc._sessions = None
    with caplog.at_level(logging.WARNING):
        svc._enrich_product_rows_with_price_anomaly([{"type_id": 34}], market_hub="jita")
    assert "ESI GET failed" in caplog.text and "type_id=34" in caplog.text


def test_a_programming_error_in_the_history_fetch_propagates(monkeypatch):
    svc = _anomaly_service(monkeypatch, _FailingHistory(fetch_error=AttributeError("fake bug")))
    svc._sessions = None
    with pytest.raises(AttributeError):
        svc._enrich_product_rows_with_price_anomaly([{"type_id": 34}], market_hub="jita")
