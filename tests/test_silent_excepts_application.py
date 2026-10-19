"""Task C2: the formerly silent `except Exception` handlers in application/characters,
application/corporations and the rest of application (outside industry, daily_planner
and market_intelligence). Expected failures log a WARNING and, where a price is
missing, give the reason in the payload; programming errors propagate. Fake values."""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

_ORDERS = [{"character_id": 1, "corporation_id": 2, "market_orders": [
    {"order_id": 1, "type_id": 34, "is_buy_order": True, "price": 4.0, "issued": "2026-10-01T00:00:00Z",
     "duration": 90},
    {"order_id": 2, "type_id": 35, "is_buy_order": False, "price": 9.0, "issued": "2026-10-01T00:00:00Z",
     "duration": 90}]}]


def _raiser(exc):
    def _f(*a, **k):
        raise exc
    return _f


class _Esi:
    def __init__(self, *, books=None, location=None):
        self._books, self._location = books, location

    def get_type_buyprices(self, type_ids):
        if self._books:
            raise self._books
        return {}

    def get_type_sellprices(self, type_ids):
        if self._books:
            raise self._books
        return {}

    def get_location_info(self, location_id):
        if self._location:
            raise self._location
        return {}


def _patch_market_services(monkeypatch, module, *, suggest=None):
    monkeypatch.setattr(module.MarketHistoryService, "fetch_and_store_history", lambda self, **k: None)
    monkeypatch.setattr(module.PricingSuggestionService, "suggest_price",
                        suggest or (lambda self, **k: {"advised_price": 8.5}))


def _char_state(esi):
    return SimpleNamespace(esi_service=esi, char_manager=SimpleNamespace(get_market_orders=lambda refresh: _ORDERS),
                           admin_settings=None, db_app=None, db_sde=None, db_oauth=None)


def _corp_state(esi):
    return SimpleNamespace(
        esi_service=esi, corp_manager=SimpleNamespace(get_market_orders=lambda corporation_id, refresh: _ORDERS),
        char_manager=SimpleNamespace(get_corp_director=lambda: SimpleNamespace(character_id=1),
                                     get_main_character=lambda: None),
        admin_settings=None, db_app=None, db_sde=None, db_oauth=None)


def _enriched_character_orders(monkeypatch, esi, **kw):
    from eve_online_industry_tracker.application.characters import service as module

    _patch_market_services(monkeypatch, module, **kw)
    return module.CharactersService(state=_char_state(esi)).get_market_orders_enriched()


def _enriched_corporation_orders(monkeypatch, esi, **kw):
    from eve_online_industry_tracker.application.corporations import service as module
    from eve_online_industry_tracker.application.market_analysis import market_history_service as mh
    from eve_online_industry_tracker.application.market_analysis import pricing_suggestion_service as ps

    # corporations.service imports both services inside the method.
    monkeypatch.setattr(mh.MarketHistoryService, "fetch_and_store_history", lambda self, **k: None)
    monkeypatch.setattr(ps.PricingSuggestionService, "suggest_price",
                        kw.get("suggest") or (lambda self, **k: {"advised_price": 8.5}))
    return module.CorporationsService(state=_corp_state(esi)).get_market_orders_enriched(corporation_id=2)


ENRICHERS = [_enriched_character_orders, _enriched_corporation_orders]
IDS = ["character", "corporation"]


@pytest.mark.parametrize("enrich", ENRICHERS, ids=IDS)
def test_an_unavailable_order_book_is_a_warning_and_the_status_stays_na(enrich, monkeypatch, caplog):
    with caplog.at_level(logging.WARNING):
        rows = enrich(monkeypatch, _Esi(books=RuntimeError("ESI GET failed after retries")))
    assert {r["price_status"] for r in rows} == {"⚪N/A"}
    assert any("order book unavailable" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("enrich", ENRICHERS, ids=IDS)
def test_a_bug_behind_the_order_book_propagates(enrich, monkeypatch):
    with pytest.raises(AttributeError):
        enrich(monkeypatch, _Esi(books=AttributeError("'NoneType' object has no attribute 'get'")))


@pytest.mark.parametrize("enrich", ENRICHERS, ids=IDS)
def test_a_failed_pricing_suggestion_gives_its_reason_on_the_order(enrich, monkeypatch, caplog):
    suggest = _raiser(OperationalError("SELECT", {}, Exception("database is locked")))
    with caplog.at_level(logging.WARNING):
        rows = enrich(monkeypatch, _Esi(), suggest=suggest)
    sell = next(r for r in rows if r["type_id"] == 35)
    assert "advised_price" not in sell
    assert "database is locked" in sell["pricing_error"]
    assert any("Pricing suggestion failed" in r.getMessage() for r in caplog.records)
    buy = next(r for r in rows if r["type_id"] == 34)
    assert "pricing_error" not in buy


@pytest.mark.parametrize("enrich", ENRICHERS, ids=IDS)
def test_a_bug_in_the_pricing_suggestion_propagates(enrich, monkeypatch):
    with pytest.raises(NameError):
        enrich(monkeypatch, _Esi(), suggest=_raiser(NameError("name 'oops' is not defined")))


@pytest.mark.parametrize("enrich", ENRICHERS, ids=IDS)
def test_a_working_pricing_suggestion_has_no_error(enrich, monkeypatch):
    rows = enrich(monkeypatch, _Esi())
    sell = next(r for r in rows if r["type_id"] == 35)
    assert sell["advised_price"] == 8.5 and "pricing_error" not in sell


# --- skills and standings rows ---------------------------------------------------------------

def _character(skills=None, standings=None):
    from eve_online_industry_tracker.application.characters.character import Character

    c = Character.__new__(Character)
    c.character_name = "Pilot"
    c.skills, c.standings = skills, standings
    return c


def test_an_unreadable_skill_row_is_a_warning_and_the_next_row_still_counts(caplog):
    c = _character(skills={"skills": [{"skill_id": 3446, "trained_skill_level": "five"},
                                      {"skill_id": 3446, "trained_skill_level": 4}]})
    with caplog.at_level(logging.WARNING):
        assert c._get_skill_level(skill_id=3446) == 4
    assert any("Unreadable skill row" in r.getMessage() for r in caplog.records)


def test_an_unreadable_standing_row_is_a_warning(caplog):
    c = _character(standings=[{"from_type": "faction", "from_id": 500001, "standing": "high"}])
    with caplog.at_level(logging.WARNING):
        assert c._get_standing(from_type="faction", from_id=500001) == 0.0
    assert any("Unreadable standing row" in r.getMessage() for r in caplog.records)


# --- pricing suggestion: the configured target margin --------------------------------------

def test_an_unreadable_min_target_margin_is_a_warning_and_uses_the_default(caplog):
    from eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service import (
        PricingSuggestionService,
    )

    svc = PricingSuggestionService.__new__(PricingSuggestionService)
    cfg = {"defaults": {"market_advisory": {"min_target_margin_pct": "ten"}}}
    svc._state = SimpleNamespace(cfg_manager=SimpleNamespace(all=lambda: cfg))
    with caplog.at_level(logging.WARNING):
        assert svc._get_min_target_margin() == 0.08
    assert any("min_target_margin_pct" in r.getMessage() for r in caplog.records)


# --- structure lookup via characters --------------------------------------------------------

def _locations(chars):
    from eve_online_industry_tracker.application.locations.service import LocationsService

    svc = LocationsService.__new__(LocationsService)
    svc._state = SimpleNamespace(esi_service=SimpleNamespace(_esi_client=object()),
                                 char_manager=SimpleNamespace(get_characters=lambda: chars))
    return svc


def _char(name, esi_get):
    return SimpleNamespace(character_name=name, ensure_esi=lambda: None, esi_client=SimpleNamespace(esi_get=esi_get))


def test_a_failed_structure_lookup_via_one_character_warns_and_tries_the_next(caplog):
    good = {"name": "Fake Keepstar", "solar_system_id": 30000142}
    svc = _locations([_char("A", _raiser(RuntimeError("ESI GET failed after retries"))),
                      _char("B", lambda *a, **k: good)])
    with caplog.at_level(logging.WARNING):
        assert svc._resolve_structure_via_characters(1029999999999) == good
    assert any("via character A failed" in r.getMessage() for r in caplog.records)


def test_a_bug_in_the_structure_lookup_via_characters_propagates():
    svc = _locations([_char("A", _raiser(TypeError("esi_get() got an unexpected keyword argument")))])
    with pytest.raises(TypeError):
        svc._resolve_structure_via_characters(1029999999999)
