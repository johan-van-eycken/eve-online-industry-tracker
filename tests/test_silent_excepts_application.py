"""Task C2: the formerly silent `except Exception` handlers in application/characters,
application/corporations and the rest of application (outside industry, daily_planner
and market_intelligence). Expected failures log a WARNING and, where a price is
missing, give the reason in the payload; programming errors propagate. Fake values."""
from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError
from eve_online_industry_tracker.infrastructure.esi_client import EsiRequestError
from eve_online_industry_tracker.application.market_analysis import pricing_suggestion_service as ps
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.application.industry import facilities
from eve_online_industry_tracker.application.industry.service import IndustryService
from fakes import _raiser, _db_error

_ORDERS = [{"character_id": 1, "corporation_id": 2, "market_orders": [
    {"order_id": 1, "type_id": 34, "is_buy_order": True, "price": 4.0, "issued": "2026-10-01T00:00:00Z",
     "duration": 90},
    {"order_id": 2, "type_id": 35, "is_buy_order": False, "price": 9.0, "issued": "2026-10-01T00:00:00Z",
     "duration": 90}]}]


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
        rows = enrich(monkeypatch, _Esi(books=EsiRequestError("ESI GET failed after retries")))
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
    svc = _locations([_char("A", _raiser(EsiRequestError("ESI GET failed after retries"))),
                      _char("B", lambda *a, **k: good)])
    with caplog.at_level(logging.WARNING):
        assert svc._resolve_structure_via_characters(1029999999999) == good
    assert any("via character A failed" in r.getMessage() for r in caplog.records)


def test_a_bug_in_the_structure_lookup_via_characters_propagates():
    svc = _locations([_char("A", _raiser(TypeError("esi_get() got an unexpected keyword argument")))])
    with pytest.raises(TypeError):
        svc._resolve_structure_via_characters(1029999999999)


# --- narrow excepts: DB and ESI errors only ----------------------------------------------------
#
# Round H2.4: the remaining except-Exception-with-a-warning sites catch
# SQLAlchemyError plus ESI_REQUEST_ERRORS only; a bug propagates as itself.
#
# pricing_suggestion_service (hub sell/buy price, cached order book levels),
# Character.refresh_market_fees, ESIClient.get_cached_data/save_to_cache and
# the startup ESI cache prune. Fake values only.

def _throw(exc):
    def _f(*a, **k):
        raise exc
    return _f


# --- pricing suggestion ----------------------------------------------------------------------------

def _pricing(price_map_fn=None, app_session_fn=None):
    svc = ps.PricingSuggestionService.__new__(ps.PricingSuggestionService)
    svc._market_pricing = SimpleNamespace(get_type_price_map=price_map_fn)
    svc._sessions = SimpleNamespace(app_session=app_session_fn)
    return svc


@pytest.mark.parametrize("method", ["_get_hub_price", "_get_hub_buy_price"])
def test_a_hub_price_esi_failure_is_a_warning(method, caplog):
    svc = _pricing(price_map_fn=_throw(EsiRequestError("ESI GET failed after retries")))
    with caplog.at_level(logging.WARNING):
        assert getattr(svc, method)(type_id=34) is None
    assert "ESI GET failed" in caplog.text


@pytest.mark.parametrize("method", ["_get_hub_price", "_get_hub_buy_price"])
def test_a_hub_price_bug_propagates(method):
    svc = _pricing(price_map_fn=_throw(TypeError("unsupported operand")))
    with pytest.raises(TypeError):
        getattr(svc, method)(type_id=34)


def test_an_order_book_levels_bug_propagates():
    svc = _pricing(app_session_fn=_throw(AttributeError("'NoneType' object has no attribute 'Session'")))
    with pytest.raises(AttributeError):
        svc._get_orderbook_levels(type_id=34)


# --- Character.refresh_market_fees -----------------------------------------------------------------

def _narrow_character():
    from eve_online_industry_tracker.application.characters.character import Character

    c = Character.__new__(Character)
    c._admin_settings = None
    c.skills = {"skills": [{"skill_id": 16622, "trained_skill_level": 4}]}
    c.standings = []
    c.character_name, c.character_id = "Pilot", 1
    c.market_fees = None
    c._db_sde = SimpleNamespace(session=SimpleNamespace(
        query=lambda model: SimpleNamespace(filter_by=lambda **k: SimpleNamespace(first=lambda: None))))
    return c


def test_a_failed_save_of_the_market_fees_is_a_warning(caplog):
    from eve_online_industry_tracker.application.characters.character import CharacterSaveError

    c = _narrow_character()
    c.save_character = _throw(CharacterSaveError("Failed to save character 'Pilot' to database. Error: locked"))
    with caplog.at_level(logging.WARNING):
        c.refresh_market_fees()
    assert "Failed to compute market fees" in caplog.text


def test_a_market_fees_bug_propagates(monkeypatch):
    from eve_online_industry_tracker.application.characters import character as character_module

    c = _narrow_character()
    c.save_character = lambda: None
    monkeypatch.setattr(character_module.market_fees, "sales_tax_fraction", _throw(TypeError("bug")))
    with pytest.raises(TypeError):
        c.refresh_market_fees()


def test_save_character_wraps_a_db_error_and_rolls_back():
    from eve_online_industry_tracker.application.characters.character import Character, CharacterSaveError

    c = Character.__new__(Character)
    c.character_name = "Pilot"
    session = MagicMock()
    session.query.side_effect = _db_error()
    c._db_app = SimpleNamespace(session=session)
    with pytest.raises(CharacterSaveError, match="Failed to save character 'Pilot'"):
        c.save_character()
    session.rollback.assert_called_once()


def test_save_character_lets_a_bug_propagate():
    from eve_online_industry_tracker.application.characters.character import Character

    c = Character.__new__(Character)
    c.character_name = "Pilot"
    session = MagicMock()
    session.query.side_effect = AttributeError("bug")
    c._db_app = SimpleNamespace(session=session)
    with pytest.raises(AttributeError):
        c.save_character()


# --- ESIClient cache helpers -----------------------------------------------------------------------

def _client():
    c = esi_client_module.ESIClient.__new__(esi_client_module.ESIClient)
    c.db_oauth = MagicMock()
    return c


def test_a_failed_cache_save_is_logged_and_rolled_back_quietly(caplog):
    c = _client()
    c.db_oauth.session.execute.side_effect = _db_error()
    c.db_oauth.session.rollback.side_effect = _db_error()   # a failing rollback does not hide the first error
    with caplog.at_level(logging.WARNING):
        c.save_to_cache("/x/", "etag", {"a": 1})
    assert "Error saving cache for /x/" in caplog.text
    assert "rollback" in caplog.text


def test_a_cache_save_bug_propagates():
    c = _client()
    c.db_oauth.session.execute.side_effect = AttributeError("bug")
    with pytest.raises(AttributeError):
        c.save_to_cache("/x/", "etag", {"a": 1})


# --- the startup ESI cache prune ---------------------------------------------------------------------

def test_a_failed_prune_is_a_warning_and_a_bug_propagates(monkeypatch, caplog):
    from utils import app_init

    session = MagicMock()
    db_oauth = SimpleNamespace(session=session, engine=None)
    cfg = SimpleNamespace(all=lambda: {"app": {"database_oauth_uri": "x", "database_app_uri": "y",
                                                "database_sde_uri": "z"}})
    monkeypatch.setattr(app_init, "init_db_oauth", lambda cfg: db_oauth)
    monkeypatch.setattr(app_init, "init_db_app", lambda cfg: SimpleNamespace(session=None))
    monkeypatch.setattr(app_init, "init_db_sde", lambda cfg: SimpleNamespace())
    from eve_online_industry_tracker.infrastructure import schema_migrations
    monkeypatch.setattr(schema_migrations, "ensure_app_schema", lambda db: None)
    from eve_online_industry_tracker.application.characters import corp_transfer_costs
    monkeypatch.setattr(corp_transfer_costs, "restamp_all_blended_corp_transfer_lots_once", lambda session: None)

    monkeypatch.setattr(esi_client_module, "prune_stale_esi_cache", _throw(_db_error()))
    with caplog.at_level(logging.WARNING):
        app_init.init_db_managers(cfg)
    assert "ESI cache cleanup failed" in caplog.text
    session.rollback.assert_called_once()

    monkeypatch.setattr(esi_client_module, "prune_stale_esi_cache", _throw(AttributeError("bug")))
    with pytest.raises(AttributeError):
        app_init.init_db_managers(cfg)


# --- warnings that had no test of their own ----------------------------------------------------
#
# F1 item 10: the warnings that had no test of their own. The public and
# corporation structure owner-name lookups (cosmetic: the structures keep
# their IDs), the market depth collector's plan-velocity read failing on its
# own (the sales-history fallback still answers) and the excluded blueprint
# type IDs entry that is not a type ID. Fake values throughout. (The job's
# ImportError path is in test_market_intelligence_job_start.py.)

SYSTEM = 30000001
CORP = 98000001


def _esi_names_failing():
    def _fail(ids):
        raise RuntimeError("ESI POST failed after retries: fake")

    return SimpleNamespace(get_universe_names=_fail)


def _service(state, sessions):
    svc = IndustryService.__new__(IndustryService)
    svc._state = state
    svc._sessions = sessions
    return svc


# --- the public structures' owner names -----------------------------------------------------------

def test_public_structure_owner_names_that_cannot_be_resolved_are_logged(monkeypatch, caplog):
    structures = [{"structure_id": 1035000000001, "type_id": 35825, "owner_id": CORP, "name": "Fake Raitaru"}]
    monkeypatch.setattr(facilities, "public_structures_cache_ttl_seconds", lambda: 3600)
    monkeypatch.setattr(facilities, "get_cached_public_structures", lambda **kw: (structures, True))
    monkeypatch.setattr(facilities, "get_type_data", lambda session, language, ids: {})
    svc = _service(SimpleNamespace(esi_service=_esi_names_failing(), db_sde=None),
                   SimpleNamespace(sde_session=lambda: None))
    with caplog.at_level(logging.WARNING):
        out, meta = svc.public_structures(system_id=SYSTEM)
    assert out[0]["owner_id"] == CORP and out[0]["owner_name"] is None       # keeps its owner ID
    assert "Public structures: owner names cannot be resolved" in caplog.text and "fake" in caplog.text
    assert meta == {"refreshing": False, "cache_fresh": True}


def test_a_programming_error_in_the_public_owner_lookup_propagates(monkeypatch):
    def _bug(ids):
        raise KeyError("fake bug")

    monkeypatch.setattr(facilities, "public_structures_cache_ttl_seconds", lambda: 3600)
    monkeypatch.setattr(facilities, "get_cached_public_structures",
                        lambda **kw: ([{"type_id": 35825, "owner_id": CORP}], True))
    monkeypatch.setattr(facilities, "get_type_data", lambda session, language, ids: {})
    svc = _service(SimpleNamespace(esi_service=SimpleNamespace(get_universe_names=_bug), db_sde=None),
                   SimpleNamespace(sde_session=lambda: None))
    with pytest.raises(KeyError):
        svc.public_structures(system_id=SYSTEM)


# --- the corporation structures' owner names ------------------------------------------------------

def test_corporation_structure_owner_names_that_cannot_be_resolved_are_logged(monkeypatch, caplog):
    rows = [SimpleNamespace(to_dict=lambda: {"structure_id": 1035000000002, "corporation_id": CORP})]
    monkeypatch.setattr(facilities, "corporation_structures_list_by_corporation_id", lambda session, cid: rows)
    character = SimpleNamespace(corporation_id=CORP)
    state = SimpleNamespace(char_manager=SimpleNamespace(get_character_by_id=lambda cid: character),
                            esi_service=_esi_names_failing(), db_sde=None)
    svc = _service(state, SimpleNamespace(app_session=lambda: None))
    with caplog.at_level(logging.WARNING):
        out = svc.corporation_structures(character_id=90000001)
    assert out[0]["corporation_id"] == CORP and "owner_name" not in out[0]      # keeps its corporation ID
    assert "Corporation structures: owner names cannot be resolved" in caplog.text and "fake" in caplog.text


# --- the market depth collector: the plan velocity read alone ---------------------------------------

def test_the_plan_velocity_read_failing_alone_warns_once_and_falls_back(monkeypatch, caplog):
    from eve_online_industry_tracker.application.industry import sales_history_service
    from eve_online_industry_tracker.application.market_intelligence.market_depth_collector import (
        MarketDepthCollector,
    )

    class _Session:
        def execute(self, *a, **k):
            raise OperationalError("SELECT fake", {}, Exception("fake plan table locked"))

        def close(self):
            pass

    class _Svc:
        def __init__(self, **k):
            pass

        def get_sold_history(self, **k):
            return [{"quantity": 60}]

    monkeypatch.setattr(sales_history_service, "SalesHistoryService", _Svc)
    c = MarketDepthCollector.__new__(MarketDepthCollector)
    c._state, c._admin = SimpleNamespace(), None
    c._sessions = SimpleNamespace(app_session=lambda: _Session())
    with caplog.at_level(logging.WARNING):
        assert c._get_effective_velocity(34, {CORP}) == pytest.approx(2.0)     # 60 units / 30 days
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "plan effective_velocity" in warnings[0] and "fake plan table locked" in warnings[0]
    assert "type_id=34" in warnings[0]


# --- the excluded blueprint type IDs -----------------------------------------------------------------

def test_an_excluded_blueprint_entry_that_is_not_a_type_id_is_logged_and_ignored(caplog):
    from eve_online_industry_tracker.application.industry.job_manager import IndustryJobManager

    cfg = {"defaults": {"industry": {"excluded_blueprint_type_ids": [1001, "1002", "not-an-id", None]}}}
    mgr = IndustryJobManager.__new__(IndustryJobManager)
    mgr._state = SimpleNamespace(cfg_manager=SimpleNamespace(all=lambda: cfg))
    with caplog.at_level(logging.WARNING):
        assert mgr._excluded_blueprint_type_ids() == {1001, 1002}
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(messages) == 2
    assert any("'not-an-id'" in m for m in messages) and any("None" in m for m in messages)
    assert all("excluded_blueprint_type_ids" in m for m in messages)
