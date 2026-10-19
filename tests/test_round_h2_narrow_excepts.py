"""Round H2.4: the remaining except-Exception-with-a-warning sites catch
SQLAlchemyError plus ESI_REQUEST_ERRORS only; a bug propagates as itself.

pricing_suggestion_service (hub sell/buy price, cached order book levels),
Character.refresh_market_fees, ESIClient.get_cached_data/save_to_cache and
the startup ESI cache prune. Fake values only.
"""
from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application.market_analysis import pricing_suggestion_service as ps
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure.esi_client import EsiRequestError


def _db_error():
    return OperationalError("SELECT", {}, Exception("database is locked"))


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

def _character():
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

    c = _character()
    c.save_character = _throw(CharacterSaveError("Failed to save character 'Pilot' to database. Error: locked"))
    with caplog.at_level(logging.WARNING):
        c.refresh_market_fees()
    assert "Failed to compute market fees" in caplog.text


def test_a_market_fees_bug_propagates(monkeypatch):
    from eve_online_industry_tracker.application.characters import character as character_module

    c = _character()
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
    monkeypatch.setattr(app_init, "init_db_app", lambda cfg: SimpleNamespace())
    monkeypatch.setattr(app_init, "init_db_sde", lambda cfg: SimpleNamespace())
    from eve_online_industry_tracker.infrastructure import schema_migrations
    monkeypatch.setattr(schema_migrations, "ensure_app_schema", lambda db: None)

    monkeypatch.setattr(esi_client_module, "prune_stale_esi_cache", _throw(_db_error()))
    with caplog.at_level(logging.WARNING):
        app_init.init_db_managers(cfg)
    assert "ESI cache cleanup failed" in caplog.text
    session.rollback.assert_called_once()

    monkeypatch.setattr(esi_client_module, "prune_stale_esi_cache", _throw(AttributeError("bug")))
    with pytest.raises(AttributeError):
        app_init.init_db_managers(cfg)
