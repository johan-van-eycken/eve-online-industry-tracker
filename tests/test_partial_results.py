"""Round J1.8: a read over several characters or corporations keeps the data
of every one that worked and names each that failed (PartialResult.errors,
the reply's `errors`, one st.warning per failed owner on the pages). Only
when every one fails is it an error. The corporation getters are covered in
test_round_j1_unknown_corporation.py. Fake values only.
"""
from __future__ import annotations

from collections import Counter
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.batch import PartialResult, batch_errors
from eve_online_industry_tracker.application.characters.character_manager import (
    CharacterManager,
    CharacterRefreshError,
)


class _Char:
    def __init__(self, name, character_id, fail=()):
        self.character_name = name
        self.character_id = character_id
        self.fail = set(fail)
        self.calls = Counter()

    def ensure_esi(self):
        pass

    def _do(self, kind):
        self.calls[kind] += 1
        if kind in self.fail:
            raise RuntimeError(f"{kind} down")
        return {"character_id": self.character_id, "character_name": self.character_name, "wallet_balance": 5.0}

    def refresh_wallet_balance(self):
        self._do("refresh_wallet_balance")

    def refresh_assets(self):
        self._do("refresh_assets")

    def refresh_market_orders(self):
        self._do("refresh_market_orders")

    def get_wallet_balance(self):
        return self._do("get_wallet_balance")

    def get_assets(self):
        return self._do("get_assets")

    def get_market_orders(self):
        return self._do("get_market_orders")


def _manager(*chars):
    mgr = CharacterManager.__new__(CharacterManager)
    mgr._character_list = list(chars)
    mgr.init_errors = []
    mgr._rebuild_indexes()
    return mgr


_GETTERS = [
    ("get_wallet_balances", "refresh_wallet_balance"),
    ("get_wallet_balances", "get_wallet_balance"),
    ("get_assets", "refresh_assets"),
    ("get_assets", "get_assets"),
    ("get_market_orders", "refresh_market_orders"),
    ("get_market_orders", "get_market_orders"),
]


# --- the character getters ------------------------------------------------------------------

@pytest.mark.parametrize("method,failing", _GETTERS)
def test_a_character_getter_keeps_the_others_data_and_names_each_failure(method, failing):
    a, b, c = _Char("Alpha", 1, fail={failing}), _Char("Bravo", 2), _Char("Charlie", 3, fail={failing})
    result = getattr(_manager(a, b, c), method)()
    assert isinstance(result, PartialResult)
    assert [r["character_id"] for r in result] == [2]
    assert set(result.errors) == {"Alpha", "Charlie"}
    assert all(f"{failing} down" in message for message in result.errors.values())


@pytest.mark.parametrize("method,failing", _GETTERS)
def test_a_character_getter_whose_every_character_fails_raises_naming_each(method, failing):
    with pytest.raises(CharacterRefreshError) as info:
        getattr(_manager(_Char("Alpha", 1, fail={failing}), _Char("Bravo", 2, fail={failing})), method)()
    assert set(info.value.errors) == {"Alpha", "Bravo"}


def test_a_character_whose_refresh_failed_is_not_read():
    a, b = _Char("Alpha", 1, fail={"refresh_assets"}), _Char("Bravo", 2)
    result = _manager(a, b).get_assets()
    assert a.calls == Counter({"refresh_assets": 1})
    assert b.calls == Counter({"refresh_assets": 1, "get_assets": 1})
    assert set(result.errors) == {"Alpha"}


def test_no_failure_has_no_errors():
    result = _manager(_Char("Alpha", 1), _Char("Bravo", 2)).get_assets()
    assert len(result) == 2 and result.errors == {} and batch_errors(result) == {}
    assert batch_errors([1, 2]) == {}


# --- the routes: `errors` next to the data ---------------------------------------------------

@pytest.fixture
def client():
    from flask_app.app import create_app
    return create_app().test_client()


def _ready(monkeypatch, module, **state):
    monkeypatch.setattr(module, "require_ready", lambda s: None)
    monkeypatch.setattr(module, "get_state", lambda: SimpleNamespace(**state))


def test_the_wallet_balances_route_names_each_failed_character(monkeypatch, client):
    import flask_app.routes.characters as routes

    mgr = _manager(_Char("Alpha", 1, fail={"refresh_wallet_balance"}), _Char("Bravo", 2))
    _ready(monkeypatch, routes, char_manager=mgr)
    body = client.get("/characters/wallet_balances").get_json()
    assert body["status"] == "success"
    assert [r["character_name"] for r in body["data"]] == ["Bravo"]
    assert body["errors"] == {"Alpha": "RuntimeError: refresh_wallet_balance down"}


def test_a_route_without_failures_has_no_errors_field(monkeypatch, client):
    import flask_app.routes.characters as routes

    _ready(monkeypatch, routes, char_manager=_manager(_Char("Alpha", 1), _Char("Bravo", 2)))
    body = client.get("/characters/wallet_balances").get_json()
    assert len(body["data"]) == 2 and "errors" not in body


def test_a_route_whose_every_character_fails_is_an_error(monkeypatch, client):
    import flask_app.routes.characters as routes

    _ready(monkeypatch, routes, char_manager=_manager(_Char("Alpha", 1, fail={"get_wallet_balance"})))
    response = client.get("/characters/wallet_balances")
    assert response.status_code == 500
    assert "Alpha" in response.get_json()["message"]


def test_the_corporation_assets_route_names_each_failed_corporation(monkeypatch, client):
    import flask_app.routes.corporations as routes

    corp_mgr = SimpleNamespace(get_assets=lambda corporation_id=None: PartialResult(
        [{"corporation_id": 2, "assets": []}], {"Acme": "RuntimeError: ESI down"}))
    _ready(monkeypatch, routes, corp_manager=corp_mgr)
    body = client.get("/corporations/assets").get_json()
    assert body["data"] == [{"corporation_id": 2, "assets": []}]
    assert body["errors"] == {"Acme": "RuntimeError: ESI down"}


@pytest.mark.parametrize("module_path,manager_attr,path", [
    ("eve_online_industry_tracker.application.characters.service", "char_manager", "/characters/market_orders?compare=0"),
    ("eve_online_industry_tracker.application.corporations.service", "corp_manager", "/corporations/7/market_orders/enriched"),
])
def test_the_enriched_market_orders_carry_the_errors(monkeypatch, client, module_path, manager_attr, path):
    import importlib

    import flask_app.routes.characters as char_routes
    import flask_app.routes.corporations as corp_routes

    service_module = importlib.import_module(module_path)
    monkeypatch.setattr(service_module, "PricingSuggestionService", lambda state: None, raising=False)
    monkeypatch.setattr(service_module, "fetch_order_books", lambda *a, **k: ({}, {}), raising=False)
    monkeypatch.setattr(service_module, "refresh_market_history", lambda *a, **k: {}, raising=False)
    import eve_online_industry_tracker.application.market_analysis.pricing_suggestion_service as pss
    import eve_online_industry_tracker.application.market_analysis.market_history_service as mhs
    monkeypatch.setattr(pss, "PricingSuggestionService", lambda state: None)
    monkeypatch.setattr(mhs, "MarketHistoryService", lambda state: None)
    monkeypatch.setattr(service_module, "MarketHistoryService", lambda state: None, raising=False)

    manager = SimpleNamespace(get_market_orders=lambda **k: PartialResult(
        [{"market_orders": []}], {"Alpha": "RuntimeError: ESI down"}))
    director = SimpleNamespace(character_id=1)
    state = {manager_attr: manager, "esi_service": None,
             "char_manager": SimpleNamespace(get_corp_director=lambda: director, get_main_character=lambda: director)}
    if manager_attr == "char_manager":
        state["char_manager"] = manager
    for module in (char_routes, corp_routes):
        _ready(monkeypatch, module, **state)
    body = client.get(path).get_json()
    assert body["status"] == "success", body
    assert body["data"] == [] and body["errors"] == {"Alpha": "RuntimeError: ESI down"}


# --- the pages: one st.warning per failed owner ----------------------------------------------

def test_show_batch_errors_warns_once_per_failed_owner(monkeypatch):
    from streamlit_ui.components import batch_errors as component

    shown = []
    monkeypatch.setattr(component, "st", SimpleNamespace(warning=lambda body: shown.append(body)))
    component.show_batch_errors({"status": "success", "data": [], "errors": {"Bolt": "bad", "Acme": "ESI down"}},
                                "Corporation")
    assert shown == ["Corporation Acme failed: ESI down. Its data is not shown.",
                     "Corporation Bolt failed: bad. Its data is not shown."]
    shown.clear()
    component.show_batch_errors({"status": "success", "data": []}, "Corporation")
    component.show_batch_errors(None, "Corporation")
    assert shown == []


def test_the_market_orders_api_refresh_returns_the_failed_owners(monkeypatch):
    from streamlit_ui.api import market_orders as api

    monkeypatch.setattr(api, "api_get_or_error", lambda path, timeout_seconds=None: (
        {"status": "success", "data": [], "errors": {"Alpha": "ESI down"}}, None))
    assert api.refresh_market_orders() == {"Alpha": "ESI down"}
    assert api.refresh_corp_market_orders(7) == {"Alpha": "ESI down"}


def test_the_market_orders_page_names_the_failed_characters_and_refreshes(monkeypatch):
    import streamlit as st
    import streamlit_ui.api.corporations as corps
    from streamlit_ui.pages import marketorders as page

    shown = []
    monkeypatch.setattr(st, "warning", lambda body, *a, **k: shown.append(str(body)))
    monkeypatch.setattr(st, "error", lambda body, *a, **k: shown.append("ERROR " + str(body)))
    session: dict = {}
    monkeypatch.setattr(st, "session_state", session)
    monkeypatch.setattr(corps, "fetch_director_corporations", lambda: [])
    monkeypatch.setattr(page, "_get_director_corp_ids", lambda: [], raising=False)
    monkeypatch.setattr(page, "fetch_market_orders", lambda: {"data": [
        {"order_id": 1, "type_name": "Tritanium", "price": 5.0, "is_buy_order": False, "owner": "Pilot"}],
        "errors": {"Bravo": "read failed"}})
    monkeypatch.setattr(page, "refresh_market_orders", lambda: {"Charlie": "ESI down"})
    for name in ("clear_market_orders_cache", "clear_corp_market_orders_cache", "_rerun",
                 "_render_orders_grid", "_render_pricing_analysis"):
        monkeypatch.setattr(page, name, lambda *a, **k: None)
    monkeypatch.setattr(st, "button", lambda *a, **k: True)

    page._render_market_orders(runtime=None, img_renderer=None)
    assert any("Character Bravo failed: read failed" in w for w in shown)
    assert session[page._REFRESH_ERRORS_KEY] == {"Character": {"Charlie": "ESI down"}}

    shown.clear()
    monkeypatch.setattr(st, "button", lambda *a, **k: False)
    page._render_market_orders(runtime=None, img_renderer=None)   # the rerun after the refresh
    assert any("Refresh: Character Charlie failed: ESI down" in w for w in shown)
    assert page._REFRESH_ERRORS_KEY not in session
