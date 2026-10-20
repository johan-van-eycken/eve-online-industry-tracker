"""Round J1.7: the corporation side mirrors the character side.

- get_corporation and _refresh_batch raise UnknownCorporationError (a
  CorporationRefreshError naming the unknown id or name) instead of
  returning None or refreshing nothing; the routes answer 404
  (code unknown_corporation).
- _refresh_batch tries every corporation, then one CorporationRefreshError
  names each failed corporation.
- The getters (assets, members, structures, market orders) try every
  corporation and then name each failure, instead of logging and returning
  the others' data as if complete.
Fake values only.
"""
from __future__ import annotations

from collections import Counter
from types import SimpleNamespace

import pytest

import flask_app.bootstrap as bootstrap
from flask_app.state import AppState
from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.corporations.corporation_manager import (
    CorporationManager,
    CorporationRefreshError,
    UnknownCorporationError,
)
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module


class _Corp:
    def __init__(self, corporation_id, name, fail=()):
        self.corporation_id = corporation_id
        self.corporation_name = name
        self.fail = set(fail)
        self.calls = Counter()

    def _do(self, kind, result=None):
        self.calls[kind] += 1
        if kind in self.fail:
            raise RuntimeError(f"{kind} down")
        return result

    def __getattr__(self, kind):
        if kind.startswith("refresh_"):
            return lambda: self._do(kind)
        if kind.startswith("get_"):
            return lambda: self._do(kind, {"corporation_id": self.corporation_id, "kind": kind})
        raise AttributeError(kind)


def _manager(*corps):
    mgr = CorporationManager.__new__(CorporationManager)
    mgr._corporations = list(corps)
    mgr._corp_by_id = {c.corporation_id: c for c in corps}
    mgr._corp_by_name = {c.corporation_name.lower(): c for c in corps}
    mgr._corporation_ids = set(mgr._corp_by_id)
    mgr._char_manager = SimpleNamespace(_character_list=[])
    mgr.init_errors = []
    return mgr


# --- unknown id or name ----------------------------------------------------------------------

def test_get_corporation_names_an_unknown_id_and_name():
    mgr = _manager(_Corp(98000001, "Acme"))
    assert mgr.get_corporation(corporation_id=98000001).corporation_name == "Acme"
    assert mgr.get_corporation(corporation_name="acme").corporation_id == 98000001
    with pytest.raises(UnknownCorporationError) as info:
        mgr.get_corporation(corporation_id=98009999)
    assert isinstance(info.value, CorporationRefreshError)
    assert list(info.value.errors) == ["corporation_id 98009999"]
    with pytest.raises(UnknownCorporationError, match="Nobody"):
        mgr.get_corporation(corporation_name="Nobody")


@pytest.mark.parametrize("call", [
    lambda m: m.refresh_all(corporation_id=98009999),
    lambda m: m.refresh_assets(corporation_name="Nobody"),
    lambda m: m.refresh_market_orders(corporation_id=98009999),
    lambda m: m.refresh_realized_profit_inputs(corporation_id=98009999),
    lambda m: m.get_assets(corporation_id=98009999),
    lambda m: m.get_market_orders(corporation_id=98009999, refresh=True),
    lambda m: m.get_market_orders(corporation_id=98009999),
])
def test_an_unknown_corporation_refreshes_nothing_and_is_named(call):
    corp = _Corp(98000001, "Acme")
    with pytest.raises(UnknownCorporationError):
        call(_manager(corp))
    assert corp.calls == Counter()


def test_the_name_lookup_by_character_keeps_none_for_a_non_director_corporation():
    mgr = _manager(_Corp(98000001, "Acme"))
    mgr._char_manager = SimpleNamespace(_character_list=[SimpleNamespace(character_id=1, corporation_id=98000002)])
    assert mgr.get_corporation_name_by_character_id(1) is None


# --- every corporation tried, each failure named -------------------------------------------

def test_the_batch_refresh_tries_every_corporation_and_names_each_failure():
    a, b, c = _Corp(1, "Acme", fail={"refresh_all"}), _Corp(2, "Bolt"), _Corp(3, "Crux", fail={"refresh_all"})
    with pytest.raises(CorporationRefreshError) as info:
        _manager(a, b, c).refresh_all()
    assert a.calls["refresh_all"] == b.calls["refresh_all"] == c.calls["refresh_all"] == 1
    assert set(info.value.errors) == {"Acme", "Crux"}
    assert "refresh_all down" in str(info.value)


def test_the_realized_profit_inputs_refresh_every_kind_and_name_each_failure():
    a = _Corp(1, "Acme", fail={"refresh_wallet_journal"})
    b = _Corp(2, "Bolt", fail={"refresh_assets"})
    with pytest.raises(CorporationRefreshError) as info:
        _manager(a, b).refresh_realized_profit_inputs()
    for corp in (a, b):
        assert all(corp.calls[k] == 1 for k in (
            "refresh_wallet_transactions", "refresh_wallet_journal", "refresh_industry_jobs", "refresh_assets"))
    assert set(info.value.errors) == {"Acme (refresh_wallet_journal)", "Bolt (refresh_assets)"}


_GETTER_FAILURES = [
    ("get_assets", "get_assets"),
    ("get_assets", "refresh_assets"),
    ("get_members", "get_members"),
    ("get_members", "refresh_members"),
    ("get_structures", "get_structures"),
    ("get_structures", "refresh_structures"),
    ("get_market_orders", "get_market_orders"),
]


@pytest.mark.parametrize("method,failing", _GETTER_FAILURES)
def test_a_getter_keeps_the_others_data_and_names_each_failure(method, failing):
    # Round J1.8: the corporations that worked are returned; result.errors names each that failed.
    a, b, c = _Corp(1, "Acme", fail={failing}), _Corp(2, "Bolt"), _Corp(3, "Crux", fail={failing})
    result = getattr(_manager(a, b, c), method)()
    assert set(result.errors) == {"Acme", "Crux"}
    assert all(f"{failing} down" in message for message in result.errors.values())
    assert b.calls[method] == 1
    if method in ("get_assets", "get_market_orders"):
        assert [r["corporation_id"] for r in result] == [2]
    else:   # members and structures are flattened: Bolt's entries only
        assert list(result) == list({"corporation_id": 2, "kind": method})


@pytest.mark.parametrize("method,failing", _GETTER_FAILURES)
def test_a_getter_whose_every_corporation_fails_raises_naming_each(method, failing):
    a, b = _Corp(1, "Acme", fail={failing}), _Corp(2, "Bolt", fail={failing})
    with pytest.raises(CorporationRefreshError) as info:
        getattr(_manager(a, b), method)()
    assert set(info.value.errors) == {"Acme", "Bolt"}


def test_a_failed_refresh_skips_that_corporations_read():
    a, b = _Corp(1, "Acme", fail={"refresh_assets"}), _Corp(2, "Bolt")
    result = _manager(a, b).get_assets()
    assert a.calls == {"refresh_assets": 1} and b.calls == {"refresh_assets": 1, "get_assets": 1}
    assert [r["corporation_id"] for r in result] == [2] and set(result.errors) == {"Acme"}


def test_a_selected_corporation_is_refreshed_and_read_once():
    a, b = _Corp(1, "Acme"), _Corp(2, "Bolt")
    mgr = _manager(a, b)
    assert mgr.get_assets(corporation_id=2) == [{"corporation_id": 2, "kind": "get_assets"}]
    assert mgr.get_market_orders(corporation_name="bolt", refresh=True) == [{"corporation_id": 2, "kind": "get_market_orders"}]
    assert b.calls == Counter({"refresh_assets": 1, "get_assets": 1, "refresh_market_orders": 1, "get_market_orders": 1})
    assert a.calls == Counter()


def test_getters_without_failures_return_every_corporation():
    mgr = _manager(_Corp(1, "Acme"), _Corp(2, "Bolt"))
    assert [r["corporation_id"] for r in mgr.get_assets()] == [1, 2]
    assert [r["corporation_id"] for r in mgr.get_market_orders()] == [1, 2]


# --- routes and bootstrap --------------------------------------------------------------------

def test_the_market_orders_route_answers_404_for_an_unknown_corporation(monkeypatch):
    from flask_app.app import create_app
    import flask_app.routes.corporations as routes

    mgr = _manager(_Corp(98000001, "Acme"))
    monkeypatch.setattr(routes, "require_ready", lambda state: None)
    monkeypatch.setattr(routes, "get_state", lambda: SimpleNamespace(corp_manager=mgr))
    response = create_app().test_client().get("/corporations/98009999/market_orders")
    assert response.status_code == 404
    body = response.get_json()
    assert body["error"]["code"] == "unknown_corporation" and "98009999" in body["message"]


def test_the_bootstrap_names_each_failed_corporation_refresh(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMIN_SETTINGS_PATH", str(tmp_path / "admin_settings.json"))
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "_admin_settings", None)
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", None)
    state = AppState()

    class _CharManager:
        init_errors: list = []

        def refresh_all(self):
            pass

    class _CorpManager:
        init_errors: list = []

        def refresh_all(self):
            state.shutdown_event.set()
            raise CorporationRefreshError("refresh_all", {"Acme": "ESI down", "Bolt": "bad"})

    monkeypatch.setattr(bootstrap, "load_config", lambda: object())
    monkeypatch.setattr(bootstrap, "init_db_managers", lambda cfg, refresh_metadata=True: (None, None, None))
    monkeypatch.setattr(bootstrap, "check_sde_industry_data", lambda s: None)
    monkeypatch.setattr(bootstrap, "init_char_manager", lambda *a, **k: _CharManager())
    monkeypatch.setattr(bootstrap, "init_corp_manager", lambda *a, **k: _CorpManager())

    bootstrap.initialize_application(state)

    assert "Corporation refresh failed for Acme: ESI down" in state.init_warnings
    assert "Corporation refresh failed for Bolt: bad" in state.init_warnings
