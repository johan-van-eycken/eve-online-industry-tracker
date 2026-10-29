"""An unknown character or corporation is an error that names it (a 404 on
the routes), never an empty result or a refresh of nothing.

The corporation side mirrors the character side:

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
from eve_online_industry_tracker.application.characters.character_manager import (
    CharacterManager,
    CharacterRefreshError,
    UnknownCharacterError,
)
from eve_online_industry_tracker.application.characters import service as characters_service
from eve_online_industry_tracker.application.corporations import service as corporations_service


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
    # The corporations that worked are returned; result.errors names each that failed.
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


# --- an unknown character ----------------------------------------------------------------------
#
# A refresh for a character_id or name the CharacterManager does
# not have raises UnknownCharacterError (a CharacterRefreshError naming the
# unknown selector) instead of refreshing nothing; the routes answer 404.
# Fake values only.

class _Char:
    def __init__(self, name, character_id):
        self.character_name = name
        self.character_id = character_id
        self.calls = []

    def ensure_esi(self):
        pass

    def refresh_all(self):
        self.calls.append("refresh_all")

    refresh_wallet_journal = refresh_wallet_transactions = refresh_industry_jobs = refresh_assets = refresh_all

    def get_assets(self):
        return []


def _char_manager(*chars):
    mgr = CharacterManager.__new__(CharacterManager)
    mgr._character_list = list(chars)
    mgr.init_errors = []
    mgr._rebuild_indexes()
    return mgr


def test_an_unknown_character_id_is_named():
    alpha = _Char("Alpha", 90000001)
    mgr = _char_manager(alpha)
    with pytest.raises(UnknownCharacterError) as info:
        mgr.refresh_all(character_id=90009999)
    assert isinstance(info.value, CharacterRefreshError)
    assert "90009999" in str(info.value)
    assert list(info.value.errors) == ["character_id 90009999"]
    assert alpha.calls == []


def test_an_unknown_character_name_is_named():
    mgr = _char_manager(_Char("Alpha", 90000001))
    with pytest.raises(UnknownCharacterError) as info:
        mgr.refresh_all(character_name="Nobody")
    assert "Nobody" in str(info.value)


def test_a_known_character_still_refreshes():
    alpha, bravo = _Char("Alpha", 90000001), _Char("Bravo", 90000002)
    mgr = _char_manager(alpha, bravo)
    mgr.refresh_all(character_id=90000002)
    mgr.refresh_all(character_name="alpha")
    assert alpha.calls == bravo.calls == ["refresh_all"]


def test_the_realized_profit_inputs_refresh_keeps_the_unknown_character_error():
    mgr = _char_manager(_Char("Alpha", 90000001))
    with pytest.raises(UnknownCharacterError) as info:
        mgr.refresh_realized_profit_inputs(character_id=90009999)
    assert "90009999" in str(info.value)


def test_get_assets_keeps_the_unknown_character_error():
    mgr = _char_manager(_Char("Alpha", 90000001))
    with pytest.raises(UnknownCharacterError):
        mgr.get_assets(character_id=90009999)


def test_the_assets_route_answers_404_for_an_unknown_character(monkeypatch):
    from flask_app.app import create_app
    import flask_app.routes.characters as routes

    monkeypatch.setattr(routes, "require_ready", lambda state: None)
    mgr = _char_manager(_Char("Alpha", 90000001))
    monkeypatch.setattr(routes, "get_state", lambda: SimpleNamespace(char_manager=mgr))

    response = create_app().test_client().get("/characters/assets?character_id=90009999")
    assert response.status_code == 404
    body = response.get_json()
    assert body["status"] == "error" and "90009999" in body["message"]


# --- the realized profit read of an unknown owner ----------------------------------------------
#
# The realized profit read of an unknown corporation_id or
# character_id is a 404 (UnknownCorporationError / UnknownCharacterError,
# codes unknown_corporation / unknown_character), not empty rows. Fake ids.

class _Ledger:
    def __init__(self, **_):
        pass

    def list_rows(self, **_):
        return []

    def owner_ids(self):
        return []

    def rebuild(self, **_):
        raise AssertionError("an unknown owner is never rebuilt")


def _state(**managers):
    return SimpleNamespace(
        esi_service=SimpleNamespace(get_market_prices=lambda: []),
        db_app=SimpleNamespace(session=None), db_sde=SimpleNamespace(session=None), db_oauth=None,
        admin_settings=None, **managers,
    )


def _corp_manager(known):
    """Like CorporationManager.get_corporation: an unknown id raises."""
    def get_corporation(corporation_id=None, corporation_name=None):
        if corporation_id not in known:
            raise UnknownCorporationError("get_corporation", f"corporation_id {corporation_id}")
        return object()

    return SimpleNamespace(get_corporation=get_corporation, refresh_realized_profit_inputs=lambda **_: None)


def _profit_char_manager(known):
    return SimpleNamespace(
        get_character_by_id=lambda character_id: object() if character_id in known else None,
        refresh_realized_profit_inputs=lambda **_: None,
    )


@pytest.mark.parametrize("refresh", [False, True])
def test_an_unknown_corporation_id_raises(monkeypatch, refresh):
    monkeypatch.setattr(corporations_service, "CorporationRealizedProfitLedgerService", _Ledger)
    svc = corporations_service.CorporationsService(state=_state(corp_manager=_corp_manager({98000001})))
    with pytest.raises(UnknownCorporationError, match="corporation_id 98000002") as info:
        svc.get_realized_profit_ledger(corporation_id=98000002, refresh=refresh)
    assert info.value.method_name == "get_realized_profit_ledger"


def test_a_known_corporation_id_and_no_id_read_as_before(monkeypatch):
    monkeypatch.setattr(corporations_service, "CorporationRealizedProfitLedgerService", _Ledger)
    monkeypatch.setattr(_Ledger, "rebuild", lambda self, **_: [])
    svc = corporations_service.CorporationsService(state=_state(corp_manager=_corp_manager({98000001})))
    assert svc.get_realized_profit_ledger(corporation_id=98000001)["rows"] == []
    assert svc.get_realized_profit_ledger()["rows"] == []


@pytest.mark.parametrize("refresh", [False, True])
def test_an_unknown_character_id_raises(monkeypatch, refresh):
    monkeypatch.setattr(characters_service, "CharacterRealizedProfitLedgerService", _Ledger)
    svc = characters_service.CharactersService(state=_state(char_manager=_profit_char_manager({7001})))
    with pytest.raises(UnknownCharacterError, match="character_id 7002"):
        svc.get_realized_profit_ledger(character_id=7002, refresh=refresh)


def test_a_known_character_id_reads_as_before(monkeypatch):
    monkeypatch.setattr(characters_service, "CharacterRealizedProfitLedgerService", _Ledger)
    monkeypatch.setattr(_Ledger, "rebuild", lambda self, **_: [])
    svc = characters_service.CharactersService(state=_state(char_manager=_profit_char_manager({7001})))
    assert svc.get_realized_profit_ledger(character_id=7001)["rows"] == []


@pytest.mark.parametrize("path, manager, code", [
    ("/corporations/realized_profit?corporation_id=98000002", "corp_manager", "unknown_corporation"),
    ("/characters/realized_profit?character_id=7002", "char_manager", "unknown_character"),
])
def test_the_routes_answer_404_with_the_code(monkeypatch, path, manager, code):
    from flask_app.app import create_app
    from flask_app.routes import characters as characters_routes
    from flask_app.routes import corporations as corporations_routes

    monkeypatch.setattr(corporations_service, "CorporationRealizedProfitLedgerService", _Ledger)
    monkeypatch.setattr(characters_service, "CharacterRealizedProfitLedgerService", _Ledger)
    for module in (characters_routes, corporations_routes):
        monkeypatch.setattr(module, "require_ready", lambda state: None, raising=False)
    app = create_app()
    managers = {"corp_manager": _corp_manager({98000001}), "char_manager": _profit_char_manager({7001})}
    app.extensions["app_state"] = _state(**managers)

    response = app.test_client().get(path)

    assert response.status_code == 404
    body = response.get_json()
    assert body["error"]["code"] == code
