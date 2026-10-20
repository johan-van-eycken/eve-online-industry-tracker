"""J2.4: the realized profit read of an unknown corporation_id or
character_id is a 404 (UnknownCorporationError / UnknownCharacterError,
codes unknown_corporation / unknown_character), not empty rows. Fake ids."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.characters import service as characters_service
from eve_online_industry_tracker.application.corporations import service as corporations_service
from eve_online_industry_tracker.application.unknown_owner import UnknownCharacterError, UnknownCorporationError


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
    return SimpleNamespace(
        get_corporation=lambda corporation_id=None, corporation_name=None: (
            object() if corporation_id in known else None
        ),
        refresh_realized_profit_inputs=lambda **_: None,
    )


def _char_manager(known):
    return SimpleNamespace(
        get_character_by_id=lambda character_id: object() if character_id in known else None,
        refresh_realized_profit_inputs=lambda **_: None,
    )


@pytest.mark.parametrize("refresh", [False, True])
def test_an_unknown_corporation_id_raises(monkeypatch, refresh):
    monkeypatch.setattr(corporations_service, "CorporationRealizedProfitLedgerService", _Ledger)
    svc = corporations_service.CorporationsService(state=_state(corp_manager=_corp_manager({98000001})))
    with pytest.raises(UnknownCorporationError, match="corporation_id 98000002"):
        svc.get_realized_profit_ledger(corporation_id=98000002, refresh=refresh)


def test_a_known_corporation_id_and_no_id_read_as_before(monkeypatch):
    monkeypatch.setattr(corporations_service, "CorporationRealizedProfitLedgerService", _Ledger)
    monkeypatch.setattr(_Ledger, "rebuild", lambda self, **_: [])
    svc = corporations_service.CorporationsService(state=_state(corp_manager=_corp_manager({98000001})))
    assert svc.get_realized_profit_ledger(corporation_id=98000001)["rows"] == []
    assert svc.get_realized_profit_ledger()["rows"] == []


@pytest.mark.parametrize("refresh", [False, True])
def test_an_unknown_character_id_raises(monkeypatch, refresh):
    monkeypatch.setattr(characters_service, "CharacterRealizedProfitLedgerService", _Ledger)
    svc = characters_service.CharactersService(state=_state(char_manager=_char_manager({7001})))
    with pytest.raises(UnknownCharacterError, match="character_id 7002"):
        svc.get_realized_profit_ledger(character_id=7002, refresh=refresh)


def test_a_known_character_id_reads_as_before(monkeypatch):
    monkeypatch.setattr(characters_service, "CharacterRealizedProfitLedgerService", _Ledger)
    monkeypatch.setattr(_Ledger, "rebuild", lambda self, **_: [])
    svc = characters_service.CharactersService(state=_state(char_manager=_char_manager({7001})))
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
    managers = {"corp_manager": _corp_manager({98000001}), "char_manager": _char_manager({7001})}
    app.extensions["app_state"] = _state(**managers)

    response = app.test_client().get(path)

    assert response.status_code == 404
    body = response.get_json()
    assert body["error"]["code"] == code
