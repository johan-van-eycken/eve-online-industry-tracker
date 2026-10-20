"""Round J1.2: a refresh for a character_id or name the CharacterManager does
not have raises UnknownCharacterError (a CharacterRefreshError naming the
unknown selector) instead of refreshing nothing; the routes answer 404.
Fake values only.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.characters.character_manager import (
    CharacterManager,
    CharacterRefreshError,
    UnknownCharacterError,
)


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
