"""Round H2.8: one failing character no longer aborts the refresh of the
others. The batch refreshes every character, then raises one
CharacterRefreshError naming each failed character; the bootstrap puts one
init_warnings line per failed character, and a character that could not be
initialised is in init_warnings too. Fake values only.
"""
from __future__ import annotations


import pytest

import flask_app.bootstrap as bootstrap
from flask_app.state import AppState
from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters import character_manager as cm
from eve_online_industry_tracker.application.characters.character_manager import (
    CharacterManager,
    CharacterRefreshError,
)
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module


class _Char:
    def __init__(self, name, error=None, character_id=None):
        self.character_name = name
        self.character_id = character_id
        self._error = error
        self.calls = []

    def ensure_esi(self):
        pass

    def refresh_all(self):
        self.calls.append("refresh_all")
        if self._error:
            raise self._error
        self.character_id = self.character_id or 90000000 + len(self.character_name)

    refresh_skills = refresh_all


def _manager(*chars):
    mgr = CharacterManager.__new__(CharacterManager)
    mgr._character_list = list(chars)
    mgr._char_by_id, mgr._char_by_name = {}, {}
    mgr.init_errors = []
    return mgr


def test_one_failing_character_does_not_stop_the_others():
    a, b, c = _Char("Alpha"), _Char("Bravo", RuntimeError("ESI down")), _Char("Charlie")
    mgr = _manager(a, b, c)
    with pytest.raises(CharacterRefreshError) as info:
        mgr.refresh_all()
    assert a.calls == b.calls == c.calls == ["refresh_all"]
    assert set(info.value.errors) == {"Bravo"}
    assert "Bravo" in str(info.value) and "ESI down" in str(info.value)
    # The characters that refreshed are indexed.
    assert mgr.get_character_by_id(a.character_id) is a
    assert mgr.get_character_by_id(c.character_id) is c


def test_every_failing_character_is_named():
    mgr = _manager(_Char("Alpha", ValueError("bad")), _Char("Bravo", KeyError("x")), _Char("Charlie"))
    with pytest.raises(CharacterRefreshError) as info:
        mgr.refresh_skills()
    assert set(info.value.errors) == {"Alpha", "Bravo"}
    assert "Alpha" in str(info.value) and "Bravo" in str(info.value)


def test_no_failure_raises_nothing():
    mgr = _manager(_Char("Alpha"), _Char("Bravo"))
    mgr.refresh_all()


def test_the_realized_profit_inputs_refresh_every_kind_and_name_each_failure():
    class _Pilot(_Char):
        def refresh_wallet_journal(self):
            self.calls.append("journal")
            raise RuntimeError("journal down")

        def refresh_wallet_transactions(self):
            self.calls.append("transactions")

        def refresh_industry_jobs(self):
            self.calls.append("jobs")

    pilot, other = _Pilot("Alpha"), _Pilot("Bravo")
    mgr = _manager(pilot, other)
    with pytest.raises(CharacterRefreshError) as info:
        mgr.refresh_realized_profit_inputs()
    assert pilot.calls == other.calls == ["journal", "transactions", "jobs"]
    assert set(info.value.errors) == {"Alpha (refresh_wallet_journal)", "Bravo (refresh_wallet_journal)"}


def test_a_character_init_failure_is_kept(monkeypatch):
    class _Boom:
        def __init__(self, *a, **k):
            raise RuntimeError("token store unreadable")

    mgr = CharacterManager.__new__(CharacterManager)
    mgr._cfg_characters = [{"character_name": "Alpha", "is_main": True, "is_corp_director": True}]
    mgr._cfgManager = mgr._db_oauth = mgr._db_app = mgr._db_sde = None
    mgr.admin_settings = None
    mgr._character_list = []
    mgr.init_errors = []
    mgr._get_existing_token = lambda name: None
    monkeypatch.setattr(cm, "Character", _Boom)
    mgr._initialize_characters()
    assert mgr._character_list == []
    assert len(mgr.init_errors) == 1
    assert "Alpha" in mgr.init_errors[0] and "token store unreadable" in mgr.init_errors[0]


def test_the_bootstrap_names_each_failed_character_in_init_warnings(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMIN_SETTINGS_PATH", str(tmp_path / "admin_settings.json"))
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "_admin_settings", None)
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", None)
    state = AppState()

    class _CharManager:
        init_errors = ["Error initializing character Delta: token store unreadable"]

        def refresh_all(self):
            state.shutdown_event.set()   # stop the initialisation after the refresh
            raise CharacterRefreshError("refresh_all", {"Alpha": "ESI down", "Bravo": "bad"})

    monkeypatch.setattr(bootstrap, "load_config", lambda: object())
    monkeypatch.setattr(bootstrap, "init_db_managers", lambda cfg, refresh_metadata=True: (None, None, None))
    monkeypatch.setattr(bootstrap, "check_sde_industry_data", lambda s: None)
    monkeypatch.setattr(bootstrap, "init_char_manager", lambda *a, **k: _CharManager())

    bootstrap.initialize_application(state)

    warnings = state.init_warnings
    assert any("Delta" in w and "token store unreadable" in w for w in warnings)
    assert any("Alpha" in w and "ESI down" in w for w in warnings)
    assert any("Bravo" in w and "bad" in w for w in warnings)
