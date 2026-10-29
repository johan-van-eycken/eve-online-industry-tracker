"""F1 item 7: CharacterManager.get_characters() raises CharacterDataError
(not a bare Exception) when a character cannot be serialized; the slot
capacities propagate it, and the active jobs' cosmetic name map logs a
WARNING and keeps the job's ID. Fake values throughout."""
from __future__ import annotations

import logging
from types import SimpleNamespace
from collections import Counter

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from eve_online_industry_tracker.application.characters.character_manager import (
    CharacterDataError,
    CharacterManager,
)
import flask_app.bootstrap as bootstrap
from flask_app.state import AppState
from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters import character_manager as cm
from eve_online_industry_tracker.application.characters.character_manager import CharacterRefreshError
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.application.characters.character_manager import UnknownCharacterError


class _Char:
    def __init__(self, error=None):
        self._error = error

    def get_character(self):
        if self._error:
            raise self._error
        return {"character_id": 90000001, "character_name": "Fake"}


def _manager(*chars):
    mgr = CharacterManager.__new__(CharacterManager)
    mgr._character_list = list(chars)
    return mgr


def test_get_characters_returns_the_serialized_characters():
    assert _manager(_Char()).get_characters() == [{"character_id": 90000001, "character_name": "Fake"}]


def test_get_characters_without_characters_is_a_value_error():
    with pytest.raises(ValueError):
        _manager().get_characters()


def test_a_db_failure_is_a_character_data_error_naming_it():
    with pytest.raises(CharacterDataError, match="fake db down") as exc_info:
        _manager(_Char(OperationalError("SELECT fake", {}, Exception("fake db down")))).get_characters()
    assert isinstance(exc_info.value.__cause__, OperationalError)
    assert not isinstance(exc_info.value, RuntimeError)     # not read as an ESI error by the ESI catch sites


def test_a_programming_error_propagates_as_is():
    with pytest.raises(AttributeError):
        _manager(_Char(AttributeError("fake bug"))).get_characters()


def test_the_active_jobs_name_map_survives_an_unreadable_character_list(session_provider, app_engine, caplog):
    from eve_online_industry_tracker.application.industry.service import IndustryService
    from eve_online_industry_tracker.infrastructure.models import CharacterIndustryJobsModel

    with Session(app_engine) as session:
        session.add(CharacterIndustryJobsModel(character_id=7, job_id=1, status="active", activity_id=1,
                                               raw={"activity_id": 1}))
        session.commit()

    def _fail():
        raise CharacterDataError("Error retrieving characters: fake")

    state = SimpleNamespace(char_manager=SimpleNamespace(get_characters=_fail), db_sde=SimpleNamespace(language="en"))
    svc = IndustryService(state=state, sessions=session_provider)
    svc._character_slot_capacities = lambda: {}
    with caplog.at_level(logging.WARNING):
        (job,) = svc.industry_active_jobs()["jobs"]
    assert job["character_id"] == 7
    assert "Error retrieving characters: fake" in caplog.text


# --- one failing character does not abort the batch --------------------------------------------
#
# Round H2.8: one failing character no longer aborts the refresh of the
# others. The batch refreshes every character, then raises one
# CharacterRefreshError naming each failed character; the bootstrap puts one
# init_warnings line per failed character, and a character that could not be
# initialised is in init_warnings too. Fake values only.

class _BatchChar:
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


def _batch_manager(*chars):
    mgr = CharacterManager.__new__(CharacterManager)
    mgr._character_list = list(chars)
    mgr._char_by_id, mgr._char_by_name = {}, {}
    mgr.init_errors = []
    return mgr


def test_one_failing_character_does_not_stop_the_others():
    a, b, c = _BatchChar("Alpha"), _BatchChar("Bravo", RuntimeError("ESI down")), _BatchChar("Charlie")
    mgr = _batch_manager(a, b, c)
    with pytest.raises(CharacterRefreshError) as info:
        mgr.refresh_all()
    assert a.calls == b.calls == c.calls == ["refresh_all"]
    assert set(info.value.errors) == {"Bravo"}
    assert "Bravo" in str(info.value) and "ESI down" in str(info.value)
    # The characters that refreshed are indexed.
    assert mgr.get_character_by_id(a.character_id) is a
    assert mgr.get_character_by_id(c.character_id) is c


def test_every_failing_character_is_named():
    mgr = _batch_manager(_BatchChar("Alpha", ValueError("bad")), _BatchChar("Bravo", KeyError("x")), _BatchChar("Charlie"))
    with pytest.raises(CharacterRefreshError) as info:
        mgr.refresh_skills()
    assert set(info.value.errors) == {"Alpha", "Bravo"}
    assert "Alpha" in str(info.value) and "Bravo" in str(info.value)


def test_no_failure_raises_nothing():
    mgr = _batch_manager(_BatchChar("Alpha"), _BatchChar("Bravo"))
    mgr.refresh_all()


def test_the_realized_profit_inputs_refresh_every_kind_and_name_each_failure():
    class _Pilot(_BatchChar):
        def refresh_wallet_journal(self):
            self.calls.append("journal")
            raise RuntimeError("journal down")

        def refresh_wallet_transactions(self):
            self.calls.append("transactions")

        def refresh_industry_jobs(self):
            self.calls.append("jobs")

    pilot, other = _Pilot("Alpha"), _Pilot("Bravo")
    mgr = _batch_manager(pilot, other)
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


# --- the getters with a character read that character only -------------------------------------
#
# Round J1.6: CharacterManager.get_assets / get_wallet_balances /
# get_market_orders with a character_id (or name) refresh that character once
# and return only its data. Before, they looped over every character,
# refreshed the selected one once per character and returned everyone's
# data. Without a selector the behaviour is unchanged. Fake values only.

class _SelectedChar:
    def __init__(self, name, character_id, refresh_error=None):
        self.character_name = name
        self.character_id = character_id
        self.refresh_error = refresh_error
        self.calls = Counter()

    def ensure_esi(self):
        pass

    def _refresh(self, kind):
        self.calls[kind] += 1
        if self.refresh_error:
            raise self.refresh_error

    def refresh_wallet_balance(self):
        self._refresh("refresh_wallet_balance")

    def refresh_assets(self):
        self._refresh("refresh_assets")

    def refresh_market_orders(self):
        self._refresh("refresh_market_orders")

    def get_wallet_balance(self):
        self.calls["get_wallet_balance"] += 1
        return {"character_id": self.character_id, "balance": 1.0}

    def get_assets(self):
        self.calls["get_assets"] += 1
        return {"character_id": self.character_id, "assets": []}

    def get_market_orders(self):
        self.calls["get_market_orders"] += 1
        return {"character_id": self.character_id, "orders": []}


def _selected_manager():
    chars = [_SelectedChar("Alpha", 90000001), _SelectedChar("Bravo", 90000002), _SelectedChar("Charlie", 90000003)]
    mgr = CharacterManager.__new__(CharacterManager)
    mgr._character_list = list(chars)
    mgr.init_errors = []
    mgr._rebuild_indexes()
    return mgr, chars


_GETTERS = [
    ("get_wallet_balances", "refresh_wallet_balance", "get_wallet_balance"),
    ("get_assets", "refresh_assets", "get_assets"),
    ("get_market_orders", "refresh_market_orders", "get_market_orders"),
]


@pytest.mark.parametrize("method,refresh,getter", _GETTERS)
@pytest.mark.parametrize("selector", [{"character_id": 90000002}, {"character_name": "bravo"}])
def test_a_selected_character_is_refreshed_once_and_returned_alone(method, refresh, getter, selector):
    mgr, (alpha, bravo, charlie) = _selected_manager()
    result = getattr(mgr, method)(**selector)
    assert [r["character_id"] for r in result] == [90000002]
    assert bravo.calls == Counter({refresh: 1, getter: 1})
    assert alpha.calls == charlie.calls == Counter()


@pytest.mark.parametrize("method,refresh,getter", _GETTERS)
def test_without_a_selector_every_character_is_refreshed_once_and_returned(method, refresh, getter):
    # Before, the whole batch was refreshed before each read: N x N refreshes.
    mgr, chars = _selected_manager()
    result = getattr(mgr, method)()
    assert [r["character_id"] for r in result] == [90000001, 90000002, 90000003]
    assert all(c.calls == Counter({refresh: 1, getter: 1}) for c in chars)


def test_market_orders_without_a_selector_or_refresh_only_reads():
    mgr, chars = _selected_manager()
    assert len(mgr.get_market_orders(refresh=False)) == 3
    assert all(c.calls == Counter({"get_market_orders": 1}) for c in chars)


def test_market_orders_without_refresh_does_not_refresh_the_selected_character():
    mgr, (alpha, bravo, charlie) = _selected_manager()
    assert [r["character_id"] for r in mgr.get_market_orders(character_id=90000003, refresh=False)] == [90000003]
    assert charlie.calls == Counter({"get_market_orders": 1})
    assert alpha.calls == bravo.calls == Counter()


@pytest.mark.parametrize("method,refresh,getter", _GETTERS)
def test_an_unknown_selected_character_is_named(method, refresh, getter):
    mgr, chars = _selected_manager()
    with pytest.raises(UnknownCharacterError, match="90009999"):
        getattr(mgr, method)(character_id=90009999)
    assert all(c.calls == Counter() for c in chars)


def test_a_failed_refresh_of_the_selected_character_names_it():
    mgr, (alpha, bravo, charlie) = _selected_manager()
    bravo.refresh_error = RuntimeError("ESI down")
    with pytest.raises(CharacterRefreshError) as info:
        mgr.get_assets(character_id=90000002)
    assert set(info.value.errors) == {"Bravo"} and "ESI down" in str(info.value)
    assert bravo.calls["get_assets"] == 0
