"""Round J1.1: a corporation whose CharacterManager or Corporation cannot be
built is logged as an ERROR, named in CorporationManager.init_errors (the
bootstrap puts each in init_warnings) and skipped; the other corporations go
on. Before, the unbound name raised UnboundLocalError, or a later failure
re-added the previous corporation. Fake values only.
"""
from __future__ import annotations

import logging
from types import SimpleNamespace

import flask_app.bootstrap as bootstrap
from flask_app.state import AppState
from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.corporations import corporation_manager as corp_mod
from eve_online_industry_tracker.application.corporations.corporation_manager import CorporationManager
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module


class _Director:
    def __init__(self, name, corporation_id):
        self.character_name = name
        self.corporation_id = corporation_id
        self.character_id = corporation_id + 1
        self.is_corp_director = True


class _Corp:
    def __init__(self, cfg, db_app, db_sde, corp_id, char_manager_corp):
        self.corporation_id = corp_id
        self.corporation_name = f"Corp {corp_id}"


def _corp_manager(monkeypatch, *, char_manager_cls, corporation_cls, directors):
    monkeypatch.setattr(corp_mod, "CharacterManager", char_manager_cls)
    monkeypatch.setattr(corp_mod, "Corporation", corporation_cls)
    cfg = SimpleNamespace(all=lambda: {})
    char_manager = SimpleNamespace(_character_list=list(directors), admin_settings=None)
    return CorporationManager(cfg, None, None, None, char_manager)


def test_a_failing_corporation_constructor_is_named_and_the_others_go_on(monkeypatch, caplog):
    class _CorpOneFails(_Corp):
        def __init__(self, cfg, db_app, db_sde, corp_id, char_manager_corp):
            if corp_id == 98000001:
                raise RuntimeError("ESI corporation lookup failed")
            super().__init__(cfg, db_app, db_sde, corp_id, char_manager_corp)

    with caplog.at_level(logging.ERROR):
        mgr = _corp_manager(
            monkeypatch,
            char_manager_cls=lambda *a, **k: SimpleNamespace(),
            corporation_cls=_CorpOneFails,
            directors=[_Director("Alpha", 98000001), _Director("Bravo", 98000002)],
        )
    assert [c.corporation_id for c in mgr._corporations] == [98000002]
    assert len(mgr.init_errors) == 1
    assert "98000001" in mgr.init_errors[0] and "Alpha" in mgr.init_errors[0]
    assert "ESI corporation lookup failed" in mgr.init_errors[0]
    assert any("98000001" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)


def test_a_failing_character_manager_skips_that_corporation(monkeypatch):
    def _char_manager(cfg, db_oauth, db_app, db_sde, corp_id, char_manager, **k):
        if corp_id == 98000002:
            raise KeyError("characters")
        return SimpleNamespace()

    built = []

    class _CorpRecorded(_Corp):
        def __init__(self, *a):
            built.append(a[3])
            super().__init__(*a)

    mgr = _corp_manager(
        monkeypatch,
        char_manager_cls=_char_manager,
        corporation_cls=_CorpRecorded,
        directors=[_Director("Alpha", 98000001), _Director("Bravo", 98000002)],
    )
    assert built == [98000001]   # no Corporation built without its character manager
    assert [c.corporation_id for c in mgr._corporations] == [98000001]
    assert len(mgr.init_errors) == 1
    assert "98000002" in mgr.init_errors[0] and "Bravo" in mgr.init_errors[0] and "characters" in mgr.init_errors[0]


def test_a_later_failure_does_not_reuse_the_previous_corporation(monkeypatch):
    class _CorpSecondFails(_Corp):
        def __init__(self, cfg, db_app, db_sde, corp_id, char_manager_corp):
            if corp_id == 98000002:
                raise ValueError("bad corporation data")
            super().__init__(cfg, db_app, db_sde, corp_id, char_manager_corp)

    mgr = _corp_manager(
        monkeypatch,
        char_manager_cls=lambda *a, **k: SimpleNamespace(),
        corporation_cls=_CorpSecondFails,
        directors=[_Director("Alpha", 98000001), _Director("Bravo", 98000002), _Director("Charlie", 98000003)],
    )
    assert sorted(c.corporation_id for c in mgr._corporations) == [98000001, 98000003]
    assert sorted(mgr._corp_by_id) == [98000001, 98000003]
    assert len(mgr.init_errors) == 1 and "98000002" in mgr.init_errors[0]


def test_the_bootstrap_puts_corporation_init_errors_in_init_warnings(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMIN_SETTINGS_PATH", str(tmp_path / "admin_settings.json"))
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "_admin_settings", None)
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", None)
    state = AppState()

    class _CharManager:
        init_errors: list = []

        def refresh_all(self):
            pass

    class _CorpManager:
        init_errors = ["Error initializing corporation 98000001 (director Alpha): ESI down"]

        def refresh_all(self):
            state.shutdown_event.set()   # stop the initialisation after the refresh

    monkeypatch.setattr(bootstrap, "load_config", lambda: object())
    monkeypatch.setattr(bootstrap, "init_db_managers", lambda cfg, refresh_metadata=True: (None, None, None))
    monkeypatch.setattr(bootstrap, "check_sde_industry_data", lambda s: None)
    monkeypatch.setattr(bootstrap, "init_char_manager", lambda *a, **k: _CharManager())
    monkeypatch.setattr(bootstrap, "init_corp_manager", lambda *a, **k: _CorpManager())

    bootstrap.initialize_application(state)

    assert any("98000001" in w and "ESI down" in w and w.startswith("Corporation initialisation failed")
               for w in state.init_warnings)
