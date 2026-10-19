"""Round H2.2: the admin ESI settings apply before the startup refresh.

- bootstrap wires the ESI error limiter (and an existing state.esi_service)
  to the admin settings right after loading them, before refresh_all.
- each Character's own ESIService (the corporations use their director's)
  gets the admin settings.

Fake values throughout.
"""
from __future__ import annotations

from types import SimpleNamespace

import flask_app.bootstrap as bootstrap
from flask_app.state import AppState
from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters import character as character_module
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure.esi_service import ESIService


def test_the_limiter_reads_the_admin_settings_before_the_startup_refresh(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMIN_SETTINGS_PATH", str(tmp_path / "admin_settings.json"))
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "_admin_settings", None)
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", None)
    state = AppState()
    existing_service = SimpleNamespace(_admin_settings=None)
    state.esi_service = existing_service
    seen = {}

    class _CharManager:
        def refresh_all(self):
            seen["limiter"] = esi_client_module._ESI_ERROR_LIMITER._admin_settings
            seen["service"] = existing_service._admin_settings
            state.shutdown_event.set()   # stop the initialisation here

    monkeypatch.setattr(bootstrap, "load_config", lambda: object())
    monkeypatch.setattr(bootstrap, "init_db_managers", lambda cfg, refresh_metadata=True: (None, None, None))
    monkeypatch.setattr(bootstrap, "check_sde_industry_data", lambda s: None)
    monkeypatch.setattr(bootstrap, "init_char_manager", lambda *a, **k: _CharManager())

    bootstrap.initialize_application(state)

    assert seen["limiter"] is state.admin_settings
    assert seen["service"] is state.admin_settings


def test_a_character_esi_service_gets_the_admin_settings(monkeypatch):
    admin = object()

    class _FakeClient:
        character_id = 90000001

        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(character_module, "ESIClient", _FakeClient)
    char = character_module.Character.__new__(character_module.Character)
    char._esi_client = None
    char._esi_service = None
    char._admin_settings = admin
    char._cfgManager = None
    char._db_oauth = None
    char.character_name = "Fake Pilot"
    char.is_main = False
    char.is_corp_director = False
    char._refresh_token = None
    char.ensure_esi()
    assert char.esi_service._admin_settings is admin


def test_the_esi_service_takes_admin_settings_in_its_constructor():
    admin = object()
    assert ESIService(SimpleNamespace(), admin_settings=admin)._admin_settings is admin
    assert ESIService(SimpleNamespace())._admin_settings is None
