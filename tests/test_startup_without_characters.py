"""Startup builds activated roster rows only; no login at startup. tmp_path SQLite, fake names."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import flask_app.bootstrap as bootstrap
from eve_online_industry_tracker.application.admin.service import AdminService
from eve_online_industry_tracker.application.characters import character_manager as cm
from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters.character_manager import CharacterManager
from eve_online_industry_tracker.infrastructure import esi_client as esi_client_module
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.esi_client import ESIClient
from eve_online_industry_tracker.infrastructure.esi_service import ESIService
from eve_online_industry_tracker.infrastructure.models import BaseOauth
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_oauth_schema
from flask_app.state import AppState


@pytest.fixture
def db(tmp_path):
    d = DatabaseManager(f"sqlite:///{tmp_path / 'oauth.db'}")
    BaseOauth.metadata.create_all(bind=d.engine)
    ensure_oauth_schema(d)
    return d


def _row(db, name, activated, is_main=0, is_dir=0):
    db.execute(
        "INSERT INTO oauth_characters (character_name, character_id, refresh_token, scopes, is_main, is_corp_director) "
        "VALUES (:n, :i, :t, '[]', :m, :d)",
        {"n": name, "i": abs(hash(name)) % 10**6, "t": "rt" if activated else None, "m": is_main, "d": is_dir})


class _FakeChar:
    def __init__(self, cfg, oauth, app, sde, name, is_main, is_dir, token, **kw):
        self.character_name, self.is_main, self.is_corp_director = name, is_main, is_dir
        self.character_id = None


def _manager(db, monkeypatch):
    monkeypatch.setattr(cm, "Character", _FakeChar)
    cfg = SimpleNamespace(all=lambda: {})
    return CharacterManager(cfg, db, None, None)


def test_manager_skips_not_activated_rows(db, monkeypatch):
    _row(db, "A", True, is_main=1)
    _row(db, "B", False)
    monkeypatch.setattr(esi_client_module.ESIClient, "register_new_character",
                        lambda self: pytest.fail("registration at startup"))
    mgr = _manager(db, monkeypatch)
    assert [c.character_name for c in mgr._character_list] == ["A"]
    assert mgr.not_activated == ["B"]


def test_manager_with_empty_roster(db, monkeypatch):
    mgr = _manager(db, monkeypatch)
    assert mgr.get_main_character() is None


def test_main_falls_back_to_first_activated_and_no_director_fallback(db, monkeypatch):
    _row(db, "A", True)
    _row(db, "B", True)
    mgr = _manager(db, monkeypatch)
    assert mgr.get_main_character().character_name == "A"
    assert mgr.get_corp_director() is None


def test_public_client_sends_no_authorization():
    esi = {"base": "https://esi.example", "token_url": "t", "verify_url": "v", "auth_url": "a",
           "headers": {"Accept": "x", "Accept-Language": "x", "X-Tenant": "x"}}
    values = {"esi": esi, "app": {"redirect_uri": "r", "user_agent": "ua"}, "oauth": {"client_id": "i"},
              "client_secret": "s", "defaults": {"scopes": [], "scopes_corp_director": []}}
    client = ESIClient(SimpleNamespace(get=values.__getitem__), MagicMock(), "", False, False, load_tokens=False)
    response = MagicMock(status_code=200, headers={})
    response.json.return_value = [{"type_id": 1, "average_price": 2.0}]
    sent = {}

    def get(url, headers=None, **kw):
        sent.update(headers)
        return response

    client._http_session = SimpleNamespace(get=get)
    assert ESIService(client).get_market_prices() == [{"type_id": 1, "average_price": 2.0}]
    assert "Authorization" not in sent


def test_health_reports_characters():
    state = SimpleNamespace(init_state="Ready", init_error=None, init_warnings=[], sde_warnings=[], db_sde=None,
                            admin_settings=None,
                            char_manager=SimpleNamespace(_character_list=[object()], not_activated=["B"]))
    payload, status = AdminService(state=state).health_payload()
    assert status == 200
    assert payload["characters"] == {"active": 1, "not_activated": ["B"]}


def test_no_background_jobs_without_a_main_character(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMIN_SETTINGS_PATH", str(tmp_path / "admin_settings.json"))
    monkeypatch.setattr(esi_client_module._ESI_ERROR_LIMITER, "_admin_settings", None)
    monkeypatch.setattr(ap, "_REALIZED_INVENTION_INPUTS_PROVIDER", None)
    state = AppState()
    calls = []
    built = {}

    class _CharManager:
        init_errors = []
        _character_list = []

        def refresh_all(self):
            pass

        def get_main_character(self):
            return None

    class _Corp:
        init_errors = []
        _corporation_ids = set()

        def refresh_all(self):
            pass

    class _Client:
        def __init__(self, *a, **k):
            built["kwargs"] = k

    class _Stop(Exception):
        pass

    class _Job:
        def __init__(self, *a, **k):
            pass

        def start(self):
            calls.append("start")

    monkeypatch.setattr(bootstrap, "load_config", lambda: object())
    monkeypatch.setattr(bootstrap, "init_db_managers", lambda cfg, refresh_metadata=True: (None, None, None))
    monkeypatch.setattr(bootstrap, "check_sde_industry_data", lambda s: None)
    monkeypatch.setattr(bootstrap, "init_char_manager", lambda *a, **k: _CharManager())
    monkeypatch.setattr(bootstrap, "init_corp_manager", lambda *a, **k: _Corp())
    monkeypatch.setattr(bootstrap, "ESIClient", _Client)
    monkeypatch.setattr(bootstrap, "ESIService", lambda client, **k: SimpleNamespace(client=client))
    monkeypatch.setattr(bootstrap, "IndustryJobManager", _Job)
    monkeypatch.setattr(bootstrap, "trigger_global_public_structures_scan", lambda **k: calls.append("scan"))
    monkeypatch.setattr(bootstrap, "public_structures_startup_scan_enabled", lambda: True)
    import eve_online_industry_tracker.application.market_intelligence.job as mij
    monkeypatch.setattr(mij, "MarketIntelligenceJob", _Job)

    bootstrap.initialize_application(state)

    assert built["kwargs"] == {"load_tokens": False}
    assert isinstance(state.esi_service.client, _Client)
    assert calls == []
