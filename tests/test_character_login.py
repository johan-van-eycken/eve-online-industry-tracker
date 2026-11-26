"""Logging in one named character from the app. tmp_path SQLite, fake names, no network."""
from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

import flask_app.restart as restart
from eve_online_industry_tracker.application.characters import login
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.esi_client import ESIClient, WrongCharacterError
from eve_online_industry_tracker.infrastructure.models import BaseOauth
from eve_online_industry_tracker.infrastructure.oauth_character_repository import OAuthCharacterRepository
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_oauth_schema

_VALUES = {
    "esi": {"base": "https://esi.example", "token_url": "t", "verify_url": "v", "auth_url": "https://sso.example/auth",
            "headers": {"Accept": "x", "Accept-Language": "x", "X-Tenant": "x"}},
    "app": {"redirect_uri": "http://localhost:8765/callback", "user_agent": "ua"},
    "oauth": {"client_id": "i"}, "client_secret": "s",
    "defaults": {"scopes": ["esi-a.v1"], "scopes_corp_director": ["esi-corp.v1"]},
}
CFG = SimpleNamespace(get=lambda key, default=None: _VALUES.get(key, default))


@pytest.fixture
def db(tmp_path):
    d = DatabaseManager(f"sqlite:///{tmp_path / 'oauth.db'}")
    BaseOauth.metadata.create_all(bind=d.engine)
    ensure_oauth_schema(d)
    OAuthCharacterRepository(d.session).add("Main")
    return d


def _rows(db):
    return db.session.execute(text("SELECT * FROM oauth_characters ORDER BY id")).fetchall()


def _fake_sso(monkeypatch, token_name):
    monkeypatch.setattr(ESIClient, "_get_authorization_code", lambda self: "code")
    monkeypatch.setattr(ESIClient, "exchange_code_for_tokens", lambda self, code: ("at", "rt", 1200))
    monkeypatch.setattr(ESIClient, "_validate_access_token_jwt",
                        lambda self, token: {"name": token_name, "sub": "CHARACTER:EVE:4242", "scp": ["esi-a.v1"]})


def test_wrong_character_is_not_stored(db, monkeypatch):
    _fake_sso(monkeypatch, "Alt")
    before = _rows(db)
    client = ESIClient(CFG, db, "Main", False, False, load_tokens=False)
    with pytest.raises(WrongCharacterError) as exc:
        client.register_new_character("Main")
    assert exc.value.got == "Alt"
    assert _rows(db) == before


def test_matching_character_ignoring_case_is_stored(db, monkeypatch):
    _fake_sso(monkeypatch, "MAIN")
    OAuthCharacterRepository(db.session).set_flags("Main", is_corp_director=True)
    ESIClient(CFG, db, "Main", False, False, load_tokens=False).register_new_character("main")
    (row,) = OAuthCharacterRepository(db.session).list_metadata()
    assert (row.character_name, row.character_id, row.has_refresh_token) == ("Main", 4242, True)
    assert row.is_corp_director is True   # kept as stored


def test_authorization_url_carries_the_scopes():
    client = ESIClient(CFG, MagicMock(), "Main", False, True, load_tokens=False)
    url = client.authorization_url()
    assert url.startswith("https://sso.example/auth?") and "esi-corp.v1" in url and "client_id=i" in url


def _wait_for(predicate, seconds=5.0):
    end = time.time() + seconds
    while not predicate():
        assert time.time() < end, "timed out"
        time.sleep(0.01)


def test_login_busy_returns_none(monkeypatch):
    release = threading.Event()

    class _Client:
        def __init__(self, cfg, db_oauth, name, is_main, is_dir, load_tokens=True):
            assert load_tokens is False and is_main is False
            self.db_oauth = db_oauth

        def authorization_url(self):
            return "https://sso.example/auth?x"

        def register_new_character(self, expected_name=None):
            release.wait(5)

    monkeypatch.setattr(login, "ESIClient", _Client)
    monkeypatch.setattr(login, "request_restart", lambda: False)
    assert login.start(CFG, MagicMock(), "Main", False) == "https://sso.example/auth?x"
    assert login.status()["state"] == "waiting"
    assert login.start(CFG, MagicMock(), "Alt", False) is None
    release.set()
    _wait_for(lambda: login.status()["state"] == "done")
    assert login.status() == {"state": "done", "character_name": "Main", "message": login.status()["message"],
                              "restarting": False}
    _wait_for(lambda: not login._lock.locked())


def test_wrong_character_status(db, monkeypatch):
    _fake_sso(monkeypatch, "Alt")
    monkeypatch.setattr(login, "request_restart", lambda: pytest.fail("no restart"))
    login.start(CFG, db, "Main", False)
    _wait_for(lambda: login.status()["state"] == "wrong_character")
    assert login.status()["message"] == "You logged in as Alt. Log in as Main."
    _wait_for(lambda: not login._lock.locked())


def test_timeout_status(db, monkeypatch):
    def _timeout(self):
        raise TimeoutError("Authorization code retrieval timed out.")

    monkeypatch.setattr(ESIClient, "_get_authorization_code", _timeout)
    monkeypatch.setattr(login, "request_restart", lambda: pytest.fail("no restart"))
    before = _rows(db)
    login.start(CFG, db, "Main", False)
    _wait_for(lambda: login.status()["state"] == "timeout")
    _wait_for(lambda: not login._lock.locked())
    assert _rows(db) == before


def test_request_restart_exits_even_if_stopping_jobs_fails(monkeypatch):
    def _stop(state):
        raise RuntimeError("stop failed")

    def _exit(code):
        raise SystemExit(code)   # the real _exit never returns

    monkeypatch.setattr(restart.multiprocessing, "parent_process", lambda: None)
    assert restart.request_restart(delay_seconds=0) is False

    # The thread's body runs inline, so its exit is seen here.
    monkeypatch.setattr(restart.threading, "Thread", lambda target, **kw: SimpleNamespace(start=target))
    monkeypatch.setattr(restart.multiprocessing, "parent_process", lambda: object())
    monkeypatch.setattr(restart.os, "_exit", _exit)
    monkeypatch.setattr(restart, "stop_background_jobs", _stop)
    with pytest.raises(SystemExit) as exc:
        restart.request_restart(delay_seconds=0)
    assert exc.value.code == restart.RESTART_EXIT_CODE == 75

    monkeypatch.setattr(restart, "stop_background_jobs", lambda state: None)
    monkeypatch.setattr(restart.os, "_exit", lambda code: None)
    assert restart.request_restart(delay_seconds=0) is True
