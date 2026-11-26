"""The roster, login and flag routes of the Characters page. tmp_path SQLite, fake names."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import text

import flask_app.routes.characters as route
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.models import BaseOauth
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_oauth_schema

_DEFAULTS = {"scopes": ["esi-a.v1", "esi-b.v1"], "scopes_corp_director": ["esi-corp.v1", "esi-corp2.v1"]}


@pytest.fixture
def db(tmp_path):
    d = DatabaseManager(f"sqlite:///{tmp_path / 'oauth.db'}")
    BaseOauth.metadata.create_all(bind=d.engine)
    ensure_oauth_schema(d)
    d.execute("INSERT INTO oauth_characters (character_name, character_id, refresh_token, scopes, is_main, is_corp_director) "
              "VALUES ('Main', 1, 'rt', 'esi-a.v1 esi-b.v1 esi-corp.v1', 1, 1), ('Alt', 2, 'rt', 'esi-a.v1 esi-b.v1', 0, 0)")
    d.execute("INSERT INTO esi_cache (endpoint, etag, data, last_updated) VALUES ('/x', 'e', '{}', 1)")
    return d


@pytest.fixture
def client(db, monkeypatch):
    from flask_app.app import create_app

    state = SimpleNamespace(db_oauth=db, cfg_manager=SimpleNamespace(get=lambda key, default=None: _DEFAULTS),
                            init_state="Initializing")
    monkeypatch.setattr(route, "get_state", lambda: state)
    monkeypatch.setattr(route, "request_restart", lambda: True)
    return create_app().test_client(), state


def test_roster_missing_scopes(client):
    c, _ = client
    rows = {r["character_name"]: r for r in c.get("/characters/roster").get_json()["data"]}
    assert rows["Main"]["missing_scopes"] == ["esi-corp2.v1"]
    assert rows["Alt"]["missing_scopes"] == []
    assert "refresh_token" not in rows["Main"]


def test_routes_need_the_oauth_db(client):
    c, state = client
    state.db_oauth = None
    assert c.get("/characters/roster").status_code == 503
    assert c.post("/characters/login", json={"character_name": "New"}).status_code == 503


def test_routes_login_conflict_and_patch_delete(client, db, monkeypatch):
    c, _ = client
    started = []
    monkeypatch.setattr(route.login, "start", lambda cfg, db_oauth, name, is_dir: started.append((name, is_dir)) or "https://sso/x")
    assert c.post("/characters/login", json={}).status_code == 400

    resp = c.post("/characters/login", json={"character_name": "New", "is_corp_director": True})
    assert resp.status_code == 200 and resp.get_json()["data"] == {"login_url": "https://sso/x"}
    # An existing row logs in under its stored name and flags.
    c.post("/characters/login", json={"character_name": "main"})
    assert started == [("New", True), ("Main", True)]
    names = [r[0] for r in db.session.execute(text("SELECT character_name FROM oauth_characters ORDER BY id"))]
    assert names == ["Main", "Alt", "New"]

    monkeypatch.setattr(route.login, "start", lambda *a: None)
    assert c.post("/characters/login", json={"character_name": "Alt"}).status_code == 409

    assert c.patch("/characters/Nobody", json={"is_main": True}).status_code == 404
    assert c.patch("/characters/Alt", json={"is_main": "yes"}).status_code == 400
    resp = c.patch("/characters/Alt", json={"is_main": True})
    assert resp.status_code == 200 and resp.get_json()["data"] == {"restarting": True}
    mains = list(db.session.execute(text("SELECT character_name FROM oauth_characters WHERE is_main")))
    assert [m[0] for m in mains] == ["Alt"]

    assert c.delete("/characters/Nobody").status_code == 404
    cache_before = list(db.session.execute(text("SELECT * FROM esi_cache")))
    resp = c.delete("/characters/Alt")
    assert resp.status_code == 200 and resp.get_json()["data"] == {"restarting": True}
    names = [r[0] for r in db.session.execute(text("SELECT character_name FROM oauth_characters ORDER BY id"))]
    assert names == ["Main", "New"]
    assert list(db.session.execute(text("SELECT * FROM esi_cache"))) == cache_before


def test_login_status_route(client, monkeypatch):
    c, _ = client
    status = {"state": "idle", "character_name": None, "message": "", "restarting": False}
    monkeypatch.setattr(route.login, "status", lambda: status)
    assert c.get("/characters/login/status").get_json()["data"] == status
