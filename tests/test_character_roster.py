"""Roster repository and the one-time seed. tmp_path SQLite, fake names."""
from __future__ import annotations

import pytest

from eve_online_industry_tracker.application.characters.roster_seed import seed_characters_once
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.models import BaseOauth
from eve_online_industry_tracker.infrastructure.oauth_character_repository import OAuthCharacterRepository
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_oauth_schema


@pytest.fixture
def db(tmp_path):
    d = DatabaseManager(f"sqlite:///{tmp_path / 'oauth.db'}")
    BaseOauth.metadata.create_all(bind=d.engine)
    ensure_oauth_schema(d)
    return d


def _rows(db):
    return {m.character_name: m for m in OAuthCharacterRepository(db.session).list_metadata()}


def _activated(db, name, is_main=0):
    db.execute(
        "INSERT INTO oauth_characters (character_name, character_id, refresh_token, access_token, scopes, is_main) "
        "VALUES (:n, :i, 'rt', 'at', '[]', :m)", {"n": name, "i": abs(hash(name)) % 10**6, "m": is_main})


def test_seed_adds_missing_names_as_not_activated(db):
    seed_characters_once(db, [{"character_name": "A", "is_main": True}, {"character_name": "B"}])
    rows = _rows(db)
    assert set(rows) == {"A", "B"}
    assert not rows["A"].has_refresh_token and not rows["B"].has_refresh_token
    assert rows["A"].is_main and not rows["B"].is_main


def test_seed_copies_flags_onto_existing_rows(db):
    _activated(db, "Main")
    _activated(db, "Gone")
    gone = seed_characters_once(db, [{"character_name": "Main", "is_main": True, "is_corp_director": True,
                                      "industry_enabled": False}])
    rows = _rows(db)
    assert gone == ["Gone"]
    assert rows["Main"].is_corp_director and not rows["Main"].industry_enabled and rows["Main"].is_main
    assert rows["Main"].has_refresh_token and rows["Main"].has_access_token
    assert rows["Gone"].has_refresh_token and not rows["Gone"].is_main


def test_seed_applies_old_fallbacks(db):
    seed_characters_once(db, [{"character_name": "A"}, {"character_name": "B"}])
    rows = _rows(db)
    assert rows["A"].is_main and rows["A"].is_corp_director
    assert not rows["B"].is_main and not rows["B"].is_corp_director


def test_seed_runs_once(db):
    seed_characters_once(db, [{"character_name": "A"}])
    assert seed_characters_once(db, [{"character_name": "Z"}]) == []
    assert set(_rows(db)) == {"A"}


def test_seed_with_no_characters_marks_done(db):
    assert seed_characters_once(db, []) == []
    assert _rows(db) == {}
    assert db.query("SELECT value FROM oauth_meta WHERE key='characters_seeded'")[0][0] == "1"


def test_set_flags_main_is_exclusive(db):
    repo = OAuthCharacterRepository(db.session)
    repo.add("A", is_main=True)
    repo.add("B")
    repo.set_flags("b", is_main=True)
    rows = _rows(db)
    assert rows["B"].is_main and not rows["A"].is_main
    with pytest.raises(KeyError):
        repo.set_flags("Nope", is_main=True)
    with pytest.raises(KeyError):
        repo.delete("Nope")
    repo.delete("A")
    assert set(_rows(db)) == {"B"}


def test_add_refuses_duplicate_name_ignoring_case(db):
    repo = OAuthCharacterRepository(db.session)
    repo.add("Alice")
    with pytest.raises(ValueError):
        repo.add("ALICE")


def test_seed_clears_a_stored_main_outside_cfg(db):
    _activated(db, "Gone", is_main=1)
    _activated(db, "Main")
    seed_characters_once(db, [{"character_name": "Main", "is_main": True}])
    rows = _rows(db)
    assert rows["Main"].is_main and not rows["Gone"].is_main


def test_init_db_managers_keeps_and_logs_unconfigured_names(monkeypatch, caplog):
    from types import SimpleNamespace
    from eve_online_industry_tracker.application.characters import corp_transfer_costs, roster_seed
    from eve_online_industry_tracker.infrastructure import esi_client, schema_migrations
    from utils import app_init

    cfg = SimpleNamespace(all=lambda: {"app": {"database_oauth_uri": "x", "database_app_uri": "y",
                                                "database_sde_uri": "z"}, "characters": []})
    monkeypatch.setattr(app_init, "init_db_oauth", lambda c: SimpleNamespace(session=None))
    monkeypatch.setattr(app_init, "init_db_app", lambda c: SimpleNamespace(session=None))
    monkeypatch.setattr(app_init, "init_db_sde", lambda c: SimpleNamespace())
    monkeypatch.setattr(schema_migrations, "ensure_app_schema", lambda db: None)
    monkeypatch.setattr(schema_migrations, "ensure_oauth_schema", lambda db: None)
    monkeypatch.setattr(esi_client, "prune_stale_esi_cache", lambda s: None)
    monkeypatch.setattr(corp_transfer_costs, "restamp_all_blended_corp_transfer_lots_once", lambda s: None)
    monkeypatch.setattr(roster_seed, "seed_characters_once", lambda db, chars: ["X"])
    app_init.init_db_managers(cfg)
    assert roster_seed.LAST_SEED_UNCONFIGURED == ["X"]
    assert "X was not in secret.json but has a login; delete it on Characters if unwanted" in caplog.text
