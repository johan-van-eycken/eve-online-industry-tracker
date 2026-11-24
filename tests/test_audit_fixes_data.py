"""Audit 2026-11-24, data group: empty order snapshots clear the old orders
(3), wallet journal keys per owner (4, 6)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.characters.character import Character
from eve_online_industry_tracker.infrastructure.models import CharacterMarketOrdersModel, CharacterWalletJournalModel
from eve_online_industry_tracker.infrastructure.schema_migrations import _ensure_journal_owner_key


def _char(session, char_id=1):
    c = object.__new__(Character)
    c._db_app = SimpleNamespace(session=session)
    c.character_name, c.character_id = f"Pilot {char_id}", char_id
    return c


def _entry(char_id, journal_id=500, amount=1e9):
    return {"character_id": char_id, "wallet_journal_id": journal_id, "amount": amount, "balance": 0.0,
            "date": "2026-11-24T10:00:00Z", "ref_type": "player_donation"}


def test_a_transfer_between_own_pilots_is_kept_in_both_journals(app_engine):
    """6: EVE gives both sides of a transfer one journal id."""
    session = sessionmaker(bind=app_engine, future=True)()
    _char(session, 1).save_wallet_journal([_entry(1, amount=-1e9)])
    _char(session, 2).save_wallet_journal([_entry(2)])
    rows = session.query(CharacterWalletJournalModel.character_id, CharacterWalletJournalModel.amount).all()
    assert sorted(rows) == [(1, -1e9), (2, 1e9)]


def test_no_open_orders_clears_the_stored_orders(app_engine):
    """3: the last orders filled, ESI returns []."""
    session = sessionmaker(bind=app_engine, future=True)()
    session.add(CharacterMarketOrdersModel(character_id=1, order_id=77, type_id=34, price=5.0, volume_remain=1,
                                           volume_total=1, is_buy_order=False, location_id=60003760, region_id=1,
                                           duration=90, issued="2026-11-20T10:00:00Z", min_volume=1, range="region"))
    session.commit()
    _char(session).save_market_orders([])
    assert session.query(CharacterMarketOrdersModel).count() == 0


class _Db:
    def __init__(self, engine):
        self.engine = engine

    def query(self, sql, params=None):
        with self.engine.begin() as c:
            return c.execute(text(sql), params or {}).fetchall()


@pytest.mark.parametrize("old_key", [
    "id INTEGER NOT NULL, character_id INTEGER NOT NULL, wallet_journal_id BIGINT NOT NULL, "
    "PRIMARY KEY (id), UNIQUE (wallet_journal_id)",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, character_id INTEGER NOT NULL, wallet_journal_id INTEGER NOT NULL UNIQUE",
])
def test_the_migration_keys_the_journal_per_owner_and_keeps_the_rows(tmp_path, old_key):
    """4/6: both DDL forms of the old global key are rebuilt; idempotent."""
    engine = create_engine(f"sqlite:///{tmp_path / 'app.db'}")
    with engine.begin() as c:
        c.execute(text(f"CREATE TABLE character_wallet_journal ({old_key})"))
        c.execute(text("INSERT INTO character_wallet_journal VALUES (1, 1, 500)"))
    _ensure_journal_owner_key(_Db(engine), "character_wallet_journal", "character_id")
    _ensure_journal_owner_key(_Db(engine), "character_wallet_journal", "character_id")
    with engine.begin() as c:
        c.execute(text("INSERT INTO character_wallet_journal VALUES (2, 2, 500)"))   # the other pilot's side
    with pytest.raises(IntegrityError):
        with engine.begin() as c:
            c.execute(text("INSERT INTO character_wallet_journal VALUES (3, 1, 500)"))   # same pilot twice
    with engine.begin() as c:
        assert c.execute(text("SELECT character_id, wallet_journal_id FROM character_wallet_journal ORDER BY id")
                         ).fetchall() == [(1, 500), (2, 500)]


# --- tokens (8, 14) -----------------------------------------------------------------------------
import time as _time

import requests

from test_esi_client_error_contract import _Session, client  # noqa: F401  (the fixture)


def _ok(body=b'[{"x": 1}]', pages=None):
    r = requests.Response()
    r.status_code, r._content = 200, body
    if pages:
        r.headers["X-Pages"] = str(pages)
    return r


def test_a_token_another_process_refreshed_is_taken_from_the_database(client, monkeypatch):
    """8: a script rotated the refresh token; posting ours would get HTTP 400."""
    stored = SimpleNamespace(access_token="script-token", refresh_token="script-refresh",
                             token_expiry=int(_time.time()) + 1000)
    client.db_oauth.session.query.return_value.filter_by.return_value.first.return_value = stored
    client.token_expiry = int(_time.time()) - 1
    monkeypatch.setattr(client, "refresh_access_token", lambda: pytest.fail("refreshed with a rotated token"))
    client._http_session = _Session(lambda: _ok())
    client.esi_get("/x/", use_cache=False)
    assert (client.access_token, client.refresh_token) == ("script-token", "script-refresh")


def test_each_page_of_a_long_walk_gets_a_fresh_token(client, monkeypatch):
    """14: the token expires between page 1 and page 2."""
    seen = []

    class _Pages:
        calls = 0

        def get(self, url, headers=None, **kw):
            seen.append(headers["Authorization"])
            _Pages.calls += 1
            client.token_expiry = int(_time.time()) - 1    # expires right after page 1
            return _ok(pages=2)

    def refresh():
        client.access_token, client.token_expiry = "fresh", int(_time.time()) + 1000

    client.db_oauth.session.query.return_value.filter_by.return_value.first.return_value = None
    monkeypatch.setattr(client, "refresh_access_token", refresh)
    monkeypatch.setattr(client, "_pagination_pause", lambda *a: None)
    client.token_expiry = int(_time.time()) + 1000
    client._http_session = _Pages()
    client.esi_get("/x/", paginate=True, use_cache=False)
    assert seen == ["Bearer token", "Bearer fresh"]
