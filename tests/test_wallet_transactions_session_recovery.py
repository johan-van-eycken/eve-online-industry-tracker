"""A failed wallet refresh must not poison the shared app session (2026-11-20:
Duke X1's failed flush stayed pending, so the retry and Knight X1's refresh
failed on Duke's rows), and a transaction already stored is skipped, not a
UNIQUE failure of the whole batch."""
from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.characters.character import Character
from eve_online_industry_tracker.application.characters.character_manager import _rollback_session
from eve_online_industry_tracker.infrastructure.models import CharacterWalletTransactionsModel


def _tx(tid, char=1):
    return {"character_id": char, "transaction_id": tid, "client_id": 9, "client_name": "x",
            "date": "2026-11-20T11:00:00Z", "is_buy": False, "is_personal": False, "journal_ref_id": tid,
            "location_id": 60003760, "quantity": 1, "type_id": 34, "type_name": "Tritanium",
            "type_group_id": 18, "type_group_name": "Mineral", "type_category_id": 4,
            "type_category_name": "Material", "unit_price": 5.0, "total_price": 5.0}


def _char(session):
    c = object.__new__(Character)
    c._db_app = SimpleNamespace(session=session)
    c.character_name = "Pilot"
    return c


def test_a_stored_transaction_is_skipped_and_the_new_ones_saved(app_engine):
    session = sessionmaker(bind=app_engine, future=True)()
    _char(session).save_wallet_transactions([_tx(1)])
    _char(session).save_wallet_transactions([_tx(1), _tx(2)])
    assert sorted(t for (t,) in session.query(CharacterWalletTransactionsModel.transaction_id)) == [1, 2]


def test_the_session_works_again_after_a_failed_refresh(app_engine):
    session = sessionmaker(bind=app_engine, future=True)()
    session.add(CharacterWalletTransactionsModel(**_tx(5)))
    session.add(CharacterWalletTransactionsModel(**_tx(5)))     # a duplicate in one flush
    try:
        session.flush()
    except Exception:
        pass
    _rollback_session(SimpleNamespace(_db_app=SimpleNamespace(session=session)))
    _char(session).save_wallet_transactions([_tx(6, char=2)])  # the next character's refresh
    assert [t for (t,) in session.query(CharacterWalletTransactionsModel.transaction_id)] == [6]


def test_a_self_trade_keeps_both_its_sell_and_its_buy(app_engine):
    """Buying from one's own sell order: ESI returns one transaction_id twice."""
    session = sessionmaker(bind=app_engine, future=True)()
    sell, buy = _tx(7), {**_tx(7), "is_buy": True}
    _char(session).save_wallet_transactions([sell, buy])
    _char(session).save_wallet_transactions([sell, buy])          # a retry: nothing doubled
    rows = session.query(CharacterWalletTransactionsModel.transaction_id, CharacterWalletTransactionsModel.is_buy)
    assert sorted(rows) == [(7, False), (7, True)]


def test_the_migration_rebuilds_the_old_key_and_keeps_the_rows(tmp_path):
    from sqlalchemy import create_engine, text
    from eve_online_industry_tracker.infrastructure.schema_migrations import _ensure_transaction_side_key
    engine = create_engine(f"sqlite:///{tmp_path / 'app.db'}")
    with engine.begin() as c:
        c.execute(text("CREATE TABLE character_wallet_transactions (id INTEGER NOT NULL, transaction_id BIGINT NOT NULL, "
                       "is_buy BOOLEAN, PRIMARY KEY (id), UNIQUE (transaction_id))"))
        c.execute(text("INSERT INTO character_wallet_transactions VALUES (1, 7, 0)"))

    class _Db:
        def __init__(self): self.engine = engine
        def query(self, sql, params=None):
            with engine.begin() as c:
                return c.execute(text(sql), params or {}).fetchall()
    _ensure_transaction_side_key(_Db(), "character_wallet_transactions")
    _ensure_transaction_side_key(_Db(), "character_wallet_transactions")   # idempotent
    with engine.begin() as c:
        c.execute(text("INSERT INTO character_wallet_transactions VALUES (2, 7, 1)"))   # the buy half
        assert c.execute(text("SELECT transaction_id, is_buy FROM character_wallet_transactions ORDER BY id")).fetchall() == [(7, 0), (7, 1)]
