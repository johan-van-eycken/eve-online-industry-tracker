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
