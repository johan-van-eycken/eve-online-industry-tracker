"""Wallet transactions refresh: new transactions are stored, known ones are skipped. Fake ids."""
from types import SimpleNamespace

from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.characters.character import Character
from eve_online_industry_tracker.application.corporations.corporation import Corporation
from eve_online_industry_tracker.infrastructure.models import (
    CharacterWalletTransactionsModel,
    CorporationWalletTransactionsModel,
)


def _tx(tx_id: int) -> dict:
    return {"transaction_id": tx_id, "date": "2026-10-01T00:00:00Z", "is_buy": True, "is_personal": True,
            "journal_ref_id": tx_id, "location_id": 60003760, "quantity": 1, "type_id": 34, "unit_price": 4.0,
            "client_id": 1000125}


class _Esi:
    def esi_get(self, endpoint: str, params=None, paginate: bool = False, **kwargs):
        if endpoint.endswith("/wallets"):
            return [{"division": 1}]
        return [_tx(1), _tx(2), _tx(3)]


def test_character_transactions_refresh_skips_known_transactions(app_session, sde_engine) -> None:
    app_session.add(CharacterWalletTransactionsModel(
        character_id=1, transaction_id=1, date="2026-10-01T00:00:00Z", is_buy=True, quantity=1, type_id=34,
        unit_price=4.0, total_price=4.0))
    app_session.commit()
    character = object.__new__(Character)
    character.character_id, character.character_name = 1, "Trader"
    character._db_app = SimpleNamespace(session=app_session)
    character._db_sde = SimpleNamespace(session=sessionmaker(bind=sde_engine)(), language="en")
    character._esi_client = _Esi()
    character.ensure_esi = lambda: None
    character._resolve_client_names = lambda ids: {}
    character._backfill_wallet_transaction_client_names = lambda: None

    character.refresh_wallet_transactions()

    assert sorted(r.transaction_id for r in app_session.query(CharacterWalletTransactionsModel)) == [1, 2, 3]


def test_corporation_transactions_refresh_skips_known_transactions(app_session, sde_engine) -> None:
    app_session.add(CorporationWalletTransactionsModel(
        corporation_id=9, division=1, transaction_id=1, date="2026-10-01T00:00:00Z", is_buy=True, quantity=1,
        type_id=34, unit_price=4.0, total_price=4.0))
    app_session.commit()
    corp = object.__new__(Corporation)
    corp.corporation_id, corp.corporation_name = 9, "Corp"
    corp._db_app = SimpleNamespace(session=app_session)
    corp._db_sde = SimpleNamespace(session=sessionmaker(bind=sde_engine)(), language="en")
    corp._default_esi_character = SimpleNamespace(ensure_esi=lambda: None, _esi_client=_Esi())
    corp._resolve_client_names = lambda ids: {}
    corp._backfill_wallet_transaction_client_names = lambda: None

    corp.refresh_wallet_transactions()

    assert sorted(r.transaction_id for r in app_session.query(CorporationWalletTransactionsModel)) == [1, 2, 3]


class _PagedEsi:
    """ESI's newest-first batches of 3 over transactions 1..8; from_id returns those before it."""

    def __init__(self):
        self.calls = []

    def esi_get(self, endpoint: str, params=None, paginate: bool = False, **kwargs):
        if endpoint.endswith("/wallets"):
            return [{"division": 1}]
        self.calls.append((params or {}).get("from_id"))
        before = (params or {}).get("from_id") or 9
        return [_tx(i) for i in range(before - 1, max(0, before - 4), -1)]


def test_character_transactions_walk_back_past_esis_newest_batch(app_session, sde_engine) -> None:
    app_session.add(CharacterWalletTransactionsModel(
        character_id=1, transaction_id=2, date="2026-10-01T00:00:00Z", is_buy=True, quantity=1, type_id=34,
        unit_price=4.0, total_price=4.0))
    app_session.commit()
    esi = _PagedEsi()
    character = object.__new__(Character)
    character.character_id, character.character_name = 1, "Trader"
    character._db_app = SimpleNamespace(session=app_session)
    character._db_sde = SimpleNamespace(session=sessionmaker(bind=sde_engine)(), language="en")
    character._esi_client = esi
    character.ensure_esi = lambda: None
    character._resolve_client_names = lambda ids: {}
    character._backfill_wallet_transaction_client_names = lambda: None

    character.refresh_wallet_transactions()

    # The batch holding stored 2 ends the walk; nothing older is asked for.
    assert sorted(r.transaction_id for r in app_session.query(CharacterWalletTransactionsModel)) == list(range(1, 9))
    assert esi.calls == [None, 6, 3]


def test_corporation_transactions_walk_back_until_esi_has_no_older(app_session, sde_engine) -> None:
    esi = _PagedEsi()
    corp = object.__new__(Corporation)
    corp.corporation_id, corp.corporation_name = 9, "Corp"
    corp._db_app = SimpleNamespace(session=app_session)
    corp._db_sde = SimpleNamespace(session=sessionmaker(bind=sde_engine)(), language="en")
    corp._default_esi_character = SimpleNamespace(ensure_esi=lambda: None, _esi_client=esi)
    corp._resolve_client_names = lambda ids: {}
    corp._backfill_wallet_transaction_client_names = lambda: None

    corp.refresh_wallet_transactions()

    assert sorted(r.transaction_id for r in app_session.query(CorporationWalletTransactionsModel)) == list(range(1, 9))
    assert esi.calls == [None, 6, 3, 1]
