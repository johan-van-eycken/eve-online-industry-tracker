"""Wallet journal refresh: every ESI page is stored, known entries are skipped."""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.characters.character import Character  # noqa: E402
from eve_online_industry_tracker.application.corporations.corporation import Corporation  # noqa: E402
from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    BaseApp,
    CharacterWalletJournalModel,
    CorporationWalletJournalModel,
)


def _entry(journal_id: int) -> dict:
    return {"id": journal_id, "amount": -10.0, "balance": 100.0, "date": "2026-10-01T00:00:00Z", "ref_type": "brokers_fee"}


class _PagedEsi:
    """Two ESI pages; without paginate=True only page 1 comes back."""

    def __init__(self, pages: list[list[dict]]) -> None:
        self.pages = pages

    def esi_get(self, endpoint: str, params=None, paginate: bool = False, **kwargs):
        if endpoint.endswith("/wallets"):
            return [{"division": 1}]
        pages = self.pages if paginate else self.pages[:1]
        return [dict(e) for page in pages for e in page]


def _session():
    engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


def test_character_journal_refresh_stores_every_page_and_skips_known_entries() -> None:
    session = _session()
    session.add(CharacterWalletJournalModel(character_id=1, **{k: v for k, v in _entry(1).items() if k != "id"}, wallet_journal_id=1))
    session.commit()
    character = object.__new__(Character)
    character.character_id = 1
    character.character_name = "Trader"
    character._db_app = SimpleNamespace(session=session)
    character._esi_client = _PagedEsi([[_entry(1), _entry(2)], [_entry(3)]])
    character.ensure_esi = lambda: None
    character._resolve_client_names = lambda ids: {}
    character._backfill_wallet_journal_party_names = lambda: None

    character.refresh_wallet_journal()

    ids = sorted(r.wallet_journal_id for r in session.query(CharacterWalletJournalModel).all())
    assert ids == [1, 2, 3]


def test_corporation_journal_refresh_stores_every_page_and_skips_known_entries() -> None:
    session = _session()
    session.add(CorporationWalletJournalModel(corporation_id=9, division=1, **{k: v for k, v in _entry(1).items() if k != "id"}, wallet_journal_id=1))
    session.commit()
    corp = object.__new__(Corporation)
    corp.corporation_id = 9
    corp.corporation_name = "Corp"
    corp._db_app = SimpleNamespace(session=session)
    corp._default_esi_character = SimpleNamespace(ensure_esi=lambda: None, _esi_client=_PagedEsi([[_entry(1), _entry(2)], [_entry(3)]]))
    corp._resolve_client_names = lambda ids: {}
    corp._backfill_wallet_journal_party_names = lambda: None

    corp.refresh_wallet_journal()

    ids = sorted(r.wallet_journal_id for r in session.query(CorporationWalletJournalModel).all())
    assert ids == [1, 2, 3]
