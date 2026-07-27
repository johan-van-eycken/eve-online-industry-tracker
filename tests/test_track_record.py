from __future__ import annotations

import os
import sys
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402
from eve_online_industry_tracker.db_models import (  # noqa: E402
    BaseApp,
    BaseSde,
    CorporationRealizedSalesLedgerModel,
)


class _SessionProvider:
    def __init__(self, app_factory, sde_factory):
        self._app_factory = app_factory
        self._sde_factory = sde_factory

    def app_session(self) -> Session:
        return self._app_factory()

    def sde_session(self) -> Session:
        return self._sde_factory()

    def oauth_session(self) -> Session:
        raise RuntimeError("oauth session not needed in this test")


class _CorpManager:
    def __init__(self, corporations):
        self._corporations = corporations

    def get_corporations(self):
        return list(self._corporations)


def _make_sessions() -> tuple[sessionmaker, sessionmaker]:
    app_engine = create_engine("sqlite:///:memory:")
    sde_engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=app_engine)
    BaseSde.metadata.create_all(bind=sde_engine)
    return sessionmaker(bind=app_engine), sessionmaker(bind=sde_engine)


def _build_service(*, app_factory, sde_factory, corp_ids=None) -> IndustryService:
    if corp_ids is None:
        corp_ids = [1]
    corporations = [{"corporation_id": cid} for cid in corp_ids]
    service = object.__new__(IndustryService)
    service._sessions = _SessionProvider(app_factory, sde_factory)  # type: ignore[attr-defined]
    service._state = SimpleNamespace(  # type: ignore[attr-defined]
        esi_service=None,
        char_manager=None,
        corp_manager=_CorpManager(corporations),
    )
    return service


_TX_COUNTER = 1


def _add_ledger_row(
    session,
    *,
    corporation_id: int,
    type_id: int,
    date: str,
    realized_margin_fraction: float | None,
    transaction_id: int | None = None,
) -> None:
    global _TX_COUNTER
    if transaction_id is None:
        transaction_id = _TX_COUNTER
        _TX_COUNTER += 1
    session.add(
        CorporationRealizedSalesLedgerModel(
            corporation_id=corporation_id,
            transaction_id=transaction_id,
            type_id=type_id,
            date=date,
            quantity=1,
            realized_margin_fraction=realized_margin_fraction,
        )
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_track_record_proven() -> None:
    """Rows with avg margin 15% -> status 'proven'."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    _add_ledger_row(session, corporation_id=1, type_id=201, date="2026-06-01", realized_margin_fraction=0.15)
    _add_ledger_row(session, corporation_id=1, type_id=201, date="2026-06-15", realized_margin_fraction=0.15)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_type_track_record(type_ids=[201], today_iso="2026-06-30")

    assert 201 in result
    rec = result[201]
    assert rec["status"] == "proven"
    assert rec["sale_count"] == 2
    assert abs(rec["avg_margin_fraction"] - 0.15) < 1e-9


def test_track_record_unprofitable() -> None:
    """Rows with avg margin -5% -> status 'unprofitable'."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    _add_ledger_row(session, corporation_id=1, type_id=202, date="2026-06-10", realized_margin_fraction=-0.05)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_type_track_record(type_ids=[202], today_iso="2026-06-30")

    assert 202 in result
    rec = result[202]
    assert rec["status"] == "unprofitable"
    assert rec["sale_count"] == 1
    assert abs(rec["avg_margin_fraction"] - (-0.05)) < 1e-9


def test_track_record_marginal() -> None:
    """Rows with avg margin 5% -> status 'marginal'."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    _add_ledger_row(session, corporation_id=1, type_id=203, date="2026-06-05", realized_margin_fraction=0.05)
    _add_ledger_row(session, corporation_id=1, type_id=203, date="2026-06-20", realized_margin_fraction=0.05)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_type_track_record(type_ids=[203], today_iso="2026-06-30")

    assert 203 in result
    rec = result[203]
    assert rec["status"] == "marginal"
    assert rec["sale_count"] == 2
    assert abs(rec["avg_margin_fraction"] - 0.05) < 1e-9


def test_track_record_untested() -> None:
    """No rows in the lookback period -> status 'untested', sale_count=0."""
    app_factory, sde_factory = _make_sessions()
    # No ledger rows added at all
    session = app_factory()
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_type_track_record(type_ids=[204], today_iso="2026-06-30")

    assert 204 in result
    rec = result[204]
    assert rec["status"] == "untested"
    assert rec["avg_margin_fraction"] is None
    assert rec["sale_count"] == 0


def test_track_record_filters_by_corp() -> None:
    """Rows for corp_id=1 and corp_id=2; only corp_id=1 in corp_manager -> only corp_id=1 rows counted."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    # Corp 1: avg 0.20 (proven)
    _add_ledger_row(session, corporation_id=1, type_id=205, date="2026-06-10", realized_margin_fraction=0.20)
    # Corp 2: avg -0.10 (would push avg unprofitable if included)
    _add_ledger_row(session, corporation_id=2, type_id=205, date="2026-06-10", realized_margin_fraction=-0.10)
    session.commit()
    session.close()

    # Only corp_id=1 in the corp_manager
    service = _build_service(app_factory=app_factory, sde_factory=sde_factory, corp_ids=[1])
    result = service.get_type_track_record(type_ids=[205], today_iso="2026-06-30")

    assert 205 in result
    rec = result[205]
    # Should be proven (only corp 1's row counted)
    assert rec["status"] == "proven"
    assert rec["sale_count"] == 1
    assert abs(rec["avg_margin_fraction"] - 0.20) < 1e-9


def test_track_record_excludes_rows_outside_lookback() -> None:
    """Rows older than lookback_days are excluded -> untested."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    # Row is 100 days before today (outside default 90-day window)
    _add_ledger_row(session, corporation_id=1, type_id=206, date="2026-03-22", realized_margin_fraction=0.20)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_type_track_record(type_ids=[206], today_iso="2026-06-30")

    assert 206 in result
    rec = result[206]
    assert rec["status"] == "untested"
    assert rec["sale_count"] == 0


def test_track_record_excludes_null_margin() -> None:
    """Rows with NULL realized_margin_fraction are excluded."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    # One row with NULL margin — should be excluded, leaving type_id untested
    _add_ledger_row(session, corporation_id=1, type_id=207, date="2026-06-10", realized_margin_fraction=None)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory)
    result = service.get_type_track_record(type_ids=[207], today_iso="2026-06-30")

    assert 207 in result
    rec = result[207]
    assert rec["status"] == "untested"
    assert rec["sale_count"] == 0


def test_track_record_no_corps() -> None:
    """If corp_manager returns no corps, all type_ids are 'untested'."""
    app_factory, sde_factory = _make_sessions()
    session = app_factory()
    _add_ledger_row(session, corporation_id=1, type_id=208, date="2026-06-10", realized_margin_fraction=0.20)
    session.commit()
    session.close()

    service = _build_service(app_factory=app_factory, sde_factory=sde_factory, corp_ids=[])
    result = service.get_type_track_record(type_ids=[208], today_iso="2026-06-30")

    assert 208 in result
    rec = result[208]
    assert rec["status"] == "untested"
    assert rec["sale_count"] == 0
    assert rec["avg_margin_fraction"] is None
