"""Realized profit: a lower-bound cost says so, and ledger rebuilds are locked.

A sale that consumed a lot whose invention cost was unknown carries
`cost_lower_bound_reason` on its ledger row (API contract), the allocation
keeps `cost_unknown_reason`, and feedback skips its cost update. When the
job's reason is later resolved (the decryptor re-snapshot), the owner's
ledger is stale and rebuilt. Every rebuild runs under a per-owner lock, per
process. Fake ids only.
"""
from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest
from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.characters import service as characters_service
from eve_online_industry_tracker.application.characters.realized_profit import (
    CharacterRealizedProfitLedgerService,
    CorporationRealizedProfitLedgerService,
)
from eve_online_industry_tracker.application.corporations import service as corporations_service
from eve_online_industry_tracker.application.daily_planner.feedback_processor import _industry_build_unit_cost
from eve_online_industry_tracker.infrastructure.models import (
    CharacterIndustryJobsModel,
    CharacterModel,
    CharacterWalletTransactionsModel,
)

_PILOT = 7001
_PRODUCT = 990003
_TARGET_BP = 990002
_REASON = "invention cost unknown: no corp character has its skills loaded"


def _seed(session, *, reason=_REASON):
    session.add(CharacterModel(character_name="Fake Pilot", character_id=_PILOT))
    session.add(CharacterIndustryJobsModel(
        character_id=_PILOT, job_id=5001, status="delivered", activity_id=1, runs=2,
        blueprint_type_id=_TARGET_BP, product_type_id=_PRODUCT, cost=40.0,
        completed_date="2026-10-01T00:00:00Z", output_quantity=2,
        total_build_cost=140.0, unit_build_cost=70.0, invention_cost_unknown_reason=reason,
        build_cost_source="market_snapshot_estimate",
    ))
    session.add(CharacterWalletTransactionsModel(
        character_id=_PILOT, transaction_id=6001, date="2026-10-02T00:00:00Z", is_buy=False,
        is_personal=True, quantity=2, type_id=_PRODUCT, unit_price=200.0, total_price=400.0,
    ))
    session.commit()


def _ledger(session):
    return CharacterRealizedProfitLedgerService(app_session=session, sde_session=None, market_prices=[])


def test_a_lower_bound_sale_names_its_reason_and_feedback_skips_it(app_session):
    _seed(app_session)
    _ledger(app_session).rebuild(character_id=_PILOT)
    [row] = _ledger(app_session).list_rows(character_id=_PILOT)
    assert row["cost_lower_bound_reason"] == _REASON
    [allocation] = row["allocation_details"]
    assert allocation["reference_id"] == 5001 and allocation["cost_unknown_reason"] == _REASON
    cost, skip_reason = _industry_build_unit_cost(row["allocation_details"])
    assert cost is None
    assert _REASON in skip_reason


def test_a_known_cost_sale_has_no_lower_bound_reason(app_session):
    _seed(app_session, reason=None)
    [row] = _ledger(app_session).rebuild(character_id=_PILOT)
    assert row["cost_lower_bound_reason"] is None
    assert _industry_build_unit_cost(row["allocation_details"]) == (70.0, None)


def test_a_resolved_job_reason_makes_the_ledger_stale(app_session):
    _seed(app_session)
    ledger = _ledger(app_session)
    ledger.rebuild(character_id=_PILOT)
    rows = ledger.list_rows(character_id=_PILOT)
    assert ledger.stale_resolved_cost_owner_ids(rows) == set()
    job = app_session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
    job.invention_cost_unknown_reason = None
    job.unit_build_cost = 90.0
    app_session.commit()
    assert ledger.stale_resolved_cost_owner_ids(rows) == {_PILOT}


def _state(engine):
    session = sessionmaker(bind=engine, future=True)()
    return SimpleNamespace(
        esi_service=SimpleNamespace(get_market_prices=lambda: []),
        db_app=SimpleNamespace(session=session), db_sde=SimpleNamespace(session=None),
        admin_settings=None,
    )


def test_the_character_read_rebuilds_a_ledger_whose_lower_bound_was_resolved(app_engine):
    state = _state(app_engine)
    _seed(state.db_app.session)
    service = characters_service.CharactersService(state=state)
    first = service.get_realized_profit_ledger(character_id=_PILOT)["rows"]
    assert first[0]["cost_lower_bound_reason"] == _REASON
    job = state.db_app.session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
    job.invention_cost_unknown_reason = None
    job.unit_build_cost = 90.0
    state.db_app.session.commit()
    [row] = service.get_realized_profit_ledger(character_id=_PILOT)["rows"]
    state.db_app.session.close()
    assert row["cost_lower_bound_reason"] is None
    assert row["allocated_cost"] == pytest.approx(180.0)


class _RacingLedger:
    """Both requests see no rows at first; the first rebuild fills them."""

    def __init__(self, barrier, rebuilt, owner_field):
        self._barrier = barrier
        self._rebuilt = rebuilt
        self._field = owner_field
        self._lock = threading.Lock()
        self._calls = 0

    def list_rows(self, **kwargs):
        owner_id = kwargs.get(self._field)
        with self._lock:
            self._calls += 1
            first_two = self._calls <= 2
        if first_two:
            self._barrier.wait()  # both requests find no rows
            return []
        return [{self._field: 1}] if self._rebuilt else []

    def owner_ids(self):
        return [1]

    def rebuild(self, **kwargs):
        time.sleep(0.05)
        self._rebuilt.append(kwargs.get(self._field))
        return []

    def stale_character_ids(self, rows):
        return set()

    def stale_resolved_cost_owner_ids(self, rows):
        return set()


def _race(read):
    errors: list[BaseException] = []

    def _run():
        try:
            read()
        except BaseException as exc:  # surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=_run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not errors


def test_two_reads_of_an_empty_character_ledger_rebuild_it_once(monkeypatch):
    barrier = threading.Barrier(2, timeout=5)
    rebuilt: list[int] = []
    shared = _RacingLedger(barrier, rebuilt, "character_id")
    monkeypatch.setattr(characters_service, "CharacterRealizedProfitLedgerService", lambda **_: shared)
    monkeypatch.setattr(characters_service, "summarize_realized_profit_rows", lambda rows: {})
    state = SimpleNamespace(
        esi_service=SimpleNamespace(get_market_prices=lambda: []),
        db_app=SimpleNamespace(session=None), db_sde=SimpleNamespace(session=None), admin_settings=None,
    )
    svc = characters_service.CharactersService(state=state)
    _race(lambda: svc.get_realized_profit_ledger(character_id=1))
    assert rebuilt == [1]


def test_two_reads_of_an_empty_corporation_ledger_rebuild_it_once(monkeypatch):
    barrier = threading.Barrier(2, timeout=5)
    rebuilt: list[int] = []
    shared = _RacingLedger(barrier, rebuilt, "corporation_id")
    monkeypatch.setattr(corporations_service, "CorporationRealizedProfitLedgerService", lambda **_: shared)
    monkeypatch.setattr(corporations_service, "summarize_realized_profit_rows", lambda rows: {})
    state = SimpleNamespace(
        esi_service=SimpleNamespace(get_market_prices=lambda: []),
        db_app=SimpleNamespace(session=None), db_sde=SimpleNamespace(session=None),
    )
    svc = corporations_service.CorporationsService(state=state)
    _race(lambda: svc.get_realized_profit_ledger(corporation_id=1))
    assert rebuilt == [1]


def test_a_refresh_rebuild_holds_the_owner_lock(monkeypatch):
    from eve_online_industry_tracker.application.characters.realized_profit import ledger_rebuild_lock

    seen: list[bool] = []

    class _Ledger:
        def __init__(self, **_):
            pass

        def list_rows(self, **_):
            return []

        def owner_ids(self):
            return [1]

        def rebuild(self, **_):
            seen.append(ledger_rebuild_lock("corporation_id", 1).locked())
            return []

    monkeypatch.setattr(corporations_service, "CorporationRealizedProfitLedgerService", _Ledger)
    monkeypatch.setattr(corporations_service, "summarize_realized_profit_rows", lambda rows: {})
    state = SimpleNamespace(
        esi_service=SimpleNamespace(get_market_prices=lambda: []),
        db_app=SimpleNamespace(session=None), db_sde=SimpleNamespace(session=None),
        corp_manager=SimpleNamespace(refresh_realized_profit_inputs=lambda **_: None),
    )
    corporations_service.CorporationsService(state=state).get_realized_profit_ledger(refresh=True)
    assert seen == [True]


def test_the_corporation_ledger_has_the_lower_bound_reason_too():
    row = SimpleNamespace(
        transaction_id=1, journal_ref_id=None, date=None, type_id=1, type_name=None, type_group_name=None,
        type_category_name=None, quantity=1, unit_price=1.0, gross_revenue=1.0, sales_tax_amount=0.0,
        other_fees_amount=0.0, total_fees_amount=0.0, net_revenue=1.0, allocated_cost=1.0, realized_profit=0.0,
        realized_margin_fraction=0.0, priced_quantity=1, unpriced_quantity=0, source_mix={},
        allocation_details=[
            {"source": "industry_build_transferred", "cost_unknown_reason": "B"},
            {"source": "industry_build_transferred", "cost_unknown_reason": "A"},
            {"source": "industry_build_transferred", "cost_unknown_reason": "B"},
        ],
        fee_capture_mode=None, confidence=None, notes=[], updated_at=None, corporation_id=1,
    )
    assert CorporationRealizedProfitLedgerService._serialize_row(row)["cost_lower_bound_reason"] == "B; A"
