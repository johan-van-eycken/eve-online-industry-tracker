"""J2.3: a cost-only change makes the realized profit ledger stale.

Each allocation of a job-referenced lot keeps the job's unit_build_cost it
was built from (`cost_basis`), and a blended corp transfer lot its asset
history row and stamped acquisition_unit_cost. The stale check compares them
with the current values for the rows' own lots only (bounded), so a
re-costed job or a re-stamped blended lot rebuilds the ledger on read even
when no lower-bound reason changed. A ledger built before the basis was
recorded is stale once. Fake ids only.
"""
from __future__ import annotations

from eve_online_industry_tracker.application.characters.realized_profit import (
    CharacterRealizedProfitLedgerService,
    CorporationRealizedProfitLedgerService,
)
from eve_online_industry_tracker.infrastructure.models import (
    CharacterIndustryJobsModel,
    CharacterModel,
    CharacterRealizedSalesLedgerModel,
    CharacterWalletTransactionsModel,
    CorporationAssetHistoryModel,
    CorporationModel,
    CorporationRealizedSalesLedgerModel,
    CorporationWalletTransactionsModel,
)

PILOT, CORP, PRODUCT = 7001, 98000001, 990003


def _job(s, *, character_id=PILOT, job_id=5001, unit=70.0, completed="2026-10-01T00:00:00Z"):
    s.add(CharacterIndustryJobsModel(
        character_id=character_id, job_id=job_id, status="delivered", activity_id=1, runs=2,
        blueprint_type_id=990002, product_type_id=PRODUCT, cost=40.0, completed_date=completed,
        output_quantity=2, total_build_cost=unit * 2, unit_build_cost=unit))


def _set_job_cost(s, unit, job_id=5001):
    job = s.query(CharacterIndustryJobsModel).filter_by(job_id=job_id).one()
    job.unit_build_cost = unit
    job.total_build_cost = unit * 2
    s.commit()


# --- characters ------------------------------------------------------------------


def _character(s):
    s.add(CharacterModel(character_name="Fake Pilot", character_id=PILOT))
    _job(s)
    s.add(CharacterWalletTransactionsModel(
        character_id=PILOT, transaction_id=6001, date="2026-10-02T00:00:00Z", is_buy=False, is_personal=True,
        quantity=2, type_id=PRODUCT, unit_price=200.0, total_price=400.0))
    s.commit()
    return CharacterRealizedProfitLedgerService(app_session=s, sde_session=None, market_prices=[])


def test_a_re_costed_job_makes_the_character_ledger_stale(app_session):
    ledger = _character(app_session)
    ledger.rebuild(character_id=PILOT)
    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(character_id=PILOT)) == set()

    _set_job_cost(app_session, 90.0)
    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(character_id=PILOT)) == {PILOT}
    [row] = ledger.rebuild(character_id=PILOT)
    assert row["allocated_cost"] == 180.0
    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(character_id=PILOT)) == set()


def test_a_character_ledger_built_without_a_cost_basis_is_stale_once(app_session):
    ledger = _character(app_session)
    ledger.rebuild(character_id=PILOT)
    row = app_session.query(CharacterRealizedSalesLedgerModel).one()
    row.allocation_details = [{k: v for k, v in a.items() if k != "cost_basis"} for a in row.allocation_details]
    app_session.commit()
    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(character_id=PILOT)) == {PILOT}
    ledger.rebuild(character_id=PILOT)
    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(character_id=PILOT)) == set()


# --- corporations ----------------------------------------------------------------


def _corporation(s, *, source, referenced):
    s.add(CorporationModel(corporation_id=CORP, corporation_name="Fake"))
    # The pilot's job also feeds the corp a lot of its own, completed after
    # the sale: the sale consumes only the history row.
    _job(s, job_id=5001, completed="2026-10-03T00:00:00Z")
    s.add(CorporationAssetHistoryModel(
        id=1, corporation_id=CORP, item_id=1, observed_at="2026-10-01T01:00:00Z", type_id=PRODUCT, quantity=2,
        acquisition_source=source, acquisition_unit_cost=70.0, acquisition_total_cost=140.0,
        acquisition_reference_type="industry_job" if referenced else None,
        acquisition_reference_id=5001 if referenced else None, acquisition_date="2026-10-01T00:00:00Z"))
    s.add(CorporationWalletTransactionsModel(
        corporation_id=CORP, transaction_id=6001, date="2026-10-02T00:00:00Z", is_buy=False, quantity=2,
        type_id=PRODUCT, unit_price=200.0, total_price=400.0))
    s.commit()
    return CorporationRealizedProfitLedgerService(app_session=s, sde_session=None, market_prices=[])


def _corp_stale(ledger):
    return ledger.stale_resolved_cost_owner_ids(ledger.list_rows(corporation_id=CORP))


def test_a_re_costed_job_makes_the_corporation_ledger_stale(app_session):
    ledger = _corporation(app_session, source="industry_build_transferred", referenced=True)
    ledger.rebuild(corporation_id=CORP)
    assert _corp_stale(ledger) == set()

    _set_job_cost(app_session, 90.0)
    assert _corp_stale(ledger) == {CORP}
    ledger.rebuild(corporation_id=CORP)
    assert _corp_stale(ledger) == set()


def test_a_re_stamped_blended_lot_makes_the_corporation_ledger_stale(app_session):
    ledger = _corporation(app_session, source="industry_build_transferred_avg", referenced=False)
    [row] = ledger.rebuild(corporation_id=CORP)
    blended = [a for a in row["allocation_details"] if a.get("asset_history_id") == 1]
    assert blended and blended[0]["cost_basis"] == 70.0
    assert _corp_stale(ledger) == set()

    history = app_session.query(CorporationAssetHistoryModel).filter_by(id=1).one()
    history.acquisition_unit_cost = 85.0
    history.acquisition_total_cost = 170.0
    app_session.commit()
    assert _corp_stale(ledger) == {CORP}
    rows = ledger.rebuild(corporation_id=CORP)
    assert 85.0 in {a["unit_cost"] for a in rows[0]["allocation_details"]}
    assert _corp_stale(ledger) == set()


def test_a_corporation_blended_lot_built_without_its_history_row_is_stale_once(app_session):
    ledger = _corporation(app_session, source="industry_build_transferred_avg", referenced=False)
    ledger.rebuild(corporation_id=CORP)
    row = app_session.query(CorporationRealizedSalesLedgerModel).one()
    row.allocation_details = [
        {k: v for k, v in a.items() if k not in ("cost_basis", "asset_history_id")} for a in row.allocation_details
    ]
    app_session.commit()
    assert _corp_stale(ledger) == {CORP}
    ledger.rebuild(corporation_id=CORP)
    assert _corp_stale(ledger) == set()


def test_an_unchanged_cost_is_not_stale(app_session):
    ledger = _corporation(app_session, source="industry_build_transferred", referenced=True)
    ledger.rebuild(corporation_id=CORP)
    _set_job_cost(app_session, 70.0)
    assert _corp_stale(ledger) == set()
