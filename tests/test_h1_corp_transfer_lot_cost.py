"""H1.1: a corp sale of a pilot-built lot keeps its lower-bound flag until the
job's cost is resolved, then takes the job's current cost; a blended
(average or multi-lot) transfer lot is flagged while any job of its type has
a partly unknown cost. Fake ids."""
from eve_online_industry_tracker.application.characters.realized_profit import (
    CorporationRealizedProfitLedgerService,
)
from eve_online_industry_tracker.application.daily_planner.feedback_processor import _industry_build_unit_cost
from eve_online_industry_tracker.infrastructure.models import (
    CharacterIndustryJobsModel,
    CorporationAssetHistoryModel,
    CorporationModel,
    CorporationWalletTransactionsModel,
)

CORP, JOB, PRODUCT = 98000001, 5001, 990003
R = "invention cost unknown: no decryptor choice"


def _seed(s, *, source="industry_build_transferred", referenced=True):
    s.add(CorporationModel(corporation_id=CORP, corporation_name="Fake"))
    s.add(CharacterIndustryJobsModel(
        character_id=7001, job_id=JOB, status="delivered", activity_id=1, runs=2,
        blueprint_type_id=990002, product_type_id=PRODUCT, cost=40.0, completed_date="2026-10-01T00:00:00Z",
        output_quantity=2, total_build_cost=140.0, unit_build_cost=70.0, invention_cost_unknown_reason=R))
    s.add(CorporationAssetHistoryModel(
        corporation_id=CORP, item_id=1, observed_at="2026-10-01T01:00:00Z", type_id=PRODUCT, quantity=2,
        acquisition_source=source, acquisition_unit_cost=70.0,
        acquisition_reference_type="industry_job" if referenced else None,
        acquisition_reference_id=JOB if referenced else None, acquisition_date="2026-10-01T00:00:00Z"))
    s.add(CorporationWalletTransactionsModel(
        corporation_id=CORP, transaction_id=6001, date="2026-10-02T00:00:00Z", is_buy=False, quantity=2,
        type_id=PRODUCT, unit_price=200.0, total_price=400.0))
    s.commit()


def _rebuild(s):
    ledger = CorporationRealizedProfitLedgerService(app_session=s, sde_session=None, market_prices=[])
    return ledger, ledger.rebuild(corporation_id=CORP)


def _resolve_job(s):
    job = s.query(CharacterIndustryJobsModel).filter_by(job_id=JOB).one()
    job.invention_cost_unknown_reason = None
    job.unit_build_cost = 90.0
    job.total_build_cost = 180.0
    s.commit()


def test_referenced_lot_is_a_lower_bound_before_and_the_jobs_current_cost_after_resolve(app_session):
    _seed(app_session)
    ledger, rows = _rebuild(app_session)
    assert rows[0]["allocated_cost"] == 140.0
    assert rows[0]["cost_lower_bound_reason"] == R

    _resolve_job(app_session)
    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(corporation_id=CORP)) == {CORP}
    _, rows = _rebuild(app_session)
    assert rows[0]["allocated_cost"] == 180.0
    assert rows[0]["cost_lower_bound_reason"] is None
    assert [a["unit_cost"] for a in rows[0]["allocation_details"]] == [90.0]
    assert _industry_build_unit_cost(rows[0]["allocation_details"]) == (90.0, None)


def test_avg_lot_is_flagged_while_a_job_of_its_type_has_a_reason(app_session):
    _seed(app_session, source="industry_build_transferred_avg", referenced=False)
    ledger, rows = _rebuild(app_session)
    assert rows[0]["cost_lower_bound_reason"] is not None
    assert R in rows[0]["cost_lower_bound_reason"]
    value, why = _industry_build_unit_cost(rows[0]["allocation_details"])
    assert value is None and R in why

    _resolve_job(app_session)
    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(corporation_id=CORP)) == {CORP}
    _, rows = _rebuild(app_session)
    assert rows[0]["cost_lower_bound_reason"] is None


def test_multi_lot_fifo_blend_without_job_reference_is_flagged(app_session):
    _seed(app_session, source="industry_build_transferred", referenced=False)
    _, rows = _rebuild(app_session)
    assert rows[0]["cost_lower_bound_reason"] is not None
    assert R in rows[0]["cost_lower_bound_reason"]
