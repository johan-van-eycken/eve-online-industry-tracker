"""J2.1: a re-costed character job re-stamps the blended corp transfer lots
of its product type (scripts/backfill_corp_transfer_costs.py's matching, now
importable from application/characters/corp_transfer_costs.py), bounded to
the affected types and idempotent; the ledger rebuilds through its stale
check, and a lot stays flagged while a contributing job has a reason. Fake ids."""
from __future__ import annotations

import pytest

from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters import character as character_module
from eve_online_industry_tracker.application.characters.character import Character
from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
    character_job_unit_costs,
    recosted_product_type_ids,
    restamp_blended_corp_transfer_lots,
)
from eve_online_industry_tracker.application.characters.realized_profit import (
    CorporationRealizedProfitLedgerService,
)
from eve_online_industry_tracker.infrastructure.models import (
    CharacterIndustryJobsModel,
    CorporationAssetHistoryModel,
    CorporationModel,
    CorporationWalletTransactionsModel,
)

CORP, PILOT = 98000001, 7001
PRODUCT, OTHER = 990003, 990013
R = "invention cost unknown: no decryptor choice"


def _job(s, job_id, *, product=PRODUCT, completed, qty, unit, reason=None):
    s.add(CharacterIndustryJobsModel(
        character_id=PILOT, job_id=job_id, status="delivered", activity_id=1, runs=qty,
        blueprint_type_id=990002, product_type_id=product, cost=1.0, completed_date=completed,
        output_quantity=qty, total_build_cost=unit * qty, unit_build_cost=unit,
        invention_cost_unknown_reason=reason))


def _row(s, row_id, *, product=PRODUCT, observed, qty, source, unit, job_id=None):
    s.add(CorporationAssetHistoryModel(
        id=row_id, corporation_id=CORP, item_id=row_id, observed_at=observed, type_id=product, quantity=qty,
        acquisition_source=source, acquisition_unit_cost=unit, acquisition_total_cost=unit * qty,
        acquisition_reference_type="industry_job" if job_id else None, acquisition_reference_id=job_id,
        acquisition_date=None))


def _seed(s):
    """Jobs 5001 (2 @ 70, a reason) and 5002 (3 @ 100) of PRODUCT; corp rows
    as the backfill stamped them: row 1 single-job (5001), row 2 a two-lot
    FIFO blend (5001 + 5002), row 3 the average fallback. OTHER has its own
    job and average row, untouched by a PRODUCT re-cost."""
    _job(s, 5001, completed="2026-06-01T00:00:00Z", qty=2, unit=70.0, reason=R)
    _job(s, 5002, completed="2026-06-02T00:00:00Z", qty=3, unit=100.0)
    _job(s, 5003, product=OTHER, completed="2026-06-01T00:00:00Z", qty=1, unit=10.0)
    _row(s, 1, observed="2026-06-03T00:00:00Z", qty=1, source="industry_build_transferred", unit=70.0, job_id=5001)
    _row(s, 2, observed="2026-06-04T00:00:00Z", qty=2, source="industry_build_transferred", unit=85.0)
    _row(s, 3, observed="2026-06-05T00:00:00Z", qty=5, source="industry_build_transferred_avg", unit=85.0)
    _row(s, 4, product=OTHER, observed="2026-06-05T00:00:00Z", qty=3, source="industry_build_transferred_avg",
         unit=10.0)
    s.commit()


def _resolve_5001(s, unit=90.0, reason=None):
    job = s.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
    job.unit_build_cost = unit
    job.total_build_cost = unit * 2
    job.invention_cost_unknown_reason = reason
    s.commit()


def _rows(s):
    return {r.id: r for r in s.query(CorporationAssetHistoryModel).all()}


def test_restamp_recomputes_the_blended_lots_of_the_type_only(app_session):
    _seed(app_session)
    _resolve_5001(app_session)

    changed = restamp_blended_corp_transfer_lots(app_session, [PRODUCT])
    app_session.commit()

    rows = _rows(app_session)
    assert changed == 2
    # The single-job row is priced from its job at ledger time: untouched.
    assert rows[1].acquisition_unit_cost == 70.0 and rows[1].acquisition_reference_id == 5001
    # FIFO over the type's transfer rows: row 1 takes one of 5001, row 2 the
    # other of 5001 and one of 5002 (a blend, no job), row 3 falls back to the average.
    assert rows[2].acquisition_unit_cost == pytest.approx((90.0 + 100.0) / 2)
    assert rows[2].acquisition_total_cost == pytest.approx(190.0)
    assert rows[2].acquisition_reference_id is None and rows[2].acquisition_reference_type is None
    assert rows[3].acquisition_source == "industry_build_transferred_avg"
    assert rows[3].acquisition_unit_cost == pytest.approx(96.0)  # (2 x 90 + 3 x 100) / 5
    assert rows[3].acquisition_total_cost == pytest.approx(480.0)
    assert rows[4].acquisition_unit_cost == 10.0  # another type: not affected


def test_restamp_is_idempotent(app_session):
    _seed(app_session)
    _resolve_5001(app_session)
    restamp_blended_corp_transfer_lots(app_session, [PRODUCT])
    app_session.commit()
    before = {i: (r.acquisition_source, r.acquisition_unit_cost, r.acquisition_reference_id)
              for i, r in _rows(app_session).items()}

    assert restamp_blended_corp_transfer_lots(app_session, [PRODUCT]) == 0
    app_session.commit()
    after = {i: (r.acquisition_source, r.acquisition_unit_cost, r.acquisition_reference_id)
             for i, r in _rows(app_session).items()}
    assert after == before


def test_restamp_of_no_types_or_a_type_without_jobs_changes_nothing(app_session):
    _seed(app_session)
    _resolve_5001(app_session)
    assert restamp_blended_corp_transfer_lots(app_session, []) == 0
    assert restamp_blended_corp_transfer_lots(app_session, [123456]) == 0
    assert _rows(app_session)[3].acquisition_unit_cost == 85.0


def test_recosted_types_are_the_jobs_whose_unit_cost_changed(app_session):
    _seed(app_session)
    before = character_job_unit_costs(app_session)
    _resolve_5001(app_session)
    _job(app_session, 5004, product=OTHER, completed="2026-06-09T00:00:00Z", qty=1, unit=12.0)  # new, not re-costed
    app_session.commit()
    assert recosted_product_type_ids(before, character_job_unit_costs(app_session)) == {PRODUCT}


def _ledger_with_sale(s):
    """A sale of all 11 units: job 5002's 3, then the 8 of the corp rows (job
    5001's lot is already in row 1 and the row 2 blend, so it is not queued twice)."""
    s.add(CorporationModel(corporation_id=CORP, corporation_name="Fake"))
    s.add(CorporationWalletTransactionsModel(
        corporation_id=CORP, transaction_id=6001, date="2026-06-06T00:00:00Z", is_buy=False, quantity=11,
        type_id=PRODUCT, unit_price=200.0, total_price=2200.0))
    s.commit()
    return CorporationRealizedProfitLedgerService(app_session=s, sde_session=None, market_prices=[])


def _blended_entries(row):
    return [a for a in row["allocation_details"] if a.get("reference_type") != "industry_job"]


def test_a_lot_stays_flagged_while_a_contributing_job_has_a_reason(app_session):
    _seed(app_session)
    ledger = _ledger_with_sale(app_session)
    _resolve_5001(app_session, unit=90.0, reason=R + " (still)")
    restamp_blended_corp_transfer_lots(app_session, [PRODUCT])
    app_session.commit()

    rows = ledger.rebuild(corporation_id=CORP)
    blended = _blended_entries(rows[0])
    assert blended and all(R in a["cost_unknown_reason"] for a in blended)
    assert rows[0]["cost_lower_bound_reason"] is not None


def test_after_resolve_the_ledger_goes_stale_and_takes_the_restamped_cost(app_session):
    _seed(app_session)
    ledger = _ledger_with_sale(app_session)
    rows = ledger.rebuild(corporation_id=CORP)
    assert {a["unit_cost"] for a in _blended_entries(rows[0])} == {85.0}

    _resolve_5001(app_session)
    restamp_blended_corp_transfer_lots(app_session, [PRODUCT])
    app_session.commit()

    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(corporation_id=CORP)) == {CORP}
    rows = ledger.rebuild(corporation_id=CORP)
    assert rows[0]["cost_lower_bound_reason"] is None
    assert sorted(a["unit_cost"] for a in _blended_entries(rows[0])) == pytest.approx([95.0, 96.0])
    assert all("cost_unknown_reason" not in a for a in rows[0]["allocation_details"])


# --- the re-cost paths re-stamp -------------------------------------------------


def test_the_decryptor_resnapshot_restamps_the_types_it_recosts(app_session, monkeypatch):
    _seed(app_session)
    job = app_session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
    job.invention_cost_unknown_reason = f"{ap.NO_DECRYPTOR_CHOICE_REASON} for blueprint 990002"
    job.raw = {"job_id": 5001, "activity_id": 1, "blueprint_type_id": 990002, "product_type_id": PRODUCT}
    app_session.commit()
    monkeypatch.setattr(ap, "build_invention_cost_per_run_by_blueprint_type", lambda **kw: {})
    monkeypatch.setattr(ap, "industry_job_material_type_ids", lambda **kw: [])
    monkeypatch.setattr(ap, "resolve_industry_job_cost_snapshot", lambda **kw: {
        "output_quantity": 2, "unit_cost": 90.0, "total_cost": 180.0, "invention_cost": 20.0,
        "invention_cost_unknown_reason": None, "source": "test"})
    from eve_online_industry_tracker.application.characters import asset_history
    monkeypatch.setattr(asset_history, "build_historical_input_cost_lookup", lambda **kw: {})

    assert ap.resnapshot_jobs_without_decryptor_choice(
        app_session=app_session, sde_session=None, market_price_map={},
        invention_refresh=ap.RealizedInventionRefresh(inputs=None)) == 1

    rows = _rows(app_session)
    assert rows[3].acquisition_unit_cost == pytest.approx(96.0)
    assert rows[4].acquisition_unit_cost == 10.0


class _Db:
    def __init__(self, session):
        self.session = session


class _Esi:
    def __init__(self, jobs):
        self._jobs = jobs

    def esi_get(self, endpoint, params=None, paginate=False):
        if endpoint.endswith("/industry/jobs"):
            return list(self._jobs)
        if endpoint.endswith("/markets/prices"):
            return []
        raise AssertionError(endpoint)


def test_a_character_job_refresh_restamps_the_types_it_recosts(app_session, monkeypatch):
    _seed(app_session)
    fetched = [{"job_id": 5001, "status": "delivered", "activity_id": 1, "runs": 2, "blueprint_type_id": 990002,
                "product_type_id": PRODUCT, "completed_date": "2026-06-01T00:00:00Z", "cost": 1.0}]
    monkeypatch.setattr(character_module, "build_market_price_map", lambda rows: {})
    monkeypatch.setattr(character_module, "build_invention_cost_per_run_by_blueprint_type", lambda **kw: {})
    monkeypatch.setattr(character_module, "industry_job_material_type_ids", lambda **kw: [])
    monkeypatch.setattr(character_module, "lookup_historical_blueprint_provenance", lambda **kw: None)
    monkeypatch.setattr(character_module, "build_historical_input_cost_lookup", lambda **kw: {})
    monkeypatch.setattr(character_module, "backfill_historical_market_costs", lambda **kw: None)
    monkeypatch.setattr(character_module, "resolve_industry_job_cost_snapshot", lambda **kw: {
        "output_quantity": 2, "unit_cost": 90.0, "total_cost": 180.0, "source": "test"})
    character = object.__new__(Character)
    character.character_id = PILOT
    character.character_name = "Builder"
    character._db_app = _Db(app_session)
    character._db_sde = _Db(None)
    character._esi_client = _Esi(fetched)
    character._esi_service = None
    character.ensure_esi = lambda: None

    character.refresh_industry_jobs()

    rows = _rows(app_session)
    assert rows[3].acquisition_unit_cost == pytest.approx(96.0)
    assert rows[4].acquisition_unit_cost == 10.0


# --- J3.1: the one-time re-stamp of every blended type ---------------------------------


def _stale_seed(s):
    """PRODUCT and OTHER both stamped from costs their jobs no longer have
    (blended rows at 85.0 and 10.0, jobs since re-costed to 90 and 12)."""
    _seed(s)
    _resolve_5001(s)
    other = s.query(CharacterIndustryJobsModel).filter_by(job_id=5003).one()
    other.unit_build_cost, other.total_build_cost = 12.0, 12.0
    s.commit()


def test_the_one_time_restamp_covers_every_blended_type_and_logs(app_session, caplog):
    import logging

    from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
        restamp_all_blended_corp_transfer_lots_once,
    )
    from eve_online_industry_tracker.infrastructure.models import AppMetaModel

    _stale_seed(app_session)
    with caplog.at_level(logging.INFO):
        changed = restamp_all_blended_corp_transfer_lots_once(app_session)

    rows = _rows(app_session)
    assert changed == 3
    assert rows[1].acquisition_unit_cost == 70.0                     # names its job: untouched
    assert rows[2].acquisition_unit_cost == pytest.approx(95.0)
    assert rows[3].acquisition_unit_cost == pytest.approx(96.0)
    assert rows[4].acquisition_unit_cost == pytest.approx(12.0)      # the other type too
    marker = app_session.get(AppMetaModel, "corp_transfer_blended_restamp_v1")
    assert marker is not None and marker.value == "3"
    assert any("One-time re-stamp" in r.getMessage() and "3" in r.getMessage() for r in caplog.records)


def test_the_one_time_restamp_runs_once_and_the_per_type_restamp_still_works(app_session, caplog):
    import logging

    from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
        restamp_all_blended_corp_transfer_lots_once,
    )

    _stale_seed(app_session)
    assert restamp_all_blended_corp_transfer_lots_once(app_session) == 3
    other = app_session.query(CharacterIndustryJobsModel).filter_by(job_id=5003).one()
    other.unit_build_cost, other.total_build_cost = 14.0, 14.0
    app_session.commit()

    caplog.clear()
    with caplog.at_level(logging.INFO):
        assert restamp_all_blended_corp_transfer_lots_once(app_session) is None   # the marker: done
    assert _rows(app_session)[4].acquisition_unit_cost == pytest.approx(12.0)
    assert not any("One-time re-stamp" in r.getMessage() for r in caplog.records)

    assert restamp_blended_corp_transfer_lots(app_session, [OTHER]) == 1
    app_session.commit()
    assert _rows(app_session)[4].acquisition_unit_cost == pytest.approx(14.0)


def test_the_one_time_restamp_on_an_empty_history_still_sets_the_marker(app_session):
    from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
        restamp_all_blended_corp_transfer_lots_once,
    )
    from eve_online_industry_tracker.infrastructure.models import AppMetaModel

    assert restamp_all_blended_corp_transfer_lots_once(app_session) == 0
    assert app_session.get(AppMetaModel, "corp_transfer_blended_restamp_v1").value == "0"
    assert restamp_all_blended_corp_transfer_lots_once(app_session) is None
