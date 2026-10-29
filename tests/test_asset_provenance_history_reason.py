"""The historical market-cost backfill and a failed or missing market history.

- A transient failure (the ESI history fetch failed: market_history_unavailable_reason,
  or an ESI error) never writes a partial lower-bound materials cost as final:
  the job is left as it was and retried at the next refresh, with a WARNING.
- A permanent gap (no history day on or before the job's start) writes the
  priced materials, names the unpriced one in historical_input_costs
  (unit_cost None, unknown_reason) and sets materials_cost_lower_bound_reason,
  which the realized-profit ledger carries as cost_lower_bound_reason.
- Only ESI, data and DB errors are caught (with a WARNING); a programming
  error propagates.
Fake values throughout."""
from __future__ import annotations

import logging
import sqlite3
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from eve_online_industry_tracker.application.characters import asset_provenance as provenance
from eve_online_industry_tracker.infrastructure.models import (
    CharacterIndustryJobsModel,
    CharacterModel,
    CharacterWalletTransactionsModel,
    CorporationIndustryJobsModel,
    MarketHistoryModel,
)
from eve_online_industry_tracker.infrastructure.schema_migrations import ensure_app_schema
from eve_online_industry_tracker.infrastructure.esi_client import EsiRequestError
from fakes import _ledger

REGION = 10000002
TRIT, PYE = 34, 35
REASON = "ESI market history unavailable: fake 502"


class _Esi:
    def __init__(self, history, failed=None, with_reason=True, raises=None):
        self._history = history
        self._failed = failed or {}
        self._raises = raises
        if with_reason:
            self.market_history_unavailable_reason = self._reason

    def get_market_history(self, type_ids, region_id=None):
        if self._raises is not None:
            raise self._raises
        return {t: self._history.get(t, []) for t in type_ids if t not in self._failed}

    def _reason(self, type_id, *, region_id=None):
        assert region_id == REGION
        return self._failed.get(int(type_id))


def _day(date, average):
    return {"date": date, "average": average, "highest": average, "lowest": average, "volume": 10, "order_count": 1}


def _price(app_session, esi, *, unavailable=None, transient=None):
    return provenance._get_or_fetch_market_price_on_date(
        type_id=TRIT, target_date="2026-10-01", app_session=app_session, esi_service=esi, region_id=REGION,
        unavailable=unavailable, transient=transient)


# --- the price lookup --------------------------------------------------------------------------------

def test_a_failed_history_fetch_records_the_esi_reason_as_transient(app_session):
    unavailable: dict[int, str] = {}
    transient: set[int] = set()
    assert _price(app_session, _Esi({}, failed={TRIT: REASON}), unavailable=unavailable, transient=transient) is None
    assert unavailable == {TRIT: REASON}
    assert transient == {TRIT}


def test_no_day_on_or_before_the_date_is_a_permanent_reason(app_session):
    unavailable: dict[int, str] = {}
    transient: set[int] = set()
    esi = _Esi({TRIT: [_day("2026-10-05", 5.0)]})
    assert _price(app_session, esi, unavailable=unavailable, transient=transient) is None
    assert "2026-10-01" in unavailable[TRIT]
    assert transient == set()


def test_a_stored_price_needs_no_fetch_and_has_no_reason(app_session):
    app_session.add(MarketHistoryModel(type_id=TRIT, region_id=REGION, date="2026-09-30", close=4.0,
                                       high=4.0, low=4.0, volume=1, order_count=1))
    app_session.flush()
    unavailable: dict[int, str] = {}
    assert _price(app_session, _Esi({}, failed={TRIT: REASON}), unavailable=unavailable) == 4.0
    assert unavailable == {}


def test_a_failed_fetch_without_a_reader_still_gets_a_reason(app_session):
    unavailable: dict[int, str] = {}
    _price(app_session, _Esi({}, failed={TRIT: REASON}, with_reason=False), unavailable=unavailable)
    assert unavailable[TRIT] and str(TRIT) in unavailable[TRIT]


def test_an_esi_error_is_a_transient_reason_with_a_warning(app_session, caplog):
    unavailable: dict[int, str] = {}
    transient: set[int] = set()
    with caplog.at_level(logging.WARNING):
        assert _price(app_session, _Esi({}, raises=EsiRequestError("HTTP 503")),
                      unavailable=unavailable, transient=transient) is None
    assert "HTTP 503" in unavailable[TRIT] and transient == {TRIT}
    assert "HTTP 503" in caplog.text


def test_a_programming_error_in_the_fetch_propagates(app_session):
    with pytest.raises(NameError):
        _price(app_session, _Esi({}, raises=NameError("oops")))


def test_an_unreadable_history_row_is_skipped_not_a_type_error(app_session, caplog):
    rows = [{"date": "2026-09-29", "average": None, "highest": 1, "lowest": 1, "volume": 1, "order_count": 1},
            _day("2026-09-30", 6.0)]
    with caplog.at_level(logging.WARNING):
        assert _price(app_session, _Esi({TRIT: rows})) == 6.0
    assert "skipped 1" in caplog.text


# --- the backfill ------------------------------------------------------------------------------------

class _Sde:
    """query(Blueprints).filter_by(...).first() -> one blueprint using Tritanium and Pyerite."""

    def __init__(self, raises=None):
        self._raises = raises

    def query(self, _model):
        if self._raises is not None:
            raise self._raises
        return self

    def filter_by(self, **_kw):
        return self

    def first(self):
        return SimpleNamespace(activities={"manufacturing": {"materials": [
            {"typeID": TRIT, "quantity": 10}, {"typeID": PYE, "quantity": 5}]}})


def _job(app_session):
    job = CharacterIndustryJobsModel(
        character_id=1, job_id=900, start_date="2026-10-01T10:00:00Z", activity_id=1,
        blueprint_type_id=700, product_type_id=701, runs=1, blueprint_material_efficiency=0,
        output_quantity=1, cost=0.0, materials_cost=80.0, build_cost_source="market_snapshot_estimate",
        historical_material_coverage_fraction=0.0, historical_input_costs=None)
    app_session.add(job)
    app_session.commit()
    return job


def _backfill(app_session, esi, sde=None):
    provenance.backfill_historical_market_costs(
        app_session=app_session, sde_session=sde or _Sde(), esi_service=esi,
        job_model=CharacterIndustryJobsModel, owner_filter={"character_id": 1})


def test_a_transient_failure_leaves_the_job_to_be_retried(app_session, caplog):
    job = _job(app_session)
    with caplog.at_level(logging.WARNING):
        _backfill(app_session, _Esi({TRIT: [_day("2026-10-01", 5.0)]}, failed={PYE: REASON}))
    app_session.refresh(job)
    assert job.historical_input_costs is None             # still picked up by the next backfill
    assert job.historical_material_coverage_fraction == 0.0
    assert job.build_cost_source == "market_snapshot_estimate"
    assert job.materials_cost == 80.0                     # the complete market estimate, not 50 as final
    assert job.materials_cost_lower_bound_reason is None
    assert REASON in caplog.text and "900" in caplog.text and "retried" in caplog.text


def test_a_permanent_gap_writes_a_named_lower_bound(app_session, caplog):
    job = _job(app_session)
    esi = _Esi({TRIT: [_day("2026-10-01", 5.0)], PYE: [_day("2026-10-09", 9.0)]})
    with caplog.at_level(logging.WARNING):
        _backfill(app_session, esi)
    app_session.refresh(job)
    costs = job.historical_input_costs
    assert costs[str(TRIT)]["unit_cost"] == 5.0
    assert costs[str(PYE)]["unit_cost"] is None and costs[str(PYE)]["quantity"] == 5
    assert "on or before 2026-10-01" in costs[str(PYE)]["unknown_reason"]
    assert job.materials_cost == 50.0
    assert job.historical_material_coverage_fraction == 10 / 15
    assert job.build_cost_source == "historical_market_price_on_start_date"
    assert "lower bound" in job.materials_cost_lower_bound_reason
    assert "on or before 2026-10-01" in job.materials_cost_lower_bound_reason
    assert "900" in caplog.text


def test_a_fully_priced_backfill_has_no_lower_bound_reason(app_session):
    job = _job(app_session)
    _backfill(app_session, _Esi({TRIT: [_day("2026-10-01", 5.0)], PYE: [_day("2026-10-01", 2.0)]}))
    app_session.refresh(job)
    assert job.materials_cost == 60.0
    assert job.materials_cost_lower_bound_reason is None


def test_the_backfill_with_no_priced_material_leaves_the_job_and_warns(app_session, caplog):
    job = _job(app_session)
    with caplog.at_level(logging.WARNING):
        _backfill(app_session, _Esi({}, failed={TRIT: REASON, PYE: REASON}))
    app_session.refresh(job)
    assert job.historical_input_costs is None
    assert job.build_cost_source == "market_snapshot_estimate"
    assert REASON in caplog.text and "900" in caplog.text


def test_a_db_error_in_the_backfill_skips_the_job_with_a_warning(app_session, caplog):
    job = _job(app_session)
    with caplog.at_level(logging.WARNING):
        _backfill(app_session, _Esi({}), sde=_Sde(raises=OperationalError("SELECT", {}, Exception("db locked"))))
    app_session.refresh(job)
    assert job.historical_input_costs is None
    assert "db locked" in caplog.text and "900" in caplog.text


def test_a_programming_error_in_the_backfill_propagates(app_session):
    _job(app_session)
    with pytest.raises(NameError):
        _backfill(app_session, _Esi({}), sde=_Sde(raises=NameError("oops")))


# --- the column and the realized-cost path -----------------------------------------------------------

def test_both_job_tables_have_a_nullable_lower_bound_column():
    for model in (CharacterIndustryJobsModel, CorporationIndustryJobsModel):
        assert model.__table__.columns["materials_cost_lower_bound_reason"].nullable


class _FakeDb:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, query, params=None):
        self._conn.execute(query, params) if params else self._conn.execute(query)
        self._conn.commit()

    def query(self, query, params=None):
        return (self._conn.execute(query, params) if params else self._conn.execute(query)).fetchall()


def test_the_schema_migration_adds_the_column_to_legacy_job_tables():
    conn = sqlite3.connect(":memory:")
    for table in ("character_industry_jobs", "corporation_industry_jobs"):
        conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER)")
    conn.commit()
    db = _FakeDb(conn)
    ensure_app_schema(db)
    ensure_app_schema(db)  # idempotent
    for table in ("character_industry_jobs", "corporation_industry_jobs"):
        assert "materials_cost_lower_bound_reason" in {row[1] for row in db.query(f"PRAGMA table_info({table})")}


_PILOT, _PRODUCT = 7001, 990003
_LOWER = "materials cost is a lower bound: 1 material(s) unpriced (no market history price on or before 2026-09-01)"


def _seed_sale(session, *, materials_reason, invention_reason=None):
    session.add(CharacterModel(character_name="Fake Pilot", character_id=_PILOT))
    session.add(CharacterIndustryJobsModel(
        character_id=_PILOT, job_id=5001, status="delivered", activity_id=1, runs=2,
        blueprint_type_id=990002, product_type_id=_PRODUCT, cost=40.0,
        completed_date="2026-10-01T00:00:00Z", output_quantity=2,
        total_build_cost=140.0, unit_build_cost=70.0, build_cost_source="historical_market_price_on_start_date",
        materials_cost_lower_bound_reason=materials_reason, invention_cost_unknown_reason=invention_reason,
    ))
    session.add(CharacterWalletTransactionsModel(
        character_id=_PILOT, transaction_id=6001, date="2026-10-02T00:00:00Z", is_buy=False,
        is_personal=True, quantity=2, type_id=_PRODUCT, unit_price=200.0, total_price=400.0,
    ))
    session.commit()


def test_a_sale_of_a_lower_bound_build_names_the_materials_reason(app_session):
    _seed_sale(app_session, materials_reason=_LOWER)
    [row] = _ledger(app_session).rebuild(character_id=_PILOT)
    assert row["cost_lower_bound_reason"] == _LOWER
    assert row["allocation_details"][0]["cost_unknown_reason"] == _LOWER


def test_the_materials_and_invention_reasons_are_both_named(app_session):
    _seed_sale(app_session, materials_reason=_LOWER, invention_reason="invention cost unknown: fake")
    [row] = _ledger(app_session).rebuild(character_id=_PILOT)
    assert "invention cost unknown: fake" in row["cost_lower_bound_reason"]
    assert _LOWER in row["cost_lower_bound_reason"]


def test_a_resolved_materials_reason_makes_the_ledger_stale(app_session):
    _seed_sale(app_session, materials_reason=_LOWER)
    ledger = _ledger(app_session)
    ledger.rebuild(character_id=_PILOT)
    rows = ledger.list_rows(character_id=_PILOT)
    assert ledger.stale_resolved_cost_owner_ids(rows) == set()
    job = app_session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
    job.materials_cost_lower_bound_reason = None
    app_session.commit()
    assert ledger.stale_resolved_cost_owner_ids(rows) == {_PILOT}
