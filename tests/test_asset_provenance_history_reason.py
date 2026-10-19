"""The historical market-cost backfill names a failed ESI market history fetch:
a material whose daily price is unknown is in historical_input_costs with
unit_cost None and the ESI reason (market_history_unavailable_reason), never
silently left out of a cost that then reads as complete. Fake values throughout."""
from __future__ import annotations

import logging
from types import SimpleNamespace

from eve_online_industry_tracker.application.characters import asset_provenance as provenance
from eve_online_industry_tracker.infrastructure.models import CharacterIndustryJobsModel, MarketHistoryModel

REGION = 10000002
TRIT, PYE = 34, 35
REASON = "ESI market history unavailable: fake 502"


class _Esi:
    def __init__(self, history, failed=None, with_reason=True):
        self._history = history
        self._failed = failed or {}
        if with_reason:
            self.market_history_unavailable_reason = self._reason

    def get_market_history(self, type_ids, region_id=None):
        return {t: self._history.get(t, []) for t in type_ids if t not in self._failed}

    def _reason(self, type_id, *, region_id=None):
        assert region_id == REGION
        return self._failed.get(int(type_id))


def _day(date, average):
    return {"date": date, "average": average, "highest": average, "lowest": average, "volume": 10, "order_count": 1}


def test_a_failed_history_fetch_records_the_esi_reason(app_session):
    unavailable: dict[int, str] = {}
    price = provenance._get_or_fetch_market_price_on_date(
        type_id=TRIT, target_date="2026-10-01", app_session=app_session,
        esi_service=_Esi({}, failed={TRIT: REASON}), region_id=REGION, unavailable=unavailable)
    assert price is None
    assert unavailable == {TRIT: REASON}


def test_a_fetch_without_a_day_on_or_before_the_date_names_that(app_session):
    unavailable: dict[int, str] = {}
    price = provenance._get_or_fetch_market_price_on_date(
        type_id=TRIT, target_date="2026-10-01", app_session=app_session,
        esi_service=_Esi({TRIT: [_day("2026-10-05", 5.0)]}), region_id=REGION, unavailable=unavailable)
    assert price is None
    assert "2026-10-01" in unavailable[TRIT]


def test_a_stored_price_needs_no_fetch_and_has_no_reason(app_session):
    app_session.add(MarketHistoryModel(type_id=TRIT, region_id=REGION, date="2026-09-30", close=4.0,
                                       high=4.0, low=4.0, volume=1, order_count=1))
    app_session.flush()
    unavailable: dict[int, str] = {}
    price = provenance._get_or_fetch_market_price_on_date(
        type_id=TRIT, target_date="2026-10-01", app_session=app_session,
        esi_service=_Esi({}, failed={TRIT: REASON}), region_id=REGION, unavailable=unavailable)
    assert price == 4.0
    assert unavailable == {}


def test_a_failed_fetch_without_a_reader_still_gets_a_reason(app_session):
    unavailable: dict[int, str] = {}
    provenance._get_or_fetch_market_price_on_date(
        type_id=TRIT, target_date="2026-10-01", app_session=app_session,
        esi_service=_Esi({}, failed={TRIT: REASON}, with_reason=False), region_id=REGION, unavailable=unavailable)
    assert unavailable[TRIT] and str(TRIT) in unavailable[TRIT]


class _Sde:
    """query(Blueprints).filter_by(...).first() -> one blueprint using Tritanium and Pyerite."""

    def query(self, _model):
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
        output_quantity=1, cost=0.0, build_cost_source="market_snapshot_estimate",
        historical_material_coverage_fraction=0.0, historical_input_costs=None)
    app_session.add(job)
    app_session.commit()
    return job


def test_the_backfill_names_a_material_whose_history_fetch_failed(app_session, caplog):
    job = _job(app_session)
    esi = _Esi({TRIT: [_day("2026-10-01", 5.0)]}, failed={PYE: REASON})
    with caplog.at_level(logging.WARNING):
        provenance.backfill_historical_market_costs(
            app_session=app_session, sde_session=_Sde(), esi_service=esi,
            job_model=CharacterIndustryJobsModel, owner_filter={"character_id": 1})
    app_session.refresh(job)
    costs = job.historical_input_costs
    assert costs[str(TRIT)]["unit_cost"] == 5.0
    assert costs[str(PYE)]["unit_cost"] is None
    assert costs[str(PYE)]["quantity"] == 5
    assert costs[str(PYE)]["unknown_reason"] == REASON
    assert job.materials_cost == 50.0   # only the priced material, the unknown one is named, not 0
    assert job.historical_material_coverage_fraction == 10 / 15
    assert REASON in caplog.text and "900" in caplog.text


def test_the_backfill_with_no_priced_material_leaves_the_job_and_warns(app_session, caplog):
    job = _job(app_session)
    esi = _Esi({}, failed={TRIT: REASON, PYE: REASON})
    with caplog.at_level(logging.WARNING):
        provenance.backfill_historical_market_costs(
            app_session=app_session, sde_session=_Sde(), esi_service=esi,
            job_model=CharacterIndustryJobsModel, owner_filter={"character_id": 1})
    app_session.refresh(job)
    assert job.historical_input_costs is None   # retried at the next refresh
    assert job.build_cost_source == "market_snapshot_estimate"
    assert REASON in caplog.text and "900" in caplog.text
