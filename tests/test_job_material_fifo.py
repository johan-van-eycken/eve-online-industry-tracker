"""Job material costing is FIFO across jobs: a job takes its inputs from the
lots acquired before it started, minus what earlier jobs already used. Fake ids."""
import eve_online_industry_tracker.application.characters.character as character_module
from eve_online_industry_tracker.application.characters.asset_provenance import (
    earlier_jobs_consumed_input_lots,
    subtract_consumed_input_lots,
)
from eve_online_industry_tracker.application.characters.character import Character
from eve_online_industry_tracker.infrastructure.models import (
    Blueprints,
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
)

import pytest
from sqlalchemy.orm import sessionmaker

TRITANIUM, BLUEPRINT, PRODUCT = 34, 5002, 102


@pytest.fixture()
def sde_session(sde_engine):
    session = sessionmaker(bind=sde_engine, future=True)()
    try:
        yield session
    finally:
        session.close()


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


def _buy(tx_id, date, quantity, unit_price):
    return CharacterWalletTransactionsModel(
        character_id=1, transaction_id=tx_id, date=date, is_buy=True, is_personal=True, quantity=quantity,
        type_id=TRITANIUM, unit_price=unit_price, total_price=quantity * unit_price)


def _job(job_id, start, end):
    return {"job_id": job_id, "status": "delivered", "activity_id": 1, "blueprint_id": 70000 + job_id,
            "blueprint_type_id": BLUEPRINT, "product_type_id": PRODUCT, "runs": 1, "successful_runs": 1,
            "cost": 0.0, "start_date": start, "end_date": end, "completed_date": end}


def _refresh(monkeypatch, app_session, sde_session, *, buys, jobs):
    sde_session.add(Blueprints(blueprintTypeID=BLUEPRINT, maxProductionLimit=10, activities={"manufacturing": {
        "materials": [{"typeID": TRITANIUM, "quantity": 1000}], "products": [{"typeID": PRODUCT, "quantity": 1}]}}))
    sde_session.commit()
    app_session.add_all(buys)
    app_session.commit()
    monkeypatch.setattr(character_module, "build_market_price_map", lambda rows: {TRITANIUM: 5.0})
    monkeypatch.setattr(character_module, "build_invention_cost_per_run_by_blueprint_type", lambda **kwargs: {})
    monkeypatch.setattr(character_module, "lookup_historical_blueprint_provenance",
                        lambda **kwargs: {"is_blueprint_copy": False, "blueprint_material_efficiency": 0})
    character = object.__new__(Character)
    character.character_id, character.character_name = 1, "Builder"
    character._db_app, character._db_sde = _Db(app_session), _Db(sde_session)
    character._esi_client = _Esi(jobs)
    character.ensure_esi = lambda: None
    character.refresh_industry_jobs()
    return {row.job_id: row for row in app_session.query(CharacterIndustryJobsModel).filter_by(character_id=1)}


def test_a_later_job_does_not_reuse_stock_an_earlier_job_consumed(monkeypatch, app_session, sde_session):
    jobs = _refresh(monkeypatch, app_session, sde_session, buys=[
        _buy(1, "2026-01-01T00:00:00Z", 1000, 4.0), _buy(2, "2026-03-01T00:00:00Z", 1000, 6.0),
    ], jobs=[_job(11, "2026-02-01T00:00:00Z", "2026-02-02T00:00:00Z"),
             _job(12, "2026-04-01T00:00:00Z", "2026-04-02T00:00:00Z")])

    assert jobs[11].historical_materials_cost == 4000.0
    assert jobs[12].historical_materials_cost == 6000.0


def test_materials_bought_while_a_job_runs_do_not_count_for_it(monkeypatch, app_session, sde_session):
    jobs = _refresh(monkeypatch, app_session, sde_session, buys=[
        _buy(1, "2026-02-05T00:00:00Z", 1000, 4.0),
    ], jobs=[_job(11, "2026-02-01T00:00:00Z", "2026-02-10T00:00:00Z")])

    assert jobs[11].historical_materials_cost is None
    assert jobs[11].materials_cost == 5000.0


def test_partial_consumption_spans_two_lots(monkeypatch, app_session, sde_session):
    jobs = _refresh(monkeypatch, app_session, sde_session, buys=[
        _buy(1, "2026-01-01T00:00:00Z", 600, 4.0), _buy(2, "2026-02-01T00:00:00Z", 600, 6.0),
    ], jobs=[_job(11, "2026-03-01T00:00:00Z", "2026-03-02T00:00:00Z"),
             _job(12, "2026-04-01T00:00:00Z", "2026-04-02T00:00:00Z")])

    assert jobs[11].historical_materials_cost == 600 * 4.0 + 400 * 6.0
    assert jobs[12].historical_materials_cost == 200 * 6.0
    assert jobs[12].historical_material_coverage_fraction == 0.2


def test_a_re_snapshot_subtracts_what_stored_earlier_jobs_took(app_session):
    def stored(job_id, start, quantity):
        return CharacterIndustryJobsModel(
            character_id=1, job_id=job_id, start_date=start, historical_input_costs={"34": {"lots": [
                {"quantity": quantity, "reference_type": "wallet_transaction", "reference_id": 1}]}})

    # 21 started first; 22 starts at the same moment as 23 but has a lower job id; 24 starts later.
    app_session.add_all([stored(21, "2026-02-01", 300), stored(22, "2026-03-01", 200), stored(24, "2026-05-01", 999)])
    app_session.commit()
    consumed = earlier_jobs_consumed_input_lots(
        app_session, job_model=CharacterIndustryJobsModel, owner_field="character_id", owner_id=1,
        start_date="2026-03-01", job_id=23)
    lookup = {34: {"unit_cost": 4.0, "lots": [
        {"quantity": 400, "unit_cost": 4.0, "reference_type": "wallet_transaction", "reference_id": 1},
        {"quantity": 400, "unit_cost": 6.0, "reference_type": "wallet_transaction", "reference_id": 2}]}}

    left = subtract_consumed_input_lots(lookup, consumed)[34]["lots"]

    assert [(lot["reference_id"], lot["quantity"]) for lot in left] == [(2, 400)]
