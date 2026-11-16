"""Units an industry job takes as input leave the same FIFO queue a sale
takes from: oldest first, without a ledger row."""
from __future__ import annotations

from eve_online_industry_tracker.application.characters.realized_profit import (
    CharacterRealizedProfitLedgerService,
    CorporationRealizedProfitLedgerService,
)
from eve_online_industry_tracker.infrastructure.models import (
    Blueprints,
    CharacterIndustryJobsModel,
    CharacterModel,
    CharacterRealizedSalesLedgerModel,
    CharacterWalletTransactionsModel,
    CorporationIndustryJobsModel,
    CorporationModel,
    CorporationWalletTransactionsModel,
)

MODULE, T2_PRODUCT = 12200, 26888
MODULE_BP, T2_BP = 12201, 26889


def _blueprints(sde_session) -> None:
    sde_session.add(Blueprints(blueprintTypeID=MODULE_BP, maxProductionLimit=100, activities={"manufacturing": {
        "materials": [{"typeID": 34, "quantity": 1}], "products": [{"typeID": MODULE, "quantity": 1}]}}))
    sde_session.add(Blueprints(blueprintTypeID=T2_BP, maxProductionLimit=100, activities={"manufacturing": {
        "materials": [{"typeID": MODULE, "quantity": 1}], "products": [{"typeID": T2_PRODUCT, "quantity": 1}]}}))
    sde_session.commit()


def _build(model, owner: dict, job_id: int, completed: str, quantity: int, unit_cost: float):
    return model(**owner, job_id=job_id, status="delivered", activity_id=1, start_date=completed,
                 end_date=completed, completed_date=completed, blueprint_type_id=MODULE_BP,
                 product_type_id=MODULE, runs=quantity, successful_runs=quantity,
                 output_quantity=quantity, unit_build_cost=unit_cost, blueprint_material_efficiency=10)


def _t2_job(model, owner: dict, job_id: int, start: str, runs: int, **extra):
    return model(**owner, job_id=job_id, status="delivered", activity_id=1, start_date=start,
                 end_date=start, completed_date=start, blueprint_type_id=T2_BP, product_type_id=T2_PRODUCT,
                 runs=runs, successful_runs=runs, blueprint_material_efficiency=2, **extra)


def _character_sale(transaction_id: int, date: str, quantity: int):
    return CharacterWalletTransactionsModel(
        character_id=1, transaction_id=transaction_id, client_name="Buyer", date=date, is_buy=False,
        is_personal=True, quantity=quantity, type_id=MODULE, type_name="Mobile Large Warp Disruptor I",
        unit_price=16_550_000.0, total_price=16_550_000.0 * quantity)


def _character_service(app_session, sde_session):
    return CharacterRealizedProfitLedgerService(app_session=app_session, sde_session=sde_session, market_prices=[])


def _march_april_october(app_session, sde_session, *, t2_runs: int = 40) -> None:
    """40 built in March at 16.6M, 40 consumed by an April T2 job, 40 built in October at 13.5M."""
    _blueprints(sde_session)
    owner = {"character_id": 1}
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    app_session.add(_build(CharacterIndustryJobsModel, owner, 1001, "2026-03-20T00:00:00Z", 40, 16_600_000.0))
    app_session.add(_t2_job(CharacterIndustryJobsModel, owner, 1002, "2026-04-01T00:00:00Z", t2_runs))
    app_session.add(_build(CharacterIndustryJobsModel, owner, 1003, "2026-10-14T00:00:00Z", 40, 13_500_000.0))


def test_a_sale_after_a_job_took_the_oldest_units_is_matched_to_the_newer_lot(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _march_april_october(app_session, sde_session)
    app_session.add(_character_sale(2001, "2026-11-16T00:00:00Z", 2))
    app_session.commit()

    rows = _character_service(app_session, sde_session).rebuild(character_id=1)

    assert len(rows) == 1  # the job's input left without a ledger row
    [allocation] = rows[0]["allocation_details"]
    assert allocation["reference_id"] == 1003
    assert allocation["unit_cost"] == 13_500_000.0
    assert rows[0]["allocated_cost"] == 27_000_000.0


def test_a_unit_taken_by_a_job_is_never_also_sold(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _march_april_october(app_session, sde_session)
    # 80 built, 40 consumed: 50 sold means 40 priced and 10 untracked.
    app_session.add(_character_sale(2001, "2026-11-01T00:00:00Z", 30))
    app_session.add(_character_sale(2002, "2026-11-16T00:00:00Z", 20))
    app_session.commit()

    rows = {row["transaction_id"]: row for row in _character_service(app_session, sde_session).rebuild(character_id=1)}

    assert sum(row["priced_quantity"] for row in rows.values()) == 40
    assert sum(row["unpriced_quantity"] for row in rows.values()) == 10
    priced_refs = [a["reference_id"] for row in rows.values() for a in row["allocation_details"] if a["unit_cost"]]
    assert set(priced_refs) == {1003}


def test_draining_more_than_the_queue_holds_creates_no_cost(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _march_april_october(app_session, sde_session, t2_runs=100)  # 100 consumed, only 40 on hand
    app_session.add(_character_sale(2001, "2026-11-16T00:00:00Z", 50))
    app_session.commit()

    service = _character_service(app_session, sde_session)
    [row] = service.rebuild(character_id=1)

    # Only the October lot is left: the April job's excess came from untracked stock.
    assert row["priced_quantity"] == 40
    assert row["unpriced_quantity"] == 10
    assert row["allocated_cost"] == 40 * 13_500_000.0
    assert app_session.query(CharacterRealizedSalesLedgerModel).count() == 1


def test_recorded_input_quantity_wins_over_the_sde_when_it_is_larger(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    owner = {"character_id": 1}
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    app_session.add(_build(CharacterIndustryJobsModel, owner, 1001, "2026-03-20T00:00:00Z", 40, 16_600_000.0))
    # 10 runs by the SDE, but the job recorded taking 30 units of the module.
    app_session.add(_t2_job(CharacterIndustryJobsModel, owner, 1002, "2026-04-01T00:00:00Z", 10,
                            historical_input_costs={str(MODULE): {"quantity": 30, "unit_cost": 16_600_000.0}}))
    app_session.commit()

    cost, source = _character_service(app_session, sde_session).next_fifo_unit_cost(
        owner_scope="character", owner_id=1, type_id=MODULE)

    assert cost == 16_600_000.0  # 10 of the 40 left
    assert source == "fifo: industry job 1001"


def test_next_fifo_unit_cost_is_the_oldest_open_lot_after_replay(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _march_april_october(app_session, sde_session)
    app_session.add(_character_sale(2001, "2026-11-16T00:00:00Z", 2))
    app_session.commit()
    service = _character_service(app_session, sde_session)

    assert service.next_fifo_unit_cost(owner_scope="character", owner_id=1, type_id=MODULE) == (
        13_500_000.0, "fifo: industry job 1003")
    cost, reason = service.next_fifo_unit_cost(owner_scope="character", owner_id=1, type_id=999)
    assert cost is None and reason
    # Read-only: no ledger rows written.
    assert app_session.query(CharacterRealizedSalesLedgerModel).count() == 0


def test_the_corporation_queue_is_drained_by_character_and_corporation_jobs(session_provider) -> None:
    """Character-built lots sit in the corp queue (industry_build_transferred),
    so the character jobs that consumed them drain it too, as do corp jobs."""
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    char, corp = {"character_id": 1}, {"corporation_id": 50}
    app_session.add(CorporationModel(corporation_id=50, corporation_name="Corp"))
    app_session.add(_build(CharacterIndustryJobsModel, char, 1001, "2026-03-20T00:00:00Z", 40, 16_600_000.0))
    app_session.add(_t2_job(CharacterIndustryJobsModel, char, 1002, "2026-04-01T00:00:00Z", 30))
    app_session.add(_build(CorporationIndustryJobsModel, corp, 1003, "2026-08-01T00:00:00Z", 40, 15_000_000.0))
    app_session.add(_t2_job(CorporationIndustryJobsModel, corp, 1004, "2026-09-01T00:00:00Z", 20))
    app_session.add(_build(CorporationIndustryJobsModel, corp, 1005, "2026-10-14T00:00:00Z", 40, 13_500_000.0))
    app_session.add(CorporationWalletTransactionsModel(
        corporation_id=50, division=1, transaction_id=3001, client_name="Buyer", date="2026-11-16T00:00:00Z",
        is_buy=False, quantity=2, type_id=MODULE, type_name="Module", unit_price=16_550_000.0,
        total_price=33_100_000.0))
    app_session.commit()
    service = CorporationRealizedProfitLedgerService(app_session=app_session, sde_session=sde_session, market_prices=[])

    # 120 in; the char job takes 30 of 1001, the corp job its last 10 and 10 of 1003.
    [row] = service.rebuild(corporation_id=50)
    [allocation] = row["allocation_details"]
    assert allocation["reference_id"] == 1003
    assert service.next_fifo_unit_cost(owner_scope="corporation", owner_id=50, type_id=MODULE) == (
        15_000_000.0, "fifo: industry job 1003")
