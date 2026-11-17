"""Units an industry job takes as input leave the same FIFO queue a sale
takes from: oldest first, without a ledger row."""
from __future__ import annotations

from sqlalchemy import event

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
    CorporationAssetHistoryModel,
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
    # 80 built, 40 consumed, the other 40 sold: all from the October job.
    app_session.add(_character_sale(2001, "2026-11-01T00:00:00Z", 30))
    app_session.add(_character_sale(2002, "2026-11-16T00:00:00Z", 10))
    app_session.commit()

    rows = {row["transaction_id"]: row for row in _character_service(app_session, sde_session).rebuild(character_id=1)}

    assert sum(row["priced_quantity"] for row in rows.values()) == 40
    assert sum(row["unpriced_quantity"] for row in rows.values()) == 0
    priced_refs = [a["reference_id"] for row in rows.values() for a in row["allocation_details"] if a["unit_cost"]]
    assert set(priced_refs) == {1003}


def test_draining_more_than_the_queue_holds_creates_no_cost(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    app_session.add(_build(CharacterIndustryJobsModel, {"character_id": 1}, 1001, "2026-03-20T00:00:00Z", 40, 16_600_000.0))
    app_session.add(_t2_job(CharacterIndustryJobsModel, {"character_id": 1}, 1002, "2026-04-01T00:00:00Z", 100))
    app_session.commit()
    service = _character_service(app_session, sde_session)

    # 100 taken, 40 on hand: the rest came from stock before tracking, and nothing is left.
    assert service.rebuild(character_id=1) == []
    cost, reason = service.next_fifo_unit_cost(owner_scope="character", owner_id=1, type_id=MODULE)
    assert cost is None and reason
    assert app_session.query(CharacterRealizedSalesLedgerModel).count() == 0


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
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
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


def _corp_service(app_session, sde_session):
    return CorporationRealizedProfitLedgerService(app_session=app_session, sde_session=sde_session, market_prices=[])


def _corp_sale(transaction_id: int, date: str, quantity: int, type_id: int = MODULE):
    return CorporationWalletTransactionsModel(
        corporation_id=50, division=1, transaction_id=transaction_id, client_name="Buyer", date=date,
        is_buy=False, quantity=quantity, type_id=type_id, type_name="Module", unit_price=16_550_000.0,
        total_price=16_550_000.0 * quantity)


def test_a_stamped_residual_transfer_lot_is_not_drained_again_by_character_jobs(session_provider) -> None:
    """The corp asset history stamped the 24 units that reached the corp hangar,
    what was left of character job 1001 after the character's own T2 job took 16."""
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    char, corp = {"character_id": 1}, {"corporation_id": 50}
    app_session.add(CorporationModel(corporation_id=50, corporation_name="Corp"))
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    app_session.add(_build(CharacterIndustryJobsModel, char, 1001, "2026-03-20T00:00:00Z", 40, 16_600_000.0))
    app_session.add(_t2_job(CharacterIndustryJobsModel, char, 1002, "2026-03-31T00:00:00Z", 16))
    app_session.add(CorporationAssetHistoryModel(
        corporation_id=50, item_id=9001, observed_at="2026-06-29T00:00:00Z", snapshot_source="asset_refresh",
        type_id=MODULE, quantity=24, acquisition_source="industry_build_transferred",
        acquisition_unit_cost=16_600_000.0, acquisition_reference_type="industry_job",
        acquisition_reference_id=1001, acquisition_date="2026-03-20T09:41:24Z"))
    app_session.add(_build(CorporationIndustryJobsModel, corp, 1003, "2026-08-01T00:00:00Z", 40, 15_000_000.0))
    app_session.add(_corp_sale(3001, "2026-11-16T00:00:00Z", 30))
    app_session.commit()

    [row] = _corp_service(app_session, sde_session).rebuild(corporation_id=50)

    assert [(a["reference_id"], a["quantity"]) for a in row["allocation_details"]] == [(1001, 24), (1003, 6)]


def test_a_stamped_transfer_lot_takes_the_identity_of_the_character_residual(session_provider) -> None:
    """The character's own FIFO leaves 24 of job 1005; the transfer backfill
    stamped those same 24 units with job 1001. They count once, as job 1005:
    oldest first, job 1001's units went into the character's own T2 job."""
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    char, corp = {"character_id": 1}, {"corporation_id": 50}
    app_session.add(CorporationModel(corporation_id=50, corporation_name="Corp"))
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    app_session.add(_build(CharacterIndustryJobsModel, char, 1001, "2026-03-20T00:00:00Z", 40, 16_600_000.0))
    app_session.add(_build(CharacterIndustryJobsModel, char, 1005, "2026-04-10T00:00:00Z", 40, 16_000_000.0))
    app_session.add(_t2_job(CharacterIndustryJobsModel, char, 1002, "2026-04-20T00:00:00Z", 56))
    app_session.add(CorporationAssetHistoryModel(
        corporation_id=50, item_id=9001, observed_at="2026-06-29T00:00:00Z", snapshot_source="asset_refresh",
        type_id=MODULE, quantity=24, acquisition_source="industry_build_transferred",
        acquisition_unit_cost=16_600_000.0, acquisition_reference_type="industry_job",
        acquisition_reference_id=1001, acquisition_date="2026-03-20T09:41:24Z"))
    app_session.add(_build(CorporationIndustryJobsModel, corp, 1003, "2026-08-01T00:00:00Z", 40, 15_000_000.0))
    app_session.add(_corp_sale(3001, "2026-11-16T00:00:00Z", 30))
    app_session.commit()

    [row] = _corp_service(app_session, sde_session).rebuild(corporation_id=50)

    assert [(a["reference_id"], a["quantity"], a["unit_cost"]) for a in row["allocation_details"]] == [
        (1005, 24, 16_000_000.0), (1003, 6, 15_000_000.0)]


def _character_buy(character_id: int, transaction_id: int, date: str, quantity: int, unit_price: float):
    return CharacterWalletTransactionsModel(
        character_id=character_id, transaction_id=transaction_id, client_name="Seller", date=date, is_buy=True,
        is_personal=True, quantity=quantity, type_id=MODULE, type_name="Module", unit_price=unit_price,
        total_price=unit_price * quantity)


def test_a_lot_the_character_sold_personally_never_reaches_the_corp_queue(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    app_session.add(CorporationModel(corporation_id=50, corporation_name="Corp"))
    app_session.add(CharacterModel(character_id=1, character_name="Trader"))
    app_session.add(_character_buy(1, 4001, "2026-03-01T00:00:00Z", 3, 15_000_000.0))
    app_session.add(_character_sale(2001, "2026-03-10T00:00:00Z", 3))
    app_session.add(_corp_sale(3001, "2026-04-01T00:00:00Z", 3))
    app_session.commit()

    [row] = _corp_service(app_session, sde_session).rebuild(corporation_id=50)

    assert row["priced_quantity"] == 0
    assert row["unpriced_quantity"] == 3


def test_one_characters_stamp_excess_never_cancels_another_characters_residual(session_provider) -> None:
    """Character 1 used all of job 1001 itself, yet 24 of it are stamped in the
    corp hangar: that excess stays its own and leaves character 2's buy alone."""
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    app_session.add(CorporationModel(corporation_id=50, corporation_name="Corp"))
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    app_session.add(CharacterModel(character_id=2, character_name="Trader"))
    app_session.add(_build(CharacterIndustryJobsModel, {"character_id": 1}, 1001, "2026-03-20T00:00:00Z", 40, 16_600_000.0))
    app_session.add(_t2_job(CharacterIndustryJobsModel, {"character_id": 1}, 1002, "2026-04-01T00:00:00Z", 40))
    app_session.add(CorporationAssetHistoryModel(
        corporation_id=50, item_id=9001, observed_at="2026-06-29T00:00:00Z", snapshot_source="asset_refresh",
        type_id=MODULE, quantity=24, acquisition_source="industry_build_transferred",
        acquisition_unit_cost=16_600_000.0, acquisition_reference_type="industry_job",
        acquisition_reference_id=1001, acquisition_date="2026-03-20T09:41:24Z"))
    app_session.add(_character_buy(2, 4001, "2026-05-01T00:00:00Z", 30, 15_000_000.0))
    app_session.add(_corp_sale(3001, "2026-11-16T00:00:00Z", 40))
    app_session.commit()

    [row] = _corp_service(app_session, sde_session).rebuild(corporation_id=50)

    assert [(a["reference_id"], a["quantity"]) for a in row["allocation_details"]] == [(1001, 24), (4001, 16)]


def test_a_character_job_never_drains_corp_lots_its_own_ledger_did_not_have(session_provider) -> None:
    """Character 2 consumes 100 Tritanium from untracked stock; character 1's
    personal buy of 100 sits pooled in the corp queue and must stay there."""
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    app_session.add(CorporationModel(corporation_id=50, corporation_name="Corp"))
    app_session.add(CharacterModel(character_id=1, character_name="Buyer"))
    app_session.add(CharacterModel(character_id=2, character_name="Builder"))
    app_session.add(CharacterWalletTransactionsModel(
        character_id=1, transaction_id=4001, client_name="Seller", date="2026-03-01T00:00:00Z", is_buy=True,
        is_personal=True, quantity=100, type_id=34, type_name="Tritanium", unit_price=4.0, total_price=400.0))
    app_session.add(CharacterIndustryJobsModel(
        character_id=2, job_id=1002, status="delivered", activity_id=1, start_date="2026-04-01T00:00:00Z",
        end_date="2026-04-01T00:00:00Z", completed_date="2026-04-01T00:00:00Z", blueprint_type_id=MODULE_BP,
        product_type_id=MODULE, runs=100, successful_runs=100, blueprint_material_efficiency=0))
    app_session.commit()

    assert _corp_service(app_session, sde_session).next_fifo_unit_cost(
        owner_scope="corporation", owner_id=50, type_id=34) == (4.0, "fifo: wallet transaction 4001")


def test_the_opening_inventory_also_covers_what_jobs_consumed(session_provider) -> None:
    """Consume 50, sell 100, then buy: 150 units must have been on hand."""
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    app_session.add(_t2_job(CharacterIndustryJobsModel, {"character_id": 1}, 1002, "2026-01-01T00:00:00Z", 50))
    app_session.add(_character_sale(2001, "2026-02-01T00:00:00Z", 100))
    app_session.add(CharacterWalletTransactionsModel(
        character_id=1, transaction_id=2002, client_name="Seller", date="2026-03-01T00:00:00Z", is_buy=True,
        is_personal=True, quantity=100, type_id=MODULE, type_name="Module", unit_price=15_000_000.0,
        total_price=1_500_000_000.0))
    app_session.commit()

    [row] = _character_service(app_session, sde_session).rebuild(character_id=1)

    assert row["priced_quantity"] == 100
    assert row["unpriced_quantity"] == 0
    assert row["allocated_cost"] == 100 * 15_000_000.0


def test_next_fifo_unit_costs_reads_every_type_from_one_replay(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _march_april_october(app_session, sde_session)
    app_session.add(_t2_job(CharacterIndustryJobsModel, {"character_id": 1}, 1004, "2026-10-20T00:00:00Z", 1))
    app_session.commit()
    for job in app_session.query(CharacterIndustryJobsModel).filter_by(blueprint_type_id=T2_BP):
        job.output_quantity, job.unit_build_cost = job.runs, 20_000_000.0  # costed: the output lot needs no SDE
    app_session.commit()
    blueprint_queries: list[str] = []
    event.listen(sde_session.get_bind(), "before_cursor_execute",
                 lambda conn, cursor, statement, *args: blueprint_queries.append(statement)
                 if "FROM blueprints" in statement else None)

    costs = _character_service(app_session, sde_session).next_fifo_unit_costs(owner_scope="character", owner_id=1)

    assert costs[MODULE] == (13_500_000.0, "fifo: industry job 1003")
    assert len(blueprint_queries) == 1  # the three jobs' blueprints in one batched lookup


def _blended_stamp(quantity: int, observed: str = "2026-06-11T00:00:00Z"):
    """A blended transfer lot: an average over character jobs, naming none."""
    return CorporationAssetHistoryModel(
        corporation_id=50, item_id=9100, observed_at=observed, snapshot_source="asset_refresh", type_id=MODULE,
        quantity=quantity, acquisition_source="industry_build_transferred_avg", acquisition_unit_cost=15_000_000.0)


def _queue(app_session, sde_session) -> list[tuple]:
    lots = _corp_service(app_session, sde_session)._replay_owner(owner_id=50)["lots_by_type"][MODULE]
    return sorted((lot.reference_id or 0, lot.quantity, lot.unit_price, str(lot.acquisition_date)[:10]) for lot in lots)


def test_a_blended_stamp_takes_the_character_residual_it_stands_for(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    app_session.add(CorporationModel(corporation_id=50, corporation_name="Corp"))
    app_session.add(_build(CharacterIndustryJobsModel, {"character_id": 1}, 1001, "2026-03-20T00:00:00Z", 25, 16_000_000.0))
    app_session.add(_blended_stamp(30))
    app_session.commit()

    # 25 as the residual (identity and cost) at the stamp's date, 5 left blended; not 55.
    assert _queue(app_session, sde_session) == [
        (0, 5, 15_000_000.0, "2026-06-11"), (1001, 25, 16_000_000.0, "2026-06-11")]


def test_a_blended_stamp_takes_the_oldest_residuals_across_characters(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _blueprints(sde_session)
    app_session.add(CorporationModel(corporation_id=50, corporation_name="Corp"))
    app_session.add(_build(CharacterIndustryJobsModel, {"character_id": 1}, 1001, "2026-03-20T00:00:00Z", 20, 16_000_000.0))
    app_session.add(_build(CharacterIndustryJobsModel, {"character_id": 2}, 1005, "2026-03-10T00:00:00Z", 20, 14_000_000.0))
    app_session.add(_blended_stamp(30))
    app_session.commit()

    # Character 2's older 20, then 10 of character 1's; its other 10 stay a residual of their own.
    assert _queue(app_session, sde_session) == [
        (1001, 10, 16_000_000.0, "2026-03-20"), (1001, 10, 16_000_000.0, "2026-06-11"),
        (1005, 20, 14_000_000.0, "2026-06-11")]
