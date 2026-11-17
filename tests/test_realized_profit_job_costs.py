"""A job's build cost comes from the ledger's FIFO replay: its inputs are
priced from the lots the replay drew for it, and its product lot enters the
same replay at that cost. Fake ids."""
from __future__ import annotations

from eve_online_industry_tracker.application.characters.realized_profit import CharacterRealizedProfitLedgerService
from eve_online_industry_tracker.infrastructure.models import (
    Blueprints,
    CharacterIndustryJobsModel,
    CharacterModel,
    CharacterWalletTransactionsModel,
    MarketHistoryModel,
)

TRITANIUM, MODULE, T2 = 34, 12200, 26888
MODULE_BP, T2_BP = 12201, 26889
JOB_FIELDS = ("materials_cost", "historical_materials_cost", "unit_build_cost", "total_build_cost",
              "historical_input_costs", "build_cost_source", "materials_cost_lower_bound_reason")


def _seed(app_session, sde_session, *, buy_tritanium: bool = True, sale: bool = False) -> None:
    sde_session.add(Blueprints(blueprintTypeID=MODULE_BP, maxProductionLimit=100, activities={"manufacturing": {
        "materials": [{"typeID": TRITANIUM, "quantity": 1}], "products": [{"typeID": MODULE, "quantity": 1}]}}))
    sde_session.add(Blueprints(blueprintTypeID=T2_BP, maxProductionLimit=100, activities={"manufacturing": {
        "materials": [{"typeID": MODULE, "quantity": 1}], "products": [{"typeID": T2, "quantity": 1}]}}))
    sde_session.commit()
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    if buy_tritanium:
        app_session.add(CharacterWalletTransactionsModel(
            character_id=1, transaction_id=501, date="2026-02-01T00:00:00Z", is_buy=True, is_personal=True,
            quantity=40, type_id=TRITANIUM, type_name="Tritanium", unit_price=5.0, total_price=200.0))
    # Both jobs carry an old cost from the job sync's own FIFO (16.6M a unit).
    app_session.add(CharacterIndustryJobsModel(
        character_id=1, job_id=1001, status="delivered", activity_id=1, start_date="2026-03-01T00:00:00Z",
        end_date="2026-03-20T00:00:00Z", completed_date="2026-03-20T00:00:00Z", blueprint_type_id=MODULE_BP,
        product_type_id=MODULE, runs=40, successful_runs=40, cost=100.0, blueprint_material_efficiency=10,
        output_quantity=40, materials_cost=664_000_000.0, total_build_cost=664_000_100.0,
        unit_build_cost=16_600_002.5, build_cost_source="historical_asset_acquisition_cost"))
    app_session.add(CharacterIndustryJobsModel(
        character_id=1, job_id=1002, status="delivered", activity_id=1, start_date="2026-04-01T00:00:00Z",
        end_date="2026-04-10T00:00:00Z", completed_date="2026-04-10T00:00:00Z", blueprint_type_id=T2_BP,
        product_type_id=T2, runs=10, successful_runs=10, cost=25.0, blueprint_material_efficiency=2,
        output_quantity=10, materials_cost=166_000_000.0, total_build_cost=166_000_025.0,
        unit_build_cost=16_600_002.5, build_cost_source="historical_asset_acquisition_cost"))
    if sale:
        app_session.add(CharacterWalletTransactionsModel(
            character_id=1, transaction_id=601, date="2026-05-01T00:00:00Z", is_buy=False, is_personal=True,
            quantity=2, type_id=T2, type_name="T2", unit_price=50.0, total_price=100.0))
    app_session.commit()


def _service(app_session, sde_session, market_prices=None):
    return CharacterRealizedProfitLedgerService(
        app_session=app_session, sde_session=sde_session, market_prices=market_prices or [])


def _jobs(app_session) -> dict[int, CharacterIndustryJobsModel]:
    return {job.job_id: job for job in app_session.query(CharacterIndustryJobsModel).all()}


def test_a_t2_job_takes_the_recomputed_cost_of_the_t1_job_that_built_its_inputs(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session)

    _service(app_session, sde_session).rebuild(character_id=1)
    jobs = _jobs(app_session)

    # T1: 40 Tritanium at 5 (the buy) + 100 install fee = 300 for 40 units.
    assert jobs[1001].materials_cost == 200.0
    assert jobs[1001].total_build_cost == 300.0
    assert jobs[1001].unit_build_cost == 7.5
    assert jobs[1001].build_cost_source == "ledger_fifo"
    assert jobs[1001].historical_input_costs[str(TRITANIUM)]["quantity"] == 40
    # T2: 10 of the T1 job's units at its recomputed 7.5, not the stale 16.6M.
    [lot] = jobs[1002].historical_input_costs[str(MODULE)]["lots"]
    assert (lot["reference_id"], lot["quantity"], lot["unit_cost"]) == (1001, 10, 7.5)
    assert jobs[1002].materials_cost == 75.0
    assert jobs[1002].unit_build_cost == 10.0  # (75 + 25) / 10


def test_an_untracked_input_is_priced_at_the_market_on_the_start_date(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session, buy_tritanium=False)
    app_session.add(MarketHistoryModel(type_id=TRITANIUM, region_id=10000002, date="2026-02-28", close=4.0, volume=1, order_count=1))
    app_session.commit()

    _service(app_session, sde_session).rebuild(character_id=1)
    job = _jobs(app_session)[1001]

    assert job.materials_cost == 160.0
    assert job.historical_materials_cost is None
    assert job.build_cost_source == "ledger_fifo+market_estimate"
    assert job.historical_input_costs[str(TRITANIUM)]["lots"][0]["source"] == "market_estimate"


def test_without_a_dated_price_the_current_market_price_is_the_fallback(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session, buy_tritanium=False)

    _service(app_session, sde_session, [{"type_id": TRITANIUM, "average_price": 6.0}]).rebuild(character_id=1)
    job = _jobs(app_session)[1001]

    assert job.materials_cost == 240.0
    assert job.materials_cost_lower_bound_reason is None


def test_a_job_with_an_input_no_lot_or_market_prices_keeps_its_cost(session_provider) -> None:
    """Never 0: without any price for its Tritanium the T1 job keeps the sync's cost."""
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session, buy_tritanium=False)

    _service(app_session, sde_session).rebuild(character_id=1)
    jobs = _jobs(app_session)

    assert jobs[1001].unit_build_cost == 16_600_002.5
    assert jobs[1001].build_cost_source == "historical_asset_acquisition_cost"
    # The T2 job draws the T1 lot at that kept cost.
    assert jobs[1002].historical_input_costs[str(MODULE)]["lots"][0]["unit_cost"] == 16_600_002.5


def test_the_unit_cost_a_job_recorded_is_the_last_market_fallback(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session, buy_tritanium=False)
    job = _jobs(app_session)[1001]
    job.historical_input_costs = {str(TRITANIUM): {"quantity": 40, "unit_cost": 3.0,
                                                    "source": "historical_market_daily_average"}}
    app_session.commit()
    service = _service(app_session, sde_session)

    service.rebuild(character_id=1)
    assert _jobs(app_session)[1001].materials_cost == 120.0
    service.rebuild(character_id=1)  # its own market estimate is then the fallback: unchanged
    assert _jobs(app_session)[1001].materials_cost == 120.0


def test_two_rebuilds_give_the_same_costs_and_no_stale_flag(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session, sale=True)
    service = _service(app_session, sde_session)

    service.rebuild(character_id=1)
    first = {job_id: tuple(getattr(job, f) for f in JOB_FIELDS) for job_id, job in _jobs(app_session).items()}
    rows = service.list_rows(character_id=1)
    assert service.stale_resolved_cost_owner_ids(rows) == set()
    service.rebuild(character_id=1)
    second = {job_id: tuple(getattr(job, f) for f in JOB_FIELDS) for job_id, job in _jobs(app_session).items()}

    assert first == second
    assert service.stale_resolved_cost_owner_ids(service.list_rows(character_id=1)) == set()
    assert rows[0]["allocated_cost"] == 20.0  # 2 T2 at their recomputed 10.0


def test_a_read_only_fifo_lookup_does_not_write_job_costs(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session)

    assert _service(app_session, sde_session).next_fifo_unit_costs(owner_scope="character", owner_id=1)[MODULE] == (
        7.5, "fifo: industry job 1001")
    app_session.commit()
    assert _jobs(app_session)[1001].unit_build_cost == 16_600_002.5


def test_an_opening_lot_priced_from_a_job_takes_its_recomputed_cost_on_the_first_rebuild(session_provider) -> None:
    """5 units sold before the first build: the opening lot is priced at the first known
    cost, job 1001's, and that is its ledger cost (7.5), not the stale 16.6M."""
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session)
    app_session.add(CharacterWalletTransactionsModel(
        character_id=1, transaction_id=602, date="2026-02-15T00:00:00Z", is_buy=False, is_personal=True,
        quantity=5, type_id=MODULE, type_name="Module", unit_price=50.0, total_price=250.0))
    app_session.commit()
    service = _service(app_session, sde_session)

    [row] = service.rebuild(character_id=1)
    assert [(a["source"], a["unit_cost"]) for a in row["allocation_details"]] == [("opening_inventory", 7.5)]
    first = {job_id: job.unit_build_cost for job_id, job in _jobs(app_session).items()}
    service.rebuild(character_id=1)
    assert {job_id: job.unit_build_cost for job_id, job in _jobs(app_session).items()} == first
