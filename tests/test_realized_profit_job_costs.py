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

    assert _service(app_session, sde_session).next_fifo_unit_costs(owner_id=1)[MODULE] == (
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


def _late_tritanium_buy(app_session) -> None:
    """The job takes its Tritanium on 1 March; the only buy is on 1 April, so the job's
    40 come from the opening lot (priced at that first known cost, 5.0)."""
    app_session.query(CharacterWalletTransactionsModel).filter_by(transaction_id=501).one().date = "2026-04-01T00:00:00Z"
    app_session.add(MarketHistoryModel(type_id=TRITANIUM, region_id=10000002, date="2026-02-28", close=4.0,
                                       volume=1, order_count=1))
    app_session.commit()


def test_a_job_drawing_from_the_opening_lot_is_priced_at_the_start_date_close(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session)
    _late_tritanium_buy(app_session)

    _service(app_session, sde_session).rebuild(character_id=1)
    job = _jobs(app_session)[1001]

    assert job.materials_cost == 160.0  # 40 at the 28 Feb close of 4.0, not the first known 5.0
    assert job.build_cost_source == "ledger_fifo+market_estimate"
    [lot] = job.historical_input_costs[str(TRITANIUM)]["lots"]
    assert (lot["source"], lot["reference_type"], lot["unit_cost"]) == ("market_estimate", "opening_inventory", 4.0)


def test_a_sale_drawing_from_the_opening_lot_keeps_the_first_known_cost(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session)
    _late_tritanium_buy(app_session)
    app_session.add(CharacterWalletTransactionsModel(
        character_id=1, transaction_id=603, date="2026-02-15T00:00:00Z", is_buy=False, is_personal=True,
        quantity=5, type_id=TRITANIUM, type_name="Tritanium", unit_price=6.0, total_price=30.0))
    app_session.commit()

    [row] = _service(app_session, sde_session).rebuild(character_id=1)

    assert [(a["source"], a["quantity"], a["unit_cost"]) for a in row["allocation_details"]] == [
        ("opening_inventory", 5, 5.0)]


# --- corp rebuilds, keyed asset history, failures ------------------------------------

import pytest  # noqa: E402

from eve_online_industry_tracker.application.characters.asset_provenance import keep_ledger_job_cost  # noqa: E402
from eve_online_industry_tracker.application.characters.realized_profit import (  # noqa: E402
    CorporationRealizedProfitLedgerService,
)
from eve_online_industry_tracker.infrastructure.models import (  # noqa: E402
    CharacterAssetHistoryModel,
    CorporationAssetHistoryModel,
    CorporationIndustryJobsModel,
    CorporationModel,
    CorporationRealizedSalesLedgerModel,
    CorporationWalletTransactionsModel,
)

CORP = 50


def _corp_seed(app_session, sde_session) -> None:
    """Character job 1001 builds 40 modules from bought Tritanium (its ledger cost: 7.5);
    the corp's T2 job 2001 takes 10 of them; keyed history rows still carry 16.6M."""
    _seed(app_session, sde_session)
    app_session.query(CharacterIndustryJobsModel).filter_by(job_id=1002).delete()
    app_session.add(CorporationModel(corporation_id=CORP, corporation_name="Corp"))
    app_session.add(CorporationIndustryJobsModel(
        corporation_id=CORP, job_id=2001, status="delivered", activity_id=1, start_date="2026-05-01T00:00:00Z",
        end_date="2026-05-05T00:00:00Z", completed_date="2026-05-05T00:00:00Z", blueprint_type_id=T2_BP,
        product_type_id=T2, runs=10, successful_runs=10, cost=25.0, blueprint_material_efficiency=2,
        output_quantity=10, unit_build_cost=16_600_002.5, build_cost_source="historical_asset_acquisition_cost"))
    app_session.add(CorporationAssetHistoryModel(
        corporation_id=CORP, item_id=77, observed_at="2026-04-01T00:00:00Z", snapshot_source="asset_refresh",
        type_id=MODULE, quantity=30, acquisition_source="industry_build_transferred",
        acquisition_unit_cost=16_600_002.5, acquisition_total_cost=30 * 16_600_002.5,
        acquisition_reference_type="industry_job", acquisition_reference_id=1001))
    app_session.add(CharacterAssetHistoryModel(
        character_id=1, item_id=78, observed_at="2026-03-20T00:00:00Z", type_id=MODULE, quantity=40,
        acquisition_source="industry_build", acquisition_unit_cost=16_600_002.5,
        acquisition_total_cost=40 * 16_600_002.5, acquisition_reference_type="industry_job",
        acquisition_reference_id=1001))
    app_session.add(CorporationWalletTransactionsModel(
        corporation_id=CORP, division=1, transaction_id=701, date="2026-06-01T00:00:00Z", is_buy=False, quantity=2,
        type_id=T2, type_name="T2", unit_price=50.0, total_price=100.0))
    app_session.commit()


def _corp_service(app_session, sde_session):
    return CorporationRealizedProfitLedgerService(app_session=app_session, sde_session=sde_session, market_prices=[])


def test_a_corp_rebuild_prices_the_character_jobs_first(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _corp_seed(app_session, sde_session)

    _corp_service(app_session, sde_session).rebuild(corporation_id=CORP)
    jobs = _jobs(app_session)
    corp_job = app_session.query(CorporationIndustryJobsModel).filter_by(job_id=2001).one()

    assert jobs[1001].unit_build_cost == 7.5  # persisted by the corp rebuild itself
    [lot] = corp_job.historical_input_costs[str(MODULE)]["lots"]
    assert (lot["reference_id"], lot["source"], lot["unit_cost"]) == (1001, "industry_build_transferred", 7.5)
    assert corp_job.unit_build_cost == 10.0


def test_a_reprice_restamps_the_keyed_asset_history_rows(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _corp_seed(app_session, sde_session)

    _corp_service(app_session, sde_session).rebuild(corporation_id=CORP)

    for model in (CorporationAssetHistoryModel, CharacterAssetHistoryModel):
        row = app_session.query(model).filter_by(acquisition_reference_id=1001).one()
        assert (row.acquisition_unit_cost, row.acquisition_total_cost) == (7.5, 7.5 * row.quantity)


def test_a_failed_rebuild_leaves_every_row_as_it_was(session_provider, monkeypatch) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _corp_seed(app_session, sde_session)
    app_session.add(CorporationRealizedSalesLedgerModel(
        corporation_id=CORP, transaction_id=699, date="2026-01-01T00:00:00Z", quantity=1))
    app_session.commit()

    def broken(self, **kwargs):
        raise RuntimeError("fee breakdown failed")

    monkeypatch.setattr(CorporationRealizedProfitLedgerService, "_fee_breakdown", broken)
    with pytest.raises(RuntimeError):
        _corp_service(app_session, sde_session).rebuild(corporation_id=CORP)

    fresh = session_provider.app_session()
    assert fresh.query(CharacterIndustryJobsModel).filter_by(job_id=1001).one().unit_build_cost == 16_600_002.5
    assert fresh.query(CorporationAssetHistoryModel).one().acquisition_unit_cost == 16_600_002.5
    assert [row.transaction_id for row in fresh.query(CorporationRealizedSalesLedgerModel)] == [699]
    assert not app_session.dirty and not app_session.new and not app_session.deleted


def test_an_opening_lot_drawn_by_several_jobs_prices_each_at_its_own_start_date(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session)
    _late_tritanium_buy(app_session)  # 28 Feb close 4.0; the buy only on 1 April
    app_session.add(MarketHistoryModel(type_id=TRITANIUM, region_id=10000002, date="2026-03-09", close=4.5,
                                       volume=1, order_count=1))
    app_session.add(CharacterIndustryJobsModel(
        character_id=1, job_id=1003, status="delivered", activity_id=1, start_date="2026-03-10T00:00:00Z",
        end_date="2026-03-25T00:00:00Z", completed_date="2026-03-25T00:00:00Z", blueprint_type_id=MODULE_BP,
        product_type_id=MODULE, runs=40, successful_runs=40, cost=0.0, blueprint_material_efficiency=10,
        output_quantity=40, unit_build_cost=1.0))
    app_session.commit()

    _service(app_session, sde_session).rebuild(character_id=1)
    jobs = _jobs(app_session)

    assert (jobs[1001].materials_cost, jobs[1003].materials_cost) == (160.0, 180.0)


def test_the_job_sync_keeps_the_ledger_materials_and_recomputes_the_total() -> None:
    existing = CharacterIndustryJobsModel(
        job_id=1001, materials_cost=200.0, historical_materials_cost=200.0, historical_input_costs={"34": {}},
        historical_material_cost_source="ledger_fifo", historical_material_coverage_fraction=1.0,
        build_cost_source="ledger_fifo", total_build_cost=300.0, unit_build_cost=7.5)
    row = {"materials_cost": 999.0, "historical_materials_cost": 999.0, "historical_input_costs": None,
           "build_cost_source": "mixed", "cost": 150.0, "copy_cost": 10.0, "invention_cost": 40.0,
           "output_quantity": 50, "total_build_cost": 1.0, "unit_build_cost": 1.0}

    keep_ledger_job_cost(row, existing)

    assert (row["materials_cost"], row["historical_input_costs"], row["build_cost_source"]) == (
        200.0, {"34": {}}, "ledger_fifo")
    assert (row["total_build_cost"], row["unit_build_cost"]) == (400.0, 8.0)  # 200 + 150 + 10 + 40, over 50


def test_a_sync_change_to_a_ledger_priced_job_makes_the_ledger_stale(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _seed(app_session, sde_session, sale=True)
    service = _service(app_session, sde_session)
    service.rebuild(character_id=1)
    assert service.stale_resolved_cost_owner_ids(service.list_rows(character_id=1)) == set()

    job = _jobs(app_session)[1002]
    row = {name: getattr(job, name) for name in ("cost", "copy_cost", "output_quantity")} | {"invention_cost": 50.0}
    keep_ledger_job_cost(row, job)
    for name, value in row.items():
        setattr(job, name, value)
    app_session.commit()

    assert job.unit_build_cost == 15.0  # (75 + 25 + 50) / 10
    assert service.stale_resolved_cost_owner_ids(service.list_rows(character_id=1)) == {1}


# --- the facility's material bonus, the lock order ------------------------------------

import threading  # noqa: E402

from eve_online_industry_tracker.application.characters import realized_profit as rp  # noqa: E402
from eve_online_industry_tracker.infrastructure.models import IndustryProfilesModel  # noqa: E402

RAITARU = 1051424764880


def _facility_job(app_session, sde_session, *, profile: bool, recorded: int | None = None) -> None:
    """Job 1001: 10 runs of a blueprint taking 100 Tritanium a run, ME 0, at the Raitaru."""
    sde_session.add(Blueprints(blueprintTypeID=MODULE_BP, maxProductionLimit=100, activities={"manufacturing": {
        "materials": [{"typeID": TRITANIUM, "quantity": 100}], "products": [{"typeID": MODULE, "quantity": 1}]}}))
    sde_session.commit()
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    if profile:
        app_session.add(IndustryProfilesModel(character_id=1, profile_name="Raitaru", facility_id=RAITARU,
                                              material_efficiency_bonus=0.01))
    app_session.add(CharacterWalletTransactionsModel(
        character_id=1, transaction_id=501, date="2026-02-01T00:00:00Z", is_buy=True, is_personal=True,
        quantity=5000, type_id=TRITANIUM, type_name="Tritanium", unit_price=5.0, total_price=25000.0))
    app_session.add(CharacterIndustryJobsModel(
        character_id=1, job_id=1001, status="delivered", activity_id=1, start_date="2026-03-01T00:00:00Z",
        end_date="2026-03-20T00:00:00Z", completed_date="2026-03-20T00:00:00Z", blueprint_type_id=MODULE_BP,
        product_type_id=MODULE, runs=10, successful_runs=10, cost=0.0, blueprint_material_efficiency=0,
        facility_id=RAITARU, output_quantity=10, unit_build_cost=1.0,
        **({"build_cost_source": "ledger_fifo", "historical_input_costs": {str(TRITANIUM): {
            "quantity": recorded, "unit_cost": 5.0, "source": "ledger_fifo", "lots": []}}} if recorded else {})))
    app_session.commit()


def test_a_job_at_a_structure_with_a_material_bonus_drains_the_reduced_quantity(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _facility_job(app_session, sde_session, profile=True)

    replay = _service(app_session, sde_session)._replay_owner(owner_id=1)

    assert replay["inputs"]["draws"][(TRITANIUM, "wallet_transaction", 501)] == 990  # 1000 less the Raitaru's 1 %
    assert replay["inputs"]["me_only_jobs"] == 0


def test_an_earlier_me_only_quantity_does_not_keep_the_overdraw(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _facility_job(app_session, sde_session, profile=True, recorded=1000)

    _service(app_session, sde_session).rebuild(character_id=1)
    job = _jobs(app_session)[1001]

    assert job.historical_input_costs[str(TRITANIUM)]["quantity"] == 990
    assert job.materials_cost == 990 * 5.0


def test_a_job_at_a_facility_without_a_profile_uses_the_blueprint_me_only(session_provider) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _facility_job(app_session, sde_session, profile=False)

    replay = _service(app_session, sde_session)._replay_owner(owner_id=1)

    assert replay["inputs"]["draws"][(TRITANIUM, "wallet_transaction", 501)] == 1000
    assert replay["inputs"]["me_only_jobs"] == 1


def test_a_corp_rebuild_holds_every_character_lock_in_id_order_until_its_commit(session_provider, monkeypatch) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _corp_seed(app_session, sde_session)
    app_session.add(CharacterWalletTransactionsModel(
        character_id=3, transaction_id=502, date="2026-02-01T00:00:00Z", is_buy=True, is_personal=True,
        quantity=1, type_id=TRITANIUM, unit_price=5.0, total_price=5.0))
    app_session.commit()
    events: list[str] = []

    class _Recording:
        def __init__(self, name):
            self.name, self._lock = name, threading.Lock()

        def __enter__(self):
            self._lock.acquire()
            events.append(f"acquire {self.name}")

        def __exit__(self, *exc):
            events.append(f"release {self.name}")
            self._lock.release()

    locks: dict[tuple, _Recording] = {}
    monkeypatch.setattr(rp, "ledger_rebuild_lock",
                        lambda field, owner: locks.setdefault((field, owner), _Recording(f"{field}:{owner}")))
    real_commit = app_session.commit
    monkeypatch.setattr(app_session, "commit", lambda: (events.append("commit"), real_commit())[1])

    _corp_service(app_session, sde_session).rebuild(corporation_id=CORP)

    assert events == ["acquire character_id:1", "acquire character_id:3", "commit",
                      "release character_id:3", "release character_id:1"]


def _rigged_facility(app_session, sde_session, monkeypatch, *, effect_activity: str) -> list:
    """Three manufacturing jobs (three products) and a copy job at a Raitaru whose one
    rig has an effect for `effect_activity`; returns the get_type_data calls made."""
    for offset in range(3):
        sde_session.add(Blueprints(blueprintTypeID=MODULE_BP + 10 * offset, maxProductionLimit=100, activities={
            "manufacturing": {"materials": [{"typeID": TRITANIUM, "quantity": 100}],
                              "products": [{"typeID": MODULE + 10 * offset, "quantity": 1}]}}))
    sde_session.commit()
    app_session.add(CharacterModel(character_id=1, character_name="Builder"))
    app_session.add(IndustryProfilesModel(character_id=1, profile_name="Raitaru", facility_id=RAITARU,
                                          material_efficiency_bonus=0.01, rig_slot0_type_id=43920))
    for offset, activity in enumerate((1, 1, 1, 5)):
        app_session.add(CharacterIndustryJobsModel(
            character_id=1, job_id=1001 + offset, status="delivered", activity_id=activity,
            start_date=f"2026-03-0{offset + 1}T00:00:00Z", end_date="2026-03-20T00:00:00Z",
            completed_date="2026-03-20T00:00:00Z", blueprint_type_id=MODULE_BP + 10 * min(offset, 2),
            product_type_id=MODULE + 10 * min(offset, 2), runs=10, successful_runs=10, cost=0.0,
            blueprint_material_efficiency=0, facility_id=RAITARU, output_quantity=10, unit_build_cost=1.0))
    app_session.commit()
    monkeypatch.setattr(rp, "get_rig_effects_for_type_ids", lambda session, ids: [
        {"type_id": 43920, "effects": [{"activity": effect_activity, "metric": "material", "value": 0.02,
                                        "group": "All"}]}])
    calls: list = []
    monkeypatch.setattr(rp, "get_type_data", lambda session, language, ids: (
        calls.append(sorted(ids)), {i: {"type_id": i, "group_name": "Module", "category_name": "Module"} for i in ids})[1])
    return calls


def test_the_product_types_are_read_in_one_batched_lookup_per_replay(session_provider, monkeypatch) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    calls = _rigged_facility(app_session, sde_session, monkeypatch, effect_activity="manufacturing")

    replay = _service(app_session, sde_session)._replay_owner(owner_id=1)

    assert calls == [[MODULE, MODULE + 10, MODULE + 20]]
    # 1000 Tritanium a job, less the structure's 1 % and the rig's 2 %: ceil(1000 x 0.99 x 0.98) = 971.
    assert replay["inputs"]["untracked"] == 3 * 971


def test_without_a_manufacturing_material_rig_no_product_type_is_read(session_provider, monkeypatch) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    calls = _rigged_facility(app_session, sde_session, monkeypatch, effect_activity="research_me")

    _service(app_session, sde_session)._replay_owner(owner_id=1)

    assert calls == []


def test_only_manufacturing_jobs_look_up_their_facility(session_provider, monkeypatch) -> None:
    app_session, sde_session = session_provider.app_session(), session_provider.sde_session()
    _rigged_facility(app_session, sde_session, monkeypatch, effect_activity="research_me")
    looked_up: list[int] = []
    real = CharacterRealizedProfitLedgerService._facility_reductions
    monkeypatch.setattr(CharacterRealizedProfitLedgerService, "_facility_reductions",
                        lambda self, job: (looked_up.append(job.job_id), real(self, job))[1])

    _service(app_session, sde_session)._replay_owner(owner_id=1)

    assert sorted(looked_up) == [1001, 1002, 1003]  # not the copy job 1004
