"""A pilot-built lot moved to the corp sits in the corp FIFO queue once, not
twice (the stamped asset-history lot and the raw character job); a pilot buy
paid from the corp wallet is not a transferred lot. Fake ids."""
from eve_online_industry_tracker.application.characters.realized_profit import (
    CorporationRealizedProfitLedgerService,
)
from eve_online_industry_tracker.infrastructure.models import (
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
    CorporationAssetHistoryModel,
    CorporationModel,
    CorporationWalletTransactionsModel,
)

CORP, JOB, PRODUCT = 98000001, 5001, 990003


def _corp_tx(tx_id, day, *, is_buy, unit_price):
    return CorporationWalletTransactionsModel(
        corporation_id=CORP, transaction_id=tx_id, date=f"2026-10-0{day}T00:00:00Z", is_buy=is_buy,
        quantity=10, type_id=PRODUCT, unit_price=unit_price, total_price=10 * unit_price)


def _rows_by_tx(s):
    rows = CorporationRealizedProfitLedgerService(app_session=s, sde_session=None, market_prices=[]).rebuild(
        corporation_id=CORP)
    return {r["transaction_id"]: r for r in rows}


def test_a_transferred_build_is_one_lot_in_the_corp_queue(app_session):
    app_session.add(CorporationModel(corporation_id=CORP, corporation_name="Fake"))
    app_session.add(CharacterIndustryJobsModel(
        character_id=7001, job_id=JOB, status="delivered", activity_id=1, runs=10,
        blueprint_type_id=990002, product_type_id=PRODUCT, cost=0.0, completed_date="2026-10-01T00:00:00Z",
        output_quantity=10, total_build_cost=1000.0, unit_build_cost=100.0))
    app_session.add(CorporationAssetHistoryModel(
        corporation_id=CORP, item_id=1, observed_at="2026-10-01T01:00:00Z", type_id=PRODUCT, quantity=10,
        acquisition_source="industry_build_transferred", acquisition_unit_cost=100.0,
        acquisition_reference_type="industry_job", acquisition_reference_id=JOB,
        acquisition_date="2026-10-01T00:00:00Z"))
    app_session.add_all([
        _corp_tx(6001, 2, is_buy=False, unit_price=200.0),
        _corp_tx(6002, 3, is_buy=True, unit_price=150.0),
        _corp_tx(6003, 4, is_buy=False, unit_price=200.0),
    ])
    app_session.commit()

    rows = _rows_by_tx(app_session)

    assert rows[6001]["allocated_cost"] == 1000.0
    assert rows[6003]["allocated_cost"] == 1500.0


def test_a_pilot_buy_paid_from_the_corp_wallet_is_not_a_transferred_lot(app_session):
    app_session.add(CorporationModel(corporation_id=CORP, corporation_name="Fake"))
    # The same purchase, seen from the pilot's and from the corp's wallet.
    app_session.add(CharacterWalletTransactionsModel(
        character_id=7001, transaction_id=7001, date="2026-10-01T00:00:00Z", is_buy=True, is_personal=False,
        quantity=10, type_id=PRODUCT, unit_price=100.0, total_price=1000.0))
    app_session.add_all([
        _corp_tx(6000, 1, is_buy=True, unit_price=100.0),
        _corp_tx(6001, 2, is_buy=False, unit_price=200.0),
        _corp_tx(6002, 3, is_buy=True, unit_price=150.0),
        _corp_tx(6003, 4, is_buy=False, unit_price=200.0),
    ])
    app_session.commit()

    rows = _rows_by_tx(app_session)

    assert rows[6001]["allocated_cost"] == 1000.0
    assert rows[6003]["allocated_cost"] == 1500.0


def _snapshot(observed_day, source, *, item_id=1, reference_id=JOB):
    return CorporationAssetHistoryModel(
        corporation_id=CORP, item_id=item_id, observed_at=f"2026-10-0{observed_day}T01:00:00Z", type_id=PRODUCT,
        quantity=10, acquisition_source=source, acquisition_unit_cost=100.0,
        acquisition_reference_type="industry_job", acquisition_reference_id=reference_id,
        acquisition_date="2026-10-01T00:00:00Z")


def test_every_refresh_snapshot_of_one_transferred_item_is_one_lot(app_session):
    app_session.add(CorporationModel(corporation_id=CORP, corporation_name="Fake"))
    # Each asset refresh copies the stamped stack again; the item is one lot of 10.
    app_session.add_all([_snapshot(1, "industry_build_transferred")] + [
        _snapshot(day, "industry_build_transferred_avg") for day in (2, 3, 4, 5)])
    app_session.add_all([
        _corp_tx(6001, 2, is_buy=False, unit_price=200.0),
        _corp_tx(6002, 3, is_buy=True, unit_price=150.0),
        _corp_tx(6003, 4, is_buy=False, unit_price=200.0),
    ])
    app_session.commit()

    rows = _rows_by_tx(app_session)

    assert rows[6001]["allocated_cost"] == 1000.0
    assert rows[6003]["allocated_cost"] == 1500.0


def test_a_snapshot_of_the_corps_own_buy_is_not_a_second_lot(app_session):
    app_session.add(CorporationModel(corporation_id=CORP, corporation_name="Fake"))
    # The corp's own buy is already an event; its asset snapshots carry the same cost.
    app_session.add_all([
        CorporationAssetHistoryModel(
            corporation_id=CORP, item_id=1, observed_at=f"2026-10-0{day}T01:00:00Z", type_id=PRODUCT,
            quantity=10, acquisition_source="market_buy", acquisition_unit_cost=100.0,
            acquisition_reference_type="wallet_transaction", acquisition_reference_id=6000,
            acquisition_date="2026-10-01T00:00:00Z")
        for day in (1, 2, 3)])
    app_session.add_all([
        _corp_tx(6000, 1, is_buy=True, unit_price=100.0),
        _corp_tx(6001, 2, is_buy=False, unit_price=200.0),
        _corp_tx(6002, 3, is_buy=True, unit_price=150.0),
        _corp_tx(6003, 4, is_buy=False, unit_price=200.0),
    ])
    app_session.commit()

    rows = _rows_by_tx(app_session)

    assert rows[6001]["allocated_cost"] == 1000.0
    assert rows[6003]["allocated_cost"] == 1500.0
