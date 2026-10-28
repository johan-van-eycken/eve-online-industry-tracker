"""Asset refresh snapshots copy a stack's stamped cost; the input cost lookup
counts each acquisition once, not once per refresh. Fake ids."""
from eve_online_industry_tracker.application.characters.asset_history import (
    build_historical_input_cost_lookup,
    enrich_assets_with_acquisition_costs,
    record_historical_acquisition,
    sync_asset_history,
)
from eve_online_industry_tracker.infrastructure.models import CorporationAssetHistoryModel

TRITANIUM = 34


def _buy(session, *, observed_at, unit_cost, reference_id):
    record_historical_acquisition(
        app_session=session, owner_kind="character", owner_id=1, observed_at=observed_at, type_id=TRITANIUM,
        type_name="Tritanium", quantity=1000, acquisition_source="wallet_transaction",
        acquisition_unit_cost=unit_cost, acquisition_total_cost=unit_cost * 1000,
        acquisition_reference_type="wallet_transaction", acquisition_reference_id=reference_id)
    session.commit()


def test_refresh_snapshots_do_not_dilute_the_input_cost(app_session):
    _buy(app_session, observed_at="2026-01-01T00:00:00Z", unit_cost=4.0, reference_id=111)
    asset = {"item_id": 9001, "type_id": TRITANIUM, "quantity": 1000, "location_id": 60003760}
    for day in range(2, 12):
        enriched = enrich_assets_with_acquisition_costs(
            app_session=app_session, owner_kind="character", owner_id=1, asset_list=[dict(asset)])
        sync_asset_history(app_session=app_session, owner_kind="character", owner_id=1, asset_rows=enriched,
                           observed_at=f"2026-01-{day:02d}T00:00:00Z")
        app_session.commit()
    _buy(app_session, observed_at="2026-01-12T00:00:00Z", unit_cost=6.0, reference_id=222)

    lookup = build_historical_input_cost_lookup(
        app_session=app_session, owner_kind="character", owner_id=1, as_of="2026-01-13T00:00:00Z",
        type_ids=[TRITANIUM])[TRITANIUM]

    assert sum(lot["quantity"] for lot in lookup["lots"]) == 2000
    assert lookup["unit_cost"] == 5.0


def test_a_transfer_stamp_counts_once_per_item(app_session):
    # A pilot-built stack moved to the corp only carries its cost on snapshot rows.
    app_session.add_all([
        CorporationAssetHistoryModel(
            corporation_id=1, item_id=9001, observed_at=f"2026-01-0{day}T00:00:00Z", snapshot_source="asset_refresh",
            type_id=TRITANIUM, quantity=1000, acquisition_source="character_market_buy_transferred",
            acquisition_unit_cost=4.0, acquisition_reference_type="wallet_transaction", acquisition_reference_id=111)
        for day in (1, 2, 3)])
    app_session.commit()

    lookup = build_historical_input_cost_lookup(
        app_session=app_session, owner_kind="corporation", owner_id=1, as_of="2026-01-09T00:00:00Z",
        type_ids=[TRITANIUM])[TRITANIUM]

    assert [lot["quantity"] for lot in lookup["lots"]] == [1000]
    assert lookup["unit_cost"] == 4.0
