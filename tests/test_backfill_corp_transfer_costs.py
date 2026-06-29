"""Unit tests for scripts/backfill_corp_transfer_costs.py."""

from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from eve_online_industry_tracker.infrastructure.models import (
    BaseApp,
    CharacterIndustryJobsModel,
    CorporationAssetHistoryModel,
)

from backfill_corp_transfer_costs import (
    backfill_corp_transfer_costs,
    build_character_job_cost_map,
    fifo_match_corp_snapshots,
    weighted_average_unit_cost,
)


def _make_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------

def test_weighted_average_unit_cost_computes_correctly() -> None:
    lots = [
        ("2026-01-01", 10.0, 100, 1),
        ("2026-01-02", 20.0, 200, 2),
    ]
    # (10*100 + 20*200) / 300 = 5000/300 = 16.666...
    result = weighted_average_unit_cost(lots)
    assert abs(result - (5000.0 / 300.0)) < 1e-6


def test_weighted_average_unit_cost_single_lot() -> None:
    lots = [("2026-01-01", 15.0, 50, 1)]
    assert weighted_average_unit_cost(lots) == 15.0


def test_weighted_average_unit_cost_empty_returns_zero() -> None:
    assert weighted_average_unit_cost([]) == 0.0


def test_fifo_match_assigns_oldest_lot_to_earliest_snapshot() -> None:
    """Two snapshots and two lots — FIFO assigns oldest lot to earliest snapshot.

    Lot 1 has 5 units (exactly consumed by snapshot 1), then lot 2 is used for snapshot 2.
    """

    class FakeSnapshot:
        def __init__(self, observed_at: str, quantity: int):
            self.observed_at = observed_at
            self.quantity = quantity

    snapshots = [
        FakeSnapshot("2026-02-01T00:00:00Z", 5),
        FakeSnapshot("2026-03-01T00:00:00Z", 5),
    ]
    # Lot 1 has exactly 5 units — fully consumed by snapshot 1
    # Lot 2 has 10 units — snapshot 2 draws from it
    job_lots = [
        ("2026-01-01T00:00:00Z", 10.0, 5, 1001),
        ("2026-02-15T00:00:00Z", 20.0, 10, 1002),
    ]

    results = fifo_match_corp_snapshots(snapshots, job_lots)

    # First snapshot gets lot from job 1001 (completed before 2026-02-01)
    assert results[0] is not None
    assert results[0]["job_id"] == 1001
    assert results[0]["unit_build_cost"] == 10.0

    # Second snapshot gets lot from job 1002 (completed before 2026-03-01, lot 1 exhausted)
    assert results[1] is not None
    assert results[1]["job_id"] == 1002
    assert results[1]["unit_build_cost"] == 20.0


def test_fifo_match_returns_none_when_no_lot_precedes_snapshot() -> None:
    """A snapshot whose observed_at predates all job lots gets no match."""

    class FakeSnapshot:
        def __init__(self, observed_at: str, quantity: int):
            self.observed_at = observed_at
            self.quantity = quantity

    snapshots = [FakeSnapshot("2026-01-01T00:00:00Z", 5)]
    # Job completed AFTER the snapshot
    job_lots = [("2026-06-01T00:00:00Z", 10.0, 10, 1001)]

    results = fifo_match_corp_snapshots(snapshots, job_lots)
    assert results[0] is None


# ---------------------------------------------------------------------------
# Integration-style tests using in-memory DB
# ---------------------------------------------------------------------------

def test_backfill_sets_cost_from_single_character_job_fifo() -> None:
    """A corp history snapshot with no cost gets FIFO-matched to the correct job."""
    session = _make_session()

    # Add a completed character industry job for type_id=100
    session.add(
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9001,
            status="delivered",
            product_type_id=100,
            completed_date="2026-01-10T00:00:00Z",
            output_quantity=10,
            unit_build_cost=50.0,
            total_build_cost=500.0,
            build_cost_source="historical_asset_acquisition_cost",
            raw={},
        )
    )

    # Corp asset history snapshot for that type — no cost yet
    session.add(
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=200001,
            observed_at="2026-01-15T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=100,
            type_name="Widget",
            quantity=5,
            acquisition_source=None,
            acquisition_unit_cost=None,
            acquisition_total_cost=None,
        )
    )
    session.commit()

    summary = backfill_corp_transfer_costs(session)

    assert summary["fifo_matched"] == 1
    assert summary["avg_matched"] == 0
    assert summary["unmatched"] == 0

    snapshot = session.query(CorporationAssetHistoryModel).filter_by(item_id=200001).first()
    assert snapshot is not None
    assert snapshot.acquisition_source == "industry_build_transferred"
    assert snapshot.acquisition_unit_cost == 50.0
    assert snapshot.acquisition_total_cost == 250.0  # 50 * 5
    assert snapshot.acquisition_reference_type == "industry_job"
    assert snapshot.acquisition_reference_id == 9001
    assert snapshot.acquisition_date == "2026-01-10T00:00:00Z"


def test_backfill_uses_weighted_average_when_no_fifo_match_possible() -> None:
    """When the snapshot predates all job completions, fallback to weighted avg."""
    session = _make_session()

    # Two jobs for type_id=200, both completed AFTER the snapshot
    session.add_all([
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9010,
            status="delivered",
            product_type_id=200,
            completed_date="2026-06-01T00:00:00Z",
            output_quantity=100,
            unit_build_cost=10.0,
            total_build_cost=1000.0,
            build_cost_source="test",
            raw={},
        ),
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9011,
            status="delivered",
            product_type_id=200,
            completed_date="2026-07-01T00:00:00Z",
            output_quantity=200,
            unit_build_cost=20.0,
            total_build_cost=4000.0,
            build_cost_source="test",
            raw={},
        ),
    ])

    # Corp asset history snapshot from BEFORE any job completed
    session.add(
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=300001,
            observed_at="2026-01-01T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=200,
            type_name="Gadget",
            quantity=10,
            acquisition_source=None,
            acquisition_unit_cost=None,
            acquisition_total_cost=None,
        )
    )
    session.commit()

    summary = backfill_corp_transfer_costs(session)

    assert summary["fifo_matched"] == 0
    assert summary["avg_matched"] == 1

    snapshot = session.query(CorporationAssetHistoryModel).filter_by(item_id=300001).first()
    assert snapshot is not None
    assert snapshot.acquisition_source == "industry_build_transferred_avg"

    # Weighted avg: (10*100 + 20*200) / 300 = 5000/300 ≈ 16.667
    expected_avg = (10.0 * 100 + 20.0 * 200) / 300
    assert abs(snapshot.acquisition_unit_cost - expected_avg) < 1e-6
    assert snapshot.acquisition_reference_type is None
    assert snapshot.acquisition_reference_id is None


def test_backfill_leaves_unmatched_when_no_character_job_exists() -> None:
    """Corp history rows for type_ids with no character jobs remain unchanged."""
    session = _make_session()

    # No character jobs at all
    session.add(
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=400001,
            observed_at="2026-01-15T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=999,
            type_name="Unknown Item",
            quantity=3,
            acquisition_source=None,
            acquisition_unit_cost=None,
            acquisition_total_cost=None,
        )
    )
    session.commit()

    summary = backfill_corp_transfer_costs(session)

    assert summary["fifo_matched"] == 0
    assert summary["avg_matched"] == 0
    assert summary["unmatched"] == 1

    snapshot = session.query(CorporationAssetHistoryModel).filter_by(item_id=400001).first()
    assert snapshot is not None
    assert snapshot.acquisition_source is None
    assert snapshot.acquisition_unit_cost is None


def test_backfill_skips_already_costed_rows() -> None:
    """Corp history rows that already have an acquisition_source are not touched."""
    session = _make_session()

    session.add(
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9020,
            status="delivered",
            product_type_id=500,
            completed_date="2026-01-05T00:00:00Z",
            output_quantity=10,
            unit_build_cost=100.0,
            total_build_cost=1000.0,
            build_cost_source="test",
            raw={},
        )
    )

    # Snapshot already has cost — should not be touched
    session.add(
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=500001,
            observed_at="2026-01-10T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=500,
            type_name="Pre-costed",
            quantity=2,
            acquisition_source="wallet_transaction",
            acquisition_unit_cost=88.0,
            acquisition_total_cost=176.0,
        )
    )
    session.commit()

    summary = backfill_corp_transfer_costs(session)

    assert summary["total_uncosted"] == 0
    assert summary["fifo_matched"] == 0

    snapshot = session.query(CorporationAssetHistoryModel).filter_by(item_id=500001).first()
    assert snapshot.acquisition_source == "wallet_transaction"
    assert snapshot.acquisition_unit_cost == 88.0


def test_backfill_multiple_snapshots_for_same_type_fifo_order() -> None:
    """Multiple corp history rows for the same type_id are assigned via FIFO.

    Job 1 has exactly 5 output units, snapshot 1 consumes it entirely.
    Snapshot 2 then draws from job 2.
    """
    session = _make_session()

    # Job 1: exactly 5 units (will be fully consumed by snapshot 1)
    # Job 2: 10 units (will supply snapshot 2)
    session.add_all([
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9030,
            status="delivered",
            product_type_id=600,
            completed_date="2026-01-01T00:00:00Z",
            output_quantity=5,
            unit_build_cost=30.0,
            total_build_cost=150.0,
            build_cost_source="test",
            raw={},
        ),
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9031,
            status="delivered",
            product_type_id=600,
            completed_date="2026-02-01T00:00:00Z",
            output_quantity=10,
            unit_build_cost=60.0,
            total_build_cost=600.0,
            build_cost_source="test",
            raw={},
        ),
    ])

    # Two corp snapshots: snapshot 1 in Jan (before job 2), snapshot 2 in Feb (after job 2)
    session.add_all([
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=600001,
            observed_at="2026-01-15T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=600,
            type_name="Part A",
            quantity=5,
            acquisition_source=None,
            acquisition_unit_cost=None,
        ),
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=600002,
            observed_at="2026-02-15T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=600,
            type_name="Part A",
            quantity=5,
            acquisition_source=None,
            acquisition_unit_cost=None,
        ),
    ])
    session.commit()

    summary = backfill_corp_transfer_costs(session)

    assert summary["fifo_matched"] == 2
    assert summary["avg_matched"] == 0

    snap1 = session.query(CorporationAssetHistoryModel).filter_by(item_id=600001).first()
    snap2 = session.query(CorporationAssetHistoryModel).filter_by(item_id=600002).first()

    # Oldest snapshot gets first lot (job 9030)
    assert snap1.acquisition_unit_cost == 30.0
    assert snap1.acquisition_reference_id == 9030

    # Second snapshot gets second lot (job 9031) after first lot is exhausted
    assert snap2.acquisition_unit_cost == 60.0
    assert snap2.acquisition_reference_id == 9031
