"""Unit tests for the corp transfer cost backfill (scripts/backfill_corp_transfer_costs.py,
application/characters/corp_transfer_costs.py)."""

from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from eve_online_industry_tracker.application.characters.realized_profit import CorporationRealizedProfitLedgerService

from eve_online_industry_tracker.application.daily_planner.feedback_processor import _industry_build_unit_cost

from eve_online_industry_tracker.infrastructure.models import CorporationModel, CorporationWalletTransactionsModel
from eve_online_industry_tracker.application.characters import asset_provenance as ap
from eve_online_industry_tracker.application.characters import character as character_module
from eve_online_industry_tracker.application.characters.character import Character
from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
    character_job_unit_costs,
    recosted_product_type_ids,
    restamp_blended_corp_transfer_lots,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from eve_online_industry_tracker.infrastructure.models import (
    BaseApp,
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
    CorporationAssetHistoryModel,
)

from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
    backfill_corp_transfer_costs,
    build_character_buy_cost_map,
    build_character_job_cost_map,
    fifo_match_corp_snapshots,
    weighted_average_unit_cost,
)


@pytest.fixture
def db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()
    engine.dispose()


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


def test_fifo_match_blends_cost_when_snapshot_spans_multiple_lots() -> None:
    """When a snapshot's quantity spans two lots, unit_build_cost is a weighted average
    and job_id is None (since no single job covers the full snapshot quantity).

    Lot 1: 3 units @ 10.0 ISK each
    Lot 2: 7 units @ 20.0 ISK each
    Snapshot: 8 units  (consumes all of lot 1 and 5 from lot 2)
    Expected blended cost: (3*10 + 5*20) / 8 = (30 + 100) / 8 = 16.25
    """

    class FakeSnapshot:
        def __init__(self, observed_at: str, quantity: int):
            self.observed_at = observed_at
            self.quantity = quantity

    snapshots = [FakeSnapshot("2026-03-01T00:00:00Z", 8)]
    job_lots = [
        ("2026-01-01T00:00:00Z", 10.0, 3, 1001),
        ("2026-02-01T00:00:00Z", 20.0, 7, 1002),
    ]

    results = fifo_match_corp_snapshots(snapshots, job_lots)

    assert results[0] is not None
    # job_id must be None — no single job covers the full snapshot
    assert results[0]["job_id"] is None
    # Weighted average: (3*10 + 5*20) / 8 == 16.25
    expected = (3 * 10.0 + 5 * 20.0) / 8
    assert abs(results[0]["unit_build_cost"] - expected) < 1e-9
    assert results[0]["source"] == "industry_build_transferred"


# ---------------------------------------------------------------------------
# Integration-style tests using in-memory DB
# ---------------------------------------------------------------------------

def test_backfill_sets_cost_from_single_character_job_fifo(db_session: Session) -> None:
    """A corp history snapshot with no cost gets FIFO-matched to the correct job."""
    session = db_session

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


def test_backfill_uses_weighted_average_when_no_fifo_match_possible(db_session: Session) -> None:
    """When the snapshot predates all job completions, fallback to weighted avg."""
    session = db_session

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


def test_backfill_leaves_unmatched_when_no_character_job_exists(db_session: Session) -> None:
    """Corp history rows for type_ids with no character jobs remain unchanged."""
    session = db_session

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


def test_backfill_skips_already_costed_rows(db_session: Session) -> None:
    """Corp history rows that already have an acquisition_source are not touched."""
    session = db_session

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


def test_backfill_multi_lot_snapshot_uses_blended_cost(db_session: Session) -> None:
    """A single corp snapshot whose quantity spans two job lots gets a blended unit cost.

    Lot 1: 3 units @ 10.0  (job 9040)
    Lot 2: 10 units @ 20.0 (job 9041)
    Snapshot: 8 units (consumes all 3 from lot 1, then 5 from lot 2)
    Expected blended unit_cost = (3*10 + 5*20) / 8 = 16.25
    acquisition_reference_id must be None (multi-lot match).
    """
    session = db_session

    session.add_all([
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9040,
            status="delivered",
            product_type_id=700,
            completed_date="2026-01-01T00:00:00Z",
            output_quantity=3,
            unit_build_cost=10.0,
            total_build_cost=30.0,
            build_cost_source="test",
            raw={},
        ),
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9041,
            status="delivered",
            product_type_id=700,
            completed_date="2026-02-01T00:00:00Z",
            output_quantity=10,
            unit_build_cost=20.0,
            total_build_cost=200.0,
            build_cost_source="test",
            raw={},
        ),
    ])

    session.add(
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=700001,
            observed_at="2026-03-01T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=700,
            type_name="BlendItem",
            quantity=8,
            acquisition_source=None,
            acquisition_unit_cost=None,
            acquisition_total_cost=None,
        )
    )
    session.commit()

    summary = backfill_corp_transfer_costs(session)

    assert summary["fifo_matched"] == 1
    assert summary["avg_matched"] == 0

    snapshot = session.query(CorporationAssetHistoryModel).filter_by(item_id=700001).first()
    assert snapshot is not None
    assert snapshot.acquisition_source == "industry_build_transferred"
    # Blended: (3*10 + 5*20) / 8 == 16.25
    expected_cost = (3 * 10.0 + 5 * 20.0) / 8
    assert abs(snapshot.acquisition_unit_cost - expected_cost) < 1e-9
    assert abs(snapshot.acquisition_total_cost - expected_cost * 8) < 1e-6
    # No single job covers this snapshot
    assert snapshot.acquisition_reference_id is None
    assert snapshot.acquisition_reference_type is None


def test_backfill_excludes_null_status_jobs(db_session: Session) -> None:
    """Jobs with status=None are excluded from the cost map by the DB-level filter."""
    session = db_session

    # A job with status=None — should NOT be used for FIFO matching
    session.add(
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9050,
            status=None,
            product_type_id=800,
            completed_date="2026-01-01T00:00:00Z",
            output_quantity=10,
            unit_build_cost=75.0,
            total_build_cost=750.0,
            build_cost_source="test",
            raw={},
        )
    )

    session.add(
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=800001,
            observed_at="2026-02-01T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=800,
            type_name="NullStatusItem",
            quantity=5,
            acquisition_source=None,
            acquisition_unit_cost=None,
            acquisition_total_cost=None,
        )
    )
    session.commit()

    cost_map = build_character_job_cost_map(session)
    # type_id=800 must not appear — the only job has status=None
    assert 800 not in cost_map

    summary = backfill_corp_transfer_costs(session)
    assert summary["unmatched"] == 1
    assert summary["fifo_matched"] == 0
    assert summary["avg_matched"] == 0

    snapshot = session.query(CorporationAssetHistoryModel).filter_by(item_id=800001).first()
    assert snapshot.acquisition_source is None
    assert snapshot.acquisition_unit_cost is None


def test_backfill_multiple_snapshots_for_same_type_fifo_order(db_session: Session) -> None:
    """Multiple corp history rows for the same type_id are assigned via FIFO.

    Job 1 has exactly 5 output units, snapshot 1 consumes it entirely.
    Snapshot 2 then draws from job 2.
    """
    session = db_session

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


# ---------------------------------------------------------------------------
# New tests: Gap 1 (untracked_inventory) and Gap 2 (buy transactions)
# ---------------------------------------------------------------------------

def test_build_character_buy_cost_map_returns_fifo_sorted_by_date(db_session: Session) -> None:
    """build_character_buy_cost_map returns buy transactions sorted oldest-first per type."""
    session = db_session

    # Insert two buy transactions for the same type_id with different dates (newer first)
    session.add_all([
        CharacterWalletTransactionsModel(
            character_id=1,
            transaction_id=70002,
            date="2026-03-01T00:00:00Z",
            is_buy=True,
            is_personal=True,
            type_id=1234,
            quantity=5,
            unit_price=200.0,
            total_price=1000.0,
        ),
        CharacterWalletTransactionsModel(
            character_id=1,
            transaction_id=70001,
            date="2026-01-01T00:00:00Z",
            is_buy=True,
            is_personal=True,
            type_id=1234,
            quantity=10,
            unit_price=100.0,
            total_price=1000.0,
        ),
    ])
    session.commit()

    cost_map = build_character_buy_cost_map(session)

    assert 1234 in cost_map
    lots = cost_map[1234]
    assert len(lots) == 2
    # Oldest date must come first
    assert lots[0][0] == "2026-01-01T00:00:00Z"
    assert lots[1][0] == "2026-03-01T00:00:00Z"
    # transaction_id preserved as the 4th tuple element
    assert lots[0][3] == 70001
    assert lots[1][3] == 70002


def test_backfill_fills_untracked_inventory_items(db_session: Session) -> None:
    """Items previously stamped 'untracked_inventory' are re-processed by the backfill."""
    session = db_session

    session.add(
        CharacterIndustryJobsModel(
            character_id=1,
            job_id=9060,
            status="delivered",
            product_type_id=1111,
            completed_date="2026-01-05T00:00:00Z",
            output_quantity=20,
            unit_build_cost=75.0,
            total_build_cost=1500.0,
            build_cost_source="test",
            raw={},
        )
    )

    # Row previously stamped as untracked_inventory with no unit cost
    session.add(
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=1111001,
            observed_at="2026-01-10T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=1111,
            type_name="UntrackedWidget",
            quantity=5,
            acquisition_source="untracked_inventory",
            acquisition_unit_cost=None,
            acquisition_total_cost=None,
        )
    )
    session.commit()

    summary = backfill_corp_transfer_costs(session)

    assert summary["fifo_matched"] == 1

    snapshot = session.query(CorporationAssetHistoryModel).filter_by(item_id=1111001).first()
    assert snapshot is not None
    assert snapshot.acquisition_unit_cost == 75.0
    assert snapshot.acquisition_source == "industry_build_transferred"
    assert snapshot.acquisition_reference_type == "industry_job"
    assert snapshot.acquisition_reference_id == 9060


def test_backfill_uses_character_buy_transactions_for_unmatched_items(db_session: Session) -> None:
    """Items with no industry job but a matching buy transaction get buy-matched."""
    session = db_session

    # No industry job for this type — only a buy transaction
    session.add(
        CharacterWalletTransactionsModel(
            character_id=1,
            transaction_id=80001,
            date="2026-02-01T00:00:00Z",
            is_buy=True,
            is_personal=True,
            type_id=2222,
            quantity=10,
            unit_price=55.0,
            total_price=550.0,
        )
    )

    session.add(
        CorporationAssetHistoryModel(
            corporation_id=98000001,
            item_id=2222001,
            observed_at="2026-02-15T00:00:00Z",
            snapshot_source="asset_refresh",
            type_id=2222,
            type_name="BoughtItem",
            quantity=3,
            acquisition_source=None,
            acquisition_unit_cost=None,
            acquisition_total_cost=None,
        )
    )
    session.commit()

    summary = backfill_corp_transfer_costs(session)

    assert summary["buy_matched"] == 1
    assert summary["fifo_matched"] == 0
    assert summary["avg_matched"] == 0

    snapshot = session.query(CorporationAssetHistoryModel).filter_by(item_id=2222001).first()
    assert snapshot is not None
    assert snapshot.acquisition_source == "character_market_buy_transferred"
    assert snapshot.acquisition_unit_cost == 55.0
    assert abs(snapshot.acquisition_total_cost - 55.0 * 3) < 1e-9
    assert snapshot.acquisition_reference_type == "wallet_transaction"
    assert snapshot.acquisition_reference_id == 80001


# --- corp sales of pilot-built transfer lots ---------------------------------------------------
#
# H1.1: a corp sale of a pilot-built lot keeps its lower-bound flag until the
# job's cost is resolved, then takes the job's current cost; a blended
# (average or multi-lot) transfer lot is flagged while any job of its type has
# a partly unknown cost. Fake ids.

CORP, JOB, PRODUCT = 98000001, 5001, 990003
R = "invention cost unknown: no decryptor choice"


def _seed(s, *, source="industry_build_transferred", referenced=True):
    s.add(CorporationModel(corporation_id=CORP, corporation_name="Fake"))
    s.add(CharacterIndustryJobsModel(
        character_id=7001, job_id=JOB, status="delivered", activity_id=1, runs=2,
        blueprint_type_id=990002, product_type_id=PRODUCT, cost=40.0, completed_date="2026-10-01T00:00:00Z",
        output_quantity=2, total_build_cost=140.0, unit_build_cost=70.0, invention_cost_unknown_reason=R))
    s.add(CorporationAssetHistoryModel(
        corporation_id=CORP, item_id=1, observed_at="2026-10-01T01:00:00Z", type_id=PRODUCT, quantity=2,
        acquisition_source=source, acquisition_unit_cost=70.0,
        acquisition_reference_type="industry_job" if referenced else None,
        acquisition_reference_id=JOB if referenced else None, acquisition_date="2026-10-01T00:00:00Z"))
    s.add(CorporationWalletTransactionsModel(
        corporation_id=CORP, transaction_id=6001, date="2026-10-02T00:00:00Z", is_buy=False, quantity=2,
        type_id=PRODUCT, unit_price=200.0, total_price=400.0))
    s.commit()


def _rebuild(s):
    ledger = CorporationRealizedProfitLedgerService(app_session=s, sde_session=None, market_prices=[])
    return ledger, ledger.rebuild(corporation_id=CORP)


def _resolve_job(s):
    job = s.query(CharacterIndustryJobsModel).filter_by(job_id=JOB).one()
    job.invention_cost_unknown_reason = None
    job.unit_build_cost = 90.0
    job.total_build_cost = 180.0
    s.commit()


def test_referenced_lot_is_a_lower_bound_before_and_the_jobs_current_cost_after_resolve(app_session):
    _seed(app_session)
    ledger, rows = _rebuild(app_session)
    assert rows[0]["allocated_cost"] == 140.0
    assert rows[0]["cost_lower_bound_reason"] == R

    _resolve_job(app_session)
    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(corporation_id=CORP)) == {CORP}
    _, rows = _rebuild(app_session)
    assert rows[0]["allocated_cost"] == 180.0
    assert rows[0]["cost_lower_bound_reason"] is None
    assert [a["unit_cost"] for a in rows[0]["allocation_details"]] == [90.0]
    assert _industry_build_unit_cost(rows[0]["allocation_details"]) == (90.0, None)


def test_avg_lot_is_flagged_while_a_job_of_its_type_has_a_reason(app_session):
    _seed(app_session, source="industry_build_transferred_avg", referenced=False)
    ledger, rows = _rebuild(app_session)
    assert rows[0]["cost_lower_bound_reason"] is not None
    assert R in rows[0]["cost_lower_bound_reason"]
    value, why = _industry_build_unit_cost(rows[0]["allocation_details"])
    assert value is None and R in why

    _resolve_job(app_session)
    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(corporation_id=CORP)) == {CORP}
    _, rows = _rebuild(app_session)
    assert rows[0]["cost_lower_bound_reason"] is None


def test_multi_lot_fifo_blend_without_job_reference_is_flagged(app_session):
    _seed(app_session, source="industry_build_transferred", referenced=False)
    _, rows = _rebuild(app_session)
    assert rows[0]["cost_lower_bound_reason"] is not None
    assert R in rows[0]["cost_lower_bound_reason"]


# --- a re-costed job re-stamps the blended transfer lots ---------------------------------------
#
# J2.1: a re-costed character job re-stamps the blended corp transfer lots
# of its product type (scripts/backfill_corp_transfer_costs.py's matching, now
# importable from application/characters/corp_transfer_costs.py), bounded to
# the affected types and idempotent; the ledger rebuilds through its stale
# check, and a lot stays flagged while a contributing job has a reason. Fake ids.

CORP, PILOT = 98000001, 7001
PRODUCT, OTHER = 990003, 990013


def _job(s, job_id, *, product=PRODUCT, completed, qty, unit, reason=None):
    s.add(CharacterIndustryJobsModel(
        character_id=PILOT, job_id=job_id, status="delivered", activity_id=1, runs=qty,
        blueprint_type_id=990002, product_type_id=product, cost=1.0, completed_date=completed,
        output_quantity=qty, total_build_cost=unit * qty, unit_build_cost=unit,
        invention_cost_unknown_reason=reason))


def _row(s, row_id, *, product=PRODUCT, observed, qty, source, unit, job_id=None):
    s.add(CorporationAssetHistoryModel(
        id=row_id, corporation_id=CORP, item_id=row_id, observed_at=observed, type_id=product, quantity=qty,
        acquisition_source=source, acquisition_unit_cost=unit, acquisition_total_cost=unit * qty,
        acquisition_reference_type="industry_job" if job_id else None, acquisition_reference_id=job_id,
        acquisition_date=None))


def _restamp_seed(s):
    """Jobs 5001 (2 @ 70, a reason) and 5002 (3 @ 100) of PRODUCT; corp rows
    as the backfill stamped them: row 1 single-job (5001), row 2 a two-lot
    FIFO blend (5001 + 5002), row 3 the average fallback. OTHER has its own
    job and average row, untouched by a PRODUCT re-cost."""
    _job(s, 5001, completed="2026-06-01T00:00:00Z", qty=2, unit=70.0, reason=R)
    _job(s, 5002, completed="2026-06-02T00:00:00Z", qty=3, unit=100.0)
    _job(s, 5003, product=OTHER, completed="2026-06-01T00:00:00Z", qty=1, unit=10.0)
    _row(s, 1, observed="2026-06-03T00:00:00Z", qty=1, source="industry_build_transferred", unit=70.0, job_id=5001)
    _row(s, 2, observed="2026-06-04T00:00:00Z", qty=2, source="industry_build_transferred", unit=85.0)
    _row(s, 3, observed="2026-06-05T00:00:00Z", qty=5, source="industry_build_transferred_avg", unit=85.0)
    _row(s, 4, product=OTHER, observed="2026-06-05T00:00:00Z", qty=3, source="industry_build_transferred_avg",
         unit=10.0)
    s.commit()


def _resolve_5001(s, unit=90.0, reason=None):
    job = s.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
    job.unit_build_cost = unit
    job.total_build_cost = unit * 2
    job.invention_cost_unknown_reason = reason
    s.commit()


def _rows(s):
    return {r.id: r for r in s.query(CorporationAssetHistoryModel).all()}


def test_restamp_recomputes_the_blended_lots_of_the_type_only(app_session):
    _restamp_seed(app_session)
    _resolve_5001(app_session)

    changed = restamp_blended_corp_transfer_lots(app_session, [PRODUCT])
    app_session.commit()

    rows = _rows(app_session)
    assert changed == 2
    # The single-job row is priced from its job at ledger time: untouched.
    assert rows[1].acquisition_unit_cost == 70.0 and rows[1].acquisition_reference_id == 5001
    # FIFO over the type's transfer rows: row 1 takes one of 5001, row 2 the
    # other of 5001 and one of 5002 (a blend, no job), row 3 falls back to the average.
    assert rows[2].acquisition_unit_cost == pytest.approx((90.0 + 100.0) / 2)
    assert rows[2].acquisition_total_cost == pytest.approx(190.0)
    assert rows[2].acquisition_reference_id is None and rows[2].acquisition_reference_type is None
    assert rows[3].acquisition_source == "industry_build_transferred_avg"
    assert rows[3].acquisition_unit_cost == pytest.approx(96.0)  # (2 x 90 + 3 x 100) / 5
    assert rows[3].acquisition_total_cost == pytest.approx(480.0)
    assert rows[4].acquisition_unit_cost == 10.0  # another type: not affected


def test_restamp_is_idempotent(app_session):
    _restamp_seed(app_session)
    _resolve_5001(app_session)
    restamp_blended_corp_transfer_lots(app_session, [PRODUCT])
    app_session.commit()
    before = {i: (r.acquisition_source, r.acquisition_unit_cost, r.acquisition_reference_id)
              for i, r in _rows(app_session).items()}

    assert restamp_blended_corp_transfer_lots(app_session, [PRODUCT]) == 0
    app_session.commit()
    after = {i: (r.acquisition_source, r.acquisition_unit_cost, r.acquisition_reference_id)
             for i, r in _rows(app_session).items()}
    assert after == before


def test_restamp_of_no_types_or_a_type_without_jobs_changes_nothing(app_session):
    _restamp_seed(app_session)
    _resolve_5001(app_session)
    assert restamp_blended_corp_transfer_lots(app_session, []) == 0
    assert restamp_blended_corp_transfer_lots(app_session, [123456]) == 0
    assert _rows(app_session)[3].acquisition_unit_cost == 85.0


def test_recosted_types_are_the_jobs_whose_unit_cost_changed(app_session):
    _restamp_seed(app_session)
    before = character_job_unit_costs(app_session)
    _resolve_5001(app_session)
    _job(app_session, 5004, product=OTHER, completed="2026-06-09T00:00:00Z", qty=1, unit=12.0)  # new, not re-costed
    app_session.commit()
    assert recosted_product_type_ids(before, character_job_unit_costs(app_session)) == {PRODUCT}


def _ledger_with_sale(s):
    """A sale of all 11 units: job 5002's 3, then the 8 of the corp rows (job
    5001's lot is already in row 1 and the row 2 blend, so it is not queued twice)."""
    s.add(CorporationModel(corporation_id=CORP, corporation_name="Fake"))
    s.add(CorporationWalletTransactionsModel(
        corporation_id=CORP, transaction_id=6001, date="2026-06-06T00:00:00Z", is_buy=False, quantity=11,
        type_id=PRODUCT, unit_price=200.0, total_price=2200.0))
    s.commit()
    return CorporationRealizedProfitLedgerService(app_session=s, sde_session=None, market_prices=[])


def _blended_entries(row):
    return [a for a in row["allocation_details"] if a.get("reference_type") != "industry_job"]


def test_a_lot_stays_flagged_while_a_contributing_job_has_a_reason(app_session):
    _restamp_seed(app_session)
    ledger = _ledger_with_sale(app_session)
    _resolve_5001(app_session, unit=90.0, reason=R + " (still)")
    restamp_blended_corp_transfer_lots(app_session, [PRODUCT])
    app_session.commit()

    rows = ledger.rebuild(corporation_id=CORP)
    blended = _blended_entries(rows[0])
    assert blended and all(R in a["cost_unknown_reason"] for a in blended)
    assert rows[0]["cost_lower_bound_reason"] is not None


def test_after_resolve_the_ledger_goes_stale_and_takes_the_restamped_cost(app_session):
    _restamp_seed(app_session)
    ledger = _ledger_with_sale(app_session)
    rows = ledger.rebuild(corporation_id=CORP)
    assert {a["unit_cost"] for a in _blended_entries(rows[0])} == {85.0}

    _resolve_5001(app_session)
    restamp_blended_corp_transfer_lots(app_session, [PRODUCT])
    app_session.commit()

    assert ledger.stale_resolved_cost_owner_ids(ledger.list_rows(corporation_id=CORP)) == {CORP}
    rows = ledger.rebuild(corporation_id=CORP)
    assert rows[0]["cost_lower_bound_reason"] is None
    assert sorted(a["unit_cost"] for a in _blended_entries(rows[0])) == pytest.approx([95.0, 96.0])
    assert all("cost_unknown_reason" not in a for a in rows[0]["allocation_details"])


# --- the re-cost paths re-stamp -------------------------------------------------


def test_the_decryptor_resnapshot_restamps_the_types_it_recosts(app_session, monkeypatch):
    _restamp_seed(app_session)
    job = app_session.query(CharacterIndustryJobsModel).filter_by(job_id=5001).one()
    job.invention_cost_unknown_reason = f"{ap.NO_DECRYPTOR_CHOICE_REASON} for blueprint 990002"
    job.raw = {"job_id": 5001, "activity_id": 1, "blueprint_type_id": 990002, "product_type_id": PRODUCT}
    app_session.commit()
    monkeypatch.setattr(ap, "build_invention_cost_per_run_by_blueprint_type", lambda **kw: {})
    monkeypatch.setattr(ap, "industry_job_material_type_ids", lambda **kw: [])
    monkeypatch.setattr(ap, "resolve_industry_job_cost_snapshot", lambda **kw: {
        "output_quantity": 2, "unit_cost": 90.0, "total_cost": 180.0, "invention_cost": 20.0,
        "invention_cost_unknown_reason": None, "source": "test"})
    from eve_online_industry_tracker.application.characters import asset_history
    monkeypatch.setattr(asset_history, "build_historical_input_cost_lookup", lambda **kw: {})

    assert ap.resnapshot_jobs_without_decryptor_choice(
        app_session=app_session, sde_session=None, market_price_map={},
        invention_refresh=ap.RealizedInventionRefresh(inputs=None)) == 1

    rows = _rows(app_session)
    assert rows[3].acquisition_unit_cost == pytest.approx(96.0)
    assert rows[4].acquisition_unit_cost == 10.0


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


def test_a_character_job_refresh_restamps_the_types_it_recosts(app_session, monkeypatch):
    _restamp_seed(app_session)
    fetched = [{"job_id": 5001, "status": "delivered", "activity_id": 1, "runs": 2, "blueprint_type_id": 990002,
                "product_type_id": PRODUCT, "completed_date": "2026-06-01T00:00:00Z", "cost": 1.0}]
    monkeypatch.setattr(character_module, "build_market_price_map", lambda rows: {})
    monkeypatch.setattr(character_module, "build_invention_cost_per_run_by_blueprint_type", lambda **kw: {})
    monkeypatch.setattr(character_module, "industry_job_material_type_ids", lambda **kw: [])
    monkeypatch.setattr(character_module, "lookup_historical_blueprint_provenance", lambda **kw: None)
    monkeypatch.setattr(character_module, "build_historical_input_cost_lookup", lambda **kw: {})
    monkeypatch.setattr(character_module, "backfill_historical_market_costs", lambda **kw: None)
    monkeypatch.setattr(character_module, "resolve_industry_job_cost_snapshot", lambda **kw: {
        "output_quantity": 2, "unit_cost": 90.0, "total_cost": 180.0, "source": "test"})
    character = object.__new__(Character)
    character.character_id = PILOT
    character.character_name = "Builder"
    character._db_app = _Db(app_session)
    character._db_sde = _Db(None)
    character._esi_client = _Esi(fetched)
    character._esi_service = None
    character.ensure_esi = lambda: None

    character.refresh_industry_jobs()

    rows = _rows(app_session)
    assert rows[3].acquisition_unit_cost == pytest.approx(96.0)
    assert rows[4].acquisition_unit_cost == 10.0


# --- J3.1: the one-time re-stamp of every blended type ---------------------------------


def _stale_seed(s):
    """PRODUCT and OTHER both stamped from costs their jobs no longer have
    (blended rows at 85.0 and 10.0, jobs since re-costed to 90 and 12)."""
    _restamp_seed(s)
    _resolve_5001(s)
    other = s.query(CharacterIndustryJobsModel).filter_by(job_id=5003).one()
    other.unit_build_cost, other.total_build_cost = 12.0, 12.0
    s.commit()


def test_the_one_time_restamp_covers_every_blended_type_and_logs(app_session, caplog):
    import logging

    from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
        restamp_all_blended_corp_transfer_lots_once,
    )
    from eve_online_industry_tracker.infrastructure.models import AppMetaModel

    _stale_seed(app_session)
    with caplog.at_level(logging.INFO):
        changed = restamp_all_blended_corp_transfer_lots_once(app_session)

    rows = _rows(app_session)
    assert changed == 3
    assert rows[1].acquisition_unit_cost == 70.0                     # names its job: untouched
    assert rows[2].acquisition_unit_cost == pytest.approx(95.0)
    assert rows[3].acquisition_unit_cost == pytest.approx(96.0)
    assert rows[4].acquisition_unit_cost == pytest.approx(12.0)      # the other type too
    marker = app_session.get(AppMetaModel, "corp_transfer_blended_restamp_v1")
    assert marker is not None and marker.value == "3"
    assert any("One-time re-stamp" in r.getMessage() and "3" in r.getMessage() for r in caplog.records)


def test_the_one_time_restamp_runs_once_and_the_per_type_restamp_still_works(app_session, caplog):
    import logging

    from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
        restamp_all_blended_corp_transfer_lots_once,
    )

    _stale_seed(app_session)
    assert restamp_all_blended_corp_transfer_lots_once(app_session) == 3
    other = app_session.query(CharacterIndustryJobsModel).filter_by(job_id=5003).one()
    other.unit_build_cost, other.total_build_cost = 14.0, 14.0
    app_session.commit()

    caplog.clear()
    with caplog.at_level(logging.INFO):
        assert restamp_all_blended_corp_transfer_lots_once(app_session) is None   # the marker: done
    assert _rows(app_session)[4].acquisition_unit_cost == pytest.approx(12.0)
    assert not any("One-time re-stamp" in r.getMessage() for r in caplog.records)

    assert restamp_blended_corp_transfer_lots(app_session, [OTHER]) == 1
    app_session.commit()
    assert _rows(app_session)[4].acquisition_unit_cost == pytest.approx(14.0)


def test_the_one_time_restamp_on_an_empty_history_still_sets_the_marker(app_session):
    from eve_online_industry_tracker.application.characters.corp_transfer_costs import (
        restamp_all_blended_corp_transfer_lots_once,
    )
    from eve_online_industry_tracker.infrastructure.models import AppMetaModel

    assert restamp_all_blended_corp_transfer_lots_once(app_session) == 0
    assert app_session.get(AppMetaModel, "corp_transfer_blended_restamp_v1").value == "0"
    assert restamp_all_blended_corp_transfer_lots_once(app_session) is None
