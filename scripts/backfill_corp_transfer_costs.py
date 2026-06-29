#!/usr/bin/env python
"""Backfill acquisition costs for corp asset history records from character industry jobs.

When characters transfer manufactured items to the corporation, the corp asset history
has no cost basis (acquisition_unit_cost IS NULL, acquisition_source IS NULL).
This script matches those records to character industry jobs using FIFO chronological
matching, or falls back to a quantity-weighted average per type_id when FIFO cannot
be resolved.
"""

import sys
from collections import defaultdict
from pathlib import Path

# Add parent directory to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from config.paths import app_config_path, app_secret_path
from config.schemas import CONFIG_SCHEMA
from eve_online_industry_tracker.config.config_manager import ConfigManager
from eve_online_industry_tracker.infrastructure.database_manager import DatabaseManager
from eve_online_industry_tracker.infrastructure.models import (
    CharacterIndustryJobsModel,
    CorporationAssetHistoryModel,
)


# ---------------------------------------------------------------------------
# Cost-map builder
# ---------------------------------------------------------------------------

def build_character_job_cost_map(session) -> dict[int, list[tuple[str, float, int, int]]]:
    """Return {product_type_id: [(completed_date, unit_build_cost, output_quantity, job_id), ...]}
    sorted oldest-first (FIFO) for all completed character industry jobs that have
    a unit_build_cost recorded.
    """
    completed_statuses = {"delivered", "ready", "completed"}

    rows = (
        session.query(CharacterIndustryJobsModel)
        .filter(
            CharacterIndustryJobsModel.product_type_id.isnot(None),
            CharacterIndustryJobsModel.unit_build_cost.isnot(None),
            CharacterIndustryJobsModel.output_quantity.isnot(None),
        )
        .order_by(CharacterIndustryJobsModel.completed_date)
        .all()
    )

    cost_map: dict[int, list[tuple[str, float, int, int]]] = defaultdict(list)
    for job in rows:
        status = str(getattr(job, "status", "") or "").lower()
        if status and status not in completed_statuses:
            continue
        type_id = int(job.product_type_id)
        completed_date = job.completed_date or ""
        unit_build_cost = float(job.unit_build_cost)
        output_quantity = int(job.output_quantity)
        job_id = int(job.job_id)
        if unit_build_cost <= 0 or output_quantity <= 0:
            continue
        cost_map[type_id].append((completed_date, unit_build_cost, output_quantity, job_id))

    # Sort each list oldest-first by completed_date
    for type_id in cost_map:
        cost_map[type_id].sort(key=lambda t: t[0])

    return dict(cost_map)


# ---------------------------------------------------------------------------
# FIFO matching
# ---------------------------------------------------------------------------

def fifo_match_corp_snapshots(
    snapshots: list,
    job_lots: list[tuple[str, float, int, int]],
) -> list[dict]:
    """Attempt to match corp history snapshots to job lots in chronological order (FIFO).

    snapshots: list of CorporationAssetHistoryModel rows, ordered by observed_at
    job_lots: list of (completed_date, unit_build_cost, output_quantity, job_id), sorted oldest-first

    Returns a list of match dicts, one per snapshot:
        {
            "snapshot": <row>,
            "unit_build_cost": float,
            "job_id": int,
            "completed_date": str,
            "source": "industry_build_transferred",
        }

    If a lot cannot be chronologically matched, returns None for that snapshot entry.
    """
    results = []

    # Work with mutable copies of lot quantities
    remaining_lots: list[list] = [[d, uc, qty, jid] for (d, uc, qty, jid) in job_lots]

    for snapshot in snapshots:
        qty_needed = int(snapshot.quantity or 1)
        snap_date = str(snapshot.observed_at or "")

        matched = False
        for lot in remaining_lots:
            lot_date, unit_cost, lot_qty, job_id = lot
            if lot_qty <= 0:
                continue
            # FIFO: the job must have completed before or at the snapshot observation time
            if lot_date and snap_date and lot_date > snap_date:
                # This lot is newer than the snapshot — can't use it; stop looking further
                break

            # Consume from this lot
            take = min(qty_needed, lot_qty)
            lot[2] -= take
            qty_needed -= take

            if qty_needed <= 0:
                results.append({
                    "snapshot": snapshot,
                    "unit_build_cost": unit_cost,
                    "job_id": job_id,
                    "completed_date": lot_date,
                    "source": "industry_build_transferred",
                })
                matched = True
                break

        if not matched:
            results.append(None)

    return results


# ---------------------------------------------------------------------------
# Weighted-average fallback
# ---------------------------------------------------------------------------

def weighted_average_unit_cost(job_lots: list[tuple[str, float, int, int]]) -> float:
    """Return the quantity-weighted average unit_build_cost across all job lots."""
    total_cost = sum(uc * qty for (_, uc, qty, _) in job_lots)
    total_qty = sum(qty for (_, _, qty, _) in job_lots)
    if total_qty <= 0:
        return 0.0
    return total_cost / total_qty


# ---------------------------------------------------------------------------
# Main backfill function (testable without DB setup)
# ---------------------------------------------------------------------------

def backfill_corp_transfer_costs(session) -> dict:
    """Backfill CorporationAssetHistoryModel rows from CharacterIndustryJobsModel.

    Returns a summary dict with counts.
    """
    # Step 1: Build cost map from all character industry jobs
    cost_map = build_character_job_cost_map(session)

    # Step 2: Query corp history rows with no cost basis
    uncosted_rows = (
        session.query(CorporationAssetHistoryModel)
        .filter(
            CorporationAssetHistoryModel.acquisition_unit_cost.is_(None),
            CorporationAssetHistoryModel.acquisition_source.is_(None),
        )
        .order_by(
            CorporationAssetHistoryModel.type_id,
            CorporationAssetHistoryModel.observed_at,
        )
        .all()
    )

    if not uncosted_rows:
        print("No corp asset history rows with missing cost basis found.")
        return {"total_uncosted": 0, "fifo_matched": 0, "avg_matched": 0, "unmatched": 0}

    # Group by type_id
    rows_by_type: dict[int, list] = defaultdict(list)
    for row in uncosted_rows:
        rows_by_type[int(row.type_id)].append(row)

    fifo_matched = 0
    avg_matched = 0
    unmatched = 0

    for type_id, snapshots in rows_by_type.items():
        job_lots = cost_map.get(type_id)

        if not job_lots:
            # No character jobs for this type_id — leave as-is
            unmatched += len(snapshots)
            continue

        # Attempt FIFO match
        match_results = fifo_match_corp_snapshots(snapshots, job_lots)

        avg_cost = weighted_average_unit_cost(job_lots)

        for match, snapshot in zip(match_results, snapshots):
            if match is not None:
                unit_cost = match["unit_build_cost"]
                job_id = match["job_id"]
                completed_date = match["completed_date"]

                snapshot.acquisition_source = "industry_build_transferred"
                snapshot.acquisition_unit_cost = unit_cost
                snapshot.acquisition_total_cost = unit_cost * float(snapshot.quantity or 1)
                snapshot.acquisition_reference_type = "industry_job"
                snapshot.acquisition_reference_id = job_id
                snapshot.acquisition_date = completed_date
                fifo_matched += 1
                print(
                    f"  FIFO: type_id={type_id}, qty={snapshot.quantity}, "
                    f"unit_cost={unit_cost:.2f}, job_id={job_id}"
                )
            else:
                # Fallback: quantity-weighted average
                snapshot.acquisition_source = "industry_build_transferred_avg"
                snapshot.acquisition_unit_cost = avg_cost
                snapshot.acquisition_total_cost = avg_cost * float(snapshot.quantity or 1)
                snapshot.acquisition_reference_type = None
                snapshot.acquisition_reference_id = None
                snapshot.acquisition_date = None
                avg_matched += 1
                print(
                    f"  AVG: type_id={type_id}, qty={snapshot.quantity}, "
                    f"avg_unit_cost={avg_cost:.2f}"
                )

    if fifo_matched + avg_matched > 0:
        session.commit()

    total_uncosted = len(uncosted_rows)
    return {
        "total_uncosted": total_uncosted,
        "fifo_matched": fifo_matched,
        "avg_matched": avg_matched,
        "unmatched": unmatched,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """Run corp transfer cost backfill."""
    cfg_manager = ConfigManager(
        base_path=app_config_path(),
        secret_path=app_secret_path(),
        schema=CONFIG_SCHEMA,
    )
    cfg = cfg_manager.all()
    db_app = DatabaseManager(cfg["app"]["database_app_uri"], cfg["app"]["language"])
    session = db_app.session

    print("Starting corp transfer cost backfill...")
    summary = backfill_corp_transfer_costs(session)

    print("\n--- Summary ---")
    print(f"  Total uncosted corp history rows:  {summary['total_uncosted']}")
    print(f"  FIFO-matched (exact job):          {summary['fifo_matched']}")
    print(f"  Average-matched (weighted avg):    {summary['avg_matched']}")
    print(f"  Unmatched (no character job found):{summary['unmatched']}")
    print("\nBackfill complete!")


if __name__ == "__main__":
    main()
