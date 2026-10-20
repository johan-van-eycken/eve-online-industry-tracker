"""Acquisition costs for corp asset history rows of items characters built or
bought and transferred to the corporation.

When characters transfer manufactured items to the corporation, the corp asset
history has no cost basis (acquisition_unit_cost IS NULL, acquisition_source
IS NULL, "untracked_inventory" or "unknown"). backfill_corp_transfer_costs
(run by scripts/backfill_corp_transfer_costs.py):

1. Matches those records to character industry jobs with FIFO chronological
   matching, or falls back to a quantity-weighted average per type_id when
   FIFO cannot be resolved.
2. For items with no matching industry job, uses character market buy
   transactions as the cost source.

A blended lot (the average fallback, or a FIFO match over several jobs)
names no job, so the ledger cannot price it from its job's current cost.
restamp_blended_corp_transfer_lots re-runs the same industry-job matching for
the product types whose character jobs were re-costed (the decryptor
re-snapshot, a job refresh, the historical market backfill) and writes it to
their blended rows. restamp_all_blended_corp_transfer_lots_once does so
once over every type with a blended row (run at startup, guarded by an
app_meta marker), so the stamps are consistent from that deploy on and a
later per-type re-stamp moves only the stamps of its own re-cost.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import or_

from eve_online_industry_tracker.infrastructure.models import (
    AppMetaModel,
    CharacterIndustryJobsModel,
    CharacterWalletTransactionsModel,
    CorporationAssetHistoryModel,
)

#: The corp history sources of a transferred character build (FIFO, average).
SOURCE_TRANSFERRED_BUILD = "industry_build_transferred"
SOURCE_TRANSFERRED_BUILD_AVG = "industry_build_transferred_avg"
SOURCE_TRANSFERRED_BUY = "character_market_buy_transferred"
SOURCE_TRANSFERRED_BUY_AVG = "character_market_buy_transferred_avg"

_COMPLETED_STATUSES = frozenset({"delivered", "ready", "completed"})


# ---------------------------------------------------------------------------
# Cost-map builders
# ---------------------------------------------------------------------------

def build_character_job_cost_map(
    session, *, type_ids: Iterable[int] | None = None,
) -> dict[int, list[tuple[str, float, int, int]]]:
    """Return {product_type_id: [(completed_date, unit_build_cost, output_quantity, job_id), ...]}
    sorted oldest-first (FIFO) for all completed character industry jobs that have
    a unit_build_cost recorded (only `type_ids` when given).
    """
    # Filter status at the DB level to exclude jobs with a known non-completed status.
    # Jobs with status=None are intentionally excluded here (isnot(None) filter) because
    # a null status indicates the job completion state is unknown.
    query = session.query(CharacterIndustryJobsModel).filter(
        CharacterIndustryJobsModel.product_type_id.isnot(None),
        CharacterIndustryJobsModel.unit_build_cost.isnot(None),
        CharacterIndustryJobsModel.output_quantity.isnot(None),
        CharacterIndustryJobsModel.status.isnot(None),
    )
    if type_ids is not None:
        query = query.filter(CharacterIndustryJobsModel.product_type_id.in_(sorted({int(t) for t in type_ids})))
    rows = query.order_by(CharacterIndustryJobsModel.completed_date).all()

    cost_map: dict[int, list[tuple[str, float, int, int]]] = defaultdict(list)
    for job in rows:
        status = str(getattr(job, "status", "") or "").lower()
        if status not in _COMPLETED_STATUSES:
            continue
        type_id = int(job.product_type_id)
        completed_date = job.completed_date or ""
        unit_build_cost = float(job.unit_build_cost)
        output_quantity = int(job.output_quantity)
        job_id = int(job.job_id)
        if unit_build_cost <= 0 or output_quantity <= 0:
            continue
        cost_map[type_id].append((completed_date, unit_build_cost, output_quantity, job_id))

    # Sort each list oldest-first by completed_date (then job id: deterministic)
    for type_id in cost_map:
        cost_map[type_id].sort(key=lambda t: (t[0], t[3]))

    return dict(cost_map)


def build_character_buy_cost_map(session) -> dict[int, list[tuple[str, float, int, int]]]:
    """Return {type_id: [(date, unit_price, quantity, transaction_id), ...]}
    sorted oldest-first (FIFO) for all character market buy transactions.
    """
    rows = (
        session.query(CharacterWalletTransactionsModel)
        .filter(
            CharacterWalletTransactionsModel.is_buy == True,  # noqa: E712
            CharacterWalletTransactionsModel.type_id.isnot(None),
            CharacterWalletTransactionsModel.unit_price.isnot(None),
            CharacterWalletTransactionsModel.quantity.isnot(None),
            CharacterWalletTransactionsModel.date.isnot(None),
        )
        .order_by(CharacterWalletTransactionsModel.date)
        .all()
    )

    cost_map: dict[int, list[tuple[str, float, int, int]]] = defaultdict(list)
    for tx in rows:
        type_id = int(tx.type_id)
        date = str(tx.date or "")
        unit_price = float(tx.unit_price)
        quantity = int(tx.quantity)
        transaction_id = int(tx.transaction_id)
        if unit_price <= 0 or quantity <= 0:
            continue
        cost_map[type_id].append((date, unit_price, quantity, transaction_id))

    return dict(cost_map)


# ---------------------------------------------------------------------------
# FIFO matching
# ---------------------------------------------------------------------------

def fifo_match_corp_snapshots(
    snapshots: list,
    job_lots: list[tuple[str, float, int, int]],
    *,
    source: str = SOURCE_TRANSFERRED_BUILD,
    reference_type: str = "industry_job",
) -> list[dict | None]:
    """Attempt to match corp history snapshots to job lots in chronological order (FIFO).

    snapshots: list of CorporationAssetHistoryModel rows, ordered by observed_at
    job_lots: list of (completed_date, unit_build_cost, output_quantity, job_id), sorted oldest-first
    source: value to use for the "source" key in the result dict
    reference_type: value to use for the "reference_type" key in the result dict

    Returns a list of match dicts, one per snapshot:
        {
            "snapshot": <row>,
            "unit_build_cost": float,
            "job_id": int,
            "completed_date": str,
            "source": str,
            "reference_type": str,
        }

    If a lot cannot be chronologically matched, returns None for that snapshot entry.
    """
    results = []

    # Work with mutable copies of lot quantities
    remaining_lots: list[list] = [[d, uc, qty, jid] for (d, uc, qty, jid) in job_lots]

    for snapshot in snapshots:
        qty_needed = int(snapshot.quantity or 1)
        snap_date = str(snapshot.observed_at or "")

        # Track all lot contributions for this snapshot (lot_date, unit_cost, units_taken, job_id)
        contributions: list[tuple[str, float, int, int]] = []

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
            contributions.append((lot_date, unit_cost, take, job_id))

            if qty_needed <= 0:
                break

        if qty_needed <= 0 and contributions:
            # Successfully matched. When the snapshot's quantity spans more than one lot,
            # compute a quantity-weighted average unit_build_cost across all contributing lots
            # rather than attributing the entire snapshot to the last lot consumed. In that
            # multi-lot case no single job_id applies, so acquisition_reference_id is set to
            # None. For a single-lot match, the original job_id is preserved.
            total_taken = sum(c[2] for c in contributions)
            blended_cost = sum(c[1] * c[2] for c in contributions) / total_taken

            if len(contributions) == 1:
                _, _, _, matched_job_id = contributions[0]
                matched_lot_date, _, _, _ = contributions[0]
                result_job_id: int | None = matched_job_id
                result_date = matched_lot_date
            else:
                # Multi-lot: no single job_id; use the date of the last contributing lot
                result_job_id = None
                result_date = contributions[-1][0]

            results.append({
                "snapshot": snapshot,
                "unit_build_cost": blended_cost,
                "job_id": result_job_id,
                "completed_date": result_date,
                "source": source,
                "reference_type": reference_type,
            })
        else:
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
# The stamp of one type's rows (the backfill's passes, and the re-stamp)
# ---------------------------------------------------------------------------

def _stamps_for_type(
    snapshots: list,
    lots: list[tuple[str, float, int, int]],
    *,
    fifo_source: str,
    avg_source: str,
    reference_type: str,
) -> list[tuple[Any, dict[str, Any], str]]:
    """[(snapshot, its acquisition fields, "fifo" | "avg")]: FIFO over `lots`
    in the snapshots' order, the quantity-weighted average of all `lots` for
    a snapshot FIFO cannot cover. A multi-lot FIFO match names no reference."""
    avg_cost = weighted_average_unit_cost(lots)
    out: list[tuple[Any, dict[str, Any], str]] = []
    for match, snapshot in zip(fifo_match_corp_snapshots(
        snapshots, lots, source=fifo_source, reference_type=reference_type,
    ), snapshots):
        quantity = float(snapshot.quantity or 1)
        if match is not None:
            unit_cost = float(match["unit_build_cost"])
            job_id = match["job_id"]  # None when quantity spanned multiple lots
            out.append((snapshot, {
                "acquisition_source": fifo_source,
                "acquisition_unit_cost": unit_cost,
                "acquisition_total_cost": unit_cost * quantity,
                # When job_id is None (multi-lot blend), reference_type is also None since
                # no single record covers this snapshot.
                "acquisition_reference_type": match["reference_type"] if job_id is not None else None,
                "acquisition_reference_id": job_id,
                "acquisition_date": match["completed_date"],
            }, "fifo"))
        else:
            out.append((snapshot, {
                "acquisition_source": avg_source,
                "acquisition_unit_cost": avg_cost,
                "acquisition_total_cost": avg_cost * quantity,
                "acquisition_reference_type": None,
                "acquisition_reference_id": None,
                "acquisition_date": None,
            }, "avg"))
    return out


def _apply(snapshot: Any, fields: dict[str, Any]) -> bool:
    """Write `fields` on `snapshot`; True when a value changed."""
    changed = False
    for name, value in fields.items():
        if getattr(snapshot, name) != value:
            setattr(snapshot, name, value)
            changed = True
    return changed


# ---------------------------------------------------------------------------
# Main backfill function (testable without DB setup)
# ---------------------------------------------------------------------------

def backfill_corp_transfer_costs(session) -> dict:
    """Backfill CorporationAssetHistoryModel rows from CharacterIndustryJobsModel
    and CharacterWalletTransactionsModel.

    Returns a summary dict with counts.
    """
    # Step 1: Build cost map from all character industry jobs
    cost_map = build_character_job_cost_map(session)

    # Step 2: Query corp history rows with no cost basis (including previously stamped
    # "untracked_inventory" and "unknown" rows that may now have a matchable source)
    uncosted_rows = (
        session.query(CorporationAssetHistoryModel)
        .filter(
            CorporationAssetHistoryModel.acquisition_unit_cost.is_(None),
            or_(
                CorporationAssetHistoryModel.acquisition_source.is_(None),
                CorporationAssetHistoryModel.acquisition_source == "untracked_inventory",
                CorporationAssetHistoryModel.acquisition_source == "unknown",
            ),
        )
        .order_by(
            CorporationAssetHistoryModel.type_id,
            CorporationAssetHistoryModel.observed_at,
        )
        .all()
    )

    if not uncosted_rows:
        logging.info("No corp asset history rows with missing cost basis found.")
        return {"total_uncosted": 0, "fifo_matched": 0, "avg_matched": 0, "buy_matched": 0,
                "buy_avg_matched": 0, "unmatched": 0}

    # Group by type_id
    rows_by_type: dict[int, list] = defaultdict(list)
    for row in uncosted_rows:
        rows_by_type[int(row.type_id)].append(row)

    counts = {"fifo": 0, "avg": 0, "buy_fifo": 0, "buy_avg": 0}

    # --- Pass 1: character industry jobs (FIFO, then weighted-avg fallback) ---
    for type_id, snapshots in rows_by_type.items():
        job_lots = cost_map.get(type_id)
        if not job_lots:
            continue  # no character jobs for this type_id: the buy pass below
        for snapshot, fields, kind in _stamps_for_type(
            snapshots, job_lots, fifo_source=SOURCE_TRANSFERRED_BUILD, avg_source=SOURCE_TRANSFERRED_BUILD_AVG,
            reference_type="industry_job",
        ):
            _apply(snapshot, fields)
            counts[kind] += 1
            logging.info(
                "  %s: type_id=%s, qty=%s, unit_cost=%.2f, job_id=%s", kind.upper(), type_id,
                snapshot.quantity, fields["acquisition_unit_cost"], fields["acquisition_reference_id"],
            )

    # --- Pass 2: character market buy transactions (FIFO, then weighted-avg fallback) ---
    # Only types entirely unmatched by industry jobs: partially-matched types
    # (some FIFO, some avg) are fully handled by pass 1.
    buy_cost_map = build_character_buy_cost_map(session)
    for type_id, snapshots in rows_by_type.items():
        if cost_map.get(type_id):
            continue
        buy_lots = buy_cost_map.get(type_id)
        if not buy_lots:
            continue
        for snapshot, fields, kind in _stamps_for_type(
            snapshots, buy_lots, fifo_source=SOURCE_TRANSFERRED_BUY, avg_source=SOURCE_TRANSFERRED_BUY_AVG,
            reference_type="wallet_transaction",
        ):
            # FIFO failed (corp qty exceeds tracked buy lots): the weighted average
            _apply(snapshot, fields)
            counts["buy_" + kind] += 1
            logging.info(
                "  BUY-%s: type_id=%s, qty=%s, unit_cost=%.2f, tx_id=%s", kind.upper(), type_id,
                snapshot.quantity, fields["acquisition_unit_cost"], fields["acquisition_reference_id"],
            )

    if sum(counts.values()) > 0:
        session.commit()

    total_uncosted = len(uncosted_rows)
    return {
        "total_uncosted": total_uncosted,
        "fifo_matched": counts["fifo"],
        "avg_matched": counts["avg"],
        "buy_matched": counts["buy_fifo"],
        "buy_avg_matched": counts["buy_avg"],
        "unmatched": total_uncosted - sum(counts.values()),
    }


# ---------------------------------------------------------------------------
# Re-stamp after a job re-cost
# ---------------------------------------------------------------------------

def restamp_blended_corp_transfer_lots(session, type_ids: Iterable[int]) -> int:
    """Re-stamp the blended corp transfer lots of `type_ids` from their
    character jobs' current unit_build_cost; returns the number of rows changed.

    The backfill's industry-job pass (FIFO, else the quantity-weighted
    average) is re-run over every transferred-build row of the type, in
    observed order, and written to the blended rows (the average fallback and
    the multi-lot FIFO matches): they name no job, so the ledger cannot price
    them from a job's current cost. A row naming one job is left as stamped
    (the ledger prices it from that job). Bounded to `type_ids`, idempotent
    (the same jobs and costs give the same stamps), and a type without a
    costed job keeps its rows. Does not commit: the caller's transaction."""
    wanted = sorted({int(t) for t in type_ids if t is not None})
    if not wanted:
        return 0
    cost_map = build_character_job_cost_map(session, type_ids=wanted)
    rows = (
        session.query(CorporationAssetHistoryModel)
        .filter(
            CorporationAssetHistoryModel.type_id.in_(wanted),
            CorporationAssetHistoryModel.acquisition_source.in_(
                (SOURCE_TRANSFERRED_BUILD, SOURCE_TRANSFERRED_BUILD_AVG)
            ),
        )
        .order_by(
            CorporationAssetHistoryModel.type_id,
            CorporationAssetHistoryModel.observed_at,
            CorporationAssetHistoryModel.id,
        )
        .all()
    )
    rows_by_type: dict[int, list] = defaultdict(list)
    for row in rows:
        rows_by_type[int(row.type_id)].append(row)

    changed = 0
    for type_id, snapshots in rows_by_type.items():
        job_lots = cost_map.get(type_id)
        if not job_lots:
            logging.warning(
                "Blended corp transfer lots of type %s not re-stamped: no character job of it has a build cost",
                type_id,
            )
            continue
        for snapshot, fields, _ in _stamps_for_type(
            snapshots, job_lots, fifo_source=SOURCE_TRANSFERRED_BUILD, avg_source=SOURCE_TRANSFERRED_BUILD_AVG,
            reference_type="industry_job",
        ):
            if snapshot.acquisition_reference_type == "industry_job":
                continue  # names its job: priced from it at ledger time
            if _apply(snapshot, fields):
                changed += 1
    if changed:
        logging.info("Re-stamped %d blended corp transfer lot(s) of %d type(s)", changed, len(rows_by_type))
    return changed


#: The app_meta key of the one-time re-stamp of every blended type.
BLENDED_RESTAMP_MARKER_KEY = "corp_transfer_blended_restamp_v1"


def restamp_all_blended_corp_transfer_lots_once(session) -> int | None:
    """Re-stamp the blended corp transfer lots of every type that has one,
    once: the stamps written before the per-type re-stamp existed may be from
    job costs since changed, so the first per-type re-stamp of a type would
    move many stamps unrelated to its re-cost. Run at startup; the app_meta
    marker (BLENDED_RESTAMP_MARKER_KEY, its value the number of rows changed)
    is written in the same transaction, so it runs again at the next start
    when it fails. Returns the number of rows changed (logged), None when
    the marker says it already ran. Commits."""
    if session.get(AppMetaModel, BLENDED_RESTAMP_MARKER_KEY) is not None:
        return None
    type_ids = sorted({
        int(type_id)
        for (type_id,) in session.query(CorporationAssetHistoryModel.type_id)
        .filter(
            CorporationAssetHistoryModel.acquisition_source.in_(
                (SOURCE_TRANSFERRED_BUILD, SOURCE_TRANSFERRED_BUILD_AVG)
            ),
            or_(
                CorporationAssetHistoryModel.acquisition_reference_type.is_(None),
                CorporationAssetHistoryModel.acquisition_reference_type != "industry_job",
            ),
        )
        .distinct()
        .all()
        if type_id is not None
    })
    changed = restamp_blended_corp_transfer_lots(session, type_ids)
    session.merge(AppMetaModel(
        key=BLENDED_RESTAMP_MARKER_KEY, value=str(changed), updated_at=datetime.now(timezone.utc),
    ))
    session.commit()
    logging.info(
        "One-time re-stamp of the blended corp transfer lots: %d row(s) changed over %d type(s) with a "
        "blended lot (marker %s set)", changed, len(type_ids), BLENDED_RESTAMP_MARKER_KEY,
    )
    return changed


def character_job_unit_costs(
    session, *, character_id: int | None = None,
) -> dict[int, tuple[int | None, float | None]]:
    """{job_id: (product_type_id, unit_build_cost)} over the character jobs
    (one character's when given): the before/after of a re-cost."""
    query = session.query(
        CharacterIndustryJobsModel.job_id,
        CharacterIndustryJobsModel.product_type_id,
        CharacterIndustryJobsModel.unit_build_cost,
    )
    if character_id is not None:
        query = query.filter(CharacterIndustryJobsModel.character_id == int(character_id))
    return {
        int(job_id): (int(product) if product is not None else None, float(cost) if cost is not None else None)
        for job_id, product, cost in query.all()
        if job_id is not None
    }


def recosted_product_type_ids(
    before: dict[int, tuple[int | None, float | None]],
    after: dict[int, tuple[int | None, float | None]],
) -> set[int]:
    """The product types of the jobs in both whose unit_build_cost changed."""
    out: set[int] = set()
    for job_id, (product, cost) in after.items():
        if job_id in before and before[job_id][1] != cost:
            for type_id in (product, before[job_id][0]):
                if type_id is not None:
                    out.add(int(type_id))
    return out
