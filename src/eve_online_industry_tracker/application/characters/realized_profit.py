from __future__ import annotations

import json
import logging
import threading
from collections import defaultdict
from dataclasses import asdict, replace
from datetime import datetime
from typing import Any

from sqlalchemy import or_

from eve_online_industry_tracker.application import market_fees
from eve_online_industry_tracker.application.characters.asset_provenance import (
    ASSET_SOURCE_INDUSTRY_BUILD,
    ASSET_SOURCE_MARKET_BUY,
    COMPLETED_JOB_STATUSES,
    FifoLot,
    _safe_float,
    _safe_int,
    completed_job_output_lot,
    industry_job_input_quantities,
)
from eve_online_industry_tracker.db_models import (
    Blueprints,
    CharacterIndustryJobsModel,
    CharacterModel,
    CharacterRealizedSalesLedgerModel,
    CharacterWalletJournalModel,
    CharacterWalletTransactionsModel,
    CorporationAssetHistoryModel,
    CorporationIndustryJobsModel,
    CorporationModel,
    CorporationRealizedSalesLedgerModel,
    CorporationWalletJournalModel,
    CorporationWalletTransactionsModel,
)


logger = logging.getLogger(__name__)

ASSET_SOURCE_UNTRACKED = "untracked_inventory"

# One lock per ledger owner for a rebuild: two requests that rebuild the same
# owner at once race on UNIQUE(owner, transaction_id). The locks are per
# process only: two app processes on one database are not serialized.
_LEDGER_REBUILD_LOCKS: dict[tuple[str, int], threading.Lock] = {}
_LEDGER_REBUILD_LOCKS_GUARD = threading.Lock()


def ledger_rebuild_lock(owner_field: str, owner_id: int) -> threading.Lock:
    """The one lock (per process) for rebuilding `owner_id`'s ledger;
    `owner_field` is "character_id" or "corporation_id"."""
    with _LEDGER_REBUILD_LOCKS_GUARD:
        return _LEDGER_REBUILD_LOCKS.setdefault((str(owner_field), int(owner_id)), threading.Lock())


def cost_lower_bound_reason(allocation_details: Any) -> str | None:
    """Why a sale's allocated cost is a lower bound: the distinct
    `cost_unknown_reason`s of its allocations, in order, or None."""
    reasons: list[str] = []
    for entry in allocation_details or []:
        reason = entry.get("cost_unknown_reason") if isinstance(entry, dict) else None
        if reason and str(reason) not in reasons:
            reasons.append(str(reason))
    return "; ".join(reasons) or None
ASSET_SOURCE_OPENING_INVENTORY = "opening_inventory"


def _parse_date(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    if raw_value.endswith("Z"):
        raw_value = raw_value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw_value)
    except ValueError:
        return None


def _append_lot(lots_by_type: dict[int, list[FifoLot]], *, type_id: int, lot: FifoLot) -> None:
    lots_by_type.setdefault(int(type_id), []).append(lot)


def _consume_lots(
    lots: list[FifoLot], *, quantity: int, cost_unknown_reason_by_job_id: dict[int, str] | None = None,
) -> dict[str, Any]:
    """FIFO-take `quantity` from `lots`. An allocation whose lot has a
    partly unknown build cost carries `cost_unknown_reason`: the lot's own,
    or its industry job's (`cost_unknown_reason_by_job_id`, which also covers
    the asset-history and transferred lots that point at the job)."""
    remaining = max(0, int(quantity or 0))
    total_cost = 0.0
    priced_quantity = 0
    by_source: dict[str, dict[str, Any]] = {}
    allocations: list[dict[str, Any]] = []

    while remaining > 0 and lots:
        head = lots[0]
        lot_quantity = max(0, int(head.quantity or 0))
        if lot_quantity <= 0:
            lots.pop(0)
            continue
        take = min(remaining, lot_quantity)
        chunk_cost = float(take) * float(head.unit_price)
        source = str(head.source or "unknown")
        cost_unknown_reason = head.cost_unknown_reason
        if cost_unknown_reason is None and cost_unknown_reason_by_job_id and head.reference_type == "industry_job":
            cost_unknown_reason = cost_unknown_reason_by_job_id.get(int(head.reference_id or 0))
        slot = by_source.setdefault(source, {"quantity": 0, "cost": 0.0})
        slot["quantity"] = int(slot.get("quantity") or 0) + int(take)
        slot["cost"] = float(slot.get("cost") or 0.0) + float(chunk_cost)
        allocations.append(
            {
                "source": source,
                "quantity": int(take),
                "unit_cost": float(head.unit_price),
                "total_cost": float(chunk_cost),
                "acquisition_date": head.acquisition_date,
                "reference_id": head.reference_id,
                "reference_type": head.reference_type,
                **({"cost_unknown_reason": cost_unknown_reason} if cost_unknown_reason else {}),
                **({"asset_history_id": int(head.asset_history_id)} if head.asset_history_id is not None else {}),
            }
        )
        remaining -= int(take)
        priced_quantity += int(take)
        total_cost += float(chunk_cost)

        new_quantity = lot_quantity - int(take)
        if new_quantity > 0:
            lots[0] = replace(head, quantity=int(new_quantity))
        else:
            lots.pop(0)

    return {
        "total_cost": float(total_cost),
        "priced_quantity": int(priced_quantity),
        "unpriced_quantity": int(remaining),
        "by_source": by_source,
        "allocations": allocations,
    }


#: Transfer lots whose cost is a character job's build cost
#: (scripts/backfill_corp_transfer_costs.py).
_TRANSFERRED_BUILD_SOURCES = frozenset({"industry_build_transferred", "industry_build_transferred_avg"})


#: Relative tolerance of the cost-basis comparison (float round trips through JSON).
_COST_BASIS_TOLERANCE = 1e-9
_SQL_IN_CHUNK = 500


def _same_cost(a: Any, b: Any) -> bool:
    a, b = _safe_float(a), _safe_float(b)
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= _COST_BASIS_TOLERANCE * max(1.0, abs(a), abs(b))


def _with_cost_basis(allocations: list[dict[str, Any]], unit_cost_by_job_id: dict[int, float]) -> None:
    """Record on each allocation the cost its lot was priced from
    (`cost_basis`): the job's current positive unit_build_cost (None: none)
    for a lot naming an industry job, the stamped cost for a blended transfer
    lot (`asset_history_id`). stale_resolved_cost_owner_ids compares it."""
    for entry in allocations:
        job_id = _safe_int(entry.get("reference_id"))
        if entry.get("reference_type") == "industry_job" and job_id:
            entry["cost_basis"] = unit_cost_by_job_id.get(int(job_id))
        elif entry.get("asset_history_id") is not None:
            entry["cost_basis"] = _safe_float(entry.get("unit_cost"))


def _is_blended_transfer_lot(source: Any, reference_type: Any) -> bool:
    """A transferred build lot that names no single job: the average
    fallback, or a FIFO match spanning several jobs."""
    return str(source or "") in _TRANSFERRED_BUILD_SOURCES and reference_type != "industry_job"


def _reprice_job_lot(lot: FifoLot, unit_cost_by_job_id: dict[int, float]) -> FifoLot:
    """`lot` at its industry job's current unit build cost when it points at
    a job that has one; otherwise unchanged (the job's reason, if any, is
    attached at allocation)."""
    if lot.reference_type != "industry_job" or lot.reference_id is None:
        return lot
    current = unit_cost_by_job_id.get(int(lot.reference_id))
    if current is None or current == lot.unit_price:
        return lot
    return replace(lot, unit_price=float(current))


def _opening_inventory_lot(events: list[dict[str, Any]]) -> FifoLot | None:
    running_balance = 0
    min_balance = 0
    first_known_unit_cost: float | None = None
    first_known_date: Any = None

    for event in events:
        kind = str(event.get("kind") or "")
        quantity = _safe_int(event.get("quantity")) or 0

        if kind == "buy":
            tx = event.get("tx")
            quantity = _safe_int(getattr(tx, "quantity", None)) or 0
            unit_price = _safe_float(getattr(tx, "unit_price", None))
            if first_known_unit_cost is None and unit_price is not None and unit_price > 0 and quantity > 0:
                first_known_unit_cost = float(unit_price)
                first_known_date = getattr(tx, "date", None)
            running_balance += int(quantity)
        elif kind == "job":
            unit_cost = _safe_float(event.get("unit_cost"))
            if first_known_unit_cost is None and unit_cost is not None and unit_cost > 0 and quantity > 0:
                first_known_unit_cost = float(unit_cost)
                job = event.get("job")
                first_known_date = getattr(job, "completed_date", None) or getattr(job, "end_date", None)
            running_balance += int(quantity)
        elif kind == "asset_history":
            lot = event.get("lot")
            if lot is not None:
                lot_qty = _safe_int(getattr(lot, "quantity", None)) or 0
                lot_price = _safe_float(getattr(lot, "unit_price", None))
                if first_known_unit_cost is None and lot_price is not None and lot_price > 0 and lot_qty > 0:
                    first_known_unit_cost = float(lot_price)
                    first_known_date = getattr(lot, "acquisition_date", None)
                running_balance += int(lot_qty)
        elif kind == "sell":
            tx = event.get("tx")
            quantity = _safe_int(getattr(tx, "quantity", None)) or 0
            running_balance -= int(quantity)
        elif kind == "consume":
            # Consumes drain the opening lot first, so it is sized for them too.
            running_balance -= int(quantity)

        min_balance = min(min_balance, int(running_balance))

    opening_quantity = max(0, -int(min_balance))
    if opening_quantity <= 0 or first_known_unit_cost is None or first_known_unit_cost <= 0:
        return None

    return FifoLot(
        quantity=int(opening_quantity),
        unit_price=float(first_known_unit_cost),
        acquisition_date=first_known_date,
        reference_id=None,
        reference_type=ASSET_SOURCE_OPENING_INVENTORY,
        source=ASSET_SOURCE_OPENING_INVENTORY,
    )


def _journal_fee_breakdown(*, gross_revenue: float, journal: CharacterWalletJournalModel | None) -> tuple[float, float, float, str, list[str]]:
    notes: list[str] = []
    if journal is None:
        notes.append("No linked wallet journal entry was found for this sale.")
        return 0.0, 0.0, float(gross_revenue), "gross_only", notes

    sales_tax_amount = max(0.0, float(_safe_float(getattr(journal, "tax", None)) or 0.0))
    journal_amount = _safe_float(getattr(journal, "amount", None))

    if journal_amount is not None and journal_amount > 0:
        total_fees = max(0.0, float(gross_revenue) - float(journal_amount))
        if sales_tax_amount > total_fees:
            sales_tax_amount = total_fees
        other_fees_amount = max(0.0, total_fees - sales_tax_amount)
        return other_fees_amount, sales_tax_amount, float(journal_amount), "journal_amount", notes

    if sales_tax_amount > 0:
        notes.append("Linked wallet journal entry has no positive amount; net revenue is estimated from gross revenue minus tax.")
        return 0.0, sales_tax_amount, max(0.0, float(gross_revenue) - sales_tax_amount), "gross_less_tax", notes

    notes.append("Linked wallet journal entry has no usable amount or tax; fees are not captured for this sale.")
    return 0.0, 0.0, float(gross_revenue), "gross_only", notes


def _gross_only_fee_breakdown(*, gross_revenue: float) -> tuple[float, float, float, str, list[str]]:
    return 0.0, 0.0, float(gross_revenue), "gross_only", [
        "Corporation realized profit currently does not have wallet journal-backed fee capture.",
    ]


_CORP_FEE_MATCH_WINDOW_SECONDS = 3600
# Tight window for transaction_tax date-proximity fallback (fires when context_id is NULL):
# tax entries appear at nearly the same second as their sell transaction.
_CORP_TAX_DATE_FALLBACK_WINDOW_SECONDS = 120
# How far after a broker_fee entry to search for its sell transactions:
_CORP_BROKER_ATTRIBUTION_WINDOW_SECONDS = 4 * 3600
# Max time gap between consecutive fills of the SAME order (larger gap → new order):
_CORP_BROKER_CLUSTER_GAP_SECONDS = 90 * 60


def _parse_eve_date(date_str: Any) -> datetime | None:
    if date_str is None:
        return None
    if isinstance(date_str, datetime):
        return date_str
    try:
        return datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
    except ValueError:
        return None


def _corp_journal_fee_breakdown(
    *,
    gross_revenue: float,
    sell_date: datetime | None,
    sell_transaction_id: int | None = None,
    sell_division: int | None = None,
    brokers_fee_entries: list[Any],
    transaction_tax_entries: list[Any],
    used_journal_ids: set[int],
    pre_computed_broker_fee: float | None = None,
) -> tuple[float, float, float, str, list[str]]:
    """Match corp sell transaction to its brokers_fee and transaction_tax journal entries.

    pre_computed_broker_fee: when set, this per-unit broker fee was already determined by
    proportionally splitting a multi-unit order's lump-sum broker_fee journal entry across
    all units in that order. It is used directly instead of date-proximity matching.

    For brokers_fee without pre_computed_broker_fee: date-proximity within
    ±_CORP_FEE_MATCH_WINDOW_SECONDS.
    For transaction_tax: context_id exact match first; falls back to date proximity within
    ±_CORP_TAX_DATE_FALLBACK_WINDOW_SECONDS when context_id is NULL.
    Each journal entry is consumed once (via used_journal_ids) to prevent double-matching.
    """
    notes: list[str] = []
    gross = float(gross_revenue)

    def _find_by_date(entries: list[Any], window: int, division: int | None) -> Any | None:
        best_entry = None
        best_delta: float | None = None
        for entry in entries:
            if getattr(entry, "wallet_journal_id", None) in used_journal_ids:
                continue
            if division is not None and getattr(entry, "division", None) is not None:
                if entry.division != division:
                    continue
            entry_date = _parse_eve_date(entry.date)
            if entry_date and sell_date:
                sd = sell_date.replace(tzinfo=None)
                ed = entry_date.replace(tzinfo=None)
                delta = abs((ed - sd).total_seconds())
                if delta <= window and (best_delta is None or delta < best_delta):
                    best_entry = entry
                    best_delta = delta
        return best_entry

    def _find_tax_entry(division: int | None) -> Any | None:
        # Try exact context_id match first
        if sell_transaction_id is not None:
            for entry in transaction_tax_entries:
                if getattr(entry, "wallet_journal_id", None) in used_journal_ids:
                    continue
                if division is not None and getattr(entry, "division", None) is not None:
                    if entry.division != division:
                        continue
                ctx = getattr(entry, "context_id", None)
                if ctx == sell_transaction_id:
                    return entry
                # Entry has a non-null context_id that doesn't match — skip for date fallback
                if ctx is not None:
                    continue
                # context_id is NULL — eligible for date-proximity fallback (handled below)
        # Date-proximity fallback: only entries where context_id IS NULL are eligible —
        # entries with a non-null context_id that didn't match the exact-match pass above
        # belong to a different transaction and must not be attributed here.
        null_ctx_entries = [e for e in transaction_tax_entries if getattr(e, "context_id", None) is None]
        return _find_by_date(null_ctx_entries, _CORP_TAX_DATE_FALLBACK_WINDOW_SECONDS, division)

    broker_fee = 0.0
    has_broker = False

    if pre_computed_broker_fee is not None:
        broker_fee = float(pre_computed_broker_fee)
        has_broker = True
    else:
        broker_entry = _find_by_date(brokers_fee_entries, _CORP_FEE_MATCH_WINDOW_SECONDS, sell_division)
        if broker_entry is not None:
            candidate_fee = abs(float(_safe_float(getattr(broker_entry, "amount", None)) or 0.0))
            # Reject the match when the implied broker rate is implausible — this prevents a large
            # broker fee (placed for an expensive item simultaneously) from being attributed to a
            # cheap-item sell that happens to be the nearest timestamp in the window.
            _min_r, _max_r = 0.003, 0.25
            rate_ok = gross <= 0 or (_min_r <= candidate_fee / gross <= _max_r)
            if rate_ok:
                eid = _safe_int(getattr(broker_entry, "wallet_journal_id", None))
                if eid is not None:
                    used_journal_ids.add(int(eid))
                broker_fee = candidate_fee
                has_broker = True
            else:
                notes.append(
                    f"Broker fee match rejected by rate check "
                    f"(fee={candidate_fee:.2f}, gross={gross:.2f}, "
                    f"rate={candidate_fee / gross:.4f} outside [{_min_r},{_max_r}])."
                )

    tax_entry = _find_tax_entry(sell_division)
    tax_fee = 0.0
    has_tax = False
    if tax_entry is not None:
        eid = _safe_int(getattr(tax_entry, "wallet_journal_id", None))
        if eid is not None:
            used_journal_ids.add(int(eid))
        tax_fee = abs(float(_safe_float(getattr(tax_entry, "amount", None)) or 0.0))
        has_tax = True

    if has_broker and has_tax:
        mode = "journal_matched"
    elif has_broker or has_tax:
        mode = "journal_partial"
        notes.append("Only one of broker fee or transaction tax was matched from the wallet journal.")
    else:
        notes.append("No matching wallet journal fee entries found for this corp sale; fees are not captured.")
        return 0.0, 0.0, gross, "gross_only", notes

    net_revenue = max(0.0, gross - broker_fee - tax_fee)
    return broker_fee, tax_fee, net_revenue, mode, notes


#: The fee capture modes of a sale whose fees were assumed (the character's skills unknown).
ASSUMED_FEE_CAPTURE_MODES = ("assumed_market_fees", "journal_plus_assumed_broker_fee")


def fee_inputs_fingerprint(raw_market_fees: Any, settings: market_fees.MarketFeeSettings) -> str:
    """The inputs of a character's fee estimate, as a string a ledger row
    keeps (fee_inputs): the seven industry/* fee settings, and the stored
    skills and standings (Jita 4-4) or, without them, why the fees are
    assumed. Equal fingerprints give equal fees."""
    payload = raw_market_fees
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = None
    jita = payload.get("jita_4_4") if isinstance(payload, dict) else None
    jita = jita if isinstance(jita, dict) else {}
    reason = market_fees.fees_from_stored_market_fees(raw_market_fees, settings)[2]
    return json.dumps({"settings": asdict(settings), "skills": jita.get("skills"), "standings": jita.get("standings"),
                       "assumed": reason}, sort_keys=True, default=str)


def _character_market_fees(raw_market_fees: Any, admin_settings: Any) -> tuple[float, float, str | None]:
    """(sales tax, broker fee, reason) for a character: from its stored
    skills and standings (Character.refresh_market_fees) with the current
    admin settings industry/*, through application.market_fees like the
    price suggestion (not the stored rates, which lag a settings change).
    Without known skills: the fees of an untrained character without
    standings (the highest) and the reason, starting "Fees assumed". A
    stored invalid fee setting raises AdminSettingInvalid naming it."""
    settings = market_fees.MarketFeeSettings.from_admin(admin_settings)
    sales_tax, broker_fee, reason = market_fees.fees_from_stored_market_fees(raw_market_fees, settings)
    return sales_tax, broker_fee, (reason[:1].upper() + reason[1:]) if reason else None


def _apply_estimated_market_fees(
    *,
    gross_revenue: float,
    sales_tax_amount: float,
    other_fees_amount: float,
    fee_capture_mode: str,
    notes: list[str],
    sales_tax_fraction: float | None,
    broker_fee_fraction: float | None,
    journal: CharacterWalletJournalModel | None,
    fee_reason: str | None = None,
) -> tuple[float, float, float, str, list[str]]:
    """The sale's fees with the estimates added: sales tax when no journal
    entry gives it, and the broker / order update fees. With `fee_reason`
    (the character's skills unknown, so the fractions are the assumed
    highest ones) the reason is noted once and the mode is an assumed one
    (ASSUMED_FEE_CAPTURE_MODES)."""
    normalized_notes = list(notes)
    normalized_sales_tax = float(max(0.0, sales_tax_amount))
    normalized_other_fees = float(max(0.0, other_fees_amount))
    normalized_mode = str(fee_capture_mode or "gross_only")
    estimated = "estimated_market_fees" if fee_reason is None else "assumed_market_fees"
    source = "the character market fee profile" if fee_reason is None else "assumed fees (see below)"
    applied = False

    if journal is None and sales_tax_fraction is not None and normalized_sales_tax <= 0.0:
        normalized_sales_tax = max(0.0, float(gross_revenue) * float(sales_tax_fraction))
        normalized_notes.append(f"Sales tax was estimated from {source}.")
        normalized_mode = estimated
        applied = True

    if broker_fee_fraction is not None and broker_fee_fraction > 0.0:
        estimated_broker_fee = max(0.0, float(gross_revenue) * float(broker_fee_fraction))
        if estimated_broker_fee > 0.0:
            normalized_other_fees += estimated_broker_fee
            normalized_notes.append(
                f"Broker / order update fees were estimated from {source}; exact relist history is not stored."
            )
            applied = True
            if normalized_mode == "journal_amount":
                normalized_mode = ("journal_plus_estimated_broker_fee" if fee_reason is None
                                   else "journal_plus_assumed_broker_fee")
            elif normalized_mode != estimated:
                normalized_mode = estimated

    if applied and fee_reason:
        normalized_notes.append(fee_reason)

    normalized_net_revenue = max(0.0, float(gross_revenue) - normalized_sales_tax - normalized_other_fees)
    return normalized_other_fees, normalized_sales_tax, normalized_net_revenue, normalized_mode, normalized_notes


def _confidence(*, priced_quantity: int, unpriced_quantity: int, fee_capture_mode: str) -> str:
    if unpriced_quantity > 0:
        return "Low"
    if fee_capture_mode in ASSUMED_FEE_CAPTURE_MODES:
        return "Low"   # the fees are the assumed highest ones (the character's skills unknown)
    if fee_capture_mode in ("journal_amount", "journal_matched", "journal_partial"):
        return "High"
    return "Medium"


def _normalize_untracked_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    source_mix = normalized.get("source_mix") or {}
    priced_quantity = int(normalized.get("priced_quantity") or 0)
    unpriced_quantity = int(normalized.get("unpriced_quantity") or 0)
    allocated_cost = float(normalized.get("allocated_cost") or 0.0)
    net_revenue = _safe_float(normalized.get("net_revenue"))

    if unpriced_quantity > 0 and normalized.get("realized_profit") is None and net_revenue is not None:
        # A row built while untracked sales got no profit: read it with the cost-0 rule, no rebuild needed.
        normalized["realized_profit"] = float(net_revenue) - allocated_cost
        normalized["realized_margin_fraction"] = (
            normalized["realized_profit"] / float(net_revenue) if net_revenue > 0 else None
        )
    if source_mix or priced_quantity > 0 or unpriced_quantity <= 0:
        return normalized

    normalized["source_mix"] = {
        ASSET_SOURCE_UNTRACKED: {
            "quantity": int(unpriced_quantity),
            "cost": float(allocated_cost),
        }
    }

    allocation_details = normalized.get("allocation_details") or []
    if not allocation_details:
        normalized["allocation_details"] = [
            {
                "source": ASSET_SOURCE_UNTRACKED,
                "quantity": int(unpriced_quantity),
                "unit_cost": None,
                "total_cost": float(allocated_cost),
                "acquisition_date": None,
                "reference_id": None,
                "reference_type": "untracked_inventory",
            }
        ]

    return normalized


def _transfer_stamp_rows(app_session: Any) -> list[Any]:
    """Every corp's asset history rows stamped as transferred from a character,
    one per item (its earliest costed row: every asset refresh copies a stack
    again), oldest observed first."""
    query = app_session.query(CorporationAssetHistoryModel).filter(
        CorporationAssetHistoryModel.acquisition_unit_cost.isnot(None),
        CorporationAssetHistoryModel.acquisition_source.like("%transferred%"),
    )
    out: list[Any] = []
    seen_items: set[tuple[int, int]] = set()
    for row in query.order_by(CorporationAssetHistoryModel.observed_at, CorporationAssetHistoryModel.id).all():
        item_id = _safe_int(getattr(row, "item_id", None))
        if item_id is not None:
            if (int(row.corporation_id), item_id) in seen_items:
                continue
            seen_items.add((int(row.corporation_id), item_id))
        type_id = _safe_int(getattr(row, "type_id", None))
        unit_cost = _safe_float(getattr(row, "acquisition_unit_cost", None))
        quantity = _safe_int(getattr(row, "quantity", None))
        if type_id is None or type_id <= 0 or unit_cost is None or unit_cost <= 0 or not quantity or quantity <= 0:
            continue
        out.append(row)
    return out


class _BaseRealizedProfitLedgerService:
    owner_id_field: str
    ledger_model: Any
    transaction_model: Any
    industry_job_model: Any
    owner_model: Any

    def __init__(self, *, app_session: Any, sde_session: Any, market_prices: list[dict[str, Any]] | None = None,
                 admin_settings: Any = None):
        self._app_session = app_session
        self._sde_session = sde_session
        self._market_prices = market_prices or []
        # The admin settings industry/* of the fee estimates, read at each rebuild (None: the schema defaults).
        self._admin_settings = admin_settings

    def rebuild(self, *, owner_id: int | None = None) -> list[dict[str, Any]]:
        owner_ids = [int(owner_id)] if owner_id is not None else self.owner_ids()

        persisted_rows: list[Any] = []
        for current_owner_id in owner_ids:
            persisted_rows.extend(self._rebuild_owner(owner_id=int(current_owner_id)))

        self._app_session.commit()
        return [self._serialize_row(row) for row in persisted_rows]

    def owner_ids(self) -> list[int]:
        """Every owner a rebuild of all owners covers."""
        return [int(getattr(row, self.owner_id_field)) for row in self._app_session.query(self.owner_model).all()]

    def stale_resolved_cost_owner_ids(self, rows: list[dict[str, Any]]) -> set[int]:
        """The owners with a row whose lower-bound reason no longer matches
        its industry job's (the job was re-snapshotted, e.g. once the
        overview published its decryptor choices): rebuilt on read."""
        current: dict[int, str] | None = None
        stale: set[int] = set()
        for row in rows:
            owner_id = _safe_int(row.get(self.owner_id_field))
            if owner_id is None or owner_id in stale:
                continue
            for entry in row.get("allocation_details") or []:
                if not isinstance(entry, dict) or not entry.get("cost_unknown_reason"):
                    continue
                if entry.get("reference_type") != "industry_job":
                    continue
                if current is None:
                    current = self._load_cost_unknown_reason_by_job_id()
                if current.get(int(_safe_int(entry.get("reference_id")) or 0)) != entry.get("cost_unknown_reason"):
                    stale.add(int(owner_id))
                    break
        return stale | self._stale_cost_basis_owner_ids(rows, skip=stale)

    def _stale_cost_basis_owner_ids(self, rows: list[dict[str, Any]], *, skip: set[int]) -> set[int]:
        """The owners with a row whose lot cost changed since it was built:
        an allocation's `cost_basis` against its job's current unit_build_cost
        or its blended lot's current stamped cost. Only the jobs and asset
        history rows the rows name are read. A job-referenced or blended
        allocation without a basis (a ledger built before it was recorded)
        is stale once."""
        stale: set[int] = set()
        job_bases: list[tuple[int, int, Any]] = []
        history_bases: list[tuple[int, int, Any]] = []
        for row in rows:
            owner_id = _safe_int(row.get(self.owner_id_field))
            if owner_id is None or owner_id in skip or owner_id in stale:
                continue
            for entry in row.get("allocation_details") or []:
                if not isinstance(entry, dict):
                    continue
                job_id = _safe_int(entry.get("reference_id"))
                is_job = entry.get("reference_type") == "industry_job" and bool(job_id)
                history_id = _safe_int(entry.get("asset_history_id"))
                if (is_job or history_id is not None or self._needs_cost_basis(entry)) and "cost_basis" not in entry:
                    stale.add(int(owner_id))
                    break
                if is_job:
                    job_bases.append((int(owner_id), int(job_id), entry.get("cost_basis")))
                elif history_id is not None:
                    history_bases.append((int(owner_id), int(history_id), entry.get("cost_basis")))
        if job_bases:
            current = self._load_job_unit_cost_by_job_id(job_ids={job_id for _, job_id, _ in job_bases})
            stale |= {o for o, job_id, basis in job_bases if not _same_cost(current.get(job_id), basis)}
        if history_bases:
            current_history = self._current_asset_history_unit_costs({h for _, h, _ in history_bases})
            stale |= {o for o, h, basis in history_bases if not _same_cost(current_history.get(h), basis)}
        return stale

    def _needs_cost_basis(self, entry: dict[str, Any]) -> bool:
        """An allocation that should carry a cost basis beyond the job
        references (the corp's blended transfer lots)."""
        return False

    def _current_asset_history_unit_costs(self, history_ids: set[int]) -> dict[int, float | None]:
        """{asset history row id: its current acquisition_unit_cost}; the
        owner kinds with blended lots override it."""
        return {}

    def list_rows(self, *, owner_id: int | None = None) -> list[dict[str, Any]]:
        query = self._app_session.query(self.ledger_model)
        if owner_id is not None:
            query = query.filter(getattr(self.ledger_model, self.owner_id_field) == int(owner_id))
        rows = query.order_by(self.ledger_model.date.desc(), self.ledger_model.transaction_id.desc()).all()
        return [_normalize_untracked_payload(self._serialize_row(row)) for row in rows]

    def next_fifo_unit_costs(self, *, owner_scope: str, owner_id: int) -> dict[int, tuple[float | None, str]]:
        """{type_id: (unit cost of its oldest lot still open after replaying
        every buy, build, sale and job input, short source label)} from one
        replay of the owner; (None, reason) when that lot has no cost. Types
        with an empty queue are left out. Read-only: no ledger rows."""
        service_class = _SERVICE_BY_SCOPE.get(str(owner_scope))
        if service_class is None:
            raise ValueError(f"owner_scope must be 'character' or 'corporation', not {owner_scope!r}")
        service = self if isinstance(self, service_class) else service_class(
            app_session=self._app_session, sde_session=self._sde_session,
            market_prices=self._market_prices, admin_settings=self._admin_settings)
        out: dict[int, tuple[float | None, str]] = {}
        for type_id, lots in service._replay_owner(owner_id=int(owner_id))["lots_by_type"].items():
            head = next((lot for lot in lots if int(lot.quantity or 0) > 0), None)
            if head is None:
                continue
            if _safe_float(head.unit_price) is None or float(head.unit_price) <= 0:
                out[int(type_id)] = (None, f"fifo: oldest lot of type {int(type_id)} has no cost")
            elif head.reference_id is not None:
                out[int(type_id)] = (float(head.unit_price), "fifo: {} {}".format(
                    str(head.reference_type or head.source).replace("_", " "), int(head.reference_id)))
            else:
                out[int(type_id)] = (float(head.unit_price), f"fifo: {head.source or 'unknown'}")
        return out

    def next_fifo_unit_cost(
        self, *, owner_scope: str, owner_id: int, type_id: int,
    ) -> tuple[float | None, str | None]:
        """next_fifo_unit_costs for one type; (None, reason) when its queue is empty."""
        costs = self.next_fifo_unit_costs(owner_scope=owner_scope, owner_id=int(owner_id))
        return costs.get(int(type_id)) or (
            None, f"fifo: no open lot of type {int(type_id)} for {owner_scope} {int(owner_id)}")

    def _load_input_consuming_jobs(self, *, owner_id: int) -> list[Any]:
        """The jobs whose inputs left this owner's queue: its own jobs."""
        return self._load_industry_jobs(owner_id=int(owner_id))

    def _load_blueprints(self, blueprint_type_ids: set[int | None]) -> dict[int, Any]:
        """{blueprintTypeID: SDE row} in one query per chunk, for a replay."""
        ids = sorted(i for i in blueprint_type_ids if i) if self._sde_session is not None else []
        out: dict[int, Any] = {}
        for start in range(0, len(ids), _SQL_IN_CHUNK):
            for bp in self._sde_session.query(Blueprints).filter(
                    Blueprints.blueprintTypeID.in_(ids[start:start + _SQL_IN_CHUNK])).all():
                out[int(bp.blueprintTypeID)] = bp
        return out

    def _replay_owner(self, *, owner_id: int) -> dict[str, Any]:
        """Replay the owner's events per item in (date, id) order through one
        FIFO queue: buys, finished jobs and asset-history lots fill it; sales
        and industry job inputs (taken when the job starts) drain it, oldest
        first. Returns the queues left (`lots_by_type`), each sale with its
        allocation (`sales`, in order) and the job-input counts (`inputs`).
        Writes nothing."""
        wallet_transactions = self._load_wallet_transactions(owner_id=int(owner_id))
        industry_jobs = self._load_industry_jobs(owner_id=int(owner_id))
        asset_history_lots = self._load_asset_history_lots(owner_id=int(owner_id))

        uncancelled_excess = self._merge_character_lots(asset_history_lots)

        # A lot that points at an industry job is priced from the job's
        # current unit_build_cost, not the cost stamped when the lot was
        # written: a job re-snapshotted since (the decryptor re-snapshot, a
        # later job refresh) would otherwise leave the lot at a stale lower
        # bound with its reason gone.
        job_unit_cost_by_job_id = self._load_job_unit_cost_by_job_id()
        asset_history_lots = {
            type_id: [_reprice_job_lot(lot, job_unit_cost_by_job_id) for lot in lots]
            for type_id, lots in asset_history_lots.items()
        }

        cost_unknown_reason_by_job_id = self._load_cost_unknown_reason_by_job_id()

        lots_by_type: dict[int, list[FifoLot]] = {}
        events_by_type: dict[int, list[dict[str, Any]]] = {}
        for tx in wallet_transactions:
            type_id = _safe_int(getattr(tx, "type_id", None))
            quantity = _safe_int(getattr(tx, "quantity", None))
            if not type_id or type_id <= 0 or not quantity or quantity <= 0:
                continue
            is_buy = getattr(tx, "is_buy", None)
            if is_buy is None:
                continue
            events_by_type.setdefault(int(type_id), []).append(
                {
                    "kind": "buy" if bool(is_buy) else "sell",
                    "dt": _parse_date(getattr(tx, "date", None)),
                    "sort_id": int(_safe_int(getattr(tx, "transaction_id", None)) or 0),
                    "tx": tx,
                }
            )

        market_price_map = {
            int(row.get("type_id")): float(row.get("average_price") or row.get("adjusted_price"))
            for row in self._market_prices
            if isinstance(row, dict)
            and _safe_int(row.get("type_id")) is not None
            and _safe_float(row.get("average_price") or row.get("adjusted_price")) is not None
        }
        for job in industry_jobs:
            lot = completed_job_output_lot(job, sde_session=self._sde_session, market_price_map=market_price_map)
            if lot is None:
                continue
            quantity, unit_cost = lot
            product_type_id = int(getattr(job, "product_type_id"))
            completed_date = getattr(job, "completed_date", None) or getattr(job, "end_date", None)
            events_by_type.setdefault(int(product_type_id), []).append(
                {
                    "kind": "job",
                    "dt": _parse_date(completed_date),
                    "sort_id": int(_safe_int(getattr(job, "job_id", None)) or 0),
                    "job": job,
                    "quantity": int(quantity),
                    "unit_cost": float(unit_cost),
                }
            )

        # Inputs leave when the job starts, from the same queue a sale takes from.
        consuming_jobs = self._load_input_consuming_jobs(owner_id=int(owner_id))
        blueprints = self._load_blueprints({_safe_int(getattr(job, "blueprint_type_id", None)) for job in consuming_jobs})
        for job in consuming_jobs:
            start_date = _parse_date(getattr(job, "start_date", None))
            if start_date is None:
                continue
            job_inputs = industry_job_input_quantities(job, sde_session=self._sde_session, blueprints=blueprints)
            for input_type_id, input_quantity in job_inputs.items():
                events_by_type.setdefault(int(input_type_id), []).append(
                    {
                        "kind": "consume",
                        "dt": start_date,
                        "sort_id": int(_safe_int(getattr(job, "job_id", None)) or 0),
                        "quantity": int(input_quantity),
                    }
                )

        # Units moved to a corp hangar leave when the corp first saw the stack.
        for stamp in self._load_transfer_out_stamps(owner_id=int(owner_id)):
            events_by_type.setdefault(int(stamp.type_id), []).append(
                {
                    "kind": "transfer",
                    "dt": _parse_date(stamp.observed_at),
                    "sort_id": int(stamp.id),
                    "quantity": int(stamp.quantity),
                }
            )

        for ah_type_id, ah_lots in asset_history_lots.items():
            for lot in ah_lots:
                events_by_type.setdefault(int(ah_type_id), []).append(
                    {
                        "kind": "asset_history",
                        "dt": _parse_date(lot.acquisition_date),
                        "sort_id": 0,
                        "lot": lot,
                    }
                )

        sales: list[tuple[Any, dict[str, Any]]] = []
        # draws: {(type_id, reference_type, reference_id): units} the job inputs took per lot.
        # transfers: {stamp row id: [(reference_type, reference_id) or None when untracked, units]}.
        inputs: dict[str, Any] = {"jobs": set(), "drained": 0, "untracked": 0, "draws": defaultdict(int),
                                  "transfers": {}, "transfer_draws": defaultdict(int), "transferred": 0, "transfer_untracked": 0}
        for type_id, events in events_by_type.items():
            events.sort(key=lambda item: (item.get("dt") or datetime.min, int(item.get("sort_id") or 0)))
            lots = lots_by_type.setdefault(int(type_id), [])
            opening_lot = _opening_inventory_lot(events)
            if opening_lot is not None:
                lots.append(opening_lot)
            for event in events:
                kind = str(event.get("kind") or "")
                if kind == "buy":
                    tx = event.get("tx")
                    unit_price = _safe_float(getattr(tx, "unit_price", None))
                    quantity = _safe_int(getattr(tx, "quantity", None)) or 0
                    if unit_price is None or unit_price <= 0 or quantity <= 0:
                        continue
                    _append_lot(
                        lots_by_type,
                        type_id=int(type_id),
                        lot=FifoLot(
                            quantity=int(quantity),
                            unit_price=float(unit_price),
                            acquisition_date=getattr(tx, "date", None),
                            reference_id=_safe_int(getattr(tx, "transaction_id", None)),
                            reference_type="wallet_transaction",
                            source=ASSET_SOURCE_MARKET_BUY,
                        ),
                    )
                    continue

                if kind == "job":
                    job = event.get("job")
                    quantity = _safe_int(event.get("quantity")) or 0
                    unit_cost = _safe_float(event.get("unit_cost"))
                    if unit_cost is None or unit_cost <= 0 or quantity <= 0:
                        continue
                    _append_lot(
                        lots_by_type,
                        type_id=int(type_id),
                        lot=FifoLot(
                            quantity=int(quantity),
                            unit_price=float(unit_cost),
                            acquisition_date=getattr(job, "completed_date", None) or getattr(job, "end_date", None),
                            reference_id=_safe_int(getattr(job, "job_id", None)),
                            reference_type="industry_job",
                            source=ASSET_SOURCE_INDUSTRY_BUILD,
                        ),
                    )
                    continue

                if kind == "asset_history":
                    lot = event.get("lot")
                    if lot is not None:
                        _append_lot(lots_by_type, type_id=int(type_id), lot=lot)
                    continue

                if kind == "consume":
                    # Like a sale, without a row; units beyond the queue came from untracked stock.
                    taken = _consume_lots(lots, quantity=int(event["quantity"]))
                    inputs["jobs"].add(int(event["sort_id"]))
                    inputs["drained"] += int(taken["priced_quantity"])
                    inputs["untracked"] += int(taken["unpriced_quantity"])
                    for entry in taken["allocations"]:
                        if entry["reference_id"] is not None:
                            inputs["draws"][(int(type_id), entry["reference_type"], int(entry["reference_id"]))] += int(entry["quantity"])
                    continue

                if kind == "transfer":
                    taken = _consume_lots(lots, quantity=int(event["quantity"]))
                    pieces = [((entry["reference_type"], entry["reference_id"]), int(entry["quantity"]))
                              for entry in taken["allocations"]]
                    if taken["unpriced_quantity"]:
                        pieces.append((None, int(taken["unpriced_quantity"])))
                    inputs["transfers"][int(event["sort_id"])] = pieces
                    for (reference_type, reference_id), quantity in pieces[:len(taken["allocations"])]:
                        if reference_id is not None:
                            inputs["transfer_draws"][(int(type_id), reference_type, int(reference_id))] += quantity
                    inputs["transferred"] += int(taken["priced_quantity"])
                    inputs["transfer_untracked"] += int(taken["unpriced_quantity"])
                    continue

                if kind != "sell":
                    continue

                tx = event.get("tx")
                quantity = _safe_int(getattr(tx, "quantity", None)) or 0
                if quantity <= 0:
                    continue
                allocation = _consume_lots(
                    lots, quantity=int(quantity), cost_unknown_reason_by_job_id=cost_unknown_reason_by_job_id,
                )
                _with_cost_basis(allocation["allocations"], job_unit_cost_by_job_id)
                if int(allocation["unpriced_quantity"]) > 0:
                    slot = allocation["by_source"].setdefault(ASSET_SOURCE_UNTRACKED, {"quantity": 0, "cost": 0.0})
                    slot["quantity"] = int(slot.get("quantity") or 0) + int(allocation["unpriced_quantity"])
                    slot["cost"] = float(slot.get("cost") or 0.0)
                    allocation["allocations"].append(
                        {
                            "source": ASSET_SOURCE_UNTRACKED,
                            "quantity": int(allocation["unpriced_quantity"]),
                            "unit_cost": None,
                            "total_cost": 0.0,
                            "acquisition_date": None,
                            "reference_id": None,
                            "reference_type": "untracked_inventory",
                        }
                    )
                sales.append((tx, allocation))

        return {"lots_by_type": lots_by_type, "sales": sales, "inputs": inputs,
                "uncancelled_excess": uncancelled_excess}

    def _rebuild_owner(self, *, owner_id: int) -> list[Any]:
        self._app_session.query(self.ledger_model).filter(getattr(self.ledger_model, self.owner_id_field) == int(owner_id)).delete()

        replay = self._replay_owner(owner_id=int(owner_id))
        inputs = replay["inputs"]
        if inputs["jobs"]:
            logger.info(
                "%s %s ledger: %d industry job(s) took %d tracked input unit(s) from the FIFO queues, %d untracked",
                self.owner_id_field, int(owner_id), len(inputs["jobs"]), inputs["drained"], inputs["untracked"],
            )
        if inputs["transfers"]:
            logger.info(
                "%s %s ledger: %d transfer(s) to a corp hangar took %d tracked unit(s) from the FIFO queues, %d untracked",
                self.owner_id_field, int(owner_id), len(inputs["transfers"]), inputs["transferred"],
                inputs["transfer_untracked"],
            )
        if replay["uncancelled_excess"]:
            excess = sorted(replay["uncancelled_excess"].items(), key=lambda item: -item[1])
            logger.info(
                "%s %s ledger: stamped transfer units with no character residual left, kept as stamped "
                "(%d type(s), largest type:units %s)",
                self.owner_id_field, int(owner_id), len(excess), ", ".join(f"{t}:{q}" for t, q in excess[:10]),
            )
        journal_by_id = self._load_journal_map(owner_id=int(owner_id))
        owner_context = self._load_owner_context(owner_id=int(owner_id))

        persisted_rows: list[Any] = []
        for tx, allocation in replay["sales"]:
            quantity = _safe_int(getattr(tx, "quantity", None)) or 0
            gross_revenue = _safe_float(getattr(tx, "total_price", None))
            if gross_revenue is None:
                unit_price = _safe_float(getattr(tx, "unit_price", None)) or 0.0
                gross_revenue = float(unit_price) * float(quantity)
            journal_ref_id = _safe_int(getattr(tx, "journal_ref_id", None))
            journal = journal_by_id.get(int(journal_ref_id)) if journal_ref_id is not None else None
            owner_context["current_tx"] = tx
            other_fees_amount, sales_tax_amount, net_revenue, fee_capture_mode, notes = self._fee_breakdown(
                gross_revenue=float(gross_revenue),
                journal=journal,
                owner_context=owner_context,
            )
            if ASSET_SOURCE_OPENING_INVENTORY in allocation["by_source"]:
                opening_slot = allocation["by_source"][ASSET_SOURCE_OPENING_INVENTORY]
                notes.append(
                    "{quantity} unit(s) were matched to estimated opening inventory using the earliest known tracked unit cost for this item.".format(
                        quantity=int(opening_slot.get("quantity") or 0)
                    )
                )
            if allocation["unpriced_quantity"] > 0:
                notes.append(f"{allocation['unpriced_quantity']} unit(s) have no recorded purchase "
                             "(stock from before tracking, trades, loot, gifts) and are counted at cost 0.")
            # Units without a cost basis count at cost 0; the note and Low confidence say so.
            realized_profit = float(net_revenue) - float(allocation["total_cost"])
            realized_margin_fraction = float(realized_profit) / float(net_revenue) if net_revenue > 0 else None

            row = self.ledger_model(
                **self._owner_fields(owner_id=int(owner_id)),
                transaction_id=int(_safe_int(getattr(tx, "transaction_id", None)) or 0),
                journal_ref_id=journal_ref_id,
                date=getattr(tx, "date", None),
                type_id=_safe_int(getattr(tx, "type_id", None)),
                type_name=getattr(tx, "type_name", None),
                type_group_name=getattr(tx, "type_group_name", None),
                type_category_name=getattr(tx, "type_category_name", None),
                quantity=int(quantity),
                unit_price=_safe_float(getattr(tx, "unit_price", None)),
                gross_revenue=float(gross_revenue),
                sales_tax_amount=float(sales_tax_amount),
                other_fees_amount=float(other_fees_amount),
                total_fees_amount=float(sales_tax_amount) + float(other_fees_amount),
                net_revenue=float(net_revenue),
                allocated_cost=float(allocation["total_cost"]),
                realized_profit=realized_profit,
                realized_margin_fraction=realized_margin_fraction,
                priced_quantity=int(allocation["priced_quantity"]),
                unpriced_quantity=int(allocation["unpriced_quantity"]),
                source_mix=allocation["by_source"],
                allocation_details=allocation["allocations"],
                fee_capture_mode=fee_capture_mode,
                confidence=_confidence(
                    priced_quantity=int(allocation["priced_quantity"]),
                    unpriced_quantity=int(allocation["unpriced_quantity"]),
                    fee_capture_mode=fee_capture_mode,
                ),
                notes=notes,
                **self._extra_row_fields(owner_context),
            )
            self._app_session.add(row)
            persisted_rows.append(row)

        self._app_session.flush()
        return persisted_rows

    def _load_wallet_transactions(self, *, owner_id: int) -> list[Any]:
        return self._app_session.query(self.transaction_model).filter(getattr(self.transaction_model, self.owner_id_field) == int(owner_id)).all()

    def _load_industry_jobs(self, *, owner_id: int) -> list[Any]:
        return self._app_session.query(self.industry_job_model).filter(getattr(self.industry_job_model, self.owner_id_field) == int(owner_id)).all()

    def _load_journal_map(self, *, owner_id: int) -> dict[int, Any]:
        return {}

    def _load_cost_unknown_reason_by_job_id(self) -> dict[int, str]:
        """{job_id: why its build cost is partly unknown} over character and
        corporation jobs (EVE job ids are unique across both): a corp sale
        can consume a lot a character built. The invention cost reason and
        the materials cost lower-bound reason, both when both are set."""
        out: dict[int, str] = {}
        for model in (CharacterIndustryJobsModel, CorporationIndustryJobsModel):
            rows = (
                self._app_session.query(
                    model.job_id, model.invention_cost_unknown_reason, model.materials_cost_lower_bound_reason)
                .filter(or_(model.invention_cost_unknown_reason.isnot(None),
                            model.materials_cost_lower_bound_reason.isnot(None)))
                .all()
            )
            for job_id, invention_reason, materials_reason in rows:
                reason = "; ".join(str(r) for r in (invention_reason, materials_reason) if r)
                if job_id is not None and reason:
                    out[int(job_id)] = reason
        return out

    def _load_job_unit_cost_by_job_id(self, *, job_ids: set[int] | None = None) -> dict[int, float]:
        """{job_id: current positive unit_build_cost} over character and
        corporation jobs (EVE job ids are unique across both); only `job_ids`
        when given."""
        out: dict[int, float] = {}
        chunks: list[list[int] | None] = [None]
        if job_ids is not None:
            ids = sorted(job_ids)
            chunks = [ids[i:i + _SQL_IN_CHUNK] for i in range(0, len(ids), _SQL_IN_CHUNK)]
        for model in (CharacterIndustryJobsModel, CorporationIndustryJobsModel):
            for chunk in chunks:
                query = self._app_session.query(model.job_id, model.unit_build_cost).filter(
                    model.unit_build_cost.isnot(None)
                )
                if chunk is not None:
                    query = query.filter(model.job_id.in_(chunk))
                for job_id, unit_build_cost in query.all():
                    unit_cost = _safe_float(unit_build_cost)
                    if job_id is not None and unit_cost is not None and unit_cost > 0:
                        out[int(job_id)] = unit_cost
        return out

    def _load_asset_history_lots(self, *, owner_id: int) -> dict[int, list[FifoLot]]:
        """Return FifoLots sourced from asset history, keyed by type_id. Subclasses may override."""
        return {}

    def _merge_character_lots(self, asset_history_lots: dict[int, list[FifoLot]]) -> dict[int, int]:
        """Merge character-held lots into `asset_history_lots` (the corp
        does); returns {type_id: stamped units left uncancelled}."""
        return {}

    def _load_transfer_out_stamps(self, *, owner_id: int) -> list[Any]:
        """The corp asset history stamps of units this owner moved to a corp
        hangar (the character does)."""
        return []

    def _load_owner_context(self, *, owner_id: int) -> dict[str, Any]:
        return {}

    def _extra_row_fields(self, owner_context: dict[str, Any]) -> dict[str, Any]:
        return {}

    def _owner_fields(self, *, owner_id: int) -> dict[str, Any]:
        return {self.owner_id_field: int(owner_id)}

    def _fee_breakdown(self, *, gross_revenue: float, journal: Any, owner_context: dict[str, Any]) -> tuple[float, float, float, str, list[str]]:
        return _gross_only_fee_breakdown(gross_revenue=float(gross_revenue))

    @staticmethod
    def _serialize_row(row: Any) -> dict[str, Any]:
        payload = {
            "transaction_id": int(row.transaction_id),
            "journal_ref_id": int(row.journal_ref_id) if row.journal_ref_id is not None else None,
            "date": row.date,
            "type_id": row.type_id,
            "type_name": row.type_name,
            "type_group_name": row.type_group_name,
            "type_category_name": row.type_category_name,
            "quantity": int(row.quantity),
            "unit_price": row.unit_price,
            "gross_revenue": row.gross_revenue,
            "sales_tax_amount": row.sales_tax_amount,
            "other_fees_amount": row.other_fees_amount,
            "total_fees_amount": row.total_fees_amount,
            "net_revenue": row.net_revenue,
            "allocated_cost": row.allocated_cost,
            "realized_profit": row.realized_profit,
            "realized_margin_fraction": row.realized_margin_fraction,
            "priced_quantity": int(row.priced_quantity or 0),
            "unpriced_quantity": int(row.unpriced_quantity or 0),
            "source_mix": row.source_mix or {},
            "allocation_details": row.allocation_details or [],
            "fee_capture_mode": row.fee_capture_mode,
            "confidence": row.confidence,
            "notes": row.notes or [],
            "updated_at": row.updated_at.isoformat() if row.updated_at is not None else None,
            # Contract: why allocated_cost is a lower bound (part of a build
            # cost is unknown), None when it is not.
            "cost_lower_bound_reason": cost_lower_bound_reason(row.allocation_details),
        }
        if hasattr(row, "fee_inputs"):
            payload["fee_inputs"] = row.fee_inputs
        if hasattr(row, "character_id"):
            payload["character_id"] = int(row.character_id)
        if hasattr(row, "corporation_id"):
            payload["corporation_id"] = int(row.corporation_id)
        return payload


class CharacterRealizedProfitLedgerService(_BaseRealizedProfitLedgerService):
    owner_id_field = "character_id"
    ledger_model = CharacterRealizedSalesLedgerModel
    transaction_model = CharacterWalletTransactionsModel
    industry_job_model = CharacterIndustryJobsModel
    owner_model = CharacterModel

    def rebuild(self, *, character_id: int | None = None) -> list[dict[str, Any]]:
        return super().rebuild(owner_id=character_id)

    def list_rows(self, *, character_id: int | None = None) -> list[dict[str, Any]]:
        return super().list_rows(owner_id=character_id)

    def stale_character_ids(self, rows: list[dict[str, Any]]) -> set[int]:
        """The characters with a row whose fee inputs (fee_inputs) differ
        from the current ones: an industry/* fee setting, or the character's
        stored skills or standings, changed since the row was built. A row
        without them (built before they were kept) is stale."""
        settings = market_fees.MarketFeeSettings.from_admin(self._admin_settings)
        current: dict[int, str] = {}
        stale: set[int] = set()
        for row in rows:
            character_id = _safe_int(row.get("character_id"))
            if character_id is None or character_id in stale:
                continue
            if character_id not in current:
                character = self._app_session.query(CharacterModel).filter_by(character_id=character_id).first()
                current[character_id] = fee_inputs_fingerprint(
                    getattr(character, "market_fees", None) if character is not None else None, settings)
            if row.get("fee_inputs") != current[character_id]:
                stale.add(character_id)
        return stale

    def _extra_row_fields(self, owner_context: dict[str, Any]) -> dict[str, Any]:
        return {"fee_inputs": owner_context.get("fee_inputs")}

    def _load_transfer_out_stamps(self, *, owner_id: int) -> list[Any]:
        """The corp transfer stamps naming one of this character's jobs or buys."""
        if getattr(self, "_stamp_rows", None) is None:
            self._stamp_rows = _transfer_stamp_rows(self._app_session)  # once per service, for every character
        job_ids = {int(job_id) for (job_id,) in self._app_session.query(CharacterIndustryJobsModel.job_id)
                   .filter(CharacterIndustryJobsModel.character_id == int(owner_id))}
        tx_ids = {int(tx_id) for (tx_id,) in self._app_session.query(CharacterWalletTransactionsModel.transaction_id)
                  .filter(CharacterWalletTransactionsModel.character_id == int(owner_id),
                          CharacterWalletTransactionsModel.is_buy.is_(True))}
        return [
            row for row in self._stamp_rows
            if (row.acquisition_reference_type == "industry_job" and _safe_int(row.acquisition_reference_id) in job_ids)
            or (row.acquisition_reference_type == "wallet_transaction" and _safe_int(row.acquisition_reference_id) in tx_ids)
        ]

    def _load_wallet_transactions(self, *, owner_id: int) -> list[Any]:
        return (
            self._app_session.query(self.transaction_model)
            .filter(
                self.transaction_model.character_id == int(owner_id),
                self.transaction_model.is_personal.is_not(False),
            )
            .all()
        )

    def _load_journal_map(self, *, owner_id: int) -> dict[int, Any]:
        wallet_journals = self._app_session.query(CharacterWalletJournalModel).filter_by(character_id=int(owner_id)).all()
        return {
            int(row.wallet_journal_id): row
            for row in wallet_journals
            if _safe_int(getattr(row, "wallet_journal_id", None)) is not None
        }

    def _load_owner_context(self, *, owner_id: int) -> dict[str, Any]:
        character = self._app_session.query(CharacterModel).filter_by(character_id=int(owner_id)).first()
        raw_market_fees = getattr(character, "market_fees", None) if character is not None else None
        sales_tax_fraction, broker_fee_fraction, fee_reason = _character_market_fees(
            raw_market_fees, self._admin_settings)
        return {
            "sales_tax_fraction": sales_tax_fraction,
            "broker_fee_fraction": broker_fee_fraction,
            "fee_reason": fee_reason,
            "fee_inputs": fee_inputs_fingerprint(
                raw_market_fees, market_fees.MarketFeeSettings.from_admin(self._admin_settings)),
        }

    def _fee_breakdown(self, *, gross_revenue: float, journal: Any, owner_context: dict[str, Any]) -> tuple[float, float, float, str, list[str]]:
        other_fees_amount, sales_tax_amount, net_revenue, fee_capture_mode, notes = _journal_fee_breakdown(
            gross_revenue=float(gross_revenue),
            journal=journal,
        )
        return _apply_estimated_market_fees(
            gross_revenue=float(gross_revenue),
            sales_tax_amount=float(sales_tax_amount),
            other_fees_amount=float(other_fees_amount),
            fee_capture_mode=fee_capture_mode,
            notes=notes,
            sales_tax_fraction=_safe_float(owner_context.get("sales_tax_fraction")),
            broker_fee_fraction=_safe_float(owner_context.get("broker_fee_fraction")),
            journal=journal,
            fee_reason=owner_context.get("fee_reason"),
        )


class CorporationRealizedProfitLedgerService(_BaseRealizedProfitLedgerService):
    owner_id_field = "corporation_id"
    ledger_model = CorporationRealizedSalesLedgerModel
    transaction_model = CorporationWalletTransactionsModel
    industry_job_model = CorporationIndustryJobsModel
    owner_model = CorporationModel

    def rebuild(self, *, corporation_id: int | None = None) -> list[dict[str, Any]]:
        return super().rebuild(owner_id=corporation_id)

    def list_rows(self, *, corporation_id: int | None = None) -> list[dict[str, Any]]:
        return super().list_rows(owner_id=corporation_id)

    def _load_character_source_lots(self) -> dict[int, dict[int, list[FifoLot]]]:
        """Return {character_id: {type_id: FifoLots}} from character industry jobs and
        character market buys, oldest first.

        These represent items manufactured or purchased by characters, then transferred to the
        corporation. Buys paid from the corp wallet are left out: the corp's own transactions
        already hold them. _merge_character_lots nets them against the character's own replay.
        """
        lots_by_character: dict[int, dict[int, list[FifoLot]]] = {}
        completed_statuses = COMPLETED_JOB_STATUSES

        char_jobs = (
            self._app_session.query(CharacterIndustryJobsModel)
            .filter(
                CharacterIndustryJobsModel.unit_build_cost.isnot(None),
                CharacterIndustryJobsModel.output_quantity.isnot(None),
            )
            .all()
        )
        for job in char_jobs:
            product_type_id = _safe_int(getattr(job, "product_type_id", None))
            if product_type_id is None:
                continue
            status = str(getattr(job, "status", "") or "").strip().lower()
            if status and status not in completed_statuses:
                continue
            unit_build_cost = _safe_float(getattr(job, "unit_build_cost", None))
            output_quantity = _safe_int(getattr(job, "output_quantity", None))
            if unit_build_cost is None or unit_build_cost <= 0 or not output_quantity or output_quantity <= 0:
                continue
            completed_date = getattr(job, "completed_date", None) or getattr(job, "end_date", None)
            lots_by_character.setdefault(int(job.character_id), {}).setdefault(int(product_type_id), []).append(
                FifoLot(
                    quantity=int(output_quantity),
                    unit_price=float(unit_build_cost),
                    acquisition_date=completed_date,
                    reference_id=_safe_int(getattr(job, "job_id", None)),
                    reference_type="industry_job",
                    source="industry_build_transferred",
                )
            )

        char_buys = (
            self._app_session.query(CharacterWalletTransactionsModel)
            .filter(
                CharacterWalletTransactionsModel.is_buy.is_(True),
                CharacterWalletTransactionsModel.is_personal.is_not(False),
                CharacterWalletTransactionsModel.unit_price.isnot(None),
            )
            .all()
        )
        for tx in char_buys:
            type_id = _safe_int(getattr(tx, "type_id", None))
            if type_id is None:
                continue
            unit_price = _safe_float(getattr(tx, "unit_price", None))
            quantity = _safe_int(getattr(tx, "quantity", None))
            if unit_price is None or unit_price <= 0 or not quantity or quantity <= 0:
                continue
            lots_by_character.setdefault(int(tx.character_id), {}).setdefault(int(type_id), []).append(
                FifoLot(
                    quantity=int(quantity),
                    unit_price=float(unit_price),
                    acquisition_date=getattr(tx, "date", None),
                    reference_id=_safe_int(getattr(tx, "transaction_id", None)),
                    reference_type="wallet_transaction",
                    source="character_market_buy_transferred",
                )
            )

        for lots_by_type in lots_by_character.values():
            for lots in lots_by_type.values():
                lots.sort(key=lambda lot: (_parse_date(lot.acquisition_date) or datetime.min))
        return lots_by_character

    def _load_asset_history_lots(self, *, owner_id: int) -> dict[int, list[FifoLot]]:
        """Load FifoLots from CorporationAssetHistoryModel rows that have a stamped acquisition_unit_cost.

        These represent finished goods transferred from characters who built them, backfilled
        by scripts/backfill_corp_transfer_costs.py with acquisition_source values such as
        'industry_build_transferred', 'industry_build_transferred_avg', and
        'character_market_buy_transferred'.

        Only transfer rows: the corp's own jobs and buys are already events. Every asset
        refresh copies a stack again, so one item is one lot, from its earliest costed row.
        """
        lots_by_type: dict[int, list[FifoLot]] = {}
        blended_reason_by_type_id: dict[int, str] | None = None
        # Every corp's stamps, read once per service: the character replays reuse them.
        if getattr(self, "_stamp_rows", None) is None:
            self._stamp_rows = _transfer_stamp_rows(self._app_session)
        for row in (r for r in self._stamp_rows if int(r.corporation_id) == int(owner_id)):
            type_id = int(row.type_id)
            unit_cost = float(row.acquisition_unit_cost)
            quantity = int(row.quantity)
            acquisition_date = getattr(row, "acquisition_date", None) or getattr(row, "observed_at", None)
            source = str(getattr(row, "acquisition_source", None) or "asset_history")
            reference_id = _safe_int(getattr(row, "acquisition_reference_id", None))
            reference_type = str(getattr(row, "acquisition_reference_type", None) or "asset_history")
            cost_unknown_reason: str | None = None
            if _is_blended_transfer_lot(source, reference_type):
                if blended_reason_by_type_id is None:
                    blended_reason_by_type_id = self._blended_cost_unknown_reason_by_type_id()
                cost_unknown_reason = blended_reason_by_type_id.get(int(type_id))
            lots_by_type.setdefault(int(type_id), []).append(
                FifoLot(
                    quantity=int(quantity),
                    unit_price=float(unit_cost),
                    acquisition_date=acquisition_date,
                    reference_id=reference_id,
                    reference_type=reference_type,
                    source=source,
                    cost_unknown_reason=cost_unknown_reason,
                    asset_history_id=(
                        _safe_int(getattr(row, "id", None)) if _is_blended_transfer_lot(source, reference_type) else None
                    ),
                    transfer_stamp_id=_safe_int(getattr(row, "id", None)),
                )
            )
        return lots_by_type

    def _needs_cost_basis(self, entry: dict[str, Any]) -> bool:
        return _is_blended_transfer_lot(entry.get("source"), entry.get("reference_type"))

    def _current_asset_history_unit_costs(self, history_ids: set[int]) -> dict[int, float | None]:
        ids = sorted(history_ids)
        out: dict[int, float | None] = {}
        for start in range(0, len(ids), _SQL_IN_CHUNK):
            for row_id, unit_cost in (
                self._app_session.query(CorporationAssetHistoryModel.id, CorporationAssetHistoryModel.acquisition_unit_cost)
                .filter(CorporationAssetHistoryModel.id.in_(ids[start:start + _SQL_IN_CHUNK]))
                .all()
            ):
                out[int(row_id)] = _safe_float(unit_cost)
        return out

    def _blended_cost_unknown_reason_by_type_id(self) -> dict[int, str]:
        """{product type: why a blended transfer lot of it is a lower bound}.
        A blended lot (scripts/backfill_corp_transfer_costs.py: the
        quantity-weighted average, or a FIFO match over several jobs) names no
        job, so it is flagged whenever any character job of its type, the jobs
        the blend is taken from, has a partly unknown build cost. A re-costed
        job re-stamps the blended lots of its type
        (corp_transfer_costs.restamp_blended_corp_transfer_lots); a resolved
        reason then makes the ledger stale (stale_resolved_cost_owner_ids)."""
        rows = (
            self._app_session.query(
                CharacterIndustryJobsModel.product_type_id,
                CharacterIndustryJobsModel.job_id,
                CharacterIndustryJobsModel.invention_cost_unknown_reason,
                CharacterIndustryJobsModel.materials_cost_lower_bound_reason,
            )
            .filter(or_(CharacterIndustryJobsModel.invention_cost_unknown_reason.isnot(None),
                        CharacterIndustryJobsModel.materials_cost_lower_bound_reason.isnot(None)))
            .order_by(CharacterIndustryJobsModel.job_id)
            .all()
        )
        by_type: dict[int, list[tuple[int, str]]] = defaultdict(list)
        for product_type_id, job_id, invention_reason, materials_reason in rows:
            reason = "; ".join(str(r) for r in (invention_reason, materials_reason) if r)
            if product_type_id is not None and job_id is not None and reason:
                by_type[int(product_type_id)].append((int(job_id), reason))
        return {
            type_id: (
                f"blended transfer cost: {len(jobs)} character job(s) of this type have a partly unknown "
                f"build cost (job {jobs[0][0]}: {jobs[0][1]})"
            )
            for type_id, jobs in by_type.items()
        }

    def stale_resolved_cost_owner_ids(self, rows: list[dict[str, Any]]) -> set[int]:
        """Also stale: a row with a blended transfer lot whose lower-bound
        reason no longer matches the current one for its type."""
        stale = super().stale_resolved_cost_owner_ids(rows)
        current: dict[int, str] | None = None
        for row in rows:
            owner_id = _safe_int(row.get(self.owner_id_field))
            type_id = _safe_int(row.get("type_id"))
            if owner_id is None or owner_id in stale or type_id is None:
                continue
            for entry in row.get("allocation_details") or []:
                if not isinstance(entry, dict) or not entry.get("cost_unknown_reason"):
                    continue
                if not _is_blended_transfer_lot(entry.get("source"), entry.get("reference_type")):
                    continue
                if current is None:
                    current = self._blended_cost_unknown_reason_by_type_id()
                if current.get(int(type_id)) != entry.get("cost_unknown_reason"):
                    stale.add(int(owner_id))
                    break
        return stale

    def _merge_character_lots(self, asset_history_lots: dict[int, list[FifoLot]]) -> dict[int, int]:
        """Char-job and char-buy lots cover char-built items sold before the transfer was
        captured in asset history (Manufacturing instead of opening_inventory).

        Per character, each lot enters only what that character's own ledger replay left of
        it: its jobs' inputs and its personal sales never reached the corp. A stamped
        transfer lot from the character's job or buy is the character's units arriving in
        the corp hangar: the stamp gives the quantity, the character's FIFO residual (oldest
        first) the identity and cost, since the backfill's job is only a guess at which
        build the stack came from. The rest of a stamped lot's residual stayed with the
        character; other residuals stay in the queue. A placed residual keeps the stamp's
        date (its place in the corp queue). Stamped units beyond the character's residuals
        are kept as stamped and returned per type."""
        character_service = CharacterRealizedProfitLedgerService(
            app_session=self._app_session, sde_session=self._sde_session,
            market_prices=self._market_prices, admin_settings=self._admin_settings)
        character_service._stamp_rows = getattr(self, "_stamp_rows", None)
        uncancelled: dict[int, int] = defaultdict(int)
        for character_id, lots_by_type in self._load_character_source_lots().items():
            replay = character_service._replay_owner(owner_id=int(character_id))
            taken: dict[tuple[int, Any, int], int] = defaultdict(int, replay["inputs"]["draws"])
            for tx, allocation in replay["sales"]:
                for entry in allocation["allocations"]:
                    if entry["reference_id"] is not None:
                        taken[(int(tx.type_id), entry["reference_type"], int(entry["reference_id"]))] += int(entry["quantity"])
            transfers = replay["inputs"]["transfers"]
            for key, quantity in replay["inputs"]["transfer_draws"].items():
                taken[key] += int(quantity)
            for type_id, lots in lots_by_type.items():
                type_lots = asset_history_lots.setdefault(int(type_id), [])
                source_by_key = {(lot.reference_type, lot.reference_id): lot for lot in lots}
                stamps = [lot for lot in type_lots if (lot.reference_type, lot.reference_id) in source_by_key]
                stamped_keys = {(lot.reference_type, lot.reference_id) for lot in stamps}
                stamp_ids = {id(lot) for lot in stamps}
                type_lots[:] = [lot for lot in type_lots if id(lot) not in stamp_ids]
                # Each stamp's units are the units the character's own FIFO moved out for it,
                # at the stamp's place in the corp queue.
                for stamp in stamps:
                    for key, quantity in transfers.get(stamp.transfer_stamp_id, [(None, int(stamp.quantity))]):
                        source_lot = source_by_key.get(key) if key is not None else None
                        if source_lot is None:
                            if key is None:
                                uncancelled[int(type_id)] += int(quantity)
                            type_lots.append(replace(stamp, quantity=int(quantity)))
                            continue
                        type_lots.append(replace(source_lot, quantity=int(quantity), acquisition_date=stamp.acquisition_date))
                for lot in lots:
                    key = (lot.reference_type, lot.reference_id)
                    left = int(lot.quantity) - taken.get((int(type_id), lot.reference_type, int(lot.reference_id or 0)), 0)
                    if left > 0 and key not in stamped_keys:
                        type_lots.append(replace(lot, quantity=left))
        return dict(uncancelled)

    def _load_owner_context(self, *, owner_id: int) -> dict[str, Any]:
        """Pre-load journal entries and pre-compute per-unit broker fees for corp sells.

        CorporationWalletTransactionsModel has no journal_ref_id, so broker fees and
        transaction taxes cannot be matched by reference ID.

        Broker fees: a corp market order's broker_fee journal entry covers ALL units in
        that order as a single lump sum paid at order creation time. We pre-compute a
        per-unit allocation by grouping sell transactions of the same type_id and price
        that cluster chronologically after the fee entry, then splitting the fee equally
        across the cluster. This prevents the lump sum from being attributed entirely to
        the first unit that sells.

        Transaction tax: matched by context_id when available, with a narrow date-proximity
        fallback for entries where context_id is NULL (common for corp journal entries).
        """
        def _query_by_ref_type(ref_type: str) -> list[Any]:
            return (
                self._app_session.query(CorporationWalletJournalModel)
                .filter(
                    CorporationWalletJournalModel.corporation_id == int(owner_id),
                    CorporationWalletJournalModel.ref_type == ref_type,
                )
                .order_by(CorporationWalletJournalModel.date)
                .all()
            )

        brokers_fee_entries = _query_by_ref_type("brokers_fee")
        transaction_tax_entries = _query_by_ref_type("transaction_tax")

        sell_transactions = (
            self._app_session.query(CorporationWalletTransactionsModel)
            .filter(
                CorporationWalletTransactionsModel.corporation_id == int(owner_id),
                CorporationWalletTransactionsModel.is_buy.is_(False),
            )
            .all()
        )

        per_unit_broker_fees, pre_consumed_journal_ids = self._precompute_per_unit_broker_fees(
            sell_transactions=sell_transactions,
            brokers_fee_entries=brokers_fee_entries,
        )

        return {
            "brokers_fee_entries": brokers_fee_entries,
            "transaction_tax_entries": transaction_tax_entries,
            "per_unit_broker_fees": per_unit_broker_fees,
            # Initialise used_journal_ids with entries already consumed by pre-computation
            # so the date-proximity fallback cannot re-match them for other sells.
            "used_journal_ids": set(pre_consumed_journal_ids),
        }

    @staticmethod
    def _precompute_per_unit_broker_fees(
        sell_transactions: list[Any],
        brokers_fee_entries: list[Any],
    ) -> tuple[dict[int, float], set[int]]:
        """Proportionally split corp market order broker fees across individual sell transactions.

        Returns (per_unit_broker_fee_by_tx_id, consumed_journal_ids).

        For each broker_fee journal entry (processed oldest-first):
        1. Find unmatched sell transactions that occurred AFTER the fee within
           _CORP_BROKER_ATTRIBUTION_WINDOW_SECONDS.
        2. Group those sells by (type_id, unit_price).
        3. Within each group, take the first cluster of consecutive sells separated by
           at most _CORP_BROKER_CLUSTER_GAP_SECONDS (longer gaps indicate a new order).
        4. If fee_amount / (cluster units × unit_price) falls in a plausible broker-rate
           range (0.3%–25%), split fee_amount over the cluster by each sell's share of its value.
        5. Try groups in order of earliest first sell (the order that started filling
           soonest after the fee was paid is the most likely match).
        """
        if not brokers_fee_entries or not sell_transactions:
            return {}, set()

        _MIN_RATE = 0.003
        _MAX_RATE = 0.25

        # Index sell transactions
        sell_data: list[tuple[datetime, int, int, float, int]] = []
        for tx in sell_transactions:
            if getattr(tx, "is_buy", None):
                continue
            tx_id = _safe_int(getattr(tx, "transaction_id", None))
            type_id = _safe_int(getattr(tx, "type_id", None))
            unit_price = _safe_float(getattr(tx, "unit_price", None))
            quantity = _safe_int(getattr(tx, "quantity", None))
            date = _parse_date(getattr(tx, "date", None))
            if tx_id is None or type_id is None or not unit_price or unit_price <= 0 or date is None:
                continue
            if not quantity or quantity <= 0:
                continue
            sell_data.append((date, int(tx_id), int(type_id), float(unit_price), int(quantity)))
        sell_data.sort(key=lambda s: s[0])

        matched_tx_ids: set[int] = set()
        consumed_journal_ids: set[int] = set()
        per_unit_broker_fee: dict[int, float] = {}

        fee_entries_sorted = sorted(
            brokers_fee_entries,
            key=lambda e: (_parse_date(getattr(e, "date", None)) or datetime.min),
        )

        for fee_entry in fee_entries_sorted:
            fee_amount = abs(float(_safe_float(getattr(fee_entry, "amount", None)) or 0.0))
            journal_id = _safe_int(getattr(fee_entry, "wallet_journal_id", None))
            if fee_amount <= 0:
                continue
            fee_date = _parse_date(getattr(fee_entry, "date", None))
            if fee_date is None:
                continue
            fee_date_naive = fee_date.replace(tzinfo=None)

            # Unmatched sells that occurred after this fee within the attribution window
            candidates: list[tuple[datetime, int, int, float, int]] = [
                s for s in sell_data
                if s[1] not in matched_tx_ids
                and 0 < (s[0].replace(tzinfo=None) - fee_date_naive).total_seconds()
                <= _CORP_BROKER_ATTRIBUTION_WINDOW_SECONDS
            ]
            if not candidates:
                continue

            # Group by (type_id, rounded unit_price) — round to nearest 10 ISK for float safety
            groups: dict[tuple[int, int], list[tuple[datetime, int, int, float, int]]] = defaultdict(list)
            for s in candidates:
                key = (s[2], int(round(s[3] / 10) * 10))
                groups[key].append(s)

            # Iterate groups ordered by earliest first sell (most temporally proximate to fee)
            group_items = sorted(
                groups.items(),
                key=lambda kv: min(s[0] for s in kv[1]),
            )

            best_fee_by_tx_id: dict[int, float] | None = None

            for (_type_id, _rounded_price), group in group_items:
                sorted_group = sorted(group, key=lambda s: s[0])
                # Build first cluster of consecutive fills separated by ≤ cluster gap
                cluster: list[tuple] = [sorted_group[0]]
                for i in range(1, len(sorted_group)):
                    gap = (
                        sorted_group[i][0].replace(tzinfo=None)
                        - sorted_group[i - 1][0].replace(tzinfo=None)
                    ).total_seconds()
                    if gap <= _CORP_BROKER_CLUSTER_GAP_SECONDS:
                        cluster.append(sorted_group[i])
                    else:
                        break

                cluster_value = sum(s[3] * s[4] for s in cluster)
                if cluster_value <= 0:
                    continue
                implied_rate = fee_amount / cluster_value
                if _MIN_RATE <= implied_rate <= _MAX_RATE:
                    best_fee_by_tx_id = {s[1]: fee_amount * s[3] * s[4] / cluster_value for s in cluster}
                    break  # Take first valid group (earliest sells after the fee)

            if best_fee_by_tx_id is not None:
                per_unit_broker_fee.update(best_fee_by_tx_id)
                matched_tx_ids.update(best_fee_by_tx_id)
                if journal_id is not None:
                    consumed_journal_ids.add(int(journal_id))

        return per_unit_broker_fee, consumed_journal_ids

    def _fee_breakdown(self, *, gross_revenue: float, journal: Any, owner_context: dict[str, Any]) -> tuple[float, float, float, str, list[str]]:
        tx = owner_context.get("current_tx")
        sell_date = _parse_eve_date(getattr(tx, "date", None)) if tx is not None else None
        sell_transaction_id = _safe_int(getattr(tx, "transaction_id", None)) if tx is not None else None
        sell_division = getattr(tx, "division", None) if tx is not None else None

        # Retrieve the shared used_journal_ids set — must NOT use `or set()` since an
        # empty set is falsy, which would create a new local set and break cross-call dedup.
        used_journal_ids = owner_context.get("used_journal_ids")
        if not isinstance(used_journal_ids, set):
            used_journal_ids = set()
            owner_context["used_journal_ids"] = used_journal_ids

        per_unit_broker_fees: dict[int, float] = owner_context.get("per_unit_broker_fees") or {}
        pre_computed_broker_fee = per_unit_broker_fees.get(int(sell_transaction_id)) if sell_transaction_id is not None else None

        return _corp_journal_fee_breakdown(
            gross_revenue=float(gross_revenue),
            sell_date=sell_date,
            sell_transaction_id=sell_transaction_id,
            sell_division=sell_division,
            brokers_fee_entries=owner_context.get("brokers_fee_entries") or [],
            transaction_tax_entries=owner_context.get("transaction_tax_entries") or [],
            used_journal_ids=used_journal_ids,
            pre_computed_broker_fee=pre_computed_broker_fee,
        )


_SERVICE_BY_SCOPE: dict[str, type[_BaseRealizedProfitLedgerService]] = {
    "character": CharacterRealizedProfitLedgerService,
    "corporation": CorporationRealizedProfitLedgerService,
}


def summarize_realized_profit_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    realized_rows = [row for row in rows if row.get("realized_profit") is not None]
    total_realized_profit = sum(float(row.get("realized_profit") or 0.0) for row in realized_rows)
    total_net_revenue = sum(float(row.get("net_revenue") or 0.0) for row in rows)
    total_allocated_cost = sum(float(row.get("allocated_cost") or 0.0) for row in rows)
    total_fees = sum(float(row.get("total_fees_amount") or 0.0) for row in rows)
    fully_priced_count = sum(1 for row in rows if int(row.get("unpriced_quantity") or 0) == 0)
    return {
        "row_count": len(rows),
        "realized_row_count": len(realized_rows),
        "fully_priced_count": fully_priced_count,
        "coverage_fraction": (float(fully_priced_count) / float(len(rows))) if rows else 0.0,
        "total_net_revenue": float(total_net_revenue),
        "total_allocated_cost": float(total_allocated_cost),
        "total_fees": float(total_fees),
        "total_realized_profit": float(total_realized_profit),
    }