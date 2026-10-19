from __future__ import annotations

import json
import threading
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from typing import Any

from eve_online_industry_tracker.application import market_fees
from eve_online_industry_tracker.application.characters.asset_provenance import (
    ASSET_SOURCE_INDUSTRY_BUILD,
    ASSET_SOURCE_MARKET_BUY,
    FifoLot,
    resolve_industry_job_cost_snapshot,
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


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (OverflowError, TypeError, ValueError):
        return None


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


def _get_mfg_activity(activities: Any) -> dict[str, Any] | None:
    if not isinstance(activities, dict):
        return None
    manufacturing = activities.get("manufacturing")
    return manufacturing if isinstance(manufacturing, dict) else None


def _output_quantity_per_run(*, sde_session: Any, blueprint_type_id: int, product_type_id: int) -> int | None:
    blueprint = sde_session.query(Blueprints).filter_by(blueprintTypeID=int(blueprint_type_id)).first()
    if blueprint is None:
        return None
    manufacturing = _get_mfg_activity(getattr(blueprint, "activities", None))
    if not manufacturing:
        return None
    products = manufacturing.get("products") or []
    if not isinstance(products, list) or not products:
        return None
    for product in products:
        if not isinstance(product, dict):
            continue
        if _safe_int(product.get("typeID")) == int(product_type_id):
            quantity = _safe_int(product.get("quantity"))
            return quantity if quantity and quantity > 0 else None
    first = products[0]
    if not isinstance(first, dict):
        return None
    quantity = _safe_int(first.get("quantity"))
    return quantity if quantity and quantity > 0 else None


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
            }
        )
        remaining -= int(take)
        priced_quantity += int(take)
        total_cost += float(chunk_cost)

        new_quantity = lot_quantity - int(take)
        if new_quantity > 0:
            lots[0] = FifoLot(
                quantity=int(new_quantity),
                unit_price=float(head.unit_price),
                acquisition_date=head.acquisition_date,
                reference_id=head.reference_id,
                reference_type=head.reference_type,
                source=head.source,
                cost_unknown_reason=head.cost_unknown_reason,
            )
        else:
            lots.pop(0)

    return {
        "total_cost": float(total_cost),
        "priced_quantity": int(priced_quantity),
        "unpriced_quantity": int(remaining),
        "by_source": by_source,
        "allocations": allocations,
    }


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

    if normalized.get("realized_profit") is None and net_revenue is not None:
        normalized["realized_profit"] = float(net_revenue) - float(allocated_cost)
        if float(net_revenue) > 0:
            normalized["realized_margin_fraction"] = float(normalized["realized_profit"]) / float(net_revenue)

    return normalized


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
        return stale

    def list_rows(self, *, owner_id: int | None = None) -> list[dict[str, Any]]:
        query = self._app_session.query(self.ledger_model)
        if owner_id is not None:
            query = query.filter(getattr(self.ledger_model, self.owner_id_field) == int(owner_id))
        rows = query.order_by(self.ledger_model.date.desc(), self.ledger_model.transaction_id.desc()).all()
        return [_normalize_untracked_payload(self._serialize_row(row)) for row in rows]

    def _rebuild_owner(self, *, owner_id: int) -> list[Any]:
        self._app_session.query(self.ledger_model).filter(getattr(self.ledger_model, self.owner_id_field) == int(owner_id)).delete()

        wallet_transactions = self._load_wallet_transactions(owner_id=int(owner_id))
        industry_jobs = self._load_industry_jobs(owner_id=int(owner_id))
        asset_history_lots = self._load_asset_history_lots(owner_id=int(owner_id))

        # Always load char-job and char-buy lots regardless of whether an asset history lot
        # exists for the same type_id. FIFO attribution only charges costs to actually-sold
        # units; extra lots that aren't consumed simply remain unused in the queue, so there
        # is no double-counting risk. This allows char-built items sold before the corp job
        # was captured in asset history to be correctly attributed as Manufacturing instead
        # of falling through to opening_inventory (Trade).
        supplemental_lots = self._load_supplemental_lots(owner_id=int(owner_id))

        # Merge supplemental lots into asset_history_lots (may overlap on type_id when both
        # a corp asset history row and char-job lots exist for the same product type)
        for supp_type_id, supp_lots in supplemental_lots.items():
            asset_history_lots.setdefault(int(supp_type_id), []).extend(supp_lots)

        journal_by_id = self._load_journal_map(owner_id=int(owner_id))
        cost_unknown_reason_by_job_id = self._load_cost_unknown_reason_by_job_id()
        owner_context = self._load_owner_context(owner_id=int(owner_id))

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

        completed_statuses = {"delivered", "ready", "completed"}
        market_price_map = {
            int(row.get("type_id")): float(row.get("average_price") or row.get("adjusted_price"))
            for row in self._market_prices
            if isinstance(row, dict)
            and _safe_int(row.get("type_id")) is not None
            and _safe_float(row.get("average_price") or row.get("adjusted_price")) is not None
        }
        for job in industry_jobs:
            status = str(getattr(job, "status", "") or "").strip().lower()
            if status and status not in completed_statuses:
                continue
            product_type_id = _safe_int(getattr(job, "product_type_id", None))
            blueprint_type_id = _safe_int(getattr(job, "blueprint_type_id", None))
            if not product_type_id or not blueprint_type_id:
                continue
            completed_date = getattr(job, "completed_date", None) or getattr(job, "end_date", None)
            if not completed_date:
                continue
            snapshot = resolve_industry_job_cost_snapshot(
                job=job,
                sde_session=self._sde_session,
                market_price_map=market_price_map,
            )
            quantity = _safe_int(snapshot.get("output_quantity")) or 0
            unit_cost = _safe_float(snapshot.get("unit_cost"))
            if unit_cost is None or unit_cost <= 0:
                continue
            if quantity <= 0:
                runs = _safe_int(getattr(job, "successful_runs", None)) or _safe_int(getattr(job, "runs", None)) or 1
                quantity_per_run = _output_quantity_per_run(
                    sde_session=self._sde_session,
                    blueprint_type_id=int(blueprint_type_id),
                    product_type_id=int(product_type_id),
                )
                if not quantity_per_run or quantity_per_run <= 0:
                    continue
                quantity = int(quantity_per_run) * int(runs)
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

        persisted_rows: list[Any] = []
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

                if kind != "sell":
                    continue

                tx = event.get("tx")
                quantity = _safe_int(getattr(tx, "quantity", None)) or 0
                if quantity <= 0:
                    continue
                gross_revenue = _safe_float(getattr(tx, "total_price", None))
                if gross_revenue is None:
                    unit_price = _safe_float(getattr(tx, "unit_price", None)) or 0.0
                    gross_revenue = float(unit_price) * float(quantity)
                allocation = _consume_lots(
                    lots, quantity=int(quantity), cost_unknown_reason_by_job_id=cost_unknown_reason_by_job_id,
                )
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
                    notes.append(f"{allocation['unpriced_quantity']} unit(s) could not be matched to a historical cost basis.")
                realized_profit = None
                realized_margin_fraction = None
                if allocation["unpriced_quantity"] == 0 or int(allocation["priced_quantity"]) == 0:
                    realized_profit = float(net_revenue) - float(allocation["total_cost"])
                    if net_revenue > 0:
                        realized_margin_fraction = float(realized_profit) / float(net_revenue)

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
        can consume a lot a character built."""
        out: dict[int, str] = {}
        for model in (CharacterIndustryJobsModel, CorporationIndustryJobsModel):
            rows = (
                self._app_session.query(model.job_id, model.invention_cost_unknown_reason)
                .filter(model.invention_cost_unknown_reason.isnot(None))
                .all()
            )
            for job_id, reason in rows:
                if job_id is not None and reason:
                    out[int(job_id)] = str(reason)
        return out

    def _load_asset_history_lots(self, *, owner_id: int) -> dict[int, list[FifoLot]]:
        """Return FifoLots sourced from asset history, keyed by type_id. Subclasses may override."""
        return {}

    def _load_supplemental_lots(self, *, owner_id: int) -> dict[int, list[FifoLot]]:
        """Return additional FifoLots to merge into the FIFO queue. Subclasses may override."""
        return {}

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

    def _load_character_source_lots(self, corporation_id: int) -> dict[int, list[FifoLot]]:
        """Return FifoLots from character industry jobs and character market buys.

        These represent items manufactured or purchased by characters, then transferred to the
        corporation. All lots are loaded unconditionally; FIFO only charges costs to actually-sold
        units, so surplus lots simply remain unused in the queue with no double-counting risk.
        """
        lots_by_type: dict[int, list[FifoLot]] = {}
        completed_statuses = {"delivered", "ready", "completed"}

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
            lots_by_type.setdefault(int(product_type_id), []).append(
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
            lots_by_type.setdefault(int(type_id), []).append(
                FifoLot(
                    quantity=int(quantity),
                    unit_price=float(unit_price),
                    acquisition_date=getattr(tx, "date", None),
                    reference_id=_safe_int(getattr(tx, "transaction_id", None)),
                    reference_type="wallet_transaction",
                    source="character_market_buy_transferred",
                )
            )

        # Sort each type's lots oldest-first
        for type_id_key in lots_by_type:
            lots_by_type[type_id_key].sort(
                key=lambda lot: (_parse_date(lot.acquisition_date) or datetime.min)
            )

        return lots_by_type

    def _load_asset_history_lots(self, *, owner_id: int) -> dict[int, list[FifoLot]]:
        """Load FifoLots from CorporationAssetHistoryModel rows that have a stamped acquisition_unit_cost.

        These represent finished goods transferred from characters who built them, backfilled
        by scripts/backfill_corp_transfer_costs.py with acquisition_source values such as
        'industry_build_transferred', 'industry_build_transferred_avg', and
        'character_market_buy_transferred'.
        """
        rows = (
            self._app_session.query(CorporationAssetHistoryModel)
            .filter(
                CorporationAssetHistoryModel.corporation_id == int(owner_id),
                CorporationAssetHistoryModel.acquisition_unit_cost.isnot(None),
            )
            .all()
        )
        lots_by_type: dict[int, list[FifoLot]] = {}
        for row in rows:
            type_id = _safe_int(getattr(row, "type_id", None))
            unit_cost = _safe_float(getattr(row, "acquisition_unit_cost", None))
            quantity = _safe_int(getattr(row, "quantity", None))
            if type_id is None or type_id <= 0 or unit_cost is None or unit_cost <= 0 or not quantity or quantity <= 0:
                continue
            acquisition_date = getattr(row, "acquisition_date", None) or getattr(row, "observed_at", None)
            source = str(getattr(row, "acquisition_source", None) or "asset_history")
            reference_id = _safe_int(getattr(row, "acquisition_reference_id", None))
            reference_type = str(getattr(row, "acquisition_reference_type", None) or "asset_history")
            lots_by_type.setdefault(int(type_id), []).append(
                FifoLot(
                    quantity=int(quantity),
                    unit_price=float(unit_cost),
                    acquisition_date=acquisition_date,
                    reference_id=reference_id,
                    reference_type=reference_type,
                    source=source,
                )
            )
        return lots_by_type

    def _load_supplemental_lots(self, *, owner_id: int) -> dict[int, list[FifoLot]]:
        """Delegate to _load_character_source_lots for the corp service."""
        return self._load_character_source_lots(corporation_id=int(owner_id))

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
        4. If fee_amount / (cluster_size × unit_price) falls in a plausible broker-rate
           range (0.3%–25%), assign fee_amount / cluster_size to each sell in the cluster.
        5. Try groups in order of earliest first sell (the order that started filling
           soonest after the fee was paid is the most likely match).
        """
        if not brokers_fee_entries or not sell_transactions:
            return {}, set()

        _MIN_RATE = 0.003
        _MAX_RATE = 0.25

        # Index sell transactions
        sell_data: list[tuple[datetime, int, int, float]] = []
        for tx in sell_transactions:
            if getattr(tx, "is_buy", None):
                continue
            tx_id = _safe_int(getattr(tx, "transaction_id", None))
            type_id = _safe_int(getattr(tx, "type_id", None))
            unit_price = _safe_float(getattr(tx, "unit_price", None))
            date = _parse_date(getattr(tx, "date", None))
            if tx_id is None or type_id is None or not unit_price or unit_price <= 0 or date is None:
                continue
            sell_data.append((date, int(tx_id), int(type_id), float(unit_price)))
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
            candidates: list[tuple[datetime, int, int, float]] = [
                s for s in sell_data
                if s[1] not in matched_tx_ids
                and 0 < (s[0].replace(tzinfo=None) - fee_date_naive).total_seconds()
                <= _CORP_BROKER_ATTRIBUTION_WINDOW_SECONDS
            ]
            if not candidates:
                continue

            # Group by (type_id, rounded unit_price) — round to nearest 10 ISK for float safety
            groups: dict[tuple[int, int], list[tuple[datetime, int, int, float]]] = defaultdict(list)
            for s in candidates:
                key = (s[2], int(round(s[3] / 10) * 10))
                groups[key].append(s)

            # Iterate groups ordered by earliest first sell (most temporally proximate to fee)
            group_items = sorted(
                groups.items(),
                key=lambda kv: min(s[0] for s in kv[1]),
            )

            best_cluster: list[tuple] | None = None
            best_per_unit: float | None = None

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

                n = len(cluster)
                price = cluster[0][3]
                if price <= 0:
                    continue
                implied_rate = fee_amount / (n * price)
                if _MIN_RATE <= implied_rate <= _MAX_RATE:
                    best_cluster = cluster
                    best_per_unit = fee_amount / n
                    break  # Take first valid group (earliest sells after the fee)

            if best_cluster is not None and best_per_unit is not None:
                for s in best_cluster:
                    per_unit_broker_fee[s[1]] = best_per_unit
                    matched_tx_ids.add(s[1])
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