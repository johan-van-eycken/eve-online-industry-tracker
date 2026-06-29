from __future__ import annotations

from datetime import datetime
import json
from typing import Any

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
    CorporationIndustryJobsModel,
    CorporationModel,
    CorporationRealizedSalesLedgerModel,
    CorporationWalletJournalModel,
    CorporationWalletTransactionsModel,
)


ASSET_SOURCE_UNTRACKED = "untracked_inventory"
ASSET_SOURCE_OPENING_INVENTORY = "opening_inventory"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
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
    except Exception:
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


def _consume_lots(lots: list[FifoLot], *, quantity: int) -> dict[str, Any]:
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


def _parse_eve_date(date_str: Any) -> datetime | None:
    if date_str is None:
        return None
    if isinstance(date_str, datetime):
        return date_str
    try:
        return datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
    except Exception:
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
) -> tuple[float, float, float, str, list[str]]:
    """Match corp sell transaction to its brokers_fee and transaction_tax journal entries.

    brokers_fee is matched by date proximity within ±_CORP_FEE_MATCH_WINDOW_SECONDS.
    transaction_tax is matched by exact context_id == sell_transaction_id when available,
    falling back to date proximity.
    Each journal entry is consumed once (via used_journal_ids) to prevent double-matching.
    """
    notes: list[str] = []
    gross = float(gross_revenue)

    def _find_nearest(
        entries: list[Any],
        use_context_id: bool = False,
        tid: int | None = None,
        division: int | None = None,
    ) -> Any | None:
        for entry in entries:
            if entry.wallet_journal_id in used_journal_ids:
                continue
            # Division filter: skip cross-division entries when both sides specify a division
            if division is not None and getattr(entry, "division", None) is not None:
                if entry.division != division:
                    continue
            if use_context_id:
                # Exact match via context_id — no date fallback
                if tid is not None and getattr(entry, "context_id", None) == tid:
                    return entry
            else:
                # Date-proximity match
                entry_date = _parse_eve_date(entry.date)
                if entry_date and sell_date:
                    if abs((entry_date - sell_date).total_seconds()) <= _CORP_FEE_MATCH_WINDOW_SECONDS:
                        return entry
        return None

    broker_entry = _find_nearest(brokers_fee_entries, division=sell_division)
    tax_entry = _find_nearest(transaction_tax_entries, use_context_id=True, tid=sell_transaction_id, division=sell_division)

    broker_fee = 0.0
    tax_fee = 0.0

    if broker_entry is not None:
        eid = _safe_int(getattr(broker_entry, "wallet_journal_id", None))
        if eid is not None:
            used_journal_ids.add(int(eid))
        broker_fee = abs(float(_safe_float(getattr(broker_entry, "amount", None)) or 0.0))

    if tax_entry is not None:
        eid = _safe_int(getattr(tax_entry, "wallet_journal_id", None))
        if eid is not None:
            used_journal_ids.add(int(eid))
        tax_fee = abs(float(_safe_float(getattr(tax_entry, "amount", None)) or 0.0))

    if broker_entry is not None and tax_entry is not None:
        mode = "journal_matched"
    elif broker_entry is not None or tax_entry is not None:
        mode = "journal_partial"
        notes.append("Only one of broker fee or transaction tax was matched from the wallet journal.")
    else:
        notes.append("No matching wallet journal fee entries found for this corp sale; fees are not captured.")
        return 0.0, 0.0, gross, "gross_only", notes

    net_revenue = max(0.0, gross - broker_fee - tax_fee)
    return broker_fee, tax_fee, net_revenue, mode, notes


def _extract_market_fee_rates(raw_market_fees: Any) -> tuple[float | None, float | None]:
    payload = raw_market_fees
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = None
    if not isinstance(payload, dict):
        return None, None
    jita = payload.get("jita_4_4") or {}
    if not isinstance(jita, dict):
        return None, None
    rates = jita.get("rates") or {}
    if not isinstance(rates, dict):
        return None, None
    return _safe_float(rates.get("sales_tax_fraction")), _safe_float(rates.get("broker_fee_fraction"))


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
) -> tuple[float, float, float, str, list[str]]:
    normalized_notes = list(notes)
    normalized_sales_tax = float(max(0.0, sales_tax_amount))
    normalized_other_fees = float(max(0.0, other_fees_amount))
    normalized_mode = str(fee_capture_mode or "gross_only")

    if journal is None and sales_tax_fraction is not None and normalized_sales_tax <= 0.0:
        normalized_sales_tax = max(0.0, float(gross_revenue) * float(sales_tax_fraction))
        normalized_notes.append("Sales tax was estimated from the character market fee profile.")
        normalized_mode = "estimated_market_fees"

    if broker_fee_fraction is not None and broker_fee_fraction > 0.0:
        estimated_broker_fee = max(0.0, float(gross_revenue) * float(broker_fee_fraction))
        if estimated_broker_fee > 0.0:
            normalized_other_fees += estimated_broker_fee
            normalized_notes.append(
                "Broker / order update fees were estimated from the character market fee profile; exact relist history is not stored."
            )
            if normalized_mode == "journal_amount":
                normalized_mode = "journal_plus_estimated_broker_fee"
            elif normalized_mode != "estimated_market_fees":
                normalized_mode = "estimated_market_fees"

    normalized_net_revenue = max(0.0, float(gross_revenue) - normalized_sales_tax - normalized_other_fees)
    return normalized_other_fees, normalized_sales_tax, normalized_net_revenue, normalized_mode, normalized_notes


def _confidence(*, priced_quantity: int, unpriced_quantity: int, fee_capture_mode: str) -> str:
    if unpriced_quantity > 0:
        return "Low"
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

    def __init__(self, *, app_session: Any, sde_session: Any, market_prices: list[dict[str, Any]] | None = None):
        self._app_session = app_session
        self._sde_session = sde_session
        self._market_prices = market_prices or []

    def rebuild(self, *, owner_id: int | None = None) -> list[dict[str, Any]]:
        if owner_id is not None:
            owner_ids = [int(owner_id)]
        else:
            owner_ids = [int(getattr(row, self.owner_id_field)) for row in self._app_session.query(self.owner_model).all()]

        persisted_rows: list[Any] = []
        for current_owner_id in owner_ids:
            persisted_rows.extend(self._rebuild_owner(owner_id=int(current_owner_id)))

        self._app_session.commit()
        return [self._serialize_row(row) for row in persisted_rows]

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
        journal_by_id = self._load_journal_map(owner_id=int(owner_id))
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
                allocation = _consume_lots(lots, quantity=int(quantity))
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

    def _load_owner_context(self, *, owner_id: int) -> dict[str, Any]:
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
        }
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

    def _load_wallet_transactions(self, *, owner_id: int) -> list[Any]:
        return (
            self._app_session.query(self.transaction_model)
            .filter(self.transaction_model.character_id == int(owner_id))
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
        sales_tax_fraction, broker_fee_fraction = _extract_market_fee_rates(getattr(character, "market_fees", None) if character is not None else None)
        return {
            "sales_tax_fraction": sales_tax_fraction,
            "broker_fee_fraction": broker_fee_fraction,
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

    def _load_owner_context(self, *, owner_id: int) -> dict[str, Any]:
        """Pre-load brokers_fee and transaction_tax journal entries for date-proximity matching.

        CorporationWalletTransactionsModel has no journal_ref_id, so we match fee entries
        to sell transactions by date proximity (within _CORP_FEE_MATCH_WINDOW_SECONDS).
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

        return {
            "brokers_fee_entries": _query_by_ref_type("brokers_fee"),
            "transaction_tax_entries": _query_by_ref_type("transaction_tax"),
            "used_journal_ids": set(),
        }

    def _fee_breakdown(self, *, gross_revenue: float, journal: Any, owner_context: dict[str, Any]) -> tuple[float, float, float, str, list[str]]:
        tx = owner_context.get("current_tx")
        sell_date = _parse_eve_date(getattr(tx, "date", None)) if tx is not None else None
        sell_transaction_id = getattr(tx, "transaction_id", None) if tx is not None else None
        sell_division = getattr(tx, "division", None) if tx is not None else None
        return _corp_journal_fee_breakdown(
            gross_revenue=float(gross_revenue),
            sell_date=sell_date,
            sell_transaction_id=sell_transaction_id,
            sell_division=sell_division,
            brokers_fee_entries=owner_context.get("brokers_fee_entries") or [],
            transaction_tax_entries=owner_context.get("transaction_tax_entries") or [],
            used_journal_ids=owner_context.get("used_journal_ids") or set(),
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