from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional

from sqlalchemy import desc

from classes.database_models import Blueprints


ASSET_SOURCE_INDUSTRY_BUILD = "industry_build"
ASSET_SOURCE_INDUSTRY_COPY = "industry_copy"
ASSET_SOURCE_INDUSTRY_INVENTION = "industry_invention"
ASSET_SOURCE_MARKET_BUY = "market_buy"
ASSET_SOURCE_UNKNOWN = "unknown"

REFERENCE_TYPE_INDUSTRY_JOB = "industry_job"
REFERENCE_TYPE_WALLET_TRANSACTION = "wallet_transaction"


@dataclass(frozen=True)
class CostInfo:
    source: str
    unit_cost: Optional[float]
    total_cost: Optional[float]
    reference_type: Optional[str]
    reference_id: Optional[int]
    acquisition_date: Optional[str]


@dataclass(frozen=True)
class FifoLot:
    quantity: int
    unit_price: float
    acquisition_date: Optional[str] = None
    reference_id: Optional[int] = None
    reference_type: Optional[str] = None
    source: Optional[str] = None


def _parse_date(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # ESI timestamps are usually like: 2023-01-01T12:34:56Z
    # datetime.fromisoformat doesn't accept 'Z', so normalize.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def build_fifo_remaining_lots_by_type(
    *,
    wallet_transactions: Iterable[Any],
    industry_jobs: Iterable[Any] | None = None,
    sde_session: Any | None = None,
    market_prices: list[dict[str, Any]] | None = None,
    on_hand_quantities_by_type: dict[int, int],
) -> dict[int, list[FifoLot]]:
    """Return remaining FIFO lots per type_id, aligned to current on-hand quantities.

    Reconstructs FIFO inventory lots from wallet transactions:
    - Buys add lots.
    - Sells consume lots FIFO (oldest first).

    Because transaction history can be incomplete, we align the resulting lots
    to the current on-hand quantity:
    - If transaction-derived remaining > on-hand, we drop excess from the *oldest*
      lots (FIFO-consistent: missing consumption removes oldest first).
    - If transaction-derived remaining < on-hand, we leave as-is (the remaining
      quantity has unknown cost basis).
    """

    market_price_map = _build_price_map(market_prices)

    tx_by_type: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for tx in wallet_transactions or []:
        tid = _safe_int(getattr(tx, "type_id", None))
        if not tid or tid <= 0:
            continue

        qty = _safe_int(getattr(tx, "quantity", None))
        if qty is None or qty <= 0:
            continue

        is_buy = getattr(tx, "is_buy", None)
        # If we can't tell, skip.
        if is_buy is None:
            continue

        unit_price = _safe_float(getattr(tx, "unit_price", None))
        # For sells, unit price is irrelevant; for buys, we need it.
        if is_buy is True and (unit_price is None or unit_price <= 0):
            continue

        date_s = getattr(tx, "date", None)
        dt = _parse_date(date_s)
        tx_id = _safe_int(getattr(tx, "transaction_id", None))

        tx_by_type[int(tid)].append(
            {
                "kind": ("buy" if bool(is_buy) else "sell"),
                "quantity": int(qty),
                "unit_price": float(unit_price or 0.0),
                "date": (str(date_s) if date_s is not None else None),
                "dt": dt,
                "sort_id": int(tx_id or 0),
                "reference_id": tx_id,
                "reference_type": REFERENCE_TYPE_WALLET_TRANSACTION,
                "source": ASSET_SOURCE_MARKET_BUY,
            }
        )

    # Add industry-job outputs as FIFO "buy" lots (best-effort).
    # This allows FIFO valuation for items you produced (manufacturing, copying, invention),
    # not only bought.
    if industry_jobs is not None and sde_session is not None:
        completed_statuses = {"delivered", "ready", "completed"}
        qty_per_run_cache: dict[tuple[int, int], Optional[int]] = {}

        # Caches for invention lookups/costing.
        invention_bp_cache: dict[int, dict[str, Any] | None] = {}
        invention_cost_per_attempt_cache: dict[int, float | None] = {}
        invention_output_bp_type_cache: dict[int, int | None] = {}

        def _job_activity_id(job_obj: Any) -> Optional[int]:
            raw = getattr(job_obj, "raw", None)
            if not isinstance(raw, dict):
                return None
            v = raw.get("activity_id")
            if v is None:
                v = raw.get("activityID")
            try:
                return int(v) if v is not None else None
            except Exception:
                return None

        def _get_invention_activity_for_blueprint(input_blueprint_type_id: int) -> dict[str, Any] | None:
            tid = int(input_blueprint_type_id)
            if tid <= 0:
                return None
            cached = invention_bp_cache.get(tid)
            if cached is not None:
                inv = cached.get("invention") if isinstance(cached, dict) else None
                return inv if isinstance(inv, dict) else None

            bp = (
                sde_session.query(Blueprints)
                .filter_by(blueprintTypeID=int(tid))
                .first()
            )
            if bp is None:
                invention_bp_cache[tid] = None
                return None
            activities = getattr(bp, "activities", None)
            if not isinstance(activities, dict):
                invention_bp_cache[tid] = None
                return None
            inv = activities.get("invention")
            if not isinstance(inv, dict):
                invention_bp_cache[tid] = None
                return None
            invention_bp_cache[tid] = {"invention": inv}
            return inv

        def _invention_output_blueprint_type_id(input_blueprint_type_id: int) -> int | None:
            tid = int(input_blueprint_type_id)
            cached = invention_output_bp_type_cache.get(tid)
            if tid in invention_output_bp_type_cache:
                return cached
            inv = _get_invention_activity_for_blueprint(int(tid))
            if not isinstance(inv, dict):
                invention_output_bp_type_cache[tid] = None
                return None
            products = inv.get("products")
            if not isinstance(products, list) or not products:
                invention_output_bp_type_cache[tid] = None
                return None
            out_tid = None
            for p in products:
                if not isinstance(p, dict):
                    continue
                v = p.get("typeID")
                if v is None:
                    v = p.get("type_id")
                try:
                    out_tid = int(v) if v is not None else None
                except Exception:
                    out_tid = None
                if out_tid is not None and out_tid > 0:
                    break
            invention_output_bp_type_cache[tid] = out_tid
            return out_tid

        def _invention_material_cost_per_attempt(input_blueprint_type_id: int) -> float | None:
            tid = int(input_blueprint_type_id)
            if tid in invention_cost_per_attempt_cache:
                return invention_cost_per_attempt_cache.get(tid)
            inv = _get_invention_activity_for_blueprint(int(tid))
            if not isinstance(inv, dict):
                invention_cost_per_attempt_cache[tid] = None
                return None
            mats = inv.get("materials")
            if not isinstance(mats, list) or not mats:
                invention_cost_per_attempt_cache[tid] = 0.0
                return 0.0
            total = 0.0
            any_priced = False
            for m in mats:
                if not isinstance(m, dict):
                    continue
                mtid = m.get("typeID")
                if mtid is None:
                    mtid = m.get("type_id")
                qty = m.get("quantity")
                try:
                    mtid_i = int(mtid) if mtid is not None else 0
                    qty_i = int(qty or 0)
                except Exception:
                    continue
                if mtid_i <= 0 or qty_i <= 0:
                    continue
                unit = market_price_map.get(int(mtid_i)) if isinstance(market_price_map, dict) else None
                if unit is None or float(unit) <= 0:
                    continue
                any_priced = True
                total += float(qty_i) * float(unit)
            out = float(total) if any_priced else None
            invention_cost_per_attempt_cache[tid] = out
            return out

        def _output_qty_per_run(blueprint_type_id: int, product_type_id: int) -> Optional[int]:
            key = (int(blueprint_type_id), int(product_type_id))
            if key in qty_per_run_cache:
                return qty_per_run_cache[key]
            bp = (
                sde_session.query(Blueprints)
                .filter_by(blueprintTypeID=int(blueprint_type_id))
                .first()
            )
            if bp is None:
                qty_per_run_cache[key] = None
                return None
            mfg = _get_mfg_activity(getattr(bp, "activities", None))
            if not mfg:
                qty_per_run_cache[key] = None
                return None
            products = mfg.get("products")
            if not isinstance(products, list) or not products:
                qty_per_run_cache[key] = None
                return None
            out_q: Optional[int] = None
            for p in products:
                if not isinstance(p, dict):
                    continue
                if _safe_int(p.get("typeID")) == int(product_type_id):
                    out_q = _safe_int(p.get("quantity"))
                    break
            if out_q is None and products:
                out_q = _safe_int(products[0].get("quantity")) if isinstance(products[0], dict) else None
            if not out_q or out_q <= 0:
                qty_per_run_cache[key] = None
                return None
            qty_per_run_cache[key] = int(out_q)
            return int(out_q)

        for job in industry_jobs or []:
            status = str(getattr(job, "status", "") or "").lower()
            completed_date = getattr(job, "completed_date", None) or getattr(job, "end_date", None)
            if status and status not in completed_statuses:
                continue
            if completed_date is None:
                continue

            raw = getattr(job, "raw", None)
            act_id = _job_activity_id(job)

            # ESI activity IDs (common): 1=manufacturing, 5=copying, 8=invention.
            ACT_MFG = 1
            ACT_COPY = 5
            ACT_INV = 8

            blueprint_type_id = _safe_int(getattr(job, "blueprint_type_id", None))
            product_type_id = _safe_int(getattr(job, "product_type_id", None))

            job_cost_total = _safe_float(getattr(job, "cost", None))
            job_id = _safe_int(getattr(job, "job_id", None))
            date_s = completed_date
            dt = _parse_date(date_s)

            # Best-effort: infer manufacturing when activity_id is missing.
            if act_id is None:
                try:
                    if product_type_id and blueprint_type_id and int(product_type_id) != int(blueprint_type_id):
                        act_id = ACT_MFG
                except Exception:
                    act_id = None

            # Manufacturing job outputs: value as built inventory.
            if act_id == ACT_MFG:
                if not product_type_id or not blueprint_type_id:
                    continue
                runs = _safe_int(getattr(job, "successful_runs", None)) or _safe_int(getattr(job, "runs", None)) or 1
                runs = max(1, int(runs))
                qpr = _output_qty_per_run(int(blueprint_type_id), int(product_type_id))
                if qpr is None:
                    continue
                lot_qty = int(qpr) * int(runs)
                if lot_qty <= 0:
                    continue
                unit_cost = estimate_industry_job_unit_cost(
                    sde_session=sde_session,
                    blueprint_type_id=int(blueprint_type_id),
                    product_type_id=int(product_type_id),
                    runs=int(runs),
                    job_cost=job_cost_total,
                    market_price_map=(market_price_map or {}),
                )
                if unit_cost is None or unit_cost <= 0:
                    continue

                tx_by_type[int(product_type_id)].append(
                    {
                        "kind": "buy",
                        "quantity": int(lot_qty),
                        "unit_price": float(unit_cost),
                        "date": (str(date_s) if date_s is not None else None),
                        "dt": dt,
                        "sort_id": int(job_id or 0),
                        "reference_id": job_id,
                        "reference_type": REFERENCE_TYPE_INDUSTRY_JOB,
                        "source": ASSET_SOURCE_INDUSTRY_BUILD,
                    }
                )
                continue

            # Copying job outputs: blueprint copies (BPCs) are assets with the blueprint type_id.
            if act_id == ACT_COPY:
                if not blueprint_type_id:
                    continue
                copies = None
                try:
                    copies = _safe_int(getattr(job, "runs", None))
                except Exception:
                    copies = None
                if copies is None and isinstance(raw, dict):
                    try:
                        copies = int(raw.get("runs") or 0)
                    except Exception:
                        copies = None
                copies_i = max(0, int(copies or 0))
                if copies_i <= 0:
                    continue

                unit_cost = None
                if job_cost_total is not None and float(job_cost_total) > 0:
                    unit_cost = float(job_cost_total) / float(copies_i)
                else:
                    # Fallback: if we can't price via job cost, use market as a proxy.
                    try:
                        unit_cost = float((market_price_map or {}).get(int(blueprint_type_id)) or 0.0)
                    except Exception:
                        unit_cost = None

                if unit_cost is None or float(unit_cost) <= 0:
                    continue

                tx_by_type[int(blueprint_type_id)].append(
                    {
                        "kind": "buy",
                        "quantity": int(copies_i),
                        "unit_price": float(unit_cost),
                        "date": (str(date_s) if date_s is not None else None),
                        "dt": dt,
                        "sort_id": int(job_id or 0),
                        "reference_id": job_id,
                        "reference_type": REFERENCE_TYPE_INDUSTRY_JOB,
                        "source": ASSET_SOURCE_INDUSTRY_COPY,
                    }
                )
                continue

            # Invention job outputs: invented blueprint copies (BPCs).
            if act_id == ACT_INV:
                # Determine output blueprint type ID (prefer ESI product_type_id, else SDE invention products).
                out_bp_type_id = None
                if product_type_id is not None and int(product_type_id) > 0:
                    out_bp_type_id = int(product_type_id)
                elif blueprint_type_id is not None and int(blueprint_type_id) > 0:
                    out_bp_type_id = _invention_output_blueprint_type_id(int(blueprint_type_id))
                if out_bp_type_id is None or int(out_bp_type_id) <= 0:
                    continue

                # How many invented BPCs were produced? Prefer successful_runs; if missing, assume 1.
                successes = _safe_int(getattr(job, "successful_runs", None))
                if successes is None and isinstance(raw, dict):
                    try:
                        successes = int(raw.get("successful_runs") or 0)
                    except Exception:
                        successes = None
                if successes is None:
                    successes = 1
                successes_i = max(0, int(successes or 0))
                if successes_i <= 0:
                    continue

                attempts = _safe_int(getattr(job, "runs", None))
                if attempts is None and isinstance(raw, dict):
                    try:
                        attempts = int(raw.get("runs") or 0)
                    except Exception:
                        attempts = None
                attempts_i = max(1, int(attempts or successes_i or 1))

                # Best-effort attempt-cost estimate: invention materials at market + job fee (if present).
                attempt_mat_cost = None
                if blueprint_type_id is not None and int(blueprint_type_id) > 0 and market_price_map:
                    attempt_mat_cost = _invention_material_cost_per_attempt(int(blueprint_type_id))

                total_cost = 0.0
                any_cost = False
                if attempt_mat_cost is not None and float(attempt_mat_cost) > 0:
                    total_cost += float(attempt_mat_cost) * float(attempts_i)
                    any_cost = True
                if job_cost_total is not None and float(job_cost_total) > 0:
                    total_cost += float(job_cost_total)
                    any_cost = True

                if not any_cost or float(total_cost) <= 0:
                    # Fallback: use market price of the output blueprint as proxy.
                    try:
                        proxy = float((market_price_map or {}).get(int(out_bp_type_id)) or 0.0)
                    except Exception:
                        proxy = 0.0
                    if proxy <= 0:
                        continue
                    unit_cost = float(proxy)
                else:
                    unit_cost = float(total_cost) / float(successes_i)

                if unit_cost <= 0:
                    continue

                tx_by_type[int(out_bp_type_id)].append(
                    {
                        "kind": "buy",
                        "quantity": int(successes_i),
                        "unit_price": float(unit_cost),
                        "date": (str(date_s) if date_s is not None else None),
                        "dt": dt,
                        "sort_id": int(job_id or 0),
                        "reference_id": job_id,
                        "reference_type": REFERENCE_TYPE_INDUSTRY_JOB,
                        "source": ASSET_SOURCE_INDUSTRY_INVENTION,
                    }
                )
                continue

    lots_by_type: dict[int, list[FifoLot]] = {}
    for tid, txs in tx_by_type.items():
        # Sort oldest -> newest.
        txs.sort(
            key=lambda r: (
                r.get("dt") or datetime.min,
                int(r.get("sort_id") or 0),
            )
        )

        lots: list[FifoLot] = []
        for r in txs:
            q = int(r.get("quantity") or 0)
            if q <= 0:
                continue
            if str(r.get("kind")) == "buy":
                up = float(r.get("unit_price") or 0.0)
                if up <= 0:
                    continue
                lots.append(
                    FifoLot(
                        quantity=q,
                        unit_price=up,
                        acquisition_date=r.get("date"),
                        reference_id=r.get("reference_id"),
                        reference_type=r.get("reference_type"),
                        source=r.get("source"),
                    )
                )
            else:
                sell = q
                while sell > 0 and lots:
                    head = lots[0]
                    take = min(sell, int(head.quantity))
                    sell -= int(take)
                    new_qty = int(head.quantity) - int(take)
                    if new_qty > 0:
                        lots[0] = FifoLot(
                            quantity=new_qty,
                            unit_price=float(head.unit_price),
                            acquisition_date=head.acquisition_date,
                            reference_id=head.reference_id,
                            reference_type=head.reference_type,
                            source=head.source,
                        )
                    else:
                        lots.pop(0)

        # Align to current on-hand.
        on_hand = int(on_hand_quantities_by_type.get(int(tid), 0) or 0)
        if on_hand <= 0:
            lots_by_type[int(tid)] = []
            continue

        remaining = sum(int(l.quantity) for l in lots)
        if remaining > on_hand:
            excess = int(remaining) - int(on_hand)
            # FIFO-consistent adjustment: missing consumption removes oldest lots first.
            while excess > 0 and lots:
                head = lots[0]
                take = min(excess, int(head.quantity))
                excess -= int(take)
                new_qty = int(head.quantity) - int(take)
                if new_qty > 0:
                    lots[0] = FifoLot(
                        quantity=new_qty,
                        unit_price=float(head.unit_price),
                        acquisition_date=head.acquisition_date,
                        reference_id=head.reference_id,
                        reference_type=head.reference_type,
                        source=head.source,
                    )
                else:
                    lots.pop(0)

        lots_by_type[int(tid)] = lots

    # Ensure types with on-hand but no history are present.
    for tid in (on_hand_quantities_by_type or {}).keys():
        try:
            tid_i = int(tid)
        except Exception:
            continue
        lots_by_type.setdefault(tid_i, [])

    return lots_by_type


def fifo_allocate_cost(
    *,
    lots: list[FifoLot] | None,
    quantity: int,
) -> tuple[float, int]:
    """Return (total_cost, priced_quantity) for consuming `quantity` using FIFO lots."""
    q = int(quantity or 0)
    if q <= 0:
        return 0.0, 0
    if not lots:
        return 0.0, 0

    remaining = q
    total_cost = 0.0
    priced_qty = 0
    for lot in lots:
        if remaining <= 0:
            break
        lot_qty = int(lot.quantity)
        if lot_qty <= 0:
            continue
        take = min(remaining, lot_qty)
        if take <= 0:
            continue
        total_cost += float(take) * float(lot.unit_price)
        priced_qty += int(take)
        remaining -= int(take)

    return float(total_cost), int(priced_qty)


def fifo_allocate_cost_breakdown(
    *,
    lots: list[FifoLot] | None,
    quantity: int,
) -> dict[str, Any]:
    """Allocate FIFO lots and return a breakdown.

    Returns a dict with:
      - total_cost
      - priced_quantity
      - remaining_unpriced_quantity
      - by_source: {source: {"cost": float, "quantity": int}}
    """

    q = int(quantity or 0)
    if q <= 0:
        return {
            "total_cost": 0.0,
            "priced_quantity": 0,
            "remaining_unpriced_quantity": 0,
            "by_source": {},
        }
    if not lots:
        return {
            "total_cost": 0.0,
            "priced_quantity": 0,
            "remaining_unpriced_quantity": int(q),
            "by_source": {},
        }

    remaining = int(q)
    total_cost = 0.0
    priced_qty = 0
    by_source: dict[str, dict[str, Any]] = {}

    for lot in lots:
        if remaining <= 0:
            break
        lot_qty = int(getattr(lot, "quantity", 0) or 0)
        if lot_qty <= 0:
            continue
        take = min(remaining, lot_qty)
        if take <= 0:
            continue

        unit_price = float(getattr(lot, "unit_price", 0.0) or 0.0)
        chunk_cost = float(take) * float(unit_price)

        src = getattr(lot, "source", None) or "unknown"
        if not isinstance(src, str) or not src:
            src = "unknown"
        slot = by_source.get(src)
        if slot is None:
            slot = {"cost": 0.0, "quantity": 0}
            by_source[src] = slot
        slot["cost"] = float(slot.get("cost") or 0.0) + float(chunk_cost)
        slot["quantity"] = int(slot.get("quantity") or 0) + int(take)

        total_cost += float(chunk_cost)
        priced_qty += int(take)
        remaining -= int(take)

    return {
        "total_cost": float(total_cost),
        "priced_quantity": int(priced_qty),
        "remaining_unpriced_quantity": int(remaining),
        "by_source": by_source,
    }


def _build_price_map(market_prices: list[dict[str, Any]] | None) -> dict[int, float]:
    """Map type_id -> price using ESI /markets/prices/ payload.

    Prefers average_price, falls back to adjusted_price.
    """
    out: dict[int, float] = {}
    if not market_prices:
        return out
    for row in market_prices:
        if not isinstance(row, dict):
            continue
        raw_type_id = row.get("type_id")
        if raw_type_id is None:
            continue
        try:
            type_id = int(raw_type_id)
        except Exception:
            continue
        if type_id <= 0:
            continue
        price = row.get("average_price")
        if price is None:
            price = row.get("adjusted_price")
        if price is None:
            continue
        try:
            price_f = float(price)
        except Exception:
            continue
        if price_f <= 0:
            continue
        out[type_id] = price_f
    return out


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _get_mfg_activity(activities: Any) -> Optional[dict[str, Any]]:
    if not isinstance(activities, dict):
        return None
    mfg = activities.get("manufacturing")
    if isinstance(mfg, dict):
        return mfg
    return None


def estimate_industry_job_unit_cost(
    *,
    sde_session,
    blueprint_type_id: int,
    product_type_id: int,
    runs: int,
    job_cost: float | None,
    market_price_map: dict[int, float],
) -> Optional[float]:
    if not blueprint_type_id or not product_type_id:
        return None
    if runs <= 0:
        runs = 1

    bp = sde_session.query(Blueprints).filter_by(blueprintTypeID=int(blueprint_type_id)).first()
    if bp is None:
        return None

    mfg = _get_mfg_activity(getattr(bp, "activities", None))
    if not mfg:
        return None

    materials = mfg.get("materials")
    products = mfg.get("products")
    if not isinstance(materials, list) or not isinstance(products, list):
        return None

    output_qty_per_run: Optional[int] = None
    for p in products:
        if not isinstance(p, dict):
            continue
        if _safe_int(p.get("typeID")) == int(product_type_id):
            output_qty_per_run = _safe_int(p.get("quantity"))
            break
    if output_qty_per_run is None and products:
        # Fallback to first product if blueprint doesn't list product_type_id explicitly.
        output_qty_per_run = _safe_int(products[0].get("quantity")) if isinstance(products[0], dict) else None

    if not output_qty_per_run or output_qty_per_run <= 0:
        return None

    material_cost_total = 0.0
    for m in materials:
        if not isinstance(m, dict):
            continue
        mat_type_id = _safe_int(m.get("typeID"))
        qty = _safe_int(m.get("quantity"))
        if not mat_type_id or not qty or qty <= 0:
            continue
        unit_price = market_price_map.get(int(mat_type_id))
        if unit_price is None:
            continue
        material_cost_total += float(qty) * float(unit_price)

    # Scale by runs.
    material_cost_total *= float(runs)

    job_fee = float(job_cost or 0.0)
    total_cost = material_cost_total + job_fee

    total_output = float(output_qty_per_run) * float(runs)
    if total_output <= 0:
        return None

    return total_cost / total_output


def build_cost_map_for_assets(
    *,
    app_session,
    sde_session,
    owner_kind: str,
    owner_id: int,
    asset_type_ids: Iterable[int],
    asset_quantities_by_type: dict[int, int] | None = None,
    wallet_tx_model,
    industry_job_model,
    market_prices: list[dict[str, Any]] | None,
) -> dict[int, CostInfo]:
    """Return type_id -> CostInfo for an owner.

    Strategy:
    - Prefer most recent completed industry job producing the type.
    - Else prefer most recent market buy transaction.
    - Else unknown.

    Notes:
    - This is a best-effort provenance inference; ESI assets do not include acquisition history.
    """
    type_ids = sorted({int(x) for x in asset_type_ids if isinstance(x, int) or str(x).isdigit()})
    if not type_ids:
        return {}

    market_price_map = _build_price_map(market_prices)

    # Pull wallet transactions (buy + sell), newest first.
    tx_rows = (
        app_session.query(wallet_tx_model)
        .filter(wallet_tx_model.type_id.in_(type_ids))
        .filter(getattr(wallet_tx_model, owner_kind) == int(owner_id))
        .order_by(desc(wallet_tx_model.date))
        .all()
    )
    tx_by_type: dict[int, list[Any]] = {}
    for r in tx_rows:
        tid = getattr(r, "type_id", None)
        if tid is None:
            continue
        try:
            tid_int = int(tid)
        except Exception:
            continue
        tx_by_type.setdefault(tid_int, []).append(r)

    def _estimate_unit_cost_from_transactions(type_id: int, on_hand_qty: int) -> Optional[tuple[float, Any]]:
        """Estimate unit cost of current on-hand inventory using transaction history.

        We walk transactions backwards (newest -> oldest), adjusting the required
        quantity by sells, then allocating buys to cover current on-hand qty.

        This is quantity-aware and tends to value inventory using recent buys.
        Returns (unit_cost, reference_tx).
        """
        if on_hand_qty <= 0:
            return None
        txs = tx_by_type.get(type_id) or []
        if not txs:
            return None

        remaining = int(on_hand_qty)
        allocated_qty = 0
        allocated_cost = 0.0
        reference_tx = None

        for tx in txs:
            is_buy = getattr(tx, "is_buy", None)
            qty = _safe_int(getattr(tx, "quantity", None)) or 0
            if qty <= 0:
                continue

            # If this transaction is a sell, inventory was higher before the sell.
            if is_buy is False:
                remaining += qty
                continue

            # If this is a buy, allocate it towards the current on-hand inventory.
            if is_buy is True:
                unit_price = _safe_float(getattr(tx, "unit_price", None))
                if unit_price is None or unit_price <= 0:
                    continue

                take = min(remaining, qty)
                if take <= 0:
                    continue
                allocated_qty += take
                allocated_cost += float(take) * float(unit_price)
                remaining -= take
                if reference_tx is None:
                    reference_tx = tx
                if remaining <= 0:
                    break

        if allocated_qty <= 0:
            return None
        return (allocated_cost / float(allocated_qty), reference_tx)

    # Pull completed jobs (most recent per product type).
    # ESI job statuses vary; we treat delivered/ready/completed as "completed".
    completed_statuses = {"delivered", "ready", "completed"}
    job_rows = (
        app_session.query(industry_job_model)
        .filter(industry_job_model.product_type_id.in_(type_ids))
        .filter(getattr(industry_job_model, owner_kind) == int(owner_id))
        .order_by(desc(industry_job_model.end_date))
        .all()
    )
    last_job_by_type: dict[int, Any] = {}
    for r in job_rows:
        status = str(getattr(r, "status", "") or "").lower()
        if status and status not in completed_statuses:
            continue
        tid = getattr(r, "product_type_id", None)
        if tid is None:
            continue
        tid_int = int(tid)
        if tid_int not in last_job_by_type:
            last_job_by_type[tid_int] = r

    out: dict[int, CostInfo] = {}
    for tid in type_ids:
        job = last_job_by_type.get(tid)
        if job is not None:
            blueprint_type_id = _safe_int(getattr(job, "blueprint_type_id", None)) or 0
            product_type_id = _safe_int(getattr(job, "product_type_id", None)) or 0
            runs = _safe_int(getattr(job, "successful_runs", None)) or _safe_int(getattr(job, "runs", None)) or 1
            job_cost = _safe_float(getattr(job, "cost", None))

            unit_cost = estimate_industry_job_unit_cost(
                sde_session=sde_session,
                blueprint_type_id=blueprint_type_id,
                product_type_id=product_type_id,
                runs=runs,
                job_cost=job_cost,
                market_price_map=market_price_map,
            )

            out[tid] = CostInfo(
                source=ASSET_SOURCE_INDUSTRY_BUILD,
                unit_cost=unit_cost,
                total_cost=None,
                reference_type=REFERENCE_TYPE_INDUSTRY_JOB,
                reference_id=_safe_int(getattr(job, "job_id", None)),
                acquisition_date=getattr(job, "end_date", None),
            )
            continue

        on_hand_qty = int((asset_quantities_by_type or {}).get(tid, 0) or 0)
        est = _estimate_unit_cost_from_transactions(tid, on_hand_qty)
        if est is not None:
            unit_cost, ref_tx = est
            out[tid] = CostInfo(
                source=ASSET_SOURCE_MARKET_BUY,
                unit_cost=unit_cost,
                total_cost=None,
                reference_type=REFERENCE_TYPE_WALLET_TRANSACTION,
                reference_id=_safe_int(getattr(ref_tx, "transaction_id", None)) if ref_tx is not None else None,
                acquisition_date=getattr(ref_tx, "date", None) if ref_tx is not None else None,
            )
            continue

        out[tid] = CostInfo(
            source=ASSET_SOURCE_UNKNOWN,
            unit_cost=None,
            total_cost=None,
            reference_type=None,
            reference_id=None,
            acquisition_date=None,
        )

    return out
