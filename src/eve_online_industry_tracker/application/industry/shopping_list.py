from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def aggregate_shopping_list(selected_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate procurement materials from selected overview rows into a shopping list.

    Each material line is ONE batch's take-or-buy plan: ``quantity`` needed, of
    which the producer takes ``take_quantity`` from owned stock and buys
    ``buy_quantity``. Owned stock exists once, so it is not scaled by batches:

        need = sum over rows of (quantity per batch x that row's batches)
        buy  = max(0, need - owned)

    per type_id. The rows do not carry the total stock, only what each row
    took. Every row is planned against the full stock, and within a row the
    lines spend it in turn, so a row's summed take is stock the producer saw.
    ``owned`` is the largest such figure over the selected rows, so it is never
    more stock than the producer reported.

    A line without an explicit ``buy_quantity`` counts nothing as owned and
    buys its full quantity. A "take" / "split" line without the split comes
    from an overview cached before the producer wrote it; those types are
    named in one WARNING per call. Every gap therefore errs toward over-buy,
    never under-buy.

    Returns a list of dicts sorted by (buy * unit_price) descending:
        {"type_id": int, "type_name": str, "need": int, "buy": int, "unit_price": float | None}
    """
    accumulated: dict[int, dict[str, Any]] = {}
    owned_by_type_id: dict[int, int] = {}
    stale_type_names: dict[int, str] = {}

    for row in selected_rows:
        if not isinstance(row, dict):
            continue
        mj = row.get("manufacturing_job")
        if not isinstance(mj, dict):
            continue
        # procurement_materials: the manufacturing job's buy list.
        # invention_procurement_materials: an invented T2 row's (and its nested
        # T2 sub-builds') invention inputs for whole attempts, same line shape.
        lines = [
            mat
            for key in ("procurement_materials", "invention_procurement_materials")
            if isinstance(mj.get(key), dict)
            for mat in mj[key].values()
        ]
        if not lines:
            continue

        batches = max(1, int(row.get("max_batches_total") or 1))
        row_take_by_type_id: dict[int, int] = {}

        for mat in lines:
            if not isinstance(mat, dict):
                continue
            try:
                mat_type_id = int(mat["type_id"])
            except (KeyError, TypeError, ValueError):
                continue

            quantity_per_batch = int(mat.get("quantity") or 0)
            if "buy_quantity" not in mat and str(mat.get("sourcing_strategy") or "").lower() in _OWNED_STRATEGIES:
                stale_type_names.setdefault(mat_type_id, str(mat.get("type_name") or mat_type_id))
            take_per_batch = max(0, min(quantity_per_batch, quantity_per_batch - _buy_per_batch(mat, quantity_per_batch)))
            row_take_by_type_id[mat_type_id] = row_take_by_type_id.get(mat_type_id, 0) + take_per_batch

            need_total = quantity_per_batch * batches
            if mat_type_id in accumulated:
                accumulated[mat_type_id]["need"] += need_total
            else:
                accumulated[mat_type_id] = {
                    "type_id": mat_type_id,
                    "type_name": str(mat.get("type_name") or mat_type_id),
                    "need": need_total,
                    "buy": 0,
                    "unit_price": _safe_float(mat.get("unit_price")),
                }

        for mat_type_id, row_take in row_take_by_type_id.items():
            owned_by_type_id[mat_type_id] = max(owned_by_type_id.get(mat_type_id, 0), row_take)

    for mat_type_id, item in accumulated.items():
        item["buy"] = max(0, item["need"] - owned_by_type_id.get(mat_type_id, 0))

    if stale_type_names:
        logger.warning(
            "Shopping list: %s came from an older overview without a take/buy split; "
            "counted as fully bought (may over-buy). Refresh the Industry Builder overview for exact stock use.",
            ", ".join(sorted(stale_type_names.values())),
        )

    result = list(accumulated.values())
    result.sort(key=lambda r: (r["buy"] * (r["unit_price"] or 0.0)), reverse=True)
    return result


_OWNED_STRATEGIES = frozenset({"take", "split", "mixed"})


def _buy_per_batch(mat: dict[str, Any], quantity_per_batch: int) -> int:
    """What one batch buys: the explicit buy_quantity, else everything.

    Without the split, nothing is known to be owned, whatever the strategy says:
    a "take" label on a merged take + buy line does not mean it is all owned.
    """
    if "buy_quantity" in mat:
        return int(mat["buy_quantity"])
    return quantity_per_batch


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f >= 0 else None
    except (TypeError, ValueError):
        return None
