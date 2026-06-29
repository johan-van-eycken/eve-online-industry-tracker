from __future__ import annotations

from typing import Any


def aggregate_shopping_list(selected_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate procurement materials from selected overview rows into a shopping list.

    For each selected row × its recommended batch count, accumulate the market-purchase
    quantity (buy_quantity) for each material type across all rows.

    Returns a list of dicts sorted by (buy * unit_price) descending:
        {"type_id": int, "type_name": str, "need": int, "buy": int, "unit_price": float | None}
    """
    accumulated: dict[int, dict[str, Any]] = {}

    for row in selected_rows:
        if not isinstance(row, dict):
            continue
        mj = row.get("manufacturing_job")
        if not isinstance(mj, dict):
            continue
        proc = mj.get("procurement_materials")
        if not isinstance(proc, dict):
            continue

        batches = max(1, int(row.get("max_batches_total") or 1))

        for mat in proc.values():
            if not isinstance(mat, dict):
                continue
            try:
                mat_type_id = int(mat["type_id"])
            except (KeyError, TypeError, ValueError):
                continue

            quantity_per_batch = int(mat.get("quantity") or 0)
            strategy = str(mat.get("sourcing_strategy") or "buy").lower()

            # Prefer explicit buy_quantity; fall back based on strategy
            if "buy_quantity" in mat:
                buy_per_batch = int(mat["buy_quantity"])
            elif strategy == "take":
                buy_per_batch = 0
            elif strategy in ("split", "mixed"):
                # "split" and "mixed" both represent partial inventory coverage —
                # without an explicit buy_quantity we must buy everything.
                buy_per_batch = quantity_per_batch
            else:
                # "buy" (and any unrecognised strategy): treat as full buy
                buy_per_batch = quantity_per_batch

            need_total = quantity_per_batch * batches
            buy_total = buy_per_batch * batches

            if mat_type_id in accumulated:
                accumulated[mat_type_id]["need"] += need_total
                accumulated[mat_type_id]["buy"] += buy_total
            else:
                accumulated[mat_type_id] = {
                    "type_id": mat_type_id,
                    "type_name": str(mat.get("type_name") or mat_type_id),
                    "need": need_total,
                    "buy": buy_total,
                    "unit_price": _safe_float(mat.get("unit_price")),
                }

    result = list(accumulated.values())
    result.sort(key=lambda r: (r["buy"] * (r["unit_price"] or 0.0)), reverse=True)
    return result


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f >= 0 else None
    except (TypeError, ValueError):
        return None
