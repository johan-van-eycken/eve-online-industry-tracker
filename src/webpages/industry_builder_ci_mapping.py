from __future__ import annotations

import math
from typing import Any


def _safe_int(v: Any, *, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _safe_float_opt(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def map_invention_materials_breakdown_to_rows(
    inv_mats_bd: Any,
    *,
    attempts_scale: float,
) -> list[dict[str, Any]]:
    """Map backend invention materials breakdown into the Streamlit table rows.

    This is intentionally a pure helper (no Streamlit/pandas imports) so it can be
    unit-tested.
    """

    rows: list[dict[str, Any]] = []
    try:
        attempts_scale_f = float(attempts_scale)
    except Exception:
        attempts_scale_f = 1.0
    if attempts_scale_f <= 0:
        attempts_scale_f = 1.0

    # In the UI, invention attempts must be whole-number jobs.
    job_runs = max(1, int(math.ceil(float(attempts_scale_f))))

    if not isinstance(inv_mats_bd, list):
        return rows

    for r in inv_mats_bd:
        if not isinstance(r, dict):
            continue

        type_id = _safe_int(r.get("type_id"), default=0)

        req = _safe_int(r.get("required_quantity"), default=0)
        on_hand = _safe_int(r.get("inventory_on_hand_qty"), default=0)

        # Action should reflect the (ceiled) job plan, not a single attempt.
        total_required_qty = int(req) * int(job_runs)
        inv_used_total = min(int(total_required_qty), int(on_hand))
        buy_now_total = max(0, int(total_required_qty) - int(inv_used_total))

        if inv_used_total > 0 and buy_now_total == 0:
            action = "take"
        elif inv_used_total > 0 and buy_now_total > 0:
            action = "take+buy"
        else:
            action = "buy"
        eff_cost = _safe_float_opt(r.get("effective_cost_isk"))
        inv_cost = _safe_float_opt(r.get("inventory_cost_isk"))
        buy_cost = _safe_float_opt(r.get("buy_cost_isk"))
        eff_unit = _safe_float_opt(r.get("effective_unit_price_isk"))

        rows.append(
            {
                "type_id": int(type_id),
                "Material": str(r.get("type_name") or r.get("type_id") or ""),
                "Job Runs": None,
                "Qty": float(int(req) * int(job_runs)),
                "Action": str(action),
                "Effective Cost": (float(eff_cost) * float(job_runs)) if eff_cost is not None else None,
                "Effective / unit": float(eff_unit) if eff_unit is not None else None,
                "Inventory Cost": (float(inv_cost) * float(job_runs)) if inv_cost is not None else None,
                "Buy Cost": (float(buy_cost) * float(job_runs)) if buy_cost is not None else None,
            }
        )

    return rows
