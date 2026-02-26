import json
import math
from typing import Any

import streamlit as st  # pyright: ignore[reportMissingImports]

from utils.formatters import (
    blueprint_image_url,
    format_decimal_eu,
    format_duration,
    format_isk_eu,
    format_pct_eu,
    type_icon_url,
)


def parse_json_cell(value: Any) -> Any:
    try:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            return json.loads(s)
    except Exception:
        return None
    return None


def coerce_fraction(value: Any, *, default: float) -> float:
    try:
        v = float(value)
    except Exception:
        v = float(default)
    if v >= 1.0:
        v = v / 100.0
    return float(min(1.0, max(0.0, v)))


def safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def safe_int_opt(v: Any) -> int | None:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def safe_float_opt(v: Any) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def blueprint_passes_filters(
    bp: dict,
    *,
    maximize_blueprint_runs: bool,
    bp_type_filter: str,
    skill_req_filter: bool,
    reactions_filter: bool,
    location_filter: str,
) -> bool:
    if not isinstance(bp, dict):
        return False

    flags = bp.get("flags") or {}
    is_bpc = bool(flags.get("is_blueprint_copy")) if isinstance(flags, dict) else False
    if bool(maximize_blueprint_runs):
        if not is_bpc:
            return False
    else:
        if bp_type_filter == "Originals (BPO)" and is_bpc:
            return False
        if bp_type_filter == "Copies (BPC)" and not is_bpc:
            return False

    if skill_req_filter:
        if not bool(bp.get("skill_requirements_met", False)):
            return False

    if reactions_filter is False:
        flags = bp.get("flags") or {}
        is_reaction_bp = bool(flags.get("is_reaction_blueprint")) if isinstance(flags, dict) else False
        if is_reaction_bp:
            return False

    if location_filter != "All":
        loc = bp.get("location") or {}
        disp = (loc.get("display_name") if isinstance(loc, dict) else None) or ""
        if str(disp) != str(location_filter):
            return False

    return True


def industry_invention_cache_key(
    *,
    character_id: int,
    blueprint_type_id: int,
    profile_id: int | None,
    pricing_key: str,
) -> str:
    # Include the pricing_key (market pricing + assumptions) and profile_id so cached
    # invention rows match the current UI context.
    return f"{int(character_id)}:{int(blueprint_type_id)}:p{int(profile_id or 0)}:{str(pricing_key)}"


def min_known_positive(a: Any, b: Any) -> float | None:
    vals: list[float] = []
    try:
        if a is not None:
            vals.append(float(a))
    except Exception:
        pass
    try:
        if b is not None:
            vals.append(float(b))
    except Exception:
        pass
    vals = [v for v in vals if v > 0]
    return min(vals) if vals else None


def attach_aggrid_autosize(grid_options: dict[str, Any], *, JsCode: Any) -> None:
    if not isinstance(grid_options, dict) or JsCode is None:
        return

    grid_options["autoSizeStrategy"] = {"type": "fitCellContents"}

    js_autosize_all = JsCode(
        """
            function(params) {
                setTimeout(function() {
                    try {
                        // AG Grid API surface differs across versions.
                        // Prefer auto-size to cell contents (incl header). Fall back to fit-to-grid.
                        if (params && params.columnApi && params.columnApi.autoSizeAllColumns) {
                            params.columnApi.autoSizeAllColumns(false); // skipHeader=false
                        } else if (params && params.api && params.api.autoSizeAllColumns) {
                            params.api.autoSizeAllColumns(false);
                        } else if (params && params.api && params.api.sizeColumnsToFit) {
                            params.api.sizeColumnsToFit();
                        }
                    } catch (e) {}
                }, 50);
            }
        """
    )

    grid_options["onFirstDataRendered"] = js_autosize_all
    grid_options["onGridSizeChanged"] = js_autosize_all
    grid_options["onSortChanged"] = js_autosize_all
    grid_options["onFilterChanged"] = js_autosize_all


MATERIALS_TABLE_COLUMN_CONFIG = {
    "Icon": st.column_config.ImageColumn("Icon", width="small"),
    "Unit Price": st.column_config.NumberColumn("Unit Price", format="%.2f ISK"),
    "Effective Cost": st.column_config.NumberColumn("Effective Cost", format="%.0f ISK"),
    "Market Buy Cost": st.column_config.NumberColumn("Market Buy Cost", format="%.0f ISK"),
    "FIFO (Market)": st.column_config.NumberColumn("FIFO (Market)", format="%.0f ISK"),
    "FIFO (Built)": st.column_config.NumberColumn("FIFO (Built)", format="%.0f ISK"),
    "Build Cost": st.column_config.NumberColumn("Build Cost", format="%.0f ISK"),
    "Build ROI": st.column_config.NumberColumn("Build ROI", format="%.2f%%"),
}


BUILD_TREE_CAPTION = (
    "Buy Cost is the pure market buy cost (Qty × Unit). Inventory Cost is the FIFO-valued cost of the portion taken from stock (best-effort). "
    "Effective Cost is the planner-chosen total cost. When 'Prefer consuming inventory (FIFO)' is enabled and inventory is partially available, "
    "Effective Cost becomes Inventory (FIFO) + min(buy shortfall, build shortfall). "
    "FIFO inventory costing note: inventory lots are valued as a snapshot and are not reserved across branches. "
    "If the same material appears in multiple submanufacturing steps, it may look like it is 'taken' more than once."
)
