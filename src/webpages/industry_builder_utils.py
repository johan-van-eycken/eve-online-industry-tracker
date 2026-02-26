import json
import math
from typing import Any

import streamlit as st  # pyright: ignore[reportMissingImports]


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


def type_icon_url(type_id: Any, *, size: int = 32) -> str | None:
    try:
        tid = int(type_id)
    except Exception:
        return None
    if tid <= 0:
        return None
    return f"https://images.evetech.net/types/{tid}/icon?size={int(size)}"


def blueprint_image_url(blueprint_type_id: Any, *, is_bpc: bool, size: int = 32) -> str | None:
    try:
        tid = int(blueprint_type_id)
    except Exception:
        return None
    if tid <= 0:
        return None
    variation = "bpc" if bool(is_bpc) else "bp"
    return f"https://images.evetech.net/types/{tid}/{variation}?size={int(size)}"


def format_duration(seconds: float | int | None) -> str:
    try:
        s = int(round(float(seconds or 0.0)))
    except Exception:
        s = 0
    if s < 0:
        s = 0

    day_s = 24 * 3600

    days = s // day_s
    s = s % day_s
    hours = s // 3600
    s = s % 3600
    minutes = s // 60
    secs = s % 60

    # Standard UI format: D H m s
    parts: list[str] = []
    if days:
        parts.append(f"{days}D")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def format_decimal_eu(value: Any, *, decimals: int = 2, missing: str = "-") -> str:
    """Format a numeric value using EU separators.

    - Thousands separator: '.'
    - Decimal separator  : ','

    This is used for Streamlit fallback tables when AgGrid is unavailable.
    """

    try:
        if value is None:
            return missing
        if isinstance(value, float) and math.isnan(value):
            return missing
        if isinstance(value, str) and not value.strip():
            return missing
        v = float(value)
    except Exception:
        return missing

    s = f"{v:,.{int(decimals)}f}"  # 1,234,567.89
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def format_isk_eu(value: Any, *, decimals: int = 2, missing: str = "-") -> str:
    s = format_decimal_eu(value, decimals=decimals, missing=missing)
    return f"{s} ISK" if s != missing else missing


def format_pct_eu(value: Any, *, decimals: int = 2, missing: str = "-") -> str:
    s = format_decimal_eu(value, decimals=decimals, missing=missing)
    return f"{s}%" if s != missing else missing


def js_eu_number_formatter(*, JsCode: Any, locale: str, decimals: int) -> Any:
    if JsCode is None:
        return None
    return JsCode(
        f"""
            function(params) {{
            if (params.value === null || params.value === undefined || params.value === \"\") return \"\";
            const n = Number(params.value);
            if (isNaN(n)) return \"\";
            return new Intl.NumberFormat('{str(locale)}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n);
            }}
        """
    )


def js_eu_isk_formatter(*, JsCode: Any, locale: str, decimals: int) -> Any:
    if JsCode is None:
        return None
    return JsCode(
        f"""
            function(params) {{
            if (params.value === null || params.value === undefined || params.value === \"\") return \"\";
            const n = Number(params.value);
            if (isNaN(n)) return \"\";
            return new Intl.NumberFormat('{str(locale)}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n) + ' ISK';
            }}
        """
    )


def js_eu_pct_formatter(*, JsCode: Any, locale: str, decimals: int) -> Any:
    if JsCode is None:
        return None
    return JsCode(
        f"""
            function(params) {{
                if (params.value === null || params.value === undefined || params.value === \"\") return \"\";
                const n = Number(params.value);
                if (isNaN(n)) return \"\";
                return new Intl.NumberFormat('{str(locale)}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n) + '%';
            }}
        """
    )


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
