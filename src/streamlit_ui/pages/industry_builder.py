import streamlit as st
from typing import Any, cast

from streamlit_ui.components.aggrid_formatters import js_eu_isk_formatter, js_eu_number_formatter, js_icon_cell_renderer
from streamlit_ui.components.formatters import format_duration
from streamlit_ui.state.industry_builder_ui import (
    build_overview_grid_frame,
    build_debug_payload_preview,
    filter_overview_rows,
)
from streamlit_ui.state.industry_snapshot_page import (
    format_scope_refreshed_at,
    prepare_shared_industry_snapshot_page,
    render_pricing_batch_panel,
)
from streamlit_ui.state.industry_builder_ui import meta_group_label, ordered_meta_group_names, get_meta_group_name
from streamlit_ui.components.webpage_ui import require_aggrid
from streamlit_ui.api.industry_profiles import build_industry_profile_options
from streamlit_ui.state.industry_builder_page import (
    fetch_industry_profiles_cached,
    start_overview_refresh_job,
)
from streamlit_ui.api.industry_builder import start_product_overview_refresh
from typing import cast, Any


def _format_age_minutes(value: Any) -> str:
    try:
        minutes = float(value or 0.0)
    except Exception:
        return "N/A"
    if minutes <= 0:
        return "0 min"
    if minutes >= 1440:
        return f"{minutes / 1440.0:.1f} d"
    if minutes >= 60:
        return f"{minutes / 60.0:.1f} h"
    return f"{minutes:.0f} min"


def _render_profitability_drilldown(filtered_overview_rows: list[dict[str, Any]]) -> None:
    if not filtered_overview_rows:
        return

    drilldown_options: dict[str, str] = {}
    for row in filtered_overview_rows:
        overview_row_id = str(row.get("overview_row_id") or "")
        if not overview_row_id:
            continue
        drilldown_options[overview_row_id] = "{name} | Profit {profit} | Confidence {confidence}".format(
            name=str(row.get("type_name") or row.get("type_id") or overview_row_id),
            profit=(f"{float(row.get('profit_amount') or 0.0):,.0f} ISK" if row.get("profit_amount") is not None else "N/A"),
            confidence=str(row.get("pricing_confidence") or "N/A"),
        )

    if not drilldown_options:
        return

    selected_overview_row_id = st.selectbox(
        "Why profitable drilldown",
        options=list(drilldown_options.keys()),
        format_func=lambda key: drilldown_options.get(str(key), str(key)),
        key="industry_builder_profitability_drilldown_id",
    )
    selected_row = next(
        (
            row
            for row in filtered_overview_rows
            if str(row.get("overview_row_id") or "") == str(selected_overview_row_id)
        ),
        None,
    )
    if not isinstance(selected_row, dict):
        return

    manufacturing_job = cast(dict[str, Any], selected_row.get("manufacturing_job") or {})
    procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
    if not isinstance(procurement_materials, dict):
        procurement_materials = {}

    with st.expander("Why profitable", expanded=False):
        metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)
        metric_col_1.metric("Net Proceeds", f"{float(selected_row.get('net_proceeds') or 0.0):,.2f}" if selected_row.get("net_proceeds") is not None else "N/A")
        metric_col_2.metric("Total Cost", f"{float((manufacturing_job or {}).get('total_cost') or 0.0):,.2f}" if manufacturing_job.get("total_cost") is not None else "N/A")
        metric_col_3.metric("Profit", f"{float(selected_row.get('profit_amount') or 0.0):,.2f}" if selected_row.get("profit_amount") is not None else "N/A")
        metric_col_4.metric("Confidence", str(selected_row.get("pricing_confidence") or "N/A"))

        signal_col_left, signal_col_right = st.columns(2)
        with signal_col_left:
            st.markdown("**Price and market signals**")
            st.write(
                {
                    "market_price_source": selected_row.get("market_price_source"),
                    "market_hub": selected_row.get("market_hub_label") or selected_row.get("market_hub"),
                    "market_price_age": _format_age_minutes(selected_row.get("market_price_age_minutes")),
                    "region_daily_volume": selected_row.get("region_daily_volume"),
                    "region_daily_volume_7d_avg": selected_row.get("region_daily_volume_7d_avg"),
                    "hub_buy_liquidity": selected_row.get("hub_buy_liquidity"),
                    "hub_sell_liquidity": selected_row.get("hub_sell_liquidity"),
                    "hub_buy_orders": selected_row.get("hub_buy_order_count"),
                    "hub_sell_orders": selected_row.get("hub_sell_order_count"),
                }
            )
        with signal_col_right:
            st.markdown("**Cost and fees**")
            st.write(
                {
                    "material_cost": manufacturing_job.get("material_cost"),
                    "job_cost": manufacturing_job.get("total_job_cost"),
                    "gross_sale_value": selected_row.get("gross_sale_value"),
                    "broker_fee_amount": selected_row.get("broker_fee_amount"),
                    "sales_tax_amount": selected_row.get("sales_tax_amount"),
                    "profit_margin_pct": (
                        float(selected_row.get("profit_margin_fraction") or 0.0) * 100.0
                        if selected_row.get("profit_margin_fraction") is not None
                        else None
                    ),
                    "isk_per_hour": selected_row.get("isk_per_hour"),
                }
            )

        reasons = selected_row.get("pricing_confidence_reasons") or []
        if isinstance(reasons, list) and reasons:
            st.markdown("**Confidence reasoning**")
            for reason in reasons:
                st.write(f"- {reason}")

        top_material_rows = sorted(
            [
                {
                    "Type": str(material.get("type_name") or material.get("type_id") or "Material"),
                    "Qty": int(material.get("quantity") or 0),
                    "Unit Price": material.get("unit_price"),
                    "Line Total": material.get("line_total"),
                    "Source": material.get("price_source"),
                    "Est. Cost?": "⚠ est." if bool(material.get("uses_unknown_owned_cost_basis")) else "",
                }
                for material in procurement_materials.values()
                if isinstance(material, dict)
            ],
            key=lambda row: float(row.get("Line Total") or 0.0),
            reverse=True,
        )[:8]
        if top_material_rows:
            st.markdown("**Top material cost drivers**")
            st.dataframe(top_material_rows, width="stretch", hide_index=True)

        activity_breakdown = manufacturing_job.get("activity_breakdown") or {}
        if isinstance(activity_breakdown, dict) and activity_breakdown:
            activity_rows = [
                {
                    "Activity": str(activity_name),
                    "Duration (s)": activity_payload.get("duration_seconds"),
                    "Job Cost": activity_payload.get("total_job_cost") or activity_payload.get("job_cost"),
                    "Estimated Item Value": activity_payload.get("estimated_item_value"),
                }
                for activity_name, activity_payload in activity_breakdown.items()
                if isinstance(activity_payload, dict)
            ]
            if activity_rows:
                st.markdown("**Activity breakdown**")
                st.dataframe(activity_rows, width="stretch", hide_index=True)


def _render_overview_grid(
    *,
    runtime: Any,
    filtered_overview_rows: list[dict[str, Any]],
) -> None:
    if not filtered_overview_rows:
        st.info("No overview rows available for the current selection.")
        return

    df, height, grid_state_key = build_overview_grid_frame(filtered_overview_rows)
    if df.empty:
        st.info("No overview rows available for the current selection.")
        return

    icon_renderer = js_icon_cell_renderer(JsCode=runtime.js_code, size_px=24)

    gb = runtime.grid_options_builder.from_dataframe(df)
    gb.configure_default_column(
        resizable=True,
        sortable=True,
        filter=True,
        wrapText=False,
        autoHeight=False,
        wrapHeaderText=False,
        autoHeaderHeight=False,
        cellStyle={"whiteSpace": "nowrap", "lineHeight": "1.2"},
    )

    for col, width in [
        ("Icon", 72),
        ("Activity", 120),
        ("BPC Source", 160),
        ("BPO Source", 160),
        ("Meta Group", 130),
        ("Category", 140),
        ("Market Hub", 140),
        ("Market Price Source", 180),
        ("Region Daily Volume", 150),
        ("Region Daily Volume (7d Avg)", 180),
        ("Hub Buy Liquidity", 150),
        ("Hub Sell Liquidity", 150),
        ("Hub Buy Orders", 135),
        ("Hub Sell Orders", 135),
        ("Pricing Confidence", 140),
        ("Profit Margin %", 130),
    ]:
        if col in df.columns:
            gb.configure_column(
                col,
                minWidth=width,
                wrapText=False,
                autoHeight=False,
                wrapHeaderText=False,
                autoHeaderHeight=False,
                cellStyle={"whiteSpace": "nowrap", "lineHeight": "1.2"},
            )

    if "Icon" in df.columns:
        gb.configure_column(
            "Icon",
            headerName="",
            width=72,
            cellRenderer=icon_renderer,
            suppressSizeToFit=True,
            sortable=False,
            filter=False,
        )

    if "Step" in df.columns:
        gb.configure_column("Step", hide=True)

    if "Type" in df.columns:
        gb.configure_column("Type", hide=True)

    for col in ["ID", "Qty", "Runs"]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=0),
                minWidth=105,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )
            if col == "ID":
                gb.configure_column(col, hide=True)

    for col in ["_path", "_parent_path", "_depth", "_sort_order", "_has_children"]:
        if col in df.columns:
            gb.configure_column(col, hide=True)

    for col in [
        "Material Cost",
        "Job Cost",
        "Total Cost",
        "Market Unit Price",
        "Gross Sale Value",
        "Broker Fee",
        "Sales Tax",
        "Net Proceeds",
        "Profit",
        "ISK/Hour",
        "Hub Buy Liquidity",
        "Hub Sell Liquidity",
    ]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_isk_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                minWidth=130,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )

    for col in ["Profit Margin %"]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                minWidth=130,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )

    for col in ["Days of Supply", "Sell-Through Rate %", "Liquidity Score"]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                minWidth=130,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )

    for col in [
        "Region Daily Volume",
        "Region Daily Volume (7d Avg)",
        "Hub Buy Orders",
        "Hub Sell Orders",
    ]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=0),
                minWidth=130,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )

    if "Liquidity Indicator" in df.columns:
        gb.configure_column(
            "Liquidity Indicator",
            minWidth=140,
            wrapHeaderText=False,
            autoHeaderHeight=False,
            cellStyle=runtime.js_code(
                """
                function(params) {
                    var v = params.value;
                    if (!v) return {};
                    if (v === 'Very High') return { color: '#22c55e', fontWeight: '700' };
                    if (v === 'High')      return { color: '#86efac', fontWeight: '600' };
                    if (v === 'Medium')    return { color: '#facc15', fontWeight: '500' };
                    if (v === 'Low')       return { color: '#f97316', fontWeight: '600' };
                    if (v === 'Very Low')  return { color: '#ef4444', fontWeight: '700' };
                    return { color: '#9ca3af' };
                }
                """
            ),
        )

    if "Price Anomaly Risk" in df.columns:
        gb.configure_column(
            "Price Anomaly Risk",
            minWidth=140,
            wrapHeaderText=False,
            autoHeaderHeight=False,
            cellStyle=runtime.js_code(
                """
                function(params) {
                    var v = params.value;
                    if (!v) return {};
                    if (v === 'High')   return { color: '#ef4444', fontWeight: '700' };
                    if (v === 'Medium') return { color: '#f97316', fontWeight: '600' };
                    if (v === 'Low')    return { color: '#eab308', fontWeight: '500' };
                    return {};
                }
                """
            ),
        )

    for col in ["Price Anomaly Reasons"]:
        if col in df.columns:
            gb.configure_column(col, minWidth=300, wrapHeaderText=False, autoHeaderHeight=False)

    for col in ["Price vs Material Ratio", "Price vs History Ratio"]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=1),
                minWidth=150,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )

    if "History 7d Avg" in df.columns:
        gb.configure_column(
            "History 7d Avg",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=js_eu_isk_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
            minWidth=140,
            wrapHeaderText=False,
            autoHeaderHeight=False,
        )

    for col in ["Return on Capital %", "Prep Time %", "Mfg Cost Index %"]:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                minWidth=130,
                wrapHeaderText=False,
                autoHeaderHeight=False,
            )

    if "Blueprint ME" in df.columns:
        gb.configure_column(
            "Blueprint ME",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=0),
            minWidth=110,
            wrapHeaderText=False,
            autoHeaderHeight=False,
            cellStyle=runtime.js_code(
                """
                function(params) {
                    var v = params.value;
                    if (v === null || v === undefined || v === '') return {};
                    var n = Number(v);
                    if (n >= 10) return { color: '#22c55e', fontWeight: '700' };
                    if (n >= 7)  return { color: '#86efac' };
                    if (n >= 4)  return { color: '#facc15' };
                    return { color: '#f97316', fontWeight: '600' };
                }
                """
            ),
        )

    if "Manufacture Window" in df.columns:
        gb.configure_column(
            "Manufacture Window",
            minWidth=140,
            wrapHeaderText=False,
            autoHeaderHeight=False,
            cellStyle=runtime.js_code(
                """
                function(params) {
                    var v = String(params.value || '');
                    if (v === 'OK')        return { color: '#22c55e', fontWeight: '600' };
                    if (v.indexOf('Risk') >= 0) return { color: '#f97316', fontWeight: '600' };
                    return {};
                }
                """
            ),
        )

    for col in ["Fragile Margin", "SDE Fallback", "Material Contention"]:
        if col in df.columns:
            gb.configure_column(
                col,
                minWidth=140,
                wrapHeaderText=False,
                autoHeaderHeight=False,
                cellStyle=runtime.js_code(
                    """
                    function(params) {
                        if (params.value) return { color: '#f97316', fontWeight: '600' };
                        return {};
                    }
                    """
                ),
            )

    if "Job Duration" in df.columns:
        gb.configure_column(
            "Job Duration",
            minWidth=140,
            wrapHeaderText=False,
            autoHeaderHeight=False,
        )

    if "Market Volume" in df.columns:
        gb.configure_column(
            "Market Volume",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=0),
            minWidth=120,
            wrapHeaderText=False,
            autoHeaderHeight=False,
        )

    if hasattr(gb, "configure_selection"):
        gb.configure_selection("single", use_checkbox=False)

    gb.configure_grid_options(
        suppressRowTransform=True,
        ensureDomOrder=True,
        tooltipShowDelay=0,
        rowHeight=32,
        headerHeight=36,
        animateRows=True,
        suppressCellFocus=False,
        rowSelection="single",
    )

    grid_options = gb.build()
    grid_options["treeData"] = True
    grid_options["enableRangeSelection"] = True
    grid_options["ensureDomOrder"] = True
    grid_options["copyHeadersToClipboard"] = True
    grid_options["suppressCopyRowsToClipboard"] = False
    grid_options["getDataPath"] = runtime.js_code(
        """
        function(data) {
            try {
                if (!data || data._path === null || data._path === undefined) return [];
                var path = String(data._path);
                if (!path) return [];
                return path.split('|||').filter(function(part) {
                    return part !== null && part !== undefined && String(part).length > 0;
                });
            } catch (e) {
                return [];
            }
        }
        """
    )
    grid_options["groupDefaultExpanded"] = 0
    grid_options["isGroupOpenByDefault"] = runtime.js_code(
        """
        function() {
            return false;
        }
        """
    ) 
    grid_options["autoGroupColumnDef"] = {
        "headerName": "Step",
        "pinned": "left",
        "minWidth": 320,
        "cellRendererParams": {
            "suppressCount": True,
            "innerRenderer": runtime.js_code(
                """
                function(params) {
                    if (!params || !params.data) return '';
                    return String(params.data.Step || '');
                }
                """
            ),
        },
    }
    grid_options["onFirstDataRendered"] = runtime.js_code(
        """
        function(params) {
            params.api.autoSizeAllColumns();
        }
        """
    )

    runtime.aggrid_fn(
        df,
        gridOptions=grid_options,
        update_mode="NO_UPDATE",
        update_on=[],
        allow_unsafe_jscode=True,
        enable_enterprise_modules=True,
        theme="streamlit",
        height=height,
        fit_columns_on_grid_load=False,
        key=f"industry_builder_products_overview_{grid_state_key}",
    )


def _render_debug_panel(filtered_overview_rows: list[dict[str, Any]]) -> None:
    debug_options: dict[str, str] = {}
    for row in filtered_overview_rows:
        overview_row_id = str(row.get("overview_row_id") or "")
        product_name = str(row.get("type_name") or row.get("type_id") or "")
        product_type_id = int(row.get("type_id") or 0)
        debug_options[overview_row_id] = f"{product_name} ({product_type_id})"

    if debug_options:
        selected_debug_blueprint_id = st.selectbox(
            "Debug blueprint payload",
            options=list(debug_options.keys()),
            format_func=lambda x: debug_options.get(str(x), str(x)),
            key="industry_builder_debug_blueprint_id",
        )
        selected_debug_payload = next(
            (
                row
                for row in filtered_overview_rows
                if str(row.get("overview_row_id") or "") == str(selected_debug_blueprint_id)
            ),
            None,
        )

        with st.expander("Raw data (for debugging)", expanded=False):
            st.write(build_debug_payload_preview(selected_debug_payload))
            if st.checkbox("Show full nested payload", key="industry_builder_show_full_debug_payload"):
                st.write(selected_debug_payload or {})
    else:
        with st.expander("Raw data (for debugging)", expanded=False):
            st.write({})


def _render_overview_columns_about() -> None:
    def _content() -> None:
        st.markdown("**Days of Supply**")
        st.caption(
            "Hub sell orderbook depth ÷ 7-day average daily region volume. "
            "How many days it would take to sell through all current hub sell orders at the recent daily sales pace. "
            "Blank if the 7-day average is zero or missing."
        )
        st.markdown("**Sell-Through Rate %**")
        st.caption(
            "7-day average daily region volume ÷ hub sell orderbook depth × 100. "
            "The inverse of Days of Supply — what fraction of the hub orderbook turns over per day. "
            "Blank if hub sell liquidity is zero."
        )
        st.markdown("**Liquidity Indicator**")
        st.caption("Text label derived from Days of Supply:")
        st.markdown(
            "| DOS | Label |\n"
            "|---|---|\n"
            "| < 1 day | Very High |\n"
            "| 1–3 days | High |\n"
            "| 3–7 days | Medium |\n"
            "| 7–30 days | Low |\n"
            "| ≥ 30 days | Very Low |\n"
            "| 7d avg missing | Unknown |"
        )
        st.markdown("**Liquidity Score**")
        st.caption(
            "Composite 0–100 score. "
            "70% from Days of Supply (linear: 1 day → 96.7, 30 days → 0) "
            "and 30% from hub sell order count (linear: 100 orders → 100, capped). "
            "Returns 0 if the 7-day average is missing."
        )
        st.markdown("**Price Anomaly Risk**")
        st.caption(
            "Flags products where the current market price looks manipulated or unreliable. "
            "Checked against two signals: how many times higher the price is vs. material cost (Tier 1), "
            "and how far it deviates from the 7-day historical average (Tier 2). "
            "High = strong manipulation signal (e.g. price is 1,000×+ material cost or 5×+ historical average). "
            "Medium = elevated but less extreme. Low = thin orderbook only. None = no anomaly detected."
        )
        st.markdown("**Price Anomaly Reasons**")
        st.caption(
            "Human-readable explanation of what triggered the anomaly flag. "
            "Shows the specific ratios and thresholds that were breached."
        )
        st.markdown("**Price vs Material Ratio**")
        st.caption(
            "Current market price ÷ unit material cost. "
            "A ratio above ~1.2 is a normal margin. Above 100 is suspicious. Above 1,000 strongly suggests manipulation."
        )
        st.markdown("**Price vs History Ratio**")
        st.caption(
            "Current market price ÷ 7-day historical average price (from ESI market history). "
            "A ratio above 2 means the price is elevated vs. recent history. Above 5 is a strong manipulation signal."
        )
        st.markdown("**History 7d Avg**")
        st.caption(
            "The mean closing price over the last 7 days of ESI market history for this item in the selected hub's region. "
            "Lags by 1 day — today's manipulated price does not affect this baseline."
        )
        st.markdown("**Return on Capital %**")
        st.caption(
            "Profit ÷ total manufacturing cost × 100. "
            "Tells you how efficiently your capital is working — a 200M build returning 5M (2.5% ROC) is less attractive "
            "than a 10M build returning 2M (20% ROC), even if absolute ISK/hour is similar."
        )
        st.markdown("**Manufacture Window**")
        st.caption(
            "OK = the hub sell orderbook will last longer than your build time. "
            "⚠ At Risk = the current supply sells through before you finish — the market may already be restocked by the time you list. "
            "Blank if Days of Supply or build time is unavailable."
        )
        st.markdown("**Blueprint ME**")
        st.caption(
            "Material Efficiency level of the blueprint being used (0–10). "
            "ME10 is maximum — every level below increases material cost. "
            "Green = ME10, yellow = ME4–6, orange = ME0–3. "
            "A low ME may be the reason a product shows a thin margin."
        )
        st.markdown("**Prep Time %**")
        st.caption(
            "What fraction of total job time is spent on prerequisite activities (invention, copying, research) "
            "rather than the manufacturing step itself. "
            "60% prep means most of your slot time is overhead, not production."
        )
        st.markdown("**Fragile Margin**")
        st.caption(
            "⚠ flagged when profit margin is between 0% and 5%. "
            "A single undercut or small price move can eliminate the profit entirely. "
            "High execution risk."
        )
        st.markdown("**SDE Fallback**")
        st.caption(
            "⚠ flagged when the row uses SDE blueprint defaults rather than an owned blueprint. "
            "This means you don't actually have a BPC or BPO — the row is theoretical only."
        )
        st.markdown("**Material Contention**")
        st.caption(
            "⚠ flagged when this product competes for the same owned inventory as a higher-ranked build. "
            "Profit may be overstated if materials are allocated to a more profitable product first."
        )
        st.markdown("**Mfg Cost Index %**")
        st.caption(
            "The system manufacturing cost index as a percentage of the job's estimated item value. "
            "Higher = more expensive system. A 5% cost index is roughly neutral; above 8% starts eroding margins significantly."
        )

    if hasattr(st, "popover"):
        with st.popover("?", help="About the data columns"):
            _content()
    else:
        with st.expander("?", expanded=False):
            _content()


def _render_page_about() -> None:
    def _content() -> None:
        st.caption(
            "Shows everything you can manufacture based on your owned blueprints, "
            "valued against your chosen trade hub. Each product row contains the full build tree — "
            "materials, required skills, job duration, costs, and profitability."
        )
        st.caption(
            "**Owned Blueprints** — Scope the blueprint inventory to a single character, "
            "a character plus its corporation hangar, one corporation, or all of the above."
        )
        st.caption(
            "**Character Skills** — Determines manufacturing time and whether skill requirements are met. "
            "Does not affect which blueprints are visible."
        )
        st.caption(
            "**Industry Profile** — Your saved facility setup: system cost index, facility tax, "
            "and structure rig bonuses. Changes take effect after Refresh Overview."
        )
        st.caption(
            "**Filters** — All filter and setting changes take effect only after clicking Refresh Overview."
        )
        st.caption(
            "**Build tree** — Use the chevrons in the Step column to expand sub-jobs "
            "(invention chains, reaction intermediates, component manufacturing)."
        )
        st.caption(
            "**Volume columns** — Region Daily Volume is true regional market history from ESI. "
            "Hub Buy/Sell Liquidity is live order-book depth at the selected hub. "
            "The 7d Avg smooths the latest daily volume over 7 reported days."
        )

    if hasattr(st, "popover"):
        with st.popover("?", help="About this page"):
            _content()
    else:
        with st.expander("?", expanded=False):
            _content()


def render() -> None:
    runtime = require_aggrid()
    try:
        page_context = prepare_shared_industry_snapshot_page(
            title="Industry Builder",
            intro_caption="",
            refresh_caption="",
            refresh_button_label="Refresh Overview",
            refresh_button_key="industry_builder_refresh_overview",
            no_rows_message="No manufacturable product rows are available yet.",
            render_about_fn=_render_page_about,
            render_refresh_button=False,
            render_selector_section_ui=False,
            render_status_panels=False,
        )
    except Exception as e:
        st.error(str(e))
        return
    if page_context is None:
        return

    # Extract all meta groups from overview data
    all_meta_groups = {"Tech I", "Tech II", "Tech III", "Faction", "Storyline", "Other"}

    # Get selector data for form
    character_options = page_context.character_options
    selected_character_id = int(st.session_state.get("industry_builder_character_id", page_context.default_character_id_value))
    owned_blueprint_scope_options = page_context.owned_blueprint_scope_options
    owned_blueprint_scope_labels = page_context.owned_blueprint_scope_labels

    industry_profiles = fetch_industry_profiles_cached(character_id=int(selected_character_id))
    industry_profile_options, industry_profile_labels, default_industry_profile_id = build_industry_profile_options(
        cast(list[dict[str, Any]], industry_profiles)
    )

    # Create filter form with 3-column layout
    with st.form("industry_builder_filters"):
        # SELECTOR ROW: Owned Blueprints, Character Skills, Industry Profile
        selector_cols = st.columns(3, gap="large")

        with selector_cols[0]:
            default_scope = str(st.session_state.get("industry_builder_owned_blueprints_scope", page_context.default_owned_blueprint_scope))
            scope_index = owned_blueprint_scope_options.index(default_scope) if default_scope in owned_blueprint_scope_options else 0
            owned_blueprint_scope = st.selectbox(
                "Owned Blueprints",
                options=owned_blueprint_scope_options,
                format_func=lambda x: owned_blueprint_scope_labels.get(str(x), str(x)),
                index=scope_index,
                key="form_owned_blueprints_scope",
            )
            refreshed_at = page_context.scope_last_refreshed_at.get(str(owned_blueprint_scope))
            st.caption(f"Data: {format_scope_refreshed_at(refreshed_at)}")

        with selector_cols[1]:
            default_char_id = int(st.session_state.get("industry_builder_character_id", selected_character_id))
            char_options = list(character_options.keys())
            char_index = char_options.index(default_char_id) if default_char_id in char_options else 0
            character_id = st.selectbox(
                "Character Skills",
                options=char_options,
                format_func=lambda x: character_options.get(int(x), str(x)),
                index=char_index,
                key="form_character_id",
            )
            # Update industry profiles when character changes
            if character_id != selected_character_id:
                industry_profiles = fetch_industry_profiles_cached(character_id=int(character_id))
                industry_profile_options, industry_profile_labels, default_industry_profile_id = build_industry_profile_options(
                    cast(list[dict[str, Any]], industry_profiles)
                )

        with selector_cols[2]:
            default_profile_id = int(st.session_state.get("industry_builder_industry_profile_id", page_context.default_industry_profile_id))
            profile_index = industry_profile_options.index(default_profile_id) if default_profile_id in industry_profile_options else 0
            industry_profile_id = st.selectbox(
                "Industry Profile",
                options=industry_profile_options,
                format_func=lambda x: industry_profile_labels.get(int(x), str(x)),
                index=profile_index,
                key="form_industry_profile_id",
            )
            if not industry_profiles:
                st.caption("No saved profiles")

        st.divider()

        # FILTER ROWS: Meta Groups, Misc, Market, Profit Filters, Quality Filters
        left_col, market_col, profit_col, quality_col = st.columns([2, 1, 1, 1], gap="large")

        # LEFT COLUMN: Meta Groups + Misc
        with left_col:
            st.caption("**Meta Group Filters**")
            meta_cols = st.columns(3)
            _saved_meta_groups: dict[str, bool] = st.session_state.get("industry_builder_meta_groups") or {}
            meta_group_selections = {}
            for i, meta_group_name in enumerate(ordered_meta_group_names(all_meta_groups)):
                with meta_cols[i % 3]:
                    _default_meta = _saved_meta_groups.get(
                        meta_group_name,
                        meta_group_name in {"Tech I", "Tech II", "Faction", "Storyline", "Other"},
                    )
                    enabled = st.toggle(
                        meta_group_label(meta_group_name),
                        value=_default_meta,
                        key=f"form_meta_{meta_group_name}",
                    )
                    if enabled:
                        meta_group_selections[meta_group_name] = True

            st.markdown("<hr style='margin: 0.25rem 0;'/>", unsafe_allow_html=True)
            st.caption("**Misc**")
            misc_cols = st.columns(3)

            with misc_cols[0]:
                maximize_bp_runs = st.toggle(
                    "Maximize BP runs",
                    value=bool(st.session_state.get("industry_builder_maximize_bp_runs_pending", True)),
                    key="form_maximize_bp",
                )
                have_bpc_bpo = st.toggle(
                    "I have a BPC/BPO",
                    value=bool(st.session_state.get("industry_builder_have_blueprint_source_only", True)),
                    key="form_have_bpc",
                )

            with misc_cols[1]:
                group_bpcs = st.toggle(
                    "Group identical BPCs",
                    value=bool(st.session_state.get("industry_builder_group_identical_bpcs", True)),
                    key="form_group_bpcs",
                )
                have_skills = st.toggle(
                    "I have the skills",
                    value=bool(st.session_state.get("industry_builder_have_skills_only", True)),
                    key="form_have_skills",
                )

            with misc_cols[2]:
                build_from_bpc = st.toggle(
                    "Build from BPC",
                    value=bool(st.session_state.get("industry_builder_build_from_bpc", True)),
                    key="form_build_bpc",
                )
                include_reactions = st.toggle(
                    "Include reactions",
                    value=bool(st.session_state.get("industry_builder_include_reactions", False)),
                    disabled=not page_context.reactions_allowed_for_profile,
                    key="form_reactions",
                )

        # MIDDLE COLUMN: Market
        with market_col:
            st.caption("**Market**")
            hub_options = ["jita", "amarr", "dodixie", "rens", "hek"]
            default_hub = str(st.session_state.get("industry_builder_market_hub", "jita"))
            hub_index = hub_options.index(default_hub) if default_hub in hub_options else 0
            market_hub = st.selectbox(
                "Trade Hub",
                options=hub_options,
                format_func=lambda v: {
                    "jita": "Jita 4-4",
                    "amarr": "Amarr VIII (Oris)",
                    "dodixie": "Dodixie IX - Moon 20",
                    "rens": "Rens VI - Moon 8",
                    "hek": "Hek VIII - Moon 12",
                }[v],
                index=hub_index,
                key="form_market_hub",
            )

            material_options = ["sell", "buy"]
            default_material = str(st.session_state.get("industry_builder_material_price_side", "sell"))
            material_index = material_options.index(default_material) if default_material in material_options else 0
            material_side = st.selectbox(
                "Input Pricing",
                options=material_options,
                format_func=lambda v: "Buy from Sell Orders" if v == "sell" else "Buy with Buy Orders",
                index=material_index,
                key="form_material_side",
            )

            product_options = ["sell", "buy"]
            default_product = str(st.session_state.get("industry_builder_product_price_side", "sell"))
            product_index = product_options.index(default_product) if default_product in product_options else 0
            product_side = st.selectbox(
                "Output Pricing",
                options=product_options,
                format_func=lambda v: "Place Sell Orders" if v == "sell" else "Sell to Buy Orders",
                index=product_index,
                key="form_product_side",
            )

        # RIGHT COLUMN: Profit Filters
        with profit_col:
            st.caption("**Profit Filters**")
            positive_only = st.toggle(
                "Positive profit only",
                value=bool(st.session_state.get("industry_builder_positive_profit_only", False)),
                key="form_positive_profit",
            )

            min_margin = st.number_input(
                "Min Margin (%)",
                min_value=0.0,
                step=0.5,
                value=float(st.session_state.get("industry_builder_min_margin_pct", 0.0)),
                key="form_min_margin",
            )

            min_isk_hour = st.number_input(
                "Min ISK/Hour",
                min_value=0.0,
                step=100000.0,
                value=float(st.session_state.get("industry_builder_min_isk_per_hour", 0.0)),
                key="form_min_isk",
            )

            min_volume = st.number_input(
                "Min Region Daily Volume",
                min_value=0,
                step=1,
                value=int(st.session_state.get("industry_builder_min_region_daily_volume", 0)),
                key="form_min_volume",
            )

        # RIGHT-MOST COLUMN: Quality Filters
        with quality_col:
            st.caption("**Exclude Liquidity**")
            _liq_options = ["Very High", "High", "Medium", "Low", "Very Low", "Unknown"]
            _liq_default = list(st.session_state.get("industry_builder_liquidity_exclude", ["Very Low", "Unknown"]))
            liquidity_exclude = st.multiselect(
                "Exclude Liquidity",
                options=_liq_options,
                default=_liq_default,
                key="form_liquidity_exclude",
                label_visibility="collapsed",
            )

            st.caption("**Exclude Price Anomaly Risk**")
            _anomaly_options = ["None", "Low", "Medium", "High"]
            _anomaly_default = list(st.session_state.get("industry_builder_anomaly_exclude", ["High"]))
            anomaly_exclude = st.multiselect(
                "Exclude Price Anomaly Risk",
                options=_anomaly_options,
                default=_anomaly_default,
                key="form_anomaly_exclude",
                label_visibility="collapsed",
            )

        # Form submit button
        submitted = st.form_submit_button("Refresh Overview", use_container_width=True)

    # Apply selectors and filters when form is submitted
    if submitted:
        # Store selector values in session state
        st.session_state["industry_builder_owned_blueprints_scope"] = owned_blueprint_scope
        st.session_state["industry_builder_character_id"] = int(character_id)
        st.session_state["industry_builder_industry_profile_id"] = int(industry_profile_id)

        # Store misc filter values in session state
        st.session_state["industry_builder_maximize_bp_runs_pending"] = maximize_bp_runs
        st.session_state["industry_builder_group_identical_bpcs"] = group_bpcs
        st.session_state["industry_builder_build_from_bpc"] = build_from_bpc
        st.session_state["industry_builder_have_blueprint_source_only"] = have_bpc_bpo
        st.session_state["industry_builder_have_skills_only"] = have_skills
        st.session_state["industry_builder_include_reactions"] = include_reactions
        st.session_state["industry_builder_meta_groups"] = meta_group_selections

        # Store market filter values in session state
        st.session_state["industry_builder_market_hub"] = market_hub
        st.session_state["industry_builder_material_price_side"] = material_side
        st.session_state["industry_builder_product_price_side"] = product_side

        # Store profit filter values in session state
        st.session_state["industry_builder_positive_profit_only"] = positive_only
        st.session_state["industry_builder_min_margin_pct"] = min_margin
        st.session_state["industry_builder_min_isk_per_hour"] = min_isk_hour
        st.session_state["industry_builder_min_region_daily_volume"] = min_volume

        # Store quality filter values in session state
        st.session_state["industry_builder_liquidity_exclude"] = liquidity_exclude
        st.session_state["industry_builder_anomaly_exclude"] = anomaly_exclude

        # Start a refresh job to fetch new overview data with the selected filters
        try:
            start_overview_refresh_job(
                default_character_id_value=page_context.default_character_id_value,
                default_industry_profile_id=page_context.default_industry_profile_id,
                default_owned_blueprint_scope=page_context.default_owned_blueprint_scope,
                reactions_allowed_for_profile=page_context.reactions_allowed_for_profile,
                start_refresh_fn=start_product_overview_refresh,
            )
        except Exception as e:
            st.error(f"Failed to start refresh: {e}")
            return

        # Rerun to show refresh progress
        st.rerun()

    enabled_meta_groups = set(meta_group_selections.keys())
    filtered_overview_rows = filter_overview_rows(
        page_context.overview_rows,
        tuple(sorted(enabled_meta_groups)),
        have_skills,
        positive_only,
        min_margin,
        min_isk_hour,
        min_volume,
        tuple(sorted(liquidity_exclude)) if liquidity_exclude else (),
        tuple(sorted(anomaly_exclude)) if anomaly_exclude else (),
    )

    if not filtered_overview_rows:
        st.info("No manufacturable product rows match the current filters.")
        return

    # Render pricing info with job manager status below form
    overview_meta = cast(dict[str, Any], st.session_state.get("industry_builder_overview_meta") or {})
    job_manager_status = cast(dict[str, Any], st.session_state.get("industry_builder_job_manager_status") or {})
    pricing_panel_col, about_col = st.columns([20, 1])
    with pricing_panel_col:
        render_pricing_batch_panel(
            cast(dict[str, Any], overview_meta.get("pricing_batch") or overview_meta),
            job_manager_status=job_manager_status,
        )
    with about_col:
        _render_overview_columns_about()

    _render_overview_grid(
        runtime=runtime,
        filtered_overview_rows=filtered_overview_rows,
    )
    _render_profitability_drilldown(filtered_overview_rows)
    _render_debug_panel(filtered_overview_rows)

