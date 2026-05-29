import streamlit as st
from typing import Any, cast

from streamlit_ui.components.aggrid_formatters import js_eu_isk_formatter, js_eu_number_formatter, js_icon_cell_renderer
from streamlit_ui.components.formatters import format_duration
from streamlit_ui.state.industry_builder_ui import (
    build_overview_grid_frame,
    build_debug_payload_preview,
    filter_overview_rows,
)
from streamlit_ui.state.industry_snapshot_page import prepare_shared_industry_snapshot_page
from streamlit_ui.components.webpage_ui import require_aggrid


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


def _build_unknown_opening_stock_rows(filtered_overview_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    audit_rows: list[dict[str, Any]] = []
    for row in filtered_overview_rows:
        if not isinstance(row, dict):
            continue
        manufacturing_job = cast(dict[str, Any], row.get("manufacturing_job") or {})
        procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
        if not isinstance(procurement_materials, dict):
            continue
        for material in procurement_materials.values():
            if not isinstance(material, dict):
                continue
            if not bool(material.get("uses_unknown_owned_cost_basis")):
                continue
            audit_rows.append(
                {
                    "Product": str(row.get("type_name") or row.get("type_id") or "Unknown"),
                    "Material": str(material.get("type_name") or material.get("type_id") or "Unknown"),
                    "Qty": int(material.get("quantity") or 0),
                    "Fallback Unit Price": material.get("unit_price"),
                    "Fallback Line Total": material.get("line_total"),
                    "Fallback Source": material.get("price_source"),
                    "Profit": row.get("profit_amount"),
                    "Confidence": row.get("pricing_confidence"),
                    "Overview Row Id": str(row.get("overview_row_id") or ""),
                }
            )
    return sorted(audit_rows, key=lambda item: float(item.get("Fallback Line Total") or 0.0), reverse=True)


def _render_unknown_opening_stock_audit(filtered_overview_rows: list[dict[str, Any]]) -> None:
    audit_rows = _build_unknown_opening_stock_rows(filtered_overview_rows)
    with st.expander("Unknown opening stock audit", expanded=False):
        if not audit_rows:
            st.caption("No current overview rows are taking owned inventory with missing cost basis.")
            return
        total_line_value = sum(float(row.get("Fallback Line Total") or 0.0) for row in audit_rows)
        unique_products = len({str(row.get("Overview Row Id") or "") for row in audit_rows})
        metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
        metric_col_1.metric("Audit Rows", len(audit_rows))
        metric_col_2.metric("Affected Products", unique_products)
        metric_col_3.metric("Fallback Value", f"{total_line_value:,.2f} ISK")
        st.caption(
            "These rows consumed owned inventory, but no owned acquisition cost was available, so job costing fell back to market pricing for those units."
        )
        st.dataframe(audit_rows, width="stretch", hide_index=True)


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

    runtime.aggrid_fn(
        df,
        gridOptions=grid_options,
        update_mode="NO_UPDATE",
        update_on=[],
        allow_unsafe_jscode=True,
        enable_enterprise_modules=True,
        theme="streamlit",
        height=height,
        fit_columns_on_grid_load=True,
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
        )
    except Exception as e:
        st.error(str(e))
        return
    if page_context is None:
        return

    _applied_meta_groups = (
        st.session_state.get("industry_builder_enabled_meta_groups_applied")
        or set(st.session_state.get("industry_builder_enabled_meta_groups_pending") or set())
        or page_context.enabled_meta_groups
    )
    filtered_overview_rows = filter_overview_rows(
        page_context.overview_rows,
        tuple(sorted(_applied_meta_groups)),
        bool(st.session_state.get("industry_builder_have_skills_only_applied", True)),
        bool(st.session_state.get("industry_builder_positive_profit_only_applied", False)),
        float(st.session_state.get("industry_builder_min_margin_pct_applied", 0.0) or 0.0),
        float(st.session_state.get("industry_builder_min_isk_per_hour_applied", 0.0) or 0.0),
        int(st.session_state.get("industry_builder_min_region_daily_volume_applied", 0) or 0),
    )
    if not filtered_overview_rows:
        st.info("No manufacturable product rows match the current filters.")
        return

    _render_overview_grid(
        runtime=runtime,
        filtered_overview_rows=filtered_overview_rows,
    )
    _render_profitability_drilldown(filtered_overview_rows)
    _render_unknown_opening_stock_audit(filtered_overview_rows)
    _render_debug_panel(filtered_overview_rows)

