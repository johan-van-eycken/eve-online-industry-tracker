import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]

from streamlit_ui.components.aggrid_formatters import js_eu_isk_formatter, js_eu_number_formatter, js_eu_pct_formatter, js_icon_cell_renderer, js_margin_pct_cell_style
from streamlit_ui.components.assets_data import get_item_image_url as build_item_image_url
from streamlit_ui.components.formatters import format_isk_short
from streamlit_ui.api.market_orders import (
    clear_market_orders_cache,
    fetch_market_orders,
    refresh_market_orders,
    clear_corp_market_orders_cache,
    fetch_corp_market_orders,
    refresh_corp_market_orders,
)
from streamlit_ui.api.client import api_get
from streamlit_ui.components.webpage_ui import AgGridRuntime, aggrid_height, require_aggrid


def _rerun() -> None:
    st.rerun()


def get_item_image_url(order):
    type_id = order.get("type_id")
    type_category = order.get("type_category_name", "")
    is_bpc = order.get("is_blueprint_copy", False)
    if type_category == "Blueprint":
        variation = "bpc" if is_bpc else "bp"
    elif type_category == "Permanent SKIN":
        variation = "skins"
    else:
        variation = "icon"
    return build_item_image_url(
        type_id=type_id,
        type_category_name=type_category,
        is_blueprint_copy=is_bpc,
        size=32,
    )


# ── Stat card grid (same pattern as Realized Profit page) ────────────────────

def _render_stat_card_grid(cards: list[tuple[str, str]]) -> None:
    st.markdown(
        """
        <style>
        .mo-stat-tile {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
            gap: 0;
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 12px;
            overflow: hidden;
            background: rgba(255,255,255,0.02);
        }
        .mo-stat-card {
            padding: 12px 14px;
            min-height: 70px;
            position: relative;
        }
        .mo-stat-card:not(:last-child)::after {
            content: "";
            position: absolute;
            top: 14px;
            right: 0;
            width: 1px;
            height: calc(100% - 28px);
            background: rgba(255,255,255,0.10);
        }
        .mo-stat-label {
            color: #d4d4d8;
            font-size: 0.78rem;
            font-weight: 600;
            line-height: 1.15;
            margin-bottom: 8px;
        }
        .mo-stat-value {
            font-size: 1.3rem;
            font-weight: 700;
            line-height: 1.0;
            color: #f4f4f5;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    cards_html = "".join(
        f"<div class='mo-stat-card'>"
        f"<div class='mo-stat-label'>{label}</div>"
        f"<div class='mo-stat-value'>{value}</div>"
        f"</div>"
        for label, value in cards
    )
    st.markdown(f"<div class='mo-stat-tile'>{cards_html}</div>", unsafe_allow_html=True)


def _colored(value: str, *, color: str) -> str:
    return f"<span style='color:{color}'>{value}</span>"


# ── Summary stats computation ─────────────────────────────────────────────────

def _compute_sell_stats(orders: list[dict]) -> dict:
    count = len(orders)
    total_listed = sum(float(o.get("total_price") or 0) for o in orders)

    # Est. profit at current price: total_price × net_margin% / 100
    # (net_margin = (net_proceeds - cost) / price × 100, so cost-profit = price × qty × margin/100 = total_price × margin/100)
    profit_cur_parts = [
        float(o.get("total_price") or 0) * float(o["net_margin_pct_current"]) / 100
        for o in orders
        if o.get("net_margin_pct_current") is not None and o.get("total_price")
    ]
    est_profit_current = sum(profit_cur_parts) if profit_cur_parts else None

    # Est. profit at advised price: advised_price × volume_remain × margin_adv% / 100
    profit_adv_parts = []
    for o in orders:
        if (
            o.get("advised_price") and
            o.get("net_margin_pct_advised") is not None and
            o.get("total_price") and
            float(o.get("price") or 0) > 0
        ):
            vol_remain = float(o["total_price"]) / float(o["price"])
            adv_total = float(o["advised_price"]) * vol_remain
            profit_adv_parts.append(adv_total * float(o["net_margin_pct_advised"]) / 100)
    est_profit_advised = sum(profit_adv_parts) if profit_adv_parts else None

    # ISK/day totals
    isk_day_cur = [float(o["isk_per_day_current"]) for o in orders if o.get("isk_per_day_current") is not None]
    isk_day_adv = [float(o["isk_per_day_advised"]) for o in orders if o.get("isk_per_day_advised") is not None]
    total_isk_day_current = sum(isk_day_cur) if isk_day_cur else None
    total_isk_day_advised = sum(isk_day_adv) if isk_day_adv else None

    # Weighted-average margin at current price
    margin_w = [
        (float(o["net_margin_pct_current"]), float(o.get("total_price") or 0))
        for o in orders
        if o.get("net_margin_pct_current") is not None and o.get("total_price")
    ]
    avg_margin_current: float | None = None
    if margin_w:
        total_w = sum(w for _, w in margin_w)
        avg_margin_current = sum(m * w for m, w in margin_w) / total_w if total_w > 0 else None

    below_breakeven = sum(1 for o in orders if (o.get("net_margin_pct_current") or 0) < 0)

    return {
        "count": count,
        "total_listed": total_listed,
        "est_profit_current": est_profit_current,
        "est_profit_advised": est_profit_advised,
        "total_isk_day_current": total_isk_day_current,
        "total_isk_day_advised": total_isk_day_advised,
        "avg_margin_current": avg_margin_current,
        "below_breakeven": below_breakeven,
    }


def _render_sell_stats_box(stats: dict) -> None:
    profit_color_cur = "#27ae60" if (stats["est_profit_current"] or 0) >= 0 else "#c0392b"
    profit_color_adv = "#27ae60" if (stats["est_profit_advised"] or 0) >= 0 else "#c0392b"
    margin_color = (
        "#c0392b" if (stats["avg_margin_current"] or 0) < 0
        else "#e67e22" if (stats["avg_margin_current"] or 0) < 5
        else "#27ae60" if (stats["avg_margin_current"] or 0) >= 15
        else "#f4f4f5"
    )
    risk_color = "#c0392b" if stats["below_breakeven"] > 0 else "#27ae60"

    left_cards = [
        ("Active Orders", str(stats["count"])),
        ("Total Listed ISK", format_isk_short(stats["total_listed"])),
        (
            "Avg Margin % (Current)",
            _colored(f"{stats['avg_margin_current']:.1f}%", color=margin_color)
            if stats["avg_margin_current"] is not None else "—",
        ),
        (
            "Orders at Risk",
            _colored(str(stats["below_breakeven"]), color=risk_color),
        ),
    ]
    right_cards = [
        (
            "Est. Profit (Current)",
            _colored(format_isk_short(stats["est_profit_current"]), color=profit_color_cur)
            if stats["est_profit_current"] is not None else "—",
        ),
        (
            "Est. Profit (Advised)",
            _colored(format_isk_short(stats["est_profit_advised"]), color=profit_color_adv)
            if stats["est_profit_advised"] is not None else "—",
        ),
        (
            "Est. ISK/day (Current)",
            format_isk_short(stats["total_isk_day_current"])
            if stats["total_isk_day_current"] is not None else "—",
        ),
        (
            "Est. ISK/day (Advised)",
            format_isk_short(stats["total_isk_day_advised"])
            if stats["total_isk_day_advised"] is not None else "—",
        ),
    ]

    col_l, col_r = st.columns([4, 6])
    with col_l:
        _render_stat_card_grid(left_cards)
    with col_r:
        _render_stat_card_grid(right_cards)


def _render_buy_stats_box(df: pd.DataFrame) -> None:
    cards = [
        ("Active Buy Orders", str(len(df))),
        ("Total Buy Value", format_isk_short(df["Total Price"].sum())),
        ("Total Escrow", format_isk_short(df["Escrow Remaining"].sum())),
    ]
    _render_stat_card_grid(cards)


# ── Grid row builders ─────────────────────────────────────────────────────────

def _build_order_rows(all_orders: list[dict], *, priority_map: dict | None = None) -> tuple[list[dict], list[dict]]:
    sell_orders: list[dict] = []
    buy_orders: list[dict] = []
    for order in all_orders:
        # Core fields — Station/Region/Range deliberately excluded here, added last below
        core = {
            "Owner": order.get("owner", ""),
            "Icon": get_item_image_url(order),
            "Type": order.get("type_name", ""),
            "Price": order.get("price", 0),
            "Price Status": order.get("price_status") or "N/A",
            "Price Difference": order.get("price_difference") or 0,
            "Volume": order.get("volume", 0),
            "Total Price": order.get("total_price", 0),
            "Expires In": order.get("expires_in", ""),
        }
        # Trailing location columns — always last
        location = {
            "Station": order.get("station", ""),
            "Region": order.get("region", ""),
            "Range": order.get("range", ""),
        }

        if order.get("is_buy_order"):
            buy_orders.append({**core, "Min. Volume": order.get("min_volume", 0), "Escrow Remaining": order.get("escrow_remaining", 0), **location})
        else:
            sell_order: dict = dict(core)
            if order.get("advised_price"):
                sell_order["Advised Price"] = order.get("advised_price", 0)
                sell_order["Advice Confidence"] = order.get("advised_price_confidence", "N/A")
            if order.get("estimated_sell_days_advised") is not None:
                sell_order["Est. Days (Adv.)"] = round(float(order["estimated_sell_days_advised"]), 1)
            if order.get("isk_per_day_advised") is not None:
                sell_order["ISK/day (Adv.)"] = order["isk_per_day_advised"]
            if order.get("net_margin_pct_current") is not None:
                sell_order["Margin % (Current)"] = round(float(order["net_margin_pct_current"]), 1)
            if order.get("net_margin_pct_advised") is not None:
                sell_order["Margin % (Advised)"] = round(float(order["net_margin_pct_advised"]), 1)
            # Relist Priority column
            if priority_map is not None:
                key = (order.get("type_name", ""), float(order.get("price") or 0), order.get("station", ""))
                sell_order["Relist Priority"] = priority_map.get(key, "")
            # Location always last
            sell_order.update(location)
            sell_orders.append(sell_order)

    return sell_orders, buy_orders


# ── Grid renderer ─────────────────────────────────────────────────────────────

def _render_orders_grid(
    df: pd.DataFrame,
    *,
    runtime: AgGridRuntime,
    img_renderer: object,
    isk_cols: list[str],
    min_volume: bool,
    key: str,
) -> None:
    gb = runtime.grid_options_builder.from_dataframe(df)
    gb.configure_default_column(resizable=True, sortable=True, filter=True, minWidth=80)
    gb.configure_column("Icon", headerName="", width=46, cellRenderer=img_renderer, suppressSizeToFit=True, resizable=False, sortable=False, filter=False)
    if "Type" in df.columns:
        gb.configure_column("Type", minWidth=140)

    right = {"textAlign": "right"}
    for col in isk_cols:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_isk_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                cellStyle=right,
                minWidth=160,
            )

    if "Volume" in df.columns:
        gb.configure_column("Volume", cellStyle=right, minWidth=90)

    if "Est. Days (Adv.)" in df.columns:
        gb.configure_column(
            "Est. Days (Adv.)",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=1),
            cellStyle=right,
            minWidth=100,
        )

    margin_style = js_margin_pct_cell_style(JsCode=runtime.js_code)
    margin_fmt = js_eu_pct_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=1)
    for col in ("Margin % (Current)", "Margin % (Advised)"):
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=margin_fmt,
                cellStyle=margin_style,
                minWidth=120,
            )

    if min_volume and "Min. Volume" in df.columns:
        gb.configure_column(
            "Min. Volume",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=0),
            cellStyle=right,
            minWidth=90,
        )

    if "Relist Priority" in df.columns:
        priority_style = runtime.js_code(
            """
            function(params) {
                var v = params.value;
                if (v === 'High')   return {color: '#ef4444', fontWeight: '700', textAlign: 'center'};
                if (v === 'Medium') return {color: '#f59e0b', fontWeight: '600', textAlign: 'center'};
                if (v === 'Low')    return {color: '#9ca3af', textAlign: 'center'};
                return {textAlign: 'center'};
            }
            """
        )
        gb.configure_column("Relist Priority", cellStyle=priority_style, minWidth=100, maxWidth=130)

    # Cap location columns so they don't dominate the layout
    for col, max_w in (("Station", 220), ("Region", 160), ("Range", 90)):
        if col in df.columns:
            gb.configure_column(col, minWidth=80, maxWidth=max_w)

    # Auto-size all columns to content. Deferred 150ms so cells have painted first.
    # AG Grid >= 31 merged columnApi into api; fall back to columnApi for older builds.
    auto_size = runtime.js_code(
        """
        function(e) {
            var gridApi = e.api;
            var colApi  = e.columnApi;
            setTimeout(function() {
                if (gridApi && typeof gridApi.autoSizeAllColumns === 'function') {
                    gridApi.autoSizeAllColumns(false);
                } else if (colApi && typeof colApi.autoSizeAllColumns === 'function') {
                    colApi.autoSizeAllColumns(false);
                }
            }, 150);
        }
        """
    )
    gb.configure_grid_options(onFirstDataRendered=auto_size)

    runtime.aggrid_fn(
        df,
        gridOptions=gb.build(),
        allow_unsafe_jscode=True,
        theme="streamlit",
        height=aggrid_height(row_count=len(df), height_max=700),
        key=key,
    )


# ── Pricing analysis panel ────────────────────────────────────────────────────

def _render_pricing_analysis(selected_order: dict) -> None:
    if not selected_order.get("pricing_breakdown"):
        st.info("No pricing analysis available for this order yet.")
        return

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Current Price", format_isk_short(selected_order.get("price", 0)))
        if selected_order.get("advised_price"):
            advised = selected_order.get("advised_price", 0)
            st.metric("Advised Price", format_isk_short(advised))
            diff = advised - selected_order.get("price", 0)
            diff_pct = selected_order.get("price_difference_pct") or 0
            st.metric("Difference", format_isk_short(diff), f"{diff_pct:+.1f}%")

    with col2:
        st.metric("Confidence", selected_order.get("advised_price_confidence", "N/A"))
        st.caption(selected_order.get("pricing_reasoning", ""))

    with col3:
        if selected_order.get("cost_basis"):
            cost_basis = selected_order.get("cost_basis", 0)
            source = selected_order.get("acquisition_source", "unknown")
            cost_source = selected_order.get("cost_basis_source", "asset")

            if cost_source == "market_order_fallback":
                source_label = "Order Price (no cost record found)"
            elif cost_source == "asset_history":
                base = "Built" if source == "manufactured" else "Bought" if source == "bought" else source.title()
                source_label = f"{base} (from history)"
            else:
                source_label = "Built" if source == "manufactured" else "Bought" if source == "bought" else source.title()

            st.metric("Your Cost", format_isk_short(cost_basis))
            if cost_source == "market_order_fallback":
                st.warning("No acquisition cost found — order price used as estimate. Margin figures are unreliable.")
            else:
                st.caption(f"({source_label})")

            break_even = selected_order.get("break_even_price")
            if break_even:
                st.metric("Break-even Price", format_isk_short(break_even))
        else:
            st.caption("No cost data available")

    net_margin_adv = selected_order.get("net_margin_pct_advised")
    net_margin_cur = selected_order.get("net_margin_pct_current")
    est_days_adv = selected_order.get("estimated_sell_days_advised")
    est_days_cur = selected_order.get("estimated_sell_days_current")
    isk_day_adv = selected_order.get("isk_per_day_advised")
    isk_day_cur = selected_order.get("isk_per_day_current")

    if any(v is not None for v in [net_margin_adv, net_margin_cur, est_days_adv, isk_day_adv]):
        st.write("**Profitability (after sales tax & broker fee):**")
        pm1, pm2, pm3 = st.columns(3)

        with pm1:
            if net_margin_adv is not None:
                st.metric("Net Margin (Advised)", f"{net_margin_adv:.1f}%")
            if net_margin_cur is not None:
                delta = (net_margin_adv - net_margin_cur) if net_margin_adv is not None else None
                st.metric("Net Margin (Current)", f"{net_margin_cur:.1f}%",
                          delta=f"{delta:+.1f}pp" if delta is not None else None)

        with pm2:
            if est_days_adv is not None:
                st.metric("Est. Days to Sell (Advised)", f"{est_days_adv:.1f}d")
            if est_days_cur is not None:
                delta = (est_days_adv - est_days_cur) if est_days_adv is not None else None
                st.metric("Est. Days to Sell (Current)", f"{est_days_cur:.1f}d",
                          delta=f"{delta:+.1f}d" if delta is not None else None,
                          delta_color="inverse")

        with pm3:
            if isk_day_adv is not None:
                st.metric("ISK/day (Advised)", format_isk_short(isk_day_adv))
            if isk_day_cur is not None:
                delta = (isk_day_adv - isk_day_cur) if isk_day_adv is not None else None
                st.metric("ISK/day (Current)", format_isk_short(isk_day_cur),
                          delta=format_isk_short(delta) if delta is not None else None)

    expiry_detail = selected_order.get("expiry_urgency_detail")
    if expiry_detail:
        st.warning(
            f"**Expiry urgency:** {expiry_detail.get('reason', '')}"
            + (f"  \nOriginal advised: **{format_isk_short(expiry_detail.get('original_advised'))}** → "
               f"urgency-adjusted: **{format_isk_short(expiry_detail.get('urgency_adjusted_price'))}**")
        )

    hold_signal = selected_order.get("hold_signal")
    if hold_signal and hold_signal.get("suggested"):
        est_price_7d = hold_signal.get("estimated_price_7d")
        total_gain = hold_signal.get("estimated_gain_total", 0)
        st.info(
            f"**Hold suggestion:** {hold_signal.get('reason', '')}"
            + (f"  \nEstimated price in 7 days: **{format_isk_short(est_price_7d)}**" if est_price_7d else "")
            + (f"  \nTotal extra gain if held: **{format_isk_short(total_gain)}**" if total_gain else "")
        )

    relist_risk = selected_order.get("relist_risk")
    if relist_risk and relist_risk.get("at_risk"):
        relist_cost = relist_risk.get("estimated_relist_cost", 0)
        unsold_qty = relist_risk.get("estimated_unsold_quantity", 0)
        st.warning(
            f"**Relist risk:** {relist_risk.get('reason', '')}"
            + (f"  \nEst. {unsold_qty} units unsold at expiry — relist cost ~**{format_isk_short(relist_cost)}**" if relist_cost else "")
        )

    st.write("**Pricing Components:**")
    breakdown = selected_order.get("pricing_breakdown", {})
    components = breakdown.get("components", {})
    for comp_name, comp_data in components.items():
        value = comp_data.get("value", 0)
        weight = comp_data.get("weight", 0) * 100
        friendly_name = comp_name.replace("_", " ").title()
        st.write(f"• {friendly_name}: {format_isk_short(value)} ({weight:.0f}% weight)", unsafe_allow_html=True)
        if comp_name == "your_cost_basis" and comp_data.get("raw_cost"):
            source = selected_order.get("acquisition_source", "unknown")
            source_label = "Built" if source == "manufactured" else "Bought" if source == "bought" else source.title()
            margin = comp_data.get("margin_pct", 5)
            st.caption(f"  Raw cost: {format_isk_short(comp_data['raw_cost'])}, with {margin}% margin ({source_label})")
        elif comp_name == "your_recent_sales" and comp_data.get("sample_size"):
            st.caption(f"  Based on {comp_data['sample_size']} recent sales ({comp_data.get('confidence', 'N/A')} confidence)")
        elif comp_name == "market_trend" and comp_data.get("trend_pct"):
            st.caption(f"  42w avg: {format_isk_short(comp_data.get('avg_42w', 0))}, 7d avg: {format_isk_short(comp_data.get('avg_7d', 0))}, trend: {comp_data['trend_pct']:+.1f}%")
        elif comp_name == "market_hub_price" and comp_data.get("volatility_dampened"):
            st.caption(f"  Volatility-dampened (vol={comp_data.get('volatility_pct', 0):.1f}%) — hub weight reduced to {weight:.0f}%")
        elif comp_name == "supply_demand" and comp_data.get("days_of_supply") is not None:
            st.caption(f"  {comp_data.get('label', '')}")
        elif comp_name == "buy_sell_spread" and comp_data.get("spread_pct") is not None:
            st.caption(f"  {comp_data.get('label', '')}")

    fill_rate = selected_order.get("fill_rate_velocity")
    if fill_rate:
        fr_conf = fill_rate.get("confidence", "low")
        fr_txns = fill_rate.get("transaction_count", 0)
        fr_days = fill_rate.get("day_span", 0)
        fr_raw = fill_rate.get("daily_velocity_raw", 0)
        fr_adj = fill_rate.get("daily_velocity_adjusted", 0)
        fr_median = fill_rate.get("median_historical_price", 0)
        elastic = fill_rate.get("elasticity_factor", 1.0)
        st.write(f"**Historical Fill Rate** ({fr_conf} confidence — {fr_txns} sales over {fr_days}d):")
        st.caption(
            f"Raw velocity: {fr_raw:.2f} units/day  |  "
            f"Adjusted (elasticity {elastic:.2f}×): {fr_adj:.2f} units/day  |  "
            f"Median historical sale price: {format_isk_short(fr_median)}"
        )

    conc = selected_order.get("seller_concentration")
    if conc and conc.get("risk_label"):
        sniper = conc.get("sniper_risk", False)
        icon = "🔴" if sniper else ("🟡" if conc.get("front_2_volume_pct", 0) > 60 else "🟢")
        st.write(f"**Queue Concentration:** {icon} {conc.get('risk_label', '')}")
        if conc.get("note"):
            st.caption(
                f"Top level: {conc.get('front_1_volume_pct', 0):.0f}% of visible volume  |  "
                f"Top 2 levels: {conc.get('front_2_volume_pct', 0):.0f}%  |  "
                f"{conc.get('num_price_levels', 0)} price levels  |  "
                f"{conc.get('note', '')}"
            )

    price_band = selected_order.get("price_band")
    min_margin_pct = selected_order.get("min_target_margin_pct")
    if price_band:
        st.write(f"**Price Band** (min target margin: {min_margin_pct or 8:.1f}%):")
        pb_cols = st.columns(3)
        for tier_key, col in [("aggressive", pb_cols[0]), ("target", pb_cols[1]), ("premium", pb_cols[2])]:
            tier = price_band.get(tier_key, {})
            if not tier:
                continue
            with col:
                st.metric(tier.get("label", tier_key.title()), format_isk_short(tier.get("price")) if tier.get("price") else "—")
                details = []
                if tier.get("estimated_sell_days") is not None:
                    details.append(f"~{tier['estimated_sell_days']:.1f}d to sell")
                if tier.get("isk_per_day") is not None:
                    details.append(f"{format_isk_short(tier['isk_per_day'])}/day")
                if tier.get("net_margin_pct") is not None:
                    details.append(f"{tier['net_margin_pct']:.1f}% margin")
                if details:
                    st.caption("  \n".join(details))


# ── Corp order row builders ───────────────────────────────────────────────────

def _build_corp_order_rows(corp_orders: list[dict]) -> tuple[list[dict], list[dict]]:
    """Build sell/buy row dicts from raw corp order dicts (no enrichment)."""
    sell_orders: list[dict] = []
    buy_orders: list[dict] = []
    for order in corp_orders:
        core = {
            "Owner": order.get("corporation_name", "Corp"),
            "Icon": get_item_image_url(order),
            "Type": order.get("type_name", ""),
            "Price": order.get("price", 0),
            "Volume": str(order.get("volume_remain", 0)) + "/" + str(order.get("volume_total", 0)),
            "Total Price": float(order.get("price") or 0) * float(order.get("volume_remain") or 0),
        }
        location = {
            "Station": order.get("location_name") or f"Location {order.get('location_id', 'Unknown')}",
            "Region": order.get("region_name") or (f"Region {order.get('region_id')}" if order.get("region_id") else "Unknown"),
            "Range": order.get("range", ""),
        }
        if order.get("is_buy_order"):
            buy_orders.append({**core, "Min. Volume": order.get("min_volume", 1), "Escrow Remaining": order.get("escrow", 0), **location})
        else:
            sell_orders.append({**core, **location})
    return sell_orders, buy_orders


def _render_character_orders_tab(runtime: object, img_renderer: object) -> None:
    """Render existing character market orders content (unchanged logic)."""
    all_orders = []
    try:
        response = fetch_market_orders()
        all_orders = response.get("data", [])
    except Exception as e:
        st.error(f"Error fetching market orders: {str(e)}")
        return

    # Build Relist Priority map: (type_name, price, station) → "High" / "Medium" / "Low" / ""
    sell_raw_all = [o for o in all_orders if not o.get("is_buy_order")]
    sell_raw_sorted = sorted(sell_raw_all, key=lambda o: o.get("reprice_priority_score", 0), reverse=True)
    priority_map: dict = {}
    for rank, o in enumerate(sell_raw_sorted, start=1):
        score = o.get("reprice_priority_score", 0)
        label = "" if score <= 0 else "High" if rank <= 3 else "Medium" if rank <= 10 else "Low"
        key = (o.get("type_name", ""), float(o.get("price") or 0), o.get("station", ""))
        priority_map[key] = label

    selected_owner = "All"
    sell_orders, buy_orders = _build_order_rows(all_orders, priority_map=priority_map)

    if sell_orders:
        df = pd.DataFrame(sell_orders)
        df = df.reset_index(drop=True).sort_values(by=["Type", "Price"], ascending=[True, True])

        # Owner filter + refresh button
        owner_col, refresh_col, filler = st.columns([1, 2, 3])
        with owner_col:
            owners = ["All"] + sorted(df["Owner"].unique())
            selected_owner = st.selectbox("Filter by Owner", owners, index=0, label_visibility="hidden")
            if selected_owner != "All":
                df = df[df["Owner"] == selected_owner]
        with refresh_col:
            st.write("<br>", unsafe_allow_html=True)
            if st.button("Refresh Market Orders"):
                with st.spinner("Refreshing market orders..."):
                    try:
                        refresh_market_orders()
                    except Exception as e:
                        st.error(f"Refresh failed: {str(e)}")
                clear_market_orders_cache()
                _rerun()
        with filler:
            st.write("")

        st.subheader("Selling")

        # Stats box — computed from the filtered raw enriched orders
        sell_raw_filtered = [
            o for o in all_orders
            if not o.get("is_buy_order") and (selected_owner == "All" or o.get("owner") == selected_owner)
        ]
        stats = _compute_sell_stats(sell_raw_filtered)
        _render_sell_stats_box(stats)
        st.write("")  # spacing below stat box

        isk_cols = ["Price", "Total Price", "Price Difference"]
        if "Advised Price" in df.columns:
            isk_cols.append("Advised Price")
        if "ISK/day (Adv.)" in df.columns:
            isk_cols.append("ISK/day (Adv.)")

        _render_orders_grid(df, runtime=runtime, img_renderer=img_renderer, isk_cols=isk_cols, min_volume=False, key="market_orders_sell")
    else:
        st.info("No market sell orders found.")

    if buy_orders:
        st.subheader("Buying")
        df_buy = pd.DataFrame(buy_orders)
        df_buy = df_buy.reset_index(drop=True).sort_values(by=["Type", "Price"], ascending=[True, False])
        if selected_owner != "All":
            df_buy = df_buy[df_buy["Owner"] == selected_owner]

        _render_buy_stats_box(df_buy)
        st.write("")

        _render_orders_grid(df_buy, runtime=runtime, img_renderer=img_renderer, isk_cols=["Price", "Total Price", "Price Difference", "Escrow Remaining"], min_volume=True, key="market_orders_buy")
    else:
        st.info("No market buy orders found.")

    # Pricing analysis panel
    if sell_orders:
        st.divider()
        st.subheader("📊 Pricing Analysis")
        full_sell_orders = [o for o in all_orders if not o.get("is_buy_order")]
        if full_sell_orders:
            order_options = [
                f"{o.get('type_name', 'Unknown')} @ {o.get('station', 'Unknown')} - {format_isk_short(o.get('price', 0))}/unit"
                for o in full_sell_orders
            ]
            selected_idx = st.selectbox(
                "Select order to view detailed pricing analysis",
                range(len(order_options)),
                format_func=lambda i: order_options[i],
            )
            _render_pricing_analysis(full_sell_orders[selected_idx])


def _get_director_corp_ids() -> list[int]:
    """Return corporation IDs for all director-managed corporations."""
    try:
        corps_response = api_get("/corporations", timeout_seconds=30) or {}
        corps = corps_response.get("data", []) if isinstance(corps_response, dict) else []
        return [
            int(c["corporation_id"])
            for c in corps
            if isinstance(c, dict) and c.get("has_director_access") and c.get("corporation_id")
        ]
    except Exception:
        return []


def _render_corporation_orders_tab(runtime: object, img_renderer: object) -> None:
    """Render corporation market orders tab."""
    corp_ids = _get_director_corp_ids()

    # Collect orders from each director-access corporation.
    all_corp_data: list[dict] = []
    fetch_errors: list[str] = []
    for corp_id in corp_ids:
        try:
            response = fetch_corp_market_orders(corp_id)
            entry = response.get("data")
            if isinstance(entry, list):
                all_corp_data.extend(entry)
            elif isinstance(entry, dict):
                all_corp_data.append(entry)
        except Exception as e:
            fetch_errors.append(f"Corp {corp_id}: {e}")

    for err in fetch_errors:
        st.error(f"Error fetching corporation market orders: {err}")

    if not corp_ids and not all_corp_data:
        st.info("No director-access corporations found.")
        return

    # Flatten orders from all corporations, annotating each order with corp name.
    all_orders: list[dict] = []
    for corp_entry in all_corp_data:
        corp_name = corp_entry.get("corporation_name", "Unknown Corp")
        for order in corp_entry.get("market_orders", []):
            all_orders.append({**order, "corporation_name": corp_name})

    # Refresh button
    refresh_col, filler = st.columns([2, 4])
    with refresh_col:
        if st.button("Refresh Corp Orders"):
            with st.spinner("Refreshing corporation market orders..."):
                for corp_id in corp_ids:
                    try:
                        refresh_corp_market_orders(corp_id)
                    except Exception as e:
                        st.error(f"Refresh failed for corp {corp_id}: {str(e)}")
            clear_corp_market_orders_cache()
            _rerun()
    with filler:
        st.write("")

    sell_orders, buy_orders = _build_corp_order_rows(all_orders)

    if sell_orders:
        st.subheader("Selling")
        df = pd.DataFrame(sell_orders)
        df = df.reset_index(drop=True).sort_values(by=["Type", "Price"], ascending=[True, True])

        # Simple sell stats
        total_listed = df["Total Price"].sum()
        cards = [
            ("Active Sell Orders", str(len(df))),
            ("Total Listed ISK", format_isk_short(total_listed)),
        ]
        _render_stat_card_grid(cards)
        st.write("")

        _render_orders_grid(
            df,
            runtime=runtime,
            img_renderer=img_renderer,
            isk_cols=["Price", "Total Price"],
            min_volume=False,
            key="corp_market_orders_sell",
        )
    else:
        st.info("No corporation sell orders found.")

    if buy_orders:
        st.subheader("Buying")
        df_buy = pd.DataFrame(buy_orders)
        df_buy = df_buy.reset_index(drop=True).sort_values(by=["Type", "Price"], ascending=[True, False])

        _render_buy_stats_box(df_buy)
        st.write("")

        _render_orders_grid(
            df_buy,
            runtime=runtime,
            img_renderer=img_renderer,
            isk_cols=["Price", "Total Price", "Escrow Remaining"],
            min_volume=True,
            key="corp_market_orders_buy",
        )
    else:
        st.info("No corporation buy orders found.")


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.header("Market Orders")

    runtime = require_aggrid()
    img_renderer = js_icon_cell_renderer(JsCode=runtime.js_code, size_px=24)

    tab_char, tab_corp = st.tabs(["Character Orders", "Corporation Orders"])

    with tab_char:
        _render_character_orders_tab(runtime=runtime, img_renderer=img_renderer)

    with tab_corp:
        _render_corporation_orders_tab(runtime=runtime, img_renderer=img_renderer)
