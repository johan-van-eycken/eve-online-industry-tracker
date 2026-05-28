import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]

from streamlit_ui.components.aggrid_formatters import js_eu_isk_formatter, js_eu_number_formatter, js_icon_cell_renderer
from streamlit_ui.components.assets_data import get_item_image_url as build_item_image_url
from streamlit_ui.components.formatters import format_isk_short
from streamlit_ui.api.market_orders import clear_market_orders_cache, fetch_market_orders, refresh_market_orders
from streamlit_ui.components.webpage_ui import AgGridRuntime, aggrid_height, require_aggrid


def _rerun() -> None:
    st.rerun()

def get_item_image_url(order):
    """Get the image URL for a single order item."""
    type_id = order.get("type_id")
    type_category = order.get("type_category_name", "")
    is_bpc = order.get("is_blueprint_copy", False)

    # Determine image variation
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


def _build_order_rows(all_orders: list[dict]) -> tuple[list[dict], list[dict]]:
    sell_orders: list[dict] = []
    buy_orders: list[dict] = []
    for order in all_orders:
        common_fields = {
            "Owner": order.get("owner", ""),
            "Icon": get_item_image_url(order),
            "Type": order.get("type_name", ""),
            "Price": order.get("price", 0),
            "Price Status": order.get("price_status") or "N/A",
            "Price Difference": order.get("price_difference") or 0,
            "Volume": order.get("volume", 0),
            "Total Price": order.get("total_price", 0),
            "Expires In": order.get("expires_in", ""),
            "Station": order.get("station", ""),
            "Region": order.get("region", ""),
            "Range": order.get("range", ""),
        }
        if order.get("is_buy_order"):
            buy_orders.append(
                {
                    **common_fields,
                    "Min. Volume": order.get("min_volume", 0),
                    "Escrow Remaining": order.get("escrow_remaining", 0),
                }
            )
        else:
            sell_order = dict(common_fields)
            # Add advised price if available
            if order.get("advised_price"):
                sell_order["Advised Price"] = order.get("advised_price", 0)
                sell_order["Advice Confidence"] = order.get("advised_price_confidence", "N/A")
            sell_orders.append(sell_order)
    return sell_orders, buy_orders


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
    gb.configure_default_column(resizable=True, sortable=True, filter=True)
    gb.configure_column("Icon", headerName="", width=60, cellRenderer=img_renderer, suppressSizeToFit=True)

    right = {"textAlign": "right"}
    for col in isk_cols:
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn", "numberColumnFilter"],
                valueFormatter=js_eu_isk_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=2),
                cellStyle=right,
                minWidth=120,
            )

    if "Volume" in df.columns:
        gb.configure_column("Volume", cellStyle=right, minWidth=110)

    if min_volume and "Min. Volume" in df.columns:
        gb.configure_column(
            "Min. Volume",
            type=["numericColumn", "numberColumnFilter"],
            valueFormatter=js_eu_number_formatter(JsCode=runtime.js_code, locale=runtime.locale, decimals=0),
            cellStyle=right,
            minWidth=110,
        )

    runtime.aggrid_fn(
        df,
        gridOptions=gb.build(),
        allow_unsafe_jscode=True,
        theme="streamlit",
        height=aggrid_height(row_count=len(df), height_max=700),
        key=key,
    )


# -- Main Render Function --
def render():
    st.header("Market Orders")

    runtime = require_aggrid()
    img_renderer = js_icon_cell_renderer(JsCode=runtime.js_code, size_px=24)

    all_orders = []
    try:
        response = fetch_market_orders()
        all_orders = response.get("data", [])
    except Exception as e:
        st.error(f"Error fetching market orders: {str(e)}")
        return

    selected_owner = "All"
    sell_orders, buy_orders = _build_order_rows(all_orders)
    
    if sell_orders:
        # Build DataFrame first
        df = pd.DataFrame(sell_orders)
        df = df.reset_index(drop=True).sort_values(
            by=["Type", "Price"], ascending=[True, True]
        )

        # Filters
        owner, refresh_market_orders, filler = st.columns([1, 2, 3])

        with owner:
            owners = ["All"] + sorted(df["Owner"].unique())
            selected_owner = st.selectbox(
                "Filter by Owner", owners, index=0, label_visibility="hidden"
            )
            if selected_owner != "All":
                df = df[df["Owner"] == selected_owner]
        with refresh_market_orders:
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

        # Overview
        st.write(
            "Active Orders: <strong>{}</strong>&nbsp;&nbsp;Total ISK: <strong>{}</strong>".format(
                len(df), format_isk_short(df["Total Price"].sum())
            ),
            unsafe_allow_html=True,
        )

        isk_cols = ["Price", "Total Price", "Price Difference"]
        if "Advised Price" in df.columns:
            isk_cols.append("Advised Price")

        _render_orders_grid(
            df,
            runtime=runtime,
            img_renderer=img_renderer,
            isk_cols=isk_cols,
            min_volume=False,
            key="market_orders_sell",
        )
    else:
        st.info("No market sell orders found.")

    if buy_orders:
        st.subheader("Buying")
        df = pd.DataFrame(buy_orders)
        df = df.reset_index(drop=True).sort_values(
            by=["Type", "Price"], ascending=[True, False]
        )

        # Filters
        if selected_owner != "All":
            df = df[df["Owner"] == selected_owner]

        # Overview
        st.write(
            "Active Orders: <strong>{}</strong>&nbsp;&nbsp;Total ISK: <strong>{}</strong>&nbsp;&nbsp;Total Escrow: <strong>{}</strong>".format(
                len(df),
                format_isk_short(df["Total Price"].sum()),
                format_isk_short(df["Escrow Remaining"].sum()),
            ),
            unsafe_allow_html=True,
        )

        _render_orders_grid(
            df,
            runtime=runtime,
            img_renderer=img_renderer,
            isk_cols=["Price", "Total Price", "Price Difference", "Escrow Remaining"],
            min_volume=True,
            key="market_orders_buy",
        )
    else:
        st.info("No market buy orders found.")

    # Details section for pricing analysis (sell orders)
    if sell_orders:
        st.divider()
        st.subheader("📊 Pricing Analysis")

        # Rebuild full dataframe with order data for selection
        full_sell_orders = []
        for order in all_orders:
            if not order.get("is_buy_order"):
                full_sell_orders.append(order)

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

            selected_order = full_sell_orders[selected_idx]

            if selected_order.get("pricing_breakdown"):
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Current Price", format_isk_short(selected_order.get("price", 0)))
                    if selected_order.get("advised_price"):
                        advised = selected_order.get("advised_price", 0)
                        st.metric("Advised Price", format_isk_short(advised))
                        diff = advised - selected_order.get("price", 0)
                        diff_pct = selected_order.get("price_difference_pct", 0)
                        st.metric("Difference", format_isk_short(diff), f"{diff_pct:+.1f}%")

                with col2:
                    st.metric("Confidence", selected_order.get("advised_price_confidence", "N/A"))
                    st.caption(selected_order.get("pricing_reasoning", ""))

                with col3:
                    if selected_order.get("cost_basis"):
                        cost_basis = selected_order.get("cost_basis", 0)
                        source = selected_order.get("acquisition_source", "unknown")
                        source_label = "Built" if source == "manufactured" else "Bought" if source == "bought" else source.title()
                        st.metric("Your Cost", format_isk_short(cost_basis))
                        st.caption(f"({source_label})")
                    else:
                        st.caption("No cost data")

                # Show breakdown components
                st.write("**Pricing Components:**")
                breakdown = selected_order.get("pricing_breakdown", {})
                components = breakdown.get("components", {})

                for comp_name, comp_data in components.items():
                    value = comp_data.get("value", 0)
                    weight = comp_data.get("weight", 0) * 100

                    friendly_name = comp_name.replace("_", " ").title()
                    st.write(
                        f"• {friendly_name}: {format_isk_short(value)} ({weight:.0f}% weight)",
                        unsafe_allow_html=True
                    )

                    if comp_name == "your_cost_basis" and comp_data.get("raw_cost"):
                        source = selected_order.get("acquisition_source", "unknown")
                        source_label = "Built" if source == "manufactured" else "Bought" if source == "bought" else source.title()
                        margin = comp_data.get("margin_pct", 5)
                        st.caption(f"  Raw cost: {format_isk_short(comp_data['raw_cost'])}, with {margin}% margin ({source_label})")
                    elif comp_name == "your_recent_sales" and comp_data.get("sample_size"):
                        st.caption(f"  Based on {comp_data['sample_size']} recent sales ({comp_data.get('confidence', 'N/A')} confidence)")
                    elif comp_name == "market_trend" and comp_data.get("trend_pct"):
                        st.caption(f"  42w avg: {format_isk_short(comp_data.get('avg_42w', 0))}, 7d avg: {format_isk_short(comp_data.get('avg_7d', 0))}, trend: {comp_data['trend_pct']:+.1f}%")
            else:
                st.info("No pricing analysis available for this order yet.")

