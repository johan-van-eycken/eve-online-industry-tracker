import streamlit as st
import pandas as pd

from utils.flask_api import api_get
from utils.formatters import format_isk_short


# -- Cached API calls --
@st.cache_data(ttl=3600)
def get_all_orders():
    return api_get("/refresh_market_orders") or []


def get_item_image_url(order):
    """Get the image URL for a single order item."""
    type_id = order.get("Type ID")
    type_category = order.get("type_category_name", "")
    is_bpc = order.get("is_blueprint_copy", False)

    # Determine image variation
    if type_category == "Blueprint":
        variation = "bpc" if is_bpc else "bp"
    elif type_category == "Permanent SKIN":
        variation = "skins"
    else:
        variation = "icon"

    return f"https://images.evetech.net/types/{type_id}/{variation}?size=32"


# -- Main Render Function --
def render():
    st.header("Market Orders")

    all_orders = []
    try:
        response = get_all_orders()
        all_orders = response.get("data", [])
    except Exception as e:
        st.error(f"Error fetching market orders: {str(e)}")
        return

    # Split into sell and buy orders
    sell_orders = []
    buy_orders = []
    for order in all_orders:
        if order.get("is_buy_order"):
            buy_orders.append(
                {
                    "Owner": order.get("Owner", ""),
                    "Icon": get_item_image_url(order),
                    "Type": order.get("Type", ""),
                    "Price": order.get("Price", 0),
                    "Price Status": order.get("Price Status", "N/A"),
                    "Price Difference": order.get("Price Difference", ""),
                    "Volume": order.get("Volume", 0),
                    "Total Price": order.get("Total Price", 0),
                    "Range": order.get("Range", ""),
                    "Min. Volume": order.get("Min. Volume", 0),
                    "Expires In": order.get("Expires In", ""),
                    "Escrow Remaining": order.get("Escrow Remaining", 0),
                    "Station": order.get("Station", ""),
                    "Region": order.get("Region", ""),
                }
            )
        else:
            sell_orders.append({
                "Owner": order.get("Owner", ""),
                "Icon": get_item_image_url(order),
                "Type": order.get("Type", ""),
                "Price": order.get("Price", 0),
                "Price Status": order.get("Price Status", "N/A"),
                "Price Difference": order.get("Price Difference", ""),
                "Volume": order.get("Volume", 0),
                "Total Price": order.get("Total Price", 0),
                "Expires In": order.get("Expires In", ""),
                "Station": order.get("Station", ""),
                "Region": order.get("Region", ""),
                "Range": order.get("Range", "")
            })
    
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
                st.cache_data.clear()
                st.rerun()
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

        # Sell Orders
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Icon": st.column_config.ImageColumn("", width="small"),
                "Price": st.column_config.NumberColumn("Price", format="%.2f ISK"),
                "Total Price": st.column_config.NumberColumn(
                    "Total Price", format="%.2f ISK"
                ),
                "Price Difference": st.column_config.NumberColumn(
                    "Price Difference", format="%.2f ISK"
                ),
            },
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

        # Buy Orders
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Icon": st.column_config.ImageColumn("", width="small"),
                "Price": st.column_config.NumberColumn("Price", format="%.2f ISK"),
                "Total Price": st.column_config.NumberColumn(
                    "Total Price", format="%.2f ISK"
                ),
                "Price Difference": st.column_config.NumberColumn(
                    "Price Difference", format="%.2f ISK"
                ),
            },
        )
    else:
        st.info("No market buy orders found.")

