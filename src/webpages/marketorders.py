import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]
import sys
import traceback

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode  # type: ignore
except Exception:  # pragma: no cover
    AgGrid = None  # type: ignore
    GridOptionsBuilder = None  # type: ignore
    JsCode = None  # type: ignore
    _AGGRID_IMPORT_ERROR = traceback.format_exc()
else:
    _AGGRID_IMPORT_ERROR = None

from utils.flask_api import api_get
from utils.aggrid_formatters import js_eu_isk_formatter, js_eu_number_formatter, js_icon_cell_renderer
from utils.formatters import format_isk_short


def _rerun() -> None:
    st.rerun()


# -- Cached API calls --
@st.cache_data(ttl=3600)
def get_all_orders():
    return api_get("/characters/market_orders") or {}

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

    return f"https://images.evetech.net/types/{type_id}/{variation}?size=32"


# -- Main Render Function --
def render():
    st.header("Market Orders")

    if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
        st.error(
            "streamlit-aggrid is required but could not be imported in this Streamlit process. "
            "Install it in the same Python environment and restart Streamlit."
        )
        st.caption(f"Python: {sys.executable}")
        if _AGGRID_IMPORT_ERROR:
            with st.expander("Import error details", expanded=False):
                st.code(_AGGRID_IMPORT_ERROR)
        st.code(f"{sys.executable} -m pip install streamlit-aggrid")
        st.stop()

    eu_locale = "nl-NL"  # '.' thousands, ',' decimals
    right = {"textAlign": "right"}

    img_renderer = js_icon_cell_renderer(JsCode=JsCode, size_px=24)

    all_orders = []
    try:
        response = get_all_orders()
        all_orders = response.get("data", [])
    except Exception as e:
        st.error(f"Error fetching market orders: {str(e)}")
        return

    selected_owner = "All"

    # Split into sell and buy orders
    sell_orders = []
    buy_orders = []
    for order in all_orders:
        if order.get("is_buy_order"):
            buy_orders.append(
                {
                    "Owner": order.get("owner", ""),
                    "Icon": get_item_image_url(order),
                    "Type": order.get("type_name", ""),
                    "Price": order.get("price", 0),
                    "Price Status": order.get("price_status", "N/A"),
                    "Price Difference": order.get("price_difference", ""),
                    "Volume": order.get("volume", 0),
                    "Total Price": order.get("total_price", 0),
                    "Range": order.get("range", ""),
                    "Min. Volume": order.get("min_volume", 0),
                    "Expires In": order.get("expires_in", ""),
                    "Escrow Remaining": order.get("escrow_remaining", 0),
                    "Station": order.get("station", ""),
                    "Region": order.get("region", ""),
                }
            )
        else:
            sell_orders.append({
                "Owner": order.get("owner", ""),
                "Icon": get_item_image_url(order),
                "Type": order.get("type_name", ""),
                "Price": order.get("price", 0),
                "Price Status": order.get("price_status", "N/A"),
                "Price Difference": order.get("price_difference", ""),
                "Volume": order.get("volume", 0),
                "Total Price": order.get("total_price", 0),
                "Expires In": order.get("expires_in", ""),
                "Station": order.get("station", ""),
                "Region": order.get("region", ""),
                "Range": order.get("range", "")
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

        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(resizable=True, sortable=True, filter=True)
        gb.configure_column("Icon", headerName="", width=60, cellRenderer=img_renderer, suppressSizeToFit=True)
        for c in ["Price", "Total Price", "Price Difference"]:
            if c in df.columns:
                gb.configure_column(
                    c,
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
                    cellStyle=right,
                    minWidth=120,
                )
        for c in ["Volume", "Min. Volume"]:
            if c in df.columns:
                gb.configure_column(
                    c,
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                    cellStyle=right,
                    minWidth=110,
                )

        grid_options = gb.build()
        height = min(700, 40 + (len(df) * 35))
        AgGrid(
            df,
            gridOptions=grid_options,
            allow_unsafe_jscode=True,
            theme="streamlit",
            height=height,
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

        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(resizable=True, sortable=True, filter=True)
        gb.configure_column("Icon", headerName="", width=60, cellRenderer=img_renderer, suppressSizeToFit=True)
        for c in ["Price", "Total Price", "Price Difference", "Escrow Remaining"]:
            if c in df.columns:
                gb.configure_column(
                    c,
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
                    cellStyle=right,
                    minWidth=120,
                )
        for c in ["Volume", "Min. Volume"]:
            if c in df.columns:
                gb.configure_column(
                    c,
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                    cellStyle=right,
                    minWidth=110,
                )

        grid_options = gb.build()
        height = min(700, 40 + (len(df) * 35))
        AgGrid(
            df,
            gridOptions=grid_options,
            allow_unsafe_jscode=True,
            theme="streamlit",
            height=height,
        )
    else:
        st.info("No market buy orders found.")

