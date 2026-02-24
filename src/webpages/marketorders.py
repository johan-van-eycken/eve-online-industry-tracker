import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode  # type: ignore
except Exception:  # pragma: no cover
    AgGrid = None  # type: ignore
    GridOptionsBuilder = None  # type: ignore
    JsCode = None  # type: ignore

from utils.flask_api import api_get
from utils.formatters import format_isk_short

from webpages.industry_builder_utils import attach_aggrid_autosize


def _rerun() -> None:
    st.rerun()


# -- Cached API calls --
@st.cache_data(ttl=3600)
def get_all_orders():
    return api_get("/characters/market_orders") or []

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

    def _render_orders_aggrid(df: pd.DataFrame) -> None:
        if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
            st.dataframe(
                df,
                width="stretch",
                hide_index=True,
                column_config={
                    "Icon": st.column_config.ImageColumn("", width="small"),
                    "Price": st.column_config.NumberColumn("Price", format="%.2f ISK"),
                    "Total Price": st.column_config.NumberColumn("Total Price", format="%.2f ISK"),
                    "Price Difference": st.column_config.NumberColumn("Price Difference", format="%.2f ISK"),
                    "Escrow Remaining": st.column_config.NumberColumn("Escrow Remaining", format="%.2f ISK"),
                },
            )
            return

        eu_locale = "nl-NL"  # '.' thousands, ',' decimals

        img_renderer = JsCode(
            """
                (function() {
                    function IconRenderer() {}

                    IconRenderer.prototype.init = function(params) {
                        this.eGui = document.createElement('div');
                        this.eGui.style.display = 'flex';
                        this.eGui.style.alignItems = 'center';
                        this.eGui.style.justifyContent = 'center';
                        this.eGui.style.width = '100%';

                        this.eImg = document.createElement('img');
                        this.eImg.style.width = '28px';
                        this.eImg.style.height = '28px';
                        this.eImg.style.display = 'block';
                        this.eImg.src = params.value ? String(params.value) : '';

                        this.eGui.appendChild(this.eImg);
                    };

                    IconRenderer.prototype.getGui = function() {
                        return this.eGui;
                    };

                    IconRenderer.prototype.refresh = function(params) {
                        if (this.eImg) {
                            this.eImg.src = params.value ? String(params.value) : '';
                        }
                        return true;
                    };

                    return IconRenderer;
                })()
            """
        )

        def _js_eu_number(decimals: int) -> JsCode:
            return JsCode(
                f"""
                    function(params) {{
                        if (params.value === null || params.value === undefined || params.value === "") return "";
                        const n = Number(params.value);
                        if (isNaN(n)) return "";
                        return new Intl.NumberFormat('{eu_locale}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n);
                    }}
                """
            )

        def _js_eu_isk(decimals: int) -> JsCode:
            return JsCode(
                f"""
                    function(params) {{
                        if (params.value === null || params.value === undefined || params.value === "") return "";
                        const n = Number(params.value);
                        if (isNaN(n)) return "";
                        return new Intl.NumberFormat('{eu_locale}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n) + ' ISK';
                    }}
                """
            )

        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)

        # Legacy column removed from backend, but hide defensively if present.
        if "Price Status" in df.columns:
            gb.configure_column("Price Status", hide=True)

        if "Icon" in df.columns:
            gb.configure_column(
                "Icon",
                header_name="",
                width=56,
                pinned="left",
                sortable=False,
                filter=False,
                suppressAutoSize=True,
                cellRenderer=img_renderer,
            )

        right = {"textAlign": "right"}

        for c in ["Price", "Total Price", "Price Difference", "Escrow Remaining"]:
            if c in df.columns:
                if c == "Price Difference":
                    gb.configure_column(
                        c,
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=_js_eu_isk(2),
                        minWidth=160,
                        cellStyle=JsCode(
                            """
                            function(params) {
                                try {
                                    var v = params.value;
                                    var n = Number(v);
                                    if (v === null || v === undefined || v === '' || isNaN(n) || n === 0) {
                                        return { textAlign: 'right' };
                                    }
                                    // Positive = best price, Negative = undercut.
                                    if (n > 0) {
                                        return { textAlign: 'right', color: '#2ecc71', fontWeight: '600' };
                                    }
                                    return { textAlign: 'right', color: '#e74c3c', fontWeight: '600' };
                                } catch (e) {
                                    return { textAlign: 'right' };
                                }
                            }
                            """
                        ),
                    )
                else:
                    gb.configure_column(
                        c,
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=_js_eu_isk(2),
                        minWidth=140,
                        cellStyle=right,
                    )

        for c in ["Volume", "Min. Volume"]:
            if c in df.columns:
                if c == "Volume":
                    # Volume is a string like "12/34" (remain/total).
                    gb.configure_column(
                        c,
                        minWidth=120,
                        cellStyle=right,
                    )
                else:
                    gb.configure_column(
                        c,
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=_js_eu_number(0),
                        minWidth=110,
                        cellStyle=right,
                    )

        grid_options = gb.build()
        attach_aggrid_autosize(grid_options, JsCode=JsCode)
        grid_options["autoSizeStrategy"] = {"type": "fitCellContents"}

        # Row color coding based on expiry window.
        # - red: expires within 5 days (or already expired)
        # - orange: expires within 14 days
        if "Expires In" in df.columns:
            grid_options["getRowStyle"] = JsCode(
                r"""
                function(params) {
                    try {
                        if (!params || !params.data) return null;
                        var raw = params.data['Expires In'];
                        if (raw === null || raw === undefined) return null;
                        var s = String(raw);
                        if (s.toLowerCase().indexOf('expired') >= 0) {
                            return { backgroundColor: 'rgba(231, 76, 60, 0.10)' };
                        }
                        var d = 0, h = 0, m = 0;
                        var md = s.match(/(\d+)\s*d/);
                        var mh = s.match(/(\d+)\s*h/);
                        var mm = s.match(/(\d+)\s*m/);
                        if (md && md[1]) d = Number(md[1]);
                        if (mh && mh[1]) h = Number(mh[1]);
                        if (mm && mm[1]) m = Number(mm[1]);
                        if (isNaN(d) || isNaN(h) || isNaN(m)) return null;

                        var days = d + (h / 24.0) + (m / 1440.0);
                        if (days <= 5) {
                            return { backgroundColor: 'rgba(231, 76, 60, 0.10)' };
                        }
                        if (days <= 14) {
                            return { backgroundColor: 'rgba(243, 156, 18, 0.12)' };
                        }
                        return null;
                    } catch (e) {
                        return null;
                    }
                }
                """
            )

        height = min(650, 40 + (len(df) * 32))
        AgGrid(
            df,
            gridOptions=grid_options,
            allow_unsafe_jscode=True,
            theme="streamlit",
            height=height,
        )

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
                    "Owner": order.get("owner", ""),
                    "Icon": get_item_image_url(order),
                    "Type": order.get("type_name", ""),
                    "Price": order.get("price", 0),
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

        # Sell Orders
        _render_orders_aggrid(df)
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
        _render_orders_aggrid(df)
    else:
        st.info("No market buy orders found.")

