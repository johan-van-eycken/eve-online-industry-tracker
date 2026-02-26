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

from utils.app_init import load_config, init_db_app
from utils.flask_api import api_get, api_post
from utils.aggrid_formatters import js_eu_isk_formatter, js_eu_number_formatter, js_eu_pct_formatter

#-- Cached API calls --
@st.cache_data(ttl=3600)
def get_all_materials():
    try:
        r = api_get("/materials")
        return [m["name"] for m in r.get("materials", [])]
    except Exception as e:
        st.error(f"Error fetching materials: {e}")
        return []

@st.cache_data(ttl=3600)
def get_all_facilities():
    try:
        r = api_get("/facilities")
        if isinstance(r.get("data"), list):
            return r.get("data")
        return []
    except Exception as e:
        st.error(f"Error fetching facilities: {e}")
        return []

#-- Main Render Function --
def render():
    st.header("Ore Calculator (MILP Version)")

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

    def _render_aggrid_table(
        df_in: pd.DataFrame,
        *,
        isk_cols: list[str] | None = None,
        pct_cols: list[str] | None = None,
        num_cols_0: list[str] | None = None,
        num_cols_2: list[str] | None = None,
        height_max: int = 700,
    ) -> None:
        if df_in is None or df_in.empty:
            st.info("No data.")
            return

        df_tbl = df_in.copy()
        gb = GridOptionsBuilder.from_dataframe(df_tbl)
        gb.configure_default_column(resizable=True, sortable=True, filter=True)

        for c in (isk_cols or []):
            if c in df_tbl.columns:
                gb.configure_column(
                    c,
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_isk_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
                    cellStyle=right,
                    minWidth=120,
                )
        for c in (pct_cols or []):
            if c in df_tbl.columns:
                gb.configure_column(
                    c,
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_pct_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
                    cellStyle=right,
                    minWidth=110,
                )
        for c in (num_cols_0 or []):
            if c in df_tbl.columns:
                gb.configure_column(
                    c,
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=0),
                    cellStyle=right,
                    minWidth=110,
                )
        for c in (num_cols_2 or []):
            if c in df_tbl.columns:
                gb.configure_column(
                    c,
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=js_eu_number_formatter(JsCode=JsCode, locale=eu_locale, decimals=2),
                    cellStyle=right,
                    minWidth=110,
                )

        grid_options = gb.build()
        height = min(height_max, 40 + (len(df_tbl) * 35))
        AgGrid(
            df_tbl,
            gridOptions=grid_options,
            allow_unsafe_jscode=True,
            theme="streamlit",
            height=height,
        )

    all_materials = get_all_materials()
    st.write("All Minerals: " + ", ".join(all_materials))

    left, right = st.columns([1, 2])

    with right:
        st.subheader("Optimizer Options")
        try:
            cfgManager = load_config()
            db = init_db_app(cfgManager)
        except Exception as e:
            st.error(f"Failed to load database: {e}")
            st.stop()

        try:
            df = db.load_df("characters")
        except Exception:
            st.warning("No character data found. Run main.py first.")
            st.stop()

        chars_map = dict(zip(df["character_id"], df["character_name"]))
        main_id = df.loc[df["is_main"] == True, "character_id"].iloc[0] if "is_main" in df.columns and df["is_main"].any() else None
        character_id = st.selectbox(
            "Select Character",
            list(chars_map.keys()),
            index=list(chars_map.keys()).index(main_id) if main_id in chars_map else 0,
            format_func=lambda x: chars_map[x],
            key="opt_character_id"
        )
        all_implants = {
            0: "None",
            1: "Zainou 'Beancounter' Reprocessing RX-801",
            2: "Zainou 'Beancounter' Reprocessing RX-802",
            4: "Zainou 'Beancounter' Reprocessing RX-804"
        }
        implant_pct = st.selectbox(
            "Reprocessing Implants",
            options=list(all_implants.keys()),
            format_func=lambda ipct: all_implants[ipct],
            key="opt_implant_pct"
        )
        all_facilities = get_all_facilities()
        if not all_facilities:
            st.warning("No facilities available. Check your Flask API.")
            st.stop()

        facility_options = {f["id"]: f["name"] for f in all_facilities}
        facility_id = st.selectbox(
            "Facility",
            options=list(facility_options.keys()),
            format_func=lambda fid: facility_options[fid],
            key="opt_facility_id"
        )

        mode = st.selectbox("Mode", ["min_cost"], key="opt_mode")
        with st.expander("Debug Options"):
            show_yield_table = st.checkbox("Show per-ore batch yields (debug)", value=False, key="dbg_yields")
            show_coverage_debug = st.checkbox("Show coverage & surplus breakdown (debug)", value=False, key="dbg_coverage")
            show_formula = st.checkbox("Show reprocessing formula details", value=False, key="dbg_formula")
        only_compressed = st.checkbox("Only use compressed ores", value=False, key="opt_only_compressed")

    with left:
        st.subheader("Material Requirements")
        with st.form("ore_calc_form"):
            demands = {m: st.number_input(m, min_value=0, value=0, step=1000, key=f"req_{m}") for m in all_materials}
            submitted = st.form_submit_button("Optimize")
        if submitted:
            clean_demands = {k: v for k, v in demands.items() if v > 0}
            if not clean_demands:
                st.warning("Enter at least one non-zero demand.")
                return
            st.session_state["last_clean_demands"] = clean_demands

            status_box = st.empty()
            status_box.info("Running ore optimizer (MILP)...")
            with st.spinner("Solving integer program (tiered order allocation)..."):
                opt_resp = api_post("/optimize", {
                    "demands": clean_demands,
                    "character_id": character_id,
                    "implant_pct": implant_pct,
                    "facility_id": facility_id,
                    "mode": mode,
                    "only_compressed": only_compressed,
                })
            if opt_resp is None or opt_resp.get("status") != "success":
                status_box.error(f"Optimization failed: {opt_resp.get('message', 'Unknown error')}")
                return
            status_box.success("Optimization complete.")

            result = opt_resp.get("data", {})
            solution = result.get("solution", [])
            if not solution:
                st.info("No ores selected.")
                return

            # Build solution DF
            df_sol = pd.DataFrame(solution)
            base_cols = ["ore_id", "ore_name", "batches", "batch_size", "ore_units", "cost"]
            if "unit_price" in df_sol.columns:
                base_cols.insert(base_cols.index("cost"), "unit_price")
            elif "avg_unit_price" in df_sol.columns:
                df_sol["unit_price"] = df_sol["avg_unit_price"]
                base_cols.insert(base_cols.index("cost"), "unit_price")
            df_sol = df_sol[[c for c in base_cols if c in df_sol.columns]]

            # --- FIXED Ores Purchase Summary ---
            ore_yields_map = {ore["id"]: ore for ore in opt_resp.get("ore_yields", [])}
            ore_summary_rows = []
            for row in solution:
                ore_id = row["ore_id"]
                batches = row["batches"]
                ore_units = row["ore_units"]
                ore_info = ore_yields_map.get(ore_id, {})
                batch_volume = ore_info.get("batch_volume", 0)
                # Calculate total volume: batches * batch_volume
                ore_volume = batches * batch_volume
                ore_summary_rows.append({
                    "Ore ID": ore_id,
                    "Ore": row["ore_name"],
                    "Ore Units": ore_units,
                    "Avg Unit Price": row.get("unit_price") or row.get("avg_unit_price"),
                    "Total Cost (ISK)": row["cost"],
                    "Volume (m3)": ore_volume
                })
            df_ore_summary = pd.DataFrame(ore_summary_rows)
            if not df_ore_summary.empty and df_ore_summary["Avg Unit Price"].notna().any():
                df_ore_summary["ISK / Ore Unit"] = df_ore_summary["Total Cost (ISK)"] / df_ore_summary["Ore Units"]

            # Depth-aware raw materials comparator
            raw_df = pd.DataFrame(result.get("raw_comparator", []))

            # Per-ore effective contribution
            df_eff = pd.DataFrame(result.get("effective_contributions", []))

            surplus_dict = {k: v for k, v in result.get("surplus", {}).items() if (v or 0) > 0}
            df_surplus = None
            if surplus_dict:
                resale = result.get("resale", {})
                resale_toggle = result.get("resale_toggle", False)
                df_surplus = pd.DataFrame(
                    [(m, qty, (qty * resale.get(m, 0)) if (resale_toggle and resale) else 0.0)
                     for m, qty in surplus_dict.items()],
                    columns=["Mineral", "Surplus Units", "Resale (80%) ISK" if resale_toggle else "Value (0)"]
                )

            tiered_total_cost = result.get("tiered_total_cost", 0.0)
            total_cost_with_reprocessing = result.get("total_cost_with_reprocessing", result["total_cost"])
            savings = tiered_total_cost - total_cost_with_reprocessing
            savings_pct = (savings / tiered_total_cost * 100) if tiered_total_cost > 0 else 0

            st.session_state["opt_display"] = {
                "total_cost": result["total_cost"],
                "total_cost_with_reprocessing": total_cost_with_reprocessing,
                "tiered_total_cost": tiered_total_cost,
                "savings": savings,
                "savings_pct": savings_pct,
                "ore_total_volume": df_ore_summary["Volume (m3)"].sum(),
                "total_ore_volume_m3": result.get("total_ore_volume_m3", 0.0),
                "raw_total_volume": result.get("raw_total_volume", 0.0),
                "total_raw_volume": result.get("total_raw_volume", 0.0),
                "reprocessing_fee": result.get("reprocessing_fee", 0.0),
                "total_yielded_materials": result.get("total_yielded_materials", {}),
                "df_solution": df_sol,
                "df_ore_summary": df_ore_summary,
                "df_raw": raw_df,
                "df_eff": df_eff,
                "df_surplus": df_surplus,
                "surplus_dict": surplus_dict,
                "show_yield_table": show_yield_table,
                "show_coverage_debug": show_coverage_debug,
                "show_formula": show_formula,
                "ore_yields": result.get("ore_yields", []),
                "req_mat_prices": result.get("req_mat_prices", {}),
                "demand_coverage": result.get("demand_coverage", {}),
            }
            st.session_state["last_opt_resp"] = result  # <-- store the last response

    # -------------------------
    # OUTPUT (full width area)
    # -------------------------
    opt_display = st.session_state.get("opt_display")
    opt_resp = st.session_state.get("last_opt_resp")

    if not opt_display or not opt_resp:
        return

    st.markdown("---")
    st.subheader("Cost & Volume Comparison")
    total_cost_with_reprocessing = opt_display["total_cost_with_reprocessing"]
    tiered_cost = opt_display["tiered_total_cost"]
    ore_volume_m3 = opt_display.get("total_ore_volume_m3", 0.0)
    raw_volume_m3 = opt_display.get("total_raw_volume", 0.0)
    reprocessing_fee = opt_display.get("reprocessing_fee", 0.0)

    colc1, colc2, colc3 = st.columns(3)
    colc1.metric("Total Cost incl. Reprocessing (ISK)", f"{total_cost_with_reprocessing:,.2f}", help="Total cost including ore purchase and reprocessing fee.")
    colc1.metric("Ore Volume (m³)", f"{ore_volume_m3:,.2f}" if ore_volume_m3 is not None else "N/A", help="Estimated total ore volume to purchase.")
    colc1.metric("Reprocessing Fee (ISK)", f"{reprocessing_fee:,.2f}", help="ISK fee charged by the facility for reprocessing.")

    colc2.metric("Direct Minerals Cost (ISK)", f"{tiered_cost:,.2f}", help="Total ISK if you buy all minerals directly from the market.")
    colc2.metric("Direct Minerals Volume (m³)", f"{raw_volume_m3:,.2f}" if raw_volume_m3 is not None else "N/A", help="Total volume of minerals if you buy everything directly from the market.")

    colc3.metric("Savings (ISK)", f"{opt_display['savings']:,.2f}", f"{opt_display['savings_pct']:+.2f}%", help="Difference between optimized ore solution and direct mineral purchase.")
    colc3.metric("Savings Volume (m³)", f"{(raw_volume_m3 - ore_volume_m3):,.2f}" if ore_volume_m3 is not None else "N/A", help="Volume difference between direct minerals and optimized ore solution.")

    # Show total yielded minerals if available
    if "total_yielded_materials" in opt_display and opt_display["total_yielded_materials"]:
        st.caption("Total Yielded Materials: " + ", ".join(
            f"{mat}: {qty:,.0f}" for mat, qty in opt_display["total_yielded_materials"].items()
        ))

    # Side-by-side: Optimized Ore Solution / Raw Minerals comparator
    sol_col, raw_col = st.columns(2)
    with sol_col:
        st.subheader("Optimized Ore Solution")
        df_solution = opt_display["df_solution"].rename(
            columns={
                "ore_id": "Ore ID",
                "ore_name": "Ore",
                "batches": "Batches",
                "batch_size": "Batch Size",
                "ore_units": "Ore Units",
                "cost": "Total Cost (ISK)",
                "unit_price": "Unit Price (ISK)",
            }
        )
        _render_aggrid_table(
            df_solution,
            num_cols_0=["Ore ID", "Batches", "Batch Size", "Ore Units"],
            isk_cols=["Unit Price (ISK)", "Total Cost (ISK)"],
        )
        st.caption("Each row shows an ore type selected by the optimizer. 'Ore Units' = Batches × Batch Size.")

        st.markdown("**Ores Purchase Summary**")
        _render_aggrid_table(
            opt_display["df_ore_summary"],
            num_cols_0=["Ore ID", "Ore Units"],
            num_cols_2=["Volume (m3)", "ISK / Ore Unit"],
            isk_cols=["Avg Unit Price", "Total Cost (ISK)"],
        )
        st.caption("Summary of all ores purchased, including total volume.")

    with raw_col:
        st.subheader("Raw Minerals (Direct Market Purchase)")
        _render_aggrid_table(
            opt_display["df_raw"],
            num_cols_0=["quantity", "Qty", "Demand", "Yielded", "Surplus", "Shortfall"],
            isk_cols=["Total Cost (ISK)", "Unit Price (ISK)", "Cost"],
        )
        st.caption("Minerals required if you buy everything directly from the market.")

    # Optional debug sections
    if opt_display["show_yield_table"]:
        st.subheader("Per-Ore Batch Yields (Post-Skills & Facility)")
        df_yields = []
        for ore in opt_display["ore_yields"]:
            for mat, qty in ore.get("batch_yields", {}).items():
                df_yields.append({
                    "Ore": ore["name"],
                    "Ore ID": ore["id"],
                    "Portion Size": ore.get("batch_size"),
                    "Material": mat,
                    "Yield per Batch": qty
                })
        if df_yields:
            _render_aggrid_table(
                pd.DataFrame(df_yields),
                num_cols_0=["Ore ID", "Portion Size", "Yield per Batch"],
            )
        else:
            st.info("No yield data available.")

    if opt_display["show_coverage_debug"]:
        st.subheader("Demand Coverage & Surplus (Debug)")
        demand_coverage = opt_resp.get("demand_coverage", {})
        if demand_coverage:
            # Convert dict to DataFrame
            df_coverage = pd.DataFrame.from_dict(demand_coverage, orient="index")
            # Optional: round for readability
            df_coverage = df_coverage[["demand", "yielded", "surplus", "shortfall"]]
            df_coverage = df_coverage.rename(columns={
                "demand": "Demand",
                "yielded": "Yielded",
                "surplus": "Surplus",
                "shortfall": "Shortfall"
            })
            _render_aggrid_table(
                df_coverage,
                num_cols_2=["Demand", "Yielded", "Surplus", "Shortfall"],
            )
            st.caption("Shows for each mineral: demand, actual yield, surplus (overproduction), and any shortfall.")

    if opt_display["show_formula"]:
        st.subheader("Reprocessing Yield Formula & Multipliers")
        st.markdown("""
            **Reprocessing Yield Formula**

            batch_output = base * FacilityBase
                        * (1 + 0.02*Refining)
                        * (1 + 0.02*ReprocessingEfficiency)
                        * (1 + 0.02*SpecificOreProcessing)
                        * (1 + Rig + Structure + Implants)

            Values shown are final post-skill batch yields.
        """)
        # Show multipliers for the first ore as an example
        if opt_resp["ore_yields"]:
            ore = opt_resp["ore_yields"][0]
            st.write(f"Example Ore: {ore['name']} (ID: {ore['id']})")
            st.write(f"Portion Size: {ore.get('batch_size')}")
            st.write("Batch Yields:")
            st.json(ore.get("batch_yields", {}))