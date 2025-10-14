import streamlit as st
import requests
import pandas as pd
import os

FLASK_HOST = os.getenv("FLASK_HOST", "localhost")
FLASK_PORT = os.getenv("FLASK_PORT", "5000")
API_BASE = f"http://{FLASK_HOST}:{FLASK_PORT}"

#-- Cached API calls --
@st.cache_data(ttl=3600)
def get_all_materials():
    try:
        r = requests.get(f"{API_BASE}/materials")
        r.raise_for_status()
        return [m["name"] for m in r.json().get("materials", [])]
    except Exception as e:
        st.error(f"Error fetching materials: {e}")
        return []

@st.cache_data(ttl=3600)
def get_all_facilities():
    return api_get("/facilities") or []

#-- API Helpers --
def api_post(path, payload):
    r = requests.post(f"{API_BASE}{path}", json=payload)
    if r.status_code != 200:
        st.error(f"{path} failed: {r.text}")
        return None
    return r.json()

def api_get(path):
    r = requests.get(f"{API_BASE}{path}")
    if r.status_code != 200:
        st.error(f"{path} failed: {r.text}")
        return None
    return r.json()

#-- Main Render Function --
def render(char_manager_all):
    st.header("Ore Calculator (MILP Version)")

    all_materials = get_all_materials()
    st.write("All Minerals: " + ", ".join(all_materials))

    left, right = st.columns([1, 2])

    with right:
        st.subheader("Optimizer Options")
        chars_map = {c.character_id: c.character_name for c in char_manager_all.character_list}
        main_id = getattr(char_manager_all.get_main_character(), "character_id", None)
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
            if opt_resp is None or opt_resp.get("status") != "ok":
                status_box.error("Optimization failed.")
                return
            status_box.success("Optimization complete.")

            solution = opt_resp["solution"]
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
            raw_df = pd.DataFrame(opt_resp.get("raw_comparator", []))

            # Per-ore effective contribution
            df_eff = pd.DataFrame(opt_resp.get("effective_contributions", []))

            surplus_dict = {k: v for k, v in opt_resp["surplus"].items() if (v or 0) > 0}
            df_surplus = None
            if surplus_dict:
                resale = opt_resp.get("resale", {})
                resale_toggle = opt_resp.get("resale_toggle", False)
                df_surplus = pd.DataFrame(
                    [(m, qty, (qty * resale.get(m, 0)) if (resale_toggle and resale) else 0.0)
                     for m, qty in surplus_dict.items()],
                    columns=["Mineral", "Surplus Units", "Resale (80%) ISK" if resale_toggle else "Value (0)"]
                )

            tiered_total_cost = opt_resp.get("tiered_total_cost", 0.0)
            total_cost_with_reprocessing = opt_resp.get("total_cost_with_reprocessing", opt_resp["total_cost"])
            savings = tiered_total_cost - total_cost_with_reprocessing
            savings_pct = (savings / tiered_total_cost * 100) if tiered_total_cost > 0 else 0

            st.session_state["opt_display"] = {
                "total_cost": opt_resp["total_cost"],
                "total_cost_with_reprocessing": total_cost_with_reprocessing,
                "tiered_total_cost": tiered_total_cost,
                "savings": savings,
                "savings_pct": savings_pct,
                "ore_total_volume": df_ore_summary["Volume (m3)"].sum(),
                "total_ore_volume_m3": opt_resp.get("total_ore_volume_m3", 0.0),
                "raw_total_volume": opt_resp.get("raw_total_volume", 0.0),
                "total_raw_volume": opt_resp.get("total_raw_volume", 0.0),
                "reprocessing_fee": opt_resp.get("reprocessing_fee", 0.0),
                "total_yielded_materials": opt_resp.get("total_yielded_materials", {}),
                "df_solution": df_sol,
                "df_ore_summary": df_ore_summary,
                "df_raw": raw_df,
                "df_eff": df_eff,
                "df_surplus": df_surplus,
                "surplus_dict": surplus_dict,
                "show_yield_table": show_yield_table,
                "show_coverage_debug": show_coverage_debug,
                "show_formula": show_formula,
                "ore_yields": opt_resp.get("ore_yields", []),
                "req_mat_prices": opt_resp.get("req_mat_prices", {}),
                "demand_coverage": opt_resp.get("demand_coverage", {}),
            }
            st.session_state["last_opt_resp"] = opt_resp  # <-- store the last response

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
        st.dataframe(
            opt_display["df_solution"].rename(columns={
                "ore_id": "Ore ID", "ore_name": "Ore", "batches": "Batches",
                "batch_size": "Batch Size", "ore_units": "Ore Units", "cost": "Total Cost (ISK)",
                "unit_price": "Unit Price (ISK)"
            }),
            use_container_width=True
        )
        st.caption("Each row shows an ore type selected by the optimizer. 'Ore Units' = Batches × Batch Size.")

        st.markdown("**Ores Purchase Summary**")
        st.dataframe(opt_display["df_ore_summary"], use_container_width=True)
        st.caption("Summary of all ores purchased, including total volume.")

    with raw_col:
        st.subheader("Raw Minerals (Direct Market Purchase)")
        st.dataframe(opt_display["df_raw"], use_container_width=True)
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
            st.dataframe(pd.DataFrame(df_yields), use_container_width=True)
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
            st.dataframe(df_coverage.style.format("{:,.2f}"), use_container_width=True)
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