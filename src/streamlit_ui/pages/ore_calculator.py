import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]
from datetime import datetime

from streamlit_ui.api.client import api_post
from streamlit_ui.api.ore_calculator import fetch_all_facilities, fetch_all_materials
from streamlit_ui.components.selectors import select_character_id
from streamlit_ui.components.webpage_ui import render_aggrid_table, require_aggrid


MODE_OPTIONS = {
    "min_cost": "Lowest ISK cost",
    "min_volume": "Lowest hauling volume",
    "min_ore_types": "Fewest ore types",
    "balanced": "Balanced cost and volume",
}


def _format_timestamp(ts: float | int | None) -> str:
    if ts is None:
        return "N/A"
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "N/A"


def _build_sourcing_plan_rows(solution: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for solution_row in solution:
        ore_name = str(solution_row.get("ore_name") or solution_row.get("ore_id") or "")
        for index, tier in enumerate(solution_row.get("tiers") or [], start=1):
            if not isinstance(tier, dict):
                continue
            rows.append(
                {
                    "Ore": ore_name,
                    "Tier": index,
                    "Order ID": tier.get("order_id"),
                    "Location ID": tier.get("location_id"),
                    "Batches": tier.get("batches"),
                    "Ore Units": tier.get("ore_units"),
                    "Unit Price (ISK)": tier.get("unit_price"),
                    "Cost (ISK)": tier.get("cost"),
                }
            )
    return rows

#-- Main Render Function --
def render():
    st.header("Ore Calculator (MILP Version)")

    runtime = require_aggrid()

    try:
        all_materials = fetch_all_materials()
    except Exception as e:
        st.error(f"Error fetching materials: {e}")
        return

    st.write("All Minerals: " + ", ".join(all_materials))

    left, right = st.columns([1, 2])

    with right:
        st.subheader("Optimizer Options")
        character_id = select_character_id(
            label="Select Character",
            key="opt_character_id",
            default_to_main=True,
        )
        if character_id is None:
            st.stop()
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
        try:
            all_facilities = fetch_all_facilities()
        except Exception as e:
            st.error(f"Error fetching facilities: {e}")
            return

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

        mode = st.selectbox(
            "Mode",
            options=list(MODE_OPTIONS.keys()),
            format_func=lambda value: MODE_OPTIONS.get(str(value), str(value)),
            key="opt_mode",
        )
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
            ore_yields_map = {ore["id"]: ore for ore in result.get("ore_yields", [])}
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
            total_cost_with_reprocessing = result.get(
                "total_cost_with_reprocessing",
                float(result["total_cost"]) + float(result.get("reprocessing_fee", 0.0) or 0.0),
            )
            savings = tiered_total_cost - total_cost_with_reprocessing
            savings_pct = (savings / tiered_total_cost * 100) if tiered_total_cost > 0 else 0
            sourcing_plan_df = pd.DataFrame(_build_sourcing_plan_rows(solution))
            price_provenance = result.get("price_provenance", {}) or {}

            st.session_state["opt_display"] = {
                "total_cost": result["total_cost"],
                "total_cost_with_reprocessing": total_cost_with_reprocessing,
                "optimization_mode": result.get("optimization_mode", mode),
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
                "df_sourcing_plan": sourcing_plan_df,
                "surplus_dict": surplus_dict,
                "show_yield_table": show_yield_table,
                "show_coverage_debug": show_coverage_debug,
                "show_formula": show_formula,
                "ore_yields": result.get("ore_yields", []),
                "req_mat_prices": result.get("req_mat_prices", {}),
                "demand_coverage": result.get("demand_coverage", {}),
                "price_provenance": price_provenance,
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
    st.caption(f"Optimization mode: {MODE_OPTIONS.get(str(opt_display.get('optimization_mode')), str(opt_display.get('optimization_mode')))}")

    price_provenance = opt_display.get("price_provenance") or {}
    if price_provenance:
        with st.expander("Price provenance and freshness", expanded=False):
            source = str(price_provenance.get("source") or "ESI sell orders")
            region_id = price_provenance.get("region_id")
            generated_at = _format_timestamp(price_provenance.get("generated_at"))
            st.caption(f"Source: {source} | Region: {region_id or 'N/A'} | Generated: {generated_at}")

            ore_orders = price_provenance.get("ore_orders") or {}
            material_orders = price_provenance.get("material_orders") or {}
            meta_left, meta_right = st.columns(2)
            with meta_left:
                st.markdown("**Ore order snapshot**")
                st.write(
                    {
                        "type_count": ore_orders.get("type_count"),
                        "cached_type_count": ore_orders.get("cached_type_count"),
                        "total_orders": ore_orders.get("total_orders"),
                        "cache_ttl_seconds": ore_orders.get("cache_ttl_seconds"),
                        "oldest_fetched_at": _format_timestamp(ore_orders.get("oldest_fetched_at")),
                        "newest_fetched_at": _format_timestamp(ore_orders.get("newest_fetched_at")),
                    }
                )
            with meta_right:
                st.markdown("**Material order snapshot**")
                st.write(
                    {
                        "type_count": material_orders.get("type_count"),
                        "cached_type_count": material_orders.get("cached_type_count"),
                        "total_orders": material_orders.get("total_orders"),
                        "cache_ttl_seconds": material_orders.get("cache_ttl_seconds"),
                        "oldest_fetched_at": _format_timestamp(material_orders.get("oldest_fetched_at")),
                        "newest_fetched_at": _format_timestamp(material_orders.get("newest_fetched_at")),
                    }
                )

            price_rows = pd.DataFrame(price_provenance.get("price_rows") or [])
            if not price_rows.empty:
                render_aggrid_table(
                    price_rows,
                    runtime=runtime,
                    isk_cols=["Unit Price (ISK)"],
                    number_cols_0=["Type ID"],
                )

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
        render_aggrid_table(
            df_solution,
            runtime=runtime,
            isk_cols=["Unit Price (ISK)", "Total Cost (ISK)"],
            number_cols_0=["Ore ID", "Batches", "Batch Size", "Ore Units"],
        )
        st.caption("Each row shows an ore type selected by the optimizer. 'Ore Units' = Batches × Batch Size.")

        st.markdown("**Ores Purchase Summary**")
        render_aggrid_table(
            opt_display["df_ore_summary"],
            runtime=runtime,
            isk_cols=["Avg Unit Price", "Total Cost (ISK)"],
            number_cols_0=["Ore ID", "Ore Units"],
            number_cols_2=["Volume (m3)", "ISK / Ore Unit"],
        )
        st.caption("Summary of all ores purchased, including total volume.")

        if not opt_display["df_sourcing_plan"].empty:
            st.markdown("**Order-book sourcing plan**")
            render_aggrid_table(
                opt_display["df_sourcing_plan"],
                runtime=runtime,
                isk_cols=["Unit Price (ISK)", "Cost (ISK)"],
                number_cols_0=["Tier", "Order ID", "Location ID", "Batches", "Ore Units"],
            )
            st.caption("Shows which sell-order tiers were consumed for each ore in the optimized solution.")

    with raw_col:
        st.subheader("Raw Minerals (Direct Market Purchase)")
        render_aggrid_table(
            opt_display["df_raw"],
            runtime=runtime,
            isk_cols=["Total Cost (ISK)", "Unit Price (ISK)", "Cost"],
            number_cols_0=["quantity", "Qty", "Demand", "Yielded", "Surplus", "Shortfall"],
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
            render_aggrid_table(
                pd.DataFrame(df_yields),
                runtime=runtime,
                number_cols_0=["Ore ID", "Portion Size", "Yield per Batch"],
            )
        else:
            st.info("No yield data available.")

    if opt_display["show_coverage_debug"]:
        st.subheader("Demand Coverage & Surplus (Debug)")
        demand_coverage = opt_display.get("demand_coverage", {})
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
            render_aggrid_table(
                df_coverage,
                runtime=runtime,
                number_cols_2=["Demand", "Yielded", "Surplus", "Shortfall"],
            )
            st.caption("Shows for each mineral: demand, actual yield, surplus (overproduction), and any shortfall.")

    if opt_display.get("df_eff") is not None and not opt_display["df_eff"].empty:
        st.subheader("Effective Ore Contributions")
        render_aggrid_table(
            opt_display["df_eff"],
            runtime=runtime,
            number_cols_0=["Batches"],
            number_cols_2=["Yield per Batch", "Total Yield", "Demand"],
            pct_cols=["Coverage %", "Share of Yielded %"],
        )
        st.caption("Shows how each selected ore contributes to each demanded mineral.")

    if opt_display.get("df_surplus") is not None and opt_display["df_surplus"] is not None and not opt_display["df_surplus"].empty:
        st.subheader("Surplus Minerals")
        render_aggrid_table(
            opt_display["df_surplus"],
            runtime=runtime,
            isk_cols=[column for column in opt_display["df_surplus"].columns if "ISK" in str(column)],
            number_cols_2=["Surplus Units"],
        )
        st.caption("Surplus output after fulfilling demand, including an 80% resale estimate when price data is available.")

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
        if opt_display["ore_yields"]:
            ore = opt_display["ore_yields"][0]
            st.write(f"Example Ore: {ore['name']} (ID: {ore['id']})")
            st.write(f"Portion Size: {ore.get('batch_size')}")
            st.write("Batch Yields:")
            st.json(ore.get("batch_yields", {}))