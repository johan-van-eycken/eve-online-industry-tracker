import streamlit as st
import requests
import pandas as pd
import os
import time

FLASK_HOST = os.getenv("FLASK_HOST", "localhost")
FLASK_PORT = os.getenv("FLASK_PORT", "5000")
API_BASE = f"http://{FLASK_HOST}:{FLASK_PORT}"

MINERALS = ["Tritanium", "Pyerite", "Mexallon", "Isogen", "Nocxium", "Zydrine", "Megacyte"]

def api_post(path, payload):
    r = requests.post(f"{API_BASE}{path}", json=payload)
    if r.status_code != 200:
        st.error(f"{path} failed: {r.text}")
        return None
    return r.json()

def render(cfg, char_manager_all):
    st.header("Ore Calculator (MILP Version)")

    # Two input columns (left demands, right options)
    left, right = st.columns([1, 2])

    with right:
        st.subheader("Optimizer Options")
        resale_toggle = st.checkbox("Assume 80% resale of surplus", value=False, key="opt_resale_toggle")
        surplus_penalty = st.number_input(
            "Surplus penalty (ISK/unit) when NOT reselling",
            min_value=0.0, value=0.0, step=0.5,
            help="Applied per surplus mineral unit if resale is OFF.",
            key="opt_surplus_penalty"
        )
        facility_id = st.number_input("Facility ID", min_value=1, value=1, step=1, key="opt_facility_id")
        mode = st.selectbox("Mode", ["min_cost"], key="opt_mode")

        exclude_moon = st.checkbox("Exclude moon ores", value=True, key="opt_exclude_moon")
        max_ores = st.number_input("Max distinct ores (0 = no cap)", min_value=0, value=0, step=1, key="opt_max_ores")
        sparsity_penalty_ui = st.number_input(
            "Sparsity penalty per ore (ISK, soft)",
            min_value=0.0, value=0.0, step=1000.0,
            help="Adds this ISK penalty for each distinct ore used.",
            key="opt_sparsity_penalty"
        )

        show_yield_table = st.checkbox("Show per-ore batch yields (debug)", value=False, key="dbg_yields")
        show_coverage_debug = st.checkbox("Show coverage & surplus breakdown (debug)", value=False, key="dbg_coverage")
        show_formula = st.checkbox("Show reprocessing formula details", value=False, key="dbg_formula")

        st.subheader("Character")
        chars_map = {c.character_id: c.character_name for c in char_manager_all.character_list}
        main_id = getattr(char_manager_all.get_main_character(), "character_id", None)
        character_id = st.selectbox(
            "Select Character",
            list(chars_map.keys()),
            index=list(chars_map.keys()).index(main_id) if main_id in chars_map else 0,
            format_func=lambda x: chars_map[x],
            key="opt_character_id"
        )

    with left:
        st.subheader("Material Requirements")
        demands = {m: st.number_input(m, min_value=0, value=0, step=1000, key=f"req_{m}") for m in MINERALS}

        if st.button("Optimize", key="opt_btn"):
            clean_demands = {k: v for k, v in demands.items() if v > 0}
            if not clean_demands:
                st.warning("Enter at least one non-zero demand.")
                return
            st.session_state["last_clean_demands"] = clean_demands

            status_box = st.empty()
            status_box.info("Fetching reprocessing yields...")
            with st.spinner("Calculating per-batch reprocessing yields..."):
                yields_resp = api_post("/reprocessing/yield", {
                    "character_id": character_id,
                    "facility_id": facility_id
                })
            if yields_resp is None:
                status_box.error("Failed fetching yields.")
                return

            ores = yields_resp["ores"]
            ores_dict = {o["id"]: o for o in ores}
            ore_yields = {o["id"]: {"portionSize": o["portionSize"], "batch_yields": o["batch_yields"]} for o in ores}
            st.session_state["ore_yields"] = ore_yields
            ore_ids = [o["id"] for o in ores]

            status_box.info("Fetching market order ladders (minerals & ores)...")
            with st.spinner("Pulling market prices and order depth..."):
                price_resp = api_post("/market/prices", {"minerals": MINERALS, "ores": ore_ids})
            if price_resp is None:
                status_box.error("Failed fetching market prices.")
                return
            mineral_prices = {m: price_resp["minerals"][m]["best_price"] for m in MINERALS if m in price_resp["minerals"]}

            resale = None
            if resale_toggle:
                resale = {m: mineral_prices.get(m, 0) * 0.8 for m in MINERALS}

            status_box.info("Running ore optimizer (MILP)...")
            with st.spinner("Solving integer program (tiered order allocation)..."):
                opt_resp = api_post("/optimize", {
                    "demands": clean_demands,
                    "character_id": character_id,
                    "facility_id": facility_id,
                    "ore_ids": ore_ids,
                    "mode": mode,
                    "resale": resale,
                    "surplus_penalty": 0.0 if resale_toggle else surplus_penalty,
                    "exclude_moon_ores": exclude_moon,
                    "max_ores": max_ores if max_ores > 0 else None,
                    "sparsity_penalty": sparsity_penalty_ui
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
            base_cols = ["ore_id", "ore_name", "batches", "portionSize", "ore_units", "cost"]
            if "unit_price" in df_sol.columns:
                base_cols.insert(base_cols.index("cost"), "unit_price")
            elif "avg_unit_price" in df_sol.columns:
                df_sol["unit_price"] = df_sol["avg_unit_price"]
                base_cols.insert(base_cols.index("cost"), "unit_price")
            df_sol = df_sol[[c for c in base_cols if c in df_sol.columns]]
            df_sol["ISK per Ore Unit"] = df_sol["cost"] / df_sol["ore_units"]

            # Ores Purchase Summary
            ore_summary_rows = []
            for row in solution:
                oid = row["ore_id"]
                vol_per_unit = ores_dict.get(oid, {}).get("volume")
                ore_volume = (row["ore_units"] * vol_per_unit) if vol_per_unit else None
                ore_summary_rows.append({
                    "Ore": row["ore_name"],
                    "Ore ID": oid,
                    "Batches": row["batches"],
                    "Ore Units": row["ore_units"],
                    "Avg Unit Price": row.get("unit_price") or row.get("avg_unit_price"),
                    "Total Cost (ISK)": row["cost"],
                    "Volume (m3)": ore_volume
                })
            df_ore_summary = pd.DataFrame(ore_summary_rows)
            if not df_ore_summary.empty and df_ore_summary["Avg Unit Price"].notna().any():
                df_ore_summary["ISK / Ore Unit"] = df_ore_summary["Total Cost (ISK)"] / df_ore_summary["Ore Units"]

            # Depth-aware raw minerals comparator
            tiered_detail = []
            tiered_total_cost = 0.0
            tiered_total_volume = 0.0
            mineral_unit_volumes = {m: price_resp["minerals"][m]["unit_volume"] for m in clean_demands if m in price_resp["minerals"]}
            for mineral, qty in clean_demands.items():
                orders = price_resp["minerals"].get(mineral, {}).get("orders", [])
                remaining = qty
                fill_cost = 0.0
                filled = 0
                for o in orders:
                    if remaining <= 0:
                        break
                    take = min(o.get("volume_remain", 0), remaining)
                    if take <= 0:
                        continue
                    fill_cost += take * o["price"]
                    filled += take
                    remaining -= take
                avg_fill_price = (fill_cost / filled) if filled > 0 else None
                unit_vol = mineral_unit_volumes.get(mineral, 0.01)
                tiered_total_cost += fill_cost
                tiered_total_volume += qty * unit_vol
                tiered_detail.append((
                    mineral, qty, filled, remaining, avg_fill_price,
                    fill_cost, unit_vol, qty * unit_vol
                ))
            raw_df = pd.DataFrame(
                tiered_detail,
                columns=[
                    "Mineral", "Required Units", "Filled Units", "Shortage Units",
                    "Avg Fill Price", "Fill Cost (ISK)", "Unit Volume (m3)", "Total Volume (m3)"
                ]
            )

            # Per-ore effective contribution
            rows_eff = []
            for row in solution:
                oid = row["ore_id"]
                batches = row["batches"]
                portion = ore_yields[oid]["portionSize"]
                batch_yields = ore_yields[oid]["batch_yields"]
                covered_units = 0.0
                for mineral, req in clean_demands.items():
                    produced = batches * batch_yields.get(mineral, 0)
                    covered_units += min(produced, req)
                eff = row["cost"] / covered_units if covered_units > 0 else None
                rows_eff.append({
                    "Ore": row["ore_name"],
                    "Batches": batches,
                    "Ore Units": batches * portion,
                    "Cost": row["cost"],
                    "Covered Demand Units": covered_units,
                    "ISK / Covered Unit": eff
                })
            df_eff = pd.DataFrame(rows_eff)

            surplus_dict = {k: v for k, v in opt_resp["surplus"].items() if (v or 0) > 0}
            df_surplus = None
            if surplus_dict:
                df_surplus = pd.DataFrame(
                    [(m, qty, (qty * (resale.get(m, 0))) if (resale_toggle and resale) else 0.0)
                     for m, qty in surplus_dict.items()],
                    columns=["Mineral", "Surplus Units", "Resale (80%) ISK" if resale_toggle else "Value (0)"]
                )

            ore_total_volume = df_ore_summary["Volume (m3)"].sum() if not df_ore_summary.empty and df_ore_summary["Volume (m3)"].notna().any() else None
            total_cost = opt_resp["total_cost"]
            savings = tiered_total_cost - total_cost
            savings_pct = (savings / tiered_total_cost * 100) if tiered_total_cost > 0 else 0

            # Persist for rendering
            st.session_state["opt_display"] = {
                "total_cost": total_cost,
                "tiered_total_cost": tiered_total_cost,
                "savings": savings,
                "savings_pct": savings_pct,
                "ore_total_volume": ore_total_volume,
                "raw_total_volume": tiered_total_volume,
                "df_solution": df_sol,
                "df_ore_summary": df_ore_summary,
                "df_raw": raw_df,
                "df_eff": df_eff,
                "df_surplus": df_surplus,
                "resale_toggle": resale_toggle,
                "surplus_penalty": surplus_penalty,
                "resale": resale,
                "surplus_dict": surplus_dict,
                "show_yield_table": show_yield_table,
                "show_coverage_debug": show_coverage_debug,
                "show_formula": show_formula,
                "ores": ores,
                "clean_demands": clean_demands,
                "ore_yields": ore_yields
            }

    # -------------------------
    # OUTPUT (full width area)
    # -------------------------
    opt_display = st.session_state.get("opt_display")
    if not opt_display:
        return

    st.markdown("---")
    st.subheader("Cost & Volume Comparison")
    colc1, colc2, colc3 = st.columns(3)
    colc1.metric("Ore Solution Cost (ISK)", f"{opt_display['total_cost']:,.2f}")
    colc2.metric("Direct Minerals Cost (ISK)", f"{opt_display['tiered_total_cost']:,.2f}")
    colc3.metric("Savings (ISK)", f"{opt_display['savings']:,.2f}", f"{opt_display['savings_pct']:+.2f}%")
    if opt_display["ore_total_volume"] is not None:
        st.caption(f"Estimated Ore Volume: {opt_display['ore_total_volume']:,.2f} m3  |  Direct Minerals Volume: {opt_display['raw_total_volume']:,.2f} m3")
    else:
        st.caption(f"Direct Minerals Volume: {opt_display['raw_total_volume']:,.2f} m3 (ore volumes unavailable for comparison)")

    # Side-by-side: Optimized Ore Solution / Raw Minerals comparator
    sol_col, raw_col = st.columns(2)
    with sol_col:
        st.subheader("Optimized Ore Solution")
        st.dataframe(opt_display["df_solution"], use_container_width=True)
        st.caption("Decision variable = batches; ore_units = batches * portionSize.")
        st.markdown("**Ores Purchase Summary**")
        st.dataframe(opt_display["df_ore_summary"], use_container_width=True)
        st.markdown("**Per-Ore Effective Contributions**")
        st.dataframe(opt_display["df_eff"], use_container_width=True)

    with raw_col:
        st.subheader("Raw Minerals (Depth-Aware)")
        st.dataframe(opt_display["df_raw"], use_container_width=True)
        st.markdown("**Surplus Summary**")
        if opt_display["df_surplus"] is not None:
            st.dataframe(opt_display["df_surplus"], use_container_width=True)
        else:
            st.write("No surplus minerals.")

    # Optional debug sections
    if opt_display["show_yield_table"]:
        st.subheader("Per-Ore Batch Yields (Post-Skills)")
        yield_rows = []
        for o in opt_display["ores"]:
            portion = o["portionSize"]
            for mineral, qty in o["batch_yields"].items():
                yield_rows.append({
                    "ore_id": o["id"],
                    "ore_name": o["name"],
                    "portionSize": portion,
                    "mineral": mineral,
                    "units per batch": qty,
                    "units per ore unit": qty / portion
                })
        st.dataframe(pd.DataFrame(yield_rows), use_container_width=True)

    if opt_display["show_coverage_debug"]:
        st.subheader("Demand Coverage & Surplus (Debug)")
        produced_totals = {m: 0.0 for m in opt_display["clean_demands"].keys()}
        for row in opt_display["df_solution"].to_dict("records"):
            oid = row["ore_id"]
            batches = row["batches"]
            batch_yields = opt_display["ore_yields"][oid]["batch_yields"]
            for mineral in opt_display["clean_demands"].keys():
                produced_totals[mineral] += batches * batch_yields.get(mineral, 0)
        coverage_rows = []
        for mineral, req in opt_display["clean_demands"].items():
            prod = produced_totals.get(mineral, 0.0)
            surplus = max(0.0, prod - req)
            coverage = (prod / req * 100) if req > 0 else 0.0
            coverage_rows.append({
                "Mineral": mineral,
                "Required": req,
                "Produced": prod,
                "Surplus": surplus,
                "Coverage %": coverage
            })
        st.dataframe(pd.DataFrame(coverage_rows), use_container_width=True)

    if opt_display["show_formula"]:
        st.markdown("""
**Reprocessing Yield Formula**

batch_output = base * FacilityBase
             * (1 + 0.02*Refining)
             * (1 + 0.02*ReprocessingEfficiency)
             * (1 + 0.02*SpecificOreProcessing)
             * (1 + Rig + Structure + Implants)

Values shown are final post-skill batch yields.
        """)