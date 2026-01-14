import streamlit as st  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingModuleSource, reportMissingImports]

from typing import Any

from utils.app_init import load_config, init_db_app
from utils.flask_api import api_get


@st.cache_data(ttl=60)
def _get_industry_profiles(character_id: int) -> dict | None:
    return api_get(f"/industry_profiles/{int(character_id)}")


def render():
    st.subheader("Industry Builder")

    db: Any = None

    try:
        cfgManager = load_config()
        db = init_db_app(cfgManager)
    except Exception as e:
        st.error(f"Failed to load database: {e}")
        st.stop()

    if db is None:
        st.stop()

    characters_df: pd.DataFrame | None = None
    try:
        characters_df = db.load_df("characters")
    except Exception:
        st.warning("No character data found. Run main.py first.")
        st.stop()

    assert characters_df is not None

    character_map = dict(zip(characters_df["character_id"], characters_df["character_name"]))

    selected_character_id = st.selectbox(
        "Select a character",
        options=characters_df["character_id"].tolist(),
        format_func=lambda character_id: character_map.get(character_id, str(character_id)),
    )

    if not selected_character_id:
        return

    # Industry profile selector (affects job cost/time estimates)
    profiles_resp = _get_industry_profiles(int(selected_character_id))
    profiles = (profiles_resp or {}).get("data") if (profiles_resp or {}).get("status") == "success" else []
    profile_options: list[int | None] = [None]
    profile_label_by_id: dict[int | None, str] = {None: "(No profile)"}
    default_profile_id = None
    for p in profiles or []:
        pid = p.get("id")
        if pid is None:
            continue
        profile_options.append(int(pid))
        label = str(p.get("profile_name") or pid)
        if p.get("is_default"):
            label = f"⭐ {label}"
            default_profile_id = int(pid)
        profile_label_by_id[int(pid)] = label

    selected_profile_id = st.selectbox(
        "Industry Profile",
        options=profile_options,
        index=(profile_options.index(default_profile_id) if default_profile_id in profile_options else 0),
        format_func=lambda pid: profile_label_by_id.get(pid, str(pid)),
        help="Select an Industry Profile to estimate job fees and job time.",
    )

    try:
        url = f"/industry_builder_data/{selected_character_id}"
        if selected_profile_id is not None:
            url += f"?profile_id={int(selected_profile_id)}"
        response = api_get(url) or {}
    except Exception as e:
        st.error(f"Error calling backend: {e}")
        return

    if response.get("status") != "success":
        st.error(f"API error: {response.get('message', 'Unknown error')}")
        return

    industry_data = response.get("data", [])
    if not industry_data:
        st.info("No industry jobs found for this character.")
        return

    def _flatten_for_table(bp: dict) -> dict:
        if not isinstance(bp, dict):
            return {}

        flags = bp.get("flags") or {}
        loc = bp.get("location") or {}
        solar = (loc.get("solar_system") or {}) if isinstance(loc, dict) else {}
        mj = bp.get("manufacture_job") or {}
        props = (mj.get("properties") or {}) if isinstance(mj, dict) else {}
        cost = (props.get("job_cost") or {}) if isinstance(props, dict) else {}
        time_eff = (props.get("total_time_efficiency") or {}) if isinstance(props, dict) else {}

        est_seconds = time_eff.get("estimated_job_time_seconds")
        try:
            est_hours = (float(est_seconds) / 3600.0) if est_seconds is not None else None
        except Exception:
            est_hours = None

        return {
            "type_id": bp.get("type_id"),
            "type_name": bp.get("type_name"),
            "owned": bp.get("owned"),
            "owner_name": bp.get("owner_name"),
            "location": (loc.get("display_name") if isinstance(loc, dict) else None),
            "solar_system": (solar.get("name") if isinstance(solar, dict) else None),
            "solar_system_security": (solar.get("security_status") if isinstance(solar, dict) else None),
            "top_location_type": (loc.get("top_location_type") if isinstance(loc, dict) else None),
            "is_blueprint_copy": (flags.get("is_blueprint_copy") if isinstance(flags, dict) else None),
            "skill_requirements_met": bp.get("skill_requirements_met"),
            "blueprint_material_efficiency_percent": bp.get("blueprint_material_efficiency_percent"),
            "blueprint_time_efficiency_percent": bp.get("blueprint_time_efficiency_percent"),
            "total_material_cost": bp.get("total_material_cost"),
            "total_product_value": bp.get("total_product_value"),
            "profit_margin": bp.get("profit_margin"),
            "estimated_job_fee_isk": cost.get("total_job_cost_isk"),
            "estimated_job_time_seconds": est_seconds,
            "estimated_job_time_hours": est_hours,
            "job_system_cost_index": cost.get("system_cost_index"),
            "job_surcharge_rate": cost.get("surcharge_rate_total_fraction"),
            "job_rig_group": cost.get("rig_group_label"),
        }

    df = pd.DataFrame([_flatten_for_table(bp) for bp in industry_data])

    def _format_duration(seconds: float | int | None) -> str:
        try:
            s = int(round(float(seconds or 0.0)))
        except Exception:
            s = 0
        if s < 0:
            s = 0

        month_s = 30 * 24 * 3600
        day_s = 24 * 3600

        months = s // month_s
        s = s % month_s
        days = s // day_s
        s = s % day_s

        hours = s // 3600
        s = s % 3600
        minutes = s // 60
        secs = s % 60

        parts = []
        if months:
            parts.append(f"{months}M")
        if days:
            parts.append(f"{days}D")
        parts.append(f"{hours:02d}:{minutes:02d}:{secs:02d}")
        return " ".join(parts)

    # Make estimates easier to read if present.
    if "estimated_job_fee_isk" in df.columns:
        df["estimated_job_fee_isk"] = df["estimated_job_fee_isk"].fillna(0.0)

    # Prefer seconds for accurate formatting.
    if "estimated_job_time_seconds" in df.columns:
        df["estimated_job_time"] = df["estimated_job_time_seconds"].apply(_format_duration)
    elif "estimated_job_time_hours" in df.columns:
        df["estimated_job_time"] = (df["estimated_job_time_hours"].fillna(0.0) * 3600.0).apply(_format_duration)

    with st.expander("Filters", expanded=True):
        col_bp_type, col_req, col_loc = st.columns(3)
        with col_bp_type:
            bp_type_filter = st.radio(
                "Blueprint Type",
                options=["All", "Originals (BPO)", "Copies (BPC)"],
                index=0,
            )
        with col_req:
            skill_req_filter = st.checkbox("I have the skills", value=True)
            reactions_filter = st.checkbox("Include Reactions", value=False, help="Reactions can only be done in 0.4-secure space or lower.")
        with col_loc:
            location_options = ["All"]
            if "location" in df.columns:
                locs = df["location"].dropna().astype(str)
                locs = [str(s) for s in locs.tolist() if str(s).strip()]
                location_options += sorted(set(locs))
            location_filter = st.selectbox("Location", options=location_options, index=0)

    filtered_df = df.copy()

    if "is_blueprint_copy" in filtered_df.columns:
        if bp_type_filter == "Originals (BPO)":
            filtered_df = filtered_df[filtered_df["is_blueprint_copy"] == False]
        elif bp_type_filter == "Copies (BPC)":
            filtered_df = filtered_df[filtered_df["is_blueprint_copy"] == True]

    if skill_req_filter and "skill_requirements_met" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["skill_requirements_met"] == True]
    
    if reactions_filter == False and "type_name" in filtered_df.columns:
        filtered_df = filtered_df[~filtered_df["type_name"].str.contains(" Reaction Formula$", na=False)]

    if location_filter != "All" and "location" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["location"].astype(str) == str(location_filter)]

    st.write(f"Showing {len(filtered_df)} of {len(df)} blueprints")

    # Keep the main table focused: hide debug/internal fields.
    hidden_cols = {
        "job_system_cost_index",
        "job_surcharge_rate",
        "job_rig_group",
        "estimated_job_time_seconds",
        "estimated_job_time_hours",
        "top_location_type",
    }
    display_df = filtered_df.drop(columns=[c for c in hidden_cols if c in filtered_df.columns], errors="ignore")

    column_config = {}
    if "estimated_job_fee_isk" in display_df.columns:
        column_config["estimated_job_fee_isk"] = st.column_config.NumberColumn(
            "Est. Job Fee",
            format="%.0f ISK",
            help="Estimated installation fee based on selected Industry Profile (system cost index, structure bonuses, rigs, and surcharge).",
        )
    if "estimated_job_time" in display_df.columns:
        column_config["estimated_job_time"] = st.column_config.TextColumn(
            "Est. Job Time",
            help="Estimated manufacturing time (M D HH:MM:SS) based on blueprint TE and structure bonuses/rigs.",
        )
    if "profit_margin" in display_df.columns:
        column_config["profit_margin"] = st.column_config.NumberColumn("Profit", format="%.0f ISK")
    if "total_material_cost" in display_df.columns:
        column_config["total_material_cost"] = st.column_config.NumberColumn("Mat. Cost", format="%.0f ISK")
    if "total_product_value" in display_df.columns:
        column_config["total_product_value"] = st.column_config.NumberColumn("Prod. Value", format="%.0f ISK")
    if "solar_system_security" in display_df.columns:
        column_config["solar_system_security"] = st.column_config.NumberColumn(
            "Sec",
            format="%.2f",
            help="Solar system security status (best-effort).",
        )

    st.dataframe(display_df, width="stretch", hide_index=True, column_config=column_config)
    st.divider()

    if filtered_df.empty:
        return

    st.subheader("Blueprint Details")

    blueprint_options = (
        filtered_df["type_name"].dropna().astype(str).tolist()
        if "type_name" in filtered_df.columns
        else []
    )
    if not blueprint_options:
        st.info("No blueprint details available.")
        return

    selected_blueprint_name = st.selectbox("Select a blueprint to view details", blueprint_options)
    if not selected_blueprint_name:
        return

    full_bp_data = next(
        (bp for bp in industry_data if bp.get("type_name") == selected_blueprint_name),
        None,
    )

    if not full_bp_data:
        st.warning("Blueprint not found in response.")
        return

    bp_id = full_bp_data.get("type_id")
    bp_name = full_bp_data.get("type_name", "Unknown")
    flags = full_bp_data.get("flags", {}) or {}
    is_bpc = bool(flags.get("is_blueprint_copy")) if isinstance(flags, dict) else False
    variation = "bpc" if is_bpc else "bp"

    owned = bool(full_bp_data.get("owned", False))
    owner_name = full_bp_data.get("owner_name", "Unknown")
    owner_display = owner_name if owned else "Not Owned"
    skill_requirements_met = bool(full_bp_data.get("skill_requirements_met", False))

    manufacture_job = full_bp_data.get("manufacture_job", {}) or {}
    mj_props = manufacture_job.get("properties", {}) or {}
    mj_cost = mj_props.get("job_cost", {}) or {}
    mj_time = mj_props.get("total_time_efficiency", {}) or {}
    mj_me = mj_props.get("total_material_efficiency", {}) or {}

    materials = manufacture_job.get("required_materials", []) or []
    products = full_bp_data.get("products", []) or []
    required_skills = manufacture_job.get("required_skills", []) or []

    materials_data = []
    missing_material_prices = 0
    for mat in materials:
        base_qty = mat.get("quantity_me0", 0) or 0
        adjusted_qty = mat.get("quantity_after_efficiency", base_qty) or 0
        unit_price = mat.get("unit_price_isk")
        if unit_price is None:
            unit_price = mat.get("average_price_isk")
        try:
            unit_price = float(unit_price) if unit_price is not None else None
        except Exception:
            unit_price = None
        if unit_price is None or unit_price <= 0:
            missing_material_prices += 1
            unit_price = None

        total_cost = (adjusted_qty * unit_price) if unit_price is not None else None
        materials_data.append(
            {
                "Material": mat.get("type_name", "Unknown"),
                "Base Quantity": base_qty,
                "Adjusted Quantity": adjusted_qty,
                "Unit Price": unit_price,
                "Total Cost": total_cost,
            }
        )

    products_data = []
    for prod in products:
        qty = prod.get("quantity", 0) or 0
        unit_price = prod.get("average_price", 0.0) or 0.0
        products_data.append(
            {
                "Product": prod.get("type_name", "Unknown"),
                "Quantity": qty,
                "Unit Price (Average)": unit_price,
                "Total Value": qty * unit_price,
            }
        )

    total_materials_cost = sum((row["Total Cost"] or 0.0) for row in materials_data)
    total_products_value = sum(row["Total Value"] for row in products_data)
    profit_margin = total_products_value - total_materials_cost

    col_bp_icon, col_bp_title = st.columns([1, 11])
    with col_bp_icon:
        if bp_id:
            st.markdown(
                f"<img src='https://images.evetech.net/types/{bp_id}/{variation}?size=64' alt='Icon' />",
                unsafe_allow_html=True,
            )
    with col_bp_title:
        st.markdown(f"### {bp_name}")
        if bp_id:
            st.caption(f"Type ID: {bp_id}")

    col1, col2, col3 = st.columns(3)
    with col1:
        # Prefer backend totals (they incorporate the same adjusted quantities)
        try:
            total_materials_cost = float(full_bp_data.get("total_material_cost") or total_materials_cost)
            total_products_value = float(full_bp_data.get("total_product_value") or total_products_value)
            profit_margin = float(full_bp_data.get("profit_margin") or profit_margin)
        except Exception:
            pass

        st.markdown(
            f"""
            **Material Cost:** {total_materials_cost:,.2f} ISK  
            **Product Value:** {total_products_value:,.2f} ISK  
            **Profit Margin:** {profit_margin:,.2f} ISK
            """
        )
    with col2:
        loc = full_bp_data.get("location") or {}
        solar = (loc.get("solar_system") or {}) if isinstance(loc, dict) else {}
        loc_display = (loc.get("display_name") if isinstance(loc, dict) else None) or "Unknown"
        try:
            sec = solar.get("security_status") if isinstance(solar, dict) else None
            sec_str = f" ({float(sec):.2f})" if sec is not None else ""
        except Exception:
            sec_str = ""
        st.markdown(
            f"""
            **Owner:** {owner_display}  
            **Blueprint Type:** {"Copy (BPC)" if is_bpc else "Original (BPO)"}  
            **Location:** {loc_display}{sec_str}
            """
        )
    with col3:
        skills_implants = mj_time.get("skills_and_implants", {}) or {}

        est_fee = mj_cost.get("total_job_cost_isk")
        est_seconds = mj_time.get("estimated_job_time_seconds")
        ci = mj_cost.get("system_cost_index")
        surcharge = mj_cost.get("surcharge_rate_total_fraction")

        eiv = mj_cost.get("estimated_item_value_total_isk")
        gross_cost = mj_cost.get("gross_cost_isk")
        gross_after_bonuses = mj_cost.get("gross_cost_after_bonuses_isk")
        taxes = mj_cost.get("taxes_isk")
        eff_cost_red = mj_cost.get("structure_cost_reduction_fraction")
        eff_mat_red = mj_me.get("structure_material_reduction_fraction")
        eff_time_red = mj_time.get("structure_time_reduction_fraction")
        te_mult = mj_time.get("blueprint_time_multiplier")
        skill_time_red = skills_implants.get("skill_time_reduction_fraction")
        implant_time_red = skills_implants.get("implant_time_reduction_fraction")
        skill_implant_time_red = skills_implants.get("skills_and_implants_time_reduction_fraction")
        implant_details = skills_implants.get("implant_details")

        lines = [f"**Skill requirements:** {'Yes' if skill_requirements_met else 'No'}"]
        if est_fee is not None or est_seconds is not None:
            try:
                if est_fee is not None:
                    lines.append(f"**Est. Job Fee:** {float(est_fee):,.0f} ISK")
                if est_seconds is not None:
                    lines.append(f"**Est. Job Time:** {_format_duration(est_seconds)}")
                if ci is not None:
                    lines.append(f"**System Cost Index:** {float(ci):.2%}")
                if surcharge is not None:
                    lines.append(f"**Surcharge Rate:** {float(surcharge):.2%}")
            except Exception:
                pass

        st.markdown("  \n".join(lines))

        # Client parity: show the same breakdown the in-game UI shows (EIV basis).
        with st.expander("Client Parity (Job Cost Breakdown)"):
            parts: list[str] = []
            try:
                if eiv is not None:
                    parts.append(f"**Estimated Item Value (EIV):** {float(eiv):,.0f} ISK")
                if ci is not None and eiv is not None:
                    parts.append(f"**System cost index:** {float(ci):.2%}  →  {float(eiv) * float(ci):,.0f} ISK")
                if eff_cost_red is not None and gross_cost is not None:
                    bonus_delta = float(gross_cost) - float(gross_after_bonuses or 0.0)
                    parts.append(
                        f"**Bonuses (cost reduction):** -{float(eff_cost_red):.2%}  →  -{bonus_delta:,.0f} ISK"
                    )
                if gross_after_bonuses is not None:
                    parts.append(f"**Job gross cost:** {float(gross_after_bonuses):,.0f} ISK")
                if surcharge is not None and eiv is not None:
                    parts.append(f"**Taxes / surcharge:** {float(surcharge):.2%}  →  {float(eiv) * float(surcharge):,.0f} ISK")
                if taxes is not None:
                    parts.append(f"**Total taxes:** {float(taxes):,.0f} ISK")
                if est_fee is not None:
                    parts.append(f"**Total job cost:** {float(est_fee):,.0f} ISK")

                # Extra modifiers (useful for validating structure/rig effects).
                extra: list[str] = []
                if eff_mat_red is not None:
                    extra.append(f"ME (structure+rig): {float(eff_mat_red):.2%}")
                if eff_time_red is not None:
                    extra.append(f"TE (structure+rig): {float(eff_time_red):.2%}")
                if te_mult is not None:
                    extra.append(f"Blueprint TE multiplier: {float(te_mult):.2f}×")
                if skill_time_red is not None:
                    extra.append(f"Skills time reduction (Industry/Adv): {float(skill_time_red):.2%}")
                if implant_time_red is not None:
                    extra.append(f"Implants time reduction: {float(implant_time_red):.2%}")
                if skill_implant_time_red is not None:
                    extra.append(f"Skills + implants time reduction: {float(skill_implant_time_red):.2%}")

                # Optional detail list (type IDs only; names depend on language/SDE formatting).
                if isinstance(implant_details, list):
                    bonuses = []
                    for d in implant_details:
                        if not isinstance(d, dict):
                            continue
                        tid = d.get("type_id")
                        bonus_pct = d.get("manufacturing_time_bonus_percent")
                        if tid is None or bonus_pct is None:
                            continue
                        try:
                            bonuses.append(f"{int(tid)}: {float(bonus_pct):.1f}%")
                        except Exception:
                            continue
                    if bonuses:
                        extra.append("Implant bonuses (typeID: %): " + ", ".join(bonuses))
                if extra:
                    parts.append("**Modifiers:** " + " | ".join(extra))
            except Exception:
                parts = []

            if parts:
                st.markdown("  \n".join(parts))
            else:
                st.info("No job-cost breakdown data available for this blueprint.")

    st.divider()

    st.markdown("### Input Materials")
    if missing_material_prices:
        st.caption(
            f"Price data missing for {missing_material_prices} material(s). "
            "ESI market prices do not include every type (unpublished, non-market, or no recent market data)."
        )
    if materials_data:
        materials_df = pd.DataFrame(materials_data)
        st.dataframe(
            materials_df,
            width="stretch",
            hide_index=True,
            column_config={
                "Material": st.column_config.TextColumn("Material", width="medium"),
                "Base Quantity": st.column_config.NumberColumn("Base Qty", format="%d"),
                "Adjusted Quantity": st.column_config.NumberColumn("Adjusted Qty (ME)", format="%d"),
                "Unit Price": st.column_config.NumberColumn("Unit Price", format="%.2f ISK"),
                "Total Cost": st.column_config.NumberColumn("Total Cost", format="%.2f ISK"),
            },
        )
    else:
        st.info("No materials required")

    st.divider()

    st.markdown("### Output Products")
    if products_data:
        products_df = pd.DataFrame(products_data)
        st.dataframe(
            products_df,
            width="stretch",
            hide_index=True,
            column_config={
                "Product": st.column_config.TextColumn("Product", width="medium"),
                "Quantity": st.column_config.NumberColumn("Quantity", format="%d"),
                "Unit Price (Average)": st.column_config.NumberColumn("Unit Price", format="%.2f ISK"),
                "Total Value": st.column_config.NumberColumn("Total Value", format="%.2f ISK"),
            },
        )
    else:
        st.info("No products defined")

    st.divider()

    st.markdown("### Required Skills")
    if required_skills:
        skills_data = []
        for skill in required_skills:
            skills_data.append(
                {
                    "Category": skill.get("category_name", "Unknown"),
                    "Group": skill.get("group_name", "Unknown"),
                    "Skill": skill.get("type_name", "Unknown"),
                    "Level": skill.get("required_level", 0),
                    "Character Level": skill.get("character_level", 0),
                    "Met": skill.get("met", False),
                }
            )
        skills_df = pd.DataFrame(skills_data)
        st.dataframe(
            skills_df,
            width="stretch",
            hide_index=True,
            column_config={
                "Category": st.column_config.TextColumn("Category"),
                "Group": st.column_config.TextColumn("Group"),
                "Skill": st.column_config.TextColumn("Skill"),
                "Level": st.column_config.NumberColumn("Level", format="%d"),
                "Character Level": st.column_config.NumberColumn("Character Level", format="%d"),
                "Met": st.column_config.CheckboxColumn("Met"),
            },
        )
    else:
        st.info("No skills required")

    with st.expander("View Raw Blueprint Data (Debug)"):
        st.json(full_bp_data)
