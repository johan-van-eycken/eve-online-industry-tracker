import streamlit as st  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingModuleSource, reportMissingImports]

from utils.app_init import load_config, init_db_app
from utils.flask_api import api_get


def render():
    st.subheader("Industry Builder")

    try:
        cfgManager = load_config()
        db = init_db_app(cfgManager)
    except Exception as e:
        st.error(f"Failed to load database: {e}")
        st.stop()

    try:
        characters_df = db.load_df("characters")
    except Exception:
        st.warning("No character data found. Run main.py first.")
        st.stop()

    character_map = dict(zip(characters_df["character_id"], characters_df["character_name"]))

    selected_character_id = st.selectbox(
        "Select a character",
        options=characters_df["character_id"].tolist(),
        format_func=lambda character_id: character_map.get(character_id, str(character_id)),
    )

    if not selected_character_id:
        return

    try:
        response = api_get(f"/industry_builder_data/{selected_character_id}")
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

    df = pd.DataFrame(industry_data)

    with st.expander("Filters", expanded=True):
        col_bp_type, col_req, _ = st.columns(3)
        with col_bp_type:
            bp_type_filter = st.radio(
                "Blueprint Type",
                options=["All", "Originals (BPO)", "Copies (BPC)"],
                index=0,
            )
        with col_req:
            skill_req_filter = st.checkbox("I have the skills", value=True)

    filtered_df = df.copy()

    if "is_blueprint_copy" in filtered_df.columns:
        if bp_type_filter == "Originals (BPO)":
            filtered_df = filtered_df[filtered_df["is_blueprint_copy"] == False]
        elif bp_type_filter == "Copies (BPC)":
            filtered_df = filtered_df[filtered_df["is_blueprint_copy"] == True]

    if skill_req_filter and "skill_requirements_met" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["skill_requirements_met"] == True]

    st.write(f"Showing {len(filtered_df)} of {len(df)} blueprints")
    st.dataframe(filtered_df, width="stretch", hide_index=True)
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
    is_bpc = bool(full_bp_data.get("is_blueprint_copy"))
    variation = "bpc" if is_bpc else "bp"

    owned = bool(full_bp_data.get("owned", False))
    owner_name = full_bp_data.get("owner_name", "Unknown")
    owner_display = owner_name if owned else "Not Owned"
    skill_requirements_met = bool(full_bp_data.get("skill_requirements_met", False))

    materials = full_bp_data.get("materials", []) or []
    products = full_bp_data.get("products", []) or []
    required_skills = full_bp_data.get("required_skills", []) or []

    materials_data = []
    for mat in materials:
        base_qty = mat.get("quantity", 0) or 0
        adjusted_qty = mat.get("adjusted_quantity", base_qty) or 0
        unit_price = mat.get("adjusted_price", 0.0) or 0.0
        materials_data.append(
            {
                "Material": mat.get("type_name", "Unknown"),
                "Base Quantity": base_qty,
                "Adjusted Quantity": adjusted_qty,
                "Unit Price (Adjusted)": unit_price,
                "Total Cost": adjusted_qty * unit_price,
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

    total_materials_cost = sum(row["Total Cost"] for row in materials_data)
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
        st.markdown(
            f"""
            **Material Cost:** {total_materials_cost:,.2f} ISK  
            **Product Value:** {total_products_value:,.2f} ISK  
            **Profit Margin:** {profit_margin:,.2f} ISK
            """
        )
    with col2:
        st.markdown(
            f"""
            **Owner:** {owner_display}  
            **Blueprint Type:** {"Copy (BPC)" if is_bpc else "Original (BPO)"}
            """
        )
    with col3:
        st.markdown(
            f"""
            **Skill requirements:** {"Yes" if skill_requirements_met else "No"}
            """
        )

    st.divider()

    st.markdown("### Input Materials")
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
                "Unit Price (Adjusted)": st.column_config.NumberColumn("Unit Price", format="%.2f ISK"),
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
                    "Level": skill.get("level", 0),
                    "Character Level": skill.get("character_level", 0),
                    "Met": skill.get("skill_requirement_met", False),
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
