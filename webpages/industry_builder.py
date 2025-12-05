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
        df = db.load_df("characters")
    except Exception:
        st.warning("No character data found. Run main.py first.")
        st.stop()

    # Create a mapping of character_id to character_name
    character_map = dict(zip(df["character_id"], df["character_name"]))

    # Use character_id as the value, but display character_name
    selected_character_id = st.selectbox(
        "Select a character",
        options=df["character_id"].tolist(),
        format_func=lambda x: character_map[x],
    )

    if selected_character_id:
        try:
            response = api_get(f"/industry_builder_data/{selected_character_id}")
            if response.get("status") != "success":
                st.error(f"API error: {response.get('message', 'Unknown error')}")
                return

            industry_data = response.get("data", [])
            if not industry_data:
                st.info("No industry jobs found for this character.")
                return

            # Convert to DataFrame
            df = pd.DataFrame(industry_data)

            # Add filters on top of the page
            with st.expander("Filters", expanded=True):
                col_bp_type, col_req, col_filler = st.columns(3)
                with col_bp_type:
                    bp_type_filter = st.radio(
                        "Blueprint Type",
                        options=["All", "Originals (BPO)", "Copies (BPC)"],
                        index=0,
                    )
                with col_req:
                    skill_req_filter = st.checkbox(
                        "I have the skills", value=True
                    )
                with col_filler:
                    st.write("")  # Just a spacer

            # Apply filters
            filtered_df = df.copy()

            if bp_type_filter == "Originals (BPO)":
                filtered_df = filtered_df[
                    (filtered_df["is_blueprint_copy"] == False)
                    | (filtered_df["is_blueprint_copy"].isna())
                ]
            elif bp_type_filter == "Copies (BPC)":
                filtered_df = filtered_df[filtered_df["is_blueprint_copy"] == True]

            if skill_req_filter:
                filtered_df = filtered_df[filtered_df["skill_requirements_met"] == True]
            
            # Display count
            st.write(f"Showing {len(filtered_df)} of {len(df)} blueprints")

            # Display filtered data
            st.dataframe(filtered_df)
            st.divider()

            # Blueprint selector for detailed view
            if not filtered_df.empty:
                st.subheader("Blueprint Details")

                # Create blueprint options with type_name
                blueprint_options = filtered_df["type_name"].tolist()
                selected_blueprint_name = st.selectbox(
                    "Select a blueprint to view details", blueprint_options
                )

                if selected_blueprint_name:
                    # Find the full blueprint data from industry_data
                    full_bp_data = next((
                        bp for bp in industry_data if bp["type_name"] == selected_blueprint_name
                    ), None)

                    if full_bp_data:
                        bp_id = full_bp_data.get("type_id")
                        bp_name = full_bp_data.get("type_name", "Unknown")
                        if full_bp_data.get("is_blueprint_copy"):
                            bp_name += " Copy"
                        variation = (
                            "bpc" if full_bp_data.get("is_blueprint_copy") else "bp"
                        )
                        me_value = full_bp_data.get("blueprint_material_efficiency", 0)
                        te_value = full_bp_data.get("blueprint_time_efficiency", 0)
                        runs_value = full_bp_data.get("blueprint_runs", 0)
                        total_mat_cost = full_bp_data.get("total_material_cost", 0.0)
                        total_prod_value = full_bp_data.get("total_product_value", 0.0)
                        profit_margin = full_bp_data.get("profit_margin", 0.0)
                        skill_requirements_met = full_bp_data.get("skill_requirements_met", False)
                        owner = (
                            full_bp_data.get("owner_name", "Unknown")
                            if full_bp_data.get("owned")
                            else "Not Owned"
                        )

                        col_bp_icon, col_bp = st.columns([1, 11])
                        with col_bp_icon:
                            st.markdown(
                                f"<img src='https://images.evetech.net/types/{bp_id}/{variation}?size=64' alt='Icon' />",
                                unsafe_allow_html=True,
                            )
                        with col_bp:
                            st.markdown(f"### {bp_name}")
                            st.caption(f"Type ID: {bp_id}")

                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.markdown(
                                    f"""
                                    **Material Efficiency:** {me_value if me_value is not None else 'N/A'}%  
                                    **Time Efficiency:** {te_value if te_value is not None else 'N/A'}%  
                                    **Runs:** {runs_value if runs_value is not None and runs_value > 0 else 'Unlimited'}  
                                    """,
                                    unsafe_allow_html=True,
                                )
                            with col2:
                                st.markdown(
                                    f"""
                                    **Material Cost:** {total_mat_cost:,.2f} ISK  
                                    **Product Value:** {total_prod_value:,.2f} ISK  
                                    **Profit Margin:** {profit_margin:,.2f} ISK  
                                    """,
                                    unsafe_allow_html=True,
                                )
                            with col3:
                                st.markdown(
                                    f"""
                                    **Owner:** {owner}  
                                    **Skill requirements:** {"Yes" if skill_requirements_met else "No"}
                                    """,
                                    unsafe_allow_html=True,
                                )

                        st.divider()

                        # Input Materials
                        st.markdown("### Input Materials")
                        materials = full_bp_data.get("materials", [])
                        if materials:
                            materials_data = []
                            for mat in materials:
                                materials_data.append({
                                    "Material": mat.get("type_name", "Unknown"),
                                    "Base Quantity": mat.get("quantity", 0),
                                    "Adjusted Quantity": mat.get("adjusted_quantity", mat.get("quantity", 0)),
                                    "Unit Price (Adjusted)": mat.get("adjusted_price", 0.0) or 0.0,
                                    "Total Cost": (mat.get("adjusted_quantity", mat.get("quantity", 0)) * (mat.get("adjusted_price", 0.0) or 0.0)),
                                })

                            materials_df = pd.DataFrame(materials_data)
                            st.dataframe(
                                materials_df,
                                use_container_width=True,
                                hide_index=True,
                                column_config={
                                    "Material": st.column_config.TextColumn("Material", width="medium"),
                                    "Base Quantity": st.column_config.NumberColumn("Base Qty", format="%d"),
                                    "Adjusted Quantity": st.column_config.NumberColumn("Adjusted Qty (ME)", format="%d"),
                                    "Unit Price (Adjusted)": st.column_config.NumberColumn("Unit Price", format="%.2f ISK"),
                                    "Total Cost": st.column_config.NumberColumn("Total Cost", format="%.2f ISK"),
                                },
                            )

                            # Summary
                            total_materials_cost = sum(mat["Total Cost"] for mat in materials_data)
                            st.metric("Total Materials Cost", f"{total_materials_cost:,.2f} ISK")
                        else:
                            st.info("No materials required")

                        st.divider()

                        # Output Products
                        st.markdown("### Output Products")
                        products = full_bp_data.get("products", [])
                        if products:
                            products_data = []
                            for prod in products:
                                products_data.append({
                                    "Product": prod.get("type_name", "Unknown"),
                                    "Quantity": prod.get("quantity", 0),
                                    "Unit Price (Average)": prod.get("average_price", 0.0)or 0.0,
                                    "Total Value": (prod.get("quantity", 0) * (prod.get("average_price", 0.0) or 0.0)),
                                })

                            products_df = pd.DataFrame(products_data)
                            st.dataframe(
                                products_df,
                                use_container_width=True,
                                hide_index=True,
                                column_config={
                                    "Product": st.column_config.TextColumn("Product", width="medium"),
                                    "Quantity": st.column_config.NumberColumn("Quantity", format="%d"),
                                    "Unit Price (Average)": st.column_config.NumberColumn("Unit Price", format="%.2f ISK"),
                                    "Total Value": st.column_config.NumberColumn("Total Value", format="%.2f ISK"),
                                },
                            )

                            # Summary
                            total_products_value = sum(prod["Total Value"] for prod in products_data)
                            st.metric("Total Products Value", f"{total_products_value:,.2f} ISK")
                        else:
                            st.info("No products defined")

                        st.divider()

                        # Required Skills
                        st.markdown("### Required Skills")
                        skills = full_bp_data.get("required_skills", [])
                        if skills:
                            skills_data = []
                            for skill in skills:
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
                                use_container_width=True,
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

                        # Raw Data (Complete)
                        with st.expander("View Raw Blueprint Data (Debug)"):
                            st.json(full_bp_data)

        except Exception as e:
            st.error(f"Error fetching industry data: {e}")
