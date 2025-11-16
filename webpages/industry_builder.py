import streamlit as st  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingModuleSource, reportMissingImports]

from utils.app_init import load_config, init_db_app
from utils.flask_api import api_get


def render():
    st.subheader("Industry Builder")

    try:
        response = api_get(f"/industry_builder_data")
        if response.get("status") != "success":
            st.error(f"API error: {response.get('message', 'Unknown error')}")
            return

        industry_data = response.get("data", [])
        if not industry_data:
            st.info("No industry jobs found for this character.")
            return

        # Convert to DataFrame
        df = pd.DataFrame(industry_data)

        # Add filters in sidebar
        st.sidebar.subheader("Filters")

        # Owned filter
        owned_filter = st.sidebar.radio(
            "Ownership",
            options=["All", "Owned Only", "Not Owned"],
            index=0
        )

        # Blueprint type filter
        blueprint_type_filter = st.sidebar.radio(
            "Blueprint Type",
            options=["All", "Originals (BPO)", "Copies (BPC)"],
            index=0
        )

        # Apply filters
        filtered_df = df.copy()

        if owned_filter == "Owned Only":
            filtered_df = filtered_df[filtered_df["owned"] == True]
        elif owned_filter == "Not Owned":
            filtered_df = filtered_df[filtered_df["owned"] == False]

        if blueprint_type_filter == "Originals (BPO)":
            filtered_df = filtered_df[
                (filtered_df["is_blueprint_copy"] == False) | 
                (filtered_df["is_blueprint_copy"].isna())
            ]
        elif blueprint_type_filter == "Copies (BPC)":
            filtered_df = filtered_df[filtered_df["is_blueprint_copy"] == True]

        # Display count
        st.write(f"Showing {len(filtered_df)} of {len(df)} blueprints")

        # Display filtered data
        st.dataframe(filtered_df)

    except Exception as e:
        st.error(f"Error fetching industry data: {e}")
