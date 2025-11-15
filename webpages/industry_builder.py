import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]

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
    
    # Dropdown to select character
    char_options = df.set_index("character_id")["character_name"].to_dict()
    selected_id = st.selectbox(
        "Select character:",
        options=list(char_options.keys()),
        format_func=lambda x: char_options[x]
    )

    if not selected_id:
        return

    try:
        response = api_get(f"/industry_builder/{selected_id}")
        if response.get("status") != "success":
            st.error(f"API error: {response.get('message', 'Unknown error')}")
            return
        
        industry_data = response.get("data", [])
        if not industry_data:
            st.info("No industry jobs found for this character.")
            return
        
        industry_df = pd.DataFrame(industry_data)
        st.dataframe(industry_df)
    except Exception as e:
        st.error(f"Error fetching industry data: {e}")
