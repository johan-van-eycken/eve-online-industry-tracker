import streamlit as st
from typing import Optional

from utils.app_init import load_config
from utils.table_viewer import render_table_viewer
from classes.database_manager import DatabaseManager

def render():
    """
    Reusable table viewer for Streamlit.
    """
    db: Optional[DatabaseManager] = None
    selected_table: Optional[str] = None

    try:
        cfgManager = load_config()
        cfg = cfgManager.all()
    except Exception as e:
        st.error(f"Failed to load config: {e}")
        st.stop()
    
    result = render_table_viewer(cfg, row_limit=2000)
    if result is not None:
        db, selected_table = result

    """
    Table administration for Streamlit.
    """
    st.subheader("Database Table Administration")

    if st.button("Drop table"):
        if db is not None and selected_table is not None:
            db.drop_table(selected_table)
            st.success(f"Table {selected_table} dropped from database {db.get_db_name()}.")
            selected_table = None
