import streamlit as st

from utils.table_viewer import render_table_viewer
from classes.database_manager import DatabaseManager
from typing import Optional

def render(cfg):
    """
    Reusable table viewer for Streamlit.
    """
    db: Optional[DatabaseManager] = None
    selected_table: Optional[str] = None
    
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
