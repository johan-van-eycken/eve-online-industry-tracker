import streamlit as st
import pandas as pd

from classes.database_manager import DatabaseManager
from typing import Optional, List, Tuple

def render_table_viewer(cfg, row_limit: Optional[int] = 1000) -> Optional[Tuple[DatabaseManager, Optional[str]]]:
    """
    Reusable table viewer for Streamlit.

    - db_folder: folder containing .db files
    - row_limit: max rows to display
    """
    cfg_language = cfg["app"]["language"]

    st.subheader("Database Table Viewer")

    # --- Database selection ---
    databases: List[str] = []
    databases.append(cfg["app"]["database_oauth_uri"])
    databases.append(cfg["app"]["database_app_uri"])
    databases.append(cfg["app"]["database_sde_uri"])

    selected_db = st.selectbox(
        "Select a database",
        databases
    )

    if not selected_db:
        return
    
    db = DatabaseManager(selected_db, cfg_language)

    # --- Table selection ---
    tables = db.list_tables()
    if st.button("Refresh Tables"):
        tables = db.list_tables()
        st.success("Table list refreshed!")

    if not tables:
        st.warning(f"No tables found in {selected_db}.")
        return (db, None)

    selected_table = st.selectbox("Select a table to view", tables)

    # --- SQL search ---
    where_clause = st.text_input(
        "Enter SQL WHERE clause (optional)",
        value=""
    )

    if not selected_table:
        return (db, None)

    try:
        query = f"SELECT * FROM {selected_table}"
        if where_clause.strip():
            query += f" WHERE {where_clause}"
        if row_limit:
            query += f" LIMIT {row_limit}"

        df = pd.read_sql(query, db.engine)
        st.dataframe(df)
    except Exception as e:
        st.error(f"Failed to load table/query: {e}")

    return (db, selected_table)
