import streamlit as st
import glob
import os
import pandas as pd
from classes.database_manager import DatabaseManager
from typing import Optional

def render_table_viewer(db_folder: str = "database", row_limit: Optional[int] = 1000):
    """
    Reusable table viewer for Streamlit.

    - db_folder: folder containing .db files
    - row_limit: max rows to display
    """
    st.subheader("Database Table Viewer")

    # --- Database selection ---
    db_files = glob.glob(os.path.join(db_folder, "*.db"))

    if st.button("Refresh Databases"):
        db_files = glob.glob(os.path.join(db_folder, "*.db"))
        st.success("Database list refreshed!")

    if not db_files:
        st.warning(f"No database files found in {db_folder}/")
        return

    selected_db_file = st.selectbox(
        "Select a database",
        db_files,
        format_func=lambda x: os.path.basename(x)
    )

    if not selected_db_file:
        return

    db = DatabaseManager(selected_db_file)

    # --- Table selection ---
    tables = db.list_tables()
    if st.button("Refresh Tables"):
        tables = db.list_tables()
        st.success("Table list refreshed!")

    if not tables:
        st.warning(f"No tables found in {os.path.basename(selected_db_file)}.")
        return

    selected_table = st.selectbox("Select a table to view", tables)

    # --- SQL search ---
    where_clause = st.text_input(
        "Enter SQL WHERE clause (optional)",
        value=""
    )

    if not selected_table:
        return

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
