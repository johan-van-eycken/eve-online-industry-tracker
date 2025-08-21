import streamlit as st
import pandas as pd
from classes.database_manager import DatabaseManager

st.set_page_config(page_title="EVE Online Industry Tracker", layout="wide")
st.title("EVE Online Industry Tracker")

# ---------------------------
# Database initialiseren
# ---------------------------
db = DatabaseManager("database/eve_data.db")

# ---------------------------
# Tabs: Characters + Database Maintenance
# ---------------------------
tab1, tab2 = st.tabs(["Characters", "Database Maintenance"])

# ---------------------------
# Characters tab
# ---------------------------
with tab1:
    st.subheader("Characters")
    try:
        df = db.load_df("characters")
    except Exception:
        st.warning("No character data found. Run main.py first.")
        st.stop()

    # Fallback voor image_url
    if "image_url" not in df.columns:
        df["image_url"] = df["character_id"].apply(
            lambda cid: f"https://images.evetech.net/characters/{cid}/portrait?size=128"
        )

    # Maak kaarten per rij, max 3 kolommen naast elkaar
    cards_per_row = 5
    for i in range(0, len(df), cards_per_row):
        cols = st.columns(cards_per_row)
        for j, col in enumerate(cols):
            if i + j >= len(df):
                break
            row = df.iloc[i + j]

            # Kaart in een container met donkere achtergrond en padding
            with col:
                st.markdown(
                    f"""
                    <div style="
                        background-color: rgba(30,30,30,0.95);
                        padding: 25px;
                        border-radius: 12px;
                        box-shadow: 2px 2px 10px rgba(0,0,0,0.6);
                        text-align: center;  /* centrum uitlijning */
                        margin-bottom: 10px;
                    ">
                        <img src="{row['image_url']}" width="128" style="border-radius:8px; margin-bottom:10px; display:block; margin-left:auto; margin-right:auto;" />
                        <div style="font-size:16px; line-height:1.3; color:#f0f0f0;">
                            <b style="font-size:20px;">{row['name']}</b><br><br>
                            Birthday: {row.get('birthday', 'N/A')}<br>
                            Bloodline ID: {row.get('bloodline_id', 'N/A')}<br>
                            Corporation ID: {row.get('corporation_id', 'N/A')}<br>
                            Gender: {row.get('gender', 'N/A')}<br>
                            Race ID: {row.get('race_id', 'N/A')}<br>
                            Security Status: {row.get('security_status', 'N/A')}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )


# ---------------------------
# Database Maintenance tab
# ---------------------------
with tab2:
    st.subheader("Database Maintenance")

    # Alle tabellen ophalen
    tables = db.list_tables()
    if not tables:
        st.warning("No tables found in the database.")
    else:
        selected_table = st.selectbox("Select a table to view", tables)
        if selected_table:
            try:
                df = db.load_df(selected_table)
                st.dataframe(df)
            except Exception as e:
                st.error(f"Failed to load table: {e}")
