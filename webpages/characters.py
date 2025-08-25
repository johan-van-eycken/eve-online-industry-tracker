import streamlit as st
from classes.database_manager import DatabaseManager
from utils.formatters import format_isk, format_date, format_date_into_age

def render():
    st.subheader("Characters")
    db = DatabaseManager("eve_data.db")

    try:
        df = db.load_df("characters")
    except Exception:
        st.warning("No character data found. Run main.py first.")
        st.stop()

    if "image_url" not in df.columns:
        df["image_url"] = df["character_id"].apply(
            lambda cid: f"https://images.evetech.net/characters/{cid}/portrait?size=128"
        )

    # Button refresh wallet balances
    if st.button("Refresh Wallet Balances"):
        
        st.success("Character Wallet balances refreshed!")

    cards_per_row = 5
    for i in range(0, len(df), cards_per_row):
        cols = st.columns(cards_per_row)
        for j, col in enumerate(cols):
            if i + j >= len(df):
                break
            row = df.iloc[i + j]

            with col:
                st.markdown(
                    f"""
                    <div style="
                        background-color: rgba(30,30,30,0.95);
                        padding: 25px;
                        border-radius: 12px;
                        box-shadow: 2px 2px 10px rgba(0,0,0,0.6);
                        text-align: center;
                        margin-bottom: 10px;
                    ">
                        <img src="{row['image_url']}" width="128" style="border-radius:8px; margin-bottom:10px; display:block; margin-left:auto; margin-right:auto;" />
                        <div style="font-size:16px; line-height:1.3; color:#f0f0f0;">
                            <b style="font-size:20px;">{row['name']}</b><br>
                            <br>
                            <b>Wallet Balance:<br>
                            {format_isk(row.get('wallet_balance'))}</b><br>
                            <br>
                            <div style="font-size:16px; text-align:left;">
                                Birthday: {format_date(row.get('birthday'))}<br>
                                Age: {format_date_into_age(row.get('birthday'))}<br>
                                Gender: {row.get('gender', 'N/A')}<br>
                                Corporation ID: {row.get('corporation_id', 'N/A')}<br>
                                Race: {row.get('race', 'N/A')}<br>
                                Bloodline: {row.get('bloodline', 'N/A')}<br>
                                Security Status: {row.get('security_status', 'N/A'):.2f}
                            </div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
