import streamlit as st
from classes.database_manager import load_df

st.set_page_config(page_title="EVE Online Industry Tracker", layout="wide")

st.title("EVE Online Industry Tracker")

tab1, tab2 = st.tabs(["Assets", "Wallet Transactions"])

with tab1:
    st.subheader("Assets")
    try:
        st.dataframe(load_df("assets"))
    except:
        st.warning("No assets data found. Run main.py first.")

with tab2:
    st.subheader("Wallet Transactions")
    try:
        st.dataframe(load_df("wallet_transactions"))
    except:
        st.warning("No wallet data found. Run main.py first.")
