import streamlit as st
from classes.config_manager import ConfigManager

cfg = ConfigManager()

st.set_page_config(page_title="EVE Online Industry Tracker", layout="wide")
st.title("EVE Online Industry Tracker")

menu = ["Characters", "Database Maintenance"]
choice = st.sidebar.selectbox("Navigation", menu)

if choice == "Characters":
    from webpages.characters import render
    render()
elif choice == "Database Maintenance":
    from webpages.database_maintenance import render
    render()
