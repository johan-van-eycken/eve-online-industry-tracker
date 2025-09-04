import streamlit as st
import logging
from classes.config_manager import ConfigManager
from config.schemas import CONFIG_SCHEMA

try:
    cfg = ConfigManager(base_path="config/config.json", schema=CONFIG_SCHEMA, secret_path="config/secret.json").all()
except Exception as e:
    logging.error(f"Failed to initialize schema: {e}")
    raise e

st.set_page_config(page_title="EVE Online Industry Tracker", layout="wide")
st.title("EVE Online Industry Tracker")

menu = ["Characters", "Corporations", "Database Maintenance"]
choice = st.sidebar.selectbox("Navigation", menu)

if choice == "Characters":
    from webpages.characters import render
    render(cfg)
elif choice == "Corporations":
    from webpages.corporations import render
    render(cfg)
elif choice == "Database Maintenance":
    from webpages.database_maintenance import render
    render(cfg)
