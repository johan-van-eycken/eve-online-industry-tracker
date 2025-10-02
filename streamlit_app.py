import streamlit as st
import logging
import requests
import os
from classes.config_manager import ConfigManager
from config.schemas import CONFIG_SCHEMA

# Read environment variables for Flask host and port
FLASK_HOST = os.getenv("FLASK_HOST", "localhost")
FLASK_PORT = os.getenv("FLASK_PORT", "5000")

try:
    cfg = ConfigManager(base_path="config/config.json", schema=CONFIG_SCHEMA, secret_path="config/secret.json").all()
except Exception as e:
    logging.error(f"Failed to initialize schema: {e}")
    raise e

st.set_page_config(page_title="EVE Online Industry Tracker", layout="wide")
st.title("EVE Online Industry Tracker")

menu_nav = ["Characters", "Corporations", "Ore Calculator"]
menu_admin = ["", "Database Maintenance", "Restart Flask App"]

choice_nav = st.sidebar.selectbox("Navigation", menu_nav)
choice_admin = st.sidebar.selectbox("Admin", menu_admin)

if choice_admin is not None and choice_admin != "":
    choice_nav = None
if choice_admin == "Restart Flask App":
    try:
        response = requests.post(f"http://{FLASK_HOST}:{FLASK_PORT}/restart")
        if response.status_code == 200:
            st.success("Flask app is restarting...")
        else:
            st.error(f"Failed to restart Flask app: {response.text}")
    except requests.exceptions.ConnectionError as e:
        # ConnectionError is expected because Flask shuts down immediately
        st.success("Flask app is restarting...")
    except Exception as e:
        st.error(f"Error restarting Flask app: {e}")
elif choice_admin == "Database Maintenance":
    from webpages.database_maintenance import render
    render(cfg)

if choice_nav == "Characters":
    from webpages.characters import render
    render(cfg)
elif choice_nav == "Corporations":
    from webpages.corporations import render
    render(cfg)
elif choice_nav == "Ore Calculator":
    from webpages.ore_calculator import render
    render(cfg)
