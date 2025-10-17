import streamlit as st
import logging
import requests
import os
from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.character_manager import CharacterManager
from classes.corporation_manager import CorporationManager
from config.schemas import CONFIG_SCHEMA

# Read environment variables for Flask host and port
FLASK_HOST = os.getenv("FLASK_HOST", "localhost")
FLASK_PORT = os.getenv("FLASK_PORT", "5000")

# Load Configurations
try:
    cfgManager = ConfigManager(base_path="config/config.json", secret_path="config/secret.json", schema=CONFIG_SCHEMA)
    cfg = cfgManager.all()
    cfg_language = cfg["app"]["language"]
    cfg_characters = cfg["characters"]
    if len(cfg_characters) == 0:
        raise ValueError("No characters found in config!")
    cfg_oauth_db_uri = cfg["app"]["database_oauth_uri"]
    cfg_app_db_uri = cfg["app"]["database_app_uri"]
    cfg_sde_db_uri = cfg["app"]["database_sde_uri"]
except Exception as e:
    logging.error(f"Failed to load config: {e}")
    raise ValueError(f"Failed to load config: {e}")

# Initialize Databases and Schemas
try:
    db_oauth = DatabaseManager(cfg_oauth_db_uri, cfg_language)
    db_app = DatabaseManager(cfg_app_db_uri, cfg_language)
    db_sde = DatabaseManager(cfg_sde_db_uri, cfg_language)
except Exception as e:
    logging.error(f"Database initializations failed. {e}", exc_info=True)
    raise ValueError(f"Database initializations failed. {e}")

# Initialize Character Manager
try:
    char_manager_all = CharacterManager(cfgManager, db_oauth, db_app, db_sde)
    char_manager_all.refresh_all()
except Exception as e:
    logging.error(f"Failed to initialize characters: {e}")
    raise ValueError(f"Failed to initialize characters: {e}")

st.set_page_config(page_title="EVE Online Industry Tracker", layout="wide")
st.title("EVE Online Industry Tracker")

menu_nav = ["Characters", "Corporations", "Market Orders", "Ore Calculator"]
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
    render(char_manager_all)
elif choice_nav == "Market Orders":
    from webpages.marketorders import render
    render()
