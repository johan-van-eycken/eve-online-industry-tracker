import streamlit as st # pyright: ignore[reportMissingImports]
import requests # pyright: ignore[reportMissingModuleSource, reportMissingImports]

# Flask backend
from utils.flask_api import api_get

st.set_page_config(page_title="EVE Online Industry Tracker", layout="wide")
st.title("EVE Online Industry Tracker")

menu_nav = ["Characters", "Corporations", "Market Orders", "Industry Builder", "Ore Calculator"]
menu_admin = ["", "Database Maintenance", "Restart Flask App"]

choice_nav = st.sidebar.selectbox("Navigation", menu_nav)
choice_admin = st.sidebar.selectbox("Admin", menu_admin)

if choice_admin is not None and choice_admin != "":
    choice_nav = None

# Admin actions
if choice_admin == "Restart Flask App":
    if st.button("Confirm Restart"):
        try:
            response = api_get("/shutdown")
            st.success("Flask app is restarting... Please wait a few seconds.")
            st.info("The page will automatically reconnect when Flask is back online.")
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
            # ConnectionError is expected because Flask shuts down immediately
            st.success("Flask app is restarting... Please wait a few seconds.")
            st.info("The page will automatically reconnect when Flask is back online.")
        except Exception as e:
            st.error(f"Error restarting Flask app: {e}")
elif choice_admin == "Database Maintenance":
    from webpages.database_maintenance import render
    render()

# Navigation actions
if choice_nav == "Characters":
    from webpages.characters import render
    render()
elif choice_nav == "Corporations":
    from webpages.corporations import render
    render()
elif choice_nav == "Industry Builder":
    from webpages.industry_builder import render
    render()
elif choice_nav == "Ore Calculator":
    from webpages.ore_calculator import render
    render()
elif choice_nav == "Market Orders":
    from webpages.marketorders import render
    render()
