import streamlit as st # pyright: ignore[reportMissingImports]
import requests # pyright: ignore[reportMissingModuleSource, reportMissingImports]
import time

# Flask backend
from utils.flask_api import api_get, api_post


def _rerun() -> None:
    # Streamlit renamed rerun APIs across versions.
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()

st.set_page_config(page_title="EVE Online Industry Tracker", layout="wide")
st.title("EVE Online Industry Tracker")

menu_nav = ["Characters", "Corporations", "Market Orders", "Industry Builder", "Ore Calculator", "Settings"]
menu_admin = ["", "Database Maintenance", "Restart Flask App", "Public Structures Scan"]

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
elif choice_admin == "Public Structures Scan":
    st.subheader("Public Structures Scan")

    if st.button("Confirm Scan"):
        try:
            response = api_post("/public_structures_scan/start", {})
            if response is not None:
                started = bool((response.get("meta") or {}).get("started"))
                if started:
                    st.success("Global public structures scan started in the background.")
                else:
                    st.info("Global public structures scan is already running.")
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
            st.error(f"Failed to contact Flask API: {e}")
        except Exception as e:
            st.error(f"Error starting public structures scan: {e}")

    auto_refresh = st.checkbox("Auto-refresh", value=True, help="Refresh status every ~2s while running")

    status = api_get("/public_structures_status")
    if status is not None:
        data = status.get("data") or {}
        gs = data.get("global_scan") or {}
        running = bool(gs.get("running"))

        total_ids = gs.get("total_ids")
        cursor = gs.get("cursor")
        attempted = gs.get("attempted")
        rows_written = gs.get("rows_written")

        if isinstance(total_ids, int) and isinstance(cursor, int) and total_ids > 0:
            pct = max(0.0, min(1.0, float(cursor) / float(total_ids)))
            st.progress(pct)
            st.caption(f"Progress: {cursor}/{total_ids} ({pct * 100:.1f}%)")

        metrics_cols = st.columns(3)
        metrics_cols[0].metric("Running", "yes" if running else "no")
        metrics_cols[1].metric("Attempted", str(attempted) if attempted is not None else "-")
        metrics_cols[2].metric("Rows written", str(rows_written) if rows_written is not None else "-")

        if gs.get("error"):
            st.error(f"Scan error: {gs.get('error')}")
        elif (not running) and gs.get("finished_at"):
            st.success("Scan finished.")

        with st.expander("Details", expanded=False):
            st.json(gs)

        if auto_refresh and running:
            time.sleep(2)
            _rerun()
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
elif choice_nav == "Settings":
    from webpages.industry_profiles import render
    render()
