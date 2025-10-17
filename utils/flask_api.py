import streamlit as st
import requests
import os

FLASK_HOST = os.getenv("FLASK_HOST", "localhost")
FLASK_PORT = os.getenv("FLASK_PORT", "5000")
API_BASE = f"http://{FLASK_HOST}:{FLASK_PORT}"

#-- API Helpers --
def api_post(path, payload):
    r = requests.post(f"{API_BASE}{path}", json=payload)
    if r.status_code != 200:
        st.error(f"{path} failed: {r.text}")
        return None
    return r.json()

def api_get(path):
    r = requests.get(f"{API_BASE}{path}")
    if r.status_code != 200:
        st.error(f"{path} failed: {r.text}")
        return None
    return r.json()