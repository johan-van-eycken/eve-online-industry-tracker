import streamlit as st
from utils.table_viewer import render_table_viewer

def render():
    render_table_viewer(row_limit=2000)  # optional row limit