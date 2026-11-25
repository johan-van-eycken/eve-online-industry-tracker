"""One row of stat tiles in the app's tile style (the Realized Profit and
Wallet tiles): a bordered strip, each tile a label, a value and an optional
grey line under it."""
from __future__ import annotations

from html import escape

import streamlit as st

_CSS = """
<style>
.stat-tiles {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    border: 1px solid rgba(255,255,255,0.10);
    border-radius: 12px;
    background: rgba(255,255,255,0.02);
    margin-bottom: 0.75rem;
}
.stat-tile { padding: 12px 14px; position: relative; }
.stat-tile:not(:last-child)::after {
    content: ""; position: absolute; top: 14px; right: 0;
    width: 1px; height: calc(100% - 28px); background: rgba(255,255,255,0.10);
}
.stat-tile-label { color: #d4d4d8; font-size: 0.78rem; font-weight: 600; margin-bottom: 6px; }
.stat-tile-value { color: #f4f4f5; font-size: 1.4rem; font-weight: 700; line-height: 1.1; }
.stat-tile-sub { color: #a1a1aa; font-size: 0.78rem; margin-top: 6px; line-height: 1.3; }
</style>
"""


def stat_tiles_html(tiles: list[tuple[str, str, str | None]]) -> str:
    """The tiles' HTML; every text is escaped (item and pilot names are game data)."""
    cells = "".join(
        "<div class='stat-tile'>"
        f"<div class='stat-tile-label'>{escape(label)}</div>"
        f"<div class='stat-tile-value'>{escape(value)}</div>"
        + (f"<div class='stat-tile-sub'>{escape(sub)}</div>" if sub else "")
        + "</div>"
        for label, value, sub in tiles
    )
    return f"{_CSS}<div class='stat-tiles'>{cells}</div>"


def render_stat_tiles(tiles: list[tuple[str, str, str | None]]) -> None:
    st.markdown(stat_tiles_html(tiles), unsafe_allow_html=True)
