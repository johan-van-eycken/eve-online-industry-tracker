"""The industry profile page's job-fee lines (round 5 Task 10, carries 9, 11)."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from streamlit_ui.pages import industry_profiles as page  # noqa: E402


def test_the_npc_create_preview_asks_for_a_station_before_one_is_picked():
    line = page._npc_create_fee_line(None, None, scc_rates=(0.04, 0.04))
    assert line == "pick a station first"
    assert "re-save" not in line


def test_the_npc_create_preview_shows_the_rates_once_a_station_is_picked():
    line = page._npc_create_fee_line(60003760, 0.0025, scc_rates=(0.04, 0.04))
    assert line.startswith("Facility tax 0.25%")


def test_fee_lines_use_the_rates_fetched_once_per_render(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(page, "api_get", lambda path, *a, **k: calls.append(path) or None)
    for tax in (0.01, 0.02, 0.03):
        page._job_fee_rates_line({"facility_tax": tax}, scc_rates=(0.04, 0.02))
    assert calls == []
