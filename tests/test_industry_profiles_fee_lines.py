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


def test_one_admin_settings_read_gives_every_rate(monkeypatch):
    """Fix round 1, minor 6: the page reads /admin_settings once per render
    and derives the NPC defaults and the SCC rates from that one read."""
    calls: list[str] = []
    payload = {"status": "success", "data": {"values": {"industry": {
        "npc_station_facility_tax": 0.003, "scc_surcharge": 0.05, "research_scc_surcharge": 0.02,
    }}}}
    monkeypatch.setattr(page, "api_get", lambda path, *a, **k: calls.append(path) or payload)
    values = page._admin_industry_values()
    assert page._npc_admin_defaults(values) == (0.003, 0.05)
    assert page._job_scc_rates(values) == (0.05, 0.02)
    assert calls == ["/admin_settings"]


def test_the_rates_fall_back_to_the_defaults_without_admin_settings(monkeypatch):
    monkeypatch.setattr(page, "api_get", lambda path, *a, **k: None)
    values = page._admin_industry_values()
    assert page._npc_admin_defaults(values) == (page._NPC_STATION_FACILITY_TAX, page._NPC_STATION_SCC_SURCHARGE)
    assert page._job_scc_rates(values) == (page.DEFAULT_SCC_SURCHARGE, page.DEFAULT_RESEARCH_SCC_SURCHARGE)
