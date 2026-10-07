"""The 'costed as if a T2 BPO is owned' flag reaches the grid cell and the drilldown."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from streamlit_ui.pages import industry_builder  # noqa: E402,F401  (import smoke)
from streamlit_ui.state import industry_builder_ui as ui  # noqa: E402


def _row(flag):
    job = {"job_tree": {"node_type": "product", "activity": "product", "type_id": 5002, "children": []}}
    if flag is not None:
        job["assumes_owned_t2_bpo"] = flag
    return {"overview_row_id": "r1", "type_id": 5002, "type_name": "T2 Module",
            "pricing_confidence": "High", "manufacturing_job": job}


def test_drilldown_condition_reads_manufacturing_job():
    assert ui.row_assumes_owned_t2_bpo(_row(True)) is True
    assert ui.row_assumes_owned_t2_bpo(_row(False)) is False
    assert ui.row_assumes_owned_t2_bpo(_row(None)) is False
    # The producer never sets it at the top level; that must not count.
    top = _row(None)
    top["assumes_owned_t2_bpo"] = True
    assert ui.row_assumes_owned_t2_bpo(top) is False


def test_grid_cell_marks_flagged_rows_only():
    assert ui.pricing_confidence_cell("High", {"assumes_owned_t2_bpo": True}) == "High · assumes T2 BPO"
    assert ui.pricing_confidence_cell("High", {}) == "High"
    assert ui.pricing_confidence_cell(None, {"assumes_owned_t2_bpo": True}) is None


def test_flattened_grid_row_carries_the_marker():
    flagged = ui.flatten_overview_job_tree_rows([_row(True)])
    plain = ui.flatten_overview_job_tree_rows([_row(None)])
    assert [r["Pricing Confidence"] for r in flagged] == ["High · assumes T2 BPO"]
    assert [r["Pricing Confidence"] for r in plain] == ["High"]
