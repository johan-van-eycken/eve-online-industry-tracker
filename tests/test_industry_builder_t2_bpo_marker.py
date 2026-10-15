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


def test_drilldown_caption_covers_nested_t2_components():
    caption = industry_builder.ASSUMES_OWNED_T2_BPO_CAPTION
    assert "Nested T2 components" in caption
    assert "owned T2 BPO" in caption and "max ME/TE" in caption


def test_the_drilldown_says_which_decryptor_and_chance_were_used():
    reason = ("Attainment Decryptor: highest profit per invention cycle "
              "(4,971.67 ISK vs 4,110.00 ISK without a decryptor)")
    row = {"manufacturing_job": {
        "invention_decryptor": {"type_id": 34202, "type_name": "Attainment Decryptor", "reason": reason},
        "invention_chance": {"base_probability": 0.34, "skill_factor": 1.4083, "decryptor_multiplier": 1.8,
                             "effective_probability": 0.8618, "skills_unknown_reason": None},
    }}
    lines = ui.invention_summary_lines(row)
    assert lines[0] == f"Decryptor: {reason}"
    assert lines[1] == "Invention chance 86.2%: SDE base 34.0% × skills 1.408 (best inventor) × decryptor 1.8"


def test_the_drilldown_says_when_skills_were_unknown():
    row = {"manufacturing_job": {"invention_chance": {
        "base_probability": 0.34, "skill_factor": None, "decryptor_multiplier": 1.0,
        "effective_probability": 0.34, "skills_unknown_reason": "no corp character has its skills loaded",
    }}}
    assert ui.invention_summary_lines(row) == [
        "Invention chance 34.0%: SDE base 34.0% × decryptor 1 (skills unknown: no corp character has its skills loaded)"
    ]
    assert ui.invention_summary_lines({"manufacturing_job": {}}) == []


def test_an_unknown_invention_node_is_labelled_unknown():
    assert ui.tree_node_activity_label({"activity": "invention", "recommendation_action": "unknown", "runs": 0}) == "unknown"
