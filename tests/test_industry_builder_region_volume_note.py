"""An unknown region daily volume (its ESI history fetch failed) reaches the
Industry Builder with its reason: a "Region Volume Note" next to the volume
columns, a drilldown caption and the debug payload, never only an empty
cell. Fake values throughout."""
from __future__ import annotations

import inspect

from streamlit_ui.pages import industry_builder
from streamlit_ui.state import industry_builder_ui as ui

REASON = "ESI market history unavailable: fake 502"


def _row(reason):
    job = {"job_tree": {"node_type": "product", "activity": "product", "type_id": 5002, "children": [
        {"node_type": "material", "label": "Tritanium", "type_id": 34, "region_daily_volume": 900}]},
        "region_daily_volume": None, "region_daily_volume_unavailable_reason": reason}
    return {"overview_row_id": "r1", "type_id": 5002, "type_name": "Module", "pricing_confidence": "Low",
            "region_daily_volume": None if reason else 40, "region_daily_volume_unavailable_reason": reason,
            "manufacturing_job": job}


def test_the_note_names_why_the_volume_is_unknown():
    assert ui.region_volume_note(_row(REASON)) == f"region daily volume unknown: {REASON}"
    assert ui.region_volume_note(_row(None)) == ""


def test_the_note_falls_back_to_the_manufacturing_job():
    row = _row(REASON)
    row["region_daily_volume_unavailable_reason"] = None
    assert REASON in ui.region_volume_note(row)


def test_the_product_grid_row_carries_the_note_and_a_material_row_does_not():
    product, material = ui.flatten_overview_job_tree_rows([_row(REASON)])
    assert product["Region Daily Volume"] is None
    assert product["Region Volume Note"] == f"region daily volume unknown: {REASON}"
    assert material["Region Daily Volume"] == 900 and material["Region Volume Note"] == ""
    (plain, _material) = ui.flatten_overview_job_tree_rows([_row(None)])
    assert plain["Region Daily Volume"] == 40 and plain["Region Volume Note"] == ""


def test_the_note_column_sits_next_to_the_volume_columns():
    keys = list(ui.flatten_overview_job_tree_rows([_row(REASON)])[0])
    assert keys.index("Region Volume Note") == keys.index("Region Daily Volume (7d Avg)") + 1


def test_the_drilldown_and_debug_payload_show_the_reason():
    assert "region_volume_note(selected_row)" in inspect.getsource(industry_builder)
    preview = ui.build_debug_payload_preview(_row(REASON))
    assert preview["manufacturing_job"]["region_daily_volume_unavailable_reason"] == REASON


# --- the minimum region daily volume filter ---------------------------------------------------------

def _filter_row(type_id, volume, reason=None):
    return {"type_id": type_id, "type_name": f"Fake {type_id}", "meta_group_name": "Tech I",
            "profit_amount": 10.0, "profit_margin_fraction": 0.5, "isk_per_hour": 100.0,
            "region_daily_volume": volume, "region_daily_volume_unavailable_reason": reason,
            "manufacturing_job": {}}


def _args(rows, min_volume, meta_group=None):
    meta_group = meta_group or ui.get_meta_group_name(rows[0])
    return (rows, (meta_group,), False, False, 0.0, 0.0, min_volume, (), ())


def test_unknown_volume_rows_stay_hidden_but_are_counted_with_their_reason():
    rows = [_filter_row(1, 500), _filter_row(2, None, REASON), _filter_row(3, None, REASON),
            _filter_row(4, None), _filter_row(5, 3)]
    kept = ui.filter_overview_rows(*_args(rows, 10))
    assert [r["type_id"] for r in kept] == [1]           # conservative: unknown is left out
    note = ui.unknown_region_volume_hidden_note(*_args(rows, 10))
    assert note.startswith("3 rows hidden: region daily volume unknown (")
    assert REASON in note and "no reason given" in note
    assert "Fake 5" not in note                          # a known low volume is not counted


def test_no_note_without_a_minimum_or_without_unknown_rows():
    rows = [_filter_row(1, 500), _filter_row(2, None, REASON)]
    assert ui.unknown_region_volume_hidden_note(*_args(rows, 0)) is None
    assert ui.unknown_region_volume_hidden_note(*_args([_filter_row(1, 500), _filter_row(5, 3)], 10)) is None


def test_a_row_hidden_by_another_filter_is_not_counted():
    rows = [_filter_row(1, 500), _filter_row(2, None, REASON)]
    rows[1]["meta_group_name"] = "Tech II"
    assert ui.unknown_region_volume_hidden_note(*_args(rows, 10, meta_group=ui.get_meta_group_name(rows[0]))) is None


def test_one_row_is_singular():
    rows = [_filter_row(1, 500), _filter_row(2, None, REASON)]
    assert ui.unknown_region_volume_hidden_note(*_args(rows, 10)) == (
        f"1 row hidden: region daily volume unknown ({REASON})")


