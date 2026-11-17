"""The sales table opens with the most recent sale first."""
from streamlit_ui.pages.realized_profit import _table_frame


def test_sales_table_is_sorted_newest_first():
    rows = [
        {"date": "2026-10-14T11:52:00Z", "character_id": 1, "type_name": "B", "quantity": 1},
        {"date": "2026-11-16T13:59:00Z", "character_id": 1, "type_name": "C", "quantity": 1},
        {"date": "2026-03-20T08:00:00Z", "character_id": 1, "type_name": "A", "quantity": 1},
    ]
    frame = _table_frame(rows, {1: "Duke"}, owner_label="Character", owner_id_key="character_id")
    assert list(frame["Item"]) == ["C", "B", "A"]
