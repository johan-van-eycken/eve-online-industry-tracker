from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from streamlit_ui.shopping_list import aggregate_shopping_list


def _make_row(
    type_id: int,
    type_name: str,
    max_batches: int,
    materials: dict,
) -> dict:
    return {
        "type_id": type_id,
        "type_name": type_name,
        "max_batches_total": max_batches,
        "manufacturing_job": {"procurement_materials": materials},
    }


def test_aggregates_buy_quantities_across_items() -> None:
    """Two items both needing Tritanium (34) — quantities sum correctly."""
    row_a = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=2,
        materials={
            "34": {
                "type_id": 34,
                "type_name": "Tritanium",
                "quantity": 100,
                "buy_quantity": 100,
                "sourcing_strategy": "buy",
                "unit_price": 5.0,
            }
        },
    )
    row_b = _make_row(
        type_id=1002,
        type_name="Item B",
        max_batches=3,
        materials={
            "34": {
                "type_id": 34,
                "type_name": "Tritanium",
                "quantity": 50,
                "buy_quantity": 50,
                "sourcing_strategy": "buy",
                "unit_price": 5.0,
            }
        },
    )
    result = aggregate_shopping_list([row_a, row_b])
    assert len(result) == 1
    item = result[0]
    assert item["type_id"] == 34
    assert item["need"] == 2 * 100 + 3 * 50  # 350
    assert item["buy"] == 350
    assert item["unit_price"] == 5.0


def test_take_strategy_contributes_zero_to_buy() -> None:
    """A material fully in inventory (sourcing_strategy=take) should have buy=0."""
    row = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=1,
        materials={
            "35": {
                "type_id": 35,
                "type_name": "Pyerite",
                "quantity": 200,
                "buy_quantity": 0,
                "sourcing_strategy": "take",
                "unit_price": 10.0,
            }
        },
    )
    result = aggregate_shopping_list([row])
    assert len(result) == 1
    assert result[0]["buy"] == 0
    assert result[0]["need"] == 200


def test_split_strategy_uses_buy_quantity() -> None:
    """A mixed-source material should only count buy_quantity toward buy."""
    row = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=1,
        materials={
            "36": {
                "type_id": 36,
                "type_name": "Mexallon",
                "quantity": 100,
                "buy_quantity": 40,
                "take_quantity": 60,
                "sourcing_strategy": "split",
                "unit_price": 8.0,
            }
        },
    )
    result = aggregate_shopping_list([row])
    assert len(result) == 1
    assert result[0]["need"] == 100
    assert result[0]["buy"] == 40


def test_sorted_by_total_isk_descending() -> None:
    """Output is sorted by buy * unit_price descending."""
    row = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=1,
        materials={
            "34": {
                "type_id": 34, "type_name": "Tritanium",
                "quantity": 10, "buy_quantity": 10,
                "sourcing_strategy": "buy", "unit_price": 5.0,
            },
            "35": {
                "type_id": 35, "type_name": "Pyerite",
                "quantity": 100, "buy_quantity": 100,
                "sourcing_strategy": "buy", "unit_price": 50.0,
            },
        },
    )
    result = aggregate_shopping_list([row])
    # Pyerite: 100 * 50 = 5000, Tritanium: 10 * 5 = 50 — Pyerite first
    assert result[0]["type_id"] == 35
    assert result[1]["type_id"] == 34


def test_empty_rows_returns_empty_list() -> None:
    assert aggregate_shopping_list([]) == []


def test_row_with_no_manufacturing_job_is_skipped() -> None:
    row = {"type_id": 1001, "type_name": "Broken Row", "max_batches_total": 1}
    result = aggregate_shopping_list([row])
    assert result == []


def test_fallback_when_buy_quantity_key_missing() -> None:
    """If buy_quantity key is absent, fall back: buy strategy -> full quantity, take -> 0."""
    row = _make_row(
        type_id=1001,
        type_name="Item A",
        max_batches=1,
        materials={
            "34": {
                "type_id": 34, "type_name": "Tritanium",
                "quantity": 100,
                # NO buy_quantity key
                "sourcing_strategy": "buy",
                "unit_price": 5.0,
            },
            "35": {
                "type_id": 35, "type_name": "Pyerite",
                "quantity": 50,
                # NO buy_quantity key
                "sourcing_strategy": "take",
                "unit_price": 10.0,
            },
        },
    )
    result = aggregate_shopping_list([row])
    trit = next(r for r in result if r["type_id"] == 34)
    pye = next(r for r in result if r["type_id"] == 35)
    assert trit["buy"] == 100
    assert pye["buy"] == 0
