# tests/test_fixture_sanitisation.py
from __future__ import annotations

from eve_online_industry_tracker.application.daily_planner.fixture_export import (
    IDENTITY_KEY_SUBSTRINGS,
    sanitise_overview_rows,
)

RAW = [
    {
        "type_id": 12345,
        "type_name": "Hobgoblin II",
        "quantity": 100,
        "isk_per_hour": 4_200_000.0,
        "profit_amount": 1_500_000.0,
        "profit_margin_fraction": 0.18,
        "corporation_id": 98000001,
        "character_name": "Some Pilot",
        "structure_id": 1029999999999,
        "wallet_balance": 8_400_000_000.0,
        "manufacturing_job": {
            "runs": 20,
            "material_cost": 55_000_000.0,
            "materials": {"34": {"type_id": 34, "quantity": 1000}},
        },
    }
]


def test_structure_and_public_identifiers_are_preserved():
    out = sanitise_overview_rows(RAW)
    assert len(out) == 1
    row = out[0]
    assert row["type_id"] == 12345
    assert row["type_name"] == "Hobgoblin II"
    assert set(row["manufacturing_job"]) == {"runs", "material_cost", "materials"}
    assert row["manufacturing_job"]["materials"]["34"]["type_id"] == 34


def test_identity_keys_are_dropped_at_every_depth():
    out = sanitise_overview_rows(RAW)[0]
    for key in ("corporation_id", "character_name", "structure_id", "wallet_balance"):
        assert key not in out
    assert IDENTITY_KEY_SUBSTRINGS  # guard against an empty deny-list


def test_magnitudes_are_replaced_but_types_and_signs_kept():
    out = sanitise_overview_rows(RAW)[0]
    assert out["isk_per_hour"] != RAW[0]["isk_per_hour"]
    assert isinstance(out["isk_per_hour"], float)
    assert out["isk_per_hour"] > 0
    assert isinstance(out["quantity"], int)
    assert out["quantity"] > 0


def test_sanitisation_is_deterministic():
    assert sanitise_overview_rows(RAW) == sanitise_overview_rows(RAW)
