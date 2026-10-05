# tests/test_fixture_sanitisation.py
from __future__ import annotations

from eve_online_industry_tracker.application.daily_planner.fixture_export import (
    IDENTITY_KEY_SUBSTRINGS,
    sanitise_overview_rows,
)

REDACTED = "<redacted>"

RAW = [
    {
        "type_id": 12345,
        "type_name": "Hobgoblin II",
        "meta_group_name": "Tech II",
        "quantity": 100,
        "isk_per_hour": 4_200_000.0,
        "profit_amount": 1_500_000.0,
        "profit_margin_fraction": 0.18,
        "corporation_id": 98000001,
        "character_name": "Some Pilot",
        "structure_id": 1029999999999,
        "wallet_balance": 8_400_000_000.0,
        "blueprint_source_kind": "owned_blueprint_copy",
        "market_price_fetched_at": "2026-09-01T00:00:00Z",
        "manufacturing_job": {
            "runs": 20,
            "material_cost": 55_000_000.0,
            "materials": {"34": {"type_id": 34, "quantity": 1000}},
            "job_id": 555111222,
            "industry_profile": {
                "profile_name": "My Home Citadel",
                "region_id": 10000002,
            },
        },
        "blueprint_copy": {
            "ship_name": "Some Pilot's Hobgoblin II",
            "blueprint_material_efficiency": 10,
        },
        "notes_list": [
            {"corp_id": 98000001, "label": "internal note"},
        ],
    }
]


def test_structure_and_public_identifiers_are_preserved():
    out = sanitise_overview_rows(RAW)
    assert len(out) == 1
    row = out[0]
    assert row["type_id"] == 12345
    assert row["type_name"] == "Hobgoblin II"
    assert row["meta_group_name"] == "Tech II"
    assert set(row["manufacturing_job"]) == {"runs", "material_cost", "materials", "industry_profile"}
    assert row["manufacturing_job"]["materials"]["34"]["type_id"] == 34


def test_type_name_and_meta_group_name_survive_verbatim():
    row = sanitise_overview_rows(RAW)[0]
    assert row["type_name"] == RAW[0]["type_name"]
    assert row["meta_group_name"] == RAW[0]["meta_group_name"]


def test_identity_keys_are_dropped_at_top_level():
    out = sanitise_overview_rows(RAW)[0]
    for key in ("corporation_id", "character_name", "structure_id", "wallet_balance"):
        assert key not in out
    assert IDENTITY_KEY_SUBSTRINGS  # guard against an empty deny-list


def test_identity_key_nested_inside_a_dict_is_dropped():
    row = sanitise_overview_rows(RAW)[0]
    manufacturing_job = row["manufacturing_job"]
    assert "job_id" not in manufacturing_job
    # sibling keys at the same depth must survive
    assert "runs" in manufacturing_job
    assert "industry_profile" in manufacturing_job


def test_identity_key_inside_a_dict_inside_a_list_is_dropped():
    row = sanitise_overview_rows(RAW)[0]
    note = row["notes_list"][0]
    assert "corp_id" not in note
    # non-identity sibling key survives, but its string value is redacted
    assert note["label"] == REDACTED


def test_profile_name_is_redacted_when_nested_under_industry_profile():
    row = sanitise_overview_rows(RAW)[0]
    profile = row["manufacturing_job"]["industry_profile"]
    assert profile["profile_name"] == REDACTED
    # region_id is numeric, so it is scrambled (not dropped) but present
    assert isinstance(profile["region_id"], int)


def test_ship_name_is_redacted_when_nested_under_blueprint_copy():
    row = sanitise_overview_rows(RAW)[0]
    blueprint_copy = row["blueprint_copy"]
    assert blueprint_copy["ship_name"] == REDACTED
    assert "Some Pilot" not in blueprint_copy["ship_name"]


def test_non_allow_listed_string_value_is_redacted():
    row = sanitise_overview_rows(RAW)[0]
    assert row["market_price_fetched_at"] == REDACTED
    assert row["manufacturing_job"]["industry_profile"]["profile_name"] == REDACTED


def test_timestamp_string_is_redacted():
    row = sanitise_overview_rows(RAW)[0]
    assert row["market_price_fetched_at"] == REDACTED


def test_magnitudes_are_replaced_but_types_and_signs_kept():
    out = sanitise_overview_rows(RAW)[0]
    assert out["isk_per_hour"] != RAW[0]["isk_per_hour"]
    assert isinstance(out["isk_per_hour"], float)
    assert out["isk_per_hour"] > 0
    assert isinstance(out["quantity"], int)
    assert out["quantity"] > 0


def test_sanitisation_is_deterministic():
    assert sanitise_overview_rows(RAW) == sanitise_overview_rows(RAW)


import ast  # noqa: E402
import os  # noqa: E402

import pytest  # noqa: E402

from eve_online_industry_tracker.application.daily_planner.fixture_export import (  # noqa: E402
    BLUEPRINT_SOURCE_KINDS,
)


@pytest.mark.parametrize("kind", sorted(BLUEPRINT_SOURCE_KINDS))
def test_a_blueprint_source_kind_survives_sanitisation(kind):
    (row,) = sanitise_overview_rows(
        [{"type_id": 1, "manufacturing_job": {"blueprint_source_kind": kind}}]
    )
    assert row["manufacturing_job"]["blueprint_source_kind"] == kind


def test_an_unknown_blueprint_source_kind_is_still_redacted():
    (row,) = sanitise_overview_rows(
        [{"type_id": 1, "manufacturing_job": {"blueprint_source_kind": "Some Pilot's hangar"}}]
    )
    assert row["manufacturing_job"]["blueprint_source_kind"] == "<redacted>"


def test_every_literal_source_kind_the_producer_assigns_is_allow_listed():
    producer = os.path.join(
        os.path.dirname(__file__), "..", "src", "eve_online_industry_tracker",
        "application", "industry", "service.py",
    )
    with open(producer, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    written = {
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "blueprint_source_kind" for t in node.targets)
        and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
    }
    assert written, "scan found no blueprint_source_kind assignments"
    assert written <= BLUEPRINT_SOURCE_KINDS, written - BLUEPRINT_SOURCE_KINDS
