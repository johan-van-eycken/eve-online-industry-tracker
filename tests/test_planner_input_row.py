# tests/test_planner_input_row.py
from __future__ import annotations

import dataclasses

import pytest

from eve_online_industry_tracker.application.daily_planner.input_row import (
    PlannerInputError,
    PlannerInputRow,
)


class _MetaGroups:
    def meta_group_id(self, type_id):
        return 2 if type_id == 12345 else None


GOOD = {
    "type_id": 12345,
    "type_name": "Hobgoblin II",
    "quantity": 10,
    "isk_per_hour": 4_000_000.0,
    "profit_amount": 1_000_000.0,
    "profit_margin_fraction": 0.2,
    "days_of_supply": 6.5,
    "pipeline_units_in_jobs": 40,
    "pipeline_units_on_market": 60,
    "pipeline_days_supply": 12.5,
    "price_trend_7d_pct": 3.5,
    "price_trend_30d_pct": -2.0,
    # blueprint_type_id nests under manufacturing_job.blueprint_sde on real
    # rows (industry/service.py:6921, :7251) -- there is no top-level key.
    "manufacturing_job": {
        "runs": 20,
        "material_cost": 50_000_000.0,
        "blueprint_sde": {"blueprint_type_id": 999},
    },
}


def _build(row):
    return PlannerInputRow.from_overview(row, meta_groups=_MetaGroups())


def test_a_complete_row_builds():
    got = _build(GOOD)
    assert got.type_id == 12345
    assert got.runs == 20
    assert got.material_cost_per_unit == 5_000_000.0
    assert got.meta_group_id == 2
    assert got.pipeline_days_supply == 12.5
    assert got.blueprint_type_id == 999


def test_material_cost_absent_key_yields_none_not_a_raise():
    """material_cost can legitimately be absent: _enrich_product_rows_with_material_prices
    (industry/service.py:7413, :7432, :7454, :7457) returns/continues before
    ever setting it when pricing could not be computed. That is ordinary
    data, not a rename -- it must not raise.
    """
    row = dict(GOOD, manufacturing_job={"runs": 20})
    got = _build(row)
    assert got.material_cost_per_unit is None


def test_material_cost_none_value_yields_none_not_a_raise():
    row = dict(GOOD, manufacturing_job={"runs": 20, "material_cost": None})
    got = _build(row)
    assert got.material_cost_per_unit is None


def test_material_cost_non_numeric_value_raises_naming_the_nested_field():
    row = dict(GOOD, manufacturing_job={"runs": 20, "material_cost": "not-a-number"})
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "manufacturing_job.material_cost"
    assert exc.value.type_id == 12345


def test_malformed_manufacturing_job_raises_naming_the_container():
    row = dict(GOOD, manufacturing_job="nope")
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "manufacturing_job"


def test_missing_type_id_raises():
    row = dict(GOOD)
    del row["type_id"]
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "type_id"


def test_non_numeric_type_id_raises():
    row = dict(GOOD, type_id="not-an-int")
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "type_id"


def test_zero_quantity_raises_rather_than_dividing_by_zero():
    row = dict(GOOD, quantity=0)
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "quantity"


def test_non_numeric_quantity_raises():
    row = dict(GOOD, quantity="ten")
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "quantity"


def test_a_renamed_producer_key_raises_instead_of_zeroing():
    row = dict(GOOD)
    row["isk_per_hr"] = row.pop("isk_per_hour")
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "isk_per_hour"


def test_non_numeric_isk_per_hour_raises():
    row = dict(GOOD, isk_per_hour="lots")
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "isk_per_hour"


def test_missing_type_name_falls_back_to_the_type_id_string():
    """type_name is display-only: the SDE can lack a name for a type
    (infrastructure/sde/blueprints.py's _enrich_type_entry falls back to
    {"type_id": type_id}), and the producer itself tolerates that with
    `type_name or type_id` (industry/service.py:2526, :7193). Losing the
    rename guard on this one display-only field is a fair trade for not
    aborting the whole plan over one un-named product.
    """
    row = dict(GOOD)
    del row["type_name"]
    got = _build(row)
    assert got.type_name == "12345"


def test_renamed_runs_key_raises_instead_of_falling_back_to_quantity():
    """quantity on the row is product_quantity_per_run * effective_runs (a
    batch-units total, industry/service.py:6916) -- NOT a run count.
    Silently falling back to it for a renamed `runs` key would multiply
    material quantities by the wrong factor several tasks downstream.
    """
    row = dict(GOOD)
    row["manufacturing_job"] = dict(row["manufacturing_job"])
    row["manufacturing_job"]["run_count"] = row["manufacturing_job"].pop("runs")
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "manufacturing_job.runs"


def test_non_numeric_runs_raises():
    row = dict(GOOD)
    row["manufacturing_job"] = dict(row["manufacturing_job"], runs="lots")
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "manufacturing_job.runs"


def test_optional_fields_become_none_without_raising():
    row = dict(GOOD)
    del row["pipeline_days_supply"]
    del row["price_trend_30d_pct"]
    del row["days_of_supply"]
    got = _build(row)
    assert got.pipeline_days_supply is None
    assert got.price_trend_30d_pct is None
    assert got.days_of_supply is None


def test_unknown_meta_group_is_none_not_an_error():
    row = dict(GOOD, type_id=55555)
    assert _build(row).meta_group_id is None


def test_blueprint_type_id_is_none_without_the_nested_key():
    row = dict(GOOD)
    row["manufacturing_job"] = {"runs": 20, "material_cost": 50_000_000.0}
    assert _build(row).blueprint_type_id is None


def test_the_row_is_frozen_and_keeps_raw_for_legacy_reads():
    got = _build(GOOD)
    with pytest.raises(dataclasses.FrozenInstanceError):
        got.type_id = 1  # type: ignore[misc]
    assert got.raw["type_name"] == "Hobgoblin II"


def test_raw_is_a_live_reference_not_a_copy():
    """chain_planner.py (~lines 176-246) writes decision keys back into
    decision.overview_row, which is this same dict -- that mutation is
    load-bearing for the chain-planner-to-character-assigner handoff, so
    `raw` must alias the caller's dict rather than copy it.
    """
    row = dict(GOOD)
    got = _build(row)
    row["needs_me_research"] = True
    assert got.raw["needs_me_research"] is True


def test_a_non_dict_row_raises():
    with pytest.raises(PlannerInputError) as exc:
        PlannerInputRow.from_overview(["not", "a", "dict"], meta_groups=_MetaGroups())
    assert exc.value.field == "<row>"


def test_producer_null_for_isk_per_hour_is_preserved_as_none_not_an_error():
    """The producer (service.py:2260-2293) always writes the isk_per_hour,
    profit_amount and profit_margin_fraction keys, but their VALUE is
    legitimately None whenever net_proceeds, total_cost or time_seconds are
    themselves unavailable (e.g. a product with no sale history yet). The key
    being *present but null* is business-as-usual and must not raise -- only
    the key being entirely *absent* (a rename) should raise.
    """
    row = dict(GOOD, isk_per_hour=None, profit_amount=None, profit_margin_fraction=None)
    got = _build(row)
    assert got.isk_per_hour is None
    assert got.profit_amount is None
    assert got.profit_margin_fraction is None
