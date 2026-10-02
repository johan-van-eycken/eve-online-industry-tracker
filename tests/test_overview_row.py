# tests/test_overview_row.py
from __future__ import annotations

from eve_online_industry_tracker.application.industry import overview_row as orow

ROW = {
    "type_id": 12345,
    "type_name": "Hobgoblin II",
    "quantity": 10,
    "blueprint_type_id": 999,
    "isk_per_hour": 4_000_000.0,
    "profit_amount": 1_000_000.0,
    "profit_margin_fraction": 0.2,
    "days_of_supply": 6.5,
    "meta_group_name": "Tech II",
    "pipeline_units_in_jobs": 40,
    "pipeline_units_on_market": 60,
    "pipeline_days_supply": 12.5,
    "price_trend_7d_pct": 3.5,
    "price_trend_30d_pct": -2.0,
    "manufacturing_job": {"runs": 20, "material_cost": 50_000_000.0},
}


def test_manufacturing_job_returns_empty_dict_when_absent_or_wrong_type():
    assert orow.get_manufacturing_job({}) == {}
    assert orow.get_manufacturing_job({"manufacturing_job": "nope"}) == {}


def test_runs_comes_from_manufacturing_job():
    assert orow.get_effective_runs(ROW) == 20


def test_runs_falls_back_to_product_quantity():
    assert orow.get_effective_runs({"quantity": 7}) == 7


def test_material_cost_per_unit_divides_the_total_by_quantity():
    # 50,000,000 over 10 units
    assert orow.get_material_cost_per_unit(ROW) == 5_000_000.0


def test_material_cost_per_unit_is_none_without_a_usable_quantity():
    assert orow.get_material_cost_per_unit({"manufacturing_job": {"material_cost": 10.0}}) is None


def test_material_cost_per_unit_is_none_when_the_cost_is_absent():
    assert orow.get_material_cost_per_unit({"quantity": 10}) is None


def test_pipeline_accessors_read_the_producer_keys():
    assert orow.get_pipeline_units_in_jobs(ROW) == 40
    assert orow.get_pipeline_units_on_market(ROW) == 60
    assert orow.get_pipeline_days_supply(ROW) == 12.5


def test_pipeline_days_supply_is_none_when_the_producer_could_not_compute_it():
    assert orow.get_pipeline_days_supply({"pipeline_days_supply": None}) is None
    assert orow.get_pipeline_days_supply({}) is None


def test_price_trend_accessors_use_the_7d_and_30d_keys():
    assert orow.get_price_trend_7d_pct(ROW) == 3.5
    assert orow.get_price_trend_30d_pct(ROW) == -2.0
    assert orow.get_price_trend_30d_pct({}) is None


def test_price_trend_7d_defaults_to_zero_meaning_flat():
    assert orow.get_price_trend_7d_pct({}) == 0.0


def test_scalar_accessors():
    assert orow.get_isk_per_hour(ROW) == 4_000_000.0
    assert orow.get_profit_amount(ROW) == 1_000_000.0
    assert orow.get_profit_margin_fraction(ROW) == 0.2
    assert orow.get_days_of_supply(ROW) == 6.5
    assert orow.get_blueprint_type_id(ROW) == 999
    assert orow.get_blueprint_type_id({}) is None
    assert orow.get_meta_group_name(ROW) == "Tech II"


def test_blueprint_type_id_prefers_the_nested_blueprint_sde_path():
    row = {
        "blueprint_type_id": 999,
        "manufacturing_job": {"blueprint_sde": {"blueprint_type_id": 777}},
    }
    assert orow.get_blueprint_type_id(row) == 777


def test_blueprint_type_id_falls_back_to_a_top_level_key():
    assert orow.get_blueprint_type_id({"blueprint_type_id": 999}) == 999


def test_skill_requirements_met_reads_the_precomputed_boolean():
    # The producer sets a single precomputed boolean under
    # manufacturing_job.skills.skill_requirements_met; the accessor just reads it.
    # It does NOT iterate skill entries comparing level vs trained_skill_level.
    row_met = {"manufacturing_job": {"skills": {"skill_requirements_met": True}}}
    row_not_met = {"manufacturing_job": {"skills": {"skill_requirements_met": False}}}
    assert orow.skill_requirements_met(row_met) is True
    assert orow.skill_requirements_met(row_not_met) is False


def test_skill_requirements_met_is_false_when_skills_missing_or_malformed():
    assert orow.skill_requirements_met({}) is False
    assert orow.skill_requirements_met({"manufacturing_job": {"skills": "nope"}}) is False
    assert orow.skill_requirements_met({"manufacturing_job": "nope"}) is False


def test_get_meta_group_name_normalizes_structure_tech_ii_to_tech_ii():
    assert orow.get_meta_group_name({"meta_group_name": "Structure Tech II"}) == "Tech II"


def test_get_meta_group_name_normalizes_abyssal_to_tech_i():
    assert orow.get_meta_group_name({"meta_group_name": "Abyssal"}) == "Tech I"


def test_get_meta_group_name_passes_through_unrecognized_names():
    assert orow.get_meta_group_name({"meta_group_name": "Some Weird Name"}) == "Some Weird Name"
    assert orow.get_meta_group_name({}) == ""


# --- batch materials (F4) -------------------------------------------------------

def test_batch_materials_read_the_producers_scaled_quantities():
    """manufacturing_job.materials is keyed by str(type_id) and its `quantity`
    is already runs x per-run x (1 - ME/structure reduction)."""
    row = {"manufacturing_job": {"materials": {
        "34": {"type_id": 34, "quantity": 1800, "quantity_per_run": 100},
        "35": {"type_id": 35, "quantity": 90, "quantity_per_run": 5},
    }}}
    assert orow.get_batch_materials(row) == {34: 1800, 35: 90}


def test_batch_materials_are_none_when_the_producer_wrote_none():
    """Unknown is not 'needs nothing': callers must tell the two apart."""
    assert orow.get_batch_materials({}) is None
    assert orow.get_batch_materials({"manufacturing_job": {"materials": None}}) is None
    assert orow.get_batch_materials({"manufacturing_job": {"materials": []}}) is None


def test_batch_materials_skip_malformed_entries():
    row = {"manufacturing_job": {"materials": {
        "34": {"type_id": 34, "quantity": 10},
        "x": "nope",
        "36": {"type_id": 36, "quantity": "lots"},
        "37": {"type_id": 0, "quantity": 5},
        "38": {"type_id": 38, "quantity": 0},
    }}}
    assert orow.get_batch_materials(row) == {34: 10}
