"""Job fees in the producer: per attempt, per copy run, per research level; unknown fee inputs."""

from __future__ import annotations

from types import SimpleNamespace
import pytest
from industry_service_fakes import (
    _blueprint_row,
    _build_service,
    _invention_overview,
    _owned_t2_bpc,
)


# --- round 5 Task 8: fees per attempt, per copy run and per research level (D4, A2) ---


# Fake rates: facility tax 6% + SCC 4% = 10% on manufacturing, copy and
# invention job values; 6% + research SCC 2% = 8% on research job values.
_FEE_PROFILE = {
    "installation_cost_modifier": 0.1, "facility_tax": 0.06, "material_efficiency_bonus": 0.0,
    "time_efficiency_bonus": 0.0, "facility_cost_bonus": 0.0,
    "system_cost_indices": [
        {"activity": "manufacturing", "cost_index": 0.05},
        {"activity": "copying", "cost_index": 0.03},
        {"activity": "invention", "cost_index": 0.02},
        {"activity": "researching_material_efficiency", "cost_index": 0.02},
        {"activity": "researching_time_efficiency", "cost_index": 0.04},
    ],
    "structure_rigs": [],
}


def test_the_invention_publishes_its_fee_per_attempt_and_per_source_copy_run(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, profile=_FEE_PROFILE)[5002]["manufacturing_job"]
    invention = mj["activity_breakdown"]["invention"]
    # One attempt: job value 0.02 x the T2 blueprint's EIV 50 = 1.0, x (index
    # 0.02 + surcharge 0.10) = 0.12.
    assert invention["job_cost_per_attempt"] == pytest.approx(0.12)
    # One source-copy run: job value 0.02 x source EIV 3 x 5 = 0.3, x (0.03 + 0.10).
    assert invention["source_copy_job_cost_per_run"] == pytest.approx(0.039)
    # The expected-value fee is the per-attempt fee x the expected attempts.
    assert invention["total_job_cost"] == pytest.approx(0.12 * invention["runs"])


def _owned_bpo_manufacturing_job(*, level_one_seconds: int = 105, modifier_skills: list | None = None) -> dict:
    original_asset = SimpleNamespace(
        type_id=9001, item_id=42, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
    )
    blueprint_row = _blueprint_row()
    blueprint_row["research_material_job"] = {"time_seconds": level_one_seconds}
    blueprint_row["research_time_job"] = {"time_seconds": level_one_seconds}
    service = _build_service(
        blueprint_rows=[blueprint_row], profile=_FEE_PROFILE,
        character_modifiers={"modifier_skills": modifier_skills or [], "implants": []}, trained_skill_levels={},
        owned_assets=([], [original_asset], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    (row,) = service.industry_manufacturing_product_overview(
        build_from_bpc=False, have_blueprint_source_only=True, maximize_bp_runs=False, character_id=1,
    )
    mj = row["manufacturing_job"]
    assert mj["blueprint_source_kind"] == "owned_blueprint_original"
    return mj


def _owned_bpo_research_table(*, level_one_seconds: int = 105) -> dict:
    return _owned_bpo_manufacturing_job(level_one_seconds=level_one_seconds)["research_job_cost_by_level"]


def test_an_owned_bpo_row_publishes_its_cumulative_research_fee_per_level() -> None:
    table = _owned_bpo_research_table()
    # Per-run EIV 10 x 5 = 50. EVE's research job value: EIV x 0.02 x the
    # dimensionless level multiplier (rank-1 table seconds / 105); fee = job
    # value x (index + the 8 % research surcharge).
    me = table["research_material"]
    assert len(me) == 11 and me[0] == 0.0
    assert me[1] == pytest.approx(50 * 0.02 * (105 / 105) * (0.02 + 0.08))
    assert me[5] == pytest.approx(50 * 0.02 * (3360 / 105) * (0.02 + 0.08))
    assert me[10] == pytest.approx(50 * 0.02 * (256000 / 105) * (0.02 + 0.08))
    te = table["research_time"]
    assert te[5] == pytest.approx(50 * 0.02 * (3360 / 105) * (0.04 + 0.08))


def test_the_research_fee_uses_the_level_multiplier_not_the_rank_scaled_seconds() -> None:
    """Fix round 1: the old formula multiplied by the rank-scaled duration in
    SECONDS, so a rank-20 blueprint's ME 0 -> 10 fee was 20 x 105 = 2100x
    the real one. Real ESI jobs (same facility and activity, a week apart)
    cost the same per ISK of EIV at rank 20 and rank 40: the fee does not
    scale with rank, only the duration does. Fake EIV 50, fake index 0.02."""
    rank_one = _owned_bpo_research_table(level_one_seconds=105)
    rank_twenty = _owned_bpo_research_table(level_one_seconds=20 * 105)
    expected = 50 * 0.02 * (256000 / 105) * (0.02 + 0.08)
    assert rank_one["research_material"][10] == pytest.approx(expected)
    assert rank_twenty["research_material"][10] == pytest.approx(expected)


def test_an_owned_bpo_row_publishes_its_cumulative_research_duration_per_level() -> None:
    """Task 8c fix round 1: the research-time model the fee uses (rank x the
    rank-1 level table, then the skill / structure time reductions), per
    level, so the planner can show a research action's duration for its
    exact level span. Fake rank 20, no skills, no structure time bonus."""
    durations = _owned_bpo_manufacturing_job(level_one_seconds=20 * 105)["research_duration_seconds_by_level"]
    te = durations["research_time"]
    assert len(te) == 11 and te[0] == 0
    assert te[5] == 20 * 3360
    # TE 0 -> 20 is levels 0 -> 10.
    assert te[10] - te[0] == 20 * 256000
    assert durations["research_material"][10] == 20 * 256000


def test_the_unskilled_research_duration_leaves_the_research_skills_out() -> None:
    """Fix round 2: the planner applies the ASSIGNED pilot's skills to the
    unskilled table. The producer character here has Research 5 (-25% TE
    research time); the unskilled table leaves that out. Fake rank 20."""
    mj = _owned_bpo_manufacturing_job(level_one_seconds=20 * 105, modifier_skills=[
        {"type_name": "Research", "trained_skill_level": 5},
    ])
    skilled = mj["research_duration_seconds_by_level"]["research_time"]
    unskilled = mj["research_duration_seconds_by_level_unskilled"]["research_time"]
    assert unskilled[10] == 20 * 256000
    assert skilled[10] == 20 * 256000 * 3 // 4
    # ME research has no Research bonus: both tables agree.
    assert mj["research_duration_seconds_by_level"]["research_material"] == (
        mj["research_duration_seconds_by_level_unskilled"]["research_material"]
    )


def test_the_base_research_duration_leaves_skills_and_implants_out() -> None:
    """Task 12 fix round 1: the planner times research with the assigned
    pilot's own skills and implants, on a table that has neither. A fake
    implant cuts the producer character's TE research time 10 %."""
    implant = {"modifiers": [{"activity": "research_te", "metric": "time", "value": 0.1}]}
    original_asset = SimpleNamespace(
        type_id=9001, item_id=42, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
    )
    blueprint_row = _blueprint_row()
    blueprint_row["research_material_job"] = {"time_seconds": 20 * 105}
    blueprint_row["research_time_job"] = {"time_seconds": 20 * 105}
    service = _build_service(
        blueprint_rows=[blueprint_row], profile=_FEE_PROFILE,
        character_modifiers={"modifier_skills": [], "implants": [implant]}, trained_skill_levels={},
        owned_assets=([], [original_asset], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    (row,) = service.industry_manufacturing_product_overview(
        build_from_bpc=False, have_blueprint_source_only=True, maximize_bp_runs=False, character_id=1,
    )
    mj = row["manufacturing_job"]
    assert mj["research_duration_seconds_by_level_unskilled"]["research_time"][10] == round(20 * 256000 * 0.9)
    assert mj["research_duration_seconds_by_level_base"]["research_time"][10] == 20 * 256000
    assert mj["research_implant_time_multiplier_by_character_id"]["1"] == pytest.approx(
        {"research_material": 1.0, "research_time": 0.9}
    )


def test_a_blueprint_without_research_time_publishes_an_unknown_duration() -> None:
    durations = _owned_bpo_manufacturing_job(level_one_seconds=0)["research_duration_seconds_by_level"]
    assert durations["research_time"] is None


def test_a_row_without_an_owned_bpo_publishes_no_research_fee_table(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, profile=_FEE_PROFILE)[5002]["manufacturing_job"]
    assert "research_job_cost_by_level" not in mj


def test_a_bpc_variant_of_an_owned_bpo_publishes_the_research_fee_table_too() -> None:
    """The planner schedules ME/TE research whenever the corp owns the BPO,
    also when the kept row builds from an owned BPC of it: that row carries
    the table as well (else the research fee would read unknown)."""
    original_asset = SimpleNamespace(
        type_id=9001, item_id=42, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
    )
    copy_asset = SimpleNamespace(
        type_id=9001, item_id=43, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    service = _build_service(
        blueprint_rows=[_blueprint_row()], profile=_FEE_PROFILE,
        character_modifiers={"modifier_skills": [], "implants": []}, trained_skill_levels={},
        owned_assets=([copy_asset], [original_asset], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    (row,) = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=True, maximize_bp_runs=False, character_id=1,
    )
    mj = row["manufacturing_job"]
    assert mj["blueprint_source_kind"] != "owned_blueprint_original"
    me = mj["research_job_cost_by_level"]["research_material"]
    assert me[5] == pytest.approx(50 * 0.02 * (3360 / 105) * (0.02 + 0.08))


def test_an_owned_t2_bpc_covering_batch_one_still_publishes_the_invention_fees(monkeypatch) -> None:
    """Batch 1 invents nothing, but the planner's invent / copy actions for
    the later batches still need the fee rates (D4)."""
    from eve_online_industry_tracker.application.industry import overview_row as orow

    row = _invention_overview(
        monkeypatch, profile=_FEE_PROFILE, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
    )[5002]
    plan = row["manufacturing_job"]["invention_bpc_plan"]
    assert plan["job_cost_per_attempt"] == pytest.approx(0.12)
    assert plan["source_copy_job_cost_per_run"] == pytest.approx(0.039)
    assert orow.get_invention_fee_per_attempt(row) == pytest.approx(0.12)
    assert orow.get_source_copy_fee_per_run(row) == pytest.approx(0.039)


# --- round 5 Task 8 fix round 1: a component's per-run job fee on its parent ---


def _parent_of_component_row() -> dict:
    """Blueprint 9100 builds 6001 from 2 x the component 5001 per run (fake)."""
    parent = _blueprint_row()
    parent["blueprint_type_id"] = 9100
    parent["blueprint"] = {"type_id": 9100, "type_name": "Parent Blueprint", "base_price": 1000.0}
    parent["manufacturing_job"] = {
        **parent["manufacturing_job"],
        "materials": [{"type_id": 5001, "type_name": "Test Module", "quantity": 2, "base_price": 100.0,
                       "group_name": "Shield Boosters", "category_name": "Module"}],
        "products": [{"type_id": 6001, "type_name": "Parent Thing", "quantity": 1, "base_price": 500.0,
                      "group_name": "Shield Boosters", "category_name": "Module", "meta_group_name": "Tech I"}],
    }
    return parent


def test_a_parent_publishes_its_components_job_fee_per_run() -> None:
    """The component 5001: per-run EIV 10 Tritanium x 5 = 50, manufacturing
    index 0.05, + the 10 % surcharge on the job value = 7.5 per run, the same figure as the
    component's own row (one formula, _plan_activity_costs)."""
    service = _build_service(
        blueprint_rows=[_blueprint_row(), _parent_of_component_row()], profile=_FEE_PROFILE,
        character_modifiers={"modifier_skills": [], "implants": []}, trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}, 6001: {"adjusted_price": 500.0}},
    )
    rows = {
        int(r["type_id"]): r for r in service.industry_manufacturing_product_overview(
            build_from_bpc=False, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
        )
    }
    entry = rows[6001]["manufacturing_job"]["materials"]["5001"]
    assert entry["component_job_fee_per_run"] == pytest.approx(50 * (0.05 + 0.10))
    own = rows[5001]["manufacturing_job"]
    assert entry["component_job_fee_per_run"] == pytest.approx(own["manufacturing_job_cost"] / own["runs"])
    # A raw material has no blueprint: no fee key.
    assert "component_job_fee_per_run" not in rows[5001]["manufacturing_job"]["materials"]["34"]


# --- Task 8b fix round 1: an unknown fee input is unknown with its reason ---


def test_a_profile_without_a_facility_tax_leaves_every_fee_unknown_with_the_reason(monkeypatch, caplog) -> None:
    import logging

    profile = {k: v for k, v in _FEE_PROFILE.items() if k not in ("facility_tax", "installation_cost_modifier")}
    with caplog.at_level(logging.WARNING):
        mj = _invention_overview(monkeypatch, profile=profile)[5002]["manufacturing_job"]
    assert mj["manufacturing_job_cost"] is None
    assert "facility tax" in mj["manufacturing_job_cost_unknown_reason"]
    invention = mj["activity_breakdown"]["invention"]
    assert invention["job_cost_per_attempt"] is None
    assert "facility tax" in invention["job_cost_per_attempt_unknown_reason"]
    assert invention["source_copy_job_cost_per_run"] is None
    assert "facility tax" in invention["source_copy_job_cost_per_run_unknown_reason"]
    assert any("facility tax" in r.getMessage() for r in caplog.records if r.levelno == logging.WARNING)


def test_a_missing_research_index_is_the_published_reason() -> None:
    profile = {**_FEE_PROFILE, "system_cost_indices": [
        e for e in _FEE_PROFILE["system_cost_indices"] if e["activity"] != "researching_material_efficiency"
    ]}
    original_asset = SimpleNamespace(
        type_id=9001, item_id=42, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
    )
    blueprint_row = _blueprint_row()
    blueprint_row["research_material_job"] = {"time_seconds": 105}
    blueprint_row["research_time_job"] = {"time_seconds": 105}
    service = _build_service(
        blueprint_rows=[blueprint_row], profile=profile,
        character_modifiers={"modifier_skills": [], "implants": []}, trained_skill_levels={},
        owned_assets=([], [original_asset], {}, {}, {}),
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    (row,) = service.industry_manufacturing_product_overview(
        build_from_bpc=False, have_blueprint_source_only=True, maximize_bp_runs=False, character_id=1,
    )
    mj = row["manufacturing_job"]
    assert mj["research_job_cost_by_level"]["research_material"] is None
    assert "research_material cost index" in mj["research_job_cost_by_level_unknown_reason"]["research_material"]
    assert mj["research_job_cost_by_level"]["research_time"] is not None
    assert "research_time" not in mj["research_job_cost_by_level_unknown_reason"]


def test_a_partly_priced_component_has_no_fee_and_says_why() -> None:
    """A material without an adjusted price used to give a partial EIV, so a
    low fee that looked exact."""
    blueprint_row = _blueprint_row()
    blueprint_row["manufacturing_job"]["materials"].append(
        {"type_id": 35, "type_name": "Pyerite", "quantity": 4, "group_name": "Mineral", "category_name": "Material"},
    )
    service = _build_service(
        blueprint_rows=[blueprint_row], profile=_FEE_PROFILE,
        character_modifiers={"modifier_skills": [], "implants": []}, trained_skill_levels={},
        owned_assets=([], [], {}, {}, {}),
        # No adjusted, average or SDE base price for 35.
        adjusted_price_map={34: {"adjusted_price": 5.0}, 5001: {"adjusted_price": 100.0}},
    )
    (row,) = service.industry_manufacturing_product_overview(
        build_from_bpc=False, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    mj = row["manufacturing_job"]
    assert mj["manufacturing_job_cost"] is None
    assert "EIV" in mj["manufacturing_job_cost_unknown_reason"]
