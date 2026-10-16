"""Task 15: producer fixes from the round-5 whole-branch review.

Probes F (unknown job fees), G (the best inventor must be able to run the
invention) and H (a once-only WARNING first met while warnings are quiet),
the aggregate-only rig profile (M1), the deduplicated helpers (M2, M3, M5)
and the composed T2 cases A, D and E pinned exactly. Fake characters, fake
prices and fake slot data throughout; the skill and blueprint ids are the
harness's public SDE ids.
"""
from __future__ import annotations

import logging
import math
import time
from types import MethodType, SimpleNamespace

import pytest

import test_industry_service_calculations as h
from eve_online_industry_tracker.application.daily_planner.assigned_invention import assigned_invention
from eve_online_industry_tracker.application.industry import invention_model as im
from eve_online_industry_tracker.application.industry.invention_model import (
    NO_DECRYPTOR,
    CharacterSlotCapacity,
    DecryptorOption,
    InventionContext,
    quiet_warnings,
)
from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

PLUS_RUN = DecryptorOption(type_id=34203, type_name="PlusRun", probability_multiplier=0.6,
                           me_modifier=-2, te_modifier=2, max_run_modifier=9)

# A lowsec profile with every activity's index and one per-effect Modules rig.
PROFILE = {
    "facility_tax": 0.01, "material_efficiency_bonus": 0.01, "time_efficiency_bonus": 0.0,
    "facility_cost_bonus": 0.0, "system_security_status": 0.3, "system_region_id": 10000002,
    "system_cost_indices": [
        {"activity": "manufacturing", "cost_index": 0.05},
        {"activity": "invention", "cost_index": 0.06},
        {"activity": "copying", "cost_index": 0.04},
    ],
    "structure_rigs": [{
        "type_id": 43920, "security_multipliers": {"high": 1.0, "low": 1.9, "null": 2.1, "wormhole": 2.1},
        "effects": [{"activity": "manufacturing", "metric": "material", "group": "Modules",
                     "value": 0.02, "security_scaled": True}],
    }],
}
LV_STRONG = {11433: 5, 11454: 5, 21790: 4}
LV_WEAK = {11433: 1, 11454: 1, 21790: 1}


@pytest.fixture(autouse=True)
def _fresh_once_warnings():
    IndustryService._reset_once_warnings()
    yield
    IndustryService._reset_once_warnings()


def _bpc(runs: int, me: int = 5, te: int = 10) -> SimpleNamespace:
    return SimpleNamespace(
        type_id=9002, item_id=81, is_blueprint_copy=True,
        blueprint_material_efficiency=me, blueprint_time_efficiency=te, blueprint_runs=runs,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1, character_id=None,
    )


def _service(monkeypatch, *, options=(), profile=None, rows=None, prices=None, levels=None, slots=None,
             owned=None, real_confidence=False):
    rows = rows or h._rows_with(probability_pct=30.0, bpc_runs=10, t2_limit=10)
    prices = prices or {**h._INVENTION_FAKE_PRICES, 34203: 300.0, 5002: 2000.0}
    service = h._decryptor_service(monkeypatch, list(options), prices=prices, blueprint_rows=rows,
                                   profile=profile, real_confidence=real_confidence)
    if owned is not None:
        service._get_owned_blueprint_assets = MethodType(
            lambda self, *, owned_blueprints_scope: ([owned], [], {}, {}, {}), service)
    if levels is not None:
        service._get_inventor_skill_levels = MethodType(lambda self: levels, service)
    if slots is not None:
        service._get_corp_slot_capacity = MethodType(lambda self: slots, service)
    return service


def _overview(service, *, maximize=True):
    return service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=maximize, character_id=1)


def _t2(rows):
    return [r for r in rows if r["type_id"] == 5002][0]


def _shopping(row, batches):
    out = {}
    for b in batches:
        row["max_batches_total"] = b
        out[b] = {line["type_id"]: (line["need"], line["buy"]) for line in aggregate_shopping_list([row])}
    return out


def _lanes():
    return ({1: LV_STRONG, 2: LV_WEAK},
            {1: CharacterSlotCapacity(3, {"Science": 5}, None), 2: CharacterSlotCapacity(5, {"Science": 1}, None)})


# --- I1: unknown job fees (probe F) -------------------------------------------------

_F_PROFILE = {**PROFILE, "system_cost_indices": [{"activity": "manufacturing", "cost_index": 0.05}]}
_F_INVENTION_REASON = "invention (the industry profile has no invention cost index)"
_F_COPY_REASON = "invention source copying (the industry profile has no copying cost index)"


def _probe_f_row(monkeypatch, *, real_confidence=True):
    rows = h._rows_with(probability_pct=30.0, bpc_runs=10, t2_limit=10)
    rows[1]["manufacturing_job"]["products"][0]["category_name"] = "Module"
    prices = {**h._INVENTION_FAKE_PRICES, 5002: 2000.0}
    service = h._invention_service(monkeypatch, prices=prices, blueprint_rows=rows, profile=_F_PROFILE,
                                   real_confidence=real_confidence)
    return service, _t2(_overview(service))


def test_f_unknown_invention_and_copy_fees_are_named_on_the_row(monkeypatch):
    _service_, row = _probe_f_row(monkeypatch)
    mj = row["manufacturing_job"]
    assert mj["activity_breakdown"]["manufacturing"]["total_job_cost"] == pytest.approx(50.0)
    assert mj["job_fee_unknown_reasons"] == [_F_INVENTION_REASON, _F_COPY_REASON]
    # The known part stays visible, but flagged: never presented as exact.
    assert mj["total_job_cost"] == pytest.approx(50.0)
    assert mj["total_cost"] == pytest.approx(1186.6666666666667)
    assert mj["total_cost_excludes_unknown_job_fees"] is True
    reasons = row["pricing_confidence_reasons"]
    assert any(r.startswith(f"Job fees unknown for: {_F_INVENTION_REASON}; {_F_COPY_REASON}") for r in reasons)
    assert row["overall_pricing_confidence"] != "High"


def test_f_unknown_fees_cap_the_shown_confidence_below_high(monkeypatch):
    service, row = _probe_f_row(monkeypatch)
    # A market good enough for High on its own.
    row.update({
        "market_unit_price": 2000.0, "market_price_sample_size": 10, "market_price_fetched_at": time.time(),
        "hub_sell_liquidity": 1000, "hub_sell_order_count": 50, "region_daily_volume": 100,
        "region_daily_volume_7d_avg": 100.0, "liquidity_indicator": "High",
    })
    service._enrich_product_rows_with_pricing_confidence([row], product_price_side="sell")
    assert row["pricing_confidence"] == "Medium"
    del row["manufacturing_job"]["job_fee_unknown_reasons"]
    service._enrich_product_rows_with_pricing_confidence([row], product_price_side="sell")
    assert row["pricing_confidence"] == "High"


def test_f_the_row_cost_estimate_is_unknown_when_a_fee_is(monkeypatch):
    service, row = _probe_f_row(monkeypatch)
    ctx = h._planning_context(service, maximize_bp_runs=True)
    total, why = service._row_total_cost_estimate(ctx, row)
    assert total is None
    assert why == f"job fees unknown for: {_F_INVENTION_REASON}; {_F_COPY_REASON}"


def test_no_profile_flags_every_fee_and_the_materials_only_total(monkeypatch):
    service = _service(monkeypatch, profile=None)
    mj = _t2(_overview(service))["manufacturing_job"]
    assert mj["total_job_cost"] is None
    assert mj["total_cost"] == pytest.approx(mj["material_cost"])
    assert mj["total_cost_excludes_unknown_job_fees"] is True
    assert mj["job_fee_unknown_reasons"][0] == "manufacturing (the industry profile has no manufacturing cost index)"


def test_a_fully_priced_row_has_no_fee_flag(monkeypatch):
    rows = h._rows_with(probability_pct=30.0, bpc_runs=10, t2_limit=10)
    rows[1]["manufacturing_job"]["products"][0]["category_name"] = "Module"
    service = _service(monkeypatch, profile=PROFILE, rows=rows)
    row = _t2(_overview(service))
    mj = row["manufacturing_job"]
    assert "job_fee_unknown_reasons" not in mj
    assert mj["total_cost_excludes_unknown_job_fees"] is False
    ctx = h._planning_context(service, maximize_bp_runs=True)
    total, why = service._row_total_cost_estimate(ctx, row)
    assert why is None and total == pytest.approx(mj["total_cost"])


# A sub-build with unknown fees is bought, not built (fix round 1, minor 1):
# test_producer_review_fixes_round1.test_a_sub_build_with_unknown_fees_is_bought_with_the_reason.


def test_the_decryptor_comparison_names_the_excluded_fees(monkeypatch):
    # Probe A: no profile, so every fee is unknown in every candidate alike.
    levels, slots = _lanes()
    service = _service(monkeypatch, options=[PLUS_RUN], levels=levels, slots=slots)
    choice = _t2(_overview(service))["manufacturing_job"]["invention_decryptor"]
    assert choice["type_name"] == "PlusRun"
    assert "job fees unknown" in choice["reason"]
    assert "left out of every candidate's cost" in choice["reason"]


def test_fee_basis_is_known_parts_when_every_candidate_lacks_the_same_fees():
    rows = [
        {"manufacturing_job": {"job_fee_unknown_reasons": ["invention (x)"]}},
        {"manufacturing_job": {"job_fee_unknown_reasons": ["invention (x)"]}},
        None,
    ]
    include_known, note = IndustryService._decryptor_fee_basis(rows)
    assert include_known is True
    assert note == "job fees unknown for: invention (x) — left out of every candidate's cost"


def test_fee_basis_drops_every_fee_when_the_candidates_differ():
    rows = [
        {"manufacturing_job": {"job_fee_unknown_reasons": ["invention (x)"]}},
        {"manufacturing_job": {}},
    ]
    include_known, note = IndustryService._decryptor_fee_basis(rows)
    assert include_known is False
    assert note.startswith("job fees unknown for some candidates")


def test_fee_basis_is_exact_when_every_fee_is_known():
    assert IndustryService._decryptor_fee_basis([{"manufacturing_job": {}}]) == (True, None)


# --- I3: the best inventor must be able to run the invention (probe G) --------------

def test_g_a_pilot_without_the_encryption_skill_is_never_the_inventor(monkeypatch):
    levels = {1: {11433: 5, 11454: 5}, 2: {11433: 3, 11454: 3, 21790: 3}}
    slots = {1: CharacterSlotCapacity(1, {}, None), 2: CharacterSlotCapacity(1, {}, None)}
    service = _service(monkeypatch, levels=levels, slots=slots)
    inv = _t2(_overview(service))["manufacturing_job"]["activity_breakdown"]["invention"]
    chance = inv["invention_chance"]
    p = 0.30 * (1 + 6 / 30 + 3 / 40)  # 0.3825, pilot 2's
    assert chance["inventor_character_id"] == 2
    assert chance["probability"] == pytest.approx(p)
    assert chance["skills_unknown_reason"] is None
    assert inv["success_probability"] == pytest.approx(p)
    assert inv["elapsed_parallel"]["lanes"] == 1
    assert inv["whole_attempts"] == inv["best_inventor_whole_attempts"] == math.ceil(1 / p)


def test_g_nobody_qualifies_uses_the_base_chance_with_a_reason():
    skill_entries = [dict(s) for s in h._HARNESS_INVENTION_SKILLS]
    context = InventionContext(
        inventor_skill_levels_by_character_id={1: {11433: 5, 11454: 5}},
        encryption_skill_type_ids=frozenset({21790}),
    )
    chance = im.resolve_invention_chance(
        context, blueprint_type_id=9002, skill_entries=skill_entries, base_probability=0.3, decryptor=NO_DECRYPTOR,
    )
    assert chance.skill_factor is None and chance.inventor_character_id is None
    assert chance.probability == pytest.approx(0.3)
    assert chance.skills_unknown_reason == im.NO_QUALIFYING_INVENTOR_REASON
    # The lanes keep their own "nobody qualifies" reason (the row's build-time note).
    lanes = im.invention_lanes(
        InventionContext(
            inventor_skill_levels_by_character_id={1: {11433: 5, 11454: 5}},
            encryption_skill_type_ids=frozenset({21790}),
            slot_capacity_by_character_id={1: CharacterSlotCapacity(1, {}, None)},
        ), chance, skill_entries, probability_floor=0.01,
    )
    assert lanes.sequential_reason == im.NO_QUALIFYING_INVENTOR_REASON


def test_g_planner_case_a_sizes_at_the_qualifying_pilots_chance(monkeypatch):
    """An owned 4-run BPC covers batch 1, so the planner sizes from the
    later-batch plan at the row's best-inventor chance: pilot 2's 0.3825,
    not the unqualified pilot 1's 0.40. Pilot 2 keeps the producer's figures."""
    levels = {1: {11433: 5, 11454: 5}, 2: {11433: 3, 11454: 3, 21790: 3}}
    slots = {1: CharacterSlotCapacity(1, {}, None), 2: CharacterSlotCapacity(1, {}, None)}
    service = _service(monkeypatch, levels=levels, slots=slots, owned=_bpc(4))
    row = _t2(_overview(service))
    mj = row["manufacturing_job"]
    assert "invention" not in mj["activity_breakdown"]
    plan = mj["invention_bpc_plan"]
    p = 0.30 * (1 + 6 / 30 + 3 / 40)
    action = assigned_invention(row, character_id=2, skill_levels_by_id=levels[2], probability_floor=0.01,
                                price_map={})
    assert action.best_probability == pytest.approx(p)
    assert action.probability == pytest.approx(p)
    assert "(best inventor)" in action.note
    batch_runs = plan["batch_runs"]
    assert action.expected_attempts == pytest.approx(batch_runs / 10 / p)
    assert action.whole_attempts == math.ceil(math.ceil(batch_runs / 10) / p)


# --- I2: once-only WARNINGs survive quiet_warnings (probe H) ------------------------

def test_h_a_rig_security_warning_first_met_while_quiet_is_still_logged(monkeypatch, caplog):
    rows = h._rows_with(probability_pct=30.0, bpc_runs=10, t2_limit=10)
    rows[1]["manufacturing_job"]["products"][0]["category_name"] = "Module"
    profile = {k: v for k, v in PROFILE.items() if k not in ("system_security_status", "system_region_id")}
    profile["system_id"] = 30000142
    service = _service(monkeypatch, options=[PLUS_RUN], profile=profile, rows=rows)
    ctx = h._planning_context(service, maximize_bp_runs=True)
    with caplog.at_level(logging.WARNING):
        service._decryptor_choice_for(ctx, 9002)  # quiet candidate builds first
        t2 = rows[1]
        service._build_product_row(ctx, row=t2, raw_product=t2["manufacturing_job"]["products"][0], row_index=1,
                                   product_index=0, blueprint_copy_assets=[], blueprint_original_asset=None,
                                   effective_runs=10)
    msgs = [r.getMessage() for r in caplog.records if "security multiplier" in r.getMessage()]
    assert len(msgs) == 1


@pytest.mark.parametrize("warn", [
    lambda: IndustryService._warn_rig_security_once("fake reason"),
    lambda: IndustryService._manufacturing_rig_groups({"type_id": 999001, "type_name": "Fake"}),
    lambda: IndustryService._profile_rig_reduction(
        profile_payload={"structure_rig_material_bonus": 0.05}, activity="manufacturing", metric="material",
        manufacturing_group=frozenset()),
])
def test_once_only_warnings_register_nothing_while_quiet(warn, caplog):
    with caplog.at_level(logging.WARNING):
        with quiet_warnings():
            warn()
        assert caplog.records == []
        warn()
        assert len(caplog.records) == 1
        warn()
        assert len(caplog.records) == 1


def test_once_only_warnings_are_scoped_per_refresh(monkeypatch, caplog):
    service = _service(monkeypatch)
    IndustryService._warn_rig_security_once("fake reason")
    _overview(service)
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        IndustryService._warn_rig_security_once("fake reason")
    assert [r.getMessage() for r in caplog.records] == ["Rig bonus security multiplier: fake reason"]


# --- M1: an aggregate-only rig profile applies no rig bonus -------------------------

_AGGREGATE_ONLY = {**PROFILE, "structure_rigs": [], "structure_rig_material_bonus": 0.05,
                   "structure_rig_time_bonus": 0.2}


@pytest.mark.parametrize("metric", ["material", "time"])
def test_m1_aggregate_only_profile_gives_no_rig_bonus_and_warns(metric, caplog):
    with caplog.at_level(logging.WARNING):
        reduction = IndustryService._profile_rig_reduction(
            profile_payload=_AGGREGATE_ONLY, activity="manufacturing", metric=metric,
            manufacturing_group=frozenset({"Ships"}))
    assert reduction == 0.0
    assert any("no rig ids" in r.getMessage() for r in caplog.records)


def test_m1_uncovered_product_under_aggregate_only_profile_gets_no_rig_and_a_reason(monkeypatch):
    rows = h._rows_with(probability_pct=30.0, bpc_runs=10, t2_limit=10)
    service = _service(monkeypatch, profile=_AGGREGATE_ONLY, rows=rows, real_confidence=True)
    row = [r for r in _overview(service) if r["type_id"] == 6001][0]
    mj = row["manufacturing_job"]
    # Structure 1% only: no rig on top (3 Tritanium per run, 1 run).
    assert mj["material_reduction"] == pytest.approx(0.01)
    assert mj["rig_bonus_unknown_reason"] == IndustryService._aggregate_only_rig_reason(_AGGREGATE_ONLY)
    assert mj["rig_bonus_unknown_metrics"] == ["material", "time"]
    assert any(r.startswith("Rig bonus unknown: profile has no rig ids") for r in row["pricing_confidence_reasons"])


def test_m1_producer_and_planner_agree_on_an_aggregate_only_profile():
    from eve_online_industry_tracker.application.daily_planner.facility_bonus import sub_job_facility_reductions

    planner = sub_job_facility_reductions(_AGGREGATE_ONLY, {"category_name": "Module", "group_name": "Fake"})
    producer = IndustryService._profile_rig_reduction(
        profile_payload=_AGGREGATE_ONLY, activity="manufacturing", metric="material",
        manufacturing_group=frozenset({"Modules"}))
    assert planner["rig_material_reduction"] == producer == 0.0


def test_m1_no_reason_without_an_aggregate():
    assert IndustryService._aggregate_only_rig_reason(PROFILE) is None
    assert IndustryService._aggregate_only_rig_reason({"structure_rigs": []}) is None
    assert IndustryService._aggregate_only_rig_reason(None) is None
    assert IndustryService._aggregate_only_rig_reason(_AGGREGATE_ONLY).startswith(
        "rig bonus unknown: profile has no rig ids, only the aggregate structure rig material, time bonus")


# --- M2 / M3 / M4 / M5: one copy of each formula ------------------------------------

def test_m2_one_research_level_table():
    from eve_online_industry_tracker.application.industry.job_cost import RESEARCH_RANK1_LEVEL_SECONDS

    assert not hasattr(IndustryService, "_RESEARCH_RANK1_TARGET_DURATION_SECONDS")
    for level in range(1, 11):
        assert IndustryService._research_target_duration_seconds(
            level_one_duration_seconds=210, target_level=level,
        ) == IndustryService._round_duration_seconds(2 * RESEARCH_RANK1_LEVEL_SECONDS[level])


def test_m3_copy_process_value_helper():
    service = object.__new__(IndustryService)
    service._adm = MethodType(lambda self, category, key, fallback=None: fallback, service)
    assert service._copy_process_value(50.0, 3) == pytest.approx(50.0 * 3 * 0.02)
    assert service._copy_process_value(None, 3) is None
    assert service._copy_process_value(50.0, -1) == 0.0


def test_m4_the_row_publishes_no_lane_whole_attempts(monkeypatch):
    levels, slots = _lanes()
    service = _service(monkeypatch, options=[PLUS_RUN], levels=levels, slots=slots)
    inv = _t2(_overview(service))["manufacturing_job"]["activity_breakdown"]["invention"]
    assert "lane_whole_attempts" not in inv
    assert inv["whole_attempts"] == inv["elapsed_parallel"]["lane_whole_jobs"] == 5


_TAX_UNKNOWN = "sales tax unknown (no character to read Accounting from)"
_BROKER_UNKNOWN = "broker fee unknown (no character to read Broker Relations and standings from)"


@pytest.mark.parametrize("context, expected", [
    ({"rates": {"sales_tax_fraction": 0.05, "broker_fee_fraction": 0.03}, "broker_fee_applies": True},
     (30.0, 50.0, 920.0, None)),
    ({"rates": {"sales_tax_fraction": 0.05, "broker_fee_fraction": 0.03}, "broker_fee_applies": False},
     (0.0, 50.0, 950.0, None)),
    ({"rates": {"sales_tax_fraction": None, "broker_fee_fraction": None}, "broker_fee_applies": True},
     (None, None, None, _TAX_UNKNOWN)),
    ({"rates": {"sales_tax_fraction": 0.05, "broker_fee_fraction": None}, "broker_fee_applies": True},
     (None, 50.0, None, _BROKER_UNKNOWN)),
    ({"rates": {"sales_tax_fraction": 0.05, "broker_fee_fraction": None}, "broker_fee_applies": False},
     (None, 50.0, 950.0, None)),
])
def test_m5_one_sale_proceeds_formula(context, expected):
    assert IndustryService._sale_fee_amounts(1000.0, context) == expected
    broker, tax, net, why = expected
    fraction = IndustryService._net_sale_fraction(context)
    assert (fraction is None) == (net is None)
    if net is not None:
        assert fraction == pytest.approx(net / 1000.0)


def test_m5_unknown_broker_fee_leaves_net_proceeds_unknown_with_a_reason(monkeypatch):
    context = {"rates": {"sales_tax_fraction": 0.05, "broker_fee_fraction": None}, "broker_fee_applies": True}
    service = _service(monkeypatch)
    rows = [{"type_id": 5002, "quantity": 2, "market_unit_price": 100.0, "manufacturing_job": {"runs": 2}}]
    IndustryService._enrich_product_rows_with_sale_proceeds(
        service, rows, character_id=1, market_hub="jita", product_price_side="sell", fee_context=context)
    assert rows[0]["net_proceeds"] is None
    assert rows[0]["manufacturing_job"]["net_proceeds_unknown_reason"] == _BROKER_UNKNOWN


# --- Composed T2 cases A, D and E stay exact ----------------------------------------

def test_case_a_lanes_and_plus_run_decryptor_exact(monkeypatch):
    levels, slots = _lanes()
    service = _service(monkeypatch, options=[PLUS_RUN], levels=levels, slots=slots)
    row = _t2(_overview(service))
    mj = row["manufacturing_job"]
    inv = mj["activity_breakdown"]["invention"]
    assert mj["runs"] == 10 and mj["invention_decryptor"]["type_name"] == "PlusRun"
    assert inv["success_probability"] == pytest.approx(0.258)
    assert inv["runs"] == pytest.approx(2.2549948135119284)
    assert (inv["whole_attempts"], inv["best_inventor_whole_attempts"]) == (5, 4)
    assert (inv["elapsed_duration_seconds"], inv["duration_seconds"]) == (40, 91)
    assert (mj["time_seconds"], mj["elapsed_time_seconds"]) == (1077, 1080)
    assert _shopping(row, (1, 2, 3, 5, 10)) == {
        1: {34203: (5, 5), 204: (10, 10), 34: (100, 100)},
        2: {34203: (10, 10), 204: (20, 20), 34: (200, 200)},
        3: {34203: (10, 10), 204: (20, 20), 34: (300, 300)},
        5: {34203: (15, 15), 204: (30, 30), 34: (500, 500)},
        10: {34203: (30, 30), 204: (60, 60), 34: (1000, 1000)},
    }


def test_case_d_owned_bpc_lanes_decryptor_lowsec_rig_exact(monkeypatch):
    levels, slots = _lanes()
    rows = h._rows_with(probability_pct=30.0, bpc_runs=10, t2_limit=10)
    rows[1]["manufacturing_job"]["products"][0]["category_name"] = "Module"
    bpc = _bpc(4)
    service = _service(monkeypatch, options=[PLUS_RUN], profile=PROFILE, rows=rows, levels=levels, slots=slots,
                       owned=bpc)
    ctx = h._planning_context(service)
    t2 = rows[1]
    row = service._build_single_product_row(
        ctx, row=t2, raw_product=t2["manufacturing_job"]["products"][0], row_index=1, product_index=1,
        blueprint_copy_assets=[bpc], blueprint_original_asset=None, effective_runs=10)
    mj = row["manufacturing_job"]
    assert [(s["runs"], s["blueprint_material_efficiency"], s["source"], s["materials"]) for s in mj["job_segments"]] == [
        (4, 5, "owned_blueprint_copy", {"34": 37}), (6, 0, "invented_blueprint_copy", {"34": 58}),
    ]
    assert mj["total_job_cost"] == pytest.approx(50.18536057367068)
    assert (mj["time_seconds"], mj["elapsed_time_seconds"]) == (1007, 1064)
    assert "job_fee_unknown_reasons" not in mj
    assert {k: v["quantity"] for k, v in mj["invention_procurement_materials"].items()} == {"204": 10, "34203": 5}
    assert {k: v["quantity"] for k, v in mj["procurement_materials"].items()} == {"34": 95}
    assert _shopping(row, (1, 2, 3, 4)) == {
        1: {34203: (5, 5), 204: (10, 10), 34: (95, 95)},
        2: {34203: (5, 5), 204: (10, 10), 34: (191, 191)},
        3: {34203: (10, 10), 204: (20, 20), 34: (287, 287)},
        4: {34203: (10, 10), 204: (20, 20), 34: (383, 383)},
    }


def test_case_e_nested_t2_under_t1_parent_exact(monkeypatch):
    levels, slots = _lanes()
    rows = h._nested_invention_blueprint_rows(probability_pct=30.0)
    rows[1]["manufacturing_job"]["products"][0]["category_name"] = "Module"
    rows[2]["manufacturing_job"]["products"][0]["category_name"] = "Module"
    rows[2]["research_material_job"] = {"time_seconds": 210}
    rows[2]["research_time_job"] = {"time_seconds": 210}
    profile = {**PROFILE, "system_cost_indices": [*PROFILE["system_cost_indices"],
                                                  {"activity": "research_material", "cost_index": 0.03},
                                                  {"activity": "research_time", "cost_index": 0.03}]}
    bpo = SimpleNamespace(
        type_id=9100, item_id=91, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=-1,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1, character_id=None,
    )
    service = _service(monkeypatch, profile=profile, rows=rows, levels=levels, slots=slots, owned=bpo,
                       prices={34: 5.0, 204: 100.0, 5002: 10_000.0, 7100: 50_000.0})
    (parent,) = [r for r in _overview(service) if r["type_id"] == 7100]
    mj = parent["manufacturing_job"]
    nested = mj["recursive_activity_breakdown"]["manufacturing:5002"]
    inv = nested["nested"]["invention:9002"]
    assert nested["recommended_action"] == "build"
    assert (inv["whole_attempts"], inv["elapsed_time_seconds"], inv["time_seconds"]) == (3, 40, 94)
    assert inv["attempts"] == pytest.approx(2.3255813953488373)
    assert inv["success_probability"] == pytest.approx(0.43)
    assert inv["job_cost"] == pytest.approx(0.2558139534883721)
    assert (mj["time_seconds"], mj["elapsed_time_seconds"]) == (3101, 3060)
    assert mj["total_job_cost"] == pytest.approx(10050.318604651162)
    assert "job_fee_unknown_reasons" not in mj
    assert _shopping(parent, (1, 2, 3, 10)) == {
        1: {204: (6, 6), 34: (94, 94)}, 2: {204: (12, 12), 34: (188, 188)},
        3: {204: (18, 18), 34: (282, 282)}, 10: {204: (60, 60), 34: (940, 940)},
    }


# --- I1 in the UI: a lower-bound total is never shown as exact ---------------------

def test_ui_marks_a_row_with_unknown_job_fees():
    from streamlit_ui.state import industry_builder_ui as ui

    flagged = {"total_cost": 1186.6666666666667, "total_cost_excludes_unknown_job_fees": True,
               "job_fee_unknown_reasons": [_F_INVENTION_REASON]}
    assert ui.pricing_confidence_cell("Medium", flagged) == f"Medium · {ui.JOB_FEES_UNKNOWN_MARKER}"
    assert ui.pricing_confidence_cell("Medium", {**flagged, "assumes_owned_t2_bpo": True}) == (
        f"Medium · {ui.ASSUMES_T2_BPO_MARKER} · {ui.JOB_FEES_UNKNOWN_MARKER}")
    assert ui.total_cost_label(flagged) == "≥ 1,186.67 (job fees unknown)"
    assert ui.total_cost_label({"total_cost": 10.0, "total_cost_excludes_unknown_job_fees": False}) == "10.00"
    assert ui.total_cost_label({"total_cost": None}) == "N/A"
