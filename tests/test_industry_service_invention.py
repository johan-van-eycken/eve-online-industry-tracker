"""Invention in the producer: inputs and fees, nested T2, whole attempts, decryptors, invented levels."""

from __future__ import annotations

from types import MethodType, SimpleNamespace
from typing import Any, cast
import pytest
from eve_online_industry_tracker.application.industry.invention_model import NO_DECRYPTOR, DecryptorOption
from eve_online_industry_tracker.application.industry.service import IndustryService
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService
from eve_online_industry_tracker.application.industry.shopping_list import LATER_BATCH_PLAN_VERSION
from industry_service_fakes import (
    _ACCELERANT_LIKE,
    _DECRYPTOR_PRICES,
    _FEES_KNOWN_PROFILE,
    _INVENTION_FAKE_PRICES,
    _INVENTION_ONLY_PROFILE,
    _NESTED_FAKE_PRICES,
    _NESTED_FEES_PROFILE,
    _decryptor_overview,
    _decryptor_service,
    _find_first_activity_node,
    _invented_parent_rows,
    _invented_t2_blueprint_rows,
    _invention_overview,
    _invention_service,
    _later_plan,
    _nested_invention_blueprint_rows,
    _owned_t2_bpc,
    _owned_t2_bpc_me,
    _planning_context,
    _rows_with,
    _shopping_line,
)


def test_invented_t2_row_cost_includes_expected_invention_inputs(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch)[5002]["manufacturing_job"]

    # 1 run: Tritanium 10 x 5 = 50. Invention per run =
    # (2 datacores x 100) / (0.5 probability x 10 runs per BPC) = 40.
    assert mj["runs"] == 1
    assert mj["material_cost"] == pytest.approx(90.0)
    assert mj["invention_material_cost"] == pytest.approx(40.0)
    assert mj["total_cost"] == pytest.approx((mj["total_job_cost"] or 0.0) + 90.0)
    assert mj["expected_invention_materials"]["204"]["quantity"] == pytest.approx(0.4)
    assert mj["expected_invention_materials"]["204"]["quantity_per_attempt"] == 2
    assert mj["expected_invention_materials"]["204"]["unit_price"] == 100.0


def test_invented_t2_invention_cost_scales_with_runs(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[5002]["manufacturing_job"]

    # 10 runs at ME 2: Tritanium 98 x 5 = 490; invention 40 per run x 10 = 400.
    assert mj["runs"] == 10
    assert mj["invention_material_cost"] == pytest.approx(400.0)
    assert mj["material_cost"] == pytest.approx(890.0)


def test_invention_inputs_use_owned_cost_like_other_materials(monkeypatch) -> None:
    # Owned datacores at 60 each are taken before buying, as for every material.
    mj = _invention_overview(
        monkeypatch, owned_item_inventory=({204: 100}, {204: 60.0}),
    )[5002]["manufacturing_job"]

    assert mj["expected_invention_materials"]["204"]["unit_price"] == 60.0
    assert mj["invention_material_cost"] == pytest.approx(24.0)  # 2 x 60 / (0.5 x 10)


def test_invention_inputs_are_not_in_the_batch_materials(monkeypatch) -> None:
    # The planner buys datacores from its own invent action; `materials` (what
    # the manufacture action buys) and `procurement_materials` stay free of them.
    mj = _invention_overview(monkeypatch)[5002]["manufacturing_job"]

    assert "204" not in mj["materials"]
    assert "204" not in mj["procurement_materials"]


def test_t1_row_cost_is_unchanged_by_invention_inputs(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch)[6001]["manufacturing_job"]

    assert mj["material_cost"] == 15.0  # 3 Tritanium x 5, nothing else
    assert "invention_material_cost" not in mj
    assert "expected_invention_materials" not in mj


def test_t2_row_from_owned_bpc_has_no_invention_cost(monkeypatch) -> None:
    bpc = SimpleNamespace(
        type_id=9002, item_id=77, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    mj = _invention_overview(monkeypatch, owned_assets=([bpc], [], {}, {}, {}))[5002]["manufacturing_job"]

    assert mj["blueprint_source_kind"] == "owned_blueprint_copy"
    assert mj["material_cost"] == 50.0
    assert "invention_material_cost" not in mj


def test_shopping_list_buys_datacores_once_for_an_invented_t2_row(monkeypatch) -> None:
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction
    from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder
    from eve_online_industry_tracker.application.industry import overview_row as orow

    row = _invention_overview(monkeypatch)[5002]
    assert row["manufacturing_job"]["invention_material_cost"] == pytest.approx(40.0)
    # The row carries the Portfolio buy line for the datacores; the daily
    # planner must not read it (it buys them via the invent action only).
    assert row["manufacturing_job"]["invention_procurement_materials"]["204"]["quantity"] == 4
    assert 204 not in (orow.get_batch_materials(row) or {})

    def action(action_type, materials=None, runs=None):
        return AssignedAction(
            type_id=5002, type_name="T2 Module", action_type=action_type,
            character_id=1, character_name="Pilot", quantity=None, runs=runs,
            estimated_cost_isk=None, estimated_profit_isk=None,
            estimated_completion=None, notes=None, materials=materials,
        )

    blueprint_data = {
        1001: {
            "manufacturing": {"materials": [], "products": [{"type_id": 6001, "quantity": 1}]},
            "invention": {
                "materials": [{"type_id": 204, "type_name": "Datacore", "quantity": 2}],
                "products": [{"type_id": 9002, "quantity": 10}],
            },
        },
        9002: {
            "manufacturing": {
                "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 10}],
                "products": [{"type_id": 5002, "quantity": 1}],
            },
        },
    }

    class _Resolver:
        def is_blueprint(self, type_id):
            return False

        def prefetch(self, type_ids):
            return None

    class _Admin:
        def get(self, section, key, default=None):
            return default

    items = ShoppingListBuilder().build(
        # The invent action carries the producer's whole attempts (A4: the
        # best inventor's, CharacterAssigner sets them).
        assigned_actions=[
            action("manufacture", orow.get_batch_materials(row)),
            action("invent", runs=orow.get_invention_costs(row)["whole_attempts"]),
        ],
        corp_assets=[], market_depth_cache={34: {"vwap_5d": 5.0}, 204: {"vwap_5d": 100.0}},
        admin_settings=_Admin(), blueprint_data=blueprint_data, meta_resolver=_Resolver(),
    )

    datacores = [i for i in items if i.type_id == 204]
    assert len(datacores) == 1
    # Whole attempts x 2, from the invent action only: the Portfolio line's 4.
    assert datacores[0].quantity == 4
    assert datacores[0].shopping_category == "invention_input"


# --- fix round 1: fees on the same expected-attempt model, pricing gaps -------

# Only invention has a cost index (2%), no facility tax or reductions, so the
# row's total_job_cost is exactly the invention fee (job_cost.py): job value
# per attempt = 0.02 x the invented T2 blueprint's per-run EIV (10 Tritanium x
# adjusted 5 = 50) = 1.0, times (index 0.02 + SCC 0.04), times the attempts.
_SCC = 0.04
_T2_INVENTION_FEE_PER_ATTEMPT = 0.02 * 50 * (0.02 + _SCC)  # 0.06


def test_one_run_invention_fee_is_amortized_not_a_whole_attempt(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, profile=_INVENTION_ONLY_PROFILE)[5002]["manufacturing_job"]

    # attempts = 1 run / (0.5 x 10) = 0.2 (was ceil(1/0.5) = 2 whole attempts -> 8.0).
    invention = mj["activity_breakdown"]["invention"]
    assert invention["runs"] == pytest.approx(0.2)
    assert invention["total_job_cost"] == pytest.approx(_T2_INVENTION_FEE_PER_ATTEMPT * 0.2)  # 0.012
    assert mj["total_job_cost"] == pytest.approx(0.012)
    assert mj["total_cost"] == pytest.approx(0.012 + 90.0)


def test_fees_and_inputs_use_sde_runs_per_bpc_not_the_t2_limit(monkeypatch) -> None:
    # 10-run invented BPC, 300-run T2 limit, 300 runs, 34%:
    # attempts = 300 / (0.34 x 10) = 88.235... (the T2 limit would give ceil(1/0.34) = 3).
    rows = _rows_with(probability_pct=34.0, bpc_runs=10, t2_limit=300)
    mj = _invention_overview(
        monkeypatch, profile=_INVENTION_ONLY_PROFILE, blueprint_rows=rows, maximize_bp_runs=True,
    )[5002]["manufacturing_job"]

    attempts = 300 / (0.34 * 10)
    invention = mj["activity_breakdown"]["invention"]
    assert mj["runs"] == 300
    assert invention["runs_per_invented_blueprint"] == 10
    assert invention["runs"] == pytest.approx(attempts)
    assert invention["total_job_cost"] == pytest.approx(_T2_INVENTION_FEE_PER_ATTEMPT * attempts)
    assert mj["invention_material_cost"] == pytest.approx(2 * 100 * attempts)
    # Tritanium at ME 2: 300 runs x 10 x 0.98 = 2940 units x 5.
    assert mj["material_cost"] == pytest.approx(2940 * 5 + 2 * 100 * attempts)


def test_job_tree_adds_up_to_the_row_cost(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, profile=_INVENTION_ONLY_PROFILE)[5002]["manufacturing_job"]

    tree = mj["job_tree"]
    assert tree["material_cost"] == pytest.approx(mj["material_cost"])
    assert tree["total_job_cost"] == pytest.approx(mj["total_job_cost"])
    invention_node = _find_first_activity_node(tree, "invention")
    assert invention_node is not None and invention_node["runs"] == pytest.approx(0.2)


def test_partly_owned_bpc_amortizes_only_the_missing_runs(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    bpc = SimpleNamespace(
        type_id=9002, item_id=78, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=4,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    service._get_owned_blueprint_assets = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope: ([bpc], [], {}, {}, {}), service,
    )
    ctx = service._build_planning_context(
        force_refresh=False, build_from_bpc=True, include_reactions=False, maximize_bp_runs=False,
        group_identical_bpcs=True, have_blueprint_source_only=False, market_hub="jita",
        material_price_side="sell", product_price_side="sell", industry_profile_id=None,
        owned_blueprints_scope="all_characters", character_id=1, progress_callback=None,
    )
    t2_row = _invented_t2_blueprint_rows()[1]
    row = service._build_single_product_row(
        ctx, row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[bpc],
        blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    service._enrich_product_rows_with_material_prices([row])
    mj = row["manufacturing_job"]

    # 4 runs from the owned BPC, 6 invented: 2 x 100 x 6 / (0.5 x 10) = 240.
    assert mj["expected_invention_materials"]["204"]["invented_runs"] == 6
    assert mj["invention_material_cost"] == pytest.approx(240.0)
    # D3: two jobs. 4 runs at the owned ME 0 = 40 Tritanium; 6 runs on the
    # invented BPC at ME 2 = ceil(60 x 0.98) = 59. Was 100 at ME 0 for all 10.
    assert mj["materials"]["34"]["quantity"] == 99
    assert mj["material_cost"] == pytest.approx(99 * 5 + 240.0)
    segments = mj["job_segments"]
    assert [(s["runs"], s["blueprint_material_efficiency"], s["source"]) for s in segments] == [
        (4, 0, "owned_blueprint_copy"), (6, 2, "invented_blueprint_copy"),
    ]
    assert [s["materials"]["34"] for s in segments] == [40, 59]
    # Time per job: 4 x 100 s at TE 0, 6 x 100 s at TE 4 = 576.
    assert mj["manufacturing_time_seconds"] == 400 + 576


def test_owned_t2_bpo_row_has_no_invention_cost(monkeypatch) -> None:
    bpo = SimpleNamespace(
        type_id=9002, item_id=79, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    profile = {
        "installation_cost_modifier": 0.0, "facility_tax": 0.0, "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0, "facility_cost_bonus": 0.0,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    mj = _invention_overview(monkeypatch, owned_assets=([], [bpo], {}, {}, {}), profile=profile)[5002]["manufacturing_job"]

    assert mj["blueprint_source_kind"] == "copied_from_owned_blueprint_original"
    assert "invention" not in mj["activity_breakdown"]
    assert "invention_material_cost" not in mj
    assert mj["material_cost"] == pytest.approx(50.0)
    # Manufacturing only: 50 EIV x (0.05 + SCC 0.04) = 4.5 (the fixture T2
    # blueprint has no copy time, and the 2% invention index must add nothing).
    assert mj["total_job_cost"] == pytest.approx(4.5)
    assert mj["elapsed_time_seconds"] == mj["time_seconds"]


_ASSUMES_T2_BPO_REASON = "costed as if a T2 BPO is owned (build_from_bpc off)"


def test_t2_row_without_build_from_bpc_is_flagged_as_assuming_an_owned_t2_bpo(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, build_from_bpc=False, real_confidence=True)[5002]["manufacturing_job"]

    assert mj["assumes_owned_t2_bpo"] is True
    assert _ASSUMES_T2_BPO_REASON in mj["pricing_confidence_reasons"]
    # Labelling only: still no invention in the cost.
    assert "invention_material_cost" not in mj
    assert "invention" not in mj["activity_breakdown"]


def test_t2_row_with_build_from_bpc_is_not_flagged(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, build_from_bpc=True, real_confidence=True)[5002]["manufacturing_job"]

    assert not mj.get("assumes_owned_t2_bpo")
    assert _ASSUMES_T2_BPO_REASON not in mj["pricing_confidence_reasons"]


def test_t1_row_without_build_from_bpc_is_not_flagged(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, build_from_bpc=False, real_confidence=True)[6001]["manufacturing_job"]

    assert not mj.get("assumes_owned_t2_bpo")
    assert _ASSUMES_T2_BPO_REASON not in mj["pricing_confidence_reasons"]


def test_owned_t2_bpo_without_build_from_bpc_is_not_flagged(monkeypatch) -> None:
    bpo = SimpleNamespace(
        type_id=9002, item_id=79, is_blueprint_copy=False,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=None,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    mj = _invention_overview(
        monkeypatch, build_from_bpc=False, real_confidence=True, owned_assets=([], [bpo], {}, {}, {}),
    )[5002]["manufacturing_job"]

    assert not mj.get("assumes_owned_t2_bpo")
    assert _ASSUMES_T2_BPO_REASON not in mj["pricing_confidence_reasons"]


def test_unpriced_invention_input_lowers_confidence_and_is_named(monkeypatch, caplog) -> None:
    # No sell price and no adjusted price for the datacore anywhere.
    with caplog.at_level("WARNING"):
        rows = _invention_overview(monkeypatch, prices={34: 5.0}, real_confidence=True)
    mj = rows[5002]["manufacturing_job"]
    priced = _invention_overview(monkeypatch, real_confidence=True)[5002]["manufacturing_job"]

    assert mj["expected_invention_materials"]["204"]["unit_price"] is None
    assert mj["invention_material_cost"] is None
    assert mj["material_cost"] == pytest.approx(50.0)
    assert mj["material_type_count"] == 2 and mj["priced_material_count"] == 1
    assert priced["material_pricing_confidence"] == "High"
    assert mj["material_pricing_confidence"] != "High"
    assert any("Datacore" in reason for reason in mj["pricing_confidence_reasons"])
    assert any("invention input 204" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")


def test_invention_input_without_a_plan_price_falls_back_to_the_market_map(monkeypatch) -> None:
    service = _invention_service(monkeypatch, prices={34: 5.0})
    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    row = next(r for r in rows if r["type_id"] == 5002)
    assert row["manufacturing_job"]["expected_invention_materials"]["204"]["unit_price"] is None

    # The pricing step's own market lookup now has the datacore.
    monkeypatch.setattr(
        MarketPricingService, "get_type_price_map",
        lambda self, *, type_ids, hub="jita", side="sell", progress_callback=None: {
            204: {"unit_price": 100.0, "price_source": "market_fallback"},
        },
    )
    service._enrich_product_rows_with_material_prices([row])
    mj = row["manufacturing_job"]

    assert mj["expected_invention_materials"]["204"]["unit_price"] == 100.0
    assert mj["expected_invention_materials"]["204"]["price_source"] == "market_fallback"
    assert mj["expected_invention_materials"]["204"]["line_total"] == pytest.approx(40.0)
    assert mj["invention_material_cost"] == pytest.approx(40.0)


def test_missing_sde_runs_and_probability_log_warnings(monkeypatch, caplog) -> None:
    rows = _invented_t2_blueprint_rows()
    product = rows[0]["invention_job"]["products"][0]
    product["probability_pct"] = 0.0
    product["quantity"] = 0
    with caplog.at_level("WARNING"):
        mj = _invention_overview(monkeypatch, blueprint_rows=rows)[5002]["manufacturing_job"]

    messages = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any("no SDE success probability" in m for m in messages)
    assert any("no SDE runs per invented BPC" in m for m in messages)
    invention = mj["activity_breakdown"]["invention"]
    assert invention["success_probability"] == pytest.approx(0.01)  # the admin floor's default
    assert invention["runs_per_invented_blueprint"] == 10  # the T2 limit fallback


# --- fix round 2: elapsed time, breakdown sums, zero-probability guard ----------


def test_activity_breakdown_fees_sum_to_total_job_cost(monkeypatch) -> None:
    profile = {
        "installation_cost_modifier": 0.1, "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0, "facility_cost_bonus": 0.0,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    mj = _invention_overview(monkeypatch, profile=profile)[5002]["manufacturing_job"]

    fees = [
        float(entry["total_job_cost"])
        for entry in mj["activity_breakdown"].values()
        if isinstance(entry, dict) and entry.get("total_job_cost") is not None
    ]
    assert {"manufacturing", "invention", "invention_source_copying"} <= set(mj["activity_breakdown"])
    assert sum(fees) == pytest.approx(mj["total_job_cost"])


def test_elapsed_time_uses_whole_invention_jobs(monkeypatch) -> None:
    # The harness profile (fees priced, no bonuses), no skills: no time
    # reductions. 1 run, p = 0.5, 10 runs per BPC.
    mj = _invention_overview(monkeypatch)[5002]["manufacturing_job"]
    invention = mj["activity_breakdown"]["invention"]
    source_copy = mj["activity_breakdown"]["invention_source_copying"]

    # Amortized slot time (ISK/h): manufacturing 96 (TE 4) + invention 40 x 0.2 + copy 20 x 0.2.
    assert mj["time_seconds"] == 96 + 8 + 4
    # Elapsed: 1 BPC needed, ceil(1 / 0.5) = 2 whole attempts of 40 s in sequence,
    # 2 source-copy runs of 20 s, then manufacturing 96 (TE 4).
    assert invention["job_duration_seconds"] == 40
    assert invention["whole_attempts"] == 2
    assert invention["elapsed_duration_seconds"] == 80
    assert source_copy["job_duration_seconds"] == 20
    assert source_copy["elapsed_duration_seconds"] == 40
    assert mj["elapsed_time_seconds"] == 96 + 80 + 40
    node = _find_first_activity_node(mj["job_tree"], "invention")
    assert node is not None
    assert node["job_duration_seconds"] == 40 and node["duration_is_amortized"] is True


def test_manufacture_window_reads_elapsed_not_amortized_time() -> None:
    row = {
        "days_of_supply": 0.5,
        "manufacturing_job": {"time_seconds": 3600.0, "elapsed_time_seconds": 2 * 86400.0},
    }
    out = IndustryService._enrich_product_rows_with_manufacturing_signals(
        cast(Any, object.__new__(IndustryService)), [row],
    )
    # Amortized 1 h would say it sells in time; the real 2 days do not.
    assert out[0]["manufacture_window_ok"] is False


def test_zero_probability_and_zero_floor_never_divides(monkeypatch, caplog) -> None:
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    with caplog.at_level("WARNING"):
        out = _invention_overview(
            monkeypatch, blueprint_rows=rows, real_confidence=True,
            adm_overrides={"invention_probability_floor": 0.0},
        )
    mj = out[5002]["manufacturing_job"]

    assert "no invention success probability" in mj["invention_cost_unknown_reason"]
    assert mj["activity_breakdown"]["invention"]["total_job_cost"] is None
    assert "invention_material_cost" not in mj
    assert mj["elapsed_time_seconds"] is None
    assert mj["material_pricing_confidence"] == "Low"
    assert any("Invention cost unknown" in r for r in mj["pricing_confidence_reasons"])
    assert any("invention cost left unknown" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")
    # A T1 row in the same refresh is untouched.
    assert out[6001]["manufacturing_job"]["material_cost"] == pytest.approx(15.0)


def test_zero_floor_does_not_break_an_owned_bpc_row(monkeypatch) -> None:
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    bpc = SimpleNamespace(
        type_id=9002, item_id=80, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    mj = _invention_overview(
        monkeypatch, blueprint_rows=rows, owned_assets=([bpc], [], {}, {}, {}),
        adm_overrides={"invention_probability_floor": 0.0},
    )[5002]["manufacturing_job"]

    assert mj["material_cost"] == pytest.approx(50.0)
    # Batch 1 is fine; only the batches past the owned BPC runs are unknown
    # (Task I R3, see test_unknown_odds_with_owned_bpc_covering_batch_one_...).
    assert mj["invention_cost_unknown_reason"].startswith("batches past the owned T2 BPC runs")


def test_nested_t2_component_uses_expected_invention_attempts(monkeypatch) -> None:
    rows = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, profile=_NESTED_FEES_PROFILE,
    )
    mj = rows[7100]["manufacturing_job"]

    # 1 parent run -> 1 component run -> 1 / (0.5 x 10) = 0.2 expected attempts
    # (was ceil(1 / 0.5) = 2 whole attempts: 4 datacores, 400 ISK, fee 8.0).
    nested = mj["recursive_activity_breakdown"]["manufacturing:5002"]
    assert nested["recommended_action"] == "build"
    invention = nested["nested"]["invention:9002"]
    assert invention["attempts"] == pytest.approx(0.2)
    assert invention["whole_attempts"] == 2
    assert invention["job_cost"] == pytest.approx(_T2_INVENTION_FEE_PER_ATTEMPT * 0.2)  # 0.012

    inputs = [m for m in mj["expected_invention_materials"].values() if m["type_id"] == 204]
    assert len(inputs) == 1
    assert inputs[0]["quantity"] == pytest.approx(0.4)
    assert inputs[0]["quantity_per_attempt"] == 2
    assert inputs[0]["expected_attempts"] == pytest.approx(0.2)
    assert inputs[0]["line_total"] == pytest.approx(40.0)
    # The buy list gets whole attempts: 2 attempts x 2 datacores, all bought.
    buy = mj["invention_procurement_materials"]["204"]
    assert (buy["quantity"], buy["take_quantity"], buy["buy_quantity"]) == (4, 0, 4)

    # Tritanium 10 x 5 from the component, plus 40 of amortized datacores.
    assert mj["invention_material_cost"] == pytest.approx(40.0)
    assert mj["material_cost"] == pytest.approx(90.0)
    # The other fees are ~ the 4 % SCC on their job values (_NESTED_FEES_PROFILE):
    # parent manufacturing 10,000 EIV -> 400, component 50 -> 2, the source copy
    # 0.02 x 15 x 0.2 attempts -> 0.0024; the invention fee is the 0.012 above.
    assert mj["total_job_cost"] == pytest.approx(400.0 + 2.0 + 0.0024 + 0.012, abs=1e-4)
    assert mj["total_cost"] == pytest.approx(90.0 + mj["total_job_cost"])

    # Slot time: parent 200 + component 96 (1 run x 100 s, TE 4) + invention
    # 40 x 0.2 + copy 20 x 0.2.
    assert mj["time_seconds"] == 200 + 96 + 8 + 4
    # Elapsed: 2 whole attempts of 40 s and 2 source-copy runs of 20 s in sequence.
    assert mj["elapsed_time_seconds"] == 200 + 96 + 80 + 40

    # The top-level T2 row in the same refresh is unchanged.
    t2 = rows[5002]["manufacturing_job"]
    assert t2["material_cost"] == pytest.approx(90.0)
    assert t2["total_job_cost"] == pytest.approx(2.0 + 0.0024 + 0.012, abs=1e-4)
    assert t2["elapsed_time_seconds"] == 96 + 80 + 40  # manufacturing 96 (TE 4)


def test_nested_t2_zero_probability_and_zero_floor_never_divides(monkeypatch, caplog) -> None:
    with caplog.at_level("WARNING"):
        rows = _invention_overview(
            monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(probability_pct=0.0),
            prices=_NESTED_FAKE_PRICES, profile=_INVENTION_ONLY_PROFILE, real_confidence=True,
            adm_overrides={"invention_probability_floor": 0.0},
        )
    assert any("invention cost left unknown" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")

    # The top-level T2 row: invention cost unknown, never divided by zero.
    t2 = rows[5002]["manufacturing_job"]
    assert "no invention success probability" in t2["invention_cost_unknown_reason"]
    assert not t2.get("expected_invention_materials")
    assert t2["elapsed_time_seconds"] is None
    assert t2["material_pricing_confidence"] == "Low"
    assert any("Invention cost unknown" in r for r in t2["pricing_confidence_reasons"])
    # A T1 row in the same refresh is untouched.
    assert rows[6001]["manufacturing_job"]["material_cost"] == pytest.approx(15.0)


def test_nested_sub_build_with_unknown_invention_cost_is_bought(monkeypatch) -> None:
    # Unknown odds leave the sub-build's estimated_total_cost without the
    # invention, so it looked cheaper than its 10,000 ISK market price and
    # was built with no datacores bought. It is bought instead. Every other
    # fee is priced (_NESTED_FEES_PROFILE), so only the unknown invention cost
    # can make it a buy (Task 15 fix round 2).
    rows = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(probability_pct=0.0),
        prices=_NESTED_FAKE_PRICES, profile=_NESTED_FEES_PROFILE,
        adm_overrides={"invention_probability_floor": 0.0},
    )
    mj = rows[7100]["manufacturing_job"]

    assert "manufacturing:5002" not in mj["recursive_activity_breakdown"]

    def nodes(node):
        yield node
        for child in node.get("children") or []:
            yield from nodes(child)

    (bought,) = [n for n in nodes(mj["job_tree"]) if n.get("type_id") == 5002 and n.get("node_type") == "material"]
    assert bought["recommendation_action"] == "buy"
    assert bought["build_cost_unknown_reason"].startswith("build cost unknown: invention cost unknown")
    assert mj["procurement_materials"]["5002"]["quantity"] == 1
    assert mj["material_cost"] == pytest.approx(10_000.0)
    assert "invention_cost_unknown_reason" not in mj
    assert "invention_procurement_materials" not in mj
    assert "invention_procurement_materials_per_extra_batch" not in mj


def test_top_level_t2_shopping_list_buys_datacores_for_whole_attempts(monkeypatch) -> None:
    row = _invention_overview(monkeypatch)[5002]
    mj = row["manufacturing_job"]

    # 1 run, p 0.5: 2 whole attempts x 2 datacores. Cost stays amortized (40),
    # so the buy line is not priced into material_cost a second time.
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 4
    assert mj["material_cost"] == pytest.approx(90.0)
    assert "204" not in mj["procurement_materials"]
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)


def test_nested_t2_shopping_list_buys_datacores_for_whole_attempts(monkeypatch) -> None:
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
    )[7100]

    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)


def test_owned_datacores_reduce_the_invention_buy(monkeypatch) -> None:
    row = _invention_overview(monkeypatch, owned_item_inventory=({204: 3}, {204: 60.0}))[5002]

    buy = row["manufacturing_job"]["invention_procurement_materials"]["204"]
    assert (buy["quantity"], buy["take_quantity"], buy["buy_quantity"]) == (4, 3, 1)
    assert buy["sourcing_strategy"] == "split"
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 1)


@pytest.mark.parametrize(
    ("component_price", "take", "buy"),
    [
        # Component bought: its evaluated invention spends no stock, so the
        # parent's own 4 datacores take all 3 owned and buy 1.
        (1.0, 3, 1),
        # Component built: 4 nested + 4 top-level datacores, 3 taken, 5 bought.
        (10_000.0, 3, 5),
    ],
)
def test_evaluating_a_nested_invention_does_not_spend_stock(monkeypatch, component_price, take, buy) -> None:
    row = _invention_overview(
        monkeypatch, blueprint_rows=_invented_parent_rows(),
        prices={**_NESTED_FAKE_PRICES, 5002: component_price},
        owned_item_inventory=({204: 3}, {204: 60.0}),
    )[7100]
    mj = row["manufacturing_job"]

    built = mj["recursive_activity_breakdown"].get("manufacturing:5002", {}).get("recommended_action") == "build"
    assert built is (component_price > 1.0)
    line = mj["invention_procurement_materials"]["204"]
    assert (line["take_quantity"], line["buy_quantity"]) == (take, buy)
    assert isinstance(line["take_quantity"], int) and isinstance(line["buy_quantity"], int)


def test_owned_bpc_covering_batch_one_still_buys_datacores_for_later_batches(monkeypatch) -> None:
    # A 10-run owned BPC covers batch 1 (1 run) and the next 9 one-run
    # batches (fix round 1, I2: later batches use what an owned BPC covers).
    # Past those 10 runs the batches invent: 1 BPC (10 runs), p 0.5 -> 2
    # whole attempts x 2 = 4 datacores.
    row = _invention_overview(monkeypatch, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}))[5002]
    mj = row["manufacturing_job"]
    assert "invention_procurement_materials" not in mj
    plan = mj["invention_bpc_plan"]
    assert (plan["owned_blueprint_runs"], plan["runs_per_invented_blueprint"], plan["whole_attempts_per_blueprint"]) == (10, 10, 2)

    row["max_batches_total"] = 5
    assert _shopping_line(row, 204) is None  # 5 runs, all on the owned BPC
    row["max_batches_total"] = 15
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)  # runs 11..15: 1 BPC


def test_partly_owned_bpc_buys_full_batch_datacores_for_later_batches(monkeypatch) -> None:
    # 2-run invented BPCs, a 4-run owned BPC, 10 runs per batch, 3 batches.
    # Batch 1 invents 6 runs: 3 BPCs / 0.5 = 6 attempts x 2 = 12 datacores.
    # Later batches invent 10 runs: 5 BPCs / 0.5 = 10 attempts x 2 = 20 each.
    rows = _rows_with(probability_pct=50.0, bpc_runs=2, t2_limit=10)
    service = _invention_service(monkeypatch, blueprint_rows=rows)
    bpc = _owned_t2_bpc(runs=4, item_id=82)
    service._get_owned_blueprint_assets = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope: ([bpc], [], {}, {}, {}), service,
    )
    ctx = service._build_planning_context(
        force_refresh=False, build_from_bpc=True, include_reactions=False, maximize_bp_runs=False,
        group_identical_bpcs=True, have_blueprint_source_only=False, market_hub="jita",
        material_price_side="sell", product_price_side="sell", industry_profile_id=None,
        owned_blueprints_scope="all_characters", character_id=1, progress_callback=None,
    )
    t2_row = rows[1]
    row = service._build_single_product_row(
        ctx, row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[bpc],
        blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    mj = row["manufacturing_job"]
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 12
    # One later batch on its own (the no-BPC-plan fallback list): 20.
    later = _later_plan(mj, 2)
    assert later["invention_procurement_materials"]["204"]["quantity"] == 20

    row["max_batches_total"] = 3
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (52, 52)  # 12 + 2 x 20


def test_one_run_batches_share_invented_bpc_runs(monkeypatch) -> None:
    # Fix round 1, I2: no owned BPC, 1-run batches, 10-run BPCs at p 0.5.
    # 5 batches = 5 runs = 1 BPC = 2 attempts = 4 datacores (was 4 per batch, 20);
    # 25 batches = 3 BPCs = 6 attempts = 12.
    row = _invention_overview(monkeypatch)[5002]
    mj = row["manufacturing_job"]
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4

    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)
    row["max_batches_total"] = 25
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (12, 12)


def test_full_bpc_batches_without_a_decryptor_buy_what_they_did_before(monkeypatch) -> None:
    # 10-run batches on 10-run BPCs: 1 BPC = 2 attempts = 4 datacores per batch.
    row = _invention_overview(monkeypatch, maximize_bp_runs=True)[5002]
    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (20, 20)


def test_owned_datacores_are_netted_once_with_an_extra_batch_list(monkeypatch) -> None:
    # 3 owned: batch 1 takes them, the later-batch lines take 0. 10-run
    # batches: 4 datacores each, 5 batches = 20 - 3 = 17.
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, owned_item_inventory=({204: 3}, {204: 60.0}),
    )[5002]
    extra = row["manufacturing_job"]["invention_procurement_materials_per_extra_batch"]["204"]
    assert (extra["take_quantity"], extra["buy_quantity"]) == (0, 4)

    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (20, 17)


def test_nested_invention_is_in_the_extra_batch_list(monkeypatch) -> None:
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
    )[7100]
    assert row["manufacturing_job"]["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4

    # I3: one nested 10-run BPC invented in batch 1 covers 3 one-run batches
    # (was 3 x 4 = 12); 11 batches need a second BPC.
    row["max_batches_total"] = 3
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)
    row["max_batches_total"] = 11
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (8, 8)


def test_build_from_bpc_off_writes_no_invention_buy_lists(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, build_from_bpc=False)[5002]["manufacturing_job"]
    assert "invention_procurement_materials" not in mj
    assert "invention_procurement_materials_per_extra_batch" not in mj


def test_extra_batch_list_does_not_repeat_the_unknown_odds_warning(monkeypatch, caplog) -> None:
    # No owned BPC: the extra batch reuses batch 1's attempt count, so the
    # unknown-odds WARNING is logged once, as before the extra-batch list.
    with caplog.at_level("WARNING"):
        _invention_overview(
            monkeypatch, blueprint_rows=_rows_with(probability_pct=0.0, bpc_runs=10, t2_limit=10),
            adm_overrides={"invention_probability_floor": 0.0},
        )
    unknown = [r for r in caplog.records if "invention cost left unknown" in r.getMessage()]
    assert len(unknown) == 1


# --- round 4 Task I R1: nested owned BPC in batch 1, datacores for batches 2..N -


def test_nested_owned_bpc_in_batch_one_still_buys_nested_datacores_for_later_batches(monkeypatch) -> None:
    # The parent 7100 builds 1 T2 component (5002) per run. A 10-run owned 9002
    # BPC covers batch 1's component run, so batch 1 invents nothing. A later
    # batch's own plan assumes the BPC spent and invents that run: 1 run / 10
    # per BPC -> 1 BPC, p 0.5 -> 2 whole attempts x 2 = 4 datacores. Over all
    # batches (I3) the BPC's 9 runs left cover batches 2-10 first.
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]
    nested = mj["recursive_activity_breakdown"]["manufacturing:5002"]
    assert nested["recommended_action"] == "build"
    assert "invention:9002" not in nested["nested"]
    assert "invention_procurement_materials" not in mj
    later = _later_plan(mj, 2)
    extra = later["invention_procurement_materials"]["204"]
    assert (extra["quantity"], extra["take_quantity"], extra["buy_quantity"]) == (4, 0, 4)
    assert later["procurement_materials"]["34"]["quantity"] == 10

    row["max_batches_total"] = 5
    assert _shopping_line(row, 204) is None   # 4 later runs <= 9 left (was 4 x 4 = 16)
    row["max_batches_total"] = 15
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)   # 14 - 9 = 5 runs: 1 BPC


def test_nested_without_owned_bpc_extra_batch_datacores_are_unchanged(monkeypatch) -> None:
    # No owned BPC: batch 1 invents the run (4 datacores) on a 10-run BPC,
    # whose 9 spare runs cover batches 2-5 (I3; was 4 + 4 x 4 = 20).
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
    )[7100]
    mj = row["manufacturing_job"]
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 4
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4

    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4, 4)
    row["max_batches_total"] = 21
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (12, 12)   # 21 runs: 3 BPCs


def test_nested_sub_build_bought_in_batch_one_adds_no_extra_batch_datacores(monkeypatch) -> None:
    # The component is cheaper to buy (1 ISK), so it is bought in batch 1 and
    # the extra-batch list gets nothing for it, owned BPC or not.
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(),
        prices={**_NESTED_FAKE_PRICES, 5002: 1.0},
        owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]
    assert "manufacturing:5002" not in mj["recursive_activity_breakdown"]
    assert "invention_procurement_materials_per_extra_batch" not in mj
    # The rejected sub-build spends no owned BPC run (FU-61), so every later
    # batch repeats batch 1: no schedule, the component bought each batch.
    assert "extra_batch_schedule" not in mj
    assert mj["procurement_materials"]["5002"]["quantity"] == 1
    row["max_batches_total"] = 5
    component = _shopping_line(row, 5002)
    assert component is not None and component["buy"] == 5
    assert _shopping_line(row, 204) is None


# --- round 4 Task I R3: unknown odds past an owned BPC are flagged, never 0 ----


def _unknown_need_rows(rows: list[dict]) -> dict:
    from eve_online_industry_tracker.application.industry.shopping_list import invention_need_unknown_rows

    return invention_need_unknown_rows(rows)


def test_unknown_odds_with_owned_bpc_covering_batch_one_flag_the_later_batches(monkeypatch, caplog) -> None:
    # p 0 and floor 0: batch 1 runs on the owned 10-run BPC and needs no
    # invention, but batches 2..N must invent with unknown odds.
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    with caplog.at_level("WARNING"):
        row = _invention_overview(
            monkeypatch, blueprint_rows=rows, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
            real_confidence=True, adm_overrides={"invention_probability_floor": 0.0},
        )[5002]
    mj = row["manufacturing_job"]

    assert mj["material_cost"] == pytest.approx(50.0)  # batch 1 itself is unchanged
    assert "no invention success probability" in mj["invention_cost_unknown_reason"]
    assert "past the owned T2 BPC runs" in mj["invention_cost_unknown_reason"]
    assert mj["invention_datacore_need_unknown"]["from_batch"] == 2
    assert mj["material_pricing_confidence"] == "Low"
    assert mj["overall_pricing_confidence"] == "Low"
    # Batch 1 runs on the owned BPC, so its cost is exact: not the
    # "T2 cost is understated" wording (Task I fix round 1).
    assert mj["invention_datacore_need_unknown"]["batch_one_cost_exact"] is True
    assert any(
        "Datacore need unknown for batches past the owned BPC runs" in r
        and "batch 1 cost is exact, later batches will cost more" in r
        for r in mj["pricing_confidence_reasons"]
    )
    assert not any("Invention cost unknown" in r for r in mj["pricing_confidence_reasons"])
    # One WARNING for the row, not "invention cost left unknown" as well.
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert sum("datacore need unknown" in m for m in warnings) == 1
    assert not any("invention cost left unknown" in m for m in warnings)
    # No made-up datacore quantity anywhere.
    assert "invention_procurement_materials" not in mj
    assert "invention_procurement_materials_per_extra_batch" not in mj

    row["max_batches_total"] = 5
    assert _shopping_line(row, 204) is None
    flagged = _unknown_need_rows([row])
    assert list(flagged) == ["T2 Module"]
    assert "no invention success probability" in flagged["T2 Module"]
    # One batch runs on the owned BPC only: nothing unknown to flag.
    row["max_batches_total"] = 1
    assert _unknown_need_rows([row]) == {}


def test_unknown_odds_with_nested_owned_bpc_covering_batch_one_buy_the_component_later(monkeypatch, caplog) -> None:
    # Batch 1 builds the component on the owned BPC (a known cost). Later
    # batches would have to invent it with unknown odds, so they buy it, the
    # same rule as batch 1 (FU-60): no datacore guess and no flag.
    with caplog.at_level("WARNING"):
        row = _invention_overview(
            monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(probability_pct=0.0),
            prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
            real_confidence=True, adm_overrides={"invention_probability_floor": 0.0},
        )[7100]
    mj = row["manufacturing_job"]

    assert mj["recursive_activity_breakdown"]["manufacturing:5002"]["recommended_action"] == "build"
    assert _later_plan(mj, 2)["procurement_materials"]["5002"]["quantity"] == 1
    assert "invention_datacore_need_unknown" not in mj
    assert "invention_cost_unknown_reason" not in mj
    assert not any(m.startswith("Product 7100: datacore need unknown")
                   for m in (r.getMessage() for r in caplog.records if r.levelname == "WARNING"))

    row["max_batches_total"] = 3
    line = _shopping_line(row, 5002)
    assert line is not None and line["need"] == 2
    assert _unknown_need_rows([row]) == {}


def test_unknown_odds_without_owned_bpc_flag_every_batch(monkeypatch) -> None:
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    row = _invention_overview(
        monkeypatch, blueprint_rows=rows, adm_overrides={"invention_probability_floor": 0.0},
    )[5002]
    assert row["manufacturing_job"]["invention_datacore_need_unknown"]["from_batch"] == 1
    assert _shopping_line(row, 204) is None
    assert list(_unknown_need_rows([row])) == ["T2 Module"]


def test_known_odds_rows_are_not_flagged(monkeypatch) -> None:
    rows = _invention_overview(monkeypatch, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}))
    rows[5002]["max_batches_total"] = 5
    assert "invention_datacore_need_unknown" not in rows[5002]["manufacturing_job"]
    assert _unknown_need_rows(list(rows.values())) == {}



def test_batch_one_unknown_odds_keep_the_understated_wording(monkeypatch, caplog) -> None:
    # No owned BPC: batch 1 itself invents with unknown odds, so the T2 cost
    # really leaves the invention out. One WARNING (the batch-1 evaluation's).
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    with caplog.at_level("WARNING"):
        mj = _invention_overview(
            monkeypatch, blueprint_rows=rows, real_confidence=True,
            adm_overrides={"invention_probability_floor": 0.0},
        )[5002]["manufacturing_job"]
    assert mj["invention_datacore_need_unknown"]["batch_one_cost_exact"] is False
    assert mj["material_pricing_confidence"] == "Low"
    assert any(
        "Invention cost unknown" in r and "so it is understated" in r for r in mj["pricing_confidence_reasons"]
    )
    assert not any("batch 1 cost is exact" in r for r in mj["pricing_confidence_reasons"])
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert sum("invention cost left unknown" in m for m in warnings) == 1
    assert not any("datacore need unknown" in m for m in warnings)


def test_an_invented_t2_row_is_built_at_me_2_te_4(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[5002]["manufacturing_job"]

    assert mj["blueprint_source_kind"] == "unowned_blueprint_copy"
    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (2, 4)
    assert mj["materials"]["34"]["quantity"] == 98  # ceil(10 runs x 10 x 0.98), was 100
    assert mj["manufacturing_time_seconds"] == 960  # 10 x 100 s x 0.96, was 1000


def test_an_unowned_t1_bpc_without_an_invention_path_stays_me_0(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[6001]["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (0, 0)
    assert mj["materials"]["34"]["quantity"] == 30


def test_a_t2_blueprint_nothing_invents_is_not_given_the_invented_baseline(monkeypatch) -> None:
    rows = _invented_t2_blueprint_rows()[1:]  # the T2 blueprint without its invention source
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True, blueprint_rows=rows)[5002]["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (0, 0)
    assert "invention" not in mj["activity_breakdown"]
    assert mj["materials"]["34"]["quantity"] == 100


def test_a_nested_invented_component_is_built_at_me_2_without_a_copy_job(monkeypatch) -> None:
    rows = _nested_invention_blueprint_rows()
    # A real T2 blueprint has an SDE copy time. The invention supplies the BPC,
    # so no copy of the T2 blueprint may be costed (FU-58).
    rows[1]["copying_job"] = {"time_seconds": 30}
    out = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=rows,
        prices=_NESTED_FAKE_PRICES, profile=_NESTED_FEES_PROFILE,
    )
    mj = out[7100]["manufacturing_job"]
    nested = mj["recursive_activity_breakdown"]["manufacturing:5002"]

    assert nested["recommended_action"] == "build"
    assert "copying" not in nested["nested"]
    assert "invention:9002" in nested["nested"]
    # The parent (no invention path, ME 0) needs 10 components; the component
    # job runs on the invented BPC at ME 2: ceil(10 x 10 x 0.98) = 98 Tritanium.
    assert mj["procurement_materials"]["34"]["quantity"] == 98


def test_decryptor_options_are_loaded_and_priced_like_datacores(monkeypatch) -> None:
    service = _invention_service(monkeypatch, prices={**_INVENTION_FAKE_PRICES, 34201: 50.0})
    service._load_decryptor_options = MethodType(  # type: ignore[attr-defined]
        lambda self: (NO_DECRYPTOR, _ACCELERANT_LIKE), service,
    )
    ctx = _planning_context(service)

    assert ctx.decryptor_options == (NO_DECRYPTOR, _ACCELERANT_LIKE)
    assert ctx.material_price_map[34201]["unit_price"] == 50.0
    assert ctx.blueprint_row_by_type_id[9002]["blueprint_type_id"] == 9002
    # Task 3: the context now answers with the blueprint's chosen decryptor
    # (no sell price for 5002 here, so the cheapest per unit: Accelerant).
    assert ctx.invention_context.decryptor_for_blueprint(9002) is _ACCELERANT_LIKE


def test_without_an_sde_session_there_are_no_decryptors_and_a_warning(caplog) -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    service._sessions = None  # type: ignore[attr-defined]
    with caplog.at_level("WARNING"):
        assert service._load_decryptor_options() == (NO_DECRYPTOR,)
    assert any("no decryptor data" in r.getMessage() for r in caplog.records)


# --- round 5 Task 1, fix round 1: one decryptor source, one invention predicate ---


def _top_level_t2_row(service, ctx, **kwargs):
    t2_row = _invented_t2_blueprint_rows()[1]
    return service._build_single_product_row(
        ctx, row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[],
        blueprint_original_asset=None, effective_runs=10, **kwargs,
    )


def test_the_top_level_row_takes_its_decryptor_from_the_invention_context(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    ctx = _planning_context(service)
    asked: list[int] = []
    ctx.invention_context.decryptor_for_blueprint = lambda bp: (asked.append(bp), _ACCELERANT_LIKE)[1]

    mj = _top_level_t2_row(service, ctx)["manufacturing_job"]

    # ME 2 + 2 = 4, TE 4 + 10 = 14: the context's decryptor, as in the nested planner.
    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (4, 14)
    assert mj["materials"]["34"]["quantity"] == 96  # ceil(100 x 0.96)
    assert asked == [9002]


def test_an_explicit_decryptor_overrides_the_invention_context(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    ctx = _planning_context(service)
    ctx.invention_context.decryptor_for_blueprint = lambda bp: _ACCELERANT_LIKE

    mj = _top_level_t2_row(service, ctx, decryptor=NO_DECRYPTOR)["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (2, 4)


def test_a_row_that_does_not_invent_never_asks_for_a_decryptor(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    ctx = _planning_context(service)

    def _must_not_be_asked(bp):
        raise AssertionError(f"asked a decryptor for {bp}")

    ctx.invention_context.decryptor_for_blueprint = _must_not_be_asked
    t1_row = _invented_t2_blueprint_rows()[0]
    row = service._build_single_product_row(
        ctx, row=t1_row, raw_product=t1_row["manufacturing_job"]["products"][0],
        row_index=0, product_index=0, blueprint_copy_assets=[],
        blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    assert row["manufacturing_job"]["blueprint_material_efficiency"] == 0


def test_a_source_row_without_an_invention_job_dict_is_not_an_invention_path(monkeypatch, caplog) -> None:
    # The nested planner requires invention_job to be a dict; the top level now
    # uses the same predicate (it used to coerce a None job to {} and invent).
    service = _invention_service(monkeypatch)
    ctx = _planning_context(service)
    ctx.invention_row_by_blueprint_type_id[9002] = {"blueprint_type_id": 1001, "invention_job": None}

    with caplog.at_level("WARNING"):
        mj = _top_level_t2_row(service, ctx)["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (0, 0)
    assert mj["materials"]["34"]["quantity"] == 100
    # No invention, so no chance and no base-chance fallback for it.
    assert "invention" not in mj["activity_breakdown"]
    assert "invention_chance" not in mj
    assert "invention_chance_notes" not in mj
    assert not any("Invention chance" in r.getMessage() for r in caplog.records)


# --- round 5 Task 3: the best decryptor per invented item (amendment A1) ------
# Highest profit per invention cycle: the expected attempts for one successful
# BPC plus the runs that BPC yields. Fixture: p 0.5 (no skills loaded: base
# chance), 10 runs per BPC, 2 datacores x 100 per attempt, 10 Tritanium x 5 per
# run, no fees. Fake prices.
# No decryptor:            10 runs, ME 2 -> 98 Trit = 490; 2 attempts x 200 = 400.
#                          890 per cycle, 89.00 per unit.
# Accelerant-like at 50:   11 runs, ME 4 -> 106 Trit = 530; 1/0.6 attempts x 250
#                          = 416.67. 946.67 per cycle, 86.06 per unit (cheapest).
# Attainment-like at 1000: 14 runs, ME 1 -> 139 Trit = 695; 1/0.9 attempts x 1200
#                          = 1333.33. 2028.33 per cycle, 144.88 per unit.
# At a 500 ISK sell price the profit per cycle is 4110 / 4553.33 / 4971.67.

_ATTAINMENT_LIKE = DecryptorOption(
    type_id=34202, type_name="Attainment Decryptor", probability_multiplier=1.8,
    me_modifier=-1, te_modifier=4, max_run_modifier=4,
)
_SYMMETRY_LIKE = DecryptorOption(
    type_id=34206, type_name="Symmetry Decryptor", probability_multiplier=1.0,
    me_modifier=1, te_modifier=8, max_run_modifier=2,
)
_ACC_CYCLE_COST = 530 + (1 / 0.6) * (2 * 100 + 50)
_ATT_CYCLE_COST = 695 + (1 / 0.9) * (2 * 100 + 1000)


def test_the_highest_profit_per_cycle_wins_over_the_cheapest_per_unit(monkeypatch) -> None:
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE, _ATTAINMENT_LIKE], prices={**_DECRYPTOR_PRICES, 5002: 500.0},
    )[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] == 34202
    assert choice["criterion"] == "profit_per_cycle" and choice["fallback_reason"] is None
    assert choice["profit_per_cycle"] == pytest.approx(14 * 500 - _ATT_CYCLE_COST)
    assert choice["baseline_profit_per_cycle"] == pytest.approx(10 * 500 - 890)
    candidates = {c["type_id"]: c for c in choice["candidates"]}
    assert candidates[34201]["profit_per_cycle"] == pytest.approx(11 * 500 - _ACC_CYCLE_COST)
    assert candidates[34201]["cost_per_unit"] == pytest.approx(_ACC_CYCLE_COST / 11)  # the cheapest
    assert [candidates[t]["units_per_cycle"] for t in (None, 34201, 34202)] == [10, 11, 14]
    # The row is built on the chosen Attainment BPC: ME 2 - 1, 14 runs, p 0.9.
    assert mj["blueprint_material_efficiency"] == 1
    assert mj["activity_breakdown"]["invention"]["runs_per_invented_blueprint"] == 14
    assert mj["activity_breakdown"]["invention"]["success_probability"] == pytest.approx(0.9)


def test_an_unknown_sell_price_picks_the_cheapest_per_unit_and_says_so(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE, _ATTAINMENT_LIKE])[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] == 34201
    assert choice["criterion"] == "cost_per_unit"
    assert choice["fallback_reason"] == "sell price unknown: chose lowest cost per unit"
    assert "; sell price unknown: chose lowest cost per unit" in choice["reason"]
    assert choice["cost_per_unit"] == pytest.approx(_ACC_CYCLE_COST / 11)
    assert choice["baseline_cost_per_unit"] == pytest.approx(89.0)
    costs = {c["type_id"]: c["cost_per_unit"] for c in choice["candidates"]}
    assert costs[34202] == pytest.approx(_ATT_CYCLE_COST / 14)


def test_the_chosen_decryptor_sets_me_te_runs_and_buys_one_per_attempt(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE])[5002]["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (4, 14)
    assert mj["materials"]["34"]["quantity"] == 96
    invention = mj["activity_breakdown"]["invention"]
    assert invention["runs_per_invented_blueprint"] == 11
    assert invention["success_probability"] == pytest.approx(0.6)
    attempts = 10 / 11 / 0.6
    assert mj["expected_invention_materials"]["34201"]["quantity"] == pytest.approx(attempts)
    assert mj["invention_material_cost"] == pytest.approx(attempts * (2 * 100 + 50))
    # Whole attempts: ceil(10 / 11) = 1 BPC, ceil(1 / 0.6) = 2 attempts.
    buy = mj["invention_procurement_materials"]
    assert (buy["204"]["quantity"], buy["34201"]["quantity"]) == (4, 2)
    # Batch 12 starts on a fresh BPC (110 runs = 10 BPCs): one 10-run job,
    # the same whole attempts on its own.
    extra = _later_plan(mj, 12)["invention_procurement_materials"]
    assert (extra["204"]["quantity"], extra["34201"]["quantity"]) == (4, 2)
    # The fee's job value is 0.02 x the invented blueprint's per-run EIV (50)
    # per attempt: neither the datacores nor the decryptor are part of it.
    assert invention["estimated_item_value"] == pytest.approx(0.02 * 50 * attempts)


def test_a_run_modifier_that_crosses_a_bpc_boundary_halves_the_whole_attempts(monkeypatch) -> None:
    # 12 runs: 2 BPCs at 10 runs each (4 attempts, 8 datacores) without a
    # decryptor; one 12-run BPC with the +2 decryptor (2 attempts, 4 + 2).
    rows = _rows_with(probability_pct=50.0, bpc_runs=10, t2_limit=12)
    mj = _decryptor_overview(monkeypatch, [_SYMMETRY_LIKE], blueprint_rows=rows)[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["type_id"] == 34206
    buy = mj["invention_procurement_materials"]
    assert (buy["204"]["quantity"], buy["34206"]["quantity"]) == (4, 2)


def test_a_decryptor_without_a_market_price_is_never_chosen(monkeypatch) -> None:
    prices = {k: v for k, v in _DECRYPTOR_PRICES.items() if k != 34201}
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], prices=prices)[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] is None
    assert choice["candidates"][1]["skip_reason"] == "no market price for Accelerant Decryptor"
    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (2, 4)


def test_no_decryptor_data_plans_without_a_decryptor_and_says_why(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [])[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["type_id"] is None
    assert mj["invention_decryptor"]["reason"] == "no decryptor: the SDE gave no decryptor data"


def test_decryptor_selection_off_plans_without_a_decryptor(monkeypatch) -> None:
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE],
        adm_overrides={"invention_decryptor_selection_enabled": False},
    )[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["reason"] == "no decryptor: decryptor selection is off (admin setting)"
    assert mj["blueprint_material_efficiency"] == 2
    assert "34201" not in mj["expected_invention_materials"]


def test_a_blueprint_without_invention_gets_no_decryptor_payload(monkeypatch) -> None:
    out = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=_invented_t2_blueprint_rows()[1:])
    assert "invention_decryptor" not in out[5002]["manufacturing_job"]
    assert "invention_chance" not in out[5002]["manufacturing_job"]
    t1 = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE])[6001]["manufacturing_job"]
    assert "invention_decryptor" not in t1  # the T1 source row invents nothing


def test_unknown_invention_odds_compare_no_decryptor_and_warn_once(monkeypatch, caplog) -> None:
    rows = _rows_with(probability_pct=0.0, bpc_runs=10, t2_limit=10)
    with caplog.at_level("WARNING"):
        mj = _decryptor_overview(
            monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=rows,
            adm_overrides={"invention_probability_floor": 0.0},
        )[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["type_id"] is None
    assert "unknown" in mj["invention_decryptor"]["reason"]
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert sum("invention cost left unknown" in m for m in warnings) == 1


def test_a_nested_invented_component_uses_its_blueprints_decryptor(monkeypatch) -> None:
    # 5002 sells for 10,000: Accelerant earns 11 x 10,000 - 946.67 per cycle
    # against 10 x 10,000 - 890 without a decryptor.
    out = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=_nested_invention_blueprint_rows(),
        prices={**_NESTED_FAKE_PRICES, 34201: 50.0}, profile=_FEES_KNOWN_PROFILE,
    )
    mj = out[7100]["manufacturing_job"]

    # The parent's 10 components are built on an Accelerant BPC (ME 4).
    assert mj["procurement_materials"]["34"]["quantity"] == 96
    assert any(e["type_id"] == 34201 for e in mj["expected_invention_materials"].values())
    assert mj["invention_procurement_materials"]["34201"]["quantity"] == 2
    assert "invention_decryptor" not in mj  # the parent itself is not invented
    assert out[5002]["manufacturing_job"]["invention_decryptor"]["type_id"] == 34201
    assert out[5002]["manufacturing_job"]["invention_decryptor"]["criterion"] == "profit_per_cycle"


def test_the_decryptor_choice_is_made_once_per_blueprint(monkeypatch) -> None:
    service = _decryptor_service(monkeypatch, [_ACCELERANT_LIKE])
    ctx = _planning_context(service)
    calls: list[int] = []
    real = service._evaluate_decryptors
    service._evaluate_decryptors = lambda c, bp: (calls.append(bp), real(c, bp))[1]  # type: ignore[method-assign]

    first = service._decryptor_choice_for(ctx, 9002)
    assert ctx.invention_context.decryptor_for_blueprint(9002) is first.option
    assert service._decryptor_choice_for(ctx, 9002) is first
    assert calls == [9002]


def test_the_planners_shopping_list_buys_one_decryptor_per_attempt() -> None:
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction
    from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder

    invent = AssignedAction(
        type_id=5002, type_name="T2 Module", action_type="invent", character_id=1, character_name="Pilot",
        quantity=None, runs=1, estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None,
        decryptor_type_id=34201, decryptor_type_name="Accelerant Decryptor",
    )
    blueprint_data = {
        1001: {"manufacturing": {"materials": [], "products": [{"type_id": 6001, "quantity": 1}]},
               "invention": {"materials": [{"type_id": 204, "type_name": "Datacore", "quantity": 2}],
                             "products": [{"type_id": 9002, "quantity": 10}]}},
        9002: {"manufacturing": {"materials": [], "products": [{"type_id": 5002, "quantity": 1}]}},
    }

    class _Resolver:
        def is_blueprint(self, type_id):
            return False

        def prefetch(self, type_ids):
            return None

    class _Admin:
        def get(self, section, key, default=None):
            return default

    items = ShoppingListBuilder().build(
        assigned_actions=[invent], corp_assets=[],
        market_depth_cache={204: {"vwap_5d": 100.0}, 34201: {"vwap_5d": 50.0}},
        admin_settings=_Admin(), blueprint_data=blueprint_data, meta_resolver=_Resolver(),
    )
    by_type = {i.type_id: i for i in items}
    assert by_type[204].quantity == 2
    assert by_type[34201].quantity == 1
    assert by_type[34201].shopping_category == "invention_input"


# --- Task 3 fix round 1: net profit per cycle, candidate skip reasons ---------


def _fees(tax, broker, applies=True):
    return {"rates": {"sales_tax_fraction": tax, "broker_fee_fraction": broker}, "broker_fee_applies": applies}


def test_cycle_profit_is_net_of_sales_tax_and_broker_fee(monkeypatch) -> None:
    # Sell price 60. Gross: 10 x 60 - 890 = -290 vs 11 x 60 - 946.67 = -286.67,
    # so Accelerant. Net of 5% tax + 5% broker: 10 x 54 - 890 = -350 vs
    # 11 x 54 - 946.67 = -352.67, so no decryptor.
    prices = {**_DECRYPTOR_PRICES, 5002: 60.0}
    gross = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], prices=prices)[5002]["manufacturing_job"]
    net = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], prices=prices, fee_context=_fees(0.05, 0.05),
    )[5002]["manufacturing_job"]

    assert gross["invention_decryptor"]["type_id"] == 34201
    choice = net["invention_decryptor"]
    assert choice["type_id"] is None
    assert choice["criterion"] == "profit_per_cycle"
    assert choice["baseline_profit_per_cycle"] == pytest.approx(10 * 60 * 0.9 - 890)
    assert choice["candidates"][1]["profit_per_cycle"] == pytest.approx(11 * 60 * 0.9 - _ACC_CYCLE_COST)


def test_a_broker_fee_that_does_not_apply_is_not_deducted(monkeypatch) -> None:
    # Instant sale (buy side): only the 5% tax. 10 x 57 - 890 = -320 vs
    # 11 x 57 - 946.67 = -319.67, so Accelerant.
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], prices={**_DECRYPTOR_PRICES, 5002: 60.0},
        fee_context=_fees(0.05, 0.05, applies=False),
    )[5002]["manufacturing_job"]
    assert mj["invention_decryptor"]["profit_per_cycle"] == pytest.approx(11 * 57 - _ACC_CYCLE_COST)
    assert mj["invention_decryptor"]["type_id"] == 34201


def test_an_unknown_sales_tax_falls_back_to_cost_per_unit_and_names_the_fees(monkeypatch) -> None:
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], prices={**_DECRYPTOR_PRICES, 5002: 500.0},
        fee_context=_fees(None, None),
    )[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] == 34201  # cheapest per unit
    assert choice["criterion"] == "cost_per_unit"
    assert choice["fallback_reason"] == (
        "sales tax unknown (no character to read Accounting from), so net proceeds are unknown: "
        "chose lowest cost per unit"
    )


def test_the_market_fee_context_is_resolved_once_per_refresh(monkeypatch) -> None:
    service = _decryptor_service(monkeypatch, [_ACCELERANT_LIKE], prices={**_DECRYPTOR_PRICES, 5002: 500.0})
    calls: list[dict] = []
    service._resolve_npc_market_fee_context = MethodType(  # type: ignore[attr-defined]
        lambda self, **kwargs: (calls.append(kwargs), _fees(0.05, 0.05))[1], service,
    )
    # The real sale-proceeds step, so it must reuse the planning context's fees.
    service._enrich_product_rows_with_sale_proceeds = MethodType(  # type: ignore[attr-defined]
        IndustryService._enrich_product_rows_with_sale_proceeds, service,
    )
    rows = {r["type_id"]: r for r in service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=True, character_id=1,
    )}
    assert calls == [{"character_id": 1, "market_hub": "jita", "product_price_side": "sell"}]
    assert rows[5002]["net_proceeds"] == pytest.approx(10 * 500 * 0.9)


def test_a_candidate_without_a_cost_says_why(monkeypatch) -> None:
    # M4: Tritanium has no price, so no candidate has a comparable cost.
    prices = {k: v for k, v in _DECRYPTOR_PRICES.items() if k != 34}
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], prices=prices)[5002]["manufacturing_job"]
    candidates = mj["invention_decryptor"]["candidates"]
    assert [c["skip_reason"] for c in candidates] == ["an input has no price: Tritanium"] * 2

    rows = _rows_with(probability_pct=0.0, bpc_runs=10, t2_limit=10)
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=rows, adm_overrides={"invention_probability_floor": 0.0},
    )[5002]["manufacturing_job"]
    assert mj["invention_decryptor"]["candidates"][0]["skip_reason"] == (
        "invention cost unknown: no invention success probability (SDE and admin floor are both 0)"
    )


# --- Task 3 fix round 1, I2: later batches share invented BPC runs ------------
# Damage Control II (public SDE data: 2047 invents 2049 at 34%, 10 runs per
# BPC, 2 + 2 datacores per attempt). FAKE prices. 10-run batches.


def _m(type_id, name, quantity):
    return {"type_id": type_id, "type_name": name, "quantity": quantity, "category_name": "Material"}


def _dc2_rows() -> list[dict]:
    return [
        {"blueprint_type_id": 2047, "blueprint": {"type_id": 2047, "type_name": "Damage Control I Blueprint"},
         "manufacturing_job": {"materials": [_m(34, "Tritanium", 1062), _m(35, "Pyerite", 2), _m(36, "Mexallon", 53)],
                               "skill_entries": [], "time_seconds": 900, "max_production_limit": 200,
                               "products": [{"type_id": 2046, "type_name": "Damage Control I", "quantity": 1,
                                             "meta_group_name": "Tech I"}]},
         "copying_job": {"time_seconds": 720},
         "invention_job": {"materials": [_m(20416, "Datacore - Nanite Engineering", 2),
                                         _m(20415, "Datacore - Molecular Engineering", 2)],
                           "skill_entries": [], "time_seconds": 13800,
                           "products": [{"probability_pct": 34.0, "quantity": 10,
                                         "product": {"type_id": 2049, "type_name": "Damage Control II Blueprint"}}]}},
        {"blueprint_type_id": 2049, "blueprint": {"type_id": 2049, "type_name": "Damage Control II Blueprint"},
         "manufacturing_job": {"materials": [
             _m(11553, "Oscillator Capacitor Unit", 1), _m(11399, "Morphite", 1), _m(3689, "Mechanical Parts", 5),
             _m(11695, "Plasma Pulse Generator", 1), _m(11475, "R.A.M.- Armor/Hull Tech", 1),
             _m(3828, "Construction Blocks", 3), _m(11542, "Fernite Carbide Composite Armor Plate", 1),
             _m(11540, "Quantum Microprocessor", 1), _m(2046, "Damage Control I", 1)],
             "skill_entries": [], "time_seconds": 2340, "max_production_limit": 10,
             "products": [{"type_id": 2048, "type_name": "Damage Control II", "quantity": 1,
                           "meta_group_name": "Tech II"}]}},
    ]


_DC2_FAKE_PRICES = {
    34: 4.0, 35: 10.0, 36: 80.0, 2046: 30_000.0, 20416: 50_000.0, 20415: 50_000.0,
    11553: 40_000.0, 11399: 10_000.0, 3689: 9_000.0, 11695: 30_000.0, 11475: 2_000.0, 3828: 9_000.0,
    11542: 35_000.0, 11540: 45_000.0, 2048: 1_000_000.0, 34203: 250_000.0,
}
_AUGMENTATION = DecryptorOption(
    type_id=34203, type_name="Augmentation Decryptor", probability_multiplier=0.6,
    me_modifier=-2, te_modifier=2, max_run_modifier=9,
)


def _dc2_row(monkeypatch, options):
    return _decryptor_overview(monkeypatch, options, prices=_DC2_FAKE_PRICES, blueprint_rows=_dc2_rows())[2048]


def test_dc2_with_augmentation_buys_30_attempts_for_10_batches_not_50(monkeypatch) -> None:
    # 19-run BPCs at p 0.204: 1 BPC = ceil(1 / 0.204) = 5 whole attempts.
    # 10 batches x 10 runs = 100 runs = ceil(100 / 19) = 6 BPCs = 30 attempts
    # (per-batch it was 10 x 5 = 50).
    row = _dc2_row(monkeypatch, [_AUGMENTATION])
    mj = row["manufacturing_job"]
    assert mj["invention_decryptor"]["type_id"] == 34203
    plan = mj["invention_bpc_plan"]
    assert (plan["batch_runs"], plan["runs_per_invented_blueprint"], plan["whole_attempts_per_blueprint"]) == (10, 19, 5)
    assert (plan["owned_blueprint_runs"], plan["batch_one_invented_blueprints"]) == (0, 1)

    row["max_batches_total"] = 10
    for type_id in (20416, 20415):
        line = _shopping_line(row, type_id)
        assert line is not None and (line["need"], line["buy"]) == (60, 60)  # 2 x 30 attempts
    line = _shopping_line(row, 34203)
    assert line is not None and (line["need"], line["buy"]) == (30, 30)


def test_dc2_without_a_decryptor_buys_what_it_did_before(monkeypatch) -> None:
    # 10-run BPCs at p 0.34: 1 BPC per batch, 3 attempts: 10 x 3 = 30 attempts.
    row = _dc2_row(monkeypatch, [])
    row["max_batches_total"] = 10
    for type_id in (20416, 20415):
        line = _shopping_line(row, type_id)
        assert line is not None and (line["need"], line["buy"]) == (60, 60)
    assert row["manufacturing_job"]["invention_procurement_materials_per_extra_batch"]["20416"]["quantity"] == 6


def test_one_batch_buys_only_batch_ones_attempts(monkeypatch) -> None:
    row = _dc2_row(monkeypatch, [_AUGMENTATION])
    row["max_batches_total"] = 1
    line = _shopping_line(row, 34203)
    assert line is not None and (line["need"], line["buy"]) == (5, 5)


# --- round 5 Task 10 T9: three invented levels ---------------------------------


def _three_level_invention_rows() -> list[dict]:
    """5002 (T2) <- 1 x 5102 (T2) <- 1 x 5202 (T2) <- 10 Tritanium per run.
    Each T2 blueprint is invented from its own T1 source with its own
    datacore (204, 205, 206): p 0.5, 10 runs per BPC, 2 per attempt."""
    def source(bp, product, invented_bp, datacore):
        return {
            "blueprint_type_id": bp,
            "blueprint": {"type_id": bp, "type_name": f"Source {bp}", "base_price": 1000.0},
            "manufacturing_job": {
                "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 3, "category_name": "Material"}],
                "skill_entries": [], "time_seconds": 50, "max_production_limit": 10,
                "products": [{"type_id": product, "type_name": f"Source item {product}", "quantity": 1,
                              "meta_group_name": "Tech I"}],
            },
            "copying_job": {"time_seconds": 20},
            "invention_job": {
                "materials": [{"type_id": datacore, "type_name": f"Datacore {datacore}", "quantity": 2,
                               "category_name": "Material"}],
                "skill_entries": [], "time_seconds": 40,
                "products": [{"probability_pct": 50.0, "quantity": 10,
                              "product": {"type_id": invented_bp, "type_name": f"T2 Blueprint {invented_bp}"}}],
            },
        }

    def t2(bp, product, material, quantity):
        return {
            "blueprint_type_id": bp,
            "blueprint": {"type_id": bp, "type_name": f"T2 Blueprint {bp}", "base_price": 5000.0},
            "manufacturing_job": {
                "materials": [{"type_id": material, "type_name": f"Input {material}", "quantity": quantity,
                               "category_name": "Material"}],
                "skill_entries": [], "time_seconds": 100, "max_production_limit": 10,
                "products": [{"type_id": product, "type_name": f"T2 item {product}", "quantity": 1,
                              "meta_group_name": "Tech II"}],
            },
        }

    return [
        source(1001, 6001, 9002, 204), source(1101, 6101, 9102, 205), source(1201, 6201, 9202, 206),
        t2(9002, 5002, 5102, 1), t2(9102, 5102, 5202, 1), t2(9202, 5202, 34, 10),
    ]


_THREE_LEVEL_PRICES = {34: 5.0, 204: 100.0, 205: 100.0, 206: 100.0, 5102: 100_000.0, 5202: 100_000.0}


def test_three_invented_levels_buy_every_levels_datacores_for_every_batch(monkeypatch) -> None:
    # The deepest component runs on an owned 10-run 9202 BPC in batch 1; each
    # level invents 1 run otherwise: 1 BPC, 2 attempts, 4 datacores.
    deep_bpc = SimpleNamespace(
        type_id=9202, item_id=99, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    row = _invention_overview(
        monkeypatch, blueprint_rows=_three_level_invention_rows(), prices=_THREE_LEVEL_PRICES,
        owned_assets=([deep_bpc], [], {}, {}, {}),
    )[5002]
    mj = row["manufacturing_job"]

    batch_one = mj["invention_procurement_materials"]
    assert (batch_one["204"]["quantity"], batch_one["205"]["quantity"]) == (4, 4)
    assert "206" not in batch_one                       # the owned BPC covers batch 1
    # Batch 1 spent an owned run, so batches 2..N follow the extra-batch plan
    # (Task 5), which invents every level again.
    assert "invention_procurement_materials_per_extra_batch" not in mj
    extra = _later_plan(mj, 2)["invention_procurement_materials"]
    assert (extra["204"]["quantity"], extra["205"]["quantity"], extra["206"]["quantity"]) == (4, 4, 4)

    # Every level carries its spare BPC runs across batches (I3): BPCs over
    # all batches = ceil(runs past what is owned or left / 10 runs per BPC),
    # 2 whole attempts x 2 datacores = 4 per BPC.
    def needs(batches: int) -> dict[int, int | None]:
        row["max_batches_total"] = batches
        return {t: (_shopping_line(row, t) or {}).get("need") for t in (204, 205, 206)}

    # 3 batches: one invented 10-run BPC each covers all 3 runs of 204 and
    # 205; 206: batch 1 runs on the owned BPC, whose 9 runs left cover 2-3.
    assert needs(3) == {204: 4, 205: 4, 206: None}
    # 11 batches: 2 BPCs each for 204/205; 206: 10 later runs - 9 owned = 1.
    assert needs(11) == {204: 8, 205: 8, 206: 4}
    # 21 batches: 3 BPCs each; 206: 20 - 9 = 11 runs, 2 BPCs.
    assert needs(21) == {204: 12, 205: 12, 206: 8}


def test_three_invented_levels_without_owned_bpcs_carry_every_levels_spare_runs(monkeypatch) -> None:
    row = _invention_overview(
        monkeypatch, blueprint_rows=_three_level_invention_rows(), prices=_THREE_LEVEL_PRICES,
    )[5002]
    for batches, per_level in ((1, 4), (3, 4), (10, 4), (11, 8), (21, 12)):
        row["max_batches_total"] = batches
        assert {t: (_shopping_line(row, t) or {}).get("need") for t in (204, 205, 206)} == (
            {204: per_level, 205: per_level, 206: per_level}
        ), batches


def test_owned_nested_spare_runs_are_not_credited_when_another_row_may_use_them(monkeypatch) -> None:
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    deep_bpc = SimpleNamespace(
        type_id=9202, item_id=99, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    row = _invention_overview(
        monkeypatch, blueprint_rows=_three_level_invention_rows(), prices=_THREE_LEVEL_PRICES,
        owned_assets=([deep_bpc], [], {}, {}, {}),
    )[5002]
    row["max_batches_total"] = 3
    # Another selected row that builds from blueprint 9202 may spend the same
    # 9 runs left: they are not credited (no under-buy).
    other = {"type_id": 5202, "type_name": "Other", "max_batches_total": 1, "manufacturing_job": {
        "later_batch_plan_version": LATER_BATCH_PLAN_VERSION, "blueprint_sde": {"blueprint_type_id": 9202},
        "nested_invention_bpc_plans": [], "nested_blueprint_runs_left": {},
        "procurement_materials": {}, "blueprint_source_kind": "owned_blueprint_copy",
    }}
    line = next(m for m in aggregate_shopping_list([row, other]) if m["type_id"] == 206)
    assert line["need"] == 4   # batches 2-3: 2 runs, one invented BPC


# --- round 5 Task 10 carry 10: the EIV's material count ------------------------


def test_the_manufacturing_eiv_publishes_how_many_materials_it_covers(monkeypatch) -> None:
    # Tritanium has no adjusted price: EIV unknown, 0 of its 1 material priced.
    unpriced = _invention_overview(monkeypatch, prices={204: 100.0})[5002]["manufacturing_job"]
    mfg = unpriced["activity_breakdown"]["manufacturing"]
    assert mfg["estimated_item_value"] is None
    assert (mfg["estimated_item_value_priced_material_count"], mfg["estimated_item_value_material_count"]) == (0, 1)

    priced = _invention_overview(monkeypatch)[5002]["manufacturing_job"]["activity_breakdown"]["manufacturing"]
    assert priced["estimated_item_value"] is not None
    assert (priced["estimated_item_value_priced_material_count"], priced["estimated_item_value_material_count"]) == (1, 1)


def test_each_owned_bpc_job_names_its_bpc(monkeypatch) -> None:
    # Task 10 fix round 1, I1: the daily planner leaves out a BPC in a job.
    mj = _invention_overview(
        monkeypatch, maximize_bp_runs=True, owned_assets=([_owned_t2_bpc(runs=4, item_id=81)], [], {}, {}, {}),
    )[5002]["manufacturing_job"]
    owned = [s for s in mj["job_segments"] if s["source"] == "owned_blueprint_copy"]
    assert [(s["runs"], s["blueprint_item_id"]) for s in owned] == [(4, 81)]
    assert all("blueprint_item_id" not in s for s in mj["job_segments"] if s["source"] != "owned_blueprint_copy")


def test_the_eiv_line_count_uses_the_sums_own_filter() -> None:
    # Fix round 1, minor 4: one definition of which lines the EIV covers.
    entries = [{"type_id": 34, "quantity": 2}, {"type_id": 35, "quantity": 1}, {"type_id": 0}, "x"]
    total, priced, lines = IndustryService._sum_estimated_item_value_counted(
        cast(Any, entries), quantity_key="quantity", adjusted_price_map={34: {"adjusted_price": 5.0}},
    )
    assert (total, priced, lines) == (None, 1, 2)
    assert IndustryService._sum_estimated_item_value(
        cast(Any, entries), quantity_key="quantity", adjusted_price_map={34: {"adjusted_price": 5.0}},
    ) == (None, 1)


# --- Task 10 fix round 2, item 1: BPCs in active jobs are not owned runs --------


def test_a_busy_best_me_bpc_leaves_batch_one_to_the_free_bpc(monkeypatch) -> None:
    # BPC 55 (ME 4, sorted first) is in a job; the free BPC 56 (ME 2) builds.
    mj = _invention_overview(
        monkeypatch, maximize_bp_runs=True,
        owned_assets=([_owned_t2_bpc_me(runs=10, me=4, item_id=55), _owned_t2_bpc_me(runs=10, me=2, item_id=56)],
                      [], {}, {}, {}),
        busy_blueprint_item_ids={55},
    )[5002]["manufacturing_job"]
    owned = [s for s in mj["job_segments"] if s["source"] == "owned_blueprint_copy"]
    assert [(s["blueprint_item_id"], s["runs"], s["blueprint_material_efficiency"]) for s in owned] == [(56, 10, 2)]
    assert mj["materials"]["34"]["quantity"] == 98   # ceil(100 x 0.98) at BPC 56's ME 2


def test_a_busy_nested_bpc_gives_no_spare_run_credit(monkeypatch) -> None:
    deep_bpc = SimpleNamespace(
        type_id=9202, item_id=99, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    row = _invention_overview(
        monkeypatch, blueprint_rows=_three_level_invention_rows(), prices=_THREE_LEVEL_PRICES,
        owned_assets=([deep_bpc], [], {}, {}, {}), busy_blueprint_item_ids={99},
    )[5002]
    mj = row["manufacturing_job"]
    assert int(mj["nested_blueprint_runs_left"].get("9202", 0)) == 0
    assert mj["invention_procurement_materials"]["206"]["quantity"] == 4   # batch 1 invents 9202
    # As without the BPC: 1 invented BPC per 10 runs at every level.
    for batches, per_level in ((3, 4), (11, 8)):
        row["max_batches_total"] = batches
        assert (_shopping_line(row, 206) or {}).get("need") == per_level, batches


# --- Task 10 fix round 2, item 4: a nested BPC shared by two selected rows -------


def _two_parents_sharing_a_component_rows() -> list[dict]:
    import copy

    rows = _nested_invention_blueprint_rows()
    second = copy.deepcopy(rows[-1])
    second["blueprint_type_id"] = 9200
    second["blueprint"] = {**second["blueprint"], "type_id": 9200}
    second["manufacturing_job"]["products"][0] = {
        **second["manufacturing_job"]["products"][0], "type_id": 7200, "type_name": "Parent 2",
    }
    rows.append(second)
    return rows


def test_two_rows_spending_the_last_run_of_one_nested_bpc_buy_the_shortfall(monkeypatch) -> None:
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    # One owned 1-run 9002 BPC: each parent's batch 1 is planned on it, so
    # neither row buys datacores. Selected together they need 2 runs: the
    # second is invented (1 BPC, 2 whole attempts x 2 datacores = 4).
    rows = _invention_overview(
        monkeypatch, blueprint_rows=_two_parents_sharing_a_component_rows(), prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=1)], [], {}, {}, {}),
    )
    a, b = rows[7100], rows[7200]
    for row in (a, b):
        assert row["manufacturing_job"]["nested_blueprint_runs_used"] == {"9002": 1}
        assert row["manufacturing_job"]["nested_blueprint_runs_owned"] == {"9002": 1}
    assert not any(m["type_id"] == 204 for m in aggregate_shopping_list([a]))
    both = {m["type_id"]: m["need"] for m in aggregate_shopping_list([a, b])}
    assert both[204] == 4


def test_a_nested_bpc_with_runs_for_both_rows_buys_nothing_extra(monkeypatch) -> None:
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    rows = _invention_overview(
        monkeypatch, blueprint_rows=_two_parents_sharing_a_component_rows(), prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=2)], [], {}, {}, {}),
    )
    assert not any(m["type_id"] == 204 for m in aggregate_shopping_list([rows[7100], rows[7200]]))


def test_a_refused_older_row_on_the_same_blueprint_also_stops_the_spare_run_credit(monkeypatch) -> None:
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    deep_bpc = SimpleNamespace(
        type_id=9202, item_id=99, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    row = _invention_overview(
        monkeypatch, blueprint_rows=_three_level_invention_rows(), prices=_THREE_LEVEL_PRICES,
        owned_assets=([deep_bpc], [], {}, {}, {}),
    )[5002]
    row["max_batches_total"] = 3
    older = {"type_id": 8, "type_name": "Old", "max_batches_total": 1, "manufacturing_job": {
        "blueprint_sde": {"blueprint_type_id": 9202}, "procurement_materials": {},
        "blueprint_source_kind": "owned_blueprint_copy",
    }}
    line = next(m for m in aggregate_shopping_list([row, older]) if m["type_id"] == 206)
    assert line["need"] == 4


# --- Task 10 fix round 3: shared BPC runs netted exactly across rows and batches --


def _parents_sharing_rows(parents: int) -> list[dict]:
    import copy

    rows = _nested_invention_blueprint_rows()
    base = rows[-1]
    for i in range(1, parents):
        extra = copy.deepcopy(base)
        extra["blueprint_type_id"] = 9100 + 100 * i
        extra["blueprint"] = {**extra["blueprint"], "type_id": extra["blueprint_type_id"]}
        extra["manufacturing_job"]["products"][0] = {
            **extra["manufacturing_job"]["products"][0], "type_id": 7100 + 100 * i, "type_name": f"Parent {i + 1}",
        }
        rows.append(extra)
    return rows


def _datacore_need(rows: list[dict]) -> int | None:
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list

    return next((m["need"] for m in aggregate_shopping_list(rows) if m["type_id"] == 204), None)


def test_a_top_level_user_of_a_shared_bpc_is_netted(monkeypatch) -> None:
    # Probe E: the component row 5002 builds top-level on the owned 1-run BPC
    # 81, and the parent 7100 builds its nested 5002 on the same run. One of
    # the two runs is invented: 1 BPC, 2 whole attempts x 2 = 4 datacores.
    rows = _invention_overview(
        monkeypatch, blueprint_rows=_parents_sharing_rows(1), prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=1)], [], {}, {}, {}),
    )
    component, parent = rows[5002], rows[7100]
    assert component["manufacturing_job"]["nested_blueprint_runs_used"] == {"9002": 1}
    assert component["manufacturing_job"]["nested_blueprint_runs_owned"] == {"9002": 1}
    assert _datacore_need([component]) is None and _datacore_need([parent]) is None
    assert _datacore_need([component, parent]) == 4


@pytest.mark.parametrize("batches_a, batches_b, owned_runs, expected", [
    # Runs needed over both rows and all batches - owned, ceil(/ 10) BPCs x 4.
    (1, 1, 1, 4),     # 2 - 1 = 1 run: 1 BPC
    (2, 1, 1, 4),     # 3 - 1 = 2 (was 8)
    (2, 2, 1, 4),     # 4 - 1 = 3 (was 12)
    (6, 6, 1, 8),     # 12 - 1 = 11: 2 BPCs
    (11, 1, 1, 8),    # 12 - 1 = 11
    (1, 1, 2, None),  # 2 - 2 = 0
    (6, 6, 2, 4),     # 12 - 2 = 10: 1 BPC
])
def test_shared_nested_bpc_runs_are_netted_once_over_rows_and_batches(
    monkeypatch, batches_a, batches_b, owned_runs, expected,
) -> None:
    rows = _invention_overview(
        monkeypatch, blueprint_rows=_parents_sharing_rows(2), prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=owned_runs)], [], {}, {}, {}),
    )
    a, b = rows[7100], rows[7200]
    a["max_batches_total"], b["max_batches_total"] = batches_a, batches_b
    assert _datacore_need([a, b]) == expected


def test_the_same_row_selected_twice_counts_once(monkeypatch) -> None:
    rows = _invention_overview(
        monkeypatch, blueprint_rows=_parents_sharing_rows(1), prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=1)], [], {}, {}, {}),
    )
    parent = rows[7100]
    assert _datacore_need([parent, parent]) == _datacore_need([parent])


def test_a_null_job_status_counts_as_busy(session_provider) -> None:
    from eve_online_industry_tracker.infrastructure.models import CorporationIndustryJobsModel

    session = session_provider.app_session()
    try:
        for job_id, item_id, status in ((1, 501, "active"), (2, 502, None), (3, 503, "delivered")):
            session.add(CorporationIndustryJobsModel(
                corporation_id=1, job_id=job_id, blueprint_item_id=item_id, status=status,
            ))
        session.commit()
    finally:
        session.close()
    service = object.__new__(IndustryService)
    service._sessions = session_provider  # type: ignore[attr-defined]
    assert service._active_job_blueprint_item_ids() == {501, 502}
