"""The planner side of unknown job fees and qualifying
inventors, plus the review's minors. Fake pilots, prices and rows only."""
from __future__ import annotations

import time

import pytest

import test_daily_planner_assigned_inventor as a4
import test_daily_planner_decisions as dec
import test_daily_planner_fail_loud as fl
import industry_service_fakes as h
import test_industry_unknown_fees_and_inventors as r0
from eve_online_industry_tracker.application.daily_planner import character_assigner as ca
from eve_online_industry_tracker.application.daily_planner.item_decision_engine import ItemDecisionEngine
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.industry.portfolio_service import (
    PortfolioCandidateScope,
    PortfolioPlanRequest,
    IndustryPortfolioService,
)
from eve_online_industry_tracker.application.industry.service import IndustryService
from planner_skill_fakes import NO_SKILL_REQUIREMENTS
from fakes import _fresh_once_warnings  # noqa: F401  (autouse fixture)

_F2 = 1 + 6 / 30 + 3 / 40  # pilot "Qual" 3/3/3: 0.30 x 1.275 = 0.3825
_UPPER = "profit is an upper bound: job fees unknown for: invention (x)"


def _pilot(cid, name, a, b, e):
    skills = [a4._skill("Science A", a, a4.SCIENCE_A), a4._skill("Science B", b, a4.SCIENCE_B)]
    if e:
        skills.append(a4._skill("Encryption", e, a4.ENCRYPTION))
    return {"character_id": cid, "character_name": name, "skills": {"skills": skills}}


_NO_ENC, _QUAL = _pilot(1, "NoEnc", 5, 5, 0), _pilot(2, "Qual", 3, 3, 3)


# --- Only a pilot who can run the invention invents ----------------------------------

def test_probe_g_the_invent_goes_to_the_qualifying_pilot():
    row = a4._row(inventor=2, best_factor=_F2, decryptor=False, multiplier=1.0)
    (invent,) = [a for a in a4._assign(row, [_NO_ENC, _QUAL]) if a.action_type == "invent"]
    assert invent.character_id == 2
    assert invent.invention_probability == pytest.approx(0.3825)
    assert invent.runs == 6  # ceil(2 / 0.3825); the unqualified pilot's 0.40 bought 5
    assert "(best inventor)" in invent.notes


def test_probe_g_qualifying_pilot_busy_means_no_invent_and_a_reason():
    row = a4._row(inventor=2, best_factor=_F2, decryptor=False, multiplier=1.0)
    plan = a4._plan(row)
    actions = a4.CharacterAssigner().assign(plan, [a4._busy(2)], a4._Chars([_NO_ENC, _QUAL]), a4._Admin(),
                                            price_map=a4.PRICES, blueprint_data=NO_SKILL_REQUIREMENTS)
    assert [a for a in actions if a.action_type == "invent"] == []
    assert "no invention today: no free pilot with the invention skills" in plan.decisions[0].decision_reason


def test_g_case_a_bpc_plan_the_assigner_picks_the_qualifying_pilot():
    row = a4._row(inventor=2, best_factor=_F2, decryptor=False, multiplier=1.0, plan="bpc_plan")
    (invent,) = [a for a in a4._assign(row, [_NO_ENC, _QUAL]) if a.action_type == "invent"]
    assert invent.character_id == 2
    assert invent.invention_probability == pytest.approx(0.3825)
    assert invent.invention_expected_attempts == pytest.approx(20 / 10 / 0.3825)
    assert invent.runs == 6


# --- A lower-bound cost is an upper-bound profit, everywhere -------------------------

def test_the_producer_flags_the_profit_as_an_upper_bound(monkeypatch):
    _service, row = r0._probe_f_row(monkeypatch)
    assert row["profit_is_upper_bound"] is True
    assert row["manufacturing_job"]["profit_is_upper_bound"] is True
    assert orow.get_profit_upper_bound_reason(row) == (
        f"profit is an upper bound: job fees unknown for: {r0._F_INVENTION_REASON}; {r0._F_COPY_REASON}"
    )
    assert orow.get_profit_upper_bound_reason({"manufacturing_job": {}}) is None


def test_a_priced_row_profit_is_exact(monkeypatch):
    rows = h._rows_with(probability_pct=30.0, bpc_runs=10, t2_limit=10)
    rows[1]["manufacturing_job"]["products"][0]["category_name"] = "Module"
    row = r0._t2(r0._overview(r0._service(monkeypatch, profile=r0.PROFILE, rows=rows)))
    assert row["profit_is_upper_bound"] is False


def _flagged_mfg_row():
    return {"type_id": 12345, "quantity": 200,
            "manufacturing_job": {"runs": 20, "material_cost": 50_000_000.0, "total_cost": 55_000_000.0,
                                  "job_fee_unknown_reasons": ["invention (x)"], "profit_is_upper_bound": True}}


def test_a_flagged_row_gives_feedback_no_build_cost_and_says_why():
    (action,) = fl._assign(_flagged_mfg_row())
    assert action.estimated_build_cost_isk is None
    assert _UPPER in action.notes


def test_feedback_skips_the_cost_update_on_a_none_build_cost():
    from eve_online_industry_tracker.application.daily_planner.feedback_processor import _per_unit

    assert _per_unit(None, 200) is None  # FeedbackProcessor then keeps the old cost_multiplier


def test_the_decision_reason_names_the_upper_bound():
    row = {**dec._make_overview_row(), "manufacturing_job": {"job_fee_unknown_reasons": ["invention (x)"],
                                                             "profit_is_upper_bound": True}}
    decision = ItemDecisionEngine().decide(
        scored=dec._make_scored(), pipeline=dec._make_pipeline(), overview_row=row, admin_settings=dec._make_admin(),
    )
    assert decision.decision.startswith("build")
    assert decision.decision_reason.endswith(f"; {_UPPER}")
    plain = ItemDecisionEngine().decide(
        scored=dec._make_scored(), pipeline=dec._make_pipeline(), overview_row=dec._make_overview_row(),
        admin_settings=dec._make_admin(),
    )
    assert "upper bound" not in plain.decision_reason


def test_portfolio_warnings_name_the_upper_bound():
    from streamlit_ui.pages import portfolio_planner as page

    row = {"manufacturing_job": {"job_fee_unknown_reasons": ["invention (x)"], "profit_is_upper_bound": True}}
    assert _UPPER in page._warnings(row)
    assert not any("upper bound" in w for w in page._warnings({"manufacturing_job": {}}))


def test_the_inventory_priority_rank_says_it_ranks_an_upper_bound(monkeypatch):
    service = r0._service(monkeypatch)
    rows = [
        {"isk_per_hour": 10.0, "profit_is_upper_bound": True,
         "manufacturing_job": {"procurement_materials": {"34": {"type_id": 34, "sourcing_strategy": "take"}}}},
        {"isk_per_hour": 5.0, "profit_is_upper_bound": False,
         "manufacturing_job": {"procurement_materials": {"34": {"type_id": 34, "sourcing_strategy": "take"}}}},
    ]
    service._score_inventory_allocation_priority(rows)
    assert rows[0]["inventory_priority_uses_upper_bound"] is True
    assert rows[1]["inventory_priority_uses_upper_bound"] is False


# --- A sub-build with unknown fees is bought, with the reason -------------------------

def test_a_sub_build_with_unknown_fees_is_bought_with_the_reason(monkeypatch):
    rows = h._nested_invention_blueprint_rows(probability_pct=30.0)
    service = r0._service(monkeypatch, profile=r0._F_PROFILE, rows=rows,
                          prices={34: 5.0, 204: 100.0, 5002: 10_000.0, 7100: 50_000.0})
    parent = [r for r in r0._overview(service) if r["type_id"] == 7100][0]
    mj = parent["manufacturing_job"]
    assert mj["recursive_activity_breakdown"].get("manufacturing:5002") is None

    def nodes(node):
        yield node
        for child in node.get("children") or []:
            yield from nodes(child)

    (bought,) = [n for n in nodes(mj["job_tree"]) if n.get("type_id") == 5002 and n.get("node_type") == "material"]
    assert bought["recommendation_action"] == "buy"
    assert bought["build_cost_unknown_reason"] == (
        f"build cost unknown: job fees unknown for: {r0._F_INVENTION_REASON}; {r0._F_COPY_REASON}; bought"
    )


def test_estimated_total_cost_is_unknown_with_unknown_fees():
    plan = {"job_fee_unknown_reasons": ["copying (x)"], "estimated_total_cost": None}
    assert IndustryService._build_total_cost_or_none(plan) is None
    assert IndustryService._build_cost_unknown_reason(plan) == (
        "build cost unknown: job fees unknown for: copying (x); bought")
    assert IndustryService._build_cost_unknown_reason({"invention_cost_unknown_reasons": ["no odds"]}) == (
        "build cost unknown: invention cost unknown (no odds); bought")
    assert IndustryService._build_cost_unknown_reason({"estimated_total_cost": 5.0}) is None


# --- Unknown invention cost caps the shown confidence ---------------------------------

def _good_market(row):
    row.update({
        "market_unit_price": 2000.0, "market_price_sample_size": 10, "market_price_fetched_at": time.time(),
        "hub_sell_liquidity": 1000, "hub_sell_order_count": 50, "region_daily_volume": 100,
        "region_daily_volume_7d_avg": 100.0, "liquidity_indicator": "High",
    })


def test_an_unknown_invention_cost_caps_the_shown_confidence_at_low(monkeypatch):
    service, row = r0._probe_f_row(monkeypatch)
    _good_market(row)
    mj = row["manufacturing_job"]
    del mj["job_fee_unknown_reasons"]
    mj["invention_cost_unknown_reason"] = "no SDE success probability"
    service._enrich_product_rows_with_pricing_confidence([row], product_price_side="sell")
    assert row["pricing_confidence"] == "Low" == mj["pricing_confidence"]


def test_net_proceeds_unknown_is_a_confidence_reason(monkeypatch):
    service, row = r0._probe_f_row(monkeypatch)
    row["manufacturing_job"]["net_proceeds_unknown_reason"] = IndustryService._BROKER_FEE_UNKNOWN_REASON
    service._enrich_product_rows_with_pricing_confidence([row], product_price_side="sell")
    assert f"Net proceeds unknown: {IndustryService._BROKER_FEE_UNKNOWN_REASON}." in row["pricing_confidence_reasons"]


# --- Portfolio says when the confidence cap emptied it --------------------------------

def _candidate(row_id, confidence, capped):
    return {"overview_row_id": row_id, "type_id": 1, "type_name": "Fake", "pricing_confidence": confidence,
            "profit_is_upper_bound": capped, "is_portfolio_candidate": True, "profit_amount": 10.0,
            "profit_margin_fraction": 0.5, "isk_per_hour": 10.0, "region_daily_volume": 10}


def test_portfolio_hint_when_a_high_filter_drops_capped_rows():
    request = PortfolioPlanRequest(minimum_pricing_confidence="high", candidate_scope=PortfolioCandidateScope())
    out = IndustryPortfolioService.optimize_manufacturing_portfolio(
        candidates=[_candidate("a", "Medium", True), _candidate("b", "Medium", True), _candidate("c", "Low", False)],
        plan_request=request,
    )
    assert out["selected_count"] == 0
    assert out["confidence_capped_hint"] == "2 rows capped: job fees unknown"
    reasons = {s["overview_row_id"]: s["reason"] for s in out["skipped_items"]}
    assert reasons["a"] == "Below minimum pricing confidence (capped: job fees unknown)"
    assert reasons["c"] == "Below minimum pricing confidence"


def test_no_portfolio_hint_without_capped_rows():
    request = PortfolioPlanRequest(minimum_pricing_confidence="high", candidate_scope=PortfolioCandidateScope())
    out = IndustryPortfolioService.optimize_manufacturing_portfolio(
        candidates=[_candidate("c", "Low", False)], plan_request=request,
    )
    assert out["confidence_capped_hint"] is None


# --- The grid's Total Cost shows the lower bound -------------------------------------

def test_the_grid_total_cost_formatter_prefixes_a_lower_bound():
    from streamlit_ui.components.aggrid_formatters import js_eu_isk_formatter
    from streamlit_ui.state import industry_builder_ui as ui

    js = js_eu_isk_formatter(JsCode=lambda s: s, locale="en-US", decimals=2, lower_bound_field="_total_cost_lower_bound")
    assert "params.data._total_cost_lower_bound" in js and "'≥ '" in js
    assert "≥" not in js_eu_isk_formatter(JsCode=lambda s: s, locale="en-US", decimals=2)
    row = {"overview_row_id": "r1", "type_id": 5002, "type_name": "T2", "pricing_confidence": "Medium",
           "manufacturing_job": {"total_cost": 10.0, "total_cost_excludes_unknown_job_fees": True,
                                 "job_tree": {"node_type": "product", "activity": "product", "type_id": 5002,
                                              "children": []}}}
    (flat,) = ui.flatten_overview_job_tree_rows.__wrapped__([row])
    assert flat["Total Cost"] == 10.0 and flat["_total_cost_lower_bound"] is True


# --- The aggregate rig reason per metric ---------------------------------------------

@pytest.mark.parametrize("fields, text", [
    ({"structure_rig_time_bonus": 0.2}, "time"),
    ({"structure_rig_material_bonus": 0.05, "structure_rig_cost_bonus": 0.01}, "material, cost"),
])
def test_the_aggregate_rig_reason_names_its_metrics(fields, text):
    reason = IndustryService._aggregate_only_rig_reason({"structure_rigs": [], **fields})
    assert reason == (
        f"rig bonus unknown: profile has no rig ids, only the aggregate structure rig {text} bonus, "
        f"so no rig bonus applies to manufacturing (re-save the profile with its rigs)"
    )


def test_a_time_only_aggregate_does_not_claim_material_overstated(monkeypatch):
    profile = {**r0.PROFILE, "structure_rigs": [], "structure_rig_time_bonus": 0.2}
    service = r0._service(monkeypatch, profile=profile, real_confidence=True)
    row = [r for r in r0._overview(service) if r["type_id"] == 6001][0]
    (reason,) = [r for r in row["pricing_confidence_reasons"] if r.startswith("Rig bonus unknown")]
    assert "build time is overstated" in reason
    assert "material" not in reason.split(" — ")[1]


# --- has_work False, research labels, fees="none" end to end ------------------------

def test_an_unknown_fee_on_a_job_with_no_work_is_not_flagged():
    unknown = {"total_job_cost": None, "fee_unknown_reason": "x", "base_duration_seconds": 0}
    assert IndustryService._job_fee_unknown_entries(("copying", unknown, None)) == []
    assert IndustryService._job_fee_unknown_entries(("copying", {**unknown, "base_duration_seconds": 5}, False)) == []
    assert IndustryService._job_fee_unknown_entries(("copying", {**unknown, "base_duration_seconds": 5}, None)) == [
        "copying (x)"]


def test_the_research_chain_fees_carry_the_me_te_labels(monkeypatch):
    rows = h._rows_with(probability_pct=30.0, bpc_runs=10, t2_limit=10)
    rows[0]["research_material_job"] = {"time_seconds": 210}
    rows[0]["research_time_job"] = {"time_seconds": 210}
    service = r0._service(monkeypatch, profile=r0._F_PROFILE, rows=rows)
    # build_from_bpc off: the SDE-fallback row plans the research chain.
    out = service.industry_manufacturing_product_overview(
        build_from_bpc=False, have_blueprint_source_only=False, maximize_bp_runs=True, character_id=1)
    row = [r for r in out if r["type_id"] == 6001][0]
    assert row["manufacturing_job"]["blueprint_source_kind"] == "blueprint_sde_fallback"
    reasons = row["manufacturing_job"]["job_fee_unknown_reasons"]
    assert "ME research (the industry profile has no research_material cost index)" in reasons
    assert "TE research (the industry profile has no research_time cost index)" in reasons


def test_fees_none_and_known_end_to_end(monkeypatch):
    service, row = r0._probe_f_row(monkeypatch)
    ctx = h._planning_context(service, maximize_bp_runs=True)
    mj = row["manufacturing_job"]
    known, _ = service._row_total_cost_estimate(ctx, row, fees="known")
    none, _ = service._row_total_cost_estimate(ctx, row, fees="none")
    assert known == pytest.approx(none + mj["total_job_cost"])
    assert known == pytest.approx(mj["total_cost"])


def test_differing_fee_gaps_compare_without_fees(monkeypatch):
    service = r0._service(monkeypatch, options=[r0.PLUS_RUN])
    seen = []
    original = IndustryService._cycle_evaluation.__func__

    def spy(cls, ctx, option, row_out, *, fees="all"):
        seen.append(fees)
        return original(cls, ctx, option, row_out, fees=fees)

    monkeypatch.setattr(IndustryService, "_cycle_evaluation", classmethod(spy))
    monkeypatch.setattr(IndustryService, "_decryptor_fee_basis",
                        staticmethod(lambda rows: (False, "job fees unknown for some candidates (x)")))
    choice = r0._t2(r0._overview(service))["manufacturing_job"]["invention_decryptor"]
    assert seen and set(seen) == {"none"}
    assert choice["reason"].endswith("; job fees unknown for some candidates (x)")


# --- The once-only check-then-add is locked ------------------------------------------

def test_first_warning_is_thread_safe():
    import threading

    warned: set[str] = set()
    hits = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        hits.append(IndustryService._first_warning(warned, "k"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert hits.count(True) == 1
    assert isinstance(IndustryService._ONCE_WARNING_LOCK, type(threading.Lock()))




def _actions_with_jobs(row, chars, jobs):
    plan = a4._plan(row)
    actions = a4.CharacterAssigner().assign(plan, list(jobs), a4._Chars(chars), a4._Admin(), price_map=a4.PRICES, blueprint_data=NO_SKILL_REQUIREMENTS)
    return plan.decisions[0], actions


def test_b_no_free_skilled_inventor_and_no_free_slot_are_told_apart():
    row = a4._row(inventor=2, best_factor=_F2, decryptor=False, multiplier=1.0)
    decision, actions = _actions_with_jobs(row, [_NO_ENC, _QUAL], [a4._busy(2)])
    assert not [a for a in actions if a.action_type == "invent"]
    assert "no invention today: no free pilot with the invention skills" in decision.decision_reason
    assert ca.NO_FREE_SKILLED_INVENTOR_REASON == "no free pilot with the invention skills"

    row = a4._row(inventor=2, best_factor=_F2, decryptor=False, multiplier=1.0)
    decision, actions = _actions_with_jobs(row, [_NO_ENC, _QUAL], [a4._busy(1), a4._busy(2)])
    assert not [a for a in actions if a.action_type == "invent"]
    assert "no invention today: no free research slot today" in decision.decision_reason
    assert ca.NO_FREE_RESEARCH_SLOT_REASON == "no free research slot today"


def test_c_a_sub_component_with_an_unknown_fee_is_bought_with_the_reason():
    """The planner buys it, as the producer does
    (_build_cost_unknown_reason), and the parent names why."""
    import test_chain_planner_keying as ck

    materials = ck._batch_materials(fee=None, **{"54321": 200})
    materials["54321"]["component_job_fee_per_run_unknown_reason"] = "the industry profile has no manufacturing cost index"
    plan = ck._planner().plan_chain([ck._decision(ck._row(materials=materials))], ck._limited_sub_phase1(7))
    assert [d for d in plan.decisions if d.is_sub_component] == []
    (parent,) = plan.decisions
    assert parent.overview_row["component_buy_reasons"][54321] == (
        "component Widget bought: its install fee is unknown for Thing's structure "
        "(the industry profile has no manufacturing cost index)"
    )


def test_c_the_shopping_list_buys_the_whole_component_on_the_parent():
    import test_chain_planner_keying as ck
    from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder

    materials = ck._batch_materials(fee=None, **{"54321": 200})
    materials["54321"]["component_job_fee_per_run_unknown_reason"] = "no index"
    plan = ck._planner().plan_chain([ck._decision(ck._row(materials=materials))], ck._limited_sub_phase1(7))
    actions = [a for a in ca.CharacterAssigner().assign(plan, [], fl._mfg_pilot(), fl._AdminKeyError(), blueprint_data=NO_SKILL_REQUIREMENTS)
               if a.action_type in ("manufacture", "sub_manufacture")]
    assert [a.action_type for a in actions] == ["manufacture"]
    assert "component Widget bought: its install fee is unknown for Thing's structure (no index)" in actions[0].notes

    class _Resolver:
        def is_blueprint(self, type_id):
            return False

        def prefetch(self, type_ids):
            return None

    items = ShoppingListBuilder().build(
        assigned_actions=actions, corp_assets=[], market_depth_cache={54321: {"vwap_5d": 1.0}},
        admin_settings=a4._Admin(), blueprint_data={}, meta_resolver=_Resolver(),
    )
    assert {i.type_id: i.quantity for i in items}[54321] == 200


def test_a_the_decision_and_the_plan_item_carry_the_upper_bound_flag(app_session, session_provider):
    import test_daily_planner_repo as rp
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    row = {**dec._make_overview_row(), "manufacturing_job": {"job_fee_unknown_reasons": ["invention (x)"]}}
    decision = ItemDecisionEngine().decide(
        scored=dec._make_scored(), pipeline=dec._make_pipeline(), overview_row=row, admin_settings=dec._make_admin(),
    )
    assert decision.profit_is_upper_bound is True
    service = rp._service(session_provider)
    service._persist_plan_items(plan_id=1, decisions=[decision, rp._decision()], market_depth_cache={})
    flags = sorted(bool(i.isk_per_hour_is_upper_bound) for i in app_session.query(BuildPlanItemModel).all())
    assert flags == [False, True]


def test_a_feedback_skips_the_accuracy_update_on_an_upper_bound_isk_per_hour(caplog):
    from unittest.mock import MagicMock

    import test_daily_planner_feedback as fb
    from eve_online_industry_tracker.application.daily_planner.feedback_processor import FeedbackProcessor

    repo = MagicMock()
    processor = FeedbackProcessor(repo, fb._make_admin(alpha=0.2, slow_mover_timeout=60.0))
    processor._find_realized_sale = MagicMock(return_value={
        "isk_per_hour": 8_000_000.0, "sell_days": 4.0,
        "allocation_details": [{"source": "industry_build", "quantity": 10, "total_cost": 6_000_000.0}],
    })
    item = fb._make_plan_item(type_id=320)
    item.isk_per_hour_is_upper_bound = True
    repo.get_unprocessed_done_actions.return_value = [fb._make_action(type_id=320, quantity=100)]
    repo.get_weights.return_value = {320: fb._make_weights(accuracy_ema=1.3)}
    repo.get_plan_items.return_value = [item]
    with caplog.at_level("INFO"):
        assert processor.process_pending_feedback() == 1
    assert repo.record_feedback.call_args[0][2].accuracy_ema == 1.3
    assert repo.record_feedback.call_args[0][1].accuracy_ratio is None
    assert any("accuracy update skipped" in r.getMessage() and "upper bound" in r.getMessage()
               for r in caplog.records)
