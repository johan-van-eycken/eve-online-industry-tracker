# tests/test_blueprint_ledger.py
"""Round 5 Task 14: one plan-wide blueprint reservation.

EVE runs one job per blueprint. ChainPlanner keeps a single ledger of the
free BPOs (by item) and BPCs (by item, with their runs) and claims them in
plan order: research first, then top-level manufacture and copies (score
order), then invention on the BPCs left, then sub-builds on the component
BPOs left. A row that loses a claim is not built, or buys, with a reason.

One regression test per review probe (a, b, c, e, f, g) and the composed
probe d pinned. Fake ids and numbers throughout.
"""
from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

import test_daily_planner_assigned_inventor as A  # noqa: E402
import test_research_first as R  # noqa: E402

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder  # noqa: E402
from planner_skill_fakes import NO_SKILL_REQUIREMENTS


class _Adm:
    def get(self, section, key, default=None):
        raise KeyError(key)


class _NoBP:
    def is_blueprint(self, type_id):
        return False

    def prefetch(self, type_ids):
        return None


def _dec(row, type_id, name, score, meta_group, stage="manufacturing", decision="build"):
    return ItemDecision(
        type_id=type_id, type_name=name, decision=decision, decision_reason="test",
        adjusted_score=score, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=meta_group,
        pipeline_stage=stage, overview_row=row,
    )


def _bpo(item_id, type_id, *, me=10, te=20):
    return SimpleNamespace(item_id=item_id, type_id=type_id, is_blueprint_copy=False,
                           blueprint_material_efficiency=me, blueprint_time_efficiency=te)


def _bpc(item_id, type_id, runs):
    return SimpleNamespace(item_id=item_id, type_id=type_id, is_blueprint_copy=True, blueprint_runs=runs)


def _planner(me=10):
    planner = ChainPlanner(industry_service=None, session_provider=None, admin_settings=_Adm())
    planner._compute_optimal_me = lambda bp_data, runs=1: me
    return planner


def _shop(actions, market, blueprints):
    items = ShoppingListBuilder().build(
        assigned_actions=actions, corp_assets=[], market_depth_cache=market, admin_settings=A._Admin(),
        blueprint_data=blueprints, meta_resolver=_NoBP(),
    )
    return {i.type_id: i.quantity for i in items}


def _kinds(actions):
    return sorted((a.action_type, a.type_id) for a in actions)


# --- probe a: a component that is also its own top-level row ---------------------

def _widget_row():
    return {
        "type_id": 54321, "type_name": "Widget", "quantity": 100, "overview_row_id": "w",
        "isk_per_hour": 1.0, "profit_amount": 10.0, "profit_margin_fraction": 0.1,
        "manufacturing_job": {
            "runs": 10, "manufacturing_job_cost": 5.0, "material_cost": 20.0, "total_cost": 25.0,
            "materials": {"34": {"type_id": 34, "quantity": 20, "type_name": "Tritanium"}},
            "blueprint_sde": {"blueprint_type_id": 888},
            "blueprint_source_kind": "owned_blueprint_original",
            "job_segments": [{"runs": 10, "source": "owned_blueprint_original", "materials": {"34": 20}}],
            "research_job_cost_by_level": {"research_material": [0.0] * 11, "research_time": [0.0] * 11},
            "research_duration_seconds_by_level": {
                "research_material": R.SECONDS_TO_LEVEL, "research_time": R.SECONDS_TO_LEVEL,
            },
        },
    }


def _probe_a(component_te, *, widget_bpos=1, widget_decision="build"):
    parent = _dec(R._row(**R.BPO_ROW), 12345, "Thing", 2.0, 1)
    widget = _dec(_widget_row(), 54321, "Widget", 1.0, 1, decision=widget_decision)
    plan = _planner().plan_chain([parent, widget], {
        "bpo_assets_by_type_id": {
            999: [_bpo(1, 999)],
            888: [_bpo(2 + i, 888, te=component_te) for i in range(widget_bpos)],
        },
        "blueprint_data": R.BLUEPRINT_DATA, "market_depth_cache": R.MARKET, "variant_rows_by_type_id": {},
    })
    actions = CharacterAssigner().assign(plan, [], R._Chars(R._pilot(("Laboratory Operation", 3406, 5))), R._Admin(), blueprint_data=NO_SKILL_REQUIREMENTS)
    return plan, parent, actions


def test_probe_a_the_components_one_bpo_builds_its_own_row_and_the_parent_buys_the_component():
    plan, parent, actions = _probe_a(20)
    assert _kinds(actions) == [("manufacture", 12345), ("manufacture", 54321)]
    assert "component Widget bought: its BPO is used by manufacture of Widget (54321) today" in parent.decision_reason
    # The parent's full need is bought (never under-bought), plus the widget row's own inputs.
    assert _shop(actions, R.MARKET, R.BLUEPRINT_DATA) == {54321: 1000, 34: 20}
    assert sorted(d.type_id for d in plan.decisions) == [12345, 54321]


def test_probe_a_a_component_bpo_in_te_research_is_not_sub_built_from():
    plan, parent, actions = _probe_a(0)
    assert _kinds(actions) == [("manufacture", 12345), ("te_research", 888)]
    assert "component Widget bought: its BPO is used by TE research for Widget (54321) today" in parent.decision_reason
    assert _shop(actions, R.MARKET, R.BLUEPRINT_DATA) == {54321: 1000}
    assert sorted(d.type_id for d in plan.decisions) == [12345, 54321]


def test_a_second_free_component_bpo_still_gives_one_decision_per_type():
    """Two BPOs of 888: the top-level widget row takes one, the second is free,
    but the type is a top-level build row, so it is not sub-built too (one
    ItemDecision per type_id): the parent buys it."""
    plan, parent, actions = _probe_a(20, widget_bpos=2)
    assert sorted(d.type_id for d in plan.decisions) == [12345, 54321]
    assert "sub_manufacture" not in [a.action_type for a in actions]
    assert "component Widget bought: it is a top-level build row of this plan" in parent.decision_reason
    assert _shop(actions, R.MARKET, R.BLUEPRINT_DATA)[54321] == 1000


def test_a_component_whose_top_level_row_is_not_built_is_merged_into_one_sub_build_decision():
    """The widget's own row is 'watch' (not sold today): the sub-build replaces
    it, so the type still has one decision, which keeps the watch reason."""
    plan, parent, actions = _probe_a(20, widget_decision="watch")
    widgets = [d for d in plan.decisions if d.type_id == 54321]
    assert len(widgets) == 1 and widgets[0].is_sub_component
    assert "as a product: watch (test)" in widgets[0].decision_reason
    (sub,) = [a for a in actions if a.action_type == "sub_manufacture"]
    assert sub.quantity == 1000


# --- probe b: a T1 BPO shared by a T1 row and a T2 row's copy --------------------

BP_B = copy.deepcopy(A.BLUEPRINTS)
BP_B[998]["manufacturing"]["materials"] = [{"type_id": 34, "quantity": 5}]
BP_B[998]["max_production_limit"] = 30
BP_B[998]["type_name"] = "Widget I Blueprint"
BP_B[1999]["manufacturing"]["materials"] = [{"type_id": 34, "quantity": 7}]


def _t1_row(te=20):
    return {
        "type_id": 12344, "type_name": "Widget I", "quantity": 10, "overview_row_id": "t1",
        "isk_per_hour": 1.0, "profit_amount": 10.0, "profit_margin_fraction": 0.1,
        "manufacturing_job": {
            "runs": 10, "manufacturing_job_cost": 5.0, "material_cost": 50.0, "total_cost": 55.0,
            "materials": {"34": {"type_id": 34, "quantity": 50, "type_name": "Tritanium"}},
            "blueprint_sde": {"blueprint_type_id": 998},
            "blueprint_source_kind": "owned_blueprint_original",
            "job_segments": [{"runs": 10, "source": "owned_blueprint_original", "materials": {"34": 50}}],
            "research_job_cost_by_level": {"research_material": [0.0] * 11, "research_time": [0.0] * 11},
        },
    }


def _t2_row(type_id=12345):
    row = A._row()
    row["type_id"] = type_id
    row["manufacturing_job"]["blueprint_sde"] = {"blueprint_type_id": 1999}
    row.pop("needs_invention")
    return row


def _chars():
    return [A._best_inventor(), A._second_inventor(), A._copier()]


def _run(decisions, bpos, bpcs, chars=None, blueprints=BP_B):
    plan = _planner(0).plan_chain(decisions, {
        "bpo_assets_by_type_id": bpos, "bpc_assets_by_type_id": bpcs,
        "blueprint_data": blueprints, "market_depth_cache": A.PRICES, "variant_rows_by_type_id": {},
    })
    actions = CharacterAssigner().assign(plan, [], A._Chars(chars or _chars()), A._Admin(), price_map=A.PRICES, blueprint_data=NO_SKILL_REQUIREMENTS)
    return plan, actions


def test_probe_b_the_higher_scored_copy_takes_the_bpo_and_the_t1_row_is_not_built():
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    t1 = _dec(_t1_row(20), 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t2, t1], {998: [_bpo(77, 998)]}, {})
    assert [a.action_type for a in actions] == ["copy"]
    assert t1.pipeline_stage == "blocked"
    assert "not built: BPO used by copy for Widget II (12345) today" in t1.decision_reason


def test_probe_b_research_comes_first_so_no_copy_of_a_bpo_in_research():
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    t1 = _dec(_t1_row(0), 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t2, t1], {998: [_bpo(77, 998, te=0)]}, {})
    assert [(a.action_type, a.type_id) for a in actions] == [("te_research", 998)]
    assert t2.pipeline_stage == "blocked"
    assert "not built: T1 BPO (998) used by TE research for Widget I (12344) today" in t2.decision_reason
    assert "buy a T1 BPO" not in t2.decision_reason


# --- probe c: a T1 BPC used by the T1 row is not invented from too ----------------

def test_probe_c_a_bpc_the_t1_row_builds_on_is_not_used_by_invention():
    t1_row = _t1_row(20)
    mj = t1_row["manufacturing_job"]
    mj["blueprint_source_kind"] = "owned_blueprint_copy"
    mj["job_segments"] = [
        {"runs": 4, "source": "owned_blueprint_copy", "blueprint_item_id": 78, "materials": {"34": 20}},
        {"runs": 6, "source": "owned_blueprint_original", "materials": {"34": 30}},
    ]
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    t1 = _dec(t1_row, 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t2, t1], {998: [_bpo(77, 998)]}, {998: [_bpc(78, 998, 4)]})
    assert "invent" not in [a.action_type for a in actions]
    (copy_action,) = [a for a in actions if a.action_type == "copy"]
    (build,) = [a for a in actions if a.action_type == "manufacture"]
    # The BPO copies (higher score), so the T1 row builds its BPC runs only.
    assert (build.runs, build.materials) == (4, {34: 20})
    # Lanes of 3 + 1 attempts on one BPO: one job, 2 copies x 3 runs (each
    # lane gets its own BPC; same runs per copy in one job, so conservative).
    assert (copy_action.quantity, copy_action.runs) == (2, 6)
    assert "make 2 copies × 3 runs" in copy_action.notes


# --- I2: lanes from per-BPC runs --------------------------------------------------

def test_one_bpc_gives_one_invent_lane_of_at_most_its_runs():
    """The pinned Task 12 case, corrected: one 4-run BPC, two free inventors,
    two corp BPOs. One invention job per BPC, so ONE lane of 4 (the best
    inventor's whole attempts), not 3 + 1 from the same BPC; no copy."""
    row = A._row(t1_bpo=True)
    row["t1_bpo_count"], row["t1_bpc_runs_on_hand"] = 2, 4
    actions = A._assign(row, [A._best_inventor(), A._second_inventor()])
    assert [a for a in actions if a.action_type == "copy"] == []
    invents = [a for a in actions if a.action_type == "invent"]
    assert [(a.character_id, a.runs) for a in invents] == [(A.BEST_ID, 4)]


def test_two_bpcs_give_at_most_two_lanes_each_within_its_bpc_and_named():
    """BPCs of 1 and 3 runs, three free inventors: two lanes (one per BPC),
    the two best pilots, split 3 + 1. Best fit (M7): the 3-attempt lane
    takes the 3-run BPC, the 1-attempt lane the 1-run BPC."""
    row = A._row(t1_bpo=True)
    row["t1_bpo_count"] = 1
    row["t1_bpc_runs_by_item"] = [[81, 1], [82, 3]]
    row["t1_bpc_runs_on_hand"] = 4
    chars = [A._best_inventor(), A._second_inventor(), A._alt(5), A._copier()]
    invents = [a for a in A._assign(row, chars) if a.action_type == "invent"]
    assert sorted((a.blueprint_item_id, a.character_id, a.runs) for a in invents) == [
        (81, A.SECOND_ID, 1), (82, A.BEST_ID, 3),
    ]
    assert all(f"from T1 BPC item {a.blueprint_item_id} ({a.runs} run{'s' if a.runs != 1 else ''})" in a.notes
               for a in invents)


def test_the_smallest_bpc_with_enough_runs_is_used():
    """M7: one lane of 4 attempts and BPCs of 10 and 4 runs: the 4-run BPC
    fits, so the 10-run one stays for later."""
    row = A._row(t1_bpo=True)
    row["t1_bpo_count"] = 1
    row["t1_bpc_runs_by_item"] = [[81, 10], [82, 4]]
    (invent,) = [a for a in A._assign(row, [A._best_inventor(), A._copier()]) if a.action_type == "invent"]
    assert (invent.blueprint_item_id, invent.runs) == (82, 4)


def test_the_chain_planner_claims_the_best_fitting_bpc():
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    _run([t2], {}, {998: [_bpc(78, 998, 10), _bpc(79, 998, 4)]}, chars=[A._best_inventor()])
    assert t2.overview_row["t1_bpc_runs_by_item"] == [[79, 4]]


# --- probe e: T1 BPCs without a T1 BPO are invented from ---------------------------

def test_probe_e_t1_bpcs_on_hand_without_a_bpo_are_invented_from():
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    plan, actions = _run([t2], {}, {998: [_bpc(78, 998, 10)]}, chars=[A._best_inventor()])
    assert t2.pipeline_stage == "invention"
    assert "buy a T1 BPO" not in t2.decision_reason
    (invent,) = actions
    assert (invent.action_type, invent.runs, invent.blueprint_item_id) == ("invent", 4, 78)
    assert "no manufacture today" in t2.decision_reason


def test_without_bpcs_and_without_a_bpo_the_reason_still_says_buy_a_t1_bpo():
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    _plan, actions = _run([t2], {}, {})
    assert actions == []
    assert "buy a T1 BPO (Widget I Blueprint, 998) first" in t2.decision_reason


# --- probe f: a T1 row with no owned blueprint is not built ------------------------

def test_probe_f_a_t1_row_without_an_owned_bpo_or_bpc_is_not_built():
    row = _t1_row(20)
    row["manufacturing_job"]["blueprint_source_kind"] = "blueprint_sde_fallback"
    row["manufacturing_job"]["job_segments"] = [{"runs": 10, "source": "blueprint_sde_fallback", "materials": {"34": 50}}]
    t1 = _dec(row, 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t1], {}, {})
    assert actions == []
    assert t1.pipeline_stage == "blocked"
    assert "not built: no owned BPO or BPC: buy the BPO first" in t1.decision_reason
    assert _shop(actions, A.PRICES, BP_B) == {}


# --- probe g: two T2 rows, one T1 BPO -----------------------------------------------

def test_probe_g_two_hand_built_rows_do_not_both_copy_the_one_bpo():
    first, second = A._row(t1_bpo=True), A._row(t1_bpo=True)
    second["type_id"] = 12346
    first["t1_bpc_runs_on_hand"] = second["t1_bpc_runs_on_hand"] = 0
    first["t1_bpo_count"] = second["t1_bpo_count"] = 1
    plan = ChainPlan(decisions=[A._decision(first, stage="copying", score=2.0),
                                A._decision(second, stage="copying", type_id=12346, score=1.0)])
    chars = [A._best_inventor(), A._second_inventor(), A._copier(), {**A._copier(), "character_id": 9, "character_name": "C2"}]
    actions = CharacterAssigner().assign(plan, [], A._Chars(chars), A._Admin(), price_map=A.PRICES, blueprint_data=NO_SKILL_REQUIREMENTS)
    copies = [a for a in actions if a.action_type == "copy"]
    assert [c.type_id for c in copies] == [998] and copies[0].notes.startswith("Copy T1 BPO")
    assert [a for a in actions if a.type_id == 12346] == []
    assert plan.decisions[1].decision_reason == (
        "test; no manufacture today: no free T1 BPO (998) to copy: another row copies it today, no copy"
    )


def test_probe_g_the_chain_planner_gives_the_one_bpo_to_the_higher_scored_row():
    first = _dec(_t2_row(12345), 12345, "Widget II", 2.0, 2, "invention")
    second = _dec(_t2_row(12346), 12346, "Widget II b", 1.0, 2, "invention")
    _plan, actions = _run([first, second], {998: [_bpo(77, 998)]}, {},
                          chars=[*_chars(), {**A._copier(), "character_id": 9, "character_name": "C2"}])
    assert [(a.action_type, a.type_id) for a in actions] == [("copy", 998)]
    assert first.pipeline_stage == "copying"
    assert "not built: T1 BPO (998) used by copy for Widget II (12345) today" in second.decision_reason


# --- copies are stated as copies x runs per copy -----------------------------------

def test_the_copy_states_copies_times_runs_and_is_capped_at_the_max_runs_per_copy():
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    bp = copy.deepcopy(BP_B)
    bp[998]["max_production_limit"] = 3
    _plan, actions = _run([t2], {998: [_bpo(77, 998)]}, {}, blueprints=bp)
    (copy_action,) = [a for a in actions if a.action_type == "copy"]
    # 4 attempts at most 3 runs per copy: 2 copies x 3 runs (6 copied runs, conservative).
    assert (copy_action.quantity, copy_action.runs) == (2, 6)
    assert "make 2 copies × 3 runs" in copy_action.notes


# --- probe d: the composed scenario stays correct, pinned --------------------------

def test_probe_d_the_composed_plan_is_unchanged():
    from eve_online_industry_tracker.application.daily_planner.action_plan_builder import ActionPlanBuilder
    from eve_online_industry_tracker.application.daily_planner.service import _model_to_dict
    from streamlit_ui.components.daily_planner.status_bar import capital_reserved_display, compute_capital_reserved

    bp = {**copy.deepcopy(R.BLUEPRINT_DATA), **copy.deepcopy(A.BLUEPRINTS)}
    bp[1999]["manufacturing"]["materials"] = [{"type_id": 34, "quantity": 7}]
    bp[1999]["manufacturing"]["products"] = [{"type_id": 22345, "quantity": 1}]
    prices = {**R.MARKET, **A.PRICES}
    prices.pop(A.DATACORE_B)
    r1 = R._row(**R.BPO_ROW)
    r1["manufacturing_job"]["research_job_cost_by_level"] = {
        "research_material": [float(i * 100) for i in range(11)],
        "research_time": [float(i * 10) for i in range(11)],
    }
    r2 = A._row(t1_bpo=False)
    r2["type_id"] = 22345
    r2["manufacturing_job"]["blueprint_sde"] = {"blueprint_type_id": 1999}
    r2["manufacturing_job"]["materials"] = {"34": {"type_id": 34, "type_name": "Tritanium", "quantity": 140}}
    r2.pop("needs_invention")
    stock = {54321: 300, A.DATACORE_A: 3, 34: 50}
    plan = _planner(10).plan_chain(
        [_dec(r1, 12345, "Thing", 3.0, 1), _dec(r2, 22345, "Widget II", 2.0, 2, "invention")], {
            "bpo_assets_by_type_id": {999: [_bpo(11, 999, me=5, te=10)], 888: [_bpo(12, 888)], 998: [_bpo(13, 998)]},
            "bpc_assets_by_type_id": {998: [_bpc(14, 998, 2)]},
            "blueprint_data": bp, "price_map": prices, "market_depth_cache": prices,
            "variant_rows_by_type_id": {12345: [R._bpc_variant()]}, "corp_material_stock": stock,
        })
    assert [(d.type_id, d.is_sub_component, d.pipeline_stage) for d in plan.decisions] == [
        (12345, False, "manufacturing"), (22345, False, "copying"), (54321, True, "manufacturing"),
    ]
    (sub,) = [d for d in plan.decisions if d.is_sub_component]
    assert {k: sub.overview_row[k] for k in ("quantity_requested", "corp_stock_used", "quantity_needed", "sub_runs")} == {
        "quantity_requested": 990, "corp_stock_used": 300, "quantity_needed": 690, "sub_runs": 69,
    }
    chars = [
        {"character_id": 1, "character_name": "Best", "skills": {"skills": A._best_inventor()["skills"]["skills"] + [
            A._skill("Laboratory Operation", 1, 3406), A._skill("Mass Production", 1, 3387)]}},
        {"character_id": 3, "character_name": "Second", "skills": {"skills": A._second_inventor()["skills"]["skills"]}},
        {"character_id": 4, "character_name": "Copier", "skills": {"skills": [A._skill("Laboratory Operation", 1, 3406)]}},
    ]
    actions = CharacterAssigner().assign(plan, [], A._Chars(chars), A._Admin(), price_map=prices, blueprint_data=NO_SKILL_REQUIREMENTS)
    assert [(a.action_type, a.type_id, a.character_id, a.runs, a.quantity, a.estimated_job_fee_isk) for a in actions] == [
        ("me_research", 999, 1, None, None, 0.0),
        ("te_research", 999, 1, None, None, 0.0),
        ("manufacture", 12345, 1, 10, 10, 50.0),
        ("copy", 998, 4, 2, 1, 1.0),
        ("invent", 22345, 1, 2, None, 8.0),
        # The parent row publishes the component's fee (0 per run, the
        # research-first fixture): an unknown fee would buy it (Task 15 fix round 3).
        ("sub_manufacture", 54321, 1, 69, 690, 0.0),
    ]
    (invent,) = [a for a in actions if a.action_type == "invent"]
    assert invent.blueprint_item_id == 14

    class _Meta:
        def is_blueprint(self, t):
            return t in (999, 888, 998, 1999)

        def prefetch(self, t):
            return None

        def meta_group_id(self, t):
            return None

    items = ShoppingListBuilder().build(
        assigned_actions=actions, corp_assets=[SimpleNamespace(type_id=t, quantity=q) for t, q in stock.items()],
        market_depth_cache=prices, admin_settings=_Adm(), blueprint_data=bp, meta_resolver=_Meta(),
    )
    assert [(i.shopping_category, i.type_id, i.quantity, i.estimated_total) for i in items] == [
        ("current_job", 34, 75, 75.0),
        ("invention_input", A.DATACORE_A, 1, 100.0),
        ("invention_input", A.DATACORE_B, 4, None),
        ("invention_input", A.DECRYPTOR, 2, 1000.0),
    ]
    rows = ActionPlanBuilder().build(
        plan_id=0, assigned_actions=actions, shopping_items=items, industry_jobs=[], admin_settings=_Adm(),
        bpo_opportunities=[], character_name_map={}, meta_resolver=_Meta(),
    )
    value, caption = capital_reserved_display(compute_capital_reserved([_model_to_dict(r) for r in rows]))
    # Task 15 fix round 4: the cause is the unpriced Datacore B purchase
    # (estimated_total None above), and only that: every job fee is known.
    assert (value, caption) == ("unknown", "1 purchase(s) have no price")


# --- minor 2: rows not manufactured today say why -----------------------------------

@pytest.mark.parametrize("stage", ["copying", "invention"])
def test_a_copying_or_invention_row_says_no_manufacture_today(stage):
    row = A._row(t1_bpo=stage == "copying")
    decision = A._decision(row, stage=stage)
    CharacterAssigner().assign(ChainPlan(decisions=[decision]), [], A._Chars(_chars()), A._Admin(), price_map=A.PRICES, blueprint_data=NO_SKILL_REQUIREMENTS)
    assert decision.decision_reason.count("; no manufacture today: ") == 1


def test_a_not_built_row_gets_no_second_suffix():
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    _run([t2], {}, {})
    assert "not built:" in t2.decision_reason and "no manufacture today" not in t2.decision_reason


def test_a_bpo_in_research_with_no_other_source_says_no_manufacture_today():
    plan = R._plan(R._row(**R.BPO_ROW))
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    actions = R._assign(plan)
    assert "manufacture" not in actions
    assert "; no manufacture today: BPO in research" in top.decision_reason


# --- minor 4: a sub-build split over jobs runs in parallel only on free BPOs --------

def test_a_sub_build_of_several_jobs_on_one_bpo_queues_the_rest():
    """One component BPO, jobs of 40, 40 and 20 runs. The pilot has two
    manufacturing slots: the parent takes one, job 1 the other. Jobs 2 and
    3 wait on the same BPO: same pilot, no slot of their own (else they
    would have found none)."""
    bp = copy.deepcopy(R.BLUEPRINT_DATA)
    bp[888]["max_production_limit"] = 40  # 100 runs -> jobs of 40, 40, 20
    plan = _planner().plan_chain([_dec(R._row(**R.BPO_ROW), 12345, "Thing", 1.0, 1)], {
        "bpo_assets_by_type_id": {999: [_bpo(1, 999)], 888: [_bpo(2, 888)]},
        "blueprint_data": bp, "market_depth_cache": R.MARKET, "variant_rows_by_type_id": {},
    })
    pilot = {"character_id": 1, "character_name": "Pilot", "skills": {"skills": [
        {"skill_id": 3387, "skill_name": "Mass Production", "trained_skill_level": 1},
    ]}}
    actions = CharacterAssigner().assign(plan, [], R._Chars(pilot), R._Admin(), blueprint_data=NO_SKILL_REQUIREMENTS)
    subs = [a for a in actions if a.action_type == "sub_manufacture"]
    assert [a.runs for a in subs] == [40, 40, 20]
    assert {a.character_id for a in subs} == {1}
    assert "sequential" not in subs[0].notes
    assert [a.notes.split("; ")[-1] for a in subs[1:]] == [
        "sequential on the same BPO (later): starts after job 1",
        "sequential on the same BPO (later): starts after job 2",
    ]


# --- minor 5: the owned-BPC part's units add up -------------------------------------

def test_the_owned_bpc_part_rounds_up_and_the_rest_adds_up_to_the_total():
    from eve_online_industry_tracker.application.daily_planner import research_first

    row = R._row(segments=[R._segment(1, "owned_blueprint_copy", 100), R._segment(2, "owned_blueprint_original", 200)])
    row["manufacturing_job"]["runs"] = 3
    part = research_first.owned_bpc_part(row)
    # 10 units over 3 runs, 1 on the BPC: ceil(10 / 3) = 4; the BPO part is the
    # rest, 10 - 4 = 6, so the two add up to the batch exactly.
    assert part["quantity"] == 4
    assert "rest_quantity" not in part


# --- fix round 1 -------------------------------------------------------------------

def _thing_and_widget_ii(*, research_busy=False, mfg_busy=False):
    """s4: Thing (T1, score 1) is priced on BPC 78 (4 runs) + BPO 11 (TE 0,
    researched); its owned-BPC variant builds on BPC 80. Blueprint 999 is
    also the T1 source Widget II (score 2) invents from."""
    bp = {**copy.deepcopy(R.BLUEPRINT_DATA), **copy.deepcopy(A.BLUEPRINTS)}
    bp[999]["invention"] = copy.deepcopy(bp[998]["invention"])
    del bp[998]
    seg78 = {**R._segment(4, "owned_blueprint_copy", 400), "blueprint_item_id": 78}
    kept = R._row(segments=[seg78, R._segment(6, "owned_blueprint_original", 600)], kind="owned_blueprint_copy")
    variant = R._row(segments=[{**R._segment(10, "owned_blueprint_copy", 990), "blueprint_item_id": 80}],
                     kind="owned_blueprint_copy", material_cost=500.0, profit_amount=7.0, overview_row_id="bpc-variant")
    thing = _dec(kept, 12345, "Thing", 1.0, 1)
    widget = _dec(_t2_row(22345), 22345, "Widget II", 2.0, 2, "invention")
    prices = {**R.MARKET, **A.PRICES}
    plan = _planner(10).plan_chain([thing, widget], {
        "bpo_assets_by_type_id": {999: [_bpo(11, 999, te=0)]},
        "bpc_assets_by_type_id": {999: [_bpc(78, 999, 4), _bpc(80, 999, 10)]},
        "blueprint_data": bp, "market_depth_cache": prices, "variant_rows_by_type_id": {12345: [variant]},
    })
    # research_busy: both pilots' only research slot is in a job, so Thing's
    # research gets no slot and it builds from its kept row.
    jobs = [A._busy(A.BEST_ID), A._busy(A.COPIER_ID)] if research_busy else []
    if mfg_busy:  # and their only manufacturing slot too
        jobs += [{"character_id": cid, "activity_id": 1, "status": "active"} for cid in (A.BEST_ID, A.COPIER_ID)]
    chars = [A._best_inventor(), {**A._copier(), "skills": {"skills": []}}]
    actions = CharacterAssigner().assign(plan, jobs, A._Chars(chars), A._Admin(), price_map=prices, blueprint_data=NO_SKILL_REQUIREMENTS)
    return thing, widget, actions


def test_s4_the_kept_rows_bpc_is_claimed_too_so_invention_cannot_double_book_it():
    """Fix round 1, I1: research first plans the variant (BPC 80), but without
    a research slot Thing builds from its kept row (BPC 78 + BPO 11). The
    union is claimed, so Widget II has no BPC and no BPO left today."""
    thing, widget, actions = _thing_and_widget_ii(research_busy=True)
    assert "invent" not in [a.action_type for a in actions]
    assert widget.pipeline_stage == "blocked"
    (build,) = [a for a in actions if a.action_type == "manufacture"]
    assert build.materials == {54321: 1000}  # the kept row: BPC 78 + BPO 11


def test_m10_a_research_claim_that_got_no_slot_is_reworded():
    """The BPO's research got no slot, so Thing builds from it: Widget II's
    reason names that manufacture, not a research that does not happen."""
    _thing, widget, _actions = _thing_and_widget_ii(research_busy=True)
    assert "TE research for Thing" not in widget.decision_reason
    assert "T1 BPO (999) used by manufacture of Thing (12345) today" in widget.decision_reason


def test_m10_a_research_that_gets_its_slot_keeps_the_research_reason():
    _thing, widget, actions = _thing_and_widget_ii()
    assert "te_research" in [a.action_type for a in actions]
    assert "T1 BPO (999) used by TE research for Thing (12345) today" in widget.decision_reason


def test_s1_bpc_runs_on_hand_that_cover_the_attempts_claim_no_copy_bpo():
    """Fix round 1, I2: a 4-run T1 BPC covers the 4 attempts, so the T2 row
    (score 2) claims no copy BPO and the T1 row (score 1) builds from it."""
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    t1 = _dec(_t1_row(20), 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t2, t1], {998: [_bpo(77, 998)]}, {998: [_bpc(78, 998, 4)]})
    assert sorted((a.action_type, a.runs) for a in actions) == [("invent", 4), ("manufacture", 10)]
    assert t2.pipeline_stage == "invention"
    assert t2.decision_reason == "test; no manufacture today: inventing from T1 BPCs; no T2 BPC yet"
    assert t1.decision_reason == "test"


def test_s1_bpc_runs_a_t1_row_builds_on_do_not_count_for_the_copy_decision():
    """The 4-run BPC is the T1 row's own segment: net of it there are no
    runs, so the T2 row (score 2) does claim the BPO to copy."""
    t1_row = _t1_row(20)
    t1_row["manufacturing_job"]["job_segments"] = [
        {"runs": 4, "source": "owned_blueprint_copy", "blueprint_item_id": 78, "materials": {"34": 20}},
        {"runs": 6, "source": "owned_blueprint_original", "materials": {"34": 30}},
    ]
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    t1 = _dec(t1_row, 12344, "Widget I", 1.0, 1)
    _run([t2, t1], {998: [_bpo(77, 998)]}, {998: [_bpc(78, 998, 4)]})
    assert t2.pipeline_stage == "copying"


def test_extra_copy_bpos_are_claimed_only_up_to_the_lane_count():
    row = _t2_row()
    row["manufacturing_job"]["activity_breakdown"]["invention"]["elapsed_parallel"] = {
        "lanes": 2, "lanes_used": 2, "pilots_used": 2, "sequential_reason": None,
    }
    t2 = _dec(row, 12345, "Widget II", 2.0, 2, "invention")
    _run([t2], {998: [_bpo(77, 998), _bpo(78, 998), _bpo(79, 998)]}, {})
    assert t2.overview_row["t1_bpo_item_ids"] == [77, 78]


def test_without_a_lane_count_one_copy_bpo_is_claimed():
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    _run([t2], {998: [_bpo(77, 998), _bpo(78, 998)]}, {})
    assert t2.overview_row["t1_bpo_item_ids"] == [77]


def test_s3_a_bpo_in_an_active_job_is_owned_not_a_purchase():
    """Fix round 1, I3: the one BPO is installed in a job and the snapshot
    dropped it. It is owned and busy: no BPO analysis, no buy advice."""
    job = {"blueprint_item_id": 77, "blueprint_type_id": 998, "activity_id": 1, "blueprint_is_blueprint_copy": False}
    t1 = _dec(_t1_row(20), 12344, "Widget I", 1.0, 1)
    plan = _planner(0).plan_chain([t1], {
        "bpo_assets_by_type_id": {}, "bpc_assets_by_type_id": {}, "industry_jobs": [job],
        "busy_blueprint_item_ids": {77}, "blueprint_data": BP_B,
        "market_depth_cache": {**A.PRICES, 998: {"spot_sell_price": 1000.0}}, "variant_rows_by_type_id": {},
    })
    assert t1.decision_reason == "test; not built: owned blueprint in an active industry job"
    assert plan.bpo_opportunities == [] and t1.bpo_market_price is None
    assert t1.pipeline_stage == "blocked"


def test_s2_a_busy_bpc_row_gets_one_not_built_reason():
    row = _t1_row(20)
    mj = row["manufacturing_job"]
    mj["blueprint_source_kind"] = "owned_blueprint_copy"
    mj["job_segments"] = [{"runs": 10, "source": "owned_blueprint_copy", "blueprint_item_id": 78, "materials": {"34": 50}}]
    t1 = _dec(row, 12344, "Widget I", 1.0, 1)
    plan = _planner(0).plan_chain([t1], {
        "bpo_assets_by_type_id": {}, "bpc_assets_by_type_id": {998: [_bpc(78, 998, 10)]},
        "industry_jobs": [], "busy_blueprint_item_ids": {78}, "blueprint_data": BP_B,
        "market_depth_cache": A.PRICES, "variant_rows_by_type_id": {},
    })
    assert t1.decision_reason == "test; not built: BPC in an active industry job (item 78)"
    assert plan.bpo_opportunities == []


def test_a_blueprint_outside_the_corp_hangar_is_not_planned_and_not_a_purchase():
    """The producer priced the row from an owned BPO neither the corp nor a
    pilot holds in the planner's assets: not built, no buy advice."""
    t1 = _dec(_t1_row(20), 12344, "Widget I", 1.0, 1)
    plan = _planner(0).plan_chain([t1], {
        "bpo_assets_by_type_id": {}, "bpc_assets_by_type_id": {}, "blueprint_data": BP_B,
        "market_depth_cache": {**A.PRICES, 998: {"spot_sell_price": 1000.0}}, "variant_rows_by_type_id": {},
    })
    assert t1.decision_reason == (
        "test; not built: the owned blueprint it is priced from is in neither the corp's nor a pilot's "
        "assets: not planned"
    )
    assert plan.bpo_opportunities == []


@pytest.mark.parametrize("kind", ["blueprint_sde_fallback", None])
def test_buy_the_bpo_first_only_for_an_assumed_or_unowned_blueprint(kind):
    row = _t1_row(20)
    row["manufacturing_job"]["blueprint_source_kind"] = kind
    row["manufacturing_job"]["job_segments"] = [{"runs": 10, "source": kind or "blueprint_sde_fallback", "materials": {"34": 50}}]
    t1 = _dec(row, 12344, "Widget I", 1.0, 1)
    _run([t1], {}, {})
    assert t1.decision_reason == "test; not built: no owned BPO or BPC: buy the BPO first"


@pytest.mark.parametrize("decision", ["watch", "skip", "pause"])
def test_m4_the_merged_item_keeps_the_top_level_rows_product_fields(decision):
    plan, _parent, actions = _probe_a(20, widget_decision=decision)
    (widget,) = [d for d in plan.decisions if d.type_id == 54321]
    assert widget.is_sub_component and widget.decision == "build"
    assert widget.decision_reason.endswith(f"; as a product: {decision} (test)")
    assert (widget.adjusted_score, widget.isk_per_hour, widget.margin_pct, widget.days_of_supply_current,
            widget.effective_velocity, widget.velocity_unknown_reason) == (1.0, 1.0, 1.0, 1.0, 1.0, None)
    assert [a.quantity for a in actions if a.action_type == "sub_manufacture"] == [1000]


def test_m8_no_component_reason_when_the_stock_covers_it():
    parent = _dec(R._row(**R.BPO_ROW), 12345, "Thing", 2.0, 1)
    widget = _dec(_widget_row(), 54321, "Widget", 1.0, 1)
    plan = _planner().plan_chain([parent, widget], {
        "bpo_assets_by_type_id": {999: [_bpo(1, 999)], 888: [_bpo(2, 888)]},
        "blueprint_data": R.BLUEPRINT_DATA, "market_depth_cache": R.MARKET, "variant_rows_by_type_id": {},
        "corp_material_stock": {54321: 1000},
    })
    CharacterAssigner().assign(plan, [], R._Chars(R._pilot()), R._Admin(), blueprint_data=NO_SKILL_REQUIREMENTS)
    assert parent.decision_reason == "test"


def test_m8_no_component_reason_when_the_parent_gets_no_manufacture():
    parent = _dec(R._row(**R.BPO_ROW), 12345, "Thing", 2.0, 1)
    widget = _dec(_widget_row(), 54321, "Widget", 1.0, 1)
    plan = _planner().plan_chain([parent, widget], {
        "bpo_assets_by_type_id": {999: [_bpo(1, 999)], 888: [_bpo(2, 888)]},
        "blueprint_data": R.BLUEPRINT_DATA, "market_depth_cache": R.MARKET, "variant_rows_by_type_id": {},
    })
    no_slots = {"character_id": 1, "character_name": "Pilot", "skills": {"skills": []}}
    busy = [{"character_id": 1, "activity_id": 1, "status": "active"}]
    CharacterAssigner().assign(plan, busy, R._Chars(no_slots), R._Admin(), blueprint_data=NO_SKILL_REQUIREMENTS)
    assert "component Widget bought" not in parent.decision_reason


def test_m6_one_copy_of_one_run_is_worded_singular():
    row = A._row(t1_bpo=True)
    row["t1_bpc_runs_on_hand"] = 3  # 4 attempts, 1 copied run
    row["t1_max_runs_per_copy"] = 30
    (copy_action,) = [a for a in A._assign(row, [A._best_inventor(), A._copier()]) if a.action_type == "copy"]
    assert (copy_action.quantity, copy_action.runs) == (1, 1)
    assert "make 1 copy × 1 run;" in copy_action.notes


def test_more_copies_than_one_job_makes_are_capped_and_said():
    """Max 1 run per copy and 1 copy per job (the SDE limit): 4 attempts need
    4 copies of 1 run; one job makes 1, the other 3 wait for a later plan."""
    row = A._row(t1_bpo=True)
    row["t1_max_runs_per_copy"] = 1
    (copy_action,) = [a for a in A._assign(row, [A._best_inventor(), A._copier()]) if a.action_type == "copy"]
    assert (copy_action.quantity, copy_action.runs) == (1, 1)
    assert "more copies are needed than one job makes: 3 more in a later plan" in copy_action.notes


def test_an_unknown_lane_is_capped_by_its_bpc():
    from eve_online_industry_tracker.application.daily_planner.assigned_invention import AssignedInvention
    from eve_online_industry_tracker.application.daily_planner.character_assigner import _cap_lane

    unknown = AssignedInvention(None, 0.5, None, None, None, "datacore need unknown: x", "datacore need unknown: x")
    capped = _cap_lane(unknown, 3, "the 3 run(s) of T1 BPC item 81")
    assert capped.whole_attempts is None
    assert "at most 3 attempt(s): the 3 run(s) of T1 BPC item 81" in capped.note


# --- fix round 2 -------------------------------------------------------------------

def _two_t2_rows():
    a = _dec(_t2_row(12345), 12345, "Widget II", 2.0, 2, "invention")
    b = _dec(_t2_row(12346), 12346, "Widget II b", 1.0, 2, "invention")
    return a, b


def test_p1_the_copy_decision_simulates_the_whole_bpc_picks():
    """One 10-run BPC and BPO 77, two rows of 4 attempts each. Phase C claims
    whole BPCs, so row A takes BPC 78 whole and row B has none: B copies."""
    a, b = _two_t2_rows()
    _plan, actions = _run([a, b], {998: [_bpo(77, 998)]}, {998: [_bpc(78, 998, 10)]},
                          chars=[*_chars(), {**A._copier(), "character_id": 9, "character_name": "C2"}])
    assert a.overview_row["t1_bpc_runs_by_item"] == [[78, 10]] and a.overview_row["t1_bpo_item_ids"] == []
    assert (b.pipeline_stage, b.overview_row["t1_bpo_item_ids"]) == ("copying", [77])
    assert sorted((x.action_type, x.type_id) for x in actions) == [("copy", 998), ("invent", 12345)]


def test_p1_a_row_left_without_a_bpc_in_phase_c_copies_a_free_bpo():
    """The phase C fallback: phase B planned no copy (the BPC covers the
    row), but phase C leaves it without a BPC (simulated by a claim that
    returns none): it copies the free unclaimed BPO instead of being blocked."""
    t2 = _dec(_t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    planner = _planner(0)
    planner._claim_invention_bpcs = lambda ledger, decision, t1: []
    planner.plan_chain([t2], {
        "bpo_assets_by_type_id": {998: [_bpo(77, 998)]}, "bpc_assets_by_type_id": {998: [_bpc(78, 998, 10)]},
        "blueprint_data": BP_B, "market_depth_cache": A.PRICES, "variant_rows_by_type_id": {},
    })
    assert (t2.pipeline_stage, t2.overview_row["t1_bpo_item_ids"]) == ("copying", [77])
    assert "not built" not in t2.decision_reason


def test_the_used_by_wording_is_never_built_from_an_empty_holder_list():
    from eve_online_industry_tracker.application.daily_planner.chain_planner import _holders

    assert _holders([]) == "another job"
    assert _holders(["copy for X (1)", "manufacture of Y (2)"]) == "copy for X (1), manufacture of Y (2)"


def test_p2_the_copy_decision_uses_the_lanes_whole_attempts_when_known():
    """The producer's lanes need 6 attempts (whole_attempts, the lanes' total
    when they run), the best inventor alone 4: a 4-run BPC does not cover
    the lanes, so it copies."""
    row = _t2_row()
    row["manufacturing_job"]["activity_breakdown"]["invention"].update({
        "whole_attempts": 6, "best_inventor_whole_attempts": 4,
        "elapsed_parallel": {"lanes": 2, "lanes_used": 2, "pilots_used": 2, "sequential_reason": None},
    })
    t2 = _dec(row, 12345, "Widget II", 2.0, 2, "invention")
    _run([t2], {998: [_bpo(77, 998)]}, {998: [_bpc(78, 998, 4)]})
    assert t2.pipeline_stage == "copying"


def test_p2_without_lanes_the_best_inventors_attempts_decide():
    row = _t2_row()
    row["manufacturing_job"]["activity_breakdown"]["invention"].update({
        "whole_attempts": 4, "best_inventor_whole_attempts": 4,
    })
    t2 = _dec(row, 12345, "Widget II", 2.0, 2, "invention")
    _run([t2], {998: [_bpo(77, 998)]}, {998: [_bpc(78, 998, 4)]})
    assert t2.pipeline_stage == "invention"


def test_p3_no_manufacture_emitted_keeps_the_research_wording_with_no_slot():
    """Thing's research gets no slot AND no manufacturing slot is free: it
    builds nothing, so Widget II's reason must not claim a manufacture."""
    _thing, widget, actions = _thing_and_widget_ii(research_busy=True, mfg_busy=True)
    assert [a.action_type for a in actions] == []
    assert "manufacture of Thing" not in widget.decision_reason
    # Task A fix round 1 (I3): Thing's idle claims are released and Widget II
    # is re-planned, but no research slot is free either: undone, and the
    # reason says the blueprint is free and why nothing runs.
    assert ("not built: T1 BPO (999) free (its holder does not use it today), "
            "but no free research slot today") in widget.decision_reason


def test_m8_the_component_reason_checks_stock_pooled_across_parents():
    """Two parents need 600 + 600 of the component; stock holds 1000. Per
    parent each is covered, pooled 1200 > 1000 is bought: the reason stays."""
    rows = []
    for i, tid in enumerate((12345, 12346)):
        r = R._row(segments=[R._segment(10, "owned_blueprint_original", 600)])
        r["type_id"] = tid
        r["manufacturing_job"]["blueprint_sde"] = {"blueprint_type_id": 999 + i * 1000}
        rows.append(_dec(r, tid, f"Thing {i}", 2.0 - i, 1))
    widget = _dec(_widget_row(), 54321, "Widget", 0.5, 1)
    plan = _planner().plan_chain([*rows, widget], {
        "bpo_assets_by_type_id": {999: [_bpo(1, 999)], 1999: [_bpo(3, 1999)], 888: [_bpo(2, 888)]},
        "blueprint_data": R.BLUEPRINT_DATA, "market_depth_cache": R.MARKET, "variant_rows_by_type_id": {},
        "corp_material_stock": {54321: 1000},
    })
    CharacterAssigner().assign(plan, [], R._Chars(R._pilot()), R._Admin(), blueprint_data=NO_SKILL_REQUIREMENTS)
    assert all("component Widget bought: its BPO is used by manufacture of Widget (54321) today" in d.decision_reason
               for d in rows)


def test_a_row_whose_t1_bpcs_another_row_invents_from_is_not_told_to_buy_a_bpo():
    a, b = _two_t2_rows()
    _run([a, b], {}, {998: [_bpc(78, 998, 10)]})
    assert b.decision_reason == (
        "test; not built: T1 BPCs (998) used by invention for Widget II (12345) today "
        "and no T1 BPO to copy: no invention in this plan"
    )


# --- fix round 3 -------------------------------------------------------------------

def _p4(bpos):
    """Thing (T1) has a BPC-80 variant but no research; Widget II invents from
    T1 source 999."""
    bp = {**copy.deepcopy(R.BLUEPRINT_DATA), **copy.deepcopy(A.BLUEPRINTS)}
    bp[999]["invention"] = copy.deepcopy(bp[998]["invention"])
    del bp[998]
    kept = R._row(segments=[R._segment(10, "owned_blueprint_original", 1000)])
    variant = R._row(segments=[{**R._segment(10, "owned_blueprint_copy", 990), "blueprint_item_id": 80}],
                     kind="owned_blueprint_copy", material_cost=500.0, profit_amount=7.0, overview_row_id="bpc-variant")
    thing = _dec(kept, 12345, "Thing", 1.0, 1)
    widget = _dec(_t2_row(22345), 22345, "Widget II", 2.0, 2, "invention")
    prices = {**R.MARKET, **A.PRICES}
    plan = _planner(10).plan_chain([thing, widget], {
        "bpo_assets_by_type_id": {999: bpos}, "bpc_assets_by_type_id": {999: [_bpc(80, 999, 10)]},
        "blueprint_data": bp, "market_depth_cache": prices, "variant_rows_by_type_id": {12345: [variant]},
    })
    actions = CharacterAssigner().assign(plan, [], A._Chars(_chars()), A._Admin(), price_map=prices, blueprint_data=NO_SKILL_REQUIREMENTS)
    return thing, widget, actions


def test_p4_a_copy_claim_the_claimed_bpcs_make_unneeded_is_released():
    """Phase B kept BPC 80 out (Thing's variant names it) and claimed BPO 11
    to copy; Thing then built from BPO 13, so phase C got BPC 80, which
    covers the 4 attempts. The copy claim is released: the row invents, its
    stage and reason say so, and no BPO is booked for a copy that never runs."""
    thing, widget, actions = _p4([_bpo(11, 999), _bpo(13, 999)])
    assert widget.pipeline_stage == "invention"
    assert widget.overview_row["t1_bpo_item_ids"] == [] and widget.overview_row["has_t1_bpo"] is False
    assert widget.decision_reason == "test; no manufacture today: inventing from T1 BPCs; no T2 BPC yet"
    assert sorted((a.action_type, a.type_id, a.blueprint_item_id) for a in actions) == [
        ("invent", 22345, 80), ("manufacture", 12345, None),
    ]


def test_p4b_with_one_bpo_the_copy_stands_and_thing_builds_its_variant():
    thing, widget, actions = _p4([_bpo(11, 999)])
    assert widget.pipeline_stage == "copying" and widget.overview_row["t1_bpo_item_ids"] == [11]
    assert sorted(a.action_type for a in actions) == ["copy", "manufacture"]
    (build,) = [a for a in actions if a.action_type == "manufacture"]
    assert build.materials == {54321: 990}  # the BPC-80 variant


def test_m8_a_kept_row_component_reason_survives_when_the_fallback_buys_it():
    """Thing researches its only BPO (TE 0) and plans the owned-BPC part (4
    runs, 400 widgets); stock holds 500. Primary need 400 fits, but without
    a research slot the kept row builds 1000: bought, so the reason stays."""
    thing = _dec(R._row(**R.MIXED_ROW), 12345, "Thing", 2.0, 1)
    widget = _dec(_widget_row(), 54321, "Widget", 1.0, 1)
    plan = _planner().plan_chain([thing, widget], {
        "bpo_assets_by_type_id": {999: [_bpo(1, 999, te=0)], 888: [_bpo(2, 888)]},
        "blueprint_data": R.BLUEPRINT_DATA, "market_depth_cache": R.MARKET, "variant_rows_by_type_id": {},
        "corp_material_stock": {54321: 500},
    })
    busy = [{"character_id": 1, "activity_id": 3, "status": "active"}] * 10
    actions = CharacterAssigner().assign(plan, busy, R._Chars(R._pilot()), R._Admin(), blueprint_data=NO_SKILL_REQUIREMENTS)
    (build,) = [a for a in actions if a.action_type == "manufacture" and a.type_id == 12345]
    assert build.materials[54321] == 1000
    assert "component Widget bought: its BPO is used by manufacture of Widget (54321) today" in thing.decision_reason


def test_m10_a_multi_holder_reason_is_reworded_per_holder():
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        _reword_unused_research_claims,
    )

    thing = _dec({"research_claim_label": "TE research for Thing (12345)"}, 12345, "Thing", 1.0, 1)
    other = _dec({}, 22345, "Widget II", 2.0, 2)
    other.decision_reason = (
        "test; not built: T1 BPO (999) used by copy for Gadget II (2), TE research for Thing (12345) today: no copy"
    )
    _reword_unused_research_claims([thing, other], [], {12345})
    assert other.decision_reason == (
        "test; not built: T1 BPO (999) used by copy for Gadget II (2), "
        "TE research for Thing (12345) (kept for it, but it got no free slot) today: no copy"
    )
