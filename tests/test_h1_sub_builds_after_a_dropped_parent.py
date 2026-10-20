"""H1.6 minors (planner): a parent with no manufacture today gets no
sub-builds on a later re-size, also when none of the current sub-component
decisions names it (no_build covers every parent with sub-manufacture
requests); and the unknown-fee component buy reasons of one sizing do not
survive a re-size. Fake ids."""
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner import character_assigner as ca
from eve_online_industry_tracker.application.daily_planner.models import AssignedAction

PARENT, FALLEN = 990001, 990002


def _decision(type_id):
    return SimpleNamespace(type_id=type_id, decision="build", is_sub_component=False, adjusted_score=1.0,
                           overview_row={}, decision_reason="")


def test_a_parent_with_no_manufacture_is_dropped_even_when_no_sub_decision_names_it(monkeypatch):
    parent, fallen = _decision(PARENT), _decision(FALLEN)
    calls = []

    def _resize(fell_back, dropped=frozenset()):
        calls.append((set(fell_back), set(dropped)))
        return []

    plan = SimpleNamespace(decisions=[parent, fallen], resize_sub_components=_resize,
                           sub_request_parent_type_ids=frozenset({PARENT, FALLEN}))
    assigner = ca.CharacterAssigner()
    monkeypatch.setattr(assigner, "_get_characters", lambda svc: [{"character_id": 1}])
    monkeypatch.setattr(assigner, "_compute_available_slots", lambda *a: {})

    def _assign(*, decision, fell_back, **kw):
        if decision is fallen:
            fell_back.add(FALLEN)   # research first fell back to its kept BPO row
            return [AssignedAction(type_id=FALLEN, type_name="Fallen", action_type="manufacture", character_id=1,
                                   character_name="Pilot", quantity=1, runs=1, estimated_cost_isk=None,
                                   estimated_profit_isk=None, estimated_completion=None, notes=None)]
        return []                   # the parent: no manufacture today

    monkeypatch.setattr(assigner, "_assign_decision", _assign)
    monkeypatch.setattr(ca, "_reword_unused_research_claims", lambda *a: None)
    monkeypatch.setattr(ca, "_reword_released_bpcs", lambda *a: None)
    assigner.assign(plan, [], None, None)
    assert calls == [({FALLEN}, {PARENT})]


def test_a_sizings_unknown_fee_buy_reason_does_not_survive_a_re_size(monkeypatch):
    from test_research_first import BPO_ROW, _plan, _row

    from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner

    real = ChainPlanner._resolve_sub_requests
    calls = []

    def _resolve(self, sub_requests, market_depth_cache, phase1_data, *, buy_reasons=None):
        calls.append(1)
        if len(calls) == 1 and buy_reasons is not None:
            buy_reasons[54321] = {12345: "component Widget bought: fake unknown fee"}
        return real(self, sub_requests, market_depth_cache, phase1_data, buy_reasons=buy_reasons)

    monkeypatch.setattr(ChainPlanner, "_resolve_sub_requests", _resolve)
    plan = _plan(_row(**BPO_ROW), te=20)
    (top,) = [d for d in plan.decisions if not d.is_sub_component]
    assert top.overview_row["component_buy_reasons"] == {54321: "component Widget bought: fake unknown fee"}
    plan.resize_sub_components(set())
    assert top.overview_row["component_buy_reasons"] == {}
