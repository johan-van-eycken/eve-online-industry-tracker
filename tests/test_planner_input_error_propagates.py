"""A PlannerInputError raised inside a per-item handler must not be swallowed.

Phases 2-5 and feedback isolate one bad item with `except (TypeError,
ValueError)`. While PlannerInputError subclassed ValueError, a contract
violation raised beneath any of those handlers would be logged and skipped
like a malformed number, and the plan would compute on without the item.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
from eve_online_industry_tracker.application.daily_planner.feedback_processor import (
    FeedbackProcessor,
)
from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputError
from eve_online_industry_tracker.application.daily_planner.models import ItemDecision
from eve_online_industry_tracker.application.daily_planner.pipeline_analyzer import (
    PipelineAnalyzer,
)
from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService


def _violation(*_args, **_kwargs):
    raise PlannerInputError(type_id=12345, field="manufacturing_job.runs", detail="is missing")


class _AdminKeyError:
    def get(self, section, key):
        raise KeyError(key)


class _NoBlueprints:
    def is_blueprint(self, type_id):
        return False

    def prefetch(self, type_ids):
        return None


def _decision(row):
    return ItemDecision(
        type_id=int(row["type_id"]), type_name="Thing", decision="build",
        decision_reason="test", adjusted_score=1.0, absolute_profit_per_batch=1.0,
        isk_per_hour=1.0, margin_pct=1.0, days_of_supply_current=1.0,
        effective_velocity=1.0, meta_group_id=1, pipeline_stage="manufacturing",
        overview_row=row,
    )


def _bare_service():
    return DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: []),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        realized_profit_service=SimpleNamespace(),
        repo=SimpleNamespace(),
        admin_settings=SimpleNamespace(),
        session_provider=SimpleNamespace(),
    )


def test_planner_input_error_is_not_a_value_error():
    assert not issubclass(PlannerInputError, ValueError)


def test_phase_2_lets_a_contract_violation_through():
    analyzer = PipelineAnalyzer()
    analyzer._analyze_single = _violation
    with pytest.raises(PlannerInputError):
        analyzer.analyze(
            input_rows=[SimpleNamespace(type_id=12345)], industry_jobs=[], corp_assets=[],
            market_depth_cache={}, weights={}, sell_velocities={},
            meta_resolver=_NoBlueprints(),
        )


def test_phase_3_lets_a_contract_violation_through():
    svc = _bare_service()
    svc._profitability_scorer = SimpleNamespace(score=_violation)
    phase1 = {"input_rows": [SimpleNamespace(type_id=12345)], "weights": {},
              "market_depth_cache": {}, "margin_correlations": {}, "trit_trend_7d": None}
    with pytest.raises(PlannerInputError):
        svc._phase_3_score([SimpleNamespace(type_id=12345)], phase1)


def test_phase_4_lets_a_contract_violation_through():
    svc = _bare_service()
    svc._decision_engine = SimpleNamespace(decide=_violation)
    with pytest.raises(PlannerInputError):
        svc._phase_4_decide(
            [SimpleNamespace(type_id=12345)], [SimpleNamespace(type_id=12345)],
            {"overview_rows": [], "input_rows": []},
        )


def test_phase_5_chain_lets_a_contract_violation_through():
    planner = ChainPlanner(None, None, _AdminKeyError())
    planner._plan_t1_chain = _violation
    row = {"type_id": 12345, "quantity": 1, "manufacturing_job": {"runs": 1}}
    with pytest.raises(PlannerInputError):
        planner.plan_chain([_decision(row)], {})


def test_phase_5_sub_manufacture_lets_a_contract_violation_through():
    planner = ChainPlanner(None, None, _AdminKeyError())
    planner._resolve_sub_manufacture = _violation
    row = {"type_id": 12345, "quantity": 1, "manufacturing_job": {
        "runs": 1, "blueprint_sde": {"blueprint_type_id": 999},
        "materials": {"54321": {"type_id": 54321, "quantity": 10}},
    }}
    bpo = SimpleNamespace(type_id=888, is_blueprint_copy=False,
                          blueprint_material_efficiency=10, blueprint_time_efficiency=20)
    phase1 = {
        "bpo_assets_by_type_id": {888: [bpo]},
        "blueprint_data": {888: {"manufacturing": {
            "products": [{"type_id": 54321, "quantity": 10}],
            "materials": [{"type_id": 34, "quantity": 2}],
        }}},
    }
    with pytest.raises(PlannerInputError):
        planner.plan_chain([_decision(row)], phase1)


def test_feedback_lets_a_contract_violation_through():
    repo = MagicMock()
    repo.get_unprocessed_done_actions.return_value = [
        SimpleNamespace(id=1, type_id=12345, action_type="manufacture")
    ]
    processor = FeedbackProcessor(repo, _AdminKeyError())
    processor._process_single_action = _violation
    with pytest.raises(PlannerInputError):
        processor.process_pending_feedback()
