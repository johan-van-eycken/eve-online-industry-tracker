# tests/test_daily_planner_wiring.py
"""The service must build PlannerInputRows and own a TypeMetadataResolver.

These are wiring tests: they assert the service hands its sub-components the
arguments those components now require, because a signature mismatch here does
not surface until a live plan compute fails.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.daily_planner.input_row import (
    PlannerInputError,
    PlannerInputRow,
)
from eve_online_industry_tracker.application.daily_planner.service import (
    DailyPlannerService,
)

GOOD_ROW = {
    "type_id": 12345,
    "type_name": "Hobgoblin II",
    "quantity": 10,
    "blueprint_type_id": 999,
    "isk_per_hour": 4_000_000.0,
    "profit_amount": 1_000_000.0,
    "profit_margin_fraction": 0.2,
    "days_of_supply": 6.5,
    "pipeline_units_in_jobs": 40,
    "pipeline_units_on_market": 60,
    "pipeline_days_supply": 12.5,
    "price_trend_7d_pct": 3.5,
    "price_trend_30d_pct": -2.0,
    "manufacturing_job": {"runs": 20, "material_cost": 50_000_000.0},
}


def _service(session_provider, overview_rows):
    """A service whose collaborators are inert stubs.

    Only the wiring is under test here, so every collaborator is a stub; match
    the real DailyPlannerService.__init__ keyword names when you write this.
    """
    return DailyPlannerService(
        industry_service=SimpleNamespace(
            get_cached_overview_rows=lambda: overview_rows,
        ),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        pricing_suggestion_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        realized_profit_service=SimpleNamespace(),
        repo=SimpleNamespace(),
        admin_settings=SimpleNamespace(get=lambda *a, **k: None),
        session_provider=session_provider,
    )


def test_the_service_owns_a_meta_resolver(session_provider):
    svc = _service(session_provider, [GOOD_ROW])
    assert hasattr(svc, "_meta_resolver")
    assert hasattr(svc._meta_resolver, "is_blueprint")
    assert hasattr(svc._meta_resolver, "meta_group_id")


def test_build_input_rows_converts_every_overview_row(session_provider):
    svc = _service(session_provider, [GOOD_ROW])
    rows = svc._build_input_rows([GOOD_ROW])
    assert len(rows) == 1
    assert isinstance(rows[0], PlannerInputRow)
    assert rows[0].type_id == 12345
    assert rows[0].material_cost_per_unit == 5_000_000.0


def test_build_input_rows_propagates_a_contract_violation(session_provider):
    """A bad row must abort, not be skipped — a plan built on a subset is a
    plan that silently lies about what is worth building.

    Note: a row missing manufacturing_job.material_cost is NOT a contract
    violation (input_row.py's "optional, key and value" category — the
    material-pricing enrichment step legitimately omits material_cost when
    pricing isn't available yet; see commit f483a6e's review fix, already
    committed on this branch before this task). manufacturing_job.runs is
    one of the six fields that DOES guard against a producer rename, so a
    row missing it is the real contract violation this test exercises.
    """
    bad = dict(GOOD_ROW, manufacturing_job={"material_cost": 50_000_000.0})
    svc = _service(session_provider, [bad])
    with pytest.raises(PlannerInputError) as exc:
        svc._build_input_rows([bad])
    assert exc.value.field == "manufacturing_job.runs"


def test_phase_2_passes_input_rows_and_the_resolver(session_provider):
    """Guards the exact kwargs PipelineAnalyzer.analyze now requires."""
    captured = {}

    svc = _service(session_provider, [GOOD_ROW])
    svc._pipeline_analyzer = SimpleNamespace(
        analyze=lambda **kwargs: captured.update(kwargs) or []
    )
    svc._phase_2_pipeline({
        "input_rows": svc._build_input_rows([GOOD_ROW]),
        "overview_rows": [GOOD_ROW],
        "industry_jobs": [],
        "corp_assets": [],
        "market_depth_cache": {},
        "weights": {},
        "sell_velocities": {},
    })

    assert "input_rows" in captured, "analyze() must receive input_rows"
    assert "overview_rows" not in captured, "the old kwarg must be gone"
    assert captured["meta_resolver"] is svc._meta_resolver
    assert all(isinstance(r, PlannerInputRow) for r in captured["input_rows"])


def test_phase_3_passes_row_not_overview_row_plus_every_other_argument(session_provider):
    """Guards the exact kwargs ProfitabilityScorer.score now requires.

    Phase 3 is the call site this task changed the most: overview_row= became
    row=, and the row itself now comes from an input_by_type lookup instead of
    an overview_by_type one. Without this test a future edit could silently
    drop e.g. trit_trend_7d (disabling the mineral-squeeze penalty) with the
    suite still green, the same failure mode phase 2's and phase 7's kwargs
    tests already guard against.
    """
    captured = {}

    svc = _service(session_provider, [GOOD_ROW])
    svc._profitability_scorer = SimpleNamespace(
        score=lambda **kwargs: captured.update(kwargs) or SimpleNamespace(type_id=12345)
    )
    input_rows = svc._build_input_rows([GOOD_ROW])
    pipeline_state = SimpleNamespace(type_id=12345)

    scored = svc._phase_3_score(
        [pipeline_state],
        {
            "input_rows": input_rows,
            "weights": {12345: "weights-marker"},
            "market_depth_cache": {12345: "market-depth-marker"},
            "margin_correlations": {12345: "margin-corr-marker"},
            "trit_trend_7d": 3.5,
        },
    )

    assert len(scored) == 1, "the stub score() call must not be swallowed"
    assert "row" in captured, "score() must receive row="
    assert "overview_row" not in captured, "the old kwarg must be gone"
    assert isinstance(captured["row"], PlannerInputRow)
    assert captured["row"].type_id == 12345
    assert captured["pipeline"] is pipeline_state
    assert captured["weights"] == "weights-marker"
    assert captured["market_depth"] == "market-depth-marker"
    assert captured["margin_correlation"] == "margin-corr-marker"
    assert captured["trit_trend_7d"] == 3.5


def test_phase_7_passes_the_resolver(session_provider):
    captured = {}

    svc = _service(session_provider, [GOOD_ROW])
    svc._shopping_list_builder = SimpleNamespace(
        build=lambda **kwargs: captured.update(kwargs) or []
    )
    svc._phase_7_shopping([], {
        "corp_assets": [],
        "market_depth_cache": {},
        "blueprint_data": {},
    })

    assert captured["meta_resolver"] is svc._meta_resolver
