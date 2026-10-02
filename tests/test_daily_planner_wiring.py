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
from eve_online_industry_tracker.infrastructure.models import (
    CharacterIndustryJobsModel,
    CorporationIndustryJobsModel,
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


def test_phase_4_forwards_row_admin_settings_and_meta_group_id_to_decide(session_provider):
    """Guards the exact kwargs ItemDecisionEngine.decide now requires.

    The write path from decide()'s return value onward is already covered by
    test_daily_planner_end_to_end.py's read-back test, but nothing directly
    proved _phase_4_decide forwards scored/pipeline/overview_row/
    admin_settings/meta_group_id to decide() by name -- the same kwargs-drift
    failure mode phase 2's and phase 3's wiring tests above already guard
    against for their own phases.
    """
    captured = {}

    svc = _service(session_provider, [GOOD_ROW])
    svc._decision_engine = SimpleNamespace(
        decide=lambda **kwargs: captured.update(kwargs) or SimpleNamespace(type_id=12345)
    )
    input_rows = svc._build_input_rows([GOOD_ROW])
    pipeline_state = SimpleNamespace(type_id=12345)
    scored_item = SimpleNamespace(type_id=12345)

    decisions = svc._phase_4_decide(
        [scored_item],
        [pipeline_state],
        {"overview_rows": [GOOD_ROW], "input_rows": input_rows},
    )

    assert len(decisions) == 1, "the stub decide() call must not be swallowed"
    assert captured["scored"] is scored_item
    assert captured["pipeline"] is pipeline_state
    assert captured["overview_row"] == GOOD_ROW
    assert captured["admin_settings"] is svc._admin
    assert captured["meta_group_id"] == input_rows[0].meta_group_id


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


def test_get_industry_jobs_excludes_only_terminal_statuses(session_provider):
    """_get_industry_jobs must run its status filter for real, not just build a
    query object -- a delivered job wrongly counting as active is what made the
    pause decision unreachable (finding 2). paused is pinned deliberately: it is
    the case the deny-list (vs. the producer's active/ready allow-list) exists
    for -- a paused job still holds a manufacturing slot and must stay active."""
    session = session_provider.app_session()
    try:
        session.add_all([
            CorporationIndustryJobsModel(corporation_id=1, job_id=101, status="active"),
            CorporationIndustryJobsModel(corporation_id=1, job_id=102, status="paused"),
            CorporationIndustryJobsModel(corporation_id=1, job_id=103, status="delivered"),
            CorporationIndustryJobsModel(corporation_id=1, job_id=104, status="cancelled"),
            CorporationIndustryJobsModel(corporation_id=1, job_id=105, status="reverted"),
        ])
        session.commit()
    finally:
        session.close()

    svc = _service(session_provider, [])
    jobs = svc._get_industry_jobs()

    job_ids = {job.job_id for job in jobs}
    assert job_ids == {101, 102}


def test_get_industry_jobs_includes_character_jobs_not_only_corp_jobs(session_provider):
    """Defect 2 (task-18a remediation): the live database holds 945
    character-installed jobs that a corp-only query can never see. Slot
    capacity is per character, so a personally-installed job must count
    against that pilot's slots exactly like a corp job they installed --
    _get_industry_jobs must query both CorporationIndustryJobsModel and
    CharacterIndustryJobsModel, applying the same deny-list status filter to
    each (a wrong implementation that queries only the corp table would
    return job_ids == {101} here, silently dropping the character job)."""
    session = session_provider.app_session()
    try:
        session.add_all([
            CorporationIndustryJobsModel(corporation_id=1, job_id=101, status="active"),
            CharacterIndustryJobsModel(character_id=7, job_id=201, status="active"),
            CharacterIndustryJobsModel(character_id=7, job_id=202, status="delivered"),
        ])
        session.commit()
    finally:
        session.close()

    svc = _service(session_provider, [])
    jobs = svc._get_industry_jobs()

    job_ids = {job.job_id for job in jobs}
    assert job_ids == {101, 201}


# --- F2: one overview row per product -----------------------------------------

def _variant(overview_row_id, isk_per_hour, **extra):
    return dict(GOOD_ROW, overview_row_id=overview_row_id, isk_per_hour=isk_per_hour, **extra)


def test_build_input_rows_keeps_one_row_per_type_id_the_highest_isk_per_hour(
    session_provider, caplog
):
    """The producer emits one row per blueprint variant. Keyed by type_id
    downstream, the last row silently won some lookups and the first others,
    so one product got a hybrid decision (row i's pipeline, the last row's
    profitability) -- and a 20.5M ISK/h variant lost to a 9.8M one."""
    low = _variant("row-a", 9_800_000.0)
    high = _variant("row-b", 20_500_000.0)
    svc = _service(session_provider, [low, high])
    with caplog.at_level("WARNING"):
        rows = svc._build_input_rows([low, high])
    assert len(rows) == 1
    assert rows[0].isk_per_hour == 20_500_000.0
    assert rows[0].raw is high
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any("12345" in m and "row-a" in m and "row-b" in m for m in warnings), warnings


def test_dedupe_ranks_an_unknown_isk_per_hour_last(session_provider):
    unknown = _variant("row-a", None)
    known = _variant("row-b", 1.0)
    svc = _service(session_provider, [unknown, known])
    (row,) = svc._build_input_rows([unknown, known])
    assert row.raw is known


def test_dedupe_ranks_an_unscoreable_variant_last(session_provider):
    """No cost basis at all (no material cost and no profit) is unscoreable
    even with an isk/hour, so a scoreable variant beats it."""
    no_basis = _variant("row-a", 50_000_000.0, profit_amount=None,
                        manufacturing_job={"runs": 20})
    scoreable = _variant("row-b", 1.0)
    svc = _service(session_provider, [no_basis, scoreable])
    (row,) = svc._build_input_rows([no_basis, scoreable])
    assert row.raw is scoreable


def test_dedupe_breaks_a_tie_on_the_first_row_seen(session_provider):
    first = _variant("row-a", 5.0)
    second = _variant("row-b", 5.0)
    svc = _service(session_provider, [first, second])
    (row,) = svc._build_input_rows([first, second])
    assert row.raw is first


def test_distinct_products_are_all_kept_in_order(session_provider):
    a = dict(GOOD_ROW, type_id=1)
    b = dict(GOOD_ROW, type_id=2)
    svc = _service(session_provider, [a, b])
    assert [r.type_id for r in svc._build_input_rows([a, b])] == [1, 2]
