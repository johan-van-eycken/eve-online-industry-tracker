# tests/test_daily_planner_integration.py
"""Full-pipeline test over the real (sanitised) product-overview fixture.

Each assertion here checks a value that was structurally zero or dead before
the remediation: material cost, pipeline days, BPO analysis on the nested
blueprint id, slot counts, manufacture runs and shopping quantities.

The fixture is captured by hand (plan Task 3) and this module skips until it
exists. tests/test_daily_planner_end_to_end.py already drives _run_compute
over one synthetic row, so this module does not repeat that. It wires the
phase components the way DailyPlannerService's _phase_2 .. _phase_7 do, so
every phase sees real producer row shapes.

Sanitiser rule (fixture_export.py): every number is scrambled on its own and
every string except type_name / meta_group_name is redacted. So assertions
only look at single fields or counts, never at arithmetic across a row's
fields. Where a test supplies its own input (a market price, a per-run
material quantity), it picks that input so the expected result reads
straight off one field.

Not covered here: invention inputs on the shopping list. An invent action
needs an owned T1 source BPO (a corp asset) and the SDE invention activity of
that source. The overview fixture carries neither, and inventing both here
would test the test. tests/test_shopping_list_builder.py covers that path.
"""
from __future__ import annotations

import dataclasses
import json
import os
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
from eve_online_industry_tracker.application.daily_planner.character_assigner import (
    CharacterAssigner,
)
from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputRow
from eve_online_industry_tracker.application.daily_planner.item_decision_engine import (
    ItemDecisionEngine,
)
from eve_online_industry_tracker.application.daily_planner.pipeline_analyzer import (
    PipelineAnalyzer,
)
from eve_online_industry_tracker.application.daily_planner.profitability_scorer import (
    ProfitabilityScorer,
)
from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import (
    ShoppingListBuilder,
)
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager
from eve_online_industry_tracker.infrastructure.models import CorporationIndustryJobsModel

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "overview_rows_real.json")

pytestmark = pytest.mark.skipif(
    not os.path.exists(FIXTURE),
    reason="tests/fixtures/overview_rows_real.json not captured yet (plan Task 3)",
)

# Units of each material per run in this test's blueprint data. Large enough
# that ME research can save something, so BPO analysis does real arithmetic.
_PER_RUN_QTY = 100

_META_GROUP_IDS = {"Tech I": 1, "Tech II": 2, "Tech III": 14, "Faction": 4, "Storyline": 3}

# The skill payload a real character carries: skill_name + trained_skill_level,
# no active_skill_level key. 1 + 5 + 4 = 10 manufacturing slots.
_SKILLED_PILOT = {
    "character_id": 1,
    "character_name": "Pilot",
    "skills": {"skills": [
        {"skill_id": 3387, "skill_name": "Mass Production", "trained_skill_level": 5},
        {"skill_id": 24625, "skill_name": "Advanced Mass Production", "trained_skill_level": 4},
        {"skill_id": 3406, "skill_name": "Laboratory Operation", "trained_skill_level": 5},
    ]},
}


class _FixtureMetadata:
    """Stands in for TypeMetadataResolver, which needs a populated SDE.

    The meta group comes from the row's own meta_group_name (public SDE data,
    so the sanitiser keeps it). That routes Tech I rows down ChainPlanner's
    T1 path and the rest down its T2 path, as the real resolver would. With
    no corp assets passed in, is_blueprint is never asked about a real asset.
    """

    def __init__(self, rows):
        self._meta = {}
        for row in rows:
            name = orow.get_meta_group_name(row)
            self._meta[int(row["type_id"])] = _META_GROUP_IDS.get(name, 1)

    def meta_group_id(self, type_id):
        return self._meta.get(int(type_id))

    def is_blueprint(self, type_id):
        return False

    def prefetch(self, type_ids):
        return None


def _load_rows():
    with open(FIXTURE, encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture()
def overview_rows():
    return _load_rows()


@pytest.fixture()
def meta(overview_rows):
    return _FixtureMetadata(overview_rows)


@pytest.fixture()
def input_rows(overview_rows, meta):
    return [PlannerInputRow.from_overview(r, meta_groups=meta) for r in overview_rows]


@pytest.fixture()
def admin(tmp_path):
    return AdminSettingsManager(file_path=str(tmp_path / "admin.json"))


def _material_type_ids(row):
    materials = orow.get_manufacturing_job(row).get("materials") or {}
    out = []
    for entry in materials.values() if isinstance(materials, dict) else []:
        if isinstance(entry, dict) and int(entry.get("type_id") or 0) > 0:
            out.append(int(entry["type_id"]))
    return out


def _blueprint_data_from_rows(rows):
    """SDE-shaped blueprint data, keyed by each row's real nested blueprint id.

    The test SDE is empty, so this stands in for get_blueprint_manufacturing_data.
    Material type ids come from the row's own manufacturing_job.materials, each
    at _PER_RUN_QTY units per run. That way a shopping-list line's quantity is
    a fixed multiple of the action's run count, which checks a single field.
    """
    data = {}
    for row in rows:
        bp_type_id = orow.get_blueprint_type_id(row)
        if bp_type_id is None or bp_type_id in data:
            continue
        data[bp_type_id] = {
            "type_name": "",
            "manufacturing": {
                "materials": [
                    {"type_id": t, "quantity": _PER_RUN_QTY} for t in _material_type_ids(row)
                ],
                "products": [{"type_id": int(row["type_id"]), "quantity": 1}],
            },
        }
    return data


def _market_depth(rows, blueprint_data):
    """A spot price on every blueprint and a VWAP on every material.

    Product type ids get no entry on purpose, so absolute profit falls back to
    the producer's own profit_amount, as it does when Jita depth is missing.
    """
    cache = {bp: {"spot_sell_price": 1_000_000.0} for bp in blueprint_data}
    for row in rows:
        for t in _material_type_ids(row):
            cache[t] = {"vwap_5d": 1.0}
    return cache


def _run_phases(overview_rows, input_rows, meta, admin):
    """Phases 2-7, wired the way DailyPlannerService wires them."""
    blueprint_data = _blueprint_data_from_rows(overview_rows)
    market_depth = _market_depth(overview_rows, blueprint_data)

    states = PipelineAnalyzer().analyze(
        input_rows=input_rows, industry_jobs=[], corp_assets=[],
        market_depth_cache=market_depth, weights={},
        sell_velocities={r.type_id: 5.0 for r in input_rows},
        meta_resolver=meta,
    )

    # Same lookup as _phase_3_score / _phase_4_decide: by type_id, not zip(),
    # since analyze() may drop a row it could not analyse.
    input_by_type = {r.type_id: r for r in input_rows}
    scorer = ProfitabilityScorer()
    engine = ItemDecisionEngine()
    decisions = []
    for state in states:
        row = input_by_type[state.type_id]
        scored = scorer.score(
            pipeline=state, row=row, weights=None,
            market_depth=market_depth.get(row.type_id), margin_correlation=None,
        )
        decisions.append(engine.decide(
            scored=scored, pipeline=state, overview_row=row.raw,
            admin_settings=admin, meta_group_id=row.meta_group_id,
        ))

    # Phase 4's thresholds act on scrambled magnitudes, so which rows reach
    # 'build' in the fixture is arbitrary. Phase 5 onwards is what this test
    # is about, so every row is promoted to 'build' (its gating has its own
    # tests in test_daily_planner_decisions.py).
    promoted = [dataclasses.replace(d, decision="build") for d in decisions]

    chain = ChainPlanner(
        industry_service=SimpleNamespace(), session_provider=None, admin_settings=admin,
    ).plan_chain(
        top_level_decisions=promoted,
        phase1_data={
            "corp_assets": [], "industry_jobs": [], "market_depth_cache": market_depth,
            "bpo_assets_by_type_id": {}, "bpc_assets_by_type_id": {},
            "blueprint_data": blueprint_data, "invention_success_rates": {},
        },
    )

    actions = CharacterAssigner().assign(
        chain_plan=chain, industry_jobs=[],
        characters_service=SimpleNamespace(list_characters=lambda: [dict(_SKILLED_PILOT)]),
        admin_settings=admin,
    )

    return SimpleNamespace(
        states=states, decisions=decisions, promoted=promoted, chain=chain,
        actions=actions, blueprint_data=blueprint_data, market_depth=market_depth,
    )


@pytest.fixture()
def run(overview_rows, input_rows, meta, admin):
    return _run_phases(overview_rows, input_rows, meta, admin)


# ── Phase 1: the input contract over real rows ──────────────────────────────


def test_every_real_row_builds_an_input_row(input_rows):
    assert len(input_rows) > 1


def test_the_nested_blueprint_type_id_resolves_on_every_real_row(input_rows):
    """It lives under manufacturing_job.blueprint_sde, and a top-level read found nothing."""
    missing = [r.type_id for r in input_rows if r.blueprint_type_id is None]
    assert not missing, f"no blueprint_type_id resolved for type_ids {missing}"


def test_material_cost_is_never_structurally_zero(input_rows):
    """None (pricing not computed yet) is allowed. A zero would be the old phantom-key read."""
    costs = [r.material_cost_per_unit for r in input_rows if r.material_cost_per_unit is not None]
    assert costs, "no real row carries a material cost at all"
    assert all(c > 0 for c in costs)


def test_manufacturing_runs_come_from_the_nested_job(input_rows):
    assert all(r.runs > 0 for r in input_rows)


# ── Phases 2-3: pipeline and scoring ────────────────────────────────────────


@pytest.mark.parametrize(
    "pipeline_days_supply",
    [
        pytest.param(4.0, id="producer-days"),
        pytest.param(None, id="fallback-units-over-velocity"),
    ],
)
def test_pipeline_supply_on_a_real_row_yields_non_zero_pipeline_days(
    overview_rows, meta, pipeline_days_supply
):
    """A real row with this test's own pipeline supply must show pipeline days.

    The capture's market state is whatever it was that day. Today every row
    has 0 units in jobs and on market. So the test writes its own supply onto
    one real row, under the keys the producer fills
    (_enrich_product_rows_with_market_trends). PipelineAnalyzer reads days
    only through these keys: industry_jobs just set
    has_active_manufacturing_jobs, and corp orders are not one of its inputs.
    With pipeline_days_supply set, the producer's value is used. With None
    (no 7d volume), units / velocity is used. If the reads went back to the
    phantom keys, both cases would give 0.
    """
    raw = dict(overview_rows[0])
    raw["pipeline_units_in_jobs"] = 50
    raw["pipeline_units_on_market"] = 30
    raw["pipeline_days_supply"] = pipeline_days_supply
    row = PlannerInputRow.from_overview(raw, meta_groups=meta)

    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=[],
        market_depth_cache={}, weights={},
        sell_velocities={row.type_id: 5.0}, meta_resolver=meta,
    )
    assert states[0].total_pipeline_days > 0


def _first_scoreable_costed_row(input_rows):
    """A row with an isk/hour and a material cost.

    A row with no isk/hour is unscoreable by design, so it cannot show cost
    subtraction.
    """
    for row in input_rows:
        if (
            row.isk_per_hour is not None
            and row.material_cost_per_unit is not None
            and row.material_cost_per_unit > 0
        ):
            return row
    pytest.skip("the captured fixture has no row with both an isk/hour and a material cost")


def _score_at_price(row, meta, price):
    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=[], market_depth_cache={},
        weights={}, sell_velocities={row.type_id: 5.0}, meta_resolver=meta,
    )
    return ProfitabilityScorer().score(
        pipeline=states[0], row=row, weights=None,
        market_depth={"vwap_5d": price}, margin_correlation=None,
    )


def test_selling_at_material_cost_scores_zero_batch_profit(input_rows, meta):
    """If cost were not subtracted, this would be price x units, which is never 0."""
    row = _first_scoreable_costed_row(input_rows)
    scored = _score_at_price(row, meta, row.material_cost_per_unit)
    assert scored.unscoreable_reason is None
    assert scored.absolute_profit_per_batch == pytest.approx(0.0, abs=1e-6)


def test_selling_above_material_cost_scores_positive_batch_profit(input_rows, meta):
    row = _first_scoreable_costed_row(input_rows)
    scored = _score_at_price(row, meta, row.material_cost_per_unit * 2.0)
    assert scored.unscoreable_reason is None
    assert scored.absolute_profit_per_batch > 0


def test_manufacturing_jobs_are_indexed_from_the_activity_id_column(
    app_session, input_rows, meta
):
    row = input_rows[0]
    job = CorporationIndustryJobsModel(
        corporation_id=1, job_id=1, activity_id=1,
        product_type_id=row.type_id, runs=5, output_quantity=50, status="active",
    )
    app_session.add(job)
    app_session.commit()

    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[job], corp_assets=[], market_depth_cache={},
        weights={}, sell_velocities={row.type_id: 5.0}, meta_resolver=meta,
    )
    assert states[0].has_active_manufacturing_jobs is True


# ── Phase 4 ─────────────────────────────────────────────────────────────────


def test_every_real_row_gets_a_reasoned_decision(run, input_rows):
    assert len(run.decisions) == len(input_rows)
    assert all(d.decision in ("build", "watch", "pause", "skip") for d in run.decisions)
    assert all(d.decision_reason for d in run.decisions)


# ── Phase 5: chain planning ─────────────────────────────────────────────────


def test_bpo_analysis_records_a_value_or_a_skip_reason_never_a_silent_hold(run):
    """With no BPO or BPC owned, every build goes through BPO analysis.

    Before the remediation, the T1 path read a top-level blueprint_type_id that
    no producer writes, so the analysis ran on bp_type_id=0. It then reported
    a confident 'hold' built on zeros.
    """
    top_level = [d for d in run.chain.decisions if not d.is_sub_component]
    assert len(top_level) == len(run.promoted)
    silent = [
        d.type_id for d in top_level
        if d.bpo_analysis_skip_reason is None and d.projected_annual_savings is None
    ]
    assert not silent, f"BPO analysis neither ran nor recorded why not for {silent}"


def test_t1_bpo_analysis_gets_past_the_blueprint_price_lookup(run):
    """The spot price is keyed by the real nested blueprint id. Reaching any
    later step proves the chain planner looked up that id, not 0."""
    t1 = [d for d in run.chain.decisions if not d.is_sub_component and d.meta_group_id == 1]
    if not t1:
        pytest.skip("the captured fixture has no Tech I rows")
    stuck = [
        d.type_id for d in t1
        if (d.bpo_analysis_skip_reason or "").startswith("no BPO market price")
    ]
    assert not stuck, f"BPO price lookup missed the nested blueprint id for {stuck}"


# ── Phase 6: character assignment ───────────────────────────────────────────


def test_a_skilled_pilot_is_assigned_more_than_one_job(run):
    manufactures = [a for a in run.actions if a.action_type == "manufacture"]
    assert len(manufactures) > 1, (
        "one action per pilot means slot capacity is back to the base single slot"
    )
    assert all(a.character_id == 1 for a in manufactures)


def test_manufacture_actions_carry_real_runs_and_cost(run, input_rows):
    manufactures = [a for a in run.actions if a.action_type == "manufacture"]
    assert all(a.runs is not None and a.runs > 0 for a in manufactures)
    # A real overview can list one product more than once (one row per
    # blueprint variant), so match against any of that product's rows.
    wrong_runs = [
        (a.type_id, a.runs) for a in manufactures
        if a.runs not in {r.runs for r in input_rows if r.type_id == a.type_id}
    ]
    assert not wrong_runs, (
        f"manufacture runs do not come from manufacturing_job.runs: {wrong_runs}"
    )
    assert any(
        a.estimated_cost_isk is not None and a.estimated_cost_isk > 0 for a in manufactures
    ), "every manufacture action has an unknown or zero material cost"


# ── Phase 7: shopping list ──────────────────────────────────────────────────


def test_shopping_quantities_scale_with_the_job_run_count(run, meta, admin):
    """Each material is _PER_RUN_QTY units per run in this test's blueprint
    data, so each line must be _PER_RUN_QTY x the action's run count. Buying
    for a single run was the old 'materials for one run of a 20-run job' defect."""
    by_bp = run.blueprint_data
    action = next(
        (
            a for a in run.actions
            if a.action_type == "manufacture"
            and any(
                entry["manufacturing"]["materials"]
                for entry in by_bp.values()
                if entry["manufacturing"]["products"][0]["type_id"] == a.type_id
            )
        ),
        None,
    )
    if action is None:
        pytest.fail("no manufacture action whose real row lists any materials")

    shopping = ShoppingListBuilder().build(
        assigned_actions=[action], corp_assets=[], market_depth_cache=run.market_depth,
        admin_settings=admin, blueprint_data=run.blueprint_data, meta_resolver=meta,
    )
    assert shopping, "the shopping list dropped every material of a priced job"
    assert all(item.shopping_category == "current_job" for item in shopping)
    assert all(item.quantity == _PER_RUN_QTY * action.runs for item in shopping)
