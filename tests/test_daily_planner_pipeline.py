"""Tests for PipelineAnalyzer (Phase 2)."""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputRow
from eve_online_industry_tracker.application.daily_planner.pipeline_analyzer import PipelineAnalyzer
from eve_online_industry_tracker.application.industry.type_metadata import TypeMetadataResolver


class _NoBlueprints:
    def is_blueprint(self, type_id):
        return False

    def prefetch(self, type_ids):
        """No-op: this stub answers is_blueprint() from a fixed rule, not an SDE
        loader -- there is nothing to warm. Real batching (Defect 3, task-18a
        remediation) is proved separately below against a recording loader."""
        return None


class _BlueprintFor:
    def __init__(self, *blueprint_type_ids):
        self._ids = {int(t) for t in blueprint_type_ids}

    def is_blueprint(self, type_id):
        return int(type_id) in self._ids

    def prefetch(self, type_ids):
        return None


def _input_row(**overrides) -> PlannerInputRow:
    base = dict(
        type_id=12345, type_name="Hobgoblin II", quantity=10, runs=20,
        material_cost_per_unit=1_000.0, isk_per_hour=4_000_000.0,
        profit_amount=1_000_000.0, profit_margin_fraction=0.2,
        pipeline_units_in_jobs=0, pipeline_units_on_market=0,
        price_trend_7d_pct=0.0, pipeline_days_supply=None,
        price_trend_30d_pct=None, days_of_supply=5.0,
        meta_group_id=2, blueprint_type_id=None, raw={},
    )
    base.update(overrides)
    return PlannerInputRow(**base)


class TestPipelineAnalyzer:
    def setup_method(self):
        self.analyzer = PipelineAnalyzer()

    def _run(
        self,
        rows,
        industry_jobs=None,
        corp_assets=None,
        market_depth_cache=None,
        weights=None,
        sell_velocities=None,
        meta_resolver=None,
    ):
        return self.analyzer.analyze(
            input_rows=rows,
            industry_jobs=industry_jobs or [],
            corp_assets=corp_assets or [],
            market_depth_cache=market_depth_cache or {},
            weights=weights or {},
            sell_velocities=sell_velocities or {},
            meta_resolver=meta_resolver or _NoBlueprints(),
        )

    def test_effective_velocity_from_sell_velocity(self):
        """When sell_velocity > 0, effective_velocity = sell_velocity × velocity_multiplier."""
        rows = [_input_row(type_id=1, days_of_supply=5.0)]
        weights = {1: MagicMock(velocity_multiplier=1.5)}
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities, weights=weights)
        assert len(states) == 1
        assert abs(states[0].effective_velocity - 15.0) < 0.01, f"Got {states[0].effective_velocity}"

    def test_effective_velocity_fallback_when_zero_velocity(self):
        """When sell_velocity=0, fallback to 1/days_of_supply."""
        rows = [_input_row(type_id=1, days_of_supply=10.0)]
        velocities = {1: 0.0}

        states = self._run(rows, sell_velocities=velocities)
        assert len(states) == 1
        # 1/10 = 0.1
        assert abs(states[0].effective_velocity - 0.1) < 0.001, f"Got {states[0].effective_velocity}"

    def test_effective_velocity_fallback_floored_at_0_01(self):
        """effective_velocity is always >= 0.01."""
        # sell_velocity=0, days_of_supply=0 → both zero → floor at 0.01
        rows = [_input_row(type_id=1, days_of_supply=0.0)]
        velocities = {1: 0.0}

        states = self._run(rows, sell_velocities=velocities)
        assert len(states) == 1
        assert states[0].effective_velocity >= 0.01, f"Got {states[0].effective_velocity}"
        assert abs(states[0].effective_velocity - 0.01) < 0.001

    def test_effective_velocity_minimum_floor_applies(self):
        """Even with velocity=0 and huge days_of_supply, floor at 0.01."""
        rows = [_input_row(type_id=1, days_of_supply=1000.0)]
        velocities = {1: 0.0}

        states = self._run(rows, sell_velocities=velocities)
        # 1/1000 = 0.001 → floored to 0.01
        assert states[0].effective_velocity >= 0.01

    def test_effective_velocity_fallback_unknown_days_of_supply_floors_same_as_zero(self):
        """days_of_supply=None must not be read as "sells out instantly": the velocity
        fallback treats it exactly like a real 0.0 and floors at 0.01, not like a tiny
        positive number (which 1.0/x would blow up into a huge, fabricated velocity)."""
        rows = [_input_row(type_id=1, days_of_supply=None)]
        velocities = {1: 0.0}

        states = self._run(rows, sell_velocities=velocities)
        assert abs(states[0].effective_velocity - 0.01) < 0.001

    def test_days_of_supply_unknown_reports_neutral_not_zero_downstream(self):
        """days_of_supply_current feeds ProfitabilityScorer.pipeline_saturation, where 0.0
        means "nothing in the pipeline" and raises the multiplier above 1.0 — the opposite
        of conservative for a total unknown. An unknown days_of_supply must instead surface
        as that formula's neutral point (3.0 → saturation == 1.0, no adjustment)."""
        rows = [_input_row(type_id=1, days_of_supply=None)]
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities)
        assert states[0].days_of_supply_current == 3.0

    def test_momentum_signal_uses_7d_when_30d_is_none(self):
        """If price_trend_30d_pct is None, momentum_signal = price_trend_7d_pct alone."""
        rows = [_input_row(type_id=1, price_trend_7d_pct=-5.0, price_trend_30d_pct=None)]
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities)
        assert len(states) == 1
        # momentum_signal = -5.0 - (0.0 / 4.0) = -5.0 (30d treated as 0.0)
        assert abs(states[0].momentum_signal - (-5.0)) < 0.01, f"Got {states[0].momentum_signal}"

    def test_momentum_signal_computed_correctly(self):
        """momentum_signal = price_trend_7d - price_trend_30d/4."""
        rows = [_input_row(type_id=1, price_trend_7d_pct=-8.0, price_trend_30d_pct=-4.0)]
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities)
        # momentum = -8 - (-4/4) = -8 + 1 = -7
        assert abs(states[0].momentum_signal - (-7.0)) < 0.01, f"Got {states[0].momentum_signal}"

    def test_competition_index_from_cache(self):
        """competition_index read from market_depth_cache."""
        rows = [_input_row(type_id=1)]
        velocities = {1: 10.0}
        cache_entry = MagicMock()
        cache_entry.competition_index = 2.5
        cache_entry.competitor_units = 1000

        states = self._run(rows, sell_velocities=velocities, market_depth_cache={1: cache_entry})
        assert states[0].competition_index == 2.5

    def test_competition_index_none_when_no_cache(self):
        """competition_index=None when no cache entry for type."""
        rows = [_input_row(type_id=1)]
        velocities = {1: 10.0}

        states = self._run(rows, sell_velocities=velocities, market_depth_cache={})
        assert states[0].competition_index is None

    def test_has_active_manufacturing_jobs_true(self):
        """has_active_manufacturing_jobs=True when active mfg job exists."""
        rows = [_input_row(type_id=1)]
        velocities = {1: 10.0}

        job = MagicMock()
        job.activity_id = 1  # manufacturing
        job.product_type_id = 1
        job.status = "active"
        job.runs = 5
        job.output_quantity = 5
        job.end_date = None

        states = self._run(rows, industry_jobs=[job], sell_velocities=velocities)
        assert states[0].has_active_manufacturing_jobs is True

    def test_has_active_manufacturing_jobs_false(self):
        """has_active_manufacturing_jobs=False when no mfg jobs."""
        rows = [_input_row(type_id=1)]
        velocities = {1: 10.0}

        states = self._run(rows, industry_jobs=[], sell_velocities=velocities)
        assert states[0].has_active_manufacturing_jobs is False

    def test_multiple_rows_returns_multiple_states(self):
        """One PipelineState returned per input row."""
        rows = [
            _input_row(type_id=1),
            _input_row(type_id=2),
            _input_row(type_id=3),
        ]
        velocities = {1: 5.0, 2: 10.0, 3: 15.0}

        states = self._run(rows, sell_velocities=velocities)
        assert len(states) == 3
        type_ids = {s.type_id for s in states}
        assert type_ids == {1, 2, 3}

    def test_velocity_floor_applies_after_multiplier(self):
        """Even if velocity × multiplier < 0.01, floor applies."""
        rows = [_input_row(type_id=1, days_of_supply=5.0)]
        # Very small velocity * small multiplier → should be floored
        weights = {1: MagicMock(velocity_multiplier=0.001)}
        velocities = {1: 0.001}  # 0.001 * 0.001 = 0.000001 < 0.01 → floor

        states = self._run(rows, sell_velocities=velocities, weights=weights)
        assert states[0].effective_velocity >= 0.01

    # ── Task 7: real pipeline numbers (findings 4, 6, cluster B) ───────────────

    def test_pipeline_days_comes_from_the_producer_not_a_reimplementation(self):
        row = _input_row(pipeline_days_supply=12.5, pipeline_units_in_jobs=40,
                          pipeline_units_on_market=60)
        states = PipelineAnalyzer().analyze(
            input_rows=[row], industry_jobs=[], corp_assets=[],
            market_depth_cache={}, weights={}, sell_velocities={row.type_id: 10.0},
            meta_resolver=_NoBlueprints(),
        )
        assert states[0].total_pipeline_days == 12.5

    def test_pipeline_days_falls_back_to_units_over_velocity_when_producer_returns_none(self):
        row = _input_row(pipeline_days_supply=None, pipeline_units_in_jobs=40,
                          pipeline_units_on_market=60)
        states = PipelineAnalyzer().analyze(
            input_rows=[row], industry_jobs=[], corp_assets=[],
            market_depth_cache={}, weights={}, sell_velocities={row.type_id: 10.0},
            meta_resolver=_NoBlueprints(),
        )
        # (40 + 60) / 10.0
        assert states[0].total_pipeline_days == 10.0

    def test_pipeline_days_fallback_does_not_double_count_units_in_manufacturing(self):
        """pipeline_units_in_jobs (industry/service.py:1872-1889) is already
        SUM(output_quantity) over active/ready manufacturing jobs -- it IS the units
        in manufacturing. The fallback numerator must be exactly
        pipeline_units_in_jobs + pipeline_units_on_market, with no separate term
        derived from `industry_jobs`, or those units get counted twice."""
        row = _input_row(pipeline_days_supply=None, pipeline_units_in_jobs=40,
                          pipeline_units_on_market=60)
        job = MagicMock()
        job.activity_id = 1
        job.product_type_id = row.type_id
        job.status = "active"
        job.runs = 5
        job.output_quantity = 25  # would double-count if summed in again

        states = PipelineAnalyzer().analyze(
            input_rows=[row], industry_jobs=[job], corp_assets=[],
            market_depth_cache={}, weights={}, sell_velocities={row.type_id: 10.0},
            meta_resolver=_NoBlueprints(),
        )
        # (40 + 60) / 10.0 == 10.0, unaffected by the job's output_quantity
        assert states[0].total_pipeline_days == 10.0

    def test_bpc_runs_are_read_from_blueprint_runs_on_copy_assets(self):
        row = _input_row(blueprint_type_id=999)
        asset = SimpleNamespace(type_id=999, is_blueprint_copy=True, blueprint_runs=17, quantity=1)
        states = PipelineAnalyzer().analyze(
            input_rows=[row], industry_jobs=[], corp_assets=[asset],
            market_depth_cache={}, weights={}, sell_velocities={},
            meta_resolver=_BlueprintFor(999),
        )
        assert states[0].bpc_runs_available == 17

    def test_a_bpo_is_not_counted_as_bpc_runs(self):
        """A positive blueprint_runs alone must not count as BPC stock -- the
        is_blueprint_copy gate (finding 4's actual fix) has to be what excludes a
        BPO, not the `runs <= 0` check. blueprint_runs=17 (not None/0) here so the
        assertion would fail if is_blueprint_copy were ever ignored."""
        row = _input_row(blueprint_type_id=999)
        bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False, blueprint_runs=17, quantity=1)
        states = PipelineAnalyzer().analyze(
            input_rows=[row], industry_jobs=[], corp_assets=[bpo],
            market_depth_cache={}, weights={}, sell_velocities={},
            meta_resolver=_BlueprintFor(999),
        )
        assert states[0].bpc_runs_available == 0

    def test_bpc_runs_gated_on_meta_resolver_is_blueprint(self):
        """An is_blueprint_copy=True asset with blueprint_runs>0 must still be excluded when
        meta_resolver says its type_id is not actually a blueprint-category item — the
        category check must not be bypassable by the asset's own flags alone."""
        row = _input_row(blueprint_type_id=999)
        asset = SimpleNamespace(type_id=999, is_blueprint_copy=True, blueprint_runs=17, quantity=1)
        states = PipelineAnalyzer().analyze(
            input_rows=[row], industry_jobs=[], corp_assets=[asset],
            market_depth_cache={}, weights={}, sell_velocities={},
            meta_resolver=_NoBlueprints(),
        )
        assert states[0].bpc_runs_available == 0

    def test_price_trend_uses_the_7d_key(self):
        row = _input_row(price_trend_7d_pct=3.5, price_trend_30d_pct=-2.0)
        states = PipelineAnalyzer().analyze(
            input_rows=[row], industry_jobs=[], corp_assets=[],
            market_depth_cache={}, weights={}, sell_velocities={},
            meta_resolver=_NoBlueprints(),
        )
        assert states[0].price_trend_7d_pct == 3.5
        # momentum = 3.5 - (-2.0 / 4)
        assert states[0].momentum_signal == 4.0


class _RecordingLoader:
    """Mirrors tests/test_type_metadata.py's _FakeLoader and
    test_daily_planner_service_helpers.py's _RecordingLoader: records every
    batch of type_ids it was asked to load, so a test can assert it was
    invoked exactly once (batched), not once per corp asset."""

    def __init__(self, data):
        self._data = data
        self.calls: list[list[int]] = []

    def __call__(self, session, language, type_ids):
        self.calls.append(sorted(type_ids))
        return {tid: self._data[tid] for tid in type_ids if tid in self._data}


def test_is_blueprint_lookups_are_prefetched_in_one_batch_not_per_asset():
    """Defect 3 (task-18a remediation): analyze()'s corp-asset loop calls
    meta_resolver.is_blueprint(type_id) per asset. Without an up-front batched
    prefetch, TypeMetadataResolver._entry() self-heals each cache miss with a
    single-id prefetch, so a real resolver would open one SDE session (with
    its metaGroups table reflection) per distinct asset type_id -- measured
    as hundreds of reflected open/query/close cycles against this app's live
    corp_assets table of thousands of rows. A wrong implementation (no prefetch added
    to analyze(), mirroring the pre-fix code) would make loader.calls contain
    one entry per distinct type_id instead of a single batched entry."""
    row = _input_row(blueprint_type_id=999)
    assets = [
        SimpleNamespace(type_id=999, is_blueprint_copy=True, blueprint_runs=5, quantity=1),
        SimpleNamespace(type_id=888, is_blueprint_copy=False, blueprint_runs=None, quantity=1),
        SimpleNamespace(type_id=999, is_blueprint_copy=True, blueprint_runs=3, quantity=1),
    ]
    loader = _RecordingLoader({
        999: {"type_id": 999, "category_id": 9, "meta_group_id": None, "category_name": "Blueprint"},
        888: {"type_id": 888, "category_id": 9, "meta_group_id": None, "category_name": "Blueprint"},
    })
    resolver = TypeMetadataResolver(sde_session_provider=lambda: None, loader=loader)

    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=assets,
        market_depth_cache={}, weights={}, sell_velocities={},
        meta_resolver=resolver,
    )

    assert loader.calls == [[888, 999]], "the loader must be invoked exactly once, batched"
    assert states[0].bpc_runs_available == 8


def test_the_analyzer_clamps_an_out_of_band_velocity_multiplier():
    states = PipelineAnalyzer().analyze(
        input_rows=[_input_row(type_id=1)], industry_jobs=[], corp_assets=[],
        market_depth_cache={}, weights={1: SimpleNamespace(type_id=1, velocity_multiplier=10.0)},
        sell_velocities={1: 2.0}, meta_resolver=_NoBlueprints(),
    )
    assert states[0].effective_velocity == 8.0  # 2.0 x 4.0, not 2.0 x 10.0


def _analyze_one(row, sell_velocities):
    return PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=[], market_depth_cache={},
        weights={}, sell_velocities=sell_velocities, meta_resolver=_NoBlueprints(),
    )[0]


def test_no_velocity_signal_is_flagged_not_just_floored():
    state = _analyze_one(_input_row(type_id=1, days_of_supply=None), {})
    assert state.effective_velocity == 0.01
    assert state.velocity_unknown_reason == "no corp sales in 30 days and no days-of-supply estimate"


def test_a_measured_velocity_has_no_unknown_reason():
    assert _analyze_one(_input_row(type_id=1), {1: 2.0}).velocity_unknown_reason is None


def test_a_days_of_supply_fallback_has_no_unknown_reason():
    state = _analyze_one(_input_row(type_id=1, days_of_supply=5.0), {})
    assert abs(state.effective_velocity - 0.2) < 1e-12
    assert state.velocity_unknown_reason is None


def test_a_tiny_measured_velocity_floored_to_0_01_is_still_a_signal():
    state = _analyze_one(_input_row(type_id=1), {1: 0.001})
    assert state.effective_velocity == 0.01
    assert state.velocity_unknown_reason is None


def test_a_failed_sell_history_is_the_recorded_reason_when_nothing_else_is_known():
    state = PipelineAnalyzer().analyze(
        input_rows=[_input_row(type_id=1, days_of_supply=None)], industry_jobs=[],
        corp_assets=[], market_depth_cache={}, weights={}, sell_velocities={},
        sell_velocity_unavailable={1: "sell history query failed (OperationalError)"},
        meta_resolver=_NoBlueprints(),
    )[0]
    assert state.velocity_unknown_reason == (
        "sell history query failed (OperationalError) and no days-of-supply estimate"
    )
