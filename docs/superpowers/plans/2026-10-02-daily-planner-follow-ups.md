# Daily Planner Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the deferred follow-ups from the daily-planner remediation. That means silent-failure gaps (contract errors swallowed per item, unclamped learned weights, wallet unknown read as 0, a velocity floor constant), accuracy of the numbers (ME batch ceiling, sub-manufacture stock and ME, the realized-cost comparison), and visibility, tests and clean-up.

**Architecture:** No new phases and no phase reorder. `PlannerInputError` stops being a `ValueError`, so per-item handlers can no longer catch it. One new module, `daily_planner/learning_weights.py`, owns the weight band on both write and read. The SDE blueprint helpers gain a batch-level ME formula that mirrors the producer's rounding exactly. The chain planner uses it for BPO savings, optimal ME and sub-job inputs. Unknown values (wallet, velocity, BPO analysis) are carried as `None` plus a recorded reason, persisted where the UI needs them, and rendered as "unknown", not 0.

**Tech Stack:** Python 3.10–3.12, SQLAlchemy 2.x (`Mapped`/`mapped_column`), SQLite, pytest, Streamlit.

**Spec:** `docs/superpowers/specs/2026-09-17-daily-planner-remediation-design.md`. Prior controller rulings, which this plan does not re-open, are in `docs/superpowers/plans/2026-09-17-daily-planner-remediation-rulings.md`.

**Branch principle (binding, from the spec):** a missing input skips with a recorded reason, logs a WARNING, or fails loud. It never falls back to a constant.

## Global Constraints

- Python `>=3.10,<3.13`, with `from __future__ import annotations` in new modules.
- Do NOT modify the overview-row producer `src/eve_online_industry_tracker/application/industry/service.py`. The planner adapts to the producer. Reading it, and importing its static helpers in tests, is fine.
- No new third-party dependencies.
- Migrations are additive and idempotent, via `_ensure_column(db, table=, column=, ddl_type=)` in `infrastructure/schema_migrations.py`.
- SQLAlchemy models use the `Mapped[Optional[X]] = mapped_column(...)` style.
- The repo is PUBLIC: no real corp/character data, wallet balances, or live-DB counts in code, comments or tests. `tests/test_no_live_values_in_repo.py` guards this.
- Tests are flat `tests/test_*.py` files. The runner is `.venv312/bin/python -m pytest`. The baseline on main is 577 passed, 0 failed, with the local gitignored fixture `tests/fixtures/overview_rows_real.json` present. Without it, fixture tests skip.
- `tests/test_daily_planner_fail_loud.py` is an AST guard. It bans broad excepts (one allowed, in service.py `_run_compute`) and phantom row-key string literals (e.g. never write the literal `"material_cost_per_unit"`, `"market_price"` or `"estimated_material_cost"` in package code). New code must pass it.
- `*.md` is gitignored repo-wide. This plan needs `git add -f`.
- Every commit message ends with:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`

## Review Focus

1. **A learned-weight row whose value is NaN, text, `None` or a bool** (SQLite REAL affinity keeps non-numeric text verbatim). Expected: a neutral 1.0 with a WARNING naming the weight and type_id. The item stays in the plan; today `float("abc")` raises inside Phase 3's per-item handler and the item silently disappears. Pinned in Task 2 (`test_a_non_numeric_stored_weight_is_neutral_and_warned`, `test_an_item_with_a_text_weight_is_still_scored`).
2. **A `corporations.wallets` value of an unexpected shape**: a JSON scalar (`"5"`, `"null"`), undecodable text, or a division that is not an integer. Expected: wallet unknown (`None`), never 0.0, and never a crash. Today a non-numeric `division` raises `ValueError` out of phase 1. Pinned in Task 3.
3. **runs=1 vs runs=1000 for the same base quantity at the same ME.** Expected: the planner's batch quantity is exactly what the producer would compute. Pinned in Task 5 by a grid test against `IndustryService._round_material_quantity` / `_combine_reductions`.
4. **A component already fully (or partly) in corp stock.** Expected: no sub-manufacture decision when stock covers the need, and only the remainder built when it partly does. Pinned in Task 6.
5. **A sub-manufacture blueprint with zero SDE materials, or whose products do not include the component.** Expected: no sub-build (a zero-material job looks "free" and would always win build-vs-buy), with a WARNING. Today `output_per_run` falls back to 1 and the cost to 0.0. Pinned in Task 6.

---

## File Structure

**Created:**

| Path | Responsibility |
|---|---|
| `src/eve_online_industry_tracker/application/daily_planner/learning_weights.py` | The learned-weight band (`LEARNING_WEIGHT_MIN/MAX`), `clamp_weight()` on write, `read_weight()` on read. |
| `tests/test_planner_input_error_propagates.py` | Every per-item handler lets a `PlannerInputError` through. |
| `tests/test_learning_weights.py` | Read-side clamp and non-numeric handling. |
| `tests/test_daily_planner_wallet_display.py` | UI helpers render an unknown wallet as unknown. |

**Modified:**

| Path | Change |
|---|---|
| `application/daily_planner/input_row.py` | `PlannerInputError(Exception)`; public `require_batch_runs()` |
| `application/daily_planner/feedback_processor.py` | uses `learning_weights`; (Task 7 branch A) like-for-like cost comparison |
| `application/daily_planner/profitability_scorer.py`, `pipeline_analyzer.py` | `read_weight()`; velocity-unknown reason |
| `application/daily_planner/models.py` | `PipelineState.velocity_unknown_reason`, `ItemDecision.velocity_unknown_reason` |
| `application/daily_planner/item_decision_engine.py` | passes `velocity_unknown_reason` through |
| `application/daily_planner/chain_planner.py` | velocity skip, batch ME saving, optimal ME at run count, sub-manufacture stock netting + BPO ME, T3 source order |
| `application/daily_planner/character_assigner.py` | two-pass ordering, sub action runs/materials, research skip, copy gate, `_batch_runs` dedupe |
| `application/daily_planner/shopping_list_builder.py` | module-level `build_corp_stock_map`, sub job buys its own batch materials, missing-price WARNING |
| `application/daily_planner/service.py` | wallet unknown, corp stock in phase 1, persist skip reason, `_now()`, compute-step labels, per-item sell-history reasons |
| `application/daily_planner/fixture_export.py` | allow-list `blueprint_source_kind` values |
| `infrastructure/sde/blueprints.py` | `me_adjusted_batch_quantity`, `runs=` on `optimal_me_for_quantities` / `compute_optimal_me` |
| `infrastructure/models.py`, `infrastructure/schema_migrations.py` | `build_plan_item.bpo_analysis_skip_reason`; (Task 7 branch A) `daily_action_log.estimated_build_cost_isk` |
| `streamlit_ui/components/daily_planner/status_bar.py`, `tab_shopping.py`, `tab_build_plan.py`, `tab_actions.py` | unknown wallet, skip reason, action-type sets |
| tests | as listed per task |

---

# Batch 1 — silent-failure gaps

### Task 1: A contract violation can no longer be swallowed per item

`PlannerInputError` subclasses `ValueError` (`input_row.py:80`). The per-item handlers that catch `ValueError` are: `pipeline_analyzer.py:137`, `service.py:639` (phase 3), `service.py:673` (phase 4), `chain_planner.py:227` and `:239`, and `feedback_processor.py:121`. Today nothing raises `PlannerInputError` beneath them. The only raiser outside `input_row.py` is `character_assigner._batch_runs`, called from `assign()`, which has no handler. So this is latent, but a single future call under any of those handlers would turn a contract violation into a logged-and-skipped item. Fix: `PlannerInputError` stops subclassing `ValueError`. Re-raising at six sites would be six places to forget. No caller catches it as `ValueError` on purpose (verified: `grep -rn PlannerInputError src tests`). `_run_compute` already catches it explicitly before the broad handler.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/input_row.py:80-87`
- Test: `tests/test_planner_input_error_propagates.py` (create), `tests/test_daily_planner_status_banner.py`

**Interfaces:**
- Produces: `class PlannerInputError(Exception)`, with the same constructor `(*, type_id, field, detail)` and attributes `.type_id`, `.field`, `.detail`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_planner_input_error_propagates.py
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
```

Append to `tests/test_daily_planner_status_banner.py`. It reuses `_bare_service` from that file:

```python
def test_a_contract_violation_inside_a_per_item_block_still_reaches_the_banner():
    """Phase 3 isolates items with `except (TypeError, ValueError)`. A
    PlannerInputError from inside that block must still fail the compute
    with the red-banner message, not be logged and skipped."""
    svc = _bare_service()
    svc._phase_1_collect = lambda: {
        "overview_rows": [{"type_id": 12345}],
        "input_rows": [SimpleNamespace(type_id=12345)],
        "weights": {}, "market_depth_cache": {}, "margin_correlations": {},
        "trit_trend_7d": None,
    }
    svc._phase_2_pipeline = lambda phase1: [SimpleNamespace(type_id=12345)]

    def violate(**_kwargs):
        raise PlannerInputError(type_id=12345, field="manufacturing_job.runs", detail="is missing")

    svc._profitability_scorer = SimpleNamespace(score=violate)
    for name in ("_phase_4_decide", "_phase_5_chain", "_phase_6_assign",
                 "_phase_7_shopping", "_phase_8_actions"):
        setattr(svc, name, lambda *a, **k: [])
    svc._phase_9_persist = lambda **k: None

    svc._run_compute()

    status = svc.get_compute_status()
    assert status["status"] == "failed"
    assert "'manufacturing_job.runs'" in status["error"]
    assert "type_id=12345" in status["error"]
    assert "refresh the product overview" in status["error"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_planner_input_error_propagates.py tests/test_daily_planner_status_banner.py -v`
Expected: FAIL. `test_planner_input_error_is_not_a_value_error` fails its assert. Every `pytest.raises` test fails with "DID NOT RAISE". The banner test fails on `status["status"] == "failed"` (it is `"done"`).

- [ ] **Step 3: Make it not a ValueError**

Replace `input_row.py:80-81`:

```python
class PlannerInputError(Exception):
    """An overview row does not satisfy the planner's input contract.

    Deliberately NOT a ValueError subclass. Phases 2-5 and feedback isolate
    one bad item with `except (TypeError, ValueError)` (a malformed SDE or
    market number). A contract violation raised beneath one of those handlers
    must not be logged and skipped like a bad number: it has to reach
    DailyPlannerService._run_compute, which turns it into the red banner.
    """
```

Keep the `__init__` body unchanged.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv312/bin/python -m pytest tests/test_planner_input_error_propagates.py tests/test_daily_planner_status_banner.py tests/test_planner_input_row.py tests/test_daily_planner_fail_loud.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv312/bin/python -m pytest -q`
Expected: 0 failed.

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/input_row.py tests/test_planner_input_error_propagates.py tests/test_daily_planner_status_banner.py
git commit -m "fix: stop per-item ValueError handlers from swallowing a contract violation

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Clamp learned weights on read, through one shared helper

Weights are clamped on write (`feedback_processor._clamp_weight`), but read raw at `profitability_scorer.py:50-52`, `pipeline_analyzer.py:159`, `feedback_processor.py:184-186` (the old value fed into the EMA) and `service.py:368-370` (analytics display). The band moves to a new module that every reader and the writer import. `feedback_processor` keeps re-exporting `LEARNING_WEIGHT_MIN/MAX`, because `tests/test_daily_planner_feedback.py` imports them from there.

A `None` weights row means "no feedback yet", and its defined value is the neutral 1.0 (the column's own `DEFAULT 1.0`). A present row whose value is not a finite number is corrupt. It logs a WARNING and reads as neutral (see Open questions).

**Files:**
- Create: `src/eve_online_industry_tracker/application/daily_planner/learning_weights.py`
- Modify: `feedback_processor.py:21-33,184-186,202-264`, `profitability_scorer.py:50-52`, `pipeline_analyzer.py:156-159`, `service.py:368-370`
- Test: `tests/test_learning_weights.py` (create), `tests/test_daily_planner_scoring.py`, `tests/test_daily_planner_pipeline.py`, `tests/test_daily_planner_feedback.py`

**Interfaces:**
- Produces: `learning_weights.LEARNING_WEIGHT_MIN: float = 0.25`, `LEARNING_WEIGHT_MAX: float = 4.0`, `NEUTRAL_WEIGHT: float = 1.0`, `clamp_weight(value: float) -> float`, `read_weight(weights: Any | None, name: str) -> float`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_learning_weights.py
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.daily_planner.learning_weights import (
    LEARNING_WEIGHT_MAX,
    LEARNING_WEIGHT_MIN,
    NEUTRAL_WEIGHT,
    clamp_weight,
    read_weight,
)


def test_the_band_is_unchanged():
    assert (LEARNING_WEIGHT_MIN, LEARNING_WEIGHT_MAX, NEUTRAL_WEIGHT) == (0.25, 4.0, 1.0)
    assert clamp_weight(9.0) == 4.0
    assert clamp_weight(0.1) == 0.25


def test_no_weights_row_reads_as_neutral():
    assert read_weight(None, "accuracy_ema") == 1.0


@pytest.mark.parametrize("stored, expected", [
    (6.8, 4.0), (0.0, 0.25), (-2.0, 0.25), (1.3, 1.3), (float("inf"), 4.0), (2, 2.0),
])
def test_a_stored_weight_is_clamped_on_read(stored, expected):
    w = SimpleNamespace(type_id=1, velocity_multiplier=stored)
    assert read_weight(w, "velocity_multiplier") == expected


@pytest.mark.parametrize("stored", [float("nan"), "abc", "1.5", None, True])
def test_a_non_numeric_stored_weight_is_neutral_and_warned(stored, caplog):
    w = SimpleNamespace(type_id=7, cost_multiplier=stored)
    with caplog.at_level(logging.WARNING):
        assert read_weight(w, "cost_multiplier") == 1.0
    assert any(
        "cost_multiplier" in r.getMessage() and "type_id=7" in r.getMessage()
        for r in caplog.records if r.levelno == logging.WARNING
    )
```

Append to `tests/test_daily_planner_scoring.py`:

```python
def test_the_scorer_clamps_an_out_of_band_stored_weight():
    scored = ProfitabilityScorer().score(
        _make_pipeline(days_of_supply=3.0), _input_row(isk_per_hour=10_000_000),
        _make_weights(accuracy_ema=10.0, velocity_multiplier=-1.0), None, None,
    )
    assert scored.accuracy_ema == 4.0
    assert scored.velocity_multiplier == 0.25


def test_an_item_with_a_text_weight_is_still_scored():
    """float('abc') used to raise inside Phase 3's per-item handler and drop the item."""
    scored = ProfitabilityScorer().score(
        _make_pipeline(days_of_supply=3.0), _input_row(isk_per_hour=10_000_000),
        _make_weights(cost_multiplier="abc"), None, None,
    )
    assert scored.cost_multiplier == 1.0
```

Append to `tests/test_daily_planner_pipeline.py`:

```python
def test_the_analyzer_clamps_an_out_of_band_velocity_multiplier():
    from types import SimpleNamespace
    states = PipelineAnalyzer().analyze(
        input_rows=[_input_row(type_id=1)], industry_jobs=[], corp_assets=[],
        market_depth_cache={}, weights={1: SimpleNamespace(type_id=1, velocity_multiplier=10.0)},
        sell_velocities={1: 2.0}, meta_resolver=_NoBlueprints(),
    )
    assert states[0].effective_velocity == 8.0  # 2.0 x 4.0, not 2.0 x 10.0
```

Add as a method of `TestFeedbackProcessor` in `tests/test_daily_planner_feedback.py`:

```python
    def test_an_out_of_band_stored_weight_is_clamped_before_the_ema(self):
        """A hand-edited 10.0 must enter the EMA as 4.0: 0.8*4.0 + 0.2*0.8 = 3.36,
        not 0.8*10.0 + 0.2*0.8 = 8.16 clamped to 4.0."""
        action = _make_action(type_id=220)
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_weights.return_value = {220: _make_weights(accuracy_ema=10.0)}
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=220, isk_per_hour=10_000_000)]
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 8_000_000.0, "material_cost": None,
            "priced_quantity": 0, "sell_days": 5.0,
        }
        self.processor.process_pending_feedback()
        weights = self.repo.upsert_weights.call_args[0][0]
        assert abs(weights.accuracy_ema - (0.8 * 4.0 + 0.2 * 0.8)) < 1e-9
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_learning_weights.py tests/test_daily_planner_scoring.py tests/test_daily_planner_pipeline.py tests/test_daily_planner_feedback.py -v`
Expected: FAIL. `ModuleNotFoundError: ...learning_weights`, `accuracy_ema == 10.0`, `ValueError: could not convert string to float: 'abc'`, `effective_velocity == 20.0`, `accuracy_ema == 4.0`.

- [ ] **Step 3: Create the module**

```python
# src/eve_online_industry_tracker/application/daily_planner/learning_weights.py
"""The learned per-type weights' band, enforced on write AND on read.

accuracy_ema, velocity_multiplier and cost_multiplier each multiply a
product's adjusted_score directly, and they persist in plan_learning_weights.
FeedbackProcessor clamps every EMA write, so no new out-of-band value can be
stored. But a hand-edited or pre-clamp row would still be read raw, so every
reader goes through read_weight() as well.
"""
from __future__ import annotations

import logging
import math
from typing import Any

logger = logging.getLogger(__name__)

# The admin settings schema defines no bounds for learning weights, hence
# named constants: a factor of 4 either way from neutral.
LEARNING_WEIGHT_MIN = 0.25
LEARNING_WEIGHT_MAX = 4.0
#: The starting value of every weight (the columns' DEFAULT 1.0): no adjustment.
NEUTRAL_WEIGHT = 1.0


def clamp_weight(value: float) -> float:
    """`value` limited to [LEARNING_WEIGHT_MIN, LEARNING_WEIGHT_MAX]."""
    return max(LEARNING_WEIGHT_MIN, min(LEARNING_WEIGHT_MAX, value))


def read_weight(weights: Any | None, name: str) -> float:
    """The stored weight `name` of a plan_learning_weights row, clamped to the band.

    `weights is None` means the type has no feedback yet. Its defined value
    is NEUTRAL_WEIGHT, not a fallback. A row whose value is not a finite real
    number (NaN, text kept by SQLite's REAL affinity, None, a bool) is corrupt.
    It reads as neutral with a WARNING, so the item is still scored instead
    of being dropped by float() raising inside a per-item handler.
    """
    if weights is None:
        return NEUTRAL_WEIGHT
    raw = getattr(weights, name, None)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)) or math.isnan(raw):
        logger.warning(
            "Daily planner: learned weight %s=%r for type_id=%s is not a number; "
            "reading it as neutral %.1f",
            name, raw, getattr(weights, "type_id", "?"), NEUTRAL_WEIGHT,
        )
        return NEUTRAL_WEIGHT
    return clamp_weight(float(raw))
```

- [ ] **Step 4: Use it everywhere**

`feedback_processor.py`: delete lines 21-33 (the comment block, the two constants and `_clamp_weight`), and add this import after the existing `infrastructure.models` import:

```python
from eve_online_industry_tracker.application.daily_planner.learning_weights import (  # noqa: F401 -- MIN/MAX re-exported for callers
    LEARNING_WEIGHT_MAX,
    LEARNING_WEIGHT_MIN,
    clamp_weight,
    read_weight,
)
```

Then replace every `_clamp_weight(` with `clamp_weight(` (four sites, lines 202, 207, 228, 264). Replace lines 184-186:

```python
        old_accuracy_ema: float = read_weight(existing, "accuracy_ema")
        old_velocity: float = read_weight(existing, "velocity_multiplier")
        old_cost: float = read_weight(existing, "cost_multiplier")
```

`profitability_scorer.py`: add `from eve_online_industry_tracker.application.daily_planner.learning_weights import read_weight` to the imports and replace lines 50-52:

```python
        accuracy_ema = read_weight(weights, "accuracy_ema")
        velocity_multiplier = read_weight(weights, "velocity_multiplier")
        cost_multiplier = read_weight(weights, "cost_multiplier")
```

`pipeline_analyzer.py`: add the same import and replace lines 156-159:

```python
        velocity_multiplier = read_weight(weights.get(type_id), "velocity_multiplier")
```

`service.py` `get_analytics`: add the same import and replace lines 368-370:

```python
                "accuracy_ema": read_weight(w, "accuracy_ema"),
                "velocity_multiplier": read_weight(w, "velocity_multiplier"),
                "cost_multiplier": read_weight(w, "cost_multiplier"),
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv312/bin/python -m pytest tests/test_learning_weights.py tests/test_daily_planner_scoring.py tests/test_daily_planner_pipeline.py tests/test_daily_planner_feedback.py tests/test_daily_planner_fail_loud.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite, then commit**

Run: `.venv312/bin/python -m pytest -q`. Expected: 0 failed.

```bash
git add src/eve_online_industry_tracker/application/daily_planner/learning_weights.py src/eve_online_industry_tracker/application/daily_planner/feedback_processor.py src/eve_online_industry_tracker/application/daily_planner/profitability_scorer.py src/eve_online_industry_tracker/application/daily_planner/pipeline_analyzer.py src/eve_online_industry_tracker/application/daily_planner/service.py tests/test_learning_weights.py tests/test_daily_planner_scoring.py tests/test_daily_planner_pipeline.py tests/test_daily_planner_feedback.py
git commit -m "fix: clamp learned weights on read through one shared helper

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: An unreadable corp wallet is unknown, never 0.0

`service.py` returns 0.0 when listing corporations fails (`:829`), when there is no corporation (`:832`), when the wallets JSON does not decode (`:1307`), and when division 1 is absent (`:1317`). A non-integer `division` raises `ValueError` out of phase 1 (`:1311`). Every one of these cases becomes `None`.

Wallet consumers checked:
- `_phase_9_persist` already writes `None` as NULL. Its `phase1_data.get("corp_wallet", 0.0)` default becomes `None`.
- `status_bar.py:259` and `tab_shopping.py:186` coerce `None` to 0.0 with `or 0.0` and must not.
- `BuildPlanModel.corp_wallet_snapshot` is nullable.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py:760-765,816-841,1283-1317`
- Modify: `src/streamlit_ui/components/daily_planner/status_bar.py:259`, `src/streamlit_ui/components/daily_planner/tab_shopping.py:186-232,263-268`
- Test: `tests/test_daily_planner_service_helpers.py`, `tests/test_daily_planner_wallet_display.py` (create)

**Interfaces:**
- Produces: `_select_division_one_balance(wallets: Any) -> float | None` (now `None` whenever no parseable division-1 balance exists). `DailyPlannerService._get_corp_wallet() -> float | None`. `status_bar.wallet_snapshot(plan_meta: dict) -> float | None`. `tab_shopping.budget_fit(corp_wallet: float | None, total_isk: float, cumul_isk: float) -> str` (`"Yes"`/`"No"`/`"Unknown"`).

- [ ] **Step 1: Write the failing tests**

In `tests/test_daily_planner_service_helpers.py`, replace `test_wallet_balance_is_zero_when_division_one_is_absent` and `test_wallet_balance_is_zero_for_none` with the block below, and append the rest:

```python
def test_wallet_balance_is_unknown_when_division_one_is_absent():
    assert _select_division_one_balance([{"division": 2, "balance": "1"}]) is None
    assert _select_division_one_balance(None) is None


def test_wallet_balance_is_unknown_when_the_json_does_not_decode():
    assert _select_division_one_balance("{not json") is None


def test_wallet_balance_is_unknown_for_a_json_scalar():
    assert _select_division_one_balance(json.dumps(5)) is None
    assert _select_division_one_balance("null") is None


def test_a_non_numeric_division_is_skipped_not_raised():
    assert _select_division_one_balance(
        [{"division": "x", "balance": "1"}, {"division": "1", "balance": "2.5"}]
    ) == 2.5
    assert _select_division_one_balance([{"division": "x", "balance": "1"}]) is None


def _wallet_service(list_corporations):
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService
    return DailyPlannerService(
        industry_service=SimpleNamespace(),
        corporations_service=SimpleNamespace(list_corporations=list_corporations),
        characters_service=SimpleNamespace(), sales_history_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(), realized_profit_service=SimpleNamespace(),
        repo=SimpleNamespace(), admin_settings=SimpleNamespace(),
        session_provider=SimpleNamespace(),
    )


def test_a_failed_corporation_listing_makes_the_wallet_unknown():
    def boom():
        raise ValueError("no corporations cached")
    assert _wallet_service(boom)._get_corp_wallet() is None


def test_no_corporation_makes_the_wallet_unknown():
    assert _wallet_service(lambda: [])._get_corp_wallet() is None


def test_a_corp_without_division_one_has_an_unknown_wallet():
    corps = [{"wallets": [{"division": 2, "balance": "5"}]}]
    assert _wallet_service(lambda: corps)._get_corp_wallet() is None
```

```python
# tests/test_daily_planner_wallet_display.py
"""An unknown corp wallet renders as unknown, never as a 0 ISK balance."""
from __future__ import annotations

from streamlit_ui.components.daily_planner.status_bar import _fmt_isk, wallet_snapshot
from streamlit_ui.components.daily_planner.tab_shopping import budget_fit


def test_an_unknown_wallet_snapshot_stays_unknown():
    assert wallet_snapshot({"corp_wallet_snapshot": None}) is None
    assert wallet_snapshot({}) is None
    assert _fmt_isk(wallet_snapshot({})) == "—"


def test_a_real_zero_wallet_is_still_zero():
    assert wallet_snapshot({"corp_wallet_snapshot": 0.0}) == 0.0


def test_budget_fit_is_unknown_without_a_wallet():
    assert budget_fit(None, 100.0, 50.0) == "Unknown"


def test_budget_fit_compares_the_remaining_wallet_to_the_cumulative_cost():
    assert budget_fit(1000.0, 400.0, 600.0) == "Yes"
    assert budget_fit(1000.0, 400.0, 601.0) == "No"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_service_helpers.py tests/test_daily_planner_wallet_display.py -v`
Expected: FAIL. `0.0 is None` assertions fail, `ValueError: invalid literal for int()` comes from the non-numeric division, and `ImportError: cannot import name 'wallet_snapshot'`.

- [ ] **Step 3: Implement the service side**

Replace `_get_corp_wallet` (`service.py:816-841`):

```python
    def _get_corp_wallet(self) -> float | None:
        """Master wallet (division 1) balance, or None when it is unknown.

        Unknown covers: listing corporations failed, there is no corporation,
        and the corporation has no parseable division-1 balance (see
        _select_division_one_balance). A real 0 ISK balance and "we could not
        read the wallet" must stay distinguishable, so nothing here returns 0.0
        unless the balance itself is 0.
        """
        try:
            corps = self._corporations.list_corporations()
        except (KeyError, TypeError, ValueError):
            logger.exception("DailyPlannerService: failed to list corporations; corp wallet unknown")
            return None

        if not corps:
            logger.warning("DailyPlannerService: no corporation listed; corp wallet unknown")
            return None
        corp = corps[0] if isinstance(corps, list) else corps
        wallets = corp.get("wallets") if isinstance(corp, dict) else getattr(corp, "wallets", None)
        balance = _select_division_one_balance(wallets)
        if balance is None:
            logger.warning(
                "DailyPlannerService: no parseable division-1 wallet balance "
                "(absent, undecodable or malformed); corp wallet unknown, not zero"
            )
        return balance
```

Replace the body of `_select_division_one_balance` (`service.py:1283-1317`). Keep the signature and the first paragraph of the docstring, and replace its last paragraph:

```python
    Returns `None` whenever there is no parseable division-1 balance: the
    payload does not decode, decodes to neither a list nor a dict, has no
    division-1 entry, or has one whose balance `_parse_isk` cannot read.
    An entry whose `division` is not an integer is skipped, not raised.
    Only a balance that parses (including a real `"0"`) is a number.
    """
    import json as _json

    for _ in range(_MAX_WALLET_DECODE_ITERATIONS):
        if not isinstance(wallets, str):
            break
        try:
            wallets = _json.loads(wallets)
        except ValueError:
            return None

    if isinstance(wallets, list):
        for entry in wallets:
            if not isinstance(entry, dict):
                continue
            try:
                division = int(entry.get("division") or 0)
            except (TypeError, ValueError):
                continue
            if division == 1:
                return _parse_isk(entry.get("balance"))
    elif isinstance(wallets, dict):
        raw = wallets.get("1", wallets.get(1))
        if raw is not None:
            return _parse_isk(raw)
    return None
```

In `_phase_9_persist`, replace lines 760-765:

```python
        # Corp wallet: None means unknown (see _get_corp_wallet) and must reach
        # the DB as NULL. corp_wallet_snapshot is nullable for exactly this.
        _raw_corp_wallet = phase1_data.get("corp_wallet")
        corp_wallet = None if _raw_corp_wallet is None else float(_raw_corp_wallet)
```

- [ ] **Step 4: Implement the UI side**

`status_bar.py`: add below `_fmt_isk`:

```python
def wallet_snapshot(plan_meta: dict[str, Any]) -> float | None:
    """The plan's corp wallet snapshot, or None when it was unknown at compute time."""
    raw = plan_meta.get("corp_wallet_snapshot")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
```

and replace line 259 with `corp_wallet = wallet_snapshot(plan_meta)`. `_fmt_isk(None)` already renders "—".

`tab_shopping.py`: change line 9 to `from streamlit_ui.components.daily_planner.status_bar import _fmt_isk, wallet_snapshot`, and add a module-level helper:

```python
def budget_fit(corp_wallet: float | None, total_isk: float, cumul_isk: float) -> str:
    """Whether a BPO fits what the wallet has left after the shopping list."""
    if corp_wallet is None:
        return "Unknown"
    return "Yes" if (corp_wallet - total_isk) >= cumul_isk else "No"
```

Then in `render_tab_shopping` (or the render function holding lines 186-268):

```python
    corp_wallet = wallet_snapshot(plan_meta)
```

```python
    wallet_short = corp_wallet is not None and corp_wallet > 0 and total_isk > corp_wallet
```

```python
    remaining = (corp_wallet - total_isk) if corp_wallet is not None else None
```

```python
            st.metric("Remaining", _fmt_isk(remaining) if corp_wallet is not None and corp_wallet > 0 else "—")
```

```python
            row["Fits Budget?"] = budget_fit(corp_wallet, total_isk, cumul_isk)
```

The `fits = ...` line goes away. `_fmt_isk(corp_wallet)` for the "Corp Wallet" metric stays as is, because it renders `None` as "—". The "Deficit" branch only runs when `wallet_short`, so `remaining` is a number there.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_service_helpers.py tests/test_daily_planner_wallet_display.py tests/test_daily_planner_end_to_end.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite, then commit**

Run: `.venv312/bin/python -m pytest -q`. Expected: 0 failed.

```bash
git add src/eve_online_industry_tracker/application/daily_planner/service.py src/streamlit_ui/components/daily_planner/status_bar.py src/streamlit_ui/components/daily_planner/tab_shopping.py tests/test_daily_planner_service_helpers.py tests/test_daily_planner_wallet_display.py
git commit -m "fix: treat an unreadable corp wallet as unknown, not 0 ISK

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Replace the BPO velocity floor with a recorded skip

`chain_planner.py:618` floors velocity at `max(0.033, effective_velocity)`. The velocity reaching it is never 0: `PipelineAnalyzer` already floors at 0.01 when it has no signal at all (no own sales, no days of supply, `pipeline_analyzer.py:165-172`). So "unknown" cannot be detected at the chain planner today. The analyzer now records *why* it floored, on `PipelineState.velocity_unknown_reason`. The decision engine carries it to `ItemDecision`, and the chain planner skips the analysis on it. A measured velocity, however slow, is used as-is.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/models.py:9-22,49-77`
- Modify: `pipeline_analyzer.py:161-172,220-232`, `item_decision_engine.py` (the five `ItemDecision(` calls in `decide()`), `chain_planner.py:609-619`
- Test: `tests/test_daily_planner_pipeline.py`, `tests/test_daily_planner_decisions.py`, `tests/test_chain_planner_keying.py`

**Interfaces:**
- Produces: `PipelineState.velocity_unknown_reason: str | None = None` and `ItemDecision.velocity_unknown_reason: str | None = None`. Non-None means `effective_velocity` is the analyzer's 0.01 floor, not a signal. The analyzer's reason string when it has no signal is exactly `f"{why_no_sales} and no days-of-supply estimate"`, with `why_no_sales = "no corp sales in 30 days"` (Task 11 adds other `why_no_sales` values).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_daily_planner_pipeline.py`:

```python
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
```

Append to `tests/test_daily_planner_decisions.py`:

```python
def test_the_decision_carries_the_pipelines_velocity_unknown_reason():
    import dataclasses
    reason = "no corp sales in 30 days and no days-of-supply estimate"
    pipeline = dataclasses.replace(_make_pipeline(), velocity_unknown_reason=reason)
    for scored in (_make_scored(), _make_scored(unscoreable_reason="no cost basis")):
        decision = ItemDecisionEngine().decide(
            scored=scored, pipeline=pipeline, overview_row=_make_overview_row(),
            admin_settings=_make_admin(),
        )
        assert decision.velocity_unknown_reason == reason
```

Append to `tests/test_chain_planner_keying.py`, after the BPO investment tests:

```python
def test_an_unknown_sell_velocity_skips_the_analysis_with_a_reason():
    row = _bpo_row(me_current=0)
    decision = _decision(
        row, effective_velocity=0.01,
        velocity_unknown_reason="no corp sales in 30 days and no days-of-supply estimate",
    )
    phase1 = {"bpc_assets_by_type_id": {999: [object()]}, "blueprint_data": _bpo_bp_data(1000),
              "market_depth_cache": {999: {"spot_sell_price": 20_000.0}}}
    plan = _planner().plan_chain([decision], phase1)
    _assert_skipped(decision, plan.bpo_opportunities, "sell velocity unknown")


def test_a_zero_sell_velocity_skips_the_analysis_with_a_reason():
    decision, opps = _analyse(_bpo_row(me_current=0), _bpo_bp_data(1000), velocity=0.0)
    _assert_skipped(decision, opps, "zero sell velocity")


def test_a_slow_measured_velocity_is_used_as_is_not_floored():
    """5000 ISK saved per 10-unit batch at 0.02 units/day: 10 ISK/day, so a
    20,000 ISK BPO breaks even in 2000 days. The old 0.033 floor said ~1212."""
    decision, _ = _analyse(_bpo_row(me_current=0), _bpo_bp_data(1000), velocity=0.02)
    assert decision.break_even_days == 2000.0
    assert decision.projected_annual_savings == 3650.0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_pipeline.py tests/test_daily_planner_decisions.py tests/test_chain_planner_keying.py -v`
Expected: FAIL. `AttributeError: 'PipelineState' object has no attribute 'velocity_unknown_reason'`, `TypeError: ... unexpected keyword argument 'velocity_unknown_reason'`, the zero-velocity case recommends instead of skipping, and `break_even_days` is about 1212.

- [ ] **Step 3: Add the fields**

`models.py`: append to `PipelineState` (after `has_active_manufacturing_jobs`):

```python
    # None when effective_velocity is a real signal (own sales, or the
    # producer's days of supply). Otherwise names why there was none, and
    # effective_velocity is only the analyzer's 0.01 floor.
    velocity_unknown_reason: str | None = None
```

In `ItemDecision`, directly after `bpo_analysis_skip_reason`:

```python
    # Copied from PipelineState.velocity_unknown_reason (see there).
    velocity_unknown_reason: str | None = None
```

- [ ] **Step 4: Record the reason in the analyzer**

Replace `pipeline_analyzer.py:161-172` (the comment and the velocity `if/else`):

```python
        # See module docstring: an unknown days_of_supply is 0.0 here, which
        # the `> 0.0` test below routes to "no signal".
        days_of_supply_for_velocity = row.days_of_supply if row.days_of_supply is not None else 0.0

        velocity_unknown_reason: str | None = None
        if sell_velocity_per_day > 0.0:
            effective_velocity = max(0.01, sell_velocity_per_day * velocity_multiplier)
        elif days_of_supply_for_velocity > 0.0:
            effective_velocity = max(0.01, 1.0 / days_of_supply_for_velocity)
        else:
            # No signal at all. 0.01 keeps the pipeline-days division finite;
            # the reason marks it as a floor so nothing downstream reads it as
            # a measured sell rate.
            effective_velocity = 0.01
            why_no_sales = "no corp sales in 30 days"
            velocity_unknown_reason = f"{why_no_sales} and no days-of-supply estimate"
```

Add `velocity_unknown_reason=velocity_unknown_reason,` as the last argument of the `PipelineState(...)` return at the end of `_analyze_single`.

- [ ] **Step 5: Carry it through the decision engine**

In `item_decision_engine.py`, in each of the five `ItemDecision(` constructions inside `decide()`, add a line directly after `effective_velocity=pipeline.effective_velocity,`, at the same indentation:

```python
                velocity_unknown_reason=pipeline.velocity_unknown_reason,
```

The last construction is indented four spaces less. Match its indentation.

- [ ] **Step 6: Skip in the chain planner instead of flooring**

Replace `chain_planner.py:617-619`:

```python
        units_per_run = quantity / runs
        if decision.velocity_unknown_reason is not None:
            self._skip_bpo_analysis(
                decision, f"sell velocity unknown ({decision.velocity_unknown_reason})"
            )
            return
        if decision.effective_velocity <= 0:
            self._skip_bpo_analysis(
                decision, f"zero sell velocity ({decision.effective_velocity} units/day)"
            )
            return
        planned_runs_per_day = decision.effective_velocity / units_per_run
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_pipeline.py tests/test_daily_planner_decisions.py tests/test_chain_planner_keying.py tests/test_daily_planner_fail_loud.py -v`
Expected: PASS.

- [ ] **Step 8: Run the full suite, then commit**

Run: `.venv312/bin/python -m pytest -q`. Expected: 0 failed.

```bash
git add src/eve_online_industry_tracker/application/daily_planner/models.py src/eve_online_industry_tracker/application/daily_planner/pipeline_analyzer.py src/eve_online_industry_tracker/application/daily_planner/item_decision_engine.py src/eve_online_industry_tracker/application/daily_planner/chain_planner.py tests/test_daily_planner_pipeline.py tests/test_daily_planner_decisions.py tests/test_chain_planner_keying.py
git commit -m "fix: skip BPO analysis on an unknown sell velocity instead of flooring it

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

# Batch 2 — accuracy of the numbers

### Task 5: Apply the ME ceiling to the batch, as the producer does

The producer's material adjustment (`industry/service.py:4176-4181` and `:6997-6998`) is:

```
adjusted_total = _round_material_quantity(float(qty_per_run * runs) * max(0.0, 1.0 - reduction),
                                          minimum_quantity=(runs if qty_per_run > 0 else 0))
_round_material_quantity = 0 if raw <= 0 else max(minimum_quantity, ceil(raw))       # :3006-3009
reduction = _combine_reductions([me / 100.0, structure, rig, implant])                # :6452-6457, :2978-2985
```

So there is one ceiling per batch, the floor is one unit per run, and there is no `round(..., 2)` on quantities. The planner's `me_adjusted_quantity` takes the ceiling per run, so base 5 × 100 runs at ME10 saves 0 instead of 50. A new `me_adjusted_batch_quantity` mirrors the producer's arithmetic for the ME term alone: the planner has no structure profile, and both sides of a saving use the same reduction. `me_adjusted_quantity(base, me)` becomes the runs=1 case, so its existing callers and tests keep working. The BPO saving becomes a per-batch figure, divided by batch units per day, and the optimal ME is computed at the planned run count.

**Files:**
- Modify: `src/eve_online_industry_tracker/infrastructure/sde/blueprints.py:360-423`
- Modify: `src/eve_online_industry_tracker/application/daily_planner/chain_planner.py:270-273,604-628,659-713,722-740`
- Test: `tests/test_chain_planner_keying.py`, `tests/test_compute_optimal_me.py`, `tests/test_daily_planner_fail_loud.py:258`

**Interfaces:**
- Produces: `blueprints.me_adjusted_batch_quantity(base_qty_per_run: int, runs: int, me: int) -> int`, `optimal_me_for_quantities(quantities: Iterable[int], *, runs: int = 1) -> int`, `compute_optimal_me(blueprint_type_id: int, session, runs: int = 1) -> int`, `ChainPlanner._compute_optimal_me(blueprint_type_id: int, runs: int = 1) -> int | None`, `ChainPlanner._me_saving_isk_per_batch(*, row, bp_data) -> tuple[float | None, str | None]` (replaces `_me_saving_isk_per_run`).
- Consumes: Task 4's velocity skip, kept inside the rewritten block.

- [ ] **Step 1: Write the failing tests**

In `tests/test_chain_planner_keying.py`, add `me_adjusted_batch_quantity` to the existing `from eve_online_industry_tracker.infrastructure.sde.blueprints import (...)` block, then add:

```python
def test_the_ceiling_is_taken_over_the_batch_not_per_run():
    """5 units/run x 100 runs at ME10: per run ceil(4.5) = 5 saves nothing,
    the batch ceil(450) = 450 saves 50 units."""
    assert 100 * (me_adjusted_quantity(5, 0) - me_adjusted_quantity(5, 10)) == 0
    assert me_adjusted_batch_quantity(5, 100, 0) - me_adjusted_batch_quantity(5, 100, 10) == 50
    assert me_adjusted_batch_quantity(5, 1, 10) == 5
    assert me_adjusted_batch_quantity(5, 1000, 10) == 4500
    assert me_adjusted_batch_quantity(1, 1000, 10) == 1000   # never below one unit per run
    assert me_adjusted_batch_quantity(0, 10, 10) == 0
    assert me_adjusted_batch_quantity(5, 0, 10) == 0


def test_the_batch_quantity_matches_the_producers_rounding_exactly():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    for base in (1, 2, 5, 7, 10, 33, 100, 1000, 2500):
        for runs in (1, 2, 3, 10, 20, 100, 250, 1000):
            for me in range(11):
                reduction = IndustryService._combine_reductions([me / 100.0])
                expected = IndustryService._round_material_quantity(
                    float(base * runs) * max(0.0, 1.0 - reduction), minimum_quantity=runs,
                )
                assert me_adjusted_batch_quantity(base, runs, me) == expected, (base, runs, me)


def test_optimal_me_depends_on_the_run_count():
    assert optimal_me_for_quantities([5]) == 0
    assert optimal_me_for_quantities([5], runs=10) == 10
    assert optimal_me_for_quantities([1], runs=100) == 0   # the one-unit-per-run floor


def test_a_100_run_batch_of_a_small_material_has_a_real_saving():
    """Base 5 x 100 runs, ME0 -> ME10: 500 -> 450 units, 50 x 5 ISK = 250 ISK
    per batch. 2 units/day over 100-unit batches is 0.02 batches/day, so
    5 ISK/day and a 4000-day break-even ('hold'), not 'saves nothing'."""
    row = _bpo_row(me_current=0, runs=100, quantity=100)
    decision, opps = _analyse(row, _bpo_bp_data(5))
    assert decision.break_even_days == 4000.0
    assert decision.projected_annual_savings == 1825.0
    assert opps[0]["recommendation"] == "hold"
```

Replace `test_a_small_quantity_blueprint_saves_nothing_from_me_research` (it used 10 runs, where the batch now saves 5 units):

```python
def test_a_single_run_of_a_small_material_saves_nothing_from_me_research():
    """1 run of 5 base units: ceil(5 x 0.9) = 5, so no saving."""
    decision, opps = _analyse(_bpo_row(me_current=0, runs=1, quantity=1), _bpo_bp_data(5))
    assert decision.projected_annual_savings == 0.0
    assert decision.break_even_days is None
    assert decision.bpo_investment_recommended is False
    assert opps[0]["recommendation"] == "hold"
```

Replace `test_rounding_eats_the_saving_on_a_small_material_but_not_a_large_one`:

```python
def test_the_batch_saving_counts_both_materials_at_the_batch_ceiling():
    """10 runs, ME0 -> ME10 (optimal at 10 runs):
      * 5 x 10 = 50 units x 1000 ISK: 50 -> 45, saves 5 x 1000 = 5000 ISK
      * 1000 x 10 = 10000 units x 5 ISK: 10000 -> 9000, saves 1000 x 5 = 5000 ISK
    10,000 ISK per 10-unit batch. At 2 units/day that is 0.2 batches/day,
    2000 ISK/day, so a 20,000 ISK BPO breaks even in exactly 10 days."""
    row = _row(runs=10, material_cost=100_000.0,
               blueprint_material_efficiency=0, blueprint_source_kind="owned_blueprint_copy",
               materials={"34": {"type_id": 34, "quantity": 50, "unit_price": 1000.0},
                          "35": {"type_id": 35, "quantity": 9000, "unit_price": 5.0}})
    row["quantity"] = 10
    bp_data = {999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                                       "materials": [{"type_id": 34, "quantity": 5},
                                                     {"type_id": 35, "quantity": 1000}]}}}

    assert _planner()._me_saving_isk_per_batch(row=row, bp_data=bp_data[999]) == (10_000.0, None)

    decision, opps = _analyse(row, bp_data)
    assert decision.bpo_analysis_skip_reason is None
    assert decision.break_even_days == 10.0
    assert decision.projected_annual_savings == 730_000.0
    assert opps[0]["recommendation"] == "strong_buy"


def test_optimal_me_research_target_is_computed_at_the_batch_run_count():
    """The owned-BPO branch must ask for the optimum at the row's run count."""
    seen = {}
    planner = _planner()
    planner._compute_optimal_me = lambda bp_type_id, runs=1: seen.setdefault("runs", runs) and 10
    row = _row(runs=20)
    phase1 = {"bpo_assets_by_type_id": {999: [_bpo(999, me=0, te=0)]}, "blueprint_data": BLUEPRINT_DATA}
    planner.plan_chain([_decision(row)], phase1)
    assert seen["runs"] == 20
```

Change the two stubs that do not accept `runs`:
- `tests/test_chain_planner_keying.py:35` becomes `planner._compute_optimal_me = lambda bp_type_id, runs=1: optimal_me`
- `tests/test_daily_planner_fail_loud.py:258` becomes `planner._compute_optimal_me = lambda bp_type_id, runs=1: None`

Append to `TestComputeOptimalMe` in `tests/test_compute_optimal_me.py`:

```python
    def test_runs_raise_the_optimum_for_a_small_quantity(self):
        session = _make_sde_session(321, 1000, [{"typeID": 34, "quantity": 5}])
        assert compute_optimal_me(321, session) == 0
        assert compute_optimal_me(321, session, runs=10) == 10

    def test_one_unit_per_run_never_benefits_at_any_run_count(self):
        session = _make_sde_session(789, 1000, [{"typeID": 34, "quantity": 1}])
        assert compute_optimal_me(789, session, runs=100) == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_chain_planner_keying.py tests/test_compute_optimal_me.py tests/test_daily_planner_fail_loud.py -v`
Expected: FAIL with `ImportError: cannot import name 'me_adjusted_batch_quantity'`.

- [ ] **Step 3: Add the batch helpers in `blueprints.py`**

Replace `me_adjusted_quantity` and `optimal_me_for_quantities` (`blueprints.py:395-423`):

```python
def _me_reduction(me: int) -> float:
    """ME level as the producer's combined material-reduction fraction.

    IndustryService._combine_reductions([me / 100.0]) for the ME term alone:
    1 - (1 - me/100), capped at 0.99. Mirrored, not simplified to me/100, so
    the float result is bit-for-bit the producer's.
    """
    fraction = int(me) / 100.0
    if fraction <= 0.0:
        return 0.0
    return max(0.0, min(1.0 - max(0.0, 1.0 - fraction), 0.99))


def me_adjusted_batch_quantity(base_qty_per_run: int, runs: int, me: int) -> int:
    """Units of one material for a whole batch at a blueprint ME level.

    Mirrors the producer's "Material adjustment" loop (application/industry/
    service.py, `adjusted_total = self._round_material_quantity(...)`): one
    ceiling over the whole batch, never per run, and never fewer than one
    unit per run:

        max(runs, ceil(base_qty_per_run * runs * (1 - reduction)))

    0 when base_qty_per_run or runs is not positive.
    """
    base = int(base_qty_per_run)
    runs = int(runs)
    if base <= 0 or runs <= 0:
        return 0
    raw = float(base * runs) * max(0.0, 1.0 - _me_reduction(me))
    if raw <= 0:
        return 0
    return max(runs, int(math.ceil(raw)))


def me_adjusted_quantity(base_qty: int, me: int) -> int:
    """Per-run material quantity at a blueprint ME level (a one-run batch).

    qty(ME) = ceil(base_qty * (1 - 0.01 * ME)). For a batch of more than one
    run use me_adjusted_batch_quantity: the ceiling is applied to the batch
    total, so per-run rounding times runs overstates the quantity.
    """
    return me_adjusted_batch_quantity(base_qty, 1, me)


def optimal_me_for_quantities(quantities: Iterable[int], *, runs: int = 1) -> int:
    """Highest ME level (0-10) that still reduces any of these base quantities
    for a batch of `runs` runs.

    For each quantity, the last ME in 1..10 whose batch quantity is lower
    than the level below it; the maximum over quantities. 0 when none benefit.
    """
    per_material_optimal: list[int] = []
    for qty in quantities:
        if qty <= 0:
            continue
        last_useful_me = 0
        prev_qty = me_adjusted_batch_quantity(qty, runs, 0)
        for me in range(1, 11):
            curr_qty = me_adjusted_batch_quantity(qty, runs, me)
            if curr_qty < prev_qty:
                last_useful_me = me
            prev_qty = curr_qty
        per_material_optimal.append(last_useful_me)

    return max(per_material_optimal) if per_material_optimal else 0
```

Change `compute_optimal_me`'s signature to `def compute_optimal_me(blueprint_type_id: int, session, runs: int = 1) -> int:`. Add the docstring sentence "`runs` is the planned batch size; the ceiling applies to the batch (see me_adjusted_batch_quantity)." Make the last line `return optimal_me_for_quantities(quantities, runs=runs)`.

- [ ] **Step 4: Use them in the chain planner**

`chain_planner.py:272`:

```python
            optimal_me = self._compute_optimal_me(
                bp_type_id, runs=orow.get_effective_runs(decision.overview_row)
            )
```

Replace `_compute_optimal_me` (`:722-740`) signature and inner call:

```python
    def _compute_optimal_me(self, blueprint_type_id: int, runs: int = 1) -> int | None:
        """Optimal ME at a `runs`-run batch from the SDE, or None when the SDE query fails.

        Only a database error degrades (logged at WARNING); None must never be
        read as 0 -- that would make every owned BPO look fully researched.
        """
        from eve_online_industry_tracker.infrastructure.sde.blueprints import compute_optimal_me
        try:
            sde_session = self._session_provider.sde_session()
            try:
                return compute_optimal_me(blueprint_type_id, sde_session, runs=runs)
            finally:
                sde_session.close()
        except SQLAlchemyError:
            logger.warning(
                "ChainPlanner: SDE lookup of optimal ME failed for bp_type_id=%s",
                blueprint_type_id, exc_info=True,
            )
            return None
```

In `_analyze_bpo_investment`, replace everything from `saving_per_run, reason = self._me_saving_isk_per_run(...)` down to and including `saving_per_day = saving_per_run * planned_runs_per_day` with:

```python
        saving_per_batch, reason = self._me_saving_isk_per_batch(row=row, bp_data=bp_data or {})
        if saving_per_batch is None:
            self._skip_bpo_analysis(decision, reason or "ME saving unavailable")
            return

        # effective_velocity is units/day; one planned batch makes `quantity` units.
        quantity = orow.get_product_quantity(row)
        if quantity <= 0:
            self._skip_bpo_analysis(decision, f"no batch units (quantity={quantity})")
            return
        if decision.velocity_unknown_reason is not None:
            self._skip_bpo_analysis(
                decision, f"sell velocity unknown ({decision.velocity_unknown_reason})"
            )
            return
        if decision.effective_velocity <= 0:
            self._skip_bpo_analysis(
                decision, f"zero sell velocity ({decision.effective_velocity} units/day)"
            )
            return
        planned_batches_per_day = decision.effective_velocity / quantity

        saving_per_day = saving_per_batch * planned_batches_per_day
```

Update the `_analyze_bpo_investment` docstring's first paragraph to: "Break-even = BPO price / (ISK saved per planned batch by researching ME from the blueprint's current level to its optimal level × batches per day)."

Replace `_me_saving_isk_per_run` (`:659-713`) with:

```python
    def _me_saving_isk_per_batch(
        self,
        *,
        row: dict[str, Any],
        bp_data: dict[str, Any],
    ) -> tuple[float | None, str | None]:
        """ISK saved on ONE planned batch by researching ME from its current to its optimal level.

        sum over materials of
            (batch_qty(base, runs, me_current) - batch_qty(base, runs, me_target)) * unit_price
        with batch_qty = blueprints.me_adjusted_batch_quantity: the producer's
        ceiling over the whole batch, not a per-run ceiling times runs.
        me_target is the optimal ME at this batch's run count. Returns
        (None, reason) when an input is missing; never a constant.
        """
        from eve_online_industry_tracker.infrastructure.sde.blueprints import (
            me_adjusted_batch_quantity,
            optimal_me_for_quantities,
        )

        manufacturing = bp_data.get("manufacturing") if isinstance(bp_data, dict) else None
        base_materials = (manufacturing or {}).get("materials") if isinstance(manufacturing, dict) else None
        base: list[tuple[int, int]] = []
        for mat in base_materials or []:
            if not isinstance(mat, dict):
                continue
            try:
                mat_type_id = int(mat.get("type_id") or 0)
                base_qty = int(mat.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            if mat_type_id > 0 and base_qty > 0:
                base.append((mat_type_id, base_qty))
        if not base:
            return None, "no SDE base material quantities for the blueprint"

        job = orow.get_manufacturing_job(row)
        source_kind = job.get("blueprint_source_kind")
        if source_kind == "blueprint_sde_fallback":
            # No owned blueprint: the producer assumed a max-researched ME.
            return None, "blueprint ME is assumed (blueprint_sde_fallback), not owned"
        try:
            me_current = int(job["blueprint_material_efficiency"])
        except (KeyError, TypeError, ValueError):
            return None, "unknown current blueprint ME"
        try:
            runs = int(job["runs"])
        except (KeyError, TypeError, ValueError):
            return None, "unknown batch run count (manufacturing_job.runs)"
        if runs <= 0:
            return None, f"non-positive batch run count ({runs})"

        me_target = optimal_me_for_quantities((q for _, q in base), runs=runs)

        priced = _material_unit_prices(job)
        saving = 0.0
        for mat_type_id, base_qty in base:
            unit_price = priced.get(mat_type_id)
            if unit_price is None:
                return None, f"no unit price for material type_id={mat_type_id}"
            saved_units = (
                me_adjusted_batch_quantity(base_qty, runs, me_current)
                - me_adjusted_batch_quantity(base_qty, runs, me_target)
            )
            saving += max(0, saved_units) * unit_price
        return saving, None
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv312/bin/python -m pytest tests/test_chain_planner_keying.py tests/test_compute_optimal_me.py tests/test_daily_planner_fail_loud.py tests/test_daily_planner_integration.py -v`
Expected: PASS. Check that `test_a_large_quantity_blueprint_gets_the_exact_isk_saving` (20 days, 365,000) and `test_runs_per_day_uses_units_per_run_not_runs_per_batch` (200 days) pass unchanged: 5000 ISK/batch × 0.2 and 0.02 batches/day give the same figures as the old per-run path.

- [ ] **Step 6: Run the full suite, then commit**

Run: `.venv312/bin/python -m pytest -q`. Expected: 0 failed.

```bash
git add src/eve_online_industry_tracker/infrastructure/sde/blueprints.py src/eve_online_industry_tracker/application/daily_planner/chain_planner.py tests/test_chain_planner_keying.py tests/test_compute_optimal_me.py tests/test_daily_planner_fail_loud.py
git commit -m "fix: take the ME ceiling over the batch, as the producer does

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Sub-manufacture nets corp stock and uses the owned BPO's ME

Verified against the code:
- **(b) is real.** `_resolve_sub_manufacture` builds the full merged request. `ShoppingListBuilder` then subtracts the sub-built units from the parent *before* netting corp stock (`shopping_list_builder.py:86-98`), so stock of the component is never used and the corp builds what it already owns.
- **(c) is real.** `_estimate_sub_manufacture_cost` (`chain_planner.py:506-541`) prices SDE per-run quantities × runs at ME0, and the sub job's shopping line does the same (`shopping_list_builder.py:295-307`, `action.materials` is `None` for sub actions).
- **(a) is not reachable as stated.** See "Dropped after verification". The invariant it depends on (sub-components are assigned after every top-level item) is implicit in a score sort, so this task makes it explicit with a two-pass loop and a test. No phase reorder is needed.

Design, with no phase reorder: Phase 1 computes the corp material stock once (`build_corp_stock_map`, moved to module level so the chain planner and the shopping list read the same map). `_resolve_sub_manufacture` builds `need - stock`. The sub job's inputs are computed with the owned BPO's ME through Task 5's `me_adjusted_batch_quantity` and carried on the sub decision (`sub_runs`, `sub_batch_materials`) to the sub action's `runs`/`materials`. The shopping list buys those as-is, exactly like a parent's producer batch materials. Structure and rig bonuses are not applied to the sub job (see Open questions). The quantities are therefore an upper bound, never an under-buy.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/chain_planner.py:407-541`
- Modify: `src/eve_online_industry_tracker/application/daily_planner/character_assigner.py:115-130,253-275`
- Modify: `src/eve_online_industry_tracker/application/daily_planner/shopping_list_builder.py:51-52,261-293,370-398`
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py` (`_phase_1_collect`, imports)
- Modify: `src/eve_online_industry_tracker/application/daily_planner/models.py:95-98` (comment only)
- Test: `tests/test_chain_planner_keying.py`, `tests/test_shopping_list_builder.py`

**Interfaces:**
- Consumes: `blueprints.me_adjusted_batch_quantity(base_qty_per_run, runs, me) -> int` (Task 5).
- Produces: `shopping_list_builder.build_corp_stock_map(corp_assets: list[Any], meta_resolver: Any) -> dict[int, int]`. Phase-1 key `"corp_material_stock": dict[int, int]`. Sub-component `overview_row` keys `"sub_runs": int`, `"sub_batch_materials": dict[int, int]`, `"quantity_requested": int`, `"corp_stock_used": int` (with `"quantity_needed"` now meaning "units to build"). A `sub_manufacture` `AssignedAction` with `runs` and `materials` set.

- [ ] **Step 1: Write the failing tests**

In `tests/test_chain_planner_keying.py`, change two existing expectations. The test BPO is ME10, so its own inputs shrink:
- In `test_sub_manufacture_builds_a_material_whose_blueprint_is_owned`: the comment becomes `# 10 runs x 2 Tritanium at ME10 = ceil(18) = 18 x 1 ISK, versus 100 x 1 ISK on the market.` and the assert becomes `assert sub.overview_row["sub_manufacture_cost"] == 18.0`.
- In `test_sub_manufacture_quantity_is_the_parents_batch_quantity_not_one_sde_run`: the comment becomes `# 180 runs x 2 Tritanium at ME10 = 324 x 1 ISK, versus 1800 x 1 ISK on the market.` and the assert becomes `assert sub.overview_row["sub_manufacture_cost"] == 324.0`.

Then append:

```python
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan  # noqa: E402


def _sub_phase1(*, me=10, stock=None, blueprint_data=None):
    return {
        "bpo_assets_by_type_id": {888: [_bpo(888, me=me, te=20)]},
        "blueprint_data": blueprint_data or SUB_BLUEPRINT_DATA,
        "market_depth_cache": {34: {"vwap_5d": 1.0}, 54321: {"vwap_5d": 1.0}},
        "corp_material_stock": stock or {},
    }


def _subs(phase1, **row_kwargs):
    row = _row(materials=_batch_materials(**{"54321": 100}), **row_kwargs)
    plan = _planner().plan_chain([_decision(row)], phase1)
    return [d for d in plan.decisions if d.is_sub_component]


def test_sub_manufacture_records_the_me_adjusted_batch_it_will_run():
    (sub,) = _subs(_sub_phase1())
    assert sub.overview_row["sub_runs"] == 10
    assert sub.overview_row["sub_batch_materials"] == {34: 18}


def test_the_sub_job_inputs_use_the_owned_bpos_me():
    (sub,) = _subs(_sub_phase1(me=0))
    assert sub.overview_row["sub_batch_materials"] == {34: 20}
    assert sub.overview_row["sub_manufacture_cost"] == 20.0


def test_an_owned_bpo_with_unknown_me_is_not_sub_built(caplog):
    with caplog.at_level("WARNING"):
        assert _subs(_sub_phase1(me=None)) == []
    assert any("unknown ME" in r.getMessage() for r in caplog.records)


def test_a_component_fully_in_corp_stock_is_not_sub_built():
    assert _subs(_sub_phase1(stock={54321: 100})) == []
    assert _subs(_sub_phase1(stock={54321: 250})) == []


def test_only_the_part_not_in_corp_stock_is_sub_built():
    """Need 100, stock 60: build 40 = 4 runs; 2 x 4 at ME10 = ceil(7.2) = 8."""
    (sub,) = _subs(_sub_phase1(stock={54321: 60}))
    assert sub.overview_row["quantity_requested"] == 100
    assert sub.overview_row["corp_stock_used"] == 60
    assert sub.overview_row["quantity_needed"] == 40
    assert sub.overview_row["sub_runs"] == 4
    assert sub.overview_row["sub_batch_materials"] == {34: 8}
    assert sub.overview_row["market_buy_cost"] == 40.0


def test_a_sub_blueprint_with_no_materials_is_not_a_free_build(caplog):
    blueprint_data = {**SUB_BLUEPRINT_DATA, 888: {"manufacturing": {
        "products": [{"type_id": 54321, "quantity": 10}], "materials": []}}}
    with caplog.at_level("WARNING"):
        assert _subs(_sub_phase1(blueprint_data=blueprint_data)) == []
    assert any("no SDE materials" in r.getMessage() for r in caplog.records)


def test_a_sub_blueprint_that_does_not_make_the_component_is_not_used(caplog):
    blueprint_data = {**SUB_BLUEPRINT_DATA, 888: {"manufacturing": {
        "products": [{"type_id": 77777, "quantity": 10}],
        "materials": [{"type_id": 34, "quantity": 2}]}}}
    with caplog.at_level("WARNING"):
        # The index maps nothing to 888 for 54321 now, so there is no request at all.
        assert _subs(_sub_phase1(blueprint_data=blueprint_data)) == []


def _sub_decision(**overview):
    return ItemDecision(
        type_id=54321, type_name="Widget", decision="build",
        decision_reason="Sub-manufacture: cheaper than market buy", adjusted_score=0.0,
        absolute_profit_per_batch=0.0, isk_per_hour=0.0, margin_pct=0.0,
        days_of_supply_current=0.0, effective_velocity=1.0, meta_group_id=1,
        pipeline_stage="manufacturing", is_sub_component=True,
        overview_row={"type_id": 54321, "type_name": "Widget", "quantity_needed": 100,
                      "sub_runs": 10, "sub_batch_materials": {34: 18},
                      "sub_manufacture_cost": 18.0, "market_buy_cost": 100.0, **overview},
    )


def _one_slot_pilot():
    from types import SimpleNamespace as _NS
    return _NS(list_characters=lambda: [
        {"character_id": 1, "character_name": "Pilot", "skills": {"skills": []}}])


def test_a_sub_manufacture_action_carries_the_planned_runs_and_batch_materials():
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        CharacterAssigner,
    )
    (action,) = CharacterAssigner().assign(
        ChainPlan(decisions=[_sub_decision()]), [], _one_slot_pilot(), _AdminStub())
    assert (action.action_type, action.quantity, action.runs, action.materials) == (
        "sub_manufacture", 100, 10, {34: 18})


def test_sub_components_are_assigned_only_after_every_top_level_item():
    """With one manufacturing slot, the parent must get it even if a
    sub-component scores higher. Otherwise the component is built for a
    parent that never runs."""
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        CharacterAssigner,
    )
    parent = _decision(_row(), adjusted_score=1.0)
    sub = _sub_decision()
    sub.adjusted_score = 100.0
    actions = CharacterAssigner().assign(
        ChainPlan(decisions=[parent, sub]), [], _one_slot_pilot(), _AdminStub())
    assert [a.action_type for a in actions] == ["manufacture"]
```

Append to `tests/test_shopping_list_builder.py`:

```python
def test_a_sub_manufacture_action_buys_its_own_me_adjusted_batch_materials():
    sub = AssignedAction(
        type_id=54321, type_name="Widget", action_type="sub_manufacture",
        character_id=1, character_name="Pilot", quantity=100, runs=10,
        estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None, materials={34: 18},
    )
    blueprints = {888: {"manufacturing": {
        "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 2}],
        "products": [{"type_id": 54321, "quantity": 10}],
    }}}
    items = ShoppingListBuilder().build(
        assigned_actions=[sub], corp_assets=[], market_depth_cache={34: {"vwap_5d": 5.0}},
        admin_settings=_Admin(), blueprint_data=blueprints, meta_resolver=_NoBlueprints(),
    )
    assert [(i.type_id, i.quantity) for i in items] == [(34, 18)]   # not the SDE's 2 x 10


def test_build_corp_stock_map_is_shared_and_skips_blueprints():
    from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import (
        build_corp_stock_map,
    )
    assets = [SimpleNamespace(type_id=34, quantity=5), SimpleNamespace(type_id=34, quantity=7),
              SimpleNamespace(type_id=999, quantity=1)]
    assert build_corp_stock_map(assets, _BlueprintsAre(999)) == {34: 12}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_chain_planner_keying.py tests/test_shopping_list_builder.py -v`
Expected: FAIL. The cost is `20.0`, not `18.0`; there are `KeyError: 'sub_runs'` and `'quantity_requested'`; the stock tests return one sub; the zero-material test returns a sub; `action.runs is None`; the ordering test gives `["sub_manufacture"]`; there are `(34, 20)` and `ImportError: build_corp_stock_map`.

- [ ] **Step 3: Share the corp stock map**

In `shopping_list_builder.py`, move the body of `_build_corp_stock_map` into a module-level function placed above `_positive_int`, with the method's docstring:

```python
def build_corp_stock_map(corp_assets: list[Any], meta_resolver: Any) -> dict[int, int]:
    """{type_id: quantity} of material stock, excluding blueprints.

    Shared by ChainPlanner (sub-manufacture nets corp stock before deciding
    to build) and ShoppingListBuilder, so both read the same stock.
    Pre-warms meta_resolver's cache with every distinct asset type_id in one
    batched SDE query (prefetch() is idempotent); without it is_blueprint()
    below would open one SDE session per distinct type_id.
    """
    type_ids = {int(_asset_attr(a, "type_id") or 0) for a in corp_assets}
    meta_resolver.prefetch({t for t in type_ids if t > 0})

    stock: dict[int, int] = {}
    for asset in corp_assets:
        type_id = int(_asset_attr(asset, "type_id") or 0)
        if type_id <= 0:
            continue
        if meta_resolver.is_blueprint(type_id):
            continue  # blueprints are not material stock
        qty = int(_asset_attr(asset, "quantity") or 0)
        if qty <= 0:
            continue
        stock[type_id] = stock.get(type_id, 0) + qty
    return stock
```

Delete the `_build_corp_stock_map` method and change line 52 to `corp_stock: dict[int, int] = build_corp_stock_map(corp_assets, meta_resolver)`.

In `service.py`, add `from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import build_corp_stock_map` next to the `ShoppingListBuilder` import. In `_phase_1_collect`, directly after `corp_assets = self._get_corp_assets()`, add:

```python
        # Material stock (blueprints excluded), shared by chain planning and
        # the shopping list.
        corp_material_stock = build_corp_stock_map(corp_assets, self._meta_resolver)
```

Then add `"corp_material_stock": corp_material_stock,` to the returned dict after `"corp_assets"`.

- [ ] **Step 4: Net stock and apply the BPO's ME in the chain planner**

In `_collect_sub_manufacture_requests`, replace the `requests.append({...})` with:

```python
            requests.append({
                "type_id": mat_type_id,
                "type_name": str(names.get(mat_type_id) or f"type_{mat_type_id}"),
                "blueprint_type_id": mat_blueprint_type_id,
                # None = the owned BPO's ME is unknown; resolution then buys.
                "blueprint_me": _blueprint_efficiency(
                    bpo_assets_by_type_id[mat_blueprint_type_id][0], "material"
                ),
                "quantity": quantity,
            })
```

Replace `_resolve_sub_manufacture` and `_estimate_sub_manufacture_cost` (`chain_planner.py:455-541`) with:

```python
    def _resolve_sub_manufacture(
        self,
        *,
        request: dict[str, Any],
        market_depth_cache: dict[int, Any],
        phase1_data: dict[str, Any],
    ) -> ItemDecision | None:
        """Build vs buy for one merged material request, net of corp stock.

        Returns a sub-component ItemDecision (decision='build') when building
        the part corp stock does not cover is cheaper than buying it, else
        None (buy). These bypass the Phase 4 gate per the two-pass spec.
        ShoppingListBuilder subtracts the built quantity from the parents'
        purchases and nets the rest against the same corp stock.
        """
        mat_type_id = int(request["type_id"])
        mat_blueprint_type_id = int(request["blueprint_type_id"])
        qty_requested = int(request["quantity"])

        in_stock = max(0, int((phase1_data.get("corp_material_stock") or {}).get(mat_type_id, 0)))
        qty_to_build = qty_requested - in_stock
        if qty_to_build <= 0:
            logger.info(
                "ChainPlanner: type_id=%s needs %d, corp stock holds %d; not sub-manufacturing",
                mat_type_id, qty_requested, in_stock,
            )
            return None

        me = request.get("blueprint_me")
        if me is None:
            logger.warning(
                "ChainPlanner: owned BPO %s for type_id=%s has unknown ME; buying the "
                "component instead of sub-manufacturing it",
                mat_blueprint_type_id, mat_type_id,
            )
            return None

        batch = self._sub_batch(
            product_type_id=mat_type_id, blueprint_type_id=mat_blueprint_type_id,
            qty_to_build=qty_to_build, me=int(me), phase1_data=phase1_data,
        )
        if batch is None:
            return None
        runs, batch_materials = batch

        sub_cost = self._price_materials(batch_materials, market_depth_cache)
        market_cost = self._estimate_market_cost(mat_type_id, qty_to_build, market_depth_cache)
        if sub_cost is None or market_cost is None:
            return None  # Can't compare -> default to buy
        if sub_cost >= market_cost:
            return None

        mat_name = request["type_name"]
        return ItemDecision(
            type_id=mat_type_id,
            type_name=mat_name,
            decision="build",
            decision_reason="Sub-manufacture: cheaper than market buy",
            adjusted_score=0.0,
            absolute_profit_per_batch=0.0,
            isk_per_hour=0.0,
            margin_pct=0.0,
            days_of_supply_current=0.0,
            effective_velocity=1.0,
            meta_group_id=META_GROUP_T1,
            pipeline_stage="manufacturing",
            is_sub_component=True,
            overview_row={
                "type_id": mat_type_id,
                "type_name": mat_name,
                "blueprint_type_id": mat_blueprint_type_id,
                "quantity_requested": qty_requested,
                "corp_stock_used": min(in_stock, qty_requested),
                "quantity_needed": qty_to_build,
                "sub_runs": runs,
                "sub_batch_materials": batch_materials,
                "requested_by_type_ids": list(request["requested_by_type_ids"]),
                "sub_manufacture_cost": sub_cost,
                "market_buy_cost": market_cost,
            },
        )

    def _sub_batch(
        self,
        *,
        product_type_id: int,
        blueprint_type_id: int,
        qty_to_build: int,
        me: int,
        phase1_data: dict[str, Any],
    ) -> tuple[int, dict[int, int]] | None:
        """(runs, {material type_id: units for the whole sub job}) at the owned BPO's ME.

        Base quantities are the SDE's per-run figures, reduced by the BPO's ME
        with the producer's batch ceiling (blueprints.me_adjusted_batch_quantity).
        Structure and rig bonuses are not applied: the planner does not know
        where the sub job will run, so these quantities are an upper bound,
        never an under-buy. None (with a WARNING) when the blueprint cannot
        say how to build the component.
        """
        from eve_online_industry_tracker.infrastructure.sde.blueprints import (
            me_adjusted_batch_quantity,
        )

        bp_data = (phase1_data.get("blueprint_data") or {}).get(blueprint_type_id)
        manufacturing = bp_data.get("manufacturing") if isinstance(bp_data, dict) else None
        if not isinstance(manufacturing, dict):
            logger.warning(
                "ChainPlanner: no SDE manufacturing data for blueprint %s; buying type_id=%s",
                blueprint_type_id, product_type_id,
            )
            return None

        output_per_run = 0
        for product in manufacturing.get("products") or []:
            if isinstance(product, dict) and int(product.get("type_id") or 0) == product_type_id:
                output_per_run = int(product.get("quantity") or 0)
                break
        if output_per_run <= 0:
            logger.warning(
                "ChainPlanner: blueprint %s has no per-run output of type_id=%s; buying it",
                blueprint_type_id, product_type_id,
            )
            return None

        runs = math.ceil(qty_to_build / output_per_run)
        batch: dict[int, int] = {}
        for mat in manufacturing.get("materials") or []:
            if not isinstance(mat, dict):
                continue
            mat_type_id = int(mat.get("type_id") or 0)
            base_qty = int(mat.get("quantity") or 0)
            if mat_type_id <= 0 or base_qty <= 0:
                continue
            batch[mat_type_id] = batch.get(mat_type_id, 0) + me_adjusted_batch_quantity(
                base_qty, runs, me
            )
        if not batch:
            # A job with no inputs would price at 0 ISK and always beat the
            # market: that is missing SDE data, not a free build.
            logger.warning(
                "ChainPlanner: blueprint %s has no SDE materials; not sub-manufacturing type_id=%s",
                blueprint_type_id, product_type_id,
            )
            return None
        return runs, batch

    def _price_materials(
        self, materials: dict[int, int], market_depth_cache: dict[int, Any]
    ) -> float | None:
        """Market cost of a material list, or None when any line has no price."""
        total = 0.0
        for mat_type_id, quantity in materials.items():
            cost = self._estimate_market_cost(mat_type_id, quantity, market_depth_cache)
            if cost is None:
                logger.info(
                    "ChainPlanner: no market price for sub-job input type_id=%s; "
                    "cannot compare build vs buy", mat_type_id,
                )
                return None
            total += cost
        return total
```

- [ ] **Step 5: Assign sub-components last and carry their batch**

In `character_assigner.py` `assign()`, replace lines 115-130:

```python
        # Two passes: every top-level item first (highest score first), then
        # sub-components. A sub-component's quantity is its parents' need, so
        # it must never take a slot ahead of a parent. Slots only decrease, so
        # a parent left without a slot leaves none for its components either.
        build_decisions = [d for d in chain_plan.decisions if d.decision in ("build", "pause")]
        top_level = sorted(
            (d for d in build_decisions if not d.is_sub_component), key=lambda d: -d.adjusted_score
        )
        sub_components = [d for d in build_decisions if d.is_sub_component]

        for decision in [*top_level, *sub_components]:
            actions = self._assign_decision(
                decision=decision,
                row=decision.overview_row,
                char_slots=char_slots,
                characters=characters,
                now=now,
            )
            # Slots are consumed inside _assign_decision, as each action is
            # created -- two research actions on one item must not share a slot.
            assigned_actions.extend(actions)
```

In the sub-manufacture `AssignedAction(...)` (lines 258-273), set `runs=row.get("sub_runs"),` instead of `runs=None,`, and add `materials=row.get("sub_batch_materials"),` after `notes=(...)`.

- [ ] **Step 6: The shopping list buys a sub job's own batch**

In `shopping_list_builder.py` `_material_needs`, replace the docstring and the first branch:

```python
        """(material type_id, name, units needed) for one job.

        An action carrying a batch-materials mapping uses it as-is. For
        manufacture that is the producer's batch (per-run x runs after
        ME/structure reduction). For sub_manufacture it is ChainPlanner's
        batch at the owned BPO's ME. Otherwise (no mapping) the SDE per-run
        quantities x runs are used, which carry no ME/structure reduction.
        """
        mats, per_run_output = self._get_materials_and_output_for_action(
            action, blueprint_data, product_to_blueprint
        )
        names = {
            int(m.get("type_id") or 0): m.get("type_name") for m in mats if isinstance(m, dict)
        }

        if action.materials is not None:
            return [
                (t, str(names.get(t) or f"type_{t}"), int(q))
                for t, q in action.materials.items()
                if int(t) > 0 and int(q) > 0
            ]
```

Keep the following `if action.action_type == "manufacture":` info log and the SDE fallback unchanged.

In `models.py`, update the `AssignedAction.materials` comment to:

```python
    # manufacture: {material type_id: units for the whole batch} from the
    # producer (runs and ME/structure already applied). sub_manufacture: the
    # same shape from ChainPlanner, at the owned BPO's ME. None = unknown, and
    # the shopping list falls back to SDE per-run quantities x runs.
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `.venv312/bin/python -m pytest tests/test_chain_planner_keying.py tests/test_shopping_list_builder.py tests/test_daily_planner_assignment.py tests/test_daily_planner_end_to_end.py tests/test_daily_planner_fail_loud.py tests/test_planner_input_error_propagates.py -v`
Expected: PASS. The existing `test_sub_manufacture_covers_several_parents_in_order` and `test_a_sub_manufactured_component_is_not_also_bought_for_the_parent` still pass, because sub actions without `materials` keep the SDE path.

- [ ] **Step 8: Run the full suite, then commit**

Run: `.venv312/bin/python -m pytest -q`. Expected: 0 failed.

```bash
git add src/eve_online_industry_tracker/application/daily_planner/chain_planner.py src/eve_online_industry_tracker/application/daily_planner/character_assigner.py src/eve_online_industry_tracker/application/daily_planner/shopping_list_builder.py src/eve_online_industry_tracker/application/daily_planner/service.py src/eve_online_industry_tracker/application/daily_planner/models.py tests/test_chain_planner_keying.py tests/test_shopping_list_builder.py
git commit -m "fix: net corp stock and apply the owned BPO's ME before sub-manufacturing

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Investigate whether the realized cost is like-for-like with the predicted one

`FeedbackProcessor._find_realized_sale` uses the ledger row's `allocated_cost / priced_quantity` as the actual per-unit cost. The predicted per-unit cost is `AssignedAction.estimated_cost_isk / quantity`, which is `manufacturing_job.material_cost` (materials only, `character_assigner.py:287`). If `allocated_cost` also carries job install fees (and, for T2, copy/invention), then `cost_multiplier = predicted / actual` sits below 1 by construction.

While drafting, a read found `asset_provenance.py:1443`: `total_build_cost = total_materials_cost + job_cost + copy_cost + invention_cost`, and `:596-601` does the same in the estimate path. That points to Branch A, but Step 1 must confirm it end to end before any code changes.

**Files:**
- Read only (Step 1): `application/characters/realized_profit.py` (`_consume_lots`, the corp ledger build at `:560-760`), `application/characters/asset_provenance.py` (`resolve_industry_job_cost_snapshot` `:458-650`, the `total_build_cost` write near `:1443`), `application/corporations/corporation.py:1070-1090`, `application/industry/service.py:7499-7510` (producer `manufacturing_job["total_cost"]`) and its `total_job_cost` accumulation (`:4205-4300`)
- Branch A modifies: `application/industry/overview_row.py`, `application/daily_planner/models.py`, `character_assigner.py`, `action_plan_builder.py:203-220`, `feedback_processor.py:172-230,301-394`, `infrastructure/models.py:1091-1110`, `infrastructure/schema_migrations.py` (`daily_action_log` DDL and column migrations)
- Test: `tests/test_daily_planner_feedback.py`, `tests/test_daily_planner_fail_loud.py`, `tests/test_action_plan_builder.py`

**Interfaces:**
- Branch A produces: `overview_row.get_total_cost(row) -> float | None`, `AssignedAction.estimated_build_cost_isk: float | None = None`, `DailyActionLogModel.estimated_build_cost_isk: Mapped[Optional[float]]`, and `feedback_processor._industry_build_unit_cost(allocation_details: Any) -> float | None`. `_find_realized_sale` returns `{"isk_per_hour", "allocation_details", "sell_days"}`.

- [ ] **Step 1: Read-only trace (no edits)**

Answer each question with file:line evidence in the task report:

1. Which lot sources can feed a corp ledger row's `allocated_cost`? Read `source_mix` / `allocation_details` construction in `realized_profit.py` (`_consume_lots`, and the event loop that appends `FifoLot(... source=...)` lots). Expected set: `industry_build`, `market_buy`, `untracked_inventory`, opening inventory, and any `*_transferred` variants.
2. For an `industry_build` lot, what is `unit_cost`? Follow `resolve_industry_job_cost_snapshot` through both paths: the persisted `unit_build_cost`/`total_build_cost` (who writes them? `asset_provenance.py:~1443` and `corporation.py:1080`) and the market-estimate path (`:596-601`). Does it include `job.cost` (install fee), `copy_cost` and `invention_cost`?
3. What is the predicted side? Confirm that `estimated_cost_isk` = `orow.get_material_cost_total(row)` = `manufacturing_job.material_cost` (materials only).
4. What does the producer's `manufacturing_job["total_cost"]` contain (`industry/service.py:7499-7510`, and what accumulates into `total_job_cost` around `:4205-4300`)? Does it include copy and invention job costs, or only the manufacturing install cost?

Decision rule: **Confirmed** if (2) shows install fees (or copy/invention) inside the industry-build `unit_cost` while (3) is materials only. Then go to Branch A, using the producer's `total_cost` as the predicted side if (4) covers the same components. If (4) misses copy/invention while (2) includes them, still use Branch A and record that T2 items keep a residual bias, as an Open question for the controller. **Not confirmed** if (2) is materials only. Then go to Branch B.

- [ ] **Step 2 (Branch B: not confirmed): record and stop**

Add this paragraph to `_find_realized_sale`'s docstring, filling in the file:line evidence from Step 1:

```
    Cost comparability (checked 2026-10-02): an industry-build lot's unit_cost
    is materials only (<evidence file:line>), the same basis as the predicted
    manufacturing_job.material_cost, so allocated_cost / priced_quantity is a
    like-for-like actual per-unit cost and cost_multiplier is unbiased.
```

Commit as `docs: record that realized and predicted unit costs are like-for-like`, with the trailer. Task 7 ends here.

- [ ] **Step 2 (Branch A: confirmed): write the failing tests**

In `tests/test_daily_planner_feedback.py`, extend `_make_action` with `estimated_build_cost_isk: float | None = None` (set `action.estimated_build_cost_isk = estimated_build_cost_isk` next to `estimated_cost_isk`). Then replace `_run_cost_case` and the five cost tests (`test_cost_update_compares_per_unit_costs`, `test_zero_allocated_cost_skips_the_cost_update`, `test_unknown_predicted_cost_skips_the_cost_update`, `test_zero_priced_quantity_skips_the_cost_update`, `test_cost_multiplier_is_clamped_to_the_band`) with:

```python
    def _run_cost_case(self, *, build_cost, action_qty, allocations, old_cost=1.0):
        action = _make_action(type_id=200, estimated_build_cost_isk=build_cost, quantity=action_qty)
        self.repo.get_unprocessed_done_actions.return_value = [action]
        self.repo.get_weights.return_value = {200: _make_weights(cost_multiplier=old_cost)}
        self.repo.get_plan_items.return_value = [_make_plan_item(type_id=200, isk_per_hour=10_000_000)]
        self.processor._find_realized_sale.return_value = {
            "isk_per_hour": 8_000_000.0, "allocation_details": allocations, "sell_days": 5.0,
        }
        self.processor.process_pending_feedback()
        return (self.repo.upsert_weights.call_args[0][0],
                self.repo.insert_outcome.call_args[0][0])

    def test_cost_update_compares_full_build_cost_per_unit(self):
        """Both sides are materials + job fees per unit: 55M / 100 = 550k
        predicted vs 6M / 10 = 600k actual from the sale's industry-built units."""
        weights, outcome = self._run_cost_case(
            build_cost=55_000_000.0, action_qty=100,
            allocations=[{"source": "industry_build", "quantity": 10, "total_cost": 6_000_000.0}],
        )
        expected = 0.8 * 1.0 + 0.2 * (550_000.0 / 600_000.0)
        assert abs(weights.cost_multiplier - expected) < 1e-9
        assert outcome.predicted_material_cost == 550_000.0
        assert outcome.actual_material_cost == 600_000.0

    def test_only_industry_built_units_count_as_actual_cost(self):
        """A market-bought lot's unit price is not a build cost."""
        weights, outcome = self._run_cost_case(
            build_cost=55_000_000.0, action_qty=100,
            allocations=[
                {"source": "industry_build", "quantity": 10, "total_cost": 6_000_000.0},
                {"source": "market_buy", "quantity": 90, "total_cost": 1.0},
            ],
        )
        assert outcome.actual_material_cost == 600_000.0

    def test_a_sale_with_no_industry_built_units_skips_the_cost_update(self, caplog):
        with caplog.at_level(logging.INFO):
            weights, outcome = self._run_cost_case(
                build_cost=55_000_000.0, action_qty=100, old_cost=0.9,
                allocations=[{"source": "market_buy", "quantity": 10, "total_cost": 6_000_000.0}],
            )
        assert weights.cost_multiplier == 0.9
        assert outcome.actual_material_cost is None
        assert any("cost update skipped" in r.getMessage() for r in caplog.records)

    def test_zero_allocated_cost_skips_the_cost_update(self):
        weights, outcome = self._run_cost_case(
            build_cost=55_000_000.0, action_qty=100, old_cost=0.9,
            allocations=[{"source": "industry_build", "quantity": 10, "total_cost": 0.0}],
        )
        assert weights.cost_multiplier == 0.9
        assert outcome.actual_material_cost is None

    def test_unknown_predicted_build_cost_skips_the_cost_update(self, caplog):
        for cost, qty in ((None, 100), (55_000_000.0, None), (55_000_000.0, 0)):
            caplog.clear()
            with caplog.at_level(logging.INFO):
                weights, outcome = self._run_cost_case(
                    build_cost=cost, action_qty=qty, old_cost=1.1,
                    allocations=[{"source": "industry_build", "quantity": 10, "total_cost": 6e6}],
                )
            assert weights.cost_multiplier == 1.1, (cost, qty)
            assert outcome.predicted_material_cost is None
            assert any("cost update skipped" in r.getMessage() for r in caplog.records)

    def test_cost_multiplier_is_clamped_to_the_band(self):
        from eve_online_industry_tracker.application.daily_planner.feedback_processor import (
            LEARNING_WEIGHT_MAX, LEARNING_WEIGHT_MIN,
        )
        high, _ = self._run_cost_case(
            build_cost=1_000_000_000.0, action_qty=1,
            allocations=[{"source": "industry_build", "quantity": 1, "total_cost": 1.0}],
        )
        assert high.cost_multiplier == LEARNING_WEIGHT_MAX
        self.repo.reset_mock()
        low, _ = self._run_cost_case(
            build_cost=1.0, action_qty=1, old_cost=0.0,
            allocations=[{"source": "industry_build", "quantity": 1, "total_cost": 1e9}],
        )
        assert low.cost_multiplier == LEARNING_WEIGHT_MIN
```

Append to `tests/test_daily_planner_fail_loud.py`, next to the other manufacture-action tests:

```python
def test_a_manufacture_action_carries_the_batch_build_cost():
    """Feedback compares like for like: the producer's total_cost is
    materials plus job install cost, the basis of the realized unit cost."""
    row = {"type_id": 12345, "quantity": 200,
           "manufacturing_job": {"runs": 20, "material_cost": 50_000_000.0,
                                 "total_cost": 55_000_000.0}}
    (action,) = _assign(row)
    assert action.estimated_cost_isk == 50_000_000.0
    assert action.estimated_build_cost_isk == 55_000_000.0
```

Append to `tests/test_action_plan_builder.py`:

```python
def test_a_job_action_persists_its_build_cost():
    from eve_online_industry_tracker.application.daily_planner.action_plan_builder import ActionPlanBuilder
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction
    action = AssignedAction(
        type_id=12345, type_name="Thing", action_type="manufacture", character_id=1,
        character_name="Pilot", quantity=200, runs=20, estimated_cost_isk=50_000_000.0,
        estimated_profit_isk=None, estimated_completion=None, notes=None,
        estimated_build_cost_isk=55_000_000.0,
    )
    (row,) = ActionPlanBuilder()._build_job_actions(0, [action], datetime(2026, 1, 1))
    assert row.estimated_build_cost_isk == 55_000_000.0
```

(`datetime` is already imported at the top of that file.)

- [ ] **Step 3 (Branch A): run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_feedback.py tests/test_daily_planner_fail_loud.py tests/test_action_plan_builder.py -v`
Expected: FAIL with `AttributeError: ... 'estimated_build_cost_isk'`, `TypeError: unexpected keyword argument 'estimated_build_cost_isk'`, and the cost assertions.

- [ ] **Step 4 (Branch A): implement**

`overview_row.py`, after `get_material_cost_total`:

```python
def get_total_cost(row: dict[str, Any]) -> float | None:
    """The batch's full build cost (materials + job costs), or None.

    The producer writes `manufacturing_job.total_cost = total_job_cost +
    material_cost` (industry/service.py, _enrich_product_rows_with_material_prices),
    and None when neither could be priced.
    """
    return _as_optional_float(get_manufacturing_job(row).get("total_cost"))
```

`daily_planner/models.py` `AssignedAction`, after `materials`:

```python
    # manufacture only: materials + job costs for the whole batch (producer's
    # manufacturing_job.total_cost), the basis FeedbackProcessor compares
    # against a sale's realized industry-build unit cost. None = unknown.
    estimated_build_cost_isk: float | None = None
```

`character_assigner.py` manufacture `AssignedAction(...)`: add `estimated_build_cost_isk=orow.get_total_cost(row),` after `estimated_cost_isk=estimated_cost,`.

`infrastructure/models.py` `DailyActionLogModel`, after `estimated_cost_isk`:

```python
    estimated_build_cost_isk: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
```

`schema_migrations.py`: add `"estimated_build_cost_isk REAL NULL,"` after the `estimated_cost_isk` line of the `daily_action_log` CREATE TABLE DDL. Add next to the other daily planner column migrations:

```python
    _ensure_column(db_app, table="daily_action_log", column="estimated_build_cost_isk", ddl_type="REAL")
```

`action_plan_builder.py` `_build_job_actions`: add `estimated_build_cost_isk=action.estimated_build_cost_isk,` after `estimated_cost_isk=action.estimated_cost_isk,`.

`feedback_processor.py`: add below `_per_unit`:

```python
#: Allocation sources whose unit_cost is a manufacturing job's full build
#: cost (materials + job costs; asset_provenance builds total_build_cost so).
#: Confirm this set against Step 1's question 1.
_INDUSTRY_BUILD_SOURCES = frozenset({"industry_build", "industry_build_transferred"})


def _industry_build_unit_cost(allocation_details: Any) -> float | None:
    """Per-unit build cost of the sale's units that came out of an industry job.

    Market-bought and untracked lots are excluded: their unit price is not a
    build cost, so mixing them in would compare unlike things. None when the
    sale has no priced industry-built units.
    """
    if not isinstance(allocation_details, list):
        return None
    total = 0.0
    units = 0
    for entry in allocation_details:
        if not isinstance(entry, dict) or entry.get("source") not in _INDUSTRY_BUILD_SOURCES:
            continue
        quantity = _positive_number(entry.get("quantity"))
        cost = _positive_number(entry.get("total_cost"))
        if quantity is None or cost is None:
            continue
        total += cost
        units += int(quantity)
    return total / units if units > 0 else None
```

In `_process_single_action`, replace the predicted-cost block (`:172-178`):

```python
        # Predicted BUILD cost per unit: materials + job costs for the batch
        # over its units. That is the same basis as a sale's industry-build
        # allocated cost, which includes install fees (and copy/invention for T2).
        predicted_material_cost: float | None = _per_unit(
            getattr(action, "estimated_build_cost_isk", None), getattr(action, "quantity", None)
        )
```

and the actual-cost read (`:192-194`):

```python
            actual_material_cost: float | None = _industry_build_unit_cost(
                realized.get("allocation_details")
            )
```

In the skip log (`:216-225`), replace the last two format arguments and their text with `"(batch build cost=%r units=%r)"` and `getattr(action, "estimated_build_cost_isk", None), getattr(action, "quantity", None)`, and drop the `realized.get(...)` arguments. In `_find_realized_sale`'s return dict, replace the `"material_cost"` and `"priced_quantity"` entries with `"allocation_details": row.allocation_details,`. Change the comment above it to say that allocation_details carries each lot's source and cost.

- [ ] **Step 5 (Branch A): run the tests, then the full suite**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_feedback.py tests/test_daily_planner_fail_loud.py tests/test_action_plan_builder.py tests/test_daily_planner_repo.py -v`, then `.venv312/bin/python -m pytest -q`.
Expected: PASS, 0 failed. If another feedback test still passes `"material_cost"` in its realized dict, it now just skips the cost update. Fix only tests that assert on `cost_multiplier`.

- [ ] **Step 6 (Branch A): commit**

```bash
git add src/eve_online_industry_tracker/application/industry/overview_row.py src/eve_online_industry_tracker/application/daily_planner/models.py src/eve_online_industry_tracker/application/daily_planner/character_assigner.py src/eve_online_industry_tracker/application/daily_planner/action_plan_builder.py src/eve_online_industry_tracker/application/daily_planner/feedback_processor.py src/eve_online_industry_tracker/infrastructure/models.py src/eve_online_industry_tracker/infrastructure/schema_migrations.py tests/test_daily_planner_feedback.py tests/test_daily_planner_fail_loud.py tests/test_action_plan_builder.py
git commit -m "fix: compare realized and predicted cost on the same build-cost basis

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

# Batch 3 — visibility, tests and clean-up

### Task 8: Persist and show why BPO analysis was skipped

`bpo_analysis_skip_reason` only lives on the in-memory `ItemDecision`. `_build_plan_items` (`service.py:1082-1117`) writes the other BPO fields, `BuildPlanItemModel` (`infrastructure/models.py:1067-1088`) has no column for it, and the build-plan tab shows no BPO line at all. A stored row cannot tell "skipped" from "not attempted". `_model_to_dict` iterates the table's columns, so the API carries the new column without further changes.

**Files:**
- Modify: `src/eve_online_industry_tracker/infrastructure/models.py:1088`, `src/eve_online_industry_tracker/infrastructure/schema_migrations.py` (`build_plan_item` DDL, column migrations near `:871`), `src/eve_online_industry_tracker/application/daily_planner/service.py:1097-1116`, `src/streamlit_ui/components/daily_planner/tab_build_plan.py`
- Test: `tests/test_daily_planner_repo.py`, `tests/test_daily_planner_build_plan_tab.py` (create)

**Interfaces:**
- Consumes: `ItemDecision.bpo_analysis_skip_reason` (existing, and Task 4/5's new reasons).
- Produces: `BuildPlanItemModel.bpo_analysis_skip_reason: Mapped[Optional[str]]` and `tab_build_plan.bpo_analysis_caption(item: dict) -> str | None`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_daily_planner_repo.py`:

```python
def test_a_bpo_analysis_skip_reason_is_persisted(app_session, session_provider):
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    decision = _decision()
    decision.bpo_analysis_skip_reason = "no BPO market price for bp_type_id=999"
    _service(session_provider)._persist_plan_items(
        plan_id=1, decisions=[decision], market_depth_cache={}
    )
    item = app_session.query(BuildPlanItemModel).one()
    assert item.bpo_analysis_skip_reason == "no BPO market price for bp_type_id=999"


def test_ensure_app_schema_adds_the_skip_reason_column_to_a_legacy_table() -> None:
    conn = sqlite3.connect(":memory:")
    db = _FakeDb(conn)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS market_history (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "type_id INTEGER NOT NULL, region_id INTEGER NOT NULL, date TEXT NOT NULL,"
        "close REAL NOT NULL, high REAL, low REAL, volume INTEGER NOT NULL,"
        "order_count INTEGER NOT NULL, fetched_at DATETIME, updated_at DATETIME,"
        "UNIQUE(type_id, region_id, date))"
    )
    conn.execute(
        "CREATE TABLE build_plan_item (id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "plan_id INTEGER NOT NULL, type_id INTEGER NOT NULL, decision TEXT NOT NULL)"
    )
    conn.commit()

    ensure_app_schema(db)
    ensure_app_schema(db)  # idempotent

    columns = {row[1] for row in db.query("PRAGMA table_info(build_plan_item)")}
    assert "bpo_analysis_skip_reason" in columns
```

```python
# tests/test_daily_planner_build_plan_tab.py
"""The build-plan tab says why a BPO analysis is blank."""
from __future__ import annotations

from streamlit_ui.components.daily_planner.tab_build_plan import bpo_analysis_caption


def test_a_skipped_analysis_shows_its_reason():
    item = {"bpo_analysis_skip_reason": "sell velocity unknown (no corp sales in 30 days)",
            "bpo_market_price": None}
    assert bpo_analysis_caption(item) == (
        "BPO analysis skipped: sell velocity unknown (no corp sales in 30 days)"
    )


def test_a_completed_analysis_shows_its_result():
    item = {"bpo_analysis_skip_reason": None, "bpo_market_price": 20_000_000.0,
            "break_even_days": 12.4, "projected_annual_savings": 600_000_000.0}
    assert bpo_analysis_caption(item) == "BPO 20.00M ISK: 12d break-even, saves 600.00M ISK/yr"


def test_no_analysis_and_no_reason_shows_nothing():
    assert bpo_analysis_caption({"bpo_market_price": None}) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_repo.py tests/test_daily_planner_build_plan_tab.py -v`
Expected: FAIL with `AttributeError: 'BuildPlanItemModel' object has no attribute 'bpo_analysis_skip_reason'` (or the missing column), and `ImportError: cannot import name 'bpo_analysis_caption'`.

- [ ] **Step 3: Add the column, migration and write**

`infrastructure/models.py`, after `snapshot_sell_price` in `BuildPlanItemModel`:

```python
    bpo_analysis_skip_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
```

`schema_migrations.py`: in the `build_plan_item` CREATE TABLE DDL, replace `"snapshot_sell_price REAL NULL"` with `"snapshot_sell_price REAL NULL,"` followed by `"bpo_analysis_skip_reason TEXT NULL"`. After the `snapshot_sell_price` `_ensure_column`, add:

```python
    _ensure_column(db_app, table="build_plan_item", column="bpo_analysis_skip_reason", ddl_type="TEXT")
```

`service.py` `_build_plan_items`: add `bpo_analysis_skip_reason=d.bpo_analysis_skip_reason,` after `projected_annual_savings=d.projected_annual_savings,`.

- [ ] **Step 4: Show it in the build-plan tab**

`tab_build_plan.py`, add to the Helpers section:

```python
def bpo_analysis_caption(item: dict[str, Any]) -> str | None:
    """One line on the BPO investment analysis for the score breakdown, or None.

    A completed analysis shows its result, and a skipped one shows why, so
    blank BPO fields are never ambiguous between "skipped" and "not attempted".
    """
    reason = item.get("bpo_analysis_skip_reason")
    if reason:
        return f"BPO analysis skipped: {reason}"
    if item.get("bpo_market_price") is None:
        return None
    break_even = item.get("break_even_days")
    break_even_text = f"{float(break_even):.0f}d break-even" if break_even is not None else "no break-even"
    return (
        f"BPO {_fmt_isk(item.get('bpo_market_price'))}: {break_even_text}, "
        f"saves {_fmt_isk(item.get('projected_annual_savings'))}/yr"
    )
```

In `_render_score_breakdown`, after the `decision_reason` caption:

```python
    bpo_caption = bpo_analysis_caption(item)
    if bpo_caption:
        st.caption(bpo_caption)
```

- [ ] **Step 5: Run the tests, then the full suite**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_repo.py tests/test_daily_planner_build_plan_tab.py -v`, then `.venv312/bin/python -m pytest -q`.
Expected: PASS, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/infrastructure/models.py src/eve_online_industry_tracker/infrastructure/schema_migrations.py src/eve_online_industry_tracker/application/daily_planner/service.py src/streamlit_ui/components/daily_planner/tab_build_plan.py tests/test_daily_planner_repo.py tests/test_daily_planner_build_plan_tab.py
git commit -m "feat: persist and show why a BPO investment analysis was skipped

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Let the closed `blueprint_source_kind` enum survive fixture sanitisation

The sanitiser redacts every string not on `_PUBLIC_STRING_KEYS`. The local fixture confirms that every row has `blueprint_source_kind == "<redacted>"`, so the `"blueprint_sde_fallback"` branch in `chain_planner._me_saving_isk_per_batch` is never exercised by fixture tests. The producer assigns only these literals: `"unowned"`, `"owned_blueprint_copy"`, `"copied_from_owned_blueprint_original"`, `"unowned_blueprint_copy"`, `"owned_blueprint_original"`, `"blueprint_sde_fallback"` (`industry/service.py:4102-4146`, `:6949-6973`). Prerequisite-chain nodes also get `blueprint_source_kind = activity` (`:4102`), and activity is `"manufacturing"` or `"reaction"` (`:182-190`). None of these values names a player, corp or place. The ownership values disclose per-item BPO ownership, which is the same information as `has_bpo`, already passed verbatim as an accepted, documented trade-off. The allow-list is by *value*, so an unexpected value stays redacted. A test pins the producer's literals to the set.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/fixture_export.py:89-142`
- Test: `tests/test_fixture_sanitisation.py`

**Interfaces:**
- Produces: `fixture_export.BLUEPRINT_SOURCE_KINDS: frozenset[str]`.

- [ ] **Step 1: Verify the enum (read-only)**

Run: `grep -n 'blueprint_source_kind = ' src/eve_online_industry_tracker/application/industry/service.py`
Expected: only the six literals above plus `blueprint_source_kind = activity`. Then run `grep -n 'activity="' src/eve_online_industry_tracker/application/industry/service.py | grep -o 'activity="[a-z]*"' | sort -u` and confirm that `_plan_blueprint_chain_for_quantity` is only reached with `manufacturing`/`reaction`. If anything else appears, add it to the set below and note it in the commit message.

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_fixture_sanitisation.py`:

```python
import ast  # noqa: E402
import os  # noqa: E402

import pytest  # noqa: E402

from eve_online_industry_tracker.application.daily_planner.fixture_export import (  # noqa: E402
    BLUEPRINT_SOURCE_KINDS,
    sanitise_overview_rows,
)


@pytest.mark.parametrize("kind", sorted(BLUEPRINT_SOURCE_KINDS))
def test_a_blueprint_source_kind_survives_sanitisation(kind):
    (row,) = sanitise_overview_rows(
        [{"type_id": 1, "manufacturing_job": {"blueprint_source_kind": kind}}]
    )
    assert row["manufacturing_job"]["blueprint_source_kind"] == kind


def test_an_unknown_blueprint_source_kind_is_still_redacted():
    (row,) = sanitise_overview_rows(
        [{"type_id": 1, "manufacturing_job": {"blueprint_source_kind": "Some Pilot's hangar"}}]
    )
    assert row["manufacturing_job"]["blueprint_source_kind"] == "<redacted>"


def test_every_literal_source_kind_the_producer_assigns_is_allow_listed():
    producer = os.path.join(
        os.path.dirname(__file__), "..", "src", "eve_online_industry_tracker",
        "application", "industry", "service.py",
    )
    with open(producer, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    written = {
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "blueprint_source_kind" for t in node.targets)
        and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
    }
    assert written, "scan found no blueprint_source_kind assignments"
    assert written <= BLUEPRINT_SOURCE_KINDS, written - BLUEPRINT_SOURCE_KINDS
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_fixture_sanitisation.py -v`
Expected: FAIL with `ImportError: cannot import name 'BLUEPRINT_SOURCE_KINDS'`.

- [ ] **Step 4: Implement**

In `fixture_export.py`, after `_PUBLIC_NUMERIC_KEYS`:

```python
#: Every value IndustryService assigns to `blueprint_source_kind`: the
#: literals at industry/service.py:4102-4146 and :6949-6973, plus the activity
#: names a prerequisite-chain node inherits (`blueprint_source_kind = activity`).
#: A closed enum naming no player, corp or place. The ownership values disclose
#: per-item BPO ownership, the same information `has_bpo` already passes
#: verbatim (see "Known disclosure" above). Allow-listed by VALUE, so anything
#: unexpected is still redacted. tests/test_fixture_sanitisation.py pins the
#: producer's literals to this set.
BLUEPRINT_SOURCE_KINDS: frozenset[str] = frozenset({
    "unowned",
    "owned_blueprint_copy",
    "copied_from_owned_blueprint_original",
    "unowned_blueprint_copy",
    "owned_blueprint_original",
    "blueprint_sde_fallback",
    "manufacturing",
    "reaction",
})

# Keys whose string value survives verbatim only when it is one of a closed set.
_PUBLIC_ENUM_VALUES: dict[str, frozenset[str]] = {
    "blueprint_source_kind": BLUEPRINT_SOURCE_KINDS,
}
```

In `_sanitise_value`, replace the `isinstance(value, str)` branch:

```python
    if isinstance(value, str):
        if key in _PUBLIC_STRING_KEYS:
            return value
        if value in _PUBLIC_ENUM_VALUES.get(key, frozenset()):
            return value
        return _REDACTED
```

Add one sentence to the module docstring's allow-list paragraph: "`blueprint_source_kind` passes only when its value is one of the producer's closed enum (`BLUEPRINT_SOURCE_KINDS`)."

- [ ] **Step 5: Run the tests, then the full suite**

Run: `.venv312/bin/python -m pytest tests/test_fixture_sanitisation.py -v`, then `.venv312/bin/python -m pytest -q`.
Expected: PASS, 0 failed. The local fixture keeps `"<redacted>"` until it is re-captured (see Open questions).

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/fixture_export.py tests/test_fixture_sanitisation.py
git commit -m "fix: keep the closed blueprint_source_kind enum through fixture sanitisation

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Planner code clean-ups

Small changes of the same shape, each verified against current code:
- **utcnow:** `service.py:397` and `:1009` call `datetime.utcnow()` (naive UTC). The module's own `_now()` returns `datetime.now(tz=timezone.utc).replace(tzinfo=None)`, which is also naive UTC, so swapping keeps stored and compared semantics (the DB `DateTime` columns are naive). Tests: `test_daily_planner_feedback.py:81,321,407` and `test_daily_planner_repo.py:178,514,577`.
- **Formatting:** `service.py:531` reads `weights =self._repo...`.
- **Missing price:** a manufacturing material with no price logs at DEBUG (`shopping_list_builder.py:110-112`), while an invention input logs WARNING (`:153-159`). Both become WARNING.
- **Copy gate:** `character_assigner.py:229` and `:235` repeat `row.get("needs_invention") and row.get("has_t1_bpo")`.
- **Research id:** `character_assigner.py:173` and `:193` use `orow.get_blueprint_type_id(row) or type_id`, which uses a product id as a blueprint id. These now skip with a WARNING.
- **`_batch_runs`** (`character_assigner.py:480-501`) duplicates `from_overview`'s runs validation (`input_row.py:194-200`). One shared function replaces both; the messages are already identical.
- **T3 order:** `get_invention_source_blueprint_ids` picks the lowest source id (`ORDER BY b.blueprintTypeID` + `setdefault`), while `build_invention_source_index` keeps the first in dict order. `get_blueprint_manufacturing_data` has no ORDER BY, so dict order is unspecified. The index now keeps the lowest id too.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py:397,531,1009`, `shopping_list_builder.py:109-112`, `character_assigner.py:168-251,480-501`, `input_row.py:188-200`, `chain_planner.py:84-113`
- Test: `tests/test_daily_planner_feedback.py`, `tests/test_daily_planner_repo.py`, `tests/test_shopping_list_builder.py`, `tests/test_daily_planner_assignment.py`, `tests/test_planner_input_row.py`, `tests/test_chain_planner_keying.py`

**Interfaces:**
- Produces: `input_row.require_batch_runs(type_id: int, manufacturing_job: dict[str, Any]) -> int` (raises `PlannerInputError` with field `manufacturing_job.runs`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_planner_input_row.py`:

```python
@pytest.mark.parametrize("job, detail", [
    ({}, "is missing"),
    ({"runs": None}, "is missing"),
    ({"runs": "x"}, "is not an integer: 'x'"),
    ({"runs": 0}, "must be > 0, got 0"),
])
def test_batch_runs_have_one_validation(job, detail):
    from eve_online_industry_tracker.application.daily_planner.input_row import require_batch_runs
    with pytest.raises(PlannerInputError) as exc:
        require_batch_runs(12345, job)
    assert (exc.value.field, exc.value.detail) == ("manufacturing_job.runs", detail)
    assert require_batch_runs(12345, {"runs": 20}) == 20
```

Append to `tests/test_shopping_list_builder.py`:

```python
def test_an_unpriced_manufacturing_material_is_skipped_with_a_warning(caplog):
    with caplog.at_level("WARNING"):
        items = ShoppingListBuilder().build(
            assigned_actions=[_action(runs=1, materials={35: 10})], corp_assets=[],
            market_depth_cache={}, admin_settings=_Admin(), blueprint_data=BLUEPRINTS,
            meta_resolver=_NoBlueprints(),
        )
    assert items == []
    assert any("type_id=35" in r.getMessage() and r.levelno == logging.WARNING
               for r in caplog.records)
```

(Add `import logging` to that file's imports.)

Append to `tests/test_chain_planner_keying.py` (it uses the `ChainPlan` import that Task 6 added to this file):

```python
def test_the_invention_source_index_keeps_the_lowest_source_id_like_the_sde_query():
    from eve_online_industry_tracker.application.daily_planner.chain_planner import (
        build_invention_source_index,
    )
    data = {
        2000: {"invention": {"products": [{"type_id": 5000, "quantity": 1}]}},
        1000: {"invention": {"products": [{"type_id": 5000, "quantity": 1}]}},
    }
    assert build_invention_source_index(data) == {5000: 1000}


def test_research_without_a_blueprint_id_is_skipped_not_aimed_at_the_product(caplog):
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        CharacterAssigner,
    )
    row = {"type_id": 12345, "quantity": 1, "manufacturing_job": {"runs": 1},
           "needs_me_research": True, "needs_te_research": True}
    chars = SimpleNamespace(list_characters=lambda: [{
        "character_id": 1, "character_name": "Pilot",
        "skills": {"skills": [{"skill_name": "Laboratory Operation", "trained_skill_level": 5}]},
    }])
    with caplog.at_level("WARNING"):
        actions = CharacterAssigner().assign(
            ChainPlan(decisions=[_decision(row)]), [], chars, _AdminStub())
    assert [a.action_type for a in actions] == ["manufacture"]
    assert sum("research" in r.getMessage() and "blueprint type id" in r.getMessage()
               for r in caplog.records) == 2
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_planner_input_row.py tests/test_shopping_list_builder.py tests/test_chain_planner_keying.py -v`
Expected: FAIL with `ImportError: require_batch_runs`, no WARNING record (it is DEBUG), `{5000: 2000}`, and `['me_research', 'te_research', 'manufacture']`.

- [ ] **Step 3: One runs validation**

In `input_row.py`, add after `_require_key_optional_float`:

```python
def require_batch_runs(type_id: int, manufacturing_job: dict[str, Any]) -> int:
    """Blueprint runs for one batch, from manufacturing_job.runs. The single
    validation used by PlannerInputRow and CharacterAssigner.

    Required with NO fallback to `quantity` (a units total, not a run count).
    """
    if "runs" not in manufacturing_job:
        raise PlannerInputError(type_id=type_id, field="manufacturing_job.runs", detail="is missing")
    return _require_int(
        type_id, "manufacturing_job.runs", manufacturing_job.get("runs"), positive=True
    )
```

Replace `input_row.py:194-200` (the `if "runs" not in ...` block and the `runs = _require_int(...)` call) with `runs = require_batch_runs(type_id, manufacturing_job)`. Keep the comment above it.

In `character_assigner.py`, change the import to `from eve_online_industry_tracker.application.daily_planner.input_row import require_batch_runs`. Replace `_batch_runs`'s body (after its docstring) with `return require_batch_runs(type_id, orow.get_manufacturing_job(row))`. Change the docstring's first sentence to "Blueprint runs for one batch; see input_row.require_batch_runs."

- [ ] **Step 4: Research skip and copy gate**

In `CharacterAssigner`, add:

```python
    @staticmethod
    def _research_blueprint_type_id(row: dict[str, Any], type_id: int, kind: str) -> int | None:
        """The blueprint to research, or None (WARNING): a product id is not a blueprint."""
        bp_type_id = orow.get_blueprint_type_id(row)
        if bp_type_id is None:
            logger.warning(
                "CharacterAssigner: %s research flagged for type_id=%s but its row has no "
                "blueprint type id; not creating a research action",
                kind, type_id,
            )
        return bp_type_id
```

Replace the ME block (`:169-187`):

```python
        if row.get("needs_me_research"):
            bp_type_id = self._research_blueprint_type_id(row, type_id, "ME")
            char_id, char_name = (
                (None, None) if bp_type_id is None
                else self._best_research_char(char_slots, characters)
            )
            if char_id is not None:
                actions.append(AssignedAction(
                    type_id=bp_type_id,
                    type_name=type_name + " BPO",
                    action_type="me_research",
                    character_id=char_id,
                    character_name=char_name,
                    quantity=None,
                    runs=None,
                    estimated_cost_isk=None,
                    estimated_profit_isk=None,
                    estimated_completion=None,
                    notes=f"ME research: {row.get('me_current', '?')} → {row.get('me_research_target', '?')}",
                ))
                self._take_slot(char_slots, char_id, "me_research")
            elif bp_type_id is not None:
                logger.debug("CharacterAssigner: no free research slot for ME research on type_id=%s", type_id)
```

Replace the TE block (`:189-205`) the same way: `"TE"`, `action_type="te_research"`, the TE notes string, and `self._take_slot(char_slots, char_id, "te_research")`, with no `elif`, matching today's TE block.

Replace the copy gate (`:228-235`):

```python
        t1_blueprint_type_id = int(row.get("t1_blueprint_type_id") or 0)
        wants_copy = bool(row.get("needs_invention") and row.get("has_t1_bpo"))
        if wants_copy and t1_blueprint_type_id <= 0:
            logger.warning(
                "CharacterAssigner: has_t1_bpo set without a t1_blueprint_type_id for "
                "type_id=%s; not creating a copy action",
                type_id,
            )
        elif wants_copy:
```

The body under `elif wants_copy:` is unchanged.

- [ ] **Step 5: utcnow, formatting, price warning, T3 order**

- `service.py:397`: `cutoff = _now() - timedelta(days=history_days)`
- `service.py:1009`: `cutoff = (_now() - timedelta(days=14)).date().isoformat()`
- `service.py:531`: `weights = self._repo.get_weights(type_ids) if type_ids else {}`
- In `tests/test_daily_planner_feedback.py`, add `from datetime import timezone` and, below the imports, `def _utcnow() -> datetime: return datetime.now(timezone.utc).replace(tzinfo=None)`. Replace the three `datetime.utcnow()` calls with `_utcnow()`.
- In `tests/test_daily_planner_repo.py`, change the import to `from datetime import datetime, timezone`. Make `_now()` return `datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)`, and replace the two other `datetime.utcnow()` calls with `datetime.now(timezone.utc).replace(tzinfo=None)`.
- `shopping_list_builder.py:110-112`:

```python
                if estimated_unit_price is None or estimated_unit_price <= 0:
                    logger.warning(
                        "ShoppingListBuilder: skipping material type_id=%s for %s type_id=%s: "
                        "no price in market depth cache",
                        mat_type_id, action.action_type, action.type_id,
                    )
                    continue
```

- `chain_planner.build_invention_source_index`: replace `index.setdefault(invented_type_id, int(source_type_id))` with:

```python
                source = int(source_type_id)
                current = index.get(invented_type_id)
                if current is None or source < current:
                    index[invented_type_id] = source
```

  Change its docstring's last sentence to "When several sources invent into one blueprint the lowest source id wins, matching blueprints.get_invention_source_blueprint_ids (ORDER BY blueprintTypeID)."

- [ ] **Step 6: Run the tests, then the full suite**

Run: `.venv312/bin/python -m pytest tests/test_planner_input_row.py tests/test_shopping_list_builder.py tests/test_chain_planner_keying.py tests/test_daily_planner_fail_loud.py tests/test_daily_planner_assignment.py tests/test_daily_planner_feedback.py tests/test_daily_planner_repo.py -v`, then `grep -rn "utcnow" src/eve_online_industry_tracker/application/daily_planner tests/test_daily_planner_*.py`, then `.venv312/bin/python -m pytest -q`.
Expected: PASS, the grep prints nothing, and the full suite has 0 failed.

- [ ] **Step 7: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/service.py src/eve_online_industry_tracker/application/daily_planner/shopping_list_builder.py src/eve_online_industry_tracker/application/daily_planner/character_assigner.py src/eve_online_industry_tracker/application/daily_planner/input_row.py src/eve_online_industry_tracker/application/daily_planner/chain_planner.py tests/test_daily_planner_feedback.py tests/test_daily_planner_repo.py tests/test_shopping_list_builder.py tests/test_planner_input_row.py tests/test_chain_planner_keying.py
git commit -m "refactor: planner clean-ups (one runs check, research skip, utcnow, price warning, T3 order)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Failure visibility (compute banner, sell history, action types)

Verified:
- `_run_compute` shows `str(exc)` with no hint of which input failed (`service.py:495-499`).
- `_get_sell_velocities` drops a type on query failure (`:963-968`), and returns `{}` for no corporation (`:950-952`). The analyzer then reads 0.0 as "sold nothing".
- `tab_actions._CORP_LEVEL_ACTIONS` (`:21`) is unused.
- `_character_level_actions` (`:68-76`) drops unknown types silently.
- The status bar's pending count (`status_bar.py:335-339`) counts anything that is not a buy, including legacy `relist_order` rows.

The compute label comes from a context manager whose `__exit__` records the label without handling the exception. It needs no `except`, so the fail-loud guard's one-broad-except budget is unchanged. The innermost label wins.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py` (`_run_compute`, `_phase_1_collect`, `_phase_2_pipeline`, `_get_sell_velocities`, new `_ComputeStep`)
- Modify: `pipeline_analyzer.py` (`analyze`, `_analyze_single`)
- Modify: `src/streamlit_ui/components/daily_planner/status_bar.py`, `src/streamlit_ui/components/daily_planner/tab_actions.py`
- Test: `tests/test_daily_planner_fail_loud.py`, `tests/test_daily_planner_pipeline.py`, `tests/test_daily_planner_wiring.py:100-121`, `tests/test_daily_planner_tab_actions.py`

**Interfaces:**
- Consumes: Task 4's `velocity_unknown_reason` and its `why_no_sales` hook. Task 6's phase-1 `corp_material_stock`.
- Produces: `DailyPlannerService._get_sell_velocities(type_ids) -> tuple[dict[int, float], dict[int, str]]`, phase-1 key `"sell_velocity_unavailable": dict[int, str]`, `PipelineAnalyzer.analyze(..., sell_velocity_unavailable: dict[int, str] | None = None)`, `status_bar.CHARACTER_ACTION_TYPES: tuple[str, ...]`, `status_bar.CORP_LEVEL_ACTION_TYPES: frozenset[str]`, `status_bar.pending_character_action_count(actions: list[dict]) -> int`. The broad-failure status text is `f"Plan computation failed while {step}: {exc}"` when a step is known, else `str(exc)`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_daily_planner_fail_loud.py`, change `test_no_corporation_means_no_sales_history_lookups`'s assert to:

```python
    assert svc._get_sell_velocities([12345]) == ({}, {12345: "no corporation to read sell history for"})
```

and append:

```python
def test_a_failed_sell_history_query_is_recorded_per_item():
    svc = _bare_service()
    svc._corporations = SimpleNamespace(list_corporations=lambda: [{"corporation_id": 98000001}])

    def history(*, character_id, corporation_id, type_id, days):
        if type_id == 2:
            raise _sde_error()
        return [{"quantity": 30}]

    svc._sales_history = SimpleNamespace(get_sold_history=history)
    assert svc._get_sell_velocities([1, 2]) == (
        {1: 1.0}, {2: "sell history query failed (OperationalError)"}
    )


def test_a_failed_input_is_named_in_the_compute_banner():
    svc = _svc_with_broken_db()
    svc._feedback_processor = SimpleNamespace(process_pending_feedback=lambda: 0)
    svc._get_corp_wallet = lambda: None
    svc._get_overview_rows = lambda: [{"type_id": 1}]
    svc._build_input_rows = lambda rows: [SimpleNamespace(type_id=1, raw={"type_id": 1})]
    svc._get_industry_jobs = lambda: []
    svc._run_compute()  # _get_corp_assets hits the broken DB
    status = svc.get_compute_status()
    assert status["status"] == "failed"
    assert status["error"].startswith("Plan computation failed while reading corp assets: ")
    assert "database is locked" in status["error"]
```

Append to `tests/test_daily_planner_pipeline.py`:

```python
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
```

In `tests/test_daily_planner_wiring.py` `test_phase_2_passes_input_rows_and_the_resolver`, add `"sell_velocity_unavailable": {1: "x"},` to the phase-1 dict and `assert captured["sell_velocity_unavailable"] == {1: "x"}` to the asserts.

Replace `tests/test_daily_planner_tab_actions.py`'s import line and append:

```python
import logging  # noqa: E402

from streamlit_ui.components.daily_planner import tab_actions  # noqa: E402
from streamlit_ui.components.daily_planner.status_bar import (  # noqa: E402
    CHARACTER_ACTION_TYPES,
    pending_character_action_count,
)
from streamlit_ui.components.daily_planner.tab_actions import _character_level_actions  # noqa: E402


def test_an_unknown_action_type_is_logged_once(monkeypatch, caplog):
    monkeypatch.setattr(tab_actions, "_WARNED_UNKNOWN_ACTION_TYPES", set())
    actions = [{"id": 2, "action_type": "relist_order", "status": "pending"},
               {"id": 3, "action_type": "buy_bpo", "status": "pending"}]
    with caplog.at_level(logging.WARNING):
        _character_level_actions(actions)
        _character_level_actions(actions)
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "relist_order" in warnings[0] and "buy_bpo" not in warnings[0]


def test_the_tab_renders_exactly_the_character_action_types():
    assert tuple(t for t, _, _ in tab_actions._ACTION_ORDER) == CHARACTER_ACTION_TYPES


def test_the_pending_count_ignores_legacy_and_corp_level_rows():
    actions = [{"action_type": "manufacture", "status": "pending"},
               {"action_type": "relist_order", "status": "pending"},
               {"action_type": "buy_materials", "status": "pending"},
               {"action_type": "invent", "status": "done"}]
    assert pending_character_action_count(actions) == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_fail_loud.py tests/test_daily_planner_pipeline.py tests/test_daily_planner_wiring.py tests/test_daily_planner_tab_actions.py -v`
Expected: FAIL. `{} != ({}, {...})`, the error has no "Plan computation failed while" prefix, there is `TypeError: unexpected keyword argument 'sell_velocity_unavailable'`, a `KeyError`, and `ImportError: CHARACTER_ACTION_TYPES`.

- [ ] **Step 3: Name the failing step**

In `service.py`, add above `class DailyPlannerService`:

```python
class _ComputeStep:
    """Names the input or phase a compute failure came from, for the status banner.

    __exit__ only records the label (innermost wins) and returns False, so the
    exception propagates unchanged to _run_compute. There is no except clause,
    which keeps the fail-loud guard's single allowed broad handler where it is.
    """

    def __init__(self, service: "DailyPlannerService", label: str) -> None:
        self._service = service
        self._label = label

    def __enter__(self) -> "_ComputeStep":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is not None and self._service._failed_step is None:
            self._service._failed_step = self._label
        return False
```

In `__init__`, after `self._error: str | None = None`, add `self._failed_step: str | None = None`.

Replace `_run_compute`'s `try:` body and its broad handler:

```python
        self._failed_step = None
        try:
            with _ComputeStep(self, "collecting data (phase 1)"):
                phase1_data = self._phase_1_collect()

            if not phase1_data["overview_rows"]:
                logger.error(
                    "DailyPlannerService: aborting — no product overview rows available. "
                    "Open the Industry Builder and refresh the overview first, then recompute."
                )
                with self._lock:
                    self._status = "failed"
                    self._error = (
                        "No product overview available. Open Industry Builder → refresh the "
                        "product overview, then recompute the daily plan."
                    )
                return

            with _ComputeStep(self, "analysing pipelines (phase 2)"):
                pipeline_states = self._phase_2_pipeline(phase1_data)
            with _ComputeStep(self, "scoring profitability (phase 3)"):
                scored_items = self._phase_3_score(pipeline_states, phase1_data)
            with _ComputeStep(self, "deciding items (phase 4)"):
                top_level_decisions = self._phase_4_decide(scored_items, pipeline_states, phase1_data)
            with _ComputeStep(self, "planning chains (phase 5)"):
                chain_plan = self._phase_5_chain(top_level_decisions, phase1_data)
            with _ComputeStep(self, "assigning characters (phase 6)"):
                assigned_actions = self._phase_6_assign(chain_plan, phase1_data)
            with _ComputeStep(self, "building the shopping list (phase 7)"):
                shopping_items = self._phase_7_shopping(assigned_actions, phase1_data)
            with _ComputeStep(self, "building the action list (phase 8)"):
                action_log_rows = self._phase_8_actions(assigned_actions, shopping_items, phase1_data, chain_plan)
            with _ComputeStep(self, "saving the plan (phase 9)"):
                self._phase_9_persist(
                    phase1_data=phase1_data,
                    chain_plan=chain_plan,
                    action_log_rows=action_log_rows,
                )

            with self._lock:
                self._status = "done"
            logger.info("DailyPlannerService: plan computation complete")
```

Keep the `except PlannerInputError` handler unchanged. The broad handler becomes:

```python
        except Exception as exc:
            logger.exception(
                "DailyPlannerService: plan computation failed while %s", self._failed_step or "?"
            )
            with self._lock:
                self._status = "failed"
                self._error = (
                    f"Plan computation failed while {self._failed_step}: {exc}"
                    if self._failed_step else str(exc)
                )
```

Replace `_phase_1_collect`'s body after the log line. It keeps Task 6's corp stock and Task 10's formatting fix:

```python
        with _ComputeStep(self, "processing feedback on done actions"):
            feedback_count = self._feedback_processor.process_pending_feedback()
        if feedback_count > 0:
            logger.info("DailyPlannerService: processed %d feedback actions", feedback_count)

        with _ComputeStep(self, "reading the corp wallet"):
            corp_wallet = self._get_corp_wallet()

        # IndustryService overview rows, validated and cut to one row per
        # product (the producer emits one per blueprint variant). From here
        # on overview_rows holds only the kept rows, so every later phase and
        # every per-type query sees the same single row per type_id.
        with _ComputeStep(self, "reading the product overview"):
            input_rows = self._build_input_rows(self._get_overview_rows())
        overview_rows = [r.raw for r in input_rows]

        with _ComputeStep(self, "reading industry jobs"):
            industry_jobs = self._get_industry_jobs()
        with _ComputeStep(self, "reading corp assets"):
            corp_assets = self._get_corp_assets()
        # Material stock (blueprints excluded), shared by chain planning and
        # the shopping list.
        with _ComputeStep(self, "indexing corp material stock"):
            corp_material_stock = build_corp_stock_map(corp_assets, self._meta_resolver)
        with _ComputeStep(self, "reading corp market orders"):
            corp_orders = self._get_corp_orders()

        type_ids = [r.type_id for r in input_rows]
        with _ComputeStep(self, "reading learning weights"):
            weights = self._repo.get_weights(type_ids) if type_ids else {}
        with _ComputeStep(self, "reading sell history"):
            sell_velocities, sell_velocity_unavailable = self._get_sell_velocities(type_ids)

        # Market depth cache (from pre-populated tables written by MarketIntelligenceJob)
        hub = str(_adm(self._admin, "planner_market_hub", "jita"))
        with _ComputeStep(self, "reading market depth"):
            market_depth = self._repo.get_market_depth(type_ids, hub) if type_ids else {}

        margin_correlations = self._get_margin_correlations(type_ids)

        min_attempts = int(_adm(self._admin, "planner_invention_min_attempts", 10))
        with _ComputeStep(self, "reading invention success rates"):
            invention_rates = (
                self._repo.get_invention_success_rates(type_ids, min_attempts) if type_ids else {}
            )

        # Tritanium 7d price trend (for squeeze penalty gate)
        trit_trend_7d = self._get_trit_trend_7d()

        with _ComputeStep(self, "indexing corp blueprints"):
            bpo_assets_by_type_id, bpc_assets_by_type_id = self._index_blueprint_assets(corp_assets)

        # Blueprint data (SDE) — loaded for chain planning. Owned BPOs are
        # included because sub-manufacture only ever builds from an owned BPO,
        # and those blueprints are usually not themselves overview rows.
        with _ComputeStep(self, "reading blueprint data from the SDE"):
            blueprint_data = self._get_blueprint_data(
                overview_rows, extra_blueprint_type_ids=bpo_assets_by_type_id.keys()
            )

        return {
            "corp_wallet": corp_wallet,
            "overview_rows": overview_rows,
            "input_rows": input_rows,
            "industry_jobs": industry_jobs,
            "corp_assets": corp_assets,
            "corp_material_stock": corp_material_stock,
            "corp_orders": corp_orders,
            "weights": weights,
            "sell_velocities": sell_velocities,
            "sell_velocity_unavailable": sell_velocity_unavailable,
            "market_depth_cache": market_depth,
            "margin_correlations": margin_correlations,
            "invention_success_rates": invention_rates,
            "trit_trend_7d": trit_trend_7d,
            "blueprint_data": blueprint_data,
            "bpo_assets_by_type_id": bpo_assets_by_type_id,
            "bpc_assets_by_type_id": bpc_assets_by_type_id,
            "hub": hub,
        }
```

- [ ] **Step 4: Per-item sell-history reasons**

Replace `_get_sell_velocities`:

```python
    def _get_sell_velocities(self, type_ids: list[int]) -> tuple[dict[int, float], dict[int, str]]:
        """(units sold per day over 30 days, reason per type_id with no reading).

        A type id in the second map has no sell-history reading: there is no
        corporation to query for, or its query failed. That is not "sold
        nothing". PipelineAnalyzer records the reason on the item when it has
        no other velocity signal, instead of reading 0.0 as a measurement.
        """
        velocities: dict[int, float] = {}
        try:
            corp_id = self._get_corp_id()
        except (KeyError, TypeError, ValueError):
            # Same failure set _get_corp_wallet tolerates from list_corporations.
            logger.exception("DailyPlannerService: failed to resolve the corporation id")
            return velocities, {t: "corporation id unavailable" for t in type_ids}
        if corp_id <= 0:
            logger.warning("DailyPlannerService: no corporation, so no sell velocities")
            return velocities, {t: "no corporation to read sell history for" for t in type_ids}

        unavailable: dict[int, str] = {}
        for type_id in type_ids:
            try:
                txs = self._sales_history.get_sold_history(
                    character_id=0,
                    corporation_id=corp_id,
                    type_id=type_id,
                    days=30,
                )
                total_sold = sum(int(tx.get("quantity") or 0) for tx in txs)
            except (SQLAlchemyError, TypeError, ValueError) as exc:
                logger.warning(
                    "DailyPlannerService: sell history unavailable for type_id=%s",
                    type_id, exc_info=True,
                )
                unavailable[type_id] = f"sell history query failed ({type(exc).__name__})"
                continue
            velocities[type_id] = total_sold / 30.0
        return velocities, unavailable
```

`_phase_2_pipeline`: add `sell_velocity_unavailable=phase1["sell_velocity_unavailable"],` after `sell_velocities=...`.

`pipeline_analyzer.py`: add the parameter `sell_velocity_unavailable: dict[int, str] | None = None,  # type_id → why its sell history could not be read` after `sell_velocities` in `analyze()`. Pass `sell_velocity_unavailable=sell_velocity_unavailable or {},` to `_analyze_single` and add the matching keyword parameter `sell_velocity_unavailable: dict[int, str],` there. In Task 4's velocity block, replace the `elif`/`else` branches:

```python
        elif days_of_supply_for_velocity > 0.0:
            effective_velocity = max(0.01, 1.0 / days_of_supply_for_velocity)
            if type_id in sell_velocity_unavailable:
                logger.warning(
                    "PipelineAnalyzer: type_id=%s %s; velocity estimated from days of supply",
                    type_id, sell_velocity_unavailable[type_id],
                )
        else:
            # No signal at all. 0.01 keeps the pipeline-days division finite;
            # the reason marks it as a floor so nothing downstream reads it as
            # a measured sell rate.
            effective_velocity = 0.01
            why_no_sales = sell_velocity_unavailable.get(type_id, "no corp sales in 30 days")
            velocity_unknown_reason = f"{why_no_sales} and no days-of-supply estimate"
```

- [ ] **Step 5: One set of character action types**

In `status_bar.py`, near the top-level constants:

```python
#: Action types Tab 1 (tab_actions) renders, in workflow order. Defined here,
#: not in tab_actions, because tab_actions imports this module.
CHARACTER_ACTION_TYPES: tuple[str, ...] = (
    "deliver", "invent", "copy", "me_research", "te_research", "sub_manufacture", "manufacture",
)
#: Corp-level action types: shown in Tab 2 (shopping), never in Tab 1.
CORP_LEVEL_ACTION_TYPES: frozenset[str] = frozenset({"buy_materials", "buy_bpo"})


def pending_character_action_count(actions: list[dict[str, Any]]) -> int:
    """Pending actions a pilot still has to do. Rows of a type this planner
    no longer produces (e.g. a legacy relist_order) do not count."""
    return sum(
        1 for a in actions
        if str(a.get("status") or "pending") == "pending"
        and str(a.get("action_type") or "") in CHARACTER_ACTION_TYPES
    )
```

Replace `status_bar.py:335-339` with `pending_char_actions = pending_character_action_count(actions)`.

In `tab_actions.py`, delete `_CORP_LEVEL_ACTIONS` and its comment. Add `import logging` and extend the `status_bar` import with `CHARACTER_ACTION_TYPES, CORP_LEVEL_ACTION_TYPES`. Add below the imports:

```python
logger = logging.getLogger(__name__)

# Unknown action types already reported this process; Streamlit reruns the
# page on every interaction, and one warning per type is enough.
_WARNED_UNKNOWN_ACTION_TYPES: set[str] = set()
```

Replace `_character_level_actions`:

```python
def _character_level_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Actions shown in Tab 1: the types this tab renders.

    Corp-level buy actions belong to Tab 2. Any other type (e.g. one persisted
    by an older planner version) is dropped here, so it neither crashes the
    tab nor keeps "Day complete" from showing. It is logged once per type, so
    a type the planner starts producing without a section here is noticed.
    """
    rendered = set(CHARACTER_ACTION_TYPES)
    unknown = {
        str(a.get("action_type") or "") for a in actions
    } - rendered - CORP_LEVEL_ACTION_TYPES - _WARNED_UNKNOWN_ACTION_TYPES
    if unknown:
        logger.warning(
            "Daily planner actions tab: not showing action type(s) %s "
            "(not produced by this planner version)",
            ", ".join(sorted(unknown)),
        )
        _WARNED_UNKNOWN_ACTION_TYPES.update(unknown)
    return [a for a in actions if str(a.get("action_type") or "") in rendered]
```

- [ ] **Step 6: Run the tests, then the full suite**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_fail_loud.py tests/test_daily_planner_pipeline.py tests/test_daily_planner_wiring.py tests/test_daily_planner_tab_actions.py tests/test_daily_planner_end_to_end.py tests/test_daily_planner_status_banner.py -v`, then `.venv312/bin/python -m pytest -q`.
Expected: PASS, 0 failed. `test_a_failed_input_query_fails_the_compute_with_a_message` still passes: it replaces `_phase_1_collect` itself, so the label is "collecting data (phase 1)", and the message still contains "database is locked".

- [ ] **Step 7: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/service.py src/eve_online_industry_tracker/application/daily_planner/pipeline_analyzer.py src/streamlit_ui/components/daily_planner/status_bar.py src/streamlit_ui/components/daily_planner/tab_actions.py tests/test_daily_planner_fail_loud.py tests/test_daily_planner_pipeline.py tests/test_daily_planner_wiring.py tests/test_daily_planner_tab_actions.py
git commit -m "fix: name the failing input, record per-item sell-history gaps, report unknown action types

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Tighten the guard and integration tests

Verified:
- `test_every_allowed_broad_except_logs_with_a_traceback` passes on any handler whose `ast.dump` contains the substrings "logger" and "exception"/"exc_info" (`test_daily_planner_fail_loud.py:112-120`). A handler that only mentions them in a string constant passes.
- The pipeline fallback case asserts `> 0` (`test_daily_planner_integration.py:312`). The exact value is 4.0 (producer days) or 80 / 5.0 = 16.0.
- The profit tests cannot tell `* row.runs` apart from no factor: selling at cost gives 0 either way, and selling above cost is `> 0` either way (`:342-354`).
- The T1 check and the costed-row helper `pytest.skip` when the fixture lacks such rows (`:328`, `:416`). The spec requires the capture to contain T1 and T2 rows, so a missing class is a fixture defect and must fail. The local fixture has both, and 36 rows with runs > 1 plus isk/hour and material cost, so the baseline stays green.

**Files:**
- Modify: `tests/test_daily_planner_fail_loud.py:112-120`, `tests/test_daily_planner_integration.py:279-354,411-424`
- Test: the same files

**Interfaces:** none (tests only).

- [ ] **Step 1: A real traceback-log check, with its own self-test**

In `tests/test_daily_planner_fail_loud.py`, replace `test_every_allowed_broad_except_logs_with_a_traceback` with:

```python
def _logs_traceback(handler: ast.ExceptHandler) -> bool:
    """True when the handler body calls logger.exception(...) or logger.<x>(..., exc_info=True)."""
    for node in ast.walk(ast.Module(body=handler.body, type_ignores=[])):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)
                and func.value.id == "logger"):
            continue
        if func.attr == "exception":
            return True
        if any(kw.arg == "exc_info" and isinstance(kw.value, ast.Constant) and kw.value.value is True
               for kw in node.keywords):
            return True
    return False


def test_every_allowed_broad_except_logs_with_a_traceback():
    for name, path in _module_files():
        if ALLOWED_BROAD_EXCEPT.get(name, 0) == 0:
            continue
        for handler in _broad_handlers(_parse(path)):
            assert _logs_traceback(handler), (
                f"{name}:{handler.lineno}: a broad except must log the traceback"
            )


def test_the_traceback_check_needs_a_real_logging_call():
    def handler(src):
        return _broad_handlers(ast.parse(src))[0]

    assert _logs_traceback(handler("try:\n    x()\nexcept Exception:\n    logger.exception('boom')\n"))
    assert _logs_traceback(handler("try:\n    x()\nexcept Exception:\n    logger.error('b', exc_info=True)\n"))
    assert not _logs_traceback(handler("try:\n    x()\nexcept Exception:\n    s = 'logger.exception'\n"))
    assert not _logs_traceback(handler("try:\n    x()\nexcept Exception:\n    logger.error('exc_info')\n"))
    assert not _logs_traceback(handler("try:\n    x()\nexcept Exception:\n    logger.error('b', exc_info=False)\n"))
```

- [ ] **Step 2: Run it to verify the self-test bites**

Temporarily change `_logs_traceback`'s final `return False` to `return "exception" in ast.dump(handler)`, then run `.venv312/bin/python -m pytest tests/test_daily_planner_fail_loud.py::test_the_traceback_check_needs_a_real_logging_call -v`.
Expected: FAIL on the `s = 'logger.exception'` case. Revert to `return False`, run again, and it should PASS. Do not commit the temporary change.

- [ ] **Step 3: Pin the integration values**

In `tests/test_daily_planner_integration.py`, replace the parametrize and the final assert of `test_pipeline_supply_on_a_real_row_yields_non_zero_pipeline_days`:

```python
@pytest.mark.parametrize(
    "pipeline_days_supply, expected_days",
    [
        pytest.param(4.0, 4.0, id="producer-days"),
        # (50 in jobs + 30 on market) / 5.0 units/day
        pytest.param(None, 16.0, id="fallback-units-over-velocity"),
    ],
)
def test_pipeline_supply_on_a_real_row_yields_non_zero_pipeline_days(
    overview_rows, meta, pipeline_days_supply, expected_days
):
```

```python
    assert states[0].total_pipeline_days == expected_days
```

Replace `_first_scoreable_costed_row` and `test_selling_above_material_cost_scores_positive_batch_profit`:

```python
def _first_scoreable_costed_row(input_rows):
    """A multi-run row with an isk/hour and a material cost.

    A row with no isk/hour is unscoreable by design, so it cannot show cost
    subtraction. More than one run is needed to tell `x quantity` from the
    old `x quantity x runs` double count.
    """
    for row in input_rows:
        if (
            row.isk_per_hour is not None
            and row.material_cost_per_unit is not None
            and row.material_cost_per_unit > 0
            and row.runs > 1
        ):
            return row
    pytest.fail(
        "the captured fixture has no multi-run row with both an isk/hour and a "
        "material cost; re-capture it (spec: T1 and T2 rows, with and without BPC)"
    )


def test_selling_at_twice_material_cost_earns_cost_times_batch_units(input_rows, meta):
    """(2c - c) x quantity, where quantity is already units per run x runs.
    The old double count multiplied by runs again, and runs > 1 here."""
    row = _first_scoreable_costed_row(input_rows)
    scored = _score_at_price(row, meta, row.material_cost_per_unit * 2.0)
    assert scored.unscoreable_reason is None
    assert scored.absolute_profit_per_batch == pytest.approx(row.material_cost_per_unit * row.quantity)
    assert scored.absolute_profit_per_batch != pytest.approx(
        row.material_cost_per_unit * row.quantity * row.runs
    )
```

In `test_t1_bpo_analysis_gets_past_the_blueprint_price_lookup`, replace the `if not t1: pytest.skip(...)` lines with:

```python
    assert t1, "the captured fixture has no Tech I rows; re-capture it (spec: T1 and T2 rows)"
```

- [ ] **Step 4: Prove the profit test now discriminates**

Temporarily change `profitability_scorer._compute_absolute_profit`'s return to `(sell_price - row.material_cost_per_unit) * row.quantity * row.runs, None`, then run `.venv312/bin/python -m pytest tests/test_daily_planner_integration.py -k twice -v`.
Expected: FAIL. Revert the change, run again, and it should PASS. Do not commit the temporary change. (This needs the local fixture. Without it the module skips; say so in the task report.)

- [ ] **Step 5: Run the full suite**

Run: `.venv312/bin/python -m pytest -q`
Expected: 0 failed.

- [ ] **Step 6: Commit**

```bash
git add tests/test_daily_planner_fail_loud.py tests/test_daily_planner_integration.py
git commit -m "test: check for a real traceback log call and pin integration values

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Dropped after verification

- **6(a): sub-manufacture over-builds for parents that got no slot.** This is not reachable in current code. `CharacterAssigner.assign` sorts `build`/`pause` decisions by `-adjusted_score` (`character_assigner.py:116-117`). Sub-components are created with `adjusted_score=0.0` (`chain_planner.py:486`), and every top-level `build` passes the isk/hour gate with multipliers ≥ 0, so subs sort after every top-level build (the sort is stable). Slot counters only ever decrease (`_take_slot`). So a top-level parent left without a manufacturing slot means no free slot remains for any sub-component, and no `sub_manufacture` action is created. `ShoppingListBuilder` only subtracts *assigned* sub actions (`shopping_list_builder.py:75-80`), so the parent then buys the component. No phase reorder is needed. The invariant was implicit in a score sort (a negative stored weight, which Task 2 also closes, could have broken it), so Task 6 makes it explicit with a two-pass loop and pins it with `test_sub_components_are_assigned_only_after_every_top_level_item`.
- **Item 1, partly.** No `PlannerInputError` is raised beneath any per-item handler today. The only raiser outside `input_row.py` is `character_assigner._batch_runs`, called from `assign()`, which has no handler. The fix is kept (Task 1) because it is latent, and one future call would make it live.

## Open questions for the controller

1. **A corrupt learned weight (NaN or text in `plan_learning_weights`).** Task 2 reads it as neutral 1.0 with a WARNING. The alternative is failing the compute. Which one do you want?
2. **Sub-manufacture quantities ignore structure and rig material bonuses.** The planner does not know where the sub job runs, so Task 6 buys an upper bound (slightly more than needed). Is that acceptable, or should sub jobs reuse the parent's structure profile?
3. **Task 7, Branch A.** `plan_item_outcome.predicted_material_cost`/`actual_material_cost` would from then on hold per-unit *build* cost (materials plus job costs). Keep those column names, or add new columns? Also, if Step 1 finds the producer's `total_cost` lacks copy/invention while the realized cost includes them, T2 items keep a residual bias. Is that accepted?
4. **Re-capturing the local fixture after Task 9.** Re-capture so `blueprint_source_kind` is real in fixture tests? The fixture is gitignored, and whether to commit it stays your call, per the earlier ruling.
5. **No corporation listed.** Task 3 makes the wallet unknown (`None`), not 0 ISK. Confirm that is the intended product meaning.

## Self-review

- **Spec coverage:** the branch principle drives Tasks 2, 3, 4, 6, 11 (skip/WARNING/`None`, never a constant). Fail-loud policy is Task 1 and Task 12. Real numbers are Tasks 5, 6 and 7. Public-repo fixture rules are Task 9. Migrations are additive via `_ensure_column`: Task 8, and Task 7 branch A.
- **Placeholder scan:** every code step shows code. Task 7's branch B docstring has one `<evidence file:line>` slot that the trace step fills in. It is the trace's output, not a design gap.
- **Type consistency:** `velocity_unknown_reason` (Tasks 4, 5, 11), `me_adjusted_batch_quantity(base_qty_per_run, runs, me)` (Tasks 5, 6), `build_corp_stock_map(corp_assets, meta_resolver)` and `"corp_material_stock"` (Tasks 6, 11), `require_batch_runs(type_id, manufacturing_job)` (Task 10), `_get_sell_velocities -> (dict, dict)` and `"sell_velocity_unavailable"` (Task 11), `estimated_build_cost_isk` (Task 7A), `bpo_analysis_skip_reason` column (Task 8).
- **Review Focus:** each of the five lines has a named test in its owning task (Tasks 2, 3, 5, 6, 6).
