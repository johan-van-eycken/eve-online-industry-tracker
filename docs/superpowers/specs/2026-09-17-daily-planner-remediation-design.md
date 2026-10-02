# Daily Planner — Remediation & Cluster D Completion

**Date:** 2026-09-17
**Status:** Approved for implementation
**Addendum to:** `2026-08-27-daily-planner-design.md`
**Branch:** `feature/daily-planner`

---

## Why this exists

A code review of the `application/daily_planner` package (3,240 new lines, 11 files) found 15
defects. Fixing them one by one would be the wrong response, because they are not 15 independent
mistakes — they are one mistake repeated.

The planner was written against an *imagined* shape of the IndustryService overview row. Of the 33
row keys the package reads, **15 are written by nobody** — not by the producer, not by the planner
itself. Because nearly every access is defensive (`or 0.0`, `getattr(..., None)`, bare
`except Exception`), none of those misses raise. The pipeline runs to completion and persists a
plan built on zeros.

The existing unit tests pass because they synthesise the fictional shapes. `test_daily_planner_pipeline.py`
sets `corp_stock_qty` and a top-level `job.activity_id`; neither exists in production data.

So the remediation has two halves: make the planner read the data that is actually there, and make
it impossible for the same class of mistake to pass silently again.

---

## Root cause, in detail

### Keys the planner reads that nothing writes

`bpc_in_flight`, `copy_in_flight`, `corp_stock_qty`, `corp_stock_units`,
`estimated_material_cost_per_unit`, `invention_in_flight`, `market_units`,
`material_cost_per_unit`, `meta_group_id`, `pipeline_stage`, `price_trend_pct`,
`product_quantity`, `runs_per_batch`, `type_meta_group_id`, `units_on_market`.

(`pipeline_stage`, `needs_me_research`, `needs_te_research`, `needs_invention`, `invention_materials`,
`quantity_needed` and `sub_manufacture_cost` *are* written by the planner itself, as dataclass
attributes or dict keys, and are not part of the producer contract.)

### The producer already publishes the right values

Every mapping below was verified against the producer source and, where possible, against the live
`database/eve_app.db` and `database/eve_sde.db`.

| Planner reads | Actual source |
|---|---|
| `product_quantity` | `row["quantity"]` |
| `runs_per_batch` | `manufacturing_job["runs"]` |
| `estimated_material_cost_per_unit`, `material_cost_per_unit` | `manufacturing_job["material_cost"]` — a **total**, divide by quantity |
| `corp_stock_qty`, `units_on_market` | `row["pipeline_units_in_jobs"]`, `row["pipeline_units_on_market"]` (`industry/service.py:2009-2010`) |
| own `total_pipeline_days` computation | `row["pipeline_days_supply"]` — already computed (`industry/service.py:2013`) |
| `price_trend_pct` | `row["price_trend_7d_pct"]` (`industry/service.py:1941`) |
| `meta_group_id`, `type_meta_group_id` | not on the row; resolve from SDE `types.metaGroupID` by `type_id` |
| character `active_skill_level` | `trained_skill_level` |
| asset `is_blueprint` | `is_blueprint_copy` (no `is_blueprint` attribute exists anywhere) |
| BPC `runs` | `blueprint_runs` |
| job `activity_id` | not a column; present in the `raw` JSON payload |

Two observations worth recording:

- `models.py:20` declares `price_trend_7d_pct` with the comment *"may be renamed from
  price_trend_pct"*. The dataclass field is correct; the reader in `PipelineAnalyzer` is not. The
  uncertainty was known and shipped.
- `PipelineAnalyzer` reimplements a pipeline-days calculation that the producer already performs.
  The fix is deletion, not correction.

### The correct accessors already exist

`src/streamlit_ui/state/industry_builder_ui.py` contains `get_product_quantity()`,
`get_effective_runs()`, `get_manufacturing_job()` and `skill_requirements_met()`. These work against
real rows — the Industry Builder UI runs on them daily. The planner did not reuse them; it guessed
the key names instead.

This is why the design below builds on those accessors rather than deriving a fresh contract from
7,000 lines of producer code. Grounding the contract in code that demonstrably works is stronger
than grounding it in a careful reading, and it removes a duplicate implementation.

---

## Design

### 1. Shared overview-row accessors

Move the four accessors from `streamlit_ui/state/industry_builder_ui.py` into a new
`src/eve_online_industry_tracker/application/industry/overview_row.py`. Both the Streamlit UI and
the planner import from there. The UI keeps thin re-export wrappers so existing call sites are
untouched.

Accessors gain the fields the planner needs and the UI already reads elsewhere:
`get_material_cost_per_unit()`, `get_pipeline_units()`, `get_pipeline_days_supply()`,
`get_price_trend_7d_pct()`, `get_price_trend_30d_pct()`.

### 2. `PlannerInputRow`

A frozen dataclass in `application/daily_planner/input_row.py` with a `from_overview()` classmethod
built on those accessors. It is the *only* place in the package that touches a raw overview dict.

```python
@dataclass(frozen=True)
class PlannerInputRow:
    type_id: int
    type_name: str
    quantity: int
    runs: int
    material_cost_per_unit: float
    pipeline_units_in_jobs: int
    pipeline_units_on_market: int
    pipeline_days_supply: float | None
    price_trend_7d_pct: float
    price_trend_30d_pct: float | None
    meta_group_id: int | None
    isk_per_hour: float
    profit_amount: float
    profit_margin_fraction: float
    days_of_supply: float | None
    blueprint_type_id: int | None

    @classmethod
    def from_overview(cls, row: dict, *, meta_groups: MetaGroupResolver) -> "PlannerInputRow":
        ...  # raises PlannerInputError naming the missing field and the type_id
```

Required vs optional is explicit. A required field that is absent or unparseable raises
`PlannerInputError`; an optional field (`pipeline_days_supply`, `price_trend_30d_pct`) is `None` and
every consumer handles `None` deliberately.

Planner-internal annotations stay off the dict: they become fields on the planner's own dataclasses
rather than keys written back into the overview row.

### 3. Fail-loud policy

- `PlannerInputRow.from_overview()` raises on contract violation. Plan generation aborts.
- `DailyPlannerService.compute_plan_async()` lets `PlannerInputError` propagate to the recorded
  compute status; the Flask route returns it; the Streamlit status bar renders a red banner naming
  the missing field and the offending `type_id`.
- Every `except Exception` in the package narrows to `(KeyError, TypeError, ValueError)` or to the
  specific expected exception. Where a broad catch is genuinely correct (a background job that must
  not die), it logs at `error` with the traceback instead of swallowing.
- The `or 0.0` / `or 0` defaults on contract fields are removed. They remain only where zero is a
  real, meaningful default.

This changes the contract for callers: the planner can now fail where it previously returned a
plan. That is the intent — a plan built on zeros is worse than no plan.

### 4. Test strategy

**`tests/conftest.py`** — does not currently exist; every test file repeats its own `sys.path`
bootstrap. Add one with the path setup and shared fixtures (in-memory engine, `ensure_app_schema`,
planner repo).

**Real-row capture fixture.** Overview rows live only in an in-memory cache
(`get_cached_overview_rows()` reads `_state._overview_result_cache`), so they cannot be read from the
database. Capture procedure, run once:

1. Start the app, open Industry → trigger a product overview refresh.
2. Dump `_state._overview_result_cache["rows"]` to `tests/fixtures/overview_rows_real.json`
   via a capture helper script under `scripts/`.
3. Trim to ~20 representative rows: T1 and T2, with and without BPC, at least one row where
   optional fields are legitimately absent.

`tests/test_overview_row_contract.py` asserts that every field `PlannerInputRow` requires is present
in the captured fixture. When the producer renames a key, that test fails — instead of the planner
silently zeroing.

**The fixture must be sanitised before it is committed.** This repository is public
(`johan-van-eycken/eve-online-industry-tracker`), and a raw overview dump exposes the corporation's
holdings, production mix and margins — competitively relevant information in EVE.

What the tests actually need is the *shape*, not the values. So the capture script sanitises in
place:

- **Keep:** every key and the full nesting structure (this is what the contract test checks);
  `type_id` and `type_name` (public SDE data).
- **Replace:** all ISK amounts, quantities, stock levels, velocities and margins with
  synthetic-but-plausible values, generated from a fixed seed so the fixture is deterministic.
- **Drop entirely:** wallet balances, character and corporation ids and names, station and structure
  ids, order ids, API tokens.

The sanitiser is itself tested: a test asserts the committed fixture contains none of the dropped
key names. The real unsanitised dump stays local and gitignored.

**Per-cluster TDD.** Each finding below gets a test written against the real fixture (or a real ORM
object built from the live schema) that fails first.

**Note on `.gitignore`:** `*.md` is ignored repo-wide. The existing spec predates that rule and is
tracked. New spec files and this addendum need `git add -f`.

---

## Work clusters

### Cluster A — accessors and contract

Resolves findings 1, 3, 4, 5, 6, 10 and the seven unreported phantom keys.

| # | Location | Fix |
|---|---|---|
| 1 | `character_assigner.py:262` | `trained_skill_level`; pilots regain up to 11 slots |
| 3 | `service.py:777` | `is_blueprint_copy` + SDE category check in `_index_blueprint_assets` |
| 4 | `pipeline_analyzer.py:54` | `is_blueprint_copy` and `blueprint_runs` |
| 5 | `profitability_scorer.py:140` | `manufacturing_job["material_cost"] / quantity`; restores the 20M gate |
| 6 | `pipeline_analyzer.py:115` | producer pipeline fields (see Cluster B) |
| 10 | `service.py:584` | `corp` is a dict — subscript, not `getattr` |
| — | `models.py` consumers | `price_trend_7d_pct`, SDE-resolved `meta_group_id` |

### Cluster B — delete the reimplementations

`PipelineAnalyzer._compute_pipeline_days()` is removed in favour of `row["pipeline_days_supply"]`,
falling back to a single explicit computation from `pipeline_units_*` only when the producer returns
`None` (zero 7-day volume). The `>= 3d` watch gate then fires on real numbers.

### Cluster C — genuine calculation errors

| # | Location | Fix |
|---|---|---|
| 7 | `shopping_list_builder.py:61` | multiply per-run material quantities by `action.runs` |
| 8 | `shopping_list_builder.py:72` | update `already_allocated` **before** the stock-covered `continue`, so corp stock is not double-spent across jobs |
| 9 | `character_assigner.py:70` | decrement slot counters per assigned action, not per decision, so ME and TE research cannot share one slot |
| 14 | `feedback_processor.py:249` | filter realized sales to `transaction_date >= action.generated_at` and to the action's `type_id`, so pre-dating sales stop corrupting the EMA weights |

### Cluster D — complete the unfinished Phase A work

This is build-out, not repair. Approved as in scope.

**D1 — job activity indexing (finding 2).**
`CorporationIndustryJobsModel` has no `activity_id` column; the value sits in the `raw` JSON. The
live database confirms the distribution: 1=manufacturing (315 jobs), 3=TE research (21), 4=ME
research (8), 5=copying (108), 8=invention (29).

Add a real `activity_id` integer column plus an index, backfilled from
`json_extract(raw, '$.activity_id')` in `schema_migrations.ensure_app_schema`, and populate it on
ingest in the corporation job sync. `PipelineAnalyzer` then filters on the column.
`has_active_manufacturing_jobs` starts working and the `pause` decision becomes reachable.

**D2 — real freshness scoring (finding 15).**
`_compute_freshness_score` has an empty loop body and a comment saying a true check "would parse the
hash". A SHA-256 digest cannot be parsed — the design is unimplementable as written.

Replace it: add `snapshot_sell_price` to `build_plan_item`, written at plan creation. Freshness is
then the fraction of plan items whose current `spot_sell_price` has drifted less than
`planner_price_drift_threshold_pct` from their stored snapshot price. The existing whole-plan hash
is retained as a cheap "anything changed at all" short-circuit.

Also remove the write-on-read side effect: `get_active_plan()` currently persists a recomputed
freshness score on every GET. Compute it for the response; persist only on recompute.

**D3 — chain planner keying (finding 11).**
`chain_planner.py:285` looks up product `type_id` values in blueprint-keyed maps. Introduce an
explicit `product_type_id → blueprint_type_id` index from the SDE blueprint loader and use it at
every crossing point. Build-vs-buy for sub-manufacture starts running and
`ChainPlan.sub_manufacture_actions` stops returning empty.

**D4 — copy action (finding 12).**
`character_assigner.py:158` gates on `has_t1_bpo`, which nothing writes; `ChainPlanner` writes
`needs_t1_bpo`. Align on `needs_t1_bpo` and cover the now-live branch with a test — this is dead
code being switched on for the first time, so it needs behavioural tests, not just a rename.

**D5 — plan history (finding 13).**
`service.py:248` binds an ISO string to the `DateTime` column `BuildPlanModel.created_at`. Pass a
`datetime` object. `plan_history` stops being permanently empty.

**D6 — meta group resolution.**
`build_plan_item.meta_group_id` is a real column the planner never fills correctly, because the row
carries no meta group id. Add a small SDE-backed resolver over `types.metaGroupID` (verified
present) and populate it at plan-item write time.

---

## Migrations

Both additive, both in `ensure_app_schema`, both idempotent per the existing `_ensure_*` helper
pattern:

1. `corporation_industry_jobs.activity_id INTEGER` + index, backfilled from `raw`.
2. `build_plan_item.snapshot_sell_price REAL`.

---

## Out of scope

- Any change to the overview row **producer**. The planner adapts to the producer, not the reverse.
- Converting `IndustryService` to return typed objects instead of dicts. Desirable, far larger, and
  it would touch the whole Streamlit UI.
- The scoring formulas themselves. `market_timing_factor`, `pipeline_saturation` and the build/skip
  gate were checked against `2026-08-27-daily-planner-design.md:536-583` and match.
- The dead margin-normalisation block at `profitability_scorer.py:35-37`. Harmless; its fallback key
  never exists in practice.

---

## Success criteria

1. `tests/test_overview_row_contract.py` passes against the captured real fixture.
2. An integration test drives the full pipeline from the real fixture plus real ORM objects and
   produces a plan with non-zero `absolute_profit_per_batch`, non-zero `total_pipeline_days` and
   more than one slot per pilot.
3. Renaming any required key in the captured fixture makes a test fail with a message naming that
   key — verified by deliberately breaking one.
4. No `except Exception` remains in `application/daily_planner/` without either a narrowed exception
   tuple or an `error`-level log with traceback.
5. `grep -r 'or 0\.0' application/daily_planner/` returns only cases where zero is a documented,
   meaningful default.
6. `plan_history` returns rows after two consecutive computes.
7. Freshness score changes when a market price moves past the drift threshold, and a plain GET of
   the active plan performs no write.
8. The committed fixture contains no wallet balance, character/corporation identity, structure id or
   token — asserted by a test, not by inspection.
