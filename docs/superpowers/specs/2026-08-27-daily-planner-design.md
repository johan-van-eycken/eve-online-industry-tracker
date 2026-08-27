# Daily Planner — Design Spec

**Date:** 2026-08-27
**Status:** Approved for implementation

---

## Overview

A new Streamlit page — **Daily Planner** — that acts as a fully automated industry manufacturing task selector. When opened, it shows the user exactly what to do that day across all characters in the industry corporation to maximise ISK profit. The plan persists across days, adapts to market conditions, and improves its predictions over time via a self-learning feedback loop.

The user's daily workflow: open the page → deliver finished jobs → start new jobs per the action list → buy materials per the shopping list → close. Repeat daily.

---

## Scope

### In scope
- New `DailyPlannerService` orchestration layer
- Persistent build plan stored in `eve_app.db`
- Skill-aware per-character job assignment
- Full BPC/Invention/Copy chain planning with pipeline gap prevention
- BPO investment analysis (T1 only)
- ME/TE research job planning
- Material shopping list (net of corp stock)
- Self-learning feedback loop (predicted vs actual profitability/velocity)
- New Streamlit page with four tabs

### Out of scope
- Automatic in-game job submission (read-only ESI; user executes actions manually)
- T2/Faction/Deadspace BPO recommendations (these BPOs do not exist)
- Multi-corporation support (single corp assumed)
- Starting new reaction jobs (may be added later); existing reaction jobs ARE included in DELIVER actions and their reaction slots ARE counted against reaction slot availability (controlled by `Mass Reactions` + `Advanced Mass Reactions` skills — a separate pool from manufacturing and research/invention slots)

---

## Architecture

### Data sources (existing, unchanged)

| Service / Model | What the planner reads |
|---|---|
| `IndustryService` | Full profitability overview rows per blueprint (ISK/hour, margin, material cost, market intelligence, pipeline, days-of-supply) |
| `CorporationIndustryJobsModel` | Active jobs per character: activity type, completion time, delivery status |
| `CorporationAssetsModel` | All corp stock: raw materials, BPOs, BPCs across all hangar divisions (1–7) |
| `CorporationMarketOrdersModel` | Open sell orders, pipeline units on market |
| `CorporationModel.wallets` | Corp main account balance — stored as a JSON column (`wallets`) on `CorporationModel`, one entry per division. Division 1 is labeled "Master Wallet". Balance is stored as a string; parse as float. No separate `CorporationWalletModel` table exists. |
| `PricingSuggestionService` | Relist/reprice signals for open sell orders |
| `CharacterModel` (skills) | Per-character slot capacities and skill levels |

Character-level assets and wallets are **not** used for materials or capital; only character skills and slot data are read at character level.

### Verified data availability (pre-implementation checks)

| Concern | Verdict | Notes |
|---|---|---|
| FIFO cost basis — corp vs character scope | **Handled** | `_get_owned_item_inventory` correctly switches between `CharacterAssetHistoryModel` and `CorporationAssetHistoryModel` based on `owned_blueprints_scope` |
| Corp + character job slot tracking | **Handled** | `industry_active_jobs` queries both `CorporationIndustryJobsModel` and `CharacterIndustryJobsModel`; `installer_id` attributes corp jobs to the installing character |
| Sub-manufacture cost calculation | **Handled** | `_build_manufacture_job_plan` is recursive (depth ≤ 8), applies all ME/rig/skill bonuses at every level; `ShoppingListBuilder` must call this existing method rather than reimplement it |
| Blueprint physical location | **Handled** | `top_location_name` is resolved via `resolve_top_location_name_map` (ESI `/universe/structures/` + `/universe/stations/`) and included in every blueprint payload |
| `CorporationWalletModel` | **No such model** | Balance is in `CorporationModel.wallets` JSON column, division 1 = "Master Wallet", stored as string — parse as float |
| `optimal_ME_from_IndustryService` | **Does not exist** | Needs a new function — see Phase 5 |
| Market hub configurability | **Partial** | Hub is a per-request parameter defaulting to Jita; `get_material_sell_price_map` is hardcoded Jita — must NOT be used by the planner |
| Structure/location in action list | **Available** | `industry_profile.location_name` and `profile_name` are included in every overview row; planner must surface them in action list |
| Datacore/decryptor stock check | **Handled, with caveat** | `_plan_take_or_buy_material_nodes` deducts owned datacores/decryptors; BUT the `industry_hangar_flag` admin setting can restrict which hangar division is visible — items in other divisions are invisible |
| Batch-to-sale attribution in feedback loop | **Approximate** | `CorporationRealizedProfitLedgerService` uses temporal FIFO (not exact batch matching); noise is smoothed by EMA over many samples |

### New components

```
DailyPlannerService
  ├── PipelineAnalyzer      pipeline state per item (stock + market + in-flight)
  ├── ProfitabilityScorer   adjusted scoring with self-learning weights
  ├── ItemDecisionEngine    build / watch / pause / skip per item
  ├── ChainPlanner          BPC/invention/copy/BPO resolution per build item
  ├── CharacterAssigner     skill-aware job → character mapping
  ├── ShoppingListBuilder   net material needs + BPO opportunities
  ├── ActionPlanBuilder     final ordered per-character action list
  └── PlanPersistence       DB read/write for build plan + daily action log

Flask routes (new)
  POST /planner/compute       trigger plan computation (background job)
  GET  /planner/status        poll computation progress
  GET  /planner/plan          fetch current active plan
  POST /planner/action/done   mark a daily action as done
  GET  /planner/analytics     fetch self-learning accuracy stats

Streamlit page
  streamlit_ui/pages/daily_planner.py
```

---

## Data Model

Five new tables added to `eve_app.db` via `schema_migrations.py`.

### `build_plan`

Stores one active plan at a time. When recomputed, the prior plan is archived (status → `'archived'`), not deleted.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `created_at` | DATETIME | |
| `updated_at` | DATETIME | |
| `status` | TEXT | `'active'` \| `'stale'` \| `'archived'` |
| `corp_wallet_snapshot` | REAL | Corp ISK at plan creation |
| `market_snapshot_hash` | TEXT | Hash of key item prices; used for staleness detection |
| `freshness_score` | REAL | 0.0–1.0; drops as prices drift from snapshot |
| `plan_summary_json` | TEXT | High-level metadata: capital allocation, slot usage, projected ISK/week |

### `build_plan_item`

One row per evaluated product per plan.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `plan_id` | INTEGER FK → `build_plan` | |
| `type_id` | INTEGER | EVE type ID of the product |
| `type_name` | TEXT | |
| `meta_group_id` | INTEGER | From SDE; gates BPO analysis (only meta group 1 eligible) |
| `decision` | TEXT | `'build'` \| `'watch'` \| `'pause'` \| `'skip'` |
| `decision_reason` | TEXT | Plain-language explanation shown in UI |
| `target_batches` | INTEGER | Batches to maintain in pipeline. Formula: `ceil(effective_velocity × production_lead_time_days / runs_per_batch)` where `production_lead_time_days` = manufacturing job duration in days |
| `priority_score` | REAL | Composite adjusted score |
| `isk_per_hour` | REAL | At plan creation time |
| `margin_pct` | REAL | |
| `days_of_supply_current` | REAL | Market + in-flight jobs at plan time |
| `pipeline_stage` | TEXT | `'manufacturing'` \| `'invention'` \| `'copying'` \| `'researching'` \| `'watching'` |
| `bpo_investment_recommended` | BOOLEAN | NULL for T2+/Faction/Deadspace items (analysis never ran). TRUE/FALSE only for meta group 1 items. |
| `bpo_market_price` | REAL | Lowest sell price for BPO type at hub (market orders only; contract-only BPOs will show NULL) |
| `break_even_days` | REAL | |
| `projected_annual_savings` | REAL | Material savings × planned annual runs |

### `daily_action_log`

Append-only. One row per recommended action per computation. Marking `status='done'` is how the user tracks execution; these records feed back into the self-learning tables.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `plan_id` | INTEGER FK → `build_plan` | |
| `generated_at` | DATETIME | |
| `character_id` | INTEGER | Nullable for `buy_materials` and `buy_bpo` actions (corp-level, not character-specific; any character with market access can execute) |
| `character_name` | TEXT | Nullable for the same corp-level action types |
| `action_type` | TEXT | `'deliver'` \| `'manufacture'` \| `'sub_manufacture'` \| `'invent'` \| `'copy'` \| `'me_research'` \| `'te_research'` \| `'buy_materials'` \| `'buy_bpo'` \| `'relist_order'` |
| `shopping_category` | TEXT | For `buy_materials` rows: `'current_job'` \| `'future_stock'` \| `'invention_input'` |
| `type_id` | INTEGER | |
| `type_name` | TEXT | |
| `quantity` | INTEGER | |
| `runs` | INTEGER | |
| `estimated_cost_isk` | REAL | |
| `estimated_profit_isk` | REAL | |
| `estimated_completion` | DATETIME | |
| `status` | TEXT | `'pending'` \| `'done'` \| `'skipped'` |
| `processed_for_feedback` | BOOLEAN | Default FALSE. Set TRUE once this row's outcome has been written to `plan_item_outcome` and EMA weights updated. Prevents re-processing on subsequent recomputations. |
| `notes` | TEXT | Plain-language context, e.g. "BPC stock covers 1.8 days — start now" |

### `plan_item_outcome`

Written when a manufacturing batch completes and sells. Actuals are sourced from `CorporationRealizedSalesLedgerModel` (existing realized profit tracking via FIFO cost + wallet transactions), matched to `daily_action_log` records by `type_id` and completion timeframe. If no matching sale appears within the configurable slow-mover timeout (default 60 days), an outcome row is written with `actual_sell_days` set to the timeout value and `slow_mover = true`, so the velocity score for that item is penalised rather than left unresolved indefinitely.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `plan_item_id` | INTEGER FK → `build_plan_item` | |
| `type_id` | INTEGER | |
| `completed_at` | DATETIME | |
| `predicted_isk_per_hour` | REAL | |
| `actual_isk_per_hour` | REAL | `actual_profit / (manufacturing_duration_hours + actual_sell_days × 24)`. NULL for slow_mover outcomes (no realized sale). |
| `accuracy_ratio` | REAL | `actual_isk_per_hour / predicted_isk_per_hour`. NULL for slow_mover outcomes (no realized sale, so actual_isk_per_hour is NULL). |
| `predicted_sell_days` | REAL | |
| `actual_sell_days` | REAL | From wallet transaction timestamps; capped at slow-mover timeout |
| `slow_mover` | BOOLEAN | True if outcome was written due to timeout, not an actual sale |
| `predicted_material_cost` | REAL | |
| `actual_material_cost` | REAL | From `CorporationRealizedSalesLedgerModel` FIFO cost |

### `plan_learning_weights`

One row per type_id. Updated after each `plan_item_outcome` is recorded using exponential moving average (EMA).

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `type_id` | INTEGER UNIQUE | |
| `accuracy_ema` | REAL | EMA of `accuracy_ratio`; default 1.0 |
| `velocity_multiplier` | REAL | actual/predicted sell velocity EMA; default 1.0 |
| `cost_multiplier` | REAL | actual/predicted material cost EMA; default 1.0 |
| `sample_count` | INTEGER | Completed batches informing this record |
| `last_updated` | DATETIME | |
| `confidence_tier` | TEXT | `'low'` (<5 samples) \| `'medium'` (5–19) \| `'high'` (≥20) |

---

## Plan Computation Logic

Triggered via `POST /planner/compute`. Runs in a background thread. Eight sequential phases.

### Phase 1 — Data Collection

Fetches in parallel from existing services:
- Corp wallet balance
- All `IndustryService` overview rows — if overview data is older than 12 hours, the planner warns the user and offers to trigger an Industry Builder refresh before proceeding
- Active corp industry jobs per character (completion times, activity type, slot usage)
- Corp assets (materials, BPOs, BPCs) across all hangar divisions
- Corp market orders (open sell orders)
- `plan_learning_weights` per type_id
- Sell velocity per type_id from `SalesHistoryService` (used as `sell_velocity_per_day` in Phase 2). The feedback check also runs here — `daily_action_log` rows with `status='done'` and `processed_for_feedback=false` are matched against `CorporationRealizedSalesLedgerModel`, new `plan_item_outcome` records are written, and `plan_learning_weights` are updated before new scoring begins. Each processed row is then marked `processed_for_feedback=true` to prevent re-processing.

### Phase 2 — Pipeline State per Item

For every manufacturable item:

```
# sell_velocity_per_day fallback: if SalesHistoryService returns 0 (item never sold),
# use (1 / days_of_supply) from IndustryService row, floored at a minimum of 0.01 units/day
effective_velocity = max(0.01, sell_velocity_per_day × velocity_multiplier)

total_pipeline_days = (
    corp_stock_units
    + units_in_active_manufacturing_jobs
    + units_on_market
) / effective_velocity

bpc_runs_available  = sum(BPC.runs for BPC in corp_assets where product = item)
invention_in_flight = active invention jobs for this item
copy_in_flight      = active copy jobs feeding this item's invention
```

### Phase 3 — Profitability Scoring

```
adjusted_score = isk_per_hour
  × accuracy_ema            # self-learning: penalises chronic mispredicts vs actual profit
  × velocity_multiplier     # self-learning: rewards fast sellers
  × cost_multiplier         # self-learning: adjusts for systematic material cost misprediction
  × market_timing_factor    # 1.0 if 7d trend >= -5%; linear decay to 0.5 at -15%; floored at 0.5
                            # formula: max(0.5, 1.0 - (abs(trend) - 5) / 20) when trend < -5%
  × pipeline_saturation     # > 1.0 if days_of_supply < 3d (urgency boost); 1.0 at 3d; linear decay to 0.0 at 14d+
                            # formula: max(0, 1 - (days_of_supply - 3) / 11)
                            # e.g. 0 days → 1.27 (urgent), 3 days → 1.0, 14 days → 0.0
  × confidence_tier_bonus   # 1.0 / 1.05 / 1.15 for low / medium / high
```

### Phase 4 — Item Decision

| Decision | Condition |
|---|---|
| `build` | `adjusted_score` > minimum ISK/hour threshold (default 5M ISK/hr, configurable) AND pipeline < 3 days of supply |
| `watch` | Profitable but pipeline ≥ 3 days of supply (already saturated) |
| `pause` | Currently building but 7d price trend < -8% — finish in-flight, don't restart |
| `skip` | `adjusted_score` ≤ minimum threshold, high anomaly risk, or insufficient liquidity |

**Note on overlapping price thresholds:** `market_timing_factor` (applied in Phase 3) continuously penalises the score as trend falls below -5%, reaching its floor of 0.5 at -15%. This is a soft signal. `pause` (decided in Phase 4) is a hard override that triggers at -8% regardless of score — it forces an item from `build` to `pause` even if its adjusted score remains above the minimum threshold. These two mechanisms serve different roles: one modulates scoring, the other overrides the decision.

The goal is for the planner to be self-optimising through its feedback loop. Session-only UI overrides (force include / force exclude) are available in Tab 3 for the current view only and are never persisted — they reset on the next recomputation.

### Phase 5 — Full Production Chain Planning

For every `build` item:

```
if meta_group_id == 1:  # T1 items — BPO path available
    if BPO owned in corp assets:
        # compute_optimal_me(blueprint) must be implemented as a new function
        # (does not currently exist in the codebase). Algorithm:
        #   For each material in the blueprint:
        #     find the smallest ME level where
        #     ceil(base_qty × (1 - 0.01 × (ME+1))) == ceil(base_qty × (1 - 0.01 × ME))
        #     i.e. no further unit reduction from one more ME level
        #   optimal_ME = max across all materials (all must reach diminishing returns)
        #   optimal_TE = TE level where time savings per level fall below a configurable
        #                threshold (default: < 1% time saved per level)
        # This function belongs in blueprints.py or IndustryService as a static utility.
        schedule ME research jobs if current_ME < compute_optimal_me(blueprint)
        schedule TE research jobs if current_TE < compute_optimal_te(blueprint)
        plan manufacturing jobs from BPO

    elif BPC available in corp assets:
        plan manufacturing jobs from BPC
        run BPO investment analysis

    elif BPO purchasable on market:
        run BPO investment analysis
        if break_even_days < 30:       recommend BPO purchase (★ Strong Buy)
        elif break_even_days <= 90:    flag as investment opportunity
        else:                          add BPC/items to shopping list

    else:
        add BPC or finished items to shopping list

else:  # T2+ items — invention chain only, no BPO path
    if BPC available in corp assets:
        plan manufacturing jobs from BPC

    else:
        if BPO for T1 base item in corp assets:
            plan copy jobs → plan invention jobs
            # Forward look: start invention now if BPC stock < production_lead_time_days
            add datacores + decryptors to shopping list (category: 'invention_input')
        else:
            add T1 BPC or base item to shopping list
            # Flag the T1 base item BPO for BPO investment analysis.
            # Context: this analysis is for the T1 BPO as an invention enabler
            # (not as a product to manufacture and sell), so the break-even
            # calculation uses invention-driven run counts, not direct sales velocity.

# Sub-manufacture decision (applied to every required input material regardless of T1/T2)
for each required_material of a planned manufacturing job:
    if BPO owned for required_material (meta_group_id == 1):
        sub_manufacture_cost = compute manufacturing cost from BPO
                               (materials + job cost using existing IndustryService)
        market_buy_cost      = hub_sell_price × quantity_needed

        if sub_manufacture_cost < market_buy_cost:
            # Two cases for time-awareness:
            if character_has_free_manufacturing_slot_now:
                # Slot is free: start sub-manufacture immediately.
                # Parent job is scheduled to start after sub-manufacture completes.
                plan sub_manufacture job (action_type = 'sub_manufacture')
                # sub_manufacture jobs follow same character assignment rules as manufacturing
                add sub-material inputs to shopping list (category: 'current_job')
            else:
                # Slot is busy: earliest_free_slot = end_date of earliest-completing job
                if sub_manufacture_duration <= time_until_earliest_free_slot:
                    # Sub-manufacture finishes by the time the slot frees — start now
                    plan sub_manufacture job (action_type = 'sub_manufacture')
                    add sub-material inputs to shopping list (category: 'current_job')
                else:
                    # Sub-manufacture would still be running when the parent needs to start
                    add to shopping list (category: 'current_job') with note:
                        "sub-manufacture too slow for this cycle — buying from market"
        else:
            add to shopping list (category: 'current_job')
    else:
        add to shopping list (category: 'current_job')
```

**BPO Investment Analysis:**

```
bpo_market_price     = lowest sell order for BPO type_id at hub (ESIService)
me_research_cost     = Σ job_cost per ME level (current_ME → target_ME)
me_research_time     = SDE blueprint research time × character skill modifiers
material_savings/run = base_material_cost × (bonus_at_target − bonus_at_current)
total_investment     = bpo_market_price + me_research_cost + te_research_cost
break_even_days      = total_investment / (savings_per_run × planned_runs_per_day)
projected_annual_savings = savings_per_run × planned_runs_per_day × 365
```

Only items with `meta_group_id == 1` reach this analysis. T2, Faction, Deadspace, and Officer items are excluded — no purchasable BPO exists for them.

### Phase 6 — Skill-Aware Character Assignment

Available slots per character are derived from character skills using the existing slot capacity calculation already implemented in the Industry Slots page. Active manufacturing and research jobs reduce their respective slot pools. Reaction jobs occupy a separate reaction slot pool (controlled by `Mass Reactions` + `Advanced Mass Reactions`) and do not affect manufacturing or research slot availability.

Assignment priority:
1. Manufacturing and sub-manufacture jobs → character with most free manufacturing slots; ties broken by fewest total active jobs. Sub-manufacture jobs follow the same assignment rules as regular manufacturing jobs and consume the same slot type.
2. Invention jobs → character with highest relevant Science + Metallurgy skill sum and free research slot
3. Copy jobs → character with most free research slots; ties broken by highest Research skill
4. ME/TE research → character with most free research slots

ME/TE research, copy, and invention jobs all compete for the same research slot pool — the assigner never double-books a character's research capacity.

If no character has a free slot for a planned job, that job is deferred to the next plan cycle: it remains visible in Tab 3 (Build Plan) with a note explaining why it was not scheduled, but does not appear in Tab 1 (Today's Actions).

### Phase 7 — Material Shopping List

Materials are aggregated across all planned jobs and categorised so the user understands what each purchase is for.

```
# Note: in EVE, materials are consumed from the corp hangar at job start,
# so CorporationAssetsModel already excludes materials tied to active jobs.
# 'already_allocated' below prevents double-booking the same corp stock
# across two planned jobs that would both start today.
for each required_material across all planned jobs:
    net_required = quantity_needed − corp_assets_available
                                   − already_allocated_to_other_planned_jobs_today
    if net_required <= 0: skip (already covered by stock)

    determine shopping_category:
        'current_job'     — material needed for a job in today's action list
        'future_stock'    — material needed for jobs in the current plan that are
                            not starting today (pre-buy across the full planning window
                            to avoid restocking delays on subsequent days)
        'invention_input' — datacores, decryptors, T1 BPCs for invention jobs

    sub-manufacture decisions are resolved in Phase 5.
    Items where sub-manufacture was chosen appear as 'sub_manufacture' actions
    in the daily action log, not in the shopping list.
    Items where market buy was chosen (cheaper or sub-manufacture too slow)
    appear in the shopping list with their category and a note if applicable.

    # Hangar visibility caveat: corp asset queries are filtered by the
    # `industry_hangar_flag` admin setting. Datacores, decryptors, and other
    # invention inputs stored in a different hangar division than the configured
    # flag will NOT be counted in corp_assets_available and will appear in the
    # shopping list even if the corp physically has them.
    # Mitigation: if datacores appear unexpectedly in the shopping list,
    # check the industry_hangar_flag setting covers all relevant divisions.

    estimated_cost = hub_sell_price × net_required
    # cost_multiplier from plan_learning_weights is a per-product diagnostic signal
    # (how well we predicted a product's total material cost), not a per-material
    # price signal. It is used in Phase 3 scoring to improve profitability predictions
    # for manufactured items, not applied to individual shopping line items.
```

The shopping list in the UI groups rows by `shopping_category` with subtotals per category and a grand total ISK figure vs corp wallet balance.

### Phase 8 — Daily Action List

Per character, ordered:

1. **DELIVER** — corp jobs with `end_date < now` (manufacturing, copy, invention, reaction, research)
2. **START INVENTION** — highest priority items needing BPCs, research slots free
3. **START COPY** — BPO copy jobs feeding invention pipeline
4. **START ME/TE RESEARCH** — BPOs with research below target, research slots free
5. **START SUB-MANUFACTURE** — component jobs that must complete before parent manufacturing starts
6. **START MANUFACTURING** — highest priority items with BPCs ready, manufacturing slots free
7. **RELIST** — market orders flagged by `PricingSuggestionService`

Each action includes: item name, quantity/runs, estimated cost, estimated completion time, **structure name** (`industry_profile.location_name`) and **profile name** (`industry_profile.profile_name`) for all job actions (manufacture, invent, copy, me_research, te_research, sub_manufacture), and a plain-language note explaining the reasoning. Structure context is sourced from the `industry_profile` embedded in every `IndustryService` overview row — no additional lookup required.

### Phase 9 — Persistence

- Archive current active plan (`status → 'archived'`)
- Insert new `build_plan` record
- Insert `build_plan_item` rows per product decision
- Insert `daily_action_log` rows per character action
- Compute `market_snapshot_hash`: SHA-256 of hub sell prices for all `build` items' products plus their primary input materials (Tritanium, Pyerite, Mexallon, Isogen, Nocxium, Zydrine, Megacyte, Morphite and all T2 component inputs present in planned jobs)

---

## Staleness Detection

On page load, the planner re-hashes current prices of all items in the active plan and compares to `market_snapshot_hash`.

```
drifted_count  = count of items where abs(current_price - snapshot_price) / snapshot_price > drift_threshold
freshness_score = 1 - (drifted_count / total_tracked_items)
```

If `freshness_score` drops below 0.9 (i.e., more than 10% of tracked items have drifted), the UI shows a warning and prompts recomputation. Both the drift threshold (default 5%) and the freshness warning level (default 0.9) are configurable via admin settings.

---

## Self-Learning Feedback Loop

After each plan cycle, the system checks `daily_action_log` for `manufacture` actions marked `done` and `processed_for_feedback=false`. Actuals are read from `CorporationRealizedProfitLedgerService` (which exists and handles corp scope). **Attribution method:** temporal FIFO — sales are matched to batches by `type_id` chronologically, not by an exact batch-to-sale link (EVE's API provides no such link). For frequently manufactured items this introduces some noise; EMA smoothing (α=0.2) averages it out over many samples. For low-volume items (capitals, rare T2), the noise may be more pronounced and confidence tier will remain `low` longer. For each matched batch:

1. Read `actual_sell_days`, `actual_material_cost`, and realized profit from `CorporationRealizedSalesLedgerModel`
2. Write a `plan_item_outcome` record
3. Update `plan_learning_weights` using EMA (α = 0.2):

   **For normal outcomes** (`slow_mover = false`):
   ```
   accuracy_ema        = 0.8 × old_accuracy_ema + 0.2 × (actual_isk_per_hour / predicted_isk_per_hour)
   velocity_multiplier = 0.8 × old_velocity + 0.2 × (predicted_days / actual_days)
   cost_multiplier     = 0.8 × old_cost + 0.2 × (actual_material_cost / predicted_material_cost)
   ```

   **For slow mover outcomes** (`slow_mover = true`, no realized sale):
   ```
   # accuracy_ema and cost_multiplier are NOT updated (no realized data)
   velocity_multiplier = 0.8 × old_velocity + 0.2 × (predicted_days / slow_mover_timeout_days)
   # This penalises the velocity score proportionally to how far actual exceeded predicted
   ```

   ```
   confidence_tier = 'low' if sample_count < 5 else 'medium' if < 20 else 'high'
   ```

New items start at weight 1.0. After 5+ batches the system has signal; after 20+ the weight becomes a reliable predictor of future performance.

---

## Streamlit Page Design

**File:** `streamlit_ui/pages/daily_planner.py`

### Plan Status Bar (always visible)

```
[ Freshness: 94% ]  [ Last Computed: 2h ago ]  [ Corp Wallet: 14.2B ISK ]
[ Capital Reserved: 3.1B ISK ]  [ Projected ISK Return (7d): +8.4B ISK ]

[ Recompute Plan ]   ⚠ "Tritanium shifted 7.2% — consider recomputing"
```

**Capital Reserved** = sum of `estimated_cost_isk` for all pending `buy_materials` and `buy_bpo` actions + sum of estimated job install costs for all planned manufacturing and sub-manufacture jobs. Represents the ISK that will be committed if the user follows the full day's plan.

**Projected ISK Return (7d)** = sum of `estimated_profit_isk` for all in-flight and planned manufacturing jobs whose estimated completion date + velocity-adjusted sell time falls within the next 7 days. Sell time per item uses `sell_velocity_per_day × velocity_multiplier` from learning weights.

Plan recomputation is always manual — triggered by the "Recompute Plan" button. There is no automatic scheduled recomputation.

### Tab 1 — Today's Actions *(primary view)*

Per-character accordion sections. Within each character, actions are grouped and ordered by type: Deliver → Invent → Copy → ME/TE Research → Sub-Manufacture → Manufacture → Relist.

Each action row has a checkbox. Checking it calls `POST /planner/action/done` and writes `status='done'` to `daily_action_log` in real-time. "Mark All Delivered Done" convenience button per character marks only that character's DELIVER actions as done (not all action types). Sections only render when at least one action of that type exists for the character — a character with no copy jobs will show no COPY section.

Example layout (not all sections will always appear):
```
▼ Aldara Voss   [3 mfg slots free]  [2 research slots free]

  🔴 DELIVER   [ Mark All Delivered Done ]
     ☐  Tengu ×5 runs — move to corp hangar

  🟡 INVENT
     ☐  Tengu BPC ×10 attempts — ~1.2M ISK — est. 18h
        "BPC stock covers 1.8 days — start now to avoid slot gap"

  🔧 SUB-MANUFACTURE
     ☐  Crystalline Carbonide Armor Plate ×200 — ~3.1M ISK — est. 6h
        "cheaper to build than buy (market: 4.2M ISK) — must complete before Cerberus job"

  🟢 MANUFACTURE
     ☐  Cerberus ×3 runs — ~42M ISK — est. 4d 6h
     ☐  Medium Shield Extender ×10 runs — ~8M ISK — est. 14h

  🔵 RELIST
     ☐  Tengu — advised 387M (currently 371M, +4.3% margin)
```

### Tab 2 — Shopping List

Two AG-Grid sections:

**Materials to Buy** — aggregated, net of corp stock, grouped by category with subtotals:

*Current Job Materials* — needed for jobs starting today:

| Item | Quantity | Est. Unit Price | Est. Total | Source | Note |
|---|---|---|---|---|---|
| Tritanium | 4,200,000 | 5.1 ISK | 21.4M ISK | Jita sell | |
| Fullerite-C320 | 800 | 48,200 ISK | 38.6M ISK | Jita sell | |

*Future Build Stock* — needed for jobs planned in coming days:

| Item | Quantity | Est. Unit Price | Est. Total | Source | Note |
|---|---|---|---|---|---|
| Morphite | 6,200 | 890 ISK | 5.5M ISK | Jita sell | for Cerberus batch in 2d |

*Invention Inputs* — datacores, decryptors, T1 BPCs for invention jobs:

| Item | Quantity | Est. Unit Price | Est. Total | Source | Note |
|---|---|---|---|---|---|
| Caldari Encryption Methods | 20 | 12,400 ISK | 248K ISK | Jita sell | |
| Occult Process Decryptor | 5 | 4,100 ISK | 20.5K ISK | Jita sell | |

**Total to spend: X ISK** — Corp wallet: Y ISK — Remaining after purchase: Z ISK

**BPO Investment Opportunities** — T1 only, sorted by break-even speed:

| BPO | Market Price | Break-even | Annual Savings | Recommendation |
|---|---|---|---|---|
| Medium Shield Extender | 420M ISK | 22 days | 6.8B ISK | ★ Strong Buy |
| Caracal | 320M ISK | 67 days | 2.1B ISK | Consider |

T2 item BPOs never appear (they don't exist). T1 BPOs may appear either as direct product investments or as invention enablers for T2 items (e.g. a Caracal BPO appearing because it enables Cerberus invention).

### Tab 3 — Build Plan

AG-Grid of all evaluated items. Filterable by decision type. Shows: item name, decision, priority score, ISK/hour, margin %, days of supply, confidence tier, sample count, decision reason.

Session-only override buttons (force include / force exclude / reset) affect the current view only and are never written to the database. They reset when the plan is recomputed. The goal is for the self-learning feedback loop to make manual overrides unnecessary over time.

### Tab 4 — Plan Analytics

- **Accuracy** — per-item predicted vs actual ISK/hour over last N batches (default N = 20, configurable via admin settings)
- **Velocity** — predicted vs actual sell days per item; slow_mover outcomes shown distinctly
- **Plan History** — timeline of recomputations; per-cycle changes tracked as decision shifts per item (e.g. `build → watch`, `skip → build`), net ISK earned vs projected

---

## Integration with Existing Features

- **Industry Builder** — `IndustryService` overview rows are the primary profitability input; no changes to `IndustryService` itself
- **Industry Slots** — `CorporationIndustryJobsModel` data is shared; the planner reads it but does not duplicate the slots page
- **Market Orders** — `PricingSuggestionService` relist signals feed directly into the RELIST action type
- **Portfolio Planner** — the existing Portfolio Planner page remains unchanged; `DailyPlannerService` internally uses a similar allocation approach but extends it with chain planning and persistence
- **Shopping List** — the existing `aggregate_shopping_list` helper is reused and extended in `ShoppingListBuilder`

---

## Flask Route Changes

New blueprint: `flask_app/routes/daily_planner.py`

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/planner/compute` | Trigger background plan computation |
| `GET` | `/planner/status` | Poll computation progress |
| `GET` | `/planner/plan` | Fetch current active plan (build_plan + items + daily actions) |
| `POST` | `/planner/action/done` | Mark a `daily_action_log` row as done |
| `GET` | `/planner/analytics` | Fetch self-learning accuracy and velocity stats |

---

## Implementation Notes

- **Primary market hub** for shopping list and BPO price lookups: configurable via admin settings (default: `jita`). The planner must pass this hub explicitly to all `MarketPricingService` calls. `get_material_sell_price_map()` is hardcoded to Jita and must NOT be used by the planner — use `get_type_price_map(hub=configured_hub)` instead.
- Background computation follows the same pattern as `IndustryService` refresh (background thread, progress callbacks, polled by frontend)
- Plan recomputation is always manual — no scheduled auto-recompute
- Schema migrations added to `schema_migrations.py` for all five new tables
- EMA weight α = 0.2 (configurable via admin settings)
- Staleness price drift threshold = 5% (configurable via admin settings)
- IndustryService overview freshness threshold = 12 hours (configurable via admin settings)
- BPO break-even thresholds (Strong Buy: 30d, Consider: 90d) configurable via admin settings
- Slow-mover timeout = 60 days (global, configurable via admin settings); applies uniformly to all item categories
- Minimum ISK/hour threshold for `build` decision = 5M ISK/hr (configurable via admin settings)
- `freshness_score` warning level = 0.9 (configurable via admin settings)
- Tab 4 accuracy window N = 20 batches (configurable via admin settings)
- Pipeline gap prevention look-ahead: start invention/copy if current BPC stock covers less than `job_duration_days + 1` of planned manufacturing (+1 day safety buffer for job setup overhead)
- `GET /planner/plan` computes live `freshness_score` on each call and writes the updated value back to `build_plan`, so the DB always holds the most recent staleness assessment
