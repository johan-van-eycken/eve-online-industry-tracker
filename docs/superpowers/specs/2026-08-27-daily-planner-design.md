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
- Starting new reaction jobs (may be added later); existing reaction jobs ARE included in DELIVER actions and their slots ARE counted against research slot availability

---

## Architecture

### Data sources (existing, unchanged)

| Service / Model | What the planner reads |
|---|---|
| `IndustryService` | Full profitability overview rows per blueprint (ISK/hour, margin, material cost, market intelligence, pipeline, days-of-supply) |
| `CorporationIndustryJobsModel` | Active jobs per character: activity type, completion time, delivery status |
| `CorporationAssetsModel` | All corp stock: raw materials, BPOs, BPCs across all hangar divisions (1–7) |
| `CorporationMarketOrdersModel` | Open sell orders, pipeline units on market |
| `CorporationWalletModel` | Corp main account balance (capital budget) |
| `PricingSuggestionService` | Relist/reprice signals for open sell orders |
| `CharacterModel` (skills) | Per-character slot capacities and skill levels |

Character-level assets and wallets are **not** used for materials or capital; only character skills and slot data are read at character level.

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
| `target_batches` | INTEGER | Batches to maintain in pipeline |
| `priority_score` | REAL | Composite adjusted score |
| `isk_per_hour` | REAL | At plan creation time |
| `margin_pct` | REAL | |
| `days_of_supply_current` | REAL | Market + in-flight jobs at plan time |
| `pipeline_stage` | TEXT | `'manufacturing'` \| `'invention'` \| `'copying'` \| `'watching'` |
| `bpo_investment_recommended` | BOOLEAN | Only set for meta group 1 items |
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
| `character_id` | INTEGER | |
| `character_name` | TEXT | |
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
| `notes` | TEXT | Plain-language context, e.g. "BPC stock covers 1.8 days — start now" |

### `plan_item_outcome`

Written when a manufacturing batch completes and sells. Actuals are sourced from `CorporationRealizedSalesLedgerModel` (existing realized profit tracking via FIFO cost + wallet transactions), matched to `daily_action_log` records by `type_id` and completion timeframe.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `plan_item_id` | INTEGER FK → `build_plan_item` | |
| `type_id` | INTEGER | |
| `completed_at` | DATETIME | |
| `predicted_isk_per_hour` | REAL | |
| `actual_isk_per_hour` | REAL | From wallet transactions + FIFO cost |
| `accuracy_ratio` | REAL | actual / predicted |
| `predicted_sell_days` | REAL | |
| `actual_sell_days` | REAL | From wallet transaction timestamps |
| `predicted_material_cost` | REAL | |
| `actual_material_cost` | REAL | From corp wallet transactions |

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
- All `IndustryService` overview rows
- Active corp industry jobs per character (completion times, activity type, slot usage)
- Corp assets (materials, BPOs, BPCs)
- Corp market orders (open sell orders)
- `plan_learning_weights` per type_id

### Phase 2 — Pipeline State per Item

For every manufacturable item:

```
total_pipeline_days = (
    corp_stock_units
    + units_in_active_manufacturing_jobs
    + units_on_market
) / (sell_velocity_per_day × velocity_multiplier)

bpc_runs_available  = sum(BPC.runs for BPC in corp_assets where product = item)
invention_in_flight = active invention jobs for this item
copy_in_flight      = active copy jobs feeding this item's invention
```

### Phase 3 — Profitability Scoring

```
adjusted_score = isk_per_hour
  × accuracy_ema            # self-learning: penalises chronic mispredicts
  × velocity_multiplier     # self-learning: rewards fast sellers
  × market_timing_factor    # 1.0 if trend >= -5%; scales down to 0.5 at -15%
  × pipeline_saturation     # 1.0 if days_of_supply < 3d; linear decay to 0.0 at 14d+
                            # formula: max(0, 1 - (days_of_supply - 3) / 11)
  × confidence_tier_bonus   # 1.0 / 1.05 / 1.15 for low / medium / high
```

### Phase 4 — Item Decision

| Decision | Condition |
|---|---|
| `build` | Profitable, pipeline < 3 days of supply, market timing OK |
| `watch` | Profitable but pipeline ≥ 3 days of supply (already saturated) |
| `pause` | Currently building but 7d price trend < -8% — finish in-flight, don't restart |
| `skip` | Margin below threshold, high anomaly risk, or insufficient liquidity |

The goal is for the planner to be self-optimising through its feedback loop. Session-only UI overrides (force include / force exclude) are available in Tab 3 for the current view only and are never persisted — they reset on the next recomputation.

### Phase 5 — Full Production Chain Planning

For every `build` item:

```
# T1 items (meta_group_id == 1)
if BPO owned in corp assets:
    schedule ME research jobs if current_ME < target_ME (typically 10)
    schedule TE research jobs if current_TE < target_TE (typically 20)
    plan manufacturing jobs from BPO

elif BPC available in corp assets:
    plan manufacturing jobs from BPC
    run BPO investment analysis (T1 only)

elif BPO purchasable on market (meta_group_id == 1):
    run BPO investment analysis
    if break_even_days < 30: recommend BPO purchase (★ Strong Buy)
    if 30 <= break_even_days <= 90: flag as investment opportunity
    if break_even_days > 90: add BPC/items to shopping list

else:
    add BPC or finished items to shopping list

# T2 items (meta_group_id != 1) — no BPO path
if BPC available in corp assets:
    plan manufacturing jobs from BPC

else:
    if BPO for T1 base item in corp assets:
        plan copy jobs → plan invention jobs
        # Forward look: start invention now if BPC stock < production_lead_time_days
        add datacores + decryptors to shopping list (category: 'invention_input')
    else:
        add T1 BPC or base item to shopping list
        flag T1 BPO for BPO investment analysis

# Sub-manufacture decision (applied to every required input material)
for each required_material of a planned manufacturing job:
    if BPO owned for required_material (meta_group_id == 1):
        sub_manufacture_cost = compute manufacturing cost from BPO
                               (materials + job cost using existing IndustryService)
        market_buy_cost      = hub_sell_price × quantity_needed
        if sub_manufacture_cost < market_buy_cost:
            # Time-awareness: sub-manufacture job must complete before parent job starts.
            # If sub-manufacture duration > time until parent job would start,
            # fall back to market buy for this cycle and flag for next plan cycle.
            if sub_manufacture_duration <= time_until_parent_job_start:
                plan sub_manufacture job (action_type = 'sub_manufacture')
                add sub-material inputs to shopping list (category: 'current_job')
            else:
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

Available slots per character are derived from character skills using the existing slot capacity calculation already implemented in the Industry Slots page. Running jobs (including active reaction jobs) reduce available slots.

Assignment priority:
1. Manufacturing jobs → character with most free manufacturing slots; ties broken by fewest total active jobs
2. Invention jobs → character with highest relevant Science + Metallurgy skill sum and free research slot
3. Copy jobs → character with most free research slots; ties broken by highest Research skill
4. ME/TE research → character with most free research slots

ME/TE research, copy, and invention jobs all compete for the same research slot pool — the assigner never double-books a character's research capacity.

### Phase 7 — Material Shopping List

Materials are aggregated across all planned jobs and categorised so the user understands what each purchase is for.

```
for each required_material across all planned jobs:
    net_required = quantity_needed − corp_assets_available
                                   − reserved_for_in_flight_jobs
    if net_required <= 0: skip (already covered by stock)

    determine shopping_category:
        'current_job'     — material needed immediately for a job starting today
        'future_stock'    — material needed for jobs planned beyond today
                            (pre-buy to avoid restocking delays)
        'invention_input' — datacores, decryptors, T1 BPCs for invention jobs

    sub-manufacture decisions are resolved in Phase 5.
    Items where sub-manufacture was chosen appear as 'sub_manufacture' actions
    in the daily action log, not in the shopping list.
    Items where market buy was chosen (cheaper or sub-manufacture too slow)
    appear in the shopping list with their category and a note if applicable.

    estimated_cost = hub_sell_price × net_required
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

Each action includes: item name, quantity/runs, estimated cost, estimated completion time, and a plain-language note explaining the reasoning.

### Phase 9 — Persistence

- Archive current active plan (`status → 'archived'`)
- Insert new `build_plan` record
- Insert `build_plan_item` rows per product decision
- Insert `daily_action_log` rows per character action
- Compute `market_snapshot_hash`: SHA-256 of hub sell prices for all `build` items' products plus their primary input materials (Tritanium, Pyerite, Mexallon, Isogen, Nocxium, Zydrine, Megacyte, Morphite and all T2 component inputs present in planned jobs)

---

## Staleness Detection

On page load, the planner re-hashes current prices of all items in the active plan. If any key item price has drifted >5% from `market_snapshot_hash`, `freshness_score` is reduced. The UI warns the user to recompute. Threshold is configurable via admin settings.

---

## Self-Learning Feedback Loop

After each plan cycle, the system checks `daily_action_log` for `manufacture` actions marked `done` where the corresponding type_id appears in `CorporationRealizedSalesLedgerModel` with a completion timestamp after the action's `generated_at`. For each matched batch:

1. Read `actual_isk_per_hour`, `actual_sell_days`, `actual_material_cost` from `CorporationRealizedSalesLedgerModel` (which already tracks realized profit via FIFO cost + wallet transactions)
2. Write a `plan_item_outcome` record
3. Update `plan_learning_weights` for the item using EMA (α = 0.2):
   ```
   accuracy_ema = 0.8 × old_accuracy_ema + 0.2 × (actual / predicted)
   velocity_multiplier = 0.8 × old_velocity + 0.2 × (predicted_days / actual_days)
   cost_multiplier = 0.8 × old_cost + 0.2 × (actual_cost / predicted_cost)
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

### Tab 1 — Today's Actions *(primary view)*

Per-character accordion sections. Within each character, actions are grouped and ordered by type: Deliver → Invent → Copy → ME/TE Research → Manufacture → Relist.

Each action row has a checkbox. Checking it calls `POST /planner/action/done` and writes `status='done'` to `daily_action_log` in real-time. "Mark All Delivered Done" convenience button per character.

Example layout:
```
▼ Aldara Voss   [3 mfg slots free]  [2 research slots free]

  🔴 DELIVER
     ☐  Tengu ×5 runs — move to corp hangar

  🟡 INVENT
     ☐  Tengu BPC ×10 attempts — ~1.2M ISK — est. 18h
        "BPC stock covers 1.8 days — start now to avoid slot gap"

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

T2 items never appear in this section.

### Tab 3 — Build Plan

AG-Grid of all evaluated items. Filterable by decision type. Shows: item name, decision, priority score, ISK/hour, margin %, days of supply, confidence tier, sample count, decision reason.

Session-only override buttons (force include / force exclude / reset) affect the current view only and are never written to the database. They reset when the plan is recomputed. The goal is for the self-learning feedback loop to make manual overrides unnecessary over time.

### Tab 4 — Plan Analytics

- **Accuracy** — per-item predicted vs actual ISK/hour over last N batches
- **Velocity** — predicted vs actual sell days per item
- **Plan History** — timeline of recomputations, changes per cycle, net ISK earned vs projected

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

- Background computation follows the same pattern as `IndustryService` refresh (background thread, progress callbacks, polled by frontend)
- Schema migrations added to `schema_migrations.py` for all five new tables
- EMA weight α = 0.2 (configurable via admin settings)
- Staleness price drift threshold = 5% (configurable via admin settings)
- BPO break-even thresholds (Strong Buy: 30d, Consider: 90d) configurable via admin settings
- Pipeline gap prevention look-ahead: start invention/copy if current BPC stock covers less than `job_duration_days + 1` of planned manufacturing
