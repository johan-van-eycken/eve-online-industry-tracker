# Daily Planner — Design Spec

**Date:** 2026-08-27
**Status:** Approved for implementation

---

## Overview

A new Streamlit page — **Daily Planner** — that acts as a fully automated industry manufacturing task selector. When opened, it shows the user exactly what to do that day across all characters in the industry corporation to maximise ISK profit. The plan persists across days, adapts to market conditions, and improves its predictions over time via a self-learning feedback loop.

The user's daily workflow: **open the page → deliver finished jobs → sell/relist market orders → buy materials → start new jobs → close. Repeat daily.**

Rationale for this order: (1) Delivering first frees manufacturing slots and moves finished goods into the hangar. (2) Relisting/repricing sell orders immediately after delivery converts those goods into ISK pipeline as fast as possible, and updates existing orders before buying. (3) Buying materials third ensures the corp wallet reflects any ISK from step 2 and that materials are in the hangar before jobs are started. (4) Starting jobs last guarantees all prerequisites (materials, free slots) are in place. EVE will reject a job start if materials are absent — this order prevents that failure mode. The UI guides the user through this sequence with explicit cross-tab navigation cues.

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
- **Total market depth monitoring** — competitor units on market per product (not just own orders)
- **VWAP-based material cost estimates** — 5-day volume-weighted average price instead of spot price
- **Invention success rate tracking** — actual vs theoretical success rate per T2 item
- **Price momentum signal** — trend acceleration (7d vs 30d) for sharper pause/build decisions
- **Competition index scoring** — competitor supply depth as a scoring multiplier and job-selection gate
- **Margin–mineral correlation monitoring** — early warning when input price rises are squeezing product margins

### Performance architecture

Plan computation is split into two independent tiers to keep the user-triggered recompute fast:

**Tier 1 — `MarketIntelligenceJob` (background, scheduled)**
Runs every `planner_market_refresh_interval_hours` (default: 3h) automatically, without user interaction. Collects all data that does not need to be real-time:
- Market depth ESI calls (competitor units per type_id)
- VWAP computation from `MarketHistoryModel`
- Margin–mineral Pearson correlations (refreshed every 24h, stored in `margin_correlation_cache`)
- Invention outcome logging

Output written to: `market_depth_cache`, `margin_correlation_cache`.
Follows the same background job pattern as `PublicStructuresGlobalScanJob`.

**Tier 2 — `PlanComputeJob` (user-triggered)**
Triggered by "Recompute Plan". Reads Tier 1 data from cache — no ESI calls for market data.
Only fetches genuinely real-time data: corp wallet, active jobs, corp assets (fast DB reads).
Then runs Phases 2-9 on pre-collected data.

**Expected timings:**
- Without this split: 35–50 seconds (ESI calls block the user)
- With this split: 6–12 seconds for the user-triggered recompute

**Fallback:** if `market_depth_cache` has no data yet (first startup, or cache older than `planner_market_refresh_interval_hours × 2`), Phase 1 falls back to inline ESI collection with a warning banner: "Market data is stale — collecting now, this may take up to 30 seconds."

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
MarketIntelligenceJob  (new background job — runs every 3h, independent of plan compute)
  ├── MarketDepthCollector  ESI market depth + VWAP per type + hub → market_depth_cache
  └── MarginCorrelator      Pearson correlation Trit price vs margin → margin_correlation_cache (24h)

DailyPlannerService    (user-triggered recompute — reads from cache, no ESI calls)
  ├── PipelineAnalyzer      pipeline state per item (stock + market + in-flight + competitor depth)
  ├── InventionTracker      log new invention outcomes + retrieve actual success rates per type
  ├── ProfitabilityScorer   adjusted scoring with self-learning weights + competition_factor + momentum
  ├── ItemDecisionEngine    build / watch / pause / skip per item
  ├── ChainPlanner          BPC/invention/copy/BPO resolution per build item
  ├── CharacterAssigner     skill-aware job → character mapping
  ├── ShoppingListBuilder   net material needs + BPO opportunities (VWAP-based costs)
  ├── ActionPlanBuilder     final ordered per-character action list
  └── PlanPersistence       DB read/write for build plan + daily action log

Flask routes (new)
  POST  /planner/compute              trigger plan computation (background job)
  GET   /planner/status               poll computation progress
  GET   /planner/plan                 fetch current active plan
  POST  /planner/action/done          mark a daily action as done
  PATCH /planner/action/{id}          undo a done action (while unprocessed)
  GET   /planner/analytics            fetch self-learning accuracy stats
  GET   /planner/market_intel/status  market intelligence job status + last run time

Streamlit page
  streamlit_ui/pages/daily_planner.py
```

---

## Prerequisites — Required Changes to Existing Code

These changes must be completed **before** Daily Planner implementation begins. They touch existing services used by other pages and carry their own regression risk if done incorrectly.

---

### Prereq 1 — `price_trend_30d_pct` toevoegen aan `MarketHistoryService` en `IndustryService`

**Probleem:** De momentum signal (`momentum_signal = price_trend_7d_pct − price_trend_30d_pct / 4`) vereist een expliciete 30-daagse trend. De huidige `trend_pct` in `IndustryService` is `(avg_7d − avg_42w) / avg_42w × 100` — 7 dagen vs 42-weeks baseline, niet vs 30 dagen.

**Aanpassing 1 — `market_history_service.py::get_price_stats()`:**
```python
# Voeg toe naast de bestaande avg_7d / avg_42w berekening:
prices_30d = [r.close for r in rows if r.date >= cutoff_30d]
avg_30d = float(statistics.mean(prices_30d)) if prices_30d else None
trend_30d_pct = ((avg_7d - avg_30d) / avg_30d * 100) if avg_30d and avg_30d > 0 else None

# Return dict aanvullen:
"avg_30d": avg_30d,
"trend_30d_pct": trend_30d_pct,
```

**Aanpassing 2 — `industry/service.py` (regel ~1941):**
```python
# Hernoem het bestaande veld — planner en eventuele andere callers gebruiken de nieuwe naam
row["price_trend_7d_pct"] = stats.get("trend_pct")        # hernoemd van "price_trend_pct"
row["price_trend_30d_pct"] = stats.get("trend_30d_pct")   # nieuw
```

**Belangrijk — naamgeving:** het bestaande veld heet `price_trend_pct` in de codebase. De spec gebruikt `price_trend_7d_pct`. Deze rename moet in Prereq 1 worden doorgevoerd; als de implementatie `price_trend_7d_pct` opvraagt maar het veld heet nog `price_trend_pct`, gooit Phase 2/3 een `KeyError`. Grep na de aanpassing op `price_trend_pct` in de hele codebase en vervang elke vindplaats door `price_trend_7d_pct`.

**Risico:** de rename raakt alle bestaande pagina's die `price_trend_pct` lezen (Industry Builder Streamlit UI, portfolio planner). Die moeten tegelijk worden bijgewerkt in dezelfde commit. Zoek alle vindplaatsen met `grep -rn "price_trend_pct" src/` vóór de commit.

---

### Prereq 2 — `aggregate_shopping_list` verplaatsen naar de service-laag

**Probleem:** De shopping list aggregatielogica zit in `streamlit_ui/shopping_list.py` — de Streamlit presentatielaag. Een backend service (`ShoppingListBuilder`) kan die niet importeren.

**Aanpak:** Extract de logica naar een nieuwe module in de applicatielaag, maak de Streamlit-versie een dunne wrapper:

```
src/eve_online_industry_tracker/application/industry/shopping_list.py  ← nieuw
    aggregate_shopping_list(rows: list[dict]) -> list[dict]             ← verplaatst van Streamlit

src/streamlit_ui/shopping_list.py  ← behoudt zelfde publieke interface
    from eve_online_industry_tracker.application.industry.shopping_list import aggregate_shopping_list
    # re-export — Portfolio Planner en Industry Builder hoeven niet gewijzigd te worden
```

**Risico:** Medium. De Streamlit re-export houdt de bestaande callers intact, maar de verplaatsing moet zorgvuldig getest worden. Doe dit in een aparte commit vóór Daily Planner implementatie. Voer bestaande tests uit na de verplaatsing.

---

### Prereq 3 — Sub-manufacture planningsmethode lokaliseren in `IndustryService`

**Probleem:** De spec verwijst naar `_build_manufacture_job_plan` als een bestaande recursieve methode in `IndustryService`, maar deze methode bestaat niet onder die naam (grep geeft geen resultaat). De recursieve sub-manufacture berekening is waarschijnlijk verspreid over meerdere private methoden.

**Actie vóór implementatie van Phase 5:**
1. Zoek de methode die recursieve materiaalbehoeften berekent (zoektermen: `recursive`, `depth`, `sub_material`, `BOM`, `bill_of_materials`)
2. Bepaal of die direct aanroepbaar is door `ChainPlanner` of als een extract-methode blootgesteld moet worden
3. Documenteer de gevonden methode(naam) in de `ChainPlanner` implementatie — vervang de verwijzing naar `_build_manufacture_job_plan` door de werkelijke naam

**Risico:** Als de logica niet als één aanroepbare methode bestaat, moet `ChainPlanner` de recursie zelf implementeren (met depth-guard ≤ 8). Dit is een implementatierisico voor Phase 5, geen blocker voor de rest van de planner.

---

### Prereq 4 — `MarketIntelligenceJob` moet zelf market history refreshen

**Probleem:** `MarketHistoryService.fetch_and_store_history()` wordt alleen aangeroepen vanuit `IndustryService` refresh (on-demand via Industry Builder), `CorporationsService` en `CharactersService`. Er is geen scheduled refresh. Als de gebruiker Industry Builder niet heeft gedraaid, kan `MarketHistoryModel` stale of leeg zijn — wat VWAP-berekeningen onbetrouwbaar maakt.

**Aanpak:** `MarketIntelligenceJob` roept `fetch_and_store_history()` aan voor alle type_ids in het actieve plan als onderdeel van zijn 3-uurscyclus. Dit maakt de job zelf-voorzienend:

```python
# In MarketIntelligenceJob._run_cycle():
for type_id in tracked_type_ids:
    market_history_svc.fetch_and_store_history(
        type_id=type_id,
        region_id=configured_hub_region_id,
    )
# Daarna pas VWAP berekenen uit de nu-verse MarketHistoryModel data
```

**Volgorde in de cyclus:** history refresh → VWAP berekening → market depth ESI calls → schrijf naar `market_depth_cache`. Zo gebruikt VWAP altijd de meest recente history data van die cyclus.

**Risico:** Laag — additief gebruik van een bestaande methode. Let op rate limiting: `fetch_and_store_history` doet één ESI call per type_id. Met 40 items en 10 threads is dit vergelijkbaar met de bestaande Industry Builder refresh. De `_EsiErrorRateLimiter` in `ESIClient` handelt dit al af.

---

### Prereq 5 — ESI endpoint versies pinnen

**Probleem:** aanroepen die `/latest/` gebruiken kunnen stilletjes een hogere versie oppikken zodra CCP een nieuwe versie promoveert. Veldwijzigingen (hernoemingen, type-wijzigingen, weggevallen velden) zijn dan onzichtbaar totdat een berekening verkeerde data produceert. Third-party tools gebruiken explicit versioning als standaardbeschermingsmaatregel.

**Aanpak:** ga door `ESIClient` (en eventuele directe `requests`-aanroepen buiten de client) en vervang alle `/latest/` en generieke versies door de hoogste stabiele versie die het endpoint vandaag aanbiedt. Raadpleeg de ESI Swagger UI (`https://esi.evetech.net/ui/`) — elk endpoint toont zijn beschikbare versies. Sla de gebruikte versie per endpoint op als constante in de client:

```python
# In ESIClient of een aparte esi_versions.py:
ESI_MARKET_ORDERS   = "/v1/markets/{region_id}/orders/"
ESI_CORP_ASSETS     = "/v5/corporations/{corporation_id}/assets/"
ESI_CHAR_SKILLS     = "/v4/characters/{character_id}/skills/"
# ... etc.
```

CCP kondigt deprecations aan via de ESI changelog op GitHub (`/zzzcrow/eve-swagger-interface` of de officële ESI release notes). Bij een deprecation-aankondiging heb je typisch 6–12 maanden om te migreren.

**Wanneer doen:** vóór Phase A — elke nieuwe ESI-aanroep voor de Daily Planner gebruikt direct een gepinde versie.

---

### Prereq 6 — ESI response validation via Pydantic DTOs

**Probleem:** als CCP een veld hernoemt of weghaalt, faalt de huidige code ergens diep in de scoringslogica met een `KeyError` of `None`-propagatie in plaats van direct op de ESI-grens. Dit maakt de oorzaak moeilijk te traceren.

**Aanpak:** voeg Pydantic-modellen toe als DTOs aan de ESI-parseerlaag. Elke response wordt gevalideerd vóór gebruik:

```python
from pydantic import BaseModel

class EsiMarketOrder(BaseModel):
    order_id: int
    type_id: int
    price: float
    volume_remain: int
    is_buy_order: bool
    location_id: int

# In ESIClient of MarketDepthCollector:
orders = [EsiMarketOrder(**row) for row in raw_response]
```

Bij een CCP-veldwijziging gooit Pydantic direct een `ValidationError` op de ESI-grens — zichtbaar in logs, makkelijk te debuggen, geen stille datacorruptie doorheen de pijplijn.

**Scope:** alleen voor de endpoints die de Daily Planner nieuw introduceert (market orders, market history). Bestaande ESI-aanroepen buiten de planner worden niet aangepast als onderdeel van deze prereq — dat is een aparte refactor.

**Wanneer doen:** als onderdeel van Phase A, samen met de implementatie van `MarketDepthCollector`.

---

### Niet vereist — WAL mode

WAL mode is **al geïmplementeerd** in `DatabaseManager.__init__` (regels 32–43) via een SQLAlchemy event listener. Geen actie nodig.

---

## Data Model

Eight new tables added to `eve_app.db` via `schema_migrations.py`.

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
| `target_batches` | INTEGER | Batches to maintain in pipeline. Formula: `ceil(effective_velocity × production_lead_time_days / runs_per_batch)`. `production_lead_time_days` is item-type-dependent: for **T1 items** = manufacturing job duration only. For **T2 items** = copy_duration + invention_duration + manufacturing_duration (full chain lead time). Using only manufacturing duration for T2 items would structurally under-estimate the pipeline, leaving BPC gaps. Copy and invention durations are read from SDE blueprint data × character skill modifiers. |
| `priority_score` | REAL | Composite adjusted score |
| `isk_per_hour` | REAL | At plan creation time |
| `margin_pct` | REAL | |
| `days_of_supply_current` | REAL | Market + in-flight jobs at plan time |
| `pipeline_stage` | TEXT | `'manufacturing'` \| `'invention'` \| `'copying'` \| `'researching'` \| `'watching'` |
| `bpo_investment_recommended` | BOOLEAN | NULL for T2+/Faction/Deadspace items (analysis never ran). TRUE/FALSE only for meta group 1 items. |
| `bpo_market_price` | REAL | Lowest sell price for BPO type at hub (market orders only; contract-only BPOs will show NULL) |
| `break_even_days` | REAL | |
| `projected_annual_savings` | REAL | Material savings × planned annual runs |
| `effective_velocity` | REAL | Computed in Phase 2; stored so `MarketIntelligenceJob` can read last-known velocity for `competition_index` pre-computation without waiting for a plan recompute |

### `daily_action_log`

Append-only. One row per recommended action per computation. Marking `status='done'` is how the user tracks execution; these records feed back into the self-learning tables.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `plan_id` | INTEGER FK → `build_plan` | |
| `generated_at` | DATETIME | |
| `character_id` | INTEGER | Nullable for `buy_materials` and `buy_bpo` actions only (corp-level; any character with market access can execute). Always non-null for `deliver`, `manufacture`, `sub_manufacture`, `invent`, `copy`, `me_research`, `te_research`, and `relist_order` — relist orders are character-specific in EVE (only the installing character can relist their own orders). |
| `character_name` | TEXT | Nullable for the same corp-level action types as `character_id` |
| `action_type` | TEXT | `'deliver'` \| `'manufacture'` \| `'sub_manufacture'` \| `'invent'` \| `'copy'` \| `'me_research'` \| `'te_research'` \| `'buy_materials'` \| `'buy_bpo'` \| `'relist_order'` |
| `shopping_category` | TEXT | For `buy_materials` rows: `'current_job'` \| `'future_stock'` \| `'invention_input'` |
| `type_id` | INTEGER | |
| `type_name` | TEXT | |
| `quantity` | INTEGER | |
| `runs` | INTEGER | |
| `estimated_cost_isk` | REAL | |
| `estimated_profit_isk` | REAL | |
| `estimated_completion` | DATETIME | |
| `status` | TEXT | `'pending'` \| `'done'` \| `'skipped'`. `'pending'` = not yet acted on. `'done'` = user confirmed execution. `'skipped'` = user explicitly dismissed the action for this cycle (e.g. deliberately not relisting, or deferring a buy). Set via UI only (a "Skip" button per action row, same level as the done checkbox). Skipped actions do not feed into the feedback loop and are not reprocessed. `PATCH /planner/action/{id}` can also restore a skipped action to `'pending'`, provided `processed_for_feedback=false`. |
| `processed_for_feedback` | BOOLEAN | Default FALSE. Set TRUE once this row's outcome has been written to `plan_item_outcome` and EMA weights updated. Prevents re-processing on subsequent recomputations. |
| `notes` | TEXT | Plain-language context, e.g. "BPC stock covers 1.8 days — start now" |

### `plan_item_outcome`

Written when a manufacturing batch completes and sells. Actuals are sourced via `CorporationRealizedProfitLedgerService` (the service layer — always use the service, never the model directly). The underlying SQLAlchemy model is named `CorporationRealizedSalesLedgerModel` — the naming discrepancy ("Sales" vs "Profit") is intentional: the model name reflects the ESI data source (wallet transactions/sales), while the service name reflects the business purpose (realized profit). **Always import and call `CorporationRealizedProfitLedgerService`; do not reference `CorporationRealizedSalesLedgerModel` directly in any new code.** Matched to `daily_action_log` records by `type_id` and completion timeframe. There is no direct FK from `plan_item_outcome` to `daily_action_log` by design — attribution is temporal FIFO, not exact batch-to-action matching. If no matching sale appears within the configurable slow-mover timeout (default 60 days), an outcome row is written with `actual_sell_days` set to the timeout value and `slow_mover = true`, so the velocity score for that item is penalised rather than left unresolved indefinitely.

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
| `actual_material_cost` | REAL | From `CorporationRealizedProfitLedgerService` FIFO cost (do not access the model directly) |

### `margin_correlation_cache`

Written by `MarketIntelligenceJob` every 24 hours. One row per type_id. Stores the pre-computed Pearson correlation between Tritanium price and this item's realized margin. Phase 3 reads this table directly — no correlation computation at plan-compute time.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `type_id` | INTEGER UNIQUE | |
| `pearson_correlation` | REAL | −1.0 to 1.0; NULL if fewer than `planner_mineral_correlation_min_days` data points |
| `is_squeeze_sensitive` | BOOLEAN | True if `pearson_correlation` < `planner_mineral_correlation_threshold` |
| `data_points` | INTEGER | Number of days used in calculation |
| `computed_at` | DATETIME | |

### `market_depth_cache`

Written by `MarketIntelligenceJob` (every 3h by default). One row per `(type_id, hub)` — **no `plan_id` FK**. `MarketIntelligenceJob` runs independently of plan computation and has no plan context; tying it to a `plan_id` would cause the first-startup bootstrap to fail (no active plan yet). Phase 1 reads by `(type_id, hub)` directly. Each MIJ cycle does `INSERT OR REPLACE` on the `UNIQUE(type_id, hub)` constraint, keeping exactly one row per item per hub at all times.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `type_id` | INTEGER | |
| `hub` | TEXT | e.g. `'jita'` |
| `competitor_units` | INTEGER | Total sell units from all sellers except own corp orders |
| `vwap_5d` | REAL | 5-day volume-weighted average price from `MarketHistoryModel` |
| `spot_sell_price` | REAL | Best sell price at snapshot time — used by staleness check in `GET /planner/plan` |
| `competition_index` | REAL | Pre-computed by MIJ: `competitor_units / (effective_velocity × 30)`. Phase 2 reads this directly; does NOT recompute it. If MIJ has run but effective_velocity was zero, stored as NULL. |
| `snapshot_at` | DATETIME | |

**Schema constraint (must be in migration):** `UNIQUE(type_id, hub)` — required for `INSERT OR REPLACE` to work correctly. Without this constraint, each MIJ cycle appends new rows instead of updating.

VWAP formula: `sum(avg_price × volume for last 5 trading days) / sum(volume for last 5 trading days)`. Falls back to `spot_sell_price` if fewer than 3 days of history are available.

### `invention_outcome_log`

Append-only. One row per completed invention job (both successes and failures). Written when a `CorporationIndustryJobsModel` row with `activity_id = 8` transitions to `delivered`. Used to compute actual vs theoretical success rates per T2 product.

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `type_id` | INTEGER | Product type_id of the T2 BPC being invented |
| `blueprint_type_id` | INTEGER | The T1 BPO/BPC used as input |
| `decryptor_type_id` | INTEGER | Nullable; decryptor used |
| `theoretical_success_pct` | REAL | SDE base chance × skill bonuses × decryptor modifier |
| `was_success` | BOOLEAN | True if the job produced a BPC |
| `character_id` | INTEGER | Installer |
| `completed_at` | DATETIME | |

Aggregate query for actual success rate per type_id:
```sql
SELECT type_id,
       COUNT(*) FILTER (WHERE was_success) * 1.0 / COUNT(*) AS actual_success_rate,
       COUNT(*) AS total_attempts
FROM invention_outcome_log
GROUP BY type_id
```
Minimum 10 attempts before `actual_success_rate` replaces the `theoretical_success_pct` in Phase 5 calculations. Below 10 attempts, the theoretical rate is used with no correction.

### `plan_learning_weights`

One row per type_id. Updated after each `plan_item_outcome` is recorded using exponential moving average (EMA).

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER PK | |
| `type_id` | INTEGER UNIQUE | |
| `accuracy_ema` | REAL | EMA of `accuracy_ratio`; default 1.0 |
| `velocity_multiplier` | REAL | actual/predicted sell velocity EMA; default 1.0 |
| `cost_multiplier` | REAL | predicted/actual material cost EMA; default 1.0. Values < 1.0 mean materials systematically cost more than predicted (penalty); > 1.0 mean cheaper than predicted (bonus). |
| `sample_count` | INTEGER | Completed batches informing this record |
| `last_updated` | DATETIME | |
| `confidence_tier` | TEXT | `'low'` (<5 samples) \| `'medium'` (5–19) \| `'high'` (≥20) |

---

## Plan Computation Logic

Triggered via `POST /planner/compute`. Runs in a background thread. Nine sequential phases.

### Phase 1 — Data Collection

Fetches in parallel from existing services:
- Corp wallet balance
- All `IndustryService` overview rows — if overview data is older than 12 hours, the planner warns the user and offers to trigger an Industry Builder refresh before proceeding
- Active corp industry jobs per character (completion times, activity type, slot usage)
- Corp assets (materials, BPOs, BPCs) across all hangar divisions
- Corp market orders (open sell orders)
- `plan_learning_weights` per type_id
- Sell velocity per type_id from `SalesHistoryService` (used as `sell_velocity_per_day` in Phase 2). The feedback check also runs here — `daily_action_log` rows with `status='done'` and `processed_for_feedback=false` are matched via `CorporationRealizedProfitLedgerService` (do not use `CorporationRealizedSalesLedgerModel` directly), new `plan_item_outcome` records are written, and `plan_learning_weights` are updated before new scoring begins. Each processed row is then marked `processed_for_feedback=true` to prevent re-processing.

**Phase 1 — Real-time data only (fast):**
Phase 1 now collects only data that must be current at plan-compute time:
- Corp wallet balance
- Active corp industry jobs per character
- Corp assets (materials, BPOs, BPCs)
- Corp market orders (own open sell orders)
- `plan_learning_weights` per type_id
- Sell velocity from `SalesHistoryService`
- Feedback processing (`daily_action_log` done → `plan_item_outcome` → EMA update)
- **Invention outcome logging** — scan `CorporationIndustryJobsModel` for newly delivered `activity_id = 8` rows not yet in `invention_outcome_log`; write new rows (`INSERT OR IGNORE`). Fast: only new rows, append-only.

**Phase 1 — Cache reads (instant):**
Market depth, VWAP, and correlation data are read from pre-populated tables written by `MarketIntelligenceJob`:
- `market_depth_cache` → `competitor_units`, `vwap_5d`, `competition_index` per type_id
- `margin_correlation_cache` → Pearson correlation flags per type_id

If `market_depth_cache` contains no row for any of the active type_ids (first startup, or all rows older than `planner_market_refresh_interval_hours × 2`), the planner falls back to inline ESI collection and shows a warning banner: "Market data is stale — collecting now, this may take up to 30 seconds." Staleness is checked per row via `snapshot_at`; partial cache (some type_ids missing) uses inline fetch for the missing items only.

**`MarketIntelligenceJob` — what it does (runs separately, every 3h by default):**
For every type_id in the active `build_plan`:
- `ESIService.esi_get("/markets/{region_id}/orders/", type_id=X, is_buy_order=false)` — uses ETag caching (304 Not Modified returned instantly if orderbook unchanged). **ESI pagination:** this endpoint paginates at 1000 orders per page. `MarketDepthCollector` must iterate `?page=1`, `?page=2`, … until a page returns fewer than 1000 results. ETag applies per page; each page has its own `If-None-Match` header.
- **Price outlier filter:** exclude any sell order whose price deviates > `planner_price_outlier_sigma` (default 3σ) from the 30-day `avg_30d`. If > 50% of orders are outliers, log a warning and use `avg_30d` as the price estimate for this type. This guards against 1-ISK manipulation listings and cornering spikes distorting VWAP and competitor_units.
- Subtract own corp orders from remaining (non-outlier) orders to get `competitor_units`
- Query `MarketHistoryModel` for VWAP: `SELECT avg_price, volume FROM market_history WHERE type_id=? AND region_id=? ORDER BY date DESC LIMIT 5`
- Read `effective_velocity` from the most recent `build_plan_item` row for this `type_id` (the column added to that table). On first run (no prior plan), fall back to raw `sell_velocity_per_day` from `SalesHistoryService`. This resolves the chicken-and-egg: Phase 2 computes and stores `effective_velocity`; `MarketIntelligenceJob` reads it back on the next cycle.
- Pre-compute `competition_index = competitor_units / (effective_velocity × 30)`. If `effective_velocity ≤ 0`, store `NULL` (never divide by zero). Write all fields to `market_depth_cache` using `INSERT OR REPLACE` on `UNIQUE(type_id, hub)` — no `plan_id` column.

Margin–mineral correlation (refreshed every 24h by the same job):
- Compute Pearson between Tritanium `avg_price` and each item's realized margin over last 90 days
- Requires minimum `planner_mineral_correlation_min_days` (default 30) data points; skip if insufficient
- Write results to `margin_correlation_cache` with `computed_at` timestamp
- **Stale-entry cleanup:** after writing, delete rows from `margin_correlation_cache` for any `type_id` that is not present in the active `build_plan`. This prevents unbounded growth as items rotate in and out of the portfolio.
- Phase 3 reads correlation flags from `margin_correlation_cache` — no computation at plan time

### Phase 2 — Pipeline State per Item

For every manufacturable item:

```
# sell_velocity_per_day fallback: if SalesHistoryService returns 0 (item never sold),
# use (1 / days_of_supply) from IndustryService row, floored at 0.01 units/day.
# Guard: if days_of_supply is also 0 (brand-new item, no pipeline, no market),
# fall straight to the 0.01 floor — never divide by zero.
if sell_velocity_per_day > 0:
    effective_velocity = max(0.01, sell_velocity_per_day × velocity_multiplier)
else:
    days_of_supply_current = row["days_of_supply"]   # from IndustryService overview
    fallback = (1 / days_of_supply_current) if days_of_supply_current > 0 else 0.01
    effective_velocity = max(0.01, fallback)

total_pipeline_days = (
    corp_stock_units
    + units_in_active_manufacturing_jobs
    + units_on_market
) / effective_velocity

# Competitor supply depth (new)
# Read pre-computed competition_index directly from market_depth_cache (computed by MIJ).
# Do NOT recompute here — using the MIJ value keeps Phase 2 free of ESI calls.
# If cache is missing for this type_id (Phase A / first startup), competition_index = None.
competition_index = market_depth_cache.get(type_id, {}).get("competition_index")  # None if absent
competitor_units  = market_depth_cache.get(type_id, {}).get("competitor_units", 0)
competitor_supply_days = (competitor_units / effective_velocity) if competition_index is not None else None

bpc_runs_available  = sum(BPC.runs for BPC in corp_assets where product = item)
invention_in_flight = active invention jobs for this item
copy_in_flight      = active copy jobs feeding this item's invention

# Momentum signal (new) — detects accelerating vs stabilising price moves
# price_trend_7d_pct and price_trend_30d_pct both available in IndustryService overview rows
# Negative momentum_signal = price decline is accelerating (more urgent to pause)
# Positive momentum_signal = price is recovering relative to the longer trend
momentum_signal = price_trend_7d_pct - (price_trend_30d_pct / 4)
```

### Phase 3 — Profitability Scoring

```
adjusted_score = isk_per_hour
  × accuracy_ema            # self-learning: penalises chronic mispredicts vs actual profit
  × velocity_multiplier     # self-learning: rewards fast sellers
  × cost_multiplier         # self-learning: adjusts for systematic material cost misprediction
  × market_timing_factor    # replaces simple 7d trend: now uses momentum_signal for sharper signal
                            # Exact formula:
                            #   base_factor = max(0.5, min(1.0, 1.0 + (price_trend_7d_pct + 5) * 0.05))
                            #   Examples: pct=-5 → 1.0; pct=-10 → 0.75; pct=-15 → 0.5; pct=0 → 1.0
                            #   If momentum_signal < -3 (decline accelerating):
                            #     momentum_adj = max(0.8, 1.0 + momentum_signal / 30)
                            #     market_timing_factor = base_factor * momentum_adj
                            #   Else: market_timing_factor = base_factor
                            #   e.g. momentum_signal = -6 → momentum_adj = max(0.8, 0.80) = 0.80
                            #   No upward bonus for positive momentum_signal (cap remains 1.0)
                            # source: price_trend_7d_pct and price_trend_30d_pct from IndustryService rows
  × pipeline_saturation     # > 1.0 if days_of_supply < 3d (urgency boost); 1.0 at 3d; linear decay to 0.0 at 14d+
                            # formula: max(0, 1 - (days_of_supply - 3) / 11)
                            # e.g. 0 days → 1.27 (urgent), 3 days → 1.0, 14 days → 0.0
  × competition_factor      # new: penalises items with high competitor supply on market
                            # competition_index < 1.0  → 1.00 (thin market, no penalty)
                            # competition_index 1.0–2.0 → 0.90
                            # competition_index 2.0–4.0 → 0.75
                            # competition_index > 4.0   → 0.60 (flooded market, floor)
                            # rationale: high competitor supply means longer actual sell time and
                            #   price pressure not yet reflected in the 7d trend
  × mineral_squeeze_penalty # new: 1.0 normally; 0.85 if item is flagged as "mineral-squeeze sensitive"
                            #   (Pearson correlation between Tritanium price and this item's margin < -0.6)
                            #   AND Tritanium 7d trend > +5% (squeeze actively happening)
                            #   0.85 is a configurable floor (admin setting: planner_mineral_squeeze_factor)
  × confidence_tier_bonus   # 1.0 / 1.05 / 1.15 for low / medium / high
```

### Phase 4 — Item Decision

| Decision | Condition |
|---|---|
| `build` | `adjusted_score` > `planner_min_isk_per_hour` threshold (default 5M ISK/hr) AND `absolute_profit_per_batch` > `planner_min_profit_per_batch` (default 20M ISK) AND `total_pipeline_days` < 3 AND `competition_index` < 4.0 |
| `watch` | Profitable but `total_pipeline_days` ≥ 3 days of supply (already saturated) OR `competition_index` ≥ 4.0 (market flooded) |
| `pause` | There are active in-flight manufacturing jobs for this item AND (`price_trend_7d_pct` < -8% OR `momentum_signal` < -6) — finish in-flight jobs, do not start new ones. An item with no active jobs and a declining trend goes to `skip`, not `pause`. |
| `skip` | `adjusted_score` ≤ minimum threshold, `absolute_profit_per_batch` ≤ minimum batch profit, high anomaly risk, or insufficient liquidity |

`absolute_profit_per_batch = (vwap_5d − estimated_material_cost_per_unit) × runs_per_batch × output_quantity`. Uses `vwap_5d` from `market_depth_cache` as the sell price estimate (consistent with the VWAP-first approach throughout the planner). Falls back to `spot_sell_price` if `vwap_5d` is unavailable. This catches low-ISK/hour items with very short job durations where overhead (broker fees, transaction tax, slot switching) erodes the margin to near-zero in absolute terms.

**Phase A — competition_index = NULL:** During Phase A (before `MarketIntelligenceJob` has run), `competition_index` is `None` for all items. When `competition_index is None`, the `competition_index ≥ 4.0` hard gate is **skipped** (treated as if `competition_index = 0`). No item is forced to `watch` due to a null competition_index. The `competition_factor` multiplier in Phase 3 also defaults to 1.0 when `competition_index is None`.

**Component pipeline alignment — two-pass ordering:** Sub-components cannot be identified in Phase 4 because Phase 5 (ChainPlanner) has not yet run. The correct implementation is a two-pass approach:
1. **Pass 1 (Phase 2–4):** Evaluate all top-level portfolio items through Phases 2, 3, and 4 normally. This produces decisions for all items the planner directly manufactures for sale.
2. **Pass 2 (Phase 5, ChainPlanner):** For each item with `decision='build'`, ChainPlanner resolves the sub-component chain. If a required material has a BPO in corp assets and sub-manufacture is cheaper, `ChainPlanner` directly assigns `decision='build'` (chain context) to that sub-component, bypassing Phase 4 entirely. `competition_factor = 1.0` and the competition_index gate do not apply to sub-components assigned this way.

Sub-components identified in Pass 2 are added to `build_plan_item` as separate rows with `decision='build'` and `pipeline_stage='manufacturing'` — making them visible in Tab 3 with their chain-context flag.

**Note on overlapping price thresholds:** `market_timing_factor` (applied in Phase 3) continuously penalises the score as trend falls below -5%, reaching its floor of 0.5 at -15%. This is a soft signal. `pause` (decided in Phase 4) is a hard override that triggers at -8% trend OR -6 momentum_signal regardless of score — it forces an item from `build` to `pause` even if its adjusted score remains above the minimum threshold. These two mechanisms serve different roles: one modulates scoring, the other overrides the decision.

**Note on competition_index as a hard gate:** `competition_index ≥ 4.0` forces `watch` even if pipeline is below 3 days. The rationale: building into a market with 120+ competitor days of supply (4 × 30d) drives price down faster than velocity-adjusted scoring can detect. The `competition_factor` multiplier in Phase 3 already softens the score (×0.60); the hard gate prevents starting new jobs that would worsen an already-flooded market. The threshold (4.0) is configurable via admin settings as `planner_competition_index_gate`.

**Paused items and DELIVER:** Items with `decision='pause'` that have in-flight jobs completing today still appear in Tab 1 as DELIVER actions — the user must collect the finished goods. They do not appear as any START action type. This is the only action type generated for paused items.

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

# Invention cost calculation (updated — uses actual success rate from invention_outcome_log)
# effective_bpc_cost is the true cost per successful BPC, accounting for failed attempts:
#
# success_rate = actual_success_rate from invention_outcome_log if attempts >= 10
#              else theoretical_success_pct from SDE × skill bonuses × decryptor modifier
# Guard: floor success_rate at 0.05 to prevent division by zero.
# Edge case: 10 consecutive failures (all attempts fail) → actual_success_rate = 0.0.
# Using 0.0 would cause cost_per_attempt / 0.0 = ZeroDivisionError.
# A 5% floor is conservative — any real T2 blueprint has at least 20% base chance.
# Once more data accumulates the rate corrects itself; 0.05 is never the true long-run value.
effective_success_rate = max(0.05, success_rate)
#
# cost_per_attempt = sum(datacore_qty × vwap_5d for each datacore) + decryptor_cost
# effective_bpc_cost = cost_per_attempt / success_rate
#
# Example: 10 datacores at 50k each, success rate 0.42 → cost per BPC = 500k / 0.42 = 1.19M ISK
# vs theoretical 0.50 → 1.00M ISK. If actual < theoretical, use actual (conservative).
#
# This effective_bpc_cost feeds into:
# - Phase 5 BPO break-even analysis (replaces naive theoretical cost)
# - Phase 7 shopping list (invention_input category estimated cost)
# - plan_item_outcome.predicted_material_cost (includes BPC cost amortised per manufactured run)

# Sub-manufacture decision (applied to every required input material regardless of T1/T2)
# Recursion: see Prereq 3 — the exact name of the recursive manufacture planning method
# in IndustryService must be determined before implementing this section.
# Once located, ChainPlanner delegates sub-sub-manufacture recursion to that method
# (depth ≤ 8 guard must be respected) and does NOT re-implement it.
# If no single callable method exists, ChainPlanner implements its own recursive BOM
# traversal with a depth counter, capped at 8 levels.
# Only the top-level sub-manufacture decision (buy vs build) is evaluated here.
for each required_material of a planned manufacturing job:
    if BPO owned for required_material (meta_group_id == 1):
        sub_manufacture_cost = compute manufacturing cost from BPO
                               (materials + job cost using existing IndustryService)
        market_buy_cost      = vwap_5d × quantity_needed  # use VWAP (from market_depth_cache) for consistency with shopping list cost estimates; falls back to spot_sell_price if vwap_5d unavailable

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
planned_runs_per_day = ceil(effective_velocity / runs_per_batch)
                       # effective_velocity from Phase 2; runs_per_batch from blueprint SDE data.
                       # Floored at 1 run / 30 days (0.033) to avoid near-zero denominators
                       # for very slow-moving items.
break_even_days      = total_investment / (savings_per_run × planned_runs_per_day)
projected_annual_savings = savings_per_run × planned_runs_per_day × 365
```

Only items with `meta_group_id == 1` reach this analysis. T2, Faction, Deadspace, and Officer items are excluded — no purchasable BPO exists for them.

### Phase 6 — Skill-Aware Character Assignment

Available slots per character are derived from character skills using the existing slot capacity calculation already implemented in the Industry Slots page. Active manufacturing and research jobs reduce their respective slot pools. Reaction jobs occupy a separate reaction slot pool (controlled by `Mass Reactions` + `Advanced Mass Reactions`) and do not affect manufacturing or research slot availability.

**Slots freed by DELIVER:** Jobs whose `end_date < now` are treated as already delivered at the start of Phase 6 — their slots are counted as free, even though the user has not physically clicked "deliver" yet. This allows the planner to fill those slots in the same compute cycle. If the user does not execute the DELIVER action before starting new jobs, EVE will still hold the slot until delivery — this is an accepted approximation.

Assignment priority:
1. Manufacturing and sub-manufacture jobs → character with most free manufacturing slots; ties broken by fewest total active jobs. Sub-manufacture jobs follow the same assignment rules as regular manufacturing jobs and consume the same slot type.
2. Invention jobs → character with the highest sum of (item-specific required Science skill level + Metallurgy skill level) and a free research slot. The required Science skill is looked up per T2 item from the SDE blueprint invention requirements (e.g. *Caldari Starship Engineering* for Cerberus BPCs). If multiple characters have identical sums, ties are broken by fewest total active jobs.
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
                                   − already_allocated_to_other_planned_jobs
    # 'already_allocated_to_other_planned_jobs' covers ALL planned jobs in this compute cycle
    # (both today's jobs and future_stock jobs within the shopping_future_stock_days window),
    # not just today's. This prevents double-buying when two jobs need the same material.
    # Jobs are processed in priority order so high-priority jobs are allocated first.
    if net_required <= 0: skip (already covered by stock)

    determine shopping_category:
        'current_job'     — material needed for a job in today's action list
        'future_stock'    — material needed for jobs in the current plan that are not starting
                            today, up to a configurable planning window (default: 7 days ahead,
                            configurable via admin settings as `shopping_future_stock_days`).
                            Jobs beyond this window are excluded from pre-buy to avoid tying up
                            excessive capital for work that may never start.
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

    # Material cost estimate — VWAP vs spot price (updated)
    # Use vwap_5d from market_depth_cache as the primary price estimate.
    # vwap_5d smooths out daily orderbook noise and reflects what large-volume
    # buyers actually paid over the past week, which is more realistic than the
    # momentary best-sell-price for large mineral purchases (e.g. 100M Tritanium).
    # Fall back to spot_sell_price if vwap_5d is unavailable (< 3 days history).
    estimated_cost = vwap_5d × net_required   # was: hub_sell_price × net_required

    # cost_multiplier from plan_learning_weights is a per-product diagnostic signal
    # (how well we predicted a product's total material cost), not a per-material
    # price signal. It is used in Phase 3 scoring to improve profitability predictions
    # for manufactured items, not applied to individual shopping line items.
```

The shopping list in the UI groups rows by `shopping_category` with subtotals per category and a grand total ISK figure vs corp wallet balance.

### Phase 8 — Daily Action List

Per character, ordered to match the recommended daily workflow (deliver → sell → buy → start):

1. **DELIVER** — corp jobs with `end_date < now` (manufacturing, copy, invention, reaction, research)
2. **RELIST** — market orders flagged by `PricingSuggestionService`; shown immediately after DELIVER so the user reprices goods before buying materials
3. **START INVENTION** — highest priority items needing BPCs, research slots free
4. **START COPY** — BPO copy jobs feeding invention pipeline
5. **START ME/TE RESEARCH** — BPOs with research below target, research slots free
6. **START SUB-MANUFACTURE** — component jobs that must complete before parent manufacturing starts
7. **START MANUFACTURING** — highest priority items with BPCs ready, manufacturing slots free

The BUY step (Tab 2 — Shopping List) sits between RELIST and the START actions. After completing DELIVER and RELIST in Tab 1, the UI prompts the user to go to Tab 2 to buy materials before returning to Tab 1 for the START actions.

Each action includes: item name, quantity/runs, estimated cost, estimated completion time, **structure name** (`industry_profile.location_name`) and **profile name** (`industry_profile.profile_name`) for all job actions (manufacture, invent, copy, me_research, te_research, sub_manufacture), and a plain-language note explaining the reasoning. Structure context is sourced from the `industry_profile` embedded in every `IndustryService` overview row — no additional lookup required.

### Recompute after a gap (self-healing behaviour)

When the user recomputes after 1–3 days of absence, the planner does not attempt to reconstruct what happened during the gap. It reads current state and plans forward:

- **Jobs completed during the gap** appear in `CorporationIndustryJobsModel` as `status='delivered'` (background ESI polling ran while the user was away). Phase 1 picks them up; they are not regenerated as DELIVER actions — they already happened.
- **Jobs the user started manually in-game** appear as `status='active'` in `CorporationIndustryJobsModel`. Phase 6 sees those slots as occupied and does not double-book them.
- **Market orders repriced manually** appear with their updated price in `CorporationMarketOrdersModel`. RELIST actions in the new plan reflect only orders that still need repricing.
- **Materials bought manually** appear in `CorporationAssetsModel`. Phase 7 deducts them from the shopping list as if they were always there.
- **Pending actions from the expired plan** are archived with the plan (`status → 'archived'`). They are not marked `done` or `skipped` — they remain `pending` under the archived plan for audit purposes. They do not appear in the new plan's action list. The feedback loop does not process them (only `status='done'` rows feed back into EMA weights).
- **Feedback gap** — if manufacturing jobs completed and sold during the gap, the feedback processor runs at the start of Phase 1 on the next recompute and picks up all outcomes from the gap period at once. No data is lost; it is just processed in batch on return.

**The key property:** a recompute after any length of absence produces a correct, current plan. The user does not need to manually reconcile what was done or not done during the gap. The only consequence of a gap is that pending actions from the old plan are abandoned (not fed back into the learning loop), which slightly delays EMA weight convergence for items that were active during the gap.

### Phase 9 — Persistence

- Archive current active plan (`status → 'archived'`)
- Insert new `build_plan` record
- Insert `build_plan_item` rows per product decision
- Insert `daily_action_log` rows per character action. `buy_materials` and `buy_bpo` rows **are** written to `daily_action_log` (for aggregate shopping list generation and future feedback tracking) but are **not rendered in Tab 1**. They surface through Tab 2 (Shopping List). The Shopping List does not expose per-row done/skip checkboxes — buy actions are considered implicitly completed when the user marks a dependent START action as done. `processed_for_feedback` on buy_materials rows is never set to true (no feedback loop for material purchases).
- Compute `market_snapshot_hash`: SHA-256 of `spot_sell_price` values for all `build` items' products plus their direct input materials. "Direct input materials" means: all eight base minerals (Tritanium, Pyerite, Mexallon, Isogen, Nocxium, Zydrine, Megacyte, Morphite) plus any intermediate manufactured components that appear as a direct input in a planned job and are bought from market (sub-manufacture items excluded). Raw reaction outputs (gases, fullerides) used as T2 inputs are included if they appear as direct job inputs. **Exact hash format:** sort all `(type_id, price)` pairs by `type_id` ascending; serialize each as `"{type_id}:{price:.2f}"` (price rounded to 2 decimal places); concatenate with commas; SHA-256 the resulting UTF-8 string. Example: `"34:5.10,35:9.87,36:74.22,...". Price source: `spot_sell_price` from `market_depth_cache` for all tracked items. The staleness check re-hashes using the identical source and format on every `GET /planner/plan` call.

---

## Staleness Detection

On page load, the planner re-hashes current prices of all items in the active plan and compares to `market_snapshot_hash`.

```
drifted_count  = count of items where abs(current_price - snapshot_price) / snapshot_price > drift_threshold
# Guard: if no items are tracked (e.g. first run or all items were skipped), default to 1.0
freshness_score = 1.0 if total_tracked_items == 0 else 1 - (drifted_count / total_tracked_items)
```

If `freshness_score` drops below 0.9 (i.e., more than 10% of tracked items have drifted), the UI shows a warning and prompts recomputation. Both the drift threshold (default 5%) and the freshness warning level (default 0.9) are configurable via admin settings.

---

## Self-Learning Feedback Loop

After each plan cycle, the system checks `daily_action_log` for `manufacture` actions marked `done` and `processed_for_feedback=false`. Actuals are read from `CorporationRealizedProfitLedgerService` (which exists and handles corp scope). **Attribution method:** temporal FIFO — sales are matched to batches by `type_id` chronologically, not by an exact batch-to-sale link (EVE's API provides no such link). For frequently manufactured items this introduces some noise; EMA smoothing (α=0.2) averages it out over many samples. For low-volume items (capitals, rare T2), the noise may be more pronounced and confidence tier will remain `low` longer. For each matched batch:

1. Read `actual_sell_days`, `actual_material_cost`, and realized profit via `CorporationRealizedProfitLedgerService` (do not call the model directly)
2. Write a `plan_item_outcome` record
3. Update `plan_learning_weights` using EMA (α = 0.2):

   **For normal outcomes** (`slow_mover = false`):
   ```
   # Guard: floor denominators to prevent division by zero
   # actual_days can be 0 for instantly-sold items (buy orders waiting at station)
   # predicted_material_cost should never be 0 in practice, but guard defensively
   safe_actual_days           = max(0.01, actual_days)
   safe_predicted_cost        = max(0.01, predicted_material_cost)
   safe_predicted_isk_per_hr  = max(0.01, predicted_isk_per_hour)

   accuracy_ema        = 0.8 × old_accuracy_ema + 0.2 × (actual_isk_per_hour / safe_predicted_isk_per_hr)
   velocity_multiplier = 0.8 × old_velocity + 0.2 × (predicted_days / safe_actual_days)
   cost_multiplier     = 0.8 × old_cost + 0.2 × (safe_predicted_cost / actual_material_cost)
   # NOTE: predicted / actual (same direction as velocity_multiplier).
   # If actual cost > predicted → multiplier < 1.0 → score drops (item was less profitable than expected).
   # If actual cost < predicted → multiplier > 1.0 → score rises (cheaper materials, better margin).
   # Previous formulation (actual/predicted) was inverted and would have silently corrupted the feedback loop.
   ```

   **For slow mover outcomes** (`slow_mover = true`, no realized sale):
   ```
   # accuracy_ema and cost_multiplier are NOT updated (no realized data)
   velocity_multiplier = 0.8 × old_velocity + 0.2 × (predicted_days / slow_mover_timeout_days)
   # slow_mover_timeout_days is always > 0 (configurable default 60), no guard needed
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
[ Freshness: 94% ]  [ Last Computed: 2h ago ]  [ Market Data: 47m ago ]  [ Corp Wallet: 14.2B ISK ]
[ Capital Reserved: 3.1B ISK ]  [ Projected ISK Return (7d): +8.4B ISK ]

[ Recompute Plan ]   ⚠ "Tritanium shifted 7.2% — consider recomputing"
```

**Market Data age badge:** shows how long ago `MarketIntelligenceJob` last ran. Green if < `planner_market_refresh_interval_hours`, amber if approaching the refresh window, red if stale (> 2× the interval). A stale market data badge means the next recompute will use outdated competitor depth and VWAP — the planner shows a warning inline if this is the case.

**Staleness visual state:** The Freshness badge uses colour to signal urgency:
- ≥ 90%: green (fresh)
- 75–89%: amber (mild drift)
- < 75%: red + the ⚠ warning text appears; the "Recompute Plan" button switches to primary styling to draw attention

**Recompute confirmation dialog:** If the user clicks "Recompute Plan" while Tab 1 has pending (unchecked, not-skipped) actions, show a confirmation dialog:
> "You have X actions not yet marked done. Recomputing will replace today's action list. Continue?"
> [ Cancel ] [ Recompute ]

This prevents accidental loss of the current action list mid-cycle.

**Capital Reserved** = sum of `estimated_cost_isk` for all pending `buy_materials` and `buy_bpo` actions + sum of estimated job install costs for all planned manufacturing and sub-manufacture jobs. Represents the ISK that will be committed if the user follows the full day's plan.

**Projected ISK Return (7d)** = sum of `estimated_profit_isk` for all in-flight and planned manufacturing jobs whose estimated completion date + velocity-adjusted sell time falls within the next 7 days. Sell time per item uses `sell_velocity_per_day × velocity_multiplier` from learning weights.

Plan recomputation is always manual — triggered by the "Recompute Plan" button. There is no automatic scheduled recomputation.

**Plan age gate:** If the active plan's `created_at` is older than `planner_max_plan_age_hours` (configurable, default 36h), the planner switches to **expired mode** on page load. In expired mode:
- The status bar shows the plan age prominently: "Plan is 3 days old — actions are no longer valid."
- The "Recompute Plan" button switches to primary styling regardless of freshness score.
- Tab 1 replaces the action list with a centered message: "This plan is outdated. Recompute to generate today's actions." No checkboxes or Skip buttons are shown — acting on a 3-day-old action list would be misleading.
- Tab 2 and Tab 3 remain readable (shopping list and build plan are still informative as historical context).
- Tab 4 is unaffected.

The 36-hour default covers a one-night absence comfortably. A weekend absence (48–72h) clearly exceeds it and triggers expired mode, which is the correct behavior: the user should recompute, not try to execute a stale Friday plan on Monday morning.

**Plan diff on recompute:** Immediately after a successful recompute, the status bar shows a one-line change summary comparing the new plan to the previous active plan:
> "Plan updated — 2 items newly on build (Squall, Buzzard), 1 moved to watch (Deluge: competition_index 4.2), 1 moved to skip (Maller: score below threshold)."
This summary is derived by comparing `build_plan_item.decision` between the newly created plan and the archived predecessor. It disappears after the user dismisses it or navigates away. Items that did not change decision are not mentioned.

**Onboarding / warmup guidance:** When `sample_count = 0` for all items (first run ever) or `sample_count < 5` for all items (early warmup phase), the status bar shows a persistent info banner:
> "The planner is in warmup mode — all items start with default weights. Scores improve as manufacturing batches complete and sell. Expect meaningful learning after 5+ completed batches per item."
This banner disappears automatically once at least one item reaches `confidence_tier = 'medium'`.

**Computing state:** While a recompute is in progress (`GET /planner/status` returns `"running"`), the status bar replaces the Freshness/Last Computed metrics with a progress spinner and the message "Computing plan…". All four tabs remain visible but their content reflects the previous plan (read-only, no checkboxes). The "Recompute Plan" button is disabled. Once `"done"` or `"failed"` is returned, the page refreshes: on success the new plan loads; on failure an error banner appears in the status bar and the previous plan remains active.

### Tab 1 — Today's Actions *(primary view)*

**Empty / first-run state:** If no plan exists yet, Tab 1 shows a centered message: "No plan yet — click **Recompute Plan** to generate your first daily plan." All other tabs are empty with the same prompt.

**Expired state (plan older than `planner_max_plan_age_hours`):** Tab 1 shows: "This plan is X days old and is no longer valid. Recompute to generate today's actions." No action checkboxes or Skip buttons are rendered. Tab 2 and Tab 3 remain visible as read-only historical context. The status bar "Recompute Plan" button is shown in primary styling.

**Return-after-gap note for the user (shown in expired mode):** A brief explanation below the expired message: "Jobs that completed while you were away have been tracked automatically. Recomputing will plan from your current slot availability, stock, and market state — no manual reconciliation needed."

Per-character accordion sections. Within each character, actions are grouped and ordered by type matching the daily workflow: Deliver → Relist → Invent → Copy → ME/TE Research → Sub-Manufacture → Manufacture.

**Workflow progress header** (above all character accordions):
```
Step 1: Deliver  ✓ done   →   Step 2: Sell/Relist  ✓ done   →   Step 3: Buy materials  [ Go to Shopping List → ]   →   Step 4: Start jobs
```
The header tracks which steps are complete. Step 3 ("Buy materials") is always a manual link to Tab 2 — the planner cannot know when the user has finished buying. Steps 1 and 2 auto-complete when all DELIVER and RELIST actions for all characters are `done` or `skipped`. Step 4 auto-completes when all START action types are `done` or `skipped`.

**Day complete state:** When all actions across all characters are `done` or `skipped`, Tab 1 shows a "Day complete ✓ — see you tomorrow" banner in place of the character accordions. The banner includes a summary: jobs started, ISK committed, next job completing at (datetime).

Each action row has:
- A **checkbox** (`done`) — calls `POST /planner/action/done`
- A **Skip button** (`skipped`) — calls `PATCH /planner/action/{id}` with `{"status": "skipped"}`; collapses the row with a strikethrough label

Both interactions write to `daily_action_log` in real-time. "Mark All Delivered Done" convenience button per character marks only that character's DELIVER actions as done (not all action types). Sections only render when at least one action of that type exists for the character — a character with no copy jobs will show no COPY section.

**"Ready to start" vs "materials needed first" distinction:** START action rows (MANUFACTURE, SUB-MANUFACTURE, INVENT, COPY, ME/TE RESEARCH) show a readiness indicator based on whether the required materials are already in the corp hangar:
- ✅ **Ready** — materials confirmed in `CorporationAssetsModel` at plan-compute time
- 🛒 **Buy first** — materials are in the shopping list (Tab 2); link to Tab 2 inline

The readiness state is computed at plan time (Phase 7 knows exactly which materials are missing). If the user buys materials after the plan is computed but before starting the job, the indicator may be stale — it reflects plan-time state, not live inventory. A note in the UI clarifies this: "Readiness based on stock at plan time. Re-check in-game before submitting."

**Corp-level vs character-level action placement:** `buy_materials` and `buy_bpo` actions are corp-level — any character with market access can execute them. These actions do **not** appear inside any character accordion. Instead, they live in the Tab 2 Shopping List (their natural home). Tab 1 contains only character-specific actions (DELIVER, RELIST, MANUFACTURE, INVENT, COPY, ME/TE RESEARCH, SUB-MANUFACTURE). The workflow progress header Step 3 ("Buy materials → Tab 2") is the bridge between Tab 1 and Tab 2 for buy actions.

**Force-override badge:** Actions generated by a session-only force-include override (Tab 3) are shown with a ★ badge and "manual override" label so the user can distinguish system-recommended from manually added actions.

**Estimated completion times** are shown as both relative ("est. 4d 6h") and absolute ("Wed 03 Sep 18:30 EVE") to avoid ambiguity across sessions.

Example layout (not all sections will always appear):
```
Step 1: Deliver  ✓   →   Step 2: Sell/Relist  ✓   →   Step 3: Buy materials  [ Go to Shopping List → ]   →   Step 4: Start jobs

▼ Aldara Voss   [3 mfg slots free]  [2 research slots free]

  🔴 DELIVER   [ Mark All Delivered Done ]
     ☑  Tengu ×5 runs — move to corp hangar

  🔵 RELIST
     ☐  Tengu — advised 387M (currently 371M, +4.3% margin)                               [ Skip ]

  🟡 INVENT
     ☐  Tengu BPC ×10 attempts — ~1.2M ISK — est. 18h (Wed 03 Sep 18:30 EVE)             [ Skip ]
        "BPC stock covers 1.8 days — start now to avoid slot gap"

  🔧 SUB-MANUFACTURE
     ☐  Crystalline Carbonide Armor Plate ×200 — ~3.1M ISK — est. 6h (today 22:00 EVE)   [ Skip ]
        "cheaper to build than buy (market: 4.2M ISK) — must complete before Cerberus job"

  🟢 MANUFACTURE
     ☐  Cerberus ×3 runs — ~42M ISK — est. 4d 6h (Sat 06 Sep 12:00 EVE)                 [ Skip ]
     ☐  Medium Shield Extender ×10 runs — ~8M ISK — est. 14h (today 30:00 EVE)           [ Skip ]
        ★ manual override
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

**Wallet shortage handling:** If `Total to spend > Corp wallet`, the shopping list highlights the deficit in red. `current_job` rows are shown first with a "Buy today" label — these are required for jobs starting today. `future_stock` rows are labelled "Optional — can defer" and visually de-emphasised. The user can decide to skip future_stock purchases this cycle without breaking any job that starts today.

**Shopping list export:** A "Copy to clipboard" button above the Materials grid exports the full shopping list as plain text — one line per item: `Item name × quantity (est. X ISK)`. Useful for communicating buy orders to corpmates or pasting into third-party tools. Format:
```
Tritanium × 4,200,000 (est. 21.4M ISK)
Fullerite-C320 × 800 (est. 38.6M ISK)
...
Total: 142.8M ISK
```

**BPO Investment Opportunities** — T1 only, sorted by break-even speed:

| BPO | Market Price | Break-even | Annual Savings | Cumul. ISK needed | Fits budget? | Recommendation |
|---|---|---|---|---|---|---|
| Medium Shield Extender | 420M ISK | 22 days | 6.8B ISK | 420M ISK | ✓ | ★ Strong Buy |
| Caracal | 320M ISK | 67 days | 2.1B ISK | 740M ISK | ✓ | Consider |

"Cumul. ISK needed" is the running total of market prices for all BPOs above (sorted by break-even). "Fits budget?" compares the cumulative total to `Corp wallet − Total materials to spend`. This lets the user see at a glance how many BPOs they can buy in one trip.

T2 item BPOs never appear (they don't exist). T1 BPOs may appear either as direct product investments or as invention enablers for T2 items (e.g. a Caracal BPO appearing because it enables Cerberus invention).

### Tab 3 — Build Plan

AG-Grid of all evaluated items. Filterable by decision type. Shows: item name, decision, priority score, ISK/hour, margin %, days of supply, **competitor supply days**, **competition index**, **momentum signal**, confidence tier, sample count, decision reason.

The `competitor_supply_days` and `competition_index` columns make it immediately visible why an item is in `watch` despite a healthy score (flooded market) — which was previously invisible to the user. **These columns are NOT stored in `build_plan_item`** — they are derived at render time: `competition_index` is read from `market_depth_cache` by `(type_id, hub)`, and `competitor_supply_days = competition_index × 30`. If `market_depth_cache` has no row for a type_id (Phase A), both columns render as "—".

Items flagged as "mineral-squeeze sensitive" (Pearson correlation < −0.6) show a ⚠ badge in the item name column with tooltip: "margin historically sensitive to mineral price rises".

**Score breakdown drill-down (per item):** clicking any row in the grid expands an inline detail panel showing every scoring component individually:

| Factor | Value | Multiplier applied |
|---|---|---|
| Base ISK/hour | 18.4M | — |
| accuracy_ema | 0.94 | ×0.94 |
| velocity_multiplier | 1.08 | ×1.08 |
| cost_multiplier | 0.97 | ×0.97 |
| market_timing_factor | 0.91 (trend −7.2%, momentum −2.1) | ×0.91 |
| pipeline_saturation | 1.12 (1.4d supply) | ×1.12 |
| competition_factor | 0.90 (index 1.6) | ×0.90 |
| mineral_squeeze_penalty | 1.00 (not sensitive) | ×1.00 |
| confidence_tier_bonus | 1.05 (medium, 8 samples) | ×1.05 |
| **Adjusted score** | **17.2M ISK/hour** | |
| **Decision** | **build** (above 5M threshold, pipeline < 3d, index < 4.0) | |

This panel also shows the `decision_reason` text and the active force-override toggle for this item. The user can see exactly which factor is suppressing an item and make an informed override decision rather than guessing.

Session-only override buttons (force include / force exclude / reset) affect the current view only and are never written to the database. They reset when the plan is recomputed. The goal is for the self-learning feedback loop to make manual overrides unnecessary over time.

**All-watch/skip fallback:** if no item has `decision='build'` after Phase 4 (all items are saturated, unprofitable, or paused), Tab 1 shows no START actions. In this case, Tab 3 shows an additional panel: "No items selected for manufacturing today. Items closest to build threshold:" — a sorted list of the top 5 `watch` items by adjusted score with their distance to the threshold (e.g., "Flycatcher — 4.2M ISK/hour, needs +0.8M to qualify"). This gives the user actionable context rather than a silent empty plan.

### Tab 4 — Plan Analytics

- **Accuracy** — per-item predicted vs actual ISK/hour over last N batches (default N = 20, configurable via admin settings)
- **Velocity** — predicted vs actual sell days per item; slow_mover outcomes shown distinctly
- **Competition monitor** — per-item `competition_index` trend over the last 30 days (one data point per plan recompute). Shows whether competitor supply is growing, stable, or thinning. Useful for spotting markets that are slowly being flooded before the price impact appears in the trend data.
- **Invention success rates** — per T2 item: theoretical vs actual success rate, total attempts, effective BPC cost. Items with actual rate >10% below theoretical are highlighted — they may need a different decryptor or character assignment.
- **Margin–mineral correlation** — table of all "mineral-squeeze sensitive" items (correlation < −0.6) with current Tritanium 7d trend. When Tritanium is rising, these items are pre-emptively flagged. Configurable correlation threshold via admin settings (`planner_mineral_correlation_threshold`, default −0.6).
- **Plan History** — timeline of recomputations over the last `plan_history_days` (default 90 days, configurable via admin settings); per-cycle changes tracked as decision shifts per item (e.g. `build → watch`, `skip → build`), net ISK earned vs projected. Older archived plans remain in the DB but are not shown.
- **Compute errors** — if a background computation fails, the status bar shows an error banner with the failure reason (sourced from `GET /planner/status`). The previous active plan remains visible and usable until a successful recompute replaces it.

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
| `GET` | `/planner/status` | Poll computation progress; returns `{ "status": "idle" \| "running" \| "done" \| "failed", "error": "..." }` — Streamlit polls this until `done` or `failed`, then shows error banner on failure |
| `GET` | `/planner/plan` | Fetch current active plan (build_plan + items + daily actions). **Side effect:** recomputes and writes `freshness_score` back to `build_plan` on each call. This is a deliberate tradeoff (avoids a separate staleness endpoint); document in the route handler so reviewers don't flag it as a bug. |
| `POST` | `/planner/action/done` | Mark a `daily_action_log` row as done (`status → 'done'`) |
| `PATCH` | `/planner/action/{id}` | Multi-purpose status update. Body: `{"status": "<value>"}`. Three distinct behaviors: (1) `"pending"` — undo a done action (reverse accidental mark-done); (2) `"skipped"` — user explicitly dismisses the action for this cycle (Skip button in Tab 1); (3) `"pending"` on a currently-skipped row — restore a skipped action back to pending. All transitions require `processed_for_feedback=false`; returns 409 if feedback already processed. |
| `GET` | `/planner/analytics` | Fetch self-learning accuracy and velocity stats |

---

## Implementation Notes

- **Primary market hub** for shopping list and BPO price lookups: configurable via admin settings (default: `jita`). The planner must pass this hub explicitly to all `MarketPricingService` calls. `get_material_sell_price_map()` is hardcoded to Jita and must NOT be used by the planner — use `get_type_price_map(hub=configured_hub)` instead. **Note:** `get_type_price_map` already exists in `MarketPricingService` and accepts a `hub` keyword argument — no new method needed.
- Background computation follows the same pattern as `IndustryService` refresh (background thread, progress callbacks, polled by frontend)
- Plan recomputation is always manual — no scheduled auto-recompute
- Schema migrations added to `schema_migrations.py` for all eight new tables
- EMA weight α = 0.2 (configurable via admin settings)
- Staleness price drift threshold = 5% (configurable via admin settings)
- IndustryService overview freshness threshold = 12 hours (configurable via admin settings)
- BPO break-even thresholds (Strong Buy: 30d, Consider: 90d) configurable via admin settings
- Slow-mover timeout = 60 days (global default, configurable via admin settings). Applies uniformly across all item categories. **Known limitation:** 60 days may be too short for capitals or rare T2 items that legitimately take months to sell — these will accumulate slow_mover outcomes and have their velocity scores penalised unfairly. Mitigation: monitor confidence tier and velocity_multiplier for high-value items in Tab 4; a future version may add per-meta-group timeout overrides.
- Minimum ISK/hour threshold for `build` decision = 5M ISK/hr (configurable via admin settings)
- `freshness_score` warning level = 0.9 (configurable via admin settings)
- Tab 4 accuracy window N = 20 batches (configurable via admin settings)
- Tab 4 Plan History depth = 90 days rolling window (configurable via admin settings as `plan_history_days`); older archived `build_plan` records are retained in the DB but excluded from the UI history view
- Pipeline gap prevention look-ahead: start invention/copy if current BPC stock covers less than `job_duration_days + 1` of planned manufacturing (+1 day safety buffer for job setup overhead)
- `shopping_future_stock_days` = 7 days (configurable via admin settings); controls how far ahead the planner pre-buys `future_stock` materials
- `GET /planner/plan` computes live `freshness_score` on each call and writes the updated value back to `build_plan`, so the DB always holds the most recent staleness assessment. **Price source for staleness re-hash:** `spot_sell_price` from `market_depth_cache` (no live ESI call). This means staleness reflects MIJ's last snapshot, not a true live price — acceptable given MIJ runs every 3h.
- **`SalesHistoryService`** — Phase 1 calls this service to get `sell_velocity_per_day` per type_id. Method to call: `get_velocity(type_id, days=30)` (verify exact method name in the existing service before Phase A.2). This service already exists in the application layer. It is not in the architecture diagram's "Data sources" table but is a required Phase 1 dependency.
- **Prerequisites must be completed first** — see the "Prerequisites — Required Changes to Existing Code" section above. Specifically: `price_trend_30d_pct` must exist in `IndustryService` overview rows before Phase 3 scoring can use it, and `aggregate_shopping_list` must be in the application layer before `ShoppingListBuilder` can import it.
- **`price_trend_30d_pct` null handling:** if `MarketHistoryService` returns `None` for `trend_30d_pct` (fewer than 30 days of history for a type), the planner treats it as `0.0` — momentum_signal degrades to `price_trend_7d_pct` alone, which is the same behaviour as before this feature was added.
- **VWAP fallback:** if `MarketHistoryModel` has fewer than 3 days of data for a type (new item, rare drop), fall back to `spot_sell_price` from the orderbook. Log a warning in the compute status so the user knows the estimate is less reliable. With `MarketIntelligenceJob` calling `fetch_and_store_history()` every 3h, this fallback should only trigger for genuinely new items.
- **Market depth ESI calls** are batched using the region orders endpoint (`/markets/{region_id}/orders/?type_id=X`) — one call per type. With a 20-item portfolio this is 20 additional ESI calls per compute. The existing `_EsiErrorRateLimiter` in `ESIClient` already handles rate limiting; no additional throttling needed beyond the 10-thread pool cap.
- **Invention outcome logging** is idempotent — re-running Phase 1 never double-writes to `invention_outcome_log`. Use `INSERT OR IGNORE` (SQLite) keyed on `(type_id, character_id, completed_at)`.
- **Margin–mineral correlation** requires at least 30 data points (30 trading days of both Tritanium price and realized margin for the item) before the correlation is computed. Below 30 points, `mineral_squeeze_penalty = 1.0` (no penalty applied). The computation uses `scipy.stats.pearsonr` — already in `requirements.txt`.
- **Streamlit polling pattern:** `DailyPlannerService` uses a background thread + progress callback, identical to the existing `IndustryService` refresh pattern. The Streamlit page polls `/planner/status` with `st.rerun()` (not `st_autorefresh`) — the same approach used in the existing industry overview page. Verify before Phase A begins that `st.rerun()` still behaves correctly in the deployed Streamlit version (some versions introduced a loop-guard warning); if so, the same workaround already used elsewhere applies here.
- **Seasonal patterns** are explicitly out of scope for this implementation. The `MarketHistoryModel` data is retained indefinitely and will be available for a future seasonal analysis feature once 12+ months of data have accumulated.
- **competition_index gate threshold** (default 4.0) may need tuning per market. T2 destroyers (Flycatcher, Squall) have thinner markets than advanced components — a competition_index of 2.0 might already be flooded for ships but normal for minerals. A future per-category override is a possible extension; for now a single global threshold is used.

---

## Code Structure & Integration

This section maps every new artefact to its exact location in the repository, explains how it connects to existing infrastructure, and defines the constructor contracts so each component can be built and tested independently.

### Module Layout

All new files follow the same package conventions as the existing codebase.

```
# ── Prereq changes (existing files / new file in existing package) ─────────────

src/eve_online_industry_tracker/
└── application/
    └── market_analysis/
        └── market_history_service.py           # MODIFY: add avg_30d + trend_30d_pct to get_price_stats()

src/eve_online_industry_tracker/
└── application/
    └── industry/
        ├── service.py                          # MODIFY: add price_trend_30d_pct to overview row (~line 1941)
        └── shopping_list.py                    # NEW: aggregate_shopping_list() moved here from streamlit_ui

src/streamlit_ui/
└── shopping_list.py                            # MODIFY: re-export from application layer (keeps existing callers intact)

# ── New packages ───────────────────────────────────────────────────────────────

src/eve_online_industry_tracker/
└── application/
    └── market_intelligence/                    # new sub-package (Tier 1 background job)
        ├── __init__.py
        ├── job.py                              # MarketIntelligenceJob  (scheduler + orchestrator + history refresh)
        ├── market_depth_collector.py           # MarketDepthCollector   (ESI + VWAP)
        └── margin_correlator.py                # MarginCorrelator       (Pearson, 24h cadence)

src/eve_online_industry_tracker/
└── application/
    └── daily_planner/                          # new sub-package (Tier 2 plan compute)
        ├── __init__.py
        ├── service.py                          # DailyPlannerService  (orchestrator + background thread)
        ├── feedback_processor.py               # FeedbackProcessor    (EMA weight updates)
        ├── pipeline_analyzer.py                # PipelineAnalyzer     (Phase 2)
        ├── profitability_scorer.py             # ProfitabilityScorer  (Phase 3)
        ├── item_decision_engine.py             # ItemDecisionEngine   (Phase 4)
        ├── chain_planner.py                    # ChainPlanner         (Phase 5 — BPC/invention/copy/BPO)
        ├── character_assigner.py               # CharacterAssigner    (Phase 6)
        ├── shopping_list_builder.py            # ShoppingListBuilder  (Phase 7)
        └── action_plan_builder.py              # ActionPlanBuilder    (Phase 8)

src/eve_online_industry_tracker/
└── infrastructure/
    ├── models.py                               # add 8 new SQLAlchemy model classes (append, do not restructure)
    ├── schema_migrations.py                    # add 8 new CREATE TABLE IF NOT EXISTS blocks (append to ensure_app_schema)
    └── persistence/
        └── daily_planner_repo.py               # DailyPlannerRepository  (CRUD for all 8 tables)

src/eve_online_industry_tracker/
└── infrastructure/
    └── sde/
        └── blueprints.py                       # add compute_optimal_me() and compute_optimal_te() as module-level functions

src/flask_app/
├── routes/
│   └── daily_planner.py                        # daily_planner_bp Blueprint (includes /planner/market_intel/status)
├── state.py                                    # add DailyPlannerJobState + MarketIntelligenceJobState to AppState
├── background_jobs.py                          # register both jobs: MarketIntelligenceJob (scheduled) + PlanComputeJob
└── app.py                                      # register daily_planner_bp in create_app()

src/streamlit_ui/
├── api/
│   └── daily_planner.py                        # HTTP client for /planner/* endpoints
├── state/
│   └── daily_planner_page.py                   # DailyPlannerPageState dataclass
├── pages/
│   └── daily_planner.py                        # Streamlit page entry-point (tab routing only)
└── components/
    └── daily_planner/                          # one file per tab + status bar
        ├── __init__.py
        ├── status_bar.py                       # render_status_bar()  — freshness badge, recompute button, plan age gate
        ├── tab_actions.py                      # render_tab_actions() — Today's Actions (Tab 1)
        ├── tab_shopping.py                     # render_tab_shopping() — Shopping List (Tab 2)
        ├── tab_build_plan.py                   # render_tab_build_plan() — Build Plan (Tab 3)
        └── tab_analytics.py                    # render_tab_analytics() — Analytics (Tab 4)
```

---

### SQLAlchemy ORM Model Classes

Eight new classes appended to `infrastructure/models.py`. All use `BaseApp` (the existing `eve_app.db` declarative base). Naming follows the existing `*Model` convention.

| Table | Class name |
|---|---|
| `build_plan` | `BuildPlanModel` |
| `build_plan_item` | `BuildPlanItemModel` |
| `daily_action_log` | `DailyActionLogModel` |
| `plan_item_outcome` | `PlanItemOutcomeModel` |
| `plan_learning_weights` | `PlanLearningWeightsModel` |
| `market_depth_cache` | `MarketDepthCacheModel` |
| `invention_outcome_log` | `InventionOutcomeLogModel` |
| `margin_correlation_cache` | `MarginCorrelationCacheModel` |

All eight are also re-exported from `db_models.py` alongside the existing models.

---

### Schema Migrations

Appended to `schema_migrations.py::ensure_app_schema()`. Tables must be created in dependency order because `build_plan_item` and `daily_action_log` FK into `build_plan`, and `plan_item_outcome` FKs into `build_plan_item`:

1. `build_plan`
2. `build_plan_item` (FK → `build_plan`)
3. `daily_action_log` (FK → `build_plan`)
4. `plan_item_outcome` (FK → `build_plan_item`)
5. `plan_learning_weights` (no FK; `type_id` UNIQUE)
6. `market_depth_cache` (no FK; `UNIQUE(type_id, hub)`; `INSERT OR REPLACE` on MIJ cycle — no plan_id column)
7. `invention_outcome_log` (no FK; `INSERT OR IGNORE` on `(type_id, character_id, completed_at)`)
8. `margin_correlation_cache` (no FK; `type_id` UNIQUE; `INSERT OR REPLACE` on MarketIntelligenceJob cycle)

SQLite does not enforce FK constraints at the DB layer (they require `PRAGMA foreign_keys = ON`), so the order matters only for logical clarity and future portability. Use `CREATE TABLE IF NOT EXISTS` and add a `CREATE INDEX IF NOT EXISTS` on `build_plan_item.plan_id`, `daily_action_log.plan_id`, `daily_action_log.type_id`, `plan_item_outcome.plan_item_id`, and `plan_learning_weights.type_id`.

---

### DailyPlannerRepository

`infrastructure/persistence/daily_planner_repo.py` — follows the existing `*_repo.py` convention. Takes a `SessionProvider` in the constructor (same `Protocol` used by all existing repos). Provides:

```python
class DailyPlannerRepository:
    def __init__(self, session_provider: SessionProvider) -> None: ...

    # build_plan
    def get_active_plan(self) -> BuildPlanModel | None: ...
    def archive_active_plan(self) -> None: ...
    def insert_plan(self, plan: BuildPlanModel) -> int: ...          # returns new plan_id
    def update_freshness_score(self, plan_id: int, score: float) -> None: ...

    # build_plan_item
    def insert_plan_items(self, items: list[BuildPlanItemModel]) -> None: ...
    def get_plan_items(self, plan_id: int) -> list[BuildPlanItemModel]: ...

    # daily_action_log
    def insert_actions(self, actions: list[DailyActionLogModel]) -> None: ...
    def get_actions(self, plan_id: int) -> list[DailyActionLogModel]: ...
    def mark_action_done(self, action_id: int) -> None: ...
    def set_action_status(self, action_id: int, status: str) -> None: ...  # for PATCH undo / skip
    def get_unprocessed_done_actions(self) -> list[DailyActionLogModel]: ...
    def mark_action_feedback_processed(self, action_id: int) -> None: ...

    # plan_item_outcome
    def insert_outcome(self, outcome: PlanItemOutcomeModel) -> None: ...
    def get_outcomes(self, type_id: int, limit: int = 20) -> list[PlanItemOutcomeModel]: ...

    # plan_learning_weights
    def get_weights(self, type_ids: list[int]) -> dict[int, PlanLearningWeightsModel]: ...
    def upsert_weights(self, weights: PlanLearningWeightsModel) -> None: ...

    # market_depth_cache
    def upsert_market_depth(self, rows: list[MarketDepthCacheModel]) -> None: ...   # INSERT OR REPLACE
    def get_market_depth(self, type_ids: list[int], hub: str) -> dict[int, MarketDepthCacheModel]: ...  # keyed by type_id; reads by (type_id, hub) — no plan_id

    # invention_outcome_log
    def log_invention_outcome(self, outcome: InventionOutcomeLogModel) -> None: ...  # INSERT OR IGNORE
    def get_invention_success_rates(self, type_ids: list[int]) -> dict[int, float]: ...
    # returns {type_id: actual_success_rate} for items with >= planner_invention_min_attempts

    # cleanup — called at the end of Phase 9 (after the new plan is inserted)
    def purge_old_action_log_rows(self, cutoff_days: int) -> int: ...
    # DELETE FROM daily_action_log
    # WHERE processed_for_feedback = 1
    #   AND plan_id IN (
    #       SELECT id FROM build_plan
    #       WHERE created_at < datetime('now', '-{cutoff_days} days')  -- uses created_at (table column name)
    #   )
    # Returns number of rows deleted. Uses planner_history_days (default 90) as cutoff.
    # Only deletes rows that have already been processed — unprocessed rows are retained regardless of age.
```

---

### DailyPlannerService — Constructor Contract

`application/daily_planner/service.py`. Receives all dependencies via constructor injection (same pattern as `IndustryService`, `PortfolioService`):

```python
class DailyPlannerService:
    def __init__(
        self,
        industry_service: IndustryService,
        corporations_service: CorporationsService,
        characters_service: CharactersService,
        sales_history_service: SalesHistoryService,
        pricing_suggestion_service: PricingSuggestionService,
        market_pricing_service: MarketPricingService,
        realized_profit_service: CorporationRealizedProfitLedgerService,
        repo: DailyPlannerRepository,
        admin_settings: AdminSettingsManager,
        session_provider: SessionProvider,
    ) -> None: ...

    def compute_plan_async(self) -> None: ...       # starts background thread; sets job state
    def get_compute_status(self) -> dict: ...       # {"status": "idle"|"running"|"done"|"failed", "error": "..."}
    def get_active_plan(self) -> dict: ...          # plan + items + actions; also updates freshness_score
    def mark_action_done(self, action_id: int) -> None: ...
    def set_action_status(self, action_id: int, status: str) -> None: ...
    def get_analytics(self) -> dict: ...
```

Each of the eight phases is a private method (`_phase_1_collect`, `_phase_2_pipeline`, …) that returns a typed dataclass. This makes the service unit-testable in isolation by injecting stubs for individual phases.

---

### Background Job State

`flask_app/state.py` — add two new job states following the `IndustryOverviewRefreshJobState` pattern:

```python
@dataclass
class DailyPlannerJobState:
    status: str = "idle"        # "idle" | "running" | "done" | "failed"
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

@dataclass
class MarketIntelligenceJobState:
    status: str = "idle"        # "idle" | "running" | "done" | "failed"
    error: str | None = None
    last_completed_at: datetime | None = None   # used by status bar "Market Data: Xm ago"
    next_scheduled_at: datetime | None = None
```

Add both to `JobsState`. `MarketIntelligenceJob` runs on a daemon thread with a `threading.Event`-based sleep loop (same pattern as `PublicStructuresGlobalScanJob`). It wakes every `planner_market_refresh_interval_hours × 3600` seconds, runs the collection cycle, updates `last_completed_at`, then sleeps again. Also starts immediately at bootstrap so data is available before the user's first recompute.

**Database optimizations** — apply in `schema_migrations.py` alongside the new table migrations:

```sql
-- VWAP queries scan this index instead of full table scan on market_history
CREATE INDEX IF NOT EXISTS idx_market_history_type_region_date
    ON market_history (type_id, region_id, date DESC);
```

**WAL mode:** already enabled in `DatabaseManager.__init__` (lines 32–43) via SQLAlchemy event listener. No change needed. It already allows `MarketIntelligenceJob` to write concurrently with plan-compute reads.

---

### Service Initialization

`flask_app/bootstrap.py::initialize_application()` — instantiate `DailyPlannerService` after all its dependencies are ready and attach it to `AppState`:

```python
state.daily_planner = DailyPlannerService(
    industry_service=state.industry_service,
    corporations_service=state.corporations_service,
    characters_service=state.characters_service,
    sales_history_service=state.sales_history_service,
    pricing_suggestion_service=state.pricing_suggestion_service,
    market_pricing_service=state.market_pricing_service,
    realized_profit_service=CorporationRealizedProfitLedgerService(state.session_provider),
    repo=DailyPlannerRepository(state.session_provider),
    admin_settings=state.admin_settings,
    session_provider=state.session_provider,
)
```

Add `daily_planner: DailyPlannerService` to `AppState` (or `RuntimeState` — follow whichever holds `industry_service`).

---

### Blueprint Registration

`flask_app/routes/daily_planner.py`:

```python
daily_planner_bp = Blueprint("daily_planner", __name__)

@daily_planner_bp.post("/planner/compute")
def compute() -> Response:
    require_ready()
    get_state().daily_planner.compute_plan_async()
    return ok({"status": "started"})

# … other routes follow same pattern
```

`flask_app/app.py::create_app()` — append after existing blueprint registrations:

```python
from flask_app.routes.daily_planner import daily_planner_bp
app.register_blueprint(daily_planner_bp)
```

---

### compute_optimal_me / compute_optimal_te Placement

These are pure SDE-lookup functions (no DB session needed beyond the SDE DB). Place as module-level functions in `infrastructure/sde/blueprints.py`:

```python
def compute_optimal_me(blueprint_type_id: int, sde_session: Session) -> int:
    """Return the ME level where no material quantity further decreases."""
    ...

def compute_optimal_te(
    blueprint_type_id: int,
    sde_session: Session,
    time_savings_threshold_pct: float = 1.0,
) -> int:
    """Return the TE level where per-level time savings fall below threshold."""
    ...
```

`ChainPlanner` receives the SDE `Session` via the `SessionProvider` and calls these functions directly.

---

### Streamlit API Client

`streamlit_ui/api/daily_planner.py` — thin HTTP wrappers following the `streamlit_client` pattern used by existing API modules (`industry_builder.py`, `realized_profit.py`):

```python
def get_plan() -> dict: ...                          # GET /planner/plan
def get_status() -> dict: ...                        # GET /planner/status
def compute_plan() -> dict: ...                      # POST /planner/compute
def mark_action_done(action_id: int) -> dict: ...    # POST /planner/action/done
def set_action_status(action_id: int, status: str) -> dict: ...   # PATCH /planner/action/{id}
def get_analytics() -> dict: ...                     # GET /planner/analytics
```

---

### Streamlit Page State

`streamlit_ui/state/daily_planner_page.py` — follows the `@dataclass` + `st.session_state` pattern used by `portfolio_planner_page.py` and `industry_builder_page.py`:

```python
@dataclass
class DailyPlannerPageState:
    plan: dict | None = None
    status: str = "idle"          # mirrors compute job status
    active_tab: int = 0
    force_include: set[int] = field(default_factory=set)   # type_ids; session-only
    force_exclude: set[int] = field(default_factory=set)   # type_ids; session-only
    recompute_confirmed: bool = False                       # for the confirmation dialog

def get_page_state() -> DailyPlannerPageState:
    if "daily_planner" not in st.session_state:
        st.session_state["daily_planner"] = DailyPlannerPageState()
    return st.session_state["daily_planner"]
```

---

### Streamlit Page Structure

`streamlit_ui/pages/daily_planner.py` — top-level structure:

```python
state = get_page_state()

render_status_bar(state)           # always visible: freshness, wallet, recompute button

if state.plan is None:
    render_empty_state()
else:
    tab1, tab2, tab3, tab4 = st.tabs(["Today's Actions", "Shopping List", "Build Plan", "Analytics"])
    with tab1: render_tab_actions(state)
    with tab2: render_tab_shopping(state)
    with tab3: render_tab_build_plan(state)
    with tab4: render_tab_analytics(state)
```

Each `render_*` function is imported from `streamlit_ui/components/daily_planner/` (see Module Layout). The page entry-point file contains only the tab routing — no rendering logic. This keeps each tab independently editable and testable from the start.

---

### AG-Grid Reuse

- **Tab 2 (Shopping List):** Reuse `aggrid_import.build_aggrid()` from `streamlit_ui/components/aggrid_import.py`. Use column pinning for Item/Quantity/Total. Apply existing `aggrid_formatters.isk_formatter` for price columns.
- **Tab 3 (Build Plan):** Same grid helper. Add row colouring by `decision` value (green for `build`, amber for `watch`, red for `skip`, grey for `pause`) using AG-Grid `cellStyle` callbacks — follow the pattern already used in `portfolio_planner.py`.
- **Tab 4 (Analytics):** Use `st.dataframe` (not AG-Grid) for the accuracy/velocity tables — they are read-only and simpler. The plan history timeline uses `st.line_chart` or `st.altair_chart`.

---

### FeedbackProcessor

`application/daily_planner/feedback_processor.py` — separated from `DailyPlannerService` so it can be called both at compute time (Phase 1) and independently if needed:

```python
class FeedbackProcessor:
    def __init__(
        self,
        realized_profit_service: CorporationRealizedProfitLedgerService,
        repo: DailyPlannerRepository,
        admin_settings: AdminSettingsManager,
    ) -> None: ...

    def process_pending_feedback(self) -> int:
        """Match done actions to realized sales, write outcomes, update EMA weights.
        Returns the number of action rows processed."""
        ...
```

`DailyPlannerService` holds a `FeedbackProcessor` instance and calls `process_pending_feedback()` at the start of Phase 1.

---

### Admin Settings Additions

All new configurable knobs appended to `ADMIN_SETTINGS_SCHEMA` in `config/admin_settings.py`:

| Key | Default | Description |
|---|---|---|
| `planner_ema_alpha` | `0.2` | EMA smoothing factor for learning weights |
| `planner_min_isk_per_hour` | `5_000_000` | Minimum ISK/hr for `build` decision |
| `planner_price_drift_threshold_pct` | `5.0` | % price change that counts as drifted for staleness |
| `planner_freshness_warning_level` | `0.9` | `freshness_score` below this triggers UI warning |
| `planner_industry_data_max_age_hours` | `12` | Max age of IndustryService data before warning |
| `planner_bpo_strong_buy_days` | `30` | Break-even days threshold for ★ Strong Buy |
| `planner_bpo_consider_days` | `90` | Break-even days threshold for Consider |
| `planner_slow_mover_timeout_days` | `60` | Days before an unsold batch is written as slow_mover |
| `planner_shopping_future_stock_days` | `7` | Look-ahead window for future_stock material pre-buy |
| `planner_analytics_batch_window` | `20` | Number of batches shown in Tab 4 accuracy view |
| `planner_history_days` | `90` | Plan history depth in Tab 4 timeline |
| `planner_market_hub` | `"jita"` | Market hub for shopping list and BPO price lookups |
| `planner_vwap_days` | `5` | Number of trading days for VWAP calculation; falls back to spot if fewer days available |
| `planner_market_depth_threads` | `10` | Max parallel ESI calls for market depth fetch in Phase 1 |
| `planner_invention_min_attempts` | `10` | Min invention attempts before actual success rate replaces theoretical |
| `planner_competition_index_gate` | `4.0` | competition_index threshold above which item is forced to `watch` |
| `planner_mineral_correlation_threshold` | `-0.6` | Pearson correlation below which item is flagged as mineral-squeeze sensitive |
| `planner_mineral_squeeze_factor` | `0.85` | Score multiplier applied to mineral-squeeze-sensitive items when Tritanium 7d trend > +5% |
| `planner_momentum_pause_threshold` | `-6.0` | momentum_signal below which `pause` is triggered (in addition to the -8% trend gate) |
| `planner_mineral_correlation_min_days` | `30` | Minimum data points required before margin–mineral correlation is computed |
| `planner_market_refresh_interval_hours` | `3` | How often `MarketIntelligenceJob` refreshes market depth and VWAP |
| `planner_max_plan_age_hours` | `36` | Plan age threshold above which the plan switches to expired mode; Tab 1 action list is hidden |
| `planner_min_profit_per_batch` | `20_000_000` | Minimum absolute ISK profit per batch for `build` decision (prevents near-zero-margin fast jobs) |
| `planner_price_outlier_sigma` | `3.0` | Standard deviations from avg_30d beyond which a sell order is excluded as an outlier |
| `planner_optimal_te_threshold_pct` | `1.0` | Minimum % time saving per TE level to continue researching; below this threshold `compute_optimal_te()` stops. e.g. 1.0 = stop when one more TE level saves < 1% of job duration. |

---

### Testing Approach

Follow the existing test pattern in `tests/` — construct the service with all external dependencies replaced by lambda stubs or `MagicMock`. No database or ESI calls in unit tests.

**Priority test coverage:**

| File | What to test |
|---|---|
| `tests/test_daily_planner_pipeline.py` | `PipelineAnalyzer.compute()` with varied stock/jobs/market combos |
| `tests/test_daily_planner_scoring.py` | `ProfitabilityScorer.score()` — verify each multiplier applies correctly including edge cases (trend exactly -5%, -8%, -15%) |
| `tests/test_daily_planner_decisions.py` | `ItemDecisionEngine.decide()` — verify pause requires active jobs, verify skip on no active jobs + declining trend |
| `tests/test_daily_planner_shopping.py` | `ShoppingListBuilder.build()` — verify `already_allocated` prevents double-buy across two planned jobs sharing a material |
| `tests/test_daily_planner_feedback.py` | `FeedbackProcessor.process_pending_feedback()` — verify EMA update formula for normal outcomes; verify slow_mover path skips accuracy/cost EMA but updates velocity |
| `tests/test_compute_optimal_me.py` | `compute_optimal_me()` — verify correct ME level returned for a known blueprint with known material quantities |
| `tests/test_market_history_service.py` | `get_price_stats()` — verify `trend_30d_pct` is computed correctly; verify `None` is returned when fewer than 30 days of data available (prereq regression guard) |
| `tests/test_shopping_list_service.py` | `aggregate_shopping_list()` after move to application layer — verify output is identical to the original Streamlit-layer version for same input (prereq regression guard) |

Integration tests (hitting `eve_app.db` via a test fixture) are lower priority for the first implementation — the unit tests cover the critical computation logic.

---

## Implementation Phases

### How to work with agents

Each task below is sized to fit in a single agent context window. Before starting any task, an agent must:
1. Read this full spec (`docs/superpowers/specs/2026-08-27-daily-planner-design.md`)
2. Read the files listed under "Agent must read" for that task
3. Follow the existing conventions in those files — do not introduce new patterns without a spec-level reason

Tasks within a phase that have no shared output files can run in parallel. Tasks that produce files consumed by another task must complete first (dependency listed explicitly).

**Spec reference shorthand used below:**
- "Comp plan phases 1–9" = the numbered phases in the `## Plan Computation` section of this spec
- "Code Structure section" = the `## Code Structure & Integration` section of this spec

---

### Phase A — Prerequisites (before all other work)

These six changes touch existing, shared code. They must land in the repo — tested and merged — before any Phase A.1–A.4 work begins. Each is an independent commit.

| Task | Prereq | Agent must read | Dependency |
|------|--------|-----------------|------------|
| A.0.1 — Rename `price_trend_pct` → `price_trend_7d_pct` + add `price_trend_30d_pct` | Prereq 1 | `market_history_service.py`, `industry/service.py`, `streamlit_ui/` pages that read `price_trend_pct` | none |
| A.0.2 — Move `aggregate_shopping_list` to application layer | Prereq 2 | `streamlit_ui/shopping_list.py`, `streamlit_ui/pages/` callers | none |
| A.0.3 — Locate sub-manufacture method in `IndustryService` | Prereq 3 | `application/industry/service.py` (full file) | none |
| A.0.4 — Market history auto-refresh design note | Prereq 4 | `application/market_analysis/market_history_service.py` | none (design note, no code yet) |
| A.0.5 — ESI endpoint version pinning | Prereq 5 | `infrastructure/esi_client.py` (or equivalent ESI client file) | none |
| A.0.6 — Pydantic DTOs for new ESI endpoints | Prereq 6 | `infrastructure/esi_client.py`, Pydantic docs pattern in existing code | A.0.5 must be done first |

> **Note:** A.0.3 is a read-only investigation task. Its output is a written note in the ChainPlanner implementation file (or a comment in this spec). No code changes unless the method truly doesn't exist and must be stubbed.

---

### Phase A.1 — Data Layer

Goal: all database tables, SQLAlchemy models, and the repository exist. No computation logic yet. This unblocks all other Phase A tasks.

**Includes:**
- Schema migrations for all 8 new tables (append to `schema_migrations.py::ensure_app_schema()`)
- SQLAlchemy model classes for all 8 tables (append to `infrastructure/models.py`)
- `DailyPlannerRepository` — full CRUD (all methods listed in the Code Structure section)
- Admin settings additions (all 25 new keys appended to `ADMIN_SETTINGS_SCHEMA`)

**Agent must read:** `infrastructure/models.py` (existing model conventions), `infrastructure/schema_migrations.py` (migration pattern), an existing `*_repo.py` file for constructor and method style.

**Dependency:** All A.0.x prereqs complete.

**Test:** `tests/test_daily_planner_repo.py` — construct repo with a temporary SQLite DB, verify insert/get/update for each table. Run `schema_migrations.py` against an empty DB and verify all 8 tables exist.

---

### Phase A.2 — Computation Pipeline

Goal: `DailyPlannerService` and all sub-components (Phases 1–9 of comp plan) work end-to-end. No Flask routes, no Streamlit. Testable via unit tests.

**Includes:**
- `application/daily_planner/` package: `service.py`, `feedback_processor.py` (stub — no EMA logic yet), `pipeline_analyzer.py`, `profitability_scorer.py`, `item_decision_engine.py`, `chain_planner.py`, `character_assigner.py`, `shopping_list_builder.py`, `action_plan_builder.py`
- `infrastructure/sde/blueprints.py`: `compute_optimal_me()` and `compute_optimal_te()` as module-level functions
- Phase A scoring: `competition_index` = null (MIJ not running yet), `competition_factor` = 1.0, `mineral_squeeze_penalty` = 1.0 (no correlation data yet). `momentum_signal` **is computed normally** (`price_trend_7d_pct − price_trend_30d_pct / 4`) — it depends only on IndustryService overview rows, which are available after Prereq 1. The `pause` decision based on `momentum_signal < -6` is therefore active in Phase A. Only data from `market_depth_cache` and `margin_correlation_cache` is absent in Phase A.
- All scoring multipliers from Phase 3 that do NOT depend on `market_depth_cache` (velocity_factor, isk_per_hour gating, price trend gating, absolute profit gate)

**Agent must read:** this spec in full, `application/industry/service.py` (existing IndustryService pattern for background thread + progress callback), `application/industry/shopping_list.py` (after A.0.2 is done).

**Dependency:** Phase A.1 complete (repo must exist for Phase 9 persistence).

**Tests:** `test_daily_planner_pipeline.py`, `test_daily_planner_scoring.py`, `test_daily_planner_decisions.py`, `test_daily_planner_shopping.py`, `test_compute_optimal_me.py` (see Testing Approach section).

---

### Phase A.3 — Flask Routes

Goal: the Flask blueprint exposes all Phase A endpoints. Streamlit can poll plan state via HTTP.

**Includes:**
- `flask_app/routes/daily_planner.py` — blueprint with routes listed in the Code Structure section (all Phase A routes: `/planner/compute`, `/planner/status`, `/planner/plan`, `/planner/action/done`, `/planner/action/{id}`)
- `flask_app/state.py` — add `DailyPlannerJobState` to `AppState`
- `flask_app/background_jobs.py` — register `PlanComputeJob`
- `flask_app/app.py` — register `daily_planner_bp`

**Agent must read:** an existing Flask blueprint file in `flask_app/routes/`, `flask_app/state.py`, `flask_app/background_jobs.py`.

**Dependency:** Phase A.2 complete (service must exist to wire into routes).

---

### Phase A.4 — Streamlit UI (Tabs 1–3 + Status Bar)

Goal: the daily planner page is usable. User can see the plan, mark actions done, copy the shopping list, and trigger a recompute.

**Includes:**
- `streamlit_ui/api/daily_planner.py` — HTTP client for all Phase A routes
- `streamlit_ui/state/daily_planner_page.py` — `DailyPlannerPageState` dataclass
- `streamlit_ui/components/daily_planner/status_bar.py` — freshness badge, plan age badge, recompute button + confirmation dialog, expired state
- `streamlit_ui/components/daily_planner/tab_actions.py` — Tab 1: Today's Actions (ready/buy-first indicators, mark done, character sections)
- `streamlit_ui/components/daily_planner/tab_shopping.py` — Tab 2: Shopping List (AG-Grid, clipboard export)
- `streamlit_ui/components/daily_planner/tab_build_plan.py` — Tab 3: Build Plan table (AG-Grid, score breakdown drill-down)
- `streamlit_ui/components/daily_planner/tab_analytics.py` — stub only (empty placeholder, Tab 4 activated in Phase C)
- `streamlit_ui/pages/daily_planner.py` — entry-point, tab routing only

**Agent must read:** `streamlit_ui/components/aggrid_import.py`, `streamlit_ui/pages/portfolio_planner.py` (AG-Grid colour pattern), an existing `streamlit_ui/api/` file (HTTP client pattern), the Streamlit Page Structure section of this spec.

**Dependency:** Phase A.3 complete (routes must exist for the HTTP client to call).

**Manual test checklist before marking complete:**
- Empty state renders correctly (no plan yet)
- Recompute triggers and progress bar updates
- Confirmation dialog appears on recompute when plan exists
- Tab 1 shows correct character sections; "Mark Done" updates status
- Tab 2 shows aggregated shopping list; clipboard button works
- Tab 3 shows build plan with correct decision colours
- Plan age badge turns amber → red correctly
- Expired state hides Tab 1 action list and shows banner

---

### Phase B — Market Intelligence (2 weeks)

Goal: `MarketIntelligenceJob` runs in the background every 3h and feeds VWAP, competitor depth, and market momentum into plan scoring. Scoring now uses full formula.

**Task B.1 — `MarketIntelligenceJob` + data collection**

**Includes:** `application/market_intelligence/` package (`job.py`, `market_depth_collector.py`, `margin_correlator.py`), Flask route `/planner/market_intel/status`, `MarketIntelligenceJobState` in `AppState`, job registration in `background_jobs.py`.

**Agent must read:** `application/market_intelligence/` spec sections (job description, Phase 1 cache reads), `infrastructure/esi_client.py`, `application/market_analysis/market_history_service.py`, an existing background job file (e.g. `PublicStructuresGlobalScanJob`), Prereq 4, Prereq 5, Prereq 6.

**Dependency:** Phase A.1 complete (tables must exist). Runs independently of A.2–A.4.

---

**Task B.2 — Wire market cache into scoring**

**Includes:** Update `ProfitabilityScorer` to read `market_depth_cache` (competition_factor, momentum_signal, mineral_squeeze_penalty), update `ItemDecisionEngine` to apply competition_index gate and `pause` decision, update `DailyPlannerService.Phase1` to read from all three cache tables.

**Agent must read:** `profitability_scorer.py` and `item_decision_engine.py` (written in A.2), Phase 3 and Phase 4 scoring sections of this spec.

**Dependency:** B.1 complete (cache tables must be populated before scoring reads them can be meaningful).

---

**Task B.3 — UI: Market Data badge + Tab 4 competition views**

**Includes:** Market Data age badge in `status_bar.py` (green/amber/red), Tab 4 competition monitor table and margin–mineral correlation view in `tab_analytics.py` (partial — accuracy/invention views come in Phase C).

**Agent must read:** `status_bar.py` (written in A.4), `tab_analytics.py` stub (written in A.4), Tab 4 Analytics section of this spec.

**Dependency:** B.1 and A.4 complete.

---

### Phase C — Feedback Loop (1–2 weeks)

Goal: the planner starts learning from actual outcomes. Scoring weights improve automatically over time.

**Task C.1 — `FeedbackProcessor` + invention outcome logging**

**Includes:** Full `FeedbackProcessor` implementation (EMA updates, slow_mover handling, temporal FIFO attribution), `invention_outcome_log` population in Phase 5, actual success rate substitution, `daily_action_log` cleanup (`purge_old_action_log_rows`).

**Agent must read:** `application/daily_planner/feedback_processor.py` (stub from A.2), `Self-Learning Feedback Loop` section and `Phase 5` section of this spec, `CorporationRealizedProfitLedgerService` (existing service, do not call the model directly).

**Tests:** `test_daily_planner_feedback.py`.

**Dependency:** Phase A complete.

---

**Task C.2 — Tab 4 Analytics: accuracy, invention, plan history**

**Includes:** Accuracy/velocity metrics table, invention success rates table, plan history timeline in `tab_analytics.py`.

**Agent must read:** `tab_analytics.py` (written in B.3), Tab 4 Analytics section of this spec.

**Dependency:** C.1 complete (feedback data must exist to render).

---

### Phase D — Advanced (ongoing)

Low-priority enhancements. No hard deadline. Each is an independent task.

| Task | Description | Blocked on |
|------|-------------|------------|
| D.1 — ESI schema diffing | `tools/diff_esi_schema.py` weekly snapshot + diff vs live Swagger spec | Prereq 5 + 6 done |
| D.2 — Per-meta-group slow_mover timeout | Override `planner_slow_mover_timeout_days` per meta group in admin settings | Phase C complete |
| D.3 — Multi-hub support | Dodixie, Amarr as secondary hubs; hauling cost integration | Hauling cost data available |
| D.4 — Seasonal pattern detection | `MarketHistoryModel` must have 12+ months of data | 12 months data accumulated |
| D.5 — Per-category competition threshold | Override `planner_competition_index_gate` per meta group | Phase B complete |
| D.6 — BPO push notifications | Alert when BPO break-even drops into Strong Buy range | Phase A complete |
