# Daily Planner: Wallet-Bounded Slot Fill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The daily plan fills every character's free slots with jobs ranked by return on capital per day, never spending more than 90% of a fresh corp wallet balance.

**Architecture:** A new pure module `wallet_budget.py` holds the running budget and a stock ledger. Phase 4 (`ItemDecisionEngine`) applies the new margin and sell-time rules and computes rank and market-capped runs. Phase 6 (`CharacterAssigner`) charges every job against the budget: science first, then builds in rank order with run cuts, then a science fill pass. The corp wallet gets a "last read" timestamp that the whole app shows and the planner enforces.

**Tech Stack:** Python 3.12, SQLAlchemy (SQLite), Streamlit, pytest. Run tests with `/Users/jvaneycken/Documents/GitHub/eve-online-industry-tracker/.venv312/bin/python -m pytest -q -p no:cacheprovider` from the worktree root.

**Spec:** `docs/superpowers/specs/2026-11-16-daily-planner-wallet-slot-fill-design.md`

## Global Constraints

- New settings, in `ADMIN_SETTINGS_SCHEMA["daily_planner"]["settings"]` (`src/eve_online_industry_tracker/config/admin_settings.py`), same dict shape as `planner_min_isk_per_hour`:
  - `planner_min_margin_pct`: float, default 10.0, min 0, max 100, label "Minimum margin % for build decision".
  - `planner_max_days_to_sell`: float, default 45.0, min 1, max 365, label "Maximum expected days to sell".
  - `planner_wallet_reserve_pct`: float, default 10.0, min 0, max 90, label "Corp wallet reserve %".
  - `wallet_max_age_hours`: float, default 6.0, min 0.5, max 72, label "Maximum corp wallet age (hours)".
- `planner_min_isk_per_hour` and `planner_min_profit_per_batch` stay in the schema (old files still load) but no code reads them.
- Planner code reads settings only through `planner_setting` imported as `_adm` (`tests/test_daily_planner_fail_loud.py` pins this).
- Unknown is never 0 and never cheap. An unknown price, fee or velocity makes a job unaffordable or a product not built, and the reason says which input.
- One job per blueprint, and the skill gate on every job (`_SkillGate`), stay as they are.
- New DB columns are additive and nullable, added with `_ensure_column` in `ensure_app_schema` (`src/eve_online_industry_tracker/infrastructure/schema_migrations.py`).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Specs and plans are `*.md`, which `.gitignore` ignores: use `git add -f`.

## Review Focus

1. **Wallet balance at or just above the reserve:** the budget is ≤ 0 after the reserve, so the plan must contain no paid job, and the status bar must say why. → Task 2 `test_budget_zero_after_reserve_blocks_everything`.
2. **A batch whose first run alone costs more than the budget left:** the product gets no manufacture action. It must not get a 0-run action. → Task 5 `test_one_run_unaffordable_gives_no_manufacture`.
3. **Corp stock shared by two products:** stock that the higher-ranked product used must not make the lower-ranked one look cheaper. → Task 2 `test_stock_ledger_consumes_in_order`.
4. **Velocity unknown (analyzer floor 0.01):** the product must not be built, even with a high margin. → Task 3 `test_unknown_velocity_not_built`.
5. **Wallet read 7 hours ago:** the plan has no paid job, and the status bar names the wallet's age. → Task 1 `test_stale_wallet_is_unknown_for_planner` and Task 7 `test_service_passes_stale_wallet_as_blocked`.

---

### Task 1: Corp wallet "last read" timestamp

**Files:**
- Modify: `src/eve_online_industry_tracker/infrastructure/models.py` (`CorporationModel`, around line 336)
- Modify: `src/eve_online_industry_tracker/infrastructure/schema_migrations.py` (`ensure_app_schema`, next to the other `_ensure_column` calls)
- Modify: `src/eve_online_industry_tracker/application/corporations/corporation.py` (`refresh_corporation` around line 421; `get_corporation` dict around line 197; the constructor's DB load)
- Modify: `src/eve_online_industry_tracker/config/admin_settings.py` (add `wallet_max_age_hours`)
- Create: `src/eve_online_industry_tracker/application/corporations/wallet_age.py`
- Test: `tests/test_corp_wallet_age.py`

**Interfaces:**
- Produces: column `corporations.wallets_refreshed_at` (DATETIME, nullable, UTC).
- Produces: `get_corporation()` dict key `"wallets_refreshed_at"`: ISO string or None.
- Produces: `wallet_age_hours(refreshed_at: datetime | str | None, now: datetime) -> float | None` in `wallet_age.py`. None when `refreshed_at` is None or unparseable.
- Produces: `wallet_stale_reason(refreshed_at: datetime | str | None, now: datetime, max_age_hours: float) -> str | None`. Returns None when fresh, else `"corp wallet last read {age:.1f} h ago (limit {max_age_hours:g} h)"`, or `"corp wallet never read successfully"` when `refreshed_at` is None.
- Produces: `Corporation.refresh_wallet_balances(self) -> None`. One ESI call (`ESI_CORP_WALLETS`), after `ensure_esi()` the way `refresh_wallet_journal` does it. It sets `self.wallets` (division names kept from the stored wallets, else `"Master Wallet"` / `f"Division {n}"`) and `self.wallets_refreshed_at`, then `save_corporation()`. It raises on failure. `refresh_corporation` calls it instead of its own wallet code.
- Produces: `CorporationManager.refresh_wallet_balances(corporation_name=None, corporation_id=None)` via `_refresh_batch("refresh_wallet_balances", ...)`, and `CorporationsService.refresh_wallet_balances() -> None`, which calls `self._state.corp_manager.refresh_wallet_balances()`.

- [ ] **Step 1: Write the failing tests**

```python
def test_wallet_age_hours():
    now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
    assert wallet_age_hours("2026-11-16T09:00:00+00:00", now) == 3.0
    assert wallet_age_hours(None, now) is None

def test_stale_wallet_is_unknown_for_planner():
    now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
    assert wallet_stale_reason("2026-11-16T05:00:00+00:00", now, 6) == "corp wallet last read 7.0 h ago (limit 6 h)"
    assert wallet_stale_reason("2026-11-16T07:00:00+00:00", now, 6) is None
    assert wallet_stale_reason(None, now, 6) == "corp wallet never read successfully"

def test_refresh_sets_wallets_refreshed_at_only_on_success(...):
    # Fake ESI client as in tests/test_corporation_wallets_json_normalization.py.
    # Successful refresh -> record.wallets_refreshed_at set.
    # Wallet ESI call raising -> refresh raises and wallets_refreshed_at keeps its old value.

def test_refresh_wallet_balances_makes_one_esi_call(...):
    # Fake ESI client records endpoints: exactly [ESI_CORP_WALLETS]; balance and wallets_refreshed_at updated,
    # division name "Master Wallet" kept from the stored wallets.
```

- [ ] **Step 2: Run** `pytest tests/test_corp_wallet_age.py -v`. Expected: FAIL (ImportError).
- [ ] **Step 3: Implement.** Add the column, the `_ensure_column(db_app, table="corporations", column="wallets_refreshed_at", ddl_type="DATETIME")` call and the setting. In `refresh_corporation`, set `self.wallets_refreshed_at = datetime.now(timezone.utc)` right after `self.wallets = [...]`. `save_corporation` copies model columns from attributes, so nothing else is needed there. Load it in the constructor the way `updated_at` is loaded, and add it to `get_corporation()`. Write `wallet_age.py`. Add `refresh_wallet_balances` on Corporation, CorporationManager and CorporationsService.
- [ ] **Step 4: Run** the new tests plus `pytest tests/test_corporation_wallets_json_normalization.py -q`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(corp): record when the corp wallet was last read`.

### Task 2: Wallet budget and stock ledger

**Files:**
- Create: `src/eve_online_industry_tracker/application/daily_planner/wallet_budget.py`
- Modify: `src/eve_online_industry_tracker/config/admin_settings.py` (add `planner_wallet_reserve_pct`)
- Test: `tests/test_daily_planner_wallet_budget.py`

**Interfaces:**
- Produces `class WalletBudget`:
  - `__init__(self, *, balance: float | None, reserve_pct: float, blocked_reason: str | None = None)`. `blocked_reason` comes from the caller, e.g. a stale wallet. Balance None gives the reason `"corp wallet unknown"`. A spendable balance (`balance * (1 - reserve_pct/100)`) ≤ 0 gives `"no budget: corp wallet {balance:,.0f} ISK, {reserve_pct:g}% reserve"`.
  - `blocked_reason: str | None` (property). When set, every charge fails.
  - `available: float` (property): spendable minus charged, never below 0.
  - `charge(self, amount: float | None) -> bool`. Returns False, and charges nothing, for None, for a negative amount, when blocked, or when amount > available.
  - `set_aside(self, amount: float) -> float`. Moves up to `amount` from `available` into a science pot and returns what was set aside.
  - `charge_science(self, amount: float | None) -> bool`. Charges the science pot first and the rest from `available`; same failure rules as `charge`.
  - `spent: float` (property): everything charged, science included.
- Produces `class StockLedger`:
  - `__init__(self, stock: dict[int, int])`. Works on a copy.
  - `purchase_cost(self, materials: dict[int, int], unit_price: Callable[[int], float | None], *, commit: bool) -> float | None`. ISK to buy what stock does not cover. None when any uncovered type has no price. Only `commit=True` takes the stock.
- Produces `max_affordable_runs(cost_of: Callable[[int], float | None], max_runs: int, available: float) -> int`. Binary search on 1..max_runs for the largest run count whose cost is not None and ≤ available. Returns 0 when 1 run does not fit. Assumes the cost rises with the run count.

- [ ] **Step 1: Write the failing tests**

```python
def test_budget_applies_reserve():
    b = WalletBudget(balance=1_000.0, reserve_pct=10)
    assert b.available == 900.0
    assert b.charge(900.0) and not b.charge(0.01)

def test_budget_zero_after_reserve_blocks_everything():
    b = WalletBudget(balance=0.0, reserve_pct=10)
    assert b.blocked_reason == "no budget: corp wallet 0 ISK, 10% reserve"
    assert not b.charge(1.0) and not b.charge_science(1.0)

def test_unknown_amount_is_never_affordable():
    assert not WalletBudget(balance=1e9, reserve_pct=0).charge(None)

def test_blocked_reason_from_caller():
    b = WalletBudget(balance=1e9, reserve_pct=10, blocked_reason="corp wallet last read 7.0 h ago (limit 6 h)")
    assert not b.charge(1.0) and b.blocked_reason.startswith("corp wallet last read")

def test_science_pot_is_spent_first():
    b = WalletBudget(balance=1_000.0, reserve_pct=0)
    assert b.set_aside(100.0) == 100.0 and b.available == 900.0
    assert b.charge_science(150.0) and b.available == 850.0 and b.spent == 150.0

def test_stock_ledger_consumes_in_order():
    s = StockLedger({34: 100})
    price = {34: 5.0}.get
    assert s.purchase_cost({34: 60}, price, commit=True) == 0.0
    assert s.purchase_cost({34: 60}, price, commit=True) == 100.0   # 40 left in stock, 20 bought

def test_stock_ledger_unpriced_gap_is_unknown():
    assert StockLedger({}).purchase_cost({99: 1}, {}.get, commit=False) is None

def test_max_affordable_runs():
    assert max_affordable_runs(lambda r: 10.0 * r, 30, 95.0) == 9
    assert max_affordable_runs(lambda r: 10.0 * r, 30, 5.0) == 0
    assert max_affordable_runs(lambda r: None, 30, 1e9) == 0
```

- [ ] **Step 2: Run** `pytest tests/test_daily_planner_wallet_budget.py -v`. Expected: FAIL (ImportError).
- [ ] **Step 3: Implement** the module and the setting.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit** `feat(planner): wallet budget and stock ledger`.

### Task 3: New build rules in phase 4

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/item_decision_engine.py` (`_decide`: the SKIP and WATCH blocks)
- Modify: `src/eve_online_industry_tracker/application/daily_planner/models.py` (`ItemDecision`)
- Modify: `src/eve_online_industry_tracker/config/admin_settings.py` (add `planner_min_margin_pct`, `planner_max_days_to_sell`)
- Modify: `tests/test_daily_planner_decisions.py`, `tests/test_daily_planner_thresholds.py` (the old-gate assertions)
- Test: `tests/test_daily_planner_build_rules.py`

**Interfaces:**
- Produces `ItemDecision` fields, defaults None: `expected_days_to_sell: float | None`, `capital_return_per_day: float | None`, `market_cap_runs: int | None`.
- No new DB columns: the build reason already carries the days to sell and the capital return, and that reason is stored and shown.
- Produces in `item_decision_engine.py`: `market_cap_runs(*, batch_runs: int, units_per_run: float, velocity: float, pipeline_units: float, max_days: float) -> int`, equal to `min(batch_runs, floor((velocity * max_days - pipeline_units) / units_per_run))`, never below 0.

Rules, replacing the SKIP block's ISK/hour and batch-profit gates and the WATCH block's `pipeline_days >= 3.0`. The competition gate and PAUSE stay.
- `pipeline_units = pipeline.total_pipeline_days * pipeline.effective_velocity`.
- `pipeline.velocity_unknown_reason` set → skip, reason `"not built: sell velocity unknown ({reason})"`.
- The row's margin unknown (`PlannerInputRow.profit_margin_fraction is None`; the scorer then puts a placeholder 0.0 in `margin_pct`) → skip, reason `"not built: margin unknown"`. Read it from the input row in `_phase_4_decide` and pass it in as `margin_known: bool`.
- `scored.margin_pct < min_margin` → skip, reason `f"margin {m:.1f}% below {min_margin:g}%"`.
- `cap = market_cap_runs(...)`, with `units_per_run = orow.get_product_quantity(row) / batch_runs`. If cap == 0 → watch, reason `f"market full: {pipeline_days:.0f} days of supply already, max {max_days:g}"`.
- `expected_days = (pipeline_units + cap * units_per_run) / velocity`.
- `capital = orow.get_total_cost(row)`. None or ≤ 0 → skip, reason `"not built: batch cost unknown"`.
- `capital_return_per_day = (scored.absolute_profit_per_batch / capital) / max(expected_days, 1.0)`. Runs cancel out, so the full-batch profit and cost give the capped batch's ratio.
- Build reason: `f"Build: margin {m:.1f}%, {expected_days:.0f} days to sell, {100 * crpd:.2f}%/day on capital"`.

- [ ] **Step 1: Write the failing tests** using the fixtures in `tests/test_daily_planner_decisions.py`:

```python
def test_margin_below_10_is_skipped():          # margin_pct=6 -> decision "skip", reason "margin 6.0% below 10%"
def test_unknown_velocity_not_built():          # velocity_unknown_reason="no sales" -> "skip", reason starts "not built: sell velocity unknown"
def test_unknown_margin_not_built():            # margin_known=False -> "skip", reason "not built: margin unknown" (not "margin 0.0% below")
def test_market_full_is_watched():              # velocity 1/day, pipeline 50 days -> "watch", reason starts "market full"
def test_build_ranks_by_capital_return():       # profit 20M, total_cost 100M, velocity 10/day, 0 pipeline, 300 units in 30 runs
    # cap = min(30, floor(450/10)) = 30; expected_days = 30
    # capital_return_per_day == pytest.approx(0.2 / 30)
def test_low_isk_per_hour_still_builds():       # isk/h 0.4M, margin 15%, sells in 20 days -> "build"
def test_market_cap_runs():
    assert market_cap_runs(batch_runs=30, units_per_run=1, velocity=0.5, pipeline_units=10, max_days=45) == 12
```

- [ ] **Step 2: Run** `pytest tests/test_daily_planner_build_rules.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.** Mark the help text of `planner_min_isk_per_hour` and `planner_min_profit_per_batch` "No longer used by the daily planner." Do the rules, the fields and the settings. In `DailyPlannerService._phase_4_decide`, nothing changes: the engine reads the row and the pipeline it already gets.
- [ ] **Step 4: Update** the old-gate tests in `test_daily_planner_decisions.py` and `test_daily_planner_thresholds.py` to the new reasons, then run the full suite. Expected: PASS.
- [ ] **Step 5: Commit** `feat(planner): build on margin and sell time, rank by capital return`.

### Task 4: Sub-component re-size by parent scale

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/chain_planner.py` (`_sub_decisions` closure, about line 612)
- Test: `tests/test_chain_planner_resize_scale.py`

**Interfaces:**
- Produces: `chain_plan.resize_sub_components(fallen: set[int], dropped=frozenset(), scale: dict[int, float] | None = None)`. `scale[parent_type_id]` in (0, 1] multiplies each request quantity of that parent, rounded up (`math.ceil`) before `_merge_sub_requests`. Parents not in `scale` are unchanged. The existing call sites stay valid.

- [ ] **Step 1: Write the failing test.** Build a chain plan with one parent that requests 100 units of a component (reuse the setup of `tests/test_chain_planner_keying.py`). `resize(set(), scale={parent: 0.3})` returns a sub decision with `overview_row["quantity_requested"] == 30`. `resize(set())` still gives 100.
- [ ] **Step 2: Run.** Expected: FAIL (unexpected keyword `scale`).
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the test and `pytest tests -q -k chain_planner`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(planner): re-size sub-components for a cut parent batch`.

### Task 5: Charge builds to the budget, cutting runs

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/character_assigner.py` (`assign` around lines 86-232; `_assign_decision` manufacture branches around lines 761-842; the sub-component pass)
- Test: `tests/test_daily_planner_budget_assignment.py`

**Interfaces:**
- Consumes: `WalletBudget`, `StockLedger`, `max_affordable_runs` (Task 2); `ItemDecision.market_cap_runs` and `capital_return_per_day` (Task 3); `resize_sub_components(..., scale=)` (Task 4).
- Produces: `CharacterAssigner.assign(..., budget: WalletBudget | None = None, corp_material_stock: dict[int, int] | None = None)`.
  - None means no wallet limit. Only tests use that (`# shortcut: None = unlimited for the 57 existing test calls; the service always passes a budget`).
- Produces: `cut_batch(action: AssignedAction, runs: int, batch_runs: int) -> None` (module function). Sets `action.runs = runs`, and scales `quantity`, every `materials` value (ceil), `estimated_cost_isk`, `estimated_build_cost_isk`, `estimated_profit_isk` and `estimated_job_fee_isk` by `runs / batch_runs`. `None` values stay None.
- Produces: `ItemDecision.overview_row["reserved_sub_pilots"]: list[int]` on sub decisions (one pilot id per reserved job).

Behaviour:
1. Top-level order becomes `-(d.capital_return_per_day or 0)`, with ties broken by `-d.adjusted_score`.
2. For a manufacture action, `target = min(batch_runs, d.market_cap_runs or batch_runs)`.
3. `cost_of(runs)` = fee for those runs + `StockLedger.purchase_cost(scaled materials, price, commit=False)`.
   - Materials come from the same source as the shopping list, so the charge and the list agree: the action's `materials`, or when None, the shopping list's SDE fallback. Expose that fallback as the module function `action_material_needs(action, blueprint_data, product_to_blueprint) -> dict[int, int] | None` in `shopping_list_builder.py`, extracted from `ShoppingListBuilder._material_needs`, which then calls it.
   - Price is `price_map_unit_price(price_map.get(type_id))[0]` (`shopping_list_builder.py`).
   - Both manufacture branches are cut the same way. The owned-BPC "part" branch uses `batch_runs = first["runs"]`, and its None `estimated_cost_isk` stays None.
   - A component that a sub decision builds (`requested_by_type_ids` contains the parent) is priced at that sub's build cost per unit, `(sub_manufacture_cost + sub_install_fee) / quantity_needed`, for the units its reserved jobs cover. The rest is priced at market.
   - Use `max_affordable_runs(cost_of, target, budget.available)`. 0 → no manufacture action; append the reason `f"not built today: wallet ({budget.available:,.0f} ISK left, 1 run needs {cost_of(1):,.0f})"` (or `budget.blocked_reason`), and release the slot.
   - Otherwise `cut_batch`, `purchase_cost(..., commit=True)` and `budget.charge(...)`.
4. Sub-job slots are reserved at the parent's turn. For each sub decision requested by this parent that is not yet reserved, take one free manufacturing slot per job in its `sub_job_batches`. Use a pilot who passes `skill_gate.check(sub bp, "manufacturing")`, take it with `_take_slot`, and record it in `reserved_sub_pilots`. Jobs with no reservable pilot are priced at market for the parent.
5. After the top-level pass, any parent built with fewer runs than its batch goes into `scale`, and sub-components are re-sized with `resize(fell_back, dropped=no_build, scale=scale)`. Reservations are matched by count per sub type_id, not by job index. Reserved pilots beyond the new job count get their slot back (`_release_slot`). A re-size that needs more jobs than were reserved (research-first fallback) gets the extra jobs from `_best_mfg_char` as today, and their components are charged at market when no slot is found.
6. In the sub pass, a sub job uses its reserved pilot instead of `_best_mfg_char`.

- [ ] **Step 1: Write the failing tests** with the `tests/fakes.py` and `planner_skill_fakes.py` helpers used by `tests/test_daily_planner_assignment.py`:

```python
def test_runs_cut_to_budget():                    # 30-run batch at 10M/run, budget 95M -> manufacture runs == 9, budget.available < 10M
def test_one_run_unaffordable_gives_no_manufacture():  # 1 run 200M, budget 100M -> no "manufacture" action; reason contains "not built today: wallet"
def test_order_is_capital_return():               # two products, one slot: the higher capital_return_per_day gets it even with lower adjusted_score
def test_stock_lowers_the_charge():               # materials fully in corp stock -> charge == fee only
def test_sub_component_priced_at_build_cost():    # parent needs 10 components, sub build 1M each vs market 2M -> charge uses 1M x 10
def test_sub_job_without_slot_priced_at_market(): # no free slot left for the sub job -> component charged at 2M, no sub_manufacture action
def test_cut_parent_resizes_subs():               # parent cut 30 -> 9 runs -> sub quantity ceil(9/30 x need)
def test_no_budget_means_old_behaviour():         # budget=None -> same actions as before for an existing fixture
```

- [ ] **Step 2: Run** `pytest tests/test_daily_planner_budget_assignment.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement** behaviour 1–6.
- [ ] **Step 4: Run** the full suite. Expected: PASS.
- [ ] **Step 5: Commit** `feat(planner): charge builds to the corp wallet, cut runs to fit`.

### Task 6: Science first, science fill, and no jobs on an unowned BPO

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/character_assigner.py` (`assign`; extract the ME/TE block of `_assign_decision` into `_research_actions`)
- Modify: `src/eve_online_industry_tracker/application/daily_planner/chain_planner.py` (the `needs_t1_bpo` branch around line 1271)
- Test: `tests/test_daily_planner_science_fill.py`

**Interfaces:**
- Produces: `CharacterAssigner._research_actions(self, *, decision, row, char_slots, characters, now, skill_gate, research_owners) -> list[AssignedAction]`, extracted unchanged from `_assign_decision`, which then calls it.
- Produces: `CharacterAssigner.free_slots_after: dict[int, dict[str, Any]]`, set at the end of `assign`: `{char_id: {"character_name": str, "manufacturing": int, "science": int, "reason": str | None}}`.
- Produces: `science_cost(action: AssignedAction) -> float | None`. For research and copy it is the job fee; for invent it is the fee plus the action's `estimated_cost_isk` (the inputs).

Behaviour:
1. Before the top-level pass: build the science candidates and call `budget.set_aside()` with the sum of `science_cost` of the first N candidates, where N = total free research slots (`free_research`) over all pilots.
   - Candidates are, in capital-return rank order, for decisions with `decision == "build"`:
     - ME/TE research where the row has `needs_me_research` / `needs_te_research`;
     - copies of a free owned T1 BPO for a T2 build row that has no BPC;
     - invention from T1 BPCs that exist now.
   - Only candidates someone can run today count: the blueprint is free in the ledger, and `skill_gate.check(blueprint_type_id, activity)` returns no block (activity `research_material` / `research_time` / `copying` / `invention`). A skill-blocked research, like the capital rigs in plan 26 (no Rigging 4), reserves nothing.
   - Their costs are estimates from the row's fee fields; nothing is assigned yet.
2. Every science action, whether in the build pass or the fill pass, is charged with `budget.charge_science(science_cost(...))`. On False it is dropped, its slot released, and the reason `"no science today: wallet"` appended.
3. After the sub pass: the fill pass. For each candidate whose product got no science action yet, while a pilot has `free_research > 0`, emit the action through the existing helpers (`_research_actions`, `_copy_actions` with `_reserve_copy_pilots`, `_invent_action`). Ledger, skill gate and BPC-exists rules are unchanged.
4. Fill `free_slots_after`. `reason` is `"no eligible job"` when slots are left and the candidates ran out, or `budget.blocked_reason` / `"wallet"` when the budget stopped them.
5. In `chain_planner.py`, where `needs_t1_bpo` is set, also call `self._mark_not_built(decision, f"T1 BPO {t1_bp_type_id} not owned: buy it first (see BPO advice)")`. No research, copy, invention or manufacture follows; `bpo_opportunities` is still filled.

- [ ] **Step 1: Write the failing tests:**

```python
def test_science_set_aside_before_builds():   # wallet 100M, 2 research slots at 1M fee each, builds would take 100M -> both research actions exist, builds use <= 98M
def test_fill_pass_uses_free_science_slots(): # 3 free research slots, 1 build row with research and 2 more build rows with no mfg slot needing ME research -> 3 me_research actions
def test_skill_blocked_research_reserves_nothing(): # needs_me_research but no pilot has the skill -> set_aside total excludes its fee
def test_free_slots_reason_no_eligible_job(): # 2 research slots, 0 candidates -> free_slots_after[c]["science"] == 2 and reason "no eligible job"
def test_unowned_t1_bpo_gets_no_jobs():       # T2 row needs_t1_bpo -> no invent/copy/manufacture actions; reason contains "not owned: buy it first"
```

- [ ] **Step 2: Run** `pytest tests/test_daily_planner_science_fill.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement** behaviour 1–5.
- [ ] **Step 4: Run** the full suite. Expected: PASS.
- [ ] **Step 5: Commit** `feat(planner): science jobs first, fill free science slots, skip unowned BPOs`.

### Task 7: Service wiring and the wallet invariant

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py` (`_get_corp_wallet` → also return `wallets_refreshed_at`; `_phase_1_collect`; `_phase_6_assign`; `_phase_9_persist` plan summary)
- Test: `tests/test_daily_planner_wallet_invariant.py`

**Interfaces:**
- Consumes: `wallet_stale_reason` (Task 1), `WalletBudget` (Task 2), `assign(budget=, corp_material_stock=)` (Task 5), `free_slots_after` (Task 6).
- Produces: phase 1 begins with a wallet refresh. `self._corporations.refresh_wallet_balances()` runs inside `_ComputeStep(self, "refreshing the corp wallet")`. `CorporationRefreshError` and `ESI_REQUEST_ERRORS` are caught and logged at WARNING; the stored balance is used, and its age decides (no crash, no silent fresh balance).
- Produces: phase 1 keys `"corp_wallet_refreshed_at"` and `"wallet_budget": WalletBudget`. The budget uses the `planner_wallet_reserve_pct` reserve and gets `blocked_reason=wallet_stale_reason(...)` with the `wallet_max_age_hours` setting.
- Produces: plan summary (`plan_summary_json`) keys:
  - `"wallet": {"balance", "refreshed_at", "age_hours", "reserve_pct", "budget", "spent", "blocked_reason"}`;
  - `"free_slots"`: `free_slots_after`, keyed by character name.

- [ ] **Step 1: Write the failing tests** with the service fakes of `tests/test_daily_planner_service*.py`:

```python
def test_plan_never_exceeds_budget():
    # wallet 100M, reserve 10%, overview rows worth 500M of builds.
    # compute_capital_reserved(actions) (streamlit_ui/components/daily_planner/status_bar.py, the status bar's own sum):
    # .total <= 90M, .unpriced_purchases == 0, .unknown_fee_jobs == 0
def test_compute_refreshes_wallet_first():
    # fake corporations service records refresh_wallet_balances() before list_corporations()
def test_wallet_refresh_failure_uses_stored_balance():
    # refresh raises CorporationRefreshError -> compute succeeds; stored balance with its age is used
def test_service_passes_stale_wallet_as_blocked():
    # wallets_refreshed_at 7 h ago -> no manufacture/research/copy/invent actions;
    # summary["wallet"]["blocked_reason"] starts "corp wallet last read"
def test_unknown_wallet_plans_no_paid_jobs():
    # _get_corp_wallet -> None: deliver actions only
```

- [ ] **Step 2: Run.** Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the full suite. Expected: PASS.
- [ ] **Step 5: Commit** `feat(planner): plan within the corp wallet budget`.

### Task 8: UI — wallet age, budget, free slots, new columns

**Files:**
- Modify: `src/streamlit_ui/components/daily_planner/status_bar.py` (`wallet_snapshot` area, line 282; metrics around line 590)
- Modify: `src/streamlit_ui/pages/corporations.py` (`_master_wallet_balance_and_reason`, line 37)
- Test: `tests/test_daily_planner_status_bar_wallet.py`

**Interfaces:**
- Consumes: the plan summary `"wallet"` / `"free_slots"` (Task 7), `wallet_age_hours` / `wallet_stale_reason` (Task 1).
- Produces: `wallet_caption(summary_wallet: dict | None) -> str | None` in `status_bar.py`. Returns `"read {age:.1f} h ago · budget {budget:,.0f} ISK after {reserve:g}% reserve"`, prefixed with `"⚠ "` plus the blocked reason when the budget is blocked. Returns None for an old plan without the key.

Behaviour:
- Status bar "Corp Wallet" metric gets the caption.
- A "Free slots" line per character, e.g. `"Duke X1: 2 mfg, 1 science free (no eligible job)"`, shown only when something is free.
- Only the status bar shows it: it sits above all four tabs.
- The corporations page shows the master wallet's age, with `st.warning` when `wallet_stale_reason` is set.
- BPO purchase advice needs nothing new: the shopping tab's existing `remaining_after_shopping` / `budget_fit` already show what the wallet has left after the plan and whether each BPO fits.

- [ ] **Step 1: Write the failing tests:**

```python
def test_wallet_caption_fresh():
    assert wallet_caption({"age_hours": 1.5, "budget": 766_390_384, "reserve_pct": 10, "blocked_reason": None}) == "read 1.5 h ago · budget 766,390,384 ISK after 10% reserve"
def test_wallet_caption_blocked():
    assert wallet_caption({..., "blocked_reason": "corp wallet last read 7.0 h ago (limit 6 h)"}).startswith("⚠ corp wallet last read")
def test_wallet_caption_old_plan():
    assert wallet_caption(None) is None
```

- [ ] **Step 2: Run.** Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the full suite. Then start the app, recompute the plan, and check the status bar, the shopping total ≤ 90% of the wallet, and free slots. Expected: PASS, and on screen what is described above.
- [ ] **Step 5: Commit** `feat(ui): show corp wallet age, plan budget and free slots`.

### Task 9: Blueprint stock pass for leftover science slots

Spec: section 7 of the design doc.

**Files:**
- Create: `src/eve_online_industry_tracker/application/daily_planner/blueprint_stock.py`
- Modify: `src/eve_online_industry_tracker/application/daily_planner/character_assigner.py` (`assign`: a stock pass after `_fill_science`, before `free_slots_after` is filled)
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py` (`_phase_6_assign` passes the stock inputs)
- Modify: `src/eve_online_industry_tracker/config/admin_settings.py` (two settings)
- Test: `tests/test_daily_planner_blueprint_stock.py`

**Interfaces:**
- Settings (daily_planner section, same dict shape as the others): `planner_min_bpc_stock`: int, default 3, min 0, max 50, label "Minimum BPCs in stock per owned BPO"; `planner_min_invented_bpc_stock`: int, default 3, min 0, max 50, label "Minimum invented BPCs in stock per inventable T2".
- Produces in `blueprint_stock.py`:
  - `@dataclass(frozen=True) class StockGap: kind: str  # "copy" | "invent"; blueprint_type_id: int  # the BPO to copy, or the T2 blueprint to invent; source_blueprint_type_id: int | None  # T1 blueprint for "invent"; have: float; want: int; product_type_id: int | None`
  - `count_in_progress(industry_jobs, invention_probability: Callable[[int], float | None]) -> dict[int, float]`: per blueprint type_id that will gain BPCs, the BPCs on their way. A copy job (activity_id 5, not delivered) adds its `runs` (copies) to its `blueprint_type_id`. An invention job (activity_id 8) adds `runs × probability(product_type_id)` to its `product_type_id` (the invented blueprint), or `runs` when the probability is None.
  - `stock_gaps(*, bpo_type_ids: Iterable[int], bpc_counts: dict[int, int], in_progress: dict[int, float], invention_source_index: dict[int, int], min_bpc: int, min_invented: int, product_of: Callable[[int], int | None]) -> list[StockGap]`: a "copy" gap per owned BPO type with `bpc_counts + in_progress < min_bpc`. An "invent" gap per T2 blueprint whose source (via `invention_source_index`, {T2 bp: T1 bp}) is an owned BPO type and with `bpc_counts + in_progress < min_invented`. `product_of(bp)` is the manufactured product (via the SDE).
  - `rank_gaps(gaps, decision_by_product: dict[int, ItemDecision]) -> list[StockGap]`: rows that are "build" decisions first, by `capital_return_per_day` descending. Then other rows by `margin_pct` descending. Then gaps with no decision or no margin. Stable within ties.
- Consumes from earlier tasks: `WalletBudget.charge` (not `charge_science`), `science_cost(action)`, `_reserve_copy_pilots` / `_copy_actions` / `_invent_action` / `_invention_results`, the ledger, and `free_slots_after` (it is still filled after this pass, so the "no eligible job" / "wallet" reasons include stock jobs).
- `assign(..., blueprint_stock: dict[str, Any] | None = None)`: None skips the pass (existing tests). The service passes `{"bpo_assets_by_type_id", "bpc_assets_by_type_id", "busy_blueprint_item_ids", "min_bpc", "min_invented"}` from phase 1 and the two settings.

Behaviour:
1. After the science fill pass, while any pilot has `free_research > 0`, walk the ranked gaps. For a "copy" gap, emit copy jobs on a free owned BPO of that type (not busy, not claimed in the ledger), with copies sized as the planner already sizes copies of that blueprint, up to `want - have` copies. For an "invent" gap, emit invent jobs from free T1 BPCs of the source type that exist now (any free one, even one that brings T1 stock below its minimum), with the decryptor and probability the product overview row of the T2 product gives, up to the attempts needed for `want - have` expected successes.
2. A gap whose product has no overview row, or whose fee, inputs or probability are unknown, is skipped with a logged reason, never planned as free.
3. Each job is charged with `budget.charge(science_cost(action))`. On False it is dropped and its slot released, and the pilot's science reason becomes `budget.blocked_reason or "wallet"`.
4. Notes on the actions say why they exist: `f"stock: {have:g} of {want} BPCs"`.

- [ ] **Step 1: Write the failing tests**

```python
def test_count_in_progress():        # copy job runs=2 on bp 10 -> {10: 2}; invent runs=4 on product 20 with p=0.5 -> {20: 2.0}; p None -> 4
def test_copy_gap_below_minimum():   # BPO 10 owned, 1 BPC, 1 in progress, min 3 -> one copy gap have=2 want=3
def test_no_gap_at_minimum():        # 3 BPCs -> no gap
def test_invent_gap_from_owned_t1(): # BPO 10 owned, T2 bp 30 sourced from 10, 0 BPCs -> invent gap source=10 want=3
def test_rank_build_rows_first():    # build row (crpd 0.01) > skip row margin 40% > no decision
def test_stock_pass_fills_free_slots():  # 2 free research slots, one copy gap -> copy action with note "stock: 1 of 3 BPCs"
def test_stock_pass_respects_wallet():   # budget 0 left after builds -> no stock job, science reason "wallet"
def test_stock_pass_skips_unknown_fee(): # copy fee unknown -> no action, no crash
def test_invent_uses_t1_below_minimum(): # 1 T1 BPC, T1 min 3, T2 gap -> invent action uses that BPC (option B)
```

- [ ] **Step 2: Run** `pytest tests/test_daily_planner_blueprint_stock.py -v`. Expected: FAIL (ImportError).
- [ ] **Step 3: Implement** the module, the pass, the service wiring and the settings.
- [ ] **Step 4: Run** the full suite. Expected: PASS.
- [ ] **Step 5: Commit** `feat(planner): keep a minimum blueprint stock with leftover science slots`.
