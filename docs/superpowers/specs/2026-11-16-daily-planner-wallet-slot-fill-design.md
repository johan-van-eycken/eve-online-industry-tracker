# Daily Planner: fill every slot within the corp wallet

Date: 2026-11-16
Status: approved 2026-11-16

## Why

Plan 26 (2026-11-16) showed three problems:

- It reserved more capital than the corp holds: 851M ISK in the wallet against
  3.7B of purchases and 4.4B of build cost. The wallet is read in phase 1 but
  only stored for display (`corp_wallet_snapshot`); no phase limits the plan by it.
- It left most slots empty: 7 manufacture jobs and no research, copy or
  invention jobs. Only products above `planner_min_isk_per_hour` (2M) are built,
  and science jobs are only proposed as part of a build.
- The 2M ISK/hour gate rejects almost everything that made money. A historical
  analysis of 1,033 corp sales that trace fully to a build job (sub-job slot
  hours included) found realized ISK per slot-hour of 0.2–2.5M for nearly every
  profitable product. The planner's own estimates are in the same range, so the
  scoring is consistent and the gate is what is wrong.

The same analysis showed that time to sell, not slot time, is what separates
good products from bad ones. Median time from job completion to sale ranged
from under a day to about 200 days. Return on capital per day ranged from over
30% (Standup Heavy Missile) to under 0.1%. The user's goal: **the faster ISK
returns to the corp wallet with profit, the better for scaling manufacturing.**

## Goals

1. Every character's free manufacturing, research, copy and invention slots get
   a job, as far as there is an eligible job to give them.
2. The plan's capital need (purchases net of corp stock, plus job fees) never
   exceeds 90% of the corp wallet balance at plan time: 10% stays in reserve
   (the user's decision; new setting `planner_wallet_reserve_pct`, default 10).
3. Build decisions follow the new rules below, which favour fast capital return.

## Non-goals

- No change to how the product overview prices a batch, or to the scorer's
  multipliers.
- No change to the pause rule (active jobs + declining market) or the
  competition-index watch rule.
- No re-planning during the day; the plan is still computed on demand.

## Anti-goals

- A plan that looks full but cannot be executed: a job the wallet cannot pay
  for, or one that needs a blueprint already in a job, is worse than an empty
  slot.
- Unknown values read as good. An unknown sell velocity, cost or fee must never
  rank a product higher or make it look affordable.

## New build rules (the user approved these)

Replace the `planner_min_isk_per_hour` and `planner_min_profit_per_batch` gates
and the "pipeline ≥ 3 days → watch" rule with:

- **Margin gate:** build only if margin after fees ≥ 10%
  (new setting `planner_min_margin_pct`, default 10).
- **Sell-time gate:** build only if expected days to sell ≤ 45
  (new setting `planner_max_days_to_sell`, default 45).
  Expected days to sell = (stock + in production + on market + this batch) ÷
  effective sell velocity per day. That is the existing
  `PipelineState.total_pipeline_days` plus this batch. When velocity is unknown
  (`velocity_unknown_reason` is set and velocity is the 0.01 floor), the product
  is not built, and the reason says so.
- **Ranking:** return on capital per day = batch profit ÷ (batch capital ×
  max(expected days to sell, 1)). This replaces `adjusted_score` as the build
  order. ISK/hour is kept only to pick the character and slot.
  - *Batch profit* is the raw `absolute_profit_per_batch`, without the
    scorer's multipliers (designer's call: `pipeline_saturation` and the
    velocity multiplier already penalise a slow market, and the days-to-sell
    divisor would count it twice).
  - *Batch capital* is the full market cost of the batch's inputs plus its
    install fee, not net of stock (designer's call: net-of-stock capital
    depends on assignment order, and fully stocked items would rank near
    infinity). Both are for the market-capped batch of section 2.
  - The rank is fixed in phase 4. A wallet cut in phase 6 shrinks a batch
    but does not move it in the order.

The old settings stay readable, so an old `admin_settings.json` still loads,
but the decision engine no longer reads them.

## Approaches considered

For goal 2 when the wallet cannot pay for every slot (the user picked A):

- **A. Smaller batches (picked).** Fill slots in ranking order; when a batch's
  capital exceeds what is left, cut its runs to what is affordable and resize
  its sub-components. Every slot can still get a job, the jobs are shorter, and
  capital comes back sooner. That matches the user's goal.
- **B. Full batches.** Keep batch sizes and fill in ranking order until the
  wallet runs out. Simpler, but manufacturing slots stay empty.
- **C. Trim afterwards.** Build the plan as today, then drop the lowest-ranked
  actions until the shopping list fits. Rejected: freed slots are never
  refilled, and dropping a parent leaves its sub-jobs orphaned.

## Design

### 1. Decision rules (phase 4, `ItemDecisionEngine`)

The rules above. Each `ItemDecision` gets two new fields: `expected_days_to_sell`
and `capital_return_per_day`, used for the build order and written into the
build reason (no new columns: the reason is already stored and shown). A skip's reason names the rule
it failed, with the numbers ("margin 6% below 10%", "expected 80 days to sell
> 45").

### 2. Batch size by market (phase 4/5)

A batch is first capped so that expected days to sell stays ≤ 45: the runs
that the market absorbs in 45 days minus stock, in production and on market,
and never more than the overview's batch or the blueprint's runs. Sub-components
are sized from the capped batch through the existing
`resize_sub_components` path.

### 3. Wallet budget (phase 6, `CharacterAssigner`)

The assigner already walks top-level builds in rank order and then their
sub-components. It gets a running budget that starts at the corp wallet
balance minus the reserve:

- **Capital of a manufacture job** = market cost of the inputs it must buy after
  the corp stock left by higher-ranked jobs, plus its install fee. Stock is
  consumed in assignment order: a running copy of `corp_material_stock`.
- If the capital exceeds what is left, the runs are cut to the most the budget
  allows (at least 1), and the sub-components are resized. If even 1 run does
  not fit, the product gets no job today, and its reason says
  "not built today: wallet (X ISK left, 1 run needs Y)".
- Sub-components (the user's decision): a parent books its whole chain at its
  own turn, each component at the cheapest of what the chain planner already
  compares (`ChainPlanner._resolve_sub_manufacture`): corp stock first (already paid, 0 ISK
  from the wallet), then building (its own inputs to buy plus its fee) or
  buying at market, whichever is less. A component request merged over
  several parents is booked by each parent for its own share.
- So that a component booked as "build" is really built, the parent reserves
  the manufacturing slots of its sub-jobs at its own turn, on pilots with the
  skills. A sub-job that cannot get a reserved slot is booked as bought from
  the start. That way no cost is charged late and the wallet is never
  exceeded (designer's call; it replaces a late "buy instead and cut the
  parent" fallback, which would have re-sized the chain a second time).
- Pause decisions get no new job today (only their deliveries,
  `character_assigner.py` `_assign_decision`), so they take no budget.
- Research and copy jobs cost their fee; invention costs its fee plus the
  datacores and decryptors to buy. They take budget the same way. They are
  cheap, so they are charged but never shrunk.
- An unknown price or fee counts as unaffordable for that job (anti-goal: unknown
  is never cheap), and the reason says which input was unpriced.

The phase-7 shopping list stays the one place that computes purchases, so the
total it shows is the number checked against the wallet. A test asserts that
for every plan the shopping total plus fees ≤ the wallet.

**Science first (the user's decision).** Before the build pass, the assigner
sets aside what the science candidates of section 4 cost for every free
research, copy and invention slot: fees plus the invention inputs to buy. The
build pass spends only what is left. The science pass then spends the set-aside
part. Whatever of it goes unused stays in the wallet today rather than going
back to builds (designer's call: the amount is small, and giving it back would
mean running the build pass twice).

### 4. Filling science slots (phase 6)

After the build pass, the research, copy and invention slots still free are
filled from candidates in ranking order (same capital-return ranking, of the
product the job serves). Candidates:

- **Research:** ME/TE research on owned, free BPOs of products that pass the
  rules, whether or not they were built today (a BPO built from today is not
  free; a BPO in a job is not free, which the item ID fix of
  `feature/onderzoeksjob-blueprint-id` makes reliable).
- **Copy:** copies of owned, free BPOs, for T1 products that pass the rules
  (BPCs to build from later) and for T1 sources of T2 products that pass the
  rules (BPCs to invent from).
- **Invention:** inventions for T2 products that pass the rules, from T1 BPCs
  that exist now. A copy job in this plan is not a source: its BPC does not
  exist until the job is delivered (the current guard stays).

The existing skill gate and blueprint ledger apply unchanged: one job per
blueprint, and only a pilot with the skills gets it. When candidates run out
before slots do, the plan says so per character ("2 research slots free: no
eligible blueprint").

### 5. Status bar

"Capital reserved" stays as it is; it is now ≤ the wallet by construction. The
status bar adds "free slots left" per character and activity, so an unfilled
slot is visible with its reason.

### 6. Wallet age warning (the whole app, same branch)

Today a failed corp wallet refresh (expired token, missing
`esi-wallet.read_corporation_wallets.v1` scope or Accountant/Director role,
ESI down) raises in `Corporation.refresh_corporation`, but the last stored
balance stays and is shown and planned with as if it were current.

- Store when the wallet balances were last read successfully
  (`corporations.wallets_refreshed_at`, additive nullable column), set only
  when the ESI wallet call succeeds.
- The daily planner status bar (above all its tabs) and the corporation
  page show the wallet's age, and a warning when it is older than
  `wallet_max_age_hours` (new setting, default 6).
- The daily planner refreshes the corp wallet balances itself at the start of
  every compute (one ESI call, a new wallet-only refresh). If that refresh
  fails, it goes on with the stored balance, and the age rule decides.
- The daily planner records the balance's age on the plan
  (`plan_summary_json`) and treats a balance older than that limit like an
  unknown one: no paid job, and the status bar says the wallet is stale and
  since when.

## Constraints

- The planner's existing rules: fail loud on contract violations, unknown is
  never 0, one job per blueprint, skill gate on every job.
- The plan reads the corp wallet snapshot of phase 1. When it is unknown
  (`None`), zero or negative, no job that costs ISK is planned (manufacture,
  research, copy, invention all have a fee), and the status bar says why:
  no budget is not an infinite budget. Deliveries are still listed.
- A product that needs a blueprint the corp does not own (a new BPO must be
  bought, e.g. `needs_t1_bpo` for a T2 product without its T1 BPO) gets no job
  in this plan: no manufacture, research, copy or invention, and its reason
  says which BPO is missing (the user's decision). The BPO purchase advice
  (`buy_bpo` rows) is still listed, outside the budget, next to the wallet
  left after the plan.
- Existing tests must keep passing; tests that assert the old gates change
  with the rules.

## Left to the builder

- Where the market cap on batch size (section 2) is computed: decision engine
  or chain planner, whichever has the velocity and the blueprint's run limit.
- How research and copy run counts are chosen for science candidates
  (e.g. copy runs for 45 days of the product's sales).
- Exact UI wording.
