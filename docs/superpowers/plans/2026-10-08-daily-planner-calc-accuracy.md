# Daily Planner Calculation Accuracy (Round 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every OPEN code item of the 2026-10-08 audit and make invention modelling match EVE. That means an invented BPC at ME 2 / TE 4 plus the best decryptor per item, an invention chance from the corp's best inventor, partly owned batches sized as two jobs, batches 2..N sized at the invented ME, and the shopping list and Capital Reserved built on exact stock and fees.

**Architecture:** A new pure module, `application/industry/invention_model.py`, owns every invention rule: invented ME/TE and runs, the skill factor, the best inventor, the decryptor options and the one named selection function `select_best_decryptor`. The producer builds one `InventionContext` per overview refresh and threads it through the top-level row and every nested sub-build. The decryptor is chosen once per invented blueprint, on one fully invented batch of that blueprint's own row, and memoized on the planning context. Batches 2..N come from one extra-batch plan, with no owned BPC runs and no stock. That plan replaces the nested per-extra-batch machinery of rounds 3/4. The shopping list nets against the hangar stock the producer publishes. The daily planner gets job fees on its actions, the item-specific inventor ranking and decryptor buys.

**Tech Stack:** Python 3.10–3.12, SQLAlchemy 2.x, SQLite (SDE read-only), pytest, Streamlit.

**Spec:** `docs/superpowers/specs/2026-09-17-daily-planner-remediation-design.md` (binding; its "do not modify the producer" line is lifted by the user for this round). Inputs: `.superpowers/sdd/2026-10-02-daily-planner-follow-ups/open-items-audit.md` (the 24 OPEN code items, IDs C1–C9, V1–V5, T1–T10) and `progress.md` in the same folder ("Round 5" user decisions D1–D4).

**Plan line numbers are approximate (as of `a546916`). Read the current code before every edit. Each task changes code that earlier tasks also touched.**

## SDE and code verification (2026-10-08, read-only)

This was checked against `database/eve_sde.db` and the code at `a546916`. Every claim below was queried, not assumed.

- **Decryptor attributes.** `dogmaAttributes` defines them as: 1112 `inventionPropabilityMultiplier` (the SDE spells it that way), 1113 `inventionMEModifier`, 1114 `inventionTEModifier`, 1124 `inventionMaxRunModifier`. The values sit in `typeDogma.dogmaAttributes`, a JSON list of `{attributeID, value}`.
- **T2 decryptors.** These are the 8 published types of group 1304 "Generic Decryptor" (market group 1873): 34201 Accelerant (×1.2, ME +2, TE +10, runs +1), 34202 Attainment (×1.8, −1, +4, +4), 34203 Augmentation (×0.6, −2, +2, +9), 34204 Parity (×1.5, +1, −2, +3), 34205 Process (×1.1, +3, +6, 0), 34206 Symmetry (×1.0, +1, +8, +2), 34207 Optimized Attainment (×1.9, +1, −2, +2), 34208 Optimized Augmentation (×0.9, +2, 0, +7).
- **Groups that are not T2 decryptors.** The racial "Decryptors - Amarr/…" groups 728–731 have no market group. Groups 732–735 are relic data items, 979 holds the subsystem data interfaces, and 367776 is unpublished.
- **The existing loader is reused.** `infrastructure/sde/decryptors.py:get_t2_invention_decryptors` already reads exactly group 1304 with these four attributes. Nothing calls it today.
- **Invention skills.** They are in `blueprints.activities.invention.skills`. Of the 1022 published invention blueprints:
  - 920 list 2 science skills and 1 encryption skill;
  - 55 add Capital Ship Construction (group 268, Production);
  - 40 add Outpost Construction (268);
  - 6 list 2 science skills and no encryption skill (the "Exotic Plasma S" family);
  - 1 lists none (Coalesced Element).
- **Encryption skills.** These are the 7 published types in group 270 "Science" whose English name ends in "Encryption Methods": 3408, 21790, 21791, 23087, 23121, 52308, 55025.
- **Rule used:** encryption = the entry in that set; science = the other group-270 entries.
- **Rejected alternative.** The datacores' `requiredSkill1` (attr 182) was rejected as the science-skill source. 8 blueprints list a datacore whose skill differs from the required science skill, for example Multispectrum Coating I: skill Nanite Engineering, datacore Molecular Engineering.
- **Chance formula.** chance = base × (1 + (science₁ + science₂)/30 + encryption/40) × decryptor multiplier. This is EVE's formula since Crius (2014). The SDE stores only the base (`products[].probability`, 0.30 for ships, 0.34 for most modules), so only the skill identification above is verifiable against the SDE, not the formula itself.
- **Character skills.** `characters/character.py` `refresh_skills` writes `skills.skills` as every published SDE skill with `skill_id`, `skill_name` (in the DB language), `group_id` and `trained_skill_level` (0 when not injected). A loaded list therefore names every skill. A skill that is absent from a loaded list is level 0, and an empty or missing list means "not fetched", which is unknown.
- **Partly owned top-level batches are unreachable today.** `_iter_product_variants` sets `effective_runs` to the variant's own copy runs (`maximize_bp_runs`) or 1, and `_build_single_product_row` sets the BPC run map to the same copy runs. So a top-level overview batch is never partly owned. A nested sub-build can be (an owned nested BPC with fewer runs than needed, service.py:4176-4185), and today it builds only the owned runs and buys the rest. Task 4 therefore applies D3 where it is reachable (nested) and keeps the top-level path exact for direct callers.

## Global Constraints

- Python `>=3.10,<3.13`.
- `from __future__ import annotations` in new modules.
- No new third-party dependencies.
- The producer `src/eve_online_industry_tracker/application/industry/service.py` MAY be modified. The user lifted that constraint for this round.
- Migrations are additive and idempotent via `_ensure_column(db, table=, column=, ddl_type=)` in `infrastructure/schema_migrations.py`.
- The repo is PUBLIC: no real corp or character data, wallet values or live-DB counts in code, comments or tests. SDE data is public game data and may be quoted. `tests/test_no_live_values_in_repo.py` guards this.
- Runner: `.venv312/bin/python -m pytest`. The baseline is 869 passed.
- `tests/test_daily_planner_fail_loud.py` is an AST guard and must stay green. It allows no broad `except` in `application/daily_planner/` beyond the one in `service.py`. It bans phantom row-key literals such as `"estimated_material_cost"` and `"market_price"` in planner code, and every planner `_adm` key must exist in the admin schema.
- Branch principle: a missing input skips with a reason, logs a WARNING, or fails loud. Never fall back to a constant. Never under-buy; over-buy is acceptable only when data is incomplete.
- The untracked `logs/` directory is not ours. Never `git add` it. Stage files by name only.
- `*.md` is gitignored repo-wide. This plan is committed by the controller with `git add -f`.
- Every commit message ends with:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`

## Review Focus

1. **An invented item when the SDE yields no decryptor data** (no SDE session, an empty group 1304, malformed dogma rows). Expected: the item is planned with no decryptor and carries a `invention_decryptor.reason` that says why. One WARNING per refresh. The row is never dropped and no decryptor is guessed. Pinned in Task 1 (`test_without_an_sde_session_there_are_no_decryptors_and_a_warning`) and Task 3 (`test_no_decryptor_data_plans_without_a_decryptor_and_says_why`).
2. **A decryptor with no hub market price.** Expected: that decryptor is never chosen (an adjusted-price fallback would make it look cheap), its candidate entry carries `skip_reason`, and the other options are still compared. Pinned in Task 3 (`test_a_decryptor_without_a_market_price_is_never_chosen`).
3. **A character with no science or encryption skill trained, versus a character whose skills are not loaded.** Expected: trained level 0 is real data, so the skill factor is 1.0 with no reason. Skills not loaded means unknown: the base chance, one WARNING, and a `skills_unknown_reason` on the row plus a pricing-confidence reason. Pinned in Task 2 (`test_an_untrained_corp_is_a_known_factor_of_one`, `test_unloaded_skills_fall_back_to_the_base_chance_with_a_reason`).
4. **Runs per BPC changed by a decryptor so the whole-BPC count crosses a boundary** (12 runs at 10 per BPC is 2 BPCs; a +2 decryptor makes it 1). Expected: the whole-attempt buy list follows the decryptor's runs, which here halves the datacores, and adds one decryptor per attempt. Pinned in Task 3 (`test_a_run_modifier_that_crosses_a_bpc_boundary_halves_the_whole_attempts`).
5. **A T2 item whose blueprint has no invention activity** (nothing in the SDE invents it). Expected: no invented ME 2 / TE 4, no decryptor evaluation, no `invention_decryptor` or `invention_chance` keys. An unowned BPC stays ME 0 / TE 0 as today. Pinned in Task 1 (`test_a_t2_blueprint_nothing_invents_is_not_given_the_invented_baseline`) and Task 3 (`test_a_blueprint_without_invention_gets_no_decryptor_payload`).

---

## File Structure

**Created:**

| Path | Responsibility |
|---|---|
| `src/eve_online_industry_tracker/application/industry/invention_model.py` | EVE invention rules and the decryptor selection criterion. Pure and fully unit-tested. |
| `src/eve_online_industry_tracker/infrastructure/sde/skills.py` | `get_encryption_skill_type_ids(sde_session)`. |
| `tests/test_invention_model.py` | Unit tests of `invention_model`. |
| `tests/test_sde_invention_data.py` | SDE loaders for decryptors and encryption skills: in-memory, plus a skip-if-absent pin on the local SDE. |
| `tests/test_daily_planner_job_fees.py` | Job fees on planner actions (D4). |

**Modified:**

| Path | Change |
|---|---|
| `application/industry/service.py` | Invented ME/TE, decryptor data and pricing, invention chance, best decryptor, two-job batches, extra-batch plan, nested-plan hygiene (C5–C7), published stock, invention fees per attempt, V2/V3 |
| `application/industry/shopping_list.py` | Extra-batch material list (C1), published-stock netting (C8, C9) |
| `application/industry/overview_row.py` | Accessors for fees, invention chance, decryptor, job segments |
| `config/admin_settings.py` | `industry.invention_decryptor_selection_enabled` |
| `application/daily_planner/character_assigner.py` | Item-specific inventor ranking (D2), decryptor on invent actions (D1), job fees (D4), research comment (T7) |
| `application/daily_planner/chain_planner.py` | Sub-job fees (D4), one warning for missing runs (V4), shared lowest-id index builder (V5, T10) |
| `application/daily_planner/shopping_list_builder.py` | Decryptor bought per invent attempt |
| `application/daily_planner/models.py` | `AssignedAction.decryptor_type_id/decryptor_type_name/estimated_job_fee_isk`; velocity comment (T3) |
| `application/daily_planner/action_plan_builder.py` | Persist `estimated_job_fee_isk` |
| `application/daily_planner/service.py` | `Iterable[int]` type (T10) |
| `application/daily_planner/fixture_export.py` | Function names instead of line refs (T6) |
| `infrastructure/models.py`, `infrastructure/schema_migrations.py` | `daily_action_log.estimated_job_fee_isk` |
| `infrastructure/sde/blueprints.py` | `_me_reduction` docstring (T4) |
| `streamlit_ui/components/daily_planner/status_bar.py` | Capital Reserved with fees and unknowns (D4) |
| `streamlit_ui/state/industry_builder_ui.py`, `streamlit_ui/pages/industry_builder.py` | Decryptor and chance captions; tree label "unknown" (V2) |
| tests | as listed per task |

---

# Part 1: calculation correctness

### Task 1: Invented BPCs at ME 2 / TE 4, decryptor data loaded and priced (C3, C4, FU-81a, FU-47 data)

The producer models an invented BPC at ME 0 / TE 0. That happens at top level (service.py:7266-7284, `blueprint_source_kind = "unowned_blueprint_copy"`) and nested (:4191-4193). EVE's rule is ME 2 / TE 4 plus the decryptor's modifiers. The nested branch also adds a copy job of the T2 blueprint next to the invention (`include_copying_job = missing_blueprint_copy_runs > 0`, :4193; costed at :4340-4367). That is C4: the invention supplies the BPC. A plain unowned T1 BPC with no invention path stays ME 0 / TE 0 and keeps its copy job.

This task also loads the SDE decryptors into the planning context and adds their type ids to the hub price fetch, so they are priced exactly like datacores. Every plan still uses `NO_DECRYPTOR` until Task 3.

**Files:**
- Create: `src/eve_online_industry_tracker/application/industry/invention_model.py`
- Modify: `src/eve_online_industry_tracker/application/industry/service.py`. The changes are:
  - imports and the logger filter (top);
  - `_ProductPlanningContext` (:91-123);
  - a new `_load_decryptor_options`;
  - `_build_planning_context` (:6477-6569);
  - `_build_single_product_row` (blueprint source resolution :7266-7294, and the move of :7356-7358);
  - `_plan_blueprint_chain_for_quantity` (signature and :4170-4205, recursive call :4513);
  - `_build_recursive_prerequisite_plan` (signature and the call at :5091).
- Test: `tests/test_invention_model.py` (create), `tests/test_sde_invention_data.py` (create), `tests/test_industry_service_calculations.py`

**Interfaces:**
- Produces, in `invention_model`:
  - `INVENTED_BASE_ME = 2`, `INVENTED_BASE_TE = 4`
  - `@dataclass(frozen=True) DecryptorOption(type_id: int | None, type_name: str, probability_multiplier: float = 1.0, me_modifier: int = 0, te_modifier: int = 0, max_run_modifier: int = 0)`
  - `NO_DECRYPTOR: DecryptorOption` (`type_id=None`)
  - `invented_blueprint_efficiency(decryptor) -> tuple[int, int]`
  - `invented_runs_per_blueprint(base_runs: int, decryptor) -> int`
  - `decryptor_options_from_rows(rows) -> tuple[DecryptorOption, ...]` (NO_DECRYPTOR first)
  - `quiet_warnings()` (context manager), `warnings_are_quiet() -> bool`, `class QuietWarningsFilter(logging.Filter)`
  - `@dataclass InventionContext(decryptor_for_blueprint: Callable[[int], DecryptorOption])`, extended in Tasks 2 and 3
- Produces, in `IndustryService`:
  - `_load_decryptor_options(self) -> tuple[DecryptorOption, ...]`
  - `_ProductPlanningContext.decryptor_options`, `.invention_context`, `.blueprint_row_by_type_id`
  - `_build_single_product_row(..., decryptor: DecryptorOption = NO_DECRYPTOR)`
  - `_plan_blueprint_chain_for_quantity(..., invention_context: InventionContext)` and `_build_recursive_prerequisite_plan(..., invention_context: InventionContext)`. Both are required keyword parameters, so a missed call site fails loudly.

- [ ] **Step 1: Write the failing unit tests for the model**

```python
# tests/test_invention_model.py
"""EVE invention rules (application/industry/invention_model.py)."""
from __future__ import annotations

import logging

import pytest

from eve_online_industry_tracker.application.industry import invention_model as im


def _option(**overrides):
    base = dict(type_id=34201, type_name="Accelerant Decryptor", probability_multiplier=1.2,
                me_modifier=2, te_modifier=10, max_run_modifier=1)
    base.update(overrides)
    return im.DecryptorOption(**base)


def test_an_invented_bpc_without_a_decryptor_is_me_2_te_4():
    assert im.invented_blueprint_efficiency(im.NO_DECRYPTOR) == (2, 4)


@pytest.mark.parametrize(("me_mod", "te_mod", "expected"), [
    (2, 10, (4, 14)),   # Accelerant
    (-2, 2, (0, 6)),    # Augmentation
    (-1, 4, (1, 8)),    # Attainment
    (3, 6, (5, 10)),    # Process
    (1, -2, (3, 2)),    # Parity
])
def test_decryptor_modifiers_add_to_the_invented_efficiency(me_mod, te_mod, expected):
    assert im.invented_blueprint_efficiency(_option(me_modifier=me_mod, te_modifier=te_mod)) == expected


def test_invented_efficiency_stays_inside_eves_research_range():
    assert im.invented_blueprint_efficiency(_option(me_modifier=20, te_modifier=40)) == (10, 20)
    assert im.invented_blueprint_efficiency(_option(me_modifier=-9, te_modifier=-9)) == (0, 0)


def test_runs_per_invented_bpc_add_the_decryptor_run_modifier():
    assert im.invented_runs_per_blueprint(10, im.NO_DECRYPTOR) == 10
    assert im.invented_runs_per_blueprint(10, _option(max_run_modifier=9)) == 19
    assert im.invented_runs_per_blueprint(1, _option(max_run_modifier=-5)) == 1


def test_options_from_sde_rows_put_no_decryptor_first_then_type_id_order():
    rows = [
        {"type_id": 34203, "type_name": "Augmentation Decryptor", "invention_probability_multiplier": 0.6,
         "invention_me_modifier": -2, "invention_te_modifier": 2, "invention_max_run_modifier": 9},
        {"type_id": 34201, "type_name": "Accelerant Decryptor", "invention_probability_multiplier": 1.2,
         "invention_me_modifier": 2, "invention_te_modifier": 10, "invention_max_run_modifier": 1},
    ]
    options = im.decryptor_options_from_rows(rows)
    assert options[0] is im.NO_DECRYPTOR
    assert [o.type_id for o in options[1:]] == [34201, 34203]
    assert options[2].max_run_modifier == 9 and options[2].probability_multiplier == 0.6


def test_a_malformed_or_non_positive_decryptor_row_is_skipped_with_a_warning(caplog):
    rows = [
        {"type_id": 34201, "type_name": "Broken"},  # no modifiers at all
        {"type_id": 34202, "type_name": "Zero", "invention_probability_multiplier": 0.0,
         "invention_me_modifier": 0, "invention_te_modifier": 0, "invention_max_run_modifier": 0},
    ]
    with caplog.at_level(logging.WARNING):
        assert im.decryptor_options_from_rows(rows) == (im.NO_DECRYPTOR,)
    assert sum("decryptor" in r.getMessage().lower() for r in caplog.records) == 2


def test_quiet_warnings_drops_warnings_but_not_errors(caplog):
    log = logging.getLogger("test_invention_model.quiet")
    log.addFilter(im.QuietWarningsFilter())
    with caplog.at_level(logging.WARNING):
        with im.quiet_warnings():
            assert im.warnings_are_quiet()
            log.warning("hidden")
            log.error("still shown")
        assert not im.warnings_are_quiet()
        log.warning("shown")
    assert [r.getMessage() for r in caplog.records] == ["still shown", "shown"]


def test_the_default_invention_context_plans_without_a_decryptor():
    assert im.InventionContext().decryptor_for_blueprint(9002) is im.NO_DECRYPTOR
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_invention_model.py -v`
Expected: FAIL. Collection error `ModuleNotFoundError: ... invention_model`.

- [ ] **Step 3: Write the module**

```python
# src/eve_online_industry_tracker/application/industry/invention_model.py
"""EVE invention rules shared by the overview producer and the daily planner.

Game rules, not settings. An invented BPC without a decryptor is ME 2 /
TE 4, with the SDE's runs per invented BPC. A decryptor adds its ME, TE and
max-run modifiers and multiplies the success chance. Decryptor values come
from the SDE: dogma attributes 1112 (inventionPropabilityMultiplier), 1113,
1114 and 1124 on group 1304 "Generic Decryptor", read by
infrastructure/sde/decryptors.py:get_t2_invention_decryptors.
"""
from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Mapping

logger = logging.getLogger(__name__)

INVENTED_BASE_ME = 2
INVENTED_BASE_TE = 4
#: EVE's research ceilings; a blueprint's ME/TE never leaves 0..10 / 0..20.
MAX_BLUEPRINT_ME = 10
MAX_BLUEPRINT_TE = 20


@dataclass(frozen=True)
class DecryptorOption:
    """One way to invent: no decryptor (type_id None) or one SDE decryptor."""

    type_id: int | None
    type_name: str
    probability_multiplier: float = 1.0
    me_modifier: int = 0
    te_modifier: int = 0
    max_run_modifier: int = 0


NO_DECRYPTOR = DecryptorOption(type_id=None, type_name="No decryptor")


def invented_blueprint_efficiency(decryptor: DecryptorOption) -> tuple[int, int]:
    """(ME, TE) of a BPC invented with `decryptor`: 2 / 4 plus its modifiers."""
    me = min(MAX_BLUEPRINT_ME, max(0, INVENTED_BASE_ME + int(decryptor.me_modifier)))
    te = min(MAX_BLUEPRINT_TE, max(0, INVENTED_BASE_TE + int(decryptor.te_modifier)))
    return me, te


def invented_runs_per_blueprint(base_runs: int, decryptor: DecryptorOption) -> int:
    """Runs on one invented BPC: the SDE's runs plus the decryptor's, at least 1."""
    return max(1, int(base_runs) + int(decryptor.max_run_modifier))


def decryptor_options_from_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[DecryptorOption, ...]:
    """NO_DECRYPTOR first, then every usable SDE decryptor by type_id.

    `rows` have the get_t2_invention_decryptors shape. A row without the four
    modifiers, or with a non-positive id or success multiplier, is skipped
    with a WARNING: it cannot be costed, and a guessed modifier would change
    the ME, the runs or the odds.
    """
    options: list[DecryptorOption] = []
    for row in rows:
        try:
            option = DecryptorOption(
                type_id=int(row["type_id"]),
                type_name=str(row.get("type_name") or row["type_id"]),
                probability_multiplier=float(row["invention_probability_multiplier"]),
                me_modifier=int(row["invention_me_modifier"]),
                te_modifier=int(row["invention_te_modifier"]),
                max_run_modifier=int(row["invention_max_run_modifier"]),
            )
        except (KeyError, TypeError, ValueError):
            logger.warning("Invention: skipping malformed SDE decryptor row %r", dict(row))
            continue
        if int(option.type_id or 0) <= 0 or option.probability_multiplier <= 0:
            logger.warning(
                "Invention: skipping decryptor %s with a non-positive id or success multiplier",
                option.type_name,
            )
            continue
        options.append(option)
    return (NO_DECRYPTOR, *sorted(options, key=lambda o: int(o.type_id or 0)))


_quiet_state = threading.local()


@contextmanager
def quiet_warnings() -> Iterator[None]:
    """Drop WARNING records on this thread from loggers carrying
    QuietWarningsFilter. This is used while the producer evaluates plans it
    throws away (decryptor candidates, the extra-batch plan), so a missing
    input is logged once, by the plan that is kept. ERROR and above pass."""
    _quiet_state.depth = getattr(_quiet_state, "depth", 0) + 1
    try:
        yield
    finally:
        _quiet_state.depth -= 1


def warnings_are_quiet() -> bool:
    return getattr(_quiet_state, "depth", 0) > 0


class QuietWarningsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not (record.levelno == logging.WARNING and warnings_are_quiet())


logger.addFilter(QuietWarningsFilter())


def _no_decryptor_for(_blueprint_type_id: int) -> DecryptorOption:
    return NO_DECRYPTOR


@dataclass
class InventionContext:
    """Per-refresh invention inputs. One object is shared by the top-level row
    and every nested sub-build of one overview refresh."""

    decryptor_for_blueprint: Callable[[int], DecryptorOption] = _no_decryptor_for
```

- [ ] **Step 4: Run the unit tests**

Run: `.venv312/bin/python -m pytest tests/test_invention_model.py -v`
Expected: PASS (9 tests, counting the parametrized cases as 1).

- [ ] **Step 5: Write the failing SDE-loader and producer tests**

```python
# tests/test_sde_invention_data.py
"""SDE invention data: the T2 decryptors and the encryption skills.

The in-memory tests build only the rows they need. The pin test reads the
local SDE (public game data, read-only) and skips when it is not present."""
from __future__ import annotations

import json
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from eve_online_industry_tracker.infrastructure.models import Groups, Types
from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors

_LOCAL_SDE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "database", "eve_sde.db"))


def _dogma(**attrs):
    return json.dumps([{"attributeID": a, "value": v} for a, v in attrs.items()])


def test_t2_decryptors_are_read_from_type_dogma_of_group_1304_only(sde_engine):
    with sde_engine.begin() as conn:
        conn.execute(text("CREATE TABLE typeDogma (id BIGINT, dogmaAttributes TEXT, dogmaEffects TEXT)"))
        for type_id in (34201, 23179):
            conn.execute(
                text("INSERT INTO typeDogma VALUES (:id, :attrs, '[]')"),
                {"id": type_id, "attrs": json.dumps([
                    {"attributeID": 1112, "value": 1.2}, {"attributeID": 1113, "value": 2.0},
                    {"attributeID": 1114, "value": 10.0}, {"attributeID": 1124, "value": 1.0},
                ])},
            )
    with Session(sde_engine) as session:
        session.add(Groups(id=1304, name={"en": "Generic Decryptor"}, categoryID=35, published=True))
        session.add(Types(id=34201, name={"en": "Accelerant Decryptor"}, groupID=1304, published=True))
        # A racial decryptor group with the same dogma: not a T2 invention decryptor.
        session.add(Types(id=23179, name={"en": "Occult Accelerant"}, groupID=728, published=True))
        session.commit()
        rows = get_t2_invention_decryptors(session, language="en")
    assert rows == [{
        "type_id": 34201, "type_name": "Accelerant Decryptor",
        "invention_probability_multiplier": 1.2, "invention_me_modifier": 2,
        "invention_te_modifier": 10, "invention_max_run_modifier": 1,
    }]


@pytest.mark.skipif(not os.path.exists(_LOCAL_SDE), reason="local SDE database/eve_sde.db not present")
def test_the_local_sde_has_the_eight_generic_decryptors():
    engine = create_engine(f"sqlite:///file:{_LOCAL_SDE}?mode=ro&uri=true")
    try:
        with Session(engine) as session:
            rows = {r["type_id"]: r for r in get_t2_invention_decryptors(session, language="en")}
    finally:
        engine.dispose()
    assert sorted(rows) == list(range(34201, 34209))
    accelerant = rows[34201]
    assert (accelerant["invention_probability_multiplier"], accelerant["invention_me_modifier"],
            accelerant["invention_te_modifier"], accelerant["invention_max_run_modifier"]) == (1.2, 2, 10, 1)
```

Append to `tests/test_industry_service_calculations.py` (add the import at the top of the file, with the other imports):

```python
from eve_online_industry_tracker.application.industry.invention_model import NO_DECRYPTOR, DecryptorOption
```

```python
# --- round 5 Task 1: invented BPC baseline ME 2 / TE 4, decryptor data -------

_ACCELERANT_LIKE = DecryptorOption(
    type_id=34201, type_name="Accelerant Decryptor", probability_multiplier=1.2,
    me_modifier=2, te_modifier=10, max_run_modifier=1,
)


def _planning_context(service, *, maximize_bp_runs=False):
    return service._build_planning_context(
        force_refresh=False, build_from_bpc=True, include_reactions=False, maximize_bp_runs=maximize_bp_runs,
        group_identical_bpcs=True, have_blueprint_source_only=False, market_hub="jita",
        material_price_side="sell", product_price_side="sell", industry_profile_id=None,
        owned_blueprints_scope="all_characters", character_id=1, progress_callback=None,
    )


def test_an_invented_t2_row_is_built_at_me_2_te_4(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[5002]["manufacturing_job"]

    assert mj["blueprint_source_kind"] == "unowned_blueprint_copy"
    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (2, 4)
    assert mj["materials"]["34"]["quantity"] == 98  # ceil(10 runs x 10 x 0.98), was 100
    assert mj["manufacturing_time_seconds"] == 960  # 10 x 100 s x 0.96, was 1000


def test_an_unowned_t1_bpc_without_an_invention_path_stays_me_0(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[6001]["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (0, 0)
    assert mj["materials"]["34"]["quantity"] == 30


def test_a_t2_blueprint_nothing_invents_is_not_given_the_invented_baseline(monkeypatch) -> None:
    rows = _invented_t2_blueprint_rows()[1:]  # the T2 blueprint without its invention source
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True, blueprint_rows=rows)[5002]["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (0, 0)
    assert "invention" not in mj["activity_breakdown"]
    assert mj["materials"]["34"]["quantity"] == 100


def test_a_nested_invented_component_is_built_at_me_2_without_a_copy_job(monkeypatch) -> None:
    rows = _nested_invention_blueprint_rows()
    # A real T2 blueprint has an SDE copy time. The invention supplies the BPC,
    # so no copy of the T2 blueprint may be costed (FU-58).
    rows[1]["copying_job"] = {"time_seconds": 30}
    out = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=rows,
        prices=_NESTED_FAKE_PRICES, profile=_INVENTION_ONLY_PROFILE,
    )
    mj = out[7100]["manufacturing_job"]
    nested = mj["recursive_activity_breakdown"]["manufacturing:5002"]

    assert nested["recommended_action"] == "build"
    assert "copying" not in nested["nested"]
    assert "invention:9002" in nested["nested"]
    # The parent (no invention path, ME 0) needs 10 components; the component
    # job runs on the invented BPC at ME 2: ceil(10 x 10 x 0.98) = 98 Tritanium.
    assert mj["procurement_materials"]["34"]["quantity"] == 98


def test_decryptor_options_are_loaded_and_priced_like_datacores(monkeypatch) -> None:
    service = _invention_service(monkeypatch, prices={**_INVENTION_FAKE_PRICES, 34201: 50.0})
    service._load_decryptor_options = MethodType(  # type: ignore[attr-defined]
        lambda self: (NO_DECRYPTOR, _ACCELERANT_LIKE), service,
    )
    ctx = _planning_context(service)

    assert ctx.decryptor_options == (NO_DECRYPTOR, _ACCELERANT_LIKE)
    assert ctx.material_price_map[34201]["unit_price"] == 50.0
    assert ctx.blueprint_row_by_type_id[9002]["blueprint_type_id"] == 9002
    assert ctx.invention_context.decryptor_for_blueprint(9002) is NO_DECRYPTOR


def test_without_an_sde_session_there_are_no_decryptors_and_a_warning(caplog) -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    service._sessions = None  # type: ignore[attr-defined]
    with caplog.at_level("WARNING"):
        assert service._load_decryptor_options() == (NO_DECRYPTOR,)
    assert any("no decryptor data" in r.getMessage() for r in caplog.records)
```

- [ ] **Step 6: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_sde_invention_data.py tests/test_industry_service_calculations.py -k "invented_t2_row_is_built or unowned_t1_bpc_without or nothing_invents or nested_invented_component or decryptor_options_are_loaded or without_an_sde_session or group_1304 or eight_generic" -v`
Expected: `test_t2_decryptors_are_read_...` and the local-SDE pin PASS already (the loader exists). The six producer tests FAIL:
- the ME/TE asserts get `(0, 0)` and 100 / 1000;
- the nested test finds `"copying"` and 100;
- the context test fails with `AttributeError: ... '_load_decryptor_options'`.

- [ ] **Step 7: Implement in the producer**

7a. In `service.py`, after `from eve_online_industry_tracker.infrastructure.persistence import blueprints_repo`, add:

```python
from eve_online_industry_tracker.application.industry.invention_model import (
    NO_DECRYPTOR,
    DecryptorOption,
    InventionContext,
    QuietWarningsFilter,
    decryptor_options_from_rows,
    invented_blueprint_efficiency,
)
from eve_online_industry_tracker.infrastructure.sde.decryptors import get_t2_invention_decryptors
```

and directly after `logger = logging.getLogger(__name__)`:

```python
# Decryptor candidates and the extra-batch plan are evaluated inside
# invention_model.quiet_warnings(): only the plan that is kept logs.
logger.addFilter(QuietWarningsFilter())
```

7b. In `_ProductPlanningContext`, after the `activity_job_cache` field, add:

```python
    # Invention (round 5): the SDE decryptors (NO_DECRYPTOR first), the
    # refresh's invention inputs, and the blueprint rows by blueprint type id.
    decryptor_options: tuple = (NO_DECRYPTOR,)
    invention_context: InventionContext = dataclasses.field(default_factory=InventionContext)
    blueprint_row_by_type_id: dict = dataclasses.field(default_factory=dict)
```

7c. Add next to `_get_character_trained_skill_levels`:

```python
    def _load_decryptor_options(self) -> tuple[DecryptorOption, ...]:
        """(NO_DECRYPTOR, every T2 invention decryptor in the SDE).

        Without SDE access, or with no decryptor rows: (NO_DECRYPTOR,) and a
        WARNING. Inventions are then planned without a decryptor, never with
        a guessed one.
        """
        if self._sessions is None:
            logger.warning(
                "Invention: no SDE session, so no decryptor data; inventions are planned without a decryptor"
            )
            return (NO_DECRYPTOR,)
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        session = self._sessions.sde_session()
        try:
            rows = get_t2_invention_decryptors(session, language=language)
        finally:
            session.close()
        options = decryptor_options_from_rows(rows)
        if len(options) == 1:
            logger.warning(
                "Invention: the SDE lists no T2 invention decryptors; inventions are planned without a decryptor"
            )
        return options
```

7d. In `_build_planning_context`, replace the `material_price_type_ids = sorted({...})` statement with:

```python
        decryptor_options = self._load_decryptor_options()
        material_price_type_ids = sorted({
            int(entry.get("type_id") or 0)
            for row in blueprint_rows if isinstance(row, dict)
            for job_name in ["manufacturing_job", "reaction_job", "invention_job"]
            for entry in (((row.get(job_name) or {}) if isinstance(row.get(job_name), dict) else {}).get("materials") or [])
            if isinstance(entry, dict) and int(entry.get("type_id") or 0) > 0
        } | {
            # Decryptors are invention inputs: hub-priced like the datacores.
            int(option.type_id) for option in decryptor_options if option.type_id is not None
        })
```

and add these keyword arguments to the `_ProductPlanningContext(...)` call:

```python
            decryptor_options=decryptor_options,
            blueprint_row_by_type_id={
                int(row.get("blueprint_type_id") or 0): row
                for row in blueprint_rows
                if isinstance(row, dict) and int(row.get("blueprint_type_id") or 0) > 0
            },
```

7e. In `_build_single_product_row`:
- Add the keyword parameter `decryptor: DecryptorOption = NO_DECRYPTOR` after `effective_runs: int`.
- Move these three lines, now just under `# BPC allocation + prerequisite handling` (:7356-7358), up to directly before `# Blueprint source resolution (Step 3a)`, and delete the originals:

```python
        invention_source_row = ctx.invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
        invention_job = (invention_source_row or {}).get("invention_job") or {}
        has_top_level_invention_path = invention_source_row and isinstance(invention_job, dict)
```

- Replace the `else:` arm of `if blueprint_copy_asset is not None: ... elif blueprint_original_asset is not None: ...` (now `blueprint_source_kind = "unowned_blueprint_copy"`) with:

```python
            else:
                blueprint_source_kind = "unowned_blueprint_copy"
                if has_top_level_invention_path and not matched_blueprint_originals:
                    # The BPC is invented: ME 2 / TE 4 plus the decryptor
                    # (EVE). A plain unowned T1 BPC stays at ME 0 / TE 0.
                    blueprint_material_efficiency, blueprint_time_efficiency = invented_blueprint_efficiency(decryptor)
```

- In the `self._build_recursive_prerequisite_plan(...)` call, add `invention_context=ctx.invention_context,`.

7f. In `_plan_blueprint_chain_for_quantity`:
- Add the parameter `invention_context: InventionContext,` directly after `invention_row_by_blueprint_type_id: dict[int, dict[str, Any]],`.
- Move these two lines (now :4291-4292) up to directly before `if activity == "manufacturing":` (:4170), and delete the originals:

```python
        invention_source_row = invention_row_by_blueprint_type_id.get(blueprint_type_id) if activity == "manufacturing" else None
        has_invention_path = isinstance((invention_source_row or {}).get("invention_job"), dict)
```

- Replace

```python
                else:
                    blueprint_source_kind = "unowned_blueprint_copy"
                    include_copying_job = missing_blueprint_copy_runs > 0
```

with

```python
                else:
                    blueprint_source_kind = "unowned_blueprint_copy"
                    if has_invention_path:
                        # Invented: ME 2 / TE 4 plus the decryptor, and the
                        # invention supplies the BPC, so no copy job (FU-58).
                        blueprint_material_efficiency, blueprint_time_efficiency = invented_blueprint_efficiency(
                            invention_context.decryptor_for_blueprint(blueprint_type_id)
                        )
                    else:
                        include_copying_job = missing_blueprint_copy_runs > 0
```

- Add `invention_context=invention_context,` to the recursive `self._plan_blueprint_chain_for_quantity(...)` call (:4513).

7g. In `_build_recursive_prerequisite_plan`, add the parameter `invention_context: InventionContext,` after `invention_row_by_blueprint_type_id`. Pass `invention_context=invention_context,` in its `self._plan_blueprint_chain_for_quantity(...)` call.

- [ ] **Step 8: Run the new tests**

Run: the command from Step 6.
Expected: all PASS.

- [ ] **Step 9: Re-pin the existing numbers that the invented baseline moves, deriving each one by hand**

Run: `.venv312/bin/python -m pytest tests/test_industry_service_calculations.py -q`
Expected failures, and the new values:
- `test_invented_t2_invention_cost_scales_with_runs`: `material_cost` 900 → 890 (Tritanium ceil(100 × 0.98) = 98 × 5 = 490, plus 400). Change the comment to `# 10 runs at ME 2: Tritanium 98 x 5 = 490; invention 40 per run x 10 = 400.`
- `test_fees_and_inputs_use_sde_runs_per_bpc_not_the_t2_limit`: `material_cost == pytest.approx(2940 * 5 + 2 * 100 * attempts)` (300 runs × 10 × 0.98 = 2940).
- `test_elapsed_time_uses_whole_invention_jobs`: manufacturing 100 → 96 s (TE 4). Use `mj["time_seconds"] == 96 + 8 + 4` and `mj["elapsed_time_seconds"] == 96 + 80 + 40`, and update the two comments to say "manufacturing 96 (TE 4)".
- `test_nested_t2_component_uses_expected_invention_attempts`: the component now runs at TE 4. Use `mj["time_seconds"] == 200 + 96 + 8 + 4`, `mj["elapsed_time_seconds"] == 200 + 96 + 80 + 40` and `t2["elapsed_time_seconds"] == 96 + 80 + 40`.

If any other test fails, derive its new value by hand with ME 2 / TE 4 and write that derivation in the comment. Never paste the observed number. A failure that ME 2 / TE 4 does not explain is a bug in Step 7: stop and fix it.

- [ ] **Step 10: Run the full suite**

Run: `.venv312/bin/python -m pytest -q`
Expected: all pass (869 + the new tests).

- [ ] **Step 11: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/invention_model.py \
  src/eve_online_industry_tracker/application/industry/service.py \
  tests/test_invention_model.py tests/test_sde_invention_data.py tests/test_industry_service_calculations.py
git commit -m "feat: invented BPCs at ME 2 / TE 4, no copy job for nested invented BPCs, SDE decryptors loaded and priced

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Invention chance from the corp's best inventor (C2, FU-46, D2)

`_expected_invention_costs` uses the bare SDE chance (service.py:6705-6711, floored at :6726). EVE's chance is base × (1 + (science₁ + science₂)/30 + encryption/40) × decryptor multiplier. Per D2 the producer uses, per invented blueprint, the corp character with the highest skill factor for that blueprint's three skills. The admin `invention_probability_floor` stays a floor. The daily planner's `CharacterAssigner._best_invention_char` ranks pilots by a generic "Science/Advanced Industry/Metallurgy/Research" sum (character_assigner.py:32, :490-510). That rule is replaced by the same item factor, so the pilot who gets the invent action is the one the chance assumed. Without the producer's skill ids on the row it falls back to the old sum.

Skills unknown (no character skills loaded, or SDE skills that cannot be identified, see the verification section) means the base chance, one WARNING per reason per refresh, `manufacturing_job.invention_chance.skills_unknown_reason` and a pricing-confidence reason line. Skills are never invented. A loaded skill list where a skill is untrained is real data: level 0.

**Files:**
- Create: `src/eve_online_industry_tracker/infrastructure/sde/skills.py`
- Modify: `application/industry/invention_model.py`. Changes:
  - add `SCIENCE_SKILL_GROUP_ID`, `InventionSkillSet`, `invention_skill_set`, `invention_skill_factor`, `InventorChoice`, `best_inventor`, `InventionChance`, `resolve_invention_chance`, `invention_chance_payload`;
  - extend `InventionContext`.
- Modify: `application/industry/service.py`. Changes:
  - `_get_inventor_skill_levels`, `_load_encryption_skill_type_ids` (new);
  - `_build_planning_context`;
  - `_expected_invention_costs` (:6672-6772);
  - `_plan_nested_invention` (:6838-6945) and its caller (:4745);
  - `_full_batch_invention_whole_attempts` (:6993-7015) and its two callers;
  - `_plan_invention_chain_for_variant` (:7086);
  - row output (`invention_chance`);
  - pricing confidence (:2257-2272).
- Modify: `application/industry/overview_row.py` (`get_invention_skill_set`), `application/daily_planner/character_assigner.py`
- Test: `tests/test_invention_model.py`, `tests/test_sde_invention_data.py`, `tests/test_industry_service_calculations.py`, `tests/test_daily_planner_assignment.py`

**Interfaces:**
- Consumes: Task 1's `InventionContext`, `DecryptorOption`, `NO_DECRYPTOR`.
- Produces, in `invention_model`:
  - `SCIENCE_SKILL_GROUP_ID = 270`
  - `@dataclass(frozen=True) InventionSkillSet(science_skill_type_ids: tuple[int, int], encryption_skill_type_id: int)`
  - `invention_skill_set(skill_entries, encryption_skill_type_ids: frozenset[int]) -> tuple[InventionSkillSet | None, str | None]`
  - `invention_skill_factor(skill_set, levels_by_skill_type_id: Mapping[int, int]) -> float`
  - `@dataclass(frozen=True) InventorChoice(character_id: int | None, skill_factor: float | None, unknown_reason: str | None)`
  - `best_inventor(skill_set, levels_by_character_id) -> InventorChoice`
  - `@dataclass(frozen=True) InventionChance(base_probability, skill_factor, decryptor_multiplier, probability, inventor_character_id, skill_set, skills_unknown_reason)`
  - `resolve_invention_chance(context, *, blueprint_type_id: int, skill_entries, base_probability: float, decryptor: DecryptorOption) -> InventionChance`
  - `invention_chance_payload(chance, effective_probability: float) -> dict`
  - `InventionContext.inventor_skill_levels_by_character_id`, `.encryption_skill_type_ids`, `.warn_once(key, message, *args)`
- Produces, in `infrastructure.sde.skills`: `get_encryption_skill_type_ids(sde_session) -> frozenset[int]`.
- Produces, in `IndustryService`:
  - `_expected_invention_costs(..., decryptor: DecryptorOption, invention_context: InventionContext)`, and its `costs["invention_chance"]: dict`;
  - `_plan_nested_invention(..., decryptor, invention_context)`;
  - `_full_batch_invention_whole_attempts(..., decryptor, invention_context) -> tuple[int, str | None, dict | None]` (third element: the chance payload);
  - `_plan_invention_chain_for_variant(..., decryptor: DecryptorOption)`;
  - row key `manufacturing_job.invention_chance: dict` with the keys of `invention_chance_payload`.
- Produces, in `overview_row`: `get_invention_skill_set(row) -> tuple[tuple[int, int], int] | None`.
- Produces, in `character_assigner`: `skill_levels_by_id(char) -> dict[int, int]`.

- [ ] **Step 1: Write the failing model tests**

Append to `tests/test_invention_model.py`:

```python
# --- Task 2: chance from the best inventor -------------------------------------

_HEP, _CSE, _CALDARI_ENC, _CAP_CONSTRUCTION = 11433, 11454, 21790, 22242
_ENCRYPTION_IDS = frozenset({3408, 21790, 21791, 23087, 23121, 52308, 55025})


def _skill(type_id, group_id=im.SCIENCE_SKILL_GROUP_ID):
    return {"type_id": type_id, "type_name": f"skill {type_id}", "group_id": group_id}


_BANTAM_LIKE = [_skill(_HEP), _skill(_CSE), _skill(_CALDARI_ENC)]


def test_two_science_skills_and_the_encryption_skill_are_identified():
    skill_set, reason = im.invention_skill_set(_BANTAM_LIKE, _ENCRYPTION_IDS)
    assert reason is None
    assert skill_set == im.InventionSkillSet((_HEP, _CSE), _CALDARI_ENC)


def test_a_production_prerequisite_is_not_a_science_skill():
    entries = [*_BANTAM_LIKE, _skill(_CAP_CONSTRUCTION, group_id=268)]
    skill_set, reason = im.invention_skill_set(entries, _ENCRYPTION_IDS)
    assert reason is None and skill_set.science_skill_type_ids == (_HEP, _CSE)


@pytest.mark.parametrize(("entries", "encryption", "fragment"), [
    ([], _ENCRYPTION_IDS, "lists no invention skills"),
    (_BANTAM_LIKE, frozenset(), "no Encryption Methods skills"),
    ([_skill(_HEP), _skill(_CSE)], _ENCRYPTION_IDS, "found 2 + 0"),
    ([{"type_id": _HEP}, {"type_id": _CSE}, {"type_id": _CALDARI_ENC}], _ENCRYPTION_IDS, "found 0 + 1"),
])
def test_unidentifiable_invention_skills_name_the_reason(entries, encryption, fragment):
    skill_set, reason = im.invention_skill_set(entries, encryption)
    assert skill_set is None and fragment in reason


def test_the_skill_factor_is_eves_formula():
    skill_set = im.InventionSkillSet((_HEP, _CSE), _CALDARI_ENC)
    assert im.invention_skill_factor(skill_set, {_HEP: 4, _CSE: 4, _CALDARI_ENC: 4}) == pytest.approx(1 + 8 / 30 + 4 / 40)
    # A skill absent from a loaded list is not injected: level 0.
    assert im.invention_skill_factor(skill_set, {}) == 1.0


def test_the_best_inventor_has_the_highest_factor_and_ties_go_to_the_lowest_id():
    skill_set = im.InventionSkillSet((_HEP, _CSE), _CALDARI_ENC)
    levels = {
        7: {_HEP: 4, _CSE: 4, _CALDARI_ENC: 4},   # 1.3667
        3: {_HEP: 5, _CSE: 5, _CALDARI_ENC: 3},   # 1.4083
        2: {_HEP: 5, _CSE: 5, _CALDARI_ENC: 3},   # tie with 3
    }
    choice = im.best_inventor(skill_set, levels)
    assert choice.character_id == 2
    assert choice.skill_factor == pytest.approx(1 + 10 / 30 + 3 / 40)
    assert im.best_inventor(skill_set, {}).unknown_reason == im.NO_CHARACTER_SKILLS


def _context(levels):
    return im.InventionContext(inventor_skill_levels_by_character_id=levels,
                               encryption_skill_type_ids=_ENCRYPTION_IDS)


def test_the_chance_multiplies_base_skills_and_decryptor_and_caps_at_one():
    context = _context({1: {_HEP: 5, _CSE: 5, _CALDARI_ENC: 5}})
    decryptor = im.DecryptorOption(34202, "Attainment Decryptor", 1.8, -1, 4, 4)
    chance = im.resolve_invention_chance(
        context, blueprint_type_id=9002, skill_entries=_BANTAM_LIKE,
        base_probability=0.34, decryptor=decryptor,
    )
    factor = 1 + 10 / 30 + 5 / 40
    assert chance.skill_factor == pytest.approx(factor)
    assert chance.probability == pytest.approx(0.34 * factor * 1.8)
    assert chance.inventor_character_id == 1 and chance.skills_unknown_reason is None
    capped = im.resolve_invention_chance(
        context, blueprint_type_id=9002, skill_entries=_BANTAM_LIKE,
        base_probability=0.9, decryptor=decryptor,
    )
    assert capped.probability == 1.0


def test_unknown_skills_use_the_base_chance_and_warn_once_per_reason(caplog):
    context = _context({})
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            chance = im.resolve_invention_chance(
                context, blueprint_type_id=9002, skill_entries=_BANTAM_LIKE,
                base_probability=0.34, decryptor=im.NO_DECRYPTOR,
            )
    assert chance.probability == pytest.approx(0.34)
    assert chance.skill_factor is None
    assert chance.skills_unknown_reason == im.NO_CHARACTER_SKILLS
    assert sum(im.NO_CHARACTER_SKILLS in r.getMessage() for r in caplog.records) == 1


def test_a_quiet_evaluation_does_not_use_up_the_one_warning(caplog):
    context = _context({})
    with caplog.at_level(logging.WARNING):
        with im.quiet_warnings():
            im.resolve_invention_chance(context, blueprint_type_id=1, skill_entries=_BANTAM_LIKE,
                                        base_probability=0.3, decryptor=im.NO_DECRYPTOR)
        im.resolve_invention_chance(context, blueprint_type_id=1, skill_entries=_BANTAM_LIKE,
                                    base_probability=0.3, decryptor=im.NO_DECRYPTOR)
    assert sum(im.NO_CHARACTER_SKILLS in r.getMessage() for r in caplog.records) == 1


def test_the_chance_payload_names_skills_and_the_floored_chance():
    context = _context({5: {_HEP: 1}})
    chance = im.resolve_invention_chance(context, blueprint_type_id=9002, skill_entries=_BANTAM_LIKE,
                                         base_probability=0.3, decryptor=im.NO_DECRYPTOR)
    payload = im.invention_chance_payload(chance, effective_probability=0.31)
    assert payload["science_skill_type_ids"] == [_HEP, _CSE]
    assert payload["encryption_skill_type_id"] == _CALDARI_ENC
    assert payload["inventor_character_id"] == 5
    assert payload["effective_probability"] == 0.31
    assert payload["skills_unknown_reason"] is None
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_invention_model.py -v`
Expected: FAIL with `AttributeError: module ... has no attribute 'SCIENCE_SKILL_GROUP_ID'`. The Task 1 tests still PASS.

- [ ] **Step 3: Implement the model additions**

In `invention_model.py`, change `from dataclasses import dataclass` to `from dataclasses import dataclass, field`. Then add, before `_no_decryptor_for`:

```python
#: SDE groups.id of the "Science" skill group: the invention science and
#: encryption skills (verified 2026-10-08). Production prerequisites such as
#: Capital Ship Construction (group 268) are not science skills.
SCIENCE_SKILL_GROUP_ID = 270

NO_CHARACTER_SKILLS = "no corp character has its skills loaded"
_NO_SKILL_ENTRIES = "the SDE lists no invention skills for this blueprint"
_NO_ENCRYPTION_SKILLS = "no Encryption Methods skills were found in the SDE"


@dataclass(frozen=True)
class InventionSkillSet:
    """The three skills EVE's invention chance reads for one blueprint."""

    science_skill_type_ids: tuple[int, int]
    encryption_skill_type_id: int


def invention_skill_set(
    skill_entries: Iterable[Mapping[str, Any]], encryption_skill_type_ids: frozenset[int],
) -> tuple[InventionSkillSet | None, str | None]:
    """(skill set, None), or (None, why) when the SDE entries are not exactly
    two science skills plus one encryption skill. Science = a group-270 entry
    that is not an encryption skill (verified against the SDE)."""
    if not encryption_skill_type_ids:
        return None, _NO_ENCRYPTION_SKILLS
    pairs: list[tuple[int, int]] = []
    for entry in skill_entries or []:
        if not isinstance(entry, Mapping):
            continue
        try:
            pairs.append((int(entry.get("type_id") or 0), int(entry.get("group_id") or 0)))
        except (TypeError, ValueError):
            continue
    pairs = [(type_id, group_id) for type_id, group_id in pairs if type_id > 0]
    if not pairs:
        return None, _NO_SKILL_ENTRIES
    encryption = [type_id for type_id, _ in pairs if type_id in encryption_skill_type_ids]
    science = sorted({
        type_id for type_id, group_id in pairs
        if group_id == SCIENCE_SKILL_GROUP_ID and type_id not in encryption_skill_type_ids
    })
    if len(encryption) != 1 or len(science) != 2:
        return None, (
            "the SDE invention skills are not 2 science skills + 1 encryption skill "
            f"(found {len(science)} + {len(encryption)})"
        )
    return InventionSkillSet((science[0], science[1]), encryption[0]), None


def invention_skill_factor(skill_set: InventionSkillSet, levels_by_skill_type_id: Mapping[int, int]) -> float:
    """1 + (science1 + science2) / 30 + encryption / 40 (EVE, since Crius).

    `levels_by_skill_type_id` is one character's loaded skill list
    (characters/character.py lists every published skill), so a skill that is
    absent is not injected: level 0, not unknown."""
    science = sum(int(levels_by_skill_type_id.get(type_id, 0)) for type_id in skill_set.science_skill_type_ids)
    encryption = int(levels_by_skill_type_id.get(skill_set.encryption_skill_type_id, 0))
    return 1.0 + science / 30.0 + encryption / 40.0


@dataclass(frozen=True)
class InventorChoice:
    character_id: int | None
    skill_factor: float | None
    unknown_reason: str | None


def best_inventor(
    skill_set: InventionSkillSet, levels_by_character_id: Mapping[int, Mapping[int, int]],
) -> InventorChoice:
    """The corp character with the highest skill factor for `skill_set`
    (ties: lowest character id), or an unknown reason when none is loaded."""
    if not levels_by_character_id:
        return InventorChoice(None, None, NO_CHARACTER_SKILLS)
    factor, negative_id = max(
        (invention_skill_factor(skill_set, levels), -int(character_id))
        for character_id, levels in levels_by_character_id.items()
    )
    return InventorChoice(-negative_id, factor, None)


@dataclass(frozen=True)
class InventionChance:
    base_probability: float
    #: None: skills unknown, so the base chance is used (see skills_unknown_reason).
    skill_factor: float | None
    decryptor_multiplier: float
    #: min(1, base x (skill_factor or 1) x decryptor_multiplier), before the admin floor.
    probability: float
    inventor_character_id: int | None
    skill_set: InventionSkillSet | None
    skills_unknown_reason: str | None
```

Replace the `InventionContext` class with:

```python
@dataclass
class InventionContext:
    """Per-refresh invention inputs. One object is shared by the top-level row
    and every nested sub-build of one overview refresh."""

    decryptor_for_blueprint: Callable[[int], DecryptorOption] = _no_decryptor_for
    #: {character_id: {skill type_id: trained level}}, only characters whose skills are loaded.
    inventor_skill_levels_by_character_id: Mapping[int, Mapping[int, int]] = field(default_factory=dict)
    encryption_skill_type_ids: frozenset[int] = frozenset()
    _warned: set[str] = field(default_factory=set, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def warn_once(self, key: str, message: str, *args: Any) -> None:
        """One WARNING per key per refresh. While warnings are quiet nothing
        is registered, so the plan that is kept still logs it."""
        if warnings_are_quiet():
            return
        with self._lock:
            if key in self._warned:
                return
            self._warned.add(key)
        logger.warning(message, *args)


def resolve_invention_chance(
    context: InventionContext,
    *,
    blueprint_type_id: int,
    skill_entries: Iterable[Mapping[str, Any]],
    base_probability: float,
    decryptor: DecryptorOption,
) -> InventionChance:
    """EVE's invention chance for the corp's best inventor of this blueprint.

    Skills unknown: the base chance (times the decryptor), one WARNING per
    reason per refresh, and the reason on the result. Never a guessed level."""
    multiplier = float(decryptor.probability_multiplier)
    skill_set, reason = invention_skill_set(skill_entries, context.encryption_skill_type_ids)
    choice: InventorChoice | None = None
    if skill_set is not None:
        choice = best_inventor(skill_set, context.inventor_skill_levels_by_character_id)
        reason = choice.unknown_reason
    if reason is not None or choice is None or choice.skill_factor is None:
        why = reason or NO_CHARACTER_SKILLS
        context.warn_once(
            why,
            "Invention chance (first seen for blueprint %s): %s; using the SDE base chance, "
            "which overstates the invention cost",
            blueprint_type_id, why,
        )
        return InventionChance(
            base_probability, None, multiplier, min(1.0, base_probability * multiplier),
            None, skill_set, why,
        )
    return InventionChance(
        base_probability, choice.skill_factor, multiplier,
        min(1.0, base_probability * float(choice.skill_factor) * multiplier),
        choice.character_id, skill_set, None,
    )


def invention_chance_payload(chance: InventionChance, effective_probability: float) -> dict[str, Any]:
    """The row's manufacturing_job.invention_chance (read by the UI and the
    daily planner's inventor ranking)."""
    return {
        "base_probability": chance.base_probability,
        "skill_factor": chance.skill_factor,
        "decryptor_multiplier": chance.decryptor_multiplier,
        "probability": chance.probability,
        "effective_probability": effective_probability,
        # Dropped by the fixture sanitiser ("character" key substring).
        "inventor_character_id": chance.inventor_character_id,
        "science_skill_type_ids": list(chance.skill_set.science_skill_type_ids) if chance.skill_set else None,
        "encryption_skill_type_id": chance.skill_set.encryption_skill_type_id if chance.skill_set else None,
        "skills_unknown_reason": chance.skills_unknown_reason,
    }
```

- [ ] **Step 4: Run the model tests**

Run: `.venv312/bin/python -m pytest tests/test_invention_model.py -v`
Expected: PASS.

- [ ] **Step 5: Write the failing SDE, producer and planner tests**

Append to `tests/test_sde_invention_data.py`:

```python
from eve_online_industry_tracker.infrastructure.sde.skills import get_encryption_skill_type_ids


def test_encryption_skills_are_science_skills_named_encryption_methods(sde_engine):
    with Session(sde_engine) as session:
        session.add(Types(id=21790, name={"en": "Caldari Encryption Methods", "de": "Caldari-Verschlüsselung"},
                          groupID=270, published=True))
        session.add(Types(id=11433, name={"en": "High Energy Physics"}, groupID=270, published=True))
        session.add(Types(id=99001, name={"en": "Old Encryption Methods"}, groupID=270, published=False))
        session.commit()
        assert get_encryption_skill_type_ids(session) == frozenset({21790})


@pytest.mark.skipif(not os.path.exists(_LOCAL_SDE), reason="local SDE database/eve_sde.db not present")
def test_the_local_sde_has_seven_encryption_skills():
    engine = create_engine(f"sqlite:///file:{_LOCAL_SDE}?mode=ro&uri=true")
    try:
        with Session(engine) as session:
            ids = get_encryption_skill_type_ids(session)
    finally:
        engine.dispose()
    assert ids == frozenset({3408, 21790, 21791, 23087, 23121, 52308, 55025})
```

Append to `tests/test_industry_service_calculations.py`:

```python
# --- round 5 Task 2: invention chance from the corp's best inventor -----------

_INVENTION_SKILLS = [
    {"type_id": 11433, "type_name": "High Energy Physics", "group_id": 270},
    {"type_id": 11454, "type_name": "Caldari Starship Engineering", "group_id": 270},
    {"type_id": 21790, "type_name": "Caldari Encryption Methods", "group_id": 270},
]


def _skilled_overview(monkeypatch, *, levels, encryption=frozenset({21790}), real_confidence=False, **kwargs):
    rows = _invented_t2_blueprint_rows()
    rows[0]["invention_job"]["skill_entries"] = [dict(s) for s in _INVENTION_SKILLS]
    service = _invention_service(monkeypatch, blueprint_rows=rows, real_confidence=real_confidence, **kwargs)
    service._get_inventor_skill_levels = MethodType(lambda self: levels, service)  # type: ignore[attr-defined]
    service._load_encryption_skill_type_ids = MethodType(lambda self: encryption, service)  # type: ignore[attr-defined]
    out = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )
    return {row["type_id"]: row for row in out}


def test_the_best_inventors_skills_raise_the_chance(monkeypatch) -> None:
    levels = {1: {11433: 4, 11454: 4, 21790: 4}, 2: {11433: 5, 11454: 5, 21790: 3}}
    mj = _skilled_overview(monkeypatch, levels=levels)[5002]["manufacturing_job"]

    factor = 1 + 10 / 30 + 3 / 40  # pilot 2 beats pilot 1 (1 + 8/30 + 4/40)
    p = 0.5 * factor
    chance = mj["invention_chance"]
    assert chance["inventor_character_id"] == 2
    assert chance["skill_factor"] == pytest.approx(factor)
    assert chance["effective_probability"] == pytest.approx(p)
    assert mj["activity_breakdown"]["invention"]["success_probability"] == pytest.approx(p)
    # 1 run / 10 per BPC / p attempts, 2 datacores x 100 each.
    assert mj["invention_material_cost"] == pytest.approx(2 * 100 * (1 / 10) / p)
    assert chance["skills_unknown_reason"] is None


def test_an_untrained_corp_is_a_known_factor_of_one(monkeypatch) -> None:
    mj = _skilled_overview(monkeypatch, levels={1: {}})[5002]["manufacturing_job"]

    assert mj["invention_chance"]["skill_factor"] == 1.0
    assert mj["invention_chance"]["skills_unknown_reason"] is None
    assert mj["invention_material_cost"] == pytest.approx(40.0)


def test_unloaded_skills_fall_back_to_the_base_chance_with_a_reason(monkeypatch, caplog) -> None:
    with caplog.at_level("WARNING"):
        mj = _skilled_overview(monkeypatch, levels={}, real_confidence=True)[5002]["manufacturing_job"]

    assert mj["invention_chance"]["skill_factor"] is None
    assert mj["invention_chance"]["skills_unknown_reason"] == "no corp character has its skills loaded"
    assert mj["invention_material_cost"] == pytest.approx(40.0)  # base 0.5
    assert any("SDE base chance" in r for r in mj["pricing_confidence_reasons"])
    assert sum("no corp character has its skills loaded" in r.getMessage()
               for r in caplog.records if r.levelname == "WARNING") == 1


def test_the_admin_floor_still_applies_to_the_skilled_chance(monkeypatch) -> None:
    rows_levels = {1: {11433: 5, 11454: 5, 21790: 5}}
    service_rows = _invented_t2_blueprint_rows()
    service_rows[0]["invention_job"]["products"][0]["probability_pct"] = 0.0
    service_rows[0]["invention_job"]["skill_entries"] = [dict(s) for s in _INVENTION_SKILLS]
    service = _invention_service(monkeypatch, blueprint_rows=service_rows, adm_overrides={"invention_probability_floor": 0.05})
    service._get_inventor_skill_levels = MethodType(lambda self: rows_levels, service)  # type: ignore[attr-defined]
    service._load_encryption_skill_type_ids = MethodType(lambda self: frozenset({21790}), service)  # type: ignore[attr-defined]
    rows = {r["type_id"]: r for r in service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False, maximize_bp_runs=False, character_id=1,
    )}
    assert rows[5002]["manufacturing_job"]["activity_breakdown"]["invention"]["success_probability"] == pytest.approx(0.05)


def test_inventor_skills_come_from_every_loaded_corp_character() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace(char_manager=SimpleNamespace(get_characters=lambda: [  # type: ignore[attr-defined]
        {"character_id": 1, "skills": {"skills": [
            {"skill_id": 11433, "trained_skill_level": 4}, {"skill_id": 21790, "trained_skill_level": 2},
        ]}},
        {"character_id": 2, "skills": {}},          # not fetched: unknown, left out
        {"character_id": 3},                        # not fetched either
    ]))
    assert service._get_inventor_skill_levels() == {1: {11433: 4, 21790: 2}}


def test_no_character_manager_means_no_inventor_skills() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    assert service._get_inventor_skill_levels() == {}
```

Append to `tests/test_daily_planner_assignment.py`:

```python
def test_invention_goes_to_the_pilot_with_the_items_best_skill_factor():
    """D2: rank by the item's own science + encryption skills (the producer's
    invention_chance), not by a generic Science sum."""
    chance = {"science_skill_type_ids": [11433, 11454], "encryption_skill_type_id": 21790}
    generalist = _char(1, "Generalist", [_skill("Laboratory Operation", 5), _skill("Science", 5, 3402)])
    specialist = _char(2, "Specialist", [
        _skill("Laboratory Operation", 5), _skill("High Energy Physics", 5, 11433),
        _skill("Caldari Starship Engineering", 4, 11454), _skill("Caldari Encryption Methods", 4, 21790),
    ])
    plan = ChainPlan(decisions=[_decision(overview_row={
        "type_id": 12345, "needs_invention": True,
        "manufacturing_job": {"invention_chance": chance},
    })])
    actions = CharacterAssigner().assign(plan, [], _Chars([generalist, specialist]), _AdminStub())
    assert [a.character_id for a in actions if a.action_type == "invent"] == [2]


def test_skill_levels_by_id_reads_the_enriched_list():
    from eve_online_industry_tracker.application.daily_planner.character_assigner import skill_levels_by_id

    char = _char(1, "Pilot", [_skill("High Energy Physics", 3, 11433), _skill("Science", 5, 3402)])
    assert skill_levels_by_id(char) == {11433: 3, 3402: 5}
    assert skill_levels_by_id({"character_id": 1}) == {}
```

- [ ] **Step 6: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_sde_invention_data.py tests/test_industry_service_calculations.py tests/test_daily_planner_assignment.py -k "encryption or inventor or untrained or unloaded_skills or admin_floor_still or skill_factor or skill_levels_by_id" -v`
Expected: FAIL. `ModuleNotFoundError: ...sde.skills`, `KeyError: 'invention_chance'` or `AttributeError: '_get_inventor_skill_levels'`, and the planner test picks character 1.

- [ ] **Step 7: Implement**

7a. Create `src/eve_online_industry_tracker/infrastructure/sde/skills.py`:

```python
from __future__ import annotations

from typing import Any

from sqlalchemy import text

from eve_online_industry_tracker.infrastructure.sde.localization import parse_localized

#: SDE groups.id "Science", the group of every invention skill.
_SCIENCE_SKILL_GROUP_ID = 270


def get_encryption_skill_type_ids(sde_session: Any) -> frozenset[int]:
    """Type ids of the invention encryption skills: published skills of the
    SDE "Science" group whose English name ends in "Encryption Methods" (7 in
    the 2026 SDE). The English name is read on purpose, whatever the display
    language, so the rule does not depend on a translation."""
    rows = sde_session.execute(
        text("SELECT id, name FROM types WHERE groupID = :gid AND published = 1"),
        {"gid": _SCIENCE_SKILL_GROUP_ID},
    ).fetchall()
    return frozenset(
        int(type_id) for type_id, raw_name in rows
        if type_id is not None and parse_localized(raw_name, "en").endswith("Encryption Methods")
    )
```

7b. In `service.py`, extend the `invention_model` import with `invention_chance_payload, resolve_invention_chance`, and add `from eve_online_industry_tracker.infrastructure.sde.skills import get_encryption_skill_type_ids`. Add next to `_load_decryptor_options`:

```python
    def _get_inventor_skill_levels(self) -> dict[int, dict[int, int]]:
        """{character_id: {skill type_id: trained level}} for every corp
        character whose skills are loaded. A character without a loaded skill
        list is unknown and left out, never read as level 0. No WARNING here:
        resolve_invention_chance warns once when nobody is left."""
        char_manager = getattr(self._state, "char_manager", None)
        if char_manager is None:
            return {}
        try:
            characters = char_manager.get_characters() or []
        except ValueError:  # CharacterManager: no characters configured yet
            return {}
        out: dict[int, dict[int, int]] = {}
        for char in characters:
            if not isinstance(char, dict):
                continue
            skills = char.get("skills")
            entries = skills.get("skills") if isinstance(skills, dict) else None
            character_id = int(char.get("character_id") or 0)
            if character_id <= 0 or not isinstance(entries, list) or not entries:
                continue
            levels: dict[int, int] = {}
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                try:
                    skill_id = int(entry.get("skill_id") or 0)
                    level = int(entry.get("trained_skill_level") or 0)
                except (TypeError, ValueError):
                    continue
                if skill_id > 0:
                    levels[skill_id] = max(level, levels.get(skill_id, 0))
            out[character_id] = levels
        return out

    def _load_encryption_skill_type_ids(self) -> frozenset[int]:
        """The SDE's encryption skills; empty without SDE access (every chance
        is then the base chance, warned once by resolve_invention_chance)."""
        if self._sessions is None:
            return frozenset()
        session = self._sessions.sde_session()
        try:
            return get_encryption_skill_type_ids(session)
        finally:
            session.close()
```

7c. In `_build_planning_context`, add to the `_ProductPlanningContext(...)` call:

```python
            invention_context=InventionContext(
                inventor_skill_levels_by_character_id=self._get_inventor_skill_levels(),
                encryption_skill_type_ids=self._load_encryption_skill_type_ids(),
            ),
```

7d. `_expected_invention_costs`: add the keyword parameters `decryptor: DecryptorOption,` and `invention_context: InventionContext,` after `adjusted_price_map`. Replace

```python
        probability = 0.0
        if isinstance(target_entry, dict):
            try:
                probability = float(target_entry.get("probability_pct") or 0.0) / 100.0
            except Exception:
                probability = 0.0
```

with

```python
        base_probability = 0.0
        if isinstance(target_entry, dict):
            try:
                base_probability = float(target_entry.get("probability_pct") or 0.0) / 100.0
            except Exception:
                base_probability = 0.0
        # EVE: base x (1 + (sci1 + sci2)/30 + enc/40) x decryptor, for the
        # corp's best inventor of this blueprint (D2). Resolved only when
        # something is invented, so a row that invents nothing warns nothing.
        chance = (
            resolve_invention_chance(
                invention_context, blueprint_type_id=int(blueprint_type_id),
                skill_entries=[e for e in (invention_job.get("skill_entries") or []) if isinstance(e, dict)],
                base_probability=base_probability, decryptor=decryptor,
            )
            if invented_runs > 0 else None
        )
        probability = (
            chance.probability if chance is not None
            else min(1.0, base_probability * float(decryptor.probability_multiplier))
        )
```

After `costs["success_probability"] = effective_probability`, add:

```python
        if chance is not None:
            costs["invention_chance"] = invention_chance_payload(chance, effective_probability)
```

The `"no SDE success probability"` WARNING keeps testing `probability <= 0`. The skill factor and the multiplier are positive, so this is still "the base chance is 0".

7e. `_plan_nested_invention`: add the parameters `decryptor: DecryptorOption,` and `invention_context: InventionContext,` and pass both to `_expected_invention_costs`. In `_plan_blueprint_chain_for_quantity`, the nested-invention call (:4745) passes `decryptor=invention_context.decryptor_for_blueprint(blueprint_type_id), invention_context=invention_context,`.

7f. `_full_batch_invention_whole_attempts`: add `decryptor: DecryptorOption,` and `invention_context: InventionContext,`. Pass them on, and return `int(costs.get("whole_attempts") or 0), costs.get("cost_unknown_reason"), costs.get("invention_chance")`. Update the return annotation to `tuple[int, str | None, dict[str, Any] | None]` and the docstring ("…and the chance payload"). Update both callers:
- `_extra_batch_invention_requirements`: `whole_attempts, unknown_reason, _chance = self._full_batch_invention_whole_attempts(..., decryptor=decryptor, invention_context=invention_context)`. Add the two parameters to `_extra_batch_invention_requirements` and pass `decryptor=invention_context.decryptor_for_blueprint(blueprint_type_id), invention_context=invention_context` from its caller (:4837).
- In `_build_single_product_row` (:7545): `top_level_extra_attempts, extra_unknown_reason, extra_chance = self._full_batch_invention_whole_attempts(..., decryptor=decryptor, invention_context=ctx.invention_context)`. Define `extra_chance: dict[str, Any] | None = None` before the `if bool(ctx.build_from_bpc) and has_top_level_invention_path ...` block.

7g. `_plan_invention_chain_for_variant`: add `decryptor: DecryptorOption,` and pass `decryptor=decryptor, invention_context=ctx.invention_context` to `_expected_invention_costs`. Its call in `_build_single_product_row` passes `decryptor=decryptor`.

7h. Row output, after `if invention_cost_unknown_reason is not None: ...`:

```python
        top_level_chance = (
            inv_costs.get("invention_chance") if requires_invention_chain and inv_costs is not None else None
        ) or extra_chance
        if isinstance(top_level_chance, dict):
            # The chance this row's own invention assumes: batch 1's, or the
            # later batches' when an owned BPC covers batch 1.
            row_out["manufacturing_job"]["invention_chance"] = top_level_chance
```

7i. Pricing confidence, after the `if invention_cost_unknown_reason: ...` block (:2272):

```python
            invention_chance = manufacturing_job.get("invention_chance")
            if isinstance(invention_chance, dict) and invention_chance.get("skills_unknown_reason"):
                reasons.append(
                    f"Invention chance uses the SDE base chance ({invention_chance['skills_unknown_reason']}) — "
                    f"inventor skills would raise it, so the T2 cost is overstated."
                )
```

7j. `overview_row.py`, add:

```python
def get_invention_skill_set(row: dict[str, Any]) -> tuple[tuple[int, int], int] | None:
    """((science skill ids), encryption skill id) the producer's invention
    chance read for this row (manufacturing_job.invention_chance), or None
    when the row carries none or the skills were unknown."""
    chance = get_manufacturing_job(row).get("invention_chance")
    if not isinstance(chance, dict):
        return None
    science = chance.get("science_skill_type_ids")
    encryption = _as_int(chance.get("encryption_skill_type_id"))
    if not isinstance(science, list) or len(science) != 2 or encryption <= 0:
        return None
    first, second = (_as_int(s) for s in science)
    if first <= 0 or second <= 0:
        return None
    return (first, second), encryption
```

7k. `character_assigner.py`:
- Import `from eve_online_industry_tracker.application.industry.invention_model import InventionSkillSet, invention_skill_factor`.
- Add `skill_levels_by_id`:

```python
def skill_levels_by_id(char: dict[str, Any]) -> dict[int, int]:
    """{skill type_id: trained_skill_level} for one character, from the same
    enriched list as skill_levels_by_name. Ids, not names: the producer's
    invention chance names its skills by SDE type id."""
    raw = char.get("skills")
    entries = raw.get("skills") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        return {}
    levels: dict[int, int] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            skill_id = int(entry.get("skill_id") or 0)
            level = int(entry.get("trained_skill_level") or 0)
        except (TypeError, ValueError):
            continue
        if skill_id > 0:
            levels[skill_id] = max(level, levels.get(skill_id, 0))
    return levels
```

- In `_compute_available_slots`, add `"skills_by_id": skill_levels_by_id(char),` to each slot dict.
- Call `self._best_invention_char(char_slots, characters, row)` in the invention branch.
- Replace `_best_invention_char` with:

```python
    def _best_invention_char(
        self, char_slots: dict[int, dict], characters: list[dict], row: dict[str, Any] | None = None,
    ) -> tuple[int | None, str | None]:
        """The free pilot with the highest invention skill factor for this
        item: the producer's invention_chance skills (D2, the same rule as the
        chance it assumed). A row without them ranks by the generic
        SCIENCE_SKILLS sum. Ties break toward the fewest active jobs."""
        skills = orow.get_invention_skill_set(row or {})
        skill_set = InventionSkillSet(skills[0], skills[1]) if skills is not None else None
        candidates = []
        for slot in char_slots.values():
            if slot.get("free_research", 0) <= 0:
                continue
            if skill_set is not None:
                score = invention_skill_factor(skill_set, slot.get("skills_by_id") or {})
            else:
                levels = slot.get("skills") or {}
                score = float(sum(int(levels.get(name, 0) or 0) for name in SCIENCE_SKILLS))
            candidates.append((-score, slot.get("total_active_jobs", 0), slot["character_id"], slot["character_name"]))
        if not candidates:
            return None, None
        candidates.sort()
        return candidates[0][2], candidates[0][3]
```

- [ ] **Step 8: Run the new tests**

Run: the command from Step 6.
Expected: PASS.

- [ ] **Step 9: Run the full suite**

Run: `.venv312/bin/python -m pytest -q`
Expected: all pass. The test harness has no character manager and no SDE session, so every existing producer test still runs at the base chance, and its numbers do not move.

- [ ] **Step 10: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/invention_model.py \
  src/eve_online_industry_tracker/infrastructure/sde/skills.py \
  src/eve_online_industry_tracker/application/industry/service.py \
  src/eve_online_industry_tracker/application/industry/overview_row.py \
  src/eve_online_industry_tracker/application/daily_planner/character_assigner.py \
  tests/test_invention_model.py tests/test_sde_invention_data.py \
  tests/test_industry_service_calculations.py tests/test_daily_planner_assignment.py
git commit -m "feat: invention chance from the corp's best inventor, same ranking for invent actions

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Decryptors as invention inputs and the best decryptor per item (D1, FU-47)

For each invented blueprint, evaluate "no decryptor" and every SDE decryptor, and pick one with `select_best_decryptor`. That function is the single named place where the criterion lives. Proposed criterion (a): the lowest total cost per manufactured unit (materials + amortized invention inputs including the decryptor + job fees) for one fully invented batch of that blueprint's own row, with ties within 0.01 ISK/unit going to no decryptor. See "Open questions" Q1.

The choice is made once per blueprint per refresh and memoized on the planning context. The top-level row and every nested sub-build of that blueprint then use the same decryptor. Candidate builds run inside `quiet_warnings()`, so a missing input is logged once, by the row that is kept.

A decryptor is a per-attempt invention input. It gets an amortized cost in `expected_invention_materials` (the price step adds it to `material_cost`) and whole-attempt buy lines in `invention_procurement_materials` and the per-extra-batch list. It also adds runs to every invented BPC, ME and TE to the job, and a factor to the chance. The invention install fee keeps its datacore-only EIV basis, so the fee model is unchanged. An admin switch turns selection off, and "no decryptor" is then used everywhere.

In the daily planner, the invent action names the decryptor and the shopping list buys one per attempt.

**Files:**
- Modify: `application/industry/invention_model.py`. Add `DecryptorEvaluation`, `DecryptorChoice`, `select_best_decryptor`, `decryptor_choice_payload` and the reason constants.
- Modify: `application/industry/service.py`. Changes:
  - `_ProductPlanningContext`;
  - `_build_planning_context`;
  - `_expected_invention_costs` (runs modifier and decryptor input);
  - `_invention_whole_attempt_requirements` (becomes a classmethod, gains `decryptor`) and its four callers;
  - new `_decryptor_input_entries`, `_row_invents`, `_decryptor_choice_for`, `_evaluate_decryptors`, `_decryptor_price_skip_reason`, `_row_cost_per_unit_estimate`, `_build_product_row`;
  - `_build_single_product_row(fully_invented=)`;
  - `_build_variant` (:7858).
- Modify: `config/admin_settings.py` (after `invention_probability_floor`, :309-318)
- Modify: `application/industry/overview_row.py`, `application/daily_planner/models.py` (`AssignedAction`), `application/daily_planner/character_assigner.py` (invent branch :220-237), `application/daily_planner/shopping_list_builder.py` (:128-173)
- Modify: `src/streamlit_ui/state/industry_builder_ui.py`, `src/streamlit_ui/pages/industry_builder.py` (drilldown, after `ASSUMES_OWNED_T2_BPO_CAPTION` :132)
- Test: `tests/test_invention_model.py`, `tests/test_industry_service_calculations.py`, `tests/test_daily_planner_assignment.py`, `tests/test_industry_builder_t2_bpo_marker.py`

**Interfaces:**
- Consumes: Tasks 1–2 (`DecryptorOption`, `NO_DECRYPTOR`, `invented_runs_per_blueprint`, `invented_blueprint_efficiency`, `InventionContext.decryptor_for_blueprint`, `quiet_warnings`, `_expected_invention_costs(..., decryptor, invention_context)`).
- Produces, in `invention_model`:
  - `DECRYPTOR_TIE_TOLERANCE_ISK = 0.01`, `DECRYPTOR_SELECTION_OFF_REASON`, `NO_DECRYPTOR_DATA_REASON`
  - `@dataclass(frozen=True) DecryptorEvaluation(option, cost_per_unit: float | None, skip_reason: str | None = None)`
  - `@dataclass(frozen=True) DecryptorChoice(option, reason: str, cost_per_unit=None, baseline_cost_per_unit=None, evaluations: tuple = ())`
  - `select_best_decryptor(evaluations) -> DecryptorChoice`
  - `decryptor_choice_payload(choice) -> dict`
- Produces, in `IndustryService`:
  - `_decryptor_choice_for(ctx, blueprint_type_id) -> DecryptorChoice`
  - `_build_product_row(ctx, **variant) -> dict | None`
  - `_row_cost_per_unit_estimate(row_out) -> float | None`
  - `_invention_whole_attempt_requirements(invention_job, whole_attempts, decryptor=NO_DECRYPTOR)`
  - `_build_single_product_row(..., fully_invented: bool = False)`
  - row key `manufacturing_job.invention_decryptor: dict` (keys: `type_id`, `type_name`, `reason`, `cost_per_unit`, `baseline_cost_per_unit`, `probability_multiplier`, `me_modifier`, `te_modifier`, `max_run_modifier`, `candidates`)
- Produces, in `overview_row`: `get_invention_decryptor(row) -> tuple[int, str] | None`.
- Produces: `AssignedAction.decryptor_type_id: int | None = None`, `.decryptor_type_name: str | None = None`.
- Produces, in `industry_builder_ui`: `invention_summary_lines(row) -> list[str]`.
- Produces: admin setting `industry.invention_decryptor_selection_enabled` (bool, default True).

- [ ] **Step 1: Write the failing criterion tests**

Append to `tests/test_invention_model.py`:

```python
# --- Task 3: the decryptor selection criterion ----------------------------------

_ACC = im.DecryptorOption(34201, "Accelerant Decryptor", 1.2, 2, 10, 1)
_ATT = im.DecryptorOption(34202, "Attainment Decryptor", 1.8, -1, 4, 4)


def _ev(option, cost, skip=None):
    return im.DecryptorEvaluation(option=option, cost_per_unit=cost, skip_reason=skip)


def test_the_lowest_cost_per_unit_wins():
    choice = im.select_best_decryptor([_ev(im.NO_DECRYPTOR, 89.0), _ev(_ACC, 85.5), _ev(_ATT, 144.7)])
    assert choice.option is _ACC
    assert choice.cost_per_unit == 85.5 and choice.baseline_cost_per_unit == 89.0
    assert "Accelerant Decryptor" in choice.reason and "89.00" in choice.reason


def test_a_tie_within_the_tolerance_goes_to_no_decryptor():
    choice = im.select_best_decryptor([_ev(im.NO_DECRYPTOR, 89.0), _ev(_ACC, 89.0 - im.DECRYPTOR_TIE_TOLERANCE_ISK / 2)])
    assert choice.option is im.NO_DECRYPTOR
    assert choice.reason.startswith("no decryptor: lowest cost per unit")


def test_skipped_or_unknown_candidates_are_never_chosen():
    choice = im.select_best_decryptor([
        _ev(im.NO_DECRYPTOR, 89.0), _ev(_ACC, None, "no market price for Accelerant Decryptor"), _ev(_ATT, None),
    ])
    assert choice.option is im.NO_DECRYPTOR
    assert "no decryptor could be priced" in choice.reason


def test_an_unknown_baseline_compares_nothing():
    choice = im.select_best_decryptor([_ev(im.NO_DECRYPTOR, None), _ev(_ACC, 10.0)])
    assert choice.option is im.NO_DECRYPTOR
    assert "unknown" in choice.reason


def test_equal_decryptors_break_ties_by_type_id():
    cheap_twin = im.DecryptorOption(34200, "Twin", 1.2, 2, 10, 1)
    choice = im.select_best_decryptor([_ev(im.NO_DECRYPTOR, 89.0), _ev(_ACC, 80.0), _ev(cheap_twin, 80.0)])
    assert choice.option is cheap_twin


def test_the_payload_lists_every_candidate_and_the_modifiers():
    evaluations = [_ev(im.NO_DECRYPTOR, 89.0), _ev(_ACC, 85.5), _ev(_ATT, None, "no market price for Attainment Decryptor")]
    payload = im.decryptor_choice_payload(im.select_best_decryptor(evaluations))
    assert payload["type_id"] == 34201 and payload["type_name"] == "Accelerant Decryptor"
    assert (payload["probability_multiplier"], payload["me_modifier"], payload["te_modifier"],
            payload["max_run_modifier"]) == (1.2, 2, 10, 1)
    assert [c["type_id"] for c in payload["candidates"]] == [None, 34201, 34202]
    assert payload["candidates"][2]["skip_reason"] == "no market price for Attainment Decryptor"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_invention_model.py -k "lowest_cost or tie or skipped or unknown_baseline or equal_decryptors or payload_lists" -v`
Expected: FAIL. `AttributeError: ... 'DecryptorEvaluation'`.

- [ ] **Step 3: Implement the criterion**

Append to `invention_model.py`:

```python
# --- decryptor selection --------------------------------------------------

#: Cost-per-unit differences below this are ties (and ties go to no decryptor).
DECRYPTOR_TIE_TOLERANCE_ISK = 0.01
DECRYPTOR_SELECTION_OFF_REASON = "no decryptor: decryptor selection is off (admin setting)"
NO_DECRYPTOR_DATA_REASON = "no decryptor: the SDE gave no decryptor data"


@dataclass(frozen=True)
class DecryptorEvaluation:
    """One candidate's outcome: its cost per manufactured unit for one fully
    invented batch, or None (unknown), or a reason it was not evaluated."""

    option: DecryptorOption
    cost_per_unit: float | None
    skip_reason: str | None = None


@dataclass(frozen=True)
class DecryptorChoice:
    option: DecryptorOption
    reason: str
    cost_per_unit: float | None = None
    baseline_cost_per_unit: float | None = None
    evaluations: tuple[DecryptorEvaluation, ...] = ()


def select_best_decryptor(evaluations: Iterable[DecryptorEvaluation]) -> DecryptorChoice:
    """THE decryptor selection criterion, kept in this one function so it is
    easy to change (round-5 open question Q1).

    Criterion (a): the lowest total cost per manufactured unit (materials,
    amortized invention inputs including the decryptor, and job fees) for
    one fully invented batch. Ties within DECRYPTOR_TIE_TOLERANCE_ISK go to no
    decryptor, and equal decryptors go to the lowest type id. A candidate
    with a skip reason or an unknown cost is never chosen. When the
    no-decryptor cost itself is unknown nothing can be compared, so no
    decryptor is used.
    """
    evaluations = tuple(evaluations)
    baseline = next((e for e in evaluations if e.option.type_id is None), None)
    if baseline is None or baseline.cost_per_unit is None:
        return DecryptorChoice(
            NO_DECRYPTOR,
            "no decryptor: the cost per unit without a decryptor is unknown, so nothing could be compared",
            None, None, evaluations,
        )
    priced = [
        e for e in evaluations
        if e.option.type_id is not None and e.skip_reason is None and e.cost_per_unit is not None
    ]
    best = min(priced, key=lambda e: (float(e.cost_per_unit or 0.0), int(e.option.type_id or 0)), default=None)
    if best is None or float(best.cost_per_unit or 0.0) >= baseline.cost_per_unit - DECRYPTOR_TIE_TOLERANCE_ISK:
        detail = (
            f"; best decryptor {best.option.type_name} {best.cost_per_unit:,.2f} ISK"
            if best is not None else "; no decryptor could be priced"
        )
        return DecryptorChoice(
            NO_DECRYPTOR,
            f"no decryptor: lowest cost per unit ({baseline.cost_per_unit:,.2f} ISK{detail})",
            baseline.cost_per_unit, baseline.cost_per_unit, evaluations,
        )
    return DecryptorChoice(
        best.option,
        f"{best.option.type_name}: lowest cost per unit ({best.cost_per_unit:,.2f} ISK vs "
        f"{baseline.cost_per_unit:,.2f} ISK without a decryptor)",
        best.cost_per_unit, baseline.cost_per_unit, evaluations,
    )


def decryptor_choice_payload(choice: DecryptorChoice) -> dict[str, Any]:
    """The row's manufacturing_job.invention_decryptor (shown in the UI, read
    by the daily planner's invent action)."""
    return {
        "type_id": choice.option.type_id,
        "type_name": choice.option.type_name,
        "reason": choice.reason,
        "cost_per_unit": choice.cost_per_unit,
        "baseline_cost_per_unit": choice.baseline_cost_per_unit,
        "probability_multiplier": choice.option.probability_multiplier,
        "me_modifier": choice.option.me_modifier,
        "te_modifier": choice.option.te_modifier,
        "max_run_modifier": choice.option.max_run_modifier,
        "candidates": [
            {"type_id": e.option.type_id, "type_name": e.option.type_name,
             "cost_per_unit": e.cost_per_unit, "skip_reason": e.skip_reason}
            for e in choice.evaluations
        ],
    }
```

- [ ] **Step 4: Run the criterion tests**

Run: the command from Step 2.
Expected: PASS.

- [ ] **Step 5: Write the failing producer, planner and UI tests**

Append to `tests/test_industry_service_calculations.py`:

```python
# --- round 5 Task 3: the best decryptor per invented item ---------------------
# Fixture: 10 runs (maximize), p 0.5 (no skills loaded: base chance), 10 runs
# per BPC, 2 datacores x 100 per attempt, 10 Tritanium x 5 per run, no fees.
# No decryptor:  ME 2 -> 98 Trit = 490; attempts 10/10/0.5 = 2 -> 400. 89.0/unit.
# Accelerant-like (x1.2, ME+2, runs+1) at 50: ME 4 -> 96 Trit = 480;
#   attempts (10/11)/0.6 = 1.515 -> datacores 303.03 + decryptors 75.76. 85.88/unit.
# Attainment-like (x1.8, ME-1, runs+4) at 1000: ME 1 -> 99 Trit = 495;
#   attempts (10/14)/0.9 = 0.794 -> 158.73 + 793.65. 144.74/unit.

_ATTAINMENT_LIKE = DecryptorOption(
    type_id=34202, type_name="Attainment Decryptor", probability_multiplier=1.8,
    me_modifier=-1, te_modifier=4, max_run_modifier=4,
)
_SYMMETRY_LIKE = DecryptorOption(
    type_id=34206, type_name="Symmetry Decryptor", probability_multiplier=1.0,
    me_modifier=1, te_modifier=8, max_run_modifier=2,
)
_DECRYPTOR_PRICES = {**_INVENTION_FAKE_PRICES, 34201: 50.0, 34202: 1000.0, 34206: 1.0}


def _decryptor_overview(monkeypatch, options, *, prices=None, maximize_bp_runs=True, **kwargs):
    service = _invention_service(monkeypatch, prices=prices or _DECRYPTOR_PRICES, **kwargs)
    service._load_decryptor_options = MethodType(  # type: ignore[attr-defined]
        lambda self: (NO_DECRYPTOR, *options), service,
    )
    rows = service.industry_manufacturing_product_overview(
        build_from_bpc=True, have_blueprint_source_only=False,
        maximize_bp_runs=maximize_bp_runs, character_id=1,
    )
    return {row["type_id"]: row for row in rows}


def test_the_cheapest_decryptor_per_unit_is_chosen(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE, _ATTAINMENT_LIKE])[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] == 34201
    assert choice["cost_per_unit"] == pytest.approx((480 + (10 / 11 / 0.6) * (2 * 100 + 50)) / 10)
    assert choice["baseline_cost_per_unit"] == pytest.approx(89.0)
    costs = {c["type_id"]: c["cost_per_unit"] for c in choice["candidates"]}
    assert costs[34202] == pytest.approx((495 + (10 / 14 / 0.9) * (2 * 100 + 1000)) / 10)


def test_the_chosen_decryptor_sets_me_te_runs_and_buys_one_per_attempt(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE])[5002]["manufacturing_job"]

    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (4, 14)
    assert mj["materials"]["34"]["quantity"] == 96
    invention = mj["activity_breakdown"]["invention"]
    assert invention["runs_per_invented_blueprint"] == 11
    assert invention["success_probability"] == pytest.approx(0.6)
    attempts = 10 / 11 / 0.6
    assert mj["expected_invention_materials"]["34201"]["quantity"] == pytest.approx(attempts)
    assert mj["invention_material_cost"] == pytest.approx(attempts * (2 * 100 + 50))
    # Whole attempts: ceil(10 / 11) = 1 BPC, ceil(1 / 0.6) = 2 attempts.
    buy = mj["invention_procurement_materials"]
    assert (buy["204"]["quantity"], buy["34201"]["quantity"]) == (4, 2)
    # The fee basis stays the datacores' EIV only.
    assert invention["estimated_item_value"] == pytest.approx(2 * 100 * attempts)


def test_a_run_modifier_that_crosses_a_bpc_boundary_halves_the_whole_attempts(monkeypatch) -> None:
    # 12 runs: 2 BPCs at 10 runs each (4 attempts, 8 datacores) without a
    # decryptor; one 12-run BPC with the +2 decryptor (2 attempts, 4 + 2).
    rows = _rows_with(probability_pct=50.0, bpc_runs=10, t2_limit=12)
    mj = _decryptor_overview(monkeypatch, [_SYMMETRY_LIKE], blueprint_rows=rows)[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["type_id"] == 34206
    buy = mj["invention_procurement_materials"]
    assert (buy["204"]["quantity"], buy["34206"]["quantity"]) == (4, 2)


def test_a_decryptor_without_a_market_price_is_never_chosen(monkeypatch) -> None:
    prices = {k: v for k, v in _DECRYPTOR_PRICES.items() if k != 34201}
    mj = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], prices=prices)[5002]["manufacturing_job"]

    choice = mj["invention_decryptor"]
    assert choice["type_id"] is None
    assert choice["candidates"][1]["skip_reason"] == "no market price for Accelerant Decryptor"
    assert (mj["blueprint_material_efficiency"], mj["blueprint_time_efficiency"]) == (2, 4)


def test_no_decryptor_data_plans_without_a_decryptor_and_says_why(monkeypatch) -> None:
    mj = _decryptor_overview(monkeypatch, [])[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["type_id"] is None
    assert mj["invention_decryptor"]["reason"] == "no decryptor: the SDE gave no decryptor data"


def test_decryptor_selection_off_plans_without_a_decryptor(monkeypatch) -> None:
    mj = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE],
        adm_overrides={"invention_decryptor_selection_enabled": False},
    )[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["reason"] == "no decryptor: decryptor selection is off (admin setting)"
    assert mj["blueprint_material_efficiency"] == 2


def test_a_blueprint_without_invention_gets_no_decryptor_payload(monkeypatch) -> None:
    out = _decryptor_overview(monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=_invented_t2_blueprint_rows()[1:])
    assert "invention_decryptor" not in out[5002]["manufacturing_job"]
    assert "invention_chance" not in out[5002]["manufacturing_job"]


def test_unknown_invention_odds_compare_no_decryptor_and_warn_once(monkeypatch, caplog) -> None:
    rows = _rows_with(probability_pct=0.0, bpc_runs=10, t2_limit=10)
    with caplog.at_level("WARNING"):
        mj = _decryptor_overview(
            monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=rows,
            adm_overrides={"invention_probability_floor": 0.0},
        )[5002]["manufacturing_job"]

    assert mj["invention_decryptor"]["type_id"] is None
    assert "unknown" in mj["invention_decryptor"]["reason"]
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert sum("invention cost left unknown" in m for m in warnings) == 1


def test_a_nested_invented_component_uses_its_blueprints_decryptor(monkeypatch) -> None:
    out = _decryptor_overview(
        monkeypatch, [_ACCELERANT_LIKE], blueprint_rows=_nested_invention_blueprint_rows(),
        prices={**_NESTED_FAKE_PRICES, 34201: 50.0},
    )
    mj = out[7100]["manufacturing_job"]

    # The parent's 10 components are built on an Accelerant BPC (ME 4).
    assert mj["procurement_materials"]["34"]["quantity"] == 96
    assert any(e["type_id"] == 34201 for e in mj["expected_invention_materials"].values())
    assert mj["invention_procurement_materials"]["34201"]["quantity"] == 2
    assert out[5002]["manufacturing_job"]["invention_decryptor"]["type_id"] == 34201
```

Append to `tests/test_daily_planner_assignment.py`:

```python
def test_the_invent_action_names_the_producers_decryptor():
    plan = ChainPlan(decisions=[_decision(overview_row={
        "type_id": 12345, "needs_invention": True,
        "manufacturing_job": {"invention_decryptor": {"type_id": 34201, "type_name": "Accelerant Decryptor"}},
    })])
    (invent,) = [a for a in CharacterAssigner().assign(plan, [], _Chars(_research_pilot()), _AdminStub())
                 if a.action_type == "invent"]
    assert (invent.decryptor_type_id, invent.decryptor_type_name) == (34201, "Accelerant Decryptor")
    assert "Accelerant Decryptor" in invent.notes


def test_an_invent_action_without_a_decryptor_names_none():
    plan = ChainPlan(decisions=[_decision(overview_row={"type_id": 12345, "needs_invention": True})])
    (invent,) = [a for a in CharacterAssigner().assign(plan, [], _Chars(_research_pilot()), _AdminStub())
                 if a.action_type == "invent"]
    assert invent.decryptor_type_id is None
```

Append to `tests/test_industry_service_calculations.py` (daily planner shopping list):

```python
def test_the_planners_shopping_list_buys_one_decryptor_per_attempt() -> None:
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction
    from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import ShoppingListBuilder

    invent = AssignedAction(
        type_id=5002, type_name="T2 Module", action_type="invent", character_id=1, character_name="Pilot",
        quantity=None, runs=None, estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None,
        decryptor_type_id=34201, decryptor_type_name="Accelerant Decryptor",
    )
    blueprint_data = {
        1001: {"manufacturing": {"materials": [], "products": [{"type_id": 6001, "quantity": 1}]},
               "invention": {"materials": [{"type_id": 204, "type_name": "Datacore", "quantity": 2}],
                             "products": [{"type_id": 9002, "quantity": 10}]}},
        9002: {"manufacturing": {"materials": [], "products": [{"type_id": 5002, "quantity": 1}]}},
    }

    class _Resolver:
        def is_blueprint(self, type_id):
            return False

        def prefetch(self, type_ids):
            return None

    class _Admin:
        def get(self, section, key, default=None):
            return default

    items = ShoppingListBuilder().build(
        assigned_actions=[invent], corp_assets=[],
        market_depth_cache={204: {"vwap_5d": 100.0}, 34201: {"vwap_5d": 50.0}},
        admin_settings=_Admin(), blueprint_data=blueprint_data, meta_resolver=_Resolver(),
    )
    by_type = {i.type_id: i for i in items}
    assert by_type[204].quantity == 2
    assert by_type[34201].quantity == 1
    assert by_type[34201].shopping_category == "invention_input"
```

Append to `tests/test_industry_builder_t2_bpo_marker.py`:

```python
def test_the_drilldown_says_which_decryptor_and_chance_were_used():
    row = {"manufacturing_job": {
        "invention_decryptor": {"type_id": 34201, "type_name": "Accelerant Decryptor",
                                "reason": "Accelerant Decryptor: lowest cost per unit (85.88 ISK vs 89.00 ISK without a decryptor)"},
        "invention_chance": {"base_probability": 0.34, "skill_factor": 1.4083, "decryptor_multiplier": 1.2,
                             "effective_probability": 0.5746, "skills_unknown_reason": None},
    }}
    lines = ui.invention_summary_lines(row)
    assert lines[0] == "Decryptor: Accelerant Decryptor: lowest cost per unit (85.88 ISK vs 89.00 ISK without a decryptor)"
    assert lines[1] == "Invention chance 57.5%: SDE base 34.0% × skills 1.408 (best inventor) × decryptor 1.2"


def test_the_drilldown_says_when_skills_were_unknown():
    row = {"manufacturing_job": {"invention_chance": {
        "base_probability": 0.34, "skill_factor": None, "decryptor_multiplier": 1.0,
        "effective_probability": 0.34, "skills_unknown_reason": "no corp character has its skills loaded",
    }}}
    assert ui.invention_summary_lines(row) == [
        "Invention chance 34.0%: SDE base 34.0% × decryptor 1 (skills unknown: no corp character has its skills loaded)"
    ]
    assert ui.invention_summary_lines({"manufacturing_job": {}}) == []
```

Append to `tests/test_invention_model.py`:

```python
def test_the_decryptor_switch_is_an_admin_bool_defaulting_on():
    from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA

    spec = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["invention_decryptor_selection_enabled"]
    assert spec["type"] == "bool" and spec["default"] is True
```

- [ ] **Step 6: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_industry_service_calculations.py tests/test_daily_planner_assignment.py tests/test_industry_builder_t2_bpo_marker.py tests/test_invention_model.py -k "decryptor or bpc_boundary or unknown_invention_odds_compare or drilldown_says" -v`
Expected: FAIL. `KeyError: 'invention_decryptor'`, `TypeError: ... unexpected keyword argument 'decryptor_type_id'`, `AttributeError: ... invention_summary_lines`, `KeyError: 'invention_decryptor_selection_enabled'`.

- [ ] **Step 7: Implement the producer**

7a. Admin schema (`config/admin_settings.py`, after `invention_probability_floor`):

```python
            "invention_decryptor_selection_enabled": {
                "type": "bool",
                "default": True,
                "label": "Pick the best decryptor per invented item",
                "help": "Evaluate no decryptor and every T2 decryptor for each invented item and use the one "
                        "with the lowest cost per unit. Off: every invention is planned without a decryptor.",
            },
```

7b. `service.py` imports. Extend the `invention_model` import with `DECRYPTOR_SELECTION_OFF_REASON, NO_DECRYPTOR_DATA_REASON, DecryptorChoice, DecryptorEvaluation, decryptor_choice_payload, invented_runs_per_blueprint, quiet_warnings, select_best_decryptor`.

7c. `_ProductPlanningContext`, after the Task 1 fields:

```python
    decryptor_selection_enabled: bool = True
    #: {blueprint type_id: DecryptorChoice}, filled lazily by _decryptor_choice_for.
    decryptor_choice_by_blueprint_type_id: dict = dataclasses.field(default_factory=dict)
    #: Per-thread set of blueprints whose decryptors are being evaluated.
    decryptor_evaluation_guard: threading.local = dataclasses.field(default_factory=threading.local)
```

7d. `_build_planning_context`:
- Replace `decryptor_options = self._load_decryptor_options()` with:

```python
        decryptor_selection_enabled = bool(self._adm("industry", "invention_decryptor_selection_enabled", True))
        decryptor_options = self._load_decryptor_options() if decryptor_selection_enabled else (NO_DECRYPTOR,)
```

- Change `return _ProductPlanningContext(...)` to `ctx = _ProductPlanningContext(...)`, with the extra keyword `decryptor_selection_enabled=decryptor_selection_enabled,`. Then add:

```python
        # Every invented blueprint, top level or nested, uses its one best decryptor.
        ctx.invention_context.decryptor_for_blueprint = (
            lambda blueprint_type_id: self._decryptor_choice_for(ctx, blueprint_type_id).option
        )
        return ctx
```

7e. `_expected_invention_costs`. After the fallback `if runs_per_invented_blueprint <= 0: runs_per_invented_blueprint = max(1, max_production_limit or 1)`, add:

```python
        # A decryptor adds runs to every invented BPC (EVE).
        runs_per_invented_blueprint = invented_runs_per_blueprint(runs_per_invented_blueprint, decryptor)
```

Replace

```python
        per_attempt_materials = [dict(e) for e in (invention_job.get("materials") or []) if isinstance(e, dict)]
        invention_materials = [{**e, "quantity_per_attempt": int(e.get("quantity") or 0), "quantity": float(int(e.get("quantity") or 0)) * invention_attempts} for e in per_attempt_materials]
```

with

```python
        per_attempt_materials = [dict(e) for e in (invention_job.get("materials") or []) if isinstance(e, dict)]
        # The decryptor is consumed like a datacore, one per attempt, and priced
        # like one (take-or-buy against the hub price). The fee's EIV below stays
        # the datacores' (the fee model is unchanged).
        per_attempt_inputs = [*per_attempt_materials, *self._decryptor_input_entries(decryptor)]
        invention_materials = [{**e, "quantity_per_attempt": int(e.get("quantity") or 0), "quantity": float(int(e.get("quantity") or 0)) * invention_attempts} for e in per_attempt_inputs]
```

7f. Add next to `_invention_whole_attempt_requirements`, and change that method to a classmethod that also yields the decryptor:

```python
    @staticmethod
    def _decryptor_input_entries(decryptor: DecryptorOption) -> list[dict[str, Any]]:
        """The decryptor as an invention input, one per attempt; [] for none."""
        if decryptor.type_id is None:
            return []
        return [{
            "type_id": int(decryptor.type_id), "type_name": decryptor.type_name,
            "quantity": 1, "invention_decryptor": True,
        }]

    @classmethod
    def _invention_whole_attempt_requirements(
        cls, invention_job: dict[str, Any], whole_attempts: int, decryptor: DecryptorOption = NO_DECRYPTOR,
    ) -> list[dict[str, Any]]:
        """The invention inputs to actually buy or take: quantity_per_attempt x
        whole attempts, integer units (the amortized quantities are for cost
        only), the decryptor included. [] when the attempts are unknown or zero."""
        if whole_attempts <= 0:
            return []
        out: list[dict[str, Any]] = []
        for entry in [*(invention_job.get("materials") or []), *cls._decryptor_input_entries(decryptor)]:
            if not isinstance(entry, dict):
                continue
            per_attempt = int(entry.get("quantity") or 0)
            if int(entry.get("type_id") or 0) <= 0 or per_attempt <= 0:
                continue
            out.append({
                **dict(entry), "quantity": per_attempt * int(whole_attempts),
                "quantity_per_attempt": per_attempt, "whole_attempts": int(whole_attempts),
                "invention_input": True,
            })
        return out
```

Pass the decryptor at all four call sites:
- `_plan_nested_invention` (`decryptor=decryptor`);
- `_extra_batch_invention_requirements` (`decryptor=decryptor`, the parameter added in Task 2);
- the top-level batch-1 list in `_build_single_product_row` (`decryptor=decryptor`);
- the top-level extra list there (`decryptor=decryptor`).

7g. Add the selection machinery next to `_build_single_product_row`:

```python
    @staticmethod
    def _row_invents(ctx: _ProductPlanningContext, blueprint_type_id: int) -> bool:
        """Whether rows of this blueprint invent (in batch 1 or the later batches)."""
        source = ctx.invention_row_by_blueprint_type_id.get(int(blueprint_type_id))
        return (
            bool(ctx.build_from_bpc)
            and bool(source)
            and isinstance((source or {}).get("invention_job") or {}, dict)
            and not ctx.blueprint_original_assets_by_type_id.get(int(blueprint_type_id))
        )

    def _build_product_row(self, ctx: _ProductPlanningContext, **variant: Any) -> dict[str, Any] | None:
        """One overview row, invented with its blueprint's best decryptor."""
        blueprint_type_id = int(variant["row"].get("blueprint_type_id") or 0)
        if not self._row_invents(ctx, blueprint_type_id):
            return self._build_single_product_row(ctx, **variant)
        choice = self._decryptor_choice_for(ctx, blueprint_type_id)
        row_out = self._build_single_product_row(ctx, decryptor=choice.option, **variant)
        if row_out is not None:
            row_out["manufacturing_job"]["invention_decryptor"] = decryptor_choice_payload(choice)
        return row_out

    def _decryptor_choice_for(self, ctx: _ProductPlanningContext, blueprint_type_id: int) -> DecryptorChoice:
        """The decryptor to invent `blueprint_type_id` with: chosen once per
        refresh and memoized on ctx, by invention_model.select_best_decryptor."""
        blueprint_type_id = int(blueprint_type_id)
        cached = ctx.decryptor_choice_by_blueprint_type_id.get(blueprint_type_id)
        if cached is not None:
            return cached
        if not ctx.decryptor_selection_enabled:
            choice = DecryptorChoice(NO_DECRYPTOR, DECRYPTOR_SELECTION_OFF_REASON)
        elif len(ctx.decryptor_options) <= 1:
            choice = DecryptorChoice(NO_DECRYPTOR, NO_DECRYPTOR_DATA_REASON)
        else:
            evaluating = getattr(ctx.decryptor_evaluation_guard, "blueprint_type_ids", None)
            if evaluating is None:
                evaluating = set()
                ctx.decryptor_evaluation_guard.blueprint_type_ids = evaluating
            if blueprint_type_id in evaluating:
                # Met again inside its own evaluation (a material cycle): this
                # inner use gets no decryptor, and nothing is cached for it.
                return DecryptorChoice(NO_DECRYPTOR, "no decryptor: met inside its own decryptor evaluation")
            evaluating.add(blueprint_type_id)
            try:
                choice = self._evaluate_decryptors(ctx, blueprint_type_id)
            finally:
                evaluating.discard(blueprint_type_id)
        return ctx.decryptor_choice_by_blueprint_type_id.setdefault(blueprint_type_id, choice)

    def _evaluate_decryptors(self, ctx: _ProductPlanningContext, blueprint_type_id: int) -> DecryptorChoice:
        """Build one fully invented batch of the blueprint's own row per
        decryptor option and let select_best_decryptor pick. The builds are
        thrown away, so their WARNINGs are dropped (quiet_warnings); the row
        that is kept logs its own."""
        row = ctx.blueprint_row_by_type_id.get(blueprint_type_id) or {}
        manufacturing_job = row.get("manufacturing_job") or {}
        products = [
            p for p in (manufacturing_job.get("products") or [])
            if isinstance(p, dict) and int(p.get("type_id") or 0) > 0
        ] if isinstance(manufacturing_job, dict) else []
        if not products:
            return DecryptorChoice(NO_DECRYPTOR, "no decryptor: the blueprint makes no product to compare on")
        limit = int(manufacturing_job.get("max_production_limit") or 0)
        runs = limit if bool(ctx.maximize_bp_runs) and limit > 0 else 1
        evaluations: list[DecryptorEvaluation] = []
        with quiet_warnings():
            for option in ctx.decryptor_options:
                skip_reason = self._decryptor_price_skip_reason(ctx, option)
                if skip_reason is not None:
                    evaluations.append(DecryptorEvaluation(option, None, skip_reason))
                    continue
                candidate = self._build_single_product_row(
                    ctx, row=row, raw_product=products[0], row_index=0, product_index=0,
                    blueprint_copy_assets=[], blueprint_original_asset=None,
                    effective_runs=runs, decryptor=option, fully_invented=True,
                )
                evaluations.append(DecryptorEvaluation(option, self._row_cost_per_unit_estimate(candidate)))
        return select_best_decryptor(evaluations)

    @classmethod
    def _decryptor_price_skip_reason(cls, ctx: _ProductPlanningContext, option: DecryptorOption) -> str | None:
        """Why a decryptor cannot be a candidate: it has no hub market price.
        An adjusted-price fallback would make an unbuyable decryptor look cheap."""
        if option.type_id is None:
            return None
        unit_price = cls._as_float((ctx.material_price_map.get(int(option.type_id)) or {}).get("unit_price"))
        if unit_price is None or unit_price <= 0:
            return f"no market price for {option.type_name}"
        return None

    @classmethod
    def _row_cost_per_unit_estimate(cls, row_out: dict[str, Any] | None) -> float | None:
        """A row's total cost per unit at plan time: every procurement and
        amortized invention line total, plus the job fees (no fee priced at all
        reads as 0, the same for every candidate of one row). None when any
        line is unpriced or the invention cost is unknown: a partial cost
        cannot be compared."""
        if not isinstance(row_out, dict):
            return None
        mj = row_out.get("manufacturing_job") or {}
        quantity = int(row_out.get("quantity") or 0)
        if quantity <= 0 or mj.get("invention_cost_unknown_reason"):
            return None
        total = 0.0
        for key in ("procurement_materials", "expected_invention_materials"):
            for entry in (mj.get(key) or {}).values():
                line_total = cls._as_float(entry.get("line_total")) if isinstance(entry, dict) else None
                if line_total is None:
                    return None
                total += line_total
        total += float(mj.get("total_job_cost") or 0.0)
        return total / quantity
```

7h. `_build_single_product_row`. Add the parameter `fully_invented: bool = False`. After the per-variant `if bool(ctx.build_from_bpc): ...` copy-run setup, add:

```python
        if fully_invented:
            # Decryptor evaluation: one batch invented in full, whatever BPCs are owned.
            available_blueprint_copy_runs_by_type_id[blueprint_type_id] = 0
```

7i. In `industry_manufacturing_product_overview`, `_build_variant` calls `self._build_product_row(` instead of `self._build_single_product_row(` (same keyword arguments).

- [ ] **Step 8: Implement the planner and the UI**

8a. `overview_row.py`:

```python
def get_invention_decryptor(row: dict[str, Any]) -> tuple[int, str] | None:
    """(type_id, name) of the decryptor the producer chose for this row's
    invention (manufacturing_job.invention_decryptor), or None for none."""
    choice = get_manufacturing_job(row).get("invention_decryptor")
    if not isinstance(choice, dict):
        return None
    type_id = _as_int(choice.get("type_id"))
    if type_id <= 0:
        return None
    return type_id, str(choice.get("type_name") or type_id)
```

8b. `models.py`, `AssignedAction`, after `estimated_build_cost_isk`:

```python
    # invent only: the decryptor the producer chose (one per attempt), or None.
    decryptor_type_id: int | None = None
    decryptor_type_name: str | None = None
```

8c. `character_assigner.py`, invent branch: before `actions.append(AssignedAction(... action_type="invent" ...))`, add `decryptor = orow.get_invention_decryptor(row)`. Add the arguments

```python
                    decryptor_type_id=decryptor[0] if decryptor else None,
                    decryptor_type_name=decryptor[1] if decryptor else None,
```

and make the notes

```python
                    notes=(
                        "Invention: BPC stock below pipeline threshold"
                        + (f"; use {decryptor[1]}" if decryptor else "")
                    ),
```

8d. `shopping_list_builder.py`. Change the comment `# Invention inputs (datacores). Decryptors are optional ...` to `# Invention inputs: the datacores, plus the decryptor the producer chose (one per attempt).`. After `inv_mats = self._get_invention_materials(...)`, add:

```python
            if action.decryptor_type_id:
                inv_mats = [*inv_mats, {
                    "type_id": int(action.decryptor_type_id),
                    "type_name": action.decryptor_type_name or f"type_{action.decryptor_type_id}",
                    "quantity": 1,
                }]
```

8e. `industry_builder_ui.py`, next to `row_assumes_owned_t2_bpo`:

```python
def invention_summary_lines(row: dict[str, Any]) -> list[str]:
    """Drilldown captions: which decryptor the producer chose and why, and the
    invention chance it assumed. [] for a row that does not invent."""
    manufacturing_job = get_manufacturing_job(row)
    lines: list[str] = []
    decryptor = manufacturing_job.get("invention_decryptor")
    if isinstance(decryptor, dict) and decryptor.get("reason"):
        lines.append(f"Decryptor: {decryptor['reason']}")
    chance = manufacturing_job.get("invention_chance")
    if isinstance(chance, dict) and chance.get("effective_probability") is not None:
        head = (
            f"Invention chance {float(chance['effective_probability']):.1%}: "
            f"SDE base {float(chance.get('base_probability') or 0.0):.1%}"
        )
        multiplier = f"{float(chance.get('decryptor_multiplier') or 1.0):g}"
        if chance.get("skill_factor") is None:
            lines.append(f"{head} × decryptor {multiplier} (skills unknown: {chance.get('skills_unknown_reason')})")
        else:
            lines.append(f"{head} × skills {float(chance['skill_factor']):.3f} (best inventor) × decryptor {multiplier}")
    return lines
```

8f. `industry_builder.py`. Add `invention_summary_lines` to the `from streamlit_ui.state.industry_builder_ui import row_assumes_owned_t2_bpo, ...` import. After `if row_assumes_owned_t2_bpo(selected_row): st.caption(ASSUMES_OWNED_T2_BPO_CAPTION)`, add:

```python
        for line in invention_summary_lines(selected_row):
            st.caption(line)
```

- [ ] **Step 9: Run the new tests**

Run: the command from Step 6.
Expected: PASS.

- [ ] **Step 10: Run the full suite**

Run: `.venv312/bin/python -m pytest -q`
Expected: all pass. The harness loads no decryptors, so only the tests that stub `_load_decryptor_options` see decryptors. Every other number is unchanged. The test fixture is decryptor-free on purpose; the overview row gains `invention_decryptor` on inventing rows only.

- [ ] **Step 11: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/invention_model.py \
  src/eve_online_industry_tracker/application/industry/service.py \
  src/eve_online_industry_tracker/application/industry/overview_row.py \
  src/eve_online_industry_tracker/config/admin_settings.py \
  src/eve_online_industry_tracker/application/daily_planner/models.py \
  src/eve_online_industry_tracker/application/daily_planner/character_assigner.py \
  src/eve_online_industry_tracker/application/daily_planner/shopping_list_builder.py \
  src/streamlit_ui/state/industry_builder_ui.py src/streamlit_ui/pages/industry_builder.py \
  tests/test_invention_model.py tests/test_industry_service_calculations.py \
  tests/test_daily_planner_assignment.py tests/test_industry_builder_t2_bpo_marker.py
git commit -m "feat: best decryptor per invented item (lowest cost per unit), decryptors bought per attempt

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: A partly owned BPC batch is two exactly sized jobs (D3, part of C1)

Per D3, when an owned BPC covers only part of a job's runs:
- the owned runs are one job at the owned BPC's ME/TE;
- the rest is a second job, either on an invented BPC (ME/TE from Task 1 with the blueprint's decryptor) or on a copy of the owned BPO (the BPO's ME/TE);
- materials and time are rounded per job, as EVE does.

**Nested (reachable).** Today a partly owned nested BPC without a BPO builds only the owned runs and buys the rest (service.py:4182-4185). A partly owned nested BPC with a BPO sizes every run at the BPC's ME. With D3, the remaining runs are invented (the nested invention at :4726 already sizes `missing_blueprint_copy_runs`) or copied, as a second job.

**Top level (exact for direct callers).** The overview never produces a partly owned top-level batch (see the verification section). The same two-job sizing is still applied in `_build_single_product_row`, and the row publishes `manufacturing_job.job_segments`, so the batch is exact whoever calls it.

**Files:**
- Modify: `application/industry/service.py`. Changes:
  - new `_manufacturing_job_segments`;
  - `_build_single_product_row`: move the owned-runs block :7359-7363 up, size materials and time per segment (:7296-7335), and add `job_segments` to the output;
  - `_plan_blueprint_chain_for_quantity`: the owned-copy branch :4176-4185, materials :4214-4262.
- Test: `tests/test_industry_service_calculations.py`

**Interfaces:**
- Consumes: Task 1 `invented_blueprint_efficiency`, `InventionContext.decryptor_for_blueprint`; Task 3 `_build_single_product_row(decryptor=...)`.
- Produces:
  - `IndustryService._manufacturing_job_segments(*, effective_runs, owned_copy_runs_used, blueprint_source_kind, material_efficiency, time_efficiency, second_source) -> list[dict]`;
  - row key `manufacturing_job.job_segments: list[{"runs", "blueprint_material_efficiency", "blueprint_time_efficiency", "source", "material_reduction", "time_reduction", "duration_seconds", "materials": {str(type_id): int}}]`;
  - the new source value `"invented_blueprint_copy"` (key `source`, redacted by the fixture sanitiser like every non-allow-listed string).

- [ ] **Step 1: Write the failing tests**

In `tests/test_industry_service_calculations.py`, change `test_partly_owned_bpc_amortizes_only_the_missing_runs`. After the invention asserts, the materials become two jobs:

```python
    # 4 runs from the owned BPC, 6 invented: 2 x 100 x 6 / (0.5 x 10) = 240.
    assert mj["expected_invention_materials"]["204"]["invented_runs"] == 6
    assert mj["invention_material_cost"] == pytest.approx(240.0)
    # D3: two jobs. 4 runs at the owned ME 0 = 40 Tritanium; 6 runs on the
    # invented BPC at ME 2 = ceil(60 x 0.98) = 59. Was 100 at ME 0 for all 10.
    assert mj["materials"]["34"]["quantity"] == 99
    assert mj["material_cost"] == pytest.approx(99 * 5 + 240.0)
    segments = mj["job_segments"]
    assert [(s["runs"], s["blueprint_material_efficiency"], s["source"]) for s in segments] == [
        (4, 0, "owned_blueprint_copy"), (6, 2, "invented_blueprint_copy"),
    ]
    assert [s["materials"]["34"] for s in segments] == [40, 59]
    # Time per job: 4 x 100 s at TE 0, 6 x 100 s at TE 4 = 576.
    assert mj["manufacturing_time_seconds"] == 400 + 576
```

Append:

```python
# --- round 5 Task 4: a partly owned BPC batch is two jobs (D3) ----------------


def _owned_t2_bpo(*, me: int, te: int, item_id: int = 95) -> SimpleNamespace:
    return SimpleNamespace(
        type_id=9002, item_id=item_id, is_blueprint_copy=False,
        blueprint_material_efficiency=me, blueprint_time_efficiency=te, blueprint_runs=None,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )


def test_the_runs_past_an_owned_bpc_are_copied_from_the_bpo_at_its_me(monkeypatch) -> None:
    service = _invention_service(monkeypatch)
    bpc = _owned_t2_bpc(runs=4, item_id=96)
    bpo = _owned_t2_bpo(me=10, te=20)
    service._get_owned_blueprint_assets = MethodType(  # type: ignore[attr-defined]
        lambda self, *, owned_blueprints_scope: ([bpc], [bpo], {}, {}, {}), service,
    )
    ctx = _planning_context(service)
    t2_row = _invented_t2_blueprint_rows()[1]
    row = service._build_single_product_row(
        ctx, row=t2_row, raw_product=t2_row["manufacturing_job"]["products"][0],
        row_index=1, product_index=1, blueprint_copy_assets=[bpc],
        blueprint_original_asset=None, effective_runs=10,
    )
    assert row is not None
    mj = row["manufacturing_job"]

    # 4 runs at ME 0 = 40; 6 copied runs at the BPO's ME 10 = ceil(60 x 0.9) = 54.
    assert mj["materials"]["34"]["quantity"] == 94
    assert [s["source"] for s in mj["job_segments"]] == ["owned_blueprint_copy", "copied_from_owned_blueprint_original"]


def test_a_fully_covered_batch_is_one_job(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}))[5002]["manufacturing_job"]
    assert len(mj["job_segments"]) == 1
    assert mj["job_segments"][0]["materials"] == {"34": 10}


def test_a_nested_partly_owned_bpc_invents_the_rest_as_a_second_job(monkeypatch) -> None:
    # 10 parent runs need 10 components. A 4-run owned 9002 BPC (no BPO)
    # builds 4 at ME 0, and the other 6 are a second job on an invented BPC
    # (was: bought). Tritanium 40 + ceil(60 x 0.98) = 59.
    # Invention for 6 runs: ceil(6 / 10) = 1 BPC, ceil(1 / 0.5) = 2 attempts, 4 datacores.
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=4)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]

    assert mj["recursive_activity_breakdown"]["manufacturing:5002"]["quantity"] == 10
    assert "5002" not in mj["procurement_materials"]
    assert mj["procurement_materials"]["34"]["quantity"] == 99
    assert mj["invention_procurement_materials"]["204"]["quantity"] == 4


def test_a_nested_partly_owned_bpc_with_a_bpo_copies_the_rest_at_the_bpo_me(monkeypatch) -> None:
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=4)], [_owned_t2_bpo(me=10, te=20)], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]

    assert mj["procurement_materials"]["34"]["quantity"] == 94  # 40 + ceil(60 x 0.9)
    assert "invention_procurement_materials" not in mj
```

Delete `test_nested_partly_owned_bpc_invents_the_built_runs_for_later_batches`. It pins the old "build 4, buy 6" split, which `test_a_nested_partly_owned_bpc_invents_the_rest_as_a_second_job` replaces. Task 5 re-pins its later-batch half.

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_industry_service_calculations.py -k "partly_owned or past_an_owned_bpc or fully_covered_batch_is_one_job" -v`
Expected: FAIL with `KeyError: 'job_segments'` and the old quantities (100, 100, `5002` still bought, 100).

- [ ] **Step 3: Implement the top level**

3a. Add the static helper next to `_plan_activity_costs`:

```python
    @staticmethod
    def _manufacturing_job_segments(
        *,
        effective_runs: int,
        owned_copy_runs_used: int,
        blueprint_source_kind: str,
        material_efficiency: int,
        time_efficiency: int,
        second_source: tuple[int, int, str] | None,
    ) -> list[dict[str, Any]]:
        """The batch's manufacturing jobs, one per blueprint source (D3).

        An owned BPC covering part of the batch is one job at its own ME/TE for
        its runs, plus a second job for the rest on `second_source` = (ME, TE,
        source): a copy of the owned BPO, or an invented BPC. Every other batch
        is one job. Materials and time are rounded per job, as EVE does.
        """
        if (
            blueprint_source_kind == "owned_blueprint_copy"
            and second_source is not None
            and 0 < int(owned_copy_runs_used) < int(effective_runs)
        ):
            second_me, second_te, second_kind = second_source
            return [
                {"runs": int(owned_copy_runs_used), "blueprint_material_efficiency": int(material_efficiency),
                 "blueprint_time_efficiency": int(time_efficiency), "source": "owned_blueprint_copy"},
                {"runs": int(effective_runs) - int(owned_copy_runs_used), "blueprint_material_efficiency": int(second_me),
                 "blueprint_time_efficiency": int(second_te), "source": second_kind},
            ]
        return [{"runs": int(effective_runs), "blueprint_material_efficiency": int(material_efficiency),
                 "blueprint_time_efficiency": int(time_efficiency), "source": blueprint_source_kind}]
```

3b. In `_build_single_product_row`:
- Move these lines (now under `# BPC allocation + prerequisite handling`) up to directly after the blueprint source resolution `if/else`, deleting the originals:

```python
        available_target_copy_runs = int(available_blueprint_copy_runs_by_type_id.get(blueprint_type_id, 0)) if bool(ctx.build_from_bpc) else 0
        owned_target_copy_runs_used = min(max(0, available_target_copy_runs), effective_runs) if bool(ctx.build_from_bpc) else 0
        if bool(ctx.build_from_bpc):
            available_blueprint_copy_runs_by_type_id[blueprint_type_id] = max(0, available_target_copy_runs - owned_target_copy_runs_used)
        missing_top_level_copy_runs = max(0, effective_runs - owned_target_copy_runs_used) if bool(ctx.build_from_bpc) else 0
```

- Replace everything from `mfg_costs = self._plan_activity_costs(` through the end of the `# Material adjustment` loop with:

```python
        # D3: a batch partly covered by an owned BPC is two jobs, each sized
        # and timed at its own blueprint's ME/TE.
        second_source: tuple[int, int, str] | None = None
        if matched_blueprint_originals:
            bpo = matched_blueprint_originals[0]
            second_source = (
                int(bpo.blueprint_material_efficiency or 0), int(bpo.blueprint_time_efficiency or 0),
                "copied_from_owned_blueprint_original",
            )
        elif has_top_level_invention_path:
            second_source = (*invented_blueprint_efficiency(decryptor), "invented_blueprint_copy")
        job_segments = self._manufacturing_job_segments(
            effective_runs=effective_runs, owned_copy_runs_used=owned_target_copy_runs_used,
            blueprint_source_kind=blueprint_source_kind,
            material_efficiency=blueprint_material_efficiency, time_efficiency=blueprint_time_efficiency,
            second_source=second_source,
        )
        base_manufacturing_time = int(manufacturing_job.get("time_seconds") or 0)
        segment_costs = [
            self._plan_activity_costs(
                ctx, activity="manufacturing", base_time_seconds=base_manufacturing_time,
                runs=int(segment["runs"]), process_value=None,  # process_value computed below
                manufacturing_group=manufacturing_group,
                required_skill_entries=manufacturing_skill_entry_list,
                blueprint_material_efficiency=int(segment["blueprint_material_efficiency"]),
                blueprint_time_efficiency=int(segment["blueprint_time_efficiency"]),
            )
            for segment in job_segments
        ]
        for segment, segment_cost in zip(job_segments, segment_costs):
            segment["material_reduction"] = float(segment_cost.get("material_reduction", 0.0))
            segment["time_reduction"] = float(segment_cost["time_reduction"])
            segment["duration_seconds"] = self._round_duration_seconds(
                base_manufacturing_time * int(segment["runs"]) * max(0.0, 1.0 - segment["time_reduction"])
            )
            segment["materials"] = {}
        mfg_costs = segment_costs[0]
        manufacturing_material_reduction = mfg_costs.get("material_reduction", 0.0)
        manufacturing_time_reduction = mfg_costs["time_reduction"]
        manufacturing_cost_reduction = mfg_costs["cost_reduction"]

        # Material adjustment, per job (one ceiling per job, never per run).
        base_material_entries: list[dict[str, Any]] = []
        adjusted_material_entries: list[dict[str, Any]] = []
        for raw_mat in [dict(e) for e in (manufacturing_job.get("materials") or [])]:
            mat = self._compact_material_entry(raw_mat)
            qty_per_run = int(mat.get("quantity") or 0)
            base_total = qty_per_run * effective_runs
            adjusted_total = 0
            for segment in job_segments:
                segment_runs = int(segment["runs"])
                segment_quantity = self._round_material_quantity(
                    float(qty_per_run * segment_runs) * max(0.0, 1.0 - segment["material_reduction"]),
                    minimum_quantity=(segment_runs if qty_per_run > 0 else 0),
                )
                if segment_quantity > 0:
                    segment["materials"][str(int(mat.get("type_id") or 0))] = segment_quantity
                adjusted_total += segment_quantity
            mat["quantity_per_run"] = qty_per_run
            mat["base_quantity"] = base_total
            mat["quantity"] = adjusted_total
            mat["material_reduction"] = manufacturing_material_reduction
            adjusted_material_entries.append(mat)
            base_material_entries.append({**mat, "quantity": base_total})
```

- Replace `direct_manufacturing_time_seconds = self._round_duration_seconds(int(manufacturing_job.get("time_seconds") or 0) * effective_runs * max(0.0, 1.0 - manufacturing_time_reduction))` with:

```python
        direct_manufacturing_time_seconds = sum(int(segment["duration_seconds"]) for segment in job_segments)
```

- In the `"manufacturing_job": {...}` output, after `"runs": effective_runs,`, add `"job_segments": job_segments,`.

For a one-job batch every number is identical to before: one segment, the same reduction, the same ceiling and the same duration rounding.

- [ ] **Step 4: Implement the nested two jobs**

In `_plan_blueprint_chain_for_quantity`:
- Add `second_job: tuple[int, int, int] | None = None  # (runs, ME, TE) of a D3 second job` next to `owned_copy_runs_used = 0`.
- Replace

```python
                    if missing_blueprint_copy_runs > 0 and bool(matched_blueprint_originals):
                        include_copying_job = True
                    elif missing_blueprint_copy_runs > 0:
```

with

```python
                    if missing_blueprint_copy_runs > 0 and bool(matched_blueprint_originals):
                        include_copying_job = True
                        # D3: the copied runs are a second job at the BPO's ME/TE.
                        bpo = matched_blueprint_originals[0]
                        second_job = (
                            missing_blueprint_copy_runs,
                            int(bpo.blueprint_material_efficiency or 0), int(bpo.blueprint_time_efficiency or 0),
                        )
                    elif missing_blueprint_copy_runs > 0 and has_invention_path:
                        # D3: the runs past the owned BPC are a second job on an
                        # invented BPC (invented below), no longer bought.
                        second_job = (
                            missing_blueprint_copy_runs,
                            *invented_blueprint_efficiency(invention_context.decryptor_for_blueprint(blueprint_type_id)),
                        )
                    elif missing_blueprint_copy_runs > 0:
```

- After `cost_reduction = _main_act["cost_reduction"]`, add:

```python
        # (runs, material reduction, time reduction) per job: one job, or the
        # owned-BPC job plus the D3 second job.
        jobs = [(runs, material_reduction, time_reduction)]
        if second_job is not None:
            _second_act = self._compute_activity_job(
                activity=activity, base_time_seconds=0, runs=0, process_value=None,
                selected_industry_profile=selected_industry_profile,
                selected_character_modifiers=selected_character_modifiers,
                character_skill_levels_by_name=character_skill_levels_by_name,
                manufacturing_group=manufacturing_group, required_skill_entries=required_skill_entries,
                blueprint_material_efficiency=second_job[1], blueprint_time_efficiency=second_job[2],
                installation_surcharge=None,
            )
            jobs = [
                (runs - second_job[0], material_reduction, time_reduction),
                (second_job[0], _second_act.get("material_reduction", 0.0), _second_act["time_reduction"]),
            ]
```

- Replace the `adjusted_total_quantity = self._round_material_quantity(...)` statement in the materials loop with:

```python
            adjusted_total_quantity = sum(
                self._round_material_quantity(
                    float(quantity_per_run * job_runs) * max(0.0, 1.0 - job_material_reduction),
                    minimum_quantity=(job_runs if quantity_per_run > 0 else 0),
                )
                for job_runs, job_material_reduction, _job_time_reduction in jobs
            )
```

- Replace the `direct_duration_seconds = self._round_duration_seconds(...)` statement with:

```python
        direct_duration_seconds = sum(
            self._round_duration_seconds(int(job.get("time_seconds") or 0) * job_runs * max(0.0, 1.0 - job_time_reduction))
            for job_runs, _job_material_reduction, job_time_reduction in jobs
        )
```

The nested invention block (`if activity == "manufacturing" and missing_blueprint_copy_runs > 0 and not matched_blueprint_originals:`) now runs for the D3 invented second job, because `missing_blueprint_copy_runs` stays positive. It invents exactly those runs.

- [ ] **Step 5: Run the new tests**

Run: the command from Step 2.
Expected: PASS.

- [ ] **Step 6: Run the full suite**

Run: `.venv312/bin/python -m pytest -q`
Expected: all pass. A failing older test whose nested fixture has a partly owned BPC must be re-derived by hand as two jobs (owned runs at the BPC ME, the rest invented or copied). Write the derivation in its comment.

- [ ] **Step 7: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/service.py tests/test_industry_service_calculations.py
git commit -m "fix: a partly owned BPC batch is two jobs sized at their own ME/TE (D3)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Batches 2..N from one extra-batch plan at the invented ME (C1, FU-81b, audit task K)

The shopping list multiplies batch 1's `procurement_materials` by every batch (shopping_list.py:170). Batch 1 can run on owned BPC runs at their ME, top level or nested, but batches 2..N have no owned BPC runs left, so they must be sized at the invented ME with that blueprint's decryptor. When the owned BPC's ME is above the invented ME, this under-buys.

The round-3/4 fix for datacores repeats batch 1's build/buy split (`_extra_batch_invention_requirements`, `invention_requirements_per_extra_batch` threaded through both recursive functions). That cannot be made consistent with re-sized materials: a later batch may build a component that batch 1 bought, or the reverse.

Replacement: when batch 1 spent any owned BPC runs, the producer plans one extra batch with no owned BPC runs and no stock:
- the row's own job at the invented ME (or batch 1's ME for a non-invented row);
- every sub-build planned afresh;
- the datacores that plan needs.

The row publishes `procurement_materials_per_extra_batch` and takes `invention_procurement_materials_per_extra_batch` from the same plan. The shopping list uses batch 1 × 1 + extra × (N − 1). When nothing owned was spent, every later batch repeats batch 1, so no extra material list is written, and the extra datacore list is batch 1's as pure requirements (unchanged behaviour). The extra plan runs inside `quiet_warnings()`. The row's single "datacore need unknown" WARNING stays.

One behaviour change follows from this. A later batch whose nested invention odds are unknown now buys that component, the same rule batch 1 uses (FU-60). Before, it flagged the row's datacores as unknown. Two tests are rewritten for that.

**Files:**
- Modify: `application/industry/service.py`. Changes:
  - new `_plan_extra_batch`, `_blueprint_copy_runs_spent`;
  - `_build_single_product_row`: replace :7519-7587, extend the output;
  - remove `_extra_batch_invention_requirements` and the nested extra-batch block (:4836-4849);
  - drop `invention_requirements_per_extra_batch` / `extra_batch_invention_unknown_reasons` from `_plan_blueprint_chain_for_quantity`, `_build_recursive_prerequisite_plan` and `_absorb_nested_invention`.
- Modify: `application/industry/shopping_list.py` (`_row_lines_with_batch_counts` :155-177, module docstring)
- Test: `tests/test_industry_service_calculations.py`, `tests/test_shopping_list.py`

**Interfaces:**
- Consumes: Tasks 1–4 (`invented_blueprint_efficiency`, `quiet_warnings`, `_full_batch_invention_whole_attempts(..., decryptor, invention_context) -> (attempts, reason, chance)`, `_invention_whole_attempt_requirements(..., decryptor)`, `job_segments`).
- Produces:
  - `IndustryService._plan_extra_batch(ctx, *, manufacturing_job, blueprint_type_id, effective_runs, manufacturing_group, required_skill_entries, blueprint_material_efficiency, blueprint_time_efficiency, invents_top_level, invention_job, decryptor, max_production_limit) -> dict` with keys `procurement_materials: list`, `invention_requirements: list`, `unknown_reasons: list[str]`;
  - `IndustryService._blueprint_copy_runs_spent(before: dict, after: dict) -> bool`;
  - row key `manufacturing_job.procurement_materials_per_extra_batch: dict` (pure requirements, no take);
  - `shopping_list._EXTRA_BATCH_MATERIALS_KEY = "procurement_materials_per_extra_batch"`.
- Removes: `_extra_batch_invention_requirements`, and the plan keys `invention_requirements_per_extra_batch` and `extra_batch_invention_unknown_reasons`. Nothing outside service.py reads them (grep).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_shopping_list.py`:

```python
# --- round 5 Task 5: batches 2..N have their own material list ------------------


def test_an_extra_batch_material_list_replaces_batch_one_for_later_batches() -> None:
    row = _make_row(1001, "Item T2", 3, {"34": _owned_line(95, 0)})
    row["manufacturing_job"]["procurement_materials_per_extra_batch"] = {"34": _owned_line(98, 0)}
    assert _line(aggregate_shopping_list([row])) == (95 + 2 * 98, 95 + 2 * 98)


def test_the_extra_batch_material_list_is_ignored_for_a_single_batch() -> None:
    row = _make_row(1001, "Item T2", 1, {"34": _owned_line(95, 0)})
    row["manufacturing_job"]["procurement_materials_per_extra_batch"] = {"34": _owned_line(98, 0)}
    assert _line(aggregate_shopping_list([row])) == (95, 95)


def test_extra_batch_materials_net_owned_stock_once() -> None:
    row = _make_row(1001, "Item T2", 3, {"34": _owned_line(95, 10)})
    row["manufacturing_job"]["procurement_materials_per_extra_batch"] = {"34": _owned_line(98, 0)}
    assert _line(aggregate_shopping_list([row])) == (291, 281)
```

Append to `tests/test_industry_service_calculations.py`:

```python
# --- round 5 Task 5: batches 2..N from one extra-batch plan (C1) ----------------


def _owned_t2_bpc_me(*, runs: int, me: int, item_id: int = 97) -> SimpleNamespace:
    bpc = _owned_t2_bpc(runs=runs, item_id=item_id)
    bpc.blueprint_material_efficiency = me
    return bpc


def test_later_batches_are_sized_at_the_invented_me_not_the_owned_bpcs(monkeypatch) -> None:
    # A 10-run ME 5 BPC covers batch 1: ceil(100 x 0.95) = 95 Tritanium.
    # Batches 2..N invent at ME 2: ceil(100 x 0.98) = 98 each (was 95, an under-buy).
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, owned_assets=([_owned_t2_bpc_me(runs=10, me=5)], [], {}, {}, {}),
    )[5002]
    mj = row["manufacturing_job"]
    assert mj["materials"]["34"]["quantity"] == 95
    extra = mj["procurement_materials_per_extra_batch"]["34"]
    assert extra["quantity"] == 98
    assert int(extra.get("take_quantity") or 0) == 0
    # 10 runs / 10 per BPC = 1 BPC, 2 whole attempts, 4 datacores per later batch.
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4

    row["max_batches_total"] = 3
    line = _shopping_line(row, 34)
    assert line is not None and (line["need"], line["buy"]) == (95 + 2 * 98, 95 + 2 * 98)


def test_nothing_owned_spent_writes_no_extra_material_list(monkeypatch) -> None:
    mj = _invention_overview(monkeypatch, maximize_bp_runs=True)[5002]["manufacturing_job"]
    assert "procurement_materials_per_extra_batch" not in mj
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4


def test_later_batches_rebuild_a_nested_component_without_the_owned_bpc(monkeypatch) -> None:
    # Batch 1 builds the component on an owned 10-run BPC. Later batches
    # invent it: Tritanium at ME 2 (1 run: 10) and 4 datacores, no component bought.
    row = _invention_overview(
        monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(), prices=_NESTED_FAKE_PRICES,
        owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]
    extra = mj["procurement_materials_per_extra_batch"]
    assert extra["34"]["quantity"] == 10
    assert "5002" not in extra
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4


def test_later_batches_of_a_partly_owned_nested_bpc_invent_every_run(monkeypatch) -> None:
    # Replaces the round-4 "invents the built runs" test. Batch 1: 4 runs on
    # the owned BPC + 6 invented (Task 4). Later batches: all 10 invented,
    # 1 BPC, 2 attempts, 4 datacores; Tritanium ceil(100 x 0.98) = 98.
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_nested_invention_blueprint_rows(),
        prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=4)], [], {}, {}, {}),
    )[7100]
    mj = row["manufacturing_job"]
    assert mj["procurement_materials_per_extra_batch"]["34"]["quantity"] == 98
    assert mj["invention_procurement_materials_per_extra_batch"]["204"]["quantity"] == 4

    row["max_batches_total"] = 3
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (4 + 2 * 4, 4 + 2 * 4)
```

Rewrite `test_unknown_odds_with_nested_owned_bpc_covering_batch_one_flag_the_row` as:

```python
def test_unknown_odds_with_nested_owned_bpc_covering_batch_one_buy_the_component_later(monkeypatch, caplog) -> None:
    # Batch 1 builds the component on the owned BPC (a known cost). Later
    # batches would have to invent it with unknown odds, so they buy it, the
    # same rule as batch 1 (FU-60): no datacore guess and no flag.
    with caplog.at_level("WARNING"):
        row = _invention_overview(
            monkeypatch, blueprint_rows=_nested_invention_blueprint_rows(probability_pct=0.0),
            prices=_NESTED_FAKE_PRICES, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
            real_confidence=True, adm_overrides={"invention_probability_floor": 0.0},
        )[7100]
    mj = row["manufacturing_job"]

    assert mj["recursive_activity_breakdown"]["manufacturing:5002"]["recommended_action"] == "build"
    assert mj["procurement_materials_per_extra_batch"]["5002"]["quantity"] == 1
    assert "invention_datacore_need_unknown" not in mj
    assert "invention_cost_unknown_reason" not in mj
    assert not any(m.startswith("Product 7100: datacore need unknown")
                   for m in (r.getMessage() for r in caplog.records if r.levelname == "WARNING"))

    row["max_batches_total"] = 3
    line = _shopping_line(row, 5002)
    assert line is not None and line["need"] == 2
    assert _unknown_need_rows([row]) == {}
```

Delete `test_nested_extra_batch_unknown_logs_one_warning`. Its scenario no longer flags anything, and the rewritten test above asserts that no row WARNING appears.

Add one assertion to `test_nested_owned_bpc_in_batch_one_still_buys_nested_datacores_for_later_batches`, after the `extra = ...` assertion:

```python
    assert mj["procurement_materials_per_extra_batch"]["34"]["quantity"] == 10
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_shopping_list.py tests/test_industry_service_calculations.py -k "extra_batch or later_batches or nothing_owned_spent or buy_the_component_later or nested_owned_bpc_in_batch_one" -v`
Expected: FAIL. `KeyError: 'procurement_materials_per_extra_batch'`, and the shopping list counts 95 × 3.

- [ ] **Step 3: Implement the shopping list**

In `shopping_list.py`, add `_EXTRA_BATCH_MATERIALS_KEY = "procurement_materials_per_extra_batch"` next to `_EXTRA_BATCH_INVENTION_KEY`. Replace `_row_lines_with_batch_counts` with:

```python
def _row_lines_with_batch_counts(mj: dict[str, Any], batches: int) -> list[tuple[Any, int]]:
    """Each material line of a row with the number of batches it is needed for.

    procurement_materials: batch 1's manufacturing buy list. When the row
    carries procurement_materials_per_extra_batch (batch 1 ran on owned BPC
    runs that later batches no longer have), that list is batches 2..N;
    otherwise batch 1's list counts for every batch.
    invention_procurement_materials: the invention inputs (whole attempts) for
    batch 1, after the owned T2 BPC runs; absent when they cover batch 1.
    invention_procurement_materials_per_extra_batch: the invention inputs for
    one later batch. An overview cached before that list existed lacks the
    key, and batch 1's invention list then counts for every batch.
    Both extra lists take nothing from stock (the stock is netted once).
    """
    def lines(key: str) -> list[Any]:
        value = mj.get(key)
        return list(value.values()) if isinstance(value, dict) else []

    out: list[tuple[Any, int]] = []
    if isinstance(mj.get(_EXTRA_BATCH_MATERIALS_KEY), dict):
        out.extend((mat, 1) for mat in lines("procurement_materials"))
        if batches > 1:
            out.extend((mat, batches - 1) for mat in lines(_EXTRA_BATCH_MATERIALS_KEY))
    else:
        out.extend((mat, batches) for mat in lines("procurement_materials"))
    if isinstance(mj.get(_EXTRA_BATCH_INVENTION_KEY), dict):
        out.extend((mat, 1) for mat in lines("invention_procurement_materials"))
        if batches > 1:
            out.extend((mat, batches - 1) for mat in lines(_EXTRA_BATCH_INVENTION_KEY))
    else:
        out.extend((mat, batches) for mat in lines("invention_procurement_materials"))
    return out
```

In the `aggregate_shopping_list_with_stale` docstring, replace the paragraph starting "Invention inputs are the exception" with:

```
    Batch 1 may run on owned BPC runs (top level or in a sub-build) that later
    batches no longer have, so a row may carry separate lists for batches
    2..N, for its materials and its invention inputs: the need is then batch
    1 plus (batches - 1) x the extra list (see ``_row_lines_with_batch_counts``).
```

- [ ] **Step 4: Implement the producer's extra-batch plan**

4a. Add next to `_full_batch_invention_whole_attempts`:

```python
    @staticmethod
    def _blueprint_copy_runs_spent(before: dict[int, Any], after: dict[int, Any]) -> bool:
        """Whether planning batch 1 used any owned BPC run (any type)."""
        return any(float(after.get(type_id, 0) or 0) < float(runs or 0) for type_id, runs in before.items())

    def _plan_extra_batch(
        self,
        ctx: _ProductPlanningContext,
        *,
        manufacturing_job: dict[str, Any],
        blueprint_type_id: int,
        effective_runs: int,
        manufacturing_group: str | None,
        required_skill_entries: list[dict[str, Any]],
        blueprint_material_efficiency: int,
        blueprint_time_efficiency: int,
        invents_top_level: bool,
        invention_job: dict[str, Any],
        decryptor: DecryptorOption,
        max_production_limit: int,
    ) -> dict[str, Any]:
        """One batch after batch 1: what the shopping list needs for each of
        batches 2..N once the owned BPC runs batch 1 spent are gone.

        The row's own job runs at `blueprint_material_efficiency` (the invented
        ME for an invented row). Every sub-build is planned afresh without
        owned BPC runs, so it invents, copies from an owned BPO or is bought.
        Planned against no stock: pure requirements (take 0), because the
        shopping list nets owned stock once over all batches. Returns
        {"procurement_materials", "invention_requirements", "unknown_reasons"}.
        Called inside quiet_warnings(): the row logs one WARNING for any
        unknown later-batch need."""
        costs = self._plan_activity_costs(
            ctx, activity="manufacturing", base_time_seconds=int(manufacturing_job.get("time_seconds") or 0),
            runs=effective_runs, process_value=None, manufacturing_group=manufacturing_group,
            required_skill_entries=required_skill_entries,
            blueprint_material_efficiency=blueprint_material_efficiency,
            blueprint_time_efficiency=blueprint_time_efficiency,
        )
        reduction = float(costs.get("material_reduction", 0.0))
        materials: list[dict[str, Any]] = []
        for raw_mat in [dict(e) for e in (manufacturing_job.get("materials") or []) if isinstance(e, dict)]:
            mat = self._compact_material_entry(raw_mat)
            qty_per_run = int(mat.get("quantity") or 0)
            mat["quantity_per_run"] = qty_per_run
            mat["base_quantity"] = qty_per_run * effective_runs
            mat["quantity"] = self._round_material_quantity(
                float(qty_per_run * effective_runs) * max(0.0, 1.0 - reduction),
                minimum_quantity=(effective_runs if qty_per_run > 0 else 0),
            )
            mat["material_reduction"] = reduction
            materials.append(mat)
        plan = self._build_recursive_prerequisite_plan(
            adjusted_material_entries=materials, blueprint_type_id=blueprint_type_id,
            build_from_bpc=ctx.build_from_bpc, include_reactions=ctx.include_reactions,
            selected_industry_profile=ctx.selected_industry_profile,
            selected_character_modifiers=ctx.selected_character_modifiers,
            character_skill_levels_by_name=ctx.character_skill_levels_by_name,
            adjusted_market_price_map=ctx.adjusted_market_price_map,
            sell_price_map=ctx.material_price_map,
            blueprint_copy_assets_by_type_id=ctx.blueprint_copy_assets_by_type_id,
            available_blueprint_copy_runs_by_type_id={},
            available_owned_item_quantity_by_type_id={},
            owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
            blueprint_original_assets_by_type_id=ctx.blueprint_original_assets_by_type_id,
            character_name_by_id=ctx.character_name_by_id, corporation_name_by_id=ctx.corporation_name_by_id,
            manufacturing_row_by_product_type_id=ctx.manufacturing_row_by_product_type_id,
            reaction_row_by_product_type_id=ctx.reaction_row_by_product_type_id,
            invention_row_by_blueprint_type_id=ctx.invention_row_by_blueprint_type_id,
            invention_context=ctx.invention_context,
        )
        requirements = [dict(e) for e in (plan.get("invention_requirements") or []) if isinstance(e, dict)]
        unknown_reasons: list[str] = []
        if invents_top_level:
            attempts, unknown_reason, _chance = self._full_batch_invention_whole_attempts(
                invention_job=invention_job, blueprint_type_id=blueprint_type_id, runs=effective_runs,
                max_production_limit=max_production_limit, adjusted_price_map=ctx.adjusted_market_price_map,
                decryptor=decryptor, invention_context=ctx.invention_context,
            )
            if unknown_reason is not None:
                unknown_reasons.append(f"batches past the owned T2 BPC runs: {unknown_reason}")
            requirements = [
                *self._invention_whole_attempt_requirements(invention_job, attempts, decryptor=decryptor),
                *requirements,
            ]
        return {
            "procurement_materials": list(plan.get("procurement_materials") or []),
            "invention_requirements": requirements,
            "unknown_reasons": unknown_reasons,
        }
```

4b. In `_build_single_product_row`:
- Directly after the per-variant BPC run setup (after the Task 3 `fully_invented` block), add `copy_runs_before_batch_one = dict(available_blueprint_copy_runs_by_type_id)`.
- Replace everything from `# The list above is batch 1 only: the owned T2 BPC runs are spent on it.` through the `logger.warning("Product %s: datacore need unknown from batch ...")` call with:

```python
        # Batches 2..N (shopping list only). The owned BPC runs batch 1 spent,
        # at the top level or in a sub-build, are gone by then, so a later batch
        # is its own plan (_plan_extra_batch). When nothing owned was spent,
        # every later batch repeats batch 1 exactly.
        invents_top_level = bool(ctx.build_from_bpc) and bool(has_top_level_invention_path) and not matched_blueprint_originals
        extra_batch: dict[str, Any] | None = None
        if (
            bool(ctx.build_from_bpc) and not fully_invented
            and self._blueprint_copy_runs_spent(copy_runs_before_batch_one, available_blueprint_copy_runs_by_type_id)
        ):
            if invents_top_level:
                extra_me, extra_te = invented_blueprint_efficiency(decryptor)
            else:
                extra_me = int(job_segments[-1]["blueprint_material_efficiency"])
                extra_te = int(job_segments[-1]["blueprint_time_efficiency"])
            with quiet_warnings():
                extra_batch = self._plan_extra_batch(
                    ctx, manufacturing_job=manufacturing_job, blueprint_type_id=blueprint_type_id,
                    effective_runs=effective_runs, manufacturing_group=manufacturing_group,
                    required_skill_entries=manufacturing_skill_entry_list,
                    blueprint_material_efficiency=extra_me, blueprint_time_efficiency=extra_te,
                    invents_top_level=invents_top_level, invention_job=invention_job,
                    decryptor=decryptor, max_production_limit=max_production_limit,
                )
        extra_batch_requirements = (
            extra_batch["invention_requirements"] if extra_batch is not None else list(invention_requirements)
        )
        invention_procurement_materials_per_extra_batch, _ = self._plan_take_or_buy_material_nodes(
            self._merge_invention_requirements(extra_batch_requirements),
            available_owned_item_quantity_by_type_id={},
            owned_item_unit_cost_by_type_id=ctx.owned_item_unit_cost_by_type_id,
            sell_price_map=ctx.material_price_map,
            adjusted_price_map=ctx.adjusted_market_price_map,
        )
        # Why the shopping list cannot size the datacores, and from which batch
        # on: 1 when batch 1 itself invents with unknown odds (every later batch
        # repeats it), 2 when only the later batches do. Never a guessed count.
        datacore_need_unknown_reasons: list[str] = []
        datacore_need_unknown_from_batch = 2
        if invention_cost_unknown_reason is not None:
            datacore_need_unknown_reasons.append(invention_cost_unknown_reason)
            datacore_need_unknown_from_batch = 1
        elif extra_batch is not None:
            datacore_need_unknown_reasons.extend(extra_batch["unknown_reasons"])
        extra_only_unknown_reasons = [
            r for r in datacore_need_unknown_reasons if r not in (invention_cost_unknown_reason or "")
        ]
        # Batch 1's cost is exact when every unknown is a later batch's.
        batch_one_cost_exact = invention_cost_unknown_reason is None
        if extra_only_unknown_reasons:
            invention_cost_unknown_reason = "; ".join(
                [*([invention_cost_unknown_reason] if invention_cost_unknown_reason else []), *extra_only_unknown_reasons]
            )
            # One WARNING per row: the extra-batch plan ran quietly.
            logger.warning(
                "Product %s: datacore need unknown from batch %s on (%s); "
                "the shopping list flags it instead of buying 0 datacores",
                product_type_id, datacore_need_unknown_from_batch, "; ".join(datacore_need_unknown_reasons),
            )
```

The Task 2 `extra_chance` and the earlier `top_level_extra_attempts` / `_full_batch_invention_whole_attempts` block are gone with this replacement. In Step 7h of Task 2's row output, change `top_level_chance` to use only the batch-1 chance, then the extra batch's:

```python
        top_level_chance = (
            inv_costs.get("invention_chance") if requires_invention_chain and inv_costs is not None else None
        )
        if top_level_chance is None and invents_top_level:
            _attempts, _reason, top_level_chance = self._full_batch_invention_whole_attempts(
                invention_job=invention_job, blueprint_type_id=blueprint_type_id, runs=effective_runs,
                max_production_limit=max_production_limit, adjusted_price_map=ctx.adjusted_market_price_map,
                decryptor=decryptor, invention_context=ctx.invention_context,
            )
```

4c. In the row output, after the `invention_procurement_materials_per_extra_batch` block, add:

```python
        if extra_batch is not None:
            # Shopping list only: one later batch's materials, see above.
            row_out["manufacturing_job"]["procurement_materials_per_extra_batch"] = self._overview_keyed_entries(
                extra_batch["procurement_materials"], compactor=lambda e: e,
            )
```

4d. Remove the replaced nested machinery:
- Delete the method `_extra_batch_invention_requirements`.
- In `_plan_blueprint_chain_for_quantity`, delete the block `if activity == "manufacturing" and bool(build_from_bpc) and has_invention_path and not matched_blueprint_originals: extra_requirements, extra_unknown_reason = ...`. Also delete the local lists `invention_requirements_per_extra_batch` and `extra_batch_invention_unknown_reasons`, `own_invention_requirements`, and their two keys in the returned dict.
- In `_build_recursive_prerequisite_plan`, delete the same two lists and their keys.
- In `_absorb_nested_invention`, delete the parameters `invention_requirements_per_extra_batch` and `extra_batch_invention_unknown_reasons` and their two `extend` statements, and update both callers.

Grep afterwards: `grep -n "per_extra_batch\|extra_batch_invention_unknown" src/eve_online_industry_tracker/application/industry/service.py` must show only `invention_procurement_materials_per_extra_batch` and `procurement_materials_per_extra_batch`.

- [ ] **Step 5: Run the new tests**

Run: the command from Step 2.
Expected: PASS.

- [ ] **Step 6: Run the round-3/4 extra-batch tests and the full suite**

Run: `.venv312/bin/python -m pytest tests/test_industry_service_calculations.py -k "batch or unknown_odds or owned_bpc" -v`, then `.venv312/bin/python -m pytest -q`
Expected: all pass. These keep their numbers because the extra plan sizes the same whole attempts:
- `test_owned_bpc_covering_batch_one_still_buys_datacores_for_later_batches` (16);
- `test_partly_owned_bpc_buys_full_batch_datacores_for_later_batches` (52);
- `test_no_owned_bpc_datacore_buy_is_unchanged`, `test_owned_datacores_are_netted_once_with_an_extra_batch_list`;
- `test_unknown_odds_with_owned_bpc_covering_batch_one_flag_the_later_batches`, `test_batch_one_unknown_odds_keep_the_understated_wording`.

`test_nested_sub_build_bought_in_batch_one_adds_no_extra_batch_datacores` also keeps passing: the later batch buys the 1-ISK component too, and adds no datacores. Any other failure means the extra plan diverges from batch 1 where nothing owned differs: fix the code, not the test.

- [ ] **Step 7: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/service.py \
  src/eve_online_industry_tracker/application/industry/shopping_list.py \
  tests/test_industry_service_calculations.py tests/test_shopping_list.py
git commit -m "fix: size batches 2..N from one extra-batch plan at the invented ME (C1)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

# Part 2: nested plan, stock, visibility

### Task 6: Nested-plan hygiene (C5, C6, C7; FU-61, FU-63, FU-50)

- **C5.** `_build_recursive_prerequisite_plan` (:5091) and the inner loop of `_plan_blueprint_chain_for_quantity` (:4513) evaluate each child on the live stock and BPC-run maps. The child deducts while it evaluates (owned items :4456, BPC runs :4175). When buy wins nothing is restored, so later siblings see stock and runs the plan never uses. Fix: evaluate on copies and commit the copies only when the build is chosen.
- **C6.** Each invention values its amortized inputs against a fresh copy of the stock: top level :7128, nested :6884. Two inventions of one row both price the same owned datacores at the owned cost. Fix: one shared valuation stock per row, spent by the top-level invention first, then by each built nested invention (trial and commit like C5).
- **C7.** `_invention_source_copy_costs` stores fractional remaining source runs (:6796-6797), then truncates them on the next read (:6795). A 1-run owned source BPC used for 0.3 expected attempts reads as 0 runs for the next invention. Fix: read the map as a float. The whole-run elapsed time uses `floor` of the available runs.

**Files:**
- Modify: `application/industry/service.py`. Changes:
  - new `_commit_trial`;
  - `_build_recursive_prerequisite_plan` loop;
  - `_plan_blueprint_chain_for_quantity` (signature, inner loop, nested-invention call);
  - `_plan_nested_invention` (valuation stock);
  - `_plan_invention_chain_for_variant` (valuation stock);
  - `_build_single_product_row` (create the valuation stock, pass it on);
  - `_plan_extra_batch` (passes `{}`);
  - `_invention_source_copy_costs`.
- Test: `tests/test_industry_service_calculations.py`

**Interfaces:**
- Consumes: Tasks 1–5 signatures.
- Produces:
  - `IndustryService._commit_trial(target: dict | None, trial: dict) -> None`;
  - required keyword `invention_valuation_stock: dict[int, Any]` on `_plan_blueprint_chain_for_quantity`, `_build_recursive_prerequisite_plan`, `_plan_nested_invention` and `_plan_invention_chain_for_variant`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_industry_service_calculations.py`:

```python
# --- round 5 Task 6: nested-plan hygiene (C5, C6, C7) ---------------------------


def _parent_with_cheap_component_rows() -> list[dict]:
    """Parent 7200 needs 1 component 6200 and 5 Tritanium per run; the
    component's own blueprint needs 10 Tritanium. The component is cheaper to
    buy (1 ISK) than to build."""
    return [
        {
            "blueprint_type_id": 9200,
            "blueprint": {"type_id": 9200, "type_name": "Parent Blueprint", "base_price": 1000.0},
            "manufacturing_job": {
                "materials": [
                    {"type_id": 6200, "type_name": "Component", "quantity": 1, "category_name": "Commodity"},
                    {"type_id": 34, "type_name": "Tritanium", "quantity": 5, "category_name": "Material"},
                ],
                "skill_entries": [], "time_seconds": 100, "max_production_limit": 10,
                "products": [{"type_id": 7200, "type_name": "Parent", "quantity": 1, "meta_group_name": "Tech I"}],
            },
        },
        {
            "blueprint_type_id": 9300,
            "blueprint": {"type_id": 9300, "type_name": "Component Blueprint", "base_price": 1000.0},
            "manufacturing_job": {
                "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 10, "category_name": "Material"}],
                "skill_entries": [], "time_seconds": 100, "max_production_limit": 10,
                "products": [{"type_id": 6200, "type_name": "Component", "quantity": 1, "meta_group_name": "Tech I"}],
            },
        },
    ]


def test_a_rejected_sub_build_spends_no_stock(monkeypatch) -> None:
    # 10 Tritanium owned. Evaluating the component build took all 10, then the
    # component was bought anyway and the parent's own 5 were bought too (FU-61).
    row = _invention_overview(
        monkeypatch, blueprint_rows=_parent_with_cheap_component_rows(),
        prices={34: 5.0, 6200: 1.0}, owned_item_inventory=({34: 10}, {34: 1.0}),
    )[7200]
    procurement = row["manufacturing_job"]["procurement_materials"]

    assert procurement["6200"]["quantity"] == 1                       # bought
    assert (procurement["34"]["take_quantity"], procurement["34"]["buy_quantity"]) == (5, 0)


def test_owned_datacores_are_valued_once_across_a_rows_inventions(monkeypatch) -> None:
    # Parent 9100 (invented) and its component 5002 (invented) each need 4
    # amortized datacores over 10 runs; 5 are owned at 60, the market is 100.
    # The top level takes 4 at 60 (240); the component gets 1 at 60 + 3 at 100
    # (360). Was 4 x 60 twice (480): the owned datacores were counted twice (FU-63).
    row = _invention_overview(
        monkeypatch, maximize_bp_runs=True, blueprint_rows=_invented_parent_rows(),
        prices=_NESTED_FAKE_PRICES, owned_item_inventory=({204: 5}, {204: 60.0}),
    )[7100]
    mj = row["manufacturing_job"]

    assert mj["recursive_activity_breakdown"]["manufacturing:5002"]["recommended_action"] == "build"
    assert mj["invention_material_cost"] == pytest.approx(4 * 60 + 1 * 60 + 3 * 100)


def test_fractional_source_copy_runs_carry_to_the_next_invention() -> None:
    service = object.__new__(IndustryService)
    service._state = SimpleNamespace()  # type: ignore[attr-defined]
    source_row = {"blueprint_type_id": 1001, "copying_job": {"time_seconds": 20},
                  "manufacturing_job": {"materials": [{"type_id": 34, "quantity": 3}]}}
    runs = {1001: 1}
    compute = lambda **kw: {"duration_seconds": 0, "total_job_cost": None}  # noqa: E731
    costs = {"runs": 0.3, "whole_attempts": 1}

    service._invention_source_copy_costs(
        compute_activity=compute, invention_source_row=source_row, invention_costs=costs,
        available_blueprint_copy_runs_by_type_id=runs, adjusted_price_map={}, enabled=True,
    )
    assert runs[1001] == pytest.approx(0.7)
    _, missing = service._invention_source_copy_costs(
        compute_activity=compute, invention_source_row=source_row, invention_costs=costs,
        available_blueprint_copy_runs_by_type_id=runs, adjusted_price_map={}, enabled=True,
    )
    assert missing == 0  # 0.7 runs left cover 0.3 attempts (was truncated to 0, so 0.3 missing)
    assert runs[1001] == pytest.approx(0.4)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_industry_service_calculations.py -k "rejected_sub_build or valued_once or fractional_source" -v`
Expected: FAIL. The take is `(0, 5)`, the invention cost is 480.0, and `missing` is 0.3.

- [ ] **Step 3: Implement C5 (trial and commit)**

Add next to `_build_total_cost_or_none`:

```python
    @staticmethod
    def _commit_trial(target: dict[int, Any] | None, trial: dict[int, Any]) -> None:
        """Make a built child's evaluation real: the trial map replaces the
        live one in place (the caller's dict object stays shared)."""
        if target is None:
            return
        target.clear()
        target.update(trial)
```

In `_build_recursive_prerequisite_plan`, directly before `child_plan = self._plan_blueprint_chain_for_quantity(`, add:

```python
            # Evaluate the child on copies: a rejected build must not spend
            # stock, BPC runs or invention valuation stock (FU-61).
            trial_copy_runs = dict(available_blueprint_copy_runs_by_type_id or {})
            trial_stock = dict(available_owned_item_quantities)
            trial_valuation = dict(invention_valuation_stock)
```

Pass these to that call:
- `available_blueprint_copy_runs_by_type_id=trial_copy_runs`;
- `available_owned_item_quantity_by_type_id=trial_stock`;
- `invention_valuation_stock=trial_valuation`.

At the start of the `if choose_build:` block, add:

```python
                self._commit_trial(available_blueprint_copy_runs_by_type_id, trial_copy_runs)
                self._commit_trial(available_owned_item_quantities, trial_stock)
                self._commit_trial(invention_valuation_stock, trial_valuation)
```

Do the same in `_plan_blueprint_chain_for_quantity`'s inner child loop:
- copy before the recursive call: `trial_copy_runs = dict(available_copy_runs)`, `trial_stock = dict(available_owned_item_quantities)`, `trial_valuation = dict(invention_valuation_stock)`;
- pass them in;
- commit at the start of `if choose_build:` with `self._commit_trial(available_copy_runs, trial_copy_runs)`, `self._commit_trial(available_owned_item_quantities, trial_stock)`, `self._commit_trial(invention_valuation_stock, trial_valuation)`.

- [ ] **Step 4: Implement C6 (one valuation stock per row)**

- Add the keyword parameter `invention_valuation_stock: dict[int, Any],` to:
  - `_plan_blueprint_chain_for_quantity` and `_build_recursive_prerequisite_plan` (after `available_owned_item_quantity_by_type_id`);
  - `_plan_nested_invention` (after `available_owned_item_quantity_by_type_id`);
  - `_plan_invention_chain_for_variant` (after `available_owned_item_quantity_by_type_id`).
- In `_plan_nested_invention`, replace `available_owned_item_quantity_by_type_id=dict(available_owned_item_quantity_by_type_id),` in the `_plan_take_or_buy_material_nodes` call with `available_owned_item_quantity_by_type_id=invention_valuation_stock,`. Rewrite the comment above it:

```python
        # Valued against the row's one invention valuation stock (FU-63): the
        # top-level invention spends it first, then each nested invention in
        # turn. The caller evaluates a child on a copy and commits it only when
        # the child is built. The whole attempts are taken from the real stock
        # in invention_procurement_materials (_build_single_product_row).
```

- In `_plan_invention_chain_for_variant`, replace `available_owned_item_quantity_by_type_id=dict(available_owned_item_quantity_by_type_id),` likewise with `available_owned_item_quantity_by_type_id=invention_valuation_stock,`.
- In `_plan_blueprint_chain_for_quantity`, pass `invention_valuation_stock=invention_valuation_stock` to `_plan_nested_invention`.
- In `_build_single_product_row`, directly before `# Invention (Step 4)`, add:

```python
        # One valuation stock per row for every invention's amortized inputs.
        invention_valuation_stock = dict(available_owned_item_quantity_by_type_id)
```

  and pass `invention_valuation_stock=invention_valuation_stock` to `_plan_invention_chain_for_variant` and `_build_recursive_prerequisite_plan`.
- In `_plan_extra_batch`, pass `invention_valuation_stock={}`.

- [ ] **Step 5: Implement C7 (fractional source runs)**

In `_invention_source_copy_costs`, replace

```python
        available_source_runs = int(available_blueprint_copy_runs_by_type_id.get(source_bp_type_id, 0))
        source_runs_used = min(max(0, available_source_runs), attempts)
        available_blueprint_copy_runs_by_type_id[source_bp_type_id] = max(0, available_source_runs - source_runs_used)
        missing_source_runs = max(0, attempts - source_runs_used)
        whole_missing_source_runs = max(0, whole_attempts - max(0, available_source_runs))
```

with

```python
        # Expected-value model: the runs are fractional, read and kept as such
        # (int() here read a 0.7-run remainder as 0, FU-50). The whole-run
        # elapsed time counts only whole runs left on the owned copies.
        available_source_runs = max(0.0, float(available_blueprint_copy_runs_by_type_id.get(source_bp_type_id, 0) or 0))
        source_runs_used = min(available_source_runs, attempts)
        available_blueprint_copy_runs_by_type_id[source_bp_type_id] = available_source_runs - source_runs_used
        missing_source_runs = max(0.0, attempts - source_runs_used)
        whole_missing_source_runs = max(0, whole_attempts - int(math.floor(available_source_runs + 1e-9)))
```

The manufacturing readers of the same map keep `int(...)` (:4172). For a manufacturing job, a 0.7-run remainder is 0 whole runs. That is the safe direction: the job copies or invents instead.

- [ ] **Step 6: Run the new tests**

Run: the command from Step 2.
Expected: PASS.

- [ ] **Step 7: Run the full suite**

Run: `.venv312/bin/python -m pytest -q`
Expected: all pass. `test_evaluating_a_nested_invention_does_not_spend_stock` (parametrized) still holds: the whole-attempt buy list is taken from the real stock, which C6 does not change.

- [ ] **Step 8: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/service.py tests/test_industry_service_calculations.py
git commit -m "fix: rejected sub-builds spend nothing, owned datacores valued once per row, fractional source runs kept

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: The shopping list nets against the published hangar stock (C8, C9; FU-75, FU-70)

The rows do not carry the hangar stock, so `aggregate_shopping_list` uses `owned = the largest take any one row reported` (shopping_list.py:31-35, :94-95). That is a loose lower bound:
- **C9.** When no single row could take all the stock, it buys material that is already in the hangar.
- **C8.** When an owned BPC covers batch 1 there is no batch-1 invention list, and the extra list takes 0 by design. The owned datacores then count as 0 and every datacore of batches 2..N is bought.

Fix: every row publishes `manufacturing_job.owned_stock_by_type_id`, the hangar stock it was planned against (`ctx.available_owned_item_quantity_by_type_id_base`) for every type on its buy lists. Every row of one refresh sees the same stock, so the shopping list nets the aggregated need against it once: `owned = max(published stock, largest take)`. Rows cached before this key fall back to the take bound. Both are ≤ the real stock, so this never under-buys.

**Files:**
- Modify: `application/industry/service.py` (`_build_single_product_row` row output)
- Modify: `application/industry/shopping_list.py` (`aggregate_shopping_list_with_stale` :49-111, docstring)
- Test: `tests/test_shopping_list.py`, `tests/test_industry_service_calculations.py`

**Interfaces:**
- Consumes: Task 5's `extra_batch` and `invention_procurement_materials_per_extra_batch` in `_build_single_product_row`.
- Produces: row key `manufacturing_job.owned_stock_by_type_id: dict[str(type_id), int]`; `shopping_list._OWNED_STOCK_KEY = "owned_stock_by_type_id"`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_shopping_list.py`:

```python
# --- round 5 Task 7: net against the published hangar stock (C8, C9) -----------


def test_published_stock_is_netted_exactly_once() -> None:
    # 10 owned; two rows each take 8 of them. The take bound said 8 owned
    # (buy 8); the stock the producer planned against says 10 (buy 6).
    row_a = _make_row(1001, "Item A", 1, {"34": _owned_line(8, 8)})
    row_b = _make_row(1002, "Item B", 1, {"34": _owned_line(8, 8)})
    for row in (row_a, row_b):
        row["manufacturing_job"]["owned_stock_by_type_id"] = {"34": 10}
    assert _line(aggregate_shopping_list([row_a, row_b])) == (16, 6)


def test_extra_batch_datacores_net_the_published_hangar_stock() -> None:
    # An owned BPC covers batch 1, so no row took the 3 owned datacores.
    row = _invention_row(5, batch_one=None, extra=_datacore(4))
    row["manufacturing_job"]["owned_stock_by_type_id"] = {"204": 3}
    assert _line(aggregate_shopping_list([row]), 204) == (16, 13)


def test_a_row_without_published_stock_keeps_the_take_bound() -> None:
    row_a = _make_row(1001, "Item A", 1, {"34": _owned_line(8, 8)})
    row_b = _make_row(1002, "Item B", 1, {"34": _owned_line(8, 3)})
    row_b["manufacturing_job"]["owned_stock_by_type_id"] = {"34": 5}
    # Published 5 < the other row's take 8: owned is 8, never less than a take.
    assert _line(aggregate_shopping_list([row_a, row_b])) == (16, 8)
```

Append to `tests/test_industry_service_calculations.py`:

```python
# --- round 5 Task 7: rows publish the stock they were planned against ----------


def test_a_row_publishes_the_stock_of_its_buy_line_types(monkeypatch) -> None:
    mj = _invention_overview(
        monkeypatch, owned_item_inventory=({34: 3, 204: 7, 99999: 5}, {34: 1.0, 204: 60.0}),
    )[5002]["manufacturing_job"]
    assert mj["owned_stock_by_type_id"] == {"204": 7, "34": 3}


def test_owned_datacores_net_against_later_batches_when_an_owned_bpc_covers_batch_one(monkeypatch) -> None:
    # FU-75: 3 datacores in the hangar, a 10-run owned BPC covers batch 1,
    # 4 datacores per later batch, 5 batches: 16 needed, 13 bought (was 16).
    row = _invention_overview(
        monkeypatch, owned_assets=([_owned_t2_bpc(runs=10)], [], {}, {}, {}),
        owned_item_inventory=({204: 3}, {204: 60.0}),
    )[5002]
    row["max_batches_total"] = 5
    line = _shopping_line(row, 204)
    assert line is not None and (line["need"], line["buy"]) == (16, 13)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_shopping_list.py tests/test_industry_service_calculations.py -k "published_stock or published_hangar or take_bound or publishes_the_stock or net_against_later_batches" -v`
Expected: FAIL. The buys are 8 and 16, there is a `KeyError: 'owned_stock_by_type_id'`, and the producer-level line is `(16, 16)`.

- [ ] **Step 3: Implement**

3a. Producer, in the row output after the extra-batch lists:

```python
        # The hangar stock this row was planned against, for every type on its
        # buy lists. Every row of one refresh sees the same stock, so the
        # shopping list nets its aggregated need against it once (C8, C9).
        stock_type_ids = sorted({
            int(entry.get("type_id") or 0)
            for entries in (
                top_level_procurement_materials,
                invention_procurement_materials,
                invention_procurement_materials_per_extra_batch,
                (extra_batch or {}).get("procurement_materials") or [],
            )
            for entry in entries if isinstance(entry, dict)
        } - {0})
        row_out["manufacturing_job"]["owned_stock_by_type_id"] = {
            str(type_id): int(ctx.available_owned_item_quantity_by_type_id_base.get(type_id, 0) or 0)
            for type_id in stock_type_ids
        }
```

3b. `shopping_list.py`: add `_OWNED_STOCK_KEY = "owned_stock_by_type_id"` next to the other keys, plus:

```python
def _non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0
```

In `aggregate_shopping_list_with_stale`:
- Add `published_stock_by_type_id: dict[int, int] = {}` next to `owned_by_type_id`.
- In the row loop, after `lines = ...`, add `stock = mj.get(_OWNED_STOCK_KEY)`.
- In the per-line loop, after `mat_type_id` is parsed, add:

```python
            if isinstance(stock, dict) and str(mat_type_id) in stock:
                published_stock_by_type_id[mat_type_id] = max(
                    published_stock_by_type_id.get(mat_type_id, 0), _non_negative_int(stock[str(mat_type_id)]),
                )
```

- Replace the final buy loop with:

```python
    for mat_type_id, item in accumulated.items():
        owned = max(owned_by_type_id.get(mat_type_id, 0), published_stock_by_type_id.get(mat_type_id, 0))
        item["buy"] = max(0, item["need"] - owned)
```

- Replace the docstring paragraph that starts "per type_id. The rows do not carry the total stock" with:

```
    per type_id. ``owned`` is the hangar stock the producer planned the rows
    against (``manufacturing_job.owned_stock_by_type_id``, the same for every
    row of one refresh), so the aggregated need is netted against it once. A
    row cached before that key existed falls back to what it took: within a
    row the lines spend the stock in turn, so a row's summed take is stock the
    producer saw. ``owned`` is the larger of the two, never more than the
    stock the producer reported.
```

- [ ] **Step 4: Run the new tests, then the full suite**

Run: the command from Step 2, then `.venv312/bin/python -m pytest -q`
Expected: all pass. `test_shared_stock_never_assumes_more_than_any_row_reported` and the other take-bound tests use rows without the key, so they keep their numbers.

- [ ] **Step 5: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/service.py \
  src/eve_online_industry_tracker/application/industry/shopping_list.py \
  tests/test_shopping_list.py tests/test_industry_service_calculations.py
git commit -m "fix: the shopping list nets against the hangar stock the producer planned against (C8, C9)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Capital Reserved = purchases + estimated job install fees, unpriced shown as unknown (V1, FU-67, D4)

`compute_capital_reserved` sums only the pending `buy_materials` and `buy_bpo` actions, and counts an unpriced one as 0 (status_bar.py:32-51). The planner's actions carry no fee. Per D4, Capital Reserved is materials, plus BPO purchases, plus estimated install fees. An unpriced purchase shows as "unknown", not 0.

Where each fee comes from:
- **manufacture:** the producer's `manufacturing_job.manufacturing_job_cost` for the batch.
- **invent:** one attempt, the planner's unit (`attempts = runs or 1` in the shopping list builder). The producer publishes `activity_breakdown.invention.job_cost_per_attempt`.
- **copy:** one source-copy run, `activity_breakdown.invention.source_copy_job_cost_per_run`.
- **sub_manufacture:** the component's own overview row gives the fee per run (`manufacturing_job_cost / runs`; the fee is EIV-based, so linear in runs and independent of ME), times the job's runs. The same refresh and industry profile apply, which is also the parent's structure (F-Q2).
- **ME/TE research:** the producer does not estimate research fees for owned BPOs, so these stay unknown. The metric then reads "≥ X", with the count in a caption. See Open question Q2.

**Files:**
- Modify: `application/industry/service.py` (`_expected_invention_costs`, `_invention_source_copy_costs`, `_plan_invention_chain_for_variant`)
- Modify: `application/industry/overview_row.py` (fee accessors)
- Modify: `application/daily_planner/models.py` (`AssignedAction.estimated_job_fee_isk`)
- Modify: `application/daily_planner/character_assigner.py`, `chain_planner.py` (`_resolve_sub_manufacture`), `action_plan_builder.py` (`_build_job_actions`)
- Modify: `infrastructure/models.py` (`DailyActionLogModel`), `infrastructure/schema_migrations.py` (CREATE TABLE :698-720, `_ensure_column` list :877)
- Modify: `src/streamlit_ui/components/daily_planner/status_bar.py`
- Test: `tests/test_daily_planner_job_fees.py` (create), `tests/test_daily_planner_capital_reserved.py`, `tests/test_industry_service_calculations.py`, `tests/test_action_plan_builder.py`

**Interfaces:**
- Produces:
  - producer keys `activity_breakdown.invention.job_cost_per_attempt: float | None`, `activity_breakdown.invention.source_copy_job_cost_per_run: float | None`, `source_copy_costs["job_cost_per_run"]`;
  - `orow.get_manufacturing_fee(row)`, `get_manufacturing_fee_per_run(row)`, `get_invention_fee_per_attempt(row)`, `get_source_copy_fee_per_run(row)`, each `-> float | None`;
  - `AssignedAction.estimated_job_fee_isk: float | None = None`; DB column `daily_action_log.estimated_job_fee_isk REAL`;
  - `ChainPlanner._component_fee_per_run(type_id, phase1_data) -> float | None`, and `sub_job_batches[i]["fee"]`;
  - `status_bar.CapitalReserved(total: float, unpriced_purchases: int, unknown_fee_jobs: int)`, `compute_capital_reserved(actions) -> CapitalReserved`, `capital_reserved_display(reserved) -> tuple[str, str | None]`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_daily_planner_job_fees.py
"""D4: every planner job action carries its estimated install fee."""
from __future__ import annotations

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision


class _Admin:
    def get(self, section, key, default=None):
        raise KeyError(key)


class _Chars:
    def list_characters(self):
        return [{"character_id": 1, "character_name": "Pilot", "skills": {"skills": [
            {"skill_id": 3380, "skill_name": "Laboratory Operation", "trained_skill_level": 5},
            {"skill_id": 3387, "skill_name": "Mass Production", "trained_skill_level": 5},
        ]}}]


def _decision(row, **overrides):
    base = dict(
        type_id=int(row["type_id"]), type_name="Thing", decision="build", decision_reason="test",
        adjusted_score=1.0, absolute_profit_per_batch=1.0, isk_per_hour=1.0, margin_pct=1.0,
        days_of_supply_current=1.0, effective_velocity=1.0, meta_group_id=2,
        pipeline_stage="manufacturing", overview_row=row,
    )
    base.update(overrides)
    return ItemDecision(**base)


def _actions(row, **overrides):
    plan = ChainPlan(decisions=[_decision(row, **overrides)])
    return {a.action_type: a for a in CharacterAssigner().assign(plan, [], _Chars(), _Admin())}


def test_manufacture_invent_and_copy_actions_carry_their_fees():
    row = {"type_id": 12345, "quantity": 10, "needs_invention": True, "has_t1_bpo": True,
           "t1_blueprint_type_id": 999, "manufacturing_job": {
               "runs": 10, "manufacturing_job_cost": 2_500.0,
               "activity_breakdown": {"invention": {"job_cost_per_attempt": 4.4,
                                                    "source_copy_job_cost_per_run": 0.0099}},
           }}
    actions = _actions(row)
    assert actions["manufacture"].estimated_job_fee_isk == 2_500.0
    assert actions["invent"].estimated_job_fee_isk == 4.4
    assert actions["copy"].estimated_job_fee_isk == 0.0099


def test_a_research_action_has_an_unknown_fee():
    row = {"type_id": 12345, "quantity": 1, "needs_me_research": True,
           "manufacturing_job": {"runs": 1, "blueprint_sde": {"blueprint_type_id": 999}}}
    assert _actions(row)["me_research"].estimated_job_fee_isk is None


def test_a_sub_job_fee_is_the_components_fee_per_run_times_its_runs():
    planner = ChainPlanner(None, None, _Admin())
    phase1 = {"overview_rows": [{"type_id": 54321, "manufacturing_job": {"runs": 4, "manufacturing_job_cost": 40.0}}]}
    assert planner._component_fee_per_run(54321, phase1) == 10.0
    assert planner._component_fee_per_run(11111, phase1) is None
```

Append to `tests/test_daily_planner_capital_reserved.py` and rewrite its existing tests for the new return type:

```python
def test_manufacture_material_cost_is_not_counted_on_top_of_buy_materials():
    actions = [
        _a("manufacture", 300.0),
        _a("sub_manufacture", 100.0),
        _a("buy_materials", 250.0),
        _a("buy_materials", 150.0),
    ]
    reserved = status_bar.compute_capital_reserved(actions)
    assert reserved.total == 400.0
    assert reserved.unknown_fee_jobs == 2   # the two jobs carry no fee


def test_buy_bpo_counts_and_non_pending_are_ignored_and_unpriced_purchases_are_counted():
    actions = [
        _a("buy_bpo", 1000.0),
        _a("buy_materials", 50.0, status="done"),
        _a("buy_materials", None),
        _a("deliver", 9.0),
    ]
    reserved = status_bar.compute_capital_reserved(actions)
    assert (reserved.total, reserved.unpriced_purchases, reserved.unknown_fee_jobs) == (1000.0, 1, 0)


def test_empty_plan_reserves_nothing():
    assert status_bar.compute_capital_reserved([]) == status_bar.CapitalReserved(0.0, 0, 0)


def test_job_fees_are_added_to_the_purchases():
    actions = [
        {**_a("manufacture", 300.0), "estimated_job_fee_isk": 25.0},
        {**_a("invent", None), "estimated_job_fee_isk": 4.0},
        _a("buy_materials", 400.0),
    ]
    reserved = status_bar.compute_capital_reserved(actions)
    assert (reserved.total, reserved.unpriced_purchases, reserved.unknown_fee_jobs) == (429.0, 0, 0)
    assert status_bar.capital_reserved_display(reserved) == ("429 ISK", None)


def test_an_unpriced_purchase_makes_the_total_unknown():
    reserved = status_bar.CapitalReserved(total=1_000.0, unpriced_purchases=2, unknown_fee_jobs=1)
    assert status_bar.capital_reserved_display(reserved) == ("unknown", "2 purchase(s) have no price")


def test_an_unknown_fee_makes_the_total_a_lower_bound():
    reserved = status_bar.CapitalReserved(total=2_500_000.0, unpriced_purchases=0, unknown_fee_jobs=3)
    assert status_bar.capital_reserved_display(reserved) == ("≥ 2.50M ISK", "install fee unknown for 3 job(s)")


def test_capital_reserved_help_states_scope():
    h = status_bar.CAPITAL_RESERVED_HELP
    assert "install fee" in h and "unknown" in h and "≥" in h
```

Delete the old `test_capital_reserved_help_states_scope` (replaced above), and change this file's module docstring to `"""Capital Reserved: pending purchases plus estimated job install fees (D4)."""`.

Append to `tests/test_industry_service_calculations.py`:

```python
# --- round 5 Task 8: fees per attempt and per copy run (D4) ---------------------


def test_the_invention_publishes_its_fee_per_attempt_and_per_source_copy_run(monkeypatch) -> None:
    profile = {
        "installation_cost_modifier": 0.1, "material_efficiency_bonus": 0.0,
        "time_efficiency_bonus": 0.0, "facility_cost_bonus": 0.0,
        "system_cost_indices": [
            {"activity": "manufacturing", "cost_index": 0.05},
            {"activity": "copying", "cost_index": 0.03},
            {"activity": "invention", "cost_index": 0.02},
        ],
        "structure_rigs": [],
    }
    invention = _invention_overview(monkeypatch, profile=profile)[5002]["manufacturing_job"]["activity_breakdown"]["invention"]
    # One attempt: EIV 2 x 100 = 200 x 0.02 = 4.0, + 10 % surcharge.
    assert invention["job_cost_per_attempt"] == pytest.approx(4.4)
    # One source-copy run: source EIV 3 x 5 = 15 x 0.02 = 0.3 x 0.03 = 0.009, + 10 %.
    assert invention["source_copy_job_cost_per_run"] == pytest.approx(0.0099)
```

Append to `tests/test_action_plan_builder.py`, after `test_a_job_action_persists_its_build_cost`:

```python
def test_a_job_action_persists_its_install_fee():
    from eve_online_industry_tracker.application.daily_planner.action_plan_builder import ActionPlanBuilder
    from eve_online_industry_tracker.application.daily_planner.models import AssignedAction
    action = AssignedAction(
        type_id=12345, type_name="Thing", action_type="manufacture", character_id=1,
        character_name="Pilot", quantity=200, runs=20, estimated_cost_isk=50_000_000.0,
        estimated_profit_isk=None, estimated_completion=None, notes=None,
        estimated_job_fee_isk=25_000.0,
    )
    (row,) = ActionPlanBuilder()._build_job_actions(0, [action], datetime(2026, 1, 1))
    assert row.estimated_job_fee_isk == 25_000.0
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_job_fees.py tests/test_daily_planner_capital_reserved.py tests/test_action_plan_builder.py tests/test_industry_service_calculations.py -k "fee or capital or reserved or unpriced or lower_bound or help_states" -v`
Expected: FAIL. `AttributeError: 'AssignedAction' object has no attribute 'estimated_job_fee_isk'`, `AttributeError: ... 'CapitalReserved'`, `KeyError: 'job_cost_per_attempt'`.

- [ ] **Step 3: Implement the producer fees**

In `_expected_invention_costs`, after `costs["job_duration_seconds"] = ...`, add:

```python
        # The install fee of one attempt: the daily planner's invent action is
        # one attempt (D4, Capital Reserved).
        costs["job_cost_per_attempt"] = compute_activity(
            activity="invention", base_time_seconds=invention_base_time, runs=1,
            process_value=per_attempt_process_value,
        )["total_job_cost"]
```

In `_invention_source_copy_costs`, after `source_copy_costs["job_duration_seconds"] = ...`, add:

```python
        source_copy_costs["job_cost_per_run"] = compute_activity(
            activity="copying", base_time_seconds=source_base_time, runs=1,
            process_value=(float(source_pv) * float(self._adm("industry", "copy_cost_eiv_multiplier", 0.02))
                           if source_pv is not None else None),
        )["total_job_cost"]
```

In `_plan_invention_chain_for_variant`, after the `source_copy_costs, missing_source_runs = self._invention_source_copy_costs(...)` call, add:

```python
        # The planner's copy action copies one source run per attempt (D4).
        costs["source_copy_job_cost_per_run"] = (
            source_copy_costs.get("job_cost_per_run") if source_copy_costs is not None else None
        )
```

- [ ] **Step 4: Implement the accessors, the actions and the persistence**

4a. `overview_row.py`:

```python
def _activity_breakdown(row: dict[str, Any]) -> dict[str, Any]:
    breakdown = get_manufacturing_job(row).get("activity_breakdown")
    return breakdown if isinstance(breakdown, dict) else {}


def get_manufacturing_fee(row: dict[str, Any]) -> float | None:
    """The batch's manufacturing install fee (manufacturing_job_cost), or None
    when the producer could not price it (no cost index or no EIV)."""
    return _as_optional_float(get_manufacturing_job(row).get("manufacturing_job_cost"))


def get_manufacturing_fee_per_run(row: dict[str, Any]) -> float | None:
    """The manufacturing fee of one run. The fee is EIV x index, linear in
    runs and independent of ME. None when the fee or the runs are unknown."""
    fee = get_manufacturing_fee(row)
    runs = _as_int(get_manufacturing_job(row).get("runs"))
    return fee / runs if fee is not None and runs > 0 else None


def get_invention_fee_per_attempt(row: dict[str, Any]) -> float | None:
    invention = _activity_breakdown(row).get("invention")
    return _as_optional_float(invention.get("job_cost_per_attempt")) if isinstance(invention, dict) else None


def get_source_copy_fee_per_run(row: dict[str, Any]) -> float | None:
    invention = _activity_breakdown(row).get("invention")
    return _as_optional_float(invention.get("source_copy_job_cost_per_run")) if isinstance(invention, dict) else None
```

4b. `models.py`, `AssignedAction`, after `decryptor_type_name`:

```python
    # The estimated install fee of this job (D4, Capital Reserved). None =
    # unknown, never 0: the producer could not price it, or it does not
    # estimate this job type (ME/TE research).
    estimated_job_fee_isk: float | None = None
```

4c. `character_assigner.py`:
- me_research and te_research: `estimated_job_fee_isk=None,`.
- invent: `estimated_job_fee_isk=orow.get_invention_fee_per_attempt(row),`.
- copy: `estimated_job_fee_isk=orow.get_source_copy_fee_per_run(row),`.
- sub_manufacture: `estimated_job_fee_isk=job.get("fee"),`.
- manufacture: `estimated_job_fee_isk=orow.get_manufacturing_fee(row),`.

4d. `chain_planner.py`. In `_resolve_sub_manufacture`, replace the `job_batches = [...]` comprehension with:

```python
        # Linear pricing, so the per-job costs sum to sub_cost exactly. The
        # fee is per run from the component's own overview row (D4).
        fee_per_run = self._component_fee_per_run(mat_type_id, phase1_data)
        job_batches = [
            {
                **job,
                "cost": self._price_materials(job["materials"], market_depth_cache),
                "fee": fee_per_run * job["runs"] if fee_per_run is not None else None,
            }
            for job in jobs
        ]
```

and add:

```python
    @staticmethod
    def _component_fee_per_run(type_id: int, phase1_data: dict[str, Any]) -> float | None:
        """The install fee of one run of the component, from its own overview
        row: the same refresh and industry profile, which is the parent's
        structure (F-Q2). None when no kept row of it has a priced fee:
        unknown, not free."""
        for row in phase1_data.get("overview_rows") or []:
            if isinstance(row, dict) and int(row.get("type_id") or 0) == int(type_id):
                fee = orow.get_manufacturing_fee_per_run(row)
                if fee is not None:
                    return fee
        return None
```

4e. `action_plan_builder.py`, `_build_job_actions`: add `estimated_job_fee_isk=action.estimated_job_fee_isk,` after `estimated_build_cost_isk=...`.

4f. `infrastructure/models.py`, `DailyActionLogModel`, after `estimated_build_cost_isk`:

```python
    # The job's estimated install fee (D4, Capital Reserved); NULL = unknown.
    estimated_job_fee_isk: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
```

4g. `schema_migrations.py`: in the `daily_action_log` CREATE TABLE, add `"estimated_job_fee_isk REAL NULL,"` after `"estimated_build_cost_isk REAL NULL,"`. After the `estimated_build_cost_isk` `_ensure_column`, add:

```python
    _ensure_column(db_app, table="daily_action_log", column="estimated_job_fee_isk", ddl_type="REAL")
```

- [ ] **Step 5: Implement the status bar**

Replace `_CAPITAL_ACTION_TYPES` through `compute_capital_reserved` with:

```python
#: Action types that spend ISK on the market: the shopping list.
_CAPITAL_ACTION_TYPES: frozenset[str] = frozenset({"buy_materials", "buy_bpo"})
#: Action types that pay an install fee.
_JOB_ACTION_TYPES: frozenset[str] = frozenset(
    {"manufacture", "sub_manufacture", "invent", "copy", "me_research", "te_research"}
)


CAPITAL_RESERVED_HELP = (
    "ISK the plan needs: pending purchases (materials and BPO purchases) plus the estimated "
    "install fee of every pending job. Shown as unknown while a purchase has no price, "
    "and as ≥ while a job's install fee could not be estimated (e.g. ME/TE research)."
)


@dataclass(frozen=True)
class CapitalReserved:
    total: float
    unpriced_purchases: int
    unknown_fee_jobs: int


def compute_capital_reserved(actions: list[dict[str, Any]]) -> CapitalReserved:
    """ISK needed to execute the plan (D4): the pending shopping list plus
    the pending jobs' install fees.

    manufacture / sub_manufacture actions also carry the material cost of
    the materials the buy_materials rows purchase. Only their fee is added,
    or every material would count twice. An unpriced purchase or an unknown
    fee is counted, never read as 0."""
    total = 0.0
    unpriced_purchases = 0
    unknown_fee_jobs = 0
    for action in actions:
        if str(action.get("status") or "pending") != "pending":
            continue
        action_type = str(action.get("action_type") or "")
        if action_type in _CAPITAL_ACTION_TYPES:
            cost = action.get("estimated_cost_isk")
            if cost is None:
                unpriced_purchases += 1
            else:
                total += float(cost)
        elif action_type in _JOB_ACTION_TYPES:
            fee = action.get("estimated_job_fee_isk")
            if fee is None:
                unknown_fee_jobs += 1
            else:
                total += float(fee)
    return CapitalReserved(total, unpriced_purchases, unknown_fee_jobs)


def capital_reserved_display(reserved: CapitalReserved) -> tuple[str, str | None]:
    """(metric value, caption or None) for the Capital Reserved metric."""
    if reserved.unpriced_purchases:
        return "unknown", f"{reserved.unpriced_purchases} purchase(s) have no price"
    if reserved.unknown_fee_jobs:
        return f"≥ {_fmt_isk(reserved.total)}", f"install fee unknown for {reserved.unknown_fee_jobs} job(s)"
    return (_fmt_isk(reserved.total) if reserved.total else "—"), None
```

Add `from dataclasses import dataclass` to the imports. In `render_status_bar`, replace the `with c4:` body with:

```python
        with c4:
            actions = (page_state.plan or {}).get("actions") or []
            value, caption = capital_reserved_display(compute_capital_reserved(actions))
            st.metric("Capital Reserved", value, help=CAPITAL_RESERVED_HELP)
            if caption:
                st.caption(caption)
```

- [ ] **Step 6: Run the new tests, then the full suite**

Run: the command from Step 2, then `.venv312/bin/python -m pytest -q`
Expected: all pass. `tests/test_daily_planner_status_bar_freshness.py` renders plans with `"actions": []`, so it adds no caption. `test_every_planner_setting_read_exists_in_the_admin_schema` is unaffected: no new planner `_adm` key.

- [ ] **Step 7: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/service.py \
  src/eve_online_industry_tracker/application/industry/overview_row.py \
  src/eve_online_industry_tracker/application/daily_planner/models.py \
  src/eve_online_industry_tracker/application/daily_planner/character_assigner.py \
  src/eve_online_industry_tracker/application/daily_planner/chain_planner.py \
  src/eve_online_industry_tracker/application/daily_planner/action_plan_builder.py \
  src/eve_online_industry_tracker/infrastructure/models.py \
  src/eve_online_industry_tracker/infrastructure/schema_migrations.py \
  src/streamlit_ui/components/daily_planner/status_bar.py \
  tests/test_daily_planner_job_fees.py tests/test_daily_planner_capital_reserved.py \
  tests/test_industry_service_calculations.py tests/test_action_plan_builder.py
git commit -m "feat: Capital Reserved includes job install fees and shows unpriced purchases as unknown (D4)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Visibility one-liners (V2, V3, V4, V5; FU-53, FU-54, FU-12, FU-27; RM-25 index builders)

- **V2.** A top-level invention node with unknown odds shows `take` with 0 runs (service.py:7147). It should show `unknown` and carry the reason, and the tree label accepts `unknown` (industry_builder_ui.py:124).
- **V3.** When owned source-copy runs cover the fractional attempts, `invention_source_copying` is left out of `activity_breakdown` (:7434), although its whole-run elapsed time is in `elapsed_time_seconds`. List it whenever its elapsed time is positive.
- **V4.** Missing batch runs log two WARNINGs (chain_planner.py:303 and :330). The second one is skipped when the unknown optimal ME is already explained by the runs.
- **V5 + RM-25.** `build_product_to_blueprint_index` breaks ties by dict order (`setdefault`, :81). `build_invention_source_index` already keeps the lowest id. Both become one lowest-id builder.

**Files:**
- Modify: `application/industry/service.py` (`_plan_invention_chain_for_variant` node; the `invention_source_copying` condition in `_build_single_product_row`)
- Modify: `src/streamlit_ui/state/industry_builder_ui.py` (`tree_node_activity_label`)
- Modify: `application/daily_planner/chain_planner.py` (:53-117, :325-334)
- Test: `tests/test_industry_service_calculations.py`, `tests/test_chain_planner_keying.py`, `tests/test_industry_builder_t2_bpo_marker.py`

**Interfaces:**
- Produces: the tree node `recommendation_action="unknown"` plus `cost_unknown_reason`; `chain_planner._lowest_blueprint_by_activity_product(blueprint_data, activity: str) -> dict[int, int]`. The two public builders keep their signatures.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_industry_service_calculations.py`:

```python
# --- round 5 Task 9: visibility --------------------------------------------------


def test_an_invention_node_with_unknown_odds_says_unknown_not_take(monkeypatch) -> None:
    rows = _rows_with(probability_pct=0.0, bpc_runs=10, t2_limit=10)
    mj = _invention_overview(
        monkeypatch, blueprint_rows=rows, adm_overrides={"invention_probability_floor": 0.0},
    )[5002]["manufacturing_job"]
    node = _find_first_activity_node(mj["job_tree"], "invention")
    assert node is not None
    assert node["recommendation_action"] == "unknown"
    assert "no invention success probability" in node["cost_unknown_reason"]


def test_a_source_copy_covered_by_owned_runs_is_still_listed_for_its_elapsed_time(monkeypatch) -> None:
    # A 1-run owned source BPC covers the 0.2 expected attempts (no slot time,
    # no fee), but the 2 whole attempts still need 1 more copy run: 20 s.
    source_bpc = SimpleNamespace(
        type_id=1001, item_id=98, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=1,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    mj = _invention_overview(monkeypatch, owned_assets=([source_bpc], [], {}, {}, {}))[5002]["manufacturing_job"]
    copying = mj["activity_breakdown"]["invention_source_copying"]
    assert copying["duration_seconds"] == 0
    assert copying["elapsed_duration_seconds"] == 20
    assert mj["time_seconds"] == 96 + 8          # no copy slot time
    assert mj["elapsed_time_seconds"] == 96 + 80 + 20
```

Append to `tests/test_industry_builder_t2_bpo_marker.py`:

```python
def test_an_unknown_invention_node_is_labelled_unknown():
    assert ui.tree_node_activity_label({"activity": "invention", "recommendation_action": "unknown", "runs": 0}) == "unknown"
```

Append to `tests/test_chain_planner_keying.py`:

```python
def test_two_blueprints_for_one_product_resolve_to_the_lowest_id():
    data = {
        200: {"manufacturing": {"products": [{"type_id": 5, "quantity": 1}], "materials": []}},
        100: {"manufacturing": {"products": [{"type_id": 5, "quantity": 1}], "materials": []}},
    }
    assert build_product_to_blueprint_index(data) == {5: 100}


def test_missing_runs_log_one_warning_not_two(caplog):
    row = _row()
    del row["manufacturing_job"]["runs"]
    with caplog.at_level("WARNING"):
        _planner(optimal_te=20)._plan_t1_chain(
            decision=_decision(row), bp_type_id=999, bp_data=BLUEPRINT_DATA[999],
            bpo_assets_by_type_id={999: [_bpo(999, me=0, te=0)]}, bpc_assets_by_type_id={},
            market_depth_cache={}, bpo_opportunities=[], optimal_te_threshold=1.0,
            bpo_strong_buy_days=30.0, bpo_consider_days=90.0,
        )
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1 and "unusable batch run count" in warnings[0]


def test_an_unknown_optimal_te_still_warns_when_runs_are_missing(caplog):
    row = _row()
    del row["manufacturing_job"]["runs"]
    with caplog.at_level("WARNING"):
        _planner(optimal_te=None)._plan_t1_chain(
            decision=_decision(row), bp_type_id=999, bp_data=BLUEPRINT_DATA[999],
            bpo_assets_by_type_id={999: [_bpo(999, me=0, te=0)]}, bpc_assets_by_type_id={},
            market_depth_cache={}, bpo_opportunities=[], optimal_te_threshold=1.0,
            bpo_strong_buy_days=30.0, bpo_consider_days=90.0,
        )
    assert sum(r.levelname == "WARNING" for r in caplog.records) == 2
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv312/bin/python -m pytest tests/test_industry_service_calculations.py tests/test_industry_builder_t2_bpo_marker.py tests/test_chain_planner_keying.py -k "unknown_odds_says_unknown or still_listed_for_its_elapsed or labelled_unknown or lowest_id or one_warning_not_two or optimal_te_still_warns" -v`
Expected: FAIL. The node says `take`, `KeyError: 'invention_source_copying'`, the label is `take`, the index is `{5: 200}`, and 2 warnings are logged where 1 is expected. The TE test already PASSES and pins the kept behaviour.

- [ ] **Step 3: Implement**

3a. `_plan_invention_chain_for_variant`. Replace `recommendation_action=("invent" if invention_attempts > 0 else "take"),` with:

```python
                recommendation_action=(
                    "invent" if invention_attempts > 0
                    else ("unknown" if costs.get("cost_unknown_reason") else "take")
                ),
                **({"cost_unknown_reason": costs["cost_unknown_reason"]} if costs.get("cost_unknown_reason") else {}),
```

3b. `_build_single_product_row`. Replace `if source_copy_costs is not None and requires_invention_chain and source_copy_costs.get("duration_seconds", 0) > 0:` with:

```python
        if source_copy_costs is not None and requires_invention_chain and (
            source_copy_costs.get("duration_seconds", 0) > 0
            or (source_copy_costs.get("elapsed_duration_seconds") or 0) > 0
        ):
            # Listed so activity_breakdown's fees add up to total_job_cost, and
            # so a copy that only the whole attempts need (owned runs cover the
            # expected attempts: 0 slot time, no fee) still shows its elapsed time.
```

The body is unchanged. It adds `duration_seconds` (0 here) and a fee that is None for 0 runs.

3c. `industry_builder_ui.py`, `tree_node_activity_label`: change the set to `{"build", "take", "buy", "copy", "invent", "research", "unknown"}`.

3d. `chain_planner.py`. Replace both index builders with:

```python
def _lowest_blueprint_by_activity_product(blueprint_data: dict[int, dict[str, Any]], activity: str) -> dict[int, int]:
    """{product type_id of `activity`: lowest blueprint type_id with that product}.

    The lowest id wins whatever the dict order, matching
    blueprints.get_invention_source_blueprint_ids (ORDER BY blueprintTypeID).
    """
    index: dict[int, int] = {}
    for blueprint_type_id, entry in (blueprint_data or {}).items():
        if not isinstance(entry, dict):
            continue
        job = entry.get(activity)
        if not isinstance(job, dict):
            continue
        products = job.get("products")
        if not isinstance(products, list):
            continue
        for product in products:
            if not isinstance(product, dict):
                continue
            try:
                product_type_id = int(product.get("type_id") or 0)
            except (TypeError, ValueError):
                continue
            if product_type_id > 0:
                current = index.get(product_type_id)
                if current is None or int(blueprint_type_id) < current:
                    index[product_type_id] = int(blueprint_type_id)
    return index


def build_product_to_blueprint_index(blueprint_data: dict[int, dict[str, Any]]) -> dict[int, int]:
    """{product type_id: blueprint type_id} from SDE blueprint manufacturing data.

    blueprint_data is keyed by *blueprint* type_id. Anything that starts from a
    product (a material requirement, an overview row) crosses over through
    this index. When two blueprints make the same product the lowest id wins.
    """
    return _lowest_blueprint_by_activity_product(blueprint_data, "manufacturing")


def build_invention_source_index(blueprint_data: dict[int, dict[str, Any]]) -> dict[int, int]:
    """{invented blueprint type_id: source (T1) blueprint type_id}.

    In the SDE the invention activity lives on the *source* blueprint, and its
    products are the blueprints it invents into. A T2 overview row only knows
    its own (T2) blueprint, so finding the T1 BPO to copy means walking this
    index backwards. When several sources invent into one blueprint the lowest
    source id wins.
    """
    return _lowest_blueprint_by_activity_product(blueprint_data, "invention")
```

3e. `_plan_t1_chain`. Replace `if optimal_me is None or optimal_te is None:` (the second WARNING) with:

```python
            # An unknown optimal ME that the missing runs already explain was
            # logged just above; warn here only for what is still unexplained.
            if (optimal_me is None and runs_reason is None) or optimal_te is None:
```

- [ ] **Step 4: Run the new tests, then the full suite**

Run: the command from Step 2, then `.venv312/bin/python -m pytest -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/service.py \
  src/streamlit_ui/state/industry_builder_ui.py \
  src/eve_online_industry_tracker/application/daily_planner/chain_planner.py \
  tests/test_industry_service_calculations.py tests/test_industry_builder_t2_bpo_marker.py tests/test_chain_planner_keying.py
git commit -m "fix: unknown invention node, listed elapsed-only source copy, one runs warning, lowest-id blueprint index

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

# Part 3: tests, docs and clean-up

### Task 10: Tests, docs and clean-up (T1–T10; FU-03, FU-08, FU-09, FU-11, FU-15, FU-25, FU-28, FU-77, FU-79, RM-25)

None of this changes behaviour, except that the new tests pin existing behaviour. Each item is one small, same-shape edit. Run the full suite once at the end.

**Files:**
- Modify: `tests/test_daily_planner_service_helpers.py` (T1), `tests/test_daily_planner_repo.py` (T2), `application/daily_planner/models.py` (T3), `infrastructure/sde/blueprints.py` (T4), `tests/test_chain_planner_keying.py` (T5, T10), `application/daily_planner/fixture_export.py` + `tests/test_fixture_sanitisation.py` (T6), `application/daily_planner/character_assigner.py` (T7), `tests/test_shopping_list.py` (T8), `tests/test_industry_service_calculations.py` (T9), `application/daily_planner/service.py` (T10)

**Interfaces:**
- Consumes: the final code of Tasks 1–9 (T9 pins the 3-level nesting with the extra-batch plan).
- Produces: nothing new.

- [ ] **Step 1: T1, the stray blank lines**

`tests/test_daily_planner_service_helpers.py:180-183`: four blank lines sit before `def test_parse_isk_returns_none_for_unparseable_input` (the `def` that follows `test_wallet_balance_from_a_dict_keyed_by_division_string_balance`). Delete two of them, leaving the PEP 8 two.

- [ ] **Step 2: T2, a test of the get_analytics weight read path**

Append to `tests/test_daily_planner_repo.py`:

```python
def test_get_analytics_reads_out_of_band_weights_through_the_band(app_session, session_provider):
    """FU-03: get_analytics reads plan_learning_weights through read_weight,
    so a pre-clamp or hand-edited row is shown clamped, not raw."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    plan = BuildPlanModel(created_at=now, updated_at=now, status="active", freshness_score=1.0)
    app_session.add(plan)
    app_session.commit()
    app_session.add(BuildPlanItemModel(plan_id=plan.id, type_id=4242, decision="build"))
    app_session.add(PlanLearningWeightsModel(
        type_id=4242, accuracy_ema=9.0, velocity_multiplier=1.0, cost_multiplier=0.1,
        sample_count=3, last_updated=now, confidence_tier="low",
    ))
    app_session.commit()

    (item,) = _service(session_provider).get_analytics()["items"]
    assert item["accuracy_ema"] == 4.0       # LEARNING_WEIGHT_MAX
    assert item["cost_multiplier"] == 0.25   # LEARNING_WEIGHT_MIN
    assert item["velocity_multiplier"] == 1.0
```

Run: `.venv312/bin/python -m pytest tests/test_daily_planner_repo.py -k out_of_band -v`
Expected: PASS. The code already clamps; this pins it (mutation check: replace `read_weight(w, "accuracy_ema")` with `float(w.accuracy_ema)` in `service.py:get_analytics` and the test must fail, then revert).

- [ ] **Step 3: T3, T4 and T7, comments and docstrings**

`application/daily_planner/models.py:13`:

```python
    effective_velocity: float          # units/day; the analyzer's 0.01 floor when velocity_unknown_reason is set
```

`infrastructure/sde/blueprints.py`, `_me_reduction` docstring:

```python
    """ME level as the producer's combined material-reduction fraction.

    IndustryService._combine_reductions([me / 100.0]) for the ME term alone:
    1 - (1 - me/100), capped at 0.99. Bit-for-bit the producer's for ME 0-10,
    EVE's range. Outside it the two differ: the producer's _normalize_fraction
    takes abs() and rescales values >= 1, which ME 0-10 never reaches.
    """
```

`application/daily_planner/character_assigner.py`, inside `_research_blueprint_type_id`, above `if bp_type_id is None:`:

```python
        # Unreachable today: needs_me/te_research is set only by
        # ChainPlanner._plan_t1_chain for an owned BPO found through this same
        # accessor, so a flagged row always has a blueprint id. Kept so a
        # future flag source cannot schedule research on a product id.
```

- [ ] **Step 4: T5, a real SDE group in the facility-group test**

In `tests/test_chain_planner_keying.py::test_name_token_groups_need_their_category`, replace the two lines

```python
    # A hypothetical Module-category group whose name contains "component".
    assert group({"group_name": "Component Analyzer", "category_name": "Module"}) is None
```

with

```python
    # A real non-Commodity group whose name contains "component" (SDE group 1676).
    assert group({"group_name": "Named Components", "category_name": "Material"}) is None
```

- [ ] **Step 5: T6, fixture-export references and the activity-inherited pin**

In `fixture_export.py`, change the `BLUEPRINT_SOURCE_KINDS` comment's first lines to:

```python
#: Every value IndustryService assigns to `blueprint_source_kind`: the
#: literals in IndustryService._build_single_product_row and
#: _plan_blueprint_chain_for_quantity, plus the activity names a
#: prerequisite-chain node inherits (`blueprint_source_kind = activity`).
```

The rest of the comment stays.

In `tests/test_fixture_sanitisation.py`, move the mid-file imports (`import ast`, `import os`, `import pytest`, and the `BLUEPRINT_SOURCE_KINDS` import, all with `# noqa: E402`) into the top import block without the `noqa`. Add a module constant and the pin:

```python
_PRODUCER = os.path.join(
    os.path.dirname(__file__), "..", "src", "eve_online_industry_tracker",
    "application", "industry", "service.py",
)


def test_every_activity_a_prerequisite_node_inherits_is_allow_listed():
    """_plan_blueprint_chain_for_quantity sets blueprint_source_kind = activity,
    and the activities it is called with are the child_activity literals."""
    with open(_PRODUCER, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    activities = {
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "child_activity" for t in node.targets)
        and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
    }
    assert activities == {"manufacturing", "reaction"}
    assert activities <= BLUEPRINT_SOURCE_KINDS
```

Make `test_every_literal_source_kind_the_producer_assigns_is_allow_listed` use `_PRODUCER` instead of its inline path.

- [ ] **Step 6: T8, the caption test catches only its own stop**

In `tests/test_shopping_list.py::_render_tab_captions`, replace

```python
    try:
        page._render_shopping_list_tab(rows)
    except Exception:
        pass
```

with

```python
    try:
        page._render_shopping_list_tab(rows)
    except _Stop:
        pass
```

Run: `.venv312/bin/python -m pytest tests/test_shopping_list.py -k caption -v`
Expected: PASS. If another exception now surfaces from `_render_shopping_list_tab`, a Streamlit call the test does not fake is being hit. Fake that call with `monkeypatch.setattr(page.st, "<name>", lambda *a, **k: None)` like the others. Never widen the `except`.

- [ ] **Step 7: T9, a 3-level invention test (T2 → nested T2 → nested T2)**

Append to `tests/test_industry_service_calculations.py`:

```python
# --- round 5 Task 10 T9: three invented levels ---------------------------------


def _three_level_invention_rows() -> list[dict]:
    """5002 (T2) <- 1 x 5102 (T2) <- 1 x 5202 (T2) <- 10 Tritanium per run.
    Each T2 blueprint is invented from its own T1 source with its own
    datacore (204, 205, 206): p 0.5, 10 runs per BPC, 2 per attempt."""
    def source(bp, product, invented_bp, datacore):
        return {
            "blueprint_type_id": bp,
            "blueprint": {"type_id": bp, "type_name": f"Source {bp}", "base_price": 1000.0},
            "manufacturing_job": {
                "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 3, "category_name": "Material"}],
                "skill_entries": [], "time_seconds": 50, "max_production_limit": 10,
                "products": [{"type_id": product, "type_name": f"Source item {product}", "quantity": 1,
                              "meta_group_name": "Tech I"}],
            },
            "copying_job": {"time_seconds": 20},
            "invention_job": {
                "materials": [{"type_id": datacore, "type_name": f"Datacore {datacore}", "quantity": 2,
                               "category_name": "Material"}],
                "skill_entries": [], "time_seconds": 40,
                "products": [{"probability_pct": 50.0, "quantity": 10,
                              "product": {"type_id": invented_bp, "type_name": f"T2 Blueprint {invented_bp}"}}],
            },
        }

    def t2(bp, product, material, quantity):
        return {
            "blueprint_type_id": bp,
            "blueprint": {"type_id": bp, "type_name": f"T2 Blueprint {bp}", "base_price": 5000.0},
            "manufacturing_job": {
                "materials": [{"type_id": material, "type_name": f"Input {material}", "quantity": quantity,
                               "category_name": "Material"}],
                "skill_entries": [], "time_seconds": 100, "max_production_limit": 10,
                "products": [{"type_id": product, "type_name": f"T2 item {product}", "quantity": 1,
                              "meta_group_name": "Tech II"}],
            },
        }

    return [
        source(1001, 6001, 9002, 204), source(1101, 6101, 9102, 205), source(1201, 6201, 9202, 206),
        t2(9002, 5002, 5102, 1), t2(9102, 5102, 5202, 1), t2(9202, 5202, 34, 10),
    ]


_THREE_LEVEL_PRICES = {34: 5.0, 204: 100.0, 205: 100.0, 206: 100.0, 5102: 100_000.0, 5202: 100_000.0}


def test_three_invented_levels_buy_every_levels_datacores_for_every_batch(monkeypatch) -> None:
    # The deepest component runs on an owned 10-run 9202 BPC in batch 1; each
    # level invents 1 run otherwise: 1 BPC, 2 attempts, 4 datacores.
    deep_bpc = SimpleNamespace(
        type_id=9202, item_id=99, is_blueprint_copy=True,
        blueprint_material_efficiency=0, blueprint_time_efficiency=0, blueprint_runs=10,
        location_id=None, location_type=None, location_flag=None, top_location_id=None,
        container_name=None, ship_name=None, is_singleton=True, quantity=1,
    )
    row = _invention_overview(
        monkeypatch, blueprint_rows=_three_level_invention_rows(), prices=_THREE_LEVEL_PRICES,
        owned_assets=([deep_bpc], [], {}, {}, {}),
    )[5002]
    mj = row["manufacturing_job"]

    batch_one = mj["invention_procurement_materials"]
    assert (batch_one["204"]["quantity"], batch_one["205"]["quantity"]) == (4, 4)
    assert "206" not in batch_one                       # the owned BPC covers batch 1
    extra = mj["invention_procurement_materials_per_extra_batch"]
    assert (extra["204"]["quantity"], extra["205"]["quantity"], extra["206"]["quantity"]) == (4, 4, 4)

    row["max_batches_total"] = 3
    needs = {t: (_shopping_line(row, t) or {}).get("need") for t in (204, 205, 206)}
    assert needs == {204: 12, 205: 12, 206: 8}
```

Run: `.venv312/bin/python -m pytest tests/test_industry_service_calculations.py -k three_invented_levels -v`
Expected: PASS.

- [ ] **Step 8: T10, the remaining RM-25 style items**

`application/daily_planner/service.py:1070`: change `extra_blueprint_type_ids: Any = (),` to `extra_blueprint_type_ids: Iterable[int] = (),`. Add `Iterable` to the module's `typing` (or `collections.abc`) import, following the file's existing import style.

`tests/test_chain_planner_keying.py`: move the three mid-file imports into the top import block and drop their `# noqa: E402`:
- `build_invention_source_index` (now at :279);
- the `infrastructure.sde.blueprints` block (:364);
- `ChainPlan` (:657).

Merge the first into the existing `from ...chain_planner import (ChainPlanner, build_product_to_blueprint_index)` import.

- [ ] **Step 9: Run the full suite**

Run: `.venv312/bin/python -m pytest -q`
Expected: all pass.

- [ ] **Step 10: Commit**

```bash
git add tests/test_daily_planner_service_helpers.py tests/test_daily_planner_repo.py \
  src/eve_online_industry_tracker/application/daily_planner/models.py \
  src/eve_online_industry_tracker/infrastructure/sde/blueprints.py \
  src/eve_online_industry_tracker/application/daily_planner/character_assigner.py \
  tests/test_chain_planner_keying.py \
  src/eve_online_industry_tracker/application/daily_planner/fixture_export.py tests/test_fixture_sanitisation.py \
  tests/test_shopping_list.py tests/test_industry_service_calculations.py \
  src/eve_online_industry_tracker/application/daily_planner/service.py
git commit -m "test: close the round-5 test, doc and clean-up items (T1-T10)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Coverage: every OPEN code item of the audit

| Audit item | Task |
|---|---|
| C1 / FU-81b, batches 2..N at the owned ME | 4 (partial batch, D3) + 5 (extra-batch plan) |
| C2 / FU-46, chance without skills | 2 |
| C3 / FU-81a / FU-47, invented ME 0 / TE 0, no decryptors | 1 (baseline, data) + 3 (selection, inputs) |
| C4 / FU-58, nested copy job of the T2 BP | 1 |
| C5 / FU-61, rejected sub-build spends stock | 6 |
| C6 / FU-63, owned datacores valued per invention | 6 |
| C7 / FU-50, fractional source-copy runs truncated | 6 |
| C8 / FU-75, owned datacores not netted past batch 1 | 7 |
| C9 / FU-70, owned = largest per-row take | 7 |
| V1 / FU-67, Capital Reserved | 8 |
| V2 / FU-53, `take` with 0 runs on unknown odds | 9 |
| V3 / FU-54, hidden source-copy breakdown row | 9 |
| V4 / FU-12, two WARNINGs for missing runs | 9 |
| V5 / FU-27, dict-order tie-break | 9 |
| T1 / FU-08 … T10 / RM-25 | 10 (T10's duplicate index builders: 9) |

No OPEN code item is closed by a user decision alone. The seven controller-only acceptances (FU-04, FU-14, FU-32, FU-34, FU-56, RM-05, RM-35) are not OPEN; Q5 below puts them to the user.

## Self-review

1. **Spec coverage.**
   - Binding spec: the plan keeps the fail-loud policy. Missing inputs are recorded reasons and WARNINGs: no decryptor data, no market price, unknown skills, unknown fees. The AST guard and the "no `or 0.0` on contract fields" rule hold, and new planner reads go through `overview_row` accessors.
   - The spec's "producer out of scope" line is overridden by the user for this round (Global Constraints).
   - D1: Tasks 1 and 3. D2: Task 2. D3: Task 4 (with Task 5 for the extra batches). D4: Task 8.
   - Each user sub-requirement has a step:
     - SDE attribute ids are verified and recorded;
     - decryptors are priced like datacores (Task 1, 7d);
     - the invented BPC is ME 2 / TE 4 plus modifiers (Task 1);
     - decryptors are amortized and get whole-attempt lines (Task 3, 7e/7f);
     - the admin switch (Task 3, 7a);
     - the UI shows the decryptor and the reason (Task 3, 8e/8f);
     - the single named criterion function (`select_best_decryptor`);
     - the floor is kept (Task 2 test);
     - unknown skills mean base chance + WARNING + reason (Task 2);
     - reuse of the assigner's ranking: one shared factor (Task 2, 7k);
     - fees are found or added (Task 8);
     - unpriced shows as unknown (Task 8).
2. **Placeholder scan.** No TBD, TODO, "similar to" or helpers named only by their role. Every test helper used here was read in its file at `a546916`: `_invention_overview`, `_invention_service`, `_rows_with`, `_owned_t2_bpc`, `_shopping_line`, `_unknown_need_rows`, `_make_row`, `_owned_line`, `_invention_row`, `_datacore`, `_planner`, `_row`, `_decision`, `_bpo`, `_char`, `_skill`, `_Chars`, `_AdminStub`, `_research_pilot`, `_service`, `ui`. Task 10 Step 1 cites the blank lines by line number (verified at `a546916`).
3. **Type consistency.** These names are used identically across tasks:
   - `DecryptorOption` / `NO_DECRYPTOR` / `invented_blueprint_efficiency` / `invented_runs_per_blueprint` (1→3,4,5);
   - `InventionContext.decryptor_for_blueprint` (1→3,4), `.inventor_skill_levels_by_character_id` / `.encryption_skill_type_ids` / `.warn_once` (2);
   - `_expected_invention_costs(..., decryptor, invention_context)` (2→3);
   - `_full_batch_invention_whole_attempts` returns a 3-tuple from Task 2 on, and every caller in Tasks 2 and 5 unpacks three;
   - `_invention_whole_attempt_requirements(..., decryptor=)` (3→5);
   - `job_segments` (4→5);
   - `invention_valuation_stock` (6, added to every signature that Task 5's `_plan_extra_batch` calls).

   Task 5 rewrites the Task 2 row-output step (7h) and says so. `AssignedAction` gains `decryptor_type_id/decryptor_type_name` (3) and `estimated_job_fee_isk` (8). `_lowest_blueprint_by_activity_product` (9) keeps both public builder signatures.
4. **Review Focus.** Five classes are listed, each with its pinning test in the owning task. I also checked these, and each is covered by a task test:
   - decryptor ties (unit test);
   - a decryptor makes ME negative (clamped, unit test);
   - unknown invention odds with decryptors (Task 3 test);
   - a material cycle during decryptor evaluation: the guard in `_decryptor_choice_for`. There is no test fixture for a cycle because the SDE has none; the guard returns no decryptor without caching.
   - **Performance risk, which no test exercises:** decryptor evaluation builds up to 9 rows per invented blueprint once per refresh. The memo prevents repeats. If the refresh time is unacceptable, the admin switch turns it off. Measure one real refresh after Task 3 (the user's own app instance).

## Open questions for the user

1. **Selection criterion (Task 3).** The proposal is **(a): the lowest total cost per manufactured unit** (materials + amortized invention inputs including the decryptor + job fees), with ties going to no decryptor.
   - (a) is the only candidate that every row can compute when it is built: the producer has the material, datacore and decryptor prices then.
   - (b) ISK/hour needs the sale proceeds (broker fee, tax, sell price), which are enriched later and are often unknown. It would also reward a TE decryptor that raises the cost per unit, which ties up more capital per item.
   - (c) profit per invention cycle mixes in the runs per BPC, so it favours run-count decryptors even when each unit costs more.

   Switching is a change to `select_best_decryptor` and its evaluation input, nothing else. Do you want (a)?
2. **Research fees (Task 8).** ME/TE research actions have no estimated install fee: the producer does not cost research for owned BPOs. Capital Reserved then shows "≥ X" plus "install fee unknown for n job(s)". Should research fees be estimated too, from the producer's research cost model, in a follow-up?
3. **The 6 SDE blueprints with no encryption skill** (the "Exotic Plasma S" family; see the verification section). They get the base chance with a reason. Do you prefer EVE's formula with only the science term?
4. **Best inventor vs the assigned pilot.** The producer assumes the corp's best inventor per item. The planner gives the invent action to the best pilot with a free research slot. If that pilot is busy, a less skilled one invents and the real chance is lower than costed. Is that acceptable, or should the planner leave the invention for the best inventor?
5. **Seven controller-only acceptances** (audit status ACCEPTED*, never put to you):
   - FU-04, a corrupt weight row warns once per reader;
   - FU-14, no group rig when the component group is uncertain;
   - FU-32, `_ACTION_ORDER` not derived;
   - FU-34, red intermediate commits are not rewritten;
   - FU-56, invention elapsed time assumes one slot in sequence;
   - RM-05, booleans and type ids in the fixture disclose BPO ownership;
   - RM-35, analytics reads degrade to empty on DB errors.

   Confirm them, or name any to reopen.

## Dropped after verification

- **The admin settings for the invented ME/TE** (the audit's smallest fix for C3). ME 2 / TE 4 is EVE's rule, so they are constants in `invention_model`, not settings (D1 fixed the default; YAGNI).
- **Splitting the daily planner's manufacture action into two jobs.** The overview never yields a partly owned top-level batch (see the verification section), so the planner's single action is never wrong today. The producer publishes `job_segments` for when a variant can be partial.
- **Sizing a partly owned batch at min(owned, invented) ME** (the audit's first option for C1). D3 replaces it with two exactly sized jobs.
- **The nested per-extra-batch datacore machinery of rounds 3/4** (`_extra_batch_invention_requirements`, `invention_requirements_per_extra_batch`). Task 5's single extra-batch plan replaces it. Kept, it would contradict the re-sized materials.
- **Adding the decryptor to the invention fee's EIV.** The fee basis stays the datacores'. The decryptor is costed as an input only, so the fee model and its tests do not move.
- **Identifying the science skills through the datacores' `requiredSkill1`.** 8 SDE blueprints disagree with their required skills.
- **The racial, relic and subsystem "decryptor" groups** (728–735, 979). They are not T2 invention decryptors: no market group, or not consumed by T2 invention. Only group 1304 is used, exactly as the existing loader does.
- **The concern that `_normalize_fraction` takes abs() of a negative decryptor modifier.** Moot: the invented ME/TE is clamped to ≥ 0 before it reaches the producer (ME 2 − 2 = 0 at worst, TE 4 − 2 = 2).
- **A "0 fee" on the V3 breakdown row** (the audit's wording). With 0 missing source runs the copy has no process value, so the fee is None (not priced), and the sum test of breakdown fees is unaffected.




