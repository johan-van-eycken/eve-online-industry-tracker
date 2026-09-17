# Daily Planner Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Daily Planner read the data that actually exists, fix the 15 review findings plus 4 discovered during planning, finish the unfinished Phase A work, and make this class of silent failure impossible to reintroduce.

**Architecture:** One module owns every read of an IndustryService overview row (`application/industry/overview_row.py`), extracted from the Streamlit UI accessors that already work against real rows. A frozen `PlannerInputRow` dataclass is the planner's only entry point for row data and raises `PlannerInputError` naming the missing field. SDE lookups go through one `TypeMetadataResolver` wrapping the existing `get_type_data()`. Two additive SQLite migrations back the job-activity index and real freshness scoring.

**Tech Stack:** Python 3.10–3.12, SQLAlchemy 2.x (`Mapped`/`mapped_column`), SQLite, pytest, Flask, Streamlit.

**Spec:** `docs/superpowers/specs/2026-09-17-daily-planner-remediation-design.md`

## Global Constraints

- Python `>=3.10,<3.13`. `from __future__ import annotations` at the top of every new module, matching the existing package.
- Do not modify the overview-row **producer** (`application/industry/service.py`) except to add the `activity_id` write in the job sync (Task 13). The planner adapts to the producer.
- No new third-party dependencies.
- Migrations are additive only, idempotent, and go through the existing `_ensure_column(db, table=, column=, ddl_type=)` helper in `infrastructure/schema_migrations.py`.
- SQLAlchemy models use the `Mapped[Optional[X]] = mapped_column(...)` style already in `infrastructure/models.py`.
- `*.md` is gitignored repo-wide. Spec and plan files need `git add -f`.
- **This repository is public.** No real corporation data, wallet balance, character name or structure id may be committed. Test fixtures are sanitised (Task 2).
- Tests live in `tests/` as flat `test_*.py` files. Run with `python -m pytest`.
- Every commit message ends with:
  `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`

---

## File Structure

**Created:**

| Path | Responsibility |
|---|---|
| `src/eve_online_industry_tracker/application/industry/overview_row.py` | The only module that knows overview-row key names. Pure accessors, no I/O. |
| `src/eve_online_industry_tracker/application/industry/type_metadata.py` | `TypeMetadataResolver` — batch `type_id` → meta group id / category name, over the SDE. |
| `src/eve_online_industry_tracker/application/daily_planner/input_row.py` | `PlannerInputRow`, `PlannerInputError`. The planner's input contract. |
| `src/eve_online_industry_tracker/application/daily_planner/fixture_export.py` | `sanitise_overview_rows()` — strips identity, randomises magnitudes, keeps shape. |
| `tests/conftest.py` | `sys.path` bootstrap + shared in-memory DB fixtures. |
| `tests/fixtures/overview_rows_real.json` | Sanitised capture of real overview rows (committed). |
| `tests/test_overview_row.py` | Accessor unit tests. |
| `tests/test_planner_input_row.py` | Contract construction + failure-message tests. |
| `tests/test_overview_row_contract.py` | Asserts the real fixture satisfies the contract. |
| `tests/test_fixture_sanitisation.py` | Asserts the committed fixture carries no identity data. |
| `tests/test_daily_planner_integration.py` | End-to-end pipeline over the real fixture + real ORM objects. |

**Modified:** `pipeline_analyzer.py`, `profitability_scorer.py`, `character_assigner.py`, `shopping_list_builder.py`, `chain_planner.py`, `feedback_processor.py`, `service.py`, `models.py` (planner), `infrastructure/models.py`, `schema_migrations.py`, `corporations/corporation.py`, `streamlit_ui/state/industry_builder_ui.py`, `streamlit_ui/components/daily_planner/status_bar.py`, `flask_app/routes/daily_planner.py`.

---

### Task 1: Test harness foundation

No `conftest.py` exists; each test file repeats its own `sys.path` insert and sqlite bootstrap. Everything downstream needs shared fixtures.

**Files:**
- Create: `tests/conftest.py`
- Test: `tests/test_conftest_smoke.py`

**Interfaces:**
- Produces: fixtures `app_session` (SQLAlchemy `Session` on in-memory sqlite with all `BaseApp` tables created) and `planner_repo` (`DailyPlannerRepository` bound to it).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_conftest_smoke.py
from __future__ import annotations

from eve_online_industry_tracker.infrastructure.models import BuildPlanModel


def test_app_session_fixture_has_planner_tables(app_session):
    app_session.add(BuildPlanModel(status="active"))
    app_session.commit()
    assert app_session.query(BuildPlanModel).count() == 1


def test_planner_repo_fixture_is_wired(planner_repo):
    assert planner_repo is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_conftest_smoke.py -v`
Expected: FAIL — `fixture 'app_session' not found`.

- [ ] **Step 3: Write conftest**

```python
# tests/conftest.py
from __future__ import annotations

import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.infrastructure.models import BaseApp  # noqa: E402
from eve_online_industry_tracker.infrastructure.persistence.daily_planner_repo import (  # noqa: E402
    DailyPlannerRepository,
)

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture()
def app_engine():
    engine = create_engine("sqlite://", future=True)
    BaseApp.metadata.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def app_session(app_engine) -> Session:
    factory = sessionmaker(bind=app_engine, future=True)
    session = factory()
    try:
        yield session
    finally:
        session.close()


class _StaticSessionProvider:
    """Session provider that always hands back the same test session.

    close() is a no-op so repository code that closes its session does not
    invalidate the fixture for the rest of the test.
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    def app_session(self) -> Session:
        self._session.close = lambda: None  # type: ignore[method-assign]
        return self._session


@pytest.fixture()
def session_provider(app_session) -> _StaticSessionProvider:
    return _StaticSessionProvider(app_session)


@pytest.fixture()
def planner_repo(session_provider) -> DailyPlannerRepository:
    return DailyPlannerRepository(session_provider=session_provider)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_conftest_smoke.py -v`
Expected: PASS (2 passed).

If `DailyPlannerRepository.__init__` takes a different keyword than `session_provider`, read
`src/eve_online_industry_tracker/infrastructure/persistence/daily_planner_repo.py` and match its
actual signature — do not change the repository.

- [ ] **Step 5: Confirm the existing suite still passes**

Run: `python -m pytest tests/ -q`
Expected: no new failures versus the pre-task baseline. The new `conftest.py` adds `src` to
`sys.path` before each test module's own insert, which is harmless and idempotent.

- [ ] **Step 6: Commit**

```bash
git add tests/conftest.py tests/test_conftest_smoke.py
git commit -m "test: add conftest with shared in-memory app session fixtures

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Fixture sanitiser and capture route

The real overview rows exist only in `IndustryService`'s in-memory cache, so a standalone script
cannot reach them — the dump has to happen inside the running app process. A debug route does the
dump, and sanitises **before** the data leaves that process, so the raw rows never touch disk.

**Files:**
- Create: `src/eve_online_industry_tracker/application/daily_planner/fixture_export.py`
- Create: `tests/test_fixture_sanitisation.py`
- Modify: `src/flask_app/routes/daily_planner.py`

**Interfaces:**
- Produces: `sanitise_overview_rows(rows: list[dict], *, seed: int = 20260917) -> list[dict]`;
  `IDENTITY_KEY_SUBSTRINGS: tuple[str, ...]`; `GET /planner/debug/overview-fixture`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_fixture_sanitisation.py
from __future__ import annotations

from eve_online_industry_tracker.application.daily_planner.fixture_export import (
    IDENTITY_KEY_SUBSTRINGS,
    sanitise_overview_rows,
)

RAW = [
    {
        "type_id": 12345,
        "type_name": "Hobgoblin II",
        "quantity": 100,
        "isk_per_hour": 4_200_000.0,
        "profit_amount": 1_500_000.0,
        "profit_margin_fraction": 0.18,
        "corporation_id": 98000001,
        "character_name": "Some Pilot",
        "structure_id": 1029999999999,
        "wallet_balance": 8_400_000_000.0,
        "manufacturing_job": {
            "runs": 20,
            "material_cost": 55_000_000.0,
            "materials": {"34": {"type_id": 34, "quantity": 1000}},
        },
    }
]


def test_structure_and_public_identifiers_are_preserved():
    out = sanitise_overview_rows(RAW)
    assert len(out) == 1
    row = out[0]
    assert row["type_id"] == 12345
    assert row["type_name"] == "Hobgoblin II"
    assert set(row["manufacturing_job"]) == {"runs", "material_cost", "materials"}
    assert row["manufacturing_job"]["materials"]["34"]["type_id"] == 34


def test_identity_keys_are_dropped_at_every_depth():
    out = sanitise_overview_rows(RAW)[0]
    for key in ("corporation_id", "character_name", "structure_id", "wallet_balance"):
        assert key not in out
    assert IDENTITY_KEY_SUBSTRINGS  # guard against an empty deny-list


def test_magnitudes_are_replaced_but_types_and_signs_kept():
    out = sanitise_overview_rows(RAW)[0]
    assert out["isk_per_hour"] != RAW[0]["isk_per_hour"]
    assert isinstance(out["isk_per_hour"], float)
    assert out["isk_per_hour"] > 0
    assert isinstance(out["quantity"], int)
    assert out["quantity"] > 0


def test_sanitisation_is_deterministic():
    assert sanitise_overview_rows(RAW) == sanitise_overview_rows(RAW)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_fixture_sanitisation.py -v`
Expected: FAIL — `ModuleNotFoundError: ...fixture_export`.

- [ ] **Step 3: Write the sanitiser**

```python
# src/eve_online_industry_tracker/application/daily_planner/fixture_export.py
"""Sanitise real product-overview rows into a committable test fixture.

This repository is public. A raw overview dump exposes the corporation's
holdings, production mix and margins, so nothing raw may be written to disk.
What the tests need is the row *shape* — which keys exist and how they nest —
not the values, so keys and structure are preserved while magnitudes are
replaced with deterministic synthetic numbers and identity fields are dropped.
"""
from __future__ import annotations

import random
from typing import Any

# Any key containing one of these substrings is removed, at any nesting depth.
IDENTITY_KEY_SUBSTRINGS: tuple[str, ...] = (
    "character",
    "corporation",
    "corp_id",
    "owner",
    "wallet",
    "balance",
    "station",
    "structure",
    "facility",
    "location",
    "order_id",
    "job_id",
    "installer",
    "token",
    "secret",
)

# Keys whose values are public static data and must survive verbatim.
_PUBLIC_KEYS: frozenset[str] = frozenset({"type_id", "type_name", "blueprint_type_id"})


def _is_identity_key(key: str) -> bool:
    if key in _PUBLIC_KEYS:
        return False
    lowered = key.lower()
    return any(part in lowered for part in IDENTITY_KEY_SUBSTRINGS)


def _scramble_number(value: Any, rng: random.Random) -> Any:
    """Replace a magnitude with a plausible one, preserving type and sign."""
    factor = rng.uniform(0.5, 1.5)
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        scrambled = int(value * factor)
        if value != 0 and scrambled == 0:
            scrambled = 1 if value > 0 else -1
        return scrambled
    return round(float(value) * factor, 2)


def _sanitise_value(key: str, value: Any, rng: random.Random) -> Any:
    if isinstance(value, dict):
        return _sanitise_mapping(value, rng)
    if isinstance(value, list):
        return [_sanitise_value(key, item, rng) for item in value]
    if key in _PUBLIC_KEYS or isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return _scramble_number(value, rng)
    return value


def _sanitise_mapping(row: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if _is_identity_key(str(key)):
            continue
        out[str(key)] = _sanitise_value(str(key), value, rng)
    return out


def sanitise_overview_rows(
    rows: list[dict[str, Any]], *, seed: int = 20260917
) -> list[dict[str, Any]]:
    """Return sanitised copies of `rows`, safe to commit to a public repository."""
    rng = random.Random(seed)
    return [_sanitise_mapping(row, rng) for row in rows if isinstance(row, dict)]
```

Note `type_name` survives because it is public SDE data and makes fixture failures readable.
Nested material maps keyed by stringified `type_id` keep their keys — those are SDE ids, not
identity.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_fixture_sanitisation.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Add the capture route**

Append to `src/flask_app/routes/daily_planner.py`:

```python
@daily_planner_bp.get("/planner/debug/overview-fixture")
def debug_overview_fixture():
    """Dump a sanitised sample of the live overview cache for test fixtures.

    Disabled unless EVE_ENABLE_DEBUG_FIXTURE=1. Rows are sanitised inside this
    process, so raw corporation data never crosses the response boundary.
    """
    import os

    from eve_online_industry_tracker.application.daily_planner.fixture_export import (
        sanitise_overview_rows,
    )

    if os.environ.get("EVE_ENABLE_DEBUG_FIXTURE") != "1":
        return error(message="debug fixture export disabled", status_code=404)

    require_ready(get_state())
    rows = get_state().industry_service.get_cached_overview_rows()
    if not rows:
        return error(
            message="no cached overview — refresh the product overview first",
            status_code=409,
        )

    limit = int(request.args.get("limit", 20))
    return ok(data=sanitise_overview_rows(rows[:limit]))
```

If the attribute on `get_state()` is not `industry_service`, read `src/flask_app/state.py` and use
the real name.

- [ ] **Step 6: Verify the route is registered and gated**

Run: `python -c "
import sys; sys.path.insert(0, 'src')
from flask_app.routes.daily_planner import daily_planner_bp
paths = [r.rule for r in daily_planner_bp.deferred_functions and [] or []]
print('blueprint imported OK')
"`
Expected: `blueprint imported OK` with no ImportError.

- [ ] **Step 7: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/fixture_export.py \
        tests/test_fixture_sanitisation.py src/flask_app/routes/daily_planner.py
git commit -m "test: add overview-row fixture sanitiser and gated capture route

Sanitises inside the app process so raw corp data never reaches disk; this
repository is public.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Capture the real fixture — MANUAL STEP

This task needs the running app and cannot be automated. Everything after it works without the
fixture (the contract test skips), so it does not block the rest of the plan — but it is the only
source of ground truth, so do it as early as the app is available.

**Files:**
- Create: `tests/fixtures/overview_rows_real.json`

- [ ] **Step 1: Start the app with the export enabled**

```bash
EVE_ENABLE_DEBUG_FIXTURE=1 python main.py
```

- [ ] **Step 2: Warm the overview cache**

In the Streamlit UI: open **Industry Builder** → trigger a product overview refresh. Wait for it to
finish. The cache is populated only by a completed refresh.

- [ ] **Step 3: Capture**

```bash
mkdir -p tests/fixtures
curl -s 'http://localhost:5000/planner/debug/overview-fixture?limit=25' \
  | python -c "import json,sys; d=json.load(sys.stdin); json.dump(d['data'], open('tests/fixtures/overview_rows_real.json','w'), indent=2, sort_keys=True)"
```

Adjust the port to whatever `main.py` serves Flask on.

- [ ] **Step 4: Verify the capture by eye**

Run: `python -c "
import json
rows = json.load(open('tests/fixtures/overview_rows_real.json'))
print('rows:', len(rows))
print('top-level keys:', sorted(rows[0]))
print('manufacturing_job keys:', sorted(rows[0].get('manufacturing_job', {})))
"`

Confirm: more than one row; `type_id`, `type_name`, `quantity`, `isk_per_hour` present; a
`manufacturing_job` sub-dict containing `runs` and `material_cost`. Confirm no key mentions a
character, corporation, wallet or structure.

If `pipeline_units_on_market` / `pipeline_days_supply` / `price_trend_7d_pct` are absent, the
refresh did not run the enrichment passes that add them. Re-run the refresh with market history
enabled before continuing — Tasks 6 and 9 depend on those keys being real.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/overview_rows_real.json
git commit -m "test: add sanitised real overview-row fixture

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Shared overview-row accessors

Extract the accessors that already read real rows correctly out of the Streamlit UI, and add the
ones the planner needs. This is the module that makes every later task a one-line change.

**Files:**
- Create: `src/eve_online_industry_tracker/application/industry/overview_row.py`
- Modify: `src/streamlit_ui/state/industry_builder_ui.py:20-60`
- Test: `tests/test_overview_row.py`

**Interfaces:**
- Produces: `get_manufacturing_job`, `get_product_quantity`, `get_effective_runs`,
  `skill_requirements_met`, `get_material_cost_total`, `get_material_cost_per_unit`,
  `get_pipeline_units_in_jobs`, `get_pipeline_units_on_market`, `get_pipeline_days_supply`,
  `get_price_trend_7d_pct`, `get_price_trend_30d_pct`, `get_days_of_supply`, `get_isk_per_hour`,
  `get_profit_amount`, `get_profit_margin_fraction`, `get_blueprint_type_id`,
  `get_meta_group_name`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_overview_row.py
from __future__ import annotations

from eve_online_industry_tracker.application.industry import overview_row as orow

ROW = {
    "type_id": 12345,
    "type_name": "Hobgoblin II",
    "quantity": 10,
    "blueprint_type_id": 999,
    "isk_per_hour": 4_000_000.0,
    "profit_amount": 1_000_000.0,
    "profit_margin_fraction": 0.2,
    "days_of_supply": 6.5,
    "meta_group_name": "Tech II",
    "pipeline_units_in_jobs": 40,
    "pipeline_units_on_market": 60,
    "pipeline_days_supply": 12.5,
    "price_trend_7d_pct": 3.5,
    "price_trend_30d_pct": -2.0,
    "manufacturing_job": {"runs": 20, "material_cost": 50_000_000.0},
}


def test_manufacturing_job_returns_empty_dict_when_absent_or_wrong_type():
    assert orow.get_manufacturing_job({}) == {}
    assert orow.get_manufacturing_job({"manufacturing_job": "nope"}) == {}


def test_runs_comes_from_manufacturing_job():
    assert orow.get_effective_runs(ROW) == 20


def test_runs_falls_back_to_product_quantity():
    assert orow.get_effective_runs({"quantity": 7}) == 7


def test_material_cost_per_unit_divides_the_total_by_quantity():
    # 50,000,000 over 10 units
    assert orow.get_material_cost_per_unit(ROW) == 5_000_000.0


def test_material_cost_per_unit_is_none_without_a_usable_quantity():
    assert orow.get_material_cost_per_unit({"manufacturing_job": {"material_cost": 10.0}}) is None


def test_material_cost_per_unit_is_none_when_the_cost_is_absent():
    assert orow.get_material_cost_per_unit({"quantity": 10}) is None


def test_pipeline_accessors_read_the_producer_keys():
    assert orow.get_pipeline_units_in_jobs(ROW) == 40
    assert orow.get_pipeline_units_on_market(ROW) == 60
    assert orow.get_pipeline_days_supply(ROW) == 12.5


def test_pipeline_days_supply_is_none_when_the_producer_could_not_compute_it():
    assert orow.get_pipeline_days_supply({"pipeline_days_supply": None}) is None
    assert orow.get_pipeline_days_supply({}) is None


def test_price_trend_accessors_use_the_7d_and_30d_keys():
    assert orow.get_price_trend_7d_pct(ROW) == 3.5
    assert orow.get_price_trend_30d_pct(ROW) == -2.0
    assert orow.get_price_trend_30d_pct({}) is None


def test_price_trend_7d_defaults_to_zero_meaning_flat():
    assert orow.get_price_trend_7d_pct({}) == 0.0


def test_scalar_accessors():
    assert orow.get_isk_per_hour(ROW) == 4_000_000.0
    assert orow.get_profit_amount(ROW) == 1_000_000.0
    assert orow.get_profit_margin_fraction(ROW) == 0.2
    assert orow.get_days_of_supply(ROW) == 6.5
    assert orow.get_blueprint_type_id(ROW) == 999
    assert orow.get_blueprint_type_id({}) is None
    assert orow.get_meta_group_name(ROW) == "Tech II"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_overview_row.py -v`
Expected: FAIL — `ImportError: cannot import name 'overview_row'`.

- [ ] **Step 3: Write the accessor module**

```python
# src/eve_online_industry_tracker/application/industry/overview_row.py
"""Accessors for IndustryService product-overview rows.

An overview row is a plain dict produced by IndustryService. Its key names are
not guessable and several values live nested under `manufacturing_job`, so every
consumer — the Streamlit Industry Builder and the Daily Planner — reads rows
through this module. When the producer renames a key, one module breaks instead
of a scoring pipeline silently returning zeros.

Return conventions:
  * `get_*` returning a plain number never fails; absent means a documented
    neutral default (0 units, 0.0 % trend).
  * `get_*` returning `X | None` means the value is genuinely optional and
    every caller must decide what `None` implies.
"""
from __future__ import annotations

from typing import Any


def get_manufacturing_job(row: dict[str, Any]) -> dict[str, Any]:
    """The nested manufacturing-job dict, or `{}` when absent or malformed."""
    job = row.get("manufacturing_job")
    return job if isinstance(job, dict) else {}


def _as_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_product_quantity(row: dict[str, Any]) -> int:
    """Units produced per batch. The producer writes this as `quantity`."""
    return _as_int(row.get("quantity"))


def get_effective_runs(row: dict[str, Any]) -> int:
    """Blueprint runs per batch, falling back to the product quantity."""
    runs = _as_int(get_manufacturing_job(row).get("runs"))
    if runs > 0:
        return runs
    return get_product_quantity(row)


def skill_requirements_met(row: dict[str, Any]) -> bool:
    """True when every skill requirement on the manufacturing job is satisfied."""
    skills = get_manufacturing_job(row).get("skills") or {}
    if not isinstance(skills, dict):
        return True
    for entry in skills.values():
        if not isinstance(entry, dict):
            continue
        required = _as_int(entry.get("level"))
        trained = _as_int(entry.get("trained_skill_level"))
        if trained < required:
            return False
    return True


def get_material_cost_total(row: dict[str, Any]) -> float | None:
    """Total material cost for one batch, or None when the producer has no price."""
    return _as_optional_float(get_manufacturing_job(row).get("material_cost"))


def get_material_cost_per_unit(row: dict[str, Any]) -> float | None:
    """Material cost per produced unit.

    `manufacturing_job.material_cost` is a batch total; the producer itself
    divides it by the product quantity (industry/service.py:1770). None when
    either input is missing, so callers cannot silently treat it as free.
    """
    total = get_material_cost_total(row)
    if total is None:
        return None
    quantity = get_product_quantity(row)
    if quantity <= 0:
        return None
    return total / quantity


def get_pipeline_units_in_jobs(row: dict[str, Any]) -> int:
    return _as_int(row.get("pipeline_units_in_jobs"))


def get_pipeline_units_on_market(row: dict[str, Any]) -> int:
    return _as_int(row.get("pipeline_units_on_market"))


def get_pipeline_days_supply(row: dict[str, Any]) -> float | None:
    """Producer-computed days of pipeline supply; None when 7d volume was zero."""
    return _as_optional_float(row.get("pipeline_days_supply"))


def get_price_trend_7d_pct(row: dict[str, Any]) -> float:
    return _as_float(row.get("price_trend_7d_pct"))


def get_price_trend_30d_pct(row: dict[str, Any]) -> float | None:
    return _as_optional_float(row.get("price_trend_30d_pct"))


def get_days_of_supply(row: dict[str, Any]) -> float | None:
    return _as_optional_float(row.get("days_of_supply"))


def get_isk_per_hour(row: dict[str, Any]) -> float:
    return _as_float(row.get("isk_per_hour"))


def get_profit_amount(row: dict[str, Any]) -> float:
    return _as_float(row.get("profit_amount"))


def get_profit_margin_fraction(row: dict[str, Any]) -> float:
    return _as_float(row.get("profit_margin_fraction"))


def get_blueprint_type_id(row: dict[str, Any]) -> int | None:
    value = _as_int(row.get("blueprint_type_id"))
    return value if value > 0 else None


def get_meta_group_name(row: dict[str, Any]) -> str:
    """Meta group as a name — the producer has no meta group *id* on the row."""
    return str(row.get("meta_group_name") or "").strip()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_overview_row.py -v`
Expected: PASS (12 passed).

- [ ] **Step 5: Re-point the UI at the shared module**

In `src/streamlit_ui/state/industry_builder_ui.py`, delete the local bodies of
`get_product_quantity`, `get_effective_runs`, `get_manufacturing_job` and `skill_requirements_met`
and re-export instead, so existing call sites keep working unchanged:

```python
from eve_online_industry_tracker.application.industry.overview_row import (  # noqa: F401
    get_effective_runs,
    get_manufacturing_job,
    get_product_quantity,
    skill_requirements_met,
)
```

Read the existing `skill_requirements_met` body first and make the version in
`overview_row.py` match it exactly. If it differs from the implementation in Step 3, the UI's
version is authoritative — it is the one proven against real rows. Amend `overview_row.py` and its
test to match, rather than changing UI behaviour.

- [ ] **Step 6: Confirm nothing regressed**

Run: `python -m pytest tests/ -q`
Expected: no new failures.

- [ ] **Step 7: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/overview_row.py \
        src/streamlit_ui/state/industry_builder_ui.py tests/test_overview_row.py
git commit -m "refactor: extract shared overview-row accessors from the industry builder UI

The UI accessors are the only row readers proven against real data. Moving them
into the application layer lets the daily planner stop guessing key names.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: `TypeMetadataResolver`

The planner needs `meta_group_id` (not on the row — the row carries only `meta_group_name`) and a
reliable way to tell whether an asset is a blueprint. `get_type_data()` already returns both, so
this is a thin batching wrapper, not new SQL.

**Files:**
- Create: `src/eve_online_industry_tracker/application/industry/type_metadata.py`
- Test: `tests/test_type_metadata.py`

**Interfaces:**
- Consumes: `infrastructure.sde.types.get_type_data(session, language, type_ids) -> dict[int, dict]`.
- Produces: `TypeMetadataResolver(sde_session_provider, language="en")` with
  `meta_group_id(type_id) -> int | None`, `category_name(type_id) -> str`,
  `is_blueprint(type_id) -> bool`, `prefetch(type_ids: Iterable[int]) -> None`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_type_metadata.py
from __future__ import annotations

from eve_online_industry_tracker.application.industry.type_metadata import TypeMetadataResolver

TYPE_DATA = {
    12345: {"type_id": 12345, "meta_group_id": 2, "category_name": "Drone"},
    999: {"type_id": 999, "meta_group_id": 1, "category_name": "Blueprint"},
    777: {"type_id": 777, "meta_group_id": None, "category_name": ""},
}


class _FakeLoader:
    def __init__(self):
        self.calls: list[list[int]] = []

    def __call__(self, session, language, type_ids):
        self.calls.append(sorted(type_ids))
        return {tid: TYPE_DATA[tid] for tid in type_ids if tid in TYPE_DATA}


def _resolver(loader):
    return TypeMetadataResolver(sde_session_provider=lambda: None, loader=loader)


def test_meta_group_id_and_category_name():
    r = _resolver(_FakeLoader())
    assert r.meta_group_id(12345) == 2
    assert r.category_name(12345) == "Drone"


def test_is_blueprint_is_category_based_and_case_insensitive():
    r = _resolver(_FakeLoader())
    assert r.is_blueprint(999) is True
    assert r.is_blueprint(12345) is False


def test_unknown_type_returns_none_not_an_exception():
    r = _resolver(_FakeLoader())
    assert r.meta_group_id(424242) is None
    assert r.category_name(424242) == ""
    assert r.is_blueprint(424242) is False


def test_missing_meta_group_is_none():
    r = _resolver(_FakeLoader())
    assert r.meta_group_id(777) is None


def test_results_are_cached_so_the_sde_is_queried_once_per_type():
    loader = _FakeLoader()
    r = _resolver(loader)
    r.meta_group_id(12345)
    r.category_name(12345)
    r.is_blueprint(12345)
    assert loader.calls == [[12345]]


def test_prefetch_batches_into_a_single_query():
    loader = _FakeLoader()
    r = _resolver(loader)
    r.prefetch([12345, 999, 777])
    assert loader.calls == [[777, 999, 12345]]
    r.meta_group_id(999)
    assert len(loader.calls) == 1


def test_unknown_types_are_not_re_queried():
    loader = _FakeLoader()
    r = _resolver(loader)
    r.meta_group_id(424242)
    r.meta_group_id(424242)
    assert len(loader.calls) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_type_metadata.py -v`
Expected: FAIL — `ModuleNotFoundError: ...type_metadata`.

- [ ] **Step 3: Write the resolver**

```python
# src/eve_online_industry_tracker/application/industry/type_metadata.py
"""Batch type_id → SDE metadata lookups, with a per-instance cache.

The overview row carries `meta_group_name` but no meta group *id*, and nothing
on a corporation asset says whether it is a blueprint. Both answers live in the
SDE, which `get_type_data` already reads; this class batches and caches those
lookups so callers never hit the SDE once per item.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Iterable

from eve_online_industry_tracker.infrastructure.sde.types import get_type_data

logger = logging.getLogger(__name__)

BLUEPRINT_CATEGORY_NAME = "blueprint"


class TypeMetadataResolver:
    def __init__(
        self,
        *,
        sde_session_provider: Callable[[], Any],
        language: str = "en",
        loader: Callable[[Any, str, list[int]], dict[int, dict]] = get_type_data,
    ) -> None:
        self._sde_session_provider = sde_session_provider
        self._language = language
        self._loader = loader
        self._cache: dict[int, dict[str, Any]] = {}
        self._missing: set[int] = set()

    def prefetch(self, type_ids: Iterable[int]) -> None:
        """Load metadata for many types in one SDE query."""
        wanted = {
            int(tid)
            for tid in type_ids
            if int(tid or 0) > 0
            and int(tid) not in self._cache
            and int(tid) not in self._missing
        }
        if not wanted:
            return

        session = self._sde_session_provider()
        try:
            loaded = self._loader(session, self._language, sorted(wanted))
        except (KeyError, TypeError, ValueError):
            logger.exception("TypeMetadataResolver: SDE lookup failed for %d types", len(wanted))
            loaded = {}
        finally:
            close = getattr(session, "close", None)
            if callable(close):
                close()

        for tid in wanted:
            entry = loaded.get(tid)
            if isinstance(entry, dict):
                self._cache[tid] = entry
            else:
                self._missing.add(tid)

    def _entry(self, type_id: int) -> dict[str, Any]:
        tid = int(type_id or 0)
        if tid <= 0:
            return {}
        if tid not in self._cache and tid not in self._missing:
            self.prefetch([tid])
        return self._cache.get(tid, {})

    def meta_group_id(self, type_id: int) -> int | None:
        raw = self._entry(type_id).get("meta_group_id")
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def category_name(self, type_id: int) -> str:
        return str(self._entry(type_id).get("category_name") or "")

    def is_blueprint(self, type_id: int) -> bool:
        return self.category_name(type_id).strip().lower() == BLUEPRINT_CATEGORY_NAME
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_type_metadata.py -v`
Expected: PASS (7 passed).

- [ ] **Step 5: Verify against the real SDE**

Run: `python -c "
import sqlite3
c = sqlite3.connect('file:database/eve_sde.db?mode=ro&immutable=1', uri=True)
row = c.execute('''select t.id, t.metaGroupID, g.categoryID
                   from types t join groups g on g.id = t.groupID
                   where t.name like '%Blueprint%' limit 1''').fetchone()
print('sample blueprint type row:', row)
"`
Expected: a row prints. This confirms `types`/`groups` join keys exist as the resolver assumes.
If the `groups` table is named differently, no code changes are needed — `get_type_data` owns that
query — but note it for Task 9.

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/application/industry/type_metadata.py \
        tests/test_type_metadata.py
git commit -m "feat: add cached TypeMetadataResolver for meta group and blueprint lookups

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: `PlannerInputRow` contract

**Files:**
- Create: `src/eve_online_industry_tracker/application/daily_planner/input_row.py`
- Test: `tests/test_planner_input_row.py`, `tests/test_overview_row_contract.py`

**Interfaces:**
- Consumes: `application.industry.overview_row` accessors;
  `TypeMetadataResolver.meta_group_id`.
- Produces: `PlannerInputError(type_id, field, detail)` with `.type_id` and `.field` attributes;
  `PlannerInputRow` (frozen dataclass, fields as listed) with
  `PlannerInputRow.from_overview(row, *, meta_groups) -> PlannerInputRow`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_planner_input_row.py
from __future__ import annotations

import pytest

from eve_online_industry_tracker.application.daily_planner.input_row import (
    PlannerInputError,
    PlannerInputRow,
)


class _MetaGroups:
    def meta_group_id(self, type_id):
        return 2 if type_id == 12345 else None


GOOD = {
    "type_id": 12345,
    "type_name": "Hobgoblin II",
    "quantity": 10,
    "blueprint_type_id": 999,
    "isk_per_hour": 4_000_000.0,
    "profit_amount": 1_000_000.0,
    "profit_margin_fraction": 0.2,
    "days_of_supply": 6.5,
    "pipeline_units_in_jobs": 40,
    "pipeline_units_on_market": 60,
    "pipeline_days_supply": 12.5,
    "price_trend_7d_pct": 3.5,
    "price_trend_30d_pct": -2.0,
    "manufacturing_job": {"runs": 20, "material_cost": 50_000_000.0},
}


def _build(row):
    return PlannerInputRow.from_overview(row, meta_groups=_MetaGroups())


def test_a_complete_row_builds():
    got = _build(GOOD)
    assert got.type_id == 12345
    assert got.runs == 20
    assert got.material_cost_per_unit == 5_000_000.0
    assert got.meta_group_id == 2
    assert got.pipeline_days_supply == 12.5
    assert got.blueprint_type_id == 999


def test_missing_material_cost_raises_naming_the_nested_field():
    row = dict(GOOD, manufacturing_job={"runs": 20})
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "manufacturing_job.material_cost"
    assert exc.value.type_id == 12345
    assert "12345" in str(exc.value)


def test_missing_type_id_raises():
    row = dict(GOOD)
    del row["type_id"]
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "type_id"


def test_zero_quantity_raises_rather_than_dividing_by_zero():
    row = dict(GOOD, quantity=0)
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "quantity"


def test_a_renamed_producer_key_raises_instead_of_zeroing():
    row = dict(GOOD)
    row["isk_per_hr"] = row.pop("isk_per_hour")
    with pytest.raises(PlannerInputError) as exc:
        _build(row)
    assert exc.value.field == "isk_per_hour"


def test_optional_fields_become_none_without_raising():
    row = dict(GOOD)
    del row["pipeline_days_supply"]
    del row["price_trend_30d_pct"]
    del row["days_of_supply"]
    got = _build(row)
    assert got.pipeline_days_supply is None
    assert got.price_trend_30d_pct is None
    assert got.days_of_supply is None


def test_unknown_meta_group_is_none_not_an_error():
    row = dict(GOOD, type_id=55555)
    assert _build(row).meta_group_id is None


def test_the_row_is_frozen_and_keeps_raw_for_legacy_reads():
    got = _build(GOOD)
    with pytest.raises(Exception):
        got.type_id = 1  # type: ignore[misc]
    assert got.raw["type_name"] == "Hobgoblin II"


def test_a_non_dict_row_raises():
    with pytest.raises(PlannerInputError):
        PlannerInputRow.from_overview(["not", "a", "dict"], meta_groups=_MetaGroups())
```

```python
# tests/test_overview_row_contract.py
"""Guards the contract against the real producer output.

Skips until the sanitised fixture has been captured (see Task 3 of
docs/superpowers/plans/2026-09-17-daily-planner-remediation.md).
"""
from __future__ import annotations

import json
import os

import pytest

from eve_online_industry_tracker.application.daily_planner.input_row import (
    PlannerInputError,
    PlannerInputRow,
)

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "overview_rows_real.json")

pytestmark = pytest.mark.skipif(
    not os.path.exists(FIXTURE),
    reason=(
        "tests/fixtures/overview_rows_real.json not captured yet — "
        "run the app with EVE_ENABLE_DEBUG_FIXTURE=1 and GET "
        "/planner/debug/overview-fixture"
    ),
)


class _AnyMetaGroup:
    def meta_group_id(self, type_id):
        return 1


def _rows():
    with open(FIXTURE, encoding="utf-8") as fh:
        return json.load(fh)


def test_the_fixture_has_rows():
    assert len(_rows()) > 1


def test_every_real_row_satisfies_the_contract():
    failures = []
    for row in _rows():
        try:
            PlannerInputRow.from_overview(row, meta_groups=_AnyMetaGroup())
        except PlannerInputError as exc:
            failures.append((exc.type_id, exc.field, str(exc)))
    assert not failures, "real overview rows violate the planner contract: " + repr(failures)


def test_renaming_a_required_key_in_a_real_row_is_detected():
    row = dict(_rows()[0])
    row["isk_per_hour_renamed"] = row.pop("isk_per_hour", 0.0)
    with pytest.raises(PlannerInputError):
        PlannerInputRow.from_overview(row, meta_groups=_AnyMetaGroup())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_planner_input_row.py tests/test_overview_row_contract.py -v`
Expected: `test_planner_input_row.py` FAILs with `ModuleNotFoundError: ...input_row`;
`test_overview_row_contract.py` either fails the same way or skips if the fixture is absent.

- [ ] **Step 3: Write the contract**

```python
# src/eve_online_industry_tracker/application/daily_planner/input_row.py
"""The Daily Planner's input contract over IndustryService overview rows.

This is the only place in the daily_planner package that reads a raw overview
dict. Everything downstream consumes PlannerInputRow, so a producer key rename
raises here — naming the field and the type_id — instead of silently zeroing a
score eight phases later.

Required vs optional is deliberate:
  * required — the planner cannot make a meaningful decision without it.
  * optional (`X | None`) — genuinely absent sometimes; every consumer must
    handle None explicitly.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from eve_online_industry_tracker.application.industry import overview_row as orow


class PlannerInputError(ValueError):
    """An overview row does not satisfy the planner's input contract."""

    def __init__(self, *, type_id: Any, field: str, detail: str) -> None:
        self.type_id = type_id
        self.field = field
        self.detail = detail
        super().__init__(f"overview row type_id={type_id}: {field} {detail}")


class MetaGroupSource(Protocol):
    def meta_group_id(self, type_id: int) -> int | None: ...


def _require_int(type_id: Any, name: str, value: Any, *, positive: bool = False) -> int:
    if value is None:
        raise PlannerInputError(type_id=type_id, field=name, detail="is missing")
    try:
        out = int(value)
    except (TypeError, ValueError):
        raise PlannerInputError(
            type_id=type_id, field=name, detail=f"is not an integer: {value!r}"
        ) from None
    if positive and out <= 0:
        raise PlannerInputError(type_id=type_id, field=name, detail=f"must be > 0, got {out}")
    return out


def _require_float(type_id: Any, name: str, value: Any) -> float:
    if value is None:
        raise PlannerInputError(type_id=type_id, field=name, detail="is missing")
    try:
        return float(value)
    except (TypeError, ValueError):
        raise PlannerInputError(
            type_id=type_id, field=name, detail=f"is not a number: {value!r}"
        ) from None


@dataclass(frozen=True)
class PlannerInputRow:
    """One overview row, validated and normalised for the planner."""

    type_id: int
    type_name: str
    quantity: int
    runs: int
    material_cost_per_unit: float
    isk_per_hour: float
    profit_amount: float
    profit_margin_fraction: float
    pipeline_units_in_jobs: int
    pipeline_units_on_market: int
    price_trend_7d_pct: float
    pipeline_days_supply: float | None
    price_trend_30d_pct: float | None
    days_of_supply: float | None
    meta_group_id: int | None
    blueprint_type_id: int | None
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @classmethod
    def from_overview(cls, row: Any, *, meta_groups: MetaGroupSource) -> "PlannerInputRow":
        if not isinstance(row, dict):
            raise PlannerInputError(
                type_id=None, field="<row>", detail=f"is not a dict: {type(row).__name__}"
            )

        type_id = _require_int(row.get("type_id"), "type_id", row.get("type_id"), positive=True)

        type_name = str(row.get("type_name") or "").strip()
        if not type_name:
            raise PlannerInputError(type_id=type_id, field="type_name", detail="is missing")

        quantity = _require_int(type_id, "quantity", row.get("quantity"), positive=True)

        if orow.get_material_cost_total(row) is None:
            raise PlannerInputError(
                type_id=type_id,
                field="manufacturing_job.material_cost",
                detail="is missing — material cost cannot default to zero",
            )
        material_cost_per_unit = orow.get_material_cost_per_unit(row)
        if material_cost_per_unit is None:
            raise PlannerInputError(
                type_id=type_id,
                field="manufacturing_job.material_cost",
                detail="could not be converted to a per-unit cost",
            )

        return cls(
            type_id=type_id,
            type_name=type_name,
            quantity=quantity,
            runs=orow.get_effective_runs(row),
            material_cost_per_unit=material_cost_per_unit,
            isk_per_hour=_require_float(type_id, "isk_per_hour", row.get("isk_per_hour")),
            profit_amount=_require_float(type_id, "profit_amount", row.get("profit_amount")),
            profit_margin_fraction=_require_float(
                type_id, "profit_margin_fraction", row.get("profit_margin_fraction")
            ),
            pipeline_units_in_jobs=orow.get_pipeline_units_in_jobs(row),
            pipeline_units_on_market=orow.get_pipeline_units_on_market(row),
            price_trend_7d_pct=orow.get_price_trend_7d_pct(row),
            pipeline_days_supply=orow.get_pipeline_days_supply(row),
            price_trend_30d_pct=orow.get_price_trend_30d_pct(row),
            days_of_supply=orow.get_days_of_supply(row),
            meta_group_id=meta_groups.meta_group_id(type_id),
            blueprint_type_id=orow.get_blueprint_type_id(row),
            raw=row,
        )
```

Note the first `_require_int` call passes `row.get("type_id")` as the error's `type_id` too — when
the id itself is missing there is nothing better to report, and the message still says
`type_id=None`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_planner_input_row.py tests/test_overview_row_contract.py -v`
Expected: `test_planner_input_row.py` 9 passed. The contract test passes if the fixture exists,
otherwise skips.

**If the contract test fails**, the fixture is ground truth and this module is wrong. Read the
failure — it names the field — and adjust `overview_row.py` / `input_row.py` to match the real
producer output. Do not edit the fixture to make the test pass.

- [ ] **Step 5: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/input_row.py \
        tests/test_planner_input_row.py tests/test_overview_row_contract.py
git commit -m "feat: add PlannerInputRow contract that fails loud on missing row fields

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: PipelineAnalyzer — real pipeline numbers (findings 4, 6, B)

Fixes: `is_blueprint`→`is_blueprint_copy` + `runs`→`blueprint_runs` (finding 4), phantom
`corp_stock_qty`/`units_on_market` (finding 6), and deletes the reimplemented pipeline-days maths
in favour of the producer's `pipeline_days_supply` (cluster B).

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/pipeline_analyzer.py:50-62,114-119,136-145`
- Test: `tests/test_daily_planner_pipeline.py`

**Interfaces:**
- Consumes: `PlannerInputRow`, `TypeMetadataResolver.is_blueprint`.
- Produces: `PipelineAnalyzer.analyze(input_rows, industry_jobs, corp_assets, market_depth_cache, weights, sell_velocities, meta_resolver)`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_daily_planner_pipeline.py`:

```python
def test_pipeline_days_comes_from_the_producer_not_a_reimplementation():
    row = _input_row(pipeline_days_supply=12.5, pipeline_units_in_jobs=40,
                     pipeline_units_on_market=60)
    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=[],
        market_depth_cache={}, weights={}, sell_velocities={row.type_id: 10.0},
        meta_resolver=_NoBlueprints(),
    )
    assert states[0].total_pipeline_days == 12.5


def test_pipeline_days_falls_back_to_units_over_velocity_when_producer_returns_none():
    row = _input_row(pipeline_days_supply=None, pipeline_units_in_jobs=40,
                     pipeline_units_on_market=60)
    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=[],
        market_depth_cache={}, weights={}, sell_velocities={row.type_id: 10.0},
        meta_resolver=_NoBlueprints(),
    )
    # (40 + 60) / 10.0
    assert states[0].total_pipeline_days == 10.0


def test_bpc_runs_are_read_from_blueprint_runs_on_copy_assets():
    row = _input_row(blueprint_type_id=999)
    asset = SimpleNamespace(type_id=999, is_blueprint_copy=True, blueprint_runs=17, quantity=1)
    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=[asset],
        market_depth_cache={}, weights={}, sell_velocities={},
        meta_resolver=_BlueprintFor(999),
    )
    assert states[0].bpc_runs_available == 17


def test_a_bpo_is_not_counted_as_bpc_runs():
    row = _input_row(blueprint_type_id=999)
    bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False, blueprint_runs=None, quantity=1)
    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=[bpo],
        market_depth_cache={}, weights={}, sell_velocities={},
        meta_resolver=_BlueprintFor(999),
    )
    assert states[0].bpc_runs_available == 0


def test_price_trend_uses_the_7d_key():
    row = _input_row(price_trend_7d_pct=3.5, price_trend_30d_pct=-2.0)
    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=[],
        market_depth_cache={}, weights={}, sell_velocities={},
        meta_resolver=_NoBlueprints(),
    )
    assert states[0].price_trend_7d_pct == 3.5
    # momentum = 3.5 - (-2.0 / 4)
    assert states[0].momentum_signal == 4.0
```

Add these helpers at the top of the file:

```python
from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputRow


class _NoBlueprints:
    def is_blueprint(self, type_id):
        return False

    def meta_group_id(self, type_id):
        return 1


class _BlueprintFor:
    def __init__(self, *blueprint_type_ids):
        self._ids = set(blueprint_type_ids)

    def is_blueprint(self, type_id):
        return int(type_id) in self._ids

    def meta_group_id(self, type_id):
        return 1


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_daily_planner_pipeline.py -v -k "pipeline_days or bpc_runs or price_trend"`
Expected: FAIL — `analyze()` got an unexpected keyword argument `input_rows`.

- [ ] **Step 3: Rewrite the affected parts of PipelineAnalyzer**

Replace the BPC indexing block (lines 50-62):

```python
        # Index BPC runs by blueprint type_id.
        # A blueprint *copy* is is_blueprint_copy=True with blueprint_runs > 0;
        # the category check keeps non-blueprint assets out even if a future
        # schema reuses those column names.
        bpc_runs_by_type: dict[int, int] = {}
        for asset in corp_assets:
            type_id = int(_asset_attr(asset, "type_id") or 0)
            if type_id <= 0:
                continue
            if not meta_resolver.is_blueprint(type_id):
                continue
            if not _asset_attr_bool(asset, "is_blueprint_copy"):
                continue  # BPO — unlimited runs, not BPC stock
            runs = int(_asset_attr(asset, "blueprint_runs") or 0)
            if runs <= 0:
                continue
            bpc_runs_by_type[type_id] = bpc_runs_by_type.get(type_id, 0) + runs
```

Replace the pipeline-days block (lines 114-119):

```python
        # ── Pipeline days ─────────────────────────────────────────────────────
        # The producer already computes this (industry/service.py:2013). Only
        # fall back when it could not — 7-day volume of zero yields None there.
        if row.pipeline_days_supply is not None:
            total_pipeline_days = float(row.pipeline_days_supply)
        else:
            units_in_pipeline = (
                float(row.pipeline_units_in_jobs)
                + float(row.pipeline_units_on_market)
                + _units_in_manufacturing(mfg_jobs_by_type.get(type_id, []))
            )
            total_pipeline_days = units_in_pipeline / effective_velocity
```

Replace the momentum block (lines 136-145):

```python
        # ── Momentum signal ───────────────────────────────────────────────────
        price_trend_7d = row.price_trend_7d_pct
        price_trend_30d = row.price_trend_30d_pct
        trend_30d_for_signal = price_trend_30d if price_trend_30d is not None else 0.0
        momentum_signal = price_trend_7d - (trend_30d_for_signal / 4.0)
```

Change both signatures to take `input_rows: list[PlannerInputRow]` and `meta_resolver`, iterate
`for row in input_rows` with `type_id = row.type_id`, replace `row.get("days_of_supply") or 0.0`
with `row.days_of_supply if row.days_of_supply is not None else 0.0`, replace
`int(row.get("blueprint_type_id") or 0)` with `row.blueprint_type_id or 0`, and narrow the
`except Exception` on line 81 to `except (KeyError, TypeError, ValueError, ZeroDivisionError)`.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_daily_planner_pipeline.py -v`
Expected: PASS. Pre-existing tests in this file that build dicts with `corp_stock_qty` must be
rewritten to use `_input_row(...)` — those fixtures encode the bug. Delete assertions that only
held because the phantom keys returned zero.

- [ ] **Step 5: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/pipeline_analyzer.py \
        tests/test_daily_planner_pipeline.py
git commit -m "fix: read real pipeline and BPC fields in PipelineAnalyzer

Uses the producer's pipeline_days_supply instead of a reimplementation over
phantom corp_stock_qty/units_on_market keys, and reads is_blueprint_copy +
blueprint_runs instead of a non-existent is_blueprint/runs pair.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: ProfitabilityScorer — real material cost (finding 5)

`absolute_profit_per_batch` currently collapses to `sell_price × runs × output_qty` with zero
material cost, because all three cost keys it reads are phantom. The 20M ISK gate then skips almost
everything.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/profitability_scorer.py:18-37,121-148`
- Test: `tests/test_daily_planner_scoring.py`

**Interfaces:**
- Produces: `ProfitabilityScorer.score(pipeline, row: PlannerInputRow, weights, market_depth, margin_correlation, trit_trend_7d=None)`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_daily_planner_scoring.py` (reuse `_input_row` from Task 7; duplicate the helper
into this file — tests may be read out of order):

```python
def test_absolute_profit_subtracts_the_real_material_cost():
    row = _input_row(quantity=10, runs=20, material_cost_per_unit=1_000_000.0)
    scored = ProfitabilityScorer().score(
        pipeline=_pipeline_state(row.type_id),
        row=row,
        weights=None,
        market_depth={"vwap_5d": 1_500_000.0},
        margin_correlation=None,
    )
    # (1.5M - 1.0M) * 20 runs * 10 units
    assert scored.absolute_profit_per_batch == 100_000_000.0


def test_a_loss_making_item_scores_negative_absolute_profit():
    row = _input_row(quantity=1, runs=1, material_cost_per_unit=2_000_000.0)
    scored = ProfitabilityScorer().score(
        pipeline=_pipeline_state(row.type_id), row=row, weights=None,
        market_depth={"vwap_5d": 1_000_000.0}, margin_correlation=None,
    )
    assert scored.absolute_profit_per_batch == -1_000_000.0


def test_without_market_depth_it_falls_back_to_the_row_profit_amount():
    row = _input_row(profit_amount=777.0)
    scored = ProfitabilityScorer().score(
        pipeline=_pipeline_state(row.type_id), row=row, weights=None,
        market_depth=None, margin_correlation=None,
    )
    assert scored.absolute_profit_per_batch == 777.0


def test_margin_pct_is_the_fraction_as_a_percentage():
    row = _input_row(profit_margin_fraction=0.18)
    scored = ProfitabilityScorer().score(
        pipeline=_pipeline_state(row.type_id), row=row, weights=None,
        market_depth=None, margin_correlation=None,
    )
    assert scored.margin_pct == pytest.approx(18.0)
```

Add a `_pipeline_state(type_id)` helper returning a `PipelineState` with neutral values
(`effective_velocity=1.0, total_pipeline_days=0.0, competition_index=None, competitor_units=0,
bpc_runs_available=0, momentum_signal=0.0, days_of_supply_current=3.0, price_trend_7d_pct=0.0,
price_trend_30d_pct=None, has_active_manufacturing_jobs=False`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_daily_planner_scoring.py -v -k "absolute_profit or loss_making or margin_pct"`
Expected: FAIL — `score()` got an unexpected keyword argument `row`, or
`absolute_profit_per_batch == 300000000.0` (material cost ignored).

- [ ] **Step 3: Rewrite the scorer's inputs**

Change the signature's `overview_row: dict[str, Any]` to `row: PlannerInputRow`. Replace lines
31-37 with:

```python
        isk_per_hour = row.isk_per_hour
        margin_pct = row.profit_margin_fraction * 100.0
```

(The dead normalisation block at 33-37 goes away — `profit_margin_fraction` is always a fraction.)

Replace `_compute_absolute_profit` entirely:

```python
def _compute_absolute_profit(row: PlannerInputRow, market_depth: Any | None) -> float:
    """(sell price − material cost per unit) × runs × units per batch.

    Falls back to the producer's own profit_amount when no market depth is
    available. Material cost is never assumed to be zero — PlannerInputRow
    refuses to construct a row without it.
    """
    sell_price = _sell_price(market_depth)
    if sell_price is None:
        return row.profit_amount
    return (sell_price - row.material_cost_per_unit) * row.runs * row.quantity


def _sell_price(market_depth: Any | None) -> float | None:
    if market_depth is None:
        return None
    get = market_depth.get if isinstance(market_depth, dict) else (
        lambda key, default=None: getattr(market_depth, key, default)
    )
    for key in ("vwap_5d", "spot_sell_price"):
        raw = get(key)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None
```

Update the call site to `_compute_absolute_profit(row, market_depth)`.

Note `_sell_price` now also rejects a zero or negative price, where the old `vwap_5d or spot` chain
would have fallen through on `0.0` by accident and on `None` by design. Rejecting non-positive
prices is the intent.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_daily_planner_scoring.py -v`
Expected: PASS. Rewrite pre-existing tests in this file that pass `overview_row=` dicts.

- [ ] **Step 5: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/profitability_scorer.py \
        tests/test_daily_planner_scoring.py
git commit -m "fix: subtract real material cost in absolute_profit_per_batch

All three cost keys the scorer read were phantom, so profit collapsed to the
sell price and the 20M ISK gate skipped nearly everything.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: CharacterAssigner — skills, slots and invention choice (findings 1, 9 + discovered)

Three defects in one file, all about slot accounting. The root cause of two of them is that
`CharacterAssigner` hand-rolled a four-entry skill id map instead of using the SDE-backed skill
mapper the project already has.

**The existing mapper.** `application/characters/character.py:1234-1293` joins the ESI skill list
against every published skill in SDE category 16 and stores the result on the character as
`skills["skills"]` — a list of one entry per skill in the game, each carrying `skill_id`,
`skill_name`, `group_name` and `trained_skill_level`. Verified against
`database/eve_app.db`: 511 entries per character, and the industry skills resolve by name —
`Mass Production` 3387, `Advanced Mass Production` 24625, `Laboratory Operation` 3406,
`Advanced Laboratory Operation` 24624, `Science` 3402, `Research` 3403, `Metallurgy` 3409,
`Advanced Industry` 3388. `industry/service.py:157` already holds the canonical name list as
`_INDUSTRY_CHARACTER_MODIFIER_SKILL_NAMES`, and `industry/service.py:3011` has the same
name-keyed reduction as `_skill_levels_by_name`.

So `_SKILL_ID_MAP` is deleted, not extended. Skills are looked up by name.

The three defects:

1. **Finding 1, worse than reported** — the assigner reads `active_skill_level`, and that key is
   **absent from the stored payload entirely** (confirmed against the live DB). So
   `entry.get("active_skill_level") or 0` is 0 for every skill of every character, always, and
   `max_mfg`/`max_research` are `1 + 0 + 0 = 1`. On the real character measured, every relevant
   skill is at level 5 — true capacity is 11 manufacturing and 11 research slots. The planner was
   using one of each.
2. **Finding 9** — slot counters decrement per *decision*, after all its actions are built, so ME
   and TE research on one item both take the same single free slot.
3. **Discovered while planning** — `_best_invention_char` sums skills whose name contains
   `"science"` or `"metallurgy"`, but the hand-rolled map only ever populated the four slot
   skills, so the sum is always 0 and invention assignment degenerates to "first character with a
   free slot". Reading through the mapper fixes this for free, because `Science`, `Metallurgy`,
   `Research` and `Advanced Industry` are all present by name.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/character_assigner.py:66-74,233-275,346-362`
- Test: `tests/test_daily_planner_assignment.py` (new file)

**Interfaces:**
- Consumes: character dicts whose `skills["skills"]` is the enriched list from
  `characters/character.py` (`skill_name` + `trained_skill_level` per entry).
- Produces: `skill_levels_by_name(char) -> dict[str, int]` (module-level, so it is testable and
  reusable); `CharacterAssigner._take_slot(char_slots, char_id, action_type)`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_daily_planner_assignment.py
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.character_assigner import (
    CharacterAssigner,
)
from eve_online_industry_tracker.application.daily_planner.models import ChainPlan, ItemDecision


def _char(char_id, name, skills):
    return {"character_id": char_id, "character_name": name,
            "skills": {"skills": skills, "total_sp": 0}}


def _skill(skill_name, level, skill_id=0):
    """One entry in the enriched skill list produced by characters/character.py.

    Note there is deliberately no active_skill_level key — the real stored
    payload does not have one.
    """
    return {"skill_id": skill_id, "skill_name": skill_name,
            "group_name": "Production", "trained_skill_level": level}


class _Chars:
    def __init__(self, chars):
        self._chars = chars

    def list_characters(self):
        return self._chars


def _decision(**overrides):
    base = dict(
        type_id=12345, type_name="Hobgoblin II", decision="build",
        decision_reason="test", adjusted_score=1.0, absolute_profit_per_batch=1.0,
        isk_per_hour=1.0, margin_pct=1.0, days_of_supply_current=1.0,
        effective_velocity=1.0, meta_group_id=2, pipeline_stage="manufacturing",
        overview_row={"type_id": 12345},
    )
    base.update(overrides)
    return ItemDecision(**base)


def test_skill_levels_are_read_by_name_from_the_enriched_list():
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        skill_levels_by_name,
    )

    char = _char(1, "Pilot", [_skill("Mass Production", 5), _skill("Science", 4)])
    assert skill_levels_by_name(char) == {"Mass Production": 5, "Science": 4}


def test_skill_levels_of_a_character_without_skills_is_empty():
    from eve_online_industry_tracker.application.daily_planner.character_assigner import (
        skill_levels_by_name,
    )

    assert skill_levels_by_name({"character_id": 1}) == {}
    assert skill_levels_by_name({"character_id": 1, "skills": {}}) == {}


def test_slot_capacity_uses_trained_skill_levels():
    # Mass Production 5 + Advanced Mass Production 4 => 1 + 5 + 4 = 10 mfg slots
    chars = [_char(1, "Pilot", [_skill("Mass Production", 5),
                                _skill("Advanced Mass Production", 4)])]
    slots = CharacterAssigner()._compute_available_slots(chars, [], _now())
    assert slots[1]["free_mfg"] == 10


def test_a_fully_skilled_pilot_has_eleven_slots_of_each_kind():
    # The real measured character: every industry skill at 5.
    chars = [_char(1, "Pilot", [
        _skill("Mass Production", 5), _skill("Advanced Mass Production", 5),
        _skill("Laboratory Operation", 5), _skill("Advanced Laboratory Operation", 5),
    ])]
    slots = CharacterAssigner()._compute_available_slots(chars, [], _now())
    assert slots[1]["free_mfg"] == 11
    assert slots[1]["free_research"] == 11


def test_research_slot_capacity_uses_trained_skill_levels():
    chars = [_char(1, "Pilot", [_skill("Laboratory Operation", 5),
                                _skill("Advanced Laboratory Operation", 3)])]
    slots = CharacterAssigner()._compute_available_slots(chars, [], _now())
    assert slots[1]["free_research"] == 9


def test_an_unskilled_pilot_still_has_one_slot_of_each_kind():
    chars = [_char(1, "Pilot", [])]
    slots = CharacterAssigner()._compute_available_slots(chars, [], _now())
    assert slots[1]["free_mfg"] == 1
    assert slots[1]["free_research"] == 1


def test_me_and_te_research_take_two_separate_slots():
    chars = [_char(1, "Pilot", [_skill("Laboratory Operation", 1)])]  # 1 + 1 = 2 slots
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_me_research": True,
                      "needs_te_research": True, "blueprint_type_id": 999},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars(chars), _AdminStub())
    kinds = sorted(a.action_type for a in actions if a.action_type.endswith("_research"))
    assert kinds == ["me_research", "te_research"]


def test_a_single_research_slot_yields_only_one_research_action():
    chars = [_char(1, "Pilot", [])]  # 1 + 0 = 1 research slot only
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_me_research": True,
                      "needs_te_research": True, "blueprint_type_id": 999},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars(chars), _AdminStub())
    research = [a for a in actions if a.action_type.endswith("_research")]
    assert len(research) == 1


def test_invention_prefers_the_higher_science_skilled_pilot():
    low = _char(1, "Low", [_skill("Laboratory Operation", 5), _skill("Science", 1)])
    high = _char(2, "High", [_skill("Laboratory Operation", 5), _skill("Science", 5)])
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_invention": True},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars([low, high]), _AdminStub())
    invent = [a for a in actions if a.action_type == "invent"]
    assert len(invent) == 1
    assert invent[0].character_id == 2
```

Add at the top:

```python
from datetime import datetime, timezone


def _now():
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


class _AdminStub:
    def get(self, section, key, default=None):
        return default
```

No skill-id lookup is needed: the tests and the implementation both address skills by name, which
is what the mapper in `characters/character.py` provides. The names used here are exactly those in
`industry/service.py:157`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_daily_planner_assignment.py -v`
Expected: `free_mfg == 1` (not 10), only one research action when two slots exist, and the
invention test picking pilot 1.

- [ ] **Step 3: Fix all three**

Delete `_SKILL_ID_MAP` entirely and read through the project's own skill mapper instead. Add at
module level:

```python
# Slot capacity skills. Base one slot, plus one per level of each.
MFG_SLOT_SKILLS = ("Mass Production", "Advanced Mass Production")
RESEARCH_SLOT_SKILLS = ("Laboratory Operation", "Advanced Laboratory Operation")
# Skills that make a pilot a better inventor, used only to rank candidates.
SCIENCE_SKILLS = ("Science", "Advanced Industry", "Metallurgy", "Research")


def skill_levels_by_name(char: dict[str, Any]) -> dict[str, int]:
    """{skill_name: trained_skill_level} for one character.

    Reads the enriched skill list built in characters/character.py, which joins
    ESI's skill list against every published SDE skill and carries skill_name
    alongside trained_skill_level. Note the stored payload has no
    `active_skill_level` key at all — reading one yields 0 for every skill.
    """
    raw = char.get("skills") or {}
    entries = raw.get("skills") if isinstance(raw, dict) else None
    if not isinstance(entries, list):
        return {}

    levels: dict[str, int] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("skill_name") or "").strip()
        if not name:
            continue
        try:
            level = int(entry.get("trained_skill_level") or 0)
        except (TypeError, ValueError):
            continue
        levels[name] = max(level, levels.get(name, 0))
    return levels


def _slot_capacity(levels: dict[str, int], skill_names: tuple[str, ...]) -> int:
    """One base slot plus one per trained level of each contributing skill."""
    return 1 + sum(int(levels.get(name, 0)) for name in skill_names)
```

Then replace the skill-flattening block in `_compute_available_slots` (lines 254-267) with:

```python
            levels = skill_levels_by_name(char)
            max_mfg = _slot_capacity(levels, MFG_SLOT_SKILLS)
            max_research = _slot_capacity(levels, RESEARCH_SLOT_SKILLS)
```

and store `"skills": levels` in the slot map so `_best_invention_char` sees full, name-keyed
levels rather than four hardcoded entries. Drop the `_SKILL_ID_MAP` dict and the
`if isinstance(skills_raw, dict) and "skills" in skills_raw` branch — `skill_levels_by_name`
handles both the populated and the empty shape.

`trained_skill_level` is the key ESI actually returns, the key
`characters/character.py:1274` stores, and the key `industry/service.py:1231` and `:3023` already
read. `active_skill_level` appears nowhere in the stored payload.

Add a slot-taking helper on the class:

```python
    _MFG_ACTIONS = ("manufacture", "sub_manufacture")
    _RESEARCH_ACTIONS = ("invent", "copy", "me_research", "te_research")

    def _take_slot(self, char_slots: dict[int, dict], char_id: int, action_type: str) -> None:
        """Consume one slot immediately, so sibling actions cannot reuse it."""
        slot = char_slots.get(char_id)
        if slot is None:
            return
        if action_type in self._MFG_ACTIONS:
            slot["free_mfg"] = max(0, slot.get("free_mfg", 0) - 1)
        elif action_type in self._RESEARCH_ACTIONS:
            slot["free_research"] = max(0, slot.get("free_research", 0) - 1)
```

In `_assign_decision`, call `self._take_slot(char_slots, char_id, "<action_type>")` immediately
after each `actions.append(...)` — for `me_research`, `te_research`, `invent`, `copy`,
`sub_manufacture` and `manufacture`. Then **delete** the whole post-hoc decrement loop in `assign`
(lines 66-74), replacing it with:

```python
            # Slots are consumed inside _assign_decision, as each action is
            # created — two research actions on one item must not share a slot.
            assigned_actions.extend(actions)
```

`_assign_decision` needs `char_slots` passed through, which it already receives.

Finally, make invention scoring use the real science skills, now that they are actually present:

```python
    def _best_invention_char(
        self, char_slots: dict[int, dict], characters: list[dict]
    ) -> tuple[int | None, str | None]:
        """Highest science skill sum among characters with a free research slot.

        Ties break toward the character with the fewest active jobs.
        """
        candidates = []
        for slot in char_slots.values():
            if slot.get("free_research", 0) <= 0:
                continue
            levels = slot.get("skills") or {}
            science_sum = sum(int(levels.get(name, 0) or 0) for name in SCIENCE_SKILLS)
            candidates.append(
                (-science_sum, slot.get("total_active_jobs", 0),
                 slot["character_id"], slot["character_name"])
            )
        if not candidates:
            return None, None
        candidates.sort()
        return candidates[0][2], candidates[0][3]
```

This also removes the old comparison's dependence on `char_slots.get(best_id or 0, {})`, which
read a slot dict by a key that could be `0`.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_daily_planner_assignment.py -v`
Expected: PASS (10 passed).

- [ ] **Step 5: Run the whole planner suite**

Run: `python -m pytest tests/ -q -k daily_planner`
Expected: no new failures.

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/character_assigner.py \
        tests/test_daily_planner_assignment.py
git commit -m "fix: read character skills through the SDE skill mapper

CharacterAssigner hand-rolled a four-entry skill id map and read
active_skill_level, a key the stored payload does not have — so every pilot had
exactly one slot of each kind instead of up to eleven, and the invention science
sum was always zero. It now reads skill_name/trained_skill_level from the
enriched list that characters/character.py already builds. Slots are also
consumed per action rather than per decision, so ME and TE research can no
longer share one slot.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Service — blueprint asset indexing and corp wallet (findings 3, 10)

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py:577-603,770-790`
- Test: `tests/test_daily_planner_service_helpers.py` (new file)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_daily_planner_service_helpers.py
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.service import (
    _select_division_one_balance,
    index_blueprint_assets,
)


class _BlueprintsAre:
    def __init__(self, *ids):
        self._ids = set(ids)

    def is_blueprint(self, type_id):
        return int(type_id) in self._ids


def test_bpos_and_bpcs_are_indexed_separately():
    bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False, blueprint_runs=None, quantity=1)
    bpc = SimpleNamespace(type_id=999, is_blueprint_copy=True, blueprint_runs=10, quantity=1)
    module = SimpleNamespace(type_id=34, is_blueprint_copy=False, blueprint_runs=None, quantity=5)

    bpos, bpcs = index_blueprint_assets([bpo, bpc, module], _BlueprintsAre(999))

    assert list(bpos) == [999]
    assert list(bpcs) == [999]
    assert len(bpos[999]) == 1
    assert len(bpcs[999]) == 1


def test_non_blueprint_assets_are_excluded():
    module = SimpleNamespace(type_id=34, is_blueprint_copy=False, blueprint_runs=None, quantity=5)
    bpos, bpcs = index_blueprint_assets([module], _BlueprintsAre(999))
    assert bpos == {}
    assert bpcs == {}


def test_wallet_balance_from_a_list_of_divisions():
    wallets = [{"division": 1, "balance": "8,400,000.55"}, {"division": 2, "balance": "1"}]
    assert _select_division_one_balance(wallets) == 8_400_000.55


def test_wallet_balance_from_a_dict_keyed_by_division():
    assert _select_division_one_balance({"1": "500.5"}) == 500.5
    assert _select_division_one_balance({1: 500.5}) == 500.5


def test_wallet_balance_is_zero_when_division_one_is_absent():
    assert _select_division_one_balance([{"division": 2, "balance": "1"}]) == 0.0
    assert _select_division_one_balance(None) == 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_daily_planner_service_helpers.py -v`
Expected: FAIL — `ImportError: cannot import name 'index_blueprint_assets'`.

- [ ] **Step 3: Extract and fix both helpers**

Add module-level functions to `service.py` (extracted so they are testable without a service
instance):

```python
def index_blueprint_assets(
    corp_assets: list[Any], meta_resolver: Any
) -> tuple[dict[int, list[Any]], dict[int, list[Any]]]:
    """Split blueprint assets into (BPOs, BPCs), both keyed by blueprint type_id.

    Blueprint-ness comes from the SDE category — no asset column says it. A BPC
    is a blueprint with is_blueprint_copy=True; anything else blueprint-shaped is
    a BPO.
    """
    bpos: dict[int, list[Any]] = {}
    bpcs: dict[int, list[Any]] = {}
    for asset in corp_assets:
        type_id = int(_asset_attr(asset, "type_id") or 0)
        if type_id <= 0 or not meta_resolver.is_blueprint(type_id):
            continue
        target = bpcs if _asset_attr_bool(asset, "is_blueprint_copy") else bpos
        target.setdefault(type_id, []).append(asset)
    return bpos, bpcs


def _select_division_one_balance(wallets: Any) -> float:
    """Master wallet (division 1) balance from either ESI shape."""
    if isinstance(wallets, str):
        import json as _json

        try:
            wallets = _json.loads(wallets)
        except ValueError:
            return 0.0

    if isinstance(wallets, list):
        for entry in wallets:
            if isinstance(entry, dict) and int(entry.get("division") or 0) == 1:
                return _parse_isk(entry.get("balance"))
    elif isinstance(wallets, dict):
        raw = wallets.get("1", wallets.get(1))
        if raw is not None:
            return _parse_isk(raw)
    return 0.0


def _parse_isk(raw: Any) -> float:
    try:
        return float(str(raw).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0
```

Rewrite `_get_corp_wallet` to use it, and fix the dict access (finding 10 — `corp` is a dict, so
`getattr` always returned `None`):

```python
    def _get_corp_wallet(self) -> float:
        """Master wallet (division 1) balance, or 0.0 when unavailable."""
        try:
            corps = self._corporations.list_corporations()
        except (KeyError, TypeError, ValueError):
            logger.exception("DailyPlannerService: failed to list corporations")
            return 0.0

        if not corps:
            return 0.0
        corp = corps[0] if isinstance(corps, list) else corps
        wallets = corp.get("wallets") if isinstance(corp, dict) else getattr(corp, "wallets", None)
        return _select_division_one_balance(wallets)
```

Replace the body of `_index_blueprint_assets` (line ~777) with a call to the new module function,
passing `self._meta_resolver`.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_daily_planner_service_helpers.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Verify the wallet shape against the live DB**

Run: `python -c "
import sqlite3
c = sqlite3.connect('file:database/eve_app.db?mode=ro&immutable=1', uri=True)
cols = [r[1] for r in c.execute('pragma table_info(corporations)')]
print('corporations columns:', cols)
row = c.execute('select wallets from corporations limit 1').fetchone()
print('wallets value:', repr(row[0])[:200] if row else 'no rows')
"`
Expected: prints the real stored shape. If `wallets` is not a column, find where the balance lives
and adjust `_get_corp_wallet` — the extracted `_select_division_one_balance` stays as-is and its
tests remain valid.

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/service.py \
        tests/test_daily_planner_service_helpers.py
git commit -m "fix: index blueprint assets by SDE category and read corp wallet from a dict

is_blueprint is not an attribute on any model, so BPO/BPC indexes were always
empty — no research, no sub-manufacture, every T1 item downgraded to watching.
The wallet used getattr on a dict, so the snapshot was always 0.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: ShoppingListBuilder — runs, allocation and stock (findings 7, 8 + discovered)

Three bugs that compound into a shopping list that is both far too small and double-spends stock:

1. **Finding 7** — per-run material quantities are never multiplied by `action.runs`.
2. **Finding 8** — `already_allocated` is updated *after* the stock-covered `continue`, so stock is
   re-offered to the next job.
3. **Discovered, cosmetic** — `_build_corp_stock_map` excludes assets by the phantom
   `is_blueprint`, so blueprints land in the material stock map. **No measurable impact today:**
   the map is keyed by `type_id` and only ever read with a *material* `type_id`, and blueprint
   type_ids never collide with material type_ids (different SDE categories, disjoint id ranges).
   The blueprint entries are simply never looked up. Fix it anyway — one line, and it makes the
   map mean what its name says — but do not expect any change in output, and do not let it hold
   up the task if the resolver plumbing proves awkward.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/shopping_list_builder.py:37-100,129-140,160-174`
- Test: `tests/test_shopping_list_builder.py` (new file)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_shopping_list_builder.py
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.models import AssignedAction
from eve_online_industry_tracker.application.daily_planner.shopping_list_builder import (
    ShoppingListBuilder,
)


class _NoBlueprints:
    def is_blueprint(self, type_id):
        return False


class _BlueprintsAre:
    def __init__(self, *ids):
        self._ids = set(ids)

    def is_blueprint(self, type_id):
        return int(type_id) in self._ids


class _Admin:
    def get(self, section, key, default=None):
        return default


def _action(runs, type_id=12345, blueprint_type_id=999):
    return AssignedAction(
        type_id=type_id, type_name="Widget", action_type="manufacture",
        character_id=1, character_name="Pilot", quantity=None, runs=runs,
        estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None,
    )


BLUEPRINTS = {
    999: {"manufacturing": {
        "materials": [{"type_id": 34, "type_name": "Tritanium", "quantity": 100}],
        "products": [{"type_id": 12345, "quantity": 1}],
    }}
}
DEPTH = {34: {"vwap_5d": 5.0}}


def _build(actions, assets, blueprints=BLUEPRINTS, resolver=None):
    return ShoppingListBuilder().build(
        assigned_actions=actions, corp_assets=assets, market_depth_cache=DEPTH,
        admin_settings=_Admin(), blueprint_data=blueprints,
        meta_resolver=resolver or _NoBlueprints(),
    )


def test_material_quantity_scales_with_runs():
    items = _build([_action(runs=20)], [])
    assert len(items) == 1
    assert items[0].quantity == 2000  # 100 per run x 20 runs


def test_runs_of_none_is_treated_as_one_run():
    items = _build([_action(runs=None)], [])
    assert items[0].quantity == 100


def test_corp_stock_reduces_the_purchase():
    stock = SimpleNamespace(type_id=34, quantity=500, is_blueprint_copy=False)
    items = _build([_action(runs=20)], [stock])
    assert items[0].quantity == 1500


def test_stock_consumed_by_the_first_job_is_not_offered_to_the_second():
    # 1000 in stock; two jobs each needing 1000 => buy 0 then 1000.
    stock = SimpleNamespace(type_id=34, quantity=1000, is_blueprint_copy=False)
    items = _build([_action(runs=10), _action(runs=10, type_id=22222)], [stock])
    assert sum(i.quantity for i in items) == 1000


def test_fully_stocked_material_is_not_listed_at_all():
    stock = SimpleNamespace(type_id=34, quantity=10_000, is_blueprint_copy=False)
    assert _build([_action(runs=20)], [stock]) == []


def test_blueprints_do_not_count_as_material_stock():
    blueprint = SimpleNamespace(type_id=34, quantity=10_000, is_blueprint_copy=False)
    items = _build([_action(runs=20)], [blueprint], resolver=_BlueprintsAre(34))
    assert items[0].quantity == 2000


def test_estimated_total_matches_quantity_times_unit_price():
    items = _build([_action(runs=20)], [])
    assert items[0].estimated_total == items[0].quantity * items[0].estimated_unit_price
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_shopping_list_builder.py -v`
Expected: FAIL — `build()` got an unexpected keyword argument `meta_resolver`; and once that is
added, `quantity == 100` instead of 2000.

- [ ] **Step 3: Fix the builder**

Add `meta_resolver` to `build()`'s signature and pass it to `_build_corp_stock_map`. Replace the
material loop body (lines 57-91):

```python
            runs = max(1, int(action.runs or 1))
            for mat in mats:
                mat_type_id = int(mat.get("type_id") or 0)
                if mat_type_id <= 0:
                    continue
                per_run_qty = int(mat.get("quantity") or 0)
                if per_run_qty <= 0:
                    continue

                # Blueprint material quantities are per run.
                qty_needed = per_run_qty * runs

                mat_name = str(mat.get("type_name") or f"type_{mat_type_id}")
                net_required = self._compute_net_required(
                    mat_type_id=mat_type_id,
                    qty_needed=qty_needed,
                    corp_stock=corp_stock,
                    already_allocated=already_allocated,
                )

                # Claim the stock this job consumes before any early exit, or the
                # next job is told the same units are still free.
                already_allocated[mat_type_id] = (
                    already_allocated.get(mat_type_id, 0) + qty_needed
                )

                if net_required <= 0:
                    continue

                estimated_unit_price, is_vwap = self._get_price(mat_type_id, market_depth_cache)
                if estimated_unit_price is None or estimated_unit_price <= 0:
                    logger.debug("ShoppingListBuilder: no price for type_id=%s", mat_type_id)
                    continue

                shopping.append(ShoppingItem(
                    type_id=mat_type_id,
                    type_name=mat_name,
                    quantity=net_required,
                    shopping_category="current_job",
                    estimated_unit_price=estimated_unit_price,
                    estimated_total=estimated_unit_price * net_required,
                    notes=None if is_vwap else "price: spot (no VWAP data)",
                ))
```

Fix the stock map:

```python
    def _build_corp_stock_map(
        self, corp_assets: list[Any], meta_resolver: Any
    ) -> dict[int, int]:
        """{type_id: quantity} of material stock, excluding blueprints."""
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

Also delete the unused `future_stock_days` local on line 37 — it is read and never used. If a
future-stock category is wanted, it belongs in its own task.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_shopping_list_builder.py -v`
Expected: PASS (7 passed).

- [ ] **Step 5: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/shopping_list_builder.py \
        tests/test_shopping_list_builder.py
git commit -m "fix: scale shopping quantities by runs and stop double-spending corp stock

A 20-run job bought materials for one run, stock covered by one job was offered
again to the next, and blueprints were counted as material stock.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 12: FeedbackProcessor — tie realized sales to the action (finding 14)

The lookup filters only on `type_id`, so a sale that predates the action is credited to it and
corrupts the EMA weights that drive all future scoring.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/feedback_processor.py:238-294`
- Test: `tests/test_daily_planner_feedback.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_daily_planner_feedback.py`:

```python
def test_a_sale_predating_the_action_is_not_credited_to_it(planner_repo, app_session):
    from datetime import date, datetime

    from eve_online_industry_tracker.infrastructure.models import (
        CorporationRealizedSalesLedgerModel,
    )

    app_session.add(CorporationRealizedSalesLedgerModel(
        type_id=12345, realized_profit=500.0, allocated_cost=100.0,
        date=date(2026, 9, 1),
    ))
    app_session.commit()

    processor = _processor(app_session)
    action = SimpleNamespace(type_id=12345, generated_at=datetime(2026, 9, 10))
    assert processor._find_realized_sale(12345, action=action) is None


def test_a_sale_after_the_action_is_credited(planner_repo, app_session):
    from datetime import date, datetime

    from eve_online_industry_tracker.infrastructure.models import (
        CorporationRealizedSalesLedgerModel,
    )

    app_session.add(CorporationRealizedSalesLedgerModel(
        type_id=12345, realized_profit=500.0, allocated_cost=100.0,
        date=date(2026, 9, 12),
    ))
    app_session.commit()

    processor = _processor(app_session)
    action = SimpleNamespace(type_id=12345, generated_at=datetime(2026, 9, 10))
    found = processor._find_realized_sale(12345, action=action)
    assert found is not None
    assert found["material_cost"] == 100.0
    assert found["sell_days"] == 2.0


def test_without_an_action_date_no_sale_is_credited(planner_repo, app_session):
    processor = _processor(app_session)
    assert processor._find_realized_sale(12345, action=None) is None
```

Write a `_processor(session)` helper in that file constructing a `FeedbackProcessor` with the
conftest `_StaticSessionProvider` and the existing repo fixture; match the real
`FeedbackProcessor.__init__` signature. Confirm the real column names on
`CorporationRealizedSalesLedgerModel` first:

```bash
python -c "
import sys; sys.path.insert(0,'src')
from eve_online_industry_tracker.infrastructure.models import CorporationRealizedSalesLedgerModel as M
print([c.name for c in M.__table__.columns])
"
```

Use whatever that prints for the date and profit columns.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_daily_planner_feedback.py -v -k realized_sale`
Expected: FAIL — the pre-dating sale is returned instead of `None`.

- [ ] **Step 3: Add the date filter**

Replace the query in `_find_realized_sale`:

```python
    def _find_realized_sale(self, type_id: int, *, action: Any = None) -> dict[str, Any] | None:
        """Most recent realized sale of `type_id` that happened *after* `action`.

        Without an action date there is no way to attribute a sale to a planned
        action, so nothing is credited — crediting an older sale would corrupt
        the EMA weights that drive all future scoring.
        """
        generated_at = getattr(action, "generated_at", None)
        if generated_at is None:
            return None
        generated_date = (
            generated_at.date() if isinstance(generated_at, datetime) else generated_at
        )

        if self._session_provider is None:
            return self._find_realized_sale_via_service(type_id)

        try:
            from eve_online_industry_tracker.infrastructure.models import (
                CorporationRealizedSalesLedgerModel,
            )

            session = self._session_provider.app_session()
            try:
                row = (
                    session.query(CorporationRealizedSalesLedgerModel)
                    .filter(
                        CorporationRealizedSalesLedgerModel.type_id == type_id,
                        CorporationRealizedSalesLedgerModel.realized_profit.isnot(None),
                        CorporationRealizedSalesLedgerModel.date >= generated_date,
                    )
                    .order_by(CorporationRealizedSalesLedgerModel.date.desc())
                    .first()
                )
                if row is None:
                    return None

                realized_profit = float(row.realized_profit or 0.0)
                material_cost = float(row.allocated_cost or 0.0)
                sale_date = row.date
                if hasattr(sale_date, "date"):
                    sale_date = sale_date.date()
                diff = (sale_date - generated_date).days
                sell_days = max(0.1, float(diff)) if diff > 0 else 1.0
                return {
                    "isk_per_hour": realized_profit / max(0.01, sell_days * 24.0),
                    "material_cost": material_cost,
                    "sell_days": sell_days,
                }
            finally:
                session.close()
        except (KeyError, TypeError, ValueError, AttributeError):
            logger.debug(
                "FeedbackProcessor: realized sale lookup failed for type_id=%s",
                type_id, exc_info=True,
            )
            return None
```

Move the existing `realized_profit_service` fallback into
`_find_realized_sale_via_service(type_id)` so this method has one clear shape. Ensure
`from datetime import datetime` is imported at module level.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_daily_planner_feedback.py -v`
Expected: PASS, including the pre-existing tests in the file.

- [ ] **Step 5: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/feedback_processor.py \
        tests/test_daily_planner_feedback.py
git commit -m "fix: only credit realized sales that postdate the planned action

The lookup filtered on type_id alone, so a sale from before the action was
credited to it and corrupted the learning weights.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 13: D1 — real `activity_id` column (finding 2 + discovered job filter)

`CorporationIndustryJobsModel` has no `activity_id`; the value is in the `raw` JSON. So
`PipelineAnalyzer` indexes zero manufacturing jobs, `has_active_manufacturing_jobs` is always
False, and the `pause` decision is unreachable. Live data confirms the values:
1=manufacturing (315), 3=TE (21), 4=ME (8), 5=copying (108), 8=invention (29).

Also discovered: `_get_industry_jobs` queries **all** jobs with no status filter, so delivered jobs
count as active.

**Files:**
- Modify: `src/eve_online_industry_tracker/infrastructure/models.py:499-530`
- Modify: `src/eve_online_industry_tracker/infrastructure/schema_migrations.py:302-320`
- Modify: `src/eve_online_industry_tracker/application/corporations/corporation.py`
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py:625-636`
- Test: `tests/test_job_activity_migration.py` (new file)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_job_activity_migration.py
from __future__ import annotations

from eve_online_industry_tracker.infrastructure.models import CorporationIndustryJobsModel


def test_the_model_has_an_activity_id_column():
    assert "activity_id" in CorporationIndustryJobsModel.__table__.columns


def test_activity_id_is_queryable(app_session):
    app_session.add(CorporationIndustryJobsModel(
        corporation_id=1, job_id=1, activity_id=1, product_type_id=12345,
        runs=10, status="active",
    ))
    app_session.add(CorporationIndustryJobsModel(
        corporation_id=1, job_id=2, activity_id=4, blueprint_type_id=999,
        runs=1, status="active",
    ))
    app_session.commit()

    mfg = app_session.query(CorporationIndustryJobsModel).filter_by(activity_id=1).all()
    assert len(mfg) == 1
    assert mfg[0].product_type_id == 12345


def test_backfill_populates_activity_id_from_raw(app_session):
    from eve_online_industry_tracker.infrastructure.schema_migrations import (
        backfill_job_activity_ids,
    )

    app_session.add(CorporationIndustryJobsModel(
        corporation_id=1, job_id=3, activity_id=None, raw={"activity_id": 5},
    ))
    app_session.commit()

    updated = backfill_job_activity_ids(app_session)

    assert updated == 1
    row = app_session.query(CorporationIndustryJobsModel).filter_by(job_id=3).one()
    assert row.activity_id == 5


def test_backfill_leaves_existing_values_alone(app_session):
    from eve_online_industry_tracker.infrastructure.schema_migrations import (
        backfill_job_activity_ids,
    )

    app_session.add(CorporationIndustryJobsModel(
        corporation_id=1, job_id=4, activity_id=1, raw={"activity_id": 8},
    ))
    app_session.commit()

    backfill_job_activity_ids(app_session)

    row = app_session.query(CorporationIndustryJobsModel).filter_by(job_id=4).one()
    assert row.activity_id == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_job_activity_migration.py -v`
Expected: FAIL — `activity_id` not in columns.

- [ ] **Step 3: Add the column, the migration and the backfill**

In `infrastructure/models.py`, add to `CorporationIndustryJobsModel` (and to
`CharacterIndustryJobsModel` if it also lacks it — check first):

```python
    activity_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
```

In `schema_migrations.py`, inside the existing job-table loop at line 302:

```python
        _ensure_column(db_app, table=table, column="activity_id", ddl_type="INTEGER")
```

and after that loop:

```python
    _ensure_index(
        db_app,
        ddl=(
            "CREATE INDEX IF NOT EXISTS ix_corporation_industry_jobs_activity_id "
            "ON corporation_industry_jobs (activity_id)"
        ),
        name="ix_corporation_industry_jobs_activity_id",
    )
    for table in ("character_industry_jobs", "corporation_industry_jobs"):
        _backfill_activity_id_sql(db_app, table=table)
```

```python
def _backfill_activity_id_sql(db: DatabaseManager, *, table: str) -> None:
    """Populate activity_id from the stored raw ESI payload, once."""
    try:
        db.execute(
            f"UPDATE {table} "
            "SET activity_id = json_extract(raw, '$.activity_id') "
            "WHERE activity_id IS NULL AND raw IS NOT NULL "
            "AND json_extract(raw, '$.activity_id') IS NOT NULL"
        )
        logging.info("Backfilled %s.activity_id from raw", table)
    except Exception as e:
        logging.warning("Failed backfilling %s.activity_id: %s", table, str(e))


def backfill_job_activity_ids(session: Any) -> int:
    """ORM-level backfill used by tests and by a one-off repair.

    Returns the number of rows updated.
    """
    from eve_online_industry_tracker.infrastructure.models import (
        CorporationIndustryJobsModel,
    )

    rows = (
        session.query(CorporationIndustryJobsModel)
        .filter(CorporationIndustryJobsModel.activity_id.is_(None))
        .all()
    )
    updated = 0
    for row in rows:
        raw = row.raw if isinstance(row.raw, dict) else None
        if not raw:
            continue
        value = raw.get("activity_id")
        if value is None:
            continue
        try:
            row.activity_id = int(value)
        except (TypeError, ValueError):
            continue
        updated += 1
    if updated:
        session.commit()
    return updated
```

Add `from typing import Any` to the imports if absent.

In `corporations/corporation.py`, find where industry-job rows are built from the ESI payload and
set `activity_id=int(payload.get("activity_id") or 0) or None` alongside the other fields, so new
syncs populate the column directly.

In `service.py`, filter out finished jobs:

```python
    _TERMINAL_JOB_STATUSES = ("delivered", "cancelled", "reverted")

    def _get_industry_jobs(self) -> list[Any]:
        """Active corp industry jobs — delivered and cancelled jobs are not active."""
        try:
            session = self._session_provider.app_session()
            try:
                from eve_online_industry_tracker.infrastructure.models import (
                    CorporationIndustryJobsModel,
                )

                return (
                    session.query(CorporationIndustryJobsModel)
                    .filter(
                        CorporationIndustryJobsModel.status.notin_(
                            self._TERMINAL_JOB_STATUSES
                        )
                        | CorporationIndustryJobsModel.status.is_(None)
                    )
                    .all()
                )
            finally:
                session.close()
        except (KeyError, TypeError, ValueError):
            logger.exception("DailyPlannerService: failed to get industry jobs")
            return []
```

In `pipeline_analyzer.py`, simplify the indexing now that the column exists:

```python
        for job in industry_jobs:
            if int(_job_attr(job, "activity_id") or 0) != ACTIVITY_MANUFACTURING:
                continue
            product_type_id = _job_attr(job, "product_type_id")
            if product_type_id is None:
                continue
            mfg_jobs_by_type.setdefault(int(product_type_id), []).append(job)
```

importing `ACTIVITY_MANUFACTURING` from `character_assigner` or defining it locally as `1`.
Keep the `raw` fallback in `character_assigner._compute_available_slots` for one release so a
database that has not yet run the migration still counts slots.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_job_activity_migration.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Verify the migration against a copy of the real DB**

Never migrate the live database in a test step. Copy it first:

```bash
cp database/eve_app.db /tmp/eve_app_migration_test.db
python -c "
import sqlite3
c = sqlite3.connect('/tmp/eve_app_migration_test.db')
c.execute('ALTER TABLE corporation_industry_jobs ADD COLUMN activity_id INTEGER')
c.execute('''UPDATE corporation_industry_jobs
             SET activity_id = json_extract(raw, \"\$.activity_id\")
             WHERE activity_id IS NULL AND raw IS NOT NULL''')
c.commit()
for row in c.execute('select activity_id, count(*) from corporation_industry_jobs group by activity_id'):
    print(row)
"
```

Expected: `(1, 315) (3, 21) (4, 8) (5, 108) (8, 29)` — matching the `json_extract` distribution
measured during planning. Then `rm /tmp/eve_app_migration_test.db`.

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/infrastructure/models.py \
        src/eve_online_industry_tracker/infrastructure/schema_migrations.py \
        src/eve_online_industry_tracker/application/corporations/corporation.py \
        src/eve_online_industry_tracker/application/daily_planner/service.py \
        src/eve_online_industry_tracker/application/daily_planner/pipeline_analyzer.py \
        tests/test_job_activity_migration.py
git commit -m "feat: add real activity_id column to industry jobs and filter finished jobs

activity_id lived only in the raw ESI JSON, so the planner indexed no
manufacturing jobs at all and the pause decision was unreachable. Job queries
also counted delivered jobs as active.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 14: D5 + D6 — plan history and meta group persistence (findings 13, D6)

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py:241-262,795-815`
- Test: `tests/test_daily_planner_repo.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_daily_planner_repo.py`:

```python
def test_plan_history_returns_recent_plans(app_session, session_provider):
    from datetime import datetime, timedelta

    from eve_online_industry_tracker.infrastructure.models import BuildPlanModel

    now = datetime.utcnow()
    app_session.add(BuildPlanModel(created_at=now, status="active", freshness_score=1.0))
    app_session.add(BuildPlanModel(created_at=now - timedelta(days=5),
                                   status="superseded", freshness_score=0.8))
    app_session.add(BuildPlanModel(created_at=now - timedelta(days=200),
                                   status="superseded", freshness_score=0.5))
    app_session.commit()

    service = _service(session_provider)
    history = service.get_active_plan()["plan_history"]

    assert len(history) == 2  # the 200-day-old plan is outside the 90-day window
    assert history[0]["status"] == "active"


def test_plan_items_persist_the_resolved_meta_group_id(app_session, session_provider):
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    service = _service(session_provider)
    service._persist_plan_items(plan_id=1, decisions=[_decision(meta_group_id=2)])

    item = app_session.query(BuildPlanItemModel).one()
    assert item.meta_group_id == 2
```

Write a `_service(session_provider)` helper constructing `DailyPlannerService` with stubs for its
collaborators; match its real `__init__` signature. If `_persist_plan_items` does not exist as a
separate method, extract it from `_phase_9_persist` as part of Step 3 — a plan-item writer that
takes `(plan_id, decisions)` is worth having on its own.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_daily_planner_repo.py -v -k "plan_history or meta_group"`
Expected: `plan_history` is `[]` (the string/DateTime bind fails and the bare `except` swallows
it); `meta_group_id` is `None`.

- [ ] **Step 3: Fix both**

Replace the plan-history block (lines 241-262):

```python
        # Plan history (last 90 days)
        history_days = int(_adm(self._admin, "planner_history_days", 90))
        cutoff = datetime.utcnow() - timedelta(days=history_days)
        ph_session = self._session_provider.app_session()
        try:
            plan_history_rows = (
                ph_session.query(BuildPlanModel)
                .filter(BuildPlanModel.created_at >= cutoff)
                .order_by(BuildPlanModel.created_at.desc())
                .limit(90)
                .all()
            )
            plan_history = [
                {
                    "id": int(p.id),
                    "created_at": p.created_at.isoformat() if p.created_at else None,
                    "status": str(p.status),
                    "freshness_score": float(p.freshness_score or 1.0),
                }
                for p in plan_history_rows
            ]
        finally:
            ph_session.close()
```

`cutoff` is now a `datetime`, not `.isoformat()` — SQLite rejected the string comparison against a
`DateTime` column, and the bare `except` hid it. Move `from datetime import timedelta` to the
module imports rather than the inline import.

The `try/except Exception: plan_history = []` wrapper goes away deliberately: a broken history
query is a bug to see, not to hide. Task 18 covers the surrounding error policy.

For meta group, pass the resolver's value when building plan items:

```python
                meta_group_id=decision.meta_group_id,
```

`ItemDecision.meta_group_id` is populated from `PlannerInputRow.meta_group_id`, which the
`TypeMetadataResolver` fills from the SDE. Make sure `_phase_4_decide` copies
`row.meta_group_id` into the `ItemDecision` instead of reading the phantom row key.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_daily_planner_repo.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/service.py \
        tests/test_daily_planner_repo.py
git commit -m "fix: bind a datetime for plan history and persist the resolved meta group id

created_at >= an ISO string silently failed on SQLite and the bare except hid
it, so plan_history was permanently empty.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 15: D2 — real freshness scoring (finding 15)

`_compute_freshness_score` has an empty loop body and always returns 1.0, then writes that back to
the DB on every read. Its own comment says a true check "would parse the hash" — a SHA-256 cannot
be parsed, so the design needs per-item snapshot prices.

**Files:**
- Modify: `src/eve_online_industry_tracker/infrastructure/models.py` (`BuildPlanItemModel`)
- Modify: `src/eve_online_industry_tracker/infrastructure/schema_migrations.py`
- Modify: `src/eve_online_industry_tracker/application/daily_planner/service.py:835-885`
- Modify: `src/flask_app/routes/daily_planner.py` (remove the write-on-read comment)
- Test: `tests/test_daily_planner_freshness.py` (new file)

**Interfaces:**
- Produces: `compute_freshness_score(plan_items, market_depth, drift_threshold_pct) -> float` as a
  module-level pure function; `BuildPlanItemModel.snapshot_sell_price`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_daily_planner_freshness.py
from __future__ import annotations

from types import SimpleNamespace

from eve_online_industry_tracker.application.daily_planner.service import (
    compute_freshness_score,
)


def _item(type_id, snapshot):
    return SimpleNamespace(type_id=type_id, snapshot_sell_price=snapshot)


def test_no_drift_scores_one():
    items = [_item(1, 100.0), _item(2, 200.0)]
    depth = {1: {"spot_sell_price": 100.0}, 2: {"spot_sell_price": 200.0}}
    assert compute_freshness_score(items, depth, 5.0) == 1.0


def test_all_items_drifted_scores_zero():
    items = [_item(1, 100.0), _item(2, 200.0)]
    depth = {1: {"spot_sell_price": 150.0}, 2: {"spot_sell_price": 100.0}}
    assert compute_freshness_score(items, depth, 5.0) == 0.0


def test_half_drifted_scores_one_half():
    items = [_item(1, 100.0), _item(2, 200.0)]
    depth = {1: {"spot_sell_price": 100.0}, 2: {"spot_sell_price": 300.0}}
    assert compute_freshness_score(items, depth, 5.0) == 0.5


def test_drift_just_under_the_threshold_is_still_fresh():
    items = [_item(1, 100.0)]
    assert compute_freshness_score(items, {1: {"spot_sell_price": 104.9}}, 5.0) == 1.0


def test_drift_just_over_the_threshold_is_stale():
    items = [_item(1, 100.0)]
    assert compute_freshness_score(items, {1: {"spot_sell_price": 105.1}}, 5.0) == 0.0


def test_drift_is_symmetric_for_price_drops():
    items = [_item(1, 100.0)]
    assert compute_freshness_score(items, {1: {"spot_sell_price": 94.9}}, 5.0) == 0.0


def test_items_without_a_comparable_price_are_excluded_not_counted_stale():
    items = [_item(1, 100.0), _item(2, None), _item(3, 300.0)]
    depth = {1: {"spot_sell_price": 100.0}}  # 3 has no current price
    assert compute_freshness_score(items, depth, 5.0) == 1.0


def test_no_comparable_items_scores_one():
    assert compute_freshness_score([_item(1, None)], {}, 5.0) == 1.0
    assert compute_freshness_score([], {}, 5.0) == 1.0


def test_a_zero_snapshot_price_is_not_divided_by():
    assert compute_freshness_score([_item(1, 0.0)], {1: {"spot_sell_price": 50.0}}, 5.0) == 1.0


def test_the_model_has_a_snapshot_price_column():
    from eve_online_industry_tracker.infrastructure.models import BuildPlanItemModel

    assert "snapshot_sell_price" in BuildPlanItemModel.__table__.columns
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_daily_planner_freshness.py -v`
Expected: FAIL — `ImportError: cannot import name 'compute_freshness_score'`.

- [ ] **Step 3: Add the column, migration and real computation**

In `infrastructure/models.py`, add to `BuildPlanItemModel`:

```python
    snapshot_sell_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
```

In `schema_migrations.py`:

```python
    _ensure_column(db_app, table="build_plan_item", column="snapshot_sell_price", ddl_type="REAL")
```

Add the pure function to `service.py`:

```python
def compute_freshness_score(
    plan_items: list[Any],
    market_depth: dict[int, Any],
    drift_threshold_pct: float,
) -> float:
    """Fraction of plan items whose sell price has not drifted past the threshold.

    Each item stores the sell price it was planned against
    (`snapshot_sell_price`); freshness compares that with the current price.
    Items with no snapshot or no current price are excluded from the
    denominator — unknown is not the same as stale.

    Returns 1.0 when nothing is comparable, so a plan is never marked stale
    purely for lack of data.
    """
    threshold = abs(float(drift_threshold_pct)) / 100.0
    comparable = 0
    drifted = 0

    for item in plan_items:
        snapshot = _get_attr(item, "snapshot_sell_price")
        if snapshot is None:
            continue
        try:
            snapshot_price = float(snapshot)
        except (TypeError, ValueError):
            continue
        if snapshot_price <= 0:
            continue

        entry = market_depth.get(int(_get_attr(item, "type_id") or 0))
        if entry is None:
            continue
        current_raw = _get_attr(entry, "spot_sell_price")
        if current_raw is None:
            continue
        try:
            current_price = float(current_raw)
        except (TypeError, ValueError):
            continue

        comparable += 1
        if abs(current_price - snapshot_price) / snapshot_price > threshold:
            drifted += 1

    if comparable == 0:
        return 1.0
    return 1.0 - (drifted / comparable)
```

Replace the body of the service's `_compute_freshness_score` with a call to it, and write
`snapshot_sell_price` when plan items are created — the sell price used is the same one
`_compute_snapshot_hash` collects, so take it from the market depth cache at plan time:

```python
                snapshot_sell_price=_spot_sell_price(market_depth_cache.get(decision.type_id)),
```

Finally, remove the write-on-read: `get_active_plan()` must compute freshness for the response
without persisting. Delete the `update_freshness_score` call on the read path and keep it only in
`_phase_9_persist`. Update the stale comment in `routes/daily_planner.py`:

```python
@daily_planner_bp.get("/planner/plan")
def plan():
    require_ready(get_state())
    return ok(data=get_state().daily_planner_service.get_active_plan())
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_daily_planner_freshness.py -v`
Expected: PASS (10 passed).

- [ ] **Step 5: Confirm the read path no longer writes**

Run: `python -m pytest tests/test_daily_planner_repo.py -v`
Then: `grep -n "update_freshness" src/eve_online_industry_tracker/application/daily_planner/service.py`
Expected: the only remaining call is inside the persist phase, not `get_active_plan`.

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/infrastructure/models.py \
        src/eve_online_industry_tracker/infrastructure/schema_migrations.py \
        src/eve_online_industry_tracker/application/daily_planner/service.py \
        src/flask_app/routes/daily_planner.py \
        tests/test_daily_planner_freshness.py
git commit -m "feat: implement real freshness scoring from per-item snapshot prices

The old version had an empty loop body and always returned 1.0, then persisted
that on every GET. A SHA-256 of all prices cannot be parsed for per-item drift,
so each plan item now stores the price it was planned against.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 16: D3 + D4 — chain planner keying and copy action (findings 11, 12)

`_resolve_sub_manufacture` looks up a *product* `type_id` in `bpo_assets_by_type_id`, which is keyed
by *blueprint* `type_id`, so nothing ever matches and `sub_manufacture_actions` is always empty.
The copy action is gated on `has_t1_bpo`, a key nothing writes.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/chain_planner.py:258-330`
- Modify: `src/eve_online_industry_tracker/application/daily_planner/character_assigner.py:158`
- Modify: `src/eve_online_industry_tracker/infrastructure/sde/blueprints.py`
- Test: `tests/test_chain_planner_keying.py` (new file)

**Interfaces:**
- Produces: `build_product_to_blueprint_index(blueprint_data) -> dict[int, int]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_chain_planner_keying.py
from __future__ import annotations

from eve_online_industry_tracker.application.daily_planner.chain_planner import (
    build_product_to_blueprint_index,
)

BLUEPRINT_DATA = {
    999: {"manufacturing": {"products": [{"type_id": 12345, "quantity": 1}],
                            "materials": [{"type_id": 34, "quantity": 100}]}},
    888: {"manufacturing": {"products": [{"type_id": 54321, "quantity": 10}],
                            "materials": []}},
}


def test_index_maps_product_type_id_to_blueprint_type_id():
    index = build_product_to_blueprint_index(BLUEPRINT_DATA)
    assert index == {12345: 999, 54321: 888}


def test_blueprints_without_products_are_skipped():
    index = build_product_to_blueprint_index({777: {"manufacturing": {"products": []}}})
    assert index == {}


def test_malformed_entries_do_not_raise():
    index = build_product_to_blueprint_index({
        1: {}, 2: {"manufacturing": None}, 3: {"manufacturing": {"products": "nope"}},
    })
    assert index == {}


def test_sub_manufacture_finds_a_bpo_owned_for_the_material(monkeypatch):
    from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner

    # Corp owns blueprint 888, which produces material 54321.
    bpo_assets_by_blueprint_type_id = {888: [object()]}
    planner = ChainPlanner()
    index = build_product_to_blueprint_index(BLUEPRINT_DATA)

    assert planner._blueprint_for_product(54321, index, bpo_assets_by_blueprint_type_id) == 888
    assert planner._blueprint_for_product(12345, index, bpo_assets_by_blueprint_type_id) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_chain_planner_keying.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_product_to_blueprint_index'`.

- [ ] **Step 3: Add the index and use it at every crossing point**

```python
def build_product_to_blueprint_index(
    blueprint_data: dict[int, dict[str, Any]]
) -> dict[int, int]:
    """{product type_id: blueprint type_id} from SDE blueprint manufacturing data.

    blueprint_data is keyed by *blueprint* type_id. Anything that starts from a
    product — a material requirement, an overview row — has to cross over
    through this index.
    """
    index: dict[int, int] = {}
    for blueprint_type_id, entry in (blueprint_data or {}).items():
        manufacturing = (entry or {}).get("manufacturing")
        if not isinstance(manufacturing, dict):
            continue
        products = manufacturing.get("products")
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
                index.setdefault(product_type_id, int(blueprint_type_id))
    return index
```

Add the lookup helper on `ChainPlanner`:

```python
    def _blueprint_for_product(
        self,
        product_type_id: int,
        product_to_blueprint: dict[int, int],
        bpo_assets_by_blueprint_type_id: dict[int, list],
    ) -> int | None:
        """The blueprint type_id the corp owns for this product, if any."""
        blueprint_type_id = product_to_blueprint.get(int(product_type_id))
        if blueprint_type_id is None:
            return None
        if not bpo_assets_by_blueprint_type_id.get(blueprint_type_id):
            return None
        return blueprint_type_id
```

Replace the broken check in `_resolve_sub_manufacture` (lines 285-288):

```python
            blueprint_type_id = self._blueprint_for_product(
                mat_type_id, product_to_blueprint, bpo_assets_by_type_id
            )
            if blueprint_type_id is None:
                continue  # no BPO for this material → buy from market
```

Thread `product_to_blueprint` in from the caller, building it once per plan with
`build_product_to_blueprint_index(phase1_data["blueprint_data"])`.

Set the key the assigner actually reads on the sub-decision's `overview_row`, and make the copy
gate use the key ChainPlanner writes. In `character_assigner.py:158`:

```python
        # Copy (T1 BPO copy to feed the invention pipeline)
        if row.get("needs_invention") and row.get("needs_t1_bpo"):
```

Confirm with `grep -n "needs_t1_bpo\|has_t1_bpo" src/` that `needs_t1_bpo` is the written key and
`has_t1_bpo` has no writer. If neither is written, this branch stays unreachable and the real fix
is to have `ChainPlanner` set `needs_t1_bpo` when the corp owns a T1 BPO for an invention target —
implement that in `_resolve_invention_chain` and cover it with a test before switching the gate on.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/test_chain_planner_keying.py tests/test_daily_planner_assignment.py -v`
Expected: PASS.

- [ ] **Step 5: Add a copy-action behavioural test**

This branch has never executed, so it needs its own test rather than just a rename. Add to
`tests/test_daily_planner_assignment.py`:

```python
def test_a_copy_action_is_created_when_a_t1_bpo_feeds_invention():
    chars = [_char(1, "Pilot", [_skill(3406, 5)])]
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_invention": True,
                      "needs_t1_bpo": True, "blueprint_type_id": 999},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars(chars), _AdminStub())
    copies = [a for a in actions if a.action_type == "copy"]
    assert len(copies) == 1
    assert copies[0].type_id == 999


def test_no_copy_action_without_a_t1_bpo():
    chars = [_char(1, "Pilot", [_skill(3406, 5)])]
    plan = ChainPlan(decisions=[_decision(
        overview_row={"type_id": 12345, "needs_invention": True},
    )])
    actions = CharacterAssigner().assign(plan, [], _Chars(chars), _AdminStub())
    assert [a for a in actions if a.action_type == "copy"] == []
```

Run: `python -m pytest tests/test_daily_planner_assignment.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/chain_planner.py \
        src/eve_online_industry_tracker/application/daily_planner/character_assigner.py \
        tests/test_chain_planner_keying.py tests/test_daily_planner_assignment.py
git commit -m "fix: cross product and blueprint type ids explicitly in chain planning

Product type_ids were looked up in blueprint-keyed maps, so build-vs-buy for
sub-manufacture never ran. The copy action was also gated on a key nothing
writes; it is now gated on needs_t1_bpo and covered by tests.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 17: D7 — invention inputs in the shopping list

The invention loop in `ShoppingListBuilder.build` is a `pass` with a comment saying full resolution
"requires SDE data". Datacores and decryptors therefore never appear on the shopping list, so the
user cannot actually start the invention jobs the plan tells them to start.

**Files:**
- Modify: `src/eve_online_industry_tracker/application/daily_planner/shopping_list_builder.py:93-98`
- Modify: `src/eve_online_industry_tracker/infrastructure/sde/blueprints.py`
- Test: `tests/test_shopping_list_builder.py`

**Interfaces:**
- Consumes: blueprint `invention` activity data keyed by blueprint type_id.
- Produces: shopping items with `shopping_category="invention_input"`.

- [ ] **Step 1: Confirm what the SDE actually provides**

```bash
python -c "
import sys; sys.path.insert(0,'src')
import sqlite3, json
c = sqlite3.connect('file:database/eve_sde.db?mode=ro&immutable=1', uri=True)
cols = [r[1] for r in c.execute('pragma table_info(blueprints)')]
print('blueprints columns:', cols)
row = c.execute('select * from blueprints limit 1').fetchone()
print('sample:', str(row)[:600])
"
```

Read the output and `infrastructure/sde/blueprints.py` to find how the invention activity's
materials are exposed. If `get_blueprint_manufacturing_data` returns only the manufacturing
activity, extend it to include `invention` materials keyed the same way — that function already
walks the activity structure, so this is an additional activity key, not a new query.

Record what you found in the commit message; the rest of this task assumes
`blueprint_data[bp_type_id]["invention"]["materials"]` is a list of
`{type_id, type_name, quantity}`.

- [ ] **Step 2: Write the failing test**

Add to `tests/test_shopping_list_builder.py`:

```python
INVENTION_BLUEPRINTS = {
    999: {
        "manufacturing": {"materials": [], "products": [{"type_id": 12345, "quantity": 1}]},
        "invention": {"materials": [
            {"type_id": 20411, "type_name": "Datacore - Gallentean Starship Eng", "quantity": 2},
        ]},
    }
}


def _invent_action(type_id=12345):
    return AssignedAction(
        type_id=type_id, type_name="Widget", action_type="invent",
        character_id=1, character_name="Pilot", quantity=None, runs=1,
        estimated_cost_isk=None, estimated_profit_isk=None,
        estimated_completion=None, notes=None,
    )


def test_invention_inputs_appear_on_the_shopping_list():
    depth = {20411: {"vwap_5d": 100_000.0}}
    items = ShoppingListBuilder().build(
        assigned_actions=[_invent_action()], corp_assets=[], market_depth_cache=depth,
        admin_settings=_Admin(), blueprint_data=INVENTION_BLUEPRINTS,
        meta_resolver=_NoBlueprints(),
    )
    datacores = [i for i in items if i.type_id == 20411]
    assert len(datacores) == 1
    assert datacores[0].quantity == 2
    assert datacores[0].shopping_category == "invention_input"


def test_datacores_in_stock_are_not_bought_again():
    depth = {20411: {"vwap_5d": 100_000.0}}
    stock = SimpleNamespace(type_id=20411, quantity=10, is_blueprint_copy=False)
    items = ShoppingListBuilder().build(
        assigned_actions=[_invent_action()], corp_assets=[stock], market_depth_cache=depth,
        admin_settings=_Admin(), blueprint_data=INVENTION_BLUEPRINTS,
        meta_resolver=_NoBlueprints(),
    )
    assert [i for i in items if i.type_id == 20411] == []


def test_an_invention_action_without_sde_data_is_skipped_quietly():
    items = ShoppingListBuilder().build(
        assigned_actions=[_invent_action(type_id=404040)], corp_assets=[],
        market_depth_cache={}, admin_settings=_Admin(),
        blueprint_data=INVENTION_BLUEPRINTS, meta_resolver=_NoBlueprints(),
    )
    assert items == []
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `python -m pytest tests/test_shopping_list_builder.py -v -k invention`
Expected: FAIL — no items returned (the loop body is `pass`).

- [ ] **Step 4: Implement the invention branch**

Replace lines 93-98:

```python
        # Invention inputs (datacores, decryptors)
        for action in invention_actions:
            inv_mats = self._get_invention_materials(action, blueprint_data, product_to_blueprint)
            for mat in inv_mats:
                mat_type_id = int(mat.get("type_id") or 0)
                if mat_type_id <= 0:
                    continue
                qty_needed = int(mat.get("quantity") or 0) * max(1, int(action.runs or 1))
                if qty_needed <= 0:
                    continue

                net_required = self._compute_net_required(
                    mat_type_id=mat_type_id,
                    qty_needed=qty_needed,
                    corp_stock=corp_stock,
                    already_allocated=already_allocated,
                )
                already_allocated[mat_type_id] = (
                    already_allocated.get(mat_type_id, 0) + qty_needed
                )
                if net_required <= 0:
                    continue

                unit_price, is_vwap = self._get_price(mat_type_id, market_depth_cache)
                if unit_price is None or unit_price <= 0:
                    logger.debug(
                        "ShoppingListBuilder: no price for invention input type_id=%s",
                        mat_type_id,
                    )
                    continue

                shopping.append(ShoppingItem(
                    type_id=mat_type_id,
                    type_name=str(mat.get("type_name") or f"type_{mat_type_id}"),
                    quantity=net_required,
                    shopping_category="invention_input",
                    estimated_unit_price=unit_price,
                    estimated_total=unit_price * net_required,
                    notes=None if is_vwap else "price: spot (no VWAP data)",
                ))
```

```python
    def _get_invention_materials(
        self,
        action: AssignedAction,
        blueprint_data: dict[int, dict[str, Any]],
        product_to_blueprint: dict[int, int],
    ) -> list[dict[str, Any]]:
        """Invention inputs for the T1 blueprint that invents this product."""
        blueprint_type_id = product_to_blueprint.get(int(action.type_id))
        if blueprint_type_id is None:
            return []
        invention = (blueprint_data.get(blueprint_type_id) or {}).get("invention")
        if not isinstance(invention, dict):
            return []
        materials = invention.get("materials")
        return materials if isinstance(materials, list) else []
```

Build `product_to_blueprint` at the top of `build()` with
`build_product_to_blueprint_index(blueprint_data)` from Task 16, and use it in
`_get_materials_for_action` too — replacing that method's O(n) fallback scan over every blueprint.

- [ ] **Step 5: Run the tests**

Run: `python -m pytest tests/test_shopping_list_builder.py -v`
Expected: PASS (10 passed).

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/shopping_list_builder.py \
        src/eve_online_industry_tracker/infrastructure/sde/blueprints.py \
        tests/test_shopping_list_builder.py
git commit -m "feat: put invention inputs on the shopping list

The invention branch was a pass, so datacores and decryptors never appeared and
the user could not start the invention jobs the plan asked for.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 18: Fail-loud sweep

With every consumer now reading validated fields, remove the defences that hid the bugs.

**Files:**
- Modify: every file in `src/eve_online_industry_tracker/application/daily_planner/`
- Modify: `src/streamlit_ui/components/daily_planner/status_bar.py`
- Test: `tests/test_daily_planner_fail_loud.py` (new file)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_daily_planner_fail_loud.py
from __future__ import annotations

import ast
import os

PACKAGE = os.path.join(
    os.path.dirname(__file__), "..", "src", "eve_online_industry_tracker",
    "application", "daily_planner",
)

ALLOWED_BROAD_EXCEPT = {
    # module -> number of deliberate broad catches, each of which must log
    # at error level with a traceback (the background compute thread).
    "service.py": 1,
}


def _module_files():
    for name in sorted(os.listdir(PACKAGE)):
        if name.endswith(".py"):
            yield name, os.path.join(PACKAGE, name)


def _broad_handlers(tree):
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        exc = node.type
        if exc is None or (isinstance(exc, ast.Name) and exc.id in ("Exception", "BaseException")):
            found.append(node)
    return found


def test_broad_excepts_are_only_where_deliberately_allowed():
    offenders = {}
    for name, path in _module_files():
        with open(path, encoding="utf-8") as fh:
            tree = ast.parse(fh.read())
        count = len(_broad_handlers(tree))
        allowed = ALLOWED_BROAD_EXCEPT.get(name, 0)
        if count > allowed:
            offenders[name] = (count, allowed)
    assert not offenders, f"unexpected broad except handlers: {offenders}"


def test_every_allowed_broad_except_logs_with_a_traceback():
    for name, path in _module_files():
        if ALLOWED_BROAD_EXCEPT.get(name, 0) == 0:
            continue
        with open(path, encoding="utf-8") as fh:
            tree = ast.parse(fh.read())
        for handler in _broad_handlers(tree):
            body = ast.dump(ast.Module(body=handler.body, type_ignores=[]))
            assert "logger" in body and ("exception" in body or "exc_info" in body), (
                f"{name}: a broad except must log the traceback"
            )


def test_no_silent_zero_defaults_remain_on_contract_fields():
    banned = (
        "estimated_material_cost_per_unit",
        "material_cost_per_unit\"",
        "corp_stock_qty",
        "units_on_market",
        "runs_per_batch",
        "product_quantity",
        "price_trend_pct\"",
        "is_blueprint\"",
        "active_skill_level",
        "has_t1_bpo",
    )
    hits = []
    for name, path in _module_files():
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        for token in banned:
            if token in text:
                hits.append((name, token))
    assert not hits, f"phantom overview-row keys still referenced: {hits}"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_daily_planner_fail_loud.py -v`
Expected: FAIL, listing the modules that still hold broad handlers and phantom keys.

- [ ] **Step 3: Narrow the handlers and delete the phantom reads**

Work through the failures module by module:

- Replace `except Exception:` with the specific tuple the block can actually raise —
  `(KeyError, TypeError, ValueError)` for dict and numeric work, `(AttributeError,)` for duck-typed
  attribute access, `SQLAlchemyError` for session work (`from sqlalchemy.exc import SQLAlchemyError`).
- In `_run_compute`, keep one broad handler — a background thread must not die silently — but it
  already calls `logger.exception`, which the second test requires.
- Delete every remaining reference to the phantom keys. If one is still read somewhere, the
  corresponding earlier task missed a call site; fix it there.

- [ ] **Step 4: Surface `PlannerInputError` in the status bar**

Let the error reach the user. In `service.py`, catch it explicitly in `_run_compute` *before* the
broad handler so the message is specific:

```python
        except PlannerInputError as exc:
            logger.error(
                "DailyPlannerService: overview row contract violation on %s (type_id=%s)",
                exc.field, exc.type_id,
            )
            with self._lock:
                self._status = "failed"
                self._error = (
                    f"Product overview is missing '{exc.field}' for type_id={exc.type_id}. "
                    "The planner cannot build a plan from incomplete market data — "
                    "refresh the product overview and recompute."
                )
            return
```

In `status_bar.py`, render a failed compute status as a red banner via `st.error(...)` showing
`status["error"]`. Read the existing status rendering first and follow its structure.

- [ ] **Step 5: Run the tests**

Run: `python -m pytest tests/test_daily_planner_fail_loud.py -v`
Expected: PASS (3 passed).

Run: `python -m pytest tests/ -q`
Expected: full suite green.

- [ ] **Step 6: Commit**

```bash
git add src/eve_online_industry_tracker/application/daily_planner/ \
        src/streamlit_ui/components/daily_planner/status_bar.py \
        tests/test_daily_planner_fail_loud.py
git commit -m "refactor: narrow exception handling and surface contract violations

Broad excepts and or-0.0 defaults are what let 19 field mismatches run silently.
A guard test now keeps them out, and a contract violation reaches the UI as a
red banner instead of a plan built on zeros.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 19: End-to-end integration test

The capstone: drive the whole pipeline from the real captured fixture plus real ORM objects, and
assert the numbers that used to be zero are not.

**Files:**
- Create: `tests/test_daily_planner_integration.py`

- [ ] **Step 1: Write the test**

```python
# tests/test_daily_planner_integration.py
"""Full-pipeline test over the real (sanitised) overview fixture.

Every assertion here corresponds to a value that was structurally zero before
the remediation: material cost, pipeline days, slot counts, BPC runs.
"""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.application.daily_planner.character_assigner import (
    CharacterAssigner,
)
from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputRow
from eve_online_industry_tracker.application.daily_planner.pipeline_analyzer import (
    PipelineAnalyzer,
)
from eve_online_industry_tracker.application.daily_planner.profitability_scorer import (
    ProfitabilityScorer,
)
from eve_online_industry_tracker.infrastructure.models import CorporationIndustryJobsModel

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "overview_rows_real.json")

pytestmark = pytest.mark.skipif(
    not os.path.exists(FIXTURE),
    reason="tests/fixtures/overview_rows_real.json not captured yet (plan Task 3)",
)


class _MetaGroups:
    def meta_group_id(self, type_id):
        return 2

    def is_blueprint(self, type_id):
        return False


@pytest.fixture()
def input_rows():
    with open(FIXTURE, encoding="utf-8") as fh:
        rows = json.load(fh)
    return [PlannerInputRow.from_overview(r, meta_groups=_MetaGroups()) for r in rows]


def test_every_real_row_builds_an_input_row(input_rows):
    assert len(input_rows) > 1


def test_material_cost_is_never_structurally_zero(input_rows):
    assert all(r.material_cost_per_unit > 0 for r in input_rows)


def test_pipeline_analysis_yields_non_zero_pipeline_days(input_rows):
    states = PipelineAnalyzer().analyze(
        input_rows=input_rows, industry_jobs=[], corp_assets=[],
        market_depth_cache={}, weights={},
        sell_velocities={r.type_id: 5.0 for r in input_rows},
        meta_resolver=_MetaGroups(),
    )
    assert len(states) == len(input_rows)
    assert any(s.total_pipeline_days > 0 for s in states), (
        "no item has any pipeline supply — the phantom-key bug is back"
    )


def test_scoring_subtracts_material_cost_from_a_real_row(input_rows):
    row = input_rows[0]
    depth = {"vwap_5d": row.material_cost_per_unit * 2.0}
    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[], corp_assets=[], market_depth_cache={},
        weights={}, sell_velocities={row.type_id: 5.0}, meta_resolver=_MetaGroups(),
    )
    scored = ProfitabilityScorer().score(
        pipeline=states[0], row=row, weights=None, market_depth=depth,
        margin_correlation=None,
    )
    expected = row.material_cost_per_unit * row.runs * row.quantity
    assert scored.absolute_profit_per_batch == pytest.approx(expected)


def test_a_skilled_pilot_gets_more_than_one_slot():
    """Mirrors the real stored payload: skill_name + trained_skill_level, no
    active_skill_level key."""
    chars = [{
        "character_id": 1, "character_name": "Pilot",
        "skills": {"skills": [
            {"skill_id": 3387, "skill_name": "Mass Production",
             "trained_skill_level": 5},
            {"skill_id": 24625, "skill_name": "Advanced Mass Production",
             "trained_skill_level": 4},
            {"skill_id": 3406, "skill_name": "Laboratory Operation",
             "trained_skill_level": 5},
        ]},
    }]
    from datetime import datetime, timezone

    now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    slots = CharacterAssigner()._compute_available_slots(chars, [], now)
    assert slots[1]["free_mfg"] == 10
    assert slots[1]["free_research"] == 6


def test_manufacturing_jobs_are_indexed_from_the_activity_id_column(app_session, input_rows):
    row = input_rows[0]
    job = CorporationIndustryJobsModel(
        corporation_id=1, job_id=1, activity_id=1,
        product_type_id=row.type_id, runs=5, output_quantity=50, status="active",
    )
    app_session.add(job)
    app_session.commit()

    states = PipelineAnalyzer().analyze(
        input_rows=[row], industry_jobs=[job], corp_assets=[], market_depth_cache={},
        weights={}, sell_velocities={row.type_id: 5.0}, meta_resolver=_MetaGroups(),
    )
    assert states[0].has_active_manufacturing_jobs is True
```

- [ ] **Step 2: Run the test**

Run: `python -m pytest tests/test_daily_planner_integration.py -v`
Expected: PASS if the fixture exists, otherwise SKIP with the Task 3 reason.

If `test_pipeline_analysis_yields_non_zero_pipeline_days` fails, the captured fixture has no
pipeline enrichment — re-capture after a refresh that includes market history (Task 3, Step 4).

- [ ] **Step 3: Run the whole suite**

Run: `python -m pytest tests/ -q`
Expected: green, with only the fixture-dependent tests skipping if Task 3 has not been done.

- [ ] **Step 4: Verify in the real app**

Start the app, refresh the product overview, then recompute the daily plan. Confirm in the UI:
a plan is produced; build items show non-zero estimated cost and profit; the shopping list
quantities scale with run counts; more than one action is assigned per pilot. If the compute fails,
the status bar now names the missing field — that is the contract working, so fix the mapping it
names.

- [ ] **Step 5: Commit**

```bash
git add tests/test_daily_planner_integration.py
git commit -m "test: add end-to-end daily planner test over the real overview fixture

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Task(s) |
|---|---|
| Shared overview-row accessors | 4 |
| `PlannerInputRow` contract | 6 |
| Fail-loud policy | 18 (+ error surfacing in 18 Step 4) |
| `conftest.py` | 1 |
| Sanitised capture fixture | 2, 3 |
| Contract test vs real fixture | 6 |
| Cluster A (findings 1, 3, 4, 5, 6, 10 + phantom keys) | 7, 8, 9, 10 |
| Cluster B (delete reimplementation) | 7 |
| Cluster C (findings 7, 8, 9, 14) | 9, 11, 12 |
| D1 activity_id (finding 2) | 13 |
| D2 freshness (finding 15) | 15 |
| D3 chain keying (finding 11) | 16 |
| D4 copy action (finding 12) | 16 |
| D5 plan history (finding 13) | 14 |
| D6 meta group resolution | 5, 14 |
| Migrations (both) | 13, 15 |
| Success criteria 1-8 | 6, 15, 18, 19 |

All 15 review findings are covered. Four defects discovered while planning are also covered and
were **not** in the spec — flag these to the user:

1. `_best_invention_char` sums skill names that are never populated, so invention pilot choice is
   arbitrary (Task 9). Root cause shared with finding 1: the assigner hand-rolled a four-entry
   skill id map instead of using the SDE-backed mapper in `characters/character.py`. Task 9 now
   deletes that map and reads skills by name.
2. `_get_industry_jobs` applies no status filter, so delivered jobs count as active (Task 13).
3. `_build_corp_stock_map` excludes stock by the phantom `is_blueprint`, so blueprints land in the
   material stock map (Task 11). Cosmetic only — blueprint type_ids are never looked up there.
4. The invention branch of the shopping list is a `pass`, so invention inputs never appear
   (Task 17). This is build-out and belongs to cluster D.

**Correction to the spec:** the spec's Cluster A row for finding 1 says the fix is
`trained_skill_level`. That is necessary but not sufficient — `active_skill_level` is absent from
the stored payload entirely, and the deeper problem is the hand-rolled id map. Task 9 supersedes
that row.

**Type consistency:** `PlannerInputRow` field names are used identically in Tasks 6-11 and 19.
`meta_resolver` is the parameter name for the `TypeMetadataResolver` in every consumer
(`PipelineAnalyzer.analyze`, `index_blueprint_assets`, `ShoppingListBuilder.build`).
`build_product_to_blueprint_index` is defined in Task 16 and reused in Task 17.
`compute_freshness_score` is module-level in both its definition and its test.

**Known softness:** Tasks 12 and 14 need helper constructors (`_processor`, `_service`) whose exact
arguments depend on `__init__` signatures not fully read during planning; both steps say to match
the real signature rather than invent one. Task 17 Step 1 is an investigation step because the
SDE's invention-activity shape was not confirmed during planning.
