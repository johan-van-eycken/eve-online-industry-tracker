"""Guards that keep the Daily Planner failing loudly.

Broad `except Exception:` handlers, `or 0` defaults and reads of overview-row
keys the producer never writes are what let 19 field mismatches run silently
for months. Every planner consumer now reads validated fields, so these tests
keep the old defences from creeping back in.
"""
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

#: Overview-row keys no producer writes at the level the planner used to read
#: them. A string constant equal to one of these is how a phantom dict-key
#: read (`row.get("x")`, `row["x"]`, `getattr(o, "x")`) shows up in the AST.
BANNED_KEYS = frozenset({
    "estimated_material_cost_per_unit",
    "material_cost_per_unit",
    "corp_stock_qty",
    "units_on_market",
    "runs_per_batch",
    "product_quantity",
    "price_trend_pct",
    "is_blueprint",
    "active_skill_level",
    "estimated_material_cost",
    # item_decision_engine used to infer a watch item's stage from these;
    # nothing in the code base writes any of them.
    "invention_in_flight",
    "bpc_in_flight",
    "copy_in_flight",
    # ChainPlanner's BPO opportunity dict is keyed bpo_market_price.
    "market_price",
})


def _module_files():
    for name in sorted(os.listdir(PACKAGE)):
        if name.endswith(".py"):
            yield name, os.path.join(PACKAGE, name)


def _parse(path):
    with open(path, encoding="utf-8") as fh:
        return ast.parse(fh.read())


def _broad_handlers(tree):
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        exc = node.type
        if exc is None or (isinstance(exc, ast.Name) and exc.id in ("Exception", "BaseException")):
            found.append(node)
    return found


def _docstring_nodes(tree):
    ids = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            ids.add(id(body[0].value))
    return ids


def banned_string_constants(source: str, banned=BANNED_KEYS) -> list[tuple[int, str]]:
    """(line, key) for every non-docstring string constant exactly equal to a banned key."""
    tree = ast.parse(source)
    docstrings = _docstring_nodes(tree)
    hits = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
            and node.value in banned
        ):
            hits.append((node.lineno, node.value))
    return sorted(hits)


def test_broad_excepts_are_only_where_deliberately_allowed():
    offenders = {}
    for name, path in _module_files():
        handlers = _broad_handlers(_parse(path))
        allowed = ALLOWED_BROAD_EXCEPT.get(name, 0)
        if len(handlers) > allowed:
            offenders[name] = ([h.lineno for h in handlers], allowed)
    assert not offenders, f"unexpected broad except handlers (lines, allowed): {offenders}"


def test_every_allowed_broad_except_logs_with_a_traceback():
    for name, path in _module_files():
        if ALLOWED_BROAD_EXCEPT.get(name, 0) == 0:
            continue
        for handler in _broad_handlers(_parse(path)):
            body = ast.dump(ast.Module(body=handler.body, type_ignores=[]))
            assert "logger" in body and ("exception" in body or "exc_info" in body), (
                f"{name}:{handler.lineno}: a broad except must log the traceback"
            )


def test_no_phantom_overview_row_keys_remain():
    hits = []
    for name, path in _module_files():
        with open(path, encoding="utf-8") as fh:
            for line, key in banned_string_constants(fh.read()):
                hits.append((name, line, key))
    assert not hits, f"phantom overview-row keys still referenced: {hits}"


def test_scanner_catches_a_key_read_but_not_docstrings_or_identifiers():
    source = '''
"""Module docstring mentioning units_on_market."""

def f(row):
    """Function docstring: units_on_market is gone."""
    units_on_market = 3          # an identifier, not a key read
    # a comment about "units_on_market"
    return row.get("units_on_market"), units_on_market

class C:
    """Class docstring: units_on_market."""
'''
    hits = banned_string_constants(source, banned={"units_on_market"})
    assert hits == [(8, "units_on_market")]


def test_scanner_catches_subscript_and_getattr_reads():
    source = 'a = row["runs_per_batch"]\nb = getattr(o, "is_blueprint", None)\n'
    hits = banned_string_constants(source, banned={"runs_per_batch", "is_blueprint"})
    assert hits == [(1, "runs_per_batch"), (2, "is_blueprint")]


def test_every_planner_setting_read_exists_in_the_admin_schema():
    """`_adm` falls back on KeyError only for test stubs: a real key must exist.

    AdminSettingsManager.get raises KeyError for an unknown key, and `_adm`
    returns its fallback on KeyError -- so a typo in a setting name would
    otherwise silently pin that setting to its hard-coded default forever.
    """
    from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA

    known = set(ADMIN_SETTINGS_SCHEMA["daily_planner"]["settings"])
    used = set()
    for name, path in _module_files():
        for node in ast.walk(_parse(path)):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            fname = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if fname != "_adm":
                continue
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    used.add((name, arg.value))
                    break
    assert used, "found no _adm(...) setting reads -- the scan is broken"
    unknown = sorted((n, k) for n, k in used if k not in known)
    assert not unknown, f"settings read by the planner but missing from the schema: {unknown}"


# ──────────────────────────────────────────────────────────────────────────────
# Behaviour that the narrowing changes
# ──────────────────────────────────────────────────────────────────────────────

import logging  # noqa: E402
from types import SimpleNamespace  # noqa: E402

import pytest  # noqa: E402
from sqlalchemy.exc import OperationalError  # noqa: E402

from eve_online_industry_tracker.application.daily_planner.chain_planner import ChainPlanner  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.character_assigner import (  # noqa: E402
    CharacterAssigner,
)
from eve_online_industry_tracker.application.daily_planner.input_row import PlannerInputError  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.models import (  # noqa: E402
    ChainPlan,
    ItemDecision,
)
from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService  # noqa: E402


def _sde_error():
    return OperationalError("SELECT 1", {}, Exception("database is locked"))


class _BrokenSdeProvider:
    def __init__(self, exc):
        self._exc = exc

    def sde_session(self):
        raise self._exc


class _AdminKeyError:
    def get(self, section, key):
        raise KeyError(key)


def _decision(row, **overrides):
    base = dict(
        type_id=int(row["type_id"]), type_name="Thing", decision="build",
        decision_reason="test", adjusted_score=1.0, absolute_profit_per_batch=1.0,
        isk_per_hour=1.0, margin_pct=1.0, days_of_supply_current=1.0,
        effective_velocity=1.0, meta_group_id=1, pipeline_stage="manufacturing",
        overview_row=row,
    )
    base.update(overrides)
    return ItemDecision(**base)


# --- ChainPlanner: optimal ME/TE no longer swallow to 0 --------------------------

def test_an_sde_failure_makes_optimal_me_and_te_unknown_not_zero(caplog):
    planner = ChainPlanner(None, _BrokenSdeProvider(_sde_error()), _AdminKeyError())
    with caplog.at_level(logging.WARNING):
        assert planner._compute_optimal_me(999) is None
        assert planner._compute_optimal_te(999, 1.0) is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2
    assert all("999" in r.getMessage() for r in warnings)


def test_a_non_database_error_in_optimal_me_propagates():
    planner = ChainPlanner(None, _BrokenSdeProvider(RuntimeError("bug")), _AdminKeyError())
    with pytest.raises(RuntimeError):
        planner._compute_optimal_me(999)


def test_an_unknown_optimal_level_schedules_no_research_and_says_why(caplog):
    row = {"type_id": 12345, "type_name": "Thing", "quantity": 1,
           "manufacturing_job": {"runs": 1, "blueprint_sde": {"blueprint_type_id": 999}}}
    bpo = SimpleNamespace(type_id=999, is_blueprint_copy=False,
                          blueprint_material_efficiency=0, blueprint_time_efficiency=0)
    planner = ChainPlanner(None, None, _AdminKeyError())
    planner._compute_optimal_me = lambda bp_type_id: None
    planner._compute_optimal_te = lambda bp_type_id, threshold: None

    with caplog.at_level(logging.WARNING):
        planner.plan_chain([_decision(row)], {"bpo_assets_by_type_id": {999: [bpo]}})

    assert "needs_me_research" not in row
    assert "needs_te_research" not in row
    assert any("optimal" in r.getMessage() and r.levelno == logging.WARNING
               for r in caplog.records)


# --- DailyPlannerService: T1 source lookup only degrades on a DB error ----------

def _fake_bp_service():
    session = SimpleNamespace(close=lambda: None)
    return SimpleNamespace(_session_provider=SimpleNamespace(sde_session=lambda: session))


def _bp_row(bp_type_id):
    return {"type_id": 12345,
            "manufacturing_job": {"blueprint_sde": {"blueprint_type_id": bp_type_id}}}


def test_a_non_database_error_in_the_source_lookup_propagates(monkeypatch):
    from eve_online_industry_tracker.infrastructure.sde import blueprints as sde_blueprints

    def broken(session, ids):
        raise RuntimeError("a real bug")

    monkeypatch.setattr(sde_blueprints, "get_invention_source_blueprint_ids", broken)
    with pytest.raises(RuntimeError):
        DailyPlannerService._get_blueprint_data(_fake_bp_service(), [_bp_row(1999)])


# --- CharacterAssigner: the manufacture action reads the real run count ---------

def _mfg_pilot():
    return SimpleNamespace(list_characters=lambda: [
        {"character_id": 1, "character_name": "Pilot", "skills": {"skills": [], "total_sp": 0}}
    ])


def _assign(row):
    plan = ChainPlan(decisions=[_decision(row, meta_group_id=2)])
    return [a for a in CharacterAssigner().assign(plan, [], _mfg_pilot(), _AdminKeyError())
            if a.action_type == "manufacture"]


def test_a_manufacture_action_carries_the_batch_runs_and_material_cost():
    """runs lives at manufacturing_job.runs and the batch's material cost at
    manufacturing_job.material_cost. Reading top-level runs_per_batch / runs /
    estimated_material_cost gave every job 1 run and a 0 ISK cost, so the
    shopping list bought materials for one run of a 20-run job."""
    row = {"type_id": 12345, "quantity": 200,
           "manufacturing_job": {"runs": 20, "material_cost": 50_000_000.0}}
    (action,) = _assign(row)
    assert action.runs == 20
    assert action.estimated_cost_isk == 50_000_000.0


def test_an_unpriced_batch_has_an_unknown_cost_not_zero():
    row = {"type_id": 12345, "quantity": 200, "manufacturing_job": {"runs": 20}}
    (action,) = _assign(row)
    assert action.estimated_cost_isk is None


def test_a_manufacture_decision_without_runs_is_a_contract_violation():
    row = {"type_id": 12345, "quantity": 200, "manufacturing_job": {}}
    with pytest.raises(PlannerInputError) as exc:
        _assign(row)
    assert exc.value.field == "manufacturing_job.runs"
    assert exc.value.type_id == 12345


# --- DailyPlannerService: collaborator reads that used to swallow ---------------

def _bare_service():
    return DailyPlannerService(
        industry_service=SimpleNamespace(get_cached_overview_rows=lambda: []),
        corporations_service=SimpleNamespace(list_corporations=lambda: []),
        characters_service=SimpleNamespace(list_characters=lambda: []),
        sales_history_service=SimpleNamespace(),
        pricing_suggestion_service=SimpleNamespace(),
        market_pricing_service=SimpleNamespace(),
        realized_profit_service=SimpleNamespace(),
        repo=SimpleNamespace(),
        admin_settings=SimpleNamespace(),
        session_provider=SimpleNamespace(),
    )


def test_corp_id_is_read_from_an_object_shaped_corporation_too():
    svc = _bare_service()
    svc._corporations = SimpleNamespace(
        list_corporations=lambda: [SimpleNamespace(corporation_id=98000001)])
    assert svc._get_corp_id() == 98000001


def test_no_corporation_means_no_sales_history_lookups():
    svc = _bare_service()  # sales_history_service has no get_sold_history at all
    assert svc._get_sell_velocities([12345]) == {}


def test_a_pricing_service_without_get_suggestions_warns(caplog):
    svc = _bare_service()
    with caplog.at_level(logging.WARNING):
        assert svc._get_pricing_suggestions() == []
    assert any("get_suggestions" in r.getMessage() for r in caplog.records
               if r.levelno == logging.WARNING)


