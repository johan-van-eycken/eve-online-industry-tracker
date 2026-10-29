"""The SDE's industry tables: imported, read, and loud when missing (SDE update).

CCP's SDE (build 3607705 on) ships industryTargetFilters,
industryModifierSources, industryAssemblyLines and industryInstallationTypes.
The importer stores them as every other table (one row per id, nested
lists as JSON); infrastructure.sde.industry reads them back into the rig
catalog (domain.rig_groups) and the shipyard lines
(domain.industry_facility). An SDE imported before they were added has
none of them: the app says so (an ERROR naming the re-import) and every
calculation that needs them raises, never a constant fallback.

Fake tables only (tests/sde_industry_fakes.py).
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
import sqlite3

import pytest
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

import sde_industry_fakes as fakes
from eve_online_industry_tracker.infrastructure.sde import industry

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _importer():
    spec = importlib.util.spec_from_file_location("import_sde_under_test", os.path.join(_ROOT, "scripts", "import_sde.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_fake_sde(directory):
    tables = {
        "industryTargetFilters": fakes.TARGET_FILTERS,
        "industryModifierSources": fakes.MODIFIER_SOURCES,
        "industryAssemblyLines": fakes.ASSEMBLY_LINES,
        "industryInstallationTypes": fakes.INSTALLATION_TYPES,
        "groups": {gid: {"categoryID": cat, "name": {"en": name, "de": f"{name} (de)"}, "published": True}
                   for gid, (cat, name) in fakes.GROUPS.items()},
        "categories": {cid: {"name": {"en": name, "de": f"{name} (de)"}, "published": True}
                       for cid, name in fakes.CATEGORIES.items()},
        "types": {900001: {"groupID": 25, "name": {"en": "Fake Frigate"}, "packagedVolume": 2500.0,
                           "volume": 16500.0}},
        "compressibleTypes": fakes.COMPRESSIBLE_TYPES,
        "typeDogma": fakes.TYPE_DOGMA,
        "dogmaEffects": fakes.DOGMA_EFFECTS,
        "industryActivities": fakes.INDUSTRY_ACTIVITIES,
    }
    for name, rows in tables.items():
        (directory / f"{name}.yaml").write_text(yaml.safe_dump(rows), encoding="utf-8")


@pytest.fixture()
def imported_fake_sde(tmp_path):
    sde_dir = tmp_path / "sde"
    sde_dir.mkdir()
    _write_fake_sde(sde_dir)
    db = tmp_path / "eve_sde.db"
    with open(os.path.join(_ROOT, "config", "import_sde.json"), encoding="utf-8") as fh:
        tables = json.load(fh)["TABLES_TO_IMPORT"]
    _importer().import_sde_to_sqlite(str(sde_dir), db_uri=f"sqlite:///{db}", tables_to_import=tables)
    engine = create_engine(f"sqlite:///{db}")
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def sde_without_industry_tables():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE types (id INTEGER PRIMARY KEY, volume FLOAT, packagedVolume FLOAT)"))
        conn.execute(text("CREATE TABLE groups (id INTEGER PRIMARY KEY, categoryID INTEGER, name JSON)"))
        conn.execute(text("CREATE TABLE categories (id INTEGER PRIMARY KEY, name JSON)"))
    try:
        yield engine
    finally:
        engine.dispose()


# --- the importer ----------------------------------------------------------------

def test_the_importer_imports_the_four_industry_tables():
    with open(os.path.join(_ROOT, "config", "import_sde.json"), encoding="utf-8") as fh:
        tables = set(json.load(fh)["TABLES_TO_IMPORT"])
    assert set(industry.INDUSTRY_TABLES) <= tables


def test_the_importer_stores_ids_and_nested_lists_as_json(imported_fake_sde):
    """flatten_row: one row per YAML key (id), nested lists as JSON text."""
    with imported_fake_sde.connect() as conn:
        row = conn.execute(text("SELECT * FROM industryTargetFilters WHERE id = 8")).mappings().one()
        line = conn.execute(text("SELECT * FROM industryInstallationTypes WHERE id = 35877")).mappings().one()
    assert json.loads(row["categoryIDs"]) == [32]
    assert json.loads(row["groupIDs"])[:2] == [358, 380]
    assert row["name"] == "Medium T2 Ships"
    assert json.loads(line["assemblyLines"]) == [{"assemblyLineID": 176}, {"assemblyLineID": 177}]


def test_the_tables_read_back_as_the_sde_has_them(imported_fake_sde):
    with Session(imported_fake_sde) as session:
        tables = industry.read_industry_tables(session)
    assert tables["industryTargetFilters"] == fakes.TARGET_FILTERS
    assert tables["industryModifierSources"] == fakes.MODIFIER_SOURCES
    assert tables["industryAssemblyLines"] == fakes.ASSEMBLY_LINES
    assert tables["industryInstallationTypes"] == fakes.INSTALLATION_TYPES


def test_the_imported_sde_gives_the_same_industry_data_as_the_fake_tables(imported_fake_sde):
    with Session(imported_fake_sde) as session:
        loaded = industry.load_industry_data(session)
    assert loaded == fakes.fake_industry_data()


# --- missing tables ------------------------------------------------------------------

def test_an_old_sde_lists_the_missing_tables(sde_without_industry_tables):
    with Session(sde_without_industry_tables) as session:
        assert industry.missing_industry_tables(session) == list(industry.INDUSTRY_TABLES)


def test_loading_from_an_old_sde_raises_and_names_the_reimport(sde_without_industry_tables):
    with Session(sde_without_industry_tables) as session, pytest.raises(industry.SdeIndustryDataMissing) as err:
        industry.load_industry_data(session)
    message = str(err.value)
    assert "re-run scripts/import_sde.py" in message
    for table in industry.INDUSTRY_TABLES:
        assert table in message


def test_the_schema_check_names_every_missing_piece(sde_without_industry_tables):
    with sde_without_industry_tables.begin() as conn:
        conn.execute(text("DROP TABLE types"))
        conn.execute(text("CREATE TABLE types (id INTEGER PRIMARY KEY, volume FLOAT)"))
    with Session(sde_without_industry_tables) as session:
        problems = industry.sde_schema_problems(session)
    assert any("types.packagedVolume" in p for p in problems)
    assert any("industryTargetFilters" in p for p in problems)
    assert all("re-run scripts/import_sde.py" in p for p in problems)


def test_the_schema_check_is_quiet_for_a_current_sde(imported_fake_sde):
    with Session(imported_fake_sde) as session:
        assert industry.sde_schema_problems(session) == []


# --- the process-wide data -----------------------------------------------------------

def test_with_no_data_and_no_loader_every_use_raises():
    industry.reset_industry_data()
    with pytest.raises(industry.SdeIndustryDataMissing, match="re-run scripts/import_sde.py"):
        industry.rig_catalog()
    with pytest.raises(industry.SdeIndustryDataMissing):
        industry.shipyard_lines()


def test_a_missing_table_is_logged_once_as_an_error_and_retried(caplog):
    industry.reset_industry_data()
    calls = []

    def loader():
        calls.append(1)
        if len(calls) < 3:
            raise industry.SdeIndustryDataMissing("SDE industry tables missing: industryAssemblyLines; "
                                                  "re-run scripts/import_sde.py --download --import --force")
        return fakes.fake_industry_data()

    industry.configure_industry_data_loader(loader)
    with caplog.at_level(logging.ERROR):
        for _ in range(2):
            with pytest.raises(industry.SdeIndustryDataMissing):
                industry.rig_catalog()
        # After the re-import the next use loads the tables; then they are cached.
        assert industry.rig_catalog() == fakes.fake_industry_data().rig_catalog
        industry.shipyard_lines()
    assert len(calls) == 3
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "re-run scripts/import_sde.py" in errors[0].getMessage()


def test_the_producer_and_planner_raise_when_the_tables_are_missing():
    from eve_online_industry_tracker.application.daily_planner import facility_bonus
    from eve_online_industry_tracker.application.industry.service import IndustryService

    industry.reset_industry_data()
    frigate = {"type_id": 1, "group_id": 25, "group_name": "Frigate", "category_id": 6, "category_name": "Ship"}
    with pytest.raises(industry.SdeIndustryDataMissing):
        IndustryService._manufacturing_rig_groups(frigate)
    with pytest.raises(industry.SdeIndustryDataMissing):
        IndustryService._infer_manufacturing_group(frigate)
    with pytest.raises(industry.SdeIndustryDataMissing):
        IndustryService._facility_gate(manufacturing_group="Capital Ships", profile_payload={}, group_id=485)
    with pytest.raises(industry.SdeIndustryDataMissing):
        facility_bonus.component_rig_groups(frigate)


# --- startup ---------------------------------------------------------------------------

def test_startup_on_an_old_sde_logs_an_error_and_a_health_warning(sde_without_industry_tables, caplog):
    from eve_online_industry_tracker.application.admin.service import AdminService
    from flask_app.bootstrap import check_sde_industry_data

    industry.reset_industry_data()
    state = _state(sde_without_industry_tables)
    with caplog.at_level(logging.ERROR):
        check_sde_industry_data(state)
    assert state.sde_warnings and all("re-run scripts/import_sde.py" in w for w in state.sde_warnings)
    assert any(r.levelno == logging.ERROR and "re-run scripts/import_sde.py" in r.getMessage()
               for r in caplog.records)
    payload, status = AdminService(state=state).health_payload()
    assert status == 200 and payload["warnings"] == state.sde_warnings
    with pytest.raises(industry.SdeIndustryDataMissing):
        industry.rig_catalog()


def test_startup_on_a_current_sde_loads_the_tables(imported_fake_sde):
    from eve_online_industry_tracker.application.admin.service import AdminService
    from flask_app.bootstrap import check_sde_industry_data

    industry.reset_industry_data()
    state = _state(imported_fake_sde)
    check_sde_industry_data(state)
    assert state.sde_warnings == [] and state.init_warnings == []
    assert industry.rig_catalog() == fakes.fake_industry_data().rig_catalog
    assert "warnings" not in AdminService(state=state).health_payload()[0]


def test_health_drops_the_warning_once_the_sde_is_reimported_live(tmp_path):
    """/health re-checks the SDE while it has a warning; after a re-import
    into the running app's SDE the industry data loads and the warning goes."""
    from eve_online_industry_tracker.application.admin.service import AdminService
    from flask_app.bootstrap import check_sde_industry_data

    db = tmp_path / "eve_sde.db"
    engine = create_engine(f"sqlite:///{db}")
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE types (id INTEGER PRIMARY KEY, volume FLOAT, packagedVolume FLOAT)"))
        industry.reset_industry_data()
        state = _state(engine)
        check_sde_industry_data(state)
        assert AdminService(state=state).health_payload()[0]["warnings"]

        sde_dir = tmp_path / "sde"
        sde_dir.mkdir()
        _write_fake_sde(sde_dir)
        with open(os.path.join(_ROOT, "config", "import_sde.json"), encoding="utf-8") as fh:
            tables = json.load(fh)["TABLES_TO_IMPORT"]
        _importer().import_sde_to_sqlite(str(sde_dir), db_uri=f"sqlite:///{db}", tables_to_import=tables)

        payload, status = AdminService(state=state).health_payload()
        assert status == 200 and "warnings" not in payload
        assert state.sde_warnings == []
        assert industry.rig_catalog() == fakes.fake_industry_data().rig_catalog
    finally:
        engine.dispose()


def test_the_hint_names_download_and_a_restart():
    assert "scripts/import_sde.py --download --import --force" in industry.REIMPORT_HINT
    assert "restart the app" in industry.REIMPORT_HINT


def test_an_empty_industry_table_is_not_loaded(imported_fake_sde):
    """A table that exists but has no rows (an import in progress or one
    that failed) counts as missing; the load is not tried."""
    with imported_fake_sde.begin() as conn:
        conn.execute(text("DELETE FROM industryAssemblyLines"))
    with Session(imported_fake_sde) as session:
        assert industry.missing_industry_tables(session) == ["industryAssemblyLines"]
        with pytest.raises(industry.SdeIndustryDataMissing, match="industryAssemblyLines"):
            industry.load_industry_data(session)


def test_a_request_that_needs_the_tables_gets_a_503_and_no_traceback(caplog):
    """SdeIndustryDataMissing is a 503 with code sde_reimport_required and
    the hint, logged without a traceback per request."""
    from flask_app.app import create_app

    app = create_app()

    def needs_the_tables():
        raise industry.SdeIndustryDataMissing(f"SDE industry tables missing: industryTargetFilters; "
                                              f"{industry.REIMPORT_HINT}")

    app.add_url_rule("/_test_needs_sde_tables", "needs_sde_tables", needs_the_tables)
    with caplog.at_level(logging.DEBUG):
        response = app.test_client().get("/_test_needs_sde_tables")
    assert response.status_code == 503
    body = response.get_json()
    assert body["error"]["code"] == "sde_reimport_required"
    assert "re-run scripts/import_sde.py" in body["message"]
    assert body["hint"] == industry.REIMPORT_HINT
    assert not any(r.exc_info for r in caplog.records)


# --- the local SDE ---------------------------------------------------------------------

from sde_industry_fakes import LOCAL_SDE as _LOCAL_SDE, skip_without_local_sde, _state


def test_the_local_sde_tables_have_ccps_row_counts():
    skip_without_local_sde()
    with sqlite3.connect(f"file:{_LOCAL_SDE}?mode=ro", uri=True) as conn:
        present = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        missing = [t for t in industry.INDUSTRY_TABLES if t not in present]
        if missing:
            pytest.skip(f"local SDE lacks {', '.join(missing)}: re-run scripts/import_sde.py --download --import --force")
        counts = {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in industry.INDUSTRY_TABLES}
    assert counts == {"industryTargetFilters": 18, "industryModifierSources": 220,
                      "industryAssemblyLines": 146, "industryInstallationTypes": 102}
