"""Ore compression, the ore reprocessing skill and the industry activities from the SDE.

CCP's SDE ships compressibleTypes (a base ore, ice or gas -> its compressed
form), typeDogma attribute 790 reprocessingSkillType (the skill that
reprocesses an ore) and industryActivities (the activity ids and names).
They replace a "Compressed" name test, a hand-made ore -> skill map and two
hard-coded activity id -> name maps. An SDE imported without the tables says
so (an ERROR, a /health warning, a 503 naming the re-import); the ore
calculator and the industry jobs never guess.

Fake tables only (tests/sde_industry_fakes.py), except the local-SDE tests at
the end, which skip when database/eve_sde.db lacks the tables.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
import sqlite3
from types import SimpleNamespace

import pytest
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import sde_industry_fakes as fakes
from eve_online_industry_tracker.domain import industry_activities as domain_activities
from eve_online_industry_tracker.infrastructure.sde import activities, industry, ore

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _importer():
    spec = importlib.util.spec_from_file_location("import_sde_under_test", os.path.join(_ROOT, "scripts", "import_sde.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _config_tables() -> list[str]:
    with open(os.path.join(_ROOT, "config", "import_sde.json"), encoding="utf-8") as fh:
        return json.load(fh)["TABLES_TO_IMPORT"]


@pytest.fixture()
def imported_fake_sde(tmp_path):
    sde_dir = tmp_path / "sde"
    sde_dir.mkdir()
    for name, rows in {"compressibleTypes": fakes.COMPRESSIBLE_TYPES, "typeDogma": fakes.TYPE_DOGMA,
                       "dogmaEffects": fakes.DOGMA_EFFECTS, "industryActivities": fakes.INDUSTRY_ACTIVITIES}.items():
        (sde_dir / f"{name}.yaml").write_text(yaml.safe_dump(rows), encoding="utf-8")
    db = tmp_path / "eve_sde.db"
    _importer().import_sde_to_sqlite(str(sde_dir), db_uri=f"sqlite:///{db}",
                                     tables_to_import=["compressibleTypes", "typeDogma", "dogmaEffects",
                                                       "industryActivities"])
    engine = create_engine(f"sqlite:///{db}")
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def empty_sde():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE types (id INTEGER PRIMARY KEY, volume FLOAT, packagedVolume FLOAT)"))
    try:
        yield engine
    finally:
        engine.dispose()


# --- the importer ----------------------------------------------------------------

def test_the_importer_imports_compressible_types_and_industry_activities():
    tables = set(_config_tables())
    assert {"compressibleTypes", "industryActivities", "typeDogma"} <= tables
    assert set(ore.ORE_TABLES) <= tables and set(activities.ACTIVITY_TABLES) <= tables


def test_compressible_types_import_as_one_row_per_type_id(imported_fake_sde):
    with imported_fake_sde.connect() as conn:
        rows = conn.execute(text('SELECT id, "compressedTypeID" FROM "compressibleTypes" ORDER BY id')).fetchall()
        activity = conn.execute(text('SELECT * FROM "industryActivities" WHERE id = 3')).mappings().one()
    assert [tuple(r) for r in rows] == sorted((b, row["compressedTypeID"]) for b, row in fakes.COMPRESSIBLE_TYPES.items())
    assert activity["name"] == "Time Efficiency Research"


def test_the_imported_tables_give_the_same_data_as_the_fakes(imported_fake_sde):
    with Session(imported_fake_sde) as session:
        assert ore.load_ore_data(session) == fakes.fake_ore_data()
        assert activities.load_industry_activities(session) == fakes.fake_industry_activities()


# --- compression -------------------------------------------------------------------

def test_compression_goes_both_ways():
    data = fakes.fake_ore_data()
    assert data.is_compressed(fakes.FAKE_ORE_COMPRESSED)
    assert not data.is_compressed(fakes.FAKE_ORE_BASE)
    assert data.base_of(fakes.FAKE_ORE_COMPRESSED) == fakes.FAKE_ORE_BASE
    assert data.base_of(fakes.FAKE_ORE_BASE) is None
    assert data.compressed_form_of(fakes.FAKE_ORE_BASE) == fakes.FAKE_ORE_COMPRESSED
    assert data.compressed_form_of(fakes.FAKE_ORE_COMPRESSED) is None


def test_a_batch_compressed_type_is_not_a_compressed_form():
    """Batch Compressed ores (the pre-2021 compression) are not in
    compressibleTypes: the name does not make them compressed."""
    data = fakes.fake_ore_data()
    assert not data.is_compressed(fakes.FAKE_ORE_LEGACY_COMPRESSED)
    assert data.base_of(fakes.FAKE_ORE_LEGACY_COMPRESSED) is None


def test_a_compressed_type_listed_twice_is_unreadable():
    with pytest.raises(ValueError, match="compressed form of more than one"):
        ore.ore_data_from_tables(dogma_effects={}, compressible_types={1: {"compressedTypeID": 9}, 2: {"compressedTypeID": 9}},
                                 type_dogma={})


def test_a_row_without_a_compressed_type_is_unreadable():
    with pytest.raises(ValueError, match="compressedTypeID"):
        ore.ore_data_from_tables(compressible_types={1: {}}, type_dogma={}, dogma_effects={})


# --- the reprocessing skill ---------------------------------------------------------

def test_an_ore_uses_its_attribute_790():
    data = fakes.fake_ore_data()
    assert data.reprocessing_skill(fakes.FAKE_ORE_BASE) == fakes.SKILL_SIMPLE_ORE_PROCESSING
    assert data.reprocessing_skill(fakes.FAKE_ORE_COMPRESSED) == fakes.SKILL_SIMPLE_ORE_PROCESSING
    assert data.reprocessing_skill(fakes.FAKE_ORE_LEGACY_COMPRESSED) == fakes.SKILL_COMPLEX_ORE_PROCESSING


def test_a_compressed_ore_without_its_own_790_uses_its_base_ores():
    data = fakes.fake_ore_data()
    assert data.reprocessing_skill(fakes.FAKE_ORE_COMPRESSED_NO_SKILL) == fakes.SKILL_COMPLEX_ORE_PROCESSING


def test_a_type_without_790_has_no_reprocessing_skill():
    data = fakes.fake_ore_data()
    assert data.reprocessing_skill(fakes.FAKE_ORE_NO_SKILL) is None
    assert data.reprocessing_skill(fakes.FAKE_GAS_COMPRESSED) is None
    assert data.reprocessing_skill(123456789) is None


# --- the ore calculator --------------------------------------------------------------

def _seed_ore_sde(engine):
    from eve_online_industry_tracker.infrastructure.models import Categories, Groups, TypeMaterials, Types

    with Session(engine) as session:
        session.add(Categories(id=25, name={"en": "Asteroid"}, published=True))
        session.add(Categories(id=16, name={"en": "Skill"}, published=True))
        session.add(Groups(id=9801, categoryID=25, name={"en": "Fake Ore"}, published=True))
        session.add(Groups(id=1218, categoryID=16, name={"en": "Resource Processing"}, published=True))
        for tid, name in [(fakes.FAKE_ORE_BASE, "Fakeite"), (fakes.FAKE_ORE_COMPRESSED, "Squashed Fakeite"),
                          (fakes.FAKE_ORE_NO_SKILL, "Unskilled Fakeite"),
                          (fakes.FAKE_ORE_LEGACY_COMPRESSED, "Batch Compressed Fakeite")]:
            session.add(Types(id=tid, groupID=9801, name={"en": name}, published=True, volume=0.1, portionSize=100))
            session.add(TypeMaterials(id=tid, materials=[{"materialTypeID": 34, "quantity": 400}]))
        for sid, name in fakes.SKILL_NAMES.items():
            session.add(Types(id=sid, groupID=1218, name={"en": name, "de": f"{name} (de)"}, published=True))
        session.add(Types(id=34, groupID=18, name={"en": "Tritanium"}, published=True, volume=0.01, portionSize=1))
        session.commit()


def test_build_all_ores_carries_the_sde_compression_and_skill(sde_engine):
    from eve_online_industry_tracker.infrastructure.sde.static_data import build_all_ores

    _seed_ore_sde(sde_engine)
    with Session(sde_engine) as session:
        ores = {o["id"]: o for o in build_all_ores(session, "en")}
        german = {o["id"]: o for o in build_all_ores(session, "de")}
    base, squashed = ores[fakes.FAKE_ORE_BASE], ores[fakes.FAKE_ORE_COMPRESSED]
    assert (base["compressed"], base["compressedFromTypeID"], base["compressedTypeID"]) == (
        False, None, fakes.FAKE_ORE_COMPRESSED)
    assert (squashed["compressed"], squashed["compressedFromTypeID"], squashed["compressedTypeID"]) == (
        True, fakes.FAKE_ORE_BASE, None)
    assert base["reprocessingSkillTypeID"] == fakes.SKILL_SIMPLE_ORE_PROCESSING
    assert base["reprocessingSkill"] == "Simple Ore Processing"
    assert german[fakes.FAKE_ORE_BASE]["reprocessingSkill"] == "Simple Ore Processing (de)"
    assert ores[fakes.FAKE_ORE_LEGACY_COMPRESSED]["compressed"] is False
    assert ores[fakes.FAKE_ORE_NO_SKILL]["reprocessingSkill"] is None


def test_build_all_ores_without_the_ore_data_raises_with_the_hint(sde_engine):
    from eve_online_industry_tracker.infrastructure.sde.static_data import build_all_ores

    _seed_ore_sde(sde_engine)
    ore.reset_ore_data()
    with Session(sde_engine) as session, pytest.raises(industry.SdeIndustryDataMissing,
                                                       match="re-run scripts/import_sde.py"):
        build_all_ores(session, "en")


def _ore(type_id, name, skill_type_id, *, compressed=False):
    return {"id": type_id, "name": name, "portionSize": 100, "volume": 0.1, "compressed": compressed,
            "groupID": 462, "categoryID": 25,
            "reprocessingSkillTypeID": skill_type_id,
            "reprocessingSkill": fakes.SKILL_NAMES.get(skill_type_id) if skill_type_id else None,
            "materials": [{"name": "Tritanium", "quantity": 400}]}


def _npc_station():
    from eve_online_industry_tracker.infrastructure.static_data import facility_repo

    return facility_repo.facility_from_choice(facility_type="npc_station", rig="none", security_band="high",
                                              tax=None, npc_station_id=fakes.FAKE_NPC_STATION_ID,
                                              npc_station=fakes.fake_npc_station, data=fakes.fake_ore_data())


def test_the_yield_uses_the_sde_skill_not_the_name():
    from eve_online_industry_tracker.infrastructure.static_data.yield_calc import compute_yields

    # The name would say Simple Ore Processing; the SDE says Complex.
    ores = [_ore(1, "Veldspar", fakes.SKILL_COMPLEX_ORE_PROCESSING)]
    skills = {fakes.SKILL_SIMPLE_ORE_PROCESSING: 0, fakes.SKILL_COMPLEX_ORE_PROCESSING: 5}
    (row,) = compute_yields(ores, skills, _npc_station(), 0.0)
    assert row["batch_yield_percent"] == pytest.approx(50 * 1.10)
    assert row["batch_yields"] == {"Tritanium": 220}


def test_an_ore_without_a_reprocessing_skill_is_left_out_with_a_warning(caplog):
    from eve_online_industry_tracker.infrastructure.static_data.yield_calc import compute_yields

    ores = [_ore(1, "Fakeite", fakes.SKILL_SIMPLE_ORE_PROCESSING), _ore(2, "Unskilled Fakeite", None)]
    with caplog.at_level(logging.WARNING):
        rows = compute_yields(ores, {}, _npc_station(), 0.0)
    assert [r["id"] for r in rows] == [1]
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "Unskilled Fakeite" in warnings[0] and "790" in warnings[0]


def test_an_ore_without_reprocessing_materials_is_left_out_quietly(caplog):
    """No typeMaterials: there is nothing to reprocess (the optimizer drops
    rows without yields anyway), so no skill is needed and none is warned of."""
    from eve_online_industry_tracker.infrastructure.static_data.yield_calc import compute_yields

    no_materials = {**_ore(2, "Mutanite", None), "materials": []}
    with caplog.at_level(logging.WARNING):
        rows = compute_yields([no_materials], {}, _npc_station(), 0.0)
    assert rows == [] and not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_only_compressed_follows_the_sde_not_the_name(monkeypatch, tmp_path):
    from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager
    from eve_online_industry_tracker.infrastructure.static_data import optimizer_service

    ores = [_ore(1, "Fakeite", fakes.SKILL_SIMPLE_ORE_PROCESSING),
            _ore(2, "Squashed Fakeite", fakes.SKILL_SIMPLE_ORE_PROCESSING, compressed=True),
            _ore(3, "Batch Compressed Fakeite", fakes.SKILL_SIMPLE_ORE_PROCESSING)]
    seen = {}

    def fake_optimize(**kwargs):
        seen["ores"] = [o["id"] for o in kwargs["ores"]]
        return {"status": "infeasible"}

    class FakeEsi:
        def get_material_prices(self, ids):
            return {}

        def get_ore_prices(self, ids):
            return {}

    monkeypatch.setattr(optimizer_service, "build_all_ores", lambda session, language: ores)
    monkeypatch.setattr(optimizer_service, "build_all_materials",
                        lambda session, language: [{"id": 34, "name": "Tritanium", "volume": 0.01}])
    monkeypatch.setattr(optimizer_service, "optimize_ore_tiered", fake_optimize)
    character = SimpleNamespace(reprocessing_skills={fakes.SKILL_SIMPLE_ORE_PROCESSING: 5})
    payload = {"demands": {"Tritanium": 1000}, "facility_id": 1}
    admin_settings = AdminSettingsManager(tmp_path / "admin_settings.json")
    admin_settings.set_bulk({"reprocessing": {"facility_type": "athanor", "tax": 0.01}})
    for only_compressed, expected in [(True, [2]), (False, [1, 2, 3])]:
        optimizer_service.run_optimize({**payload, "only_compressed": only_compressed}, character=character,
                                       esi_service=FakeEsi(), sde_session=None, language="en",
                                       admin_settings=admin_settings)
        assert seen["ores"] == expected


# --- the industry activities ------------------------------------------------------------

def test_the_sde_lists_the_activities_and_todays_labels_stay():
    data = fakes.fake_industry_activities()
    assert data.names == {a: row["name"] for a, row in fakes.INDUSTRY_ACTIVITIES.items()}
    assert data.labels == {1: "Manufacturing", 3: "TE Research", 4: "ME Research", 5: "Copying",
                           8: "Invention", 9: "Reaction"}


def test_an_activity_the_sde_does_not_know_is_unknown():
    data = fakes.fake_industry_activities()
    assert data.label(7) == "Unknown (7)"
    assert data.label(None) == "Unknown (None)"
    assert activities.activity_label(11) == "Unknown (11)"
    assert activities.activity_label(3) == "TE Research"


def test_a_label_override_only_renames_an_activity_the_sde_has():
    data = activities.industry_activities_from_table({1: {"name": "Manufacturing"}})
    assert data.labels == {1: "Manufacturing"}
    assert data.label(3) == "Unknown (3)"


def test_an_activity_without_a_name_is_unreadable():
    with pytest.raises(ValueError, match="no name"):
        activities.industry_activities_from_table({1: {"description": "Manufacturing of things"}})


def test_a_localized_activity_name_reads_in_english():
    data = activities.industry_activities_from_table({5: {"name": {"en": "Copying", "de": "Kopieren"}}})
    assert data.names == {5: "Copying"}


def test_the_app_constants_are_activities_the_labels_know():
    labels = fakes.fake_industry_activities().labels
    app_ids = {domain_activities.ACTIVITY_MANUFACTURING, *domain_activities.LAB_ACTIVITY_IDS,
               *domain_activities.REACTION_ACTIVITY_IDS}
    assert app_ids <= set(labels)
    assert set(domain_activities.DISPLAY_LABEL_OVERRIDES) <= set(labels)


def test_the_streamlit_constants_are_the_domain_constants():
    from streamlit_ui import eve_constants

    assert eve_constants.ACTIVITY_MANUFACTURING == domain_activities.ACTIVITY_MANUFACTURING
    assert eve_constants.ACTIVITY_LAB_IDS == domain_activities.LAB_ACTIVITY_IDS
    assert eve_constants.ACTIVITY_REACTION_IDS == domain_activities.REACTION_ACTIVITY_IDS
    assert not hasattr(eve_constants, "ACTIVITY_ID_TO_NAME")


def _jobs_service(session_provider, app_engine, activity_id):
    from eve_online_industry_tracker.application.industry.service import IndustryService
    from eve_online_industry_tracker.infrastructure.models import CharacterIndustryJobsModel

    with Session(app_engine) as session:
        session.add(CharacterIndustryJobsModel(character_id=7, job_id=1, status="active", activity_id=activity_id,
                                               raw={"activity_id": activity_id}))
        session.commit()
    state = SimpleNamespace(char_manager=None, db_sde=SimpleNamespace(language="en"))
    return IndustryService(state=state, sessions=session_provider)


@pytest.mark.parametrize("activity_id, label", [(1, "Manufacturing"), (3, "TE Research"), (4, "ME Research"),
                                                (9, "Reaction"), (7, "Unknown (7)")])
def test_active_jobs_carry_the_sde_label(session_provider, app_engine, activity_id, label):
    svc = _jobs_service(session_provider, app_engine, activity_id)
    (job,) = svc.industry_active_jobs()["jobs"]
    assert (job["activity_id"], job["activity_name"]) == (activity_id, label)


def test_active_jobs_without_the_activities_raise_with_the_hint(session_provider, app_engine):
    svc = _jobs_service(session_provider, app_engine, 1)
    activities.reset_industry_activities()
    with pytest.raises(industry.SdeIndustryDataMissing, match="re-run scripts/import_sde.py"):
        svc.industry_active_jobs()


# --- missing tables ------------------------------------------------------------------

def test_the_schema_check_names_the_new_tables(empty_sde):
    with Session(empty_sde) as session:
        problems = industry.sde_schema_problems(session)
    for table in ("compressibleTypes", "typeDogma", "industryActivities"):
        assert any(table in p for p in problems), table
    assert any("ore calculator" in p for p in problems)
    assert any("industry activit" in p for p in problems)
    assert all("re-run scripts/import_sde.py" in p for p in problems)


def test_an_empty_compressible_types_table_counts_as_missing(imported_fake_sde):
    with imported_fake_sde.begin() as conn:
        conn.execute(text('DELETE FROM "compressibleTypes"'))
    with Session(imported_fake_sde) as session:
        assert industry.missing_tables(session, ore.ORE_TABLES) == ["compressibleTypes"]
        with pytest.raises(industry.SdeIndustryDataMissing, match="compressibleTypes"):
            ore.load_ore_data(session)


def test_loading_from_an_sde_without_the_tables_raises_and_names_the_reimport(empty_sde):
    with Session(empty_sde) as session:
        with pytest.raises(industry.SdeIndustryDataMissing, match="compressibleTypes.*re-run scripts/import_sde.py"):
            ore.load_ore_data(session)
        with pytest.raises(industry.SdeIndustryDataMissing, match="industryActivities.*re-run scripts/import_sde.py"):
            activities.load_industry_activities(session)


def test_unreadable_tables_raise_with_the_hint(imported_fake_sde):
    with imported_fake_sde.begin() as conn:
        conn.execute(text('UPDATE "industryActivities" SET name = NULL WHERE id = 1'))
        conn.execute(text('UPDATE "compressibleTypes" SET "compressedTypeID" = :c'),
                     {"c": fakes.FAKE_ORE_COMPRESSED})
    with Session(imported_fake_sde) as session:
        with pytest.raises(industry.SdeIndustryDataMissing, match="unreadable.*re-run scripts/import_sde.py"):
            ore.load_ore_data(session)
        with pytest.raises(industry.SdeIndustryDataMissing, match="unreadable.*re-run scripts/import_sde.py"):
            activities.load_industry_activities(session)


def test_with_no_data_and_no_loader_every_use_raises():
    ore.reset_ore_data()
    activities.reset_industry_activities()
    with pytest.raises(industry.SdeIndustryDataMissing, match="re-run scripts/import_sde.py"):
        ore.ore_data()
    with pytest.raises(industry.SdeIndustryDataMissing, match="re-run scripts/import_sde.py"):
        activities.activity_label(1)


def test_a_failed_ore_load_is_logged_once_and_retried(caplog):
    ore.reset_ore_data()
    calls = []

    def loader():
        calls.append(1)
        if len(calls) < 3:
            raise industry.SdeIndustryDataMissing(f"SDE tables missing: compressibleTypes; {industry.REIMPORT_HINT}")
        return fakes.fake_ore_data()

    ore.configure_ore_data_loader(loader)
    with caplog.at_level(logging.ERROR):
        for _ in range(2):
            with pytest.raises(industry.SdeIndustryDataMissing):
                ore.ore_data()
        assert ore.ore_data() == fakes.fake_ore_data()
        ore.ore_data()
    assert len(calls) == 3
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1


def test_the_rig_data_does_not_need_the_ore_or_activity_tables():
    """Each dataset fails on its own: a DB without compressibleTypes keeps its
    rig groups and shipyard gate."""
    ore.reset_ore_data()
    activities.reset_industry_activities()
    assert industry.rig_catalog() == fakes.fake_industry_data().rig_catalog


# --- startup and /health ---------------------------------------------------------------

def _state(engine):
    return SimpleNamespace(db_sde=SimpleNamespace(Session=sessionmaker(bind=engine)), init_warnings=[],
                           sde_warnings=[], init_state="Ready", init_error=None)


def test_startup_without_the_tables_logs_errors_and_health_warnings(empty_sde, caplog):
    from eve_online_industry_tracker.application.admin.service import AdminService
    from flask_app.bootstrap import check_sde_industry_data

    industry.reset_industry_data()
    ore.reset_ore_data()
    activities.reset_industry_activities()
    state = _state(empty_sde)
    with caplog.at_level(logging.ERROR):
        check_sde_industry_data(state)
    joined = " ".join(state.sde_warnings)
    assert "compressibleTypes" in joined and "industryActivities" in joined
    assert any(r.levelno == logging.ERROR and "compressibleTypes" in r.getMessage() for r in caplog.records)
    payload, status = AdminService(state=state).health_payload()
    assert status == 200 and payload["warnings"] == state.sde_warnings
    with pytest.raises(industry.SdeIndustryDataMissing):
        ore.ore_data()
    with pytest.raises(industry.SdeIndustryDataMissing):
        activities.industry_activities()


def test_startup_on_a_current_sde_loads_ore_data_and_activities(tmp_path):
    from flask_app.bootstrap import check_sde_industry_data

    sde_dir = tmp_path / "sde"
    sde_dir.mkdir()
    import test_sde_industry_tables as industry_tests

    industry_tests._write_fake_sde(sde_dir)
    db = tmp_path / "eve_sde.db"
    _importer().import_sde_to_sqlite(str(sde_dir), db_uri=f"sqlite:///{db}", tables_to_import=_config_tables())
    engine = create_engine(f"sqlite:///{db}")
    try:
        industry.reset_industry_data()
        ore.reset_ore_data()
        activities.reset_industry_activities()
        state = _state(engine)
        check_sde_industry_data(state)
        assert state.sde_warnings == []
        assert ore.ore_data() == fakes.fake_ore_data()
        assert activities.industry_activities() == fakes.fake_industry_activities()
    finally:
        engine.dispose()


# --- the local SDE ---------------------------------------------------------------------

_LOCAL_SDE = os.path.join(_ROOT, "database", "eve_sde.db")


def _local_conn_or_skip(tables):
    if not os.path.exists(_LOCAL_SDE):
        pytest.skip("local SDE database/eve_sde.db not present")
    conn = sqlite3.connect(f"file:{_LOCAL_SDE}?mode=ro", uri=True)
    present = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    missing = [t for t in tables if t not in present]
    if missing:
        conn.close()
        pytest.skip(f"local SDE lacks {', '.join(missing)}: re-run scripts/import_sde.py --download --import --force")
    return conn


def _english_type_names(conn) -> dict[int, str]:
    return {tid: json.loads(name).get("en") for tid, name in conn.execute("SELECT id, name FROM types") if name}


def test_the_local_compressible_types_have_ccps_shape():
    """One row per base type, each with its own compressed type, none of which
    is itself a base or a missing type."""
    conn = _local_conn_or_skip(("compressibleTypes", "types"))
    with conn:
        count, distinct_ids, distinct_compressed, nulls = conn.execute(
            'SELECT count(*), count(DISTINCT id), count(DISTINCT "compressedTypeID"), '
            'sum("compressedTypeID" IS NULL) FROM "compressibleTypes"').fetchone()
        both = conn.execute('SELECT count(*) FROM "compressibleTypes" WHERE "compressedTypeID" IN '
                            '(SELECT id FROM "compressibleTypes")').fetchone()[0]
        unknown = conn.execute('SELECT count(*) FROM "compressibleTypes" c WHERE c."compressedTypeID" NOT IN '
                               '(SELECT id FROM types) OR c.id NOT IN (SELECT id FROM types)').fetchone()[0]
    assert count > 0 and count == distinct_ids == distinct_compressed
    assert (nulls, both, unknown) == (0, 0, 0)


def test_the_local_ore_skills_are_ccps():
    data = fakes.local_ore_data_or_skip()
    conn = _local_conn_or_skip(("types",))
    with conn:
        names = _english_type_names(conn)

    def skill_name(type_id):
        skill = data.reprocessing_skill(type_id)
        return names.get(skill) if skill is not None else None

    assert skill_name(1230) == "Simple Ore Processing"            # Veldspar
    assert data.reprocessing_skill(1230) == 60377
    assert data.compressed_form_of(1230) == 62516 and data.base_of(62516) == 1230
    assert skill_name(62516) == "Simple Ore Processing"           # Compressed Veldspar
    assert skill_name(22) == "Complex Ore Processing"             # Arkonor (the old map said Variegated)
    assert skill_name(1229) == "Variegated Ore Processing"        # Gneiss (the old map said Complex)
    assert skill_name(45501) == "Uncommon Moon Ore Processing"    # Chromite (the old map said Common)
    assert skill_name(74533) == "Complex Ore Processing"          # Ducinium (the old map named a skill that does not exist)
    assert not data.is_compressed(28367)                          # Batch Compressed Arkonor
    assert skill_name(28367) == "Complex Ore Processing"          # its own attribute 790


def test_every_local_compressed_ore_with_a_skill_has_its_own_790_equal_to_its_bases():
    """Where the base has a reprocessing skill, the compressed form has its
    own attribute 790 and it is the base's (so the fallback to the base is
    not needed in this build); where neither has one (gas), neither does."""
    data = fakes.local_ore_data_or_skip()
    with_skill = {c: b for c, b in data.base_by_compressed.items() if data.skill_by_type.get(b) is not None}
    assert with_skill
    for compressed, base in with_skill.items():
        assert data.skill_by_type.get(compressed) == data.skill_by_type[base], compressed
    for compressed, base in data.base_by_compressed.items():
        if base not in with_skill.values():
            assert data.skill_by_type.get(compressed) is None, compressed


def test_every_local_ore_the_calculator_reprocesses_has_a_skill():
    data = fakes.local_ore_data_or_skip()
    conn = _local_conn_or_skip(("types", "groups", "typeMaterials"))
    with conn:
        ore_ids = [r[0] for r in conn.execute(
            "SELECT t.id FROM types t JOIN groups g ON g.id = t.groupID JOIN typeMaterials m ON m.id = t.id "
            "WHERE t.published = 1 AND g.published = 1 AND g.categoryID = 25")]
    assert ore_ids
    assert [t for t in ore_ids if data.reprocessing_skill(t) is None] == []


def test_every_local_activity_has_a_label_and_the_app_ids_are_in_the_sde():
    data = fakes.local_industry_activities_or_skip()
    assert data.names and all(data.labels[a] and not data.labels[a].startswith("Unknown") for a in data.names)
    app_ids = {domain_activities.ACTIVITY_MANUFACTURING, *domain_activities.LAB_ACTIVITY_IDS,
               *domain_activities.REACTION_ACTIVITY_IDS}
    assert app_ids <= set(data.names)
    assert set(domain_activities.DISPLAY_LABEL_OVERRIDES) <= set(data.names)


def test_the_local_sde_names_differ_from_todays_labels_only_where_reported():
    data = fakes.local_industry_activities_or_skip()
    assert {a: (data.names[a], data.labels[a]) for a in data.names if data.names[a] != data.labels[a]} == {
        3: ("Time Efficiency Research", "TE Research"),
        4: ("Material Efficiency Research", "ME Research"),
        9: ("Reactions", "Reaction"),
    }
