"""Packaged volumes come from the SDE's types.packagedVolume (SDE update).

The SDE (build 3607705 on) ships packagedVolume for every type that has a
volume; our own database/data/static_data/repackaged_volumes.json (and its
group-level fallback) was wrong for several groups and is gone. A packaged
(non-singleton) item takes packagedVolume, an assembled one its volume.

Fake SDE rows only; the local-SDE checks read database/eve_sde.db read-only
and skip when it (or the column) is absent.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
from types import SimpleNamespace

import pytest

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LOCAL_SDE = os.path.join(_ROOT, "database", "eve_sde.db")


def _importer():
    spec = importlib.util.spec_from_file_location("import_sde_under_test", os.path.join(_ROOT, "scripts", "import_sde.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- the asset volume rule ----------------------------------------------------------

def _type_row(volume, packaged):
    return SimpleNamespace(volume=volume, packagedVolume=packaged)


def test_a_packaged_item_takes_the_sde_packaged_volume():
    from eve_online_industry_tracker.infrastructure.sde.types import asset_volumes

    assert asset_volumes(_type_row(118000.0, 10000.0), is_singleton=False) == (118000.0, 10000.0, 10000.0)


def test_an_assembled_item_takes_its_volume():
    from eve_online_industry_tracker.infrastructure.sde.types import asset_volumes

    assert asset_volumes(_type_row(118000.0, 10000.0), is_singleton=True) == (118000.0, 10000.0, 118000.0)


def test_a_type_without_a_packaged_volume_takes_its_volume():
    from eve_online_industry_tracker.infrastructure.sde.types import asset_volumes

    assert asset_volumes(_type_row(5.0, None), is_singleton=False) == (5.0, None, 5.0)


def test_an_unknown_type_has_no_volume():
    from eve_online_industry_tracker.infrastructure.sde.types import asset_volumes

    assert asset_volumes(None, is_singleton=False) == (0.0, None, 0.0)


# --- the ORM and get_type_data -------------------------------------------------------

def test_the_orm_reads_packaged_volume_and_no_own_column():
    from eve_online_industry_tracker.infrastructure.models import Groups, Types

    assert "packagedVolume" in Types.__table__.columns
    assert "repackaged_volume" not in Types.__table__.columns
    assert "repackaged_volume" not in Groups.__table__.columns


def test_get_type_data_reports_the_sde_packaged_volume(sde_engine):
    from sqlalchemy.orm import Session

    from eve_online_industry_tracker.infrastructure.models import Groups, Types
    from eve_online_industry_tracker.infrastructure.sde.types import get_type_data

    with Session(sde_engine) as session:
        session.add(Groups(id=9001, categoryID=6, name={"en": "Fake Cruiser Group"}))
        session.add(Types(id=900001, groupID=9001, name={"en": "Fake Cruiser"}, volume=118000.0,
                          packagedVolume=10000.0))
        session.commit()
        data = get_type_data(session, "en", [900001])[900001]
    assert data["volume"] == 118000.0
    assert data["repackaged_volume"] == 10000.0
    assert "group_repackaged_volume" not in data


# --- the importer ---------------------------------------------------------------------

def test_the_importer_has_no_own_volume_file_any_more():
    importer = _importer()
    assert not hasattr(importer, "load_repackaged_volumes")
    with open(os.path.join(_ROOT, "config", "import_sde.json"), encoding="utf-8") as fh:
        assert "REPACKAGED_JSON_PATH" not in json.load(fh)
    assert not os.path.exists(os.path.join(_ROOT, "database", "data", "static_data", "repackaged_volumes.json"))


def test_the_importer_stores_packaged_volume_as_the_sde_has_it(tmp_path):
    sde_dir = tmp_path / "sde"
    sde_dir.mkdir()
    (sde_dir / "types.yaml").write_text(
        "900001:\n  groupID: 9001\n  name:\n    en: Fake Cruiser\n  packagedVolume: 10000.0\n  volume: 118000.0\n"
        "900002:\n  groupID: 9002\n  name:\n    en: Fake Ore\n  packagedVolume: 0.1\n  volume: 0.1\n",
        encoding="utf-8")
    (sde_dir / "groups.yaml").write_text("9001:\n  categoryID: 6\n  name:\n    en: Fake Cruiser Group\n",
                                         encoding="utf-8")
    db = tmp_path / "sde.db"
    _importer().import_sde_to_sqlite(str(sde_dir), db_uri=f"sqlite:///{db}", tables_to_import=["types", "groups"])
    with sqlite3.connect(db) as conn:
        type_columns = {r[1] for r in conn.execute("PRAGMA table_info(types)")}
        group_columns = {r[1] for r in conn.execute("PRAGMA table_info(groups)")}
        rows = dict(conn.execute("SELECT id, packagedVolume FROM types").fetchall())
    assert "repackaged_volume" not in type_columns and "repackaged_volume" not in group_columns
    assert rows == {900001: 10000.0, 900002: 0.1}


# --- the local SDE ------------------------------------------------------------------

def _local_sde_with_packaged_volume():
    if not os.path.exists(_LOCAL_SDE):
        pytest.skip("local SDE database/eve_sde.db not present")
    conn = sqlite3.connect(f"file:{_LOCAL_SDE}?mode=ro", uri=True)
    if "packagedVolume" not in {r[1] for r in conn.execute("PRAGMA table_info(types)")}:
        conn.close()
        pytest.skip("local SDE predates types.packagedVolume: re-run scripts/import_sde.py")
    return conn


def test_the_local_sde_has_a_packaged_volume_wherever_it_has_a_volume():
    with _local_sde_with_packaged_volume() as conn:
        (volume_only,) = conn.execute(
            "SELECT count(*) FROM types WHERE volume IS NOT NULL AND packagedVolume IS NULL").fetchone()
        (larger,) = conn.execute("SELECT count(*) FROM types WHERE packagedVolume > volume").fetchone()
    assert volume_only == 0
    assert larger == 0


@pytest.mark.parametrize("group_id, packaged", [
    (358, 10000.0),       # Heavy Assault Cruiser (our file said 5000)
    (832, 10000.0),       # Logistics (our file said 2500)
    (1538, 1300000.0),    # Force Auxiliary (our file said 130,000,000)
    (4594, 1300000.0),    # Lancer Dreadnought (our file said 130,000,000)
])
def test_the_groups_our_file_had_wrong_take_the_sde_value(group_id, packaged):
    with _local_sde_with_packaged_volume() as conn:
        values = {r[0] for r in conn.execute(
            "SELECT packagedVolume FROM types WHERE groupID = ? AND published = 1", (group_id,))}
    assert values == {packaged}


def test_the_porpoise_takes_the_sde_value():
    """Type 42244 Porpoise (Industrial Command Ship): our file said 500,000."""
    with _local_sde_with_packaged_volume() as conn:
        (packaged,) = conn.execute("SELECT packagedVolume FROM types WHERE id = 42244").fetchone()
    assert packaged == 50000.0
