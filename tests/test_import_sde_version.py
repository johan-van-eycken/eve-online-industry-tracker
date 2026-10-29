"""The importer records the build it imported, not the one online.

The extracted SDE says its own build in _sde.yaml (sde: buildNumber,
releaseDate). The importer records that build; the online latest version is
only a fallback, and a WARNING says when the extracted files are older.
--import --force works offline. Fake SDE files only.
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys

import pytest

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="module")
def importer():
    spec = importlib.util.spec_from_file_location("import_sde_version_under_test",
                                                  os.path.join(_ROOT, "scripts", "import_sde.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sde_dir(tmp_path, build=3607705, release="2026-10-12T11:08:38Z"):
    sde_dir = tmp_path / "tmp_sde"
    sde_dir.mkdir()
    (sde_dir / "_sde.yaml").write_text(f"sde:\n  buildNumber: {build}\n  releaseDate: '{release}'\n",
                                       encoding="utf-8")
    (sde_dir / "categories.yaml").write_text("6:\n  name:\n    en: Ship\n", encoding="utf-8")
    return sde_dir


def test_the_extracted_version_is_read_from_sde_yaml(importer, tmp_path):
    assert importer.read_extracted_sde_version(str(_sde_dir(tmp_path))) == {
        "buildNumber": 3607705, "releaseDate": "2026-10-12T11:08:38Z"}


def test_no_sde_yaml_gives_no_extracted_version(importer, tmp_path):
    assert importer.read_extracted_sde_version(str(tmp_path)) is None


@pytest.mark.parametrize("extracted, latest, recorded, warns", [
    ({"buildNumber": 3607705, "releaseDate": "a"}, {"buildNumber": 3607705, "releaseDate": "a"}, 3607705, False),
    ({"buildNumber": 3607705, "releaseDate": "a"}, {"buildNumber": 3619879, "releaseDate": "b"}, 3607705, True),
    ({"buildNumber": 3607705, "releaseDate": "a"}, None, 3607705, False),
    (None, {"buildNumber": 3619879, "releaseDate": "b"}, 3619879, True),
    (None, None, None, True),
])
def test_the_recorded_version_is_the_extracted_one(importer, extracted, latest, recorded, warns):
    version, warning = importer.version_to_record(extracted, latest)
    assert (version or {}).get("buildNumber") == recorded
    assert bool(warning) == warns


def test_an_older_extracted_sde_says_to_add_download(importer):
    _, warning = importer.version_to_record({"buildNumber": 3607705, "releaseDate": "a"},
                                            {"buildNumber": 3619879, "releaseDate": "b"})
    assert "extracted SDE is older than the latest; add --download" in warning


def _run_main(importer, monkeypatch, *args):
    monkeypatch.chdir(_ROOT)
    monkeypatch.setattr(sys, "argv", ["import_sde.py", *args])
    importer.main()


def test_import_force_works_offline_and_records_the_extracted_build(importer, tmp_path, monkeypatch, capsys):
    sde_dir = _sde_dir(tmp_path)
    db = tmp_path / "sde.db"
    monkeypatch.setattr(importer, "get_latest_sde_version", lambda url: None)  # offline
    _run_main(importer, monkeypatch, "--import", "--force", "--tables", "categories",
              "--db", f"sqlite:///{db}", "--tmp", str(sde_dir))
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT build_number, release_date FROM sde_version WHERE is_current = 1").fetchall() == [
            (3607705, "2026-10-12T11:08:38Z")]
        assert conn.execute("SELECT count(*) FROM categories").fetchone() == (1,)


def test_import_records_the_extracted_build_when_a_newer_one_is_online(importer, tmp_path, monkeypatch, capsys):
    sde_dir = _sde_dir(tmp_path)
    db = tmp_path / "sde.db"
    monkeypatch.setattr(importer, "get_latest_sde_version",
                        lambda url: {"buildNumber": 3619879, "releaseDate": "2026-10-15T11:08:03Z"})
    _run_main(importer, monkeypatch, "--import", "--force", "--tables", "categories",
              "--db", f"sqlite:///{db}", "--tmp", str(sde_dir))
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT build_number FROM sde_version WHERE is_current = 1").fetchall() == [(3607705,)]
    assert "extracted SDE is older than the latest; add --download" in capsys.readouterr().out


def test_the_up_to_date_check_still_needs_the_online_version(importer, tmp_path, monkeypatch, capsys):
    sde_dir = _sde_dir(tmp_path)
    db = tmp_path / "sde.db"
    monkeypatch.setattr(importer, "get_latest_sde_version", lambda url: None)
    _run_main(importer, monkeypatch, "--import", "--tables", "categories", "--db", f"sqlite:///{db}",
              "--tmp", str(sde_dir))
    assert "Could not fetch latest SDE version" in capsys.readouterr().out
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM sde_version").fetchone() == (0,)


# --- a partial import never looks current ------------------------------------------

def test_the_import_returns_the_tables_that_failed(importer, tmp_path):
    sde_dir = _sde_dir(tmp_path)
    (sde_dir / "groups.yaml").write_text("not: [valid", encoding="utf-8")  # unparsable YAML
    failed = importer.import_sde_to_sqlite(str(sde_dir), db_uri=f"sqlite:///{tmp_path / 'sde.db'}",
                                           tables_to_import=["categories", "groups"])
    assert failed == ["groups"]


def test_a_failed_table_records_no_version_keeps_the_files_and_exits_non_zero(importer, tmp_path, monkeypatch,
                                                                               capsys):
    sde_dir = _sde_dir(tmp_path)
    (sde_dir / "groups.yaml").write_text("not: [valid", encoding="utf-8")
    db = tmp_path / "sde.db"
    monkeypatch.setattr(importer, "get_latest_sde_version", lambda url: None)
    with pytest.raises(SystemExit) as exit_info:
        _run_main(importer, monkeypatch, "--import", "--cleanup", "--force", "--tables", "categories", "groups",
                  "--db", f"sqlite:///{db}", "--tmp", str(sde_dir))
    assert exit_info.value.code not in (0, None)
    out = capsys.readouterr().out
    assert "groups" in out and "not recorded" in out and "re-run" in out
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM sde_version").fetchone() == (0,)
        assert conn.execute("SELECT count(*) FROM categories").fetchone() == (1,)  # imported tables stay
    assert (sde_dir / "_sde.yaml").exists()  # --cleanup skipped: the files stay for the retry
