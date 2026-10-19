"""Round H2.1: a corrupt or non-object admin_settings.json, or a non-object
category, does not silently mean "all defaults".

- The load problem is logged as an ERROR and listed (prefixed ERROR) by
  problems() (/health) and notices() (the Admin Settings page).
- The first save backs the unreadable file up before writing, and says so
  (in notices()/problems()) for the rest of the process.

Fake values throughout.
"""
from __future__ import annotations

import json
import logging

from eve_online_industry_tracker.config.admin_settings import AdminSettingsManager


def _write(tmp_path, text: str):
    path = tmp_path / "admin_settings.json"
    path.write_text(text, encoding="utf-8")
    return path


def _backups(tmp_path):
    return sorted(p for p in tmp_path.iterdir() if ".bak" in p.name)


def test_corrupt_file_is_an_error_in_problems_and_notices(tmp_path, caplog):
    path = _write(tmp_path, "{not json")
    with caplog.at_level(logging.ERROR):
        mgr = AdminSettingsManager(path)
    assert any(r.levelno == logging.ERROR and "could not be read" in r.getMessage() for r in caplog.records)
    problems = mgr.problems()
    assert problems and problems[0].startswith("ERROR")
    assert str(path) in problems[0]
    assert any("could not be read" in n for n in mgr.notices())


def test_non_object_file_is_an_error(tmp_path):
    path = _write(tmp_path, json.dumps([1, 2, 3]))
    mgr = AdminSettingsManager(path)
    assert mgr.problems() and mgr.problems()[0].startswith("ERROR")
    assert "not a JSON object" in mgr.problems()[0]


def test_non_object_category_is_an_error(tmp_path):
    path = _write(tmp_path, json.dumps({"reprocessing": "athanor"}))
    mgr = AdminSettingsManager(path)
    problems = mgr.problems()
    assert any(p.startswith("ERROR") and "reprocessing" in p for p in problems)


def test_first_save_backs_up_the_unreadable_file_and_says_so(tmp_path):
    path = _write(tmp_path, "{not json")
    mgr = AdminSettingsManager(path)
    mgr.set("reprocessing", "facility_type", "athanor")
    backups = _backups(tmp_path)
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == "{not json"
    assert json.loads(path.read_text(encoding="utf-8"))["reprocessing"]["facility_type"] == "athanor"
    # The load problem is gone, the backup is named.
    assert not any("could not be read" in p for p in mgr.problems())
    assert any(str(backups[0]) in n for n in mgr.notices())
    # A second save does not back up again (the file is the app's own now).
    mgr.set("reprocessing", "facility_type", "tatara")
    assert len(_backups(tmp_path)) == 1


def test_reset_and_bulk_save_back_up_too(tmp_path):
    path = _write(tmp_path, json.dumps({"reprocessing": 5}))
    mgr = AdminSettingsManager(path)
    mgr.reset_to_defaults()
    assert len(_backups(tmp_path)) == 1
    path2 = tmp_path / "other" / "admin_settings.json"
    path2.parent.mkdir()
    path2.write_text("[]", encoding="utf-8")
    mgr2 = AdminSettingsManager(path2)
    mgr2.set_bulk({"reprocessing": {"facility_type": "athanor"}})
    assert len([p for p in path2.parent.iterdir() if ".bak" in p.name]) == 1


def test_a_readable_file_has_no_load_problem_and_no_backup(tmp_path):
    path = _write(tmp_path, json.dumps({"reprocessing": {"facility_type": "athanor"}}))
    mgr = AdminSettingsManager(path)
    assert not any(p.startswith("ERROR") for p in mgr.problems())
    mgr.set("reprocessing", "facility_type", "tatara")
    assert _backups(tmp_path) == []


def test_missing_file_is_not_a_problem(tmp_path):
    mgr = AdminSettingsManager(tmp_path / "admin_settings.json")
    assert mgr.problems() == []
