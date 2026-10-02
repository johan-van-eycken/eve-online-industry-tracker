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
