"""The one job-cost model (job_cost.py), one test per activity.

Each test rebuilds EVE's relationship from fake values: job value x index x
(1 - structure reduction) + job value x (facility tax + the activity's SCC),
with the activity's own job value. The relationship was fitted on real ESI
jobs (Task 8b); the numbers here are invented.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.industry.job_cost import (  # noqa: E402
    JobSurchargeRates,
    job_install_cost,
    research_level_multiplier,
)
from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402

# Fake facility: 3 % role bonus, 1 % facility tax, default SCC rates.
_RATES = JobSurchargeRates(facility_tax=0.01, scc=0.04, research_scc=0.02)
_REDUCTION = 0.03


def _fee(activity: str, job_value: float, index: float) -> float:
    total = job_install_cost(
        activity=activity, process_value=job_value, system_index=index,
        structure_cost_reduction=_REDUCTION, surcharge_rates=_RATES,
    )["total_job_cost"]
    assert total is not None
    return total


def _service() -> IndustryService:
    return IndustryService(state=type("S", (), {"admin_settings": None})())


def test_manufacturing_charges_the_surcharge_on_the_material_eiv() -> None:
    # Fake: 4 runs of materials worth 2,500 per run, index 0.012.
    eiv = 4 * 2_500.0
    assert _fee("manufacturing", eiv, 0.012) == pytest.approx(eiv * (0.012 * 0.97 + 0.05))
    # The old form (surcharge on the index cost) was far lower.
    assert _fee("manufacturing", eiv, 0.012) > eiv * 0.012 * 0.97 * 1.05 * 3


def test_reaction_uses_the_manufacturing_form() -> None:
    eiv = 7_000.0
    assert _fee("reaction", eiv, 0.02) == pytest.approx(_fee("manufacturing", eiv, 0.02))


def test_copying_charges_on_two_percent_of_the_eiv_per_copied_run() -> None:
    # Fake: 3 copies x 5 runs of a blueprint whose materials are worth 8,000.
    job_value = 0.02 * 8_000.0 * 3 * 5
    assert _fee("copying", job_value, 0.015) == pytest.approx(job_value * (0.015 * 0.97 + 0.05))


def test_invention_charges_on_two_percent_of_the_invented_blueprints_eiv() -> None:
    # Fake: 6 attempts at a T2 blueprint whose materials are worth 90,000;
    # the datacores' value plays no part.
    job_value = 0.02 * 90_000.0 * 6
    assert _fee("invention", job_value, 0.009) == pytest.approx(job_value * (0.009 * 0.97 + 0.05))


@pytest.mark.parametrize("activity", ["research_material", "research_time"])
def test_research_charges_the_research_scc_on_the_level_scaled_job_value(activity: str) -> None:
    # Fake: materials worth 12,000, level 3 -> 7, index 0.004.
    service = _service()
    job_value = service._research_process_value(12_000.0, 7) - service._research_process_value(12_000.0, 3)
    assert job_value == pytest.approx(12_000.0 * 0.02 * (19000 - 595) / 105)
    assert job_value == pytest.approx(12_000.0 * 0.02 * research_level_multiplier(3, 7))
    assert _fee(activity, job_value, 0.004) == pytest.approx(job_value * (0.004 * 0.97 + 0.01 + 0.02))


def test_research_fee_is_rank_independent_and_linear_in_levels() -> None:
    """Any span from a to b costs [b] - [a]; the rank never enters."""
    service = _service()
    full = service._research_process_value(1_000.0, 10)
    assert full == pytest.approx(1_000.0 * 0.02 * 256000 / 105)
    assert research_level_multiplier(0, 1) == pytest.approx(1.0)
    assert research_level_multiplier(9, 10) + research_level_multiplier(0, 9) == pytest.approx(research_level_multiplier(0, 10))
    assert research_level_multiplier(5, 5) == 0.0
