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


# --- fix round 1: an unknown input makes the fee unknown, with its reason ---

from eve_online_industry_tracker.application.industry.job_cost import profile_facility_tax  # noqa: E402


def test_a_profile_facility_tax_is_used_as_stored() -> None:
    assert profile_facility_tax({"facility_tax": 0.015}, scc=0.04) == (0.015, None)


def test_a_legacy_profile_derives_its_facility_tax_from_the_modifier() -> None:
    tax, reason = profile_facility_tax({"installation_cost_modifier": 0.05}, scc=0.04)
    assert tax == pytest.approx(0.01) and reason is None
    # The profile's own stored SCC is what its modifier was built with.
    tax, reason = profile_facility_tax({"installation_cost_modifier": 0.05, "scc_surcharge": 0.03}, scc=0.04)
    assert tax == pytest.approx(0.02) and reason is None


@pytest.mark.parametrize("profile", [
    None,
    {},
    {"installation_cost_modifier": None, "facility_tax": None},
    {"installation_cost_modifier": 0.01},  # below SCC: inconsistent
    {"facility_tax": -0.01},
    {"facility_tax": "n/a"},
])
def test_an_underivable_facility_tax_is_unknown_with_a_reason(profile) -> None:
    tax, reason = profile_facility_tax(profile, scc=0.04)
    assert tax is None
    assert reason


def test_an_unknown_facility_tax_makes_every_fee_unknown_with_its_reason() -> None:
    rates = JobSurchargeRates(facility_tax=None, unknown_reason="no facility tax")
    result = job_install_cost(
        activity="manufacturing", process_value=1_000.0, system_index=0.02,
        structure_cost_reduction=0.0, surcharge_rates=rates,
    )
    assert result["total_job_cost"] is None
    assert result["fee_unknown_reason"] == "no facility tax"


@pytest.mark.parametrize("value, index, needle", [
    (None, 0.02, "EIV"),
    (0.0, 0.02, "EIV"),
    (1_000.0, 0.0, "cost index"),
])
def test_an_unknown_value_or_index_names_its_cause(value, index, needle) -> None:
    result = job_install_cost(
        activity="copying", process_value=value, system_index=index,
        structure_cost_reduction=0.0, surcharge_rates=_RATES,
    )
    assert result["total_job_cost"] is None
    assert needle in result["fee_unknown_reason"]


def test_a_priced_fee_has_no_unknown_reason() -> None:
    result = job_install_cost(
        activity="copying", process_value=10.0, system_index=0.02,
        structure_cost_reduction=0.0, surcharge_rates=_RATES,
    )
    assert result["fee_unknown_reason"] is None


# --- fix round 1: the profile page shows the producer's two totals ---

from eve_online_industry_tracker.application.industry.job_cost import job_fee_rates_summary  # noqa: E402


def test_the_profile_summary_shows_both_totals_from_the_admin_rates():
    summary = job_fee_rates_summary({"facility_tax": 0.01, "scc_surcharge": 0.04}, scc=0.05, research_scc=0.03)
    assert summary == (
        "Facility tax 1.00% | Manufacturing / reaction / copy / invention total 6.00% (SCC 5.00%) | "
        "ME/TE research total 4.00% (SCC 3.00%)"
    )


def test_the_profile_summary_names_an_unknown_facility_tax():
    summary = job_fee_rates_summary({"installation_cost_modifier": 0.01}, scc=0.04, research_scc=0.02)
    assert summary.startswith("Job fees unknown: ")


def test_the_scc_help_says_reactions_are_not_validated():
    from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA

    help_text = ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["scc_surcharge"]["help"]
    assert "reaction" in help_text and "not validated" in help_text
