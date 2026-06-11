"""Property-based tests for IndustryService pure calculation methods.

Uses Hypothesis to verify algebraic invariants and boundary conditions
across the manufacturing cost, time, material, and pricing calculations.
"""

from __future__ import annotations

import math
import os
import sys

from hypothesis import given, assume, settings
from hypothesis import strategies as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

positive_floats = st.floats(min_value=0.01, max_value=1e12, allow_nan=False, allow_infinity=False)
non_negative_floats = st.floats(min_value=0.0, max_value=1e12, allow_nan=False, allow_infinity=False)
fraction_floats = st.floats(min_value=0.0, max_value=0.99, allow_nan=False, allow_infinity=False)
percentage_floats = st.floats(min_value=0.0, max_value=99.0, allow_nan=False, allow_infinity=False)
skill_levels = st.integers(min_value=0, max_value=5)
research_levels = st.integers(min_value=1, max_value=10)
material_quantities = st.integers(min_value=1, max_value=100_000)
run_counts = st.integers(min_value=1, max_value=1000)


# ---------------------------------------------------------------------------
# _normalize_fraction
# ---------------------------------------------------------------------------

class TestNormalizeFraction:
    @given(st.floats(min_value=0.0, max_value=0.99, allow_nan=False, allow_infinity=False))
    def test_fractions_pass_through(self, value: float):
        """Values already in [0, 1) should be returned as-is."""
        result = IndustryService._normalize_fraction(value)
        assert 0.0 <= result <= 0.99
        assert math.isclose(result, value, abs_tol=1e-9)

    @given(st.floats(min_value=1.0, max_value=99.0, allow_nan=False, allow_infinity=False))
    def test_percentages_become_fractions(self, value: float):
        """Values >= 1 should be divided by 100 until < 1."""
        result = IndustryService._normalize_fraction(value)
        assert 0.0 <= result <= 0.99
        assert result < 1.0

    @given(st.floats(min_value=-1e6, max_value=-0.01, allow_nan=False, allow_infinity=False))
    def test_negatives_become_positive(self, value: float):
        """Negative values should be abs'd then normalized."""
        result = IndustryService._normalize_fraction(value)
        assert result >= 0.0

    def test_none_returns_zero(self):
        assert IndustryService._normalize_fraction(None) == 0.0

    def test_non_numeric_returns_zero(self):
        assert IndustryService._normalize_fraction("not_a_number") == 0.0

    @given(percentage_floats)
    def test_output_bounded(self, value: float):
        """Output is always in [0.0, 0.99]."""
        result = IndustryService._normalize_fraction(value)
        assert 0.0 <= result <= 0.99


# ---------------------------------------------------------------------------
# _combine_reductions
# ---------------------------------------------------------------------------

class TestCombineReductions:
    def test_empty_list_returns_zero(self):
        assert IndustryService._combine_reductions([]) == 0.0

    @given(fraction_floats)
    def test_single_reduction_identity(self, reduction: float):
        """A single reduction should return that reduction (after normalization)."""
        result = IndustryService._combine_reductions([reduction])
        expected = IndustryService._normalize_fraction(reduction)
        assert math.isclose(result, expected, abs_tol=1e-9)

    @given(st.lists(fraction_floats, min_size=1, max_size=5))
    def test_output_bounded(self, reductions: list[float]):
        """Combined reductions must be in [0, 0.99]."""
        result = IndustryService._combine_reductions(reductions)
        assert 0.0 <= result <= 0.99

    @given(st.lists(fraction_floats, min_size=2, max_size=5))
    def test_monotonic_with_more_reductions(self, reductions: list[float]):
        """Adding more positive reductions should not decrease the combined result."""
        assume(all(r > 0 for r in reductions))
        for i in range(1, len(reductions)):
            partial = IndustryService._combine_reductions(reductions[:i])
            full = IndustryService._combine_reductions(reductions[:i + 1])
            assert full >= partial - 1e-9

    @given(st.lists(fraction_floats, min_size=1, max_size=5))
    def test_multiplier_formula(self, reductions: list[float]):
        """Verify the multiplier formula: 1 - prod(1 - r_i)."""
        normalized = [IndustryService._normalize_fraction(r) for r in reductions]
        multiplier = 1.0
        for r in normalized:
            if r > 0:
                multiplier *= (1.0 - r)
        expected = max(0.0, min(1.0 - multiplier, 0.99))
        result = IndustryService._combine_reductions(reductions)
        assert math.isclose(result, expected, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# _round_duration_seconds
# ---------------------------------------------------------------------------

class TestRoundDurationSeconds:
    @given(st.floats(min_value=0.01, max_value=1e9, allow_nan=False, allow_infinity=False))
    def test_positive_rounds_up(self, seconds: float):
        result = IndustryService._round_duration_seconds(seconds)
        assert result >= 1
        assert result == max(1, int(math.ceil(seconds)))

    @given(st.floats(min_value=-1e6, max_value=0.0, allow_nan=False, allow_infinity=False))
    def test_non_positive_returns_zero(self, seconds: float):
        assert IndustryService._round_duration_seconds(seconds) == 0

    @given(st.integers(min_value=1, max_value=1_000_000))
    def test_integer_input_unchanged(self, seconds: int):
        """Exact integers should stay the same."""
        assert IndustryService._round_duration_seconds(float(seconds)) == seconds


# ---------------------------------------------------------------------------
# _round_material_quantity
# ---------------------------------------------------------------------------

class TestRoundMaterialQuantity:
    @given(
        st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
        st.integers(min_value=1, max_value=100),
    )
    def test_rounds_up_with_minimum(self, raw: float, minimum: int):
        result = IndustryService._round_material_quantity(raw, minimum_quantity=minimum)
        assert result >= minimum
        assert result >= int(math.ceil(raw))

    @given(st.floats(min_value=-1e6, max_value=0.0, allow_nan=False, allow_infinity=False))
    def test_non_positive_returns_zero(self, raw: float):
        assert IndustryService._round_material_quantity(raw, minimum_quantity=1) == 0

    @given(st.integers(min_value=1, max_value=100_000), run_counts)
    def test_minimum_is_runs(self, quantity: int, runs: int):
        """In manufacturing, minimum_quantity=runs ensures at least 1 per run."""
        result = IndustryService._round_material_quantity(float(quantity), minimum_quantity=runs)
        assert result >= runs


# ---------------------------------------------------------------------------
# _skill_time_reduction
# ---------------------------------------------------------------------------

class TestSkillTimeReduction:
    @given(skill_levels, skill_levels)
    def test_manufacturing_bounded(self, industry: int, advanced: int):
        skills = {"Industry": industry, "Advanced Industry": advanced}
        result = IndustryService._skill_time_reduction(
            activity="manufacturing",
            skill_levels_by_name=skills,
        )
        assert 0.0 <= result <= 0.99

    @given(skill_levels, skill_levels)
    def test_copying_bounded(self, science: int, advanced: int):
        skills = {"Science": science, "Advanced Industry": advanced}
        result = IndustryService._skill_time_reduction(
            activity="copying",
            skill_levels_by_name=skills,
        )
        assert 0.0 <= result <= 0.99

    @given(skill_levels, skill_levels)
    def test_research_material_bounded(self, metallurgy: int, advanced: int):
        skills = {"Metallurgy": metallurgy, "Advanced Industry": advanced}
        result = IndustryService._skill_time_reduction(
            activity="research_material",
            skill_levels_by_name=skills,
        )
        assert 0.0 <= result <= 0.99

    def test_zero_skills_no_reduction(self):
        result = IndustryService._skill_time_reduction(
            activity="manufacturing",
            skill_levels_by_name={},
        )
        assert result == 0.0

    @given(skill_levels, skill_levels)
    def test_higher_skills_more_reduction(self, industry: int, advanced: int):
        """Higher skill levels should give equal or greater reduction."""
        assume(industry > 0 or advanced > 0)
        low = IndustryService._skill_time_reduction(
            activity="manufacturing",
            skill_levels_by_name={"Industry": 0, "Advanced Industry": 0},
        )
        high = IndustryService._skill_time_reduction(
            activity="manufacturing",
            skill_levels_by_name={"Industry": industry, "Advanced Industry": advanced},
        )
        assert high >= low

    @given(skill_levels, skill_levels)
    def test_manufacturing_formula(self, industry: int, advanced: int):
        """Verify manufacturing uses 4% per Industry level + 3% per Advanced Industry level."""
        skills = {"Industry": industry, "Advanced Industry": advanced}
        result = IndustryService._skill_time_reduction(
            activity="manufacturing",
            skill_levels_by_name=skills,
        )
        expected = IndustryService._combine_reductions([
            0.04 * industry,
            0.03 * advanced,
            IndustryService._manufacturing_required_skill_time_reduction([]),
        ])
        assert math.isclose(result, expected, abs_tol=1e-9)


# ---------------------------------------------------------------------------
# _job_cost_total
# ---------------------------------------------------------------------------

class TestJobCostTotal:
    @given(positive_floats, positive_floats, fraction_floats, non_negative_floats)
    def test_total_is_sum_of_parts(self, process_value: float, cost_index: float, reduction: float, surcharge: float):
        """total = discounted + surcharge_cost."""
        result = IndustryService._job_cost_total(
            process_value=process_value,
            cost_index=cost_index,
            cost_reduction=reduction,
            installation_surcharge=surcharge,
        )
        if result["total_job_cost"] is not None:
            expected = result["job_cost_before_surcharge"] + result["installation_surcharge"]
            assert math.isclose(result["total_job_cost"], expected, rel_tol=1e-9)

    @given(positive_floats, positive_floats, fraction_floats, non_negative_floats)
    def test_base_cost_formula(self, process_value: float, cost_index: float, reduction: float, surcharge: float):
        """base = process_value * cost_index."""
        result = IndustryService._job_cost_total(
            process_value=process_value,
            cost_index=cost_index,
            cost_reduction=reduction,
            installation_surcharge=surcharge,
        )
        if result["base_job_cost"] is not None:
            assert math.isclose(result["base_job_cost"], process_value * cost_index, rel_tol=1e-9)

    @given(positive_floats, positive_floats, fraction_floats, non_negative_floats)
    def test_discount_reduces_cost(self, process_value: float, cost_index: float, reduction: float, surcharge: float):
        """Discounted cost <= base cost."""
        result = IndustryService._job_cost_total(
            process_value=process_value,
            cost_index=cost_index,
            cost_reduction=reduction,
            installation_surcharge=surcharge,
        )
        if result["base_job_cost"] is not None:
            assert result["job_cost_before_surcharge"] <= result["base_job_cost"] + 1e-6

    def test_zero_process_value_returns_none(self):
        result = IndustryService._job_cost_total(
            process_value=0.0,
            cost_index=0.05,
            cost_reduction=0.0,
            installation_surcharge=0.0,
        )
        assert result["total_job_cost"] is None

    def test_none_process_value_returns_none(self):
        result = IndustryService._job_cost_total(
            process_value=None,
            cost_index=0.05,
            cost_reduction=0.0,
            installation_surcharge=0.0,
        )
        assert result["total_job_cost"] is None

    def test_zero_cost_index_returns_none(self):
        result = IndustryService._job_cost_total(
            process_value=1_000_000.0,
            cost_index=0.0,
            cost_reduction=0.0,
            installation_surcharge=0.0,
        )
        assert result["total_job_cost"] is None

    @given(positive_floats, positive_floats, non_negative_floats)
    def test_zero_reduction_no_discount(self, process_value: float, cost_index: float, surcharge: float):
        """With 0% cost reduction, discounted == base."""
        result = IndustryService._job_cost_total(
            process_value=process_value,
            cost_index=cost_index,
            cost_reduction=0.0,
            installation_surcharge=surcharge,
        )
        if result["base_job_cost"] is not None:
            assert math.isclose(
                result["job_cost_before_surcharge"],
                result["base_job_cost"],
                rel_tol=1e-9,
            )

    @given(positive_floats, positive_floats, fraction_floats)
    def test_zero_surcharge(self, process_value: float, cost_index: float, reduction: float):
        """With 0% surcharge, total == discounted."""
        result = IndustryService._job_cost_total(
            process_value=process_value,
            cost_index=cost_index,
            cost_reduction=reduction,
            installation_surcharge=0.0,
        )
        if result["total_job_cost"] is not None:
            assert math.isclose(
                result["total_job_cost"],
                result["job_cost_before_surcharge"],
                rel_tol=1e-9,
            )

    @given(positive_floats, positive_floats, fraction_floats, non_negative_floats)
    def test_all_values_non_negative(self, process_value: float, cost_index: float, reduction: float, surcharge: float):
        """All cost components must be >= 0."""
        result = IndustryService._job_cost_total(
            process_value=process_value,
            cost_index=cost_index,
            cost_reduction=reduction,
            installation_surcharge=surcharge,
        )
        for key in ("base_job_cost", "job_cost_before_surcharge", "installation_surcharge", "total_job_cost"):
            if result[key] is not None:
                assert result[key] >= 0.0


# ---------------------------------------------------------------------------
# _research_target_duration_seconds
# ---------------------------------------------------------------------------

class TestResearchTargetDuration:
    @given(st.integers(min_value=1, max_value=100_000), research_levels)
    def test_output_non_negative(self, base: int, level: int):
        result = IndustryService._research_target_duration_seconds(
            level_one_duration_seconds=base,
            target_level=level,
        )
        assert result >= 0

    @given(st.integers(min_value=1, max_value=100_000))
    def test_higher_levels_take_longer(self, base: int):
        """Research duration increases monotonically with target level."""
        durations = [
            IndustryService._research_target_duration_seconds(
                level_one_duration_seconds=base,
                target_level=level,
            )
            for level in range(1, 11)
        ]
        for i in range(1, len(durations)):
            assert durations[i] >= durations[i - 1]

    def test_zero_base_returns_zero(self):
        result = IndustryService._research_target_duration_seconds(
            level_one_duration_seconds=0,
            target_level=5,
        )
        assert result == 0

    def test_zero_level_returns_zero(self):
        result = IndustryService._research_target_duration_seconds(
            level_one_duration_seconds=105,
            target_level=0,
        )
        assert result == 0

    @given(st.integers(min_value=1, max_value=100_000))
    def test_scales_linearly_with_rank(self, base: int):
        """Duration should scale linearly with the base time (rank)."""
        d1 = IndustryService._research_target_duration_seconds(
            level_one_duration_seconds=base,
            target_level=5,
        )
        d2 = IndustryService._research_target_duration_seconds(
            level_one_duration_seconds=base * 2,
            target_level=5,
        )
        # Allow +1 tolerance due to ceiling rounding
        assert abs(d2 - d1 * 2) <= 2


# ---------------------------------------------------------------------------
# _resolve_eiv_pricing
# ---------------------------------------------------------------------------

class TestResolveEivPricing:
    def test_adjusted_price_preferred(self):
        """ESI adjusted price takes priority over average and SDE base."""
        price, source = IndustryService._resolve_eiv_pricing(
            type_id=34,
            type_payload={"base_price": 1.0},
            adjusted_price_map={34: {"adjusted_price": 5.0, "average_price": 3.0}},
        )
        assert price == 5.0
        assert source == "esi_adjusted_price"

    def test_average_price_fallback(self):
        price, source = IndustryService._resolve_eiv_pricing(
            type_id=34,
            type_payload={"base_price": 1.0},
            adjusted_price_map={34: {"adjusted_price": None, "average_price": 3.0}},
        )
        assert price == 3.0
        assert source == "esi_average_price"

    def test_base_price_fallback(self):
        price, source = IndustryService._resolve_eiv_pricing(
            type_id=34,
            type_payload={"base_price": 1.0},
            adjusted_price_map={},
        )
        assert price == 1.0
        assert source == "sde_base_price"

    def test_no_price_returns_none(self):
        price, source = IndustryService._resolve_eiv_pricing(
            type_id=34,
            type_payload={},
            adjusted_price_map={},
        )
        assert price is None
        assert source is None


# ---------------------------------------------------------------------------
# _sum_estimated_item_value
# ---------------------------------------------------------------------------

class TestSumEstimatedItemValue:
    @given(st.lists(
        st.tuples(
            st.integers(min_value=1, max_value=100_000),
            st.integers(min_value=1, max_value=10_000),
            positive_floats,
        ),
        min_size=1,
        max_size=10,
        unique_by=lambda t: t[0],  # unique type_ids
    ))
    def test_sum_is_quantity_times_price(self, items: list[tuple[int, int, float]]):
        """Total should be sum of (unit_price * quantity)."""
        entries = [{"type_id": tid, "quantity": qty} for tid, qty, _ in items]
        price_map = {tid: {"adjusted_price": price} for tid, _, price in items}
        total, count = IndustryService._sum_estimated_item_value(
            entries, quantity_key="quantity", adjusted_price_map=price_map,
        )
        assert count == len(items)
        expected = sum(price * qty for _, qty, price in items)
        assert math.isclose(total, expected, rel_tol=1e-6)

    def test_empty_returns_none(self):
        total, count = IndustryService._sum_estimated_item_value(
            [], quantity_key="quantity", adjusted_price_map={},
        )
        assert total is None
        assert count == 0


# ---------------------------------------------------------------------------
# _profile_base_reduction
# ---------------------------------------------------------------------------

class TestProfileBaseReduction:
    @given(fraction_floats)
    def test_material_reduction_bounded(self, bonus: float):
        profile = {"material_efficiency_bonus": bonus}
        result = IndustryService._profile_base_reduction(
            profile_payload=profile, activity="manufacturing", metric="material",
        )
        assert 0.0 <= result <= 0.99

    @given(fraction_floats)
    def test_time_reduction_bounded(self, bonus: float):
        profile = {"time_efficiency_bonus": bonus}
        result = IndustryService._profile_base_reduction(
            profile_payload=profile, activity="manufacturing", metric="time",
        )
        assert 0.0 <= result <= 0.99

    @given(fraction_floats)
    def test_cost_reduction_bounded(self, bonus: float):
        profile = {"facility_cost_bonus": bonus}
        result = IndustryService._profile_base_reduction(
            profile_payload=profile, activity="manufacturing", metric="cost",
        )
        assert 0.0 <= result <= 0.99

    def test_none_profile_returns_zero(self):
        assert IndustryService._profile_base_reduction(
            profile_payload=None, activity="manufacturing", metric="material",
        ) == 0.0

    def test_material_only_for_manufacturing_reaction(self):
        """Material reduction only applies to manufacturing/reaction activities."""
        profile = {"material_efficiency_bonus": 0.05}
        assert IndustryService._profile_base_reduction(
            profile_payload=profile, activity="copying", metric="material",
        ) == 0.0


# ---------------------------------------------------------------------------
# _profile_installation_surcharge
# ---------------------------------------------------------------------------

class TestProfileInstallationSurcharge:
    @given(non_negative_floats)
    def test_non_negative(self, value: float):
        profile = {"installation_cost_modifier": value}
        result = IndustryService._profile_installation_surcharge(profile)
        assert result >= 0.0

    def test_none_profile_returns_zero(self):
        assert IndustryService._profile_installation_surcharge(None) == 0.0

    def test_negative_clamped_to_zero(self):
        assert IndustryService._profile_installation_surcharge({"installation_cost_modifier": -5.0}) == 0.0


# ---------------------------------------------------------------------------
# _system_cost_index
# ---------------------------------------------------------------------------

class TestSystemCostIndex:
    @given(non_negative_floats)
    def test_returns_value_from_matching_activity(self, cost_index: float):
        profile = {
            "system_cost_indices": [
                {"activity": "manufacturing", "cost_index": cost_index},
            ],
        }
        result = IndustryService._system_cost_index(
            profile_payload=profile, activity="manufacturing",
        )
        assert result == max(0.0, cost_index)

    def test_aliases_match(self):
        """research_material should match researching_material_efficiency."""
        profile = {
            "system_cost_indices": [
                {"activity": "researching_material_efficiency", "cost_index": 0.042},
            ],
        }
        result = IndustryService._system_cost_index(
            profile_payload=profile, activity="research_material",
        )
        assert math.isclose(result, 0.042, abs_tol=1e-9)

    def test_missing_activity_returns_zero(self):
        profile = {
            "system_cost_indices": [
                {"activity": "copying", "cost_index": 0.01},
            ],
        }
        assert IndustryService._system_cost_index(
            profile_payload=profile, activity="manufacturing",
        ) == 0.0

    def test_none_profile_returns_zero(self):
        assert IndustryService._system_cost_index(
            profile_payload=None, activity="manufacturing",
        ) == 0.0


# ---------------------------------------------------------------------------
# _implant_reduction
# ---------------------------------------------------------------------------

class TestImplantReduction:
    @given(fraction_floats)
    def test_matching_implant_applied(self, value: float):
        payload = {
            "implants": [
                {
                    "modifiers": [
                        {"metric": "time", "activity": "manufacturing", "value": value},
                    ],
                },
            ],
        }
        result = IndustryService._implant_reduction(
            character_modifier_payload=payload, activity="manufacturing", metric="time",
        )
        assert 0.0 <= result <= 0.99

    def test_non_matching_metric_ignored(self):
        payload = {
            "implants": [
                {
                    "modifiers": [
                        {"metric": "cost", "activity": "manufacturing", "value": 0.04},
                    ],
                },
            ],
        }
        result = IndustryService._implant_reduction(
            character_modifier_payload=payload, activity="manufacturing", metric="time",
        )
        assert result == 0.0

    def test_none_payload_returns_zero(self):
        assert IndustryService._implant_reduction(
            character_modifier_payload=None, activity="manufacturing", metric="time",
        ) == 0.0


# ---------------------------------------------------------------------------
# _as_float
# ---------------------------------------------------------------------------

class TestAsFloat:
    @given(st.floats(allow_nan=False, allow_infinity=False))
    def test_float_passthrough(self, value: float):
        assert IndustryService._as_float(value) == value

    @given(st.integers(min_value=-1_000_000, max_value=1_000_000))
    def test_int_converts(self, value: int):
        assert IndustryService._as_float(value) == float(value)

    def test_none_returns_none(self):
        assert IndustryService._as_float(None) is None

    def test_non_numeric_returns_none(self):
        assert IndustryService._as_float("abc") is None

    @given(st.text())
    def test_never_raises(self, value: str):
        """_as_float should never raise, regardless of input."""
        result = IndustryService._as_float(value)
        assert result is None or isinstance(result, float)


# ---------------------------------------------------------------------------
# _resolve_preferred_unit_value priority chain
# ---------------------------------------------------------------------------

class TestResolvePreferredUnitValue:
    def test_explicit_unit_price_wins(self):
        price, source = IndustryService._resolve_preferred_unit_value(
            type_id=34,
            type_payload={
                "unit_price": 10.0,
                "acquisition_unit_cost": 8.0,
                "type_average_price": 5.0,
                "type_adjusted_price": 4.0,
            },
            sell_price_map={34: {"unit_price": 9.0}},
            adjusted_price_map={34: {"adjusted_price": 3.0}},
        )
        assert price == 10.0

    def test_acquisition_cost_second(self):
        price, source = IndustryService._resolve_preferred_unit_value(
            type_id=34,
            type_payload={
                "acquisition_unit_cost": 8.0,
                "type_average_price": 5.0,
            },
            sell_price_map={34: {"unit_price": 9.0}},
            adjusted_price_map={34: {"adjusted_price": 3.0}},
        )
        assert price == 8.0
        assert source == "owned_asset_acquisition_cost"

    def test_sell_price_third(self):
        price, source = IndustryService._resolve_preferred_unit_value(
            type_id=34,
            type_payload={"type_average_price": 5.0},
            sell_price_map={34: {"unit_price": 9.0, "price_source": "market"}},
            adjusted_price_map={34: {"adjusted_price": 3.0}},
        )
        assert price == 9.0

    def test_average_price_fourth(self):
        price, source = IndustryService._resolve_preferred_unit_value(
            type_id=34,
            type_payload={"type_average_price": 5.0, "type_adjusted_price": 4.0},
            sell_price_map=None,
            adjusted_price_map={34: {"adjusted_price": 3.0}},
        )
        assert price == 5.0
        assert source == "asset_average_price"

    def test_adjusted_price_fifth(self):
        price, source = IndustryService._resolve_preferred_unit_value(
            type_id=34,
            type_payload={"type_adjusted_price": 4.0},
            sell_price_map=None,
            adjusted_price_map={},
        )
        assert price == 4.0
        assert source == "asset_adjusted_price"

    def test_eiv_fallback(self):
        price, source = IndustryService._resolve_preferred_unit_value(
            type_id=34,
            type_payload={},
            sell_price_map=None,
            adjusted_price_map={34: {"adjusted_price": 3.0}},
        )
        assert price == 3.0
        assert source == "esi_adjusted_price"


# ---------------------------------------------------------------------------
# _manufacturing_required_skill_time_reduction
# ---------------------------------------------------------------------------

class TestManufacturingRequiredSkillTimeReduction:
    def test_industry_skill_excluded(self):
        """Industry and Advanced Industry skills should be excluded."""
        entries = [
            {"type_name": "Industry", "trained_skill_level": 5},
            {"type_name": "Advanced Industry", "trained_skill_level": 5},
        ]
        result = IndustryService._manufacturing_required_skill_time_reduction(entries)
        assert result == 0.0

    @given(skill_levels)
    def test_other_skill_contributes(self, level: int):
        """Non-Industry skills should contribute 1% per level."""
        entries = [{"type_name": "Mechanics", "trained_skill_level": level}]
        result = IndustryService._manufacturing_required_skill_time_reduction(entries)
        expected = IndustryService._combine_reductions([0.01 * level])
        assert math.isclose(result, expected, abs_tol=1e-9)

    def test_empty_returns_zero(self):
        assert IndustryService._manufacturing_required_skill_time_reduction([]) == 0.0


# ---------------------------------------------------------------------------
# _profile_rig_reduction
# ---------------------------------------------------------------------------

class TestProfileRigReduction:
    @given(fraction_floats)
    def test_matching_rig_effect_applied(self, value: float):
        profile = {
            "structure_rigs": [
                {
                    "effects": [
                        {"metric": "material", "activity": "manufacturing", "group": "All", "value": value},
                    ],
                },
            ],
        }
        result = IndustryService._profile_rig_reduction(
            profile_payload=profile,
            activity="manufacturing",
            metric="material",
        )
        assert 0.0 <= result <= 0.99

    def test_group_filtering(self):
        """Rig effects with specific group should only apply when manufacturing_group matches."""
        profile = {
            "structure_rigs": [
                {
                    "effects": [
                        {"metric": "material", "activity": "manufacturing", "group": "Modules", "value": 0.02},
                    ],
                },
            ],
        }
        # Matches
        result_match = IndustryService._profile_rig_reduction(
            profile_payload=profile,
            activity="manufacturing",
            metric="material",
            manufacturing_group="Modules",
        )
        assert result_match > 0.0

        # Doesn't match
        result_no_match = IndustryService._profile_rig_reduction(
            profile_payload=profile,
            activity="manufacturing",
            metric="material",
            manufacturing_group="Drones",
        )
        assert result_no_match == 0.0

    def test_fallback_to_aggregate_keys(self):
        """When no rig effects array, should use aggregate bonus keys."""
        profile = {"structure_rig_material_bonus": 0.044}
        result = IndustryService._profile_rig_reduction(
            profile_payload=profile,
            activity="manufacturing",
            metric="material",
        )
        expected = IndustryService._normalize_fraction(0.044)
        assert math.isclose(result, expected, abs_tol=1e-9)

    def test_none_profile_returns_zero(self):
        assert IndustryService._profile_rig_reduction(
            profile_payload=None,
            activity="manufacturing",
            metric="material",
        ) == 0.0


# ---------------------------------------------------------------------------
# Integration property: full reduction pipeline
# ---------------------------------------------------------------------------

class TestReductionPipelineIntegration:
    @given(
        skill_levels,  # industry
        skill_levels,  # advanced industry
        fraction_floats,  # profile base material reduction
        fraction_floats,  # rig material reduction
        fraction_floats,  # implant material reduction
    )
    @settings(max_examples=200)
    def test_total_material_reduction_bounded(
        self,
        industry: int,
        advanced: int,
        base_reduction: float,
        rig_reduction: float,
        implant_reduction: float,
    ):
        """Combining all material reduction sources should stay in [0, 0.99]."""
        # This simulates the full reduction chain for a manufacturing material calculation
        reductions = [base_reduction, rig_reduction, implant_reduction]
        combined = IndustryService._combine_reductions(reductions)
        assert 0.0 <= combined <= 0.99

    @given(
        positive_floats,  # base_quantity
        run_counts,       # runs
        fraction_floats,  # material_reduction
    )
    def test_material_quantity_invariants(
        self,
        base_quantity: float,
        runs: int,
        material_reduction: float,
    ):
        """Reduced material quantity should be <= original, and >= runs (minimum)."""
        assume(base_quantity >= 1.0)
        original = IndustryService._round_material_quantity(
            base_quantity * runs, minimum_quantity=runs,
        )
        reduced = IndustryService._round_material_quantity(
            base_quantity * (1.0 - material_reduction) * runs,
            minimum_quantity=runs,
        )
        # Reduced should be <= original (reduction can't increase quantity)
        assert reduced <= original + 1  # +1 for ceil rounding edge case
        # Should never go below runs (1 per run)
        assert reduced >= runs
