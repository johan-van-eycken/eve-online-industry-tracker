# src/eve_online_industry_tracker/application/daily_planner/facility_bonus.py
"""Structure and rig material bonuses for a sub-manufacture job.

The one place the daily planner reaches into IndustryService's private
statics. A sub-component is built in its parent's structure (ruling F-Q2),
so its inputs must be reduced exactly as the producer reduces the parent's:
the producer's own combination, profile readers and group inference are
reused here rather than re-implemented. The producer is not modified.

Where the producer would over-apply a rig bonus to a sub-component, this
module is stricter. When unsure, no rig bonus is applied: an over-buy is
recoverable, an under-buy stalls the job.
"""
from __future__ import annotations

from typing import Any

def _category_is(*names: str) -> Any:
    allowed = frozenset(names)
    return lambda category: category in allowed


# The category each inferred manufacturing group requires before it is
# trusted. The producer infers most groups from name tokens ("missile",
# "drone", "structure", "component", ship words) and files anything else
# under "Modules", so e.g. "Missile Launcher Heavy" (a Module) reads as Ammo &
# Charges and "Infrastructure Upgrades" as Structures. A group whose category
# does not match is a guess and counts as unknown (only "All" rigs apply).
_EXPECTED_CATEGORY: dict[str, Any] = {
    "Ammo & Charges": _category_is("charge"),
    "Drones": _category_is("drone"),
    "Structures": lambda category: category.startswith("structure"),
    "Advanced Components": _category_is("commodity"),
    "Capital Components": _category_is("commodity"),
    "Advanced Capital Components": _category_is("commodity"),
    "Modules": _category_is("module", "subsystem"),
    **{ship_group: _category_is("ship") for ship_group in (
        "Basic Small Ships", "Advanced Small Ships", "Basic Medium Ships",
        "Advanced Medium Ships", "Basic Large Ships", "Advanced Large Ships", "Capital Ships",
    )},
}

FACILITY_KEYS = ("structure_material_reduction", "rig_material_reduction", "rig_applicability")


def _producer() -> Any:
    # Lazy: the producer module is heavy and imports flask_app.
    from eve_online_industry_tracker.application.industry.service import IndustryService

    return IndustryService


def combine_reductions(reductions: list[Any]) -> float:
    """IndustryService._combine_reductions: 1 - prod(1 - r), capped at 0.99."""
    return _producer()._combine_reductions(list(reductions))


def structure_material_reduction(profile: Any) -> float:
    """The profile's structure material bonus for manufacturing."""
    return _producer()._profile_base_reduction(
        profile_payload=profile, activity="manufacturing", metric="material"
    )


def component_manufacturing_group(entry: Any) -> str | None:
    """The producer's manufacturing group for a component, or None when unsure.

    IndustryService._infer_manufacturing_group_uncached, minus its guesses:
    an inferred group is accepted only when the component's category is the
    one that group requires (_EXPECTED_CATEGORY). "Modules" is the
    producer's catch-all for any group name it cannot classify (R.A.M.s,
    Fuel Blocks, ...); the other groups come from name tokens that also hit
    modules ("Missile Launcher Heavy", "Drone Damage Modules", "Smart Bomb")
    or unrelated groups ("Infrastructure Upgrades"). A group not in the
    table is not trusted either.
    The uncached variant is used because the producer's cache is keyed by
    group/category/meta-group ids, which a material entry may lack.
    """
    if not isinstance(entry, dict):
        return None
    group = _producer()._infer_manufacturing_group_uncached(entry)
    category = str(entry.get("category_name") or "").strip().lower()
    accepts = _EXPECTED_CATEGORY.get(group) if group is not None else None
    if accepts is None or not accepts(category):
        return None
    return group


def _material_rig_effects(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """The profile's per-effect manufacturing material rig entries.

    The part of IndustryService._profile_rig_reduction's filter that does
    not depend on the product's group.
    """
    producer = _producer()
    valid_activities = set(producer._ACTIVITY_EFFECT_ALIASES["manufacturing"])
    return [
        effect
        for rig in (profile.get("structure_rigs") or []) if isinstance(rig, dict)
        for effect in (rig.get("effects") or []) if isinstance(effect, dict)
        if str(effect.get("metric") or "") == "material"
        and str(effect.get("activity") or "") in valid_activities
        and producer._normalize_fraction(effect.get("value")) > 0.0
    ]


def _rig_material_reduction(profile: dict[str, Any], group: str) -> float:
    return _producer()._profile_rig_reduction(
        profile_payload=profile, activity="manufacturing", metric="material",
        manufacturing_group=group,
    )


def sub_job_facility_reductions(profile: Any, component_entry: Any) -> dict[str, Any]:
    """Material bonuses a sub job gets in its parent's structure.

    `profile` is the parent row's manufacturing_job.industry_profile;
    `component_entry` is the parent's manufacturing_job.materials entry for
    the component (it carries the component's SDE group/category names).

    - structure: the producer's _profile_base_reduction.
    - rig: the producer's _profile_rig_reduction for the component's group,
      called only when a per-effect rig entry covers that group or "All".
      The producer's helper alone would over-apply: with no group it accepts
      every rig, and with no matching effect it falls back to the aggregate
      structure_rig_material_bonus, which names no group.

    rig_applicability: "applies", "not_covered" (rigs exist, none for this
    group), "no_rigs", "no_profile", or "unknown" (the component's group is
    not known for sure, or only the aggregate bonus exists). Unknown applies
    only an "All" rig, which covers any group.
    """
    out: dict[str, Any] = {"structure_material_reduction": 0.0,
                           "rig_material_reduction": 0.0, "rig_applicability": "no_profile"}
    if not isinstance(profile, dict):
        return out
    out["structure_material_reduction"] = structure_material_reduction(profile)

    effects = _material_rig_effects(profile)
    if not effects:
        aggregate = _producer()._normalize_fraction(profile.get("structure_rig_material_bonus"))
        out["rig_applicability"] = "unknown" if aggregate > 0.0 else "no_rigs"
        return out

    group = component_manufacturing_group(component_entry)
    if group is None:
        out["rig_applicability"] = "unknown"
        if any(str(e.get("group") or "All") == "All" for e in effects):
            # "" as the group makes the producer skip every group-specific effect.
            out["rig_material_reduction"] = _rig_material_reduction(profile, "")
        return out
    if not any(str(e.get("group") or "All") in {"All", group} for e in effects):
        out["rig_applicability"] = "not_covered"
        return out
    out["rig_material_reduction"] = _rig_material_reduction(profile, group)
    out["rig_applicability"] = "applies"
    return out


def facility_reduction(request: dict[str, Any]) -> float:
    """Combined structure + rig material fraction of one sub-manufacture request."""
    return combine_reductions([
        request.get("structure_material_reduction") or 0.0,
        request.get("rig_material_reduction") or 0.0,
    ])
