"""Rig values, security multipliers and the facility gate, from the SDE (Task 11b).

- Each rig effect's value is the rig attribute its SDE effect actually
  modifies with (modifierInfo.modifyingAttributeID): 2714/2713 for reactor
  rigs, 2653 for the Thukker capital-component material bonus, 2594/2593 for
  the standard engineering rigs.
- A rig's bonus scales by the structure's security band: hiSecModifier 2355,
  lowSecModifier 2356, nullSecModifier 2357 (wormhole space: nullsec).
- Capital ships need Standup Capital Shipyard I, supercapitals Standup
  Supercapital Shipyard I; which structures fit them comes from the
  modules' canFitShipType* / canFitShipGroup* attributes.

Public SDE ids only, no real profile or system. The SDE tests read the local
database/eve_sde.db read-only and skip when it is absent.
"""
from __future__ import annotations

import json
import logging
import os

import pytest

import sde_industry_fakes as fakes
from eve_online_industry_tracker.domain import rig_groups as rg
from eve_online_industry_tracker.domain import security_band as sb

_LOCAL_SDE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "database", "eve_sde.db"))
_needs_sde = pytest.mark.skipif(not os.path.exists(_LOCAL_SDE), reason="local SDE database/eve_sde.db not present")

# Public SDE rig type ids.
_REACTOR_EFFICIENCY_I, _REACTOR_EFFICIENCY_II = 46496, 46497
_THUKKER_M_CAP_COMP, _THUKKER_L_CAP_COMP = 45544, 45546
_THUKKER_M_ADV_COMP, _THUKKER_XL = 45640, 45548
_EQUIPMENT_ME_M_I, _EQUIPMENT_ME_M_II = 43920, 43921

_STANDARD = {"high": 1.0, "low": 1.9, "null": 2.1, "wormhole": 2.1}
_THUKKER = {"high": 0.1, "low": 1.9, "null": 0.1, "wormhole": 0.1}
_REACTOR = {"high": None, "low": 1.0, "null": 1.1, "wormhole": 1.1}


def _producer():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    return IndustryService


def _entry(group_id, group_name, category_id, category_name):
    return {"type_id": 1, "group_id": group_id, "group_name": group_name,
            "category_id": category_id, "category_name": category_name}


def _rig(group, value, multipliers, *, activity="manufacturing", metric="material", type_id=1):
    return {"type_id": type_id, "security_multipliers": multipliers, "effects": [
        {"activity": activity, "metric": metric, "group": group, "value": value, "security_scaled": True}]}


def _profile(*rigs, band=None, **extra):
    profile = {"material_efficiency_bonus": 0.01, "structure_rigs": list(rigs), **extra}
    if band is not None:
        profile["system_security_band"] = band
    return profile


@pytest.fixture(autouse=True)
def _fresh_warnings():
    S = _producer()
    for name in ("_rig_security_warned", "_facility_gate_warned"):
        getattr(S, name, set()).clear()
    yield


@pytest.fixture(scope="module")
def sde_session():
    if not os.path.exists(_LOCAL_SDE):
        pytest.skip("local SDE database/eve_sde.db not present")
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    engine = create_engine(f"sqlite:///file:{_LOCAL_SDE}?mode=ro&uri=true")
    with Session(engine) as session:
        yield session
    engine.dispose()


def _rigs(session, *type_ids, catalog=None):
    """The rigs' effects from the SDE's dogma, labelled by `catalog` (default:
    the installed one, the fake tables' in the tests)."""
    from eve_online_industry_tracker.infrastructure.sde.rig_effects import get_rig_effects_for_type_ids

    return {r["type_id"]: r for r in get_rig_effects_for_type_ids(session, type_ids, catalog=catalog)}


def _values(rig):
    return {(e["activity"], e["group"], e["metric"]): pytest.approx(e["value"]) for e in rig["effects"]}


# --- 1. reactor rigs ------------------------------------------------------------

@pytest.mark.parametrize("type_id, me, te", [(_REACTOR_EFFICIENCY_I, 0.02, 0.20), (_REACTOR_EFFICIENCY_II, 0.024, 0.24)])
def test_reactor_rigs_read_their_values_from_2714_and_2713(sde_session, type_id, me, te):
    rig = _rigs(sde_session, type_id)[type_id]
    expected = {}
    for label in ("Hybrid Reactions", "Composite Reactions", "Biochemical Reactions"):
        expected[("reaction", label, "material")] = me
        expected[("reaction", label, "time")] = te
    assert _values(rig) == expected
    assert {e["modifying_attribute_id"] for e in rig["effects"]} == {2713, 2714}
    assert all(e["security_scaled"] for e in rig["effects"])
    assert rig["security_multipliers"] == _REACTOR  # disallowInHighSec: no highsec multiplier


@pytest.mark.parametrize("group_id, group_name, label", [
    (974, "Hybrid Polymers", "Hybrid Reactions"),
    (429, "Composite", "Composite Reactions"),
    (712, "Biochemical Material", "Biochemical Reactions"),
    (4096, "Molecular-Forged Materials", "Biochemical Reactions"),
    (428, "Intermediate Materials", "Composite Reactions"),
])
def test_a_reaction_rig_reduces_the_reaction_by_its_target_filter(group_id, group_name, label):
    S = _producer()
    product = _entry(group_id, group_name, 4, "Material")
    groups = S._manufacturing_rig_groups(product, activity="reaction")
    assert groups == {label}
    profile = _profile(*(_rig(g, 0.024, _REACTOR, activity="reaction", metric=m, type_id=i)
                         for i, (g, m) in enumerate((g, m) for g in fakes.fake_industry_data().rig_catalog.reaction_labels for m in ("material", "time"))),
                       band="low")
    for metric in ("material", "time"):
        assert S._profile_rig_reduction(profile_payload=profile, activity="reaction", metric=metric,
                                        manufacturing_group=groups) == pytest.approx(0.024)
    # Never a manufacturing job.
    assert S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                    manufacturing_group=S._manufacturing_rig_groups(product)) == 0.0


def test_a_reaction_rig_for_another_reaction_group_does_not_apply():
    S = _producer()
    hybrid = S._manufacturing_rig_groups(_entry(974, "Hybrid Polymers", 4, "Material"), activity="reaction")
    profile = _profile(_rig("Composite Reactions", 0.024, _REACTOR, activity="reaction"), band="null")
    assert S._profile_rig_reduction(profile_payload=profile, activity="reaction", metric="material",
                                    manufacturing_group=hybrid) == 0.0


def test_a_manufacturing_rig_never_applies_to_a_reaction():
    S = _producer()
    hybrid = S._manufacturing_rig_groups(_entry(974, "Hybrid Polymers", 4, "Material"), activity="reaction")
    profile = _profile(_rig("All", 0.02, _STANDARD), band="null")
    assert S._profile_rig_reduction(profile_payload=profile, activity="reaction", metric="material",
                                    manufacturing_group=hybrid) == 0.0


def test_a_reaction_job_gets_the_reactor_rig_material_and_time():
    """_compute_activity_job for a reaction, as the recursive plan calls it
    (reaction groups from _manufacturing_rig_groups(..., activity="reaction"))."""
    S = _producer()
    service = S(state=type("S", (), {"admin_settings": None})())
    groups = S._manufacturing_rig_groups(_entry(429, "Composite", 4, "Material"), activity="reaction")
    profile = _profile(_rig("Composite Reactions", 0.024, _REACTOR, activity="reaction"),
                       _rig("Composite Reactions", 0.24, _REACTOR, activity="reaction", metric="time", type_id=2),
                       band="null", facility_tax=0.0)
    profile["material_efficiency_bonus"] = 0.0
    job = service._compute_activity_job(
        activity="reaction", base_time_seconds=10_000, runs=1, process_value=None,
        selected_industry_profile=profile, selected_character_modifiers=None, character_skill_levels_by_name={},
        manufacturing_group=groups,
    )
    assert job["material_reduction"] == pytest.approx(0.024 * 1.1)
    assert job["time_reduction"] == pytest.approx(0.24 * 1.1)
    assert job["duration_seconds"] == 10_000 - int(10_000 * 0.264)


# --- 2. Thukker values -------------------------------------------------------------

def test_thukker_capital_component_material_values_come_from_2653(sde_session):
    rigs = _rigs(sde_session, _THUKKER_M_CAP_COMP, _THUKKER_L_CAP_COMP, _THUKKER_M_ADV_COMP, _THUKKER_XL)
    cap = ("manufacturing", "Capital Components", "material")
    adv_cap = ("manufacturing", "Advanced Capital Components", "material")
    adv = ("manufacturing", "Advanced Components", "material")
    assert _values(rigs[_THUKKER_M_CAP_COMP])[cap] == 0.037
    assert _values(rigs[_THUKKER_L_CAP_COMP])[cap] == 0.037
    # M-Set Thukker Advanced Component: 2.0 % for Advanced Components (2594),
    # 3.7 % for Advanced Capital Components (2653).
    assert _values(rigs[_THUKKER_M_ADV_COMP])[adv] == 0.02
    assert _values(rigs[_THUKKER_M_ADV_COMP])[adv_cap] == 0.037
    assert _values(rigs[_THUKKER_XL])[cap] == 0.037
    assert _values(rigs[_THUKKER_XL])[adv_cap] == 0.037
    for type_id in (_THUKKER_M_CAP_COMP, _THUKKER_L_CAP_COMP, _THUKKER_M_ADV_COMP, _THUKKER_XL):
        rig = rigs[type_id]
        assert rig["security_multipliers"] == _THUKKER
        thukker = [e for e in rig["effects"] if e["effect_name"].startswith("rigThukker")]
        assert thukker and all(e["modifying_attribute_id"] == 2653 for e in thukker)


def test_a_thukker_rig_in_highsec_gives_a_tenth():
    S = _producer()
    cap = S._manufacturing_rig_groups(_entry(873, "Capital Construction Components", 17, "Commodity"))
    for band, expected in (("high", 0.0037), ("low", 0.037 * 1.9), ("null", 0.0037), ("wormhole", 0.0037)):
        profile = _profile(_rig("Capital Components", 0.037, _THUKKER), band=band)
        assert S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                        manufacturing_group=cap) == pytest.approx(expected)


@_needs_sde
def test_every_rig_effect_value_is_the_attribute_its_effect_modifies_with(sde_session):
    """For every engineering and reactor rig in the SDE, each effect's value
    is -(the rig's modifyingAttributeID value) / 100."""
    from sqlalchemy import text

    rows = sde_session.execute(text(
        "SELECT t.id FROM types t JOIN groups g ON g.id = t.groupID WHERE g.categoryID = 66 AND t.published = 1 "
        "AND (json_extract(g.name, '$.en') LIKE 'Structure Engineering Rig%' "
        "OR json_extract(g.name, '$.en') LIKE 'Structure %Reactor Rig%')")).fetchall()
    type_ids = [r[0] for r in rows]
    attrs = {
        tid: {a["attributeID"]: a["value"] for a in json.loads(raw)}
        for tid, raw in sde_session.execute(text(
            f"SELECT id, dogmaAttributes FROM typeDogma WHERE id IN ({','.join(map(str, type_ids))})")).fetchall()
    }
    checked = 0
    for type_id, rig in _rigs(sde_session, *type_ids).items():
        assert rig["effects"], type_id
        for e in rig["effects"]:
            assert e["value"] == pytest.approx(-attrs[type_id][e["modifying_attribute_id"]] / 100.0)
            checked += 1
    assert len(type_ids) > 100 and checked > 200


# --- the attribute bindings against CCP's per-type modifier sources ---------------

_ACTIVITY_FROM_CCP = {"researchMaterial": "research", "researchTime": "research"}
_ACTIVITY_OURS = {"research_me": "research", "research_te": "research"}


@_needs_sde
def test_every_rig_effect_matches_the_sde_modifier_sources(sde_session):
    """Each rig's (activity, metric, structure attribute, filter) is CCP's
    (the SDE's industryModifierSources), for every engineering and reactor
    rig in it, with the catalog read from the same SDE. CCP's lab rigs swap
    the ME/TE research entries (37182/37183 list 2565 under researchTime);
    the SDE attribute descriptions say 2565 is ME research, so the two
    research activities are compared together."""
    from sqlalchemy import text

    from eve_online_industry_tracker.infrastructure.sde import industry

    catalog = fakes.local_industry_data_or_skip().rig_catalog
    sources = industry.read_industry_tables(sde_session)["industryModifierSources"]
    rig_ids = {
        r[0] for r in sde_session.execute(text(
            "SELECT t.id FROM types t JOIN groups g ON g.id = t.groupID WHERE g.categoryID = 66 "
            "AND (json_extract(g.name, '$.en') LIKE 'Structure Engineering Rig%' "
            "OR json_extract(g.name, '$.en') LIKE 'Structure %Reactor Rig%')")).fetchall()
    }
    sde_rigs = sorted(type_id for type_id in sources if type_id in rig_ids)
    filter_by_label = {f.label: f.filter_id for f in (*catalog.manufacturing_filters, *catalog.reaction_filters)}
    rigs = _rigs(sde_session, *sde_rigs, catalog=catalog)
    for type_id in sde_rigs:
        ccp = {
            (_ACTIVITY_FROM_CCP.get(activity, activity), metric, int(e["dogmaAttributeID"]), e.get("filterID"))
            for activity, metrics in sources[type_id].items()
            for metric, entries in metrics.items() for e in entries
        }
        ours = {
            (_ACTIVITY_OURS.get(e["activity"], e["activity"]), e["metric"], e["modified_attribute_id"],
             filter_by_label.get(e["group"]))
            for e in rigs[type_id]["effects"]
        }
        assert ours == ccp, type_id
    assert len(sde_rigs) > 100


@_needs_sde
def test_the_research_attributes_are_bound_by_their_sde_description(sde_session):
    from sqlalchemy import text

    descriptions = dict(sde_session.execute(text(
        "SELECT id, description FROM dogmaAttributes WHERE id BETWEEN 2563 AND 2570")).fetchall())
    words = {"invention": "Invention", "research_me": "Material Efficiency", "research_te": "Time Efficiency",
             "copying": "Copying"}
    for attribute_id, (activity, metric) in rg.UNFILTERED_ATTRIBUTE_BINDINGS.items():
        assert words[activity] in descriptions[attribute_id]
        assert ("cost" if metric == "cost" else "time") in descriptions[attribute_id].lower()


# --- 3. security multipliers -------------------------------------------------------

@_needs_sde
def test_the_security_attribute_ids_and_names_are_the_sdes(sde_session):
    from sqlalchemy import text

    names = dict(sde_session.execute(text(
        "SELECT id, name FROM dogmaAttributes WHERE id IN (1970, 2355, 2356, 2357, 2358)")).fetchall())
    assert names == {1970: "disallowInHighSec", 2355: "hiSecModifier", 2356: "lowSecModifier",
                     2357: "nullSecModifier", 2358: "securityModifier"}
    rigs = _rigs(sde_session, _EQUIPMENT_ME_M_I, _EQUIPMENT_ME_M_II)
    assert rigs[_EQUIPMENT_ME_M_II]["security_multipliers"] == _STANDARD
    assert _values(rigs[_EQUIPMENT_ME_M_II]) == {("manufacturing", "Modules", "material"): 0.024}


@pytest.mark.parametrize("band, expected", [
    ("high", 0.024), ("low", 0.024 * 1.9), ("null", 0.0504), ("wormhole", 0.0504),
])
def test_a_t2_me_rig_scales_by_the_security_band(band, expected):
    S = _producer()
    modules = S._manufacturing_rig_groups(_entry(508, "Missile Launcher Heavy", 7, "Module"))
    profile = _profile(_rig("Modules", 0.024, _STANDARD), band=band)
    assert S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                    manufacturing_group=modules) == pytest.approx(expected)


def test_nullsec_t2_me_rig_is_exactly_5_04_percent():
    S = _producer()
    profile = _profile(_rig("Modules", 0.024, _STANDARD), band="null")
    assert S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                    manufacturing_group={"Modules"}) == pytest.approx(0.0504, abs=1e-12)


@pytest.mark.parametrize("security_status, region_id, band", [
    (1.0, None, "high"), (0.45, None, "high"), (0.450192, None, "high"),
    (0.449944, None, "low"), (0.1, None, "low"), (0.029147, None, "low"),
    (0.0, None, "null"), (-0.000393, None, "null"), (-1.0, None, "null"),
    (-0.99, 11000001, "wormhole"), (-0.99, 11000033, "wormhole"),
    (None, None, None),
])
def test_the_security_band_boundaries(security_status, region_id, band):
    assert sb.security_band(security_status, region_id) == band


def test_the_band_comes_from_the_profiles_system():
    S = _producer()
    rig = _rig("Modules", 0.024, _STANDARD)
    lowsec = {"structure_rigs": [rig], "system_id": 31000001, "system_security_status": 0.3}
    jspace = {"structure_rigs": [rig], "system_id": 31000002, "system_security_status": -0.99,
              "system_region_id": 11000010}
    assert S._profile_rig_reduction(profile_payload=lowsec, activity="manufacturing", metric="material",
                                    manufacturing_group={"Modules"}) == pytest.approx(0.024 * 1.9)
    assert S._profile_rig_reduction(profile_payload=jspace, activity="manufacturing", metric="material",
                                    manufacturing_group={"Modules"}) == pytest.approx(0.024 * 2.1)


@pytest.mark.parametrize("multipliers, expected", [(_STANDARD, 0.024), (_THUKKER, 0.0024), (_REACTOR, 0.024)])
def test_an_unknown_band_takes_the_smallest_multiplier_and_warns(multipliers, expected, caplog):
    S = _producer()
    profile = {"structure_rigs": [_rig("Modules", 0.024, multipliers)], "system_id": 31000003}
    with caplog.at_level(logging.WARNING):
        got = S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                       manufacturing_group={"Modules"})
    assert got == pytest.approx(expected)
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("security band is unknown" in w and "31000003" in w for w in warnings)


def test_a_profile_with_no_system_takes_the_smallest_multiplier_and_says_why(caplog):
    S = _producer()
    with caplog.at_level(logging.WARNING):
        got = S._profile_rig_reduction(profile_payload=_profile(_rig("Modules", 0.037, _THUKKER)),
                                       activity="manufacturing", metric="material", manufacturing_group={"Modules"})
    assert got == pytest.approx(0.0037)
    assert any("no solar system" in r.getMessage() for r in caplog.records)


def test_the_unknown_band_warning_is_logged_once(caplog):
    S = _producer()
    profile = _profile(_rig("Modules", 0.024, _STANDARD))
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                     manufacturing_group={"Modules"})
    assert len([r for r in caplog.records if "security" in r.getMessage()]) == 1


def test_a_reactor_rig_in_highsec_gives_no_bonus_and_warns(caplog):
    S = _producer()
    profile = _profile(_rig("Hybrid Reactions", 0.024, _REACTOR, activity="reaction"), band="high")
    with caplog.at_level(logging.WARNING):
        got = S._profile_rig_reduction(profile_payload=profile, activity="reaction", metric="material",
                                       manufacturing_group={"Hybrid Reactions"})
    assert got == 0.0
    assert any("cannot be fitted in high" in r.getMessage() for r in caplog.records)


def test_an_unscaled_effect_and_the_structure_bonus_ignore_the_band():
    S = _producer()
    unscaled = {"type_id": 1, "security_multipliers": _STANDARD, "effects": [
        {"activity": "manufacturing", "metric": "material", "group": "Modules", "value": 0.02}]}
    profile = _profile(unscaled, band="null")
    assert S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                    manufacturing_group={"Modules"}) == pytest.approx(0.02)
    assert S._profile_base_reduction(profile_payload=profile, activity="manufacturing", metric="material") == (
        pytest.approx(0.01))


def test_research_rigs_scale_by_the_band_too():
    S = _producer()
    profile = _profile(_rig("All", 0.2, _STANDARD, activity="research_me", metric="time"), band="low")
    assert S._profile_rig_reduction(profile_payload=profile, activity="research_material", metric="time") == (
        pytest.approx(0.38))


# --- producer / planner parity under the band --------------------------------------

@pytest.mark.parametrize("band", ["high", "low", "null", "wormhole", None])
@pytest.mark.parametrize("entry", [
    _entry(508, "Missile Launcher Heavy", 7, "Module"),
    _entry(873, "Capital Construction Components", 17, "Commodity"),
    _entry(913, "Advanced Capital Construction Components", 17, "Commodity"),
    _entry(334, "Construction Components", 17, "Commodity"),
    _entry(25, "Frigate", 6, "Ship"),
    {"type_id": 4242},
])
def test_producer_and_planner_agree_under_security_multipliers(band, entry):
    from eve_online_industry_tracker.application.daily_planner import facility_bonus

    S = _producer()
    profile = _profile(
        _rig("Modules", 0.024, _STANDARD, type_id=1),
        _rig("Capital Components", 0.037, _THUKKER, type_id=2),
        _rig("Advanced Capital Components", 0.037, _THUKKER, type_id=3),
        _rig("Advanced Components", 0.02, _THUKKER, type_id=3),
        _rig("All Ships", 0.02, _STANDARD, type_id=4),
        band=band, structure_rig_material_bonus=0.05,
    )
    producer = S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material",
                                        manufacturing_group=S._manufacturing_rig_groups(entry))
    planner = facility_bonus.sub_job_facility_reductions(profile, entry)
    assert planner["rig_material_reduction"] == producer


def test_the_planner_sub_job_gets_the_nullsec_value():
    from eve_online_industry_tracker.application.daily_planner import facility_bonus

    profile = _profile(_rig("Capital Components", 0.037, _THUKKER), band="low")
    out = facility_bonus.sub_job_facility_reductions(profile, _entry(873, "Capital Construction Components", 17, "Commodity"))
    assert out["rig_material_reduction"] == pytest.approx(0.037 * 1.9)
    assert out["rig_applicability"] == "applies"


# The facility gate and the reaction gate: tests/test_facility_gate_and_reactions.py.
