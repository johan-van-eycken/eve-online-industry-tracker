"""The facility gate and the reaction gate, from CCP's data (Task 11b fix round 1).

Which hulls need a shipyard comes from CCP's assembly lines (Hoboleaks
industryassemblylines / industryinstallationtypes, snapshot in
tests/fixtures/hoboleaks/): Standup Manufacturing Plant I runs line 175
"Structure Basic Manufacturing" (Freighters and Jump Freighters included),
Standup Capital Shipyard I line 176 (the capital hulls), Standup Supercapital
Shipyard I lines 176 and 177 (plus Titans and Supercarriers). Which structures
fit a service module, and where it may be onlined, comes from the SDE.

Public SDE ids only, no real profile or system. The SDE tests read the local
database/eve_sde.db read-only and skip when it is absent.
"""
from __future__ import annotations

import json
import logging
import os
from types import SimpleNamespace

import pytest

from eve_online_industry_tracker.domain import industry_facility as fac
from eve_online_industry_tracker.domain import security_band as sb

_LOCAL_SDE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "database", "eve_sde.db"))
_needs_sde = pytest.mark.skipif(not os.path.exists(_LOCAL_SDE), reason="local SDE database/eve_sde.db not present")
_HOBOLEAKS = os.path.join(os.path.dirname(__file__), "fixtures", "hoboleaks")


def _producer():
    from eve_online_industry_tracker.application.industry.service import IndustryService

    return IndustryService


@pytest.fixture(autouse=True)
def _fresh_warnings():
    S = _producer()
    for name in ("_rig_security_warned", "_facility_gate_warned", "_reaction_gate_warned",
                 "_aggregate_rig_warned"):
        getattr(S, name, set()).clear()
    yield


def _hobo(name):
    with open(os.path.join(_HOBOLEAKS, name), encoding="utf-8") as fh:
        return json.load(fh)


# The service modules' fitting data as the SDE has it (checked by
# test_the_service_module_fits_are_the_sdes).
MODULE_FITS = {
    35878: {"structure_type_ids": [], "structure_group_ids": [1404, 1406, 1657],
            "disallow_in_high_sec": False, "disallow_in_empire_space": False, "requires_sov_upgrade": False},
    35881: {"structure_type_ids": [35826, 35827, 35833, 35834, 40340, 47512, 47513, 47514, 47515, 47516],
            "structure_group_ids": [],
            "disallow_in_high_sec": True, "disallow_in_empire_space": False, "requires_sov_upgrade": False},
    35877: {"structure_type_ids": [35827], "structure_group_ids": [],
            "disallow_in_high_sec": False, "disallow_in_empire_space": True, "requires_sov_upgrade": True},
    45537: {"structure_type_ids": [], "structure_group_ids": [1406],
            "disallow_in_high_sec": True, "disallow_in_empire_space": False, "requires_sov_upgrade": False},
    45538: {"structure_type_ids": [], "structure_group_ids": [1406],
            "disallow_in_high_sec": True, "disallow_in_empire_space": False, "requires_sov_upgrade": False},
    45539: {"structure_type_ids": [], "structure_group_ids": [1406],
            "disallow_in_high_sec": True, "disallow_in_empire_space": False, "requires_sov_upgrade": False},
}
STRUCTURES = {  # type id, SDE group id
    "Raitaru": (35825, 1404), "Azbel": (35826, 1404), "Sotiyo": (35827, 1404),
    "Astrahus": (35832, 1657), "Fortizar": (35833, 1657), "Keepstar": (35834, 1657),
    "Palatine Keepstar": (40340, 1657), "'Moreau' Fortizar": (47512, 1657),
    "Athanor": (35835, 1406), "Tatara": (35836, 1406),
}


def structure_profile(name, band="null", *, npc_space=False):
    """A profile in a structure; npc_space=None leaves the system's faction unknown."""
    type_id, group_id = STRUCTURES[name]
    profile = {"structure_type_id": type_id, "location_type": "structure", "system_security_band": band,
               "system_id": 31000100,
               "structure_service_fits": fac.structure_service_fits(type_id, group_id, MODULE_FITS)}
    if npc_space is not None:
        profile["system_faction_id"] = 500001 if npc_space else None
    return profile


_DREAD = (485, "Dreadnought")
_FREIGHTER = (513, "Freighter")
_JUMP_FREIGHTER = (902, "Jump Freighter")
_TITAN = (30, "Titan")
_SUPERCARRIER = (659, "Supercarrier")


def _can(hull, profile):
    S = _producer()
    group_id, name = hull
    label = S._infer_manufacturing_group({"group_id": group_id, "group_name": name, "category_id": 6,
                                         "category_name": "Ship"})
    return S._facility_can_manufacture_group(manufacturing_group=label, profile_payload=profile, group_id=group_id)


# --- I1: the hull groups come from CCP's assembly lines ------------------------------

def test_the_shipyard_hull_groups_are_ccps_assembly_lines():
    lines = _hobo("industryassemblylines.json")
    installations = _hobo("industryinstallationtypes.json")

    def groups(line_id):
        return {d["groupID"] for d in lines[str(line_id)].get("details_per_group") or []}

    def line_ids(type_id):
        return {entry["assemblyLine"] for entry in installations[str(type_id)]["assembly_lines"]}

    assert lines["175"]["name"] == "Structure Basic Manufacturing"
    assert lines["176"]["name"] == "Structure Capital Shipyard"
    assert lines["177"]["name"] == "Structure Supercapital Shipyard"
    assert fac.CAPITAL_SHIPYARD_GROUP_IDS == groups(176)
    assert fac.SUPERCAPITAL_SHIPYARD_GROUP_IDS == groups(177)
    for service, type_ids in fac.SERVICE_MODULE_TYPE_IDS.items():
        if service in fac.SERVICE_ASSEMBLY_LINES:
            for type_id in type_ids:
                assert line_ids(type_id) == set(fac.SERVICE_ASSEMBLY_LINES[service]), service
    # The basic line builds Freighters and Jump Freighters and no shipyard hull.
    assert {513, 902} <= groups(175)
    assert not groups(175) & (groups(176) | groups(177))
    # NPC station manufacturing builds Freighters and Jump Freighters but no capital hull.
    station = groups(35)
    assert lines["35"]["name"] == "STATION 0.5+ Manufacturing"
    assert {513, 902} <= station and not station & (groups(176) | groups(177))


@pytest.mark.parametrize("group_id, required", [
    (485, fac.CAPITAL_SHIPYARD), (547, fac.CAPITAL_SHIPYARD), (883, fac.CAPITAL_SHIPYARD),
    (1538, fac.CAPITAL_SHIPYARD), (4594, fac.CAPITAL_SHIPYARD), (5120, fac.CAPITAL_SHIPYARD),
    (30, fac.SUPERCAPITAL_SHIPYARD), (659, fac.SUPERCAPITAL_SHIPYARD),
    (513, None), (902, None), (27, None), (25, None), (873, None),
])
def test_the_required_shipyard_per_hull_group(group_id, required):
    assert fac.required_shipyard(group_id) == required


@pytest.mark.parametrize("structure", ["Raitaru", "Astrahus", "Athanor", "Azbel", "Fortizar"])
@pytest.mark.parametrize("band", ["high", "low", "null"])
@pytest.mark.parametrize("hull", [_FREIGHTER, _JUMP_FREIGHTER])
def test_freighters_are_built_wherever_a_manufacturing_plant_fits(structure, band, hull):
    assert _can(hull, structure_profile(structure, band))


@pytest.mark.parametrize("hull", [_FREIGHTER, _JUMP_FREIGHTER])
def test_freighters_are_built_in_an_npc_station(hull):
    assert _can(hull, {"location_type": "station", "facility_type": "npc_station"})


@pytest.mark.parametrize("group_id, name", [(873, "Capital Construction Components"),
                                            (913, "Advanced Capital Construction Components")])
@pytest.mark.parametrize("profile", [
    structure_profile("Raitaru", "high"), structure_profile("Astrahus", "high"), structure_profile("Athanor", "low"),
    {"location_type": "station", "facility_type": "npc_station"}, {"facility_type": "upwell_structure"},
])
def test_capital_components_are_built_wherever_the_profile_manufactures(group_id, name, profile):
    S = _producer()
    label = S._infer_manufacturing_group({"group_id": group_id, "group_name": name, "category_id": 17,
                                         "category_name": "Commodity"})
    assert S._facility_can_manufacture_group(manufacturing_group=label, profile_payload=profile, group_id=group_id)


@pytest.mark.parametrize("structure", ["Azbel", "Sotiyo", "Fortizar", "Keepstar", "Palatine Keepstar",
                                       "'Moreau' Fortizar"])
def test_capital_ships_are_built_where_the_capital_shipyard_fits(structure):
    for band in ("low", "null", "wormhole"):
        assert _can(_DREAD, structure_profile(structure, band))
    # Standup Capital Shipyard I: disallowInHighSec.
    assert not _can(_DREAD, structure_profile(structure, "high"))


@pytest.mark.parametrize("structure", ["Raitaru", "Astrahus", "Athanor", "Tatara"])
@pytest.mark.parametrize("hull", [_DREAD, _TITAN])
def test_capital_ships_stay_out_of_structures_without_the_shipyard(structure, hull):
    assert not _can(hull, structure_profile(structure, "null"))


def test_an_npc_station_builds_no_capital_ship():
    for profile in ({"location_type": "station"}, {"facility_type": "npc_station"}):
        assert not _can(_DREAD, profile)
        assert not _can(_TITAN, profile)


@pytest.mark.parametrize("profile", [
    {}, {"facility_type": "upwell_structure"}, {"structure_type_id": 35827},
    {"structure_type_id": 1, "structure_type_name": "Sotiyo"},
])
@pytest.mark.parametrize("hull", [_DREAD, _TITAN])
def test_an_unknown_facility_does_not_build_capitals_and_warns(profile, hull, caplog):
    with caplog.at_level(logging.WARNING):
        assert not _can(hull, profile)
    assert any("facility unknown" in r.getMessage() for r in caplog.records)
    assert _can(_FREIGHTER, profile)  # not a shipyard hull


def test_an_unknown_band_does_not_build_capitals_and_says_why(caplog):
    profile = structure_profile("Azbel")
    del profile["system_security_band"]
    S = _producer()
    with caplog.at_level(logging.WARNING):
        decision = S._facility_gate(manufacturing_group="Capital Ships", profile_payload=profile, group_id=485)
        assert not _can(_DREAD, profile)
    assert not decision.allowed and decision.uncertain
    assert "band is unknown" in decision.reason and "31000100" in decision.reason
    assert any("band is unknown" in r.getMessage() for r in caplog.records)


def test_no_profile_is_no_facility_constraint():
    assert _can(_TITAN, None)


# --- M1: supercapitals: NPC nullsec refused, the sovereignty upgrade is assumed -------

@pytest.mark.parametrize("hull", [_TITAN, _SUPERCARRIER])
def test_supercapitals_are_built_only_in_a_sovereign_nullsec_sotiyo(hull, caplog):
    with caplog.at_level(logging.WARNING):
        assert _can(hull, structure_profile("Sotiyo", "null"))
    assert any("sovereignty hub upgrade assumed" in r.getMessage() for r in caplog.records)
    for structure in ("Azbel", "Fortizar", "Keepstar", "Raitaru"):
        assert not _can(hull, structure_profile(structure, "null"))
    for band in ("high", "low", "wormhole"):
        assert not _can(hull, structure_profile("Sotiyo", band))


def test_a_sotiyo_in_npc_nullsec_builds_no_supercapital():
    S = _producer()
    decision = S._facility_gate(manufacturing_group="All Ships", profile_payload=structure_profile(
        "Sotiyo", "null", npc_space=True), group_id=30)
    assert not decision.allowed and not decision.uncertain
    assert "NPC" in decision.reason
    # A capital ship is fine there.
    assert _can(_DREAD, structure_profile("Sotiyo", "null", npc_space=True))


def test_a_sotiyo_whose_space_owner_is_unknown_builds_no_supercapital(caplog):
    with caplog.at_level(logging.WARNING):
        assert not _can(_TITAN, structure_profile("Sotiyo", "null", npc_space=None))
    assert any("NPC" in r.getMessage() for r in caplog.records)


def test_the_assumed_sovereignty_warning_is_logged_once(caplog):
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            _can(_TITAN, structure_profile("Sotiyo", "null"))
    assert len([r for r in caplog.records if "sovereignty" in r.getMessage()]) == 1


# --- M4: the gate's reason reaches the response -------------------------------------

def test_the_candidate_filter_reports_each_hidden_row_and_its_reason():
    S = _producer()
    rows = [
        {"type_id": 19720, "type_name": "Revelation", "group_id": 485, "manufacturing_group": "Capital Ships"},
        {"type_id": 20185, "type_name": "Charon", "group_id": 513, "manufacturing_group": "Basic Large Ships"},
        {"type_id": 671, "type_name": "Erebus", "group_id": 30, "manufacturing_group": "All Ships"},
    ]
    hidden: list[dict] = []
    kept = S._filter_overview_rows_for_portfolio_candidates(
        rows, industry_profile_payload=structure_profile("Raitaru", "low"), facility_gate_hidden=hidden)
    assert [r["type_id"] for r in kept] == [20185]
    assert [h["type_id"] for h in hidden] == [19720, 671]
    assert all(h["reason"] for h in hidden)
    assert "Capital Shipyard" in hidden[0]["reason"]
    assert "Supercapital Shipyard" in hidden[1]["reason"]


# --- I2: the reaction gate -----------------------------------------------------------

@pytest.mark.parametrize("structure", ["Athanor", "Tatara"])
@pytest.mark.parametrize("band", ["low", "null", "wormhole"])
def test_a_refinery_outside_highsec_runs_reactions(structure, band):
    assert _producer()._reactions_allowed_for_profile(structure_profile(structure, band))


@pytest.mark.parametrize("structure", ["Athanor", "Tatara"])
def test_a_refinery_in_highsec_runs_no_reactions(structure):
    assert not _producer()._reactions_allowed_for_profile(structure_profile(structure, "high"))


@pytest.mark.parametrize("structure", ["Raitaru", "Azbel", "Sotiyo", "Astrahus", "Fortizar"])
@pytest.mark.parametrize("band", ["low", "null"])
def test_a_structure_that_cannot_fit_a_reactor_runs_no_reactions(structure, band):
    assert not _producer()._reactions_allowed_for_profile(structure_profile(structure, band))


def test_an_npc_station_runs_no_reactions():
    assert not _producer()._reactions_allowed_for_profile(
        {"location_type": "station", "facility_type": "npc_station", "system_security_band": "low"})


@pytest.mark.parametrize("profile", [
    None,
    {"system_security_status": 0.1},                                   # no structure: the old <0.5 rule said yes
    {"structure_type_id": 35835, "system_security_band": "low"},       # no SDE service data
])
def test_an_unknown_facility_runs_no_reactions_and_warns_once(profile, caplog):
    S = _producer()
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            assert not S._reactions_allowed_for_profile(profile)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "eaction" in r.getMessage()]
    assert len(warnings) == 1


def test_an_unknown_band_runs_no_reactions_and_warns(caplog):
    profile = structure_profile("Athanor")
    del profile["system_security_band"]
    with caplog.at_level(logging.WARNING):
        assert not _producer()._reactions_allowed_for_profile(profile)
    assert any("band is unknown" in r.getMessage() for r in caplog.records)


def test_the_separate_security_reader_is_gone():
    assert not hasattr(_producer(), "_profile_system_security_status")


# --- M2: the aggregate rig bonus fallback ---------------------------------------------

@pytest.mark.parametrize("band, multiplier", [
    ("high", 0.1), ("low", 1.9), ("null", 0.1), ("wormhole", 0.1), (None, 0.1),
])
def test_the_aggregate_fallback_takes_the_smallest_multiplier_and_warns(band, multiplier, caplog):
    S = _producer()
    profile = {"structure_rig_material_bonus": 0.037}
    if band is not None:
        profile["system_security_band"] = band
    with caplog.at_level(logging.WARNING):
        got = S._profile_rig_reduction(profile_payload=profile, activity="manufacturing", metric="material")
    assert got == pytest.approx(0.037 * multiplier)
    assert any("aggregate" in r.getMessage() for r in caplog.records)


@_needs_sde
def test_the_smallest_engineering_rig_multiplier_per_band_is_the_sdes(sde_session):
    from sqlalchemy import text

    from eve_online_industry_tracker.infrastructure.sde.rig_effects import get_rig_effects_for_type_ids

    ids = [r[0] for r in sde_session.execute(text(
        "SELECT t.id FROM types t JOIN groups g ON g.id = t.groupID WHERE t.published = 1 AND g.categoryID = 66 "
        "AND json_extract(g.name, '$.en') LIKE 'Structure Engineering Rig%'")).fetchall()]
    rigs = get_rig_effects_for_type_ids(sde_session, ids)
    for band in sb.BANDS:
        assert sb.ENGINEERING_RIG_MIN_MULTIPLIER[band] == min(
            r["security_multipliers"][band] for r in rigs if r["security_multipliers"][band] is not None)
    assert sb.ENGINEERING_RIG_MIN_MULTIPLIER_ANY_BAND == min(sb.ENGINEERING_RIG_MIN_MULTIPLIER.values())


def test_the_ui_rig_totals_are_scaled_for_the_band():
    from streamlit_ui.pages import industry_profiles as page

    rig_bonus_map = {
        43921: {"type_id": 43921, "material_reduction": 0.024, "time_reduction": 0.0, "cost_reduction": 0.0,
                "security_multipliers": {"high": 1.0, "low": 1.9, "null": 2.1, "wormhole": 2.1},
                "effects": [{"activity": "manufacturing", "group": "Modules", "metric": "material", "value": 0.024,
                             "security_scaled": True}]},
        45544: {"type_id": 45544, "material_reduction": 0.037, "time_reduction": 0.0, "cost_reduction": 0.0,
                "security_multipliers": {"high": 0.1, "low": 1.9, "null": 0.1, "wormhole": 0.1},
                "effects": [{"activity": "manufacturing", "group": "Capital Components", "metric": "material",
                             "value": 0.037, "security_scaled": True}]},
    }
    me, _, _ = page._compute_rig_bonuses([43921], rig_bonus_map, security_band="null")
    assert me == pytest.approx(0.0504)
    me, _, _ = page._compute_rig_bonuses([45544], rig_bonus_map, security_band="high")
    assert me == pytest.approx(0.0037)
    me, _, _ = page._compute_rig_bonuses([45544], rig_bonus_map, security_band=None)  # smallest
    assert me == pytest.approx(0.0037)
    table = page._rig_effects_table([43921], {43921: "rig"}, rig_bonus_map, security_band="low")
    assert table.loc[table["Manufacturing Group"] == "Modules", "ME"].item() == "5%"  # 4.56 %
    lines = page._rig_breakdown_lines([45544], {45544: "rig"}, rig_bonus_map, security_band="low")
    assert "ME 7%" in lines[0]  # 7.03 %


def test_the_ui_effects_table_lists_reactor_rigs_under_reactions():
    from streamlit_ui.pages import industry_profiles as page

    rig_bonus_map = {46497: {"type_id": 46497, "security_multipliers": {"high": None, "low": 1.0, "null": 1.1,
                                                                         "wormhole": 1.1},
                             "effects": [{"activity": "reaction", "group": "Hybrid Reactions", "metric": "material",
                                          "value": 0.024, "security_scaled": True}]}}
    table = page._rig_effects_table([46497], {46497: "rig"}, rig_bonus_map, security_band="low")
    assert table.loc[table["Manufacturing Group"] == "Hybrid Reactions", "ME"].item() == "2%"


# --- M3 / M6: security_band edge cases ------------------------------------------------

def test_a_scaled_effect_without_multipliers_gives_no_bonus_and_says_why(caplog):
    S = _producer()
    rig = {"type_id": 7, "effects": [{"activity": "manufacturing", "metric": "material", "group": "Modules",
                                       "value": 0.024, "security_scaled": True}]}
    with caplog.at_level(logging.WARNING):
        got = S._profile_rig_reduction(profile_payload={"structure_rigs": [rig], "system_security_band": "null"},
                                       activity="manufacturing", metric="material", manufacturing_group={"Modules"})
    assert got == 0.0
    assert any("no security multiplier" in r.getMessage() for r in caplog.records)
    assert sb.rig_security_multiplier(rig, "null")[0] == 0.0


def test_a_none_top_level_status_falls_back_to_the_industry_system():
    profile = {"system_security_status": None, "system_region_id": None,
               "industry_system": {"security_status": 0.3, "region_id": 11000005}}
    assert sb.profile_security_band(profile) == ("wormhole", None)
    profile["industry_system"]["region_id"] = 10000001
    assert sb.profile_security_band(profile) == ("low", None)


# --- the SDE ---------------------------------------------------------------------------

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


@_needs_sde
def test_the_service_module_fits_are_the_sdes(sde_session):
    from eve_online_industry_tracker.infrastructure.sde.structure_services import (
        get_service_module_fits, get_type_group_id)

    assert get_service_module_fits(sde_session, MODULE_FITS) == MODULE_FITS
    for type_id, group_id in STRUCTURES.values():
        assert get_type_group_id(sde_session, type_id) == group_id


# --- M5: the profile context end to end -------------------------------------------------

_LOWSEC_SYSTEM = 30002537     # a public lowsec system (security 0.4), not a user's
_NPC_NULL_SYSTEM = 30003268   # a public NPC nullsec system (Syndicate)


class _Sessions:
    def __init__(self, app, sde):
        self._app, self._sde = app, sde
        self.sde_calls = 0

    def app_session(self):
        return self._app

    def sde_session(self):
        self.sde_calls += 1
        return self._sde

    def oauth_session(self):  # pragma: no cover - not used
        raise RuntimeError


def _service_with_profile(app_session, sde_session, **profile):
    from eve_online_industry_tracker.infrastructure.models import IndustryProfilesModel

    row = IndustryProfilesModel(character_id=90000001, profile_name="Test profile", is_default=True, **profile)
    app_session.add(row)
    app_session.commit()
    sessions = _Sessions(app_session, sde_session)
    service = _producer()(state=SimpleNamespace(esi_service=None, admin_settings=None, db_sde=None),
                          sessions=sessions)
    return service, sessions, row.id


@_needs_sde
def test_the_profile_context_carries_band_region_faction_fits_and_rig_multipliers(app_session, sde_session):
    service, sessions, profile_id = _service_with_profile(
        app_session, sde_session, system_id=_LOWSEC_SYSTEM, location_type="structure", facility_type="Athanor",
        structure_type_id=35835, rig_slot0_type_id=46497, rig_slot1_type_id=43921)
    payload = service._resolve_industry_profile_context(character_id=None, industry_profile_id=profile_id)
    assert payload["system_security_band"] == "low"
    assert payload["system_region_id"] == 10000030
    assert "system_faction_id" in payload
    fits = payload["structure_service_fits"]
    assert fits["reactor"]["fits"] and fits["manufacturing_plant"]["fits"]
    assert not fits["capital_shipyard"]["fits"]
    rigs = {r["type_id"]: r for r in payload["structure_rigs"]}
    assert rigs[46497]["security_multipliers"]["low"] == 1.0
    assert rigs[43921]["security_multipliers"]["null"] == 2.1
    assert service._reactions_allowed_for_profile(payload)
    assert service._profile_rig_reduction(profile_payload=payload, activity="manufacturing", metric="material",
                                          manufacturing_group={"Modules"}) == pytest.approx(0.024 * 1.9)
    # The service fits are cached per structure type.
    before = sessions.sde_calls
    assert service._structure_service_fits(35835) is fits or service._structure_service_fits(35835) == fits
    assert sessions.sde_calls == before


@_needs_sde
def test_the_profile_context_marks_npc_nullsec(app_session, sde_session):
    service, _, profile_id = _service_with_profile(
        app_session, sde_session, system_id=_NPC_NULL_SYSTEM, location_type="structure", facility_type="Sotiyo",
        structure_type_id=35827)
    payload = service._resolve_industry_profile_context(character_id=None, industry_profile_id=profile_id)
    assert payload["system_security_band"] == "null"
    assert payload["system_faction_id"]
    assert not service._facility_can_manufacture_group(manufacturing_group="All Ships", profile_payload=payload,
                                                       group_id=30)
    assert service._facility_can_manufacture_group(manufacturing_group="Capital Ships", profile_payload=payload,
                                                   group_id=485)


@_needs_sde
def test_the_rig_catalog_lists_every_engineering_and_reactor_rig(sde_session):
    service = _producer()(state=SimpleNamespace(esi_service=None, admin_settings=None, db_sde=None,
                                                _structure_rigs_cache=None),
                          sessions=_Sessions(None, sde_session))
    catalog = {r["type_id"]: r for r in service.structure_rigs()}
    for type_id in (46496, 46497, 45544, 45548, 37182, 37183, 43920, 43921):
        assert type_id in catalog, type_id
        assert catalog[type_id]["effects"] and catalog[type_id]["security_multipliers"]
        assert catalog[type_id]["name"]
    assert 48003 not in catalog  # Upwell outpost conversion rig
    assert any("Copy" in r["name"] for r in catalog.values())
