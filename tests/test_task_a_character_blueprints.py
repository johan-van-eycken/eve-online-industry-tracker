"""Task A item 1: blueprints in a pilot's own hangar.

The producer prices rows from character blueprints too (owned_blueprints_
scope, default "all_characters"). The planner now reads them from the same
source (blueprints_repo.get_character_blueprints) and books them in the
ledger bound to their owner: only that pilot can install a job from one
(manufacture, copy, invention, research). Such a job goes to the owner, or
is not planned with the reason "owner <name> has no free slot". Fake ids.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(__file__))

import test_blueprint_ledger as L  # noqa: E402
import test_daily_planner_assigned_inventor as A  # noqa: E402
import test_task_a_release_replan as T  # noqa: E402

from eve_online_industry_tracker.application.daily_planner.blueprint_ledger import BlueprintLedger  # noqa: E402
from eve_online_industry_tracker.application.daily_planner.character_assigner import CharacterAssigner  # noqa: E402
from planner_skill_fakes import NO_SKILL_REQUIREMENTS

OWNER_ID, OTHER_ID = 5, 6


def _owned_bpo(item_id, type_id, owner=OWNER_ID, **levels):
    bpo = L._bpo(item_id, type_id, **levels)
    bpo.character_id = owner
    return bpo


def _owned_bpc(item_id, type_id, runs, owner=OWNER_ID):
    bpc = L._bpc(item_id, type_id, runs)
    bpc.character_id = owner
    return bpc


def _owner(*extra):
    return {"character_id": OWNER_ID, "character_name": "Owner", "skills": {"skills": list(extra)}}


def _other():
    """More free slots of every kind than the owner: picked first for a corp blueprint."""
    return {"character_id": OTHER_ID, "character_name": "Other", "skills": {"skills": [
        A._skill("Mass Production", 3, 3387), A._skill("Laboratory Operation", 3, 3406),
        A._skill("Science A", 5, A.SCIENCE_A), A._skill("Science B", 5, A.SCIENCE_B),
        A._skill("Encryption", 5, A.ENCRYPTION),
    ]}}


def _busy(character_id, activity_id):
    end = datetime.now(tz=timezone.utc).replace(tzinfo=None) + timedelta(days=1)
    return {"character_id": character_id, "activity_id": activity_id, "status": "active", "end_date": end}


def _run(decisions, bpos, bpcs, chars, jobs=()):
    plan = L._planner(0).plan_chain(decisions, {
        "bpo_assets_by_type_id": bpos, "bpc_assets_by_type_id": bpcs,
        "blueprint_data": L.BP_B, "market_depth_cache": A.PRICES, "variant_rows_by_type_id": {},
    })
    actions = CharacterAssigner().assign(plan, list(jobs), A._Chars(chars), A._Admin(), price_map=A.PRICES, blueprint_data=NO_SKILL_REQUIREMENTS)
    return plan, actions


def _who(actions):
    return [(a.action_type, a.type_id, a.character_id) for a in actions]


# --- the ledger knows the owner -----------------------------------------------------

def test_the_ledger_binds_a_character_blueprint_to_its_owner():
    ledger = BlueprintLedger({998: [_owned_bpo(77, 998), L._bpo(78, 998)]}, {998: [_owned_bpc(79, 998, 5)]})
    assert (ledger.owner_of(77), ledger.owner_of(78), ledger.owner_of(79)) == (OWNER_ID, None, OWNER_ID)


# --- manufacture --------------------------------------------------------------------

def test_a_row_priced_from_a_character_bpo_is_built_by_its_owner():
    """The producer priced Widget I from the owner's BPO 77. Other has more
    free manufacturing slots, but only the owner can install it."""
    t1 = L._dec(L._t1_row(20), 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t1], {998: [_owned_bpo(77, 998)]}, {}, [_other(), _owner()])
    assert [(a.action_type, a.character_id, a.runs, a.materials) for a in actions] == [
        ("manufacture", OWNER_ID, 10, {34: 50}),
    ]
    assert "not built" not in t1.decision_reason
    assert t1.overview_row["blueprint_owner_by_item"] == {77: OWNER_ID}


def test_an_owner_without_a_free_slot_leaves_the_row_unbuilt():
    t1 = L._dec(L._t1_row(20), 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t1], {998: [_owned_bpo(77, 998)]}, {}, [_other(), _owner()],
                          jobs=[_busy(OWNER_ID, 1)])
    assert actions == []
    assert "no manufacture today: owner Owner has no free slot" in t1.decision_reason
    assert L._shop(actions, A.PRICES, L.BP_B) == {}


def test_a_corp_bpo_still_goes_to_the_pilot_with_the_most_free_slots():
    """Control: the same row on a corp BPO is Other's."""
    t1 = L._dec(L._t1_row(20), 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t1], {998: [L._bpo(77, 998)]}, {}, [_other(), _owner()])
    assert _who(actions) == [("manufacture", 12344, OTHER_ID)]


# --- research -----------------------------------------------------------------------

def test_research_on_a_character_bpo_goes_to_its_owner():
    t1 = L._dec(L._t1_row(0), 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t1], {998: [_owned_bpo(77, 998, te=0)]}, {}, [_other(), _owner()])
    assert ("te_research", 998, OWNER_ID) in _who(actions)
    assert all(a.character_id == OWNER_ID for a in actions)


def test_research_on_a_character_bpo_is_not_given_to_another_pilot():
    """The owner's research slot is busy: no research (the BPO is not in the
    lab), so the row builds from it, by its owner."""
    t1 = L._dec(L._t1_row(0), 12344, "Widget I", 1.0, 1)
    _plan, actions = _run([t1], {998: [_owned_bpo(77, 998, te=0)]}, {}, [_other(), _owner()],
                          jobs=[_busy(OWNER_ID, 8)])
    assert _who(actions) == [("manufacture", 12344, OWNER_ID)]


# --- copy -----------------------------------------------------------------------------

def test_a_character_t1_bpo_is_copied_by_its_owner():
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    _plan, actions = _run([t2], {998: [_owned_bpo(77, 998)]}, {}, [_other(), _owner()])
    assert _who(actions) == [("copy", 998, OWNER_ID)]


def test_a_character_t1_bpo_whose_owner_is_busy_is_not_copied():
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    _plan, actions = _run([t2], {998: [_owned_bpo(77, 998)]}, {}, [_other(), _owner()],
                          jobs=[_busy(OWNER_ID, 8)])
    assert actions == []
    assert "owner Owner has no free slot" in t2.decision_reason


# --- invention ------------------------------------------------------------------------

def _owner_inventor(*extra):
    return _owner(A._skill("Science A", 3, A.SCIENCE_A), A._skill("Science B", 3, A.SCIENCE_B),
                  A._skill("Encryption", 3, A.ENCRYPTION), *extra)


def test_a_character_t1_bpc_is_invented_from_by_its_owner():
    """Other is the better inventor, but the BPC is the owner's."""
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    _plan, actions = _run([t2], {}, {998: [_owned_bpc(701, 998, 10)]}, [_other(), _owner_inventor()])
    assert [(a.action_type, a.character_id, a.blueprint_item_id) for a in actions] == [
        ("invent", OWNER_ID, 701),
    ]


def test_a_character_t1_bpc_whose_owner_is_busy_is_not_invented_from():
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    _plan, actions = _run([t2], {}, {998: [_owned_bpc(701, 998, 10)]}, [_other(), _owner_inventor()],
                          jobs=[_busy(OWNER_ID, 8)])
    assert actions == []
    assert "no invention today: owner Owner has no free slot" in t2.decision_reason


def test_corp_and_owner_bpcs_split_over_the_right_pilots():
    """A corp BPC and the owner's BPC: Other invents on the corp one, the
    owner on its own."""
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    row = t2.overview_row
    row["manufacturing_job"]["activity_breakdown"]["invention"]["whole_attempts"] = 20
    _plan, actions = _run([t2], {}, {998: [L._bpc(700, 998, 10), _owned_bpc(701, 998, 10)]},
                          [_other(), _owner_inventor()])
    assert sorted((a.blueprint_item_id, a.character_id) for a in actions if a.action_type == "invent") == [
        (700, OTHER_ID), (701, OWNER_ID),
    ]


# --- sub-builds use corp BPOs only ------------------------------------------------------

def test_a_component_with_only_a_character_bpo_is_bought_not_sub_built():
    parent = L._dec(L.R._row(**L.R.BPO_ROW), 12345, "Thing", 2.0, 1)
    plan = L._planner().plan_chain([parent], {
        "bpo_assets_by_type_id": {999: [L._bpo(1, 999)], 888: [_owned_bpo(2, 888)]},
        "blueprint_data": L.R.BLUEPRINT_DATA, "market_depth_cache": L.R.MARKET, "variant_rows_by_type_id": {},
    })
    assert [d.type_id for d in plan.decisions if d.is_sub_component] == []


# --- phase 1 reads the character blueprints -------------------------------------------

class _Query:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *_a):
        return self

    def all(self):
        return list(self._rows)


def test_phase_1_reads_the_character_blueprints_from_the_producers_source():
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService

    rows = [SimpleNamespace(item_id=77, type_id=998, character_id=OWNER_ID, is_blueprint_copy=False)]
    closed = []
    session = SimpleNamespace(query=lambda _m: _Query(rows), close=lambda: closed.append(True))
    svc = DailyPlannerService.__new__(DailyPlannerService)
    svc._session_provider = SimpleNamespace(app_session=lambda: session)
    svc._character_assigner = CharacterAssigner()
    svc._characters = SimpleNamespace(list_characters=lambda: [{"character_id": OWNER_ID}])
    assert svc._get_character_blueprint_assets() == rows
    assert closed == [True]


def test_phase_1_skips_the_blueprints_of_a_pilot_the_app_no_longer_knows(session_provider):
    """The producer's rule (IndustryService._resolve_scope_owner_ids): only the
    known pilots' blueprints count. A removed pilot's asset rows stay in the
    table, but the planner must not plan from them."""
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService
    from eve_online_industry_tracker.db_models import CharacterAssetsModel

    session = session_provider.app_session()
    session.add_all([
        CharacterAssetsModel(character_id=character_id, item_id=item_id, type_id=998, quantity=1,
                             location_id=6001, type_category_name="Blueprint", is_blueprint_copy=False,
                             is_singleton=True, is_container=False, is_asset_safety_wrap=False,
                             is_ship=False, is_office_folder=False)
        for character_id, item_id in ((OWNER_ID, 77), (OWNER_ID + 1, 78))
    ])
    session.commit()
    session.close()
    svc = DailyPlannerService.__new__(DailyPlannerService)
    svc._session_provider = session_provider
    svc._character_assigner = CharacterAssigner()
    svc._characters = SimpleNamespace(list_characters=lambda: [{"character_id": OWNER_ID}])
    assert [row.item_id for row in svc._get_character_blueprint_assets()] == [77]


def test_phase_1_reads_only_the_corp_assets_of_the_known_pilots_corporations(session_provider):
    """The producer's rule for the "all" scopes: the known pilots'
    corporations. A corporation no known pilot is in any more keeps its asset
    rows, but the planner must not count its blueprints or stock."""
    from eve_online_industry_tracker.application.daily_planner.service import DailyPlannerService
    from eve_online_industry_tracker.db_models import CorporationAssetsModel

    session = session_provider.app_session()
    session.add_all([
        CorporationAssetsModel(corporation_id=corporation_id, item_id=item_id, type_id=34, quantity=100,
                               location_id=6001, type_category_name="Material", is_blueprint_copy=False,
                               is_singleton=False)
        for corporation_id, item_id in ((5001, 87), (5002, 88))
    ])
    session.commit()
    session.close()
    svc = DailyPlannerService.__new__(DailyPlannerService)
    svc._session_provider = session_provider
    svc._character_assigner = CharacterAssigner()
    svc._characters = SimpleNamespace(list_characters=lambda: [{"character_id": OWNER_ID, "corporation_id": 5001}])
    assert [row.item_id for row in svc._get_corp_assets()] == [87]


def test_character_blueprints_merge_into_the_corp_index():
    from eve_online_industry_tracker.application.daily_planner.service import merge_blueprint_indexes

    corp = ({998: ["corp bpo"]}, {998: ["corp bpc"]})
    chars = ({998: ["char bpo"], 999: ["char bpo 2"]}, {})
    assert merge_blueprint_indexes(corp, chars) == (
        {998: ["corp bpo", "char bpo"], 999: ["char bpo 2"]}, {998: ["corp bpc"]},
    )


def test_a_copy_prefers_a_corp_t1_bpo_over_a_pilots_own():
    """A pilot's BPO (77) and a corp BPO (78): the copy takes the corp one,
    which any pilot can install, so the owner's busy lab blocks nothing."""
    t2 = L._dec(L._t2_row(), 12345, "Widget II", 2.0, 2, "invention")
    _plan, actions = _run([t2], {998: [_owned_bpo(77, 998), L._bpo(78, 998)]}, {}, [_other(), _owner()],
                          jobs=[_busy(OWNER_ID, 8)])
    assert _who(actions) == [("copy", 998, OTHER_ID)]
    assert t2.overview_row["t1_bpo_item_ids"] == [78]


def _released_pair(owner):
    high = A._decision(T._t2_row(12345, A.ENCRYPTION, [[701, 10]]), type_id=12345, score=2.0)
    high.overview_row["blueprint_owner_by_item"] = {701: owner}
    low = A._decision(T._t2_row(12346, T.OTHER_ENCRYPTION, []), type_id=12346, score=1.0)
    return high, low


def test_a_released_pilot_bpc_stays_bound_to_its_owner():
    """The higher row releases the owner's BPC 701; Lo can run the lower
    row's invention but does not own 701, so nobody invents from it."""
    high, low = _released_pair(OWNER_ID)
    actions = T._assign([high, low], [T._pilot(2, "Lo", T.OTHER_ENCRYPTION), _owner()])
    assert actions == []
    assert "its T1 BPC(s) 701 stay unused today" in high.decision_reason


def test_a_released_pilot_bpc_goes_to_its_owner_on_a_lower_row():
    high, low = _released_pair(2)
    actions = T._assign([high, low], [T._pilot(2, "Lo", T.OTHER_ENCRYPTION)])
    assert [(a.action_type, a.type_id, a.character_id, a.blueprint_item_id) for a in actions] == [
        ("invent", 12346, 2, 701),
    ]
