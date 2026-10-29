from __future__ import annotations

from types import MethodType, SimpleNamespace


from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402
from eve_online_industry_tracker.db_models import Blueprints
from fakes import _CharManager


def _build_service(
    *,
    sessions,
    characters: list[dict],
    skill_levels_by_char: dict[int, dict[int, int]],
) -> IndustryService:
    service = object.__new__(IndustryService)
    service._sessions = sessions  # type: ignore[attr-defined]
    service._state = SimpleNamespace(  # type: ignore[attr-defined]
        esi_service=None,
        char_manager=_CharManager(characters),
    )

    # Mock _get_character_trained_skill_levels
    def _mock_get_skill_levels(self_inner, *, character_id: int) -> dict[int, int]:
        return dict(skill_levels_by_char.get(character_id, {}))

    service._get_character_trained_skill_levels = MethodType(_mock_get_skill_levels, service)  # type: ignore[attr-defined]

    return service


def _add_blueprint(new_sde_session, blueprint_type_id: int, mfg_skills: list[dict], *, activities=None) -> None:
    """Insert a Blueprints row with the given manufacturing skill requirements,
    in the SDE's shape: activities keyed by name ("manufacturing", "reaction",
    ...), each with raw {typeID, level} skills."""
    if activities is None:
        activities = {"manufacturing": {"skills": mfg_skills, "time": 600}}
    session = new_sde_session()
    session.add(
        Blueprints(
            blueprintTypeID=blueprint_type_id,
            maxProductionLimit=1,
            activities=activities,
        )
    )
    session.commit()
    session.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_skill_qual_all_trained(session_provider) -> None:
    """Character has all required skills at the required level -> True."""
    _add_blueprint(session_provider.sde_session, blueprint_type_id=1001, mfg_skills=[
        {"typeID": 3380, "level": 3},
        {"typeID": 3388, "level": 2},
    ])

    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 42, "character_name": "Builder"}],
        skill_levels_by_char={42: {3380: 3, 3388: 5}},
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[1001])

    assert result == {1001: {42: True}}


def test_skill_qual_missing_skill(session_provider) -> None:
    """Character is missing one required skill entirely -> False."""
    _add_blueprint(session_provider.sde_session, blueprint_type_id=2001, mfg_skills=[
        {"typeID": 3380, "level": 1},
        {"typeID": 9999, "level": 1},  # character doesn't have this skill
    ])

    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 10, "character_name": "Rookie"}],
        skill_levels_by_char={10: {3380: 5}},  # 9999 not trained
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[2001])

    assert result == {2001: {10: False}}


def test_skill_qual_skill_too_low(session_provider) -> None:
    """Skill trained but below the required level -> False."""
    _add_blueprint(session_provider.sde_session, blueprint_type_id=3001, mfg_skills=[
        {"typeID": 3380, "level": 5},
    ])

    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 7, "character_name": "Partial"}],
        skill_levels_by_char={7: {3380: 4}},  # trained to 4, need 5
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[3001])

    assert result == {3001: {7: False}}


def test_skill_qual_no_sde_blueprint(session_provider) -> None:
    """Blueprint not in SDE -> its requirements are unknown -> nobody qualifies
    (never plan what cannot be executed; it used to count as none required)."""
    # No blueprint inserted for type_id 9999

    service = _build_service(
        sessions=session_provider,
        characters=[
            {"character_id": 1, "character_name": "A"},
            {"character_id": 2, "character_name": "B"},
        ],
        skill_levels_by_char={1: {}, 2: {}},
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[9999])

    assert result == {9999: {1: False, 2: False}}


def test_skill_qual_empty_input(session_provider) -> None:
    """Empty input list -> return empty dict."""

    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 1, "character_name": "A"}],
        skill_levels_by_char={1: {3380: 5}},
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[])

    assert result == {}


def test_skill_qual_one_pilot_lacks_a_skill_the_other_has(session_provider) -> None:
    """Plan 21's Capital Algid blueprint: Energy Weapon Rigging 4. A pilot at 3
    does not qualify, one at 4 does."""
    _add_blueprint(session_provider.sde_session, blueprint_type_id=31429, mfg_skills=[
        {"typeID": 3380, "level": 5},
        {"typeID": 26258, "level": 4},
    ])
    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 1, "character_name": "At3"}, {"character_id": 2, "character_name": "At4"}],
        skill_levels_by_char={1: {3380: 5, 26258: 3}, 2: {3380: 5, 26258: 4}},
    )

    assert service.get_blueprint_skill_qualification(blueprint_type_ids=[31429]) == {31429: {1: False, 2: True}}


def test_skill_qual_a_reaction_formula_is_checked_on_its_reaction_skills(session_provider) -> None:
    _add_blueprint(session_provider.sde_session, blueprint_type_id=46166, mfg_skills=[], activities={
        "reaction": {"skills": [{"typeID": 45746, "level": 2}], "time": 3600},
    })
    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 1, "character_name": "R1"}, {"character_id": 2, "character_name": "R2"}],
        skill_levels_by_char={1: {45746: 1}, 2: {45746: 2}},
    )

    assert service.get_blueprint_skill_qualification(blueprint_type_ids=[46166]) == {46166: {1: False, 2: True}}


def test_skill_qual_a_blueprint_without_a_manufacturing_activity_qualifies_nobody(session_provider) -> None:
    _add_blueprint(session_provider.sde_session, blueprint_type_id=5001, mfg_skills=[], activities={"copying": {"time": 60}})
    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 1, "character_name": "A"}],
        skill_levels_by_char={1: {3380: 5}},
    )

    assert service.get_blueprint_skill_qualification(blueprint_type_ids=[5001]) == {5001: {1: False}}


def test_skill_qual_a_pilot_whose_skills_cannot_be_read_does_not_qualify(session_provider) -> None:
    from eve_online_industry_tracker.application.errors import ServiceError

    _add_blueprint(session_provider.sde_session, blueprint_type_id=6001, mfg_skills=[])  # no skill required
    service = _build_service(
        sessions=session_provider,
        characters=[{"character_id": 1, "character_name": "Unread"}, {"character_id": 2, "character_name": "Read"}],
        skill_levels_by_char={2: {}},
    )

    def _levels(self_inner, *, character_id: int) -> dict[int, int]:
        if character_id == 1:
            raise ServiceError("fake: skills not readable", status_code=400)
        return {}

    service._get_character_trained_skill_levels = MethodType(_levels, service)  # type: ignore[attr-defined]

    assert service.get_blueprint_skill_qualification(blueprint_type_ids=[6001]) == {6001: {1: False, 2: True}}
