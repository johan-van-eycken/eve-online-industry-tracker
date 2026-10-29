from __future__ import annotations

from types import MethodType, SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from eve_online_industry_tracker.application.industry.service import IndustryService  # noqa: E402
from eve_online_industry_tracker.db_models import BaseApp, BaseSde, Blueprints  # noqa: E402


class _SessionProvider:
    def __init__(self, app_factory, sde_factory):
        self._app_factory = app_factory
        self._sde_factory = sde_factory

    def app_session(self) -> Session:
        return self._app_factory()

    def sde_session(self) -> Session:
        return self._sde_factory()

    def oauth_session(self) -> Session:
        raise RuntimeError("oauth session not needed in this test")


class _CharManager:
    def __init__(self, characters):
        self._characters = characters

    def get_characters(self):
        return list(self._characters)


def _make_sessions() -> tuple[sessionmaker, sessionmaker]:
    app_engine = create_engine("sqlite:///:memory:")
    sde_engine = create_engine("sqlite:///:memory:")
    BaseApp.metadata.create_all(bind=app_engine)
    BaseSde.metadata.create_all(bind=sde_engine)
    return sessionmaker(bind=app_engine), sessionmaker(bind=sde_engine)


def _build_service(
    *,
    app_factory,
    sde_factory,
    characters: list[dict],
    skill_levels_by_char: dict[int, dict[int, int]],
) -> IndustryService:
    service = object.__new__(IndustryService)
    service._sessions = _SessionProvider(app_factory, sde_factory)  # type: ignore[attr-defined]
    service._state = SimpleNamespace(  # type: ignore[attr-defined]
        esi_service=None,
        char_manager=_CharManager(characters),
    )

    # Mock _get_character_trained_skill_levels
    def _mock_get_skill_levels(self_inner, *, character_id: int) -> dict[int, int]:
        return dict(skill_levels_by_char.get(character_id, {}))

    service._get_character_trained_skill_levels = MethodType(_mock_get_skill_levels, service)  # type: ignore[attr-defined]

    return service


def _add_blueprint(sde_factory, blueprint_type_id: int, mfg_skills: list[dict], *, activities=None) -> None:
    """Insert a Blueprints row with the given manufacturing skill requirements,
    in the SDE's shape: activities keyed by name ("manufacturing", "reaction",
    ...), each with raw {typeID, level} skills."""
    if activities is None:
        activities = {"manufacturing": {"skills": mfg_skills, "time": 600}}
    session = sde_factory()
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

def test_skill_qual_all_trained() -> None:
    """Character has all required skills at the required level -> True."""
    app_factory, sde_factory = _make_sessions()
    _add_blueprint(sde_factory, blueprint_type_id=1001, mfg_skills=[
        {"typeID": 3380, "level": 3},
        {"typeID": 3388, "level": 2},
    ])

    service = _build_service(
        app_factory=app_factory,
        sde_factory=sde_factory,
        characters=[{"character_id": 42, "character_name": "Builder"}],
        skill_levels_by_char={42: {3380: 3, 3388: 5}},
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[1001])

    assert result == {1001: {42: True}}


def test_skill_qual_missing_skill() -> None:
    """Character is missing one required skill entirely -> False."""
    app_factory, sde_factory = _make_sessions()
    _add_blueprint(sde_factory, blueprint_type_id=2001, mfg_skills=[
        {"typeID": 3380, "level": 1},
        {"typeID": 9999, "level": 1},  # character doesn't have this skill
    ])

    service = _build_service(
        app_factory=app_factory,
        sde_factory=sde_factory,
        characters=[{"character_id": 10, "character_name": "Rookie"}],
        skill_levels_by_char={10: {3380: 5}},  # 9999 not trained
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[2001])

    assert result == {2001: {10: False}}


def test_skill_qual_skill_too_low() -> None:
    """Skill trained but below the required level -> False."""
    app_factory, sde_factory = _make_sessions()
    _add_blueprint(sde_factory, blueprint_type_id=3001, mfg_skills=[
        {"typeID": 3380, "level": 5},
    ])

    service = _build_service(
        app_factory=app_factory,
        sde_factory=sde_factory,
        characters=[{"character_id": 7, "character_name": "Partial"}],
        skill_levels_by_char={7: {3380: 4}},  # trained to 4, need 5
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[3001])

    assert result == {3001: {7: False}}


def test_skill_qual_no_sde_blueprint() -> None:
    """Blueprint not in SDE -> its requirements are unknown -> nobody qualifies
    (never plan what cannot be executed; it used to count as none required)."""
    app_factory, sde_factory = _make_sessions()
    # No blueprint inserted for type_id 9999

    service = _build_service(
        app_factory=app_factory,
        sde_factory=sde_factory,
        characters=[
            {"character_id": 1, "character_name": "A"},
            {"character_id": 2, "character_name": "B"},
        ],
        skill_levels_by_char={1: {}, 2: {}},
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[9999])

    assert result == {9999: {1: False, 2: False}}


def test_skill_qual_empty_input() -> None:
    """Empty input list -> return empty dict."""
    app_factory, sde_factory = _make_sessions()

    service = _build_service(
        app_factory=app_factory,
        sde_factory=sde_factory,
        characters=[{"character_id": 1, "character_name": "A"}],
        skill_levels_by_char={1: {3380: 5}},
    )

    result = service.get_blueprint_skill_qualification(blueprint_type_ids=[])

    assert result == {}


def test_skill_qual_one_pilot_lacks_a_skill_the_other_has() -> None:
    """Plan 21's Capital Algid blueprint: Energy Weapon Rigging 4. A pilot at 3
    does not qualify, one at 4 does."""
    app_factory, sde_factory = _make_sessions()
    _add_blueprint(sde_factory, blueprint_type_id=31429, mfg_skills=[
        {"typeID": 3380, "level": 5},
        {"typeID": 26258, "level": 4},
    ])
    service = _build_service(
        app_factory=app_factory,
        sde_factory=sde_factory,
        characters=[{"character_id": 1, "character_name": "At3"}, {"character_id": 2, "character_name": "At4"}],
        skill_levels_by_char={1: {3380: 5, 26258: 3}, 2: {3380: 5, 26258: 4}},
    )

    assert service.get_blueprint_skill_qualification(blueprint_type_ids=[31429]) == {31429: {1: False, 2: True}}


def test_skill_qual_a_reaction_formula_is_checked_on_its_reaction_skills() -> None:
    app_factory, sde_factory = _make_sessions()
    _add_blueprint(sde_factory, blueprint_type_id=46166, mfg_skills=[], activities={
        "reaction": {"skills": [{"typeID": 45746, "level": 2}], "time": 3600},
    })
    service = _build_service(
        app_factory=app_factory,
        sde_factory=sde_factory,
        characters=[{"character_id": 1, "character_name": "R1"}, {"character_id": 2, "character_name": "R2"}],
        skill_levels_by_char={1: {45746: 1}, 2: {45746: 2}},
    )

    assert service.get_blueprint_skill_qualification(blueprint_type_ids=[46166]) == {46166: {1: False, 2: True}}


def test_skill_qual_a_blueprint_without_a_manufacturing_activity_qualifies_nobody() -> None:
    app_factory, sde_factory = _make_sessions()
    _add_blueprint(sde_factory, blueprint_type_id=5001, mfg_skills=[], activities={"copying": {"time": 60}})
    service = _build_service(
        app_factory=app_factory,
        sde_factory=sde_factory,
        characters=[{"character_id": 1, "character_name": "A"}],
        skill_levels_by_char={1: {3380: 5}},
    )

    assert service.get_blueprint_skill_qualification(blueprint_type_ids=[5001]) == {5001: {1: False}}


def test_skill_qual_a_pilot_whose_skills_cannot_be_read_does_not_qualify() -> None:
    from eve_online_industry_tracker.application.errors import ServiceError

    app_factory, sde_factory = _make_sessions()
    _add_blueprint(sde_factory, blueprint_type_id=6001, mfg_skills=[])  # no skill required
    service = _build_service(
        app_factory=app_factory,
        sde_factory=sde_factory,
        characters=[{"character_id": 1, "character_name": "Unread"}, {"character_id": 2, "character_name": "Read"}],
        skill_levels_by_char={2: {}},
    )

    def _levels(self_inner, *, character_id: int) -> dict[int, int]:
        if character_id == 1:
            raise ServiceError("fake: skills not readable", status_code=400)
        return {}

    service._get_character_trained_skill_levels = MethodType(_levels, service)  # type: ignore[attr-defined]

    assert service.get_blueprint_skill_qualification(blueprint_type_ids=[6001]) == {6001: {1: False, 2: True}}
