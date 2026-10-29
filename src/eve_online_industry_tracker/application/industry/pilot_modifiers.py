"""IndustryService: pilot skills, implants, slots and the time/material reductions they give.

A mixin of IndustryService (service.py); it holds no state of its own."""

from __future__ import annotations

import logging
import json
import math
import time
from collections.abc import Mapping
from typing import Any

from sqlalchemy import bindparam, text
from eve_online_industry_tracker.db_models import Blueprints
from eve_online_industry_tracker.application.errors import ServiceError
from eve_online_industry_tracker.infrastructure.industry_adapter import get_type_data
from eve_online_industry_tracker.application.industry.research_time import research_skill_reductions
from eve_online_industry_tracker.application.industry.invention_model import CharacterSlotCapacity
from eve_online_industry_tracker.application.industry.character_slots import (
    activity_skill_requirements,
    meets_skill_levels,
    required_skill_levels,
    research_slot_capacity,
    skill_levels_by_name as _character_skill_levels_by_name,
)
from eve_online_industry_tracker.infrastructure.sde.blueprints import round_material_quantity
from eve_online_industry_tracker.infrastructure.sde.skills import get_encryption_skill_type_ids


# The service module's logger: it carries the QuietWarningsFilter (see service.py).
logger = logging.getLogger("eve_online_industry_tracker.application.industry.service")


class PilotModifiersMixin:
    _INDUSTRY_CHARACTER_MODIFIER_SKILL_NAMES: tuple[str, ...] = (
        "Industry",
        "Advanced Industry",
        "Science",
        "Research",
        "Metallurgy",
        "Laboratory Operation",
        "Advanced Laboratory Operation",
        "Mass Production",
        "Advanced Mass Production",
    )

    def _get_character(self, *, character_id: int) -> Any:
        if not character_id:
            raise ServiceError("Character ID is required.", status_code=400)

        character = self._state.char_manager.get_character_by_id(int(character_id))
        if not character:
            raise ServiceError(f"Character ID {character_id} not found", status_code=400)
        return character

    @staticmethod
    def _classify_industry_modifier_attribute(attribute_name: str) -> tuple[str | None, str]:
        normalized = str(attribute_name or "").strip().lower()
        if not normalized:
            return None, "other"

        if "manufact" in normalized:
            activity = "manufacturing"
        elif "invent" in normalized:
            activity = "invention"
        elif "copy" in normalized:
            activity = "copying"
        elif "research" in normalized and "material" in normalized:
            activity = "research_me"
        elif "research" in normalized and "time" in normalized:
            activity = "research_te"
        elif "research" in normalized:
            activity = "research"
        else:
            activity = None

        if any(token in normalized for token in ["material", "efficiency", "waste"]):
            metric = "material"
        elif any(token in normalized for token in ["time", "duration"]):
            metric = "time"
        elif any(token in normalized for token in ["cost", "installation", "isk"]):
            metric = "cost"
        else:
            metric = "other"

        return activity, metric

    @classmethod
    def _is_industry_modifier_attribute(cls, attribute_name: str) -> bool:
        normalized = str(attribute_name or "").strip().lower()
        if not normalized:
            return False
        return any(
            token in normalized
            for token in ["manufact", "copy", "invent", "research", "material", "time", "cost", "installation"]
        )

    def _get_character_implant_industry_modifiers(self, *, character_id: int) -> list[dict[str, Any]]:
        character = self._get_character(character_id=character_id)
        raw_implants = getattr(character, "implants", None) or []

        implant_type_ids: list[int] = []
        for raw_implant_id in raw_implants:
            try:
                implant_type_id = int(raw_implant_id)
            except (TypeError, ValueError):
                continue
            if implant_type_id > 0:
                implant_type_ids.append(implant_type_id)
        implant_type_ids = sorted(set(implant_type_ids))
        if not implant_type_ids:
            return []

        session: Any = self._sessions.sde_session()
        language = getattr(getattr(self._state, "db_sde", None), "language", None) or "en"
        type_map = get_type_data(session, language, implant_type_ids)

        dogma_rows = session.execute(
            text("SELECT id, dogmaAttributes FROM typeDogma WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
            {"ids": implant_type_ids},
        ).fetchall()

        attr_map_by_type_id: dict[int, dict[int, float]] = {}
        attribute_ids: set[int] = set()
        for type_id, attributes_raw in dogma_rows:
            attr_map: dict[int, float] = {}
            try:
                attributes = json.loads(attributes_raw) if isinstance(attributes_raw, str) else (attributes_raw or [])
            except (TypeError, ValueError) as exc:
                logger.warning("Implants: the dogma attributes of type %s cannot be parsed (%s); its industry bonuses "
                               "are left out", type_id, exc)
                attributes = []
            for attribute in attributes or []:
                if not isinstance(attribute, dict):
                    continue
                attribute_id = attribute.get("attributeID")
                raw_value = attribute.get("value")
                if attribute_id is None or raw_value is None:
                    continue
                try:
                    parsed_attribute_id = int(attribute_id)
                    parsed_value = float(raw_value)
                except (TypeError, ValueError):
                    continue
                attr_map[parsed_attribute_id] = parsed_value
                attribute_ids.add(parsed_attribute_id)
            attr_map_by_type_id[int(type_id)] = attr_map

        attribute_name_by_id: dict[int, str] = {}
        if attribute_ids:
            attribute_rows = session.execute(
                text("SELECT id, name FROM dogmaAttributes WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                {"ids": sorted(attribute_ids)},
            ).fetchall()
            attribute_name_by_id = {int(row[0]): str(row[1] or "") for row in attribute_rows}

        implants_out: list[dict[str, Any]] = []
        for implant_type_id in implant_type_ids:
            type_payload = type_map.get(int(implant_type_id)) or {}
            modifier_entries: list[dict[str, Any]] = []
            for attribute_id, raw_value in (attr_map_by_type_id.get(int(implant_type_id)) or {}).items():
                attribute_name = attribute_name_by_id.get(int(attribute_id)) or ""
                if not self._is_industry_modifier_attribute(attribute_name):
                    continue
                activity, metric = self._classify_industry_modifier_attribute(attribute_name)
                if activity is None:
                    continue
                if metric not in {"material", "time", "cost"}:
                    continue
                modifier_entries.append(
                    {
                        "attribute_id": int(attribute_id),
                        "attribute_name": attribute_name,
                        "activity": activity,
                        "metric": metric,
                        "value": float(raw_value),
                    }
                )

            if not modifier_entries:
                continue

            implants_out.append(
                {
                    "type_id": int(implant_type_id),
                    "type_name": type_payload.get("type_name") or str(implant_type_id),
                    "modifiers": sorted(
                        modifier_entries,
                        key=lambda entry: (
                            str(entry.get("activity") or ""),
                            str(entry.get("metric") or ""),
                            str(entry.get("attribute_name") or ""),
                        ),
                    ),
                }
            )

        return implants_out

    def _get_character_industry_modifier_payload(self, *, character_id: int | None) -> dict[str, Any] | None:
        if character_id is None:
            return None

        character = self._get_character(character_id=int(character_id))
        skills_payload = getattr(character, "skills", None) or {}
        skill_entries = skills_payload.get("skills") if isinstance(skills_payload, dict) else []
        if not isinstance(skill_entries, list):
            skill_entries = []

        modifier_skill_names = {name.lower(): name for name in self._INDUSTRY_CHARACTER_MODIFIER_SKILL_NAMES}
        modifier_skills: list[dict[str, Any]] = []
        for entry in skill_entries:
            if not isinstance(entry, dict):
                continue
            skill_name = str(entry.get("skill_name") or entry.get("type_name") or "").strip()
            if not skill_name or skill_name.lower() not in modifier_skill_names:
                continue
            modifier_skills.append(
                {
                    "type_id": int(entry.get("skill_id") or entry.get("type_id") or 0),
                    "type_name": skill_name,
                    "trained_skill_level": int(entry.get("trained_skill_level") or 0),
                    "group_name": entry.get("group_name"),
                }
            )

        modifier_skills.sort(key=lambda entry: str(entry.get("type_name") or ""))

        implants = self._get_character_implant_industry_modifiers(character_id=int(character_id))
        return {
            "character_id": int(getattr(character, "character_id", None) or character_id),
            "character_name": getattr(character, "character_name", None),
            "modifier_skills": modifier_skills,
            "implant_count": len(implants),
            "implants": implants,
        }

    def _corp_characters(self) -> list[dict[str, Any]]:
        """The character manager's characters, [] when there are none."""
        char_manager = getattr(self._state, "char_manager", None)
        if char_manager is None:
            return []
        try:
            characters = char_manager.get_characters() or []
        except ValueError:  # CharacterManager: no characters configured yet
            return []
        return [c for c in characters if isinstance(c, dict)]

    @staticmethod
    def _loaded_skill_levels(char: dict[str, Any]) -> tuple[int, dict[int, int]] | None:
        """(character_id, {skill type_id: trained level}), or None when the
        character has no loaded skill list (unknown, never level 0)."""
        skills = char.get("skills")
        entries = skills.get("skills") if isinstance(skills, dict) else None
        try:
            character_id = int(char.get("character_id") or 0)
        except (TypeError, ValueError):
            return None
        if character_id <= 0 or not isinstance(entries, list) or not entries:
            return None
        levels: dict[int, int] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                skill_id = int(entry.get("skill_id") or 0)
                level = int(entry.get("trained_skill_level") or 0)
            except (TypeError, ValueError):
                continue
            if skill_id > 0:
                levels[skill_id] = max(level, levels.get(skill_id, 0))
        return character_id, levels

    def _get_inventor_skill_levels(self) -> dict[int, dict[int, int]]:
        """{character_id: {skill type_id: trained level}} for every corp
        character whose skills are loaded. A character without a loaded skill
        list is unknown and left out, never read as level 0. A character with
        industry_enabled=False (excluded from industry by the user, the same
        flag the UI filters on) is never the inventor. No WARNING here:
        resolve_invention_chance warns once when nobody is left."""
        out: dict[int, dict[int, int]] = {}
        for char in self._corp_characters():
            if not char.get("industry_enabled", True):
                continue
            loaded = self._loaded_skill_levels(char)
            if loaded is not None:
                out[loaded[0]] = loaded[1]
        return out

    def _get_corp_slot_capacity(self) -> dict[int, CharacterSlotCapacity]:
        """{character_id: total research slots + skill levels by name} for
        every industry-enabled corp character: the daily planner's slot count
        (character_slots.research_slot_capacity, as in
        CharacterAssigner._compute_available_slots) before it deducts active
        jobs. Total slots, not free ones: the parallel invention lanes are
        steady-state capacity (Task 12)."""
        out: dict[int, CharacterSlotCapacity] = {}
        for char in self._corp_characters():
            if not char.get("industry_enabled", True):
                continue
            try:
                character_id = int(char.get("character_id") or char.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if character_id <= 0:
                continue
            levels = _character_skill_levels_by_name(char)
            out[character_id] = CharacterSlotCapacity(
                research_slot_capacity(levels), levels, self._pilot_industry_payload(character_id),
            )
        return out

    def _pilot_industry_payload(self, character_id: int) -> dict[str, Any] | None:
        """A corp pilot's own industry payload (skills and implants), the
        one convention for "another pilot" (Task 12 fix round 1). None, with
        a WARNING, when the character cannot be read: its lane is then timed
        at its skills without implants (longer, never understated)."""
        try:
            return self._get_character_industry_modifier_payload(character_id=int(character_id))
        except ServiceError as exc:
            logger.warning(
                "Product overview: no industry payload for character %s (%s); its parallel lanes are "
                "timed at its skills without implants", character_id, exc,
            )
            return None

    def _load_encryption_skill_type_ids(self) -> frozenset[int]:
        """The SDE's encryption skills; empty without SDE access (every chance
        is then the base chance, warned once by resolve_invention_chance)."""
        if self._sessions is None:
            return frozenset()
        session = self._sessions.sde_session()
        try:
            return get_encryption_skill_type_ids(session)
        finally:
            session.close()

    def _get_character_trained_skill_levels(self, *, character_id: int) -> dict[int, int]:
        character = self._get_character(character_id=character_id)

        skills_payload = getattr(character, "skills", None) or {}
        if not isinstance(skills_payload, dict):
            return {}

        skill_entries = skills_payload.get("skills") or []
        if not isinstance(skill_entries, list):
            return {}

        trained_skill_levels: dict[int, int] = {}
        for entry in skill_entries:
            if not isinstance(entry, dict):
                continue
            try:
                skill_type_id = int(entry.get("skill_id") or entry.get("type_id") or 0)
            except (TypeError, ValueError):
                continue
            if skill_type_id <= 0:
                continue
            try:
                trained_skill_level = int(entry.get("trained_skill_level") or 0)
            except (TypeError, ValueError):
                trained_skill_level = 0
            trained_skill_levels[skill_type_id] = max(trained_skill_level, trained_skill_levels.get(skill_type_id, 0))

        return trained_skill_levels

    def _get_character_standing(self, *, character_id: int, from_type: str, from_id: int | None) -> float:
        if not from_id:
            return 0.0

        character = self._get_character(character_id=character_id)
        standings = getattr(character, "standings", None)
        if not isinstance(standings, list):
            return 0.0

        for standing in standings:
            if not isinstance(standing, dict):
                continue
            try:
                if str(standing.get("from_type") or "") != str(from_type):
                    continue
                if int(standing.get("from_id") or 0) != int(from_id):
                    continue
                return float(standing.get("standing") or 0.0)
            except (TypeError, ValueError):
                continue
        return 0.0

    @staticmethod
    def _normalize_fraction(value: Any) -> float:
        try:
            normalized = abs(float(value or 0.0))
        except (TypeError, ValueError):
            return 0.0
        while normalized >= 1.0:
            normalized /= 100.0
        return max(0.0, min(normalized, 0.99))

    @classmethod
    def _combine_reductions(cls, reductions: list[Any]) -> float:
        multiplier = 1.0
        for raw_reduction in reductions:
            reduction = cls._normalize_fraction(raw_reduction)
            if reduction <= 0.0:
                continue
            multiplier *= max(0.0, 1.0 - reduction)
        return max(0.0, min(1.0 - multiplier, 0.99))

    @staticmethod
    def _round_duration_seconds(raw_seconds: float) -> int:
        if raw_seconds <= 0:
            return 0
        return max(1, int(math.ceil(raw_seconds)))

    _round_material_quantity = staticmethod(round_material_quantity)

    @staticmethod
    def _skill_levels_by_name(character_modifier_payload: dict[str, Any] | None) -> dict[str, int]:
        if not isinstance(character_modifier_payload, dict):
            return {}
        out: dict[str, int] = {}
        for entry in character_modifier_payload.get("modifier_skills") or []:
            if not isinstance(entry, dict):
                continue
            skill_name = str(entry.get("type_name") or "").strip()
            if not skill_name:
                continue
            try:
                trained_skill_level = int(entry.get("trained_skill_level") or 0)
            except (TypeError, ValueError):
                trained_skill_level = 0
            out[skill_name] = max(trained_skill_level, out.get(skill_name, 0))
        return out

    @classmethod
    def _manufacturing_required_skill_time_reduction(cls, required_skill_entries: list[dict[str, Any]]) -> float:
        reductions: list[float] = []
        for entry in required_skill_entries:
            if not isinstance(entry, dict):
                continue
            skill_name = str(entry.get("type_name") or "").strip()
            if skill_name in {"Industry", "Advanced Industry"}:
                continue
            try:
                trained_skill_level = int(entry.get("trained_skill_level") or 0)
            except (TypeError, ValueError):
                trained_skill_level = 0
            if trained_skill_level <= 0:
                continue
            reductions.append(0.01 * trained_skill_level)
        return cls._combine_reductions(reductions)

    @classmethod
    def _skill_time_reduction(
        cls,
        *,
        activity: str,
        skill_levels_by_name: dict[str, int],
        required_skill_entries: list[dict[str, Any]] | None = None,
    ) -> float:
        reductions: list[float] = []
        if activity in {"manufacturing", "reaction"}:
            reductions.append(0.04 * int(skill_levels_by_name.get("Industry", 0) or 0))
            reductions.append(0.03 * int(skill_levels_by_name.get("Advanced Industry", 0) or 0))
            reductions.append(
                cls._manufacturing_required_skill_time_reduction(required_skill_entries or [])
            )
        elif activity == "copying":
            reductions.append(0.05 * int(skill_levels_by_name.get("Science", 0) or 0))
            reductions.append(0.03 * int(skill_levels_by_name.get("Advanced Industry", 0) or 0))
        elif activity in ("research_material", "research_time"):
            # The one research-skill table, shared with the daily planner.
            reductions.extend(research_skill_reductions(activity, skill_levels_by_name))
        elif activity == "invention":
            reductions.append(0.03 * int(skill_levels_by_name.get("Advanced Industry", 0) or 0))
        return cls._combine_reductions(reductions)

    @classmethod
    def _implant_reduction(
        cls,
        *,
        character_modifier_payload: dict[str, Any] | None,
        activity: str,
        metric: str,
    ) -> float:
        if not isinstance(character_modifier_payload, dict):
            return 0.0
        reductions: list[float] = []
        valid_activities = set(cls._ACTIVITY_EFFECT_ALIASES.get(activity, (activity,)))
        for implant in character_modifier_payload.get("implants") or []:
            if not isinstance(implant, dict):
                continue
            for modifier in implant.get("modifiers") or []:
                if not isinstance(modifier, dict):
                    continue
                if str(modifier.get("metric") or "") != metric:
                    continue
                modifier_activity = str(modifier.get("activity") or "")
                if modifier_activity not in valid_activities:
                    continue
                reductions.append(modifier.get("value"))
        return cls._combine_reductions(reductions)

    #: The skills that change each activity's duration (_skill_time_reduction).
    _TIME_SKILLS_BY_ACTIVITY: dict[str, tuple[str, ...]] = {
        "manufacturing": ("Industry", "Advanced Industry"),
        "reaction": ("Industry", "Advanced Industry"),
        "copying": ("Science", "Advanced Industry"),
        "research_material": ("Metallurgy", "Advanced Industry"),
        "research_time": ("Research", "Advanced Industry"),
        "invention": ("Advanced Industry",),
    }

    @classmethod
    def _pilot_time_fingerprint(cls, activity: str, payload: Mapping[str, Any]) -> tuple[Any, ...]:
        """A lane pilot's cache key: only what changes `activity`'s duration
        and fee (its time skills, and its implants' time and cost
        reductions for that activity), so pilots with the same figures share
        one cached job."""
        levels = cls._skill_levels_by_name(dict(payload))
        return (
            tuple(int(levels.get(name, 0) or 0) for name in cls._TIME_SKILLS_BY_ACTIVITY.get(activity, ())),
            cls._implant_reduction(character_modifier_payload=dict(payload), activity=activity, metric="time"),
            cls._implant_reduction(character_modifier_payload=dict(payload), activity=activity, metric="cost"),
        )

    # EVE skill IDs that grant additional industry slots
    _SKILL_MASS_PRODUCTION = 3387          # +1 manufacturing slot per level
    _SKILL_ADV_MASS_PRODUCTION = 24625     # +1 manufacturing slot per level
    _SKILL_LABORATORY_OPERATION = 3406     # +1 research/copy/invention slot per level
    _SKILL_ADV_LABORATORY_OPERATION = 24624  # +1 research/copy/invention slot per level
    _SKILL_REACTIONS = 45746               # +1 reaction slot per level
    _SKILL_MASS_REACTIONS = 45748          # +1 reaction slot per level

    def _character_slot_capacities(self) -> dict[int, dict[str, int]]:
        """Return max industry slots per character based on skills.

        Slot formula per category:
        - Manufacturing: 1 (base) + Mass Production lvl + Advanced Mass Production lvl
        - Research/Copy/Invention: 1 (base) + Laboratory Operation lvl + Advanced Laboratory Operation lvl
        - Reactions: 1 (base) + Reactions lvl + Mass Reactions lvl
        """
        char_manager = getattr(self._state, "char_manager", None)
        if char_manager is None:
            return {}

        try:
            characters = char_manager.get_characters() or []
        except ValueError:
            return {}  # get_characters: "No characters available in the manager."
        # CharacterDataError (the characters cannot be read) propagates: the slots are planning input.

        slot_skill_ids = {
            self._SKILL_MASS_PRODUCTION,
            self._SKILL_ADV_MASS_PRODUCTION,
            self._SKILL_LABORATORY_OPERATION,
            self._SKILL_ADV_LABORATORY_OPERATION,
            self._SKILL_REACTIONS,
            self._SKILL_MASS_REACTIONS,
        }

        capacities: dict[int, dict[str, int]] = {}
        for char in characters:
            cid = int(char.get("character_id") or 0)
            if cid <= 0:
                continue

            skills_data = char.get("skills") or {}
            skill_list = skills_data.get("skills") or [] if isinstance(skills_data, dict) else []

            skill_levels: dict[int, int] = {}
            for s in skill_list:
                sid = s.get("skill_id")
                if sid in slot_skill_ids:
                    skill_levels[sid] = int(s.get("trained_skill_level") or 0)

            manufacturing_slots = (
                1
                + skill_levels.get(self._SKILL_MASS_PRODUCTION, 0)
                + skill_levels.get(self._SKILL_ADV_MASS_PRODUCTION, 0)
            )
            research_slots = (
                1
                + skill_levels.get(self._SKILL_LABORATORY_OPERATION, 0)
                + skill_levels.get(self._SKILL_ADV_LABORATORY_OPERATION, 0)
            )
            reaction_slots = (
                1
                + skill_levels.get(self._SKILL_REACTIONS, 0)
                + skill_levels.get(self._SKILL_MASS_REACTIONS, 0)
            )

            capacities[cid] = {
                "manufacturing_max": manufacturing_slots,
                "research_max": research_slots,
                "reaction_max": reaction_slots,
            }

        return capacities

    def get_blueprint_skill_qualification(
        self,
        *,
        blueprint_type_ids: list[int],
    ) -> dict[int, dict[int, bool]]:
        """
        For each blueprint_type_id, whether each configured character meets
        the SDE skills of its manufacturing activity (a reaction formula's
        reaction): the shared job skill check (character_slots), the one the
        daily planner assigns jobs by.

        Never plan what cannot be executed: a blueprint not in the SDE, or
        without a manufacturing / reaction activity, qualifies nobody, and a
        character whose skills cannot be read does not qualify (both logged
        at WARNING). The SDE keys activities by name; this used to read
        activities["1"], found nothing, and let every character qualify.

        Returns: {blueprint_type_id: {character_id: bool}}
        """
        if not blueprint_type_ids:
            return {}

        characters = self._state.char_manager.get_characters()
        character_ids: list[int] = [int(c["character_id"]) for c in characters if c.get("character_id")]

        # Trained skill levels per character; None: they cannot be read.
        char_skill_levels: dict[int, dict[int, int] | None] = {}
        for character_id in character_ids:
            try:
                char_skill_levels[character_id] = self._get_character_trained_skill_levels(
                    character_id=character_id
                )
            except ServiceError as exc:
                logger.warning("Blueprint skill qualification: the skills of character %s cannot be read (%s); it "
                               "qualifies for no blueprint", character_id, exc)
                char_skill_levels[character_id] = None

        # The SDE's raw activities, shaped as get_blueprint_manufacturing_data's
        # activity_skills for the shared helper.
        sde_session: Any = self._sessions.sde_session()
        try:
            blueprint_rows = (
                sde_session.query(Blueprints)
                .filter(Blueprints.blueprintTypeID.in_(blueprint_type_ids))
                .all()
            )
        finally:
            close = getattr(sde_session, "close", None)
            if callable(close):
                close()
        blueprint_data: dict[int, dict[str, Any]] = {}
        for bp in blueprint_rows:
            activities = bp.activities if isinstance(bp.activities, dict) else {}
            blueprint_data[int(bp.blueprintTypeID)] = {"activity_skills": {
                name: [
                    {"type_id": skill.get("typeID"), "level": skill.get("level")}
                    for skill in (activity.get("skills") or []) if isinstance(skill, dict)
                ]
                for name, activity in activities.items() if isinstance(activity, dict)
            }}

        result: dict[int, dict[int, bool]] = {}
        for bp_type_id in blueprint_type_ids:
            entries, unknown = activity_skill_requirements(blueprint_data, bp_type_id, "manufacturing")
            if entries is None:
                logger.warning("Blueprint skill qualification: %s; no character qualifies", unknown)
                result[bp_type_id] = {cid: False for cid in character_ids}
                continue
            required_levels = required_skill_levels(entries)
            result[bp_type_id] = {
                cid: char_skill_levels.get(cid) is not None
                and meets_skill_levels(required_levels, char_skill_levels[cid] or {})
                for cid in character_ids
            }

        return result
