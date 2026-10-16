"""Engineering and reactor rig effects, read from the SDE's dogma.

Every rig bonus is a dogma effect whose modifierInfo says which attribute
of the structure it modifies (modifiedAttributeID, e.g. 2538 "equipment
manufacture material multiplier") and which attribute of the rig holds the
value (modifyingAttributeID). The value attribute differs per rig family:
2594/2593/2595 for the standard engineering rigs, 2653 for the Thukker
capital-component material bonus, 2714/2713 for the reactor rigs. Reading
it from the effect, not from a fixed list, gives every rig its own value.

The modified structure attribute decides what the effect reduces
(domain.rig_groups.RigCatalog.attribute_bindings: activity, metric and the
CCP target filter, read from the SDE's industryTargetFilters and
industryModifierSources by infrastructure/sde/industry).

The rig's security effect (e.g. 6842) scales its value attributes by the
system's security band (domain.security_band); each effect records whether
it is scaled, and each rig its per-band multipliers.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Iterable

from sqlalchemy import bindparam, text

from eve_online_industry_tracker.domain import rig_groups as _rig_groups
from eve_online_industry_tracker.domain import security_band as _security
from eve_online_industry_tracker.infrastructure.sde import industry as _sde_industry

logger = logging.getLogger(__name__)

# SDE dogma attribute names (ids resolved from the SDE: 2355, 2356, 2357,
# 2358 and 1970 at the time of writing).
_ATTR_HISEC = "hiSecModifier"
_ATTR_LOWSEC = "lowSecModifier"
_ATTR_NULLSEC = "nullSecModifier"
_ATTR_SECURITY = "securityModifier"
_ATTR_DISALLOW_HISEC = "disallowInHighSec"


def _safe_json_loads(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return value


def _rows(session: Any, sql: str, ids: Iterable[Any], key: str = "ids") -> list[Any]:
    values = sorted(set(ids))
    if not values:
        return []
    return session.execute(text(sql).bindparams(bindparam(key, expanding=True)), {key: values}).fetchall()


def _attribute_map(attrs_raw: Any) -> dict[int, float]:
    out: dict[int, float] = {}
    for a in _safe_json_loads(attrs_raw) or []:
        if not isinstance(a, dict) or a.get("attributeID") is None or a.get("value") is None:
            continue
        try:
            out[int(a["attributeID"])] = float(a["value"])
        except (TypeError, ValueError):
            continue
    return out


def _effect_ids(effects_raw: Any) -> list[int]:
    out: list[int] = []
    for e in _safe_json_loads(effects_raw) or []:
        if isinstance(e, dict) and e.get("effectID") is not None:
            try:
                out.append(int(e["effectID"]))
            except (TypeError, ValueError):
                continue
    return out


def _item_modifiers(modifier_info: Any) -> list[dict[str, Any]]:
    return [m for m in (_safe_json_loads(modifier_info) or []) if isinstance(m, dict)]


def get_rig_effects_for_type_ids(
    sde_session: Any, rig_type_ids: Iterable[int], *, catalog: _rig_groups.RigCatalog | None = None,
) -> list[dict]:
    """One entry per rig type: its industry effects and security multipliers.

    Each effect is labelled by `catalog` (default: the SDE's, from
    infrastructure/sde/industry; SdeIndustryDataMissing when the SDE lacks
    the industry tables).

    effects: [{effect_id, effect_name, activity, group, metric, value,
    modified_attribute_id, modifying_attribute_id, security_scaled}], value a
    fraction (0.024 == 2.4 %) before the security multiplier.
    security_multipliers: {band: multiplier, or None where the rig cannot be
    fitted}. material/time/cost_reduction: the largest effect value per
    metric (a summary for display; the producer reads the effects)."""
    ids = sorted({int(x) for x in rig_type_ids if x is not None and int(x) != 0})
    if not ids:
        return []
    bindings = (catalog if catalog is not None else _sde_industry.rig_catalog()).attribute_bindings

    dogma_by_type_id = {
        int(tid): (_attribute_map(attrs_raw), _effect_ids(effects_raw))
        for tid, attrs_raw, effects_raw in _rows(
            sde_session, "SELECT id, dogmaAttributes, dogmaEffects FROM typeDogma WHERE id IN :ids", ids)
    }
    all_effect_ids = {eid for _, effect_ids in dogma_by_type_id.values() for eid in effect_ids}
    effects_by_id = {
        int(eid): (str(name or ""), _item_modifiers(modifier_info))
        for eid, name, modifier_info in _rows(
            sde_session, "SELECT id, name, modifierInfo FROM dogmaEffects WHERE id IN :ids", all_effect_ids)
    }

    named = (_ATTR_HISEC, _ATTR_LOWSEC, _ATTR_NULLSEC, _ATTR_SECURITY, _ATTR_DISALLOW_HISEC)
    attr_rows = _rows(sde_session, "SELECT id, name, defaultValue FROM dogmaAttributes WHERE name IN :names",
                      named, key="names")
    attr_id_by_name = {str(name): int(aid) for aid, name, _ in attr_rows}
    modifying_ids = {
        int(m["modifyingAttributeID"])
        for _, modifiers in effects_by_id.values() for m in modifiers if m.get("modifyingAttributeID") is not None
    }
    default_by_attr_id = {int(aid): float(default or 0.0) for aid, _, default in attr_rows}
    default_by_attr_id.update({
        int(aid): float(default or 0.0)
        for aid, default in _rows(sde_session, "SELECT id, defaultValue FROM dogmaAttributes WHERE id IN :ids",
                                  modifying_ids)
    })
    missing = [n for n in named if n not in attr_id_by_name]
    if missing:
        logger.warning("Rig effects: the SDE has no dogma attribute %s; rig security multipliers are not read",
                       ", ".join(missing))

    def attr(attr_map: dict[int, float], attribute_id: int | None) -> float | None:
        if attribute_id is None:
            return None
        return attr_map.get(attribute_id, default_by_attr_id.get(attribute_id, 0.0))

    rigs_out: list[dict] = []
    for type_id in ids:
        attr_map, effect_ids = dogma_by_type_id.get(int(type_id), ({}, []))
        security_attr_id = attr_id_by_name.get(_ATTR_SECURITY)
        # Value attributes the rig's own security effect multiplies.
        scaled_attr_ids = {
            int(m["modifiedAttributeID"])
            for eid in effect_ids for m in effects_by_id.get(eid, ("", []))[1]
            if security_attr_id is not None and m.get("modifyingAttributeID") == security_attr_id
            and m.get("domain") == "itemID" and m.get("modifiedAttributeID") is not None
        }
        security_multipliers: dict[str, float | None] | None = None
        if scaled_attr_ids and not missing:
            disallow_hisec = bool(attr(attr_map, attr_id_by_name[_ATTR_DISALLOW_HISEC]))
            null = attr(attr_map, attr_id_by_name[_ATTR_NULLSEC])
            security_multipliers = {
                _security.HIGH: None if disallow_hisec else attr(attr_map, attr_id_by_name[_ATTR_HISEC]),
                _security.LOW: attr(attr_map, attr_id_by_name[_ATTR_LOWSEC]),
                _security.NULL: null,
                _security.WORMHOLE: null,
            }

        effects_out: list[dict] = []
        for eid in effect_ids:
            name, modifiers = effects_by_id.get(eid, ("", []))
            for m in modifiers:
                if m.get("func") != "ItemModifier" or m.get("domain") != "structureID":
                    continue
                try:
                    modified = int(m["modifiedAttributeID"])
                    modifying = int(m["modifyingAttributeID"])
                except (KeyError, TypeError, ValueError):
                    continue
                binding = bindings.get(modified)
                if binding is None:
                    continue
                value = max(0.0, -float(attr(attr_map, modifying) or 0.0) / 100.0)
                if value <= 0:
                    continue
                effects_out.append({
                    "effect_id": int(eid),
                    "effect_name": name,
                    "activity": binding.activity,
                    "group": binding.label,
                    "metric": binding.metric,
                    "value": float(value),
                    "modified_attribute_id": modified,
                    "modifying_attribute_id": modifying,
                    "security_scaled": modifying in scaled_attr_ids,
                })

        def summary(metric: str) -> float:
            return max((e["value"] for e in effects_out if e["metric"] == metric), default=0.0)

        rigs_out.append({
            "type_id": int(type_id),
            "time_reduction": summary("time"),
            "material_reduction": summary("material"),
            "cost_reduction": summary("cost"),
            "security_multipliers": security_multipliers,
            "effects": effects_out,
        })

    return rigs_out


def compute_combined_reduction(reductions: Iterable[float]) -> float:
    """Combine multiplicatively: total = 1 - Π(1 - r_i)."""

    mul = 1.0
    for r in reductions:
        try:
            r_f = float(r or 0.0)
        except Exception:
            continue

        # Defensive normalization:
        # - Most of our pipeline uses fractions (0.024 == 2.4%).
        # - Some upstream sources or historical payloads may provide percentages (2.4 == 2.4%).
        while r_f >= 1.0:
            r_f /= 100.0

        if r_f <= 0:
            continue
        mul *= 1.0 - r_f

    total = 1.0 - mul
    return max(0.0, min(total, 1.0))
