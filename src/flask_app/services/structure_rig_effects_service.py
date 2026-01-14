from __future__ import annotations

import json
from typing import Any, Iterable

from sqlalchemy import bindparam, text


_RIG_ATTR_TIME_REDUCTION = 2593
_RIG_ATTR_MATERIAL_REDUCTION = 2594
_RIG_ATTR_COST_REDUCTION = 2595


_RIG_GROUP_TOKEN_LABELS: dict[str, str] = {
    # Manufacturing
    "Equipment": "Modules",
    "Ammo": "Ammo & Charges",
    "Drone": "Drones",
    "Smallship": "Basic Small Ships",
    "Mediumship": "Basic Medium Ships",
    "Mediumships": "Basic Medium Ships",
    "Largeship": "Basic Large Ships",
    "AdvComponent": "Advanced Components",
    "AdvSmship": "Advanced Small Ships",
    "AdvMedShip": "Advanced Medium Ships",
    "AdvLarShip": "Advanced Large Ships",
    "BasCapComp": "Capital Components",
    "CapShip": "Capital Ships",
    "AdvCapComponent": "Advanced Capital Components",
    "Structure": "Structures",
    # Reactions
    "ReactionBio": "Biochemical Reactions",
    "ReactionComp": "Composite Reactions",
    "ReactionHyb": "Hybrid Reactions",
    # Variants
    "ThukkerAdvCapComp": "Advanced Capital Components",
    "ThukkerBasCapComp": "Capital Components",
    "AllShip": "All Ships",
}


def _camel_to_words(s: str) -> str:
    out: list[str] = []
    buf = ""
    for ch in s:
        if buf and ch.isupper() and (not buf[-1].isupper()):
            out.append(buf)
            buf = ch
        else:
            buf += ch
    if buf:
        out.append(buf)
    return " ".join(out).strip()


def _parse_rig_effect(effect_name: str) -> tuple[str | None, str | None, str | None]:
    """Return (activity, group_token, metric) for known rig effects.

    Examples:
      rigAmmoManufactureMaterialBonus -> (manufacturing, Ammo, material)
      rigInventionTimeBonus -> (invention, None, time)
    """

    if not effect_name or not effect_name.startswith("rig"):
        return None, None, None

    metric: str | None
    if ("Material" in effect_name) or ("MatBonus" in effect_name):
        metric = "material"
    elif "Time" in effect_name:
        metric = "time"
    elif "Cost" in effect_name:
        metric = "cost"
    else:
        metric = None

    activity: str | None = None
    group_token: str | None = None

    rest = effect_name[3:]

    # Research ME/TE rigs encode the "activity" in the prefix (rigME* / rigTE*).
    if rest.startswith("ME") and "Research" in rest:
        return "research_me", None, metric
    if rest.startswith("TE") and "Research" in rest:
        return "research_te", None, metric

    # Reaction rigs are named like rigReactionBioMatBonus / rigReactionBioTimeBonus
    if rest.startswith("Reaction"):
        group_part = rest
        for suffix in [
            "MatBonus",
            "TimeBonus",
            "CostBonus",
            "MaterialBonus",
            "TimeBonus",
            "CostBonus",
        ]:
            if suffix in group_part:
                group_part = group_part.split(suffix, 1)[0]
                break
        return "manufacturing", group_part or None, metric

    for token, activity_key in [
        ("Manufacture", "manufacturing"),
        ("Invention", "invention"),
        ("Research", "research"),
        ("Copying", "copying"),
        ("Copy", "copying"),
        ("Reaction", "manufacturing"),
    ]:
        idx = rest.find(token)
        if idx >= 0:
            activity = activity_key
            if idx > 0:
                group_token = rest[:idx]
            break

    return activity, group_token, metric


def _safe_json_loads(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return value


def get_rig_effects_for_type_ids(sde_session, rig_type_ids: Iterable[int]) -> list[dict]:
    """Return rig effects for the provided rig type IDs.

    Output items match the subset shape from `/structure_rigs`:
      {type_id, time_reduction, material_reduction, cost_reduction, effects:[{activity, group, metric, value}, ...]}

    Values are returned as reduction fractions (0.15 means 15% reduction).
    """

    ids = sorted({int(x) for x in rig_type_ids if x is not None and int(x) != 0})
    if not ids:
        return []

    dogma_rows = (
        sde_session.execute(
            text(
                "SELECT id, dogmaAttributes, dogmaEffects FROM typeDogma WHERE id IN :ids"
            ).bindparams(bindparam("ids", expanding=True)),
            {"ids": ids},
        )
        .fetchall()
    )

    dogma_by_type_id: dict[int, dict] = {}
    all_effect_ids: set[int] = set()

    for tid, attrs_raw, effects_raw in dogma_rows:
        attr_map: dict[int, float] = {}
        effect_ids: list[int] = []

        attrs = _safe_json_loads(attrs_raw) or []
        if isinstance(attrs, list):
            for a in attrs:
                if not isinstance(a, dict):
                    continue
                aid = a.get("attributeID")
                val = a.get("value")
                if aid is None or val is None:
                    continue
                try:
                    attr_map[int(aid)] = float(val)
                except Exception:
                    continue

        effs = _safe_json_loads(effects_raw) or []
        if isinstance(effs, list):
            for e in effs:
                if not isinstance(e, dict):
                    continue
                eid = e.get("effectID")
                if eid is None:
                    continue
                try:
                    eid_i = int(eid)
                    effect_ids.append(eid_i)
                    all_effect_ids.add(eid_i)
                except Exception:
                    continue

        dogma_by_type_id[int(tid)] = {"attr_map": attr_map, "effect_ids": effect_ids}

    effect_name_by_id: dict[int, str] = {}
    if all_effect_ids:
        eff_rows = (
            sde_session.execute(
                text("SELECT id, name FROM dogmaEffects WHERE id IN :ids").bindparams(
                    bindparam("ids", expanding=True)
                ),
                {"ids": sorted(all_effect_ids)},
            )
            .fetchall()
        )
        effect_name_by_id = {int(r[0]): (r[1] or "") for r in eff_rows}

    rigs_out: list[dict] = []
    for type_id in ids:
        d = dogma_by_type_id.get(int(type_id)) or {}
        attr_map = d.get("attr_map") or {}

        time_val = attr_map.get(_RIG_ATTR_TIME_REDUCTION, 0.0)
        mat_val = attr_map.get(_RIG_ATTR_MATERIAL_REDUCTION, 0.0)
        cost_val = attr_map.get(_RIG_ATTR_COST_REDUCTION, 0.0)

        time_reduction = max(0.0, (-float(time_val)) / 100.0)
        material_reduction = max(0.0, (-float(mat_val)) / 100.0)
        cost_reduction = max(0.0, (-float(cost_val)) / 100.0)

        effects_out: list[dict] = []
        for eid in d.get("effect_ids") or []:
            ename = effect_name_by_id.get(int(eid)) or ""
            if ename in {"rigSlot"}:
                continue
            if not ename.startswith("rig"):
                continue
            if "Bonus" not in ename:
                continue

            activity, group_token, metric = _parse_rig_effect(ename)
            if activity is None or metric is None:
                continue

            if metric == "material":
                val = material_reduction
            elif metric == "time":
                val = time_reduction
            else:
                val = cost_reduction

            if val <= 0:
                continue

            if not group_token:
                group = "All"
            else:
                group = _RIG_GROUP_TOKEN_LABELS.get(group_token) or _camel_to_words(group_token)

            effects_out.append(
                {
                    "effect_id": int(eid),
                    "effect_name": ename,
                    "activity": activity,
                    "group": group,
                    "metric": metric,
                    "value": float(val),
                }
            )

        rigs_out.append(
            {
                "type_id": int(type_id),
                "time_reduction": float(time_reduction),
                "material_reduction": float(material_reduction),
                "cost_reduction": float(cost_reduction),
                "effects": effects_out,
            }
        )

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
        # If we accidentally treat 2.4 as a fraction, we get negative multipliers and clamp to 100%.
        # Normalize repeatedly to handle values like 240 (meaning 2.40%) as well.
        while r_f >= 1.0:
            r_f /= 100.0

        if r_f <= 0:
            continue
        mul *= 1.0 - r_f
    total = 1.0 - mul
    return max(0.0, min(total, 1.0))


def compute_rig_reduction_for(
    *,
    rigs_payload: list[dict],
    activity: str,
    group: str | None,
    metric: str,
) -> float:
    """Compute the effective rig reduction for one job.

    - activity: manufacturing/invention/copying/research_me/research_te
    - group: group label (e.g. "Modules")
    - metric: material/time/cost

    Rig effects with group == "All" always apply.
    """

    if not rigs_payload:
        return 0.0

    wanted_group = (group or "").strip()

    reductions: list[float] = []
    for rig in rigs_payload:
        for e in (rig.get("effects") or []):
            if not isinstance(e, dict):
                continue
            if str(e.get("activity") or "") != str(activity):
                continue
            if str(e.get("metric") or "") != str(metric):
                continue

            e_group = str(e.get("group") or "").strip()
            if e_group not in {"All", wanted_group}:
                continue

            try:
                reductions.append(float(e.get("value") or 0.0))
            except Exception:
                continue

    return compute_combined_reduction(reductions)
