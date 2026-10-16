"""Service-module fitting data from the SDE (domain.industry_facility)."""
from __future__ import annotations

import json
from typing import Any, Iterable

from sqlalchemy import bindparam, text

_DISALLOW_HISEC = "disallowInHighSec"
_DISALLOW_EMPIRE = "disallowInEmpireSpace"
_REQUIRES_SOV_UPGRADE = ("requiresSovHubUpgrade", "requiresIHubUpgrade")


def _attribute_ids(session: Any) -> tuple[set[int], set[int], dict[str, int]]:
    type_ids: set[int] = set()
    group_ids: set[int] = set()
    named: dict[str, int] = {}
    rows = session.execute(text(
        "SELECT id, name FROM dogmaAttributes WHERE name LIKE 'canFitShip%' "
        "OR name IN ('disallowInHighSec', 'disallowInEmpireSpace', 'requiresSovHubUpgrade', 'requiresIHubUpgrade')"
    )).fetchall()
    for aid, name in rows:
        name = str(name or "")
        if name.startswith("canFitShipType"):
            type_ids.add(int(aid))
        elif name.startswith("canFitShipGroup"):
            group_ids.add(int(aid))
        else:
            named[name] = int(aid)
    return type_ids, group_ids, named


def get_service_module_fits(session: Any, service_type_ids: Iterable[int]) -> dict[int, dict[str, Any]]:
    """{service module type id: {"structure_type_ids", "structure_group_ids",
    "disallow_in_high_sec", "disallow_in_empire_space", "requires_sov_upgrade"}}
    from each module's canFitShipType* / canFitShipGroup* and onlining
    attributes. A module missing from the SDE is left out."""
    ids = sorted({int(t) for t in service_type_ids})
    if not ids:
        return {}
    type_attr_ids, group_attr_ids, named = _attribute_ids(session)
    rows = session.execute(
        text("SELECT id, dogmaAttributes FROM typeDogma WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
        {"ids": ids},
    ).fetchall()
    out: dict[int, dict[str, Any]] = {}
    for tid, attrs_raw in rows:
        attrs = json.loads(attrs_raw) if isinstance(attrs_raw, str) else (attrs_raw or [])
        values: dict[int, float] = {}
        for a in attrs or []:
            if isinstance(a, dict) and a.get("attributeID") is not None and a.get("value") is not None:
                values[int(a["attributeID"])] = float(a["value"])
        out[int(tid)] = {
            "structure_type_ids": sorted(int(v) for a, v in values.items() if a in type_attr_ids and v),
            "structure_group_ids": sorted(int(v) for a, v in values.items() if a in group_attr_ids and v),
            "disallow_in_high_sec": bool(values.get(named.get(_DISALLOW_HISEC, -1))),
            "disallow_in_empire_space": bool(values.get(named.get(_DISALLOW_EMPIRE, -1))),
            "requires_sov_upgrade": any(bool(values.get(named.get(n, -1))) for n in _REQUIRES_SOV_UPGRADE),
        }
    return out


def get_type_group_id(session: Any, type_id: int) -> int | None:
    row = session.execute(text("SELECT groupID FROM types WHERE id = :tid"), {"tid": int(type_id)}).fetchone()
    return int(row[0]) if row and row[0] is not None else None
