from __future__ import annotations

import json
from typing import Any

from sqlalchemy import bindparam, text

from eve_online_industry_tracker.infrastructure.sde.types import get_type_data

# Dogma attribute IDs (confirmed in SDE dogmaAttributes table)
_ATTR_INVENTION_PROB_MULT = 1112
_ATTR_INVENTION_ME_MOD = 1113
_ATTR_INVENTION_TE_MOD = 1114
_ATTR_INVENTION_MAX_RUN_MOD = 1124

# SDE category for T2 decryptor items (Categories.id == 35)
_DECRYPTOR_CATEGORY_ID = 35

# Within category 35, the classic T2 invention decryptors live in the
# 'Generic Decryptor' group (Groups.id == 1304). Other groups in the same
# category contain reverse-engineering decryptors and subsystem data interfaces.
_T2_GENERIC_DECRYPTOR_GROUP_ID = 1304


def get_t2_invention_decryptors(sde_session: Any, *, language: str) -> list[dict[str, Any]]:
    """Return decryptor items with invention modifiers.

    Output rows include:
    - type_id, type_name
    - invention_probability_multiplier (float)
    - invention_me_modifier (int)
    - invention_te_modifier (int)
    - invention_max_run_modifier (int)

    Notes:
    - We intentionally restrict to the SDE category 'Decryptors' (id=35) to avoid
      including Sleeper/Talocan relic decoders that also have invention-ish dogma.
        - The SDE 'Decryptors' category also includes reverse-engineering decryptors
            and subsystem data interfaces. For the T2 invention UI we restrict to the
            'Generic Decryptor' group (id=1304), which matches CCP's T2 decryptor list.
    - Missing attributes default to neutral values.
    """

    if sde_session is None:
        return []

    # Find type IDs for the T2 decryptor group
    try:
        type_rows = (
            sde_session.execute(
                text("SELECT id FROM types WHERE published = 1 AND groupID = :gid"),
                {"gid": int(_T2_GENERIC_DECRYPTOR_GROUP_ID)},
            ).fetchall()
        )
    except Exception:
        return []

    type_ids = [int(r[0]) for r in (type_rows or []) if r and r[0] is not None]
    type_ids = sorted({tid for tid in type_ids if tid > 0})
    if not type_ids:
        return []

    # Load dogmaAttributes for these type IDs (best-effort; not all types have typeDogma rows)
    try:
        td_rows = (
            sde_session.execute(
                text("SELECT id, dogmaAttributes FROM typeDogma WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                {"ids": type_ids},
            ).fetchall()
        )
    except Exception:
        td_rows = []

    attrs_by_type_id: dict[int, dict[int, float]] = {}
    for tid, attrs_raw in td_rows or []:
        if tid is None:
            continue
        try:
            attrs = json.loads(attrs_raw) if attrs_raw else []
        except Exception:
            attrs = []
        if not isinstance(attrs, list):
            continue
        m: dict[int, float] = {}
        for a in attrs:
            if not isinstance(a, dict):
                continue
            aid = a.get("attributeID")
            val = a.get("value")
            if aid is None or val is None:
                continue
            try:
                aid_i = int(aid)
            except Exception:
                continue
            if aid_i not in {
                _ATTR_INVENTION_PROB_MULT,
                _ATTR_INVENTION_ME_MOD,
                _ATTR_INVENTION_TE_MOD,
                _ATTR_INVENTION_MAX_RUN_MOD,
            }:
                continue
            try:
                m[aid_i] = float(val)
            except Exception:
                continue
        if m:
            attrs_by_type_id[int(tid)] = m

    # Attach type names
    type_data = get_type_data(sde_session, language, type_ids)

    out: list[dict[str, Any]] = []
    for tid in type_ids:
        t = type_data.get(int(tid)) or {"type_id": int(tid), "type_name": str(tid)}
        m = attrs_by_type_id.get(int(tid), {})

        # If it has no relevant dogma attributes at all, it's not useful for invention.
        if not m:
            continue

        prob_mult = float(m.get(_ATTR_INVENTION_PROB_MULT, 1.0) or 1.0)
        me_mod = int(m.get(_ATTR_INVENTION_ME_MOD, 0.0) or 0.0)
        te_mod = int(m.get(_ATTR_INVENTION_TE_MOD, 0.0) or 0.0)
        run_mod = int(m.get(_ATTR_INVENTION_MAX_RUN_MOD, 0.0) or 0.0)

        out.append(
            {
                "type_id": int(t.get("type_id") or tid),
                "type_name": t.get("type_name"),
                "invention_probability_multiplier": prob_mult,
                "invention_me_modifier": int(me_mod),
                "invention_te_modifier": int(te_mod),
                "invention_max_run_modifier": int(run_mod),
            }
        )

    # Stable ordering by name
    out.sort(key=lambda r: str(r.get("type_name") or ""))
    return out
