import logging

import streamlit as st
import pandas as pd

from streamlit_ui.api.client import api_get, api_post, api_put, api_delete
from streamlit_ui.state.session_state import ensure_state_defaults
from streamlit_ui.api.streamlit_client import cached_api_get
from streamlit_ui.components.selectors import select_character_id
from streamlit_ui.components.webpage_ui import render_aggrid_table, require_aggrid
from eve_online_industry_tracker.config.admin_settings import ADMIN_SETTINGS_SCHEMA
from eve_online_industry_tracker.domain import security_band as _security_band
from eve_online_industry_tracker.application.industry.job_cost import (
    DEFAULT_RESEARCH_SCC_SURCHARGE,
    DEFAULT_SCC_SURCHARGE,
    job_fee_rates_summary,
)

logger = logging.getLogger(__name__)


@st.cache_data(ttl=300)
def _get_industry_profiles(character_id: int) -> dict | None:
    # Cache the backend response to reduce repeated calls on Streamlit reruns.
    return api_get(f"/industry_profiles/{int(character_id)}")


def _extract_manufacturing_cost_index(system_cost_index_payload: dict) -> float:
    try:
        cost_indices = system_cost_index_payload.get("cost_indices") or []
        for entry in cost_indices:
            if entry.get("activity") == "manufacturing":
                return float(entry.get("cost_index") or 0.0)
    except Exception:
        return 0.0
    return 0.0


def _describe_profile_location(profile: dict, *, resolved_structure_type_name: str | None = None) -> str:
    # Preferred labels for the existing profiles list.
    location_type = (profile.get("location_type") or "").lower()
    facility_type = (profile.get("facility_type") or "").strip()

    if location_type == "station" or facility_type.lower() == "npc_station":
        return "NPC Station"

    if location_type == "structure":
        if resolved_structure_type_name:
            return f"Upwell Structure - {resolved_structure_type_name}"
        if facility_type and facility_type.lower() not in {"structure", "upwell", "upwell_structure"}:
            return f"Upwell Structure - {facility_type}"
        return "Upwell Structure"

    if facility_type:
        return facility_type
    return profile.get("location_type") or "N/A"


def _compute_combined_reduction(reductions: list[float]) -> float:
    # Combine multiplicatively: total = 1 - Π(1 - r_i)
    # Values must be fractions in [0, 1] (e.g. 0.024 for 2.4%). Values > 1 are
    # treated as out-of-range and clamped to 1.0 rather than silently divided.
    total = 0.0
    try:
        mul = 1.0
        for r in reductions:
            r = float(r or 0.0)
            if r <= 0:
                continue
            r = min(r, 1.0)
            mul *= (1.0 - r)
        total = 1.0 - mul
    except Exception:
        total = 0.0
    return max(0.0, min(total, 1.0))


def _compute_rig_bonuses(
    rig_type_ids: list[int],
    rig_bonus_map: dict[int, dict],
    *,
    security_band: str | None = None,
) -> tuple[float, float, float]:
    """Combined rig ME / TE / cost for display and the saved aggregate: per
    rig and metric its largest effect value, scaled by the rig's multiplier
    for the system's band (domain.security_band; the smallest when unknown)."""
    per_metric: dict[str, list[float]] = {"material": [], "time": [], "cost": []}
    for tid in rig_type_ids:
        if int(tid or 0) == 0:
            continue
        rig = rig_bonus_map.get(int(tid)) or {}
        effects = [e for e in (rig.get("effects") or []) if isinstance(e, dict)]
        for metric, values in per_metric.items():
            if effects:
                scaled = [_security_band.scaled_effect_value(rig, e, security_band)
                          for e in effects if str(e.get("metric") or "") == metric]
                values.append(max(scaled, default=0.0))
            else:
                values.append(float(rig.get(f"{metric}_reduction") or 0.0))

    return (
        _compute_combined_reduction(per_metric["material"]),
        _compute_combined_reduction(per_metric["time"]),
        _compute_combined_reduction(per_metric["cost"]),
    )


def _rig_bonus_save_fields(rig_type_ids: list[int], rig_bonus_map: dict[int, dict]) -> dict[str, float]:
    """The structure_rig_*_bonus a profile saves: the rigs' raw, unscaled
    bonuses. The producer reads them only for a profile without rig ids and
    then applies the smallest security multiplier for the band itself
    (IndustryService._profile_rig_reduction), so scaling them here as well
    would count the band twice. Only the displayed totals are band-scaled."""
    per_metric: dict[str, list[float]] = {"material": [], "time": [], "cost": []}
    for tid in rig_type_ids:
        if int(tid or 0) == 0:
            continue
        rig = rig_bonus_map.get(int(tid)) or {}
        effects = [e for e in (rig.get("effects") or []) if isinstance(e, dict)]
        for metric, values in per_metric.items():
            if effects:
                values.append(max((float(e.get("value") or 0.0) for e in effects
                                   if str(e.get("metric") or "") == metric), default=0.0))
            else:
                values.append(float(rig.get(f"{metric}_reduction") or 0.0))
    return {
        "structure_rig_material_bonus": _compute_combined_reduction(per_metric["material"]),
        "structure_rig_time_bonus": _compute_combined_reduction(per_metric["time"]),
        "structure_rig_cost_bonus": _compute_combined_reduction(per_metric["cost"]),
    }


def _stored_rig_bonus_lines(profile: dict) -> list[str]:
    """The profile's stored rig ME/TE lines, labelled as unscaled: they are the
    rigs' raw bonuses (_rig_bonus_save_fields), while the "Total rig bonuses"
    caption beside them is scaled by the system's security band."""
    label = "(stored, unscaled, before security band)"
    return [
        f"**Rig Material Bonus {label}:** {profile['structure_rig_material_bonus']:.2%}",
        f"**Rig Time Bonus {label}:** {profile['structure_rig_time_bonus']:.2%}",
    ]


def _security_band_of_system(system: dict | None) -> str | None:
    if not isinstance(system, dict):
        return None
    return _security_band.security_band(system.get("security_status"), system.get("region_id"))


# NPC station facility tax (fraction): admin setting industry/npc_station_facility_tax,
# read per render and stored in an NPC profile when it is created; its schema
# default when the settings cannot be read (the caption says so). The SCC
# default is the producer's one DEFAULT_SCC_SURCHARGE.
_NPC_STATION_FACILITY_TAX = float(
    ADMIN_SETTINGS_SCHEMA["industry"]["settings"]["npc_station_facility_tax"]["default"])

#: Shown when /admin_settings cannot be read (Task 14 minor 3): the rates
#: on the page are then the defaults, not the stored settings.
ADMIN_SETTINGS_UNAVAILABLE_CAPTION = (
    "Admin settings could not be read: the fee rates shown are the default rates, "
    "not the stored settings."
)


def _admin_industry_values() -> tuple[dict, str | None]:
    """(the admin settings' industry section, a caption when the read
    failed), read once per page render. A failed read gives {} (every rate
    then uses its default) and ADMIN_SETTINGS_UNAVAILABLE_CAPTION, with a
    WARNING, so the defaults are never shown as the stored settings. A
    stored invalid value (the reply's "invalid") is left out too, so its
    default rate is shown, and the caption names it."""
    resp = api_get("/admin_settings")
    if resp and resp.get("status") == "success":
        data = resp.get("data") or {}
        vals = data.get("values", {}).get("industry", {})
        vals = dict(vals) if isinstance(vals, dict) else {}
        invalid = (data.get("invalid") or {}).get("industry") or {}
        problems = []
        for key, entry in sorted(invalid.items()):
            vals.pop(key, None)
            problems.append(f"industry/{key}={(entry or {}).get('value')!r} ({(entry or {}).get('problem')})")
        if problems:
            return vals, ("Invalid admin settings: " + "; ".join(problems)
                          + ". The default rate is shown for them; fix them on the Application Settings page.")
        return vals, None
    logger.warning(
        "Industry profiles: /admin_settings read failed (%s); showing the default fee rates",
        (resp or {}).get("message") if isinstance(resp, dict) else "no response",
    )
    return {}, ADMIN_SETTINGS_UNAVAILABLE_CAPTION


def _npc_admin_defaults(industry_values: dict) -> tuple[float, float]:
    """NPC facility tax and SCC surcharge (falls back to module defaults)."""
    return (
        float(industry_values.get("npc_station_facility_tax", _NPC_STATION_FACILITY_TAX)),
        float(industry_values.get("scc_surcharge", DEFAULT_SCC_SURCHARGE)),
    )


def _job_scc_rates(industry_values: dict) -> tuple[float, float]:
    """(SCC, ME/TE research SCC): the admin settings the producer charges
    (falls back to the producer's defaults)."""
    return (
        float(industry_values.get("scc_surcharge", DEFAULT_SCC_SURCHARGE)),
        float(industry_values.get("research_scc_surcharge", DEFAULT_RESEARCH_SCC_SURCHARGE)),
    )


def _job_fee_rates_line(profile: dict, *, scc_rates: tuple[float, float]) -> str:
    """`scc_rates` is _job_scc_rates() of the one admin settings read per render."""
    scc, research_scc = scc_rates
    return job_fee_rates_summary(profile, scc=scc, research_scc=research_scc)


def _npc_create_fee_line(
    selected_facility_id: int | None, npc_facility_tax: float | None, *, scc_rates: tuple[float, float],
) -> str:
    """The NPC create form's fee line. Before a station is picked the tax is
    not loaded yet, which is not a profile to re-save."""
    if selected_facility_id is None:
        return "pick a station first"
    return _job_fee_rates_line({"facility_tax": npc_facility_tax}, scc_rates=scc_rates)


def _rig_breakdown_lines(
    rig_type_ids: list[int], rig_options: dict[int, str], rig_bonus_map: dict[int, dict],
    *, security_band: str | None = None,
) -> list[str]:
    lines: list[str] = []
    for tid in rig_type_ids:
        tid = int(tid or 0)
        if tid == 0:
            continue
        bonus = rig_bonus_map.get(tid) or {}
        name = rig_options.get(tid, str(tid))

        effects = bonus.get("effects") or []
        if not effects:
            lines.append(f"- {name}: no SDE rig effects detected")
            continue

        # Group by activity+group.
        grouped: dict[tuple[str, str], dict[str, list[float]]] = {}
        for e in effects:
            if not isinstance(e, dict):
                continue
            activity = str(e.get("activity") or "")
            group = str(e.get("group") or "")
            metric = str(e.get("metric") or "")
            value = _security_band.scaled_effect_value(bonus, e, security_band)
            if not activity or not metric or value <= 0:
                continue
            key = (activity, group or "All")
            grouped.setdefault(key, {}).setdefault(metric, []).append(value)

        parts_all: list[str] = []
        for (activity, group), metrics in sorted(grouped.items()):
            parts = []
            me = _compute_combined_reduction(metrics.get("material") or [])
            te = _compute_combined_reduction(metrics.get("time") or [])
            cost = _compute_combined_reduction(metrics.get("cost") or [])
            if me > 0:
                parts.append(f"ME {me:.0%}")
            if te > 0:
                parts.append(f"TE {te:.0%}")
            if cost > 0:
                parts.append(f"Cost {cost:.0%}")
            if parts:
                label = activity.replace("_", " ").title()
                parts_all.append(f"{label} ({group}): " + " | ".join(parts))

        if not parts_all:
            lines.append(f"- {name}: no usable SDE rig effects")
        else:
            lines.append(f"- {name}: " + " / ".join(parts_all))
    return lines


def _rig_effects_table(
    rig_type_ids: list[int],
    rig_options: dict[int, str],
    rig_bonus_map: dict[int, dict],
    *,
    security_band: str | None = None,
) -> pd.DataFrame:
    """Rig effects table based on SDE dogma-derived `effects`.

    `/structure_rigs` returns per-rig structured effects:
      - activity: manufacturing/invention/copying/research
      - group: a normalized group label (e.g. "Ammo & Charges" or "All")
      - metric: material/time/cost
      - value: reduction fraction
    """

    effects: list[dict] = []
    for tid in rig_type_ids:
        tid = int(tid or 0)
        if tid == 0:
            continue
        bonus = rig_bonus_map.get(tid) or {}
        for e in bonus.get("effects") or []:
            if isinstance(e, dict):
                # Scaled for the system's band, as the producer applies it.
                effects.append({**e, "value": _security_band.scaled_effect_value(bonus, e, security_band)})

    # Rows as requested.
    rows = [
        ("Modules", "manufacturing"),
        ("Ammo & Charges", "manufacturing"),
        ("Drones", "manufacturing"),
        ("Basic Small Ships", "manufacturing"),
        ("Basic Medium Ships", "manufacturing"),
        ("Basic Large Ships", "manufacturing"),
        ("Advanced Components", "manufacturing"),
        ("Advanced Small Ships", "manufacturing"),
        ("Advanced Medium Ships", "manufacturing"),
        ("Advanced Large Ships", "manufacturing"),
        ("Capital Components", "manufacturing"),
        ("Capital Ships", "manufacturing"),
        ("Advanced Capital Components", "manufacturing"),
        ("Structures", "manufacturing"),
        ("Biochemical Reactions", "reaction"),
        ("Composite Reactions", "reaction"),
        ("Hybrid Reactions", "reaction"),
        ("Invention", "invention"),
        ("Copying", "copying"),
        ("Research ME", "research_me"),
        ("Research TE", "research_te"),
    ]

    def _matches(row_activity: str, row_group: str, e: dict) -> bool:
        act = str(e.get("activity") or "")
        group = str(e.get("group") or "")
        metric = str(e.get("metric") or "")
        if not act or not metric:
            return False

        if row_activity == "research_me":
            return act == "research_me"
        if row_activity == "research_te":
            return act == "research_te"

        if row_activity in {"invention", "copying"}:
            return act == row_activity

        # manufacturing / reaction
        return act == row_activity and (group == row_group)

    table = []
    for group, activity in rows:
        ms: list[float] = []
        ts: list[float] = []
        cs: list[float] = []
        for e in effects:
            if not _matches(activity, group, e):
                continue
            metric = str(e.get("metric") or "")
            value = float(e.get("value") or 0.0)
            if value <= 0:
                continue

            if metric == "material":
                ms.append(value)
            elif metric == "time":
                ts.append(value)
            elif metric == "cost":
                cs.append(value)

        me = _compute_combined_reduction(ms)
        te = _compute_combined_reduction(ts)
        cost = _compute_combined_reduction(cs)

        table.append(
            {
                "Manufacturing Group": group,
                "ME": ("—" if me <= 0 else f"{me:.0%}"),
                "TE": ("—" if te <= 0 else f"{te:.0%}"),
                "Cost": ("—" if cost <= 0 else f"{cost:.0%}"),
            }
        )

    return pd.DataFrame(table)


def _rerun() -> None:
    st.rerun()


def _format_rig_name(name: str) -> str:
    if not name:
        return ""
    out = str(name)

    # Compact common long phrases.
    out = out.replace("Material Efficiency", "ME")
    out = out.replace("Time Efficiency", "TE")

    # Remove the most common blueprint suffix if it ever slips through.
    out = out.replace(" Blueprint", "")
    return out


def _infer_upwell_size_from_type_name(type_name: str | None) -> str | None:
    if not type_name:
        return None

    n = str(type_name).lower()
    # Engineering complexes / citadels / refineries (common cases)
    medium = ["astra" , "raitaru", "athanor"]
    large = ["fortizar", "azbel", "tatara"]
    xl = ["keepstar", "sotiyo"]

    if any(x in n for x in xl):
        return "XL"
    if any(x in n for x in large):
        return "L"
    if any(x in n for x in medium):
        return "M"
    return None


def _load_rig_options() -> tuple[dict[int, str], dict[int, dict], list[int]]:
    rig_options = {0: "None"}
    rig_bonus_map: dict[int, dict] = {}
    try:
        rigs_resp = cached_api_get("/industry_structure_rigs")
        if rigs_resp and rigs_resp.get("status") == "success":
            rigs = rigs_resp.get("data") or []
            for rig in rigs:
                tid = int(rig.get("type_id"))
                rig_options[tid] = _format_rig_name(rig.get("name") or str(tid))
                rig_bonus_map[tid] = rig
    except Exception:
        pass

    rig_option_ids = [0] + sorted(
        [tid for tid in rig_options.keys() if tid != 0],
        key=lambda tid: str(rig_options.get(tid, tid)).lower(),
    )
    return rig_options, rig_bonus_map, rig_option_ids


def _load_profile_context(selected_character_id: int, profiles: list[dict]) -> dict[str, dict]:
    context: dict[str, dict] = {
        "system_cost_index_map": {},
        "public_structure_type_name_by_id": {},
        "corp_structure_type_name_by_id": {},
        "public_structure_type_id_by_id": {},
        "corp_structure_type_id_by_id": {},
        "facility_bonuses_by_type_id": {},
        "solar_system_by_id": {},
    }
    try:
        system_ids = sorted({int(profile.get("system_id")) for profile in profiles if profile.get("system_id") is not None})
        for system_id in system_ids:
            ci_resp = cached_api_get(f"/industry_system_cost_index/{system_id}")
            if ci_resp and ci_resp.get("status") == "success":
                context["system_cost_index_map"][system_id] = _extract_manufacturing_cost_index(ci_resp.get("data") or {})

        corp_resp = cached_api_get(f"/corporation_structures/{selected_character_id}")
        if corp_resp and corp_resp.get("status") == "success":
            for structure in corp_resp.get("data") or []:
                structure_id = structure.get("structure_id")
                type_name = structure.get("type_name") or structure.get("type")
                if structure_id is not None and type_name:
                    context["corp_structure_type_name_by_id"][int(structure_id)] = str(type_name)
                try:
                    type_id = structure.get("type_id")
                    if structure_id is not None and type_id is not None:
                        context["corp_structure_type_id_by_id"][int(structure_id)] = int(type_id)
                except Exception:
                    pass

        for system_id in system_ids:
            public_resp = cached_api_get(f"/public_structures/{system_id}")
            if public_resp and public_resp.get("status") == "success":
                for structure in public_resp.get("data") or []:
                    structure_id = structure.get("station_id")
                    type_name = structure.get("type_name") or structure.get("type")
                    if structure_id is not None and type_name:
                        context["public_structure_type_name_by_id"][int(structure_id)] = str(type_name)
                    try:
                        type_id = structure.get("type_id")
                        if structure_id is not None and type_id is not None:
                            context["public_structure_type_id_by_id"][int(structure_id)] = int(type_id)
                    except Exception:
                        pass
    except Exception:
        pass
    # Security status and region of each profile's system, in a try of its
    # own: the rig totals are shown for the system's band, and a failure here
    # must not cost the cost indices above.
    try:
        wanted = {int(profile.get("system_id")) for profile in profiles if profile.get("system_id") is not None}
        if wanted:
            systems_resp = cached_api_get("/solar_systems")
            if systems_resp and systems_resp.get("status") == "success":
                for system in systems_resp.get("data") or []:
                    if isinstance(system, dict) and system.get("id") in wanted:
                        context["solar_system_by_id"][int(system["id"])] = system
            else:
                logger.warning("Industry profiles: the solar systems did not load; rig totals use the smallest "
                               "security multiplier")
    except Exception:
        logger.warning("Industry profiles: loading the solar systems failed; rig totals use the smallest "
                       "security multiplier", exc_info=True)
    return context


def _render_existing_profiles(
    profiles: list[dict],
    *,
    rig_options: dict[int, str],
    rig_bonus_map: dict[int, dict],
    rig_option_ids: list[int],
    profile_context: dict[str, dict],
    scc_rates: tuple[float, float],
) -> None:
    for profile in profiles:
        with st.expander(f"{'⭐ ' if profile['is_default'] else ''}{profile['profile_name']}"):
            col1, col2, col3 = st.columns([2, 2, 1])

            is_profile_npc_station = (
                (profile.get("location_type") or "").lower() == "station"
                or (profile.get("facility_type") or "").lower() == "npc_station"
            )

            rig0 = int(profile.get("rig_slot0_type_id") or 0)
            rig1 = int(profile.get("rig_slot1_type_id") or 0)
            rig2 = int(profile.get("rig_slot2_type_id") or 0)
            computed_mat = float(profile.get("structure_rig_material_bonus") or 0.0)
            computed_time = float(profile.get("structure_rig_time_bonus") or 0.0)
            computed_cost = float(profile.get("structure_rig_cost_bonus") or 0.0)

            with col1:
                st.write(f"**Location:** {profile['location_name'] or 'Not set'}")

                resolved_type_name = None
                resolved_type_id = None
                try:
                    if (profile.get("location_type") or "").lower() == "structure" and profile.get("location_id") is not None:
                        location_id = int(profile.get("location_id"))
                        resolved_type_name = profile_context["corp_structure_type_name_by_id"].get(location_id) or profile_context["public_structure_type_name_by_id"].get(location_id)
                        resolved_type_id = profile_context["corp_structure_type_id_by_id"].get(location_id) or profile_context["public_structure_type_id_by_id"].get(location_id)
                except Exception:
                    resolved_type_name = None
                    resolved_type_id = None

                st.write(f"**Type:** {_describe_profile_location(profile, resolved_structure_type_name=resolved_type_name)}")

                try:
                    if resolved_type_id is not None:
                        cached = profile_context["facility_bonuses_by_type_id"].get(int(resolved_type_id))
                        if cached is None:
                            bonus_resp = cached_api_get(f"/structure_type_bonuses/{int(resolved_type_id)}")
                            if bonus_resp and bonus_resp.get("status") == "success":
                                cached = ((bonus_resp.get("data") or {}).get("bonuses") or {})
                            else:
                                cached = {}
                            profile_context["facility_bonuses_by_type_id"][int(resolved_type_id)] = cached

                        me = float((cached or {}).get("material_reduction") or 0.0)
                        te = float((cached or {}).get("time_reduction") or 0.0)
                        cost = float((cached or {}).get("cost_reduction") or 0.0)
                        if (me + te + cost) > 0:
                            st.write("**Facility Bonus:** " f"Cost {cost:.0%} | ME {me:.0%} | TE {te:.0%}")
                except Exception:
                    pass

            with col2:
                profile_system_id = profile.get("system_id")
                current_ci = None
                if profile_system_id is not None:
                    current_ci = profile_context["system_cost_index_map"].get(int(profile_system_id))

                if current_ci is not None:
                    st.write(f"**System Cost Index (live):** {float(current_ci):.2%}")
                else:
                    stored_ci = profile.get("manufacturing_cost_index")
                    if stored_ci is None:
                        st.write("**System Cost Index:** N/A")
                    else:
                        st.write(f"**System Cost Index (stored):** {float(stored_ci):.2%}")
                        st.caption("Cost index could not be loaded live — stored value may be stale. Open the system's profile and re-select it to refresh.")

                # The producer's own rates: the profile's facility tax plus
                # the admin SCC rates (research has its own).
                fee_label = "**NPC job fees:**" if is_profile_npc_station else "**Job fees:**"
                st.write(f"{fee_label} {_job_fee_rates_line(profile, scc_rates=scc_rates)}")

                if not is_profile_npc_station:
                    for line in _stored_rig_bonus_lines(profile):
                        st.write(line)
                    st.markdown("**Rigs**")

                    def _default_index(saved_id: int) -> int:
                        return rig_option_ids.index(saved_id) if saved_id in rig_option_ids else 0

                    rig0 = st.selectbox(
                        "Rig Slot 1",
                        options=rig_option_ids,
                        index=_default_index(int(profile.get("rig_slot0_type_id") or 0)),
                        format_func=lambda x: rig_options.get(x, str(x)),
                        key=f"edit_rig0_{profile['id']}",
                    )
                    rig1 = st.selectbox(
                        "Rig Slot 2",
                        options=rig_option_ids,
                        index=_default_index(int(profile.get("rig_slot1_type_id") or 0)),
                        format_func=lambda x: rig_options.get(x, str(x)),
                        key=f"edit_rig1_{profile['id']}",
                    )
                    rig2 = st.selectbox(
                        "Rig Slot 3",
                        options=rig_option_ids,
                        index=_default_index(int(profile.get("rig_slot2_type_id") or 0)),
                        format_func=lambda x: rig_options.get(x, str(x)),
                        key=f"edit_rig2_{profile['id']}",
                    )

                    profile_band = _security_band_of_system(
                        profile_context.get("solar_system_by_id", {}).get(int(profile_system_id))
                        if profile_system_id is not None else None
                    )
                    computed_mat, computed_time, computed_cost = _compute_rig_bonuses(
                        [int(rig0), int(rig1), int(rig2)],
                        rig_bonus_map,
                        security_band=profile_band,
                    )
                    st.caption(f"Total rig bonuses — ME: {computed_mat:.2%} | TE: {computed_time:.2%} | Cost: {computed_cost:.2%}")

                    breakdown = _rig_breakdown_lines([int(rig0), int(rig1), int(rig2)], rig_options, rig_bonus_map,
                                                     security_band=profile_band)
                    if breakdown:
                        with st.expander("Rig bonus details"):
                            st.markdown("\n".join(breakdown))

            with col3:
                if not is_profile_npc_station and st.button("Save rigs", key=f"save_rigs_{profile['id']}"):
                    payload = {
                        "rig_slot0_type_id": None if int(rig0) == 0 else int(rig0),
                        "rig_slot1_type_id": None if int(rig1) == 0 else int(rig1),
                        "rig_slot2_type_id": None if int(rig2) == 0 else int(rig2),
                        **_rig_bonus_save_fields([int(rig0), int(rig1), int(rig2)], rig_bonus_map),
                    }
                    update_response = api_put(f"/industry_profiles/{profile['id']}", payload)
                    if update_response and update_response.get("status") == "success":
                        st.success("Profile updated")
                        _get_industry_profiles.clear()
                        _rerun()
                    else:
                        st.error(f"Error: {update_response.get('message') if update_response else 'API connection failed'}")

                if st.button("Delete", key=f"delete_{profile['id']}"):
                    delete_response = api_delete(f"/industry_profiles/{profile['id']}")
                    if delete_response and delete_response.get("status") == "success":
                        st.success("Profile deleted")
                        _get_industry_profiles.clear()
                        _rerun()
                    else:
                        st.error(f"Error: {delete_response.get('message') if delete_response else 'API connection failed'}")


def _render_create_profile_section(
    selected_character_id: int,
    *,
    rig_options: dict[int, str],
    rig_bonus_map: dict[int, dict],
    rig_option_ids: list[int],
    runtime,
    scc_rates: tuple[float, float],
    admin_industry: dict,
) -> None:
    st.markdown("### Create New Profile")

    solar_systems_response = cached_api_get("/solar_systems")
    if solar_systems_response is None:
        st.error("Failed to connect to API. Please check if Flask is running.")
        return
    if solar_systems_response.get("status") != "success":
        st.error("Failed to load solar systems.")
        return

    solar_systems = solar_systems_response.get("data", [])
    if not solar_systems:
        st.warning("No solar systems data available.")
        return

    system_options = {
        system["id"]: f"{system['name']} ({system.get('security_status', 0.0):.1f}) - {system.get('region_name', 'Unknown')}"
        for system in solar_systems
    }
    selected_system_id = st.selectbox(
        "System",
        options=list(system_options.keys()),
        format_func=lambda x: system_options[x],
    )
    selected_system = next((system for system in solar_systems if system["id"] == selected_system_id), None)

    system_mfg_cost_index = 0.0
    try:
        cost_index_resp = cached_api_get(f"/industry_system_cost_index/{selected_system_id}")
        if cost_index_resp and cost_index_resp.get("status") == "success":
            system_mfg_cost_index = _extract_manufacturing_cost_index(cost_index_resp.get("data") or {})
    except Exception:
        system_mfg_cost_index = 0.0

    if selected_system:
        st.caption(
            f"**Region:** {selected_system.get('region_name', 'Unknown')} | "
            f"**Constellation:** {selected_system.get('constellation_name', 'Unknown')} | "
            f"**Security:** {selected_system.get('security_status', 0.0):.2f}"
            f" | **System Index Cost:** {float(system_mfg_cost_index):.2%}"
        )

    stations: list[dict] = []
    try:
        npc_stations_response = cached_api_get(f"/npc_stations/{selected_system_id}")
        try:
            public_structures = cached_api_get(f"/public_structures/{selected_system_id}")
        except Exception as e:
            st.warning(f"Public structures unavailable: {e}")
            public_structures = {"status": "success", "data": []}

        try:
            corporation_structures = cached_api_get(f"/corporation_structures/{selected_character_id}")
        except Exception as e:
            st.warning(f"Corporation structures unavailable: {e}")
            corporation_structures = {"status": "success", "data": []}

        if npc_stations_response is None or npc_stations_response.get("status") != "success":
            message = npc_stations_response.get("message") if npc_stations_response else "API connection failed"
            st.error(f"Failed to load NPC stations: {message}")
            return

        if public_structures is None or public_structures.get("status") != "success":
            message = public_structures.get("message") if public_structures else "API connection failed"
            st.warning(f"Public structures unavailable: {message}")
            public_structures = {"status": "success", "data": []}

        if corporation_structures is None or corporation_structures.get("status") != "success":
            message = corporation_structures.get("message") if corporation_structures else "API connection failed"
            st.warning(f"Corporation structures unavailable: {message}")
            corporation_structures = {"status": "success", "data": []}

        stations = npc_stations_response.get("data", [])
        for station in stations:
            if isinstance(station, dict):
                station["__kind"] = "npc_station"
        stations.extend(public_structures.get("data", []))
        for structure in public_structures.get("data", []) or []:
            if isinstance(structure, dict):
                structure["__kind"] = "public_structure"

        for structure in corporation_structures.get("data", []):
            if structure.get("system_id") != selected_system_id:
                continue
            structure_id = structure.get("structure_id")
            structure_name = structure.get("structure_name")
            if structure_id is None or not structure_name:
                continue
            stations.append(
                {
                    "station_id": int(structure_id),
                    "station_name": str(structure_name),
                    "system_id": int(selected_system_id),
                    "owner_id": structure.get("corporation_id"),
                    "type_id": structure.get("type_id"),
                    "type_name": structure.get("type_name"),
                    "services": structure.get("services"),
                    "__kind": "corp_structure",
                }
            )
    except ValueError as e:
        st.warning(f"Failed to load stations/structures: {e}")
    except Exception as e:
        st.error(f"Failed to load stations/structures: {e}")
        return

    station_options = {"none": "No Station (Structure)"}
    for station in stations:
        station_options[str(station["station_id"])] = station["station_name"]

    selected_station_key = st.selectbox(
        "Select Station or Structure",
        options=list(station_options.keys()),
        format_func=lambda x: station_options[x],
    )

    selected_station = None
    selected_kind = None
    selected_upwell_type_name = None
    selected_upwell_type_id = None
    if selected_station_key != "none":
        selected_station = next((station for station in stations if str(station["station_id"]) == selected_station_key), None)
        if isinstance(selected_station, dict):
            selected_kind = selected_station.get("__kind")
            selected_upwell_type_name = selected_station.get("type_name") or selected_station.get("structure_type_name") or selected_station.get("type")
            try:
                tid_raw = selected_station.get("type_id")
                if tid_raw is None:
                    selected_upwell_type_id = None
                elif isinstance(tid_raw, (int, float)):
                    selected_upwell_type_id = int(tid_raw)
                else:
                    tid_s = str(tid_raw).strip()
                    selected_upwell_type_id = int(tid_s) if tid_s else None
            except Exception:
                selected_upwell_type_id = None

        if selected_station:
            st.markdown("**Station Details**")
            col1, _ = st.columns(2)
            with col1:
                owner_label = selected_station.get("owner_name") or selected_station.get("owner") or selected_station.get("owner_id") or "N/A"
                st.write(f"**Owner:** {owner_label}")
            if selected_station.get("services"):
                with st.expander("Available Services"):
                    services = selected_station.get("services")
                    if isinstance(services, dict):
                        for name, value in services.items():
                            if isinstance(value, dict):
                                state = value.get("state") or value.get("status")
                                st.write(f"• {name}{f' ({state})' if state else ''}")
                            else:
                                st.write(f"• {name}{f' ({value})' if value else ''}")
                    elif isinstance(services, list):
                        for service in services:
                            if isinstance(service, str):
                                st.write(f"• {service}")
                            elif isinstance(service, dict):
                                name = service.get("service_name") or service.get("name") or service.get("service") or "Unknown"
                                state = service.get("state") or service.get("status")
                                st.write(f"• {name}{f' ({state})' if state else ''}")
                            else:
                                st.write(f"• {service}")
                    else:
                        st.write(f"• {services}")

    st.markdown("**Profile Settings**")
    is_npc_station = selected_station_key != "none" and selected_kind == "npc_station"

    facility_me_bonus_pct = 0.0
    facility_te_bonus_pct = 0.0
    facility_cost_bonus_pct = 0.0
    if not is_npc_station:
        try:
            if selected_upwell_type_id is not None:
                bonus_resp = cached_api_get(f"/structure_type_bonuses/{int(selected_upwell_type_id)}")
                if bonus_resp and bonus_resp.get("status") == "success":
                    bonuses = (bonus_resp.get("data") or {}).get("bonuses") or {}
                    facility_me_bonus_pct = float(bonuses.get("material_reduction") or 0.0) * 100.0
                    facility_te_bonus_pct = float(bonuses.get("time_reduction") or 0.0) * 100.0
                    facility_cost_bonus_pct = float(bonuses.get("cost_reduction") or 0.0) * 100.0
        except Exception:
            pass

        st.markdown("**Facility Bonus**")
        st.caption("Base facility bonuses from SDE (typeBonus).")
        colb1, colb2, colb3 = st.columns(3)
        with colb1:
            st.write(f"Cost bonus: {facility_cost_bonus_pct:.0f}%")
        with colb2:
            st.write(f"ME Bonus: {facility_me_bonus_pct:.0f}%")
        with colb3:
            st.write(f"TE Bonus: {facility_te_bonus_pct:.0f}%")

    npc_facility_tax = None
    npc_scc_surcharge = None
    upwell_facility_tax_pct = None
    upwell_scc_surcharge = None
    selected_facility_id = None
    if selected_station_key != "none":
        try:
            selected_facility_id = int(selected_station_key)
        except Exception:
            selected_facility_id = None

    if is_npc_station and selected_facility_id is not None:
        npc_facility_tax, npc_scc_surcharge = _npc_admin_defaults(admin_industry)

    col1, col2 = st.columns(2)
    with col1:
        st.write(f"**System Index Cost (live):** {float(system_mfg_cost_index):.2%}")
    with col2:
        if is_npc_station:
            if selected_facility_id is not None:
                st.caption(
                    f"NPC station job fees — Facility tax: {float(npc_facility_tax or 0.0):.2%} | "
                    f"SCC surcharge: {float(npc_scc_surcharge or 0.0):.2%}"
                )
            total_npc_rate = float(npc_facility_tax or 0.0) + float(npc_scc_surcharge or 0.0)
            st.write(f"**NPC job fees:** {_npc_create_fee_line(selected_facility_id, npc_facility_tax, scc_rates=scc_rates)}")
            installation_cost_modifier = total_npc_rate * 100.0
        else:
            installation_cost_modifier = st.number_input(
                "Installation Tax (%)",
                min_value=-100.0,
                max_value=100.0,
                value=0.0,
                step=0.1,
            )
            upwell_facility_tax_pct = float(installation_cost_modifier)
            upwell_scc_surcharge = _npc_admin_defaults(admin_industry)[1]  # SCC surcharge
            st.caption(f"SCC surcharge (admin setting): {float(upwell_scc_surcharge):.2%}")
            st.write(f"**Job fees:** {_job_fee_rates_line({'facility_tax': upwell_facility_tax_pct / 100.0}, scc_rates=scc_rates)}")

    rig_slot0_type_id = 0
    rig_slot1_type_id = 0
    rig_slot2_type_id = 0
    structure_rig_material_bonus = 0.0
    structure_rig_time_bonus = 0.0
    structure_rig_cost_bonus = 0.0

    if is_npc_station:
        st.caption("Structure rigs are not applicable to NPC stations.")
    else:
        size = _infer_upwell_size_from_type_name(selected_upwell_type_name)
        filtered_rig_option_ids = rig_option_ids
        if size in {"M", "L", "XL"}:
            tag = f"{size}-Set"
            filtered_rig_option_ids = [
                0,
                *[
                    tid
                    for tid in rig_option_ids
                    if tid != 0 and tag.lower() in str(rig_options.get(tid, "")).lower()
                ],
            ]

        st.markdown("**Structure Rigs**")
        if size in {"M", "L", "XL"}:
            st.caption(f"Filtered for {size}-Set rigs based on selected structure type.")

        col3, col4, col5 = st.columns(3)
        with col3:
            rig_slot0_type_id = st.selectbox("Rig Slot 1", options=filtered_rig_option_ids, format_func=lambda x: rig_options.get(x, str(x)))
        with col4:
            rig_slot1_type_id = st.selectbox("Rig Slot 2", options=filtered_rig_option_ids, format_func=lambda x: rig_options.get(x, str(x)))
        with col5:
            rig_slot2_type_id = st.selectbox("Rig Slot 3", options=filtered_rig_option_ids, format_func=lambda x: rig_options.get(x, str(x)))

        selected_band = _security_band_of_system(selected_system)
        structure_rig_material_bonus, structure_rig_time_bonus, structure_rig_cost_bonus = _compute_rig_bonuses(
            [int(rig_slot0_type_id), int(rig_slot1_type_id), int(rig_slot2_type_id)],
            rig_bonus_map,
            security_band=selected_band,
        )

        st.caption(
            f"Total rig bonuses — ME: {structure_rig_material_bonus:.2%} | TE: {structure_rig_time_bonus:.2%} | Cost: {structure_rig_cost_bonus:.2%}"
        )
        breakdown = _rig_breakdown_lines(
            [int(rig_slot0_type_id), int(rig_slot1_type_id), int(rig_slot2_type_id)],
            rig_options,
            rig_bonus_map,
            security_band=selected_band,
        )
        if breakdown:
            with st.expander("Rig bonus details"):
                st.markdown("\n".join(breakdown))

        st.markdown("**Overview**")
        st.caption("Facility bonus + fitting + rig effects (from SDE dogma).")
        st.write(
            "**Facility bonus:** "
            f"Cost {float(facility_cost_bonus_pct) / 100.0:.0%} | "
            f"ME {float(facility_me_bonus_pct) / 100.0:.0%} | "
            f"TE {float(facility_te_bonus_pct) / 100.0:.0%}"
        )
        st.write(
            "**Facility fitting:** "
            f"Slot 1: {rig_options.get(int(rig_slot0_type_id), 'None')} | "
            f"Slot 2: {rig_options.get(int(rig_slot1_type_id), 'None')} | "
            f"Slot 3: {rig_options.get(int(rig_slot2_type_id), 'None')}"
        )

        effects_df = _rig_effects_table(
            [int(rig_slot0_type_id), int(rig_slot1_type_id), int(rig_slot2_type_id)],
            rig_options,
            rig_bonus_map,
            security_band=selected_band,
        )
        render_aggrid_table(
            effects_df,
            runtime=runtime,
            height_max=500,
            key="industry_profiles_rig_effects",
        )

    with st.form("create_profile_form"):
        profile_name = st.text_input("Profile Name", placeholder="e.g., Jita 4-4 Tatara")
        is_default = st.checkbox("Set as default")
        submitted = st.form_submit_button("Create Profile", type="primary")

    if not submitted:
        return

    location_id = None
    location_name = None
    location_type = "structure"
    if selected_station_key != "none":
        location_id = int(selected_station_key)
        location_name = station_options[selected_station_key]
        location_type = "station" if is_npc_station else "structure"

    region_id = selected_system.get("region_id") if selected_system else None
    system_id = selected_system_id
    facility_id = location_id
    facility_type = "npc_station" if is_npc_station else (selected_upwell_type_name or "upwell_structure")

    create_response = api_post(
        "/industry_profiles",
        {
            "character_id": selected_character_id,
            "profile_name": profile_name,
            "is_default": is_default,
            "region_id": region_id,
            "system_id": system_id,
            "facility_id": facility_id,
            "facility_type": facility_type,
            "facility_tax": (
                float(npc_facility_tax)
                if (is_npc_station and npc_facility_tax is not None)
                else (float(upwell_facility_tax_pct) / 100.0 if (upwell_facility_tax_pct is not None) else None)
            ),
            "scc_surcharge": (
                float(npc_scc_surcharge)
                if (is_npc_station and npc_scc_surcharge is not None)
                else (float(upwell_scc_surcharge) if (upwell_scc_surcharge is not None) else None)
            ),
            "facility_cost_bonus": float(facility_cost_bonus_pct or 0.0) / 100.0,
            "structure_type_id": int(selected_upwell_type_id) if (not is_npc_station and selected_upwell_type_id is not None) else None,
            "location_id": location_id,
            "location_name": location_name,
            "location_type": location_type,
            "material_efficiency_bonus": float(facility_me_bonus_pct or 0.0) / 100.0,
            "time_efficiency_bonus": float(facility_te_bonus_pct or 0.0) / 100.0,
            "installation_cost_modifier": (
                (float(npc_facility_tax or 0.0) + float(npc_scc_surcharge or 0.0))
                if is_npc_station
                else ((float(upwell_facility_tax_pct or 0.0) / 100.0) + float(upwell_scc_surcharge or 0.0))
            ),
            # Raw rig bonuses (the displayed totals above are band-scaled).
            **_rig_bonus_save_fields(
                [int(rig_slot0_type_id), int(rig_slot1_type_id), int(rig_slot2_type_id)], rig_bonus_map),
            "rig_slot0_type_id": None if int(rig_slot0_type_id) == 0 else int(rig_slot0_type_id),
            "rig_slot1_type_id": None if int(rig_slot1_type_id) == 0 else int(rig_slot1_type_id),
            "rig_slot2_type_id": None if int(rig_slot2_type_id) == 0 else int(rig_slot2_type_id),
        },
    )

    if create_response and create_response.get("status") == "success":
        st.success("Profile created successfully!")
        _get_industry_profiles.clear()
        st.session_state["show_create_new_profile"] = False
        _rerun()
    else:
        st.error(f"Error: {create_response.get('message') if create_response else 'API connection failed'}")

def render():
    st.subheader("Industry Profiles")

    runtime = require_aggrid()

    ensure_state_defaults({"show_create_new_profile": False})

    selected_character_id = select_character_id(
        label="Select a character",
        key="industry_profiles_character_id",
        default_to_main=False,
    )

    if selected_character_id is None:
        st.stop()
    rig_options, rig_bonus_map, rig_option_ids = _load_rig_options()

    # Fetch profiles (cached to avoid repeated backend calls on reruns)
    response = _get_industry_profiles(int(selected_character_id))
    if response is None or response.get("status") != "success":
        st.error(
            f"Error loading profiles: {response.get('message') if response else 'API connection failed'}"
        )
        return

    profiles = response.get("data", [])
    profile_context = _load_profile_context(int(selected_character_id), profiles)
    # One /admin_settings read per render, not one per rendered profile.
    admin_industry, admin_unavailable = _admin_industry_values()
    scc_rates = _job_scc_rates(admin_industry)
    if admin_unavailable:
        st.caption(admin_unavailable)

    st.markdown("### Existing Profiles")
    if profiles:
        _render_existing_profiles(
            profiles,
            rig_options=rig_options,
            rig_bonus_map=rig_bonus_map,
            rig_option_ids=rig_option_ids,
            profile_context=profile_context,
            scc_rates=scc_rates,
        )
    else:
        st.info("No profiles found. Create one below.")

    # Create new profile section is hidden by default.
    if not st.session_state.get("show_create_new_profile"):
        if st.button("Create new profile", type="primary"):
            st.session_state["show_create_new_profile"] = True
            _rerun()
        return

    st.divider()
    _render_create_profile_section(
        int(selected_character_id),
        rig_options=rig_options,
        rig_bonus_map=rig_bonus_map,
        rig_option_ids=rig_option_ids,
        runtime=runtime,
        scc_rates=scc_rates,
        admin_industry=admin_industry,
    )