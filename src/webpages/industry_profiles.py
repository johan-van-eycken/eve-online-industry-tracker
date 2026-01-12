import streamlit as st
import pandas as pd

from utils.app_init import load_config, init_db_app
from utils.flask_api import cached_api_get, api_get, api_post, api_put, api_delete


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
    total = 0.0
    try:
        mul = 1.0
        for r in reductions:
            r = float(r or 0.0)
            if r <= 0:
                continue
            mul *= (1.0 - r)
        total = 1.0 - mul
    except Exception:
        total = 0.0
    return max(0.0, min(total, 1.0))


def _compute_rig_bonuses(
    rig_type_ids: list[int],
    rig_bonus_map: dict[int, dict],
) -> tuple[float, float, float]:
    rig_material_reductions = [
        (rig_bonus_map.get(int(tid)) or {}).get("material_reduction", 0.0)
        for tid in rig_type_ids
        if int(tid) != 0
    ]
    rig_time_reductions = [
        (rig_bonus_map.get(int(tid)) or {}).get("time_reduction", 0.0)
        for tid in rig_type_ids
        if int(tid) != 0
    ]
    rig_cost_reductions = [
        (rig_bonus_map.get(int(tid)) or {}).get("cost_reduction", 0.0)
        for tid in rig_type_ids
        if int(tid) != 0
    ]

    structure_rig_material_bonus = _compute_combined_reduction([float(x or 0.0) for x in rig_material_reductions])
    structure_rig_time_bonus = _compute_combined_reduction([float(x or 0.0) for x in rig_time_reductions])
    structure_rig_cost_bonus = _compute_combined_reduction([float(x or 0.0) for x in rig_cost_reductions])

    return structure_rig_material_bonus, structure_rig_time_bonus, structure_rig_cost_bonus


# NPC station job fee components (fractions).
_NPC_STATION_FACILITY_TAX = 0.0025
_NPC_STATION_SCC_SURCHARGE = 0.04
_UPWELL_SCC_SURCHARGE = 0.04


def _rig_breakdown_lines(rig_type_ids: list[int], rig_options: dict[int, str], rig_bonus_map: dict[int, dict]) -> list[str]:
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
            value = float(e.get("value") or 0.0)
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
                effects.append(e)

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
        ("Biochemical Reactions", "manufacturing"),
        ("Composite Reactions", "manufacturing"),
        ("Hybrid Reactions", "manufacturing"),
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

        # manufacturing
        return act == "manufacturing" and (group == row_group)

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

def render():
    st.subheader("Industry Profiles")

    if "show_create_new_profile" not in st.session_state:
        st.session_state["show_create_new_profile"] = False

    try:
        cfgManager = load_config()
        db = init_db_app(cfgManager)
    except Exception as e:
        st.error(f"Failed to load database: {e}")
        st.stop()

    try:
        df = db.load_df("characters")
    except Exception:
        st.warning("No character data found. Run main.py first.")
        st.stop()

    # Character selection
    character_map = dict(zip(df["character_id"], df["character_name"]))
    selected_character_id = st.selectbox(
        "Select a character",
        options=df["character_id"].tolist(),
        format_func=lambda x: character_map[x],
    )

    if not selected_character_id:
        return

    # Load structure rigs once (used for both editing existing profiles and creating new ones).
    rig_options = {0: "None"}
    rig_bonus_map: dict[int, dict] = {}
    try:
        rigs_resp = cached_api_get("/structure_rigs")
        if rigs_resp and rigs_resp.get("status") == "success":
            rigs = rigs_resp.get("data") or []
            for r in rigs:
                tid = int(r.get("type_id"))
                rig_options[tid] = _format_rig_name(r.get("name") or str(tid))
                rig_bonus_map[tid] = r
    except Exception:
        pass

    rig_option_ids = [0] + sorted(
        [tid for tid in rig_options.keys() if tid != 0],
        key=lambda tid: str(rig_options.get(tid, tid)).lower(),
    )

    # Fetch profiles (cached to avoid repeated backend calls on reruns)
    response = _get_industry_profiles(int(selected_character_id))
    if response is None or response.get("status") != "success":
        st.error(
            f"Error loading profiles: {response.get('message') if response else 'API connection failed'}"
        )
        return

    profiles = response.get("data", [])

    # Preload current system cost index per system_id for existing profiles.
    system_cost_index_map: dict[int, float] = {}
    public_structure_type_name_by_id: dict[int, str] = {}
    corp_structure_type_name_by_id: dict[int, str] = {}
    public_structure_type_id_by_id: dict[int, int] = {}
    corp_structure_type_id_by_id: dict[int, int] = {}
    facility_bonuses_by_type_id: dict[int, dict] = {}
    try:
        system_ids = sorted({int(p.get("system_id")) for p in profiles if p.get("system_id") is not None})
        for sid in system_ids:
            ci_resp = cached_api_get(f"/industry_system_cost_index/{sid}")
            if ci_resp and ci_resp.get("status") == "success":
                system_cost_index_map[sid] = _extract_manufacturing_cost_index(ci_resp.get("data") or {})

        # Resolve structure type names for nicer display.
        # - corporation structures: fetch once (contains SDE-enriched type_name)
        corp_resp = cached_api_get(f"/corporation_structures/{selected_character_id}")
        if corp_resp and corp_resp.get("status") == "success":
            for s in corp_resp.get("data") or []:
                sid = s.get("structure_id")
                tn = s.get("type_name") or s.get("type")
                if sid is not None and tn:
                    corp_structure_type_name_by_id[int(sid)] = str(tn)
                try:
                    tid = s.get("type_id")
                    if sid is not None and tid is not None:
                        corp_structure_type_id_by_id[int(sid)] = int(tid)
                except Exception:
                    pass

        # - public structures: fetch per system_id (contains SDE-enriched type_name)
        for sid in system_ids:
            ps_resp = cached_api_get(f"/public_structures/{sid}")
            if ps_resp and ps_resp.get("status") == "success":
                for s in ps_resp.get("data") or []:
                    stid = s.get("station_id")
                    tn = s.get("type_name") or s.get("type")
                    if stid is not None and tn:
                        public_structure_type_name_by_id[int(stid)] = str(tn)
                    try:
                        tid = s.get("type_id")
                        if stid is not None and tid is not None:
                            public_structure_type_id_by_id[int(stid)] = int(tid)
                    except Exception:
                        pass
    except Exception:
        pass

    # Display existing profiles
    st.markdown("### Existing Profiles")
    if profiles:
        for profile in profiles:
            with st.expander(
                f"{'⭐ ' if profile['is_default'] else ''}{profile['profile_name']}"
            ):
                col1, col2, col3 = st.columns([2, 2, 1])

                is_profile_npc_station = (
                    (profile.get("location_type") or "").lower() == "station"
                    or (profile.get("facility_type") or "").lower() == "npc_station"
                )

                with col1:
                    st.write(f"**Location:** {profile['location_name'] or 'Not set'}")

                    resolved_type_name = None
                    resolved_type_id = None
                    try:
                        if (profile.get("location_type") or "").lower() == "structure" and profile.get("location_id") is not None:
                            loc_id = int(profile.get("location_id"))
                            resolved_type_name = corp_structure_type_name_by_id.get(loc_id) or public_structure_type_name_by_id.get(loc_id)
                            resolved_type_id = corp_structure_type_id_by_id.get(loc_id) or public_structure_type_id_by_id.get(loc_id)
                    except Exception:
                        resolved_type_name = None
                        resolved_type_id = None

                    st.write(f"**Type:** {_describe_profile_location(profile, resolved_structure_type_name=resolved_type_name)}")

                    # Base facility bonuses (Upwell structures only; sourced from SDE typeBonus).
                    try:
                        if resolved_type_id is not None:
                            cached = facility_bonuses_by_type_id.get(int(resolved_type_id))
                            if cached is None:
                                b_resp = cached_api_get(f"/structure_type_bonuses/{int(resolved_type_id)}")
                                if b_resp and b_resp.get("status") == "success":
                                    cached = ((b_resp.get("data") or {}).get("bonuses") or {})
                                else:
                                    cached = {}
                                facility_bonuses_by_type_id[int(resolved_type_id)] = cached

                            me = float((cached or {}).get("material_reduction") or 0.0)
                            te = float((cached or {}).get("time_reduction") or 0.0)
                            cost = float((cached or {}).get("cost_reduction") or 0.0)

                            if (me + te + cost) > 0:
                                st.write(
                                    "**Facility Bonus:** "
                                    f"Cost {cost:.0%} | ME {me:.0%} | TE {te:.0%}"
                                )
                    except Exception:
                        pass

                with col2:
                    # Do not trust/persist stored system cost index; show current one if we can.
                    profile_system_id = profile.get("system_id")
                    current_ci = None
                    if profile_system_id is not None:
                        current_ci = system_cost_index_map.get(int(profile_system_id))

                    if current_ci is not None:
                        st.write(f"**System Cost Index (live):** {float(current_ci):.2%}")
                    else:
                        stored_ci = profile.get("manufacturing_cost_index")
                        if stored_ci is None:
                            st.write("**System Cost Index (live):** N/A")
                        else:
                            st.write(f"**System Cost Index (stored):** {float(stored_ci):.2%}")
                    if is_profile_npc_station:
                        facility_tax = profile.get("facility_tax")
                        scc_surcharge = profile.get("scc_surcharge")

                        if facility_tax is None and scc_surcharge is None:
                            st.write("**NPC job fees:** N/A")
                        else:
                            facility_tax = float(facility_tax or 0.0)
                            scc_surcharge = float(scc_surcharge or 0.0)
                            total = facility_tax + scc_surcharge
                            st.write(
                                f"**NPC job fees:** Facility {facility_tax:.2%} | SCC {scc_surcharge:.2%} | Total {total:.2%}"
                            )
                    else:
                        facility_tax = profile.get("facility_tax")
                        scc_surcharge = profile.get("scc_surcharge")
                        if facility_tax is None and scc_surcharge is None:
                            st.write("**Job fees:** N/A")
                        else:
                            facility_tax = float(facility_tax or 0.0)
                            scc_surcharge = float(scc_surcharge or 0.0)
                            total = facility_tax + scc_surcharge
                            st.write(
                                f"**Job fees:** Facility {facility_tax:.2%} | SCC {scc_surcharge:.2%} | Total {total:.2%}"
                            )
                    if not is_profile_npc_station:
                        st.write(f"**Rig Material Bonus:** {profile['structure_rig_material_bonus']:.2%}")
                        st.write(f"**Rig Time Bonus:** {profile['structure_rig_time_bonus']:.2%}")

                        # Edit rig slots (preselected from stored rig_slot*_type_id)
                        st.markdown("**Rigs**")
                        saved_rig0 = int(profile.get("rig_slot0_type_id") or 0)
                        saved_rig1 = int(profile.get("rig_slot1_type_id") or 0)
                        saved_rig2 = int(profile.get("rig_slot2_type_id") or 0)

                        def _default_index(saved_id: int) -> int:
                            return rig_option_ids.index(saved_id) if saved_id in rig_option_ids else 0

                        rig0 = st.selectbox(
                            "Rig Slot 1",
                            options=rig_option_ids,
                            index=_default_index(saved_rig0),
                            format_func=lambda x: rig_options.get(x, str(x)),
                            key=f"edit_rig0_{profile['id']}",
                        )
                        rig1 = st.selectbox(
                            "Rig Slot 2",
                            options=rig_option_ids,
                            index=_default_index(saved_rig1),
                            format_func=lambda x: rig_options.get(x, str(x)),
                            key=f"edit_rig1_{profile['id']}",
                        )
                        rig2 = st.selectbox(
                            "Rig Slot 3",
                            options=rig_option_ids,
                            index=_default_index(saved_rig2),
                            format_func=lambda x: rig_options.get(x, str(x)),
                            key=f"edit_rig2_{profile['id']}",
                        )

                        computed_mat, computed_time, computed_cost = _compute_rig_bonuses(
                            [int(rig0), int(rig1), int(rig2)],
                            rig_bonus_map,
                        )
                        st.caption(
                            f"Total rig bonuses — ME: {computed_mat:.2%} | TE: {computed_time:.2%} | Cost: {computed_cost:.2%}"
                        )

                        breakdown = _rig_breakdown_lines([int(rig0), int(rig1), int(rig2)], rig_options, rig_bonus_map)
                        if breakdown:
                            with st.expander("Rig bonus details"):
                                st.markdown("\n".join(breakdown))

                with col3:
                    if not is_profile_npc_station:
                        if st.button("Save rigs", key=f"save_rigs_{profile['id']}"):
                            payload = {
                                "rig_slot0_type_id": None if int(rig0) == 0 else int(rig0),
                                "rig_slot1_type_id": None if int(rig1) == 0 else int(rig1),
                                "rig_slot2_type_id": None if int(rig2) == 0 else int(rig2),
                                "structure_rig_material_bonus": computed_mat,
                                "structure_rig_time_bonus": computed_time,
                                "structure_rig_cost_bonus": computed_cost,
                            }
                            update_response = api_put(f"/industry_profiles/{profile['id']}", payload)
                            if update_response and update_response.get("status") == "success":
                                st.success("Profile updated")
                                _get_industry_profiles.clear()
                                _rerun()
                            else:
                                st.error(
                                    f"Error: {update_response.get('message') if update_response else 'API connection failed'}"
                                )

                    if st.button("Delete", key=f"delete_{profile['id']}"):
                        delete_response = api_delete(f"/industry_profiles/{profile['id']}")
                        if delete_response and delete_response.get("status") == "success":
                            st.success("Profile deleted")
                            _get_industry_profiles.clear()
                            _rerun()
                        else:
                            st.error(f"Error: {delete_response.get('message') if delete_response else 'API connection failed'}")
    else:
        st.info("No profiles found. Create one below.")

    # Create new profile section is hidden by default.
    if not st.session_state.get("show_create_new_profile"):
        if st.button("Create new profile", type="primary"):
            st.session_state["show_create_new_profile"] = True
            _rerun()
        return

    st.divider()

    # Create new profile - Single step form
    st.markdown("### Create New Profile")

    # NOTE: We keep the system/station selectors outside a form so dependent options
    # update immediately. We wrap only the final create action (name/default + submit)
    # in a form to reduce reruns while typing.

    # Fetch solar systems
    solar_systems_response = cached_api_get("/solar_systems")
    if solar_systems_response is None:
        st.error("Failed to connect to API. Please check if Flask is running.")
        return
    elif solar_systems_response.get("status") != "success":
        st.error("Failed to load solar systems.")
        return

    solar_systems = solar_systems_response.get("data", [])
    if not solar_systems:
        st.warning("No solar systems data available.")
        return

    system_options = {
        s["id"]: f"{s['name']} ({s.get('security_status', 0.0):.1f}) - {s.get('region_name', 'Unknown')}"
        for s in solar_systems
    }
    selected_system_id = st.selectbox(
        "System",
        options=list(system_options.keys()),
        format_func=lambda x: system_options[x],
    )
    selected_system = next((s for s in solar_systems if s["id"] == selected_system_id), None)

    # Fetch and show current system manufacturing cost index in the system details.
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

    # Fetch NPC stations
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
            e = npc_stations_response.get("message") if npc_stations_response else "API connection failed"
            st.error(f"Failed to load NPC stations: {e}")
            st.stop()

        if public_structures is None or public_structures.get("status") != "success":
            e = public_structures.get("message") if public_structures else "API connection failed"
            st.warning(f"Public structures unavailable: {e}")
            public_structures = {"status": "success", "data": []}

        if corporation_structures is None or corporation_structures.get("status") != "success":
            e = corporation_structures.get("message") if corporation_structures else "API connection failed"
            st.warning(f"Corporation structures unavailable: {e}")
            corporation_structures = {"status": "success", "data": []}
        
        stations = npc_stations_response.get("data", [])
        for s in stations:
            if isinstance(s, dict):
                s["__kind"] = "npc_station"
        stations.extend(public_structures.get("data", []))
        for s in public_structures.get("data", []) or []:
            if isinstance(s, dict):
                s["__kind"] = "public_structure"

        # Merge corporation-owned structures in this system.
        corp_structs = corporation_structures.get("data", [])
        for s in corp_structs:
            system_id = s.get("system_id")
            if system_id != selected_system_id:
                continue
            structure_id = s.get("structure_id")
            structure_name = s.get("structure_name")
            if structure_id is None or not structure_name:
                continue

            stations.append(
                {
                    "station_id": int(structure_id),
                    "station_name": str(structure_name),
                    "system_id": int(system_id),
                    "owner_id": s.get("corporation_id"),
                    "type_id": s.get("type_id"),
                    "type_name": s.get("type_name"),
                    "services": s.get("services"),
                    "__kind": "corp_structure",
                }
            )
    except ValueError as ve:
        st.warning(f"Failed to load stations/structures: {ve}")
    except Exception as e:
        st.error(f"Failed to load stations/structures: {e}")
        st.stop()

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
        selected_station = next(
            (s for s in stations if str(s["station_id"]) == selected_station_key),
            None,
        )
        if isinstance(selected_station, dict):
            selected_kind = selected_station.get("__kind")
            selected_upwell_type_name = (
                selected_station.get("type_name")
                or selected_station.get("structure_type_name")
                or selected_station.get("type")
            )
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
            col1, col2 = st.columns(2)
            with col1:
                owner_label = (
                    selected_station.get("owner_name")
                    or selected_station.get("owner")
                    or selected_station.get("owner_id")
                    or "N/A"
                )
                st.write(f"**Owner:** {owner_label}")
            # Hide extra factors that are usually 1.0/noisy for end users.
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
                                continue
                            if isinstance(service, dict):
                                name = (
                                    service.get("service_name")
                                    or service.get("name")
                                    or service.get("service")
                                    or "Unknown"
                                )
                                state = service.get("state") or service.get("status")
                                st.write(f"• {name}{f' ({state})' if state else ''}")
                                continue
                            st.write(f"• {service}")
                    else:
                        st.write(f"• {services}")

    st.markdown("**Profile Settings**")

    is_npc_station = selected_station_key != "none" and selected_kind == "npc_station"

    # Facility (structure base) bonuses: derived from SDE (typeBonus) when possible.
    facility_me_bonus_pct = 0.0
    facility_te_bonus_pct = 0.0
    facility_cost_bonus_pct = 0.0
    if not is_npc_station:
        try:
            if selected_upwell_type_id is not None:
                b_resp = cached_api_get(f"/structure_type_bonuses/{int(selected_upwell_type_id)}")
                if b_resp and b_resp.get("status") == "success":
                    bonuses = (b_resp.get("data") or {}).get("bonuses") or {}
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

    # Installation tax:
    # - NPC stations: fixed per facility (fetch via ESI)
    # - Upwell: user input
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
        # As of 2026-01, ESI does not provide a reliable per-facility tax field.
        # Use fixed NPC station defaults.
        npc_facility_tax = _NPC_STATION_FACILITY_TAX
        npc_scc_surcharge = _NPC_STATION_SCC_SURCHARGE

    col1, col2 = st.columns(2)
    with col1:
        st.write(f"**System Index Cost (live):** {float(system_mfg_cost_index):.2%}")
    with col2:
        if is_npc_station:
            # Display both components explicitly.
            st.caption(
                f"NPC station job fees — Facility tax: {float(npc_facility_tax or 0.0):.2%} | "
                f"SCC surcharge: {float(npc_scc_surcharge or 0.0):.2%}"
            )
            total_npc_rate = float(npc_facility_tax or 0.0) + float(npc_scc_surcharge or 0.0)
            st.write(f"**Total NPC surcharge:** {total_npc_rate:.2%}")
            # Keep `installation_cost_modifier` in percent for payload consistency.
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
            upwell_scc_surcharge = _UPWELL_SCC_SURCHARGE
            st.caption(f"SCC surcharge (fixed): {float(upwell_scc_surcharge):.2%}")
            st.write(
                f"**Total job surcharge:** {(upwell_facility_tax_pct / 100.0 + float(upwell_scc_surcharge)):.2%}"
            )

    rig_slot0_type_id = 0
    rig_slot1_type_id = 0
    rig_slot2_type_id = 0
    structure_rig_material_bonus = 0.0
    structure_rig_time_bonus = 0.0
    structure_rig_cost_bonus = 0.0

    if is_npc_station:
        st.caption("Structure rigs are not applicable to NPC stations.")
    else:
        # If we can infer the Upwell structure size, filter the rig options to matching M/L/XL rigs.
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
            rig_slot0_type_id = st.selectbox(
                "Rig Slot 1",
                options=filtered_rig_option_ids,
                format_func=lambda x: rig_options.get(x, str(x)),
            )
        with col4:
            rig_slot1_type_id = st.selectbox(
                "Rig Slot 2",
                options=filtered_rig_option_ids,
                format_func=lambda x: rig_options.get(x, str(x)),
            )
        with col5:
            rig_slot2_type_id = st.selectbox(
                "Rig Slot 3",
                options=filtered_rig_option_ids,
                format_func=lambda x: rig_options.get(x, str(x)),
            )

        structure_rig_material_bonus, structure_rig_time_bonus, structure_rig_cost_bonus = _compute_rig_bonuses(
            [int(rig_slot0_type_id), int(rig_slot1_type_id), int(rig_slot2_type_id)],
            rig_bonus_map,
        )

        st.caption(f"Total rig bonuses — ME: {structure_rig_material_bonus:.2%} | TE: {structure_rig_time_bonus:.2%} | Cost: {structure_rig_cost_bonus:.2%}")
        breakdown = _rig_breakdown_lines(
            [int(rig_slot0_type_id), int(rig_slot1_type_id), int(rig_slot2_type_id)],
            rig_options,
            rig_bonus_map,
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
        )
        st.dataframe(effects_df, width="stretch", hide_index=True)

    with st.form("create_profile_form"):
        profile_name = st.text_input(
            "Profile Name",
            placeholder="e.g., Jita 4-4 Tatara",
        )
        is_default = st.checkbox("Set as default")
        submitted = st.form_submit_button("Create Profile", type="primary")

    if submitted:
        location_id = None
        location_name = None
        location_type = "structure"
        if selected_station_key != "none":
            location_id = int(selected_station_key)
            location_name = station_options[selected_station_key]
            location_type = "station" if is_npc_station else "structure"

        # Persist the stable identifiers (system/location). Do NOT persist system cost index.
        region_id = selected_system.get("region_id") if selected_system else None
        system_id = selected_system_id
        facility_id = location_id
        if is_npc_station:
            facility_type = "npc_station"
        else:
            # store exact upwell type name if available (e.g. Raitaru/Athanor/...)
            facility_type = selected_upwell_type_name or "upwell_structure"

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
                "location_id": location_id,
                "location_name": location_name,
                "location_type": location_type,
                # Reuse existing fields for facility ME/TE bonuses.
                "material_efficiency_bonus": float(facility_me_bonus_pct or 0.0),
                "time_efficiency_bonus": float(facility_te_bonus_pct or 0.0),
                # Store as a total surcharge fraction (facility + SCC) for consistent downstream calculations.
                "installation_cost_modifier": (
                    (float(npc_facility_tax or 0.0) + float(npc_scc_surcharge or 0.0))
                    if is_npc_station
                    else ((float(upwell_facility_tax_pct or 0.0) / 100.0) + float(upwell_scc_surcharge or 0.0))
                ),
                "structure_rig_material_bonus": structure_rig_material_bonus,
                "structure_rig_time_bonus": structure_rig_time_bonus,
                "structure_rig_cost_bonus": structure_rig_cost_bonus,
                "rig_slot0_type_id": None if int(rig_slot0_type_id) == 0 else int(rig_slot0_type_id),
                "rig_slot1_type_id": None if int(rig_slot1_type_id) == 0 else int(rig_slot1_type_id),
                "rig_slot2_type_id": None if int(rig_slot2_type_id) == 0 else int(rig_slot2_type_id),
            },
        )

        if create_response.get("status") == "success":
            st.success("Profile created successfully!")
            _get_industry_profiles.clear()
            st.session_state["show_create_new_profile"] = False
            _rerun()
        else:
            st.error(f"Error: {create_response.get('message')}")