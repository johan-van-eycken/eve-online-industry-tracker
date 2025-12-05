import streamlit as st
import pandas as pd

from utils.app_init import load_config, init_db_app
from utils.flask_api import api_get, api_post, api_put, api_delete

# --- Cached API calls 900sec (15min) ---
@st.cache_data(ttl=900)
def cached_api_get(endpoint: str):
    return api_get(endpoint)

def render():
    st.subheader("Industry Profiles")

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

    # Fetch profiles
    response = api_get(f"/industry_profiles/{selected_character_id}")
    if response is None or response.get("status") != "success":
        st.error(
            f"Error loading profiles: {response.get('message') if response else 'API connection failed'}"
        )
        return

    profiles = response.get("data", [])

    # Display existing profiles
    st.markdown("### Existing Profiles")
    if profiles:
        for profile in profiles:
            with st.expander(
                f"{'⭐ ' if profile['is_default'] else ''}{profile['profile_name']}"
            ):
                col1, col2, col3 = st.columns([2, 2, 1])

                with col1:
                    st.write(f"**Location:** {profile['location_name'] or 'Not set'}")
                    st.write(f"**Type:** {profile['location_type'] or 'N/A'}")
                    st.write(f"**ME Bonus:** {profile['material_efficiency_bonus']}%")
                    st.write(f"**TE Bonus:** {profile['time_efficiency_bonus']}%")

                with col2:
                    st.write(f"**Cost Index:** {profile['manufacturing_cost_index']:.4f}")
                    st.write(f"**Installation Tax:** {profile['installation_cost_modifier']:.2%}")
                    st.write(f"**Rig Material Bonus:** {profile['structure_rig_material_bonus']:.2%}")
                    st.write(f"**Rig Time Bonus:** {profile['structure_rig_time_bonus']:.2%}")

                with col3:
                    if st.button("Delete", key=f"delete_{profile['id']}"):
                        delete_response = api_delete(f"/industry_profiles/{profile['id']}")
                        if delete_response.get("status") == "success":
                            st.success("Profile deleted")
                            st.experimental_rerun()
                        else:
                            st.error(f"Error: {delete_response.get('message')}")
    else:
        st.info("No profiles found. Create one below.")

    st.divider()

    # Create new profile - Single step form
    st.markdown("### Create New Profile")

    profile_name = st.text_input(
        "Profile Name",
        placeholder="e.g., Jita 4-4 Tatara",
    )
    is_default = st.checkbox("Set as default")

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

    if selected_system:
        st.caption(
            f"**Region:** {selected_system.get('region_name', 'Unknown')} | "
            f"**Constellation:** {selected_system.get('constellation_name', 'Unknown')} | "
            f"**Security:** {selected_system.get('security_status', 0.0):.2f}"
        )

    # Fetch NPC stations
    try:
        npc_stations_response = cached_api_get(f"/npc_stations/{selected_system_id}")
        public_structures = cached_api_get(f"/public_structures/{selected_system_id}")
        try:
            df_struct = db.query("corporation_structures", params={"system_id": selected_system_id})
        except Exception as e:
            st.error(f"Failed to load corporation structures: {e}")
            st.stop()
        
        if npc_stations_response is None or npc_stations_response.get("status") != "success":
            e = npc_stations_response.get("message") if npc_stations_response else "API connection failed"
            st.error(f"Failed to load NPC stations: {e}")
            st.stop()
        if public_structures is None or public_structures.get("status") != "success":
            e = public_structures.get("message") if public_structures else "API connection failed"
            st.error(f"Failed to load public structures: {e}")
            st.stop()
        
        stations = npc_stations_response.get("data", [])
        stations.extend(public_structures.get("data", []))
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
    if selected_station_key != "none":
        selected_station = next(
            (s for s in stations if str(s["station_id"]) == selected_station_key),
            None,
        )
        if selected_station:
            st.markdown("**Station Details**")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Owner:** {selected_station.get('owner_id', 'N/A')}")
                st.write(f"**Reprocessing Efficiency:** {selected_station.get('reprocessing_efficiency', 0):.1%}")
            with col2:
                st.write(f"**Manufacturing Factor:** {selected_station.get('manufacturing_factor', 1.0):.2f}")
                st.write(f"**Research Factor:** {selected_station.get('research_factor', 1.0):.2f}")
            if selected_station.get("services"):
                with st.expander("Available Services"):
                    for service in selected_station["services"]:
                        st.write(f"• {service['service_name']}")

    st.markdown("**Industry Bonuses**")
    col1, col2 = st.columns(2)
    with col1:
        material_efficiency_bonus = st.number_input(
            "ME Bonus (%)",
            min_value=0.0,
            max_value=25.0,
            value=0.0,
            step=0.1,
        )
        time_efficiency_bonus = st.number_input(
            "TE Bonus (%)",
            min_value=0.0,
            max_value=25.0,
            value=0.0,
            step=0.1,
        )
    with col2:
        manufacturing_cost_index = st.number_input(
            "Manufacturing Cost Index",
            min_value=0.0,
            max_value=1.0,
            value=0.0,
            step=0.0001,
            format="%.4f",
        )
        installation_cost_modifier = st.number_input(
            "Installation Tax (%)",
            min_value=-100.0,
            max_value=100.0,
            value=0.0,
            step=0.1,
        )

    st.markdown("**Structure Rigs**")
    col3, col4, col5 = st.columns(3)
    with col3:
        structure_rig_material_bonus = st.number_input(
            "Material Reduction (%)",
            min_value=0.0,
            max_value=50.0,
            value=0.0,
            step=0.1,
        )
    with col4:
        structure_rig_time_bonus = st.number_input(
            "Time Reduction (%)",
            min_value=0.0,
            max_value=50.0,
            value=0.0,
            step=0.1,
        )
    with col5:
        structure_rig_cost_bonus = st.number_input(
            "Cost Reduction (%)",
            min_value=0.0,
            max_value=50.0,
            value=0.0,
            step=0.1,
        )

    if st.button("Create Profile", type="primary", use_container_width=True):
        location_id = None
        location_name = None
        location_type = "structure"
        if selected_station_key != "none":
            location_id = int(selected_station_key)
            location_name = station_options[selected_station_key]
            location_type = "station"

        create_response = api_post(
            "/industry_profiles",
            {
                "character_id": selected_character_id,
                "profile_name": profile_name,
                "is_default": is_default,
                "location_id": location_id,
                "location_name": location_name,
                "location_type": location_type,
                "material_efficiency_bonus": material_efficiency_bonus,
                "time_efficiency_bonus": time_efficiency_bonus,
                "manufacturing_cost_index": manufacturing_cost_index / 100.0
                    if manufacturing_cost_index > 1
                    else manufacturing_cost_index,
                "installation_cost_modifier": installation_cost_modifier / 100.0,
                "structure_rig_material_bonus": structure_rig_material_bonus / 100.0,
                "structure_rig_time_bonus": structure_rig_time_bonus / 100.0,
                "structure_rig_cost_bonus": structure_rig_cost_bonus / 100.0,
            },
        )

        if create_response.get("status") == "success":
            st.success("Profile created successfully!")
            st.experimental_rerun()
        else:
            st.error(f"Error: {create_response.get('message')}")