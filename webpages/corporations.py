import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]
import json

from utils.app_init import load_config, init_db_app
from utils.formatters import format_datetime, format_date_countdown, format_isk
from utils.flask_api import api_get, api_post

def render():
    # -- Custom Style --
    st.markdown("""
    <style>
    .tile-member {
        background-color: #23272f;
        padding: 14px;
        border-radius: 10px;
        box-shadow: 1px 1px 6px rgba(0,0,0,0.3);
        text-align: center;
        margin-bottom: 10px;
        border: 1px solid #444;
    }
    .tile-member.ceo {
        border: 3px solid gold;
    }
    .tile-member .ceo-label {
        color: gold;
        font-weight: bold;
        font-size: 13px;
        margin-bottom: 2px;
    }
    .tile-member .member-label {
        color: #aaa;
        font-weight: bold;
        font-size: 13px;
        margin-bottom: 2px;
    }
    .tile-structure {
        background-color: #23272f;
        padding: 16px;
        border-radius: 10px;
        box-shadow: 1px 1px 6px rgba(0,0,0,0.3);
        text-align: center;
        margin-bottom: 10px;
    }
    .tooltip {
        position: relative;
        display: inline-block;
        cursor: pointer;
    }
    .tooltip .tooltiptext {
        visibility: hidden;
        width: 520px;
        background-color: #1e293b;
        color: #f0f0f0;
        text-align: left;
        padding: 10px;
        border-radius: 8px;
        position: absolute;
        z-index: 10;
        bottom: 125%;
        left: 50%;
        transform: translateX(-50%);
        opacity: 0;
        transition: opacity 0.3s;
        font-size: 13px;
        line-height: 1.4;
        box-shadow: 0 2px 8px rgba(0,0,0,0.5);
    }
    .tooltip:hover .tooltiptext {
        visibility: visible;
        opacity: 1;
    }
    .tooltip .service-online {
        color: #22c55e;
        font-weight: bold;
    }
    .tooltip .service-offline {
        color: #ef4444;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

    st.subheader("Corporations")

    try:
        cfgManager = load_config()
        db = init_db_app(cfgManager)
    except Exception as e:
        st.error(f"Failed to load database: {e}")
        st.stop()

    try:
        df = db.load_df("corporations")
    except Exception:
        st.warning("No corporation data found. Run main.py first.")
        st.stop()

    # CEO character_name ophalen uit characters tabel
    try:
        df_chars = db.load_df("characters")
        ceo_lookup = df_chars.set_index("character_id")["character_name"].to_dict()
    except Exception:
        ceo_lookup = {}

    # Voeg een logo-url toe indien gewenst (EVE image server)
    if "logo_url" not in df.columns:
        df["logo_url"] = df["corporation_id"].apply(
            lambda cid: f"https://images.evetech.net/corporations/{cid}/logo?size=128"
        )

    # Toon corporations als tegels
    cards_per_row = 4
    for i in range(0, len(df), cards_per_row):
        cols = st.columns(cards_per_row)
        for j, col in enumerate(cols):
            if i + j >= len(df):
                break
            row = df.iloc[i + j]
            ceo_name = ceo_lookup.get(row.get("ceo_id"), "Unknown")
            tax_rate = row.get("tax_rate")
            tax_rate_str = f"{tax_rate*100:.2f}%" if tax_rate is not None else "N/A"
            war_eligible = row.get("war_eligible")
            war_eligible_str = "Yes" if war_eligible else "No"

            # --- Extract Master Wallet balance ---
            master_wallet_balance = None
            wallets_raw = row.get("wallets", None)
            if wallets_raw:
                try:
                    wallets = json.loads(wallets_raw) if isinstance(wallets_raw, str) else wallets_raw
                    master_wallet = next((w for w in wallets if str(w.get("division")) == "1"), None)
                    if master_wallet:
                        master_wallet_balance = master_wallet.get("balance", 0.0)
                except Exception:
                    master_wallet_balance = None
            # fallback if not found
            if master_wallet_balance is None:
                master_wallet_balance = 0.0

            with col:
                st.markdown(
                    f"""
                    <div style="background-color: rgba(30,30,30,0.95); padding: 20px; border-radius: 10px; box-shadow: 2px 2px 10px rgba(0,0,0,0.5); text-align: center; margin-bottom: 10px;">
                        <img src="{row['logo_url']}" width="96" style="border-radius:8px; margin-bottom:10px;" />
                        <div style="font-size:16px; color:#f0f0f0;">
                            <b style="font-size:18px;">{row.get('corporation_name', 'Unknown')}</b>&nbsp;
                            <span style="color:#aaa;">[{row.get('ticker', '')}]</span><br>
                            <b style="font-size:14px; color:#888;">(ID: {row.get('corporation_id', '')})</b><br>
                            CEO: {ceo_name}<br>
                            Members: {row.get('member_count', 'N/A')}<br>
                            Tax Rate: {tax_rate_str}<br>
                            War Eligible: {war_eligible_str}<br><br>
                            <b>Master Wallet Balance:</b><br>
                            <b>{format_isk(master_wallet_balance)}</b>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

    st.divider()

    # Detailweergave: selecteer een corporation
    st.subheader("Corporation Details")
    corp_options = df.set_index("corporation_id")["corporation_name"].to_dict()
    selected_id = st.selectbox(
        "Select corporation:",
        options=list(corp_options.keys()),
        format_func=lambda x: corp_options[x]
    )

    if not selected_id:
        return

    # Tabs voor Members en Structures
    members_tab, structures_tab, assets_tab = st.tabs(["Members", "Structures", "Assets"])

    # --- MEMBERS TAB ---
    with members_tab:
        try:
            df_members = db.load_df("corporation_members")
            df_members["corporation_id"] = df_members["corporation_id"].astype(int)
            df_members["character_id"] = df_members["character_id"].astype(int)
            ceo_id = int(df[df["corporation_id"] == selected_id]["ceo_id"].iloc[0])

            df_chars = db.load_df("characters")
            wallet_lookup = df_chars.set_index("character_id")["wallet_balance"].to_dict()

            df_members = df_members[df_members["corporation_id"] == int(selected_id)]
            if df_members.empty:
                st.info("No members found for this corporation.")
            else:
                # CEO als eerste tile, rest alfabetisch
                df_ceo = df_members[df_members["character_id"] == ceo_id]
                df_others = df_members[df_members["character_id"] != ceo_id].sort_values("character_name")
                df_members_sorted = pd.concat([df_ceo, df_others], ignore_index=True) if not df_ceo.empty else df_others

                def format_titles(titles):
                    if not titles or titles in ("", "null"):
                        return ""
                    if isinstance(titles, str):
                        try:
                            titles = json.loads(titles)
                        except Exception:
                            return titles  # Dit is gewone tekst, geen HTML!
                    if isinstance(titles, dict):
                        return titles.get("title_name", "")
                    if isinstance(titles, list):
                        return ", ".join(
                            t.get("title_name", str(t)) if isinstance(t, dict) else str(t)
                            for t in titles
                        )
                    return str(titles)

                cards_per_row = 5
                for i in range(0, len(df_members_sorted), cards_per_row):
                    cols = st.columns(cards_per_row)
                    for j, col in enumerate(cols):
                        if i + j >= len(df_members_sorted):
                            break
                        member = df_members_sorted.iloc[i + j]
                        titles_str = format_titles(member["titles"])
                        portrait_url = f"https://images.evetech.net/characters/{member['character_id']}/portrait?size=64"
                        is_ceo = member["character_id"] == ceo_id
                        border = "3px solid gold" if is_ceo else "1px solid #444"
                        crown = "👑 " if is_ceo else ""
                        div_class = "tile-member ceo" if is_ceo else "tile-member"
                        ceo_label_html = '<div class="ceo-label">CEO</div>' if is_ceo else '<div class="member-label">MEMBER</div>'
                        wallet_balance = wallet_lookup.get(member['character_id'], 0.0)
                        with col:
                            st.markdown(
                                f"""
                                <div class="{div_class}">
                                    {ceo_label_html}
                                    <img src="{portrait_url}" width="64" style="border-radius:8px; margin-bottom:8px;" />
                                    <div style="font-size:15px; color:#f0f0f0;">
                                        <b>{crown}{member['character_name']}</b><br>
                                        <span style="color:#aaa;">ID: {member['character_id']}</span><br>
                                        <span style="font-size:13px;">{titles_str}</span><br>
                                        <br>
                                        <span><b> Wallet Balance:</b><br>
                                            {format_isk(wallet_balance)}
                                    </div>
                                </div>
                                """,
                                unsafe_allow_html=True
                            )
        except Exception as e:
            st.info(f"No members found for this corporation. ({e})")

    # --- STRUCTURES TAB ---
    with structures_tab:
        try:
            df_struct = db.load_df("corporation_structures")
        except Exception as e:
            st.error(f"Failed to load corporation structures: {e}")
            st.exception(e)  # Shows the full stack trace in Streamlit
            st.stop()
        
        try:
            # Ensure types match for filtering
            df_struct["corporation_id"] = df_struct["corporation_id"].astype(int)
            selected_id_int = int(selected_id)

            df_struct_filtered = df_struct[df_struct["corporation_id"] == selected_id_int]

            # Now continue with rendering cards using df_struct_filtered
            cards_per_row = 3
            for i in range(0, len(df_struct_filtered), cards_per_row):
                cols = st.columns(cards_per_row)
                for j, col in enumerate(cols):
                    if i + j >= len(df_struct_filtered):
                        break
                    struct = df_struct_filtered.iloc[i + j]
                    # Structure afbeelding
                    type_id = struct.get('type_id', '')
                    type_img_url = f"https://images.evetech.net/types/{type_id}/icon?size=128" if type_id else ""
                
                    with col:
                        fuel_expires_str = format_datetime(struct.get('fuel_expires'))
                        st.markdown(
                            f"""
                            <div class="tile-structure">
                                <div class="tooltip">
                                    <img src="{type_img_url}" width="128" style="border-radius:8px; margin-bottom:8px;" />
                                    <div class="tooltiptext">
                                        <span>{struct.get('type_description', '')}</span><br><br>
                                        {struct.get('services', '')}
                                    </div>
                                </div>
                                <div style="font-size:15px; color:#f0f0f0;">
                                    <b>{struct.get('structure_name', 'Unknown')}</b><br>
                                    <span style="color:#aaa;">ID: {struct.get('structure_id', '')}</span><br>
                                    <span>Location: {struct.get('system_name', '')} - {struct.get('region_name', '')}</span><br>
                                    <span>Type: {struct.get('type_name', '')} - {struct.get('group_name', '')}</span><br>
                                    <span>Status: {struct.get('state', '')}</span><br><br>
                                    <span>Fuel Expires: {format_datetime(struct.get('fuel_expires'))} ({format_date_countdown(struct.get('fuel_expires'))})</span><br><br>
                                </div>
                            </div>
                        """,
                            unsafe_allow_html=True
                        )
        except Exception as e:
            st.info("No structures found for this corporation.")
            st.error(str(e))
        
    # --- CORPORATION ASSETS TAB ---
    with assets_tab:
        st.subheader("Assets")

        # Location info, cached for 3600 seconds (1 hour)
        @st.cache_data(ttl=3600) 
        def get_location_info_cached(location_ids):
            try:
                response = api_post(f"/locations", payload={"location_ids": list(map(int, location_ids))})
                return response
            except Exception as e:
                st.error(f"Error fetching location info from backend: {e}")
                return {}

        # Load and filter corporation assets
        try:
            assets_df = db.load_df("corporation_assets")
            assets_df = assets_df[assets_df["corporation_id"] == selected_id]
        except Exception:
            st.warning("No corporation assets data available.")
            st.stop()

        # Filter Structures
        assets_df = assets_df[assets_df["location_type"] != "solar_system"]

        # Get unique station IDs
        location_ids = assets_df["top_location_id"].unique()
        location_info_map = get_location_info_cached(location_ids)

        # For each location, fetch and assign its name using the API
        location_data = location_info_map.get("data", {})
        for loc_id in location_ids:
            location_info = location_data.get(str(loc_id)) or {}
            location_name = location_info.get("name", str(loc_id))
            assets_df.loc[assets_df["location_id"] == loc_id, "location_name"] = location_name

        # Build a mapping of location_id to location_name for dropdown display
        location_names = {
            location_id: assets_df[assets_df["location_id"] == location_id]["location_name"].iloc[0]
            if "location_name" in assets_df.columns else str(location_id)
            for location_id in location_ids
        }

        # Sort location_ids by their names alphabetically
        sorted_location_ids = sorted(location_names.keys(), key=lambda x: location_names[x].lower())

        col1, col2 = st.columns([4,1])
        with col1:
            # Precompile asset map for dropdown
            asset_map = {
                f"{row['type_name']}": row['item_id']
                for _, row in assets_df.iterrows()
            }
            dropdown_options = ["Find asset by name:"] + sorted(list(asset_map.keys()))
            selected_asset_label = st.selectbox(
                "Find asset by name:",
                options=dropdown_options,
                label_visibility="collapsed"
            )

            selected_location_id = None
            selected_asset_id = None
            if selected_asset_label != "Find asset by name:":
                selected_asset_id = asset_map[selected_asset_label]
                selected_asset_row = assets_df[assets_df["item_id"] == selected_asset_id].iloc[0]
                selected_location_id = selected_asset_row["top_location_id"]
            
            if selected_location_id is not None and selected_location_id in sorted_location_ids:
                loc_index = sorted_location_ids.index(selected_location_id)
            else:
                loc_index = 0
            
            selected_location_id = st.selectbox(
                "Select a Location:",
                options=sorted_location_ids,
                format_func=lambda x: location_names[x],
                index=loc_index,
            )
        with col2:
            # Button to refresh assets
            if st.button("Refresh Assets"):
                refreshed_data = api_get("/refresh_assets")
                if refreshed_data:
                    st.success("Assets refreshed successfully.")
                else:
                    st.error("Failed to refresh assets.")

        st.divider()

        def add_item_images(df):
            df = df.copy()
            # Determine image variation for each row
            def get_variation(row):
                if "type_category_name" in row and row["type_category_name"] == "Blueprint":
                    if "is_blueprint_copy" in row and row["is_blueprint_copy"]:
                        return "bpc"
                    else:
                        return "bp"
                elif "type_category_name" in row and row["type_category_name"] == "Permanent SKIN":
                    return "skins"
                else:
                    return "icon"
            
            df["image_variation"] = df.apply(get_variation, axis=1)
            df["image_url"] = df.apply(
                lambda row: f'https://images.evetech.net/types/{row["type_id"]}/{row["image_variation"]}?size=32',
                axis=1
            )
            return df

        if selected_location_id:
            # Find OfficeFolder item_ids for the selected location
            office_folder_item_ids = assets_df[
                (assets_df["location_flag"] == "OfficeFolder") &
                (assets_df["location_id"] == selected_location_id)
            ]["item_id"].unique()

            # Containers are either directly at the location or inside its OfficeFolder
            division_flags = ["CorpSAG1", "CorpSAG2", "CorpSAG3", "CorpSAG4", "CorpSAG5", "CorpSAG6", "CorpSAG7"]
            containers = assets_df[
                (
                    (assets_df["location_id"] == selected_location_id) |
                    (assets_df["location_id"].isin(office_folder_item_ids))
                ) &
                (assets_df["is_container"]) &
                (assets_df["location_flag"].isin(division_flags))
            ].sort_values(by="container_name")

            # For each division, show containers and items
            for division_flag, division_label in zip(division_flags, [
                "*1st Division*",
                "*2nd Division*",
                "*3rd Division*",
                "*4th Division*",
                "*5th Division*",
                "*6th Division*",
                "*7th Division*",
            ]):
                division_containers = containers[containers["location_flag"] == division_flag]
                # Items directly in the division folder (not in a container)
                direct_items = assets_df[
                    (
                        (assets_df["location_id"] == selected_location_id) |
                        (assets_df["location_id"].isin(office_folder_item_ids))
                    ) &
                    (assets_df["location_flag"] == division_flag) &
                    (~assets_df["is_container"])
                ]
                has_items = not direct_items.empty
                for _, container in division_containers.iterrows():
                    items_in_container = assets_df[assets_df["location_id"] == container["item_id"]]
                    if not items_in_container.empty:
                        has_items = True
                        break
                if has_items:
                    st.markdown(division_label)
                    # Show direct items first
                    if not direct_items.empty:
                        st.markdown("Direct items in division:")
                        df = add_item_images(direct_items)
                        df["total_volume"] = df["type_volume"] * df["quantity"]
                        df["total_average_price"] = df["type_average_price"] * df["quantity"]
                        display_columns = ["image_url","type_name", "quantity", "type_volume", "total_volume", "type_average_price", "total_average_price", "type_group_name","type_category_name"]
                        df_display = df[display_columns].sort_values(by="type_name")
                        column_config = {
                            "image_url": st.column_config.ImageColumn("", width="auto"),
                            "type_name": st.column_config.TextColumn("Name", width="auto"),
                            "quantity": st.column_config.NumberColumn("Quantity", width="auto"),
                            "type_volume": st.column_config.NumberColumn("Volume", width="auto"),
                            "total_volume": st.column_config.NumberColumn("Total Volume", width="auto"),
                            "type_average_price": st.column_config.NumberColumn("Value", width="auto"),
                            "total_average_price": st.column_config.NumberColumn("Total Value", width="auto"),
                            "type_group_name": st.column_config.TextColumn("Group", width="auto"),
                            "type_category_name": st.column_config.TextColumn("Category", width="auto"),
                        }
                        st.dataframe(df_display, use_container_width=True, column_config=column_config, hide_index=True)
                    # Then show containers and their items
                    for _, container in division_containers.iterrows():
                        items_in_container = assets_df[assets_df["location_id"] == container["item_id"]]
                        is_selected = selected_asset_id in items_in_container["item_id"].values
                        if not items_in_container.empty:
                            total_average_price = (items_in_container["type_average_price"] * items_in_container["quantity"]).sum()
                            with st.expander(
                                f"{container['container_name']} ({items_in_container['type_name'].nunique()} unique items, Total Value: {total_average_price:,.2f} ISK)",
                                expanded=is_selected
                            ):
                                used_volume = (items_in_container["type_volume"] * items_in_container["quantity"]).sum()
                                max_capacity = container.get("type_capacity", None)
                                if max_capacity and max_capacity > 0:
                                    percent_full = min(used_volume / max_capacity, 1.0)
                                    st.progress(percent_full, text=f"{used_volume:,.2f} / {max_capacity:,.2f} m³ used")
                                else:
                                    st.info("No capacity information available for this container.")

                                if not items_in_container.empty:
                                    df = add_item_images(items_in_container)
                                    df["total_volume"] = df["type_volume"] * df["quantity"]
                                    df["total_average_price"] = df["type_average_price"] * df["quantity"]
                                    display_columns = ["image_url","type_name", "quantity", "type_volume", "total_volume", "type_average_price", "total_average_price", "type_group_name","type_category_name"]
                                    df_display = df[display_columns].sort_values(by="type_name")
                                    column_config = {
                                        "image_url": st.column_config.ImageColumn("", width="auto"),
                                        "type_name": st.column_config.TextColumn("Name", width="auto"),
                                        "quantity": st.column_config.NumberColumn("Quantity", width="auto"),
                                        "type_volume": st.column_config.NumberColumn("Volume", width="auto"),
                                        "total_volume": st.column_config.NumberColumn("Total Volume", width="auto"),
                                        "type_average_price": st.column_config.NumberColumn("Value", width="auto"),
                                        "total_average_price": st.column_config.NumberColumn("Total Value", width="auto"),
                                        "type_group_name": st.column_config.TextColumn("Group", width="auto"),
                                        "type_category_name": st.column_config.TextColumn("Category", width="auto"),
                                    }
                                    st.dataframe(df_display, use_container_width=True, column_config=column_config, hide_index=True)
                                else:
                                    st.info("No items in this container.")

            st.markdown("**Corporation Deliveries:**")
            # Exclude these flags from Corporation Deliveries
            exclude_flags = [
                "RigSlot0", "RigSlot1", "RigSlot2",
                "ServiceSlot0", "ServiceSlot1", "ServiceSlot2"
            ]

            # Corporation Deliveries (excluding OfficeFolder, containers, ships, asset safety wraps, and excluded flags)
            deliveries_items = assets_df[
                (assets_df["location_id"] == selected_location_id) &
                ~(assets_df["is_container"] |
                assets_df["is_ship"] |
                assets_df["is_asset_safety_wrap"] |
                assets_df["is_office_folder"]) &
                (~assets_df["location_flag"].isin(exclude_flags + ["StructureFuel", "QuantumCoreRoom"]))
            ]
            if deliveries_items.empty:
                with st.expander("No corporation deliveries found at this location."):
                    st.info("No corporation deliveries found at this location.")
            else:
                total_average_price = (deliveries_items["type_average_price"] * deliveries_items["quantity"]).sum()
                st.markdown(f"Items: {deliveries_items['type_name'].nunique()} - Total Volume: {deliveries_items['type_volume'].dot(deliveries_items['quantity']):,.2f} m³ - Total Value: {total_average_price:,.2f} ISK")
                df = add_item_images(deliveries_items)
                df["total_volume"] = df["type_volume"] * df["quantity"]
                df["total_average_price"] = df["type_average_price"] * df["quantity"]
                display_columns = ["image_url","type_name", "quantity", "type_volume", "total_volume","type_average_price","total_average_price","type_group_name","type_category_name"]
                df_display = df[display_columns].sort_values(by="type_name")
                column_config = {
                    "image_url": st.column_config.ImageColumn("", width="auto"),
                    "type_name": st.column_config.TextColumn("Name", width="auto"),
                    "quantity": st.column_config.NumberColumn("Quantity", width="auto"),
                    "type_volume": st.column_config.NumberColumn("Volume", width="auto"),
                    "total_volume": st.column_config.NumberColumn("Total Volume", width="auto"),
                    "type_average_price": st.column_config.NumberColumn("Value", width="auto"),
                    "total_average_price": st.column_config.NumberColumn("Total Value", width="auto"),
                    "type_group_name": st.column_config.TextColumn("Group", width="auto"),
                    "type_category_name": st.column_config.TextColumn("Category", width="auto"),
                }
                st.dataframe(df_display, use_container_width=True, column_config=column_config, hide_index=True)
            st.divider()

            # Structure Fuel section
            structure_fuel_items = assets_df[
                (assets_df["location_id"] == selected_location_id) &
                (assets_df["location_flag"] == "StructureFuel")
            ]
            if not structure_fuel_items.empty:
                st.markdown("**Structure Fuel:**")
                total_fuel_value = (structure_fuel_items["type_average_price"] * structure_fuel_items["quantity"]).sum()
                st.markdown(f"Items: {structure_fuel_items['type_name'].nunique()} - Total Volume: {structure_fuel_items['type_volume'].dot(structure_fuel_items['quantity']):,.2f} m³ - Total Value: {total_fuel_value:,.2f} ISK")
                df = add_item_images(structure_fuel_items)
                df["total_volume"] = df["type_volume"] * df["quantity"]
                df["total_average_price"] = df["type_average_price"] * df["quantity"]
                display_columns = ["image_url","type_name", "quantity", "type_volume", "total_volume","type_average_price","total_average_price","type_group_name","type_category_name"]
                df_display = df[display_columns].sort_values(by="type_name")
                column_config = {
                    "image_url": st.column_config.ImageColumn("", width="auto"),
                    "type_name": st.column_config.TextColumn("Name", width="auto"),
                    "quantity": st.column_config.NumberColumn("Quantity", width="auto"),
                    "type_volume": st.column_config.NumberColumn("Volume", width="auto"),
                    "total_volume": st.column_config.NumberColumn("Total Volume", width="auto"),
                    "type_average_price": st.column_config.NumberColumn("Value", width="auto"),
                    "total_average_price": st.column_config.NumberColumn("Total Value", width="auto"),
                    "type_group_name": st.column_config.TextColumn("Group", width="auto"),
                    "type_category_name": st.column_config.TextColumn("Category", width="auto"),
                }
                st.dataframe(df_display, use_container_width=True, column_config=column_config, hide_index=True)
                st.divider()

            # Quantum Core Room section
            quantum_core_items = assets_df[
                (assets_df["location_id"] == selected_location_id) &
                (assets_df["location_flag"] == "QuantumCoreRoom")
            ]
            if not quantum_core_items.empty:
                st.markdown("**Quantum Core Room:**")
                total_core_value = (quantum_core_items["type_average_price"] * quantum_core_items["quantity"]).sum()
                st.markdown(f"Items: {quantum_core_items['type_name'].nunique()} - Total Volume: {quantum_core_items['type_volume'].dot(quantum_core_items['quantity']):,.2f} m³ - Total Value: {total_core_value:,.2f} ISK")
                df = add_item_images(quantum_core_items)
                df["total_volume"] = df["type_volume"] * df["quantity"]
                df["total_average_price"] = df["type_average_price"] * df["quantity"]
                display_columns = ["image_url","type_name", "quantity", "type_volume", "total_volume","type_average_price","total_average_price","type_group_name","type_category_name"]
                df_display = df[display_columns].sort_values(by="type_name")
                column_config = {
                    "image_url": st.column_config.ImageColumn("", width="auto"),
                    "type_name": st.column_config.TextColumn("Name", width="auto"),
                    "quantity": st.column_config.NumberColumn("Quantity", width="auto"),
                    "type_volume": st.column_config.NumberColumn("Volume", width="auto"),
                    "total_volume": st.column_config.NumberColumn("Total Volume", width="auto"),
                    "type_average_price": st.column_config.NumberColumn("Value", width="auto"),
                    "total_average_price": st.column_config.NumberColumn("Total Value", width="auto"),
                    "type_group_name": st.column_config.TextColumn("Group", width="auto"),
                    "type_category_name": st.column_config.TextColumn("Category", width="auto"),
                }
                st.dataframe(df_display, use_container_width=True, column_config=column_config, hide_index=True)
                st.divider()

            # Get all locations that contain asset safety wraps
            assetsafety_locations = assets_df[assets_df["is_asset_safety_wrap"]]["location_id"].unique()
            
            if selected_location_id in assetsafety_locations:
                st.markdown("**Asset Safety:**")
                if assets_df[assets_df["is_asset_safety_wrap"]].empty:
                    with st.expander("No Asset Safety Wraps found at this location."):
                        st.info("No Asset Safety Wraps found at this location.")
                else:
                    for _, wrap in assets_df[assets_df["is_asset_safety_wrap"]].iterrows():
                        items_in_wrap = assets_df[assets_df["location_id"] == wrap["item_id"]]
                        # calculate total average price
                        total_average_price = (items_in_wrap["type_average_price"] * items_in_wrap["quantity"]).sum()
                        
                        label = f"{wrap['type_name']} ({items_in_wrap['quantity'].sum()} items, Total Value: {total_average_price:,.2f} ISK)"
                        with st.expander(label):
                            # Calculate used and max capacity
                            used_volume = (items_in_wrap["type_volume"] * items_in_wrap["quantity"]).sum()

                            if not items_in_wrap.empty:
                                df = add_item_images(items_in_wrap)
                                df["total_volume"] = df["type_volume"] * df["quantity"]
                                df["total_average_price"] = df["type_average_price"] * df["quantity"]
                                df["type_name"] = (df["container_name"]) if df["container_name"].notnull().all() else df["type_name"]
                                df["type_name"] = (df["ship_name"]) if df["ship_name"].notnull().all() else df["type_name"]
                                display_columns = ["image_url","type_name", "quantity", "type_volume", "total_volume", "type_average_price", "total_average_price", "type_group_name","type_category_name"]
                                df_display = df[display_columns].sort_values(by="type_name")
                                column_config = {
                                    "image_url": st.column_config.ImageColumn("", width="auto"),
                                    "type_name": st.column_config.TextColumn("Name", width="auto"),
                                    "quantity": st.column_config.NumberColumn("Quantity", width="auto"),
                                    "type_volume": st.column_config.NumberColumn("Volume", width="auto"),
                                    "total_volume": st.column_config.NumberColumn("Total Volume", width="auto"),
                                    "type_average_price": st.column_config.NumberColumn("Value", width="auto"),
                                    "total_average_price": st.column_config.NumberColumn("Total Value", width="auto"),
                                    "type_group_name": st.column_config.TextColumn("Group", width="auto"),
                                    "type_category_name": st.column_config.TextColumn("Category", width="auto"),
                                }
                                st.dataframe(df_display, use_container_width=True, column_config=column_config, hide_index=True)
                            else:
                                st.info("No items in this container.")
                st.divider()
            
            # Show ships at this location
            ships = assets_df[
                (assets_df["location_id"] == selected_location_id) &
                (assets_df["is_ship"])
            ].sort_values(by="ship_name")

            st.markdown(f"**Ships:**")
            if ships.empty:
                with st.expander("No ships found at this location."):
                    st.info("No ships found at this location.")
            else:
                total_average_price = (ships["type_average_price"] * ships["quantity"]).sum()
                total_volume = (ships["type_volume"] * ships["quantity"]).sum()
                st.markdown(f"Ships: {ships['type_name'].nunique()} - Total Volume: {total_volume:,.2f} m³ - Total Value: {total_average_price:,.2f} ISK")
                # Display ships as cards/tiles
                cards_per_row = 4
                for i in range(0, len(ships), cards_per_row):
                    cols = st.columns(cards_per_row)
                    for j, col in enumerate(cols):
                        if i + j >= len(ships):
                            break
                        ship = ships.iloc[i + j]
                        image_url = f"https://images.evetech.net/types/{ship['type_id']}/render?size=128"
                        faction_url = f"https://images.evetech.net/corporations/{int(ship.get('type_faction_id', 0))}/logo?size=64"
                        ship_category = ship.get("type_group_name", "Unknown")
                        ship_group_id = ship.get("type_group_id", 0)
                        ship_meta_group_id = ship.get("type_meta_group_id", 0)
                        custom_name = ship.get("ship_name", "No Custom Name")
                        ingame_name = ship.get("type_name", "Unknown")

                        ship_icon = f"http://localhost:5000/static/images/icons/ships/"
                        # Frigate, Assault Frigate, Interdictor, Covert Ops, Interceptor,
                        #  Stealth Bomber, Electronic Attack Ship, Prototype Exploration Ship
                        #  Expedition Frigate, Logistics Frigate
                        if ship_group_id in [25, 324, 541, 830, 831, 834, 893, 1022, 1283, 1527]:
                            ship_icon += "frigate_16.png"
                        # Destroyer, Tactical Destroyer, Command Destroyer
                        elif ship_group_id in [420, 1305, 1534]:
                            ship_icon += "destroyer_16.png"
                        # Cruiser, Heavy Assault Cruiser, Force Recon Ship, Logistic, Heavy Interdiction Cruiser
                        #  Combat Recon Ship, Strategic Cruiser, Flag Cruiser
                        elif ship_group_id in [26, 358, 832, 833, 894, 906, 963, 1972]:
                            ship_icon += "cruiser_16.png"
                        # Combat Battlecruiser, Command Ship, Attack Battlecruiser
                        elif ship_group_id in [419, 540, 1201]:
                            ship_icon += "battleCruiser_16.png"
                        # Battleship, Elite Battleship, Black Ops, Marauder
                        elif ship_group_id in [27, 381, 898, 900]:
                            ship_icon += "battleship_16.png"
                        # Dreadnought, Lancer Dreadnought
                        elif ship_group_id in [485, 4594]:
                            ship_icon += "dreadnought_16.png"
                        # Carrier, Supercarrier, Force Auxiliary
                        elif ship_group_id in [547, 659, 1538]:
                            ship_icon += "carrier_16.png"
                        # Titan
                        elif ship_group_id == 30:
                            ship_icon += "titan_16.png"
                        # Hauler, Deep Space Transport, Blockade Runner
                        elif ship_group_id in [28, 380, 1202]:
                            ship_icon += "industrial_16.png"
                        # Industrial Command Ship
                        elif ship_group_id == 941:
                            ship_icon += "industrialCommand_16.png"
                        # Freighter, Capital Industrial Ship, Jump Freighter
                        elif ship_group_id in [513, 883, 902]:
                            ship_icon += "freighter_16.png"
                        # Mining Barge, Exhumer
                        elif ship_group_id in [463, 543]:
                            ship_icon += "miningBarge_16.png"
                        elif ship_group_id == 29:
                            ship_icon += "capsule_16.png"
                        elif ship_group_id == 31:
                            ship_icon += "shuttle_16.png"
                        elif ship_group_id == 237:
                            ship_icon += "rookie_16.png"
                        else:
                            ship_icon += "ship_16.png"
                        
                        ship_icon_overlay_tech = f"http://localhost:5000/static/images/icons/overlay/"
                        if ship_meta_group_id == 2:
                            ship_icon_overlay_tech += "tech_2.png"
                        elif ship_meta_group_id == 3:
                            ship_icon_overlay_tech += "tech_3.png"
                        elif ship_meta_group_id == 4:
                            ship_icon_overlay_tech += "tech_faction.png"
                        
                        ship_quantity = f"x{ship.get('quantity', 1)} {'Packaged' if not ship.get('is_singleton', False) else ''}"

                        with col:
                            st.markdown(
                                f"""
                                <div class="tooltip" style="display: flex; align-items: center; background-color: rgba(30,30,30,0.95); padding: 0px; border-radius: 10px; box-shadow: 2px 2px 10px rgba(0,0,0,0.6); margin-bottom: 10px; background-image: url('{faction_url}'); background-size: 64px 64px; background-repeat: no-repeat; background-position: 80% top; background-blend-mode: darken;">
                                    <img src="{image_url}" width="96" style="border-radius:8px; margin-right:18px;" />
                                    {f'<img src="{ship_icon_overlay_tech}" style="position: absolute; top: 0px; left: 0px; width: 24px; height: 24px; border-radius:6px;" />' if ship_icon_overlay_tech.endswith(".png") else '&nbsp;'}
                                    <div style="flex:1; color:#f0f0f0;">
                                        <div style="font-size:14px; color:#b0b0b0;">
                                            <img src="{ship_icon}" width="16" style="border-radius:6px; margin-right:4px;" />
                                            {ship_category}
                                        </div>
                                        <div style="font-size:16px; font-weight:bold; margin-top:4px;">{custom_name if custom_name is not None else ingame_name}</div>
                                        <div style="font-size:14px; color:#b0b0b0; margin-top:1px;">{ingame_name if custom_name is not None else '&nbsp;'}</div>
                                    </div>
                                    <span style="position: absolute; bottom: 8px; right: 12px; background: rgba(0,0,0,0.85); font-size: 14px; font-weight: bold; padding: 2px 8px; border-radius: 8px; z-index: 2; box-shadow: 0 1px 4px rgba(0,0,0,0.4);">{ship_quantity}</span>
                                    <span class="tooltiptext">
                                        {custom_name if custom_name is not None else ingame_name}<br />
                                        <br />
                                        Est. Value: {ship.get('type_average_price', 0) * ship.get('quantity', 0):,.2f} ISK<br />
                                        Volume: {ship.get('type_volume', 0) * ship.get('quantity', 0):,.2f} m³
                                    </span>
                                </div>
                                """,
                                unsafe_allow_html=True
                            )