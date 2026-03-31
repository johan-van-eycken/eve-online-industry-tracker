import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]
import json
from typing import cast

from utils.assets_ui import (
    ASSET_FLOAT_COLUMNS,
    ASSET_INT_COLUMNS,
    ASSET_ISK_COLUMNS,
    apply_location_names,
    build_asset_display_frame,
    render_ship_cards,
    summarize_asset_items,
)
from utils.formatters import format_datetime, format_date_countdown, format_isk, format_date_into_age
from utils.session_state import ensure_state_defaults, ensure_valid_state_value
from utils.streamlit_api import cached_api_get
from utils.aggrid_formatters import js_icon_cell_renderer
from utils.webpage_ui import render_aggrid_table, require_aggrid


def _extract_master_wallet_balance(wallets_raw) -> float:
    if not wallets_raw:
        return 0.0

    try:
        wallets = json.loads(wallets_raw) if isinstance(wallets_raw, str) else wallets_raw
        master_wallet = next((wallet for wallet in wallets if str(wallet.get("division")) == "1"), None)
        if master_wallet:
            return float(master_wallet.get("balance", 0.0) or 0.0)
    except Exception:
        return 0.0
    return 0.0


def _render_asset_table(
    render_table,
    df: pd.DataFrame,
    *,
    key: str,
    height: int | None = None,
    prefer_container_names: bool = False,
    prefer_ship_names: bool = False,
) -> None:
    df_display = build_asset_display_frame(
        df,
        prefer_container_names=prefer_container_names,
        prefer_ship_names=prefer_ship_names,
    )
    render_table(
        df_display,
        key=key,
        image_cols=["image_url"],
        number_cols_0=[column for column in ASSET_INT_COLUMNS if column in df_display.columns],
        number_cols_2=[column for column in ASSET_FLOAT_COLUMNS if column in df_display.columns],
        isk_cols=[column for column in ASSET_ISK_COLUMNS if column in df_display.columns],
        height=height,
    )


def _render_asset_summary(df: pd.DataFrame) -> None:
    unique_items, total_volume, total_value = summarize_asset_items(df)
    st.markdown(
        f"Items: {unique_items} - Total Volume: {total_volume:,.2f} m³ - Total Value: {total_value:,.2f} ISK"
    )


def _render_corporation_assets_tab(render_table, corp_row: dict) -> None:
    st.subheader("Assets")

    try:
        corp_assets = corp_row.get("assets", [])
        assets_df = pd.DataFrame(corp_assets)
    except Exception:
        st.warning("No corporation assets data available.")
        st.stop()

    assets_df = assets_df[assets_df["location_type"] != "solar_system"]
    assets_df, location_names = apply_location_names(assets_df)
    sorted_location_ids = sorted(location_names.keys(), key=lambda location_id: location_names[location_id].lower())

    asset_map = {f"{row['type_name']}": row["item_id"] for _, row in assets_df.iterrows()}
    dropdown_options = ["Find asset by name:"] + sorted(list(asset_map.keys()))
    selected_asset_label = st.selectbox(
        "Find asset by name:",
        options=dropdown_options,
        label_visibility="collapsed",
    )

    selected_location_id = None
    selected_asset_id = None
    if selected_asset_label != "Find asset by name:":
        selected_asset_id = asset_map[selected_asset_label]
        selected_asset_row = assets_df[assets_df["item_id"] == selected_asset_id].iloc[0]
        selected_location_id = selected_asset_row["top_location_id"]

    loc_index = sorted_location_ids.index(selected_location_id) if selected_location_id in sorted_location_ids else 0
    selected_location_id = st.selectbox(
        "Select a Location:",
        options=sorted_location_ids,
        format_func=lambda location_id: location_names[location_id],
        index=loc_index,
    )

    st.divider()
    if not selected_location_id:
        return

    office_folder_item_ids = assets_df[
        (assets_df["location_flag"] == "OfficeFolder") & (assets_df["location_id"] == selected_location_id)
    ]["item_id"].unique()

    division_flags = ["CorpSAG1", "CorpSAG2", "CorpSAG3", "CorpSAG4", "CorpSAG5", "CorpSAG6", "CorpSAG7"]
    division_labels = [
        "*1st Division*",
        "*2nd Division*",
        "*3rd Division*",
        "*4th Division*",
        "*5th Division*",
        "*6th Division*",
        "*7th Division*",
    ]
    container_mask = (
        ((assets_df["location_id"] == selected_location_id) | (assets_df["location_id"].isin(office_folder_item_ids)))
        & (assets_df["is_container"])
        & (assets_df["location_flag"].isin(division_flags))
    )
    containers: pd.DataFrame = cast(pd.DataFrame, assets_df.loc[container_mask, :]).sort_values(by=["container_name"])

    for division_flag, division_label in zip(division_flags, division_labels):
        division_containers = containers[containers["location_flag"] == division_flag]
        direct_items = assets_df[
            ((assets_df["location_id"] == selected_location_id) | (assets_df["location_id"].isin(office_folder_item_ids)))
            & (assets_df["location_flag"] == division_flag)
            & (~assets_df["is_container"])
        ]
        has_items = not direct_items.empty
        for _, container in division_containers.iterrows():
            if not assets_df[assets_df["location_id"] == container["item_id"]].empty:
                has_items = True
                break

        if not has_items:
            continue

        st.markdown(division_label)
        if not direct_items.empty:
            st.markdown("Direct items in division:")
            _render_asset_table(
                render_table,
                direct_items,
                key=f"corp_assets_direct_{int(selected_location_id)}_{division_flag}",
                height=320,
            )

        for _, container in division_containers.iterrows():
            items_in_container = assets_df[assets_df["location_id"] == container["item_id"]]
            is_selected = selected_asset_id in items_in_container["item_id"].values
            if items_in_container.empty:
                continue

            _, _, total_value = summarize_asset_items(items_in_container)
            with st.expander(
                f"{container['container_name']} ({items_in_container['type_name'].nunique()} unique items, Total Value: {total_value:,.2f} ISK)",
                expanded=is_selected,
            ):
                used_volume = float((items_in_container["type_volume"] * items_in_container["quantity"]).sum())
                max_capacity = container.get("type_capacity", None)
                if max_capacity and max_capacity > 0:
                    percent_full = min(used_volume / max_capacity, 1.0)
                    st.progress(percent_full, text=f"{used_volume:,.2f} / {max_capacity:,.2f} m³ used")
                else:
                    st.info("No capacity information available for this container.")

                _render_asset_table(
                    render_table,
                    items_in_container,
                    key=f"corp_assets_container_{int(container['item_id'])}",
                    height=320,
                )

    st.markdown("**Corporation Deliveries:**")
    exclude_flags = ["RigSlot0", "RigSlot1", "RigSlot2", "ServiceSlot0", "ServiceSlot1", "ServiceSlot2"]
    deliveries_items = assets_df[
        (assets_df["location_id"] == selected_location_id)
        & ~(assets_df["is_container"] | assets_df["is_ship"] | assets_df["is_asset_safety_wrap"] | assets_df["is_office_folder"])
        & (~assets_df["location_flag"].isin(exclude_flags + ["StructureFuel", "QuantumCoreRoom"]))
    ]
    if deliveries_items.empty:
        with st.expander("No corporation deliveries found at this location."):
            st.info("No corporation deliveries found at this location.")
    else:
        _render_asset_summary(deliveries_items)
        _render_asset_table(
            render_table,
            deliveries_items,
            key=f"corp_assets_deliveries_{int(selected_location_id)}",
            height=420,
        )
    st.divider()

    for title, flag in [("Structure Fuel:", "StructureFuel"), ("Quantum Core Room:", "QuantumCoreRoom")]:
        items = assets_df[(assets_df["location_id"] == selected_location_id) & (assets_df["location_flag"] == flag)]
        if items.empty:
            continue
        st.markdown(f"**{title}**")
        _render_asset_summary(items)
        _render_asset_table(
            render_table,
            items,
            key=f"corp_assets_special_{int(selected_location_id)}_{flag}",
            height=320,
        )
        st.divider()

    assetsafety_locations = assets_df[assets_df["is_asset_safety_wrap"]]["location_id"].unique()
    if selected_location_id in assetsafety_locations:
        st.markdown("**Asset Safety:**")
        wraps = assets_df[assets_df["is_asset_safety_wrap"]]
        if wraps.empty:
            with st.expander("No Asset Safety Wraps found at this location."):
                st.info("No Asset Safety Wraps found at this location.")
        else:
            for _, wrap in wraps.iterrows():
                items_in_wrap = assets_df[assets_df["location_id"] == wrap["item_id"]]
                _, _, total_value = summarize_asset_items(items_in_wrap)
                label = f"{wrap['type_name']} ({items_in_wrap['quantity'].sum()} items, Total Value: {total_value:,.2f} ISK)"
                with st.expander(label):
                    if items_in_wrap.empty:
                        st.info("No items in this container.")
                    else:
                        _render_asset_table(
                            render_table,
                            items_in_wrap,
                            key=f"corp_assets_wrap_{int(wrap['item_id'])}",
                            height=420,
                            prefer_container_names=True,
                            prefer_ship_names=True,
                        )
        st.divider()

    ships = assets_df[(assets_df["location_id"] == selected_location_id) & (assets_df["is_ship"])]
    ships = ships.sort_values(by=["ship_name"])
    st.markdown("**Ships:**")
    if ships.empty:
        with st.expander("No ships found at this location."):
            st.info("No ships found at this location.")
    else:
        _render_asset_summary(ships)
        render_ship_cards(ships)

def render():
    runtime = require_aggrid()
    img_renderer = js_icon_cell_renderer(JsCode=runtime.js_code, size_px=24)

    def _render_aggrid_table(
        df_in: pd.DataFrame,
        *,
        key: str,
        isk_cols: list[str] | None = None,
        number_cols_0: list[str] | None = None,
        number_cols_2: list[str] | None = None,
        image_cols: list[str] | None = None,
        height: int | None = None,
        height_max: int = 800,
    ) -> None:
        render_aggrid_table(
            df_in,
            runtime=runtime,
            key=key,
            isk_cols=isk_cols,
            number_cols_0=number_cols_0,
            number_cols_2=number_cols_2,
            image_cols=image_cols,
            image_renderer=img_renderer,
            height=height,
            height_max=height_max,
        )

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

    # Fetch all corporations data from backend
    try:
        corporations_response = cached_api_get("/corporations") or {}
        if corporations_response.get("status") != "success":
            st.error(f"Failed to get corporations data: {corporations_response.get('message', 'Unknown error')}")
            st.stop()
        
        corporations_list = corporations_response.get("data", [])
    except Exception as e:
        st.error(f"Failed to get corporations data: {e}")
        st.stop()

    df = pd.DataFrame(corporations_list)

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
            ceo_name = row.get("ceo_name")
            tax_rate = row.get("tax_rate")
            tax_rate_str = f"{tax_rate*100:.2f}%" if tax_rate is not None else "N/A"
            war_eligible = row.get("war_eligible")
            war_eligible_str = "Yes" if war_eligible else "No"

            # --- Extract Master Wallet balance ---
            master_wallet_balance = _extract_master_wallet_balance(row.get("wallets", None))

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
                            Age: {format_date_into_age(row.get('date_founded'))}<br>
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
    corp_ids: list[int] = [int(x) for x in corp_options.keys()]
    selected_id = st.selectbox(
        "Select corporation:",
        options=corp_ids,
        format_func=lambda x: corp_options[x]
    )

    if not selected_id:
        return

    # Get selected corporation dict
    corp_row = next((c for c in corporations_list if c["corporation_id"] == selected_id), None)
    if not corp_row:
        st.warning(f"Corporation ({selected_id}) not found.")
        return

    detail_sections = ["Members", "Structures", "Assets"]
    ensure_state_defaults({"corporation_details_active_tab": "Members"})
    ensure_valid_state_value(
        "corporation_details_active_tab",
        "Members",
        valid_values=detail_sections,
        coerce=str,
    )
    selected_detail_section = st.segmented_control(
        "Corporation details",
        options=detail_sections,
        key="corporation_details_active_tab",
    )

    # --- MEMBERS TAB ---
    if selected_detail_section == "Members":
        try:
            corp_members = corp_row.get("members", [])
            if corp_members is None:
                st.info("No members found for this corporation.")
            else:
                # CEO als eerste tile, rest alfabetisch
                ceo_id = corp_row.get("ceo_id")
                df_members = pd.DataFrame(corp_members)
                ceo = df_members[df_members["character_id"] == ceo_id]
                others = df_members[df_members["character_id"] != ceo_id].sort_values("character_name")
                df_members_sorted = pd.concat([ceo, others], ignore_index=True)

                def format_titles(titles):
                    if not titles or titles in ("", "null"):
                        return ""
                    if isinstance(titles, str):
                        try:
                            titles = json.loads(titles)
                        except Exception:
                            return titles
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
                        wallet_balance = member['character_wallet_balance'] if 'character_wallet_balance' in member else 0.0
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
    if selected_detail_section == "Structures":
        try:
            corp_structures = corp_row.get("structures", [])
            df_struct = pd.DataFrame(corp_structures)
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
    if selected_detail_section == "Assets":
        _render_corporation_assets_tab(_render_aggrid_table, corp_row)