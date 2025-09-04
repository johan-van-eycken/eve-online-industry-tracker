import streamlit as st
from classes.database_manager import DatabaseManager
from utils.formatters import format_datetime, format_date, format_date_countdown
import pandas as pd
import json

def render(cfg):
    st.subheader("Corporations")
    # Voeg je CSS toe aan de pagina
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

    db = DatabaseManager(cfg["app"]["database_app_uri"])

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
            with col:
                st.markdown(
                    f"""
                    <div style="background-color: rgba(30,30,30,0.95); padding: 20px; border-radius: 10px; box-shadow: 2px 2px 10px rgba(0,0,0,0.5); text-align: center; margin-bottom: 10px;">
                        <img src="{row['logo_url']}" width="96" style="border-radius:8px; margin-bottom:10px;" />
                        <div style="font-size:16px; color:#f0f0f0;">
                            <b style="font-size:18px;">{row.get('corporation_name', 'Unknown')}</b><br>
                            <span style="color:#aaa;">[{row.get('ticker', '')}]</span><br>
                            <span>CEO: {ceo_name}</span><br>
                            <span>Members: {row.get('member_count', 'N/A')}</span><br>
                            <span>Tax Rate: {tax_rate_str}</span><br>
                            <span>War Eligible: {war_eligible_str}</span><br>
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
    tab1, tab2 = st.tabs(["Members", "Structures"])

    # --- MEMBERS TAB ---
    with tab1:
        try:
            df_members = db.load_df("corporation_members")
            df_members["corporation_id"] = df_members["corporation_id"].astype(int)
            df_members["character_id"] = df_members["character_id"].astype(int)
            ceo_id = int(df[df["corporation_id"] == selected_id]["ceo_id"].iloc[0])

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
                        with col:
                            st.markdown(
                                f"""
                                <div class="{div_class}">
                                    {ceo_label_html}
                                    <img src="{portrait_url}" width="64" style="border-radius:8px; margin-bottom:8px;" />
                                    <div style="font-size:15px; color:#f0f0f0;">
                                        <b>{crown}{member['character_name']}</b><br>
                                        <span style="color:#aaa;">ID: {member['character_id']}</span><br>
                                        <span style="font-size:13px;">{titles_str}</span>
                                    </div>
                                </div>
                                """,
                                unsafe_allow_html=True
                            )
        except Exception as e:
            st.info(f"No members found for this corporation. ({e})")

    # --- STRUCTURES TAB ---
    with tab2:
        try:
            df_struct = db.load_df("corporation_structures")
            df_struct = df_struct[df_struct["corporation_id"] == selected_id]
            if df_struct.empty:
                st.info("No structures found for this corporation.")
            else:
                cards_per_row = 3
                for i in range(0, len(df_struct), cards_per_row):
                    cols = st.columns(cards_per_row)
                    for j, col in enumerate(cols):
                        if i + j >= len(df_struct):
                            break
                        struct = df_struct.iloc[i + j]
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
                                        <span>Fuel Expires: {struct.get('fuel_expires')[:19]} ({format_date(struct.get('fuel_expires')[:19])})</span><br><br>
                                    </div>
                                </div>
                                """,
                                unsafe_allow_html=True
                            )
        except Exception:
            st.info("No structures found for this corporation.")