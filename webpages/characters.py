import streamlit as st
import requests
import json
from classes.database_manager import DatabaseManager
from utils.formatters import format_isk, format_date, format_date_into_age

FLASK_API_URL= "http://localhost:5000"

def refresh_wallet_balances():
    """
    Function to send a POST request to the Flask backend to refresh wallet balances.
    """
    try:
        response = requests.post(f"{FLASK_API_URL}/refresh_wallet_balances", json={})
        if response.status_code == 200:
            st.markdown(
                """
                <div class="success-msg">Wallet balances refreshed successfully!</div>
                <style>
                .success-msg {
                    background-color: #1c4026;
                    color: #e4ede6;
                    padding: 15px;
                    border-radius: 5px;
                    animation: fadeout 3s forwards;
                }
                @keyframes fadeout {
                    0% {opacity: 1;}
                    70% {opacity: 1;}
                    100% {opacity: 0;}
                }
                </style><br /><br />
                """,
                unsafe_allow_html=True
            )
            return response.json()["data"]
        else:
            st.error(f"Failed to refresh wallet balances: {response.json().get('message', 'Unknown error')}")
    except Exception as e:
        st.error(f"Error connecting to backend: {e}")

def render(cfg):
    # -- Customer Style --
    st.markdown("""
        <style>
        .tooltip {
            position: relative;
            display: inline-block;
            cursor: pointer;
            margin-bottom: 10px; /* equal spacing between rows */
        }

        .tooltip .tooltiptext {
            visibility: hidden;
            width: 240px;
            background-color: #1e293b;
            color: #f0f0f0;
            text-align: left;
            padding: 8px;
            border-radius: 6px;
            position: absolute;
            z-index: 10;
            bottom: 125%;
            left: 50%;
            transform: translateX(-50%);
            opacity: 0;
            transition: opacity 0.3s;
            font-size: 13px;
            line-height: 1.3;
            box-shadow: 0 2px 8px rgba(0,0,0,0.5);
        }

        /* Remove default margins for all children inside tooltip */
        .tooltip .tooltiptext * {
            margin: 0;
            padding: 0;
            font-size: 13px;
            line-height: 1.3;
        }

        /* Level/SP line with flex */
        .tooltip .tooltiptext .level-sp {
            display: flex;
            justify-content: space-between;
            margin-top: 5px;
        }

        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
        </style>
        """, unsafe_allow_html=True)

    st.subheader("Characters")
    db = DatabaseManager(cfg["app"]["database_app_uri"])

    try:
        df = db.load_df("characters")
    except Exception:
        st.warning("No character data found. Run main.py first.")
        st.stop()

    if "image_url" not in df.columns:
        df["image_url"] = df["character_id"].apply(
            lambda cid: f"https://images.evetech.net/characters/{cid}/portrait?size=128"
        )

    # Button refresh wallet balances
    if st.button("Refresh Wallet Balances"):
        refreshed_data = refresh_wallet_balances()  # Calls Flask backend
        if refreshed_data:
            # Assuming refreshed_data has the updated wallet balances; update your dataframe if necessary
            for wallet_data in refreshed_data:
                if isinstance(wallet_data, str):
                    wallet_data = json.loads(wallet_data)
                
                character_name = wallet_data.get("character_name")
                wallet_balance = wallet_data.get("wallet_balance")

                if character_name and wallet_balance:
                    df.loc[df["character_name"] == character_name, "wallet_balance"] = wallet_balance

    # By default no character tile selected
    if "selected_character" not in st.session_state:
        st.session_state.selected_character = None

    # Clickable character tiles
    cards_per_row = 5
    for i in range(0, len(df), cards_per_row):
        cols = st.columns(cards_per_row)
        for j, col in enumerate(cols):
            if i + j >= len(df):
                break
            row = df.iloc[i + j]

            with col:
                st.markdown(
                    f"""
                    <div style="background-color: rgba(30,30,30,0.95); padding: 25px; border-radius: 12px; box-shadow: 2px 2px 10px rgba(0,0,0,0.6); text-align: center; margin-bottom: 10px;">
                        <img src="{row['image_url']}" width="128" style="border-radius:8px; margin-bottom:10px; display:block; margin-left:auto; margin-right:auto;" />
                        <div style="font-size:16px; line-height:1.3; color:#f0f0f0;">
                            <b style="font-size:20px;">{row['character_name']}</b><br>
                            <br>
                            <b>Wallet Balance:<br>
                            {format_isk(row.get('wallet_balance'))}</b><br>
                            <br>
                            <div style="font-size:16px; text-align:left;">
                                Birthday: {format_date(row.get('birthday'))}<br>
                                Age: {format_date_into_age(row.get('birthday'))}<br>
                                Gender: {row.get('gender', 'N/A')}<br>
                                Corporation ID: {row.get('corporation_id', 'N/A')}<br>
                                Race: {row.get('race', 'N/A')}<br>
                                Bloodline: {row.get('bloodline', 'N/A')}<br>
                                Security Status: {row.get('security_status', 'N/A'):.2f}
                            </div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

    # Show skills if a character is selected
    st.subheader(f"Character Skills")

        # Dropdown to select character
    char_options = df.set_index("character_id")["character_name"].to_dict()
    selected_id = st.selectbox(
        "Select character:",
        options=list(char_options.keys()),
        format_func=lambda x: char_options[x]
    )

    if not selected_id:
        return

    char_row = df[df["character_id"] == selected_id].iloc[0]

    if "skills" not in char_row:
        st.info("No skills data available for this character.")
        return

    skills_data = json.loads(char_row["skills"])
    total_sp = skills_data.get("total_skillpoints", 0)
    unallocated_sp = skills_data.get("unallocated_skillpoints", 0)

    # Summary
    st.markdown(
        f"""
        **{total_sp:,}** Total Skill Points.
        """
    )
    st.markdown(
        f"""
        **{unallocated_sp:,}** Unallocated Skill Points.
        """
    )

    # Build dictionary of skills grouped by group_name
    skill_groups = {}
    for s in skills_data.get("skills", []):
        skill_groups.setdefault(s["group_name"], []).append(s)

    st.divider()

    def split_list_top_down(lst, n_cols):
        """
        Split lst into n_cols columns, filling each column top-down.
        Returns a list of lists, one per column.
        """
        n_rows = (len(lst) + n_cols - 1) // n_cols  # ceil division
        return [lst[i * n_rows : (i + 1) * n_rows] for i in range(n_cols)]

    # Sorted group names
    group_names = sorted(skill_groups.keys())  # alphabetical
    n_cols = 3
    cols = st.columns(n_cols)

    # Split top-down into columns
    col_splits = split_list_top_down(group_names, n_cols)

    for col, group_list in zip(cols, col_splits):
        for group_name in group_list:
            col.button(
                group_name,
                key=f"group_{group_name}",
                use_container_width=True,
                on_click=lambda g=group_name: setattr(st.session_state, "selected_group", g),
            )

    st.divider()

    # Show skills of selected group
    if "selected_group" in st.session_state:
        group_name = st.session_state.selected_group
        skills = sorted(skill_groups[group_name], key=lambda s: s["skill_name"])

        st.markdown(f"### {group_name}")

        # Split alphabetically into 2 columns (down first, then across)
        col1, col2 = st.columns(2)
        col_splits = split_list_top_down(skills, 2)

        for col, skill_list in zip([col1, col2], col_splits):
            for skill in skill_list:
                name = skill["skill_name"]
                desc = skill["skill_desc"]
                points = skill["skillpoints_in_skill"]
                level = skill["trained_skill_level"]
                
                rom_level = "0"
                if level == 1: rom_level = "I"
                elif level == 2: rom_level = "II"
                elif level == 3: rom_level = "III"
                elif level == 4: rom_level = "IV"
                elif level == 5: rom_level = "V"

                # Render level boxes on one line
                boxes = " ".join(
                    ["🟦" if l < level else "⬜" for l in range(5)]
                )

                col.markdown(
                    f"""<div class="tooltip">
                            <span>{boxes} &nbsp;&nbsp;{name}</span>
                            <span class="tooltiptext">
                                {desc}
                                <div class="level-sp">
                                    <span>Level {rom_level}</span>
                                    <span>{points:,} SP</span>
                                </div>
                            </span>
                        </div>""",
                    unsafe_allow_html=True,
                )