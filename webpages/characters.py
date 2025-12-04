import streamlit as st
import pandas as pd
import json
import os

from utils.app_init import load_config, init_db_app
from utils.flask_api import api_get, api_post
from utils.formatters import format_isk, format_date, format_date_into_age

# Function to refresh wallet balances
def refresh_wallet_balances():
    """
    Function to send a GET request to the Flask backend to refresh wallet balances.
    """
    try:
        response = api_get("/refresh_wallet_balances")
        if response.get("status") == "success":
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
            return response["data"]
        else:
            st.error(f"Failed to refresh wallet balances: {response.get('message', 'Unknown error')}")
    except Exception as e:
        st.error(f"Error connecting to backend: {e}")

def render():
    # -- Custom Style --
    st.markdown("""
        <style>
        .tooltip {
            position: relative;
            display: inline-block;
            cursor: pointer;
            margin-bottom: 10px;
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
        
        /* Adjusted tooltip alignement for Summarised Wallet aggregations */
        .wallet-summary .tooltip .tooltiptext {
            white-space: nowrap;  /* keep everything on one line */
            min-width: 280px;     /* wider to avoid wrapping */
        }

        .wallet-summary .tooltip .tooltiptext div {
            display: flex;
            justify-content: space-between;
        }
                
        /* Adjusted tooltip alignement for Ship tiles */
        .ship-tile .tooltip .tooltiptext {
            white-space: nowrap;  /* keep everything on one line */
            min-width: 200px;     /* wider to avoid wrapping */
        }
        
        .ship-tile .tooltip .tooltiptext div {
            display: flex;
            justify-content: space-between;
        }

        /* Remove default margins for all children inside tooltip */
        .tooltip .tooltiptext * {
            margin: 0;
            padding: 0;
            font-size: 13px;
            line-height: 1.3;
        }

        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
        </style>
        """, unsafe_allow_html=True)

    st.subheader("Characters")

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

    # Add character portraits
    if "image_url" not in df.columns:
        df["image_url"] = df["character_id"].apply(
            lambda cid: f"https://images.evetech.net/characters/{cid}/portrait?size=128"
        )

    # Button to refresh wallet balances
    if st.button("Refresh Wallet Balances"):
        refreshed_data = refresh_wallet_balances()  # Calls Flask backend
        if refreshed_data:
            for wallet_data in refreshed_data:
                if isinstance(wallet_data, str):
                    wallet_data = json.loads(wallet_data)

                character_name = wallet_data.get("character_name")
                wallet_balance = wallet_data.get("wallet_balance")

                if character_name and wallet_balance:
                    df.loc[df["character_name"] == character_name, "wallet_balance"] = wallet_balance

    # Character tiles
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

    st.divider()

    def build_tooltip(breakdown, category, formatter=format_isk, join_labels=True):
        """
        Builds a tooltip string with category left, ISK right.
        breakdown: grouped Series with MultiIndex or dict-like.
        category: 'Income' or 'Expenses'
        formatter: function to format ISK values
        join_labels: whether to join multiple index levels with '/'
        """
        if category not in breakdown.index.get_level_values(0):
            return ""
        
        items = breakdown.loc[category].abs().sort_values(ascending=False)

        tooltip_lines = []
        if isinstance(items.index, pd.MultiIndex):
            for idx, val in items.items():
                label = " / ".join(str(x) for x in idx) if join_labels else str(idx[-1])
                tooltip_lines.append(f"<div><span>{label}</span><span>{formatter(val)}</span></div>")
        else:
            for label, val in items.items():
                tooltip_lines.append(f"<div><span>{label}</span><span>{formatter(val)}</span></div>")

        return "".join(tooltip_lines)

    st.subheader("Character Details")

    # Dropdown to select character
    char_options = df.set_index("character_id")["character_name"].to_dict()
    selected_id = st.selectbox(
        "Select character:",
        options=list(char_options.keys()),
        format_func=lambda x: char_options[x]
    )

    if not selected_id:
        return

    # Tabs for Character Details
    tab_skills, journal_tab, transactions_tab, assets_tab = st.tabs(["Skills", "Wallet Journal", "Wallet Transactions", "Assets"])

    # --- CHARACTER SKILLS TAB ---
    with tab_skills:
        # --- Split into 2 main columns ---
        left_col, right_col = st.columns([2,1])  # 2/3 and 1/3 width

        # ================= LEFT COLUMN (Skills) =================
        with left_col:
            # Show skills if a character is selected
            st.subheader(f"Character Skills")

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
            st.divider()

            # Build dictionary of skills grouped by group_name
            skill_groups = {}
            for s in skills_data.get("skills", []):
                skill_groups.setdefault(s["group_name"], []).append(s)

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
                        rom_level = ["0","I","II","III","IV","V"][level] if isinstance(level, int) and level <= 5 else str(level)

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

        # ================= RIGHT COLUMN (Skill Queue) =================
        with right_col:
            st.subheader("Skill Queue")

            skill_queue = skills_data.get("skill_queue", [])
            skill_queue = sorted(skill_queue, key=lambda q: q.get("queue_position", 0))
            
            if not skill_queue:
                st.info("Skill queue is empty.")
            else:
                for q in skill_queue:
                    skill_name = q.get("skill_name", "Unknown Skill")
                    level = q.get("finished_level", "?")
                    start_time = format_date(q.get("start_time"))
                    end_time = format_date(q.get("finish_time"))

                    rom_level = ["0","I","II","III","IV","V"][level] if isinstance(level, int) and level <= 5 else str(level)

                    st.markdown(
                        f"""
                        <div style="background-color: rgba(40,40,40,0.9); padding: 12px; border-radius: 8px; margin-bottom: 8px;">
                            <b>{skill_name} → Level {rom_level}</b>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

    # --- CHARACTER JOURNAL TAB ---
    with journal_tab:
        st.subheader("Wallet Journal")

        try:
            journal_df = db.load_df("character_wallet_journal")
            journal_df = journal_df[journal_df["character_id"] == selected_id]

            journal_df["category"] = journal_df["amount"].apply(lambda x: "Income" if x > 0 else "Expenses")
            aggregated_journal = journal_df.groupby("category")["amount"].sum()
            journal_breakdown = journal_df.groupby(["category", "ref_type"])["amount"].sum()

            journal_income_tooltip = build_tooltip(journal_breakdown, "Income")
            journal_expense_tooltip = build_tooltip(journal_breakdown, "Expenses")

            st.markdown(f"""
            <div class="wallet-summary">
            <div class="tooltip">
                Total Income: {format_isk(aggregated_journal.get("Income", 0))}
                <span class="tooltiptext">{journal_income_tooltip}</span>
            </div><br />
            <div class="tooltip">
                Total Expenses: {format_isk(-aggregated_journal.get("Expenses", 0))}
                <span class="tooltiptext">{journal_expense_tooltip}</span>
            </div>
            </div><br />
            """, unsafe_allow_html=True)

            # Display wallet transaction entries
            st.dataframe(journal_df.sort_values(by="date", ascending=False), use_container_width=True)

        except Exception as e:
            st.warning(f"No wallet journal data found. {e}")
            return

    # --- WALLET TRANSACTIONS TAB ---
    with transactions_tab:
        st.subheader("Wallet Transactions")

        try:
            transactions_df = db.load_df("character_wallet_transactions")
            transactions_df = transactions_df[transactions_df["character_id"] == selected_id]

            transactions_df["category"] = transactions_df.apply(
                lambda row: "Income" if row["is_buy"] == 0 else "Expenses", axis=1
            )
            aggregated_transactions = transactions_df.groupby("category")["total_price"].sum()
            tx_breakdown = transactions_df.groupby(["category", "type_category_name"])["total_price"].sum()

            tx_income_tooltip = build_tooltip(tx_breakdown, "Income", join_labels=False)
            tx_expense_tooltip = build_tooltip(tx_breakdown, "Expenses", join_labels=False)

            st.markdown(f"""
            <div class="wallet-summary">
                <div class="tooltip">
                    Total Income: {format_isk(aggregated_transactions.get("Income", 0))}
                    <span class="tooltiptext">{tx_income_tooltip}</span>
                </div><br />
                <div class="tooltip">
                    Total Expenses: {format_isk(-aggregated_transactions.get("Expenses", 0))}
                    <span class="tooltiptext">{tx_expense_tooltip}</span>
                </div>
            </div><br />
            """, unsafe_allow_html=True)

        except Exception:
            st.warning("No wallet transactions data available.")
            st.stop()

        # Display wallet transaction entries
        st.dataframe(transactions_df.sort_values(by="date", ascending=False), use_container_width=True)
    
    # --- CHARACTER ASSETS TAB ---
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

        # Load and filter character assets
        try:
            assets_df = db.load_df("character_assets")
            assets_df = assets_df[assets_df["character_id"] == selected_id]
        except Exception:
            st.warning("No character assets data available.")
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
            # Show containers as expanders
            containers = assets_df[
                (assets_df["location_id"] == selected_location_id) &
                (assets_df["is_container"])
            ].sort_values(by="container_name")

            assetsafety_locations = assets_df[assets_df["location_flag"] == "AssetSafety"]["location_id"].unique()
            
            st.markdown("**Containers:**")
            if containers.empty:
                with st.expander("No containers found at this location."):
                    st.info("No containers found at this location.")
            else:
                for _, container in containers.iterrows():
                    items_in_container = assets_df[assets_df["location_id"] == container["item_id"]]
                    is_selected = selected_asset_id in items_in_container["item_id"].values
                    # calculate total average price
                    total_average_price = (items_in_container["type_average_price"] * items_in_container["quantity"]).sum()
                    
                    with st.expander(
                        f"{container['container_name']} ({items_in_container['type_name'].nunique()} unique items, Total Value: {total_average_price:,.2f} ISK)",
                        expanded=is_selected
                    ):
                        # Calculate used and max capacity
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

            st.divider()

            # Show hangar items
            hangar_items = assets_df[
                (assets_df["location_id"] == selected_location_id) &
                ~(assets_df["is_container"] | assets_df["is_ship"])
            ]
            is_selected_hangar = selected_asset_id in hangar_items["item_id"].values
            if is_selected_hangar:
                st.markdown("<span style='font-weight: bold; font-color: #b91c1c'>Hangar Items:</span>", unsafe_allow_html=True)
            else:
                st.markdown("<span style='font-weight: bold;'>Hangar Items:</span>", unsafe_allow_html=True)
            if hangar_items.empty:
                with st.expander("No hangar items found at this location."):
                    st.info("No hangar items found at this location.")
            else:
                total_average_price = (hangar_items["type_average_price"] * hangar_items["quantity"]).sum()
                st.markdown(f"Items: {hangar_items['type_name'].nunique()} - Total Volume: {hangar_items['type_volume'].dot(hangar_items['quantity']):,.2f} m³ - Total Value: {total_average_price:,.2f} ISK")
                df = add_item_images(hangar_items)
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

            total_average_price = (ships["type_average_price"] * ships["quantity"]).sum()
            total_volume = (ships["type_volume"] * ships["quantity"]).sum()
            st.markdown(f"**Ships:**")
            if ships.empty:
                with st.expander("No ships found at this location."):
                    st.info("No ships found at this location.")
            else:
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