import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]
import json
from typing import cast

from utils.assets_ui import (
    ASSET_FLOAT_COLUMNS,
    ASSET_INT_COLUMNS,
    ASSET_ISK_COLUMNS,
    build_asset_display_frame,
    apply_location_names,
    render_ship_cards,
    summarize_asset_items,
)
from utils.characters_api import build_character_options, fetch_characters
from utils.flask_api import api_get, api_post
from utils.aggrid_formatters import js_icon_cell_renderer
from utils.formatters import format_isk, format_date, format_date_into_age
from utils.session_state import ensure_state_defaults, ensure_valid_state_value
from utils.webpage_ui import render_aggrid_table, require_aggrid


@st.cache_data(ttl=60)
def _get_character_oauth_metadata() -> dict | None:
    return api_get("/characters/oauth")


def _build_tooltip(breakdown, category, formatter=format_isk, join_labels=True):
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


def _render_asset_table(render_table, df: pd.DataFrame, *, key: str, height: int, prefer_container_names: bool = False, prefer_ship_names: bool = False) -> None:
    df_display = build_asset_display_frame(
        df,
        prefer_container_names=prefer_container_names,
        prefer_ship_names=prefer_ship_names,
    )
    render_table(
        df_display,
        key=key,
        isk_cols=[column for column in ASSET_ISK_COLUMNS if column in df_display.columns],
        int_cols=[column for column in ASSET_INT_COLUMNS if column in df_display.columns],
        float_cols=[column for column in ASSET_FLOAT_COLUMNS if column in df_display.columns],
        height=height,
    )


def _render_asset_summary(df: pd.DataFrame) -> None:
    unique_items, total_volume, total_value = summarize_asset_items(df)
    st.markdown(
        f"Items: {unique_items} - Total Volume: {total_volume:,.2f} m³ - Total Value: {total_value:,.2f} ISK"
    )


def _render_character_assets_tab(render_table, char_row: dict, selected_id: int) -> None:
    st.subheader("Assets")
    assets_data = char_row.get("assets", [])
    if not assets_data:
        st.warning("No character assets data available.")
        st.stop()

    try:
        assets_df = pd.DataFrame(assets_data)
        assets_df = assets_df[assets_df["character_id"] == selected_id]
    except Exception:
        st.warning("No character assets data available.")
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

    container_mask = (assets_df["location_id"] == selected_location_id) & (assets_df["is_container"])
    containers: pd.DataFrame = cast(pd.DataFrame, assets_df.loc[container_mask, :]).sort_values(by=["container_name"])
    assetsafety_locations = assets_df[assets_df["location_flag"] == "AssetSafety"]["location_id"].unique()

    st.markdown("**Containers:**")
    if containers.empty:
        with st.expander("No containers found at this location."):
            st.info("No containers found at this location.")
    else:
        for _, container in containers.iterrows():
            items_in_container = assets_df[assets_df["location_id"] == container["item_id"]]
            is_selected = selected_asset_id in items_in_container["item_id"].values
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

                if items_in_container.empty:
                    st.info("No items in this container.")
                else:
                    _render_asset_table(
                        render_table,
                        items_in_container,
                        key=f"character_container_{int(container['item_id'])}",
                        height=320,
                    )

    st.divider()

    hangar_items = assets_df[(assets_df["location_id"] == selected_location_id) & ~(assets_df["is_container"] | assets_df["is_ship"])]
    if selected_asset_id in hangar_items["item_id"].values:
        st.markdown("<span style='font-weight: bold; font-color: #b91c1c'>Hangar Items:</span>", unsafe_allow_html=True)
    else:
        st.markdown("<span style='font-weight: bold;'>Hangar Items:</span>", unsafe_allow_html=True)

    if hangar_items.empty:
        with st.expander("No hangar items found at this location."):
            st.info("No hangar items found at this location.")
    else:
        _render_asset_summary(hangar_items)
        _render_asset_table(
            render_table,
            hangar_items,
            key=f"character_hangar_{int(selected_location_id)}",
            height=420,
        )

    st.divider()

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
                            key=f"character_assetsafety_wrap_{int(wrap['item_id'])}",
                            height=420,
                            prefer_container_names=True,
                            prefer_ship_names=True,
                        )
        st.divider()

    ships_mask = (assets_df["location_id"] == selected_location_id) & (assets_df["is_ship"])
    ships: pd.DataFrame = cast(pd.DataFrame, assets_df.loc[ships_mask, :]).sort_values(by=["ship_name"])

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
        df: pd.DataFrame,
        *,
        key: str,
        isk_cols: list[str] | None = None,
        int_cols: list[str] | None = None,
        float_cols: list[str] | None = None,
        height: int | None = None,
    ) -> None:
        render_aggrid_table(
            df,
            runtime=runtime,
            key=key,
            isk_cols=isk_cols,
            number_cols_0=int_cols,
            number_cols_2=float_cols,
            image_cols=["image_url"] if "image_url" in df.columns else None,
            image_renderer=img_renderer,
            height=height,
            height_max=height or 700,
        )

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

    # Fetch all characters data from backend
    try:
        characters_list = fetch_characters()
    except Exception as e:
        st.error(f"Failed to get characters data: {e}")
        st.stop()

    # Convert to DataFrame for tabular display, but keep list of dicts for details
    df = pd.DataFrame(characters_list)

    # Button to refresh wallet balances
    col_btn, col_status = st.columns([1, 6])
    with col_btn:
        if st.button("Refresh Wallets Balances"):
            try:
                response = api_get("/characters/wallet_balances") or {}
                if response.get("status") != "success":
                    raise Exception(f"{response.get('message', 'Unknown error')}")
                response_data = response.get("data", [])
                for wallet_data in response_data:
                    if isinstance(wallet_data, str):
                        wallet_data = json.loads(wallet_data)
                    character_name = wallet_data.get("character_name")
                    wallet_balance = wallet_data.get("wallet_balance")
                    if character_name and wallet_balance:
                        df.loc[df["character_name"] == character_name, "wallet_balance"] = wallet_balance
            except Exception as e:
                with col_status:
                    st.error(f"Failed to refresh wallet balances: {e}")

    # Character tiles
    cards_per_row = 5
    for i in range(0, len(df), cards_per_row):
        cols = st.columns(cards_per_row)
        for j, col in enumerate(cols):
            if i + j >= len(df):
                break
            row = df.iloc[i + j]

            security_status = row.get("security_status")
            try:
                security_status_display = f"{float(security_status):.2f}" if security_status is not None else "N/A"
            except Exception:
                security_status_display = "N/A"

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
                                Security Status: {security_status_display}
                            </div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

    st.divider()

    st.subheader("Character Details")

    # Dropdown to select character
    char_options = build_character_options(characters_list)
    selected_id = st.selectbox(
        "Select character:",
        options=list(char_options.keys()),
        format_func=lambda x: char_options[x]
    )

    if not selected_id:
        return

    # Get selected character dict
    char_row = next((c for c in characters_list if c["character_id"] == selected_id), None)
    if not char_row:
        st.warning("Character not found.")
        return

    detail_sections = ["Skills", "Wallet Journal", "Wallet Transactions", "Assets", "Settings"]
    ensure_state_defaults({"character_details_active_tab": "Skills"})
    ensure_valid_state_value(
        "character_details_active_tab",
        "Skills",
        valid_values=detail_sections,
        coerce=str,
    )
    selected_detail_section = st.segmented_control(
        "Character details",
        options=detail_sections,
        key="character_details_active_tab",
    )

    # --- CHARACTER SKILLS TAB ---
    if selected_detail_section == "Skills":
        left_col, right_col = st.columns([2,1])
        with left_col:
            st.subheader(f"Character Skills")
            skills_data = char_row.get("skills", {})
            total_sp = skills_data.get("total_skillpoints", 0)
            unallocated_sp = skills_data.get("unallocated_skillpoints", 0)

            st.markdown(f"**{total_sp:,}** Total Skill Points.")
            st.markdown(f"**{unallocated_sp:,}** Unallocated Skill Points.")
            st.divider()

            skill_groups = {}
            for s in skills_data.get("skills", []):
                skill_groups.setdefault(s["group_name"], []).append(s)
            group_names = sorted(skill_groups.keys())

            if group_names:
                ensure_valid_state_value("selected_group", group_names[0], valid_values=group_names, coerce=str)

            def _select_group(group_name: str) -> None:
                st.session_state["selected_group"] = group_name

            def split_list_top_down(lst, n_cols):
                """
                Split list into n_cols columns, filling each column top-down.
                Returns a list of lists, one per column.
                """
                n_rows = (len(lst) + n_cols - 1) // n_cols  # ceil division
                return [lst[i * n_rows : (i + 1) * n_rows] for i in range(n_cols)]
            
            n_cols = 3
            cols = st.columns(n_cols)

            # Split top-down into columns
            col_splits = split_list_top_down(group_names, n_cols)
            for col, group_list in zip(cols, col_splits):
                for group_name in group_list:
                    col.button(
                        group_name,
                        key=f"group_{group_name}",
                        width="stretch",
                        on_click=_select_group,
                        args=(group_name,),
                    )

            st.divider()

            # Show skills of selected group
            if "selected_group" in st.session_state:
                group_name = str(st.session_state["selected_group"])
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
                        boxes = " ".join(["🟦" if l < level else "⬜" for l in range(5)])

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
    if selected_detail_section == "Wallet Journal":
        st.subheader("Wallet Journal")
        journal_data = char_row.get("wallet_journal", [])
        if not journal_data:
            st.warning("No wallet journal data found.")
            st.stop()
        try:
            journal_df = pd.DataFrame(journal_data)
            journal_df["category"] = journal_df["amount"].apply(lambda x: "Income" if x > 0 else "Expenses")
            aggregated_journal = journal_df.groupby("category")["amount"].sum()
            journal_breakdown = journal_df.groupby(["category", "ref_type"])["amount"].sum()
            journal_income_tooltip = _build_tooltip(journal_breakdown, "Income")
            journal_expense_tooltip = _build_tooltip(journal_breakdown, "Expenses")

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
            journal_view = journal_df.sort_values(by=["date"], ascending=False)
            _render_aggrid_table(
                journal_view,
                key="character_wallet_journal",
                isk_cols=[c for c in ["amount", "balance"] if c in journal_view.columns],
            )

        except Exception as e:
            st.warning(f"No wallet journal data found. {e}")
            st.stop()

    # --- WALLET TRANSACTIONS TAB ---
    if selected_detail_section == "Wallet Transactions":
        st.subheader("Wallet Transactions")
        transactions_data = char_row.get("wallet_transactions", [])
        if not transactions_data:
            st.warning("No wallet transactions data available.")
            st.stop()
        try:
            transactions_df = pd.DataFrame(transactions_data)
            transactions_df["category"] = transactions_df.apply(
                lambda row: "Income" if row["is_buy"] == 0 else "Expenses", axis=1
            )
            aggregated_transactions = transactions_df.groupby("category")["total_price"].sum()
            tx_breakdown = transactions_df.groupby(["category", "type_category_name"])["total_price"].sum()
            tx_income_tooltip = _build_tooltip(tx_breakdown, "Income", join_labels=False)
            tx_expense_tooltip = _build_tooltip(tx_breakdown, "Expenses", join_labels=False)

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
        tx_view = transactions_df.sort_values(by=["date"], ascending=False)
        _render_aggrid_table(
            tx_view,
            key="character_wallet_transactions",
            isk_cols=[c for c in ["unit_price", "total_price"] if c in tx_view.columns],
            int_cols=[c for c in ["quantity"] if c in tx_view.columns],
        )

    # --- CHARACTER SETTINGS / AUTH TAB ---
    if selected_detail_section == "Settings":
        st.subheader("SSO / OAuth")
        st.caption("Shows what the backend has stored for this character. Tokens are not displayed.")

        meta = _get_character_oauth_metadata()
        if meta is None or meta.get("status") != "success":
            if isinstance(meta, dict):
                st.warning(f"OAuth metadata unavailable. {str(meta.get('message') or '')}")
            else:
                st.warning("OAuth metadata unavailable.")
        else:
            rows = meta.get("data", []) or []
            target = None
            for r in rows:
                if not isinstance(r, dict):
                    continue
                # Prefer ID match, fall back to name.
                if r.get("character_id") == selected_id:
                    target = r
                    break
            if target is None:
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    if str(r.get("character_name") or "") == str(char_row.get("character_name") or ""):
                        target = r
                        break

            if target is None:
                st.info("No OAuth record found for this character yet.")
                st.write(
                    "If this is a new character, the backend will open the EVE SSO login flow when it first needs tokens."
                )
            else:
                expires_in = target.get("expires_in_seconds")
                if isinstance(expires_in, (int, float)):
                    expires_label = f"{int(expires_in)}s"
                else:
                    expires_label = "N/A"

                c1, c2, c3 = st.columns(3)
                c1.metric("Has refresh token", "yes" if target.get("has_refresh_token") else "no")
                c2.metric("Has access token", "yes" if target.get("has_access_token") else "no")
                c3.metric("Access token expires in", expires_label)

                scopes_raw = str(target.get("scopes") or "").strip()
                scopes = [s for s in scopes_raw.split() if s]
                st.markdown("**Scopes**")
                if scopes:
                    st.code("\n".join(scopes), language="text")
                else:
                    st.write("(none stored)")

        if st.button("Refresh OAuth status"):
            _get_character_oauth_metadata.clear()
            st.rerun()
    
    # --- CHARACTER ASSETS TAB ---
    if selected_detail_section == "Assets":
        _render_character_assets_tab(_render_aggrid_table, char_row, selected_id)