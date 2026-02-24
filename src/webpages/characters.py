import streamlit as st # pyright: ignore[reportMissingImports]
import pandas as pd # pyright: ignore[reportMissingModuleSource, reportMissingImports]
import json

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, JsCode  # type: ignore
except Exception:  # pragma: no cover
    AgGrid = None  # type: ignore
    GridOptionsBuilder = None  # type: ignore
    JsCode = None  # type: ignore

from utils.flask_api import cached_api_get, api_get, api_post
from utils.formatters import format_isk, format_isk_short, format_date, format_date_into_age

from webpages.industry_builder_utils import attach_aggrid_autosize


@st.cache_data(ttl=60)
def _get_character_oauth_metadata() -> dict | None:
    return api_get("/characters/oauth")

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

    # Fetch all characters data from backend
    try:
        characters_response = cached_api_get("/characters")
        if characters_response.get("status") != "success":
            st.error(f"Failed to get characters data: {characters_response.get('message', 'Unknown error')}")
            st.stop()
        
        characters_list = characters_response.get("data", [])
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
                response = api_get("/characters/wallet_balances")
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

    def camel_heading(name: str) -> str:
        # Convert snake_case to Title Case, with common acronyms uppercased.
        parts = [p for p in str(name).split("_") if p]
        acronyms = {"id", "isk", "esi", "utc"}
        out: list[str] = []
        for p in parts:
            if p.lower() in acronyms:
                out.append(p.upper())
            else:
                out.append(p[:1].upper() + p[1:])
        return " ".join(out) if out else str(name)

    def format_isk_suffix(value) -> str:
        """EVE-style ISK formatting, but with 'ISK' as a suffix."""
        if value is None or value == "":
            value = 0.0
        try:
            v = float(value)
            s = "{:,.2f}".format(v).replace(",", "X").replace(".", ",").replace("X", ".")
            return f"{s} ISK"
        except Exception:
            return "N/A"

    def format_isk_short_suffix(value) -> str:
        """Compact ISK notation (k/m/b) with ISK as suffix."""
        if value is None or value == "":
            value = 0.0
        try:
            v = float(value)
        except Exception:
            return "N/A"
        try:
            return f"{format_isk_short(abs(v))} ISK"
        except Exception:
            return "N/A"

    def build_tooltip(breakdown, category, formatter=format_isk, join_labels=True, label_formatter=None):
        """
        Builds a tooltip string with category left, ISK right.
        breakdown: grouped Series with MultiIndex or dict-like.
        category: 'Income' or 'Expenses'
        formatter: function to format ISK values
        join_labels: whether to join multiple index levels with '/'
        label_formatter: optional callable to format the label text
        """
        if category not in breakdown.index.get_level_values(0):
            return ""
        
        items = breakdown.loc[category].abs().sort_values(ascending=False)

        tooltip_lines = []
        if isinstance(items.index, pd.MultiIndex):
            for idx, val in items.items():
                label = " / ".join(str(x) for x in idx) if join_labels else str(idx[-1])
                if callable(label_formatter):
                    try:
                        label = label_formatter(label)
                    except Exception:
                        pass
                tooltip_lines.append(f"<div><span>{label}</span><span>{formatter(val)}</span></div>")
        else:
            for label, val in items.items():
                if callable(label_formatter):
                    try:
                        label = label_formatter(label)
                    except Exception:
                        pass
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

    # Get selected character dict
    char_row = next((c for c in characters_list if c["character_id"] == selected_id), None)
    if not char_row:
        st.warning("Character not found.")
        return

    # Tabs for Character Details
    tab_skills, journal_tab, transactions_tab, assets_tab, tab_settings = st.tabs(
        ["Skills", "Wallet Journal", "Wallet Transactions", "Assets", "Settings"]
    )

    # --- CHARACTER SKILLS TAB ---
    with tab_skills:
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
    with journal_tab:
        st.subheader("Wallet Journal")
        journal_data = char_row.get("wallet_journal", [])
        if not journal_data:
            st.warning("No wallet journal data found.")
            st.stop()
        try:
            journal_df = pd.DataFrame(journal_data)

            # Parse date once (tz-aware) so we can filter by time window.
            if "date" in journal_df.columns:
                journal_df["_date_dt"] = pd.to_datetime(journal_df["date"], utc=True, errors="coerce")
            else:
                journal_df["_date_dt"] = pd.NaT

            # Time-window selector for totals (default 30 days)
            # Common overview presets: 7d, 30d, MTD, 90d, 6m, YTD, rolling 12m, all-time.
            range_options = [
                "7 days",
                "30 days",
                "Month to date",
                "90 days",
                "6 months",
                "Year to date",
                "Rolling 12 months",
                "All",
            ]
            selected_range = st.selectbox(
                "Time range",
                options=range_options,
                index=range_options.index("30 days"),
                key="wallet_journal_time_range",
            )

            cutoff = None
            try:
                now_utc = pd.Timestamp.now(tz="UTC")
                if selected_range == "7 days":
                    cutoff = now_utc - pd.Timedelta(days=7)
                elif selected_range == "30 days":
                    cutoff = now_utc - pd.Timedelta(days=30)
                elif selected_range == "Month to date":
                    cutoff = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                elif selected_range == "90 days":
                    cutoff = now_utc - pd.Timedelta(days=90)
                elif selected_range == "6 months":
                    cutoff = now_utc - pd.DateOffset(months=6)
                elif selected_range == "Year to date":
                    cutoff = now_utc.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                elif selected_range == "Rolling 12 months":
                    cutoff = now_utc - pd.DateOffset(years=1)
                else:
                    cutoff = None
            except Exception:
                cutoff = None

            if cutoff is not None:
                filtered_journal_df = journal_df[journal_df["_date_dt"].notna() & (journal_df["_date_dt"] >= cutoff)].copy()
            else:
                filtered_journal_df = journal_df.copy()

            filtered_journal_df["category"] = filtered_journal_df["amount"].apply(lambda x: "Income" if x > 0 else "Expenses")
            aggregated_journal = filtered_journal_df.groupby("category")["amount"].sum()
            journal_breakdown = filtered_journal_df.groupby(["category", "ref_type"])["amount"].sum()
            journal_income_tooltip = build_tooltip(
                journal_breakdown,
                "Income",
                formatter=format_isk_suffix,
                label_formatter=camel_heading,
            )
            journal_expense_tooltip = build_tooltip(
                journal_breakdown,
                "Expenses",
                formatter=format_isk_suffix,
                label_formatter=camel_heading,
            )

            st.markdown(f"""
            <div class="wallet-summary">
            <div class="tooltip">
                Total Income: {format_isk_short_suffix(aggregated_journal.get("Income", 0))}
                <span class="tooltiptext">{journal_income_tooltip}</span>
            </div><br />
            <div class="tooltip">
                Total Expenses: {format_isk_short_suffix(-aggregated_journal.get("Expenses", 0))}
                <span class="tooltiptext">{journal_expense_tooltip}</span>
            </div>
            </div><br />
            """, unsafe_allow_html=True)

            # Display wallet journal entries (match the selected time window)
            journal_view_df = filtered_journal_df.copy()

            def _order_same_timestamp_rows(group: pd.DataFrame) -> pd.DataFrame:
                """Order rows in a same-timestamp group so balance changes look consistent.

                We try to form a chain where for consecutive rows (newest-first):
                next.balance ≈ current.balance - current.amount
                """
                if len(group) <= 1:
                    return group
                if "balance" not in group.columns or "amount" not in group.columns:
                    return group

                g = group.copy()
                g["_bal__"] = pd.to_numeric(g["balance"], errors="coerce")
                g["_amt__"] = pd.to_numeric(g["amount"], errors="coerce")
                if g["_bal__"].isna().all() or g["_amt__"].isna().all():
                    return group

                g["_prev_bal__"] = g["_bal__"] - g["_amt__"]

                tol = 0.05  # tolerate minor float noise / rounding
                idxs = list(g.index)
                unused = set(idxs)

                def _matches(a, b) -> bool:
                    try:
                        if pd.isna(a) or pd.isna(b):
                            return False
                        return abs(float(a) - float(b)) <= tol
                    except Exception:
                        return False

                # Start row: a row whose balance is not any other row's prev-balance.
                prev_vals = {i: g.at[i, "_prev_bal__"] for i in idxs}
                bal_vals = {i: g.at[i, "_bal__"] for i in idxs}

                start_candidates: list[int] = []
                for i in idxs:
                    b = bal_vals.get(i)
                    if not any(_matches(b, prev_vals.get(j)) for j in idxs if j != i):
                        start_candidates.append(i)

                if start_candidates:
                    start = max(start_candidates, key=lambda i: (bal_vals.get(i) if pd.notna(bal_vals.get(i)) else float("-inf"), str(i)))
                else:
                    # Fallback: pick the highest balance as the most "recent" in the group.
                    start = max(idxs, key=lambda i: (bal_vals.get(i) if pd.notna(bal_vals.get(i)) else float("-inf"), str(i)))

                chain: list[int] = [start]
                unused.remove(start)
                while unused:
                    cur = chain[-1]
                    target = prev_vals.get(cur)
                    matches = [j for j in unused if _matches(bal_vals.get(j), target)]
                    if not matches:
                        break
                    # Deterministic tie-break: closest balance match, then higher balance.
                    def _rank(j):
                        bj = bal_vals.get(j)
                        try:
                            dist = abs(float(bj) - float(target))
                        except Exception:
                            dist = float("inf")
                        bal_rank = float(bj) if pd.notna(bj) else float("-inf")
                        return (dist, -bal_rank, str(j))

                    nxt = sorted(matches, key=_rank)[0]
                    chain.append(nxt)
                    unused.remove(nxt)

                # Append any leftovers in a stable, sensible order.
                if unused:
                    leftovers = sorted(
                        list(unused),
                        key=lambda i: (
                            -(float(bal_vals.get(i)) if pd.notna(bal_vals.get(i)) else float("-inf")),
                            str(i),
                        ),
                    )
                    chain.extend(leftovers)

                out = g.loc[chain].drop(columns=["_bal__", "_amt__", "_prev_bal__"], errors="ignore")
                return out

            # Order rows newest-first.
            if "_date_dt" in journal_view_df.columns:
                journal_view_df = journal_view_df.sort_values(by="_date_dt", ascending=False, kind="mergesort")
                _gb = journal_view_df.groupby("_date_dt", sort=False, dropna=False, group_keys=False)
                try:
                    # pandas >= 2.2: prevents FutureWarning about grouping columns.
                    journal_view_df = _gb.apply(_order_same_timestamp_rows, include_groups=False)
                except TypeError:
                    # Older pandas: no include_groups kwarg.
                    journal_view_df = _gb.apply(_order_same_timestamp_rows)
            elif "date" in journal_view_df.columns:
                journal_view_df = journal_view_df.sort_values(by="date", ascending=False, kind="mergesort")

            # Date formatting: YYYY-MM-DD HH24:Mi:SS (no 'T' / 'Z')
            if "date" in journal_view_df.columns:
                try:
                    dt = journal_view_df.get("_date_dt")
                    if dt is None:
                        dt = pd.to_datetime(journal_view_df["date"], utc=True, errors="coerce")
                    formatted = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
                    # For any unparsable values, fall back to a simple string cleanup.
                    raw = journal_view_df["date"].astype(str)
                    raw = raw.str.replace("T", " ", regex=False).str.replace("Z", "", regex=False)
                    journal_view_df["date"] = formatted.fillna(raw)
                except Exception:
                    try:
                        journal_view_df["date"] = (
                            journal_view_df["date"].astype(str).str.replace("T", " ", regex=False).str.replace("Z", "", regex=False)
                        )
                    except Exception:
                        pass

            # Hide columns (both in AgGrid and fallback table)
            hidden_cols = {
                "ref_type",
                "reason",
                "context_id_type",
                "context_id",
                "character_id",
                "first_party_id",
                "id",
                "second_party_id",
                "tax_receiver_id",
                "updated_at",
                "wallet_journal_id",
                "_date_dt",
            }

            # Visible subset for fallback / readability.
            fallback_cols = [c for c in journal_view_df.columns if c not in hidden_cols]
            fallback_df = journal_view_df[fallback_cols] if fallback_cols else journal_view_df

            if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
                # Fallback: show only visible columns, with Camel-Cased headings.
                view = fallback_df.copy()
                try:
                    view = view.rename(columns={c: camel_heading(c) for c in view.columns})
                except Exception:
                    pass
                st.dataframe(view, width="stretch")
            else:
                eu_locale = "nl-NL"  # '.' thousands, ',' decimals

                def _js_eu_isk(decimals: int) -> JsCode:
                    return JsCode(
                        f"""
                            function(params) {{
                                if (params.value === null || params.value === undefined || params.value === "") return "";
                                const n = Number(params.value);
                                if (isNaN(n)) return "";
                                return new Intl.NumberFormat('{eu_locale}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n) + ' ISK';
                            }}
                        """
                    )

                def _js_eu_number(decimals: int) -> JsCode:
                    return JsCode(
                        f"""
                            function(params) {{
                                if (params.value === null || params.value === undefined || params.value === "") return "";
                                const n = Number(params.value);
                                if (isNaN(n)) return "";
                                return new Intl.NumberFormat('{eu_locale}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n);
                            }}
                        """
                    )

                # Keep a readable column order (show key fields first).
                preferred_cols = [
                    "date",
                    "description",
                    "amount",
                    "balance",
                    "tax",
                    "first_party_name",
                    "second_party_name",
                    "tax_receiver_name",
                ]
                ordered_cols = [c for c in preferred_cols if c in journal_view_df.columns]
                ordered_cols += [c for c in journal_view_df.columns if c not in ordered_cols]
                journal_view_df = journal_view_df[ordered_cols]

                gb = GridOptionsBuilder.from_dataframe(journal_view_df)
                gb.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)

                # Apply Camel-Cased headings for all columns.
                for c in list(journal_view_df.columns):
                    gb.configure_column(c, header_name=camel_heading(c))

                # Hide requested columns.
                for c in hidden_cols:
                    if c in journal_view_df.columns:
                        gb.configure_column(c, hide=True)

                right = {"textAlign": "right"}
                for c in ["amount", "balance", "tax"]:
                    if c in journal_view_df.columns:
                        gb.configure_column(
                            c,
                            header_name=camel_heading(c),
                            type=["numericColumn", "numberColumnFilter"],
                            valueFormatter=_js_eu_isk(2 if c == "tax" else 2),
                            minWidth=150,
                            cellStyle=right,
                        )

                for c in ["wallet_journal_id", "character_id", "context_id", "first_party_id", "second_party_id", "tax_receiver_id", "id"]:
                    if c in journal_view_df.columns:
                        gb.configure_column(
                            c,
                            header_name=camel_heading(c),
                            type=["numericColumn", "numberColumnFilter"],
                            valueFormatter=_js_eu_number(0),
                            minWidth=130,
                            cellStyle=right,
                        )

                grid_options = gb.build()
                attach_aggrid_autosize(grid_options, JsCode=JsCode)

                # Auto-size columns to their contents.
                grid_options["autoSizeStrategy"] = {"type": "fitCellContents"}

                # Row color coding based on amount.
                # Green = positive amount, Red = negative amount.
                grid_options["getRowStyle"] = JsCode(
                    """
                    function(params) {
                        try {
                            if (!params || !params.data) return null;
                            var v = params.data.amount;
                            if (v === null || v === undefined || v === '') return null;
                            var n = Number(v);
                            if (isNaN(n) || n === 0) return null;
                            if (n > 0) {
                                return { backgroundColor: 'rgba(46, 204, 113, 0.10)' };
                            }
                            return { backgroundColor: 'rgba(231, 76, 60, 0.10)' };
                        } catch (e) {
                            return null;
                        }
                    }
                    """
                )

                height = min(650, 40 + (len(journal_view_df) * 32))
                AgGrid(
                    journal_view_df,
                    gridOptions=grid_options,
                    allow_unsafe_jscode=True,
                    theme="streamlit",
                    height=height,
                )

        except Exception as e:
            st.warning(f"No wallet journal data found. {e}")
            st.stop()

    # --- WALLET TRANSACTIONS TAB ---
    with transactions_tab:
        st.subheader("Wallet Transactions")
        transactions_data = char_row.get("wallet_transactions", [])
        if not transactions_data:
            st.warning("No wallet transactions data available.")
            st.stop()
        try:
            transactions_df = pd.DataFrame(transactions_data)
            # Parse date once (tz-aware) so we can filter by time window.
            if "date" in transactions_df.columns:
                transactions_df["_date_dt"] = pd.to_datetime(transactions_df["date"], utc=True, errors="coerce")
            else:
                transactions_df["_date_dt"] = pd.NaT

            # Time-window selector for totals/table (default 30 days)
            range_options = [
                "7 days",
                "30 days",
                "Month to date",
                "90 days",
                "6 months",
                "Year to date",
                "Rolling 12 months",
                "All",
            ]
            selected_range = st.selectbox(
                "Time range",
                options=range_options,
                index=range_options.index("30 days"),
                key="wallet_transactions_time_range",
            )

            cutoff = None
            try:
                now_utc = pd.Timestamp.now(tz="UTC")
                if selected_range == "7 days":
                    cutoff = now_utc - pd.Timedelta(days=7)
                elif selected_range == "30 days":
                    cutoff = now_utc - pd.Timedelta(days=30)
                elif selected_range == "Month to date":
                    cutoff = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                elif selected_range == "90 days":
                    cutoff = now_utc - pd.Timedelta(days=90)
                elif selected_range == "6 months":
                    cutoff = now_utc - pd.DateOffset(months=6)
                elif selected_range == "Year to date":
                    cutoff = now_utc.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                elif selected_range == "Rolling 12 months":
                    cutoff = now_utc - pd.DateOffset(years=1)
                else:
                    cutoff = None
            except Exception:
                cutoff = None

            if cutoff is not None:
                filtered_transactions_df = transactions_df[
                    transactions_df["_date_dt"].notna() & (transactions_df["_date_dt"] >= cutoff)
                ].copy()
            else:
                filtered_transactions_df = transactions_df.copy()

            # Income vs Expenses (buy = expense).
            filtered_transactions_df["category"] = filtered_transactions_df.get("is_buy").apply(
                lambda b: "Expenses" if bool(b) else "Income"
            )

            aggregated_transactions = filtered_transactions_df.groupby("category")["total_price"].sum()
            tx_breakdown = filtered_transactions_df.groupby(["category", "type_category_name"])["total_price"].sum()
            tx_income_tooltip = build_tooltip(
                tx_breakdown,
                "Income",
                formatter=format_isk_suffix,
                join_labels=False,
                label_formatter=camel_heading,
            )
            tx_expense_tooltip = build_tooltip(
                tx_breakdown,
                "Expenses",
                formatter=format_isk_suffix,
                join_labels=False,
                label_formatter=camel_heading,
            )

            st.markdown(
                f"""
                <div class="wallet-summary">
                    <div class="tooltip">
                        Total Income: {format_isk_short_suffix(aggregated_transactions.get("Income", 0))}
                        <span class="tooltiptext">{tx_income_tooltip}</span>
                    </div><br />
                    <div class="tooltip">
                        Total Expenses: {format_isk_short_suffix(abs(aggregated_transactions.get("Expenses", 0)))}
                        <span class="tooltiptext">{tx_expense_tooltip}</span>
                    </div>
                </div><br />
                """,
                unsafe_allow_html=True,
            )

        except Exception:
            st.warning("No wallet transactions data available.")
            st.stop()

        # Display wallet transaction entries (match selected time window)
        tx_view_df = filtered_transactions_df.copy()

        # Sort newest-first.
        if "_date_dt" in tx_view_df.columns:
            tx_view_df = tx_view_df.sort_values(by="_date_dt", ascending=False, kind="mergesort")
        elif "date" in tx_view_df.columns:
            tx_view_df = tx_view_df.sort_values(by="date", ascending=False, kind="mergesort")

        # Date formatting: YYYY-MM-DD HH24:Mi:SS (no 'T' / 'Z')
        if "date" in tx_view_df.columns:
            try:
                dt = tx_view_df.get("_date_dt")
                if dt is None:
                    dt = pd.to_datetime(tx_view_df["date"], utc=True, errors="coerce")
                formatted = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
                raw = tx_view_df["date"].astype(str)
                raw = raw.str.replace("T", " ", regex=False).str.replace("Z", "", regex=False)
                tx_view_df["date"] = formatted.fillna(raw)
            except Exception:
                try:
                    tx_view_df["date"] = (
                        tx_view_df["date"].astype(str).str.replace("T", " ", regex=False).str.replace("Z", "", regex=False)
                    )
                except Exception:
                    pass

        # Desired column order.
        tx_display_cols = [
            "date",
            "type_name",
            "quantity",
            "unit_price",
            "total_price",
            "client_name",
            "is_personal",
            "location_id",
            "category",
        ]

        for c in tx_display_cols + ["is_buy"]:
            if c not in tx_view_df.columns:
                tx_view_df[c] = None

        tx_grid_df = tx_view_df[tx_display_cols + ["is_buy"]].copy()

        if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
            # Fallback: show only the requested columns in the requested order.
            view = tx_grid_df[tx_display_cols].copy()
            rename = {
                "type_name": "Name",
                "quantity": "Quantity",
                "unit_price": "Unit Price",
                "total_price": "Total Price",
                "client_name": "Client Name",
                "is_personal": "Is Personal",
                "location_id": "Location ID",
            }
            try:
                view = view.rename(columns=rename)
            except Exception:
                pass
            st.dataframe(view, width="stretch")
        else:
            eu_locale = "nl-NL"  # '.' thousands, ',' decimals

            def _js_eu_isk(decimals: int) -> JsCode:
                return JsCode(
                    f"""
                        function(params) {{
                            if (params.value === null || params.value === undefined || params.value === "") return "";
                            const n = Number(params.value);
                            if (isNaN(n)) return "";
                            return new Intl.NumberFormat('{eu_locale}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n) + ' ISK';
                        }}
                    """
                )

            def _js_eu_number(decimals: int) -> JsCode:
                return JsCode(
                    f"""
                        function(params) {{
                            if (params.value === null || params.value === undefined || params.value === "") return "";
                            const n = Number(params.value);
                            if (isNaN(n)) return "";
                            return new Intl.NumberFormat('{eu_locale}', {{ minimumFractionDigits: {int(decimals)}, maximumFractionDigits: {int(decimals)} }}).format(n);
                        }}
                    """
                )

            gb = GridOptionsBuilder.from_dataframe(tx_grid_df)
            gb.configure_default_column(editable=False, sortable=True, filter=True, resizable=True)

            # Explicit headers to match requested column names.
            gb.configure_column("date", header_name="Date")
            gb.configure_column("type_name", header_name="Name")
            gb.configure_column("quantity", header_name="Quantity")
            gb.configure_column("unit_price", header_name="Unit Price")
            gb.configure_column("total_price", header_name="Total Price")
            gb.configure_column("client_name", header_name="Client Name")
            gb.configure_column("is_personal", header_name="Is Personal")
            gb.configure_column("location_id", header_name="Location ID")
            gb.configure_column("category", header_name="Category")

            # Hide helper/styling-only columns.
            gb.configure_column("is_buy", hide=True)

            right = {"textAlign": "right"}
            if "quantity" in tx_grid_df.columns:
                gb.configure_column(
                    "quantity",
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=_js_eu_number(0),
                    minWidth=110,
                    cellStyle=right,
                )
            for c in ["unit_price", "total_price"]:
                if c in tx_grid_df.columns:
                    gb.configure_column(
                        c,
                        type=["numericColumn", "numberColumnFilter"],
                        valueFormatter=_js_eu_isk(2),
                        minWidth=150,
                        cellStyle=right,
                    )
            if "location_id" in tx_grid_df.columns:
                gb.configure_column(
                    "location_id",
                    type=["numericColumn", "numberColumnFilter"],
                    valueFormatter=_js_eu_number(0),
                    minWidth=140,
                    cellStyle=right,
                )

            grid_options = gb.build()
            attach_aggrid_autosize(grid_options, JsCode=JsCode)
            grid_options["autoSizeStrategy"] = {"type": "fitCellContents"}

            # Row color coding: buy = red, sell = green.
            grid_options["getRowStyle"] = JsCode(
                """
                function(params) {
                    try {
                        if (!params || !params.data) return null;
                        var b = params.data.is_buy;
                        var isBuy = (b === true || b === 1 || b === '1' || b === 'true' || b === 'True');
                        if (isBuy) {
                            return { backgroundColor: 'rgba(231, 76, 60, 0.10)' };
                        }
                        return { backgroundColor: 'rgba(46, 204, 113, 0.10)' };
                    } catch (e) {
                        return null;
                    }
                }
                """
            )

            height = min(650, 40 + (len(tx_grid_df) * 32))
            AgGrid(
                tx_grid_df,
                gridOptions=grid_options,
                allow_unsafe_jscode=True,
                theme="streamlit",
                height=height,
            )

    # --- CHARACTER SETTINGS / AUTH TAB ---
    with tab_settings:
        st.subheader("SSO / OAuth")
        st.caption("Shows what the backend has stored for this character. Tokens are not displayed.")

        meta = _get_character_oauth_metadata()
        if meta is None or meta.get("status") != "success":
            st.warning(
                "OAuth metadata unavailable. "
                + (meta.get("message") if isinstance(meta, dict) else "")
            )
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
    with assets_tab:
        st.subheader("Assets")
        assets_data = char_row.get("assets", [])
        if not assets_data:
            st.warning("No character assets data available.")
            st.stop()
        
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
            assets_df = pd.DataFrame(assets_data)
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
            assets_df.loc[assets_df["top_location_id"] == loc_id, "location_name"] = location_name

        # Build a mapping of location_id to location_name for dropdown display
        location_names = {}
        for location_id in location_ids:
            if "location_name" not in assets_df.columns:
                location_names[location_id] = str(location_id)
                continue

            subset = assets_df[assets_df["top_location_id"] == location_id]["location_name"].dropna()
            location_names[location_id] = subset.iloc[0] if not subset.empty else str(location_id)

        # Sort location_ids by their names alphabetically
        sorted_location_ids = sorted(location_names.keys(), key=lambda x: location_names[x].lower())

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
                            display_columns = [
                                "image_url",
                                "type_name",
                                "quantity",
                                "type_volume",
                                "total_volume",
                                "acquisition_source",
                                "acquisition_unit_cost",
                                "acquisition_total_cost",
                                "acquisition_date",
                                "type_average_price",
                                "total_average_price",
                                "type_group_name",
                                "type_category_name",
                            ]
                            display_columns = [c for c in display_columns if c in df.columns]
                            df_display = df[display_columns].sort_values(by="type_name")
                            column_config = {
                                "image_url": st.column_config.ImageColumn("", width="auto"),
                                "type_name": st.column_config.TextColumn("Name", width="auto"),
                                "quantity": st.column_config.NumberColumn("Quantity", width="auto"),
                                "type_volume": st.column_config.NumberColumn("Volume", width="auto"),
                                "total_volume": st.column_config.NumberColumn("Total Volume", width="auto"),
                                "acquisition_source": st.column_config.TextColumn("Source", width="auto"),
                                "acquisition_unit_cost": st.column_config.NumberColumn("Unit Cost", width="auto"),
                                "acquisition_total_cost": st.column_config.NumberColumn("Total Cost", width="auto"),
                                "acquisition_date": st.column_config.TextColumn("Acquired", width="auto"),
                                "type_average_price": st.column_config.NumberColumn("Value", width="auto"),
                                "total_average_price": st.column_config.NumberColumn("Total Value", width="auto"),
                                "type_group_name": st.column_config.TextColumn("Group", width="auto"),
                                "type_category_name": st.column_config.TextColumn("Category", width="auto"),
                            }
                            column_config = {k: v for k, v in column_config.items() if k in df_display.columns}
                            st.dataframe(df_display, width="stretch", column_config=column_config, hide_index=True)
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
                display_columns = [
                    "image_url",
                    "type_name",
                    "quantity",
                    "type_volume",
                    "total_volume",
                    "acquisition_source",
                    "acquisition_unit_cost",
                    "acquisition_total_cost",
                    "acquisition_date",
                    "type_average_price",
                    "total_average_price",
                    "type_group_name",
                    "type_category_name",
                ]
                display_columns = [c for c in display_columns if c in df.columns]
                df_display = df[display_columns].sort_values(by="type_name")
                column_config = {
                    "image_url": st.column_config.ImageColumn("", width="auto"),
                    "type_name": st.column_config.TextColumn("Name", width="auto"),
                    "quantity": st.column_config.NumberColumn("Quantity", width="auto"),
                    "type_volume": st.column_config.NumberColumn("Volume", width="auto"),
                    "total_volume": st.column_config.NumberColumn("Total Volume", width="auto"),
                    "acquisition_source": st.column_config.TextColumn("Source", width="auto"),
                    "acquisition_unit_cost": st.column_config.NumberColumn("Unit Cost", width="auto"),
                    "acquisition_total_cost": st.column_config.NumberColumn("Total Cost", width="auto"),
                    "acquisition_date": st.column_config.TextColumn("Acquired", width="auto"),
                    "type_average_price": st.column_config.NumberColumn("Value", width="auto"),
                    "total_average_price": st.column_config.NumberColumn("Total Value", width="auto"),
                    "type_group_name": st.column_config.TextColumn("Group", width="auto"),
                    "type_category_name": st.column_config.TextColumn("Category", width="auto"),
                }
                column_config = {k: v for k, v in column_config.items() if k in df_display.columns}
                st.dataframe(df_display, width="stretch", column_config=column_config, hide_index=True)
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
                                display_columns = [
                                    "image_url",
                                    "type_name",
                                    "quantity",
                                    "type_volume",
                                    "total_volume",
                                    "acquisition_source",
                                    "acquisition_unit_cost",
                                    "acquisition_total_cost",
                                    "acquisition_date",
                                    "type_average_price",
                                    "total_average_price",
                                    "type_group_name",
                                    "type_category_name",
                                ]
                                display_columns = [c for c in display_columns if c in df.columns]
                                df_display = df[display_columns].sort_values(by="type_name")
                                column_config = {
                                    "image_url": st.column_config.ImageColumn("", width="auto"),
                                    "type_name": st.column_config.TextColumn("Name", width="auto"),
                                    "quantity": st.column_config.NumberColumn("Quantity", width="auto"),
                                    "type_volume": st.column_config.NumberColumn("Volume", width="auto"),
                                    "total_volume": st.column_config.NumberColumn("Total Volume", width="auto"),
                                    "acquisition_source": st.column_config.TextColumn("Source", width="auto"),
                                    "acquisition_unit_cost": st.column_config.NumberColumn("Unit Cost", width="auto"),
                                    "acquisition_total_cost": st.column_config.NumberColumn("Total Cost", width="auto"),
                                    "acquisition_date": st.column_config.TextColumn("Acquired", width="auto"),
                                    "type_average_price": st.column_config.NumberColumn("Value", width="auto"),
                                    "total_average_price": st.column_config.NumberColumn("Total Value", width="auto"),
                                    "type_group_name": st.column_config.TextColumn("Group", width="auto"),
                                    "type_category_name": st.column_config.TextColumn("Category", width="auto"),
                                }
                                column_config = {k: v for k, v in column_config.items() if k in df_display.columns}
                                st.dataframe(df_display, width="stretch", column_config=column_config, hide_index=True)
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