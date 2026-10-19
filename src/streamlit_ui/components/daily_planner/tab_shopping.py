from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import streamlit as st

from streamlit_ui.state.daily_planner_page import DailyPlannerPageState
from streamlit_ui.components.aggrid_import import import_aggrid
from streamlit_ui.components.daily_planner.status_bar import (
    _JOB_ACTION_TYPES,
    _fmt_isk,
    wallet_snapshot,
)


@dataclass(frozen=True)
class ShoppingBudget:
    """What the Shopping tab compares the wallet with (Task 14 minor 1, fix
    round 1 M5): the pending material lines, the pending BPO purchases and
    the pending jobs' install fees, the total Capital Reserved counts.
    unpriced_lines / unknown_fee_jobs are not in the sums, so with either
    the total is a lower bound."""

    purchases: float
    install_fees: float
    unpriced_lines: int
    unknown_fee_jobs: int
    bpo_purchases: float = 0.0

    @property
    def total(self) -> float:
        return self.purchases + self.bpo_purchases + self.install_fees

    @property
    def materials_and_fees(self) -> float:
        """What a BPO's "Fits Budget?" is checked after: the BPO rows are the
        purchases being fitted, so they are not in it."""
        return self.purchases + self.install_fees

    @property
    def unknown_lines(self) -> int:
        return self.unpriced_lines + self.unknown_fee_jobs


def _pending(action: dict[str, Any]) -> bool:
    return str(action.get("status") or "pending") == "pending"


def shopping_budget(actions: list[dict[str, Any]]) -> ShoppingBudget:
    """The pending buy_materials lines and the pending jobs' install fees.
    A done or skipped line is no longer to be paid, so it is not counted."""
    purchases, bpos, fees, unpriced, unknown_fees = 0.0, 0.0, 0.0, 0, 0
    for action in actions:
        if not _pending(action):
            continue
        action_type = str(action.get("action_type") or "")
        if action_type in ("buy_materials", "buy_bpo"):
            cost = action.get("estimated_cost_isk")
            if cost is None:
                unpriced += 1
            elif action_type == "buy_bpo":
                bpos += float(cost)
            else:
                purchases += float(cost)
        elif action_type in _JOB_ACTION_TYPES:
            fee = action.get("estimated_job_fee_isk")
            if fee is None:
                unknown_fees += 1
            else:
                fees += float(fee)
    return ShoppingBudget(purchases, fees, unpriced, unknown_fees, bpo_purchases=bpos)


def section_subtotal(rows: list[dict[str, Any]]) -> float:
    """A materials section's subtotal: its pending lines only (M5)."""
    return sum(r["Est. Total"] or 0.0 for r in rows if r.get("_status", "pending") == "pending")


def budget_fit(corp_wallet: float | None, total_isk: float, cumul_isk: float) -> str:
    """Whether a BPO fits what the wallet has left after the shopping list."""
    if corp_wallet is None:
        return "Unknown"
    return "Yes" if (corp_wallet - total_isk) >= cumul_isk else "No"


def remaining_after_shopping(corp_wallet: float | None, total_isk: float, *, unpriced_lines: int = 0) -> float | None:
    """What the wallet has left after the shopping list; negative is a deficit.

    None when the wallet is unknown, or when a line has no price (the real
    total is higher, so the exact remainder is unknown; see budget_metric).
    A genuine 0.0 wallet is a real balance, so it yields -total_isk, not an
    unknown.
    """
    if corp_wallet is None or unpriced_lines > 0:
        return None
    return corp_wallet - total_isk


def wallet_is_short(corp_wallet: float | None, total_isk: float, *, unpriced_lines: int = 0) -> bool | None:
    """True when a known wallet (0.0 included) cannot cover the shopping list.
    With unpriced lines the priced total is a lower bound: True when even it
    exceeds the wallet, else None (unknown, not "not short"). An unknown
    wallet is False (nothing to compare)."""
    if corp_wallet is None:
        return False
    if corp_wallet - total_isk < 0:
        return True
    return None if unpriced_lines > 0 else False


def budget_metric(
    corp_wallet: float | None, total_isk: float, unpriced_lines: int, *, unknown_fee_jobs: int = 0,
) -> tuple[str, str, str | None]:
    """(label, value, caption) of the budget metric: an exact Remaining or
    Deficit when every line is priced and every fee known; otherwise a
    deficit is a lower bound ("at least") and a remainder is unknown, both
    captioned. Unpriced lines and unknown install fees are named apart (M5)."""
    if corp_wallet is None:
        return "Remaining", _fmt_isk(None), None
    remaining = corp_wallet - total_isk
    missing = [
        *([f"{unpriced_lines} unpriced line(s)"] if unpriced_lines > 0 else []),
        *([f"{unknown_fee_jobs} job(s) with an unknown install fee"] if unknown_fee_jobs > 0 else []),
    ]
    if not missing:
        return ("Deficit", _fmt_isk(abs(remaining)), None) if remaining < 0 else ("Remaining", _fmt_isk(remaining), None)
    what = " and ".join(missing)
    if remaining < 0:
        return (
            "Deficit", f"at least {_fmt_isk(abs(remaining))}",
            f"{what} are not counted: the real deficit is larger.",
        )
    return (
        "Remaining", "unknown",
        f"At most {_fmt_isk(remaining)}: {what} are not counted, "
        "so the wallet may not cover the list.",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_shopping_rows(
    actions: list[dict[str, Any]],
    category: str,
) -> list[dict[str, Any]]:
    rows = []
    for a in actions:
        if str(a.get("action_type") or "") != "buy_materials":
            continue
        if str(a.get("shopping_category") or "") != category:
            continue
        qty = int(a.get("quantity") or 0)
        cost = a.get("estimated_cost_isk")
        unit_price: float | None = None
        total_isk: float | None = None
        if cost is not None and qty > 0:
            total_isk = float(cost)
            unit_price = total_isk / qty
        rows.append({
            "Item": str(a.get("type_name") or "Unknown"),
            "Quantity": qty,
            "Est. Unit Price": round(unit_price, 2) if unit_price is not None else None,
            "Est. Total": round(total_isk, 0) if total_isk is not None else None,
            "Note": str(a.get("notes") or ""),
            "_status": str(a.get("status") or "pending"),
        })
    return rows


def unpriced_total_caption(rows: list[dict[str, Any]]) -> str | None:
    """A caption for a total over `rows` when some lines have no price: they
    are still bought, but the total cannot include them (Task 8 fix round 1)."""
    unpriced = sum(1 for r in rows if r.get("Est. Total") is None)
    if not unpriced:
        return None
    return f"{unpriced} line(s) have no price: the total excludes them, so the real cost is higher."


def _build_bpo_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for item in items:
        if not item.get("bpo_investment_recommended"):
            continue
        bpo_price = item.get("bpo_market_price")
        break_even = item.get("break_even_days")
        annual_savings = item.get("projected_annual_savings")
        if bpo_price is None:
            continue
        if break_even is not None and float(break_even) < 30:
            recommendation = "Strong Buy"
        elif break_even is not None and float(break_even) <= 90:
            recommendation = "Consider"
        else:
            recommendation = "Low priority"
        rows.append({
            "BPO": str(item.get("type_name") or "Unknown"),
            "Market Price": float(bpo_price),
            "Break-even (days)": round(float(break_even), 1) if break_even is not None else None,
            "Annual Savings": round(float(annual_savings), 0) if annual_savings is not None else None,
            "Recommendation": recommendation,
        })
    rows.sort(key=lambda r: (r["Break-even (days)"] or 9999))
    return rows


def _fmt_isk_cell(v: float | None) -> str:
    if v is None:
        return "—"
    if v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v / 1_000:.1f}K"
    return f"{v:,.0f}"


def _build_clipboard_text(
    actions: list[dict[str, Any]],
    total_isk: float,
) -> str:
    lines = ["=== Daily Planner Shopping List ==="]
    categories = [
        ("current_job", "-- Current Job Materials --"),
        ("invention_input", "-- Invention Inputs --"),
    ]
    for cat, header in categories:
        cat_actions = [
            a for a in actions
            if str(a.get("action_type") or "") == "buy_materials"
            and str(a.get("shopping_category") or "") == cat
            and _pending(a)  # done / skipped lines are not to be bought (M5)
        ]
        if not cat_actions:
            continue
        lines.append(header)
        for a in cat_actions:
            qty = int(a.get("quantity") or 0)
            cost = a.get("estimated_cost_isk")
            isk_str = f"est. {_fmt_isk_cell(float(cost))}" if cost is not None else ""
            lines.append(f"{a.get('type_name', 'Unknown')} x {qty:,} ({isk_str})")
    lines.append(f"Total: {_fmt_isk(total_isk)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# AG-Grid section renderer
# ---------------------------------------------------------------------------

def _render_materials_section(
    title: str,
    rows: list[dict[str, Any]],
    deemphasise: bool,
    ag_imports: Any,
) -> float:
    """Render a materials-to-buy grid. Returns the subtotal ISK."""
    if not rows:
        st.caption(f"No {title.lower()} items.")
        return 0.0

    import pandas as pd

    subtotal = section_subtotal(rows)

    st.markdown(f"**{title}** — Subtotal (pending): {_fmt_isk(subtotal)}")

    if deemphasise:
        st.caption("Optional — can defer if wallet is short.")

    display_rows = [
        {
            "Item": r["Item"],
            "Quantity": f"{r['Quantity']:,}",
            "Est. Unit Price": _fmt_isk_cell(r["Est. Unit Price"]),
            "Est. Total": _fmt_isk_cell(r["Est. Total"]),
            "Note": r["Note"],
        }
        for r in rows
    ]
    df = pd.DataFrame(display_rows)

    AgGrid = ag_imports.AgGrid if ag_imports else None
    GridOptionsBuilder = ag_imports.GridOptionsBuilder if ag_imports else None

    if AgGrid is not None and GridOptionsBuilder is not None:
        try:
            gb = GridOptionsBuilder.from_dataframe(df)
            gb.configure_default_column(resizable=True, sortable=True, filter=True)
            gb.configure_column("Item", minWidth=200)
            gb.configure_column("Quantity", type=["numericColumn"], maxWidth=120)
            gb.configure_column("Est. Unit Price", maxWidth=140)
            gb.configure_column("Est. Total", maxWidth=140)
            go = gb.build()
            AgGrid(
                df,
                gridOptions=go,
                height=min(35 * len(display_rows) + 50, 300),
                fit_columns_on_grid_load=True,
                allow_unsafe_jscode=False,
            )
        except Exception:
            st.dataframe(df, hide_index=True, width="stretch")
    else:
        st.dataframe(df, hide_index=True, width="stretch")

    return subtotal


# ---------------------------------------------------------------------------
# Main tab renderer
# ---------------------------------------------------------------------------

def render_tab_shopping(page_state: DailyPlannerPageState) -> None:
    if page_state.plan is None:
        st.info("No plan yet — click **Recompute Plan** to generate your first daily plan.")
        return

    actions: list[dict[str, Any]] = page_state.plan.get("actions") or []
    items: list[dict[str, Any]] = page_state.plan.get("items") or []
    plan_meta: dict[str, Any] = page_state.plan.get("plan") or {}
    corp_wallet = wallet_snapshot(plan_meta)

    ag = import_aggrid()

    st.subheader("Materials to Buy")
    st.caption("Net of corp stock at plan-compute time. Check in-game before buying large quantities.")

    # Build per-category rows
    current_rows = _build_shopping_rows(actions, "current_job")
    invention_rows = _build_shopping_rows(actions, "invention_input")

    # Pending lines only, plus the pending jobs' install fees (Task 14 minor
    # 1): the same total Capital Reserved counts for them.
    budget = shopping_budget(actions)
    total_isk = budget.purchases
    all_rows = [r for r in (*current_rows, *invention_rows) if r["_status"] == "pending"]
    unpriced_lines = budget.unknown_lines
    # None = unknown (unpriced lines or fees, priced total within the wallet).
    wallet_short = wallet_is_short(corp_wallet, budget.total, unpriced_lines=unpriced_lines)

    # Current job materials (highest priority)
    _render_materials_section("Current Job Materials", current_rows, deemphasise=False, ag_imports=ag)

    st.markdown("")

    # Invention inputs
    _render_materials_section("Invention Inputs", invention_rows, deemphasise=False, ag_imports=ag)

    st.markdown("---")

    # Budget summary
    budget_label, budget_value, budget_caption = budget_metric(
        corp_wallet, budget.total, budget.unpriced_lines, unknown_fee_jobs=budget.unknown_fee_jobs,
    )
    remaining = remaining_after_shopping(corp_wallet, budget.total, unpriced_lines=unpriced_lines)
    budget_cols = st.columns(4)
    with budget_cols[0]:
        st.metric("Total to Spend", _fmt_isk(total_isk))
        st.caption("Pending lines only.")
        unpriced_caption = unpriced_total_caption(all_rows)
        if unpriced_caption:
            st.caption(unpriced_caption)
    with budget_cols[1]:
        st.metric("Install Fees", _fmt_isk(budget.install_fees))
        if budget.unknown_fee_jobs:
            st.caption(f"{budget.unknown_fee_jobs} job(s) have an unknown fee: not counted.")
    with budget_cols[2]:
        st.metric("Corp Wallet", _fmt_isk(corp_wallet))
    with budget_cols[3]:
        st.caption("Wallet less purchases (materials and BPOs), incl. install fees (as Capital Reserved).")
        if remaining is not None and remaining < 0:
            # Exact deficit (every line priced).
            st.metric("Deficit", budget_value, delta=f"-{_fmt_isk(abs(remaining))}", delta_color="inverse")
        else:
            # An unknown wallet renders "—"; unpriced lines "unknown" / "at least".
            st.metric(budget_label, budget_value)
        if budget_caption:
            st.caption(budget_caption)

    if wallet_short is None:
        st.info(
            "Some lines have no price, so whether the corp wallet covers the list is unknown."
        )
    if wallet_short:
        st.warning(
            "Total shopping cost exceeds corp wallet. "
            "Buy Current Job materials first."
        )

    # Export / clipboard
    st.markdown("---")
    st.markdown("**Export Shopping List**")
    clipboard_text = _build_clipboard_text(actions, total_isk)
    st.download_button(
        label="Download as text",
        data=clipboard_text,
        file_name="shopping_list.txt",
        mime="text/plain",
        key="shopping_download",
    )
    with st.expander("Preview (copy-paste format)", expanded=False):
        st.code(clipboard_text, language="text")

    # BPO Investment Opportunities
    bpo_rows = _build_bpo_rows(items)
    if bpo_rows:
        st.markdown("---")
        st.subheader("BPO Investment Opportunities")
        st.caption("T1 items only, sorted by break-even speed. Cumulative ISK needed assumes buying in order shown.")

        import pandas as pd

        cumul_isk = 0.0
        for row in bpo_rows:
            cumul_isk += row["Market Price"]
            row["Cumul. ISK Needed"] = cumul_isk
            # After the materials and the install fees (M5), not materials alone.
            row["Fits Budget?"] = budget_fit(corp_wallet, budget.materials_and_fees, cumul_isk)

        bpo_display = pd.DataFrame([
            {
                "BPO": r["BPO"],
                "Market Price": _fmt_isk_cell(r["Market Price"]),
                "Break-even": f"{r['Break-even (days)']}d" if r["Break-even (days)"] else "—",
                "Annual Savings": _fmt_isk_cell(r["Annual Savings"]),
                "Cumul. ISK Needed": _fmt_isk_cell(r["Cumul. ISK Needed"]),
                "Fits Budget?": r["Fits Budget?"],
                "Recommendation": r["Recommendation"],
            }
            for r in bpo_rows
        ])
        st.dataframe(bpo_display, hide_index=True, width="stretch")
