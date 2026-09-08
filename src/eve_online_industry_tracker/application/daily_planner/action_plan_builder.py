"""ActionPlanBuilder — Phase 8: build ordered DailyActionLogModel rows.

Order per spec:
  DELIVER → RELIST → INVENT → COPY → ME/TE → SUB-MFG → MFG

buy_materials and buy_bpo rows are written (for Tab 2 / Shopping List) but
NOT rendered in Tab 1. They use character_id=None, character_name=None.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from eve_online_industry_tracker.application.daily_planner.models import AssignedAction, ShoppingItem
from eve_online_industry_tracker.infrastructure.models import DailyActionLogModel

logger = logging.getLogger(__name__)

# Action type sort order (lower = earlier in workflow)
_ACTION_ORDER = {
    "deliver": 0,
    "relist_order": 1,
    "invent": 2,
    "copy": 3,
    "me_research": 4,
    "te_research": 5,
    "sub_manufacture": 6,
    "manufacture": 7,
    # Corp-level (Tab 2 only, not Tab 1)
    "buy_materials": 8,
    "buy_bpo": 9,
}


def _now() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


class ActionPlanBuilder:
    """Phase 8: convert assigned actions and shopping list into DailyActionLogModel rows."""

    def build(
        self,
        plan_id: int,
        assigned_actions: list[AssignedAction],
        shopping_items: list[ShoppingItem],
        pricing_suggestions: list[Any],   # from PricingSuggestionService
        industry_jobs: list[Any],         # for DELIVER actions
        admin_settings: Any,
        bpo_opportunities: list[Any] | None = None,  # from ChainPlan.bpo_opportunities
    ) -> list[DailyActionLogModel]:
        """Return ordered list of DailyActionLogModel rows ready for persistence."""
        now = _now()
        rows: list[DailyActionLogModel] = []

        # ── DELIVER rows (from industry_jobs with end_date < now) ─────────────
        deliver_rows = self._build_deliver_actions(plan_id, industry_jobs, now)
        rows.extend(deliver_rows)

        # ── RELIST rows (from PricingSuggestionService) ───────────────────────
        relist_rows = self._build_relist_actions(plan_id, pricing_suggestions, now)
        rows.extend(relist_rows)

        # ── Job actions (INVENT, COPY, ME/TE, SUB-MFG, MFG) ─────────────────
        action_rows = self._build_job_actions(plan_id, assigned_actions, now)
        rows.extend(action_rows)

        # ── BUY_MATERIALS rows (Tab 2 — corp-level) ───────────────────────────
        buy_rows = self._build_buy_material_actions(plan_id, shopping_items, now)
        rows.extend(buy_rows)

        # ── BUY_BPO rows (Tab 2 — corp-level) ────────────────────────────────
        if bpo_opportunities:
            bpo_rows = self._build_buy_bpo_actions(plan_id, bpo_opportunities, now)
            rows.extend(bpo_rows)

        # Sort: corp-level rows (character_id=None) after all character rows
        rows.sort(key=lambda r: (
            r.character_id is None,
            r.character_id or 0,
            _ACTION_ORDER.get(r.action_type, 99),
        ))

        return rows

    def _build_deliver_actions(
        self,
        plan_id: int,
        industry_jobs: list[Any],
        now: datetime,
    ) -> list[DailyActionLogModel]:
        """Generate DELIVER actions for jobs whose end_date < now."""
        rows: list[DailyActionLogModel] = []
        for job in industry_jobs:
            status = str(_job_attr(job, "status") or "").lower()
            if status == "delivered":
                continue  # already done

            end_date_raw = _job_attr(job, "end_date")
            if end_date_raw is None:
                continue
            try:
                if isinstance(end_date_raw, str):
                    end_date = datetime.fromisoformat(end_date_raw.replace("Z", "+00:00")).replace(tzinfo=None)
                elif isinstance(end_date_raw, datetime):
                    end_date = end_date_raw.replace(tzinfo=None)
                else:
                    continue
            except Exception:
                continue

            if end_date > now:
                continue  # not ready yet

            character_id = _job_attr(job, "character_id") or _job_attr(job, "installer_id")
            character_name = _job_attr(job, "character_name") or _job_attr(job, "installer_name")
            type_id = int(_job_attr(job, "product_type_id") or _job_attr(job, "type_id") or 0)
            type_name = str(_job_attr(job, "product_type_name") or _job_attr(job, "type_name") or "")
            runs = int(_job_attr(job, "runs") or 1)
            output_qty = int(_job_attr(job, "output_quantity") or _job_attr(job, "product_quantity") or runs)

            activity_map = {1: "manufacture", 3: "te_research", 4: "me_research", 5: "copy", 8: "invent"}
            activity_id = int(_job_attr(job, "activity_id") or 1)
            action_type = activity_map.get(activity_id, "manufacture")

            rows.append(DailyActionLogModel(
                plan_id=plan_id,
                generated_at=now,
                character_id=int(character_id) if character_id is not None else None,
                character_name=str(character_name) if character_name else None,
                action_type="deliver",
                shopping_category=None,
                type_id=type_id,
                type_name=type_name,
                quantity=output_qty,
                runs=runs,
                estimated_cost_isk=None,
                estimated_profit_isk=None,
                estimated_completion=end_date,
                status="pending",
                processed_for_feedback=False,
                notes=f"Deliver completed {action_type} job",
            ))

        return rows

    def _build_relist_actions(
        self,
        plan_id: int,
        pricing_suggestions: list[Any],
        now: datetime,
    ) -> list[DailyActionLogModel]:
        """Generate RELIST actions from PricingSuggestionService output."""
        rows: list[DailyActionLogModel] = []
        for suggestion in pricing_suggestions:
            if isinstance(suggestion, dict):
                type_id = int(suggestion.get("type_id") or 0)
                type_name = str(suggestion.get("type_name") or "")
                character_id = suggestion.get("character_id")
                character_name = suggestion.get("character_name")
                advised_price = suggestion.get("advised_price")
                current_price = suggestion.get("current_price")
            else:
                type_id = int(getattr(suggestion, "type_id", 0))
                type_name = str(getattr(suggestion, "type_name", ""))
                character_id = getattr(suggestion, "character_id", None)
                character_name = getattr(suggestion, "character_name", None)
                advised_price = getattr(suggestion, "advised_price", None)
                current_price = getattr(suggestion, "current_price", None)

            if type_id <= 0:
                continue

            margin_note = ""
            if advised_price and current_price and current_price > 0:
                pct = (float(advised_price) - float(current_price)) / float(current_price) * 100
                margin_note = f"advised {float(advised_price)/1e6:.1f}M (currently {float(current_price)/1e6:.1f}M, {pct:+.1f}%)"

            rows.append(DailyActionLogModel(
                plan_id=plan_id,
                generated_at=now,
                character_id=int(character_id) if character_id is not None else None,
                character_name=str(character_name) if character_name else None,
                action_type="relist_order",
                shopping_category=None,
                type_id=type_id,
                type_name=type_name,
                quantity=None,
                runs=None,
                estimated_cost_isk=None,
                estimated_profit_isk=None,
                estimated_completion=None,
                status="pending",
                processed_for_feedback=False,
                notes=margin_note or "Relist recommended",
            ))

        return rows

    def _build_job_actions(
        self,
        plan_id: int,
        assigned_actions: list[AssignedAction],
        now: datetime,
    ) -> list[DailyActionLogModel]:
        """Convert AssignedAction list to DailyActionLogModel rows (non-deliver/relist)."""
        rows: list[DailyActionLogModel] = []
        for action in assigned_actions:
            if action.action_type in ("deliver", "relist_order"):
                continue  # handled separately
            rows.append(DailyActionLogModel(
                plan_id=plan_id,
                generated_at=now,
                character_id=action.character_id,
                character_name=action.character_name,
                action_type=action.action_type,
                shopping_category=action.shopping_category,
                type_id=action.type_id,
                type_name=action.type_name,
                quantity=action.quantity,
                runs=action.runs,
                estimated_cost_isk=action.estimated_cost_isk,
                estimated_profit_isk=action.estimated_profit_isk,
                estimated_completion=action.estimated_completion,
                status="pending",
                processed_for_feedback=False,
                notes=action.notes,
            ))
        return rows

    def _build_buy_material_actions(
        self,
        plan_id: int,
        shopping_items: list[ShoppingItem],
        now: datetime,
    ) -> list[DailyActionLogModel]:
        """Generate buy_materials rows (Tab 2 only — character_id=None)."""
        rows: list[DailyActionLogModel] = []
        for item in shopping_items:
            rows.append(DailyActionLogModel(
                plan_id=plan_id,
                generated_at=now,
                character_id=None,
                character_name=None,
                action_type="buy_materials",
                shopping_category=item.shopping_category,
                type_id=item.type_id,
                type_name=item.type_name,
                quantity=item.quantity,
                runs=None,
                estimated_cost_isk=item.estimated_total,
                estimated_profit_isk=None,
                estimated_completion=None,
                status="pending",
                processed_for_feedback=False,
                notes=item.notes,
            ))
        return rows


    def _build_buy_bpo_actions(
        self,
        plan_id: int,
        bpo_opportunities: list[Any],
        now: datetime,
    ) -> list[DailyActionLogModel]:
        """Generate buy_bpo rows from ChainPlan.bpo_opportunities (Tab 2 only)."""
        rows: list[DailyActionLogModel] = []
        for opp in bpo_opportunities:
            if isinstance(opp, dict):
                type_id = int(opp.get("type_id") or 0)
                type_name = str(opp.get("type_name") or "")
                market_price = opp.get("bpo_market_price") or opp.get("market_price")
                break_even = opp.get("break_even_days")
                savings = opp.get("projected_annual_savings")
            else:
                type_id = int(getattr(opp, "type_id", 0))
                type_name = str(getattr(opp, "type_name", ""))
                market_price = getattr(opp, "bpo_market_price", None) or getattr(opp, "market_price", None)
                break_even = getattr(opp, "break_even_days", None)
                savings = getattr(opp, "projected_annual_savings", None)
            if type_id <= 0:
                continue
            notes_parts = []
            if break_even is not None:
                notes_parts.append(f"break-even {float(break_even):.0f}d")
            if savings is not None:
                notes_parts.append(f"saves {float(savings)/1e6:.1f}M ISK/yr")
            rows.append(DailyActionLogModel(
                plan_id=plan_id,
                generated_at=now,
                character_id=None,
                character_name=None,
                action_type="buy_bpo",
                shopping_category="bpo_investment",
                type_id=type_id,
                type_name=type_name,
                quantity=1,
                runs=None,
                estimated_cost_isk=float(market_price) if market_price is not None else None,
                estimated_profit_isk=float(savings) if savings is not None else None,
                estimated_completion=None,
                status="pending",
                processed_for_feedback=False,
                notes="; ".join(notes_parts) or "BPO investment opportunity",
            ))
        return rows


def _job_attr(job: Any, attr: str) -> Any:
    if isinstance(job, dict):
        return job.get(attr)
    return getattr(job, attr, None)
