"""IndustryService: market data on product rows: prices, fees, liquidity, trends, confidence, profit.

A mixin of IndustryService (service.py); it holds no state of its own."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import math
import time
from typing import TYPE_CHECKING, Any

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from eve_online_industry_tracker.db_models import (
    CharacterIndustryJobsModel,
    CharacterRealizedSalesLedgerModel,
    CorporationIndustryJobsModel,
    CorporationRealizedSalesLedgerModel,
    NpcCorporations,
    NpcStations,
)
from eve_online_industry_tracker.infrastructure.models import (
    CharacterMarketOrdersModel,
    CorporationMarketOrdersModel,
)
from eve_online_industry_tracker.application.market_analysis.market_history_service import (
    MarketHistoryService,
)
from eve_online_industry_tracker.application.market_pricing.service import MarketPricingService
from eve_online_industry_tracker.application import market_fees
from eve_online_industry_tracker.application.industry import overview_row as orow
from eve_online_industry_tracker.application.industry.facilities import _ESI_ERRORS

if TYPE_CHECKING:
    from eve_online_industry_tracker.application.industry.service import ProgressCallback


# The service module's logger: it carries the QuietWarningsFilter (see service.py).
logger = logging.getLogger("eve_online_industry_tracker.application.industry.service")


class MarketEnrichmentMixin:
    _SKILL_ID_ACCOUNTING = 16622
    _SKILL_ID_BROKER_RELATIONS = 3446

    def _relist_broker_fee_fraction(self, row: dict[str, Any]) -> tuple[float, str | None]:
        """(the broker fee fraction of a relist, the note when it is assumed).
        0.0 when the product sells into buy orders (no order to relist); else
        the product's own broker fee (manufacturing_job.market_fee_context)
        when known, 0.0 included; otherwise admin setting
        market_defaults/default_broker_fee, with a note naming it."""
        context = (row.get("manufacturing_job") or {}).get("market_fee_context") or {}
        if context and not context.get("broker_fee_applies"):
            return 0.0, None
        own = self._as_float((context.get("rates") or {}).get("broker_fee_fraction"))
        if own is not None:
            return own, None
        assumed = float(self._adm("market_defaults", "default_broker_fee"))
        return assumed, (f"relist cost as if the broker fee is {assumed:.2%} (its own is unknown; admin setting "
                         "market_defaults/default_broker_fee)")

    def _resolve_npc_market_fee_context(
        self,
        *,
        character_id: int | None,
        market_hub: str,
        product_price_side: str,
    ) -> dict[str, Any]:
        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        hub_context = pricing_service._market_hub_context(market_hub)
        normalized_product_price_side = MarketPricingService.normalize_order_side(product_price_side)

        sales_tax_fraction = None
        broker_fee_fraction = None
        broker_fee_applies = normalized_product_price_side == "sell"
        broker_fee_kind = "order_creation" if broker_fee_applies else "direct_sale"
        accounting_level = 0
        broker_relations_level = 0
        owner_corp_id: int | None = None
        owner_faction_id: int | None = None
        faction_standing = 0.0
        corp_standing = 0.0

        if character_id is not None:
            trained_skill_levels = self._get_character_trained_skill_levels(character_id=int(character_id))
            accounting_level = int(trained_skill_levels.get(self._SKILL_ID_ACCOUNTING, 0) or 0)
            broker_relations_level = int(trained_skill_levels.get(self._SKILL_ID_BROKER_RELATIONS, 0) or 0)
            fee_settings = market_fees.MarketFeeSettings.from_admin(self._admin)
            sales_tax_fraction = market_fees.sales_tax_fraction(fee_settings, accounting_level=accounting_level)

            sde_session: Any = self._sessions.sde_session()
            station = (
                sde_session.query(NpcStations)
                .filter(NpcStations.id == int(hub_context.get("station_id") or 0))
                .first()
            )
            if station is not None:
                try:
                    owner_corp_id = int(station.ownerID)
                except (TypeError, ValueError):
                    owner_corp_id = None
            if owner_corp_id is not None:
                corporation = (
                    sde_session.query(NpcCorporations)
                    .filter(NpcCorporations.id == int(owner_corp_id))
                    .first()
                )
                if corporation is not None and getattr(corporation, "factionID", None) is not None:
                    try:
                        owner_faction_id = int(corporation.factionID)
                    except (TypeError, ValueError):
                        owner_faction_id = None

            faction_standing = self._get_character_standing(
                character_id=int(character_id),
                from_type="faction",
                from_id=owner_faction_id,
            )
            corp_standing = self._get_character_standing(
                character_id=int(character_id),
                from_type="npc_corp",
                from_id=owner_corp_id,
            )
            broker_fee_fraction = market_fees.broker_fee_fraction(
                fee_settings, broker_relations_level=broker_relations_level,
                faction_standing=faction_standing, corp_standing=corp_standing)

        return {
            "market_hub": hub_context.get("hub"),
            "market_hub_label": hub_context.get("label"),
            "station_id": hub_context.get("station_id"),
            "region_id": hub_context.get("region_id"),
            "station_type": "npc_station",
            "output_price_side": normalized_product_price_side,
            "broker_fee_applies": broker_fee_applies,
            "broker_fee_kind": broker_fee_kind,
            "owner_corp_id": owner_corp_id,
            "owner_faction_id": owner_faction_id,
            "skills": {
                "accounting_level": accounting_level,
                "broker_relations_level": broker_relations_level,
            },
            "standings": {
                "faction_standing": faction_standing,
                "corp_standing": corp_standing,
            },
            "rates": {
                "sales_tax_fraction": sales_tax_fraction,
                "broker_fee_fraction": broker_fee_fraction,
            },
        }

    def _enrich_product_rows_with_sale_proceeds(
        self,
        product_rows: list[dict[str, Any]],
        *,
        character_id: int | None,
        market_hub: str,
        product_price_side: str,
        fee_context: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """`fee_context`: the refresh's resolved fee context (the planning
        context's), so the rows and the decryptor choice use the same fees;
        resolved here when None."""
        if not product_rows:
            return product_rows

        if fee_context is None:
            fee_context = self._resolve_npc_market_fee_context(
                character_id=character_id,
                market_hub=market_hub,
                product_price_side=product_price_side,
            )
        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue

            market_unit_price = self._as_float(row.get("market_unit_price"))
            product_quantity = int(row.get("quantity") or 0)
            if market_unit_price is None or product_quantity <= 0:
                manufacturing_job["gross_sale_value"] = None
                manufacturing_job["broker_fee_amount"] = None
                manufacturing_job["sales_tax_amount"] = None
                manufacturing_job["net_proceeds"] = None
                manufacturing_job["market_fee_context"] = fee_context
                row["gross_sale_value"] = None
                row["broker_fee_amount"] = None
                row["sales_tax_amount"] = None
                row["net_proceeds"] = None
                continue

            gross_sale_value = float(market_unit_price) * float(product_quantity)
            broker_fee_amount, sales_tax_amount, net_proceeds, net_unknown_reason = self._sale_fee_amounts(
                gross_sale_value, fee_context,
            )

            manufacturing_job["gross_sale_value"] = gross_sale_value
            manufacturing_job["broker_fee_amount"] = broker_fee_amount
            manufacturing_job["sales_tax_amount"] = sales_tax_amount
            manufacturing_job["net_proceeds"] = net_proceeds
            manufacturing_job["market_fee_context"] = fee_context
            if net_unknown_reason is not None:
                manufacturing_job["net_proceeds_unknown_reason"] = net_unknown_reason
            else:
                manufacturing_job.pop("net_proceeds_unknown_reason", None)

            row["gross_sale_value"] = gross_sale_value
            row["broker_fee_amount"] = broker_fee_amount
            row["sales_tax_amount"] = sales_tax_amount
            row["net_proceeds"] = net_proceeds

        return product_rows

    def _enrich_product_rows_with_market_activity(
        self,
        product_rows: list[dict[str, Any]],
        *,
        market_hub: str,
        progress_callback: ProgressCallback | None = None,
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        product_type_ids = sorted(
            {
                int(row.get("type_id") or 0)
                for row in product_rows
                if isinstance(row, dict) and int(row.get("type_id") or 0) > 0
            }
        )
        if not product_type_ids:
            return product_rows

        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        if progress_callback is not None:
            progress_callback(
                0.91,
                "Loading regional market history and hub liquidity",
                {"stage": "market_activity", "type_count": len(product_type_ids)},
            )
        # The types whose region history could not be fetched: unknown volume, with the reason.
        volume_unavailable: dict[int, str] = {}
        with ThreadPoolExecutor(max_workers=2) as pool:
            volume_future = pool.submit(
                pricing_service.get_region_daily_volume_map,
                type_ids=product_type_ids, hub=market_hub, unavailable=volume_unavailable,
            )
            liquidity_future = pool.submit(pricing_service.get_hub_liquidity_map, type_ids=product_type_ids, hub=market_hub)
            region_daily_volume_map = volume_future.result()
            hub_liquidity_map = liquidity_future.result()

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue

            type_id = int(row.get("type_id") or 0)
            daily_volume = region_daily_volume_map.get(type_id) or {}
            hub_liquidity = hub_liquidity_map.get(type_id) or {}
            # No region history for the type: its volume is unknown (None), not 0.
            volume_known = type_id in region_daily_volume_map
            volume_reason = None if volume_known else (
                volume_unavailable.get(type_id) or "region market history unavailable: daily volume unknown")
            region_volume = int(daily_volume.get("daily_volume") or 0) if volume_known else None
            region_order_count = int(daily_volume.get("daily_order_count") or 0) if volume_known else None

            manufacturing_job["region_daily_volume"] = region_volume
            manufacturing_job["region_daily_volume_7d_avg"] = self._as_float(daily_volume.get("daily_volume_7d_avg"))
            manufacturing_job["region_daily_volume_7d_sample_size"] = int(daily_volume.get("daily_volume_7d_sample_size") or 0)
            manufacturing_job["region_daily_order_count"] = region_order_count
            manufacturing_job["region_daily_volume_date"] = daily_volume.get("daily_volume_date")
            manufacturing_job["region_daily_volume_unavailable_reason"] = volume_reason
            # A failed order-book side is unknown (None, with the reason), not 0 units.
            liquidity_fields = {
                "hub_buy_liquidity": self._optional_int(hub_liquidity, "buy_volume_total"),
                "hub_sell_liquidity": self._optional_int(hub_liquidity, "sell_volume_total"),
                "hub_buy_order_count": self._optional_int(hub_liquidity, "buy_order_count"),
                "hub_sell_order_count": self._optional_int(hub_liquidity, "sell_order_count"),
                "liquidity_unavailable_reason": hub_liquidity.get("liquidity_unavailable_reason") or (
                    None if type_id in hub_liquidity_map else "hub liquidity unknown: no order book data returned"
                ),
            }
            manufacturing_job.update(liquidity_fields)

            row["region_daily_volume"] = region_volume
            row["region_daily_volume_7d_avg"] = self._as_float(daily_volume.get("daily_volume_7d_avg"))
            row["region_daily_volume_7d_sample_size"] = int(daily_volume.get("daily_volume_7d_sample_size") or 0)
            row["region_daily_order_count"] = region_order_count
            row["region_daily_volume_date"] = daily_volume.get("daily_volume_date")
            row["region_daily_volume_unavailable_reason"] = volume_reason
            row.update(liquidity_fields)

        if progress_callback is not None:
            progress_callback(
                0.92,
                "Applied market activity signals",
                {"stage": "liquidity", "rows": len(product_rows)},
            )

        return product_rows

    def _enrich_product_rows_with_liquidity_metrics(
        self,
        product_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        for row in product_rows:
            if not isinstance(row, dict):
                continue

            manufacturing_job = row.get("manufacturing_job") or {}
            # None: the hub's sell book could not be fetched, every metric is unknown.
            hub_sell_liquidity = self._optional_int(manufacturing_job, "hub_sell_liquidity")
            region_daily_volume_7d_avg = self._as_float(manufacturing_job.get("region_daily_volume_7d_avg"))
            hub_sell_orders = self._optional_int(manufacturing_job, "hub_sell_order_count")

            row["days_of_supply"] = self._calculate_days_of_supply(
                hub_sell_liquidity=hub_sell_liquidity,
                region_daily_volume_7d_avg=region_daily_volume_7d_avg,
            )
            row["sell_through_rate"] = self._calculate_sell_through_rate(
                region_daily_volume_7d_avg=region_daily_volume_7d_avg,
                hub_sell_liquidity=hub_sell_liquidity,
            )
            row["liquidity_indicator"] = self._calculate_liquidity_indicator(
                hub_sell_liquidity=hub_sell_liquidity,
                region_daily_volume_7d_avg=region_daily_volume_7d_avg,
                hub_sell_orders=hub_sell_orders,
            )
            row["liquidity_score"] = self._calculate_liquidity_score(
                hub_sell_liquidity=hub_sell_liquidity,
                region_daily_volume_7d_avg=region_daily_volume_7d_avg,
                hub_sell_orders=hub_sell_orders,
            )

            vol_7d = self._as_float(row.get("region_daily_volume_7d_avg")) or 0
            mfg_job_liq = row.get("manufacturing_job", {})
            runs_liq = int(mfg_job_liq.get("runs") or 1)
            qty_per_run_liq = int(mfg_job_liq.get("product_quantity_per_run") or 1)
            prod_qty = runs_liq * qty_per_run_liq
            row["production_to_demand_ratio"] = (prod_qty / vol_7d) if vol_7d > 0 else None

        return product_rows

    def _enrich_product_rows_with_price_anomaly(
        self,
        product_rows: list[dict[str, Any]],
        *,
        market_hub: str,
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        hub_context = pricing_service._market_hub_context(market_hub)
        region_id = int(hub_context.get("region_id") or 10000002)

        history_service = MarketHistoryService(state=self._state, sessions=self._sessions)

        type_ids = sorted({
            int(row.get("type_id") or 0)
            for row in product_rows
            if isinstance(row, dict) and int(row.get("type_id") or 0) > 0
        })

        # Fetch ESI history for any type_id not yet in the DB (runs once; subsequent calls hit the DB)
        missing: list[int] = []
        for tid in type_ids:
            stats = history_service.get_price_stats(type_id=tid, region_id=region_id, days=60)
            if not stats.get("has_data"):
                missing.append(tid)

        # Why a type has no history to compare with: shown on its row, not a silent skip.
        history_unavailable: dict[int, str] = {}
        if missing:
            with ThreadPoolExecutor(max_workers=20) as pool:
                futures = {
                    pool.submit(history_service.fetch_and_store_history, type_id=tid, region_id=region_id): tid
                    for tid in missing
                }
                for f in as_completed(futures):
                    try:
                        reason = f.result()
                    except (*_ESI_ERRORS, SQLAlchemyError) as exc:
                        logger.warning("Price anomaly: the market history of type_id=%s could not be fetched (%s)",
                                       futures[f], exc)
                        reason = f"market history could not be fetched: {exc}"
                    if reason:
                        history_unavailable[futures[f]] = str(reason)

        # Load price stats for all type_ids
        price_stats: dict[int, dict[str, Any]] = {}
        for tid in type_ids:
            try:
                price_stats[tid] = history_service.get_price_stats(type_id=tid, region_id=region_id, days=60)
            except SQLAlchemyError as exc:
                logger.warning("Price anomaly: the price stats of type_id=%s cannot be read (%s); no anomaly check for "
                               "it", tid, exc)
                price_stats[tid] = {"has_data": False}
                history_unavailable.setdefault(tid, f"price stats unreadable (DB error: {type(exc).__name__})")

        _risk_order: dict[str, int] = {"High": 3, "Medium": 2, "Low": 1}

        for row in product_rows:
            if not isinstance(row, dict):
                continue

            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                manufacturing_job = {}

            type_id = int(row.get("type_id") or 0)
            market_unit_price = self._as_float(row.get("market_unit_price"))
            quantity = max(1, int(row.get("quantity") or 1))
            material_cost = self._as_float(manufacturing_job.get("material_cost"))
            hub_sell_order_count = self._optional_int(row, "hub_sell_order_count")

            anomaly_reasons: list[str] = []
            risk_level: str | None = None

            def upgrade(level: str) -> str | None:
                if _risk_order.get(level, 0) > _risk_order.get(risk_level or "", 0):
                    return level
                return risk_level

            price_vs_material_ratio: float | None = None
            price_vs_history_ratio: float | None = None
            history_7d_avg: float | None = None

            if market_unit_price is not None and market_unit_price > 0:
                # Tier 1a: price vs unit material cost
                if material_cost is not None and material_cost > 0:
                    unit_material_cost = material_cost / quantity
                    if unit_material_cost > 0:
                        price_vs_material_ratio = market_unit_price / unit_material_cost
                        if price_vs_material_ratio > 1000:
                            anomaly_reasons.append(
                                f"Price is {price_vs_material_ratio:,.0f}× material cost — likely manipulated or data error"
                            )
                            risk_level = upgrade("High")
                        elif price_vs_material_ratio > 100:
                            anomaly_reasons.append(
                                f"Price is {price_vs_material_ratio:,.0f}× material cost — unusually high markup"
                            )
                            risk_level = upgrade("Medium")
                    if market_unit_price < material_cost / quantity:
                        anomaly_reasons.append("Market price is below manufacturing material cost")
                        risk_level = upgrade("Medium")

                # Tier 1b: thin orderbook (unknown when the hub's sell book could not be fetched)
                if hub_sell_order_count is None:
                    anomaly_reasons.append(
                        "Hub sell orders unknown, the thin-market check is skipped: "
                        f"{row.get('liquidity_unavailable_reason') or 'no hub order book data'}"
                    )
                elif hub_sell_order_count < 3:
                    anomaly_reasons.append(
                        f"Only {hub_sell_order_count} sell order(s) at hub — thin market, easy to manipulate"
                    )
                    risk_level = upgrade("Low")

                # Tier 2: historical price comparison
                stats = price_stats.get(type_id) or {}
                if stats.get("has_data"):
                    avg_7d = self._as_float(stats.get("avg_7d"))
                    avg_60d = self._as_float(stats.get("avg_42w"))  # avg over the 60-day window
                    baseline = avg_7d or avg_60d
                    history_7d_avg = avg_7d
                    if baseline and baseline > 0:
                        price_vs_history_ratio = market_unit_price / baseline
                        if price_vs_history_ratio > 5:
                            anomaly_reasons.append(
                                f"Price is {price_vs_history_ratio:.1f}× 7-day historical average — strong manipulation signal"
                            )
                            risk_level = upgrade("High")
                        elif price_vs_history_ratio > 2:
                            anomaly_reasons.append(
                                f"Price is {price_vs_history_ratio:.1f}× 7-day historical average — elevated"
                            )
                            risk_level = upgrade("Medium")
                        elif price_vs_history_ratio < 0.3:
                            anomaly_reasons.append(
                                f"Price is {price_vs_history_ratio:.2f}× 7-day historical average — significantly below historical norm"
                            )
                            risk_level = upgrade("Medium")
                elif history_unavailable.get(type_id):
                    anomaly_reasons.append(
                        f"No historical price check: {history_unavailable[type_id]}"
                    )

            row["price_anomaly_risk"] = risk_level
            row["price_history_unavailable_reason"] = (
                history_unavailable.get(type_id) if not (price_stats.get(type_id) or {}).get("has_data") else None
            )
            row["price_anomaly_reasons"] = anomaly_reasons
            row["price_vs_material_ratio"] = price_vs_material_ratio
            row["price_vs_history_ratio"] = price_vs_history_ratio
            row["history_7d_avg"] = history_7d_avg

        return product_rows

    def _enrich_product_rows_with_market_trends(
        self,
        rows: list[dict],
        *,
        market_history_svc: "MarketHistoryService",
        region_id: int = 10000002,
    ) -> None:
        app_session = self._sessions.app_session()

        # Batch fetch price stats for all product type_ids + their materials
        product_type_ids = [r["type_id"] for r in rows if r.get("type_id")]

        # Collect all material type_ids (top 5 by value per product)
        material_type_ids: set[int] = set()
        for row in rows:
            mfg_job = row.get("manufacturing_job", {})
            mats = mfg_job.get("procurement_materials", {})
            sorted_mats = sorted(
                mats.values(),
                key=lambda m: float(m.get("total_quantity") or m.get("quantity") or 0),
                reverse=True,
            )[:5]
            for m in sorted_mats:
                tid = m.get("type_id")
                if tid:
                    material_type_ids.add(int(tid))

        all_type_ids = set(product_type_ids) | material_type_ids

        # Fetch price stats from DB (no ESI fetch — use cached data only)
        price_stats_map: dict[int, dict] = {}
        for tid in all_type_ids:
            try:
                stats = market_history_svc.get_price_stats(type_id=tid, region_id=region_id)
                if stats.get("has_data"):
                    price_stats_map[tid] = stats
            except SQLAlchemyError as exc:
                logger.warning("Market trends: the price stats of type_id=%s cannot be read (%s); no trend for it",
                               tid, exc)

        # Pipeline query — batch for all type_ids
        pipeline_jobs: dict[int, int] = {}
        pipeline_orders: dict[int, int] = {}
        historical_margins: dict[int, float | None] = {}

        if product_type_ids:
            for tid in product_type_ids:
                # Active/ready jobs
                char_jobs = (
                    app_session.query(func.sum(CharacterIndustryJobsModel.output_quantity))
                    .filter(
                        CharacterIndustryJobsModel.product_type_id == tid,
                        CharacterIndustryJobsModel.status.in_(["active", "ready"]),
                    )
                    .scalar() or 0
                )
                corp_jobs = (
                    app_session.query(func.sum(CorporationIndustryJobsModel.output_quantity))
                    .filter(
                        CorporationIndustryJobsModel.product_type_id == tid,
                        CorporationIndustryJobsModel.status.in_(["active", "ready"]),
                    )
                    .scalar() or 0
                )
                pipeline_jobs[tid] = int(char_jobs) + int(corp_jobs)

                # Market orders
                char_orders = (
                    app_session.query(func.sum(CharacterMarketOrdersModel.volume_remain))
                    .filter(
                        CharacterMarketOrdersModel.type_id == tid,
                        CharacterMarketOrdersModel.is_buy_order == False,  # noqa: E712
                    )
                    .scalar() or 0
                )
                corp_orders = (
                    app_session.query(func.sum(CorporationMarketOrdersModel.volume_remain))
                    .filter(
                        CorporationMarketOrdersModel.type_id == tid,
                        CorporationMarketOrdersModel.is_buy_order == False,  # noqa: E712
                    )
                    .scalar() or 0
                )
                pipeline_orders[tid] = int(char_orders) + int(corp_orders)

            # Historical realized margins — last 180 days
            cutoff = (datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%d")
            for tid in product_type_ids:
                records: list[float] = []
                for Model in (CharacterRealizedSalesLedgerModel, CorporationRealizedSalesLedgerModel):
                    rows_q = (
                        app_session.query(Model.realized_margin_fraction)
                        .filter(
                            Model.type_id == tid,
                            Model.date >= cutoff,
                            Model.confidence != "Low",
                            Model.realized_margin_fraction.isnot(None),
                        )
                        .all()
                    )
                    records.extend([r[0] for r in rows_q])
                if len(records) >= 2:
                    historical_margins[tid] = sum(records) / len(records) * 100
                else:
                    historical_margins[tid] = None

        ORDER_DURATION_DAYS = 90.0

        for row in rows:
            tid = row.get("type_id")
            mfg_job = row.get("manufacturing_job", {})
            stats = price_stats_map.get(tid, {})

            # Price trend signals
            row["price_trend_7d_pct"] = stats.get("trend_pct")
            row["price_trend_30d_pct"] = stats.get("trend_30d_pct")
            row["price_avg_7d"] = stats.get("avg_7d")
            row["price_avg_42w"] = stats.get("avg_42w")
            row["price_volatility_pct"] = stats.get("volatility_pct")

            # Margin buffer
            market_price = self._as_float(row.get("market_unit_price"))
            total_cost = self._as_float(mfg_job.get("total_cost"))
            runs = int(mfg_job.get("runs") or 1)
            qty_per_run = int(mfg_job.get("product_quantity_per_run") or 1)
            total_units = runs * qty_per_run
            unit_build_cost = (total_cost / total_units) if (total_cost and total_units > 0) else None

            if market_price and unit_build_cost and unit_build_cost > 0:
                row["margin_buffer_pct"] = (market_price - unit_build_cost) / unit_build_cost * 100
            else:
                row["margin_buffer_pct"] = None

            # Projected price at delivery
            trend_30d_pct = stats.get("trend_30d_pct")
            mfg_seconds = self._as_float(mfg_job.get("manufacturing_time_seconds")) or 0
            mfg_days = mfg_seconds / 86400.0

            if market_price and trend_30d_pct is not None:
                # 7d vs 30d average: their midpoints (day 3.5 and day 15) are 11.5 days apart.
                daily_trend = trend_30d_pct / 100.0 / 11.5
                projected_price = market_price * (1.0 + daily_trend * mfg_days)
                row["projected_price_at_delivery"] = projected_price
                if unit_build_cost and unit_build_cost > 0 and projected_price > 0:
                    row["projected_margin_pct"] = (projected_price - unit_build_cost) / projected_price * 100
                else:
                    row["projected_margin_pct"] = None
            else:
                row["projected_price_at_delivery"] = None
                row["projected_margin_pct"] = None

            # Material cost trend (weighted avg of top 5 materials)
            mats = mfg_job.get("procurement_materials", {})
            sorted_mats = sorted(
                mats.values(),
                key=lambda m: float(m.get("total_quantity") or m.get("quantity") or 0),
                reverse=True,
            )[:5]
            mat_trends: list[float] = []
            mat_weights: list[float] = []
            for m in sorted_mats:
                mat_tid = m.get("type_id")
                mat_qty = float(m.get("total_quantity") or m.get("quantity") or 0)
                if mat_tid and mat_qty > 0:
                    mat_stats = price_stats_map.get(int(mat_tid), {})
                    t = mat_stats.get("trend_pct")
                    if t is not None:
                        mat_trends.append(t)
                        mat_weights.append(mat_qty)
            if mat_trends:
                total_w = sum(mat_weights)
                row["material_cost_trend_pct"] = (
                    sum(t * w for t, w in zip(mat_trends, mat_weights)) / total_w
                    if total_w > 0
                    else None
                )
            else:
                row["material_cost_trend_pct"] = None

            # Pipeline
            units_jobs = pipeline_jobs.get(tid, 0)
            units_orders = pipeline_orders.get(tid, 0)
            pipeline_total = units_jobs + units_orders
            row["pipeline_units_in_jobs"] = units_jobs
            row["pipeline_units_on_market"] = units_orders
            row["pipeline_total_units"] = pipeline_total
            vol_7d = self._as_float(row.get("region_daily_volume_7d_avg")) or 0
            row["pipeline_days_supply"] = (pipeline_total / vol_7d) if vol_7d > 0 else None

            # Historical realized margin
            hist_margin = historical_margins.get(tid)
            row["historical_realized_margin_pct"] = hist_margin
            proj_margin = self._as_float(row.get("profit_margin_fraction"))
            if hist_margin is not None and proj_margin is not None:
                row["historical_vs_projected_delta_pct"] = hist_margin - (proj_margin * 100)
            else:
                row["historical_vs_projected_delta_pct"] = None

            # Relist cost modeling
            vol_7d_avg = self._as_float(row.get("region_daily_volume_7d_avg")) or 0
            if total_units > 0 and vol_7d_avg > 0:
                selling_days: float | None = total_units / vol_7d_avg
            else:
                selling_days = None
            row["selling_time_days"] = selling_days

            # Extra relist broker fee if selling_days > ORDER_DURATION_DAYS
            broker_fee_frac, broker_fee_assumed = self._relist_broker_fee_fraction(row)
            row["relist_broker_fee_assumed"] = broker_fee_assumed
            if selling_days and selling_days > ORDER_DURATION_DAYS and market_price:
                extra_listings = int(selling_days / ORDER_DURATION_DAYS)
                relist_cost_per_unit = market_price * broker_fee_frac * extra_listings
            else:
                relist_cost_per_unit = 0.0
            row["relist_cost_per_unit"] = relist_cost_per_unit

            # Effective profit after relist costs
            profit_per_unit = self._as_float(row.get("profit_amount"))
            if profit_per_unit is not None and total_units > 0:
                profit_per_unit_val = profit_per_unit / total_units
                effective_profit_per_unit = profit_per_unit_val - relist_cost_per_unit
                row["effective_profit_with_relist"] = effective_profit_per_unit * total_units
                net_rev = self._as_float(row.get("net_proceeds"))
                if net_rev and net_rev > 0:
                    row["effective_margin_with_relist_pct"] = (
                        effective_profit_per_unit * total_units / net_rev
                    ) * 100
                else:
                    row["effective_margin_with_relist_pct"] = None
            else:
                row["effective_profit_with_relist"] = None
                row["effective_margin_with_relist_pct"] = None

    _ASSUMES_OWNED_T2_BPO_REASON = "costed as if a T2 BPO is owned (build_from_bpc off)"

    def _enrich_product_rows_with_pricing_confidence(
        self,
        product_rows: list[dict[str, Any]],
        *,
        product_price_side: str,
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        normalized_product_price_side = MarketPricingService.normalize_order_side(product_price_side)
        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        target_sample_size = max(2, pricing_service.orderbook_depth())
        ttl_minutes = max(1.0, float(pricing_service.material_price_cache_ttl_seconds()) / 60.0)
        now = time.time()

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue

            reasons: list[str] = []
            score = 0

            market_unit_price = self._as_float(row.get("market_unit_price"))
            market_price_sample_size = int(row.get("market_price_sample_size") or 0)
            market_price_fetched_at = self._as_float(row.get("market_price_fetched_at"))
            market_price_age_minutes = None
            if market_price_fetched_at is not None and market_price_fetched_at > 0:
                market_price_age_minutes = max(0.0, (float(now) - float(market_price_fetched_at)) / 60.0)

            # None: that side's hub order book could not be fetched (unknown, not 0 units).
            if normalized_product_price_side == "buy":
                relevant_liquidity = self._optional_int(row, "hub_buy_liquidity")
                relevant_order_count = self._optional_int(row, "hub_buy_order_count")
            else:
                relevant_liquidity = self._optional_int(row, "hub_sell_liquidity")
                relevant_order_count = self._optional_int(row, "hub_sell_order_count")

            region_daily_volume = int(row.get("region_daily_volume") or 0)
            region_daily_volume_7d_avg = self._as_float(row.get("region_daily_volume_7d_avg"))

            if market_unit_price is not None and market_unit_price > 0:
                score += 2
                reasons.append("Product has a usable market price.")
            else:
                reasons.append("Product has no usable market price.")

            if market_price_age_minutes is None:
                reasons.append("Price freshness timestamp is unavailable.")
            elif market_price_age_minutes <= min(30.0, ttl_minutes):
                score += 2
                reasons.append(f"Price snapshot is fresh ({market_price_age_minutes:.0f} min old).")
            elif market_price_age_minutes <= ttl_minutes:
                score += 1
                reasons.append(f"Price snapshot is moderately fresh ({market_price_age_minutes:.0f} min old).")
            else:
                reasons.append(f"Price snapshot is aging ({market_price_age_minutes:.0f} min old).")

            if market_price_sample_size >= target_sample_size:
                score += 2
                reasons.append(f"Orderbook sample covers {market_price_sample_size} levels.")
            elif market_price_sample_size > 0:
                score += 1
                reasons.append(f"Orderbook sample is thin at {market_price_sample_size} levels.")
            else:
                reasons.append("Orderbook sample size is missing.")

            _liq_threshold = int(self._adm("industry", "portfolio_liquidity_threshold"))
            _ord_threshold = int(self._adm("industry", "portfolio_order_count_threshold"))
            if relevant_liquidity is None or relevant_order_count is None:
                reasons.append(
                    f"{normalized_product_price_side.title()}-side hub liquidity is unknown "
                    f"({row.get('liquidity_unavailable_reason') or 'no hub order book data'})."
                )
            elif relevant_liquidity >= _liq_threshold or relevant_order_count >= _ord_threshold:
                score += 2
                reasons.append(
                    f"{normalized_product_price_side.title()}-side hub liquidity is healthy ({relevant_liquidity:,} units / {relevant_order_count} orders)."
                )
            elif relevant_liquidity > 0 or relevant_order_count > 0:
                score += 1
                reasons.append(
                    f"{normalized_product_price_side.title()}-side hub liquidity is present but limited ({relevant_liquidity:,} units / {relevant_order_count} orders)."
                )
            else:
                reasons.append(f"No {normalized_product_price_side}-side hub liquidity is currently visible.")

            volume_unknown_reason = row.get("region_daily_volume_unavailable_reason")
            if volume_unknown_reason:
                # No region history: the volume is unknown, not absent.
                reasons.append(f"Regional trade volume is unknown ({volume_unknown_reason}).")
            elif (region_daily_volume_7d_avg is not None and region_daily_volume_7d_avg >= 20.0) or region_daily_volume >= 20:
                score += 2
                reasons.append("Regional trade volume is active.")
            elif (region_daily_volume_7d_avg is not None and region_daily_volume_7d_avg > 0.0) or region_daily_volume > 0:
                score += 1
                reasons.append("Regional trade volume exists but is limited.")
            else:
                reasons.append("Regional trade volume is absent.")

            # Liquidity quality signal
            liquidity_indicator = str(row.get("liquidity_indicator") or "")
            if liquidity_indicator in ("Very High", "High"):
                score += 2
                reasons.append(f"Market liquidity is {liquidity_indicator.lower()} — sell-through is reliable.")
            elif liquidity_indicator == "Medium":
                score += 1
                reasons.append("Market liquidity is medium — some sell-side uncertainty.")
            elif liquidity_indicator in ("Low", "Very Low"):
                reasons.append(f"Market liquidity is {liquidity_indicator.lower()} — pricing may be unreliable.")

            # Price anomaly / manipulation signal
            price_anomaly_risk = str(row.get("price_anomaly_risk") or "")
            price_anomaly_reasons_list = row.get("price_anomaly_reasons") or []
            if price_anomaly_risk == "High":
                score = max(0, score - 6)
                detail = ("; ".join(price_anomaly_reasons_list[:1])) if price_anomaly_reasons_list else "see Price Anomaly Reasons column"
                reasons.append(f"High price anomaly risk — market may be manipulated ({detail}).")
            elif price_anomaly_risk == "Medium":
                score = max(0, score - 3)
                reasons.append("Price anomaly signals detected — treat pricing with caution.")
            elif price_anomaly_risk == "Low":
                score = max(0, score - 1)
                reasons.append("Minor price anomaly signal (thin orderbook).")

            if market_unit_price is None or market_unit_price <= 0:
                confidence = "Low"
            elif score >= 7:
                confidence = "High"
            elif score >= 4:
                confidence = "Medium"
            else:
                confidence = "Low"

            if manufacturing_job.get("assumes_owned_t2_bpo"):
                reasons.append(self._ASSUMES_OWNED_T2_BPO_REASON)

            job_fee_unknown_reasons = manufacturing_job.get("job_fee_unknown_reasons") or []
            if job_fee_unknown_reasons:
                # I1: the cost leaves these fees out, so the profit is
                # overstated: never High, whatever the market says.
                if confidence == "High":
                    confidence = "Medium"
                reasons.append(
                    f"Job fees unknown for: {'; '.join(str(r) for r in job_fee_unknown_reasons)} — "
                    f"left out of the total cost, so the cost is understated."
                )
            rig_bonus_unknown_reason = manufacturing_job.get("rig_bonus_unknown_reason")
            if rig_bonus_unknown_reason:
                impacts = [
                    self._AGGREGATE_RIG_IMPACT[m] for m in (manufacturing_job.get("rig_bonus_unknown_metrics") or [])
                    if m in self._AGGREGATE_RIG_IMPACT
                ] or ["the costs may be overstated"]
                reasons.append(
                    f"{str(rig_bonus_unknown_reason)[:1].upper()}{str(rig_bonus_unknown_reason)[1:]} — "
                    f"{'; '.join(impacts)}, never an under-buy."
                )
            net_proceeds_unknown_reason = manufacturing_job.get("net_proceeds_unknown_reason")
            if net_proceeds_unknown_reason:
                reasons.append(f"Net proceeds unknown: {net_proceeds_unknown_reason}.")

            manufacturing_job["pricing_confidence"] = confidence
            manufacturing_job["pricing_confidence_reasons"] = reasons
            manufacturing_job["market_price_age_minutes"] = market_price_age_minutes

            row["pricing_confidence"] = confidence
            row["pricing_confidence_reasons"] = reasons
            row["market_price_age_minutes"] = market_price_age_minutes

            # Material-side pricing confidence (#5)
            procurement_materials = manufacturing_job.get("procurement_materials") or manufacturing_job.get("materials") or {}
            if isinstance(procurement_materials, dict):
                mat_total = 0
                mat_priced = 0
                mat_with_known_cost = 0
                unknown_cost_basis_count = 0
                for mat in procurement_materials.values():
                    if not isinstance(mat, dict):
                        continue
                    mat_total += 1
                    if self._as_float(mat.get("unit_price")) is not None:
                        mat_priced += 1
                    if bool(mat.get("uses_unknown_owned_cost_basis", False)):
                        unknown_cost_basis_count += 1
                    else:
                        mat_with_known_cost += 1
                # Expected invention inputs (invented T2) are material lines too.
                unpriced_invention_names: list[str] = []
                invention_materials = manufacturing_job.get("expected_invention_materials")
                for mat in (invention_materials.values() if isinstance(invention_materials, dict) else []):
                    if not isinstance(mat, dict):
                        continue
                    mat_total += 1
                    if self._as_float(mat.get("unit_price")) is not None:
                        mat_priced += 1
                    else:
                        unpriced_invention_names.append(str(mat.get("type_name") or mat.get("type_id") or "?"))
                    if bool(mat.get("uses_unknown_owned_cost_basis", False)):
                        unknown_cost_basis_count += 1
                    else:
                        mat_with_known_cost += 1
                if unpriced_invention_names:
                    reasons.append(
                        f"No price for invention input(s) {', '.join(unpriced_invention_names)} — "
                        f"left out of the material cost, so the T2 cost is understated."
                    )
                if mat_total == 0:
                    material_confidence = "Low"
                elif mat_priced == mat_total and mat_with_known_cost == mat_total:
                    material_confidence = "High"
                elif mat_priced >= mat_total * 0.8:
                    material_confidence = "Medium"
                else:
                    material_confidence = "Low"
                if unknown_cost_basis_count > 0:
                    reasons.append(
                        f"{unknown_cost_basis_count} of {mat_total} material(s) are taken from inventory "
                        f"with no acquisition cost on record — cost estimated at market price."
                    )
            else:
                material_confidence = "Low"
                unknown_cost_basis_count = 0
            invention_cost_unknown_reason = manufacturing_job.get("invention_cost_unknown_reason")
            if invention_cost_unknown_reason:
                material_confidence = "Low"
                # Task 15 fix round 1 (minor 2): the T2 cost leaves the
                # invention out, so the shown confidence is Low too.
                manufacturing_job["pricing_confidence"] = "Low"
                row["pricing_confidence"] = "Low"
                datacore_need_unknown = manufacturing_job.get("invention_datacore_need_unknown")
                if isinstance(datacore_need_unknown, dict) and datacore_need_unknown.get("batch_one_cost_exact"):
                    # Only the batches past the owned BPC runs invent with
                    # unknown odds; batch 1 runs on the owned BPC.
                    reasons.append(
                        f"Datacore need unknown for batches past the owned BPC runs "
                        f"({invention_cost_unknown_reason}); batch 1 cost is exact, later batches will cost more."
                    )
                else:
                    reasons.append(
                        f"Invention cost unknown ({invention_cost_unknown_reason}) — the T2 cost leaves out "
                        f"datacores and invention fees, so it is understated."
                    )
            invention_chance = manufacturing_job.get("invention_chance")
            if isinstance(invention_chance, dict) and invention_chance.get("skills_unknown_reason"):
                reasons.append(
                    f"Invention chance uses the SDE base chance ({invention_chance['skills_unknown_reason']}) — "
                    f"inventor skills would raise it, so the T2 cost is overstated."
                )
            if isinstance(invention_chance, dict) and invention_chance.get("note"):
                # Informational (A3): the chance is exact, confidence unchanged.
                reasons.append(f"Invention chance: {invention_chance['note']}.")
            for nested_note in manufacturing_job.get("invention_chance_notes") or []:
                reasons.append(f"Invention chance: {nested_note}.")
            for lane_note in manufacturing_job.get("elapsed_lane_notes") or []:
                # Informational (Task 12): the build time's parallel lanes.
                reasons.append(f"Build time: {lane_note}.")
            for later_batch_note in manufacturing_job.get("later_batch_notes") or []:
                # Informational: the shopping list may over-buy, never under-buy.
                reasons.append(f"Shopping list: {later_batch_note}.")

            overall_confidence_map = {"High": 2, "Medium": 1, "Low": 0}
            overall_score = min(
                overall_confidence_map.get(confidence, 0),
                overall_confidence_map.get(material_confidence, 0),
            )
            overall_confidence = {2: "High", 1: "Medium"}.get(overall_score, "Low")

            manufacturing_job["material_pricing_confidence"] = material_confidence
            manufacturing_job["overall_pricing_confidence"] = overall_confidence
            manufacturing_job["unknown_cost_basis_material_count"] = unknown_cost_basis_count
            row["material_pricing_confidence"] = material_confidence
            row["overall_pricing_confidence"] = overall_confidence
            row["unknown_cost_basis_material_count"] = unknown_cost_basis_count

        return product_rows

    def _enrich_product_rows_with_profit_metrics(self, product_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue

            net_proceeds = self._as_float(row.get("net_proceeds"))
            total_cost = self._as_float(manufacturing_job.get("total_cost"))
            time_seconds = self._as_float(manufacturing_job.get("time_seconds"))

            profit_amount = None
            margin_fraction = None
            isk_per_hour = None

            if net_proceeds is not None and total_cost is not None:
                profit_amount = float(net_proceeds) - float(total_cost)
                if net_proceeds > 0:
                    margin_fraction = float(profit_amount) / float(net_proceeds)

            if profit_amount is not None and time_seconds is not None and time_seconds > 0:
                isk_per_hour = float(profit_amount) / (float(time_seconds) / 3600.0)

            vol_7d_avg = self._as_float(row.get("region_daily_volume_7d_avg")) or 0
            runs_val = int(manufacturing_job.get("runs") or 1)
            qty_per_run_val = int(manufacturing_job.get("product_quantity_per_run") or 1)
            total_units_val = runs_val * qty_per_run_val
            if total_units_val > 0 and vol_7d_avg > 0:
                selling_time_seconds = (total_units_val / vol_7d_avg) * 86400.0
            else:
                selling_time_seconds = 0.0
            total_time_seconds = (time_seconds or 0) + selling_time_seconds
            if profit_amount is not None and total_time_seconds > 0:
                isk_per_hour_effective: float | None = profit_amount / (total_time_seconds / 3600.0)
            else:
                isk_per_hour_effective = None

            # Task 15 fix round 1 (I2): a cost that leaves unknown job fees out
            # makes profit, margin and ISK/h upper bounds (orow.get_profit_upper_bound_reason).
            upper_bound = bool(manufacturing_job.get("job_fee_unknown_reasons"))
            manufacturing_job["profit_is_upper_bound"] = upper_bound
            row["profit_is_upper_bound"] = upper_bound
            manufacturing_job["profit_amount"] = profit_amount
            manufacturing_job["profit_margin_fraction"] = margin_fraction
            manufacturing_job["isk_per_hour"] = isk_per_hour
            manufacturing_job["isk_per_hour_effective"] = isk_per_hour_effective

            row["profit_amount"] = profit_amount
            row["profit_margin_fraction"] = margin_fraction
            row["isk_per_hour"] = isk_per_hour
            row["isk_per_hour_effective"] = isk_per_hour_effective

        return product_rows

    def _enrich_product_rows_with_manufacturing_signals(
        self,
        product_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        for row in product_rows:
            if not isinstance(row, dict):
                continue
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                manufacturing_job = {}

            profit_amount = self._as_float(row.get("profit_amount"))
            total_cost = self._as_float(manufacturing_job.get("total_cost"))
            time_seconds = self._as_float(manufacturing_job.get("time_seconds"))
            preparation_time_seconds = self._as_float(manufacturing_job.get("preparation_time_seconds"))
            profit_margin_fraction = self._as_float(row.get("profit_margin_fraction"))
            days_of_supply = self._as_float(row.get("days_of_supply"))
            blueprint_source_kind = str(manufacturing_job.get("blueprint_source_kind") or row.get("blueprint_source_kind") or "")
            quantity = max(1, int(row.get("quantity") or 1))

            # Return on capital: profit / total_cost
            return_on_capital: float | None = None
            if profit_amount is not None and total_cost is not None and total_cost > 0:
                return_on_capital = profit_amount / total_cost

            # Manufacture window: will the hub sell through before the build finishes?
            manufacture_window_ok: bool | None = None
            elapsed_time_seconds = orow.get_elapsed_time_seconds(row)  # whole jobs, not amortized slot time
            if days_of_supply is not None and elapsed_time_seconds is not None and elapsed_time_seconds > 0:
                build_time_days = elapsed_time_seconds / 86400.0
                manufacture_window_ok = days_of_supply >= build_time_days

            # Blueprint ME level
            blueprint_me: int | None = None
            raw_me = manufacturing_job.get("blueprint_material_efficiency")
            if raw_me is not None:
                try:
                    blueprint_me = int(raw_me)
                except (TypeError, ValueError):
                    pass

            # Prep time fraction
            prep_time_fraction_pct: float | None = None
            if time_seconds is not None and time_seconds > 0:
                prep_secs = float(preparation_time_seconds or 0.0)
                prep_time_fraction_pct = (prep_secs / time_seconds) * 100.0

            # Fragile margin: profitable but margin < 5 %
            fragile_margin = (
                profit_margin_fraction is not None
                and profit_margin_fraction > 0
                and profit_margin_fraction < 0.05
            )

            # Blueprint SDE fallback: theoretical row, no owned blueprint
            blueprint_sde_fallback = blueprint_source_kind == "blueprint_sde_fallback"

            # Material contention: competing with higher-ranked builds for same owned stock
            material_contention = not bool(row.get("inventory_allocation_optimal", True))

            # Manufacturing cost index from activity breakdown
            manufacturing_cost_index: float | None = None
            activity_breakdown = manufacturing_job.get("activity_breakdown") or {}
            if isinstance(activity_breakdown, dict):
                mfg_activity = activity_breakdown.get("manufacturing") or {}
                if isinstance(mfg_activity, dict):
                    raw_ci = mfg_activity.get("cost_index")
                    if raw_ci is not None:
                        try:
                            manufacturing_cost_index = float(raw_ci)
                        except (TypeError, ValueError):
                            pass

            row["return_on_capital"] = return_on_capital
            row["manufacture_window_ok"] = manufacture_window_ok
            row["blueprint_me"] = blueprint_me
            row["prep_time_fraction_pct"] = prep_time_fraction_pct
            row["fragile_margin"] = fragile_margin
            row["blueprint_sde_fallback"] = blueprint_sde_fallback
            row["material_contention"] = material_contention
            row["manufacturing_cost_index"] = manufacturing_cost_index

        return product_rows

    @staticmethod
    def _format_timestamp(value: Any) -> str | None:
        try:
            timestamp = float(value or 0.0)
        except (TypeError, ValueError):
            return None
        if timestamp <= 0:
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

    def _get_adjusted_market_price_map(self) -> dict[int, dict[str, Any]]:
        now = time.time()
        cache = getattr(self._state, "_industry_adjusted_market_price_cache", None)
        if cache and isinstance(cache, tuple) and len(cache) == 2 and (now - float(cache[0]) < 86400):
            return cache[1]

        if self._state.esi_service is None:
            return {}

        price_rows = self._state.esi_service.get_market_prices()
        price_map: dict[int, dict[str, Any]] = {}
        for row in price_rows or []:
            if not isinstance(row, dict):
                continue
            try:
                type_id = int(row.get("type_id") or 0)
            except (TypeError, ValueError):
                continue
            if type_id <= 0:
                continue
            price_map[type_id] = {
                "adjusted_price": row.get("adjusted_price"),
                "average_price": row.get("average_price"),
            }

        self._state._industry_adjusted_market_price_cache = (now, price_map)
        return price_map

    @staticmethod
    def _resolve_eiv_pricing(
        *,
        type_id: int,
        type_payload: dict[str, Any] | None,
        adjusted_price_map: dict[int, dict[str, Any]],
        unknown_reasons: list[str] | None = None,
    ) -> tuple[float | None, str | None]:
        """(unit EIV, "esi_adjusted_price"): EVE's estimated item value is the
        ESI adjusted price. Without a readable one it is unknown, (None,
        None), its reason appended to `unknown_reasons` when given: no
        average or SDE base price stands in (that would price the job fee on
        another basis and pass as exact). `type_payload` is not read."""
        pricing = adjusted_price_map.get(int(type_id)) or {}
        adjusted_price = pricing.get("adjusted_price")
        value: float | None = None
        if adjusted_price is not None and not isinstance(adjusted_price, bool):
            try:
                value = float(adjusted_price)
            except (TypeError, ValueError):
                value = None
        if value is not None and math.isfinite(value):
            return value, "esi_adjusted_price"
        reason = (
            f"type_id={type_id} has no ESI adjusted price: its estimated item value is unknown"
            if adjusted_price is None
            else f"the ESI adjusted price {adjusted_price!r} of type_id={type_id} is not a number: its estimated "
                 "item value is unknown"
        )
        if unknown_reasons is not None:
            unknown_reasons.append(reason)
        return None, None

    @classmethod
    def _reference_unit_price(
        cls, *, type_id: int, type_payload: dict[str, Any] | None, adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, str | None]:
        """A labelled reference value for an item (not an EIV): the ESI
        adjusted, else average, else the SDE base price, with its source."""
        pricing = adjusted_price_map.get(int(type_id)) or {}
        for value, source in (
            (pricing.get("adjusted_price"), "esi_adjusted_price"),
            (pricing.get("average_price"), "esi_average_price"),
            ((type_payload or {}).get("base_price") if isinstance(type_payload, dict) else None, "sde_base_price"),
        ):
            number = cls._as_float(value)
            if number is not None:
                return number, source
        return None, None

    @classmethod
    def _sum_estimated_item_value(
        cls,
        entries: list[dict[str, Any]],
        *,
        quantity_key: str,
        adjusted_price_map: dict[int, dict[str, Any]],
        unknown_reasons: list[str] | None = None,
    ) -> tuple[float | None, int]:
        """(EIV, priced line count). The EIV is None when no line, or ANY line,
        lacks a price or has a non-numeric quantity (fix round 1, F1): a
        partial sum would price a job fee low and pass as exact, so the fee
        reads unknown instead."""
        total, priced_count, _line_count = cls._sum_estimated_item_value_counted(
            entries, quantity_key=quantity_key, adjusted_price_map=adjusted_price_map,
            unknown_reasons=unknown_reasons,
        )
        return total, priced_count

    @classmethod
    def _sum_estimated_item_value_counted(
        cls,
        entries: list[dict[str, Any]],
        *,
        quantity_key: str,
        adjusted_price_map: dict[int, dict[str, Any]],
        unknown_reasons: list[str] | None = None,
    ) -> tuple[float | None, int, int]:
        """_sum_estimated_item_value plus the number of lines it counted
        (priced or not): the M of "N of M priced". A non-numeric quantity
        makes the EIV unknown; its reason is appended to `unknown_reasons`
        (when given), for the job fee's fee_unknown_reason."""
        total = 0.0
        priced_count = 0
        line_count = 0
        unpriced = False
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                type_id = int(entry.get("type_id") or 0)
            except (TypeError, ValueError):
                type_id = 0
            if type_id <= 0:
                continue
            line_count += 1
            unit_value, _ = cls._resolve_eiv_pricing(
                type_id=type_id,
                type_payload=entry,
                adjusted_price_map=adjusted_price_map,
                unknown_reasons=unknown_reasons,
            )
            if unit_value is None:
                unpriced = True
                continue
            try:
                quantity = int(entry.get(quantity_key) or 0)
            except (TypeError, ValueError):
                reason = f"the {quantity_key} {entry.get(quantity_key)!r} of type_id={type_id} is not a number"
                logger.warning("EIV: %s; the EIV and the job fee are unknown", reason)
                if unknown_reasons is not None:
                    unknown_reasons.append(reason)
                unpriced = True
                continue
            total += float(unit_value) * quantity
            priced_count += 1
        if priced_count == 0:
            return None, 0, line_count
        if unpriced:
            return None, priced_count, line_count
        return total, priced_count, line_count

    @classmethod
    def _resolve_preferred_unit_value(
        cls,
        *,
        type_id: int,
        type_payload: dict[str, Any] | None,
        sell_price_map: dict[int, dict[str, Any]] | None,
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, str | None]:
        payload = type_payload if isinstance(type_payload, dict) else {}

        explicit_unit_price = cls._as_float(payload.get("unit_price"))
        if explicit_unit_price is not None and explicit_unit_price > 0:
            return explicit_unit_price, str(payload.get("price_source") or "explicit_unit_price")

        acquisition_unit_cost = cls._as_float(payload.get("acquisition_unit_cost"))
        if acquisition_unit_cost is not None and acquisition_unit_cost > 0:
            return acquisition_unit_cost, "owned_asset_acquisition_cost"

        sell_pricing = (sell_price_map or {}).get(int(type_id)) or {}
        sell_unit_price = cls._as_float(sell_pricing.get("unit_price"))
        if sell_unit_price is not None and sell_unit_price > 0:
            return sell_unit_price, str(sell_pricing.get("price_source") or "market_sell_price")

        average_price = cls._as_float(payload.get("type_average_price"))
        if average_price is not None and average_price > 0:
            return average_price, "asset_average_price"

        adjusted_price = cls._as_float(payload.get("type_adjusted_price"))
        if adjusted_price is not None and adjusted_price > 0:
            return adjusted_price, "asset_adjusted_price"

        return cls._reference_unit_price(
            type_id=type_id,
            type_payload=payload,
            adjusted_price_map=adjusted_price_map,
        )

    @classmethod
    def _sum_preferred_item_value(
        cls,
        entries: list[dict[str, Any]],
        *,
        quantity_key: str,
        sell_price_map: dict[int, dict[str, Any]] | None,
        adjusted_price_map: dict[int, dict[str, Any]],
    ) -> tuple[float | None, int]:
        total = 0.0
        priced_count = 0
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                type_id = int(entry.get("type_id") or 0)
            except (TypeError, ValueError):
                type_id = 0
            if type_id <= 0:
                continue
            unit_value, _ = cls._resolve_preferred_unit_value(
                type_id=type_id,
                type_payload=entry,
                sell_price_map=sell_price_map,
                adjusted_price_map=adjusted_price_map,
            )
            if unit_value is None:
                continue
            try:
                quantity = int(entry.get(quantity_key) or 0)
            except (TypeError, ValueError):
                logger.warning("Item value: type_id=%s has a non-numeric %s %r; the line is left out of the value",
                               type_id, quantity_key, entry.get(quantity_key))
                quantity = 0
            if quantity <= 0:
                continue
            total += float(unit_value) * float(quantity)
            priced_count += 1
        if priced_count == 0:
            return None, 0
        return total, priced_count

    @staticmethod
    def _calculate_days_of_supply(hub_sell_liquidity: int | None, region_daily_volume_7d_avg: float | None) -> float | None:
        """Calculate Days of Supply (DOS) = Hub Sell Liquidity / Region Daily Volume (7d avg).
        None when either is unknown."""
        try:
            if hub_sell_liquidity is None:
                return None
            if region_daily_volume_7d_avg is None or float(region_daily_volume_7d_avg) <= 0:
                return None
            return float(hub_sell_liquidity) / float(region_daily_volume_7d_avg)
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    @staticmethod
    def _calculate_sell_through_rate(region_daily_volume_7d_avg: float | None, hub_sell_liquidity: int) -> float | None:
        """Calculate Sell-Through Rate (%) = (Region Daily Volume 7d avg / Hub Sell Liquidity) * 100"""
        try:
            if region_daily_volume_7d_avg is None or hub_sell_liquidity is None or hub_sell_liquidity <= 0:
                return None
            return (float(region_daily_volume_7d_avg) / float(hub_sell_liquidity)) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    @staticmethod
    def _calculate_liquidity_indicator(hub_sell_liquidity: int | None, region_daily_volume_7d_avg: float | None, hub_sell_orders: int | None) -> str:
        """Calculate liquidity indicator based on DOS ranges: Very High, High, Medium, Low, Very Low;
        Unknown when the hub liquidity or the region volume is unknown."""
        try:
            if hub_sell_liquidity is None:
                return "Unknown"
            if region_daily_volume_7d_avg is None or float(region_daily_volume_7d_avg) <= 0:
                return "Unknown"
            dos = float(hub_sell_liquidity) / float(region_daily_volume_7d_avg)

            if dos < 1:
                return "Very High"
            elif dos < 3:
                return "High"
            elif dos < 7:
                return "Medium"
            elif dos < 30:
                return "Low"
            else:
                return "Very Low"
        except (TypeError, ValueError, ZeroDivisionError):
            return "Unknown"

    @staticmethod
    def _calculate_liquidity_score(hub_sell_liquidity: int | None, region_daily_volume_7d_avg: float | None, hub_sell_orders: int | None) -> float | None:
        """Calculate combined liquidity score (0-100) based on DOS and order count;
        None when the hub's sell book could not be fetched or the region volume
        is unknown or unreadable (unknown, not 0; the row's
        region_daily_volume_unavailable_reason says why). A known zero volume
        (nothing sold) scores 0."""
        try:
            if hub_sell_liquidity is None or hub_sell_orders is None:
                return None
            if region_daily_volume_7d_avg is None:
                return None
            if float(region_daily_volume_7d_avg) <= 0:
                return 0.0

            dos = float(hub_sell_liquidity) / float(region_daily_volume_7d_avg)

            # DOS score: lower DOS is better (1 day = 100, 30 days = 0, linear scale)
            dos_score = max(0.0, min(100.0, 100.0 - (dos / 30.0 * 100.0)))

            # Order count score: more orders is better (log scale, 100 orders = 100)
            order_count_score = min(100.0, (float(hub_sell_orders) / 100.0) * 100.0) if hub_sell_orders > 0 else 0.0

            # Combined score: 70% DOS, 30% order count
            liquidity_score = (dos_score * 0.7) + (order_count_score * 0.3)
            return round(liquidity_score, 2)
        except (TypeError, ValueError):
            return None  # an unreadable input: unknown, not 0

    _SALES_TAX_UNKNOWN_REASON = "sales tax unknown (no character to read Accounting from)"
    _BROKER_FEE_UNKNOWN_REASON = "broker fee unknown (no character to read Broker Relations and standings from)"

    @classmethod
    def _sale_fee_amounts(
        cls, gross_sale_value: float, fee_context: dict[str, Any] | None,
    ) -> tuple[float | None, float | None, float | None, str | None]:
        """(broker fee, sales tax, net proceeds, why net is unknown) of a
        gross sale: the one sale-proceeds formula (the rows' net_proceeds and
        the decryptor comparison's _net_sale_fraction).

        net = gross - broker fee (when it applies: a sell order) - sales tax.
        An unknown sales tax, or an unknown broker fee that applies, leaves
        net unknown with the reason: never priced at 0. A broker fee that
        does not apply is 0 whether or not its rate is known; the broker fee
        amount is shown only when its rate is known.

        What the fee context cannot tell: an Accounting / Broker Relations
        skill or a standing it does not find reads as 0, the highest tax and
        fee, so profit is understated, never overstated."""
        context = fee_context or {}
        rates = context.get("rates") or {}
        tax = cls._as_float(rates.get("sales_tax_fraction"))
        broker = cls._as_float(rates.get("broker_fee_fraction"))
        applies = bool(context.get("broker_fee_applies"))
        gross = float(gross_sale_value)
        broker_amount = (gross * float(broker) if applies else 0.0) if broker is not None else None
        tax_amount = gross * float(tax) if tax is not None else None
        if tax_amount is None:
            return broker_amount, None, None, cls._SALES_TAX_UNKNOWN_REASON
        if applies and broker_amount is None:
            return None, tax_amount, None, cls._BROKER_FEE_UNKNOWN_REASON
        return broker_amount, tax_amount, gross - float(broker_amount or 0.0) - tax_amount, None

    @classmethod
    def _net_sale_fraction(cls, fee_context: dict[str, Any]) -> float | None:
        """The share of the gross sale value the corp keeps, as
        _enrich_product_rows_with_sale_proceeds computes net_proceeds
        (_sale_fee_amounts); None when net proceeds are unknown."""
        return cls._sale_fee_amounts(1.0, fee_context)[2]

    def _enrich_product_rows_with_material_prices(
        self,
        product_rows: list[dict[str, Any]],
        *,
        market_hub: str = "jita",
        material_price_side: str = "sell",
        progress_callback: ProgressCallback | None = None,
    ) -> list[dict[str, Any]]:
        if not product_rows:
            return product_rows

        material_type_ids = sorted(
            {
                int(material.get("type_id") or 0)
                for row in product_rows
                for material in list(
                    ((((row.get("manufacturing_job") or {}).get("procurement_materials") or {}) or ((row.get("manufacturing_job") or {}).get("materials") or {})).values())
                )
                if isinstance(material, dict) and int(material.get("type_id") or 0) > 0
            }
            | {
                int(material.get("type_id") or 0)
                for row in product_rows
                for material in list((((row.get("manufacturing_job") or {}).get("expected_invention_materials") or {})).values())
                if isinstance(material, dict) and int(material.get("type_id") or 0) > 0
            }
        )
        if progress_callback is not None:
            progress_callback(
                0.83,
                "Enriching material prices",
                {"material_type_count": len(material_type_ids)},
            )
        if not material_type_ids:
            return product_rows

        pricing_service = MarketPricingService(state=self._state, sessions=self._sessions)
        price_by_type_id = pricing_service.get_type_price_map(
            type_ids=material_type_ids,
            hub=market_hub,
            side=material_price_side,
            progress_callback=(
                None
                if progress_callback is None
                else lambda progress_fraction, progress_label, progress_meta=None: progress_callback(
                    0.83 + (0.07 * progress_fraction),
                    progress_label,
                    progress_meta,
                )
            ),
        )

        for row in product_rows:
            manufacturing_job = row.get("manufacturing_job") or {}
            if not isinstance(manufacturing_job, dict):
                continue
            materials = manufacturing_job.get("materials") or {}
            if not isinstance(materials, dict):
                continue
            procurement_materials = manufacturing_job.get("procurement_materials") or materials
            if not isinstance(procurement_materials, dict):
                procurement_materials = materials
            material_cost = 0.0
            certain_material_cost = 0.0
            estimated_material_cost = 0.0
            priced_material_count = 0
            for material in procurement_materials.values():
                if not isinstance(material, dict):
                    continue
                type_id = int(material.get("type_id") or 0)
                explicit_unit_price = self._as_float(material.get("unit_price"))
                if explicit_unit_price is not None and explicit_unit_price > 0:
                    unit_price = explicit_unit_price
                else:
                    pricing = price_by_type_id.get(type_id) or {}
                    unit_price = pricing.get("unit_price")
                    material["unit_price"] = unit_price
                    material["price_source"] = pricing.get("price_source")
                    material["price_volume_total"] = pricing.get("volume_total")
                    material["market_hub"] = pricing.get("hub")
                    material["market_hub_label"] = pricing.get("hub_label")
                    material["price_sample_size"] = pricing.get("sample_size")
                    material["price_cached"] = pricing.get("cached")
                    material["price_fetched_at"] = pricing.get("fetched_at")
                line_total = None
                if unit_price is not None:
                    line_total = float(unit_price) * int(material.get("quantity") or 0)
                    material_cost += float(line_total)
                    priced_material_count += 1
                    price_source = str(material.get("price_source") or "").lower()
                    is_certain = (
                        price_source.startswith("owned_asset_item_value")
                        or price_source.startswith("market_")
                        or price_source.startswith("explicit")
                    )
                    if is_certain:
                        certain_material_cost += float(line_total)
                    else:
                        estimated_material_cost += float(line_total)
                material["line_total"] = line_total
            # Invented T2: the expected invention inputs (fractional, amortized
            # per run; see _expected_invention_inputs) are part of the cost and
            # count as material lines for the priced/type counts.
            invention_materials = manufacturing_job.get("expected_invention_materials")
            invention_line_count = 0
            invention_priced_count = 0
            if isinstance(invention_materials, dict) and invention_materials:
                invention_material_cost = 0.0
                for material in invention_materials.values():
                    if not isinstance(material, dict):
                        continue
                    invention_line_count += 1
                    type_id = int(material.get("type_id") or 0)
                    unit_price = self._as_float(material.get("unit_price"))
                    if unit_price is None or unit_price <= 0:
                        pricing = price_by_type_id.get(type_id) or {}
                        unit_price = self._as_float(pricing.get("unit_price"))
                        material["unit_price"] = unit_price
                        material["price_source"] = pricing.get("price_source")
                    line_total = None
                    if unit_price is not None:
                        line_total = float(unit_price) * float(material.get("quantity") or 0.0)
                        invention_material_cost += line_total
                        invention_priced_count += 1
                        price_source = str(material.get("price_source") or "").lower()
                        if price_source.startswith(("owned_asset_item_value", "market_", "explicit")):
                            certain_material_cost += line_total
                        else:
                            estimated_material_cost += line_total
                    else:
                        logger.warning(
                            "Product overview %s: invention input %s (%s) has no price; "
                            "it is left out of material_cost",
                            row.get("type_id"), type_id, material.get("type_name"),
                        )
                    material["line_total"] = line_total
                manufacturing_job["invention_material_cost"] = invention_material_cost if invention_priced_count > 0 else None
                material_cost += invention_material_cost
            any_priced = (priced_material_count + invention_priced_count) > 0
            manufacturing_job["material_cost"] = material_cost if any_priced else None
            manufacturing_job["certain_material_cost"] = certain_material_cost if any_priced else None
            manufacturing_job["estimated_material_cost"] = estimated_material_cost if any_priced else None
            manufacturing_job["priced_material_count"] = priced_material_count + invention_priced_count
            manufacturing_job["material_type_count"] = len(procurement_materials) + invention_line_count
            total_job_cost = manufacturing_job.get("total_job_cost")
            # I1: with a fee unknown, total_job_cost / total_cost hold the
            # priced parts only. Kept (not None) so the row still ranks, but
            # flagged: the UI marks it and the confidence is capped.
            manufacturing_job["total_cost_excludes_unknown_job_fees"] = bool(
                manufacturing_job.get("job_fee_unknown_reasons")
            )
            if total_job_cost is not None or any_priced:
                manufacturing_job["total_cost"] = float(total_job_cost or 0.0) + float(material_cost or 0.0)
                manufacturing_job["certain_cost"] = float(total_job_cost or 0.0) + float(certain_material_cost or 0.0)
                manufacturing_job["estimated_cost"] = float(estimated_material_cost or 0.0)
            else:
                manufacturing_job["total_cost"] = None
                manufacturing_job["certain_cost"] = None
                manufacturing_job["estimated_cost"] = None

            job_tree = manufacturing_job.get("job_tree") or {}
            if isinstance(job_tree, dict) and job_tree:
                self._apply_material_pricing_to_job_tree(job_tree, price_by_type_id=price_by_type_id)

        if progress_callback is not None:
            progress_callback(0.90, "Applied material prices to manufacturing rows", {"rows": len(product_rows)})

        return product_rows
