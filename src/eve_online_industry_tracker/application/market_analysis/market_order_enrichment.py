"""The market-order enrichment shared by the character and corporation
market-order pages (CharactersService / CorporationsService
.get_market_orders_enriched): the order books to compare with, the expiry,
the price status, the pricing suggestion and the repricing priority score.
The callers differ only in who owns an order and which character's data
prices it, which they pass in."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Iterable

from sqlalchemy.exc import SQLAlchemyError

from eve_online_industry_tracker.application.market_pricing.order_values import (
    NA_STATUS,
    book_comparison,
    order_price_or_reason,
    order_total_price,
    readable_int,
    readable_number,
)
from eve_online_industry_tracker.config.admin_settings import AdminSettingInvalid
from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS

Book = dict[int, list[dict[str, Any]]]

#: The pricing suggestion's fields copied onto an enriched sell order, as (order key, suggestion key).
_ADVISED_FIELDS = (
    ("advised_price", "advised_price"),
    ("advised_price_confidence", "confidence"),
    ("pricing_breakdown", "breakdown"),
    ("pricing_reasoning", "reasoning"),
    ("cost_basis", "cost_basis"),
    ("acquisition_source", "acquisition_source"),
    ("cost_basis_source", "cost_basis_source"),
    ("price_difference_pct", "price_difference_pct"),
    # Profitability metrics
    ("hub_price", "hub_price"),
    ("break_even_price", "break_even_price"),
    ("net_margin_pct_advised", "net_margin_pct_advised"),
    ("net_margin_pct_current", "net_margin_pct_current"),
    ("estimated_sell_days_advised", "estimated_sell_days_advised"),
    ("estimated_sell_days_current", "estimated_sell_days_current"),
    ("isk_per_day_advised", "isk_per_day_advised"),
    ("isk_per_day_current", "isk_per_day_current"),
    ("hold_signal", "hold_signal"),
    ("relist_risk", "relist_risk"),
    ("price_band", "price_band"),
    ("min_target_margin_pct", "min_target_margin_pct"),
    ("fill_rate_velocity", "fill_rate_velocity"),
    ("seller_concentration", "seller_concentration"),
    ("expiry_urgency", "expiry_urgency"),
    ("expiry_urgency_detail", "expiry_urgency_detail"),
)


def order_type_ids(owners: Iterable[Any]) -> tuple[set[int], set[int]]:
    """(buy type ids, sell type ids) of the orders of each owner's
    "market_orders"."""
    buy_type_ids: set[int] = set()
    sell_type_ids: set[int] = set()
    for owner in owners:
        for order in (owner or {}).get("market_orders", []):
            type_id = order.get("type_id")
            if not isinstance(type_id, int):
                continue
            if order.get("is_buy_order"):
                buy_type_ids.add(type_id)
            else:
                sell_type_ids.add(type_id)
    return buy_type_ids, sell_type_ids


def fetch_order_books(esi_service: Any, buy_type_ids: set[int], sell_type_ids: set[int], *,
                      label: str) -> tuple[Book, Book]:
    """(buy order book, sell order book) of the types. A book that ESI cannot
    give is a WARNING and empty: the orders still show, their price status
    "N/A" (no book to compare with). A stored invalid setting and a bug
    propagate."""
    buy_order_book: Book = {}
    sell_order_book: Book = {}
    try:
        if buy_type_ids:
            buy_order_book = esi_service.get_type_buyprices(sorted(buy_type_ids))
    except AdminSettingInvalid:
        raise  # a stored invalid setting fails loudly, naming it
    except ESI_REQUEST_ERRORS as exc:
        logging.warning("%s: buy order book unavailable, no price comparison: %s", label, exc)
        buy_order_book = {}
    try:
        if sell_type_ids:
            sell_order_book = esi_service.get_type_sellprices(sorted(sell_type_ids))
    except AdminSettingInvalid:
        raise  # a stored invalid setting fails loudly, naming it
    except ESI_REQUEST_ERRORS as exc:
        logging.warning("%s: sell order book unavailable, no price comparison: %s", label, exc)
        sell_order_book = {}
    return buy_order_book, sell_order_book


def _expiry(order: dict[str, Any], now: datetime) -> tuple[timedelta, str, int]:
    """(time left, its "Nd Nh Nm" text or "Expired", the duration in days; 0
    when unreadable). An unreadable issue date counts as now."""
    try:
        issued_dt = datetime.fromisoformat(str(order.get("issued")).replace("Z", "+00:00"))
    except ValueError:
        issued_dt = now
    try:
        duration_days_int = int(order.get("duration") or 0)
    except (TypeError, ValueError):
        duration_days_int = 0
    expires_in_td = issued_dt + timedelta(days=duration_days_int) - now
    if expires_in_td.total_seconds() > 0:
        days = expires_in_td.days
        hours = expires_in_td.seconds // 3600
        mins = (expires_in_td.seconds % 3600) // 60
        return expires_in_td, f"{days}d {hours}h {mins}m", duration_days_int
    return expires_in_td, "Expired", duration_days_int


def enrich_market_order(
    order: dict[str, Any],
    *,
    now: datetime,
    buy_order_book: Book,
    sell_order_book: Book,
    compare_with_books: bool,
    history_unavailable: dict[int, str],
    pricing_svc: Any,
    pricing_character_id: int | None,
    corporation_id: int | None,
    owner: Any,
    station: str,
    region_name: str | None,
    is_corporation: Any,
    price_label: str,
    pricing_failure_label: str,
) -> dict[str, Any]:
    """One enriched market order: its expiry, price status against the book
    (compare_with_books), and for a sell order with a known price and a
    pricing_character_id the pricing suggestion (its failure is a WARNING
    and `pricing_error` on the order; a stored invalid setting and a bug
    propagate). owner / station / region_name / is_corporation are the
    caller's."""
    expires_in_td, expires_in, duration_days_int = _expiry(order, now)

    type_id = order.get("type_id")
    order_price, price_unknown_reason = order_price_or_reason(order, label=price_label)
    price_difference: float | None = 0.0 if order_price is not None else None
    price_status = NA_STATUS
    if compare_with_books and isinstance(type_id, int):
        book = buy_order_book if order.get("is_buy_order") else sell_order_book
        price_status, price_difference = book_comparison(
            order_price=order_price,
            book=book.get(type_id, []) if isinstance(book, dict) else [],
            is_buy_order=bool(order.get("is_buy_order")),
        )

    advised_price_data = None
    pricing_error: str | None = None
    if (not order.get("is_buy_order") and isinstance(type_id, int) and pricing_character_id is not None
            and order_price is not None):   # no suggestion from an unknown price
        days_remaining_int: int | None = max(0, expires_in_td.days) if expires_in_td.total_seconds() > 0 else None
        try:
            advised_price_data = pricing_svc.suggest_price(
                character_id=pricing_character_id,
                type_id=type_id,
                current_price=order_price,
                quantity=int(readable_int(order.get("volume_remain")) or 0),
                order_duration_days=duration_days_int or 90,
                days_remaining=days_remaining_int,
                corporation_id=corporation_id,
            )
        except AdminSettingInvalid:
            raise  # a stored invalid setting fails loudly, naming it
        except (SQLAlchemyError, ValueError, ArithmeticError) as exc:
            logging.warning(
                "Pricing suggestion failed for %s %s (type_id=%s): %s",
                pricing_failure_label,
                order.get("order_id"),
                type_id,
                exc,
            )
            advised_price_data = None
            pricing_error = f"pricing suggestion unavailable: {exc}"

    enriched_order: dict[str, Any] = {
        "order_id": order.get("order_id"),
        "owner": owner,
        "type_id": type_id,
        "type_name": order.get("type_name"),
        "price": order.get("price") if order_price is not None else None,
        "price_unknown_reason": price_unknown_reason,
        "history_unavailable_reason": (
            history_unavailable.get(type_id)
            if not order.get("is_buy_order") and isinstance(type_id, int) else None
        ),
        "price_status": price_status,
        "price_difference": price_difference,
        "volume": str(order.get("volume_remain")) + "/" + str(order.get("volume_total")),
        "total_price": order_total_price(order_price, order),
        "range": order.get("range"),
        "min_volume": order.get("min_volume"),
        "expires_in": expires_in,
        "escrow_remaining": order.get("escrow", 0),
        "station": station,
        "region": region_name or (f"Region {order.get('region_id')}" if order.get("region_id") else "Unknown"),
        "is_buy_order": order.get("is_buy_order"),
        "is_corporation": is_corporation,
        "corporation_id": corporation_id,
        "type_group_id": order.get("type_group_id", -1),
        "type_group_name": order.get("type_group_name", "Unknown"),
        "type_category_id": order.get("type_category_id", -1),
        "type_category_name": order.get("type_category_name", "Unknown"),
        "is_blueprint_copy": False,
    }
    if pricing_error:
        enriched_order["pricing_error"] = pricing_error
    if advised_price_data:
        for order_key, suggestion_key in _ADVISED_FIELDS:
            enriched_order[order_key] = advised_price_data.get(suggestion_key)
    return enriched_order


def add_reprice_priority_scores(enriched_orders: list[dict[str, Any]]) -> None:
    """Set each order's reprice_priority_score (0 for a buy order or a sell
    order without an advised price)."""
    for order in enriched_orders:
        if order.get("is_buy_order") or not order.get("advised_price"):
            order["reprice_priority_score"] = 0.0
            continue
        score = 0.0
        # Critical: losing money after fees
        if (order.get("net_margin_pct_current") or 0) < 0:
            score += 100.0
        # ISK/day improvement opportunity
        adv_day = order.get("isk_per_day_advised") or 0
        cur_day = order.get("isk_per_day_current") or 0
        if adv_day > cur_day and adv_day > 0:
            daily_gain_m = (adv_day - cur_day) / 1_000_000
            score += min(50.0, daily_gain_m * 5)
        # Expiry urgency
        if order.get("expiry_urgency"):
            score += 40.0
        # Relist risk
        if (order.get("relist_risk") or {}).get("at_risk"):
            score += 25.0
        # Being undercut at current price
        price = readable_number(order.get("price")) or 0.0
        advised = readable_number(order.get("advised_price")) or price
        if price > 0 and advised < price:
            undercut_pct = (price - advised) / price * 100
            score += min(20.0, undercut_pct * 2)
        order["reprice_priority_score"] = round(score, 1)
