"""Reading the numbers of ESI market payloads (order books, orders, history).

ESI fields can be missing, null or (in cached payloads) strings. None never
reaches float(): readable_number checks the value explicitly and gives None
for anything that is not a finite number, so the caller makes it unknown
with a reason, never a silent 0.
"""
from __future__ import annotations

import logging
import math
from typing import Any, Iterable

NA_STATUS = "⚪N/A"


def readable_number(value: Any) -> float | None:
    """`value` as a finite float; None for None, a bool, a non-numeric string,
    NaN/infinity or any other type."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    return number if math.isfinite(number) else None


def readable_int(value: Any) -> int | None:
    """`value` as an int (a readable number with no fraction); None otherwise."""
    number = readable_number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def order_price_or_reason(order: dict[str, Any], *, label: str) -> tuple[float | None, str | None]:
    """(price, None) of a market order, or (None, price_unknown_reason) with a
    WARNING when its price cannot be read: unknown, never counted as 0 ISK."""
    raw = order.get("price")
    price = readable_number(raw)
    if price is not None:
        return price, None
    logging.warning(
        "%s %s has no readable price (%r): price unknown, no book comparison or pricing suggestion",
        label,
        order.get("order_id"),
        raw,
    )
    return None, f"order price unreadable ({raw!r}): no book comparison or pricing suggestion"


def order_total_price(order_price: float | None, order: dict[str, Any]) -> float | None:
    """price x volume_remain; None (unknown) when either cannot be read."""
    volume = readable_number(order.get("volume_remain"))
    if order_price is None or volume is None:
        return None
    return order_price * volume


def refresh_market_history(market_history_svc: Any, type_ids: Iterable[int], *, label: str) -> dict[int, str]:
    """Fetch and store the market history of each type. One failed type is a
    WARNING and the loop goes on with the next (the pricing suggestions then use
    the history already stored for it). Returns why each failed type was not
    refreshed, {type_id: reason}. A stored invalid admin setting and a
    programming error propagate."""
    from sqlalchemy.exc import SQLAlchemyError

    from eve_online_industry_tracker.config.admin_settings import AdminSettingInvalid
    from eve_online_industry_tracker.infrastructure.esi_client import ESI_REQUEST_ERRORS

    unavailable: dict[int, str] = {}
    for type_id in sorted(type_ids):
        try:
            reason = market_history_svc.fetch_and_store_history(type_id=type_id, region_id=10000002)
        except AdminSettingInvalid:
            raise  # a stored invalid setting fails loudly, naming it
        except ESI_REQUEST_ERRORS + (SQLAlchemyError, ValueError) as exc:
            # A malformed value skips this type only.
            reason = f"market history refresh failed: {exc}"
            logging.warning("%s: market history refresh of type %s failed, its stored history is used: %s",
                            label, type_id, exc)
        if reason:
            unavailable[int(type_id)] = str(reason)
    return unavailable


def book_comparison(
    *,
    order_price: float | None,
    book: Iterable[Any] | None,
    is_buy_order: bool,
) -> tuple[str, float | None]:
    """(price status, price difference) of an order against the order book.

    A buy order compares with the highest readable book price ("Best price"
    when above it), a sell order with the lowest ("Undercut" when above it).
    Book rows without a readable price are skipped. No readable book price:
    "N/A" and a difference of 0 (nothing to compare). An unknown order price:
    "N/A" and a difference of None (unknown, not 0)."""
    if order_price is None:
        return NA_STATUS, None
    prices = [
        price
        for price in (readable_number(row.get("price")) for row in (book or []) if isinstance(row, dict))
        if price is not None
    ]
    if not prices:
        return NA_STATUS, 0.0
    if is_buy_order:
        difference = order_price - max(prices)
        return ("🟢Best price" if difference > 0 else "🔴Undercut"), difference
    difference = order_price - min(prices)
    return ("🔴Undercut" if difference > 0 else "🟢Best price"), difference
