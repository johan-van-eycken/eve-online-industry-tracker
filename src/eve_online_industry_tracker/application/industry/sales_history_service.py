from __future__ import annotations

import statistics
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, desc
from eve_online_industry_tracker.infrastructure.models import CharacterWalletTransactionsModel
from eve_online_industry_tracker.infrastructure.session_provider import SessionProvider, StateSessionProvider


class SalesHistoryService:
    def __init__(self, *, state: Any, sessions: SessionProvider | None = None):
        self._state = state
        self._sessions = sessions or StateSessionProvider(state=state)

    def get_sold_history(
        self,
        *,
        character_id: int,
        type_id: int,
        days: int = 30,
    ) -> list[dict[str, Any]]:
        """Get list of historical sales for a character and item type.

        Args:
            character_id: The character's ID.
            type_id: The item type ID.
            days: Number of days to look back (default 30).

        Returns:
            List of transactions sorted by date (newest first), each with:
            - date: Transaction date
            - quantity: Quantity sold
            - unit_price: Price per unit
            - total_price: Total ISK for transaction
        """
        app_session = self._sessions.app_session()
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            cutoff_str = cutoff_date.isoformat()

            transactions = app_session.query(CharacterWalletTransactionsModel).filter(
                and_(
                    CharacterWalletTransactionsModel.character_id == character_id,
                    CharacterWalletTransactionsModel.type_id == type_id,
                    CharacterWalletTransactionsModel.is_buy == False,
                    CharacterWalletTransactionsModel.date >= cutoff_str,
                )
            ).order_by(desc(CharacterWalletTransactionsModel.date)).all()

            result = [
                {
                    "date": str(tx.date) if tx.date else None,
                    "quantity": int(tx.quantity or 0),
                    "unit_price": float(tx.unit_price or 0.0),
                    "total_price": float(tx.total_price or 0.0),
                }
                for tx in transactions
            ]
            return result
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def suggest_sell_price(
        self,
        *,
        character_id: int,
        type_id: int,
        days: int = 30,
    ) -> dict[str, Any]:
        """Suggest a sell price based on character's recent sales history.

        Args:
            character_id: The character's ID.
            type_id: The item type ID.
            days: Number of days to look back (default 30).

        Returns:
            Dict with:
            - suggested_price: Recommended sell price (median of recent sales)
            - confidence: "high" (5+ sales) / "medium" (2-4 sales) / "low" (1 sale) / "none" (no data)
            - sample_size: Number of sales used
            - price_range: {"min": lowest, "max": highest} if data exists
        """
        history = self.get_sold_history(character_id=character_id, type_id=type_id, days=days)

        if not history:
            return {
                "suggested_price": None,
                "confidence": "none",
                "sample_size": 0,
                "price_range": None,
            }

        prices = [tx["unit_price"] for tx in history if tx["unit_price"] > 0]
        if not prices:
            return {
                "suggested_price": None,
                "confidence": "none",
                "sample_size": 0,
                "price_range": None,
            }

        suggested_price = float(statistics.median(prices))
        sample_size = len(prices)

        if sample_size >= 5:
            confidence = "high"
        elif sample_size >= 2:
            confidence = "medium"
        else:
            confidence = "low"

        return {
            "suggested_price": suggested_price,
            "confidence": confidence,
            "sample_size": sample_size,
            "price_range": {
                "min": float(min(prices)),
                "max": float(max(prices)),
            },
        }

    def get_volume_history(
        self,
        *,
        character_id: int,
        type_id: int,
        days: int = 30,
    ) -> dict[str, Any]:
        """Get aggregate sales volume for a character and item type.

        Args:
            character_id: The character's ID.
            type_id: The item type ID.
            days: Number of days to look back (default 30).

        Returns:
            Dict with:
            - total_quantity_sold: Sum of all units sold
            - transaction_count: Number of sale transactions
            - date_range: {"start": earliest, "end": latest} if data exists
        """
        history = self.get_sold_history(character_id=character_id, type_id=type_id, days=days)

        if not history:
            return {
                "total_quantity_sold": 0,
                "transaction_count": 0,
                "date_range": None,
            }

        total_quantity = sum(tx["quantity"] for tx in history)
        transaction_count = len(history)
        dates = [tx["date"] for tx in history if tx["date"]]

        return {
            "total_quantity_sold": int(total_quantity),
            "transaction_count": int(transaction_count),
            "date_range": {
                "start": dates[-1] if dates else None,
                "end": dates[0] if dates else None,
            } if dates else None,
        }
