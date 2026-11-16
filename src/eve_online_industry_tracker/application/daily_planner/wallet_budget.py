"""Wallet budget and stock ledger for the daily planner."""
from __future__ import annotations

from typing import Callable


class WalletBudget:
    def __init__(self, *, balance: float | None, reserve_pct: float, blocked_reason: str | None = None):
        self._charged = 0.0
        self._pot = 0.0
        self._spendable = max(balance * (1 - reserve_pct / 100), 0.0) if balance is not None else 0.0
        if balance is None:
            computed = "corp wallet unknown"
        elif self._spendable <= 0:
            computed = f"no budget: corp wallet {balance:,.0f} ISK, {reserve_pct:g}% reserve"
        else:
            computed = None
        self._blocked_reason = blocked_reason or computed

    @property
    def blocked_reason(self) -> str | None:
        return self._blocked_reason

    @property
    def available(self) -> float:
        return max(self._spendable - self._charged - self._pot, 0.0)

    @property
    def spent(self) -> float:
        return self._charged

    def charge(self, amount: float | None) -> bool:
        if amount is None or amount < 0 or self._blocked_reason or amount > self.available:
            return False
        self._charged += amount
        return True

    def set_aside(self, amount: float) -> float:
        if self._blocked_reason or amount <= 0:
            return 0.0
        moved = min(amount, self.available)
        self._pot += moved
        return moved

    def charge_science(self, amount: float | None) -> bool:
        if amount is None or amount < 0 or self._blocked_reason or amount > self._pot + self.available:
            return False
        from_pot = min(amount, self._pot)
        self._pot -= from_pot
        self._charged += amount
        return True


class StockLedger:
    def __init__(self, stock: dict[int, int]):
        self._stock = dict(stock)

    def purchase_cost(
        self,
        materials: dict[int, int],
        unit_price: Callable[[int], float | None],
        *,
        commit: bool,
    ) -> float | None:
        cost = 0.0
        used: dict[int, int] = {}
        for type_id, qty in materials.items():
            take = min(self._stock.get(type_id, 0), qty)
            used[type_id] = take
            gap = qty - take
            if gap > 0:
                price = unit_price(type_id)
                if price is None:
                    return None
                cost += gap * price
        if commit:
            for type_id, take in used.items():
                if take:
                    self._stock[type_id] -= take
        return cost


def max_affordable_runs(cost_of: Callable[[int], float | None], max_runs: int, available: float) -> int:
    lo, hi = 0, max_runs
    while lo < hi:
        mid = (lo + hi + 1) // 2
        cost = cost_of(mid)
        if cost is not None and cost <= available:
            lo = mid
        else:
            hi = mid - 1
    return lo
