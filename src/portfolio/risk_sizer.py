from __future__ import annotations

from math import floor


class RiskSizer:
    def __init__(self, lot_size: int = 100) -> None:
        self.lot_size = lot_size

    def suggest_qty(self, equity: float, risk_budget_pct: float, atr_value: float, stop_mul: float) -> int:
        per_share_risk = atr_value * stop_mul
        if per_share_risk <= 0:
            return 0
        raw_qty = (equity * risk_budget_pct) / per_share_risk
        lots = floor(raw_qty / self.lot_size)
        return max(lots * self.lot_size, 0)

    @staticmethod
    def _estimate_buy_cost(qty: int, price: float, fee_rate: float, fee_min: float, slippage: float) -> float:
        if qty <= 0 or price <= 0:
            return 0.0
        deal_price = price * (1.0 + slippage)
        gross = qty * deal_price
        fee = max(gross * fee_rate, fee_min)
        return gross + fee

    def cap_qty_by_max_cost(
        self,
        qty: int,
        price: float,
        max_cost: float,
        fee_rate: float = 0.0,
        fee_min: float = 0.0,
        slippage: float = 0.0,
    ) -> int:
        if qty <= 0 or price <= 0 or max_cost <= 0:
            return 0

        lot_qty = (int(qty) // self.lot_size) * self.lot_size
        if lot_qty <= 0:
            return 0

        deal_price = float(price) * (1.0 + float(slippage))
        if deal_price <= 0:
            return 0

        max_lots_by_gross = floor(float(max_cost) / (deal_price * self.lot_size))
        if max_lots_by_gross <= 0:
            return 0

        capped_qty = min(lot_qty, max_lots_by_gross * self.lot_size)
        while capped_qty > 0 and self._estimate_buy_cost(capped_qty, price, fee_rate, fee_min, slippage) > max_cost:
            capped_qty -= self.lot_size
        return max(capped_qty, 0)

    def scale_allocations(self, allocations: list[dict], total_cash: float) -> list[dict]:
        total_cost = sum(float(item.get("cost", 0.0)) for item in allocations)
        if total_cost <= total_cash or total_cost <= 0:
            for item in allocations:
                item["scaled_qty"] = int(item.get("qty", 0))
                item["scale_ratio"] = 1.0
            return allocations

        ratio = total_cash / total_cost
        for item in allocations:
            qty = int(item.get("qty", 0))
            lot_qty = (qty // self.lot_size) * self.lot_size
            scaled_qty = int((lot_qty * ratio) // self.lot_size) * self.lot_size
            item["scaled_qty"] = max(scaled_qty, 0)
            item["scale_ratio"] = ratio
        return allocations
