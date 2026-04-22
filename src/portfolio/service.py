from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from audit.app_logger import get_logger
from data.storage.runtime_store import RuntimeStore

logger = get_logger(__name__)


class PortfolioService:
    def __init__(self, runtime_store: RuntimeStore | None = None) -> None:
        self.runtime_store = runtime_store or RuntimeStore()
        self.trade_dir = Path(self.runtime_store.base_dir) / "trades"

    @staticmethod
    def _parse_trade_dt(trade_date: str, trade_time: str) -> datetime:
        date_text = (trade_date or "").strip()
        time_text = (trade_time or "15:00:00").strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(f"{date_text} {time_text}", fmt)
            except ValueError:
                continue
        return datetime.fromisoformat(f"{date_text}T15:00:00")

    def load_manual_trades(self) -> list[dict]:
        if not self.trade_dir.exists():
            return []

        records: list[dict] = []
        files = sorted(self.trade_dir.glob("manual_trades_*.json"))
        for file_path in files:
            payload = self.runtime_store.read_json(str(Path("trades") / file_path.name), default={"items": []})
            items = payload.get("items", []) if isinstance(payload, dict) else []
            for idx, item in enumerate(items):
                symbol = str(item.get("symbol", "")).strip().upper()
                side = str(item.get("side", "")).strip().upper()
                trade_date = str(item.get("trade_date", "")).strip()
                trade_time = str(item.get("trade_time", "15:00:00")).strip()

                try:
                    qty = int(item.get("qty", 0))
                    price = float(item.get("price", 0.0))
                    fee = float(item.get("fee", 0.0))
                    trade_dt = self._parse_trade_dt(trade_date, trade_time)
                except Exception:
                    continue

                if symbol == "" or side not in {"BUY", "SELL"} or qty <= 0 or price <= 0:
                    continue

                records.append(
                    {
                        "id": f"{file_path.name}:{idx}",
                        "symbol": symbol,
                        "side": side,
                        "trade_date": trade_date,
                        "trade_time": trade_time,
                        "qty": qty,
                        "price": price,
                        "fee": max(fee, 0.0),
                        "trade_dt": trade_dt,
                    }
                )

        records.sort(key=lambda x: (x["trade_dt"], x["id"]))
        return records

    def build_snapshot(self, as_of_date: date, initial_capital: float) -> dict:
        trades = self.load_manual_trades()
        cash = float(initial_capital)
        positions: dict[str, dict] = {}

        for tr in trades:
            symbol = tr["symbol"]
            side = tr["side"]
            qty = int(tr["qty"])
            price = float(tr["price"])
            fee = float(tr["fee"])
            trade_date = tr["trade_date"]

            pos = positions.setdefault(symbol, {"lots": []})
            lots = pos["lots"]

            if side == "BUY":
                cash -= (qty * price + fee)
                lots.append({"qty": qty, "price": price, "buy_date": trade_date})
                continue

            current_qty = sum(int(lot["qty"]) for lot in lots)
            exec_qty = min(qty, current_qty)
            if exec_qty <= 0:
                continue

            cash += (exec_qty * price - fee)
            remaining = exec_qty
            new_lots: list[dict] = []
            for lot in lots:
                lot_qty = int(lot["qty"])
                if remaining <= 0:
                    new_lots.append(lot)
                    continue
                consume = min(lot_qty, remaining)
                left = lot_qty - consume
                remaining -= consume
                if left > 0:
                    new_lots.append({"qty": left, "price": lot["price"], "buy_date": lot["buy_date"]})
            pos["lots"] = new_lots

        normalized_positions: dict[str, dict] = {}
        as_of_text = as_of_date.isoformat()
        for symbol, raw_pos in positions.items():
            lots = raw_pos.get("lots", [])
            qty = int(sum(int(lot["qty"]) for lot in lots))
            if qty <= 0:
                continue

            cost_sum = sum(float(lot["qty"]) * float(lot["price"]) for lot in lots)
            avg_price = (cost_sum / qty) if qty > 0 else 0.0
            buy_dates = [str(lot["buy_date"]) for lot in lots if lot.get("buy_date")]
            buy_date = min(buy_dates) if buy_dates else None
            sellable_qty = int(sum(int(lot["qty"]) for lot in lots if str(lot.get("buy_date")) < as_of_text))

            normalized_positions[symbol] = {
                "qty": qty,
                "avg_price": avg_price,
                "buy_date": buy_date,
                "sellable_qty": sellable_qty,
                "lots": lots,
            }

        snapshot = {
            "as_of_date": as_of_text,
            "cash": cash,
            "positions": normalized_positions,
            "trade_count": len(trades),
        }
        self.runtime_store.write_json("positions/current_positions.json", snapshot)
        return snapshot

    @staticmethod
    def estimate_equity(snapshot: dict, price_map: dict[str, float]) -> float:
        cash = float(snapshot.get("cash", 0.0))
        positions = snapshot.get("positions", {}) if isinstance(snapshot, dict) else {}
        market_value = 0.0
        for symbol, pos in positions.items():
            qty = int(pos.get("qty", 0))
            ref_price = float(price_map.get(symbol, pos.get("avg_price", 0.0)) or 0.0)
            market_value += qty * ref_price
        return cash + market_value
