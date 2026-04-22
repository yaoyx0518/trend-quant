from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd

from audit.app_logger import get_logger
from data.provider_base import IDataProvider
from data.provider_utils import normalize_symbol, parse_minute_period, safe_float, standardize_ohlcv

logger = get_logger(__name__)

try:
    import akshare as ak  # type: ignore
except Exception:  # pragma: no cover
    ak = None


class AkshareProvider(IDataProvider):
    name = "akshare"

    def fetch_daily_history(self, symbol: str, start: date, end: date, adjust: str) -> pd.DataFrame:
        if ak is None:
            logger.warning("akshare not installed, skip daily fetch")
            return pd.DataFrame()

        code = normalize_symbol(symbol)
        try:
            raw = ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
                adjust=adjust,
            )
            return standardize_ohlcv(raw, symbol)
        except Exception as exc:
            logger.warning("akshare daily fetch failed for %s: %s", symbol, exc)
            return pd.DataFrame()

    def fetch_minute_history(self, symbol: str, period: str, count: int, adjust: str) -> pd.DataFrame:
        if ak is None:
            return pd.DataFrame()

        code = normalize_symbol(symbol)
        period_num = parse_minute_period(period)
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=30)

        try:
            raw = ak.fund_etf_hist_min_em(
                symbol=code,
                period=period_num,
                adjust=adjust,
                start_date=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_date=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            )
            normalized = standardize_ohlcv(raw, symbol)
            if count > 0 and not normalized.empty:
                normalized = normalized.tail(count).reset_index(drop=True)
            return normalized
        except Exception as exc:
            logger.warning("akshare minute fetch failed for %s: %s", symbol, exc)
            return pd.DataFrame()

    def fetch_latest_quote(self, symbol: str) -> dict:
        if ak is None:
            return {"symbol": symbol, "price": None, "ts": None}

        code = normalize_symbol(symbol)
        try:
            df = ak.fund_etf_spot_em()
            if df is None or df.empty:
                return {"symbol": symbol, "price": None, "ts": None}
            row = df[df["代码"].astype(str) == code]
            if row.empty:
                return {"symbol": symbol, "price": None, "ts": None}
            item = row.iloc[0]
            return {
                "symbol": symbol,
                "name": str(item.get("名称", item.get("基金简称", "")) or "").strip() or None,
                "price": safe_float(item.get("最新价", None), None),
                "open": safe_float(item.get("今开", item.get("开盘", None)), None),
                "high": safe_float(item.get("最高", None), None),
                "low": safe_float(item.get("最低", None), None),
                "volume": safe_float(item.get("成交量", None), None),
                "amount": safe_float(item.get("成交额", None), None),
                "ts": str(item.get("数据日期", datetime.now().isoformat())),
            }
        except Exception as exc:
            logger.warning("akshare latest quote failed for %s: %s", symbol, exc)
            return {"symbol": symbol, "price": None, "ts": None}

    def fetch_trading_calendar(self, start: date, end: date) -> list[date]:
        if ak is None:
            return []
        try:
            df = ak.tool_trade_date_hist_sina()
            if df is None or df.empty:
                return []
            col = "trade_date" if "trade_date" in df.columns else df.columns[0]
            series = pd.to_datetime(df[col], errors="coerce").dropna().dt.date
            return [d for d in series.tolist() if start <= d <= end]
        except Exception as exc:
            logger.warning("akshare trade calendar fetch failed: %s", exc)
            return []
