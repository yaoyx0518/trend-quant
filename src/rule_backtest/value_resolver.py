from __future__ import annotations

import pandas as pd

from rule_backtest import indicators
from rule_backtest.models import PositionState


class ValueResolver:
    def __init__(self, strategy_cfg: dict | None = None) -> None:
        self.strategy_cfg = strategy_cfg or {}

    def resolve(
        self,
        spec: dict,
        bars: pd.DataFrame,
        position: PositionState,
        debug: bool = False,
    ) -> tuple[float | None, dict]:
        spec_type = str(spec.get("type", "")).strip()
        if spec_type == "price":
            field = str(spec.get("field", "close")).strip()
            value = indicators.latest_field(bars, field)
            return value, {"type": "price", "field": field, "value": value} if debug else {}

        if spec_type == "literal":
            try:
                value = float(spec.get("value"))
            except Exception:
                value = None
            return value, {"type": "literal", "value": value} if debug else {}

        if spec_type == "state_value":
            return self._resolve_state_value(spec=spec, position=position, debug=debug)

        if spec_type == "indicator":
            return self._resolve_indicator(spec=spec, bars=bars, debug=debug)

        return None, {"reason": "unsupported_value_spec", "spec": spec} if debug else {}

    def _resolve_state_value(
        self,
        spec: dict,
        position: PositionState,
        debug: bool,
    ) -> tuple[float | None, dict]:
        name = str(spec.get("name", "")).strip()
        value = None
        if name == "entry_price":
            value = position.entry_price if position.is_open else None
        elif name == "hard_stop":
            value = position.hard_stop if position.is_open else None
        elif name == "highest_high_since_entry":
            value = position.highest_high_since_entry if position.is_open else None
        elif name == "chandelier_stop":
            value = position.chandelier_stop if position.is_open else None
        trace = {"type": "state_value", "name": name, "value": value}
        return value, trace if debug else {}

    def _resolve_indicator(self, spec: dict, bars: pd.DataFrame, debug: bool) -> tuple[float | None, dict]:
        name = str(spec.get("name", "")).strip()
        params = spec.get("params", {}) if isinstance(spec.get("params", {}), dict) else {}
        field = str(params.get("field", "close")).strip()
        value: float | None
        trace: dict

        if name == "sma":
            value, trace = indicators.sma(bars, field=field, period=int(params.get("period", 20)))
        elif name == "ema":
            value, trace = indicators.ema(bars, field=field, period=int(params.get("period", 20)))
        elif name == "bias":
            value, trace = indicators.bias(bars, field=field, period=int(params.get("period", 20)))
        elif name == "bias_atr_normed":
            value, trace = indicators.bias_atr_normed(
                bars,
                field=field,
                period=int(params.get("period", 20)),
                atr_period=int(params.get("atr_period", 20)),
            )
        elif name == "atr":
            value, trace = indicators.atr(bars, period=int(params.get("period", 20)))
        elif name == "rsi":
            value, trace = indicators.rsi(bars, field=field, period=int(params.get("period", 14)))
        elif name in {"macd_line", "macd_signal", "macd_histogram"}:
            values, trace = indicators.macd(
                bars,
                field=field,
                fast_period=int(params.get("fast_period", 12)),
                slow_period=int(params.get("slow_period", 26)),
                signal_period=int(params.get("signal_period", 9)),
            )
            key = name.replace("macd_", "")
            value = values.get(key)
        elif name in {"bollinger_upper", "bollinger_middle", "bollinger_lower"}:
            values, trace = indicators.bollinger(
                bars,
                field=field,
                period=int(params.get("period", 20)),
                std_mul=float(params.get("std_mul", 2.0)),
            )
            key = name.replace("bollinger_", "")
            value = values.get(key)
        elif name == "volume_sma":
            value, trace = indicators.sma(bars, field="volume", period=int(params.get("period", 20)))
        elif name == "volume_ratio":
            volume = indicators.latest_field(bars, "volume")
            ma_value, ma_trace = indicators.sma(bars, field="volume", period=int(params.get("period", 20)))
            value = (volume / ma_value) if volume is not None and ma_value not in (None, 0) else None
            trace = {"volume": volume, "volume_sma": ma_value, "sma": ma_trace, "value": value}
        elif name == "momentum_return":
            value, trace = indicators.momentum_return(bars, field=field, period=int(params.get("period", 20)))
        elif name == "trend_score":
            value, trace = indicators.trend_score(bars, cfg=self.strategy_cfg)
        elif name == "trend_score_sma":
            value, trace = indicators.trend_score_series(
                bars, period=int(params.get("period", 5)), mode="sma", cfg=self.strategy_cfg
            )
        elif name == "trend_score_ema":
            value, trace = indicators.trend_score_series(
                bars, period=int(params.get("period", 5)), mode="ema", cfg=self.strategy_cfg
            )
        else:
            value, trace = None, {"reason": "unsupported_indicator", "name": name}

        if debug:
            return value, {"type": "indicator", "name": name, "params": dict(params), "trace": trace, "value": value}
        return value, {}
