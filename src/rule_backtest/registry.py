from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ParamSpec:
    type: str
    required: bool = True
    default: object | None = None
    min_value: float | None = None


@dataclass(frozen=True, slots=True)
class IndicatorSpec:
    id: str
    version: int
    label: str
    category: str
    output_type: str = "number"
    params: dict[str, ParamSpec] = field(default_factory=dict)


PRICE_FIELD_PARAM = ParamSpec(type="price_field", required=False, default="close")


def default_indicator_registry() -> dict[str, IndicatorSpec]:
    return {
        "sma": IndicatorSpec(
            id="sma",
            version=1,
            label="简单移动平均",
            category="trend",
            params={
                "field": PRICE_FIELD_PARAM,
                "period": ParamSpec(type="int", required=True, min_value=1),
            },
        ),
        "ema": IndicatorSpec(
            id="ema",
            version=1,
            label="指数移动平均",
            category="trend",
            params={
                "field": PRICE_FIELD_PARAM,
                "period": ParamSpec(type="int", required=True, min_value=1),
            },
        ),
        "bias": IndicatorSpec(
            id="bias",
            version=1,
            label="BIAS",
            category="trend",
            params={
                "field": PRICE_FIELD_PARAM,
                "period": ParamSpec(type="int", required=True, min_value=1),
            },
        ),
        "bias_atr_normed": IndicatorSpec(
            id="bias_atr_normed",
            version=1,
            label="ATR标准化BIAS",
            category="trend",
            params={
                "field": PRICE_FIELD_PARAM,
                "period": ParamSpec(type="int", required=True, min_value=1),
                "atr_period": ParamSpec(type="int", required=False, default=20, min_value=1),
            },
        ),
        "atr": IndicatorSpec(
            id="atr",
            version=1,
            label="ATR",
            category="volatility",
            params={"period": ParamSpec(type="int", required=False, default=20, min_value=1)},
        ),
        "rsi": IndicatorSpec(
            id="rsi",
            version=1,
            label="RSI",
            category="momentum",
            params={
                "field": PRICE_FIELD_PARAM,
                "period": ParamSpec(type="int", required=False, default=14, min_value=1),
            },
        ),
        "macd_line": _macd_spec("macd_line", "MACD Line"),
        "macd_signal": _macd_spec("macd_signal", "MACD Signal"),
        "macd_histogram": _macd_spec("macd_histogram", "MACD Histogram"),
        "bollinger_upper": _bollinger_spec("bollinger_upper", "布林上轨"),
        "bollinger_middle": _bollinger_spec("bollinger_middle", "布林中轨"),
        "bollinger_lower": _bollinger_spec("bollinger_lower", "布林下轨"),
        "volume_sma": IndicatorSpec(
            id="volume_sma",
            version=1,
            label="成交量均线",
            category="volume",
            params={"period": ParamSpec(type="int", required=True, min_value=1)},
        ),
        "volume_ratio": IndicatorSpec(
            id="volume_ratio",
            version=1,
            label="成交量比率",
            category="volume",
            params={"period": ParamSpec(type="int", required=True, min_value=1)},
        ),
        "momentum_return": IndicatorSpec(
            id="momentum_return",
            version=1,
            label="动量收益率",
            category="momentum",
            params={
                "field": PRICE_FIELD_PARAM,
                "period": ParamSpec(type="int", required=True, min_value=1),
            },
        ),
        "trend_score": IndicatorSpec(
            id="trend_score",
            version=1,
            label="Trend Score",
            category="trend",
            params={},
        ),
        "trend_score_sma": IndicatorSpec(
            id="trend_score_sma",
            version=1,
            label="Trend Score SMA",
            category="trend",
            params={"period": ParamSpec(type="int", required=True, min_value=1)},
        ),
        "trend_score_ema": IndicatorSpec(
            id="trend_score_ema",
            version=1,
            label="Trend Score EMA",
            category="trend",
            params={"period": ParamSpec(type="int", required=True, min_value=1)},
        ),
        "random_uniform": IndicatorSpec(
            id="random_uniform",
            version=1,
            label="随机数 [0,1)",
            category="random",
            params={"seed": ParamSpec(type="int", required=False, default=None)},
        ),
    }


def _macd_spec(indicator_id: str, label: str) -> IndicatorSpec:
    return IndicatorSpec(
        id=indicator_id,
        version=1,
        label=label,
        category="momentum",
        params={
            "field": PRICE_FIELD_PARAM,
            "fast_period": ParamSpec(type="int", required=False, default=12, min_value=1),
            "slow_period": ParamSpec(type="int", required=False, default=26, min_value=1),
            "signal_period": ParamSpec(type="int", required=False, default=9, min_value=1),
        },
    )


def registry_payload(registry: dict[str, IndicatorSpec] | None = None) -> list[dict]:
    specs = registry or default_indicator_registry()
    rows: list[dict] = []
    for key in sorted(specs):
        spec = specs[key]
        rows.append(
            {
                "id": spec.id,
                "version": spec.version,
                "label": spec.label,
                "category": spec.category,
                "output_type": spec.output_type,
                "params": {
                    name: {
                        "type": param.type,
                        "required": param.required,
                        "default": param.default,
                        "min": param.min_value,
                    }
                    for name, param in spec.params.items()
                },
            }
        )
    return rows


def _bollinger_spec(indicator_id: str, label: str) -> IndicatorSpec:
    return IndicatorSpec(
        id=indicator_id,
        version=1,
        label=label,
        category="volatility",
        params={
            "field": PRICE_FIELD_PARAM,
            "period": ParamSpec(type="int", required=False, default=20, min_value=1),
            "std_mul": ParamSpec(type="float", required=False, default=2.0, min_value=0),
        },
    )
