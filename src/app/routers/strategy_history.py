from __future__ import annotations

from pathlib import Path

import yaml
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from strategy.catalog import (
    MOMENTUM_STRATEGY_ID,
    MOMENTUM_STRATEGY_V2_ID,
    MOMENTUM_STRATEGY_V3_ID,
    TREND_STRATEGY_ID,
    resolve_strategy_config,
)

router = APIRouter(tags=["strategy-history"])
templates = Jinja2Templates(directory="web/templates")


def _load_yaml(path: str) -> dict:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _strategy_history_payload() -> list[dict]:
    strategy_cfg = _load_yaml("config/strategy.yaml").get("strategy", {})
    if not isinstance(strategy_cfg, dict):
        strategy_cfg = {}

    momentum_v1 = resolve_strategy_config(strategy_cfg, MOMENTUM_STRATEGY_ID)
    momentum_v2 = resolve_strategy_config(strategy_cfg, MOMENTUM_STRATEGY_V2_ID)
    momentum_v3 = resolve_strategy_config(strategy_cfg, MOMENTUM_STRATEGY_V3_ID)
    trend_v1 = resolve_strategy_config(strategy_cfg, TREND_STRATEGY_ID)

    buy_filter_labels = {
        "price_above_ma20": "Price > MA20",
        "price_above_ma60": "Price > MA60",
        "price_above_ma200": "Price > MA200",
    }
    sell_signal_labels = {
        "hard_stop": "Hard stop",
        "chandelier_stop": "Chandelier stop",
        "ma_breakdown_max": "MA breakdown (price < max(MA30, MA40, MA60))",
    }

    def format_buy_filters(cfg: dict) -> list[str]:
        items = cfg.get("buy_filters", []) if isinstance(cfg.get("buy_filters", []), list) else []
        if not items:
            return ["No extra buy filter (only ranking and holding constraints)."]
        output: list[str] = []
        for item in items:
            key = str(item)
            if key == "trend_score_max":
                output.append(f"Buy-day trend_score <= {float(cfg.get('max_entry_trend_score', 20.0))}")
            else:
                output.append(buy_filter_labels.get(key, key))
        return output

    def format_sell_signals(cfg: dict) -> list[str]:
        items = cfg.get("sell_signals", []) if isinstance(cfg.get("sell_signals", []), list) else []
        if not items:
            return ["No technical sell signal (only rebalance exits)."]
        return [sell_signal_labels.get(str(item), str(item)) for item in items]

    momentum_versions = [
        {
            "id": MOMENTUM_STRATEGY_V3_ID,
            "name": "Momentum TopN v3",
            "summary": "Based on v2, with an additional buy-day trend score cap.",
            "buy_signal": [
                f"Weekly rebalance day entry (rebalance_weekday={int(momentum_v3.get('rebalance_weekday', 1))}).",
                "Candidates are selected from hybrid-score top 50%, then filled by score until max_holdings.",
            ],
            "buy_filter": [
                *format_buy_filters(momentum_v3),
                f"Maximum holdings limit: max_holdings={int(momentum_v3.get('max_holdings', 5))}.",
            ],
            "sell_signal": [
                "Leave top-50% hybrid-score bucket on rebalance day -> rebalance sell.",
                *format_sell_signals(momentum_v3),
            ],
            "improvements_vs_v1": [
                "Added MA60 buy filter to avoid weak medium-term structure entries.",
                "Added dynamic MA-band exit using max(MA30, MA40, MA60) to leave weakening holdings earlier.",
                "Added buy-day trend_score upper bound (<=20) to avoid over-extended entries.",
            ],
        },
        {
            "id": MOMENTUM_STRATEGY_V2_ID,
            "name": "Momentum TopN v2",
            "summary": "Based on v1 ranking/rebalance, with stricter buy filter and an MA breakdown exit rule.",
            "buy_signal": [
                f"Weekly rebalance day entry (rebalance_weekday={int(momentum_v2.get('rebalance_weekday', 1))}).",
                "Candidates are selected from hybrid-score top 50%, then filled by score until max_holdings.",
            ],
            "buy_filter": [
                *format_buy_filters(momentum_v2),
                f"Maximum holdings limit: max_holdings={int(momentum_v2.get('max_holdings', 5))}.",
            ],
            "sell_signal": [
                "Leave top-50% hybrid-score bucket on rebalance day -> rebalance sell.",
                *format_sell_signals(momentum_v2),
            ],
            "improvements_vs_v1": [
                "Added MA60 buy filter to avoid weak medium-term structure entries.",
                "Added dynamic MA-band exit using max(MA30, MA40, MA60) to leave weakening holdings earlier.",
            ],
        },
        {
            "id": MOMENTUM_STRATEGY_ID,
            "name": "Momentum TopN v1",
            "summary": "Hybrid momentum/trend ranking TopN strategy with weekly rebalance.",
            "buy_signal": [
                f"Weekly rebalance day entry (rebalance_weekday={int(momentum_v1.get('rebalance_weekday', 1))}).",
                "Candidates are selected from hybrid-score top 50%, then filled by score until max_holdings.",
            ],
            "buy_filter": [
                *format_buy_filters(momentum_v1),
                f"Maximum holdings limit: max_holdings={int(momentum_v1.get('max_holdings', 5))}.",
            ],
            "sell_signal": [
                "Leave top-50% hybrid-score bucket on rebalance day -> rebalance sell.",
                *format_sell_signals(momentum_v1),
            ],
            "improvements_vs_v1": [],
        },
    ]

    trend_versions = [
        {
            "id": TREND_STRATEGY_ID,
            "name": "Trend Score v1",
            "summary": "Single-asset trend score strategy with entry window and mandatory ATR stops.",
            "buy_signal": [
                (
                    "Entry window: "
                    f"{float(trend_v1.get('entry_threshold_min', trend_v1.get('entry_threshold', 10.0)))}"
                    " <= trend_score <= "
                    f"{float(trend_v1.get('entry_threshold_max', 20.0))}."
                ),
                "Requires no existing position.",
            ],
            "buy_filter": [
                f"Trend-score windows: n_short={int(trend_v1.get('n_short', 5))}, n_mid={int(trend_v1.get('n_mid', 20))}, n_long={int(trend_v1.get('n_long', 40))}.",
                "Price confirmation above MA(mid).",
            ],
            "sell_signal": [
                "Hard stop and chandelier stop apply.",
            ],
            "improvements_vs_v1": [],
        }
    ]

    return [
        {
            "id": "momentum_topn",
            "name": "Momentum TopN",
            "versions": momentum_versions,
        },
        {
            "id": "trend_score",
            "name": "Trend Score",
            "versions": trend_versions,
        },
    ]


@router.get("/strategy-history", response_class=HTMLResponse)
async def strategy_history_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="strategy_history.html",
        request=request,
        context={
            "title": "Strategy History",
            "strategy_families": _strategy_history_payload(),
        },
    )
