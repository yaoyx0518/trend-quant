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
        "price_above_ma20": "价格 > MA20",
        "price_above_ma60": "价格 > MA60",
        "price_above_ma200": "价格 > MA200",
    }
    sell_signal_labels = {
        "hard_stop": "硬止损",
        "chandelier_stop": "吊灯止损",
        "ma_breakdown_max": "均线破位（价格 < max(MA30, MA40, MA60)）",
    }

    def format_buy_filters(cfg: dict) -> list[str]:
        items = cfg.get("buy_filters", []) if isinstance(cfg.get("buy_filters", []), list) else []
        if not items:
            return ["无额外买入过滤（仅使用排名与持仓约束）。"]
        output: list[str] = []
        for item in items:
            key = str(item)
            if key == "trend_score_max":
                output.append(f"买入日 trend_score <= {float(cfg.get('max_entry_trend_score', 20.0))}")
            else:
                output.append(buy_filter_labels.get(key, key))
        return output

    def format_sell_signals(cfg: dict) -> list[str]:
        items = cfg.get("sell_signals", []) if isinstance(cfg.get("sell_signals", []), list) else []
        if not items:
            return ["无技术卖出信号（仅使用调仓退出）。"]
        return [sell_signal_labels.get(str(item), str(item)) for item in items]

    momentum_versions = [
        {
            "id": MOMENTUM_STRATEGY_V3_ID,
            "name": "动量前 N v3",
            "summary": "在 v2 基础上增加买入日趋势评分上限。",
            "buy_signal": [
                f"每周调仓日入场（rebalance_weekday={int(momentum_v3.get('rebalance_weekday', 1))}）。",
                "从混合评分前 50% 的候选中选择，并按评分补足到 max_holdings。",
            ],
            "buy_filter": [
                *format_buy_filters(momentum_v3),
                f"最大持仓限制：max_holdings={int(momentum_v3.get('max_holdings', 5))}。",
            ],
            "sell_signal": [
                "调仓日跌出混合评分前 50% -> 调仓卖出。",
                *format_sell_signals(momentum_v3),
            ],
            "improvements_vs_v1": [
                "增加 MA60 买入过滤，避免进入中期结构偏弱的标的。",
                "增加基于 max(MA30, MA40, MA60) 的动态均线带退出，更早离开走弱持仓。",
                "增加买入日 trend_score 上限（<=20），避免追入过度延伸的标的。",
            ],
        },
        {
            "id": MOMENTUM_STRATEGY_V2_ID,
            "name": "动量前 N v2",
            "summary": "在 v1 排名/调仓框架上，增加更严格的买入过滤和均线破位退出规则。",
            "buy_signal": [
                f"每周调仓日入场（rebalance_weekday={int(momentum_v2.get('rebalance_weekday', 1))}）。",
                "从混合评分前 50% 的候选中选择，并按评分补足到 max_holdings。",
            ],
            "buy_filter": [
                *format_buy_filters(momentum_v2),
                f"最大持仓限制：max_holdings={int(momentum_v2.get('max_holdings', 5))}。",
            ],
            "sell_signal": [
                "调仓日跌出混合评分前 50% -> 调仓卖出。",
                *format_sell_signals(momentum_v2),
            ],
            "improvements_vs_v1": [
                "增加 MA60 买入过滤，避免进入中期结构偏弱的标的。",
                "增加基于 max(MA30, MA40, MA60) 的动态均线带退出，更早离开走弱持仓。",
            ],
        },
        {
            "id": MOMENTUM_STRATEGY_ID,
            "name": "动量前 N v1",
            "summary": "使用动量/趋势混合排名的前 N 策略，每周调仓。",
            "buy_signal": [
                f"每周调仓日入场（rebalance_weekday={int(momentum_v1.get('rebalance_weekday', 1))}）。",
                "从混合评分前 50% 的候选中选择，并按评分补足到 max_holdings。",
            ],
            "buy_filter": [
                *format_buy_filters(momentum_v1),
                f"最大持仓限制：max_holdings={int(momentum_v1.get('max_holdings', 5))}。",
            ],
            "sell_signal": [
                "调仓日跌出混合评分前 50% -> 调仓卖出。",
                *format_sell_signals(momentum_v1),
            ],
            "improvements_vs_v1": [],
        },
    ]

    trend_versions = [
        {
            "id": TREND_STRATEGY_ID,
            "name": "趋势评分 v1",
            "summary": "单标的趋势评分策略，使用入场窗口和强制 ATR 止损。",
            "buy_signal": [
                (
                    "入场窗口："
                    f"{float(trend_v1.get('entry_threshold_min', trend_v1.get('entry_threshold', 10.0)))}"
                    " <= trend_score <= "
                    f"{float(trend_v1.get('entry_threshold_max', 20.0))}。"
                ),
                "要求当前无持仓。",
            ],
            "buy_filter": [
                f"趋势评分窗口：n_short={int(trend_v1.get('n_short', 5))}, n_mid={int(trend_v1.get('n_mid', 10))}, n_long={int(trend_v1.get('n_long', 20))}。",
                "价格确认高于 MA(mid)。",
            ],
            "sell_signal": [
                "应用硬止损和吊灯止损。",
            ],
            "improvements_vs_v1": [],
        }
    ]

    return [
        {
            "id": "momentum_topn",
            "name": "动量前 N",
            "versions": momentum_versions,
        },
        {
            "id": "trend_score",
            "name": "趋势评分",
            "versions": trend_versions,
        },
    ]


@router.get("/strategy-history", response_class=HTMLResponse)
async def strategy_history_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        name="strategy_history.html",
        request=request,
        context={
            "title": "策略历史",
            "strategy_families": _strategy_history_payload(),
        },
    )
