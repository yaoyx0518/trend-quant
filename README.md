# Trend ETF System

A 股 ETF 趋势交易系统（单用户、本地运行、人工执行交易），围绕自定义 `Trend Score` 指标实现数据更新、盘中信号、风险预算仓位建议、回测分析与可视化。

本 README 作为项目开发总览与 AI 协作输入文档，优先描述当前代码的真实实现（as-built）。

## 1. 当前能力边界

- 市场：A 股场内 ETF
- 策略：`TrendScoreStrategy`（趋势值模型）
- 执行：系统给出信号与建议数量，实际成交由人工录入
- 调度：交易日盘中轮询 + 14:45 最终信号 + 收盘后日线更新
- 存储：本地 Parquet + JSON + 日志文件（不使用数据库）
- 可视化：FastAPI + Jinja2 + ECharts

## 2. 快速启动（Windows）

### 2.1 环境

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
```

### 2.2 运行

```powershell
.\scripts\run_dev.ps1
```

如果 PowerShell 执行策略拦截脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_dev.ps1
```

访问：`http://127.0.0.1:8000`

## 3. 项目结构（代码组织）

```text
src/
  app/
    main.py                  # FastAPI 入口 + 生命周期 + 调度注册
    routers/
      overview.py            # 概览页与概览 API
      config.py              # 配置查看/更新 API
      backtest.py            # 回测页面与回测 API
      trades.py              # 手工成交录入与组合快照 API
      logs.py                # 日志查询 API

  core/
    settings.py              # app/runtime/logging 配置加载
    scheduler.py             # APScheduler 封装
    enums.py                 # SignalAction / SignalLevel

  data/
    provider_base.py         # IDataProvider 协议
    provider_efinance.py     # 主数据源
    provider_akshare.py      # 备数据源
    provider_utils.py        # 字段归一化工具
    service.py               # DataService（优先级/降级/更新编排）
    storage/
      market_store.py        # Parquet 行情存取
      runtime_store.py       # JSON 运行态存取

  strategy/
    base.py                  # IStrategy 协议
    indicators.py            # ATR / Efficiency Ratio
    trend_score_strategy.py  # 趋势值策略核心

  portfolio/
    service.py               # 由手工成交重建仓位快照（含 T+1 可卖）
    risk_sizer.py            # 风险预算仓位建议 + 等比缩放

  engine/
    signal_engine.py         # 盘中信号主引擎

  backtest/
    backtest_engine.py       # 回测主引擎
    metrics.py               # 收益、回撤、热力图、分标的统计
    benchmark.py             # 等权池基准

  notify/
    feishu_notifier.py       # 通知桩（当前写日志）
    email_notifier.py        # 通知桩（当前写日志）

  audit/
    app_logger.py            # 应用日志
    calc_logger.py           # 计算明细日志（jsonl）

web/
  templates/                 # Jinja2 页面模板
  static/style.css

config/
  app.yaml
  instruments.yaml
  strategy.yaml

data/
  market/etf/*.parquet       # ETF 历史行情
  runtime/**                 # 信号、回测、成交、仓位快照

logs/
  app/app.log
  calc/calc.jsonl
```

## 4. 核心实现类与职责

### 4.1 数据与存储

- `IDataProvider` (`src/data/provider_base.py`)
  - 统一接口：
    - `fetch_daily_history`
    - `fetch_minute_history`
    - `fetch_latest_quote`
    - `fetch_trading_calendar`

- `EfinanceProvider` / `AkshareProvider`
  - 实现同一接口，字段标准化后返回统一 OHLCV 结构。

- `DataService` (`src/data/service.py`)
  - 按 `config/app.yaml` 中 `data_provider_priority` 进行数据源优先级调用与降级。
  - 负责增量更新 ETF 池历史数据到 Parquet。

- `MarketStore`
  - `symbol -> parquet` 的读写封装。

- `RuntimeStore`
  - 统一读写运行态 JSON（signals/backtests/trades/positions/advice）。

### 4.2 策略与信号

- `IStrategy` (`src/strategy/base.py`)
  - 约束 `evaluate(symbol, bars, state, cfg)` 输出标准信号结构。

- `TrendScoreStrategy` (`src/strategy/trend_score_strategy.py`)
  - 计算 `trend_score`、`action`、`level`、`reason`、`calc_details`。
  - 包含入场、离场、预警（watch）与 T+1 阻塞逻辑。

- `SignalEngine` (`src/engine/signal_engine.py`)
  - 盘中轮询主流程：
    1. 读取配置与当前事实仓位
    2. 拉取/补齐行情 + 最新价融合
    3. 计算每标的信号
    4. 风险预算计算建议股数并做资金缩放
    5. 写入 `signals/YYYY-MM-DD.json` 和 `signals/latest_state.json`

### 4.3 组合与风控

- `PortfolioService` (`src/portfolio/service.py`)
  - 从 `manual_trades_*.json` 回放成交，重建当前现金/持仓/可卖数量。
  - SELL 按 FIFO 消耗买入 lot。

- `RiskSizer` (`src/portfolio/risk_sizer.py`)
  - `suggest_qty`：按 ATR 风险单位给出理论仓位（100 整手约束）。
  - `scale_allocations`：当总成本超现金时按比例缩放所有候选买入。

### 4.4 回测

- `BacktestEngine` (`src/backtest/backtest_engine.py`)
  - 日频遍历时间线，先卖后买，模拟手续费/滑点与 T+1。
  - 输出 summary、trades、annual/monthly、charts payload。

- `metrics.py`
  - `compute_metrics`、`compute_drawdown`、`compute_annual_returns`、`compute_monthly_heatmap`、`compute_symbol_trade_stats`。

- `benchmark.py`
  - 4 ETF 池等权基准：初始等权分配，按整手买入并持有。

### 4.5 调度与应用

- `SchedulerManager` (`src/core/scheduler.py`)
  - 注册三类任务：`poll`、`final_signal(14:45)`、`daily_update(15:30)`。

- `app.main` (`src/app/main.py`)
  - 应用生命周期中初始化 `SignalEngine` 与调度器。
  - 14:45 信号任务内置“数据不可用重试”。

## 5. 核心接口（Web/API）

### 5.1 页面路由

- `/`：系统概览
- `/config`：配置查看
- `/backtest`：回测中心
- `/trades`：手工成交录入
- `/logs`：日志页面

### 5.2 主要 API

- `GET /api/overview`
- `GET /config/api/raw`
- `POST /config/api/raw`
- `POST /backtest/api/run`
- `GET /backtest/api/list`
- `GET /backtest/api/{run_id}`
- `GET /trades/api/manual?trade_date=YYYY-MM-DD`
- `POST /trades/api/manual`
- `GET /trades/api/portfolio?as_of_date=YYYY-MM-DD`
- `GET /logs/api/calc?limit=100`

## 6. 算法核心（Trend Score）

### 6.1 指标分解

- 总公式：
  - `Trend Score = Price Direction * Confidence`

- `Price Direction`
  - 基于短中长周期 Bias 与 Slope 加权合成。
  - `Bias_n = (Close - MA_n) / ATR`
  - `Slope_n = (EMA_n(now) - EMA_n(prev)) / (ATR * n)`
  - 归一化：
    - `norm_bias = tanh(bias_mix / 2) * 100`
    - `norm_slope = tanh(slope_mix) * 100`

- `Confidence`
  - `Volume Factor` + `Efficiency Ratio` 组合。
  - `vol_ratio = volume / MA(volume, 20)`，上限按 3 截断映射到 `[0,1]`。
  - `ER` 使用 10 日效率比率（噪声越小 ER 越高）。
  - `confidence = volume_factor^w_vol * er^w_er`

### 6.2 交易规则

- 买入（全部满足）：
  1. 当前无持仓
  2. `entry_threshold_min <= trend_score <= entry_threshold_max`
  3. 当前价 > `MA_mid`

- 卖出（持仓时任一触发）：
  1. 跌破硬止损
  2. 跌破吊灯止损

- T+1：
  - 当天买入不可卖；若触发卖出但 `sellable_qty=0`，输出 WARN + `t1_blocked`。

### 6.3 仓位与资金约束

- 每标的风险预算：`equity * risk_budget_pct`
- 每股风险：`ATR * stop_atr_mul`
- 理论股数：`risk_budget / per_share_risk`，再按 100 整手向下取整
- 总候选超资金：按比例整体缩放，允许保留少量现金

## 7. 回测输出与可视化数据契约

`BacktestEngine.run()` 输出 `result.json` 主要字段：

- `summary`：策略指标（收益、回撤、夏普、Sortino、Calmar、胜率、盈亏比等）
- `benchmark_summary`：基准指标
- `annual_returns`
- `monthly_heatmap`
- `symbol_stats`
- `trades`
- `charts`
  - `dates`
  - `nav` / `benchmark_nav`
  - `drawdown`
  - `buy_points` / `sell_points`
  - `trend`（所有标的趋势值序列）
  - `holdings`（分标的 + 现金堆叠序列）
  - `kline`（每标的 K 线 + 买卖标记）

## 8. 配置说明

### 8.1 `config/app.yaml`

- 运行参数：host/port/timezone
- 数据源优先级：`data_provider_priority`
- 调度时点：`polling_times`、`final_signal_time`、`update_time_after_close`
- 重试参数：行情拉取与通知重试
- 交易整手：`lot_size`

### 8.2 `config/instruments.yaml`

- 标的池与标的级参数：
  - `symbol`
  - `enabled`
  - `risk_budget_pct`
  - `stop_atr_mul`

### 8.3 `config/strategy.yaml`

- 趋势模型参数（周期/权重/阈值）
- 成本参数（fee/slippage）
- 回测默认区间（当前主起始 `2024-01-01`，回退 `2018-01-01`）

## 9. 关键数据文件

- 行情：`data/market/etf/{symbol}.parquet`
- 每日信号：`data/runtime/signals/{date}.json`
- 信号状态：`data/runtime/signals/latest_state.json`
- 回测结果：`data/runtime/backtests/{run_id}/result.json`
- 手工成交：`data/runtime/trades/manual_trades_{date}.json`
- 仓位快照：`data/runtime/positions/current_positions.json`
- 计算日志：`logs/calc/calc.jsonl`

## 10. 扩展指南（给后续 AI/开发者）

### 10.1 新增数据源

1. 新建 `provider_xxx.py` 实现 `IDataProvider`
2. 在 `DataService.providers` 注册
3. 在 `config/app.yaml` 配置优先级

### 10.2 新增策略

1. 新建策略类实现 `IStrategy`
2. 在 `SignalEngine` / `BacktestEngine` 注入策略实例
3. 扩展 `config/strategy.yaml` 参数
4. 如需多策略并行，建议增加 `StrategyRegistry + PortfolioAllocator`

### 10.3 新增通知通道

1. 实现 notifier（参考 `notify/*_notifier.py`）
2. 在信号触发链路接入分级发送与失败重试
3. 将发送结果写入审计日志

## 11. 当前已知限制

- 通知仍为日志桩，未接入真实飞书/邮件发送。
- 交易日历在主源不可用时可能退化为工作日判断。
- 当前仅单账户、人工执行，不含自动下单。
- 项目未初始化 Git 仓库（当前目录无 `.git`）。

## 12. 开发建议顺序

1. 真实通知接入（飞书优先）
2. 盘前/盘后通知任务
3. 多策略框架（为“行业 ETF TopN 动量”预留）
4. 日志检索与配置页增强
