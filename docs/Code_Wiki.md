# Code Wiki - Trend ETF System

> A股 ETF 趋势量化交易系统 — 完整代码文档  
> 版本: v1.1-as-built | 更新日期: 2026-05-24  
> 代码基线: `E:\codex project\tread quant`

---

## 目录

1. [项目概述](#1-项目概述)
2. [整体架构](#2-整体架构)
3. [项目目录结构](#3-项目目录结构)
4. [核心模块详解](#4-核心模块详解)
   - [4.1 应用入口层 (app/)](#41-应用入口层-app)
   - [4.2 核心配置层 (core/)](#42-核心配置层-core)
   - [4.3 数据层 (data/)](#43-数据层-data)
   - [4.4 策略层 (strategy/)](#44-策略层-strategy)
   - [4.5 信号引擎层 (engine/)](#45-信号引擎层-engine)
   - [4.6 组合与风控层 (portfolio/)](#46-组合与风控层-portfolio)
   - [4.7 回测层 (backtest/)](#47-回测层-backtest)
   - [4.8 通知层 (notify/)](#48-通知层-notify)
   - [4.9 审计日志层 (audit/)](#49-审计日志层-audit)
   - [4.10 前端模板层 (web/)](#410-前端模板层-web)
5. [数据流与生命周期](#5-数据流与生命周期)
6. [数据库设计 (SQLite)](#6-数据库设计-sqlite)
7. [配置文件说明](#7-配置文件说明)
8. [API 接口文档](#8-api-接口文档)
9. [依赖关系](#9-依赖关系)
10. [项目运行方式](#10-项目运行方式)
11. [扩展指南](#11-扩展指南)
12. [已知限制与风险](#12-已知限制与风险)

---

## 1. 项目概述

本项目是一个面向 A 股场内 ETF 的趋势量化交易系统，核心围绕自定义 **Trend Score（趋势值）** 指标构建完整闭环。系统为单用户、本机运行、人工执行交易形态。

### 核心功能闭环

```
行情获取 → 本地存储 → 日内轮询信号计算 → 风险预算仓位建议 → 回测可视化 → 手工成交回写
```

### 关键设计原则

- **策略引擎解耦**: 策略与信号引擎分离,支持多策略扩展
- **数据源可插拔**: `IDataProvider` 抽象接口 + 优先级降级链
- **人工执行交易**: 系统只负责信号与建议,不进行自动下单
- **全流程日志留痕**: SQLite + JSONL 双轨记录,支持回溯审计

---

## 2. 整体架构

### 2.1 架构分层图

```
┌─────────────────────────────────────────────────────────────┐
│                     Web 前端层 (Jinja2 + ECharts)            │
│                FastAPI Routers (REST API)                    │
├─────────────────────────────────────────────────────────────┤
│                   应用生命周期 (FastAPI lifespan)             │
│          SchedulerManager (APScheduler 定时任务)             │
├──────────┬───────────────┬───────────────┬──────────────────┤
│ Signal   │   Portfolio   │   Backtest    │    Notify         │
│ Engine   │   Service     │   Engine      │    (飞书/邮件)    │
├──────────┼───────────────┼───────────────┼──────────────────┤
│ Strategy Layer (Trend Score / Momentum TopN)                │
├─────────────────────────────────────────────────────────────┤
│                    Data Layer                                │
│   DataService → TickFlowProvider                              │
│   MarketStore (SQLite) / RuntimeStore (JSON)                 │
├─────────────────────────────────────────────────────────────┤
│                    Storage: SQLite (trend_quant.db)          │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 模块依赖图

```
app/main.py  (FastAPI + lifespan)
    ├── core/settings.py       (配置加载)
    ├── core/scheduler.py      (定时任务管理)
    ├── data/storage/db.py     (SQLite 数据库单例)
    ├── engine/signal_engine.py (信号引擎)
    │     ├── data/service.py  (数据服务)
    │     ├── strategy/        (策略实现)
    │     ├── portfolio/       (仓位+风控)
    │     └── audit/           (日志)
    ├── backtest/backtest_engine.py (回测引擎)
    │     ├── strategy/        (策略实现)
    │     ├── portfolio/risk_sizer.py
    │     └── backtest/metrics.py
    └── app/routers/           (Web API路由)
```

---

## 3. 项目目录结构

```
tread quant/
├── .agents/skills/              # AI协作技能定义
├── .claude/                     # Claude Code 配置
├── .codex/                      # Codex 环境配置
├── config/                      # 配置文件目录
│   ├── app.yaml                 # 应用运行配置
│   ├── instruments.yaml         # 标的池与标的级参数
│   └── strategy.yaml            # 策略参数配置
├── data/
│   ├── market/etf/              # ETF行情数据（旧Parquet格式）
│   └── runtime/                 # 运行时数据（JSON）
│       └── advice/              # 操作建议文件
├── docs/                        # 项目文档
│   ├── dev/                     # 开发规范文档
│   ├── plans/                   # 设计规划文档
│   ├── PRD-trend-etf-system-v1.md
│   └── 项目说明文档.md
├── param optim result/          # 参数优化结果
├── scripts/                     # 脚本工具
│   ├── deploy.sh
│   ├── import_industry_etfs.py
│   ├── migrate_json_to_sqlite.py
│   └── run_dev.ps1              # Windows开发启动脚本
├── src/                         # 源代码主目录
│   ├── app/                     # Web应用层
│   ├── audit/                   # 审计日志
│   ├── backtest/                # 回测引擎
│   ├── core/                    # 核心配置与调度
│   ├── data/                    # 数据提供与存储
│   ├── engine/                  # 信号引擎
│   ├── notify/                  # 通知模块
│   ├── portfolio/               # 组合与风控
│   └── strategy/                # 策略实现
├── web/
│   ├── static/style.css
│   └── templates/               # Jinja2 页面模板
├── logs/
│   ├── app/app.log              # 应用日志
│   └── calc/calc.jsonl          # 计算明细日志
├── CLAUDE.md                    # AI协作指导
├── README.md                    # 项目说明
├── TODO.md                      # 待办事项
├── formula.txt                  # 算法公式
└── pyproject.toml               # Python项目配置
```

---

## 4. 核心模块详解

### 4.1 应用入口层 (app/)

#### 4.1.1 `src/app/main.py` — FastAPI 入口

整个系统的启动入口。通过 FastAPI `lifespan` 机制管理应用生命周期。

**关键代码结构：**

```python
# 生命周期管理
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. 创建数据目录
    Path("data").mkdir(exist_ok=True)
    # 2. 初始化 SQLite 数据库
    init_db()
    # 3. 创建信号引擎（注入数据源优先级和初始资金）
    signal_engine = SignalEngine(provider_priority=..., initial_capital=...)
    # 4. 创建调度管理器
    scheduler_manager = SchedulerManager(settings=settings)
    # 5. 注册三类定时任务
    scheduler_manager.start(poll_job=poll_job, final_job=final_job, update_job=update_job)
    # 6. 挂载到 app.state
    app.state.signal_engine = signal_engine
    app.state.scheduler_manager = scheduler_manager
    ...

app = FastAPI(title="Trend ETF System", version="0.1.0", lifespan=lifespan)
# 注册路由
app.include_router(overview.router)    # 系统概览
app.include_router(config.router)      # 配置管理
app.include_router(backtest.router)    # 回测中心
app.include_router(strategy_history.router)
app.include_router(trades.router)      # 手工成交
app.include_router(logs.router)        # 日志查询
app.include_router(instruments.router) # 标的管理
```

**三类定时任务：**

| 任务 | 触发时间 | 功能 |
|------|----------|------|
| `poll_job` | 10:00-14:30 每半小时 | 盘中轮询，计算各标的最新信号 |
| `final_job` | 14:45 | 终盘前最终信号（含重试机制） |
| `update_job` | 15:30 | 收盘后日线数据更新 |

#### 4.1.2 路由模块 (app/routers/)

| 文件 | 前缀 | 核心功能 |
|------|------|----------|
| [overview.py](file:///e:/codex%20project/tread%20quant/src/app/routers/overview.py) | `/` | 系统概览页 + `GET /api/overview` API |
| [config.py](file:///e:/codex%20project/tread%20quant/src/app/routers/config.py) | `/config` | 配置查看/更新 |
| [backtest.py](file:///e:/codex%20project/tread%20quant/src/app/routers/backtest.py) | `/backtest` | 回测运行/列表/结果 + 参数优化 |
| [trades.py](file:///e:/codex%20project/tread%20quant/src/app/routers/trades.py) | `/trades` | 手工成交录入、组合快照 |
| [logs.py](file:///e:/codex%20project/tread%20quant/src/app/routers/logs.py) | `/logs` | 计算日志查询 |
| [instruments.py](file:///e:/codex%20project/tread%20quant/src/app/routers/instruments.py) | `/instruments` | 标的管理接口 |
| [strategy_history.py](file:///e:/codex%20project/tread%20quant/src/app/routers/strategy_history.py) | `/strategy-history` | 策略历史 |

#### 4.1.3 `src/app/instrument_display.py` — 标的显示工具

```python
def build_symbol_display(symbol: str, name_map: dict) -> str
def format_symbol_display(symbol: str, name: str) -> str
def strip_etf_suffix(name: str) -> str
def load_instrument_name_map() -> dict
```

提供标的代码与名称的格式化显示，加载名称映射表。

---

### 4.2 核心配置层 (core/)

#### 4.2.1 `src/core/settings.py` — 配置加载

**Settings 数据类体系：**

```python
@dataclass(slots=True)
class AppSettings:
    name: str                        # 应用名称
    timezone: str                    # 时区 (Asia/Shanghai)
    host: str                        # 绑定地址
    port: int                        # 监听端口
    data_provider_priority: list[str] # 数据源优先级
    polling_times: list[str]         # 盘中轮询时间
    final_signal_time: str           # 终盘信号时间
    update_time_after_close: str     # 收盘后更新时间
    market_fetch_retry_times: int    # 行情拉取重试次数
    market_fetch_retry_interval_seconds: int
    notify_retry_times: int
    notify_retry_interval_seconds: int
    lot_size: int                    # 交易整手大小 (100)

@dataclass(slots=True)
class RuntimeSettings:
    account_equity_default: float    # 默认账户权益
    ensure_dirs: bool

@dataclass(slots=True)
class LoggingSettings:
    level: str                       # 日志级别
    keep_forever: bool               # 日志永久保留

@dataclass(slots=True)
class Settings:
    app: AppSettings
    runtime: RuntimeSettings
    logging: LoggingSettings

load_settings(config_path: Path | None = None) -> Settings
```

从 `config/app.yaml` 加载并解析配置。

#### 4.2.2 `src/core/scheduler.py` — 定时任务管理

```python
@dataclass(slots=True)
class SchedulerManager:
    settings: Settings
    scheduler: BackgroundScheduler | None = None

    start(poll_job, final_job, update_job) -> None
    shutdown() -> None
    jobs_snapshot() -> list[dict[str, str]]
```

基于 APScheduler 的 `BackgroundScheduler`，使用 CronTrigger 在交易日 (Mon-Fri) 注册定时任务。

#### 4.2.3 `src/core/enums.py` — 枚举定义

```python
class SignalAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"

class SignalLevel(str, Enum):
    INFO = "INFO"      # 信息
    WARN = "WARN"      # 警告
    ACTION = "ACTION"  # 行动建议
    ERROR = "ERROR"    # 错误
```

#### 4.2.4 `src/core/calendar.py` — 交易日历

```python
def is_trading_day(day: date) -> bool
```

V1 版本简化为工作日判断 (Mon-Fri)。优先通过数据源 (`DataService.is_trading_day()`) 获取真实交易日历。

---

### 4.3 数据层 (data/)

#### 4.3.1 `src/data/provider_base.py` — 数据源协议

```python
class IDataProvider(Protocol):
    name: str
    fetch_daily_history(symbol, start, end, adjust) -> pd.DataFrame
    fetch_minute_history(symbol, period, count, adjust) -> pd.DataFrame
    fetch_latest_quote(symbol) -> dict              # {symbol, price, ts, name, open, high, low, volume, amount}
    fetch_trading_calendar(start, end) -> list[date]
```

所有数据源必须实现的协议接口，支持 Duck Typing。

#### 4.3.2 数据源实现

| 文件 | 类 | 优先级 | 说明 |
|------|-----|--------|------|
| `provider_tickflow.py` | `TickFlowProvider` | 唯一 | TickFlow Starter 注册接口 |

#### 4.3.3 `src/data/service.py` — 数据服务编排层

```python
class DataService:
    providers: dict[str, IDataProvider]
    provider_priority: list[str]
    market_store: MarketStore
    runtime_store: RuntimeStore

    _ordered_providers() -> Generator         # 按优先级迭代数据源
    fetch_daily_history(symbol, start, end, adjust) -> pd.DataFrame
    fetch_minute_history(symbol, period, count, adjust) -> pd.DataFrame
    fetch_latest_quote(symbol) -> dict
    fetch_instrument_name(symbol) -> dict     # 获取标的名称
    is_trading_day(day) -> bool               # 判断交易日
    ensure_daily_history(symbol, start, end, adjust) -> dict  # 增量更新单标的历史
    backfill_daily_history(symbol, start, end, adjust) -> dict # 回填历史数据
    update_pool_daily(symbols, start, end, adjust) -> dict    # 批量更新标的池
```

核心功能：
- **优先级降级链**: 主源失败自动切换到备源
- **增量更新**: 对比本地已有数据，仅抓取缺失部分
- **去重合并**: 按时间去重排序后存储

#### 4.3.4 数据模型 `src/data/models.py`

```python
@dataclass(slots=True)
class InstrumentConfig:
    symbol: str
    enabled: bool
    risk_budget_pct: float      # 风险预算百分比
    stop_atr_mul: float         # 止损ATR倍数

@dataclass(slots=True)
class SignalSnapshot:
    ts: datetime
    symbol: str
    action: SignalAction
    level: SignalLevel
    trend_score: float
    price_direction: float
    confidence: float
    reason: str
    calc_details: dict

@dataclass(slots=True)
class ManualTradeRecord:
    trade_date: str
    symbol: str
    side: str                   # BUY / SELL
    qty: int
    price: float
    fee: float
    trade_time: str
    note: str
```

#### 4.3.5 存储层

**`src/data/storage/db.py`** — SQLite 数据库核心

```python
# 全局单例模式
_db_instance: Database | None = None

class Database:
    db_path: Path  # 默认 "data/trend_quant.db"

    _connect() -> contextmanager          # SQLite连接上下文管理
    _init_tables() -> None                # 建表（7张表 + 索引）

    # manual_trades
    add_trade(trade: dict) -> int
    get_trades_by_date(trade_date: str) -> list[dict]
    get_all_trades() -> list[dict]

    # signals
    save_signals(trade_day: str, payload: dict) -> None
    get_signals(trade_day: str) -> dict | None
    get_latest_signals(limit: int = 1) -> list[dict]
    list_signal_days() -> list[str]

    # signal_states
    save_signal_state(states: dict[str, dict]) -> None
    get_signal_state(symbol: str) -> dict | None
    get_all_signal_states() -> dict[str, dict]

    # position_snapshots
    save_position_snapshot(snapshot: dict) -> None
    get_latest_position_snapshot() -> dict | None

    # backtests
    save_backtest(run_id: str, result: dict) -> None
    get_backtest(run_id: str) -> dict | None
    list_backtests(limit: int = 40) -> list[dict]
    list_backtests_summary(limit: int = 40) -> list[dict]

    # optimization_jobs
    save_optimization_job(job_id, status, result) -> None
    get_optimization_job(job_id) -> dict | None
    get_optimization_status(job_id) -> dict | None
    get_optimization_result(job_id) -> dict | None

    # market_data
    save_market_data(symbol, df) -> None
    load_market_data(symbol) -> pd.DataFrame
    list_market_symbols() -> list[str]
    get_market_data_summary(symbol) -> dict

    # JSON → SQLite 迁移工具
    migrate_manual_trades_from_json(runtime_store) -> int
    migrate_signals_from_json(runtime_store) -> int
    migrate_signal_states_from_json(runtime_store) -> int
    migrate_position_snapshots_from_json(runtime_store) -> int
    migrate_backtests_from_json(runtime_store) -> int
    migrate_optimizations_from_json(runtime_store) -> int
    migrate_market_data_from_parquet(base_dir) -> int

# 初始化与获取
init_db(db_path="data/trend_quant.db") -> Database
get_db() -> Database   # 若未初始化则抛出 RuntimeError
```

**`src/data/storage/market_store.py`** — 行情存储封装

```python
class MarketStore:
    save_history(symbol, df) -> str       # 存入SQLite market_data表
    load_history(symbol) -> pd.DataFrame  # 从SQLite读取
    list_stored_symbols() -> list[str]    # 列出已存储标的
    path_for(symbol) -> Path              # 旧Parquet路径（兼容）
```

**`src/data/storage/runtime_store.py`** — JSON运行时存储

```python
class RuntimeStore:
    base_dir: Path  # 默认 "data/runtime"

    write_json(relative_path, payload) -> Path
    read_json(relative_path, default=None) -> Any
```

用于存储操作建议、审计日志等运行时JSON文件。

---

### 4.4 策略层 (strategy/)

#### 4.4.1 策略基础架构 `src/strategy/base.py`

**核心协议和基类：**

```python
# 决策结果数据类
@dataclass(frozen=True, slots=True)
class ExitDecision:
    triggered: bool
    reason: str
    scope: str       # "global" | "strategy"
    meta: dict

@dataclass(frozen=True, slots=True)
class EntryDecision:
    triggered: bool
    reason: str
    meta: dict

@dataclass(frozen=True, slots=True)
class FilterDecision:
    passed: bool
    reason: str
    filter_id: str
    meta: dict

# 规则协议
class ExitRule(Protocol):
    rule_id: str
    scope: str
    evaluate(symbol, signal, position, state, cfg) -> ExitDecision

class EntryFilter(Protocol):
    filter_id: str
    evaluate(signal, cfg) -> FilterDecision

class CrossSectionPlanner(Protocol):
    planner_id: str
    plan(day, signal_map, positions, cfg) -> dict

# 策略协议
class IStrategy(Protocol):
    name: str
    evaluate(symbol, bars, state, cfg) -> dict
    finalize_day(day, signal_map, positions, cfg) -> dict
    required_history_bars(cfg) -> int

# 抽象基类
class BaseStrategy(ABC):
    name = "base_strategy"

    compute_features(symbol, bars, state, cfg) -> dict        # 抽象方法
    evaluate(symbol, bars, state, cfg) -> dict                # 核心评估流程
    evaluate_entry_signal(signal, state, cfg) -> EntryDecision
    get_global_exit_rules(cfg) -> list[ExitRule]              # 全局止损规则
    get_strategy_exit_rules(cfg) -> list[ExitRule]            # 策略特有退出规则
    get_entry_filters(cfg) -> list[EntryFilter]               # 入场过滤器
    get_cross_section_planner(cfg) -> CrossSectionPlanner | None
    finalize_day(day, signal_map, positions, cfg) -> dict
    decorate_signal(signal, state, cfg) -> dict
    default_hold_reason(signal, state, cfg) -> str
    required_history_bars(cfg) -> int

# 工具函数
action_value(action) -> str
serialize_exit_decision(decision) -> dict
append_exit_decisions(signal, decisions) -> None
apply_exit_decisions(signal, state, decisions) -> bool
```

**`BaseStrategy.evaluate()` 核心流程：**

```
compute_features() → _signal_from_features()
    ├── position_qty > 0 (持仓)
    │   ├── 检查 global_exit_rules → 触发则 SELL (T+1检查sellable_qty)
    │   └── 检查 strategy_exit_rules → 触发则 SELL
    └── position_qty == 0 (空仓)
        ├── evaluate_entry_signal()
        │   └── 检查 entry_filters → 全部通过则 BUY
        └── decorate_signal() (修饰信号级别与理由)
```

#### 4.4.2 趋势值策略 `src/strategy/trend_score_strategy.py`

```python
class TrendScoreStrategy(BaseStrategy):
    name = "trend_score_v1"

    compute_features(symbol, bars, state, cfg) -> dict
    evaluate_entry_signal(signal, state, cfg) -> EntryDecision
    default_hold_reason(signal, state, cfg) -> str
    decorate_signal(signal, state, cfg) -> dict
```

**买入条件（全部满足）：**
1. 当前无持仓
2. `entry_threshold_min <= trend_score <= entry_threshold_max`
3. 当前价 > MA_mid

**预警信号：**
- `entry_watch`: trend_score 在阈值 80% 处接近但未触发
- `entry_overheat`: trend_score 超过上限

#### 4.4.3 Trend Score 计算引擎 `src/strategy/trend_score_core.py`

```python
safe_float(value, default=0.0) -> float
calculate_trend_score_snapshot(bars: pd.DataFrame, cfg: dict) -> dict
```

**算法公式：**

```
Trend Score = Price Direction × Confidence

Price Direction:
  Bias_n = (Close - MA_n) / ATR           (n ∈ {short, mid, long})
  Slope_n = (EMA_n(now) - EMA_n(prev)) / (ATR × n)
  bias_mix = Σ(w_bias × bias_n)
  slope_mix = Σ(w_slope × slope_n)
  norm_bias = tanh(bias_mix / 2) × 100
  norm_slope = tanh(slope_mix) × 100
  Price Direction = w_bias_norm × norm_bias + w_slope_norm × norm_slope

Confidence:
  vol_ratio = volume / MA(volume, 20)     (上限截断 3)
  volume_factor = vol_ratio / 3            (映射到 [0, 1])
  ER = Efficiency Ratio (10日)
  Confidence = volume_factor^w_vol × ER^w_er

Trend Score = clip(Price Direction × Confidence, -100, 100)
```

**返回的 calc_details 包含：**
price, ma_mid, atr, bias_short/mid/long, slope_short/mid/long, bias_mix, slope_mix, norm_bias, norm_slope, vol_ma, current_volume, vol_ratio, volume_factor, er

#### 4.4.4 技术指标 `src/strategy/indicators.py`

```python
atr(df: pd.DataFrame, period: int = 20) -> pd.Series
    # True Range → Rolling Mean(period)

efficiency_ratio(series: pd.Series, period: int = 10) -> pd.Series
    # |Close - Close_period_ago| / Σ(|ΔClose_i|)
```

#### 4.4.5 策略目录 `src/strategy/catalog.py`

系统支持 4 个策略ID的配置解析与目录构建：

| 策略ID | 名称 | 说明 |
|--------|------|------|
| `trend_score_v1` | Trend Score v1 | 单资产趋势值 + 止损规则 |
| `momentum_topn_v1` | Momentum TopN v1 | TopN动量排名, 周度调仓 |
| `momentum_topn_v2` | Momentum TopN v2 | v1 + MA20/MA60买入过滤 + MA退出 |
| `momentum_topn_v3` | Momentum TopN v3 | v2 + trend_score买入上限(≤20) |

```python
# 策略配置常量
TREND_STRATEGY_ID = "trend_score_v1"
MOMENTUM_STRATEGY_ID = "momentum_topn_v1"
MOMENTUM_STRATEGY_V2_ID = "momentum_topn_v2"
MOMENTUM_STRATEGY_V3_ID = "momentum_topn_v3"
MOMENTUM_STRATEGY_IDS = {MOMENTUM_STRATEGY_ID, MOMENTUM_STRATEGY_V2_ID, MOMENTUM_STRATEGY_V3_ID}

# 关键函数
normalize_strategy_id(raw_id, fallback) -> str
resolve_strategy_config(strategy_cfg, strategy_id, overrides) -> dict
    # 层级式配置合并: shared → v1 → v2 → v3 → overrides
build_strategy_catalog(strategy_cfg) -> dict
    # 构建完整的策略目录（含参数元数据）
```

#### 4.4.6 动量策略实现

| 文件 | 类 | 说明 |
|------|-----|------|
| [momentum_topn_strategy.py](file:///e:/codex%20project/tread%20quant/src/strategy/momentum_topn_strategy.py) | `MomentumTopNStrategy` | v1: TopN 动量+趋势混合排名,周度调仓 |
| [momentum_topn_v2_strategy.py](file:///e:/codex%20project/tread%20quant/src/strategy/momentum_topn_v2_strategy.py) | `MomentumTopNStrategyV2` | v2: 增加 MA20+MA60 买入过滤, MA退出 |
| [momentum_topn_v3_strategy.py](file:///e:/codex%20project/tread%20quant/src/strategy/momentum_topn_v3_strategy.py) | `MomentumTopNStrategyV3` | v3: 增加 trend_score ≤ 20 上限过滤 |

**动量排名核心逻辑（mom评分）：**

```
momentum_short = Close / Close[10天前] - 1
momentum_long  = Close / Close[20天前] - 1
momentum_mix = w_short × momentum_short + w_long × momentum_long
hybrid_score = hybrid_w_momentum × z_score(momentum_rank) + hybrid_w_trend × z_score(trend_rank)
```

#### 4.4.7 `src/strategy/features.py` — 特征构建

```python
# 趋势值特征 (用于 TrendScoreStrategy)
build_trend_score_features(bars, state, cfg) -> dict

# 动量特征 (用于 MomentumTopN 系列策略)
build_momentum_features(bars, state, cfg) -> dict
```

#### 4.4.8 `src/strategy/global_exit_rules.py` — 全局止损规则

```python
class HardStopExitRule(ExitRule):
    rule_id = "hard_stop"
    scope = "global"
    # price < hard_stop_price → 触发

class ChandelierStopExitRule(ExitRule):
    rule_id = "chandelier_stop"
    scope = "global"
    # price < chandelier_stop_price → 触发

build_global_exit_rules() -> list[ExitRule]
```

#### 4.4.9 `src/strategy/entry_filters.py` — 入场过滤器

```python
class PriceAboveMA20Filter(EntryFilter):   # filter_id = "price_above_ma20"
class PriceAboveMA60Filter(EntryFilter):   # filter_id = "price_above_ma60"
class TrendScoreMaxFilter(EntryFilter):    # filter_id = "trend_score_max"

build_entry_filters(filter_ids) -> list[EntryFilter]
```

#### 4.4.10 `src/strategy/planners.py` — 横截面调度器

```python
class WeeklyTopNRebalancePlanner(CrossSectionPlanner):
    planner_id = "weekly_topn_rebalance"
    plan(day, signal_map, positions, cfg) -> dict
    # 按 hybrid_score 排名, 选出 TopN, 计算 to_buy / to_sell
```

#### 4.4.11 `src/strategy/momentum_signal_modules.py` — 信号模块注册表

```python
# 买入过滤器注册
BUY_FILTER_REGISTRY = {"price_above_ma20", "price_above_ma60", "trend_score_max"}
DEFAULT_MOMENTUM_BUY_FILTERS = ["price_above_ma20"]

# 卖出信号注册
SELL_SIGNAL_REGISTRY = {"hard_stop", "chandelier_stop", "ma_breakdown_max"}
DEFAULT_MOMENTUM_SELL_SIGNALS = ["hard_stop", "chandelier_stop"]

normalize_signal_modules(raw, default) -> list[str]
```

#### 4.4.12 `src/strategy/strategy_exit_rules.py` — 策略级退出规则

实现 `ma_breakdown_max` 退出规则：价格跌破 `max(MA30, MA40, MA60)` 时触发卖出。

---

### 4.5 信号引擎层 (engine/)

#### 4.5.1 `src/engine/signal_engine.py` — 信号引擎

```python
class SignalEngine:
    runtime_store: RuntimeStore
    db: Database
    calc_logger: CalcLogger
    data_service: DataService
    strategy: TrendScoreStrategy
    portfolio_service: PortfolioService

    run_poll(trigger_name: str) -> dict
    run_daily_update() -> dict
    is_trading_day(day) -> bool
```

**`run_poll()` 执行流程：**

```
1. 交易日本地判断
2. 加载配置 (instruments.yaml + strategy.yaml)
3. 读取上次信号状态 (prev signal_states)
4. 从 manual_trades 重建仓位快照 → 获取现金/持仓/可卖数量
5. 遍历每个启用标的：
   ├── ensure_symbol_bars() 确保/拉取行情
   │   └── 融合最新盘中报价 (跳过 >30% 跳变)
   ├── _derive_stops() 计算硬止损/吊灯止损价格
   └── strategy.evaluate() 计算信号
6. 写入计算日志 (JSONL)
7. 更新 signal_states
8. RiskSizer 风险预算计算 → 仓位建议
   ├── suggest_qty (ATR风控单位)
   ├── cap_qty_by_max_cost (单标的上限)
   └── scale_allocations (超现金时等比压缩)
9. 写入 signals 表 → 返回信号快照
```

**异常保护：**
- 盘中价相对昨收跳变 >30% 的行情被忽略，避免污染信号状态
- 终盘信号 (14:45) 数据不可用时支持重试（次数/间隔可配置）

#### 4.5.2 `src/engine/run_context.py` — 运行上下文

```python
@dataclass(slots=True)
class RunContext:
    run_id: str
    trigger: str
    started_at: datetime
```

---

### 4.6 组合与风控层 (portfolio/)

#### 4.6.1 `src/portfolio/service.py` — 组合服务

```python
class PortfolioService:
    load_manual_trades() -> list[dict]          # 从SQLite加载所有手工成交
    build_snapshot(as_of_date, initial_capital) -> dict
        # 按时间顺序回放所有成交 (FIFO lot)
        # 计算: cash, positions(含sellable_qty), trade_count
        # 写入 position_snapshots 表
    estimate_equity(snapshot, price_map) -> float  # 估算当前权益
```

**关键逻辑：**
- **FIFO 出库**: SELL 按先入先出消耗 BUY lot
- **T+1 限制**: `sellable_qty` 只计算 `buy_date < as_of_date` 的 lot（当天买入不可卖）

#### 4.6.2 `src/portfolio/risk_sizer.py` — 风险头寸计算器

```python
class RiskSizer:
    lot_size: int  # 默认 100

    suggest_qty(equity, risk_budget_pct, atr_value, stop_mul) -> int
        # qty = floor((equity × risk_budget_pct) / (atr × stop_mul) / lot_size) × lot_size

    cap_qty_by_max_cost(qty, price, max_cost, fee_rate, fee_min, slippage) -> int
        # 限制单标的买入成本不超过 max_cost
    
    scale_allocations(allocations, total_cash) -> list[dict]
        # 当总候选成本 > total_cash 时，按比例等比缩放所有买入量
```

**头寸计算公式：**

```
每股风险 = ATR × stop_atr_mul
风险预算 = equity × risk_budget_pct
理论股数 = risk_budget / 每股风险  → 整手向下取整
```

#### 4.6.3 `src/portfolio/position_state.py` — 持仓状态

定义持仓的核心数据结构和状态管理逻辑。

#### 4.6.4 `src/portfolio/stops.py` — 止损计算

计算硬止损 (hard_stop) 和吊灯止损 (chandelier_stop) 价格。

#### 4.6.5 `src/portfolio/execution_rules.py` — 执行规则

定义买卖执行的规则逻辑（如 T+1 约束、整手约束等）。

---

### 4.7 回测层 (backtest/)

#### 4.7.1 `src/backtest/backtest_engine.py` — 回测引擎

```python
class BacktestEngine:
    DEFAULT_BENCHMARK_SYMBOL = "512500.SS"
    strategies: dict[str, BaseStrategy]  # 策略注册表

    run(payload, strategy_id, strategy_overrides, instrument_overrides,
        persist=True, include_charts=True, include_trades=True) -> dict
```

**`run()` 执行流程：**

```
1. 解析策略ID → 选择策略实现
2. 解析配置 (策略 + 标的 + 应用)
3. 加载行情数据 → 构建合并时间线
4. 日频遍历时间线 (Day Loop):
   ├── 每个标的计算当日信号
   ├── 动量策略: finalize_day() 判断是否为调仓日
   ├── 执行卖出 (先卖后买):
   │   ├── 确定卖出参考价 (止损触发→止损价, 否则收盘价)
   │   ├── 扣除滑点/手续费 → 更新现金
   │   └── 记录交易 (含盈亏)
   ├── 构建买入候选列表:
   │   ├── 趋势模式: 每标的独立信号
   │   │   ├── suggest_qty → cap_qty_by_max_cost → scale_allocations
   │   └── 动量模式: hybrid_score排名 TopN
   │       └── scale_allocations
   ├── 执行买入 → 更新持仓/现金 → 记录交易
   └── 计算当日权益 → 更新趋势历史/持仓历史
5. 计算基准收益 (等权池 or 单标的)
6. 计算策略指标 + 图表数据
```

**回测输出结构：**

```python
{
    "run_id": str,
    "status": "ok" | "no_instruments" | "no_market_data" | "no_data_in_range",
    "summary": {           # 策略指标
        "total_return", "annual_return", "max_drawdown",
        "sharpe", "sortino", "calmar", "win_rate",
        "profit_factor", "turnover", "trade_count"
    },
    "benchmark_summary": { ... },
    "annual_returns": [],  # 年度收益表
    "monthly_heatmap": {}, # 月度热力图
    "symbol_stats": [],    # 标的维度统计
    "trades": [],          # 逐笔交易
    "charts": {
        "dates", "nav", "benchmark_nav", "drawdown",
        "buy_points", "sell_points",
        "trend": {"dates", "series"},     # 多标趋势值曲线
        "holdings": {"dates", "order", "series"},  # 持仓堆叠
        "kline": {symbol: {...}},          # 每标的K线+MA
        "ranking": { ... }                 # 动量排名 (仅动量策略)
    }
}
```

#### 4.7.2 `src/backtest/metrics.py` — 指标计算

```python
compute_drawdown(daily_nav) -> list[float]
compute_metrics(daily_nav, trades, turnover_total) -> dict
    # total_return, annual_return, max_drawdown, sharpe,
    # sortino, calmar, win_rate, profit_factor, turnover, trade_count
compute_annual_returns(daily_nav, trades, benchmark_daily_nav) -> list[dict]
compute_monthly_heatmap(daily_nav) -> dict
compute_symbol_trade_stats(trades, symbols) -> list[dict]
    # 每标的: trade_count, win_rate, profit_factor, contribution
```

#### 4.7.3 `src/backtest/benchmark.py` — 基准计算

```python
equal_weight_pool_benchmark(market_data, timeline, initial_capital, lot_size) -> dict
    # 等权池基准: 初始等权分配，按整手买入并持有

single_symbol_benchmark(benchmark_data, timeline, initial_capital, lot_size, symbol) -> dict
    # 单标的基准: 指定标的买入并持有
```

#### 4.7.4 参数优化模块

| 文件 | 类/函数 | 说明 |
|------|---------|------|
| [optimization_engine.py](file:///e:/codex%20project/tread%20quant/src/backtest/optimization_engine.py) | `OptimizationEngine` | 网格搜索/LOO交叉验证 |
| [optimization_manager.py](file:///e:/codex%20project/tread%20quant/src/backtest/optimization_manager.py) | `OptimizationJobManager` | 优化任务管理(启动/查询/取消) |

---

### 4.8 通知层 (notify/)

```python
# notify/base.py
class INotifier(Protocol):
    channel: str
    send(level: str, title: str, content: str, context: dict | None = None) -> bool
```

| 文件 | 类 | 状态 |
|------|-----|------|
| `feishu_notifier.py` | `FeishuNotifier` | 桩实现(当前写日志) |
| `email_notifier.py` | `EmailNotifier` | 桩实现(当前写日志) |

---

### 4.9 审计日志层 (audit/)

#### `src/audit/app_logger.py` — 应用日志

```python
setup_logging(level: str) -> None          # 配置日志
get_logger(name: str) -> logging.Logger    # 获取logger (输出到 logs/app/app.log)
```

#### `src/audit/calc_logger.py` — 计算明细日志

```python
class CalcLogger:
    path: Path  # 默认 "logs/calc/calc.jsonl"

    log(payload: dict) -> None  # JSONL追加写入
```

---

### 4.10 前端模板层 (web/)

| 模板文件 | 对应页面 | 路由 |
|----------|----------|------|
| [overview.html](file:///e:/codex%20project/tread%20quant/web/templates/overview.html) | 系统概览 | `/` |
| [config.html](file:///e:/codex%20project/tread%20quant/web/templates/config.html) | 配置管理 | `/config` |
| [backtest.html](file:///e:/codex%20project/tread%20quant/web/templates/backtest.html) | 回测中心 | `/backtest` |
| [trades.html](file:///e:/codex%20project/tread%20quant/web/templates/trades.html) | 手工成交 | `/trades` |
| [logs.html](file:///e:/codex%20project/tread%20quant/web/templates/logs.html) | 日志查询 | `/logs` |
| [strategy_history.html](file:///e:/codex%20project/tread%20quant/web/templates/strategy_history.html) | 策略历史 | `/strategy-history` |
| [instruments.html](file:///e:/codex%20project/tread%20quant/web/templates/instruments.html) | 标的管理 | `/instruments` |
| [base.html](file:///e:/codex%20project/tread%20quant/web/templates/base.html) | 基础布局模板 | — |

前端技术栈：**Jinja2** 模板引擎 + **ECharts** 图表库 + 原生 CSS (`web/static/style.css`)

---

## 5. 数据流与生命周期

### 5.1 应用启动流程

```
app/main.py lifespan() 启动
    ├── init_db()                         # 初始化SQLite
    ├── SignalEngine(...)                 # 创建信号引擎
    ├── SchedulerManager.start(...)       # 注册定时任务
    │     ├── poll_job   (10:00-14:30 半小时)
    │     ├── final_job  (14:45 含重试)
    │     └── update_job (15:30 日线更新)
    └── 挂载到 app.state
```

### 5.2 盘中信号流程

```
定时触发 poll_job()
    ↓
SignalEngine.run_poll(trigger_name)
    ↓
[交易日判断] → 非交易日跳过
    ↓
[加载配置] instruments.yaml + strategy.yaml
    ↓
[仓位快照] PortfolioService.build_snapshot()
    │   回放 manual_trades → FIFO计算 持仓/现金/可卖数量
    ↓
[遍历标的]
    ├── MarketStore.load_history() → 本地行情
    ├── DataService.fetch_latest_quote() → 最新报价
    │   └── 融合: 忽略>30%跳变
    ├── _derive_stops() → 计算止损价
    └── strategy.evaluate() → 信号 + Trend Score
    ↓
[风险预算] RiskSizer
    ├── suggest_qty()         → 理论股数
    ├── cap_qty_by_max_cost() → 单标的上限
    └── scale_allocations()   → 超现金等比缩放
    ↓
[持久化] DB.save_signals() + DB.save_signal_state()
    ↓
[日志] CalcLogger.log() (JSONL)
```

### 5.3 回测数据流

```
用户发起 POST /backtest/api/run
    ↓
BacktestEngine.run(payload, strategy_id, overrides)
    ↓
[配置解析] strategy_cfg + instruments_cfg → resolve_strategy_config()
    ↓
[行情加载] MarketStore.load_history() → 构建日频时间线
    ↓
[Day Loop]
    ├── 每标的: strategy.evaluate() → signal_map
    ├── 动量: finalize_day() → rebalance plan
    ├── 执行卖出 (先卖后买)
    ├── 构建买入候选 → RiskSizer
    ├── 执行买入
    └── 记录: trades, daily_nav, holdings, trend_history
    ↓
[基准计算] benchmark.py → equal_weight_pool / single_symbol
    ↓
[指标计算] metrics.py → summary, annual, monthly, symbol_stats
    ↓
[返回结果 + 持久化] DB.save_backtest()
```

---

## 6. 数据库设计 (SQLite)

**数据文件**: `data/trend_quant.db`

### 6.1 表结构

#### `market_data` — 行情数据

| 列名 | 类型 | 说明 |
|------|------|------|
| symbol | TEXT | 标的代码 (PK) |
| time | TEXT | 交易日期 (PK) |
| open | REAL | 开盘价 |
| high | REAL | 最高价 |
| low | REAL | 最低价 |
| close | REAL | 收盘价 |
| volume | REAL | 成交量 |
| amount | REAL | 成交额 |
| provider | TEXT | 数据来源 |

主键: (symbol, time)  
索引: `idx_market_data_symbol_time`

#### `signals` — 信号记录

| 列名 | 类型 | 说明 |
|------|------|------|
| trade_day | TEXT | 交易日 (UNIQUE) |
| ts | TEXT | 时间戳 |
| trigger | TEXT | 触发类型 (poll_30m / final_1445) |
| status | TEXT | ok / partial_data_unavailable / data_unavailable |
| payload | TEXT | 完整JSON信号载荷 |
| created_at | TEXT | 创建时间 |

索引: `idx_signals_day`

#### `signal_states` — 最新信号状态

| 列名 | 类型 | 说明 |
|------|------|------|
| symbol | TEXT | 标的代码 (PK) |
| trend_score | REAL | 当前趋势值 |
| prev_trend_score | REAL | 前次趋势值 |
| position_qty | INTEGER | 持仓数量 |
| updated_at | TEXT | 更新时间 |

#### `manual_trades` — 手工成交记录

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 自增主键 |
| trade_date | TEXT | 交易日期 |
| symbol | TEXT | 标的代码 |
| side | TEXT | BUY / SELL |
| qty | INTEGER | 成交数量 |
| price | REAL | 成交价格 |
| fee | REAL | 手续费 |
| trade_time | TEXT | 成交时间 |
| note | TEXT | 备注 |
| created_at | TEXT | 创建时间 |

索引: `idx_trades_date`, `idx_trades_symbol`

#### `position_snapshots` — 仓位快照

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 自增主键 |
| as_of_date | TEXT | 快照日期 |
| cash | REAL | 现金 |
| positions | TEXT | 持仓JSON |
| trade_count | INTEGER | 成交笔数 |
| created_at | TEXT | 创建时间 |

#### `backtests` — 回测结果

| 列名 | 类型 | 说明 |
|------|------|------|
| run_id | TEXT | 回测ID (PK) |
| status | TEXT | ok / 错误状态 |
| strategy_id | TEXT | 策略ID |
| start_date | TEXT | 回测起始 |
| end_date | TEXT | 回测结束 |
| total_return | REAL | 总收益 |
| win_rate | REAL | 胜率 |
| profit_factor | REAL | 盈亏比 |
| sharpe | REAL | 夏普比率 |
| trade_count | INTEGER | 交易次数 |
| timeline_days | INTEGER | 时间线天数 |
| summary | TEXT | 摘要JSON |
| meta | TEXT | 元数据JSON |
| input | TEXT | 输入JSON |
| result_json | TEXT | 完整结果JSON |
| created_at | TEXT | 创建时间 |

索引: `idx_backtests_created`

#### `optimization_jobs` — 参数优化任务

| 列名 | 类型 | 说明 |
|------|------|------|
| job_id | TEXT | 任务ID (PK) |
| status | TEXT | 运行状态 |
| progress | TEXT | 进度JSON |
| summary | TEXT | 摘要JSON |
| current | TEXT | 当前进度JSON |
| result | TEXT | 结果JSON |
| created_at | TEXT | 创建时间 |
| finished_at | TEXT | 完成时间 |

---

## 7. 配置文件说明

### 7.1 `config/app.yaml` — 应用配置

```yaml
app:
  name: trend-etf-system                       # 应用名称
  timezone: Asia/Shanghai                      # 时区
  host: 127.0.0.1                              # 绑定地址
  port: 8000                                   # 监听端口
  data_provider_priority:                      # 数据源优先级
    - tickflow
  polling_times:                               # 盘中轮询时间 (工作日)
    - "10:00"  - "10:30"  - "11:00"  - "11:30"
    - "13:00"  - "13:30"  - "14:00"  - "14:30"
  final_signal_time: "14:45"                   # 终盘前信号时间
  update_time_after_close: "15:30"             # 收盘后更新时间
  market_fetch_retry_times: 3                  # 行情拉取重试次数
  market_fetch_retry_interval_seconds: 20      # 重试间隔
  notify_retry_times: 2                        # 通知重试
  notify_retry_interval_seconds: 5
  lot_size: 100                                # A股整手大小

runtime:
  account_equity_default: 200000               # 默认账户权益
  ensure_dirs: true

logging:
  level: INFO                                  # 日志级别
  keep_forever: true                           # 日志永久保留
```

### 7.2 `config/instruments.yaml` — 标的池配置

```yaml
instruments:
  - symbol: 512800.SS    # 银行ETF
    name: 银行
    enabled: true
    risk_budget_pct: 0.01       # 风险预算 (1%)
    stop_atr_mul: 1.5           # 止损ATR倍数
  - symbol: 512000.SS    # 券商ETF
    ...
  # ... 共20个行业ETF
```

当前配置 20 个行业 ETF（银行、券商、房地产、消费、酒、医药、芯片、5G、游戏、新能源车、光伏、电池、煤炭、有色金属、钢铁、军工、基建50、农业等）。

### 7.3 `config/strategy.yaml` — 策略参数

```yaml
strategy:
  id: trend_score_v1
  adjust: qfq                    # 复权方式: 前复权
  lookback_days: 120             # 回看天数

  # Trend Score 核心参数
  n_short: 5                     # 短周期MA
  n_mid: 10                      # 中周期MA
  n_long: 20                     # 长周期MA
  w_bias_short: 0.4
  w_bias_mid: 0.4
  w_bias_long: 0.2
  w_slope_short: 0.4
  w_slope_mid: 0.4
  w_slope_long: 0.2
  w_bias_norm: 0.5               # Bias归一化权重
  w_slope_norm: 0.5              # Slope归一化权重
  vol_ma_period: 20              # 成交量MA周期
  er_period: 10                  # 效率比率周期
  w_vol: 0.3                     # 量能因子权重
  w_er: 0.7                      # 效率比率权重

  # 风控参数
  atr_period: 20
  hard_stop_atr_mul_default: 1.5
  chandelier_stop_atr_mul: 2.5

  # 入场参数
  entry_threshold_min: 10
  entry_threshold_max: 20

  # 成本参数
  fee_rate: 0.000085             # 手续费率
  fee_min: 5                     # 最低手续费
  slippage: 0.002                # 滑点

  # 回测默认参数
  backtest_start_primary: "2025-01-01"
  backtest_start_fallback: "2018-01-01"

  # 动量策略子配置
  momentum_topn: { ... }
  momentum_topn_v2: { ... }
  momentum_topn_v3: { ... }
```

---

## 8. API 接口文档

### 8.1 页面路由

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 系统概览页面 |
| GET | `/config` | 配置管理页面 |
| GET | `/backtest` | 回测中心页面 |
| GET | `/trades` | 手工成交录入页面 |
| GET | `/logs` | 日志查询页面 |
| GET | `/strategy-history` | 策略历史页面 |
| GET | `/instruments` | 标的管理页面 |

### 8.2 数据 API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/overview` | 系统概览数据（信号、持仓、回测列表） |
| GET | `/config/api/raw` | 获取原始配置 |
| POST | `/config/api/raw` | 更新配置 |
| POST | `/backtest/api/run` | 运行回测 |
| GET | `/backtest/api/list` | 回测列表摘要 |
| GET | `/backtest/api/{run_id}` | 回测详情 |
| POST | `/backtest/api/optimize/params` | 获取可优化参数 |
| POST | `/backtest/api/optimize/start` | 启动参数优化 |
| GET | `/backtest/api/optimize/{job_id}/status` | 优化任务状态 |
| POST | `/backtest/api/optimize/{job_id}/cancel` | 取消优化任务 |
| GET | `/backtest/api/optimize/{job_id}/result` | 优化任务结果 |
| GET | `/trades/api/manual?trade_date=YYYY-MM-DD` | 查询成交记录 |
| POST | `/trades/api/manual` | 录入手工成交 |
| GET | `/trades/api/portfolio?as_of_date=YYYY-MM-DD` | 仓位快照 |
| GET | `/logs/api/calc?limit=100` | 计算日志查询 |
| GET | `/instruments/api/summary` | 标的信息 |

---

## 9. 依赖关系

### 9.1 Python 依赖 (`pyproject.toml`)

| 包 | 版本 | 用途 |
|----|------|------|
| fastapi | >=0.116.0 | Web 框架 |
| uvicorn[standard] | >=0.35.0 | ASGI 服务器 |
| jinja2 | >=3.1.6 | 模板引擎 |
| apscheduler | >=3.11.0 | 定时任务调度 |
| pandas | >=2.3.0 | 数据处理 |
| numpy | >=2.2.0 | 数值计算 |
| pyyaml | >=6.0.2 | YAML 配置解析 |
| pyarrow | >=21.0.0 | Parquet 文件支持 |
| pydantic | >=2.11.0 | 数据验证 |
| email-validator | >=2.2.0 | 邮件验证 |
| httpx | >=0.28.0 | HTTP 客户端 |

### 9.2 构建配置

- Python 版本要求: `>=3.11`
- 构建系统: `setuptools>=80.0`
- 代码包路径: `src/` (通过 `package-dir = {"" = "src"}` 配置)
- Lint 工具: `ruff` (行宽 100)

### 9.3 模块间依赖关系图

```
app/main.py
 ├── core/settings.py       (无内部依赖)
 ├── core/calendar.py       (无内部依赖)
 ├── core/enums.py          (无内部依赖)
 ├── core/scheduler.py      → core/settings, audit/app_logger
 ├── data/storage/db.py     (无内部依赖, 仅标准库+sqlite3)
 ├── data/storage/market_store.py  → data/storage/db
 ├── data/storage/runtime_store.py (无内部依赖)
 ├── data/provider_base.py  (无内部依赖)
 ├── data/service.py        → data/provider_*, data/storage/*
 ├── data/models.py         → core/enums
 ├── strategy/indicators.py (无内部依赖, 仅pandas+numpy)
 ├── strategy/trend_score_core.py  → strategy/indicators
 ├── strategy/features.py   → strategy/trend_score_core
 ├── strategy/base.py       → core/enums
 ├── strategy/catalog.py    → strategy/momentum_signal_modules
 ├── strategy/global_exit_rules.py  → strategy/base, strategy/trend_score_core
 ├── strategy/trend_score_strategy.py → strategy/base, strategy/features, strategy/trend_score_core
 ├── strategy/momentum_topn_strategy.py → strategy/base, strategy/features, strategy/entry_filters, strategy/planners
 ├── engine/signal_engine.py → data/service, portfolio/*, strategy/trend_score_strategy, audit/*
 ├── engine/run_context.py  (无内部依赖)
 ├── portfolio/service.py   → data/storage/*
 ├── portfolio/risk_sizer.py (无内部依赖)
 ├── backtest/backtest_engine.py  → data/storage/*, portfolio/risk_sizer, strategy/*, backtest/benchmark, backtest/metrics
 ├── backtest/metrics.py    (无内部依赖, 仅pandas+numpy)
 ├── backtest/benchmark.py  (无内部依赖)
 ├── notify/base.py         (无内部依赖)
 ├── audit/app_logger.py    (无内部依赖, 仅标准库logging)
 └── audit/calc_logger.py   (无内部依赖)
```

---

## 10. 项目运行方式

### 10.1 环境准备 (Windows)

```powershell
# 1. 创建虚拟环境
python -m venv .venv

# 2. 安装依赖
.\.venv\Scripts\python.exe -m pip install -e .
```

### 10.2 启动开发服务器

```powershell
# 方式1: 使用脚本
.\scripts\run_dev.ps1

# 方式2: 绕过PowerShell执行策略
powershell -ExecutionPolicy Bypass -File .\scripts\run_dev.ps1

# 方式3: 手动启动
$env:PYTHONPATH = "src"
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

### 10.3 访问

浏览器打开: **http://127.0.0.1:8000**

### 10.4 代码检查

```powershell
ruff check src
```

---

## 11. 扩展指南

### 11.1 新增数据源

1. 创建 `src/data/provider_xxx.py`，实现 `IDataProvider` 协议的全部4个方法
2. 在 `DataService.providers` 字典中注册
3. 在 `config/app.yaml` 的 `data_provider_priority` 中配置优先级

### 11.2 新增策略

1. 创建策略类继承 `BaseStrategy`
2. 实现 `compute_features()` 和 `evaluate_entry_signal()`
3. 可选: 覆写 `get_entry_filters()`、`get_strategy_exit_rules()`、`get_cross_section_planner()`
4. 在 `BacktestEngine.strategies` 字典中注册
5. 在 `strategy/catalog.py` 中添加对应的策略ID和默认配置

### 11.3 新增通知通道

1. 创建类实现 `INotifier` 协议的 `send()` 方法
2. 在 `SignalEngine` 信号触发链路接入分级发送与失败重试
3. 将发送结果写入审计日志

### 11.4 新增标的

编辑 `config/instruments.yaml`，添加新条目：

```yaml
- symbol: XXXXXX.SS
  name: 标的名称
  enabled: true
  risk_budget_pct: 0.01
  stop_atr_mul: 1.5
```

---

## 12. 已知限制与风险

| 限制 | 说明 |
|------|------|
| 交易日历 | 主数据源不可用时退化为工作日判断, 存在节假日误判风险 |
| 通知推送 | 飞书/邮件通知当前为日志桩实现, 未接入真实通道 |
| 自动下单 | 系统仅输出信号和建议, 不执行自动交易 |
| 多账户 | 仅单账户场景, 未做多账户隔离 |
| 单元测试 | 当前无测试套件 |

---

> **文档维护说明**: 本文档基于代码基线 `2026-05-24` 生成。当代码有重大变更时, 请同步更新此文档。
