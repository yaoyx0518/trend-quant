# 开发文档：趋势交易量化系统 V1（技术方案）

## 1. 技术栈与原则

- 后端：Python 3.11+
- Web：FastAPI + Jinja2
- 调度：APScheduler
- 数据处理：pandas / numpy
- 可视化：前端图表库（建议 ECharts）
- 存储：Parquet + JSON + 日志文件
- 原则：接口分层、配置驱动、可替换实现、可审计

## 2. 代码结构建议

```text
src/
  app/
    main.py
    routers/
      overview.py
      config.py
      backtest.py
      trades.py
      logs.py
  core/
    settings.py
    scheduler.py
    calendar.py
    enums.py
  data/
    models.py
    provider_base.py
    provider_efinance.py
    provider_akshare.py
    storage/
      market_store.py
      runtime_store.py
  strategy/
    base.py
    trend_score_strategy.py
    indicators.py
  portfolio/
    position_state.py
    risk_sizer.py
    execution_rules.py
  engine/
    signal_engine.py
    run_context.py
  backtest/
    backtest_engine.py
    metrics.py
    benchmark.py
  notify/
    base.py
    feishu_notifier.py
    email_notifier.py
  audit/
    calc_logger.py
    app_logger.py
web/
  templates/
  static/
config/
  app.yaml
  strategy.yaml
  instruments.yaml
data/
  market/etf/*.parquet
  runtime/*.json
logs/
```

## 3. 关键接口设计

### 3.1 IDataProvider

```python
class IDataProvider(Protocol):
    def fetch_daily_history(self, symbol: str, start: date, end: date, adjust: str) -> pd.DataFrame: ...
    def fetch_minute_history(self, symbol: str, period: str, count: int, adjust: str) -> pd.DataFrame: ...
    def fetch_latest_quote(self, symbol: str) -> dict: ...
    def fetch_trading_calendar(self, start: date, end: date) -> list[date]: ...
```

实现：

- `EfinanceProvider`（主）
- `AkshareProvider`（备）

### 3.2 IStrategy

```python
class IStrategy(Protocol):
    name: str
    def evaluate(self, symbol: str, bars: pd.DataFrame, state: dict, cfg: dict) -> dict: ...
```

返回：

- `trend_score`
- `signal` (`BUY/SELL/HOLD`)
- `level` (`INFO/WARN/ACTION`)
- `calc_details`（完整中间值）
- `risk_suggestion`

### 3.3 IPositionSizer

```python
class IPositionSizer(Protocol):
    def suggest_qty(self, equity: float, risk_budget_pct: float, atr: float, stop_mul: float, price: float) -> int: ...
    def scale_allocations(self, suggestions: list[dict], total_cash: float) -> list[dict]: ...
```

### 3.4 INotifier

```python
class INotifier(Protocol):
    def send(self, level: str, title: str, content: str, context: dict) -> bool: ...
```

## 4. 配置定义

### 4.1 `config/instruments.yaml`

- ETF列表、开关、风险预算、止损倍数（可覆盖默认）

### 4.2 `config/strategy.yaml`

- 趋势策略参数、买卖阈值、ATR/ER窗口
- 交易成本参数
- 回测默认区间与资金

### 4.3 `config/app.yaml`

- 调度时点
- 通知渠道开关
- 日志级别
- 数据源优先级（efinance -> akshare）

## 5. 运行时状态与文件

- `data/runtime/positions/current_positions.json`
- `data/runtime/signals/YYYY-MM-DD.json`
- `data/runtime/advice/YYYY-MM-DD.json`
- `data/runtime/trades/manual_trades_YYYY-MM-DD.json`
- `data/runtime/backtests/{run_id}/result.json`
- `logs/app/YYYY-MM-DD.log`
- `logs/calc/YYYY-MM-DD.jsonl`

说明：

- `positions` 是“事实仓位”（由手工成交回写驱动）
- `advice` 是系统建议，不直接覆盖事实仓位

## 6. 调度任务设计

- `job_market_poll_30m`
  - 交易日整点/半点执行
  - 生成INFO/WARN，必要时ACTION-SELL
- `job_final_signal_1445`
  - 14:45执行
  - 失败重试3次（20秒）
  - 成功后发送最终ACTION
- `job_daily_update_after_close`
  - 收盘后执行
  - 增量更新Parquet

## 7. 通知格式

统一消息体：

- 标题：`[LEVEL][symbol][strategy]`
- 主体：
  - 当前价格、trend score、关键分量
  - 信号原因（命中条件）
  - 建议数量、风险预算占用
  - 止损价、T+1限制提示
  - 任务时间戳与trace_id

失败处理：

- 单渠道失败重试2次（5秒）
- 双渠道都失败写 `ERROR` 日志

## 8. 回测引擎

输入：

- 标的池
- 参数快照
- 时间区间
- 初始资金

流程：

1. 读取Parquet日线数据
2. 按日循环计算趋势信号
3. 按收盘价模拟成交并计入成本
4. 执行T+1约束
5. 记录逐笔交易和每日净值
6. 生成指标与图表数据

输出：

- `summary.json`
- `daily_nav.csv/parquet`
- `trades.json`
- `charts_payload.json`

## 9. 前端页面与接口

### 9.1 概览

- 最新信号
- 当前持仓
- 调度任务状态
- 最近错误告警

### 9.2 配置页

- ETF池与参数编辑
- 保存即生效

### 9.3 回测页

- 发起回测
- 指标卡
- 净值/回撤/标的贡献图
- 买卖点标注
- 时间轴联动缩放
- 年度表 + 月度热力图

### 9.4 成交回写页

- 新增/编辑/删除当日成交
- 回写后更新事实仓位

### 9.5 日志审计页

- 条件筛选
- 查看单次决策完整中间过程

## 10. API 草案

- `GET /api/overview`
- `GET /api/signals/latest`
- `POST /api/config/save`
- `POST /api/backtest/run`
- `GET /api/backtest/{run_id}`
- `POST /api/trades/manual`
- `GET /api/trades/manual?date=YYYY-MM-DD`
- `GET /api/logs/calc`

## 11. 测试计划

- 单元测试：指标计算、信号触发、仓位缩放、T+1
- 集成测试：调度任务链路、通知重试、成交回写一致性
- 回归测试：固定样本区间下指标与交易笔数稳定
- 容错测试：数据源失败、通知失败、文件损坏恢复

## 12. 发布计划

### 12.1 第一阶段（纯Python）

- `venv` 安装依赖
- 启动 FastAPI + APScheduler
- 本机常驻运行

### 12.2 第二阶段（Docker）

- 编写 `Dockerfile` + `docker-compose.yml`
- 挂载 `config/data/logs`
- 提供一键启动命令

## 13. 扩展位（已预留）

- 策略：`MomentumTopNStrategy`
- 通知：盘前/盘后汇总任务
- 分析：LLM API报告生成
- 存储：后续可替换为PostgreSQL（通过storage adapter）
