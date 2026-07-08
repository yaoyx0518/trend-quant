# 可配置化规则回测模块设计

- 日期：2026-07-08
- 范围：新建一套规则条件驱动的单标的全仓回测模块
- 目标：让策略可以通过结构化配置定义，不依赖用户编写 Python、DSL 或公式文本

## 1. 背景与目标

当前项目已有回测模块承担了组合、仓位、策略、图表、持久化等多重职责。新的可配置化回测模块不考虑兼容旧实现，优先追求结构清晰、可测试、可扩展。

本模块里，“策略”的定义收窄为：

- 一组买入条件
- 一组卖出条件
- 空仓时买入条件全部满足则全仓买入
- 持仓时卖出条件全部满足则全仓卖出

第一阶段不处理多标的组合配置、风险预算、仓位管理、TopN 轮动、资金分配等问题。回测结果聚焦单标的一段时间表现，包括累计收益、年化收益、最大回撤、胜率、盈亏比、夏普比率、交易明细等。

## 2. 设计原则

1. 不做公式解析器。
   用户不写 `CLOSE >= SMA(20)` 这样的文本表达式，系统也不解析公式字符串。

2. 使用结构化条件模型。
   条件左右两侧都是 `ValueSpec`，由类型、名称和参数构成。未来 Web 表单可以自然生成这些结构。

3. 指标与状态值分离。
   `SMA`、`EMA`、`RSI`、`MACD` 是普通指标；`HARD_STOP`、`CHANDELIER_STOP` 是持仓状态值。两者都能作为条件里的数值，但生命周期不同。

4. 指标注册表负责可发现、可校验、可演进。
   前端通过注册表知道有哪些值可选、需要哪些参数、默认值是什么。

5. 配置先归一化再执行。
   执行器不直接读取原始配置。所有策略配置先经过校验、默认值补全和版本迁移。

6. 第一阶段条件组只开放 AND。
   数据结构保留 `all/any/group` 扩展能力，但 UI 和执行器第一阶段只需要稳定支持 `all`。

## 3. 新模块边界

建议新增目录：

```text
src/rule_backtest/
  __init__.py
  models.py
  registry.py
  indicators.py
  value_resolver.py
  condition_engine.py
  state_values.py
  engine.py
  validators.py
  metrics.py
```

职责划分：

- `models.py`：定义策略、条件、数值、交易、持仓、回测结果等数据模型。
- `registry.py`：定义指标注册表、参数元数据、版本信息。
- `indicators.py`：实现普通指标计算。
- `state_values.py`：实现依赖持仓状态的值，如硬止损和吊灯止损。
- `value_resolver.py`：在某个交易日把 `ValueSpec` 解析成具体数值。
- `condition_engine.py`：执行 `>=`、`<=` 和条件组判断。
- `engine.py`：单标的全仓买卖回测主循环。
- `validators.py`：策略配置校验、默认值补全、版本迁移。
- `metrics.py`：回测指标统计。

## 4. 策略配置模型

策略配置示例：

```yaml
schema_version: 1
id: trend_macd_stop_v1
name: Trend + MACD + Stop
trade_mode: single_symbol_all_in

entry:
  type: group
  combinator: all
  children:
    - type: condition
      left:
        type: price
        field: close
      operator: ">="
      right:
        type: indicator
        name: sma
        params:
          field: close
          period: 20

    - type: condition
      left:
        type: indicator
        name: macd_histogram
        params:
          field: close
          fast_period: 12
          slow_period: 26
          signal_period: 9
      operator: ">="
      right:
        type: literal
        value: 0

exit:
  type: group
  combinator: all
  children:
    - type: condition
      left:
        type: price
        field: close
      operator: "<="
      right:
        type: state_value
        name: chandelier_stop
        params:
          atr_period: 20
          atr_mul: 2.5
```

第一阶段支持的比较符：

- `>=`
- `<=`

后续可以增加：

- `>`
- `<`
- `==`
- `!=`
- `between`
- `cross_above`
- `cross_below`

## 5. ValueSpec 模型

条件左右两侧统一为 `ValueSpec`。

第一阶段支持：

```text
price         行情字段，如 close/high/low/open/volume/amount
literal       常数
indicator     普通指标
state_value   持仓状态值
```

示例：

```yaml
type: price
field: close
```

```yaml
type: indicator
name: rsi
params:
  field: close
  period: 14
```

第一阶段不开放算术表达式，也不支持任意指标嵌套。

例如 `trend_score` 的 5 日均线不表达为 `sma(source=trend_score, period=5)`，而是注册为独立指标：

```yaml
type: indicator
name: trend_score_sma
params:
  period: 5
```

这样未来 Web 弹窗只需要让用户选择一个明确的值并填写参数，不需要理解指标组合或表达式树。后续如果确实需要 `arithmetic`，可以在 `ValueSpec` 类型中补回，不影响当前模型。

## 6. 指标注册表

指标注册表示例：

```yaml
id: sma
version: 1
label: 简单移动平均
category: trend
output_type: number
params:
  field:
    type: price_field
    required: false
    default: close
  period:
    type: int
    required: true
    min: 1
```

`category` 只用于 UI 分组、筛选和文档展示，例如 `trend`、`momentum`、`volatility`、`volume`、`state`。它不参与执行逻辑，执行器只认 `id` 和参数 schema。

`output_type` 用于声明该值的输出类型，方便后续做条件校验和 UI 控件选择。第一阶段可用于条件左右两侧比较的值都必须是 `number` 或可空的 `number`。当前第一批指标没有非 number 输出；保留该字段是为了以后支持 `boolean`、`enum`、`date` 或其它非比较型值时不推翻注册表结构。

注册表用途：

- 给前端提供可选值列表。
- 描述每个指标需要哪些参数。
- 在保存策略前做校验。
- 在执行前补默认值。
- 管理指标参数演进。

## 7. 参数演进与可维护性

配置化系统必须避免“指标新增参数后，历史策略全部失效”。

处理规则：

1. 指标参数尽量可默认。
   例如 `sma` 如果新增 `min_periods`，应提供默认值。

2. 执行前统一归一化。
   `normalize_strategy_config()` 根据注册表补齐缺失参数。

3. 保存配置时记录版本。
   策略记录 `schema_version`，指标记录 `indicator_version`。

4. 破坏性变更使用新指标或新 major 版本。
   例如如果算法含义发生变化，不直接改 `sma@1`，而是新增 `sma@2` 或一个新指标。

5. 提供迁移报告。
   校验时返回哪些参数是默认补齐的、哪些指标版本已过时、哪些策略需要人工确认。

`SMA` 和 `EMA` 分别作为独立指标维护，不使用 `ma(method=...)` 这种混合设计。

## 8. 第一批指标清单

### 8.1 基础行情值

- `open`
- `high`
- `low`
- `close`
- `volume`
- `amount`
- `literal`

### 8.2 趋势与均线

- `sma`
- `ema`
- `bias`
- `bias_atr_normed`
- `trend_score`
- `trend_score_sma`
- `trend_score_ema`
- `price_direction`
- `trend_confidence`

命名规范：基础指标名放在最前面，演变体或标准化方式放在后面，例如 `bias`、`bias_atr_normed`、`trend_score`、`trend_score_sma`。

`bias` 定义为基础行情字段相对自身均线的偏离：

```text
(field - sma(field, period)) / sma(field, period)
```

`bias_atr_normed` 定义为经过 ATR 标准化后的偏离：

```text
(field - sma(field, period)) / atr(atr_period)
```

### 8.3 动量

- `momentum_return`
- `rsi`
- `macd_line`
- `macd_signal`
- `macd_histogram`

MACD 三个值在注册表中作为三个独立可选值暴露，底层实现可以共享计算缓存。

默认参数：

```text
fast_period = 12
slow_period = 26
signal_period = 9
```

### 8.4 波动与通道

- `atr`
- `bollinger_upper`
- `bollinger_middle`
- `bollinger_lower`

Bollinger 默认参数：

```text
period = 20
std_mul = 2.0
```

三个布林值对用户作为独立值暴露，底层可共享计算。

### 8.5 成交量

- `volume_sma`
- `volume_ratio`

`volume_ratio` 定义为：

```text
volume / sma(volume, period)
```

### 8.6 状态值

- `entry_price`
- `hard_stop`
- `highest_high_since_entry`
- `chandelier_stop`

## 9. 状态值设计

状态值不等同于普通指标。

### 9.1 hard_stop

硬止损在买入成交时初始化：

```text
hard_stop = entry_price - atr_at_entry * atr_mul
```

配置示例：

```yaml
type: state_value
name: hard_stop
params:
  atr_period: 20
  atr_mul: 1.5
```

买入后，持仓状态保存：

```text
entry_price
entry_date
atr_at_entry
hard_stop
```

### 9.2 chandelier_stop

吊灯止损每日持仓期间更新：

```text
highest_high_since_entry = max(high since entry)
chandelier_stop = highest_high_since_entry - current_atr * atr_mul
```

配置示例：

```yaml
type: state_value
name: chandelier_stop
params:
  atr_period: 20
  atr_mul: 2.5
  price_source: high
  scope: since_entry
```

用户不需要手工拼 `过去高点 - 2.5 * ATR`，而是直接选择 `chandelier_stop` 并填写参数。

## 10. 回测执行流程

每个交易日按以下顺序执行：

1. 读取当日及历史 K 线窗口。
2. 更新持仓状态值，如 `highest_high_since_entry`、`chandelier_stop`。
3. 如果当前持仓，评估卖出条件组。
4. 如果卖出条件满足，全仓卖出。
5. 如果当前空仓，评估买入条件组。
6. 如果买入条件满足，全仓买入。
7. 记录交易、现金、持仓市值、总权益。

成交假设第一阶段建议：

- 日频回测。
- 信号基于当日收盘数据。
- 买入参考价使用当日收盘价。
- 普通卖出参考价使用当日收盘价。
- 硬止损和吊灯止损卖出参考价使用止损价，而不是当日收盘价。
- 实际成交价需要叠加滑点：买入为 `reference_price * (1 + slippage)`，卖出为 `reference_price * (1 - slippage)`。
- 买入数量按整手向下取整，默认 `lot_size = 100`。
- 默认初始资金为 `1,000,000`。

建议配置：

```yaml
execution:
  signal_timing: close
  fill_timing: close
  initial_capital: 1000000
  fee_rate: 0.0000854
  fee_min: 5
  slippage: 0.002
  lot_size: 100
  instrument_type: etf
  stock_stamp_tax_rate: 0.001
```

交易成本规则：

- 佣金率默认万 0.854，即 `0.0000854`。
- 单笔佣金最低 5 元。
- ETF 暂不考虑印花税。
- 股票卖出时考虑千分之一印花税，买入不收印花税。
- 每笔交易必须保存 `commission`、`stamp_tax`、`slippage_rate`、`reference_price`、`exec_price`、`gross_amount`、`total_cost` 或 `net_proceeds`。
- 交易汇总需要保存累计佣金、累计印花税、累计交易成本、累计成交额和换手率。

## 11. 回测结果模型

结果至少包含：

```text
run_id
strategy_id
symbol
start_date
end_date
initial_capital
final_equity
summary
trades
daily_nav
condition_trace
debug_log
drawdown
annual_returns
monthly_returns
benchmark
charts
```

`summary` 包含：

- `total_return`
- `annual_return`
- `max_drawdown`
- `sharpe`
- `sortino`
- `calmar`
- `win_rate`
- `profit_factor`
- `trade_count`
- `avg_win`
- `avg_loss`
- `payoff_ratio`
- `turnover`
- `total_commission`
- `total_stamp_tax`
- `total_trading_cost`

除买入/卖出信号外，旧回测中值得保留到新模块的能力：

- 成交成本：滑点、佣金、最低佣金、印花税、交易成本拆分。
- 交易约束：整手交易、现金约束、无法买入零股。
- 持仓状态：现金、市值、总权益、买入价、买入日期、持仓期间最高价、止损价。
- 净值序列：每日 `cash`、`market_value`、`equity`。
- 回撤序列：每日 drawdown 和最大回撤。
- 收益拆分：年度收益、年度 Sharpe、月度收益热力图。
- 交易统计：胜率、盈亏比、profit factor、单笔盈亏、累计成交额、换手率。
- 可解释性：交易原因、触发条件、左右值、止损参考价、实际成交价。
- 图表数据：NAV、drawdown、K 线、买卖点、关键指标序列。
- 基准：第一阶段可先保留单标的买入持有基准，后续再扩展到指数或自定义基准。

`condition_trace` 用于解释策略为什么买入或卖出，可以按需开关：

```text
date
side
condition_id
left_value
operator
right_value
passed
reason
reference_price
exec_price
```

这对后续 Web 页面调试策略非常重要。

## 12. Debug 回测日志

当回测周期较短时，例如少于 1 个月，通常可以视为用户在调试策略或回测系统。此时需要保存足够详细的 debug 日志，使用户可以基于当时的日 K 数据逐项手算并复核：

- 每个指标值如何从原始 K 线计算出来。
- 每个条件左右两侧的最终数值是什么。
- 每个条件是否成立。
- 每笔买卖的参考价、成交价、滑点、佣金、印花税、现金变化是否正确。
- 每日持仓、市值、现金、总权益如何变化。

触发规则：

- 默认当 `end_date - start_date < 31 天` 时自动开启。
- 也允许通过参数显式开启：`debug_log_enabled: true`。
- 长周期回测默认关闭，避免结果过大和性能下降。

建议配置：

```yaml
debug:
  auto_enable_max_days: 31
  enabled: auto
  include_indicator_trace: true
  include_condition_trace: true
  include_execution_trace: true
  include_position_trace: true
```

debug 日志建议按交易日组织：

```text
date
raw_bar
history_window
indicator_trace
state_before
state_values
entry_condition_trace
exit_condition_trace
decision
execution_trace
state_after
daily_nav
```

其中 `indicator_trace` 应保存关键中间值。例如 SMA20 不只保存最终值，还应保存使用的窗口起止日期、窗口内 close 列表或摘要、period、计算结果。ATR、RSI、MACD、Bollinger、Trend Score 等也应保存足够复算的输入、参数、中间项和最终输出。

`execution_trace` 至少包含：

```text
side
reason
reference_price_source
reference_price
slippage_rate
exec_price
qty
gross_amount
commission_rate
commission_min
commission
stamp_tax_rate
stamp_tax
total_cost
net_proceeds
cash_before
cash_after
position_before
position_after
```

debug 日志不要求第一阶段直接做成漂亮页面，但必须作为结构化数据保存，优先建议放在回测结果的 `debug_log` 字段或单独的 JSON 文件中。常规 `condition_trace` 是轻量解释，`debug_log` 是可复算审计，两者用途不同。

## 13. 校验规则

保存策略前校验：

- `id` 非空且唯一。
- `trade_mode` 必须是支持的模式。
- entry/exit 至少各有一个条件。
- 条件 operator 必须在白名单内。
- 每个 `ValueSpec` 必须能被注册表识别。
- 指标参数类型、范围、默认值合法。
- `state_value` 只能在允许的上下文使用。

执行时校验：

- 数据字段存在。
- 所需历史窗口足够。
- 指标计算结果是可比较数字。
- 状态值在无持仓时的行为明确。

无持仓时读取 `hard_stop`、`chandelier_stop` 应返回 `None`，条件自动判定为不通过。

## 14. 存储建议

第一阶段可以用文件存储策略配置：

```text
config/rule_strategies/
  trend_macd_stop_v1.yaml
  sma_bias_rsi_v1.yaml
```

等 Web 策略管理成熟后再迁移到 SQLite。

但无论文件还是数据库，保存的都是同一份结构化策略定义。

## 15. 推荐实施顺序

1. 定义 `models.py`。
2. 实现指标注册表和第一批指标。
3. 实现 `ValueResolver` 和 `ConditionEngine`。
4. 实现 `PositionState`、`hard_stop`、`chandelier_stop`。
5. 实现 `SingleSymbolAllInBacktestEngine`。
6. 增加最小测试集：
   - 收盘价大于 SMA20 时买入并持有，收盘价小于 SMA20 时卖出
   - 硬止损卖出
   - 吊灯止损卖出
   - 滑点、佣金、ETF/股票印花税
   - 交易明细成本字段
   - 每日 NAV 与最大回撤
7. 后续测试集：
   - 多条件 AND
   - Trend Score 均线条件
   - MACD 条件
8. 增加策略 YAML 示例。
9. 后续再接 Web 策略配置弹窗。

## 16. 关键取舍

本方案选择结构化配置，而不是 DSL、Groovy、Aviator 或 Python 动态执行。代价是配置会比公式文本更长，但收益是：

- 前端更容易生成。
- 参数可校验。
- 指标可发现。
- 历史策略可迁移。
- 非代码用户不用学习语法。
- 执行安全，不需要运行用户输入的代码。

这更符合当前项目的长期方向。
