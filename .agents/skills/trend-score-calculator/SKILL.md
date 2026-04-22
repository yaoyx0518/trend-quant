---
name: trend-score-calculator
description: >
  计算 A 股 ETF 标的的 Trend Score（趋势值）。
  当用户要求计算某个 ETF 的趋势值、trend score、趋势评分，或分析某个标的的趋势状态时触发使用。
  注意：本 Skill 不提供数据获取功能，调用方需自行准备日K数据后传入计算。
  支持给定日K数据后计算过去 N 个交易日的趋势值，默认过去 5 个交易日。
  默认参数：MA 短周期=5，中周期=10，长周期=20，ATR 周期=20，其他权重与项目默认一致。
---

# Trend Score Calculator

## 用途

给定一个标的的日K数据（OHLCV），计算该标的过去 N 个交易日的 Trend Score 并输出表格。

## 重要前提

**本 Skill 不提供数据获取功能。** 调用方需自行通过 iFinD、efinance、akshare 或其他途径获取日K数据，保存为 CSV 后再传入本脚本计算。

## 触发场景

- "计算 510330 的趋势值"
- "看一下 512500 最近 5 天的 trend score"
- "算一下这个 ETF 的趋势评分"
- "分析一下 563300 的趋势状态"

## 输入数据格式

CSV 文件必须包含以下列：

| 列名 | 说明 | 示例 |
|------|------|------|
| `time` | 交易日期 | `2026-04-15` 或 `2026-04-15 00:00:00` |
| `open` | 开盘价 | `4.850` |
| `high` | 最高价 | `4.920` |
| `low` | 最低价 | `4.830` |
| `close` | 收盘价 | `4.883` |
| `volume` | 成交量 | `12345678` |

数据需按时间升序排列，至少提供 **60 个交易日** 的历史数据（因 MA 长周期=20，ATR 周期=20，需要足够回看来计算指标）。

## 使用方法

### 1. 准备数据

调用方自行获取日K数据并保存为 CSV：

```python
# 示例：用 akshare 获取数据后保存
import akshare as ak
df = ak.fund_etf_hist_em(symbol='510330', period='daily', adjust='qfq')
df = df.rename(columns={'日期':'time','开盘':'open','收盘':'close','最高':'high','最低':'low','成交量':'volume'})
df.to_csv('510330.csv', index=False)
```

### 2. 执行计算

```bash
python .agents/skills/trend-score-calculator/scripts/calculate_trend_score.py <csv_path> [days]
```

参数：
- `csv_path`: 日K数据 CSV 文件路径，传 `-` 表示从标准输入读取
- `days`: 计算过去多少个交易日，默认为 5

示例：
```bash
python .agents/skills/trend-score-calculator/scripts/calculate_trend_score.py 510330.csv 5
```

从标准输入传入：
```bash
cat 510330.csv | python .agents/skills/trend-score-calculator/scripts/calculate_trend_score.py - 5
```

### 3. 解析输出

脚本输出格式（制表符分隔）：
```
DAYS=5
ROWS=44
<date>\t<close>\t<trend_score>\t<price_direction>\t<confidence>\t<atr>\t<ma10>
```

将结果整理为 Markdown 表格呈现给用户。

## 计算公式

Trend Score = Price Direction × Confidence

### Price Direction
- Bias_n = (Close - MA_n) / ATR
- Slope_n = (EMA_n(今日) - EMA_n(昨日)) / (ATR × n)
- norm_bias = tanh(bias_mix / 2) × 100
- norm_slope = tanh(slope_mix) × 100
- Price Direction = 0.5 × norm_bias + 0.5 × norm_slope

### Confidence
- Volume Factor: vol_ratio / 3（上限为 1）
- ER（效率比率）: 10 日
- Confidence = volume_factor^0.3 × er^0.7

## 默认参数

| 参数 | 默认值 |
|------|--------|
| n_short (MA短周期) | 5 |
| n_mid (MA中周期) | 10 |
| n_long (MA长周期) | 20 |
| atr_period | 20 |
| w_bias_short/mid/long | 0.4 / 0.4 / 0.2 |
| w_slope_short/mid/long | 0.4 / 0.4 / 0.2 |
| w_bias_norm / w_slope_norm | 0.5 / 0.5 |
| vol_ma_period | 20 |
| er_period | 10 |
| w_vol / w_er | 0.3 / 0.7 |
