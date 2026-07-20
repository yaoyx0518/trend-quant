---
name: trend-quant-mcp
description: >
  通过 MCP 连接 Trend Quant 后端，获取 A 股 ETF 的趋势看板、标的详情、止损计算和标的列表。
  当用户需要查看市场趋势、分析 ETF 技术指标、计算止损价、或查询可用标的时触发使用。
  本 Skill 通过 MCP SSE 远程连接后端服务器，无需本地数据。
---

# Trend Quant MCP 服务

## 用途

通过 MCP 远程连接 Trend Quant 后端，获取 A 股 ETF 的趋势量化分析能力。Agent 侧通过 SSE 协议连接，调用 4 个工具获取数据。工具的详细参数和返回值由 MCP 协议自动提供，此处不再重复。

## 连接配置

```json
{
  "mcpServers": {
    "trend-quant": {
      "url": "http://121.199.173.214:8000/mcp/sse"
    }
  }
}
```

- **传输方式**：SSE（Server-Sent Events）over HTTP
- **无认证**：当前版本未启用 API Key
- **超时注意**：首次调用 `trend_dashboard` 需 5-10 秒（计算 600+ 标的），后续命中缓存秒返

---

## 领域知识

### 趋势值（Trend Score）

趋势值是 Trend Quant 的核心指标，范围 -100 ~ +100，用于判断标的当前的趋势方向和强度。

- **正趋势值** → 上升趋势，数值越大越强
- **负趋势值** → 下降趋势，数值越小越强
- **接近 0** → 震荡市，方向不明确

计算公式：Trend Score = Price Direction × Confidence

**Price Direction**（价格方向分）由两个部分合成：
- **Bias**（价格偏离）：当前价格偏离均线的程度，用 ATR 标准化。短中长周期（5/10/20 日）加权混合。
- **Slope**（均线斜率）：均线上升/下降的速度，同样用 ATR 标准化。

**Confidence**（置信度）由两个因子合成：
- **成交量因子**：近期成交量相对均量的放大程度
- **效率比率（ER）**：价格运动的平滑程度，趋势越顺畅 ER 越高

### 趋势值 MA5（Trend MA5）

趋势值的 5 日均值，是**看板中的主要排序指标**。与单点趋势值相比，MA5 更能反映趋势的持续性而非短期波动。

### 强度百分位（Strength）

在同级别分类中，某标的的 trend_ma5 在所有同类中的百分位（0-100）。数值越大表示在该分类中趋势越强。

### 趋势相位（Trend Phase）

对趋势状态的定性判断：
- **上升趋势**：趋势值和 MA5 同时向上
- **下降趋势**：趋势值和 MA5 同时向下
- **震荡**：方向不明确

### 硬止损（Hard Stop）

买入后立即生效的止损线，计算方式：买入价 − 买入日 ATR(20) × 1.5。

目的是在趋势判断错误时快速止损，防止损失扩大。止损位应该在买入前计算好，作为风险控制的重要参考。

### 吊灯止损（Chandelier Stop）

持仓期间动态调整的止损线，计算方式：买入以来最高价 − 最新 ATR(20) × 2.5。

随着价格上涨，吊灯止损会自动上移，锁定利润。但不会随价格下跌下移。

---

## 调用模式

### 模式 1：市场概览 → 深入分析

```
trend_dashboard()          → 扫全市场，找到趋势最强的板块和标的
symbol_detail(symbol, days) → 对感兴趣的标的深入分析
```

典型用法："帮我看看现在哪些板块趋势最强？" → 先调 `trend_dashboard`，在结果中找 strength 高、趋势相位刚进入"上升趋势"的标的，再调 `symbol_detail` 看技术面。

### 模式 2：发现标的 → 计算止损

```
list_instruments(keyword="XX")        → 找到标的代码
calc_stop_loss(symbol, buy_date, buy_price) → 计算止损
```

典型用法："我想买沪深300，帮我算一下止损位" → 先用 `list_instruments` 确认标的代码，再用 `calc_stop_loss` 计算硬止损和吊灯止损。

### 模式 3：风险评估

```
symbol_detail(symbol, days=60)  → 获取标的详细数据
calc_stop_loss(symbol, ...)     → 计算止损价
```

对比最新价格和止损价的距离，评估当前风险收益比。

---

## 实践注意事项

- **首次调用 `trend_dashboard` 较慢**（5-10 秒），因需计算 600+ 标的趋势值。后续命中缓存秒返。
- **`symbol_detail` 指标需要至少 30 根 K 线**做回看。设置 `days` 过小时，OHLCV 数据正确截断，但指标序列可能包含更多回看数据点。
- **标的代码格式**：支持 `510300.SS`（带后缀）和 `510300`（自动补全）。`.SS` 上海、`.SZ` 深圳。
- **`calc_stop_loss`**：若标的在 DB（instrument_metadata 表）中自定义了 `stop_atr_mul`，自动覆盖默认值 1.5。ATR 来自预计算缓存（indicator_daily.atr，单一来源）。
- **数据时效**：依赖 Trend Quant 后端每日 16:30 的数据更新，若未正常运行数据会滞后。
