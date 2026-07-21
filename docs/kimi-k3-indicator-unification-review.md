# kimi-k3 代码审查报告：core/unify-indicators 指标统一与预计算重构

> 审查人：kimi-k3 日期：2026-07-21
> 对象：`core/unify-indicators`（11 commits，46 文件，+3903/−1844）对照方案
> `docs/plans/2026-07-19-indicator-unification-and-precompute-plan.md`（v1.1）
> 方法：逐文件精读全部核心改动 + 全量 pytest + 生产库数值对账 + master 旧实现逐项对比 + 真实策略逐笔回测验证

---

## 0. 总体结论

**核心数值正确性验证全部通过，工程质量整体很高，但有 3 个应在合并前修复的实质缺陷、6 个中等问题和若干方案未落地项。**

- U1/U2/U3（统一指标库/趋势值/symbol）实现正确，与 master 旧实现逐值一致（实测，见 §4）；
- P1.1 缓存表与读取门面正确，缓存 vs 实时在生产数据上**逐值相等（diff = 0.0）**；
- P1.3 回测逐笔一致**实测通过**（3 个真实策略、36–50 笔交易逐笔相同，提速 128–189×）——但实现方式与方案文字不同（见 §3.4）；
- P1.2 除权修复范围有**实质性 bug**（§2.2）；版本校验只覆盖趋势表（§2.3）；
- P1.4 是最大的方案偏差：统一实时叠加门面写好了但**没有接线**，三处盘中实现原样保留；看板 fallback 路径有 61× 冗余计算的性能 bug（§2.1）。

---

## 1. 测试基线

```
.venv/bin/python -m pytest -q
→ 323 passed, 2 failed（139s）
```

两个失败为 `tests/integration/test_intraday_service.py` 的
`test_trend_history_not_empty` 与 `test_trend_ma5_differs_from_trend_score`——
正是方案 §9.4 记录的 master 基线既有失败，**本分支未引入任何新增测试失败**。✔

---

## 2. 严重问题（建议合并前修复）

### 2.1 看板 fallback 路径：每个交易日重复调用 `get_series`，61× 冗余计算

`src/services/dashboard.py:240-243`：

```python
data["trend_score"] = [
    get_series(str(symbol), "trend_score", db=db, since=since).get(pd.Timestamp(t), np.nan)
    for t in data["time"]
]
```

`get_series` 在 listcomp 里**按日期逐天调用**：每次调用都 `_cache_fresh`（2 条 SQL）+
`load_market_data`（全历史）+ `compute_live_series`（全历史趋势序列）。
缓存未命中的标的，成本 = 61 天 × 全量重算。

**生产实测**：当前库有 ~59 个标的走 fallback（40 个无缓存行 + 19 个陈旧），
看板全量构建耗时 **281 秒**（cProfile 下 456s，其中 432s 在这个 listcomp，
`get_series` 被调 3600 次）。P1 的性能目标（看板 10–100× 提速）在 fallback 路径上反而是严重退化。

修复：每标的取一次 series，再按日期查值：

```python
series = get_series(str(symbol), "trend_score", db=db, since=since)
data["trend_score"] = [series.get(pd.Timestamp(t), np.nan) for t in data["time"]]
```

### 2.2 D9 除权修复范围错误：2025 年之前的历史不会被修复

`src/services/indicator_builder.py:180-182`：

```python
start_text = str(trend_cfg.get("backtest_start_primary", "2025-01-01"))
start_date = date.fromisoformat(start_text)
repair_broken_symbols(broken, data_service, start_date, end_date)
```

除权后 vendor 会**回溯改写全部历史**（方案 §5-2 的核心论点），但修复只从
`backtest_start_primary`（2025-01-01）重拉，而 `save_market_data` 是 INSERT OR REPLACE
（upsert），**2025 年之前的旧复权基准段落原样保留**。

生产库实测：**569 个标的历史起点早于 2025-01-01**（最早 1993-07-28）。
这些标的除权修复后，K 线在 2025-01-01 边界仍然断裂，`rebuild_symbol` 又在
**全量库存历史**上重算指标——sma200、EMA/MACD/RSI（无限记忆）全部被污染，
D9 形同虚设。这直接违背方案 D9"该标的历史全量重拉"。

修复：`start_date` 应取该标的库内历史的 `MIN(time)`（或干脆传一个足够早的日期）。

### 2.3 `INDICATOR_FORMULA_VERSION` 变更永远不会触发重建

`src/services/indicator_builder.py:53-60` 的 `default_param_set_needs_rebuild`
只校验 `trend_param_sets` 表里的 `params_json` 与 `TREND_FORMULA_VERSION`。
`indicator_daily` 表自己的 `formula_version`（对应 `INDICATOR_FORMULA_VERSION`）
**没有任何启动校验**。

后果：将来只 bump `INDICATOR_FORMULA_VERSION`（改 core/indicators.py 公式）时，
启动不重建，`_cache_fresh` 对所有标的永久返回 False，indicator 列全部永久走
live fallback——数值仍正确（fallback 兜底设计救了它），但缓存静默失效且无任何告警。
违背方案 D5"各表校验各自的版本"与 P1.2"启动时校验……与各表 formula_version 不符 → 自动全量重建"。

顺带：`params_hash()`（indicator_builder.py:46）定义后无人调用，是死代码。

---

## 3. 中等问题

### 3.1 `services/instrument_admin.py` 引用未定义的 `logger`

第 88、112 行 `logger.warning(...)`，但模块没有 import/定义 `logger`
（已实测 `hasattr(module, 'logger') == False`）。这两条是异常兜底路径，
触发时 `NameError` 会盖掉原始异常。是从 `instruments.py` 搬迁时漏带。

### 3.2 MCP `symbol_detail` 的名称口径悄悄变了

master 上 MCP 用 `market_view._config_name_map` → `load_instrument_name_map()`
（**会 strip 掉 "ETF" 后缀**、无 benchmarks）；本分支改为
`services.instrument_admin._config_name_map`（**不 strip**、含 benchmarks，
该实现是 instruments.py 的原样搬迁）。MCP 输出的 `name`/`display_name`
因此变化（如 `沪深300` → `沪深300ETF`），且与 market_view 页面口径不一致。
违反方案 §3-5"零回归（页面/看板/MCP 输出数值不变）"。要么统一为同一种口径，
要么在报告/文档中明确这是批准的语义变更。

### 3.3 MCP `symbol_detail` 盘中叠加仍在截断窗口上计算

`src/trend_mcp/server.py:256` 先 `df = df.tail(n)`（n=days，默认 60），
随后 286-297 行的盘中叠加用**这个已截断的 df** 调 `compute_intraday_trend_score`。
EOD 指标已改为全历史计算（U6 修复），但盘中趋势值仍基于 60 根窗口——
EMA 族指标的 warmup 差异使盘中行与 EOD 序列不是"同一把尺"，
U6 的窗口修复只修了一半（market_view 路由的叠加用全量 hist，是对的；MCP 不是）。

### 3.4 看板 bulk 路径绕过新鲜度校验，陈旧时静默出 NULL

`services/dashboard.py:230-243`：`load_trend_daily_bulk` 不过滤
`formula_version`，`any()` 检查只看"窗口内有没有缓存行"，不查版本、不查
`indicator_last >= market_end`。后果：

- 缓存陈旧（如手动回填后未重建）时，最新交易日的 trend 取到 NaN，
  该标的最新的 `trend_score`/`trend_ma5`/`strength` 全部置 NULL——
  **不降级 live 计算，而是静默缺值**（生产实测当前有 6 个标的如此）；
- version 不匹配时同样照常读旧版本缓存值。

而 `get_series` 路径是有新鲜度校验和 fallback 的——同一份数据两条读路径，
行为分裂，违背"缓存与实时是同一把尺"（方案 §5-4）。

### 3.5 P1.4 统一实时叠加门面没有接线（最大方案偏差）

方案 P1.4 要求"三处现存盘中实现全部收敛到 `indicator_store` 门面"。实际：

- `indicator_store.compute_intraday_row` / `get_series_with_intraday`
  **零生产调用方**（只有 tests 引用）——递推状态列（rsi_avg_gain/loss、
  macd_ema12/26，评审"最重要发现"）为此而入表，目前全部空转；
- `market_view` 叠加、`intraday_service` 盘中看板、MCP 镜像三处实现**原样保留**，
  `intraday_service.py:440` 的 "mirrors subject_market.py" 注释还在；
- `_ma5`/`_strength`/`_assign_strength`/`_detect_trend_phase` 仍是
  `services/dashboard.py` 与 `data/intraday_service.py` 各一份
  （dashboard 还反向 import data 层的**私有** `_detect_trend_phase`）；
- MCP 仍自带一份 dashboard cache 元组（server.py:97，注释 "same strategy as
  subject_market.py" 还在），没有用共享的 `RevisionCache`。

即 U4a 的"EOD+盘中看板构建合一"与 U4b 的"mirrors 注释全灭"**未达成**。
功能数值没错（三处实现共用 core.trend），但方案 §3-1"每种计算全项目一份"
在盘中链路上没有实现。

### 3.6 P1.2 手动重建入口缺失

方案要求"手动入口：全量/单标的重建"。实际触发点只有启动漂移校验
（main.py 后台线程）和日更尾部钩子。**手动批量回填、单标的添加回填后
不会重建缓存**：读路径靠 fallback 兜底（数值正确但慢），看板则落入 §3.4
的 NULL 陷阱。建议至少在回填任务尾部挂 `rebuild_all(symbols=变动标的)`。

### 3.7 P1.3 实现方式与方案文字不同（需确认，结果反而更稳）

方案写"运行开始一次性读取该标的全部所需指标序列（一条 SQL）……
`trend_score_sma/ema` 由缓存 trend_score 序列 rolling 得到"。
实际实现**完全不读缓存**：`ValueResolver.set_context_bars` + 全序列记忆化
现场计算（`_series_for`），回测结果与缓存正确性彻底解耦。
方案 §2.3 自认"慢的是调用模式不是指标数学"，所以提速目标达成（实测 128–189×），
且这一选择其实更安全（回测不受缓存污染影响）——但属于方案变更，
且 §8.3"缓存未命中回退路径重跑一遍与缓存路径一致"的验收项随之失去意义，
应在文档中追认。

---

## 4. 独立验证结果（全部通过）

| 验证项 | 方法 | 结果 |
|---|---|---|
| core.trend vs master 标量快照版 | 合成 400 根 K 线逐键对比 | trend_score/price_direction/confidence/atr/ma_mid 及全部 calc_details **逐值一致（<1e-12）** ✔ |
| core.trend vs master 向量化版 | 全序列（400 点）对比 score/ma5/ma10/pd/conf | NaN 模式完全相同，max diff ≤ 5e-7（来自旧 `_series` 的 6 位舍入）✔ |
| snapshot == series 末行不变式（U2 核心） | 5 个随机前缀长度 | 全部成立 ✔ |
| 缓存 vs 实时（§8.4 对账） | 生产库 510300/510500/159915，trend_score 与 ATR 全序列（3200–3500 行） | **max abs diff = 0.0**，逐位相等 ✔ |
| D11 ATR 单一来源等价断言 | 同上 | indicator_daily.atr ≡ core 实时 atr，diff = 0.0 ✔ |
| §8.3 回测逐笔一致（合并门槛） | 生产库 3 个真实策略（趋势进-止损出 / 趋势进-趋势出 / 趋势进(MA200)-止损出，覆盖 trend_score、trend_score_sma、sma、止损状态）× 510300 × 800 根，memoized vs legacy（debug 路径即旧算法） | **36/50/36 笔交易逐笔一致，equity 曲线逐日一致** ✔ |
| P1.3 提速（§8.6） | 同上计时 | **128.7× / 135.1× / 188.8×**（800 根）；全历史 3437 根下 legacy 路径已无法在 9 分钟内跑完，实际提速更高 ✔ |
| WAL / 备份 | `PRAGMA journal_mode` = wal；`backup_to` 用 VACUUM INTO + 保留 3 份 | ✔（有单测覆盖） |
| 表结构 vs 方案 P1.1 | 逐列比对 | 一致：含 4 个递推状态列、trend_daily 无冗余 ATR（修订#3 落实）✔ |
| U4b 任务管理器搬迁 | 与 master 逐字 diff | `BulkBackfillJobManager` 逐字相同（仅空行差异）✔ |
| U5 小项 | fee_rate 单常量、meta 下发 state_values/stop_defaults、conftest 用 DEFAULT_STRATEGY_CONFIG、enabled 统一 | 均已落实 ✔ |
| U6 收尾 | strategy/ 目录已删除、全项目无 `from strategy` 引用、symbol_detail 主路径窗口修复、文档重写 | ✔（MCP 盘中路径例外，见 §3.3） |
| 实时行不落库（§8.5/D7） | 代码路径核查：compute_intraday_row 纯内存，无写库调用；回测/止损只读 EOD | ✔ |

---

## 5. 轻微问题（记录，不阻塞）

1. `compute_intraday_row`（目前死代码，接线前需修）：
   - 不检查缓存版本/新鲜度，fallback 时 EOD 序列是 live 算的、盘中行却从陈旧缓存状态递推，口径分裂；
   - 历史长度 < n−1 时 SMA 仍出值（full recompute 会是 NaN），短历史标的边界不一致。
2. `_cache_fresh` 只比 `MAX(time)`，不检测缓存中间空洞（§8.4"回退 4 场景"之一的"中间空洞"实际不会触发 fallback）。当前 rebuild 是 DELETE+全量 INSERT，实践中不会产生空洞，但语义与方案有出入。
3. `data/provider_utils.py:normalize_symbol` 已无调用方，是另一套 symbol 规则的残留死代码；`safe_float` 仍有三个变体（core.trend / rule_backtest / provider_utils）。
4. `get_series` 只做 `upper()` 不做 SH→SS 归一（core.symbols 才是权威），目前调用方都先归一了，属隐患而非 bug。
5. memoized 路径在退化数据下与 legacy 有理论分歧：bias 的 ma=0 时 core 得 inf（legacy 返回 None）、momentum_return 前值为 0 时同理。价格不可能为 0，仅记录。
6. 启动重建线程与日更任务理论上可能并发写缓存（sqlite 单 writer，WAL 下可能 busy 失败）；startup rebuild 失败仅记日志、无重试。方案 D-边界已声明单 worker 前提，记录在案。
7. `run_post_update_pipeline(settings, ...)` 的 `settings` 参数未使用。
8. 日更除权检测（D9）对全池 600+ 标的每天各发一次远程 fetch，网络开销大——方案已拍板，但建议观察 16:30 任务总时长。

---

## 6. 修复优先级建议

| 优先级 | 项 | 理由 |
|---|---|---|
| P0 | §2.2 除权修复起点 | 数据正确性：修复后源数据仍断，下游全错（方案 §2.5 的原话） |
| P0 | §2.1 看板 fallback 61× 冗余 | 一行修复；fallback 是"永久特性"（方案原话），不能是 61× 惩罚 |
| P1 | §2.3 indicator 版本校验 | 下次 bump INDICATOR_FORMULA_VERSION 时静默失效 |
| P1 | §3.1 logger NameError | 一行修复 |
| P1 | §3.4 看板 bulk 路径新鲜度 | 陈旧缓存出 NULL 而非降级，违反"同一把尺" |
| P2 | §3.2 MCP 名称口径、§3.3 MCP 盘中截断窗口 | 输出回归/口径分裂 |
| P2 | §3.5/§3.6 P1.4 门面接线 + 手动/回填后重建入口 | 方案目标未达成，建议要么补齐要么修订方案文本追认现状 |
| P3 | §5 各项 | 择机清理 |

## 7. 审查范围说明

- 精读：core/indicators.py、core/trend.py、core/symbols.py、data/indicator_store.py、
  data/storage/db.py（全部）、services/ 全部 5 个模块、rule_backtest/ 全部改动、
  trend_mcp/server.py、app/routers/ 全部改动、core/jobs.py、app/main.py、
  data/intraday_service.py；
- 粗读：tests 新增 7 个测试文件（覆盖面核对，未逐行审）、文档重写（README/CLAUDE.md 存在性核对）；
- 实证：全量 pytest、生产库缓存对账、master 旧实现提取对比、真实策略双路径回测、
  看板构建 cProfile。所有数字均为本机实测。
