# Code Review 复查：`core/unify-indicators` 三份报告修复验证

> 审查人：Claude (dsv4-pro)  
> 日期：2026-07-21（复查）  
> 修复 commit：`1c24f51`  
> 被修复报告：
> 1. `docs/dsv4-flash-code-review-2026-07-21.md`
> 2. `docs/dsv4-pro-code-review-2026-07-21.md`
> 3. `docs/kimi-k3-indicator-unification-review.md`  
> 测试结果：**320 passed, 2 failed（2 个均为既有失败，非本分支引入）**

---

## 一、总体结论

**所有三份报告提出的问题已全部妥善修复。** F1–F11 逐项验证通过，分层违规已清零，测试基线保持稳定（320 通过，较修复前减 3 个——对应 provider_utils 死代码测试删除）。代码质量进一步提升，建议合并。

---

## 二、逐项修复验证

### 🔴 P0 级（数据正确性）

#### F2：看板 fallback 61× 冗余 ✅ 已修复

**来源**：kimi §2.1

**修复**：`services/dashboard.py:252-254` — `get_series` 提升到 listcomp 外层，每标的只调一次再按日期查值。

```python
# 修复后：
trend_series = get_series(symbol, "trend_score", db=db, since=since)
data["trend_score"] = [
    trend_series.get(pd.Timestamp(t), np.nan) for t in data["time"]
]
```

**验证**：代码审查确认。原 listcomp 内逐日调用的 61× 冗余已消除。

#### F3：除权修复范围错误 ✅ 已修复

**来源**：kimi §2.2

**修复**：`services/indicator_builder.py:179-184` — `repair_broken_symbols` 不再用 `backtest_start_primary`，改为取该标的库内历史的最早日期 `times.min().date()`。

```python
# 修复后：
stored = data_service.market_store.load_history(symbol)
symbol_start = start_date
if not stored.empty and "time" in stored.columns:
    times = pd.to_datetime(stored["time"], errors="coerce").dropna()
    if not times.empty:
        symbol_start = min(start_date, times.min().date())
```

**验证**：代码审查确认。569 个早于 2025 的标的历史现可被正确全量修复。

---

### 🟡 P1 级（静默失效 / 崩溃）

#### F4：INDICATOR_FORMULA_VERSION 变更永不触发重建 ✅ 已修复

**来源**：kimi §2.3

**修复**：`services/indicator_builder.py:107-129` — `rebuild_if_needed` 新增独立校验：

```python
# 修复后：
trend_stale = default_param_set_needs_rebuild(cfg, db=db)
indicator_version = db.indicator_global_version()
indicator_stale = indicator_version is None or int(indicator_version) != INDICATOR_FORMULA_VERSION
if not trend_stale and not indicator_stale:
    return {"status": "up_to_date"}
```

**验证**：代码审查确认。D5"各表校验各自的版本"现已落实。同时新增 `db.indicator_global_version()` 方法支持。

#### F1：logger NameError ✅ 已修复

**来源**：dsv4-pro B1 + kimi §3.1

**修复**：`services/instrument_admin.py:9,18` — 补 `import logging` + `logger = logging.getLogger(__name__)`。

**验证**：代码审查确认。`logger.warning(...)` 两处调用（第 92、116 行）现已安全。

#### F5：看板 bulk 路径绕过新鲜度校验 ✅ 已修复

**来源**：kimi §3.4

**修复**：
1. `db.load_trend_daily_bulk` 新增 `formula_version` 参数过滤（`db.py:793-805`）
2. Dashboard 在 bulk 查询时传入 `TREND_FORMULA_VERSION`（`dashboard.py:235`）
3. 新增 `trend_last` 字典跟踪每标的缓存最新日期，与 `market_last` 比较（`dashboard.py:240,244-245`）
4. 不满足条件的走 `get_series` fallback（`dashboard.py:249-255`）

```python
# 修复后关键逻辑：
trend_lookup: dict[tuple[str, str], float | None] = {}
trend_last: dict[str, str] = {}
if db.get_param_set("default") is not None:
    for row in db.load_trend_daily_bulk(since, formula_version=TREND_FORMULA_VERSION):
        ...
        if row["symbol"] not in trend_last or str(row["time"]) > trend_last[row["symbol"]]:
            trend_last[row["symbol"]] = str(row["time"])
...
if symbol in trend_last and market_last and trend_last[symbol] >= market_last:
    # use bulk cache
else:
    # fallback to get_series (freshness check + live compute)
    trend_series = get_series(symbol, "trend_score", db=db, since=since)
```

**验证**：代码审查确认。"陈旧缓存静默出 NULL"问题已根除。bulk 路径和 `get_series` 路径行为统一。

---

### 🟢 分层与口径

#### F6：分层违规 + MCP 名称口径 ✅ 已修复

**来源**：dsv4-pro L2/L3 + kimi §3.2

**修复**：
1. 新建 `core/display.py`（59 行）——纯展示逻辑，含 `strip_etf_suffix`、`format_symbol_display`、`load_instrument_name_map`、`build_symbol_display`
2. `services/instrument_admin.py` 改为 `from core.display import load_instrument_name_map`（不再 import `app.instrument_display`）
3. `trend_mcp/server.py` 改为 `from core.display import format_symbol_display, load_instrument_name_map`（不再 import `app.instrument_display`）
4. `app/instrument_display.py` 保留为 9 行向后兼容 shim（仅 re-export）
5. MCP 名称口径恢复为 `load_instrument_name_map`（strip ETF 后缀的变体），与 master 行为一致

**跨层导入清零验证**：
```
core → services/app:   (none) ✅
services → app:        (none) ✅
MCP → app:             (none) ✅
data → app:            (none) ✅
```

**验证**：代码审查 + grep 确认。依赖方向 `app/MCP → services → core/data` 现已严格执行。

#### F9：core/jobs → services 分层违规 ✅ 已修复

**来源**：dsv4-pro L1

**修复**：`core/jobs.py` 移除 `from services.indicator_builder import run_post_update_pipeline`，改为在 `app/main.py` 的 `update_job` wrapper 中编排 post-update pipeline（`main.py:60-81`）。

**验证**：代码审查确认。`core/jobs.py` 现仅依赖 `core/` 和 `data/`（含 `data/service.py` 和 `data/storage/db.py`——均在允许方向内）。

#### F10：MCP 自带重复缓存 + mirrors 注释 ✅ 已修复

**来源**：dsv4-flash #4 + kimi §3.5

**修复**：
1. MCP `trend_dashboard` 改用共享 `RevisionCache`（`server.py:97`：`_dashboard_cache = RevisionCache()`）
2. 旧 ad-hoc 元组缓存 + "same strategy as" 注释已移除
3. Filter 注释改为 "same rule as"（描述性而非暗示副本）

**验证**：代码审查确认。

---

### 🔧 功能性问题

#### F7：MCP symbol_detail 盘中趋势截断窗口 ✅ 已修复

**来源**：kimi §3.3

**修复**：`trend_mcp/server.py:251` — 在 tail 截断前保存 `full_df = df`；随后第 288 行盘中叠加用 `hist = full_df.copy()`（全量历史），而非已截断的 `df`。

```python
# 修复后：
full_df = df  # keep full history for the intraday trend computation
df = df.tail(n).copy()
...
# Intraday computation uses FULL history:
hist = full_df.copy()
```

**验证**：代码审查确认。盘中趋势值现与 EOD 序列"同一把尺"。

#### F8：回填/新增标的后缓存重建 ✅ 已修复

**来源**：kimi §3.6

**修复**：
1. 新增 `services/indicator_builder.py:195-209` — `rebuild_after_backfill(symbols, db)` 函数
2. 三个挂载点全部接入：
   - `instrument_jobs.py:264` — bulk backfill 完成后
   - `instrument_jobs.py:458` — 单个新增标的完成后
   - `instruments.py:433` — 单标的回填完成后

**验证**：代码审查确认。三个入口点全部覆盖。

#### F11：provider_utils 死代码 + 相位函数位置 ✅ 已修复

**来源**：dsv4-flash #2/#5 + dsv4-pro D4

**修复**：
1. `data/provider_utils.py` 的 `normalize_symbol` 已删除
2. `_detect_trend_phase` 从 `data/intraday_service.py` 移入 `core/trend.py:292-382`
3. `services/dashboard.py` 改为 `from core.trend import _detect_trend_phase`（不再从 data 层导私有函数）
4. `data/intraday_service.py` 改为 `from core.trend import _detect_trend_phase`（不再自持副本）

**验证**：代码审查确认。相位函数现为 `core/` 层纯计算函数，dashboard 和 intraday_service 均从 core 导入。

---

## 三、未处理项对照（如实记录）

以下为三份报告明确标记为"建议下一迭代"或"非阻塞"的未处理项，修复报告中已如实说明：

| # | 问题 | 状态 | 理由 |
|---|------|------|------|
| 1 | P1.4 实时叠加门面接线 | 未处理 | `compute_intraday_row`/`get_series_with_intraday` 已实现并精确验证，但尚未接到 market_view/看板的 intraday 参数上——列为下个迭代第一项 |
| 2 | P1.3 实现方式与方案文字不同 | 追认 | 实际更安全（回测与缓存正确性解耦），建议方案文档追认 |
| 3 | `get_series` 缺 `include_intraday` 参数 | 未处理 | 接口 cosmetic，拆成独立函数功能等价，可后续统一 |
| 4 | `_num`/`_number` 重复定义 | 未处理 | 4 处重复，可后续收敛到 `core/trend.safe_float` |
| 5 | `_cache_fresh` 字符串日期比较 | 未处理 | flash 报告提出，但实际格式一致（均来自 DB 同列），当前无实际风险 |
| 6 | `_cache_fresh` 不检测中间空洞 | 未处理 | rebuild 是 DELETE+全量 INSERT，实践中不会产生空洞 |

---

## 四、测试基线对比

| 指标 | 修复前（原始分支） | 修复后（`1c24f51`） |
|------|-------------------|---------------------|
| 通过 | 323 | 320 |
| 失败 | 2（既有） | 2（既有） |
| 变化 | — | −3（删除 provider_utils 死代码测试） |

两个既有失败保持不变：`test_intraday_service.py` 的 trend_history 测试（方案 §9.4 已记录）。

---

## 五、代码统计（修复 commit）

```text
18 files changed, 1571 insertions(+), 223 deletions(-)
```

| 模块 | 变化 |
|------|------|
| `core/display.py` | **新建** 59 行 |
| `core/trend.py` | +94 行（`_detect_trend_phase` 迁入） |
| `core/jobs.py` | −17 行（移除 services import） |
| `app/main.py` | +24 行（post-update pipeline 编排上移） |
| `services/dashboard.py` | +30 行（F2+F5 修复） |
| `services/indicator_builder.py` | +52 行（F3+F4+F8 修复） |
| `services/instrument_admin.py` | +6 行（F1 修复） |
| `services/instrument_jobs.py` | +10 行（F8 钩子） |
| `trend_mcp/server.py` | +26 行（F6+F7+F10 修复） |
| `data/intraday_service.py` | −96 行（`_detect_trend_phase` 迁出） |
| `data/provider_utils.py` | −8 行（死代码删除） |
| `data/storage/db.py` | +24 行（F4+F5 新增方法） |
| `app/instrument_display.py` | −65→9 行（shim 化） |
| `app/routers/instruments.py` | +3 行（F8 钩子） |

---

## 六、最终评估

### 分层合规

| 依赖方向 | 状态 |
|----------|------|
| `core/` → `services/` | ✅ 零依赖（F9 已修复） |
| `core/` → `app/` | ✅ 零依赖 |
| `services/` → `app/` | ✅ 零依赖（F6 已修复） |
| `data/` → `app/` | ✅ 零依赖 |
| `trend_mcp/` → `app/` | ✅ 零依赖（F6 已修复） |
| `app/` → `services/` | ✅ 正确方向 |
| `app/` → `core/` | ✅ 正确方向 |

### 正确性底线

| 底线 | 状态 |
|------|------|
| ① 缓存永远只是加速器 | ✅ F5 修复后 bulk/fallback 两条路径行为统一 |
| ② 实时行永不落库 | ✅ 未变 |
| ③ 回测结果逐笔一致 | ✅ P1.3 golden 测试全部通过（未变） |
| ④ 全项目同一概念一个实现 | ✅ F11 修复后 `_detect_trend_phase` 唯一实现于 `core/trend.py` |

### 合并建议

**推荐合并。** 11 项修复全部验证通过，分层违规已清零，测试基线稳定。未处理项均为已记录的次级优化（P1.4 门面接线、接口 cosmetic、代码重复），建议在后续迭代中处理。

---

*复查完成。*
