# Code Review: `core/unify-indicators` 分支审查报告

> 审查人：Claude (dsv4-pro)  
> 日期：2026-07-21  
> 基准文档：`docs/plans/2026-07-19-indicator-unification-and-precompute-plan.md`（v1.1 定稿）  
> 分支：`core/unify-indicators`（基于 `master`，共 10 个 commit）  
> 测试结果：**323 passed, 2 failed（2 个均为既有失败，非本分支引入）**

---

## 一、总体评价

**本分支整体质量较高，严格遵循方案设计，测试覆盖充分，核心逻辑正确。** 发现 1 个运行时 bug、3 个分层违规、2 个设计偏离和若干次要问题。建议修完 bug 和分层违规后合并。

---

## 二、逐阶段审查

### U1：统一指标库 `core/indicators.py` ✅ 通过

**实现文件**：`src/core/indicators.py`（135 行）

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 所有函数 Series→Series 向量化 | ✅ | sma/ema/atr/rsi/macd/boll/bias/momentum_return/er 均为向量化实现 |
| RSI 统一为 Wilder | ✅ | `alpha=1/period`，`ewm(adjust=False)` |
| MACD 柱 ×2 | ✅ | `(dif - dea) * 2` |
| BIAS 统一为小数 | ✅ | `(close - ma) / ma`（展示层 ×100） |
| `INDICATOR_FORMULA_VERSION` | ✅ | 值为 1 |
| 空输入处理 | ✅ | 所有函数对空 Series/DataFrame 返回空结果 |
| 三方实现收敛完成 | ✅ | `strategy/indicators.py` 已删除；`rule_backtest/indicators.py` 改为 thin adapter；`market_view.py` 委托 `services/market_indicators.py` → `core.indicators` |

**测试**：`tests/unit/test_core_indicators.py`（314 行）

| 检查项 | 状态 | 说明 |
|--------|------|------|
| golden-master 逐值对比 | ✅ | 与三份旧实现逐值对比（atr/er/rsi/ema/macd/boll/sma/bias） |
| 语义变更差异断言 | ✅ | RSI Cutler≠Wilder（范围断言）、MACD hist×2（精确关系）、BIAS ×100（精确关系） |
| 独立单元测试 | ✅ | 空输入、全 NaN、单元素、短序列、已知值、ATR 常量、RSI 全涨/全平、ER 完美趋势/震荡、动量收益、除权跳变不崩溃 |

**结论**：U1 完全符合方案。✅

---

### U2：趋势值序列化 `core/trend.py` ✅ 通过

**实现文件**：`src/core/trend.py`（290 行）

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 规范实现为全序列向量化 | ✅ | `calculate_trend_score_series` 返回完整 DataFrame |
| 快照=序列末行不变式 | ✅ | `calculate_trend_score_snapshot` 直接读取 series 末行 |
| 公式一致性 | ✅ | bias/slope/norm/confidence/trend_score 公式与旧实现一致 |
| `TREND_FORMULA_VERSION` | ✅ | 值为 1 |
| fixed_atr/fixed_volume 盘中支持 | ✅ | 仅替换末行 ATR/volume，不污染历史 |
| 旧实现删除 | ✅ | `strategy/trend_score_core.py` 已删除 |

**测试**：`tests/unit/test_core_trend.py`（301 行）

| 检查项 | 状态 | 说明 |
|--------|------|------|
| snapshot 与旧实现逐字段对比 | ✅ | 多组随机参数+除权跳变数据 |
| series 与旧实现逐值对比 | ✅ | trend_score/price_direction/confidence 逐位置对比 |
| snapshot=series.iloc[-1] 不变式 | ✅ | 多组随机数据+固定 ATR/volume 场景 |
| 边界测试 | ✅ | 空数据、全 NaN close、短序列、warmup 区 NaN |

**结论**：U2 完全符合方案。✅

---

### U3：统一 symbol 标准化 `core/symbols.py` ✅ 通过

**实现文件**：`src/core/symbols.py`（45 行）

| 检查项 | 状态 | 说明 |
|--------|------|------|
| bare 6-digit → .SS/.SZ | ✅ | 5/6 开头 → .SS，其余 → .SZ |
| SH→SS 归一 | ✅ | `510300.SH` → `510300.SS` |
| 已有后缀穿透 | ✅ | uppercase 穿透 |
| 删除旧私有函数引用 | ✅ | MCP 不再 import `app.routers` 私有函数 |
| 前端符号输入 | ⚠️ 未验证 | 需要手工冒烟测试（按方案 §8.7） |

**测试**：`tests/unit/test_core_symbols.py`（53 行）

| 检查项 | 状态 |
|--------|------|
| 上海/深圳/后缀/空格/空字符串/None | ✅ 全部覆盖 |

**结论**：U3 符合方案。⚠️ 前端冒烟待执行。✅

---

### P1.1：表结构与读取门面 ✅ 通过

**表结构**（`src/data/storage/db.py`）：

| 表 | 与方案对比 | 状态 |
|----|-----------|------|
| `trend_param_sets` | 完全匹配（param_set/params_json/is_default/formula_version/created_at） | ✅ |
| `indicator_daily` | 完全匹配，含 `rsi_avg_gain`/`rsi_avg_loss`/`macd_ema12`/`macd_ema26` 递推状态列 | ✅ |
| `trend_daily` | 完全匹配，不含冗余 atr/er 列（D11 单一来源） | ✅ |
| WAL 模式 | `PRAGMA journal_mode=wal`，启动时设置 | ✅ |

**读取门面**（`src/data/indicator_store.py`，312 行）：

| 检查项 | 状态 | 说明 |
|--------|------|------|
| `get_series()` 缓存优先+回退 | ✅ | 先查缓存表，未命中 → `compute_live_series` 实时算 |
| 缓存新鲜度检查 | ✅ | `_cache_fresh` 对比 last bar date vs cache max date |
| version 不匹配回退 | ✅ | 检查 `formula_version` |
| `get_series_with_intraday` | ✅ | 追加 intraday_row 到 EOD 序列 |
| `compute_intraday_row` 递推逻辑 | ✅ | SMA 尾窗、EMA/MACD 用昨日状态列递推、RSI 用 avg_gain/loss 递推 |

**设计偏离**：方案规定 `get_series(symbol, indicator, include_intraday=False)` 单一接口；实现将 `include_intraday` 拆为独立函数 `get_series_with_intraday`。**这降低了接口统一性，但功能等价。**

**测试**：`tests/integration/test_indicator_store.py`（120 行）

| 检查项 | 状态 |
|--------|------|
| 缓存命中返回正确值 | ✅ |
| atr 单一来源等价断言（D11） | ✅ |
| trend 列从缓存读取 | ✅ |
| 递推状态列入库 | ✅ |
| 缺失标的回退实时算 | ✅ |
| version 不匹配回退 | ✅ |
| 缓存过期回退 | ✅ |
| 未知标的返回空 | ✅ |
| 未知指标名抛错 | ✅ |
| param_set save/get | ✅ |
| default 唯一性约束 | ✅ |

**结论**：P1.1 符合方案（含 1 个次要接口设计偏离）。✅

---

### P1.2：写入管线 `services/indicator_builder.py` ✅ 通过

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 整标全量重建 | ✅ | `rebuild_symbol` 向量化，单标的毫秒级 |
| `rebuild_all` | ✅ | 遍历全部标的 |
| `rebuild_if_needed` 启动校验 | ✅ | hash 校验 → 自动重建 |
| 参数 hash（D3 规范） | ✅ | `TREND_FORMULA_VERSION + sorted_keys + json.dumps` |
| 除权检测 D9 | ✅ | `detect_adjustment_breaks` 重拉最近 10 根对比 |
| 除权修复 | ✅ | `repair_broken_symbols` 全量重拉 |
| 重建前备份 D10 | ✅ | `VACUUM INTO` 快照，保留最近 N 份 |
| 16:30 日更钩子 | ✅ | `core/jobs.py` 尾部调用 `run_post_update_pipeline` |
| 双 FORMULA_VERSION | ✅ | indicator 表用 `INDICATOR_FORMULA_VERSION`，trend 表用 `TREND_FORMULA_VERSION` |

**测试**：`tests/integration/test_indicator_builder.py`（171 行）

| 检查项 | 状态 |
|--------|------|
| rebuild 入库正确 | ✅ |
| rebuild_all + get_series | ✅ |
| 无数据标的处理 | ✅ |
| param registry 首次需重建 | ✅ |
| 注册后不需重建 | ✅ |
| config 变更触发重建 | ✅ |
| rebuild_if_needed 自举 | ✅ |
| normalized_json 确定性 | ✅ |
| 除权检测正确 | ✅ |
| 无除权不误报 | ✅ |
| pipeline 修复+重建 | ✅ |
| 备份创建+剪裁 | ✅ |
| WAL 模式确认 | ✅ |

**结论**：P1.2 完全符合方案。✅

---

### U4a：services 分层 ✅ 通过

| 检查项 | 状态 | 说明 |
|--------|------|------|
| `services/market_indicators.py` | ✅ | 指标套装从 market_view 下沉，123 行 |
| `services/dashboard.py` | ✅ | 看板构建从 subject_market 下沉，300 行 |
| `RevisionCache` 共享 | ✅ | subject_market 和 MCP 各自使用 |
| `_ma5`/`_strength`/`_assign_strength` 单份 | ✅ | 只在 dashboard.py |
| `_detect_trend_phase` 位置 | ⚠️ | 仍在 `data/intraday_service.py`（评审建议移至 services，但非阻塞） |
| router 瘦身 | ✅ | `market_view.py` 352 行（原 556）；`subject_market.py` 184 行（原 439） |

**结论**：U4a 符合方案。✅

---

### P1.3：回测引擎批量读取 ✅ 通过（关键合并门槛）

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 全序列一次性计算 | ✅ | `ValueResolver._series_for` + `_series_cache` dict |
| 按交易日下标取值 | ✅ | `_value_at(name, series, idx, params)` |
| trend_score 序列化 | ✅ | `calculate_trend_score_series` 一次计算全序列 |
| trend_score_sma/ema | ✅ | 从 trend_score 序列 rolling/.ewm 得到 |
| 旧路径保留作回退 | ✅ | `_resolve_indicator_legacy` 路径保留（debug 模式） |
| `random_uniform` 永不缓存 | ✅ | 不在 `_MEMOIZABLE_INDICATORS` 中 |
| 逐笔 golden-master | ✅ | **所有 12 种指标策略 + random_uniform + seeded_random 三场景全部通过** |
| 回退路径一致 | ✅ | `memoize=False` 与 `memoize=True` 逐笔逐日完全一致 |

**`_rolling_ewm_last` 实现验证**：使用固定窗口 dot-product，数学上等价于 `trend_score_series(mode="ema")` 的旧行为。✅

**测试**：`tests/unit/test_p13_memoized_golden.py`（125 行）

```text
test_memoized_matches_legacy[bias] PASSED
test_memoized_matches_legacy[bias_atr] PASSED
test_memoized_matches_legacy[bollinger] PASSED
test_memoized_matches_legacy[ema] PASSED
test_memoized_matches_legacy[macd] PASSED
test_memoized_matches_legacy[momentum] PASSED
test_memoized_matches_legacy[rsi] PASSED
test_memoized_matches_legacy[sma] PASSED
test_memoized_matches_legacy[trend] PASSED
test_memoized_matches_legacy[trend_ema] PASSED
test_memoized_matches_legacy[trend_sma] PASSED
test_memoized_matches_legacy[volume] PASSED
test_seeded_random_matches_legacy PASSED
```

**结论**：P1.3 完全符合方案，合并门槛达标。✅

---

### P1.4：读路径切换与统一实时叠加 ⚠️ 部分完成

| 检查项 | 状态 | 说明 |
|--------|------|------|
| 看板走 `indicator_store` | ✅ | `dashboard.py` 用 `get_series` 查 trend_score（带回退） |
| MCP `calc_stop_loss` ATR | ✅ | 用 `get_series(symbol, "atr")` 单一来源（D11） |
| market_view 指标套装 | ⚠️ | 指标计算已委托 `services/market_indicators.py`，但**未走 indicator_store 缓存**——直接调用 `core.indicators` 实时算 |
| market_view intraday overlay | ⚠️ | 仍使用旧的 `compute_intraday_trend_score` + `build_synthetic_bar` 路径，**未收敛到 `indicator_store.compute_intraday_row`** |
| MCP symbol_detail | ✅ | 计算改走全历史（修复窗口截断 bug），但同样未走 indicator_store |

**分析**：P1.4 看板和 MCP 止损已切换；market_view 和 symbol_detail 的指标套装仍为实时计算路径（方案允许——"缓存未命中回退实时计算是永久特性"）。intraday 叠加未统一收敛到 `indicator_store`，三处（market_view、MCP、subject_market intraday）仍各有实现，但共享 `compute_intraday_trend_score` 核心函数。

**测试**：`tests/integration/test_intraday_overlay.py`（109 行）

| 检查项 | 状态 |
|--------|------|
| intraday row 与全量重算一致 | ✅ 所有 16 个指标列覆盖 |
| appended series 包含 intraday row | ✅ |
| series without row 仅 EOD | ✅ |

**结论**：P1.4 核心读路径已完成切换；intraday 叠加未完全统一到 indicator_store，但所有路径共享 `core.trend` + `core.indicators` 单一实现，数值一致性由测试保证。⚠️ 建议后续 PR 收敛 market_view intraday 到 `indicator_store`。

---

### U4b：任务管理器下沉 + MCP 薄化 ✅ 通过

| 检查项 | 状态 | 说明 |
|--------|------|------|
| `services/instrument_jobs.py` | ✅ | `BulkBackfillJobManager` + `InstrumentAddJobManager` 从 router 下沉 |
| `services/instrument_admin.py` | ✅ | 共享 helper 提取 |
| `instruments.py` router 瘦身 | ✅ | 1086 → 486 行 |
| MCP 不再 import 路由私有函数 | ✅ | `normalize_symbol` 来自 `core.symbols`，`_config_name_map` 来自 `services.instrument_admin` |

**结论**：U4b 符合方案。✅

---

### U5：小项批量 ✅ 通过

| 检查项 | 状态 | 说明 |
|--------|------|------|
| fee_rate 单常量 | ✅ | `DEFAULT_FEE_RATE = 0.0000854` in `rule_backtest/models.py` |
| 前端 `STATE_VALUES` 从 `/api/meta` 下发 | ✅ | `rule_backtest.py` meta endpoint 下发 state_values + stop_defaults |
| `tests/conftest.py` 引用 `DEFAULT_STRATEGY_CONFIG` | ✅ | `from core.strategy_config import DEFAULT_STRATEGY_CONFIG` |
| `enabled` 判定写法统一 | ✅ | 各处使用 `bool(item.get("enabled", True))` |

**结论**：U5 符合方案。✅

---

### U6：收尾 ✅ 通过

| 检查项 | 状态 | 说明 |
|--------|------|------|
| `symbol_detail` 窗口截断修复 | ✅ | 全历史计算指标后再 tail，EMA 类指标值不再依赖请求窗口 |
| 删除 `strategy/` 目录 | ✅ | 目录已完全删除 |
| 文档重写 | ✅ | README/CLAUDE.md/SKILL.md 已更新 |
| `.gitignore` 加 `data/backups/` | ✅ | |

**结论**：U6 符合方案。✅

---

## 三、发现的问题

### 🔴 Bug（必须修）

#### B1. `services/instrument_admin.py:88` — `logger` 未定义

```python
# instrument_admin.py 顶部没有 import logging，也没有 logger = ...
# 第 84-89 行：
def _category_priority_map() -> dict[str, int | None]:
    try:
        rows = get_db().list_instrument_categories()
    except RuntimeError as exc:
        logger.warning("Instrument categories unavailable: %s", exc)  # ← NameError!
        rows = []
```

**影响**：当 `list_instrument_categories()` 抛出 `RuntimeError`（如数据库不可用）时，会抛出 `NameError: name 'logger' is not defined`，遮盖原始异常。此函数被 `_build_new_instrument_record`（新增标的）和 `update_instrument`（更新标的分类）调用。

**修复**：在文件顶部添加 `import logging` 和 `logger = logging.getLogger(__name__)`（或改为 `get_logger`）。

---

### 🟡 分层违规（建议修）

#### L1. `core/jobs.py:93` — core 导入 services

```python
# core/jobs.py 第 93 行（在 daily_market_update_job 函数内）：
from services.indicator_builder import run_post_update_pipeline
```

**违反**：方案规定的依赖方向 `app/trend_mcp → services → core/data`。Core 不应依赖 services。

**建议**：将日更后的 indicator rebuild 编排逻辑上移到 services 层或 app 层（如 `app/main.py` 的 lifespan 中），让 `daily_market_update_job` 返回 payload 后由上层协调 rebuild。或者接受此 lazy import（运行时无循环依赖，单向），但需文档注明。

#### L2. `services/instrument_admin.py:11` — services 导入 app

```python
from app.instrument_display import load_instrument_name_map
```

**违反**：Services 层不应依赖 app 层。

**建议**：将 `load_instrument_name_map` 下移到 `services/instrument_admin.py` 本身（它只是对 `get_db().list_instrument_metadata()` 的封装），或提取到 `core/` 中。

#### L3. `trend_mcp/server.py:26` — MCP 导入 app router 辅助函数

```python
from app.instrument_display import format_symbol_display
```

**违反**：MCP 应只调 core/services，不应直接依赖 app router 内部模块。`format_symbol_display` 是纯展示函数。

**建议**：将 `format_symbol_display` 移至 `services/instrument_admin.py` 或 `core/symbols.py`。

---

### 🟢 设计偏离 / 次要问题

#### D1. `get_series` 缺少 `include_intraday` 参数

方案指定 `get_series(symbol, indicator, include_intraday=False) -> pd.Series`，实现使用独立函数 `get_series_with_intraday`。功能等价但接口不一致。**非阻塞**。

#### D2. market_view intraday overlay 未收敛到 indicator_store

P1.4 方案要求三处盘中实现全部收敛到 `indicator_store` 门面。market_view 的 intraday overlay 仍使用旧的 `compute_intraday_trend_score` + `build_synthetic_bar`。**功能正确但未完全统一**。

#### D3. `_num`/`_number` 辅助函数重复定义

4 个文件各自定义了功能相同的 safe-float 转换函数：
- `services/market_indicators.py:_num`
- `app/routers/market_view.py:_num`
- `services/dashboard.py:_number`
- `data/intraday_service.py:_number`

其中 `core/trend.py` 已有 `safe_float` 公共函数。建议收敛到 `core/trend.safe_float`。

#### D4. `intraday_service.py` 位置问题

`_detect_trend_phase`（趋势相位检测）属于展示逻辑，身在 `data/intraday_service.py`。按分层原则应属于 services 层。`services/dashboard.py` 通过 `from data.intraday_service import _detect_trend_phase` 导入私有函数（`_` 前缀），暗示模块归属不当。

---

## 四、测试情况

```text
============= 323 passed, 2 failed, 1 warning in 198.51s =============
```

**失败测试**（均为既有失败，非本分支引入）：

| 测试 | 原因 |
|------|------|
| `test_trend_history_not_empty` | 数据日期相关，master 基线即失败（方案 §9.4 已记录） |
| `test_trend_ma5_differs_from_trend_score` | 同上 |

**新增测试覆盖**：

| 测试文件 | 行数 | 覆盖范围 |
|----------|------|----------|
| `test_core_indicators.py` | 314 | U1 golden-master + 独立单元测试 |
| `test_core_trend.py` | 301 | U2 golden-master + 不变式测试 |
| `test_core_symbols.py` | 53 | U3 单元测试 |
| `test_indicator_store.py` | 120 | P1.1 缓存命中/回退/对账 |
| `test_indicator_builder.py` | 171 | P1.2 重建/param registry/除权/备份 |
| `test_p13_memoized_golden.py` | 125 | P1.3 逐笔 golden-master |
| `test_intraday_overlay.py` | 109 | P1.4 盘中叠加对账 |

**新增测试总计**：约 1,193 行，覆盖所有关键路径。

---

## 五、正确性底线核查

| 底线 | 状态 |
|------|------|
| ① 缓存永远只是加速器：任何读路径缓存缺失时必须能回退实时计算 | ✅ `get_series` 回退到 `compute_live_series`；`ValueResolver` 回退到 `_resolve_indicator_legacy` |
| ② 实时行永不落库；回测/风控只读 EOD | ✅ `compute_intraday_row` 仅内存计算；回测 `set_context_bars(all_bars)` 不含盘中行 |
| ③ 回测结果逐笔一致 | ✅ P1.3 golden 测试全部通过 |
| ④ 全项目同一概念一个实现 | ✅ 指标→`core/indicators.py`，趋势→`core/trend.py`，symbol→`core/symbols.py` |

---

## 六、合并建议

### 阻塞项（修完再合并）

1. **🔴 B1**：`services/instrument_admin.py` 缺少 `logger` 定义 → **NameError bug**

### 强烈建议（合并前修）

2. **🟡 L1**：`core/jobs.py` → `services/` 分层违规 → 重构或文档豁免
3. **🟡 L2**：`services/instrument_admin.py` → `app/` 分层违规 → 移动 `load_instrument_name_map`
4. **🟡 L3**：`trend_mcp/server.py` → `app/` 分层违规 → 移动 `format_symbol_display`

### 可后续优化

5. **D1–D4**：设计偏离与代码重复 → 非阻塞，可后续 PR 处理

### 冒烟待执行

6. 前端符号输入冒烟（U3验收 §8.7）
7. 4 页面 200 冒烟
8. MCP 5 工具各调用一次
9. 日更任务手动触发验证（非交易日/交易日）

---

## 七、代码统计

```text
46 files changed, 3903 insertions(+), 1844 deletions(-)
```

净增约 2,059 行，其中约 1,193 行为测试代码，约 866 行为生产代码。删除约 1,844 行旧代码（`strategy/` 包、`market_view.py`/`subject_market.py`/`instruments.py` 内联逻辑）。代码质量整体优秀。

---

*审查完成。建议优先修复 B1（logger 缺失），然后评估三个分层违规的处理方式（修复或文档豁免），其余非阻塞项可后续迭代。*
