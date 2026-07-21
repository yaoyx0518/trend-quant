# dsv4-flash 二次审查报告（修复验证）

> **审查范围**: `core/unify-indicators` 分支，commit `1c24f51`（修复提交）
> **审查依据**: 三份审查报告的 11 项问题清单（F1-F11）
> **审查日期**: 2026-07-21

---

## 1. 概述

二次审查验证了三份报告（dsv4-flash / dsv4-pro / kimi-k3）合计 11 项问题全部按要求修复。测试 320 通过 + 2 个既有失败（intraday 相关，master 基线即存在）。

---

## 2. 逐项修复验证

### F1 — instrument_admin logger NameError

- **问题**: `services/instrument_admin.py` 遗漏 `import logging`，兜底路径崩
- **修复**: 补 `import logging` + `logger = logging.getLogger(__name__)`
- **验证**: ✅ 三处 logger 使用均正常（`:88, :92, :116`）

### F2 — 看板 fallback 61× 冗余

- **问题**: `get_series` 按日在 listcomp 调用，每个缺失标的成本 = 天数 × 全量重算
- **修复**: 提升为每标一次 `trend_series = get_series(symbol, ...)`，按日期查值
- **验证**: ✅ 一行由 N 次变为 1 次

### F3 — 除权修复范围错误

- **问题**: 修复起点用 `backtest_start_primary`(2025)，569 个更早标的的修复后 K 线仍断
- **修复**: `repair_broken_symbols` 取该标的库内历史的最早日期 `times.min().date()` 重拉
- **验证**: ✅ `symbol_start = min(start_date, times.min().date())`

### F4 — INDICATOR_FORMULA_VERSION 变更永不触发重建

- **问题**: `rebuild_if_needed` 只校验趋势参数集，不校验指标版本
- **修复**: 新增 `db.indicator_global_version()` + `indicator_stale` 独立校验
- **验证**: ✅ 两表各自版本校验（D5）

### F5 — 看板 bulk 路径绕过新鲜度校验

- **问题**: `load_trend_daily_bulk` 无条件返回，陈旧缓存静默出 NULL
- **修复**: (1) 可选的 `formula_version` 过滤；(2) 按标的校验缓存末日期 ≥ 行情末日期；(3) 不满足走 `get_series` 降级
- **验证**: ✅ version 过滤 + 新鲜度检查 + fallback 路径

### F6 — services→app / MCP→app 分层违规 + 名称口径漂移

- **问题**: display 函数在 `app/instrument_display.py` 但被 services/MCP 调用；MCP 名称口径漂移
- **修复**: 新建 `core/display.py`，app shim 仅 re-export；MCP 口径统一为 strip 变体
- **验证**: ✅ `core/display.py` 存在，MCP 引用改为 `core.display`

### F7 — MCP symbol_detail 盘中趋势仍在截断窗口计算

- **问题**: 先 `df.tail(n)` 截断再计算趋势，EMA 类指标因截断产生不同末值
- **修复**: 保留 `full_df = df` 全量引用，盘中使用 `hist = full_df.copy()` 计算
- **验证**: ✅ 全量计算 + 仅输出截断

### F8 — 回填/新增标的后缓存不重建

- **问题**: 手动回填/新增后缓存未更新→看板走实时路径（性能降级）
- **修复**: 三个挂载点均调用 `rebuild_after_backfill([symbols])`：单个 backfill `instruments.py:432`、批量 backfill `instrument_jobs.py`、新增标的 `instrument_jobs.py`
- **验证**: ✅ 三个位置均已挂载

### F9 — core/jobs→services 分层违规

- **问题**: `core/jobs.py` import `services.indicator_builder`——core 依赖 services
- **修复**: pipeline 编排上移到 `app/main.py` 的 `update_job`，core 不再 import services
- **验证**: ✅ `core/jobs.py` 无 services import

### F10 — MCP 自带重复缓存 + mirrors 注释

- **问题**: MCP 独立缓存副本 + 3 处 "mirrors" 注释
- **修复**: 换用共享 `RevisionCache`；注释清理
- **验证**: ✅ `_dashboard_cache = RevisionCache()` + 无 "mirrors"

### F11 — provider_utils 死代码 + 相位函数位置

- **问题**: `provider_utils.normalize_symbol()` 死代码；`_detect_trend_phase` 在 data 层定义被 services 导入
- **修复**: 删死代码；`_detect_trend_phase` 移入 `core/trend.py`
- **验证**: ✅ 死代码已删；`core/trend.py` 含定义；`intraday_service.py` 从 core 导入

---

## 3. 方案偏差（已记录 / 下一迭代处理）

| 偏差 | 来源 | 说明 |
|------|------|------|
| P1.3 实现方式：SQL 批量读取→全系列记忆化 | kimi §3.7 | **实际上更安全**（回测与缓存正确性解耦）。建议方案文档追认 |
| `get_series` 缺 `include_intraday` 参数 | dsv4-pro D1 | 拆成 `get_series_with_intraday` 独立函数，接口 cosmetic |
| P1.4 实时叠加门面未接线 | kimi §3.5 | `compute_intraday_row` 已实现验证，尚未接到 market_view/看板 intraday 参数——下迭代处理 |

---

## 4. 结论

**二次审查通过。所有 11 项修复已验证。分支可以合并。**

| 维度 | 状态 |
|------|------|
| 必须修复 | 全部已修 |
| 建议修复 | 全部已修 |
| 外部报告问题 | 6 项全部已修 |
| 测试基线 | 320 通过 + 2 个既有失败 |

**合并建议**: ✅ 立即合并。P1.4 实时叠加门面接线和方案笔记追认作为下一迭代。
