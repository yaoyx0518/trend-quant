# kimi-k3 代码审查报告 2：core/unify-indicators 修复复审

> 审查人：kimi-k3 日期：2026-07-21（第二轮）
> 对象：修复 commit `1c24f51`（F1–F11），对照三份一审报告
> （dsv4-flash / dsv4-pro / kimi-k3）
> 方法：修复 diff 全量精读 + 全量 pytest + 生产库性能复测 + 针对 F3/F4/F5 的行为级合成数据实证 + 分层依赖静态核查
> 前序报告：`docs/kimi-k3-indicator-unification-review.md`

---

## 0. 总体结论

**F1–F11 全部修复正确，未发现修复引入的新问题，建议合并。**

- 我一审的 3 个严重问题（§2.1/§2.2/§2.3）与 6 个中等问题全部得到正确修复，且我逐个做了**行为级实证**（非仅代码阅读）；
- 看板构建耗时生产实测 **281s → 27s**（10.4×）；
- 全量测试 **320 通过 + 2 个既有失败**（= 一审 323 − 3 个随死代码删除的测试），基线声明属实；
- 一审遗留项与 flash 报告的"必须修复项"（`_cache_fresh` 日期字符串比较）经我专项核查**确认为实际无风险**（见 §3.1），同意不修或仅做防御性处理。

---

## 1. F1–F11 逐项验证

| # | 修复 | 验证方式 | 结论 |
|---|---|---|---|
| F1 | instrument_admin 补 `import logging` + logger | 导入实测 `hasattr(module,'logger')==True` | ✅ |
| F2 | 看板 fallback `get_series` 提升为每标的一次 | 生产复测 281s→27s；合成库 spy 测试确认 fallback 标的只调用 1 次 | ✅ |
| F3 | 除权修复起点改为 `min(backtest_start, 库内最早日期)` | 行为实证：mock 一个 2012 年起始的标的，`backfill_daily_history` 实收 `start_date=2012-05-28` | ✅ |
| F4 | `rebuild_if_needed` 增加 `indicator_global_version()` 独立校验（D5） | 行为实证：indicator 表版本=99 时触发重建并备份；版本=当前值时返回 `up_to_date` 不重建 | ✅ |
| F5 | bulk 查询加 `formula_version` 过滤 + 按标的比较 `trend_last >= market_last`，不满足走 `get_series` 回退 | 行为实证（合成双标的库）：新鲜标的走 bulk（零 `get_series` 调用），人为删 3 行造陈旧的标的精确 fallback 一次，且看板值 == 实时计算值（diff < 1e-9），不再静默 NULL | ✅ |
| F6 | display 助手下移 `core/display.py`，app 侧留兼容 shim；MCP 名称口径恢复 strip 变体 | 静态核查：`src/core/` 无 `from services`，`src/services/`、`src/trend_mcp/` 无 `from app`；实测 `format_symbol_display('510300.SS','沪深300ETF')=='沪深300'`（恢复 master 语义）；shim 再导出正常 | ✅ |
| F7 | MCP 盘中趋势改用全量 `full_df`，截断仅用于输出 | 代码核查：`full_df = df` 在 `tail(n)` 前保留，`hist = full_df.copy()`；EOD 与盘中现为"同一把尺" | ✅ |
| F8 | 三个回填完成点挂 `rebuild_after_backfill`（单标的 backfill / 批量回填 / 新增标的） | 代码核查：批量管理器 results 结构（`{"ok","result"}`）与过滤条件匹配；`all_failed` 在使用前定义；钩子在锁外、状态快照后执行，best-effort 带 try/except | ✅ |
| F9 | pipeline 编排上移 `app/main.py`，core/jobs 只回传 `payload["symbols"]` | 代码核查：非交易日 skip 路径正确早退；DataService 新建并 finally close；`grep` 确认 core 不再 import services | ✅ |
| F10 | MCP 改用共享 `RevisionCache`；"mirrors" 注释改写 | 代码核查 ✔（server.py:94/169/283、intraday_service.py:348 均已清理） | ✅ |
| F11 | 删 `provider_utils.normalize_symbol` 死代码（含其 3 个测试）；`_detect_trend_phase` 移入 `core/trend.py` | `grep` 确认无残留引用；dashboard 与 intraday_service 均改从 core.trend 导入 | ✅ |

### 生产数据复测细节

- 看板构建 **27.0s**（一审 281.3s），654→658 个标的；
- 一审时 6 个 NULL trend 标的复审时仍为 NULL——经逐个核查，它们是**新上市 ETF**
  （589600/589680/589720/589070/589550/551030，库存仅 7–13 根 K 线 < min_bars=22），
  属于趋势值合法不可用，**不是缓存陈旧问题**；F5 修复后陈旧场景已由合成数据实验证明会正确 fallback；
- `grep` 全库时间格式：`market_data_qfq` / `indicator_daily` / `trend_daily` 的 time
  列**全部为 19 字符 ISO 格式**（`YYYY-MM-DD HH:MM:SS`）。

---

## 2. 修复质量的几个亮点

1. **F5 的修法优于我的原建议**：不仅回退，还顺手给 bulk SQL 加了 `formula_version` 过滤，
   并把"是否用 bulk"的判定从"窗口内有没有行"（any）升级为"缓存末日期 ≥ 行情末日期"，
   与 `_cache_fresh` 语义对齐——看板两条读路径不再是"两把尺"。
2. **F4 顺带覆盖了一审未点名的场景**：`indicator_global_version()` 在空表时返回 None → 判 stale，
   使全新部署的首次启动也会走统一重建路径，与 trend 参数集的自举行为一致。
3. **F8 的批量回填钩子只收集 `status=="updated"` 的标的**做增量重建，避免全池重建；
   单标的钩子只在 "updated" 时触发，"up_to_date" 不做无谓重建——分寸正确。

---

## 3. 遗留项核对（含一审三份报告的全部未处理项）

### 3.1 flash 报告"必须修复项" `_cache_fresh` 字符串日期比较 —— 未修，但实证无风险

`indicator_store.py:160-170` 的 `str(trend_last) >= str(market_end)` 未被改动。
我核查了风险是否真的存在：

- 当前库内三张表的 time 列**全部 19 字符**（生产实测），字符串比较 == 日期比较；
- 即使未来 vendor 写入 10 字符日期（`YYYY-MM-DD`），分析两个方向的错位：
  - 缓存末日期 < 行情末日期：长串 vs 短串比较在日期部分即分出胜负 → 正确判 stale → fallback（安全）；
  - 同日不同格式：`"2026-07-21 00:00:00" >= "2026-07-21"` → True → 正确判 fresh；
  - 危险方向（短串 trend_last）在当前写入路径下不可能出现（缓存时间一律由 `str(pd.Timestamp)` 写入）。

**结论：失败方向只会退化为"不必要的 fallback"（安全），永远不会"误服陈旧数据"。**
同意列为防御性优化（`[:10]` 截断即可），不阻塞合并。

### 3.2 开发者已如实声明的未处理项（认可，建议下一迭代）

1. **P1.4 实时叠加门面接线**（我一审 §3.5）：`compute_intraday_row`/`get_series_with_intraday`
   仍只有测试调用——这是当前最大的方案偏差，建议作为下一迭代第一项；
2. **P1.3 实现方式偏离方案文字**（我一审 §3.7）：记忆化现场算 vs 方案写的 SQL 读缓存。
   我在一审已论证该选择实际上**更安全**（回测与缓存正确性解耦），建议在方案文档追认；
3. `get_series` 缺 `include_intraday` 参数（pro D1）：接口 cosmetic；
4. `_num`/`_number` 4 处重复（pro D3）：轻微；
5. `_cache_fresh` 不检测中间空洞（我一审 §5-2）：rebuild 是 DELETE+全量 INSERT，实践中不产生空洞。

### 3.3 本次复审新记录的轻微项（均不阻塞）

1. **F3/F4/F5 无新增自动化测试**：除权修复起点、indicator 版本分支、看板陈旧回退门——
   这三处是本轮修复的核心逻辑，目前只有我的临时脚本实证过，建议补 regression 测试
   （尤其 F5 的门控，未来改动看板时容易悄悄退化）；
2. `core/display.load_instrument_name_map` 吞异常时**去掉了原 app 版的 `logger.warning`**，
   DB 不可用时会静默返回空 map，错误可见性略降；
3. `_detect_trend_phase` 移入 core 后仍带下划线私有名，却被两个模块跨包导入——
   建议改名 `detect_trend_phase`（cosmetic）；
4. F9 后，pipeline 异常不再把 `daily_update` 标记为 failed（此时 daily_update 已记录成功），
   异常会冒泡到 scheduler 日志。语义上说得过去（两个 job 各自记录），但状态栏上看不到
   "日更成功但指标重建失败"的聚合视图——知晓即可；
5. F8 批量回填钩子在任务状态已置 "completed" 之后异步执行，重建失败只进日志、
   不回写任务状态（best-effort 设计如此，但前端看不到重建失败提示）。

---

## 4. 测试基线

```
.venv/bin/python -m pytest -q
→ 320 passed, 2 failed（155s）
```

- 2 个失败仍为 `test_intraday_service.py` 的两个 trend_history 断言（master 基线既有失败，方案 §9.4）；
- 320 = 一审 323 − 3（`test_provider_utils.py` 随 `normalize_symbol` 死代码删除的 3 个测试），
  与修复声明完全吻合；
- 无一审通过的测试在本轮修复后失败。

---

## 5. 与另外两份复审报告的一致性

dsv4-flash-2 与 dsv4-pro-2 的结论（F1–F11 全部验证通过、推荐合并）与本报告一致。
本报告额外独立完成的实证：F3/F4/F5 的行为级合成数据验证、看板生产性能复测（281s→27s）、
6 个 NULL 标的的根因排查（新上市 ETF，非 bug）、`_cache_fresh` 日期格式的全库审计。

## 6. 最终建议

**可以合并。** 合并后建议按优先级跟进：

| 优先级 | 事项 |
|---|---|
| 下一迭代首项 | P1.4 实时叠加门面接线（三处盘中实现收敛到 `indicator_store`），接线时先修一审 §5-1 记录的 `compute_intraday_row` 两个边界问题（不查缓存新鲜度、短历史 SMA 出值） |
| 高 | 为 F3/F4/F5 补 regression 测试 |
| 中 | 方案文档追认 P1.3 实现方式变更；`_cache_fresh` 日期比较加 `[:10]` 防御 |
| 低 | §3.3 各 cosmetic 项 |
