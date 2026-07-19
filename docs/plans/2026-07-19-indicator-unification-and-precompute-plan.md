# Trend Quant 指标统一与预计算性能工程 总体执行方案

> 版本：v1.0（评审稿） 日期：2026-07-19
> 状态：待评审。本文档自包含，供多方 review 取长补短。

---

## 1. 背景

Trend Quant 是一个 A 股 ETF 趋势跟踪系统（Python 3.11 / FastAPI / APScheduler / SQLite / pytest），当前生产形态：

- **Web 应用**（systemd 常驻，uvicorn）：4 个页面——策略管理（配置化规则回测）、标的查看（单标的 K 线+指标）、标的看板（全池趋势看板）、标的管理（增删改、回填、分类）；
- **MCP 服务**（挂载于 `/mcp/sse`）：4 个工具——trend_dashboard、intraday_dashboard、symbol_detail、calc_stop_loss、list_instruments；
- **每日任务**：16:30 增量补齐全部标的日 K（TickFlow 数据源，前复权 qfq 为主）；
- **存储**：单一 SQLite（当前约 384MB）。表：`market_data_qfq`（106 万行/610+标的，日 K 前复权）、`market_data_raw`、`instrument_metadata`（标的唯一来源）、`instrument_categories`、`rule_strategies`（配置化策略唯一来源）、`job_runs`（任务记录）、`app_config`（策略参数键值）。

近一周已完成两轮治理（本方案建立在之上）：

1. **老旧功能清理**：删除参数优化、旧组合回测引擎、旧策略包（趋势评分V1/动量前N）、信号引擎、手工交易、日志页、notify 死代码，净删约 3.4 万行；
2. **存储收归**：instruments.yaml → instrument_metadata 表、strategy.yaml → app_config 表（活键 20 个）、运行记录 json → job_runs 表、删除 parquet 时代 data/market。config/ 现只剩 app.yaml（基础设施），data/ 只剩 trend_quant.db。

当前代码约 8,400 行（src/），测试 213 通过 + 2 个既有失败（见 §9.4）。

## 2. 现状问题（本方案要解决的）

### 2.1 逻辑重复且已产生真实分歧

| 问题 | 现状 |
|---|---|
| 指标库三套 | `strategy/indicators.py`（atr+er）、`rule_backtest/indicators.py`（全套+trace）、`market_view.py` 内联全套。**RSI 一处 Wilder 一处 SMA；MACD 柱一处 ×2 一处不 ×2；BIAS 一处百分数一处小数**——页面看到的和回测判断用的不是同一个数 |
| 趋势值两套 | `strategy/trend_score_core.py`（标量快照）与 `market_view.compute_trend_indicator`（向量化重写），公式一致但无机制保证不漂移（第三处副本已随 skill 删除） |
| symbol 标准化 4 处 | `market_view`（无 SH→SS 归一）、`instruments.py`（有）、`provider_utils`（反向）、前端 JS；MCP 靠 import 路由私有函数复用 |
| 看板辅助函数复制 | `_ma5`/`_strength`/`_assign_strength` 在 subject_market 与 intraday_service 逐字两份；分类过滤在 subject_market 与 MCP 逐字两份（注释自认 "mirrors"） |
| 盘中实时支持 3 处 | market_view 盘中叠加、intraday_service 盘中看板、MCP intraday 镜像，各写各的 |

### 2.2 分层问题

- `market_view.py`（556 行）= 路由 + 全套指标计算 + 趋势向量化实现；`subject_market.py`（439 行）= 路由 + 看板聚合；`instruments.py`（1086 行）= 页面 + API + 2 个任务管理器 + ~20 helper；
- `data/intraday_service.py`（620 行）身在数据层却做看板构建、相位判定等展示逻辑；
- MCP 跨层 import 路由的私有函数（`_normalize_symbol`、`_config_name_map`、`_trend_config`）；
- 缓存逻辑复制：subject_market 与 MCP 各一份同构 dashboard cache（"same strategy as"）。

### 2.3 回测性能问题（本方案的性能主线）

规则回测引擎为 **O(n²) 调用模式**：

- `rule_backtest/engine.py` 每个交易日复制一次全历史前缀；
- `rule_backtest/indicators.py` 每天计算整条 rolling 序列再取末值；
- `trend_score_sma(5)` 每天嵌套 5 次完整趋势快照；
- `value_resolver.py` 无记忆化。

关键事实：**指标数学本身（pandas 向量化）很快，慢的是调用模式**。全项目无任何指标缓存，每次回测/看板/查看都从零重算。

### 2.4 实时数据诉求

用户要求：任何查看标的的地方（标的查看、标的看板、MCP）在交易时段都能看到**含当日实时数据**的指标。历史部分可缓存，但当日的未收盘数据不能落库、只能现场计算。当前盘中支持以 3 份复制实现存在（见 2.1）。

## 3. 改动目标

1. **唯一实现**：每种指标/趋势值/symbol 规则/看板派生计算，全项目只有一份实现；
2. **分层归位**：router 只剩 HTTP 编排，计算下沉 core，编排下沉 services，MCP 薄化；
3. **性能数量级提升**：指标以默认参数预计算入库，回测改为批量读取；所有读路径"缓存优先、未命中实时算"；
4. **统一实时叠加**："EOD 缓存 + 当日实时行"在所有查看入口一致呈现；实时行只用于查看，永不落库，回测/风控只用 EOD 确定数据；
5. **零回归**：页面/看板/MCP 输出数值不变（除已批准的语义统一项），回测结果逐笔一致。

## 4. 已锁定决策

| # | 决策 | 依据 |
|---|---|---|
| D-语义1 | RSI 统一为 Wilder（页面现行、TA-Lib 标准） | 现有 7 个策略无一使用 RSI，零影响 |
| D-语义2 | MACD 柱统一为 (DIF−DEA)×2（页面现行、国内习惯） | 策略仅用 macd_line/macd_signal，不用柱，零影响 |
| D-语义3 | BIAS 统一为小数（展示层需要时再 ×100） | 无策略使用，零影响 |
| D1 | 缓存只存 EOD；盘中实时部分永远现场计算 | 未收盘数据不具备确定性 |
| D2 | 宽表（一指标一列） | 缓存口径集合稳定有限；一条 SQL 取回一个标的全部指标序列，正是回测批量读取的形态；新口径走实时计算回退 |
| D3 | 参数注册表 `trend_param_sets`；v1 只实现 `default` 参数集；启动时 hash 校验，不匹配自动重建 | 解决"改参数缓存全废"顾虑；策略 JSON 自定义参数一律实时算，不碰缓存 |
| D4 | 整标的全量重建，不做逐行增量 | ① rolling(200)/EMA 预热窗口边界复杂；② **前复权在分红除权时会回溯改写全部历史 K 线**，行级增量无法处理；③ 整标重建可自愈 vendor 静默修数；④ 1700 根/标的向量化重算毫秒级，全池夜间重建秒级~十秒级 |
| D5 | 每行带 `formula_version`，公式变更 bump 常量触发全量重建 | 公式演进时缓存可失效重建 |
| D6 | 开启 SQLite WAL | 构建期间不阻塞读取 |
| D7 | 止损/回测只读 EOD 缓存；实时行仅供查看 | 风控与复盘必须使用确定数据 |

## 5. 核心问题与重点难点

1. **缓存正确性的地基是"单实现"**：若缓存由实现 A 计算、实时由实现 B 计算，参数 hash（D3）防不住实现漂移。故统一指标库/趋势值是硬前置（U1/U2 先于 P1）。
2. **前复权回溯性**：除权后该标的全部历史 K 线变化 → 派生指标全失效 → 只有整标重建能正确处理（D4 的核心论据）。
3. **引擎改造必须结果不变**：P1.3 改变回测引擎的取数方式，但输出必须逐笔一致——需要逐笔级 golden-master 验收（§8.3）。
4. **"缓存与实时是同一把尺"**：实时叠加行用统一实现现场算，昨日行来自统一实现预计算——任何查看入口不得出现两个口径（历史上已发生过同系统两个 ATR 的问题）。
5. **EMA 类指标的无限记忆**：RSI(Wilder)/MACD 的值依赖整条历史，截断窗口计算会导致同一天因窗口不同返回不同值（现存问题）；预计算在全历史上进行天然规避，symbol_detail 的窗口截断问题在 P1 后改为读缓存自然解决。
6. **买入价（未复权）vs qfq 序列的尺度混用**：`calc_stop_loss` 的 buy_price 是实际成交价，ATR 来自 qfq 序列，除权后两尺度不一致（现存问题，非本方案引入）。本方案处理：缓存表标注 `price_mode='qfq'`、MCP 文档注明口径；实盘级修复另立项。

## 6. 目标架构

```
src/
├─ core/                        # 领域核心：纯计算，零 HTTP 零展示
│   ├─ indicators.py            # U1★ 统一指标库（向量化 Series→Series；FORMULA_VERSION 常量）
│   ├─ trend/                   # U2★ 趋势值（序列版为规范实现；strategy/ 两文件并入撤销）
│   ├─ symbols.py               # U3★ symbol 标准化唯一实现（含 SH→SS 归一）
│   └─ calendar / benchmarks / strategy_config / settings / jobs / scheduler（已有）
├─ data/
│   ├─ storage/db.py            # +3 张新表（P1）
│   ├─ indicator_store.py       # P1★ 指标读取门面：缓存优先 + 实时叠加 + 未命中回退
│   └─ service.py / provider_* / intraday 合成 bar（保留）
├─ services/                    # U4★ 应用服务层（新建）
│   ├─ market_indicators.py     # 单标的指标套装（market_view 下沉）
│   ├─ dashboard.py             # 看板共享层：EOD+盘中合一（_ma5/_strength/相位/分类过滤/RevisionCache）
│   ├─ instrument_jobs.py       # 标的添加/批量回填任务管理器（instruments.py 下沉）
│   └─ indicator_builder.py     # P1★ 预计算写入编排（日更钩子 + 手动重建）
├─ rule_backtest/               # indicators.py 改为 core 薄适配（末值+trace）；引擎改批量读取（P1.3）
├─ app/routers/                 # 只剩 HTTP：参数解析 → services → 渲染/JSON
└─ trend_mcp/                   # 薄适配：只调 core/services，无跨层私有 import
```

依赖方向单向：`app/trend_mcp → services → core/data`。

## 7. 详细设计（分阶段）

### U1：统一指标库 `core/indicators.py`

- 全部实现为"Series 进 → Series 出"向量化函数：sma / ema / atr / rsi(Wilder) / macd(柱×2) / boll / bias(小数) / momentum_return / efficiency_ratio；
- 收敛三方实现：`strategy/indicators.py`、`rule_backtest/indicators.py`、`market_view.py` 内联；
- `rule_backtest/indicators.py` 保留薄适配层：调 core 取序列末值 + 组装 trace（trace 为回测调试特有，不进 core）；
- 模块内含 `FORMULA_VERSION` 常量（D5 的来源）。

### U2：趋势值序列化 `core/trend/`

- 规范实现 = 全序列向量化版本，输出五列：trend_score / trend_ma5 / trend_ma10 / price_direction / confidence；
- 原快照函数改为"取序列末行"的兼容包装——**从机制上保证看板序列与单点快照永为同值**；
- `market_view.compute_trend_indicator` 变一行委托；`strategy/` 目录撤销并入 `core/trend/`；
- 盘中快照（fixed_atr / fixed_volume 调用方式）保持兼容。

### P1：指标预计算与回测提速（核心工程，4 小步）

**P1.1 表结构与读取门面**

```sql
CREATE TABLE trend_param_sets (
    param_set TEXT PRIMARY KEY,            -- 'default' | 'p_<sha1[:12]>'
    params_json TEXT NOT NULL,             -- 规范化（key 排序）后的参数
    is_default INTEGER NOT NULL DEFAULT 0,
    formula_version INTEGER NOT NULL,
    created_at TEXT NOT NULL);

CREATE TABLE indicator_daily (             -- 仅依赖 K 线的指标
    symbol TEXT NOT NULL, time TEXT NOT NULL,
    atr20 REAL, vol_ma20 REAL, er10 REAL,
    sma5 REAL, sma10 REAL, sma20 REAL, sma60 REAL, sma120 REAL, sma200 REAL,
    ema5 REAL, ema10 REAL, ema20 REAL,
    rsi14 REAL, macd_dif REAL, macd_dea REAL, macd_hist REAL,
    boll_mid REAL, boll_up REAL, boll_dn REAL,
    price_mode TEXT NOT NULL DEFAULT 'qfq',
    formula_version INTEGER NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (symbol, time));

CREATE TABLE trend_daily (                 -- 依赖完整趋势参数集
    symbol TEXT NOT NULL, time TEXT NOT NULL,
    param_set TEXT NOT NULL DEFAULT 'default',
    trend_score REAL, trend_ma5 REAL, trend_ma10 REAL,
    price_direction REAL, confidence REAL,
    atr REAL, er REAL, vol_ratio REAL,     -- 冗余中间量，供止损复用
    price_mode TEXT NOT NULL DEFAULT 'qfq',
    formula_version INTEGER NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (symbol, time, param_set));
```

`data/indicator_store.py` 读取门面：

```
get_series(symbol, indicator, include_intraday=False) -> pd.Series
```

- 默认返回缓存序列（截至上一交易日）；
- `include_intraday=True`：追加"当日实时行"（见 P1.4）；
- 缓存未命中（缺行/缺标的/自定义参数）→ 回退 U1/U2 实现实时计算，**回退是永久特性不是过渡措施**。

**P1.2 写入管线 `services/indicator_builder.py`**

- 整标的全量重建（向量化，单标的毫秒级）；事务批量 upsert；
- 挂载点：16:30 `daily_market_update_job` 尾部，仅重建当日数据有变动的标的（或全量，视实测耗时）；
- 手动入口：脚本或管理端点，支持全量/单标的重建；
- 启动时校验：当前 `default` 参数集 hash 与 `formula_version` 与注册表不符 → 自动全量重建；
- 开启 WAL（D6）。

**P1.3 规则回测引擎改造（提速主战场）**

- 现状（O(n²)）见 §2.3。改为：运行开始一次性读取该标的全部所需指标序列（一条 SQL），运行中按交易日下标取值；
- `trend_score_sma(5)` 等复合指标由缓存的 trend_score 序列 rolling 得到，不再嵌套快照；
- 策略 JSON 自定义参数（indicator_config）走实时计算路径，不读缓存；
- `engine.py` + `value_resolver.py` 改造，保留原实时路径作为回退。

**P1.4 读路径切换与统一实时叠加**

- 看板（trend_ma5/strength/相位）、market_view 指标套装、MCP `calc_stop_loss` 的 ATR：改走 `indicator_store`（带回退）；
- **实时叠加**（用户核心诉求）：`get_series(..., include_intraday=True)` 时——
  - 盘中（9:30-15:00，含午休）：实时报价合成当日 bar（`build_synthetic_bar` 为唯一合成点），结合昨日缓存行增量计算今日指标（SMA 取尾窗、EMA/MACD/RSI 取昨日一个 EMA 值递推，O(1) 且与全历史重算数学等价）；
  - 收盘后~日更完成前：同上（用已收盘的当日 bar）；
  - 非交易日：直接返回缓存序列；
  - 趋势值当日行沿用 `compute_intraday_trend_score`（fixed_atr/fixed_volume 改从缓存取）；
  - 三处现存盘中实现（market_view 叠加 / intraday_service 看板 / MCP 镜像）全部收敛到该门面；
- 规则回测与止损计算**永不**走实时叠加（D7）。

### U3：统一 symbol 标准化 `core/symbols.py`

- 取最严语义（含 SH→SS 归一）；4 处 Python 实现收敛；MCP 不再 import 路由私有函数；
- 前端 JS 镜像保留为输入预览（文件头注明服务端为权威）。

### U4：services 分层

- `market_view.py` 指标套装下沉 `services/market_indicators.py`（556 → ~150 行）；
- `services/dashboard.py`：EOD（subject_market）+ 盘中（intraday_service）看板构建合一；`_ma5`/`_strength`/`_assign_strength`/`_detect_trend_phase`/分类过滤各一份；`RevisionCache`（get-or-recompute）供 subject_market 与 MCP 共用；
- `services/instrument_jobs.py`：两个任务管理器下沉（instruments.py 1086 → ~600 行）；
- MCP 只调 core/services，"mirrors" 注释全部消除。

### U5：小项批量

- fee_rate 默认值收敛为 `rule_backtest/models.py` 单常量（router/service 引用）；
- 前端 `STATE_VALUES` 与止损默认参数改由 `/rule-backtest/api/meta` 下发；
- `tests/conftest.py` 的 `default_cfg` 改为引用 `core.strategy_config.DEFAULT_STRATEGY_CONFIG`；
- `enabled` 判定写法统一。

### U7：收尾

- `symbol_detail` 窗口语义修复（P1 后改读缓存自然解决）；
- 文档重写：README / CLAUDE.md / docs/* / trend-quant-mcp SKILL.md（均已滞后于上述全部改动）；
- `app_config.strategy` 是否做页面编辑入口（届时再议）。

## 8. 测试策略

### 8.1 总体原则

- **行为固化测试（golden-master）先行**：每一步收敛实现前，先把"当前实现的输出"锁成测试；新实现必须通过这些测试（语义批准变更项除外，差异需逐一解释）；
- 每阶段独立 commit、独立验收（pytest + 页面冒烟 + MCP 冒烟），可单独回滚；
- 现有基线：213 通过 + 2 个既有失败（§9.4），任何阶段不得新增失败。

### 8.2 U1/U2 指标与趋势值一致性测试

1. **逐值相等测试**：构造多组合成 K 线（含 NaN、除权跳变、长序列 2000 根、短序列 <20 根边界），断言统一实现与"页面侧现行实现"输出逐值相等（RSI/MACD/BIAS 三个语义批准变更项：记录差异断言为"仅倍率/算法差"，即 Wilder vs SMA 的已知差异，转人工确认后更新基线）；
2. **快照=序列末行测试**（U2 核心不变式）：随机参数与随机数据，`snapshot(bars) == series(bars).iloc[-1]`；
3. 看板/图表端到端：改前截取 subject_market 与 symbol_detail 的 JSON 响应作为基线，改后逐字段比对。

### 8.3 P1.3 回测逐笔一致验收（最高优先）

- 选取代表策略集（现有 7 个策略全量）× 代表标的（宽基/行业/个股、长短历史各若干）× 固定区间：
  1. 改前跑一遍，落盘全部 trades（日期/价格/数量/费用）与 equity 曲线；
  2. 改后（缓存路径）重跑，**逐笔、逐日比对，必须完全一致**；
  3. 缓存未命中回退路径同样跑一遍，与缓存路径结果一致；
- 不一致即不合并，无例外。

### 8.4 缓存 vs 实时对账

- 全量重建后抽样 N 个标的：对缓存值与现场实时计算值做全序列比对（容差 1e-10），覆盖 warmup 区与非 warmup 区；
- 参数 hash 校验测试：修改 `app_config.strategy` → 触发重建 → default 参数集更新；
- formula_version bump → 触发全量重建的测试。

### 8.5 实时叠加测试

- 固定时钟 + mock 报价：交易时段内 `include_intraday=True` 的当日行 == 全历史现场计算的最后一行（SMA/EMA/MACD/RSI/trend_score 全覆盖）；
- 三入口一致性（验收标准）：同一标的同一时刻，market_view / subject_market / MCP symbol_detail 的当日实时指标一致；
- 边界：午休时段、15:00 后日更前、非交易日、合成 bar 缺字段（报价异常）时的行为；
- 实时行不落库断言：盘中多次调用后，`indicator_daily`/`trend_daily` 不含当日行。

### 8.6 性能基准

- 改前基线：3 个代表策略 × 5 个代表标的的回测耗时（含冷/热 DB 页缓存）；
- 改后同条件重测，给出提速倍数（预期数量级）；看板构建耗时同样记录；
- 预计算写入耗时实测：单标的 / 全池 600+ 标的，决定日更尾部挂"变动标的"还是"全量"。

### 8.7 冒烟与回归

- 每阶段：pytest 全量；4 页面 200；MCP 四工具各调用一次；日更任务手动触发（非交易日验证跳过路径，交易日观察 16:30 实跑）；
- 部署后验证：systemd 重启、日志零错误、状态栏正常。

## 9. 风险与已知问题

### 9.1 主要风险与缓解

| 风险 | 缓解 |
|---|---|
| 统一实现导致数值变化 | golden-master 先行；语义差异逐一解释；页面侧逐值相等 |
| 缓存与实时结果不一致 | 单实现前置 + 抽样对账（§8.4）+ formula_version |
| P1.3 引擎改造改错回测 | 逐笔一致验收（§8.3），不达标不合并 |
| 预计算写坏库 | 事务 upsert + WAL + 现有 .bak 备份 + 可随时全量重建 |
| 回滚 | 每阶段独立 commit；缓存表可整体 DROP（读路径有回退兜底） |

### 9.2 明确的范围外（本方案不处理）

- 并行化/多进程回测（用户风险偏好：正确性优先；缓存+调用模式改造已足够，并行属过度风险）；
- 策略 JSON 自定义参数的缓存（v1 只缓存 default 参数集）；
- 买入价与 qfq 尺度混用的实盘级修复（仅标注口径，另立项）；
- `app_config.strategy` 的页面编辑入口（U7 再议）。

### 9.3 正确性底线（不可违反）

1. 缓存永远只是加速器：任何读路径缓存缺失时必须能回退实时计算；
2. 实时行永不落库；回测/风控只读 EOD；
3. 回测结果逐笔一致是 P1.3 的合并门槛；
4. 全项目同一概念一个实现，新代码只收敛不新增副本。

### 9.4 既有问题（先于本方案存在，需顺带处理或记录）

- `tests/integration/test_intraday_service.py` 两个 trend_history 断言失败（master 基线即失败，与近两周全部改动无关，疑为数据日期相关）——U2 阶段顺带排查；
- MCP `calc_stop_loss` 买入价与 qfq 尺度问题（§5-6，记录不修）；
- 文档全面滞后（U7 统一重写）。

## 10. 执行顺序与验收门禁

```
U1 统一指标库      → 验收：§8.2-1 逐值相等 + 全量测试
U2 趋势值序列化    → 验收：§8.2-2 快照=末行 + 看板/图表基线比对
P1.1 表+读取门面   → 验收：§8.4 对账 + 回退测试
P1.2 写入管线      → 验收：§8.4 hash/version 触发重建 + §8.6 写入耗时
P1.3 引擎批量读取  → 验收：§8.3 逐笔一致（合并门槛）+ §8.6 提速倍数
P1.4 读路径+叠加   → 验收：§8.5 三入口一致 + 实时行不落库
U3 symbols         → 验收：全量测试 + MCP 冒烟
U4 services 分层   → 验收：页面/MCP 输出基线比对
U5 小项            → 验收：全量测试
U7 文档+窗口修复   → 验收：文档与实现一致性抽查
```

每阶段开工前输出细化清单，完成后输出验收报告（含测试、冒烟、性能数据）。

---

*评审时请重点关注：§4 决策表是否有误；§5 难点是否有遗漏；§7 P1 的表设计与读取门面设计是否合理；§8 测试策略是否足以兜底"零回归"。*
