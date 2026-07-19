# Trend Quant 指标统一与预计算性能工程 总体执行方案

> 版本：v1.1（定稿） 日期：2026-07-19
> 变更说明：v1.0 经两路外部评审，意见已全部采纳并入（见 §11 修订记录）。本版为执行定稿。

---

## 1. 背景

Trend Quant 是一个 A 股 ETF 趋势跟踪系统（Python 3.11 / FastAPI / APScheduler / SQLite / pytest），当前生产形态：

- **Web 应用**（systemd 常驻，单 worker uvicorn）：4 个页面——策略管理（配置化规则回测）、标的查看（单标的 K 线+指标）、标的看板（全池趋势看板）、标的管理（增删改、回填、分类）；
- **MCP 服务**（挂载于 `/mcp/sse`）：5 个工具——trend_dashboard、intraday_dashboard、symbol_detail、calc_stop_loss、list_instruments；
- **每日任务**：16:30 增量补齐全部标的日 K（TickFlow 数据源，前复权 qfq 为主）；
- **存储**：单一 SQLite（当前约 384MB）。表：`market_data_qfq`（106 万行/610+标的）、`market_data_raw`、`instrument_metadata`（标的唯一来源）、`instrument_categories`、`rule_strategies`（策略唯一来源）、`job_runs`（任务记录）、`app_config`（配置键值，其中 strategy 行为单行 JSON、含 20 个活键）。

近一周已完成两轮治理（本方案建立在之上）：

1. **老旧功能清理**：删除参数优化、旧组合回测引擎、旧策略包、信号引擎、手工交易、日志页、notify 死代码，净删约 3.4 万行；
2. **存储收归**：instruments.yaml → instrument_metadata 表、strategy.yaml → app_config 表、运行记录 json → job_runs 表、删除 parquet 时代 data/market。config/ 现只剩 app.yaml，data/ 只剩 trend_quant.db。

当前代码约 8,400 行（src/），测试基线：213 通过 + 2 个既有失败（§9.4）。

## 2. 现状问题（本方案要解决的）

### 2.1 逻辑重复且已产生真实分歧

| 问题 | 现状 |
|---|---|
| 指标库三套 | `strategy/indicators.py`（atr+er）、`rule_backtest/indicators.py`（全套+trace）、`market_view.py` 内联全套。**RSI 一处 Wilder 一处 SMA（Cutler 变体）；MACD 柱一处 ×2 一处不 ×2；BIAS 一处百分数一处小数，且周期集合也不同（页面 6/12/24 vs 回测默认 20）** |
| 趋势值两套 | `strategy/trend_score_core.py`（标量快照）与 `market_view.compute_trend_indicator`（向量化重写），公式一致但主公式逐行重写、无共享保护机制 |
| symbol 标准化 4 处 | `market_view`（无 SH→SS 归一）、`instruments.py`（有）、`provider_utils`（反向）、前端 JS；MCP 靠 import 路由私有函数复用 |
| 看板辅助函数复制 | `_ma5`/`_strength`/`_assign_strength` 在 subject_market 与 intraday_service 各一份；分类过滤在 subject_market 与 MCP 逐字两份（注释自认 "mirrors"） |
| 盘中实时支持 3 处 | market_view 盘中叠加、intraday_service 盘中看板、MCP intraday 镜像，各写各的 |

### 2.2 分层问题

- `market_view.py`（556 行）= 路由 + 全套指标计算 + 趋势向量化实现；`subject_market.py`（439 行）= 路由 + 看板聚合；`instruments.py`（1086 行）= 页面 + API + 2 个任务管理器 + ~20 helper；
- `data/intraday_service.py`（620 行）身在数据层却做看板构建、相位判定等展示逻辑；
- MCP 跨层 import 路由私有函数（`_normalize_symbol`、`_config_name_map`、`_trend_config`）；
- 缓存逻辑复制：subject_market 与 MCP 各一份同构 dashboard cache（"same strategy as"）。

### 2.3 回测性能问题（性能主线）

规则回测引擎为 **O(n²) 调用模式**：

- `rule_backtest/engine.py` 每个交易日复制一次全历史前缀；
- `rule_backtest/indicators.py` 中 EMA 类指标（rsi/macd/ema）每天计算整条 rolling 序列再取末值（sma/bollinger 只取 tail 窗口，问题较轻）；
- `trend_score_sma(5)` 每天嵌套 5 次完整趋势快照；
- `value_resolver.py` 无记忆化。

关键事实：**指标数学本身（pandas 向量化）很快，慢的是调用模式**。全项目无任何指标缓存。

### 2.4 实时数据诉求

任何查看标的的地方（标的查看、标的看板、MCP）在交易时段都应看到**含当日实时数据**的指标。历史部分可缓存；当日未收盘数据不落库、只现场计算。当前盘中支持以 3 份复制实现存在（§2.1）。

### 2.5 数据源头隐患（评审新发现）

日更机制是**纯增量 append**，从不重拉历史。除权除息后 vendor 的 qfq 历史被回溯改写，库内 K 线在除权点断裂，且全项目没有除权事件检测。**指标重建救不了断掉的源数据**，必须配套解决（见 P1.2b）。

## 3. 改动目标

1. **唯一实现**：每种指标/趋势值/symbol 规则/看板派生计算，全项目只有一份实现；
2. **分层归位**：router 只剩 HTTP 编排，计算下沉 core，编排下沉 services，MCP 薄化；
3. **性能数量级提升**：默认参数指标预计算入库，回测改批量读取；读路径一律"缓存优先、未命中实时算"；
4. **统一实时叠加**："EOD 缓存 + 当日实时行"在所有查看入口一致呈现；实时行只用于查看，永不落库；
5. **零回归**：页面/看板/MCP 输出数值不变（除已批准语义统一项），回测结果逐笔一致。

## 4. 已锁定决策（含评审修订，均已拍板）

| # | 决策 | 依据 |
|---|---|---|
| D-语义1 | RSI 统一为 Wilder | 7 个策略无一使用 RSI，零影响（已查库验证） |
| D-语义2 | MACD 柱统一为 (DIF−DEA)×2 | 策略仅用 macd_line/macd_signal，零影响 |
| D-语义3 | BIAS 统一为**小数**（展示层 ×100）；周期——页面 6/12/24 走实时算，规则回测保留自定义周期参数 | 无策略使用；单位与周期分歧一并消除 |
| D1 | 缓存只存 EOD；盘中实时永远现场算 | 未收盘数据不具备确定性 |
| D2 | 宽表（一指标一列） | 口径集合稳定有限；一条 SQL 取回一个标的全部指标序列 |
| D3 | 参数注册表 `trend_param_sets`；v1 只实现 `default`；启动时 hash 校验不匹配自动重建。**hash 输入 = `TREND_FORMULA_VERSION` + 规范化 params_json（key 排序 + 固定浮点序列化格式，防抖动误触发）** | 解决"改参数缓存全废"；自定义参数走实时算 |
| D4 | 整标的全量重建，不做逐行增量 | rolling(200)/EMA 预热边界复杂；**qfq 除权回溯改写全部历史**；可自愈 vendor 静默修数；单标的毫秒级、全池秒级~十秒级 |
| D5 | 公式版本拆分为 `INDICATOR_FORMULA_VERSION`（core/indicators.py）与 `TREND_FORMULA_VERSION`（core/trend/），各表校验各自的版本 | 趋势公式变更不牵连指标表全量重建 |
| D6 | 开启 SQLite WAL | 构建期间不阻塞读取；WAL 下备份须 `VACUUM INTO` 或先 checkpoint |
| D7 | 止损/回测只读 EOD 缓存；实时行仅供查看、永不落库 | 风控与复盘必须用确定数据 |
| D8（新） | **展示类辅助指标不进缓存**（bias6/12/24、vol_ma5/10 等）：边界 = "缓存服务回测/看板核心指标；展示辅助指标从 K 线尾窗实时算"（单标的微秒级） | 防止宽表膨胀；评审两份共识 |
| D9（新） | **除权检测配套**：日更时对每标的重拉最近 10 根与库内值比对，不一致 → 该标的历史全量重拉 + 指标重建 | ETF 分红是常态；源 K 线断裂则一切下游皆错（§2.5） |
| D10（新） | **重建前自动备份**：builder 全量重建前 `VACUUM INTO` 快照，保留最近 3 份于 `data/backups/` | 防最低频最高损的"写坏库"；此前 ".bak" 仅为手工文件 |
| D11（新） | **ATR 单一来源**：`calc_stop_loss` 及一切止损相关 ATR 只从 `indicator_daily.atr` 取；trend_daily 不冗余 ATR | 杜绝"两个 ATR"历史问题重演 |
| D-边界 | 当前为单 worker uvicorn；若未来多 worker，启动重建需加文件锁/表锁 | 记录于 P1.2 设计注记 |

## 5. 核心问题与重点难点

1. **缓存正确性的地基是"单实现"**：缓存由实现 A 算、实时由实现 B 算，则参数 hash 防不住漂移。U1/U2 是 P1 的硬前置。
2. **前复权回溯性**：除权后该标的全部历史 K 线变化 → 派生指标全失效 → 整标重建（D4）+ 除权检测（D9）双管齐下。
3. **引擎改造必须结果不变**：P1.3 改变取数方式但输出必须逐笔一致——逐笔级 golden-master 验收（§8.3）。
4. **"缓存与实时是同一把尺"**：昨日行来自统一实现预计算，今日行用同一实现现场算，任何入口不得出现两个口径。
5. **EMA 类指标无限记忆**：截断窗口算指标会导致同一天因窗口不同返回不同值（现存 symbol_detail 问题）；预计算在全历史上进行天然规避，P1 后 symbol_detail 改读缓存自然解决。
6. **盘中递推需要中间状态**：RSI(Wilder) 递推需要昨日 avg_gain/avg_loss，MACD 递推需要昨日 ema12/ema26——终值无法反推，**状态列必须入表**（评审最重要的共同发现，见 P1.1）。
7. **买入价（未复权）vs qfq 序列尺度混用**：`calc_stop_loss` 的 buy_price 是实际成交价，ATR 来自 qfq 序列（现存问题，非本方案引入）。处理：缓存表标注 `price_mode='qfq'`、MCP 文档注明口径；实盘级修复另立项。

## 6. 目标架构

```
src/
├─ core/                        # 领域核心：纯计算，零 HTTP 零展示
│   ├─ indicators.py            # U1★ 统一指标库（向量化；INDICATOR_FORMULA_VERSION）
│   ├─ trend/                   # U2★ 趋势值（序列版为规范实现；TREND_FORMULA_VERSION；strategy/ 并入撤销）
│   ├─ symbols.py               # U3★ symbol 标准化唯一实现（含 SH→SS 归一）
│   └─ calendar / benchmarks / strategy_config / settings / jobs / scheduler（已有）
├─ data/
│   ├─ storage/db.py            # +3 张新表（P1）
│   ├─ indicator_store.py       # P1★ 指标读取门面：缓存优先 + 实时叠加 + 未命中回退
│   └─ service.py / provider_* / intraday 合成 bar（保留）
├─ services/                    # U4★ 应用服务层（新建）
│   ├─ market_indicators.py     # 单标的指标套装（market_view 下沉）
│   ├─ dashboard.py             # 看板共享层：EOD+盘中合一（_ma5/_strength/相位/分类过滤/RevisionCache）
│   ├─ instrument_jobs.py       # 标的添加/批量回填任务管理器
│   └─ indicator_builder.py     # P1★ 预计算写入编排（日更钩子 + 手动重建 + 除权检测 + 重建前备份）
├─ rule_backtest/               # indicators.py 改为 core 薄适配（末值+trace）；引擎改批量读取（P1.3）
├─ app/routers/                 # 只剩 HTTP：参数解析 → services → 渲染/JSON
└─ trend_mcp/                   # 薄适配：只调 core/services，无跨层私有 import
```

依赖方向单向：`app/trend_mcp → services → core/data`。

## 7. 详细设计（分阶段）

### U1：统一指标库 `core/indicators.py`

- 全部实现为"Series 进 → Series 出"向量化函数：sma / ema / atr / rsi(Wilder) / macd(柱×2) / boll / bias(小数) / momentum_return / efficiency_ratio；
- 收敛三方实现：`strategy/indicators.py`、`rule_backtest/indicators.py`、`market_view.py` 内联；
- `rule_backtest/indicators.py` 保留薄适配：调 core 取末值 + 组 trace（trace 为回测特有，不进 core）；
- 含 `INDICATOR_FORMULA_VERSION` 常量；
- **独立单元测试**（不止 golden-master 对比）：空 Series、全 NaN、单元素、除权跳变、长度不足 period 等边界。

### U2：趋势值序列化 `core/trend/`

- 规范实现 = 全序列向量化（trend_score / trend_ma5 / trend_ma10 / price_direction / confidence 五列）；
- 原快照函数改为"取序列末行"兼容包装——机制上保证看板序列与单点快照永为同值；
- `market_view.compute_trend_indicator` 变一行委托；`strategy/` 目录并入后撤销（U6 删空目录）；
- 含 `TREND_FORMULA_VERSION` 常量；盘中 fixed_atr/fixed_volume 调用方式保持兼容。

### U3：统一 symbol 标准化 `core/symbols.py`（前移，小步）

- 取最严语义（含 SH→SS 归一）；4 处 Python 实现收敛；MCP 不再 import 路由私有函数；
- 前端 JS 镜像保留为输入预览（注明服务端为权威）；验收含前端符号输入冒烟。

### P1：指标预计算与回测提速（核心工程）

**P1.1 表结构与读取门面**

```sql
CREATE TABLE trend_param_sets (
    param_set TEXT PRIMARY KEY,            -- 'default' | 'p_<sha1[:12]>'
    params_json TEXT NOT NULL,             -- key 排序 + 固定浮点序列化格式
    is_default INTEGER NOT NULL DEFAULT 0,
    formula_version INTEGER NOT NULL,
    created_at TEXT NOT NULL);

CREATE TABLE indicator_daily (             -- 仅依赖 K 线的指标（EOD）
    symbol TEXT NOT NULL, time TEXT NOT NULL,
    atr REAL,                              -- 周期由 default 参数集锁定（=20），列名去周期化
    vol_ma20 REAL, er10 REAL,
    sma5 REAL, sma10 REAL, sma20 REAL, sma60 REAL, sma120 REAL, sma200 REAL,
    ema5 REAL, ema10 REAL, ema20 REAL,
    rsi14 REAL, macd_dif REAL, macd_dea REAL, macd_hist REAL,
    boll_mid REAL, boll_up REAL, boll_dn REAL,
    -- 以下 4 列不展示，仅供盘中实时行递推（D-难点6）
    rsi_avg_gain REAL, rsi_avg_loss REAL,
    macd_ema12 REAL, macd_ema26 REAL,
    price_mode TEXT NOT NULL DEFAULT 'qfq',
    formula_version INTEGER NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (symbol, time));

CREATE TABLE trend_daily (                 -- 依赖完整趋势参数集（EOD）
    symbol TEXT NOT NULL, time TEXT NOT NULL,
    param_set TEXT NOT NULL DEFAULT 'default',
    trend_score REAL, trend_ma5 REAL, trend_ma10 REAL,
    price_direction REAL, confidence REAL,
    price_mode TEXT NOT NULL DEFAULT 'qfq',
    formula_version INTEGER NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (symbol, time, param_set));
```

`data/indicator_store.py` 读取门面：

```
get_series(symbol, indicator, include_intraday=False) -> pd.Series
```

- 默认返回缓存序列（截至上一交易日）；`include_intraday=True` 追加当日实时行（P1.4）；
- **缓存未命中（缺行/缺标的/version 不匹配/自定义参数）→ 回退 U1/U2 实时计算；回退是永久特性**；
- 展示类辅助指标（bias6/12/24、vol_ma5/10）不走此门面，由展示层从 K 线尾窗实时算（D8 边界）。

**P1.2 写入管线 `services/indicator_builder.py`**

- 整标的全量重建（向量化，单标的毫秒级）；事务批量 upsert；
- 挂载点：16:30 `daily_market_update_job` 尾部，重建变动标的（或全量，视 §8.6 实测）；
- 手动入口：全量/单标的重建；
- 启动时校验：`default` 参数集 hash（D3 定义）与各表 `formula_version` 不符 → 自动全量重建；
- **P1.2b 除权检测（D9）**：日更时对每标的重拉最近 10 根与库内比对，不一致 → 该标的历史全量重拉 + 指标重建；
- **重建前备份（D10）**：全量重建前 `VACUUM INTO` 快照到 `data/backups/`，保留最近 3 份；
- 开启 WAL（D6）；单 worker 前提，未来多 worker 需加重建锁（D-边界）。

**P1.3 规则回测引擎改造（提速主战场）**

- 运行开始一次性读取该标的全部所需指标序列（一条 SQL），运行中按交易日下标取值；
- `trend_score_sma/ema` 由缓存 trend_score 序列 rolling 得到，不再嵌套快照；
- 策略 JSON 自定义参数（indicator_config）走实时计算路径，不读缓存；
- `engine.py` + `value_resolver.py` 改造，保留原实时路径作回退；
- **合并门槛：回测结果逐笔一致（§8.3），不一致不合并**。

**P1.4 读路径切换与统一实时叠加**

- 看板（trend_ma5/strength/相位）、market_view 指标套装、MCP `calc_stop_loss` 的 ATR（D11：只从 `indicator_daily.atr` 取）：改走 `indicator_store`（带回退）；
- **实时叠加**：`include_intraday=True` 时——
  - 盘中（9:30-15:00 含午休）：`build_synthetic_bar`（唯一合成点）+ 昨日缓存行递推今日指标：SMA 取尾窗；EMA/MACD 用昨日 `macd_ema12/ema26` 递推 DIF→DEA→柱；RSI 用昨日 `rsi_avg_gain/rsi_avg_loss` 递推；BOLL 允许"昨日尾 19 根 + 当日 bar"的 O(period) std（注明非严格 O(1)，开销可忽略）；趋势值沿用 `compute_intraday_trend_score`（fixed 值改从缓存取）；
  - 收盘后~日更完成前：同上（用已收盘当日 bar）；非交易日：直接返回缓存序列；
  - 三处现存盘中实现（market_view 叠加 / intraday_service 看板 / MCP 镜像）全部收敛到该门面；
- 规则回测与止损计算永不走实时叠加（D7）。

### U4：services 分层（拆分两步执行）

- **U4a（前移，先于 P1.4）**：`market_view.py` 指标套装下沉 `services/market_indicators.py`；`services/dashboard.py` 建立（EOD+盘中看板构建合一，`_ma5`/`_strength`/`_assign_strength`/`_detect_trend_phase`/分类过滤各一份，`RevisionCache` 共享）——让 P1.4 的读路径切换直接发生在最终归宿模块，避免同一批代码改两遍、看板基线重采；
- **U4b（P1.4 之后）**：`services/instrument_jobs.py`（instruments.py 1086 → ~600 行）；MCP 只调 core/services，"mirrors" 注释全灭。

### U5：小项批量

- fee_rate 默认值收敛为 `rule_backtest/models.py` 单常量；
- 前端 `STATE_VALUES` 与止损默认参数改由 `/rule-backtest/api/meta` 下发；
- `tests/conftest.py` 的 `default_cfg` 改为引用 `DEFAULT_STRATEGY_CONFIG`；
- `enabled` 判定写法统一。

### U6：收尾

- `symbol_detail` 窗口语义修复（P1 后改读缓存自然解决）；
- 删除空 `strategy/` 目录（U1/U2 完成后仅剩壳）；
- 文档重写：README / CLAUDE.md / docs/* / trend-quant-mcp SKILL.md；
- `app_config.strategy` 是否做页面编辑入口（届时再议）。

## 8. 测试策略

### 8.1 总体原则

- **行为固化测试（golden-master）先行**：每步收敛前先把当前输出锁成测试；新实现必须通过这些测试（语义批准变更项除外，差异逐一解释并转人工确认）；
- 统一库另配**独立单元测试**（证明"新实现对"，不仅"和旧实现一样"）；
- 每阶段独立 commit、独立验收（pytest + 页面 + MCP 冒烟），可单独回滚；
- 现有基线 213 通过 + 2 个既有失败（§9.4），任何阶段不得新增失败。

### 8.2 U1/U2 一致性测试

1. **逐值相等测试**：多组合成 K 线（含 NaN、除权跳变、2000 根长序列、<20 根短序列），统一实现 vs 页面侧现行实现逐值相等；三个语义变更项（RSI/MACD 柱/BIAS）断言为"纯算法/单位差异、趋势同向"，人工确认后更新基线；
2. **快照=序列末行**（U2 核心不变式）：随机参数与数据，`snapshot(bars) == series(bars).iloc[-1]`；
3. 端到端基线：改前截取 subject_market 与 symbol_detail 的 JSON 响应，改后逐字段比对（U4a 前移后此基线只采一次）。

### 8.3 P1.3 回测逐笔一致验收（最高优先，合并门槛）

- 样本：现有 7 个策略全量（**确保 trend_score / trend_score_sma / trend_score_ema 各至少覆盖一条**——4 个策略在用 trend_score*）× 代表标的（宽基/行业/个股、长短历史）× 固定区间；
- 改前落盘全部 trades（日期/价格/数量/费用）与 equity 曲线；改后（缓存路径）重跑**逐笔逐日比对，完全一致**；
- 缓存未命中回退路径重跑一遍，与缓存路径一致；
- 不一致即不合并，无例外。

### 8.4 缓存 vs 实时对账

- 全量重建后抽样 N 标的：缓存值 vs 实时计算全序列比对，**绝对容差 1e-10 且相对容差 1e-9**，超限人工确认来源（浮点聚合顺序漂移属正常）；
- 覆盖 warmup 区与非 warmup 区；
- 修改 `app_config.strategy` → hash 校验触发重建 → default 参数集更新；bump 两个 FORMULA_VERSION 分别只触发对应表重建；
- **回退路径 4 场景**：缓存中间空洞、version 不匹配、default 参数集缺失、标的有 K 线但无指标行；
- **等价断言**：`indicator_daily.atr` ≡ U1 实时 atr（锁死 D11 单一来源，不靠巧合）。

### 8.5 实时叠加测试

- 固定时钟 + mock 报价：盘中当日行 == 全历史现场计算最后一行（SMA/EMA/MACD/RSI/BOLL/trend_score 全覆盖）；
- **三入口一致性**：同一标的同一时刻，market_view / subject_market / MCP symbol_detail 的当日实时指标一致；
- 边界：午休、15:00 后日更前、非交易日、报价异常缺字段、**除权跳变日的合成 bar**；
- **实时行不落库断言**：盘中多次调用后两张缓存表不含当日行。

### 8.6 性能基准

- 改前基线：3 个代表策略 × 5 个代表标的回测耗时（冷/热 DB 页缓存分别记录）；
- 改后同条件重测给出提速倍数（目标：看板场景 10×–100×，回测场景 100×–1000×，以实测为准）；
- 预计算写入耗时实测：单标的 / 全池 600+ 标的 → 决定日更尾部挂"变动标的"还是"全量"。

### 8.7 冒烟与回归

- 每阶段：pytest 全量；4 页面 200；MCP 5 工具各调用一次；日更任务手动触发（非交易日验证跳过，交易日观察 16:30 实跑与除权检测日志）；
- 部署后：systemd 重启、日志零错误、状态栏正常；
- U3/U4 验收加**前端符号输入冒烟**（SH→SS 归一行为不变）。

## 9. 风险与已知问题

### 9.1 主要风险与缓解

| 风险 | 缓解 |
|---|---|
| 统一实现导致数值变化 | golden-master 先行；语义差异逐一解释；页面侧逐值相等 |
| 缓存与实时结果不一致 | 单实现前置 + 抽样对账（§8.4）+ 双 formula_version |
| P1.3 引擎改造改错回测 | 逐笔一致验收（§8.3），不达标不合并 |
| 预计算写坏库 | 事务 upsert + 重建前 VACUUM INTO 快照（D10）+ 可随时全量重建 |
| 除权改坏历史数据 | D9 检测仅触发重拉，重拉结果写库前同样走既有 upsert 逻辑 |
| 回滚 | 每阶段独立 commit；缓存表可整体 DROP（读路径有回退兜底） |

### 9.2 明确的范围外

- 并行化/多进程回测（正确性优先，缓存+调用模式改造已足够）；
- 策略自定义参数的缓存（v1 只缓存 default 参数集）；
- 买入价与 qfq 尺度混用的实盘级修复（仅标注口径）；
- `app_config.strategy` 页面编辑入口（U6 再议）；
- 多 worker 部署（单 worker 为前提，扩展时加重建锁）。

### 9.3 正确性底线（不可违反）

1. 缓存永远只是加速器：任何读路径缓存缺失时必须能回退实时计算；
2. 实时行永不落库；回测/风控只读 EOD；
3. 回测结果逐笔一致是 P1.3 的合并门槛；
4. 全项目同一概念一个实现，新代码只收敛不新增副本。

### 9.4 既有问题（先于本方案存在）

- `tests/integration/test_intraday_service.py` 两个 trend_history 断言失败（master 基线即失败，疑为数据日期相关）——U2 阶段顺带排查；
- MCP `calc_stop_loss` 买入价与 qfq 尺度问题（§5-7，记录不修）；
- 文档全面滞后（U6 统一重写）。

## 10. 执行顺序与验收门禁

```
U1 统一指标库        → 验收：§8.2-1 逐值相等 + 独立单测 + 全量测试
U2 趋势值序列化      → 验收：§8.2-2 快照=末行 + 看板/图表基线比对
U3 symbols           → 验收：全量测试 + MCP 冒烟 + 前端符号输入冒烟
P1.1 三表+读取门面   → 验收：§8.4 对账/等价断言/回退 4 场景
P1.2 写入管线        → 验收：hash/version 触发重建 + 除权检测用例 + 备份生成 + §8.6 写入耗时
U4a 看板/指标下沉    → 验收：页面/MCP 输出基线比对（只采一次基线）
P1.3 引擎批量读取    → 验收：§8.3 逐笔一致（合并门槛）+ §8.6 提速倍数
P1.4 读路径+实时叠加 → 验收：§8.5 三入口一致 + 实时行不落库
U4b 任务管理器+MCP薄化 → 验收：全量测试 + 页面冒烟
U5 小项              → 验收：全量测试
U6 文档+窗口修复+删空 strategy/ → 验收：文档与实现一致性抽查
```

## 11. v1.1 修订记录（两路外部评审意见采纳清单）

| # | 修订 | 来源 |
|---|---|---|
| 1 | `indicator_daily` 增加 `rsi_avg_gain`/`rsi_avg_loss`/`macd_ema12`/`macd_ema26` 递推状态列；BOLL 实时行允许 O(period) 尾窗计算并注明 | 两评审共识（最重要发现） |
| 2 | FORMULA_VERSION 拆分为 INDICATOR/TREND 两个常量 | 评审 1 |
| 3 | trend_daily 删除冗余 atr/er/vol_ratio；ATR 单一来源 + 等价断言（D11） | 评审 2 |
| 4 | 新增 D9 除权检测配套（评审 2"最重要发现"）、D10 重建前备份、D8 展示指标边界 | 评审 2 + 拍板 |
| 5 | 执行顺序：U3 前移 P1.1 前；U4 拆分为 U4a（前移 P1.4 前）/U4b | 评审 2 |
| 6 | 事实修正：MCP 实为 5 工具；§2.3 sma/boll 措辞精确化；"活键 20 个"澄清；".bak 备份机制"措辞修正 | 评审 2 |
| 7 | BIAS 语义补全：单位小数 + 周期口径（D-语义3 更新） | 评审 2 新发现 |
| 8 | 测试补强：统一库独立单测；回退 4 场景；容差 abs 1e-10 + rel 1e-9；trend_score* golden 覆盖；除权跳变叠加用例；RSI 差异断言措辞 | 两评审 |
| 9 | 工程细节：D3 hash 输入与浮点序列化规范化；单 worker 注记；WAL 备份须知；编号重排（U6 收尾）；删空 strategy/ 目录；前端符号输入冒烟 | 两评审 |

---

*本方案已定稿，按 §10 顺序执行。每阶段开工前输出细化清单，完成后输出验收报告（含测试、冒烟、性能数据）。*
