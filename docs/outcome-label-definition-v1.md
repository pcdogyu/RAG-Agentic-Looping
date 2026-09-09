# 真实结果标签定义 v1

版本：`prediction-outcome-label-v1`

本定义在模型训练和未来样本评估之前冻结。模型 scope 必须在预测发生前保存本版本；未预注册版本的旧预测会被排除，不能事后套用。任何期限、阈值、入场、价格、基准或成本口径变更都必须创建新版本，不能根据已观察结果修改本版本。

## 1. 预测目标与期限

支持两个明确目标：

- `absolute_up`：评价标的总回报相对零收益及中性区间的位置。
- `excess_up`：评价标的总回报减去已批准基准总回报后，相对中性区间的位置。

只支持 1、5、20 个交易日，分别使用 50、100、200 bps 的中性区间。每个 `prediction_run` 只绑定一个目标和期限，各期限独立成熟，不能用 1 日结果提前填充 5 日或 20 日标签。

## 2. 时间与价格

- 信号起点是 `signal_available_at`，不是新闻发布时间、研究开始时间或数据库创建时间。
- 入场价是信号可获得后第一个可观测交易日的价格；日线日期不能冒充盘中时间，同日收盘在保守规则下不用于当日信号。
- 退出价是入场后第 N 个实际可观测交易日的价格。周末、节假日和停牌不会按自然日补价。
- 股票标签必须使用供应商明确给出的 `adjusted_close`。只有普通 `close` 时返回 `adjusted_close_missing`，不能把拆股或分红造成的机械变化当成收益。
- 历史补采价格的 `available_at` 决定标签何时可用于训练；补采不会把信号前的交易日变成入场日。

## 3. 基准与收益字段

- 基准映射必须在 `signal_available_at` 时已经可获得且处于有效区间，使用精确资产 ID、市场和币种，不按 symbol 猜测。
- 标的和基准必须在相同入场、退出交易日期都有复权价格。缺日或币种不符时，相对标签不可用。
- `raw_return = exit_adjusted_close / entry_adjusted_close - 1`。
- `benchmark_return` 使用相同公式和日期。
- `excess_return = raw_return - benchmark_return`；`alpha_definition` 固定为 `arithmetic_asset_total_return_minus_benchmark_total_return`，不宣称是风险模型 alpha。
- v1 未配置风险模型，因此 `risk_adjusted_residual` 为空，`risk_adjustment_status=not_configured`。

标的上涨 2%、基准上涨 8% 时，同时记录 `absolute_label=up`、`relative_label=underperform` 和 `excess_return=-6%`。

## 4. 执行模拟

- 默认 `simulation_status=not_configured`、`research_result_only=true`，不生成 `net_return`，也不称为可实盘实现收益。
- 只有模型注册时不可变 scope 中已经保存 `execution_assumptions` 且 `enabled=true`，才计算模拟净收益。
- 假设必须明确 `side`、`round_trip_cost_bps`、`slippage_bps`、`borrow_bps` 和 `funding_bps`；所有成本在不可变 JSON 中独立保存，`borrow_bps` 只允许用于空头模拟。
- `gross_strategy_return` 按预注册 long/short 方向计算，`net_return = gross_strategy_return - total_cost_bps / 10000`。

## 5. 状态与排除

- `mature`：本目标所需价格与基准已经满足，标签可用。
- 未达到 N 个交易日：不写最终记录，保持 pending，后续任务重试。
- `unavailable`：期限已经形成资产价格，但目标必需的复权价、时点基准或对齐交易日缺失。
- `excluded`：预测无分数、目标/期限不受支持，或资产类别尚无相应标签政策。
- 数据源技术失败不写最终标签，由任务重试；质量报告分别披露 mature、pending、unavailable 和 excluded。

## 6. 持久化与接口

- `outcome_records` 以 `prediction_run_id` 为唯一键，最终标签只写一次。
- 保存定义版本、目标、期限、入退出时点与价格、绝对/相对标签、基准映射、执行假设、数据质量和缺失原因。
- `GET /go/outcome-labels/{assetID}` 返回标的标签历史。
- `GET /go/market-data-quality` 返回各标签状态数量；相对汇总只能读取具有 `benchmark_return` / `excess_return` 的成熟记录。
