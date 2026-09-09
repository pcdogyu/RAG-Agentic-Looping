# 可解释基线与独立校准实验契约 v1

版本：`baseline-calibration-experiment-v1`

输入数据集：`walk-forward-dataset-v1`

## 1. 目标与边界

本契约只在滚动前推数据集的开发折次中训练和比较模型。训练、独立校准、未来测试分别读取 `train`、`calibration`、`test`，查询同时强制 `fold_index >= 0` 和 `sealed=false`；最终留出集不参与调参、消融或模型选择。

当前支持标签契约中的二分类目标：

- `absolute_up`：`up` 为正类，`down` / `neutral` 为非正类；
- `excess_up`：`outperform` 为正类，`underperform` / `neutral` 为非正类。

方向分只作为单独的启发式对照或显式特征，不被解释为收益率或已经校准的概率。多分类模型和 temperature scaling 尚未实现，不以二分类 Platt 校准冒充多分类能力。

## 2. 同口径基线与消融

每个折次报告以下变体：

- `historical_rate`：只从训练分区估计带 Laplace 收缩的恒定正类率；
- `simple_rule`：按固定优先级选择可用的预期修订、价格反应、新闻新增信息或业务敞口特征，以零为阈值；
- `llm_direction`：只在 `llm_direction_score` 真实存在时评价，否则明确返回 `required_feature_unavailable`；
- `logistic_full`：使用价格、基本面、新闻新增信息和预期修订四组特征的可解释逻辑回归；
- `ablate_price`、`ablate_fundamental`、`ablate_news`、`ablate_expectations`：逐组删除特征。

逻辑回归及消融只在完整全特征的共同样本上运行，避免因覆盖率不同制造虚假的增量提升。均值、标准差和系数只从当前折次训练分区学习，模型制品保存训练截止、特征名、预处理参数、系数、样本数和由训练真值生成的版本。

若完整样本不足，模型返回 `unavailable`；若模型可评价但独立校准样本不足，则返回 `evaluated_uncalibrated`，不补造概率。

## 3. 独立概率校准

当前模型是二分类逻辑分数，因此使用 Platt sigmoid。校准器只拟合当前折次 `calibration` 分区，至少需要 30 个独立事件簇并同时包含正负结果。市场、资产类别、期限和来源模型版本必须完全匹配，预测时间不能早于校准截止，过期校准器降级为不可用。

概率只在未来 `test` 分区评价，报告 Brier、log loss、ECE 和十个可靠性分箱。每个分箱保存样本数、平均预测概率、实际发生率及 Wilson 95% 区间。概率严格位于 0 与 1 之间。

生产推理仍使用 `prediction.Service` 的同一 `calibration.Model.Apply` 路径。实验制品默认只是候选证据，不能自动注册、批准或切换线上模型；正式上线仍需 Shadow、治理硬门禁和人工批准。

## 4. 报告与复现

`evaluation_experiments` 保存数据集 ID/摘要、特征组、预处理政策、完整机器可读报告及制品摘要；`evaluation_experiment_variants` 保存每折每变体的模型、校准器和指标；`evaluation_experiment_predictions` 保存未来测试的逐样本分数、概率、标签、收益和正确性。

每个消融比较记录相同未来样本上的 accuracy 差异；双方均成功独立校准时还记录 Brier 改善量。缺少任一同口径结果时标记不可比较，不用零值代替。

相同数据集和代码配置生成相同实验 ID，幂等键不能绑定到不同实验。可通过原数据集清单定位训练、校准、测试样本及排除原因，通过逐样本预测复算汇总指标。

## 5. 管理接口

所有接口要求 `X-Admin-Token`，写接口还要求 `Idempotency-Key`：

- `POST /go/evaluation-experiments`：基于已持久化数据集生成开发实验；
- `GET /go/evaluation-experiments`：列出实验；
- `GET /go/evaluation-experiments/{experimentID}`：读回模型、校准器、消融、可靠性和逐样本未来结果。

生产没有数据集或成熟样本时保持空列表；不会创建模拟实验来冒充真实市场效果。
