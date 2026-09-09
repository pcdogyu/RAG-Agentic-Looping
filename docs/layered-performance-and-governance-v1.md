# 分层效果报告与模型治理契约 v1

## 范围

`layered-performance-report-v1` 只读取已经持久化的 `walk-forward-dataset-v1` 和
`baseline-calibration-experiment-v1` 开发分区。报告拒绝读取密封最终留出集，也不会产生模型选择、注册、晋升或自动训练动作。

## 三层报告

1. 研究层：披露候选预测数、关联研究数、完成、证据不足拒答、技术失败、缺失、覆盖率、拒答率、延迟及人工复核综合准确率。事实、关系、引用支持和拒答合理性由独立人工逐维标签计算准确率与 Wilson 95% 区间；没有该维标签时保持 `unavailable`，不以综合标签或零值替代。
2. 信号层：按时间折次和实验变体报告 accuracy、Wilson 95% 区间、正类率、预测方向偏斜、Rank IC、概率校准指标以及看多/非看多分组的绝对和超额收益。事件转载传递簇是独立样本单位；不跨折次合并显著性，小于 30 个样本明确警告。
3. 执行层：只有结果记录同时包含显式执行成本和净收益时才披露交易级净收益。没有组合权重、仓位及权益曲线时，换手、敞口和策略回撤保持不可用；资产收益或资产回撤不得冒充策略回撤。

数据质量层同时披露候选、开发分配、排除/拒绝、未成熟、缺失、最终留出计数及全部排除原因。报告中的 dataset/experiment 跟踪地址可以定位样本、模型/校准版本和排除项，但不会显示密封最终留出样本 ID。

## API

- `POST /go/evaluation-performance-reports`：管理员使用 `experiment_id`、`created_by` 和 `Idempotency-Key` 生成不可变报告。
- `GET /go/evaluation-performance-reports`：列出报告并固定声明 `development_only=true`、`final_holdout_accessed=false`。
- `GET /go/evaluation-performance-reports/{reportID}`：读取单个报告。
- `POST /go/research-quality-reviews`：管理员为研究运行写入事实、关系、引用支持及拒答合理性逐维标签，至少填写一个维度并携带 `Idempotency-Key`。
- `GET /go/research-quality-reviews`：按研究运行筛选读取不可变人工标签。

## Shadow、晋升和回滚

Shadow 比较要求正式模型处于 `approved`、候选模型处于 `shadow`，两者具有相同市场、目标、期限、信号截止和显式执行假设。监控仅生成 `observe`、`collect_more_forward_samples` 或 `alert_and_review` 记录。

新模型注册时必须在不可变 `scope.promotion_policy` 中预注册最少独立样本、最短 Shadow 周期、最大 ECE、Shadow 起点和回滚触发条件；同一版本不能用幂等重试替换该策略。晋升仍要求正确性硬门禁、足够独立样本、完整 Shadow 周期、校准阈值、未使用最终留出集选择及人工批准，实际门槛从预注册策略读取而非由晋升请求临时降低。新模型晋升时，同资产类别、市场、目标和期限的旧正式模型转为 `superseded`。回滚只允许管理员将当前 `approved` 模型切换到同范围、曾经获批的 `superseded` 版本；批准人、原因和引用版本写入治理审计。

## 故障演练

`model-failure-drill-v1` 覆盖数据源不可用、模型超时、特征分布漂移、校准过期及人工回滚门禁。演练运行真实的缺特征拒绝、PSI 漂移、校准有效期和人工批准判断，但不修改生产模型状态，数据库约束固定 `production_state_changed=false`。

- `POST /go/model-governance/failure-drills`：管理员提交 `scenario`、`created_by` 和 `Idempotency-Key`。
- `GET /go/model-governance/failure-drills`：读取不可变演练证据。
- `POST /go/model-governance/rollback`：显式人工回滚；请求包含当前/目标版本、批准人、原因和 `Idempotency-Key`。

演练通过只证明降级、告警和人工回滚门禁按设计工作，不证明候选模型质量足以晋升。真实前瞻样本、成熟结果和生产告警仍需持续积累与人工复核。
