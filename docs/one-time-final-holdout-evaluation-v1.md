# 一次性最终留出评估契约 v1

## 目标

最终留出集只用于一次独立效果估计，不能用于挑选模型、调整阈值、重新校准或生成第二次对比。系统在读取任何密封标签前锁定数据集、开发实验、开发报告、最新时间折、单一变体及其制品摘要，并要求真实审批人和审批理由。

## 接口

- `POST /go/evaluation-final-holdouts`：管理员执行一次最终评估，必须提供 `Idempotency-Key`。
- `GET /go/evaluation-final-holdouts`：管理员查看不可变聚合审计记录。
- `GET /go/evaluation-final-holdouts/{evaluationID}`：管理员读取单份聚合报告。

请求字段为 `dataset_id`、`experiment_id`、`development_report_id`、`fold_index`、`variant_name`、`variant_artifact_digest`、`approved_by` 和 `approval_reason`。开发报告必须明确没有访问最终留出，也没有进行自动模型选择；折次必须是冻结实验的最新开发折，变体名称与摘要必须精确匹配。

## 一次性与并发门禁

数据库对 `holdout_reservation_id` 建立唯一约束，同一预注册留出集最多形成一份最终报告。事务在读取密封标签前取得按留出 ID 划分的排他锁；并发请求、不同幂等键或改换变体都不能形成第二份结果。同一幂等键和完全相同的锁定请求只读回原报告。

评估必须晚于预注册的 `label_cutoff`，且密封样本数量必须与不可变数据集清单一致，所有结果均已真实成熟并在执行时点可用。缺少任何条件时不访问标签并明确拒绝。

## 输出与防泄漏

报告只公开聚合样本数、独立事件簇数、可评估/不可用数、准确率、方向比例、Rank IC 和可用时的概率校准指标。逐样本 ID、标签、分数和概率不进入报告；系统仅保存这些明细的不可逆证据摘要。原 `evaluation_dataset_samples` 始终保持 `sealed=true`。

固定治理字段为：

- `final_holdout_accessed=true`
- `sample_ids_shown=false`
- `automatic_model_selection=false`
- `selection_locked_before_holdout_access=true`
- `selection_decision=preapproved_single_variant_evaluated_once_no_model_selection`

评估成功、部分不可校准或所选变体无法覆盖样本都会形成不可变结果；不能以失败为理由换模型重试。同一留出完成后，后续模型批准仍必须由人工结合开发报告、最终报告、故障演练与研究质量证据独立决定。
