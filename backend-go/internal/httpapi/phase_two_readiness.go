package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/consensus"
)

const phaseTwoReadinessVersion = "phase-two-readiness-v1"

type phaseTwoReadinessFacts struct {
	AnalystEvidence                   int  `json:"analyst_evidence"`
	ApprovedFundamentalPlans          int  `json:"approved_fundamental_plans"`
	ActiveEquityAssets                int  `json:"active_equity_assets"`
	BenchmarkCoveredActiveEquities    int  `json:"benchmark_covered_active_equities"`
	MatureEquityOutcomes              int  `json:"mature_equity_outcomes"`
	LargestMatureEquityMarket         int  `json:"largest_mature_equity_market"`
	PendingPredictionLabels           int  `json:"pending_prediction_labels"`
	HoldoutReservations               int  `json:"holdout_reservations"`
	WalkForwardDatasets               int  `json:"walk_forward_datasets"`
	DevelopmentExperiments            int  `json:"development_experiments"`
	LayeredPerformanceReports         int  `json:"layered_performance_reports"`
	ResearchQualityReviews            int  `json:"research_quality_reviews"`
	PassedFailureDrillScenarios       int  `json:"passed_failure_drill_scenarios"`
	ApprovedPredictionModels          int  `json:"approved_prediction_models"`
	LicensedBenchmarkImportReceipts   int  `json:"licensed_benchmark_import_receipts"`
	TradabilityImportReceipts         int  `json:"tradability_import_receipts"`
	SECIdentityConfigured             bool `json:"sec_identity_configured"`
	FinalHoldoutEvaluationImplemented bool `json:"final_holdout_evaluation_implemented"`
}

type phaseTwoReadinessGate struct {
	ID            string   `json:"id"`
	Stage         string   `json:"stage"`
	Title         string   `json:"title"`
	Status        string   `json:"status"`
	Current       int      `json:"current"`
	Required      int      `json:"required"`
	Unit          string   `json:"unit"`
	Blocking      bool     `json:"blocking"`
	ExternalInput bool     `json:"external_input"`
	Dependencies  []string `json:"dependencies"`
	Action        string   `json:"action"`
	Route         string   `json:"route,omitempty"`
	Authority     string   `json:"authority"`
}

type phaseTwoReadinessReport struct {
	Version             string                  `json:"version"`
	AsOf                time.Time               `json:"as_of"`
	OverallStatus       string                  `json:"overall_status"`
	CompletedGates      int                     `json:"completed_gates"`
	TotalBlockingGates  int                     `json:"total_blocking_gates"`
	AutomaticCompletion bool                    `json:"automatic_completion"`
	Facts               phaseTwoReadinessFacts  `json:"facts"`
	Gates               []phaseTwoReadinessGate `json:"gates"`
}

func (s *Server) phaseTwoReadiness(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	now := time.Now().UTC()
	facts, err := loadPhaseTwoReadinessFacts(r.Context(), s, now)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "phase two readiness query failed")
		return
	}
	writeJSON(w, http.StatusOK, buildPhaseTwoReadinessReport(facts, now))
}

func loadPhaseTwoReadinessFacts(ctx context.Context, s *Server, asOf time.Time) (phaseTwoReadinessFacts, error) {
	if s.db == nil {
		return phaseTwoReadinessFacts{}, fmt.Errorf("phase two readiness store is unavailable")
	}
	facts := phaseTwoReadinessFacts{
		SECIdentityConfigured: consensus.ValidateSECIdentity(s.cfg.SECIdentity) == nil,
		// The current repository seals final-holdout rows, but intentionally has
		// no one-time reveal/evaluation endpoint yet. Keep that engineering gap
		// explicit instead of inferring completion from a dataset or report count.
		FinalHoldoutEvaluationImplemented: false,
	}
	err := s.db.QueryRow(ctx, `SELECT
		(SELECT count(*)::int FROM analyst_evidence_records WHERE available_at<=$1),
		(SELECT count(*)::int FROM fundamental_research_plans WHERE status='approved' AND approved_at<=$1),
		(SELECT count(*)::int FROM assets WHERE active=true AND asset_class='equity'),
		(SELECT count(*)::int FROM assets a WHERE a.active=true AND a.asset_class='equity' AND EXISTS(
			SELECT 1 FROM benchmark_mapping_observations m JOIN assets b ON b.id=m.benchmark_asset_id
			WHERE m.subject_market=a.market AND m.subject_currency=a.currency
			  AND m.valid_from<=$1 AND (m.valid_to IS NULL OR m.valid_to>$1) AND m.observed_at<=$1 AND m.available_at<=$1
			  AND ((m.scope_type='asset' AND m.scope_id=a.id) OR (m.scope_type='market' AND m.scope_id=a.market))
			  AND b.market=m.benchmark_market AND b.currency=m.benchmark_currency)),
		(SELECT count(*)::int FROM outcome_records o JOIN prediction_runs p ON p.id=o.prediction_run_id
			WHERE p.asset_class='equity' AND o.status='mature' AND o.label_available_at<=$1),
		(SELECT coalesce(max(mature_count),0)::int FROM (
			SELECT count(*) mature_count FROM outcome_records o JOIN prediction_runs p ON p.id=o.prediction_run_id
			JOIN assets a ON a.id=p.asset_id WHERE p.asset_class='equity' AND o.status='mature' AND o.label_available_at<=$1 GROUP BY a.market
		) scoped_mature),
		(SELECT count(*)::int FROM prediction_runs p JOIN prediction_models m ON m.version=p.model_version
			WHERE m.scope->>'outcome_label_definition_version'='prediction-outcome-label-v1' AND p.signal_available_at<=$1
			AND NOT EXISTS(SELECT 1 FROM outcome_records o WHERE o.prediction_run_id=p.id)),
		(SELECT count(*)::int FROM evaluation_holdout_reservations WHERE created_at<=$1),
		(SELECT count(*)::int FROM evaluation_dataset_versions WHERE created_at<=$1),
		(SELECT count(*)::int FROM evaluation_experiments WHERE created_at<=$1),
		(SELECT count(*)::int FROM evaluation_performance_reports WHERE created_at<=$1),
		(SELECT count(*)::int FROM research_quality_reviews WHERE created_at<=$1),
		(SELECT count(DISTINCT scenario)::int FROM model_failure_drills WHERE passed=true AND production_state_changed=false AND created_at<=$1),
		(SELECT count(*)::int FROM prediction_models WHERE status='approved' AND created_at<=$1),
		(SELECT count(*)::int FROM licensed_benchmark_price_import_receipts WHERE available_at<=$1),
		(SELECT count(*)::int FROM market_tradability_import_receipts WHERE available_at<=$1)`, asOf).Scan(
		&facts.AnalystEvidence, &facts.ApprovedFundamentalPlans, &facts.ActiveEquityAssets,
		&facts.BenchmarkCoveredActiveEquities, &facts.MatureEquityOutcomes, &facts.LargestMatureEquityMarket,
		&facts.PendingPredictionLabels, &facts.HoldoutReservations, &facts.WalkForwardDatasets,
		&facts.DevelopmentExperiments, &facts.LayeredPerformanceReports, &facts.ResearchQualityReviews,
		&facts.PassedFailureDrillScenarios, &facts.ApprovedPredictionModels,
		&facts.LicensedBenchmarkImportReceipts, &facts.TradabilityImportReceipts)
	return facts, err
}

func buildPhaseTwoReadinessReport(facts phaseTwoReadinessFacts, asOf time.Time) phaseTwoReadinessReport {
	gates := []phaseTwoReadinessGate{
		readinessCountGate("analyst_evidence", "M1", "真实分析师证据", facts.AnalystEvidence, 1, "条", "waiting_human_input", true, nil, "在基本面工作台登记有来源、时点和批准人的真实证据。", "/fundamental"),
		readinessDependentCountGate("approved_fundamental_plan", "M1", "已批准无新闻定时计划", facts.ApprovedFundamentalPlans, 1, "份", facts.AnalystEvidence > 0, "waiting_human_input", true, []string{"analyst_evidence"}, "用同源人工研究输入完成计划审批；系统不得自动批准。", "/fundamental"),
		readinessBooleanGate("sec_identity", "M1", "可识别 SEC 访问身份", facts.SECIdentityConfigured, "waiting_human_input", true, nil, "在服务器环境配置符合 SEC 要求的机构名称与联系邮箱，不在页面或日志展示身份内容。", "/sources"),
		readinessRatioGate("pit_benchmark_coverage", "M2", "活跃股票 PIT 基准覆盖", facts.BenchmarkCoveredActiveEquities, facts.ActiveEquityAssets, "个资产", "waiting_human_input", true, nil, "逐市场批准有来源和有效期的 PIT 基准映射。", "/fundamental"),
		readinessCountGate("mature_forward_outcomes", "M3", "单一股票市场成熟前瞻标签", facts.LargestMatureEquityMarket, 100, "条", "waiting_natural_maturity", false, nil, "等待预登记的 1/5/20 交易日预测自然成熟并执行结果任务。", ""),
		readinessCountGate("sealed_holdout", "M4", "预注册最终留出集", facts.HoldoutReservations, 1, "份", "waiting_human_input", true, []string{"mature_forward_outcomes"}, "在查看未来结果前由人工预注册最终留出时段和用途。", ""),
		readinessDependentCountGate("walk_forward_dataset", "M4", "滚动前推数据集", facts.WalkForwardDatasets, 1, "版", facts.HoldoutReservations > 0 && facts.LargestMatureEquityMarket >= 100, "ready_for_manual_action", false, []string{"mature_forward_outcomes", "sealed_holdout"}, "使用成熟 PIT 样本生成不可变数据集清单。", ""),
		readinessDependentCountGate("development_experiment", "M4", "开发集基线、消融与校准实验", facts.DevelopmentExperiments, 1, "项", facts.WalkForwardDatasets > 0, "ready_for_manual_action", false, []string{"walk_forward_dataset"}, "在滚动训练、独立校准和未来测试折上运行可解释实验。", ""),
		readinessDependentCountGate("layered_performance_report", "M5", "分层效果报告", facts.LayeredPerformanceReports, 1, "份", facts.DevelopmentExperiments > 0, "ready_for_manual_action", false, []string{"development_experiment"}, "生成同时披露覆盖、失败样本、收益、校准和统计不确定性的报告。", ""),
		readinessBooleanGate("final_holdout_evaluation", "M5", "一次性最终留出评估", facts.FinalHoldoutEvaluationImplemented, "engineering_gap", false, []string{"layered_performance_report"}, "实现经人工批准、只读一次且不可用于模型选择的最终留出评估与审计记录。", ""),
		readinessCountGate("research_quality_reviews", "M5", "人工研究质量复核", facts.ResearchQualityReviews, 1, "条", "waiting_human_input", true, nil, "复核事实、关系、引用支持和拒答是否恰当。", ""),
		readinessCountGate("failure_drills", "M5", "五类故障与回滚演练", facts.PassedFailureDrillScenarios, 5, "类", "ready_for_manual_action", false, []string{"layered_performance_report"}, "完成数据源、模型超时、特征漂移、校准失效和人工回滚门禁演练。", ""),
		readinessDependentCountGate("approved_prediction_model", "M5", "人工批准的预测模型", facts.ApprovedPredictionModels, 1, "个", facts.LayeredPerformanceReports > 0 && facts.FinalHoldoutEvaluationImplemented && facts.PassedFailureDrillScenarios >= 5, "waiting_human_input", true, []string{"layered_performance_report", "final_holdout_evaluation", "failure_drills"}, "只有真实独立证据和人工审批齐备后才允许批准；不得自动发布。", ""),
	}
	report := phaseTwoReadinessReport{Version: phaseTwoReadinessVersion, AsOf: asOf.UTC(), OverallStatus: "blocked",
		AutomaticCompletion: false, Facts: facts, Gates: gates}
	for _, gate := range gates {
		if gate.Blocking {
			report.TotalBlockingGates++
			if gate.Status == "completed" {
				report.CompletedGates++
			}
		}
	}
	if report.TotalBlockingGates > 0 && report.CompletedGates == report.TotalBlockingGates {
		report.OverallStatus = "eligible_for_human_acceptance"
	}
	return report
}

func readinessCountGate(id, stage, title string, current, required int, unit, incompleteStatus string, external bool, dependencies []string, action, route string) phaseTwoReadinessGate {
	status := incompleteStatus
	if current >= required {
		status = "completed"
	}
	return phaseTwoReadinessGate{ID: id, Stage: stage, Title: title, Status: status, Current: current, Required: required,
		Unit: unit, Blocking: true, ExternalInput: external, Dependencies: nonNilStrings(dependencies), Action: action, Route: route,
		Authority: "database_or_server_configuration"}
}

func readinessDependentCountGate(id, stage, title string, current, required int, unit string, dependencyReady bool, incompleteStatus string, external bool, dependencies []string, action, route string) phaseTwoReadinessGate {
	gate := readinessCountGate(id, stage, title, current, required, unit, incompleteStatus, external, dependencies, action, route)
	if gate.Status != "completed" && !dependencyReady {
		gate.Status = "blocked_by_dependency"
	}
	return gate
}

func readinessBooleanGate(id, stage, title string, complete bool, incompleteStatus string, external bool, dependencies []string, action, route string) phaseTwoReadinessGate {
	current := 0
	if complete {
		current = 1
	}
	return readinessCountGate(id, stage, title, current, 1, "项", incompleteStatus, external, dependencies, action, route)
}

func readinessRatioGate(id, stage, title string, current, required int, unit, incompleteStatus string, external bool, dependencies []string, action, route string) phaseTwoReadinessGate {
	gate := readinessCountGate(id, stage, title, current, required, unit, incompleteStatus, external, dependencies, action, route)
	if required == 0 {
		gate.Status = "blocked_by_dependency"
	}
	return gate
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
