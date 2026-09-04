package httpapi

import (
	"net/http"
	"sort"
)

// operation is the single source of truth for native route ownership. A route
// must not be listed here until its handler is implemented natively in Go.
type operation struct {
	ID      string
	Method  string
	Path    string
	Handler http.HandlerFunc
}

func (s *Server) operations() []operation {
	return []operation{
		// Batch 1: durable read APIs.
		{"assets_api_v1_assets_get", http.MethodGet, "/api/v1/assets", s.assets},
		{"news_api_v1_news_get", http.MethodGet, "/api/v1/news", s.news},
		{"events_api_v1_events_get", http.MethodGet, "/api/v1/events", s.events},
		{"research_runs_api_v1_research_runs_get", http.MethodGet, "/api/v1/research-runs", s.researchRuns},
		{"research_run_api_v1_research_runs__run_id__get", http.MethodGet, "/api/v1/research-runs/{runID}", s.researchRun},
		{"event_research_runs_api_v1_event_research_runs_get", http.MethodGet, "/api/v1/event-research-runs", s.eventResearchRuns},
		{"event_research_run_api_v1_event_research_runs__run_id__get", http.MethodGet, "/api/v1/event-research-runs/{runID}", s.eventResearchRun},
		{"recommendations_api_v1_recommendations_get", http.MethodGet, "/api/v1/recommendations", s.recommendations},
		{"list_conclusions_api_v1_conclusions_get", http.MethodGet, "/api/v1/conclusions", s.conclusions},
		{"get_conclusion_api_v1_conclusions__recommendation_id__get", http.MethodGet, "/api/v1/conclusions/{recommendationID}", s.conclusionDetail},
		{"get_event_conclusion_api_v1_event_conclusions__run_id__get", http.MethodGet, "/api/v1/event-conclusions/{runID}", s.eventConclusionDetail},
		{"outcomes_api_v1_outcomes_get", http.MethodGet, "/api/v1/outcomes", s.outcomes},
		{"evolutions_api_v1_evolution_get", http.MethodGet, "/api/v1/evolution", s.evolutions},
		{"model_logs_api_v1_model_logs_get", http.MethodGet, "/api/v1/model-logs", s.modelLogs},
		{"model_log_api_v1_model_logs__audit_id__get", http.MethodGet, "/api/v1/model-logs/{auditID}", s.modelLog},
		{"model_runtime_summary_api_v1_model_runtime_summary_get", http.MethodGet, "/api/v1/model-runtime-summary", s.modelRuntimeSummary},
		{"model_usage_summary_api_v1_model_usage_get", http.MethodGet, "/api/v1/model-usage", s.modelUsage},
		{"list_research_conclusions_api_v1_research_conclusions_get", http.MethodGet, "/api/v1/research-conclusions", s.researchConclusions},
		{"failed_research_runs_api_v1_failed_research_runs_get", http.MethodGet, "/api/v1/failed-research-runs", s.failedResearchRuns},

		// Batch 1 completion and batch 2: operations/configuration vertical slice.
		{"health_health_get", http.MethodGet, "/health", s.health},
		{"asset_universe_api_v1_asset_universe_get", http.MethodGet, "/api/v1/asset-universe", s.assetUniverse},
		{"industries_api_v1_industries_get", http.MethodGet, "/api/v1/industries", s.industries},
		{"asset_universe_status_api_v1_asset_universe_status_get", http.MethodGet, "/api/v1/asset-universe/status", s.assetUniverseStatus},
		{"edit_asset_api_v1_admin_assets__asset_id__patch", http.MethodPatch, "/api/v1/admin/assets/{assetID}", s.editAsset},
		{"get_source_filter_config_api_v1_source_filter_get", http.MethodGet, "/api/v1/source-filter", s.sourceFilter},
		{"update_source_filter_config_api_v1_source_filter_put", http.MethodPut, "/api/v1/source-filter", s.updateSourceFilter},
		{"reset_source_filter_config_api_v1_source_filter_delete", http.MethodDelete, "/api/v1/source-filter", s.resetSourceFilter},
		{"get_source_filter_logs_api_v1_source_filter_logs_get", http.MethodGet, "/api/v1/source-filter/logs", s.sourceFilterLogs},
		{"list_mcp_sources_api_v1_admin_mcp_sources_get", http.MethodGet, "/api/v1/admin/mcp-sources", s.mcpSources},
		{"create_mcp_source_api_v1_admin_mcp_sources_post", http.MethodPost, "/api/v1/admin/mcp-sources", s.createMCPSource},
		{"update_mcp_source_api_v1_admin_mcp_sources__source_id__put", http.MethodPut, "/api/v1/admin/mcp-sources/{sourceID}", s.updateMCPSource},
		{"delete_mcp_source_api_v1_admin_mcp_sources__source_id__delete", http.MethodDelete, "/api/v1/admin/mcp-sources/{sourceID}", s.deleteMCPSource},
		{"set_mcp_source_enabled_api_v1_admin_mcp_sources__source_id__enabled_patch", http.MethodPatch, "/api/v1/admin/mcp-sources/{sourceID}/enabled", s.enableMCPSource},
		{"discover_mcp_source_api_v1_admin_mcp_sources__source_id__discover_post", http.MethodPost, "/api/v1/admin/mcp-sources/{sourceID}/discover", s.discoverMCPSource},
		{"test_mcp_source_api_v1_admin_mcp_sources__source_id__test_post", http.MethodPost, "/api/v1/admin/mcp-sources/{sourceID}/test", s.testMCPSource},
		{"list_fact_source_groups_api_v1_admin_fact_source_groups_get", http.MethodGet, "/api/v1/admin/fact-source-groups", s.factSourceGroups},
		{"update_fact_source_group_api_v1_admin_fact_source_groups__group_id__put", http.MethodPut, "/api/v1/admin/fact-source-groups/{groupID}", s.updateFactSourceGroup},
		{"reset_fact_source_group_api_v1_admin_fact_source_groups__group_id__delete", http.MethodDelete, "/api/v1/admin/fact-source-groups/{groupID}", s.resetFactSourceGroup},
		{"test_fact_source_group_api_v1_admin_fact_source_groups__group_id__test_post", http.MethodPost, "/api/v1/admin/fact-source-groups/{groupID}/test", s.testFactSourceGroup},
		{"get_weknora_api_v1_integrations_weknora_get", http.MethodGet, "/api/v1/integrations/weknora", s.weknora},
		{"update_weknora_api_v1_admin_integrations_weknora_put", http.MethodPut, "/api/v1/admin/integrations/weknora", s.updateWeknora},
		{"test_weknora_api_v1_admin_integrations_weknora_test_post", http.MethodPost, "/api/v1/admin/integrations/weknora/test", s.testWeknora},
		{"research_queue_api_v1_research_queue_get", http.MethodGet, "/api/v1/research-queue", s.researchQueue},
		{"news_extraction_queue_api_v1_news_extraction_queue_get", http.MethodGet, "/api/v1/news-extraction-queue", s.newsExtractionQueue},
		{"model_inference_queues_api_v1_model_inference_queues_get", http.MethodGet, "/api/v1/model-inference-queues", s.modelInferenceQueues},
		{"model_queue_overview_api_v1_model_queue_overview_get", http.MethodGet, "/api/v1/model-queue-overview", s.modelQueueOverview},
		{"research_news_age_filter_api_v1_model_queues_research_news_age_filter_get", http.MethodGet, "/api/v1/model-queues/research/news-age-filter", s.researchNewsAgeFilter},
		{"update_research_news_age_filter_api_v1_model_queues_research_news_age_filter_put", http.MethodPut, "/api/v1/model-queues/research/news-age-filter", s.updateResearchNewsAgeFilter},
		{"scan_status_api_v1_scan_status_get", http.MethodGet, "/api/v1/scan/status", s.scanStatus},
		{"task_status_api_v1_tasks__task_id__get", http.MethodGet, "/api/v1/tasks/{taskID}", s.taskStatus},
		{"news_board_api_v1_news_board_get", http.MethodGet, "/api/v1/news-board", s.newsBoard},
		{"analysis_logs_api_v1_analysis_logs_get", http.MethodGet, "/api/v1/analysis-logs", s.analysisLogs},
		{"portfolio_api_v1_portfolio_get", http.MethodGet, "/api/v1/portfolio", s.portfolio},
		{"paper_order_api_v1_paper_orders_post", http.MethodPost, "/api/v1/paper-orders", s.paperOrder},
		{"stream_api_v1_stream_get", http.MethodGet, "/api/v1/stream", s.stream},

		// Batch 3: command, retry, queue-control, search, and change-feed APIs.
		{"backfill_asset_mappings_api_v1_admin_asset_universe_backfill_post", http.MethodPost, "/api/v1/admin/asset-universe/backfill", s.backfillAssetMappings},
		{"refresh_asset_universe_api_v1_admin_asset_universe_refresh_post", http.MethodPost, "/api/v1/admin/asset-universe/refresh", s.refreshAssetUniverse},
		{"admin_search_api_v1_admin_search_post", http.MethodPost, "/api/v1/admin/search", s.adminSearch},
		{"list_changed_targets_api_v1_changed_targets_get", http.MethodGet, "/api/v1/changed-targets", s.changedTargets},
		{"research_conclusion_again_api_v1_conclusions__recommendation_id__research_post", http.MethodPost, "/api/v1/conclusions/{recommendationID}/research", s.researchConclusionAgain},
		{"research_event_conclusion_again_api_v1_event_conclusions__run_id__research_post", http.MethodPost, "/api/v1/event-conclusions/{runID}/research", s.researchEventConclusionAgain},
		{"retry_event_research_run_api_v1_event_research_runs__run_id__retry_post", http.MethodPost, "/api/v1/event-research-runs/{runID}/retry", s.retryEventResearchRun},
		{"propose_evolution_api_v1_evolution_post", http.MethodPost, "/api/v1/evolution", s.proposeEvolution},
		{"execute_evolution_api_v1_evolution__candidate_id__execute_post", http.MethodPost, "/api/v1/evolution/{candidateID}/execute", s.executeEvolution},
		{"retry_failed_research_runs_api_v1_failed_research_runs_retry_post", http.MethodPost, "/api/v1/failed-research-runs/retry", s.retryFailedResearchRuns},
		{"clear_model_queue_api_v1_model_queues__queue_id__clear_post", http.MethodPost, "/api/v1/model-queues/{queueID}/clear", s.clearModelQueue},
		{"clear_model_instance_queue_api_v1_model_queues__queue_id__instances__instance_id__clear_post", http.MethodPost, "/api/v1/model-queues/{queueID}/instances/{instanceID}/clear", s.clearModelInstanceQueue},
		{"retry_model_instance_tasks_api_v1_model_queues__queue_id__instances__instance_id__retry_post", http.MethodPost, "/api/v1/model-queues/{queueID}/instances/{instanceID}/retry", s.retryModelInstanceTasks},
		{"retry_model_queue_tasks_api_v1_model_queues__queue_id__retry_post", http.MethodPost, "/api/v1/model-queues/{queueID}/retry", s.retryModelQueueTasks},
		{"retry_model_queue_task_api_v1_model_queues__queue_id__tasks_retry_post", http.MethodPost, "/api/v1/model-queues/{queueID}/tasks/retry", s.retryModelQueueTask},
		{"cancel_asset_mapping_task_api_v1_model_queues_assist_tasks_cancel_post", http.MethodPost, "/api/v1/model-queues/assist/tasks/cancel", s.cancelAssetMappingTask},
		{"clear_research_tasks_api_v1_model_queues_research_clear_post", http.MethodPost, "/api/v1/model-queues/research/clear", s.clearResearchTasks},
		{"cancel_research_task_api_v1_model_queues_research_tasks_cancel_post", http.MethodPost, "/api/v1/model-queues/research/tasks/cancel", s.cancelResearchTask},
		{"retry_news_processing_api_v1_news__news_id__retry_post", http.MethodPost, "/api/v1/news/{newsID}/retry", s.retryNewsProcessing},
		{"start_research_api_v1_research_post", http.MethodPost, "/api/v1/research", s.startResearch},
		{"retry_research_run_api_v1_research_runs__run_id__retry_post", http.MethodPost, "/api/v1/research-runs/{runID}/retry", s.retryResearchRun},
		{"start_scan_api_v1_scan_post", http.MethodPost, "/api/v1/scan", s.startScan},
		{"pause_scan_api_v1_scan_pause_post", http.MethodPost, "/api/v1/scan/pause", s.pauseScan},
		{"resume_paused_scan_api_v1_scan_resume_post", http.MethodPost, "/api/v1/scan/resume", s.resumeScan},
		{"rescan_source_filter_log_api_v1_source_filter_logs__log_id__rescan_post", http.MethodPost, "/api/v1/source-filter/logs/{logID}/rescan", s.rescanSourceFilterLog},
		{"list_target_changes_api_v1_target_changes_get", http.MethodGet, "/api/v1/target-changes", s.targetChanges},
	}
}

func operationIDs(items []operation) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	sort.Strings(ids)
	return ids
}
