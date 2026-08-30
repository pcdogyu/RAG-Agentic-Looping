package httpapi

import (
	"net/http"
	"sort"
)

// operation is the single source of truth for native route ownership. A route
// must not be listed here until its handler no longer calls the legacy API.
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
		{"scan_status_api_v1_scan_status_get", http.MethodGet, "/api/v1/scan/status", s.scanStatus},
		{"task_status_api_v1_tasks__task_id__get", http.MethodGet, "/api/v1/tasks/{taskID}", s.taskStatus},
		{"news_board_api_v1_news_board_get", http.MethodGet, "/api/v1/news-board", s.newsBoard},
		{"analysis_logs_api_v1_analysis_logs_get", http.MethodGet, "/api/v1/analysis-logs", s.analysisLogs},
		{"portfolio_api_v1_portfolio_get", http.MethodGet, "/api/v1/portfolio", s.portfolio},
		{"paper_order_api_v1_paper_orders_post", http.MethodPost, "/api/v1/paper-orders", s.paperOrder},
		{"stream_api_v1_stream_get", http.MethodGet, "/api/v1/stream", s.stream},
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
