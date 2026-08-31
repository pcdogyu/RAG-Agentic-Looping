package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	researchEventTask = "market_loop.research_event"
	researchAssetTask = "market_loop.research_asset"
)

var errResearchInactive = errors.New("research run was cancelled or superseded")

// permanentJobError records a terminal business failure without asking the
// durable queue to redeliver the same work. Research time limits are handled
// this way because the failure is already visible in the retry UI.
type permanentJobError struct{ error }

func (permanentJobError) Permanent() bool { return true }

type researchRuntime struct {
	cfg          config.Config
	db           *pgxpool.Pool
	redis        *redis.Client
	client       *http.Client
	nextEndpoint atomic.Uint64
}

func (runtime *researchRuntime) shared() *ExtractRuntime {
	return &ExtractRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis, client: runtime.client}
}

type researchEvidence struct {
	ID               string
	Claim            string
	SourceName       string
	SourceURL        string
	SourceQuality    string
	PublishedAt      time.Time
	ObservedAt       time.Time
	AsOf             time.Time
	Excerpt          string
	IndependentGroup string
	NumericValue     *float64
	NumericUnit      string
}

type assetResearchDraft struct {
	Summary               string   `json:"summary"`
	HistoricalContext     string   `json:"historical_context"`
	FinancialsAndGrowth   string   `json:"financials_and_growth"`
	ProductsOrProtocol    string   `json:"products_or_protocol"`
	Competition           string   `json:"competition"`
	ValuationOrTokenomics string   `json:"valuation_or_tokenomics"`
	Catalysts             []string `json:"catalysts"`
	Risks                 []string `json:"risks"`
	Invalidation          []string `json:"invalidation_conditions"`
	EvidenceIDs           []string `json:"evidence_ids"`
	DirectionScore        int      `json:"direction_score"`
	TransmissionPath      []string `json:"transmission_path"`
	MissingInformation    []string `json:"missing_information"`
}

type eventImpactDraft struct {
	TargetType       string   `json:"target_type"`
	TargetName       string   `json:"target_name"`
	AssetID          string   `json:"asset_id"`
	ActionID         string   `json:"action_id"`
	DirectionScore   int      `json:"direction_score"`
	TransmissionPath []string `json:"transmission_path"`
	Rationale        string   `json:"rationale"`
	EvidenceIDs      []string `json:"evidence_ids"`
	Missing          []string `json:"missing_information"`
}

type eventResearchDraft struct {
	Summary             string             `json:"summary"`
	AffectedMarkets     []string           `json:"affected_markets"`
	AffectedSectors     []string           `json:"affected_sectors"`
	Scenarios           []string           `json:"scenarios"`
	Catalysts           []string           `json:"catalysts"`
	Risks               []string           `json:"risks"`
	UnresolvedQuestions []string           `json:"unresolved_questions"`
	EvidenceIDs         []string           `json:"evidence_ids"`
	Impacts             []eventImpactDraft `json:"impacts"`
	MissingInformation  []string           `json:"missing_information"`
}

func NewResearchHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &researchRuntime{cfg: cfg, db: db, redis: redisClient, client: &http.Client{Timeout: cfg.ResearchTimeout}}
	return map[string]Handler{
		researchEventTask: runtime.researchEvent,
		researchAssetTask: runtime.researchAsset,
	}
}

func (runtime *researchRuntime) researchEvent(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, err
	}
	if len(envelope.Args) < 2 {
		return nil, errors.New("research_event requires event_id and run_id")
	}
	eventID, err := uuid.Parse(fmt.Sprint(envelope.Args[0]))
	if err != nil {
		return nil, err
	}
	runID, err := uuid.Parse(fmt.Sprint(envelope.Args[1]))
	if err != nil {
		return nil, err
	}
	run, event, err := runtime.loadEventResearch(ctx, runID, eventID)
	if err != nil {
		return nil, err
	}
	if supersededOrTerminal(run, job.ID.String()) {
		return run, nil
	}
	instanceID := runtime.nextResearchInstance(stringValue(envelope.Kwargs["model_instance_id"]))
	run["model_instance_id"] = instanceID
	run["status"], run["error"], run["updated_at"] = "running", nil, iso(time.Now())
	evidence, err := runtime.eventEvidence(ctx, runID, event, boolValue(run["historical_replay"]))
	if err != nil {
		return nil, runtime.failEventResearch(ctx, job, run, err)
	}
	run["evidence"] = evidencePayload(evidence, runID)
	appendAnalysisStep(run, analysisStep("event_evidence_gathering", "completed", "go-worker", fmt.Sprintf("已从事件关联新闻收集 %d 条证据，覆盖 %d 个独立来源。", len(evidence), independentGroupCount(evidence)), map[string]any{"evidence_count": len(evidence), "independent_sources": independentGroupCount(evidence)}))
	if err := runtime.saveEventResearch(ctx, run, evidence); err != nil {
		if errors.Is(err, errResearchInactive) {
			return map[string]any{"status": "superseded", "event_research_run_id": runID}, nil
		}
		return nil, runtime.failEventResearch(ctx, job, run, err)
	}
	draft, err := runtime.generateEventDraft(ctx, runID, event, evidence, instanceID)
	if err != nil {
		return nil, runtime.failEventResearch(ctx, job, run, err)
	}
	appendAnalysisStep(run, analysisStep("event_report_drafting", "completed", "ollama", fmt.Sprintf("已生成逐目标事件研报草稿，包含 %d 个目标，引用 %d 条证据。", len(draft.Impacts), len(draft.EvidenceIDs)), map[string]any{"direction_scores": impactScores(draft.Impacts), "citation_count": len(draft.EvidenceIDs)}))
	run["status"] = "verifying"
	if err := runtime.saveEventResearch(ctx, run, evidence); err != nil {
		if errors.Is(err, errResearchInactive) {
			return map[string]any{"status": "superseded", "event_research_run_id": runID}, nil
		}
		return nil, runtime.failEventResearch(ctx, job, run, err)
	}
	complete, missing, contradictions := verifyEventDraft(draft, evidence, parseTime(run["as_of"]))
	run["verification_round"], run["missing_requirements"], run["contradictions"] = 1, missing, contradictions
	verificationStatus := "completed"
	if !complete {
		verificationStatus = "incomplete"
	}
	appendAnalysisStep(run, analysisStep("event_report_verification", verificationStatus, "go-evidence-gate", fmt.Sprintf("第 1 轮事件研报校验%s：缺失 %d 项、矛盾 %d 项。", ternaryString(complete, "通过", "未通过"), len(missing), len(contradictions)), map[string]any{"round": 1, "evidence_complete": complete, "missing_requirements": missing, "contradictions": contradictions}))
	report := runtime.finalizeEventReport(event, draft, evidence, complete)
	run["report"] = report
	run["status"] = ternaryString(complete, "completed", "insufficient_evidence")
	run["retryable_reason"], run["error"], run["updated_at"] = nil, nil, iso(time.Now())
	appendAnalysisStep(run, analysisStep("event_report_finalization", "completed", "go-rating-engine", fmt.Sprintf("逐目标事件研报已定稿，共 %d 个目标。", len(anySlice(report["impacts"]))), map[string]any{"confidence": report["confidence"], "evidence_complete": complete, "target_count": len(anySlice(report["impacts"]))}))
	if err := runtime.saveEventResearch(ctx, run, evidence); err != nil {
		if errors.Is(err, errResearchInactive) {
			return map[string]any{"status": "superseded", "event_research_run_id": runID}, nil
		}
		return nil, runtime.failEventResearch(ctx, job, run, err)
	}
	queued, err := runtime.enqueueTargetResearches(ctx, event, report, 3)
	if err != nil {
		return nil, err
	}
	runtime.finishResearchTracking(ctx, job.ID.String(), "completed", job.Attempt, eventID.String(), stringValue(event["headline"]), stringValue(event["event_type"]), "", map[string]any{"target_research_queued": queued})
	return map[string]any{"status": run["status"], "event_id": eventID, "event_research_run_id": runID, "target_research_queued": queued}, nil
}

func (runtime *researchRuntime) researchAsset(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, err
	}
	if len(envelope.Args) < 3 {
		return nil, errors.New("research_asset requires asset_id, event_id and run_id")
	}
	assetID := fmt.Sprint(envelope.Args[0])
	runID, err := uuid.Parse(fmt.Sprint(envelope.Args[2]))
	if err != nil {
		return nil, err
	}
	run, err := runtime.loadRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if supersededOrTerminal(run, job.ID.String()) || stringValue(run["status"]) == "coalesced" {
		return run, nil
	}
	instanceID := runtime.nextResearchInstance(stringValue(envelope.Kwargs["model_instance_id"]))
	run["model_instance_id"] = instanceID
	started := time.Now().UTC()
	softCtx, cancel := context.WithTimeout(ctx, runtime.cfg.ResearchSoftLimit)
	defer cancel()
	run["status"], run["started_at"], run["error"], run["updated_at"] = "running", iso(started), nil, iso(started)
	if err := runtime.saveRun(ctx, run, nil); err != nil {
		if errors.Is(err, errResearchInactive) {
			return map[string]any{"status": "superseded", "run_id": runID}, nil
		}
		return nil, err
	}
	var event map[string]any
	if rawEventID := fmt.Sprint(envelope.Args[1]); rawEventID != "" && rawEventID != "<nil>" {
		if eventID, parseErr := uuid.Parse(rawEventID); parseErr == nil {
			event, _ = runtime.shared().loadEvent(softCtx, eventID)
		}
	}
	evidence, err := runtime.assetEvidence(softCtx, runID, event)
	if err != nil {
		return nil, runtime.handleAssetError(ctx, job, run, err)
	}
	run["evidence"] = evidencePayload(evidence, runID)
	appendAnalysisStep(run, analysisStep("evidence_gathering", "completed", "go-worker", fmt.Sprintf("已收集 %d 条标的研究证据。", len(evidence)), map[string]any{"evidence_count": len(evidence)}))
	if err := runtime.saveRun(softCtx, run, evidence); err != nil {
		if errors.Is(err, errResearchInactive) {
			return map[string]any{"status": "superseded", "run_id": runID}, nil
		}
		return nil, runtime.handleAssetError(ctx, job, run, err)
	}
	draft, err := runtime.generateAssetDraft(softCtx, runID, objectValue(run["asset"]), event, evidence, instanceID)
	if err != nil {
		return nil, runtime.handleAssetError(ctx, job, run, err)
	}
	appendAnalysisStep(run, analysisStep("report_drafting", "completed", "ollama", fmt.Sprintf("已生成研究草稿，方向分 %+d，引用 %d 条证据。", draft.DirectionScore, len(draft.EvidenceIDs)), map[string]any{"direction_score": draft.DirectionScore, "citation_count": len(draft.EvidenceIDs)}))
	run["status"] = "verifying"
	validIDs, warnings := validEvidenceIDs(draft.EvidenceIDs, evidence)
	complete := len(validIDs) > 0 && draft.Summary != ""
	missing := append([]string{}, draft.MissingInformation...)
	if len(validIDs) == 0 {
		missing = appendUnique(missing, "impact_evidence")
	}
	appendAnalysisStep(run, analysisStep("report_verification", ternaryString(complete, "completed", "incomplete"), "go-evidence-check", fmt.Sprintf("证据质量核验完成：有效引用 %d 条、提示 %d 项。", len(validIDs), len(warnings)+len(missing)), map[string]any{"evidence_complete": complete, "valid_citations": len(validIDs), "warnings": append(warnings, missing...)}))
	recommendation := runtime.finalizeAssetRecommendation(run, event, draft, evidence, validIDs, complete, append(warnings, missing...))
	run["recommendation"], run["status"], run["error"], run["retryable_reason"] = recommendation, "completed", nil, nil
	run["completed_at"], run["updated_at"] = iso(time.Now()), iso(time.Now())
	appendAnalysisStep(run, analysisStep("finalization", "completed", "go-rating-engine", fmt.Sprintf("最终状态 %s，方向分 %+d，新闻可信度 %.0f%%，评级置信度 %.0f%%。", recommendation["signal_status"], int(numberValue(recommendation["score"])), numberValue(recommendation["news_confidence"])*100, numberValue(recommendation["rating_confidence"])*100), map[string]any{"rating": recommendation["rating"], "signal_status": recommendation["signal_status"], "direction_score": recommendation["score"], "news_confidence": recommendation["news_confidence"], "rating_confidence": recommendation["rating_confidence"], "score_source": "llm"}))
	if err := runtime.saveRecommendationAndRun(softCtx, run, recommendation, evidence); err != nil {
		if errors.Is(err, errResearchInactive) {
			return map[string]any{"status": "superseded", "run_id": runID}, nil
		}
		return nil, runtime.handleAssetError(ctx, job, run, err)
	}
	runtime.finishResearchTracking(ctx, job.ID.String(), "completed", job.Attempt, assetID, stringValue(objectValue(run["asset"])["name"]), stringValue(objectValue(run["asset"])["symbol"]), "", map[string]any{"rating": recommendation["rating"], "score": recommendation["score"]})
	return map[string]any{"status": "completed", "run_id": runID, "recommendation_id": recommendation["id"]}, nil
}

func (runtime *researchRuntime) loadEventResearch(ctx context.Context, runID, eventID uuid.UUID) (map[string]any, map[string]any, error) {
	var runBody, eventBody []byte
	if err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM event_research_runs WHERE id=$1 AND event_id=$2`, runID, eventID).Scan(&runBody); err != nil {
		return nil, nil, err
	}
	if err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM news_events WHERE id=$1`, eventID).Scan(&eventBody); err != nil {
		return nil, nil, err
	}
	var run, event map[string]any
	if err := json.Unmarshal(runBody, &run); err != nil {
		return nil, nil, err
	}
	if err := json.Unmarshal(eventBody, &event); err != nil {
		return nil, nil, err
	}
	return run, event, nil
}

func (runtime *researchRuntime) loadRun(ctx context.Context, runID uuid.UUID) (map[string]any, error) {
	var body []byte
	if err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM research_runs WHERE id=$1`, runID).Scan(&body); err != nil {
		return nil, err
	}
	var run map[string]any
	if err := json.Unmarshal(body, &run); err != nil {
		return nil, err
	}
	return run, nil
}

func supersededOrTerminal(run map[string]any, taskID string) bool {
	if expected := stringValue(run["celery_task_id"]); expected != "" && expected != taskID {
		return true
	}
	switch stringValue(run["status"]) {
	case "completed", "insufficient_evidence", "cancelled":
		return true
	default:
		return false
	}
}

// nextResearchInstance assigns each concurrent claim to a different model
// endpoint. Legacy Celery queues were sharded by instance, but the durable Go
// queue is shared; trusting a migrated instance hint would therefore let all
// worker slots drain one legacy shard into the same model.
func (runtime *researchRuntime) nextResearchInstance(fallback string) string {
	capacity := len(runtime.cfg.ResearchURLs)
	if capacity == 0 {
		return fallback
	}
	if capacity == 1 {
		return "research-0"
	}
	index := (runtime.nextEndpoint.Add(1) - 1) % uint64(capacity)
	return fmt.Sprintf("research-%d", index)
}

func (runtime *researchRuntime) eventEvidence(ctx context.Context, runID uuid.UUID, event map[string]any, historical bool) ([]researchEvidence, error) {
	return runtime.newsEvidence(ctx, runID, stringSlice(event["news_item_ids"]), parseTime(event["as_of"]), historical)
}

func (runtime *researchRuntime) assetEvidence(ctx context.Context, runID uuid.UUID, event map[string]any) ([]researchEvidence, error) {
	if event == nil {
		return []researchEvidence{}, nil
	}
	return runtime.newsEvidence(ctx, runID, stringSlice(event["news_item_ids"]), parseTime(event["as_of"]), false)
}

func (runtime *researchRuntime) newsEvidence(ctx context.Context, runID uuid.UUID, newsIDs []string, boundary time.Time, historical bool) ([]researchEvidence, error) {
	values := make([]researchEvidence, 0, len(newsIDs))
	for _, rawID := range newsIDs {
		newsID, err := uuid.Parse(rawID)
		if err != nil {
			continue
		}
		item, err := runtime.shared().loadNews(ctx, newsID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return nil, err
		}
		if historical && !boundary.IsZero() && (item.PublishedAt.After(boundary) || item.ObservedAt.After(boundary) || item.AsOf.After(boundary)) {
			continue
		}
		values = append(values, researchEvidence{
			ID: uuid.NewString(), Claim: item.Title, SourceName: item.Source,
			SourceURL: item.URL, SourceQuality: item.SourceQuality,
			PublishedAt: item.PublishedAt, ObservedAt: item.ObservedAt, AsOf: item.AsOf,
			Excerpt:          truncateRunes(fallbackString(item.Summary, item.Title), 1000),
			IndependentGroup: evidenceGroup(item.Source, item.URL),
		})
	}
	return values, nil
}

func evidenceGroup(source, rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err == nil && parsed.Hostname() != "" {
		return strings.ToLower(parsed.Hostname())
	}
	return strings.ToLower(strings.TrimSpace(source))
}

func evidencePayload(evidence []researchEvidence, runID uuid.UUID) []any {
	values := make([]any, 0, len(evidence))
	for _, item := range evidence {
		var numeric any
		if item.NumericValue != nil {
			numeric = *item.NumericValue
		}
		values = append(values, map[string]any{
			"id": item.ID, "run_id": runID, "claim": item.Claim,
			"source_name": item.SourceName, "source_url": item.SourceURL,
			"source_quality": item.SourceQuality, "published_at": iso(item.PublishedAt),
			"observed_at": iso(item.ObservedAt), "as_of": iso(item.AsOf),
			"excerpt": item.Excerpt, "independent_group": item.IndependentGroup,
			"numeric_value": numeric, "numeric_unit": nullableString(item.NumericUnit),
		})
	}
	return values
}

func (runtime *researchRuntime) saveEventResearch(ctx context.Context, run map[string]any, evidence []researchEvidence) error {
	run["updated_at"] = iso(time.Now())
	encoded, err := json.Marshal(run)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(stringValue(run["id"]))
	if err != nil {
		return err
	}
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	expectedTaskID := stringValue(run["celery_task_id"])
	result, err := tx.Exec(ctx, `UPDATE event_research_runs SET status=$2,payload=$3,updated_at=now() WHERE id=$1 AND status<>'cancelled' AND ($4='' OR COALESCE(payload->>'celery_task_id','')=$4)`, runID, run["status"], encoded, expectedTaskID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return errResearchInactive
	}
	if err := persistEvidence(ctx, tx, runID, evidence); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (runtime *researchRuntime) saveRun(ctx context.Context, run map[string]any, evidence []researchEvidence) error {
	run["updated_at"] = iso(time.Now())
	encoded, err := json.Marshal(run)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(stringValue(run["id"]))
	if err != nil {
		return err
	}
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	expectedTaskID := stringValue(run["celery_task_id"])
	result, err := tx.Exec(ctx, `UPDATE research_runs SET status=$2,payload=$3,updated_at=now() WHERE id=$1 AND status<>'cancelled' AND ($4='' OR COALESCE(payload->>'celery_task_id','')=$4)`, runID, run["status"], encoded, expectedTaskID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return errResearchInactive
	}
	if evidence != nil {
		if err := persistEvidence(ctx, tx, runID, evidence); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func persistEvidence(ctx context.Context, tx pgx.Tx, runID uuid.UUID, evidence []researchEvidence) error {
	for _, item := range evidence {
		id, err := uuid.Parse(item.ID)
		if err != nil {
			return err
		}
		payload := evidencePayload([]researchEvidence{item}, runID)[0]
		encoded, _ := json.Marshal(payload)
		if _, err := tx.Exec(ctx, `INSERT INTO evidence(id,run_id,claim,source_url,source_quality,published_at,observed_at,as_of,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`, id, runID, item.Claim, item.SourceURL, item.SourceQuality, item.PublishedAt, item.ObservedAt, item.AsOf, encoded); err != nil {
			return err
		}
	}
	return nil
}

func (runtime *researchRuntime) saveRecommendationAndRun(ctx context.Context, run, recommendation map[string]any, evidence []researchEvidence) error {
	runEncoded, err := json.Marshal(run)
	if err != nil {
		return err
	}
	recommendationEncoded, err := json.Marshal(recommendation)
	if err != nil {
		return err
	}
	runID, _ := uuid.Parse(stringValue(run["id"]))
	recommendationID, _ := uuid.Parse(stringValue(recommendation["id"]))
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	expectedTaskID := stringValue(run["celery_task_id"])
	result, err := tx.Exec(ctx, `UPDATE research_runs SET status='completed',payload=$2,updated_at=now() WHERE id=$1 AND status<>'cancelled' AND ($3='' OR COALESCE(payload->>'celery_task_id','')=$3)`, runID, runEncoded, expectedTaskID)
	if err != nil || result.RowsAffected() == 0 {
		if err != nil {
			return err
		}
		return errResearchInactive
	}
	if _, err := tx.Exec(ctx, `INSERT INTO recommendations(id,run_id,asset_id,score,rating,confidence,as_of,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(run_id) DO UPDATE SET score=excluded.score,rating=excluded.rating,confidence=excluded.confidence,as_of=excluded.as_of,payload=excluded.payload`, recommendationID, runID, stringValue(objectValue(recommendation["asset"])["asset_id"]), int(numberValue(recommendation["score"])), recommendation["rating"], recommendation["confidence"], parseTime(recommendation["as_of"]), recommendationEncoded); err != nil {
		return err
	}
	if err := persistEvidence(ctx, tx, runID, evidence); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (runtime *researchRuntime) generateEventDraft(ctx context.Context, runID uuid.UUID, event map[string]any, evidence []researchEvidence, instanceID string) (eventResearchDraft, error) {
	assets := make([]map[string]any, 0, 3)
	for _, raw := range anySlice(event["candidates"]) {
		candidate := objectValue(raw)
		asset := objectValue(candidate["asset"])
		if len(assets) < 3 && asset != nil {
			assets = append(assets, map[string]any{"asset_id": asset["asset_id"], "symbol": asset["symbol"], "name": asset["name"], "asset_class": asset["asset_class"]})
		}
	}
	prompt := "事实框架：" + jsonString(withoutKey(event, "analysis_steps")) + "\n允许绑定的真实标的：" + jsonString(assets) + "\n证据：" + compactResearchEvidence(evidence, 12000) + "\n" +
		"生成最多6个互不重复的目标影响。每个目标只能输出一个 direction_score（-100到100），评级与置信度由程序计算。asset_id只能来自允许标的；成交量、交易活跃度和投资者参与度是驱动因素，不是宏观经济目标，不得成为独立impact。summary只写事件事实，不写全局方向。未知信息写入missing_information，只能引用给定evidence_ids和actions.id。"
	schema := eventDraftSchema()
	var result eventResearchDraft
	err := runtime.callResearchModel(ctx, runID, "event_research_run", "event_report_drafting", "你是证据优先的逐目标事件研究员；先判断对谁，再判断传导路径。", prompt, schema, instanceID, &result)
	if err != nil {
		return eventResearchDraft{}, err
	}
	result.Impacts = sanitizeEventImpacts(result.Impacts, event)
	return result, nil
}

func (runtime *researchRuntime) generateAssetDraft(ctx context.Context, runID uuid.UUID, asset, event map[string]any, evidence []researchEvidence, instanceID string) (assetResearchDraft, error) {
	prompt := "研究对象：" + jsonString(asset) + "\n触发事件：" + jsonString(withoutKey(event, "analysis_steps")) + "\n证据：" + compactResearchEvidence(evidence, 14000) + "\n" +
		"只评价当前研究对象。输出一个direction_score（-100到100），不得输出评级、概率或置信度；程序会计算这些字段。区分事实、推断和未知，只能引用给定evidence_ids。传导路径需从事件连接到营收、成本、利润、现金流或估值；证据不足写入missing_information，不得编造。"
	var result assetResearchDraft
	err := runtime.callResearchModel(ctx, runID, "research_run", "report_drafting", "你是证据优先的投资研究员，不给实盘指令。", prompt, assetDraftSchema(), instanceID, &result)
	if err != nil {
		return assetResearchDraft{}, err
	}
	result.DirectionScore = clampInt(result.DirectionScore, -100, 100)
	return result, nil
}

func (runtime *researchRuntime) callResearchModel(ctx context.Context, entityID uuid.UUID, entityType, operation, system, prompt string, schema map[string]any, instanceID string, target any) error {
	messages := []map[string]string{{"role": "system", "content": system}, {"role": "user", "content": prompt + "\n\n只返回符合format JSON Schema的JSON。"}}
	request := map[string]any{
		"model": runtime.cfg.ResearchModel, "messages": messages, "format": schema, "stream": false,
		"keep_alive": ollamaKeepAliveValue(runtime.cfg.OllamaKeepAlive),
		"options":    map[string]any{"temperature": 0, "num_ctx": runtime.cfg.ResearchContextLength, "num_predict": runtime.cfg.ResearchMaxOutput, "num_thread": runtime.cfg.OllamaResearchThreads},
	}
	logicalID := uuid.New()
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		for _, baseURL := range preferredEndpoints(runtime.cfg.ResearchURLs, instanceID, "research") {
			endpoint := researchEndpointIndex(runtime.cfg.ResearchURLs, baseURL)
			started := time.Now().UTC()
			body, _ := json.Marshal(request)
			httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/chat", bytes.NewReader(body))
			if err != nil {
				lastErr = err
				continue
			}
			httpRequest.Header.Set("Content-Type", "application/json")
			response, err := runtime.client.Do(httpRequest)
			if err != nil {
				lastErr = err
				runtime.persistResearchAudit(context.WithoutCancel(ctx), logicalID, entityID, entityType, operation, attempt, "failed", started, messages, schema, "", nil, err.Error(), 0, 0, endpoint)
				continue
			}
			payload, readErr := io.ReadAll(io.LimitReader(response.Body, 12<<20))
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
				if readErr != nil {
					lastErr = readErr
				} else {
					lastErr = fmt.Errorf("ollama research returned %s", response.Status)
				}
				runtime.persistResearchAudit(context.WithoutCancel(ctx), logicalID, entityID, entityType, operation, attempt, "failed", started, messages, schema, string(payload), nil, lastErr.Error(), 0, 0, endpoint)
				continue
			}
			var modelResponse ollamaResponse
			if err := json.Unmarshal(payload, &modelResponse); err != nil {
				lastErr = err
				continue
			}
			if err := json.Unmarshal([]byte(modelResponse.Message.Content), target); err != nil {
				lastErr = err
				runtime.persistResearchAudit(context.WithoutCancel(ctx), logicalID, entityID, entityType, operation, attempt, "failed", started, messages, schema, modelResponse.Message.Content, nil, err.Error(), modelResponse.PromptTokens, modelResponse.CompletionTokens, endpoint)
				continue
			}
			parsed, _ := json.Marshal(target)
			var parsedValue any
			_ = json.Unmarshal(parsed, &parsedValue)
			runtime.persistResearchAudit(context.WithoutCancel(ctx), logicalID, entityID, entityType, operation, attempt, "completed", started, messages, schema, modelResponse.Message.Content, parsedValue, "", modelResponse.PromptTokens, modelResponse.CompletionTokens, endpoint)
			return nil
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no research model endpoint configured")
	}
	return lastErr
}

func researchEndpointIndex(values []string, selected string) int {
	for index, value := range values {
		if value == selected {
			return index
		}
	}
	return 0
}

func (runtime *researchRuntime) persistResearchAudit(ctx context.Context, logicalID, entityID uuid.UUID, entityType, operation string, attempt int, status string, started time.Time, messages, schema any, raw string, parsed any, errorValue string, promptTokens, completionTokens, endpoint int) {
	if runtime.db == nil {
		return
	}
	messagesJSON, _ := json.Marshal(messages)
	schemaJSON, _ := json.Marshal(schema)
	parsedJSON, _ := json.Marshal(parsed)
	metrics, _ := json.Marshal(map[string]any{"endpoint": fmt.Sprintf("research-%d", endpoint), "lane": "research"})
	var parsedArgument, errorArgument any
	if parsed != nil {
		parsedArgument = parsedJSON
	}
	if errorValue != "" {
		errorArgument = errorValue
	}
	_, _ = runtime.db.Exec(ctx, `INSERT INTO model_call_audits(id,logical_call_id,provider,model,operation,entity_type,entity_id,attempt,status,fidelity,started_at,completed_at,duration_ms,prompt_tokens,completion_tokens,input_language,output_language,messages,schema_payload,raw_response,parsed_response,error,metrics) VALUES($1,$2,'ollama',$3,$4,$5,$6,$7,$8,'exact',$9,$10,$11,$12,$13,'other','other',$14,$15,$16,$17,$18,$19)`, uuid.New(), logicalID, runtime.cfg.ResearchModel, operation, entityType, entityID.String(), attempt, status, started, time.Now().UTC(), time.Since(started).Milliseconds(), nullableInt(promptTokens), nullableInt(completionTokens), messagesJSON, schemaJSON, raw, parsedArgument, errorArgument, metrics)
}

func assetDraftSchema() map[string]any {
	properties := map[string]any{
		"summary": map[string]any{"type": "string"}, "historical_context": map[string]any{"type": "string"},
		"financials_and_growth": map[string]any{"type": "string"}, "products_or_protocol": map[string]any{"type": "string"},
		"competition": map[string]any{"type": "string"}, "valuation_or_tokenomics": map[string]any{"type": "string"},
		"catalysts": stringArraySchema(), "risks": stringArraySchema(), "invalidation_conditions": stringArraySchema(),
		"evidence_ids": stringArraySchema(), "direction_score": map[string]any{"type": "integer", "minimum": -100, "maximum": 100},
		"transmission_path": stringArraySchema(), "missing_information": stringArraySchema(),
	}
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"summary", "historical_context", "financials_and_growth", "products_or_protocol", "competition", "valuation_or_tokenomics", "catalysts", "risks", "invalidation_conditions", "evidence_ids", "direction_score", "transmission_path", "missing_information"}, "properties": properties}
}

func eventDraftSchema() map[string]any {
	impactProperties := map[string]any{
		"target_type": map[string]any{"type": "string", "enum": []string{"economy", "supply_volume", "commodity_price", "fx_rate", "interest_rate", "sector", "tradable_asset", "risk_asset", "shipping", "other"}},
		"target_name": map[string]any{"type": "string"}, "asset_id": map[string]any{"type": []string{"string", "null"}},
		"action_id": map[string]any{"type": []string{"string", "null"}}, "direction_score": map[string]any{"type": "integer", "minimum": -100, "maximum": 100},
		"transmission_path": stringArraySchema(), "rationale": map[string]any{"type": "string"}, "evidence_ids": stringArraySchema(), "missing_information": stringArraySchema(),
	}
	properties := map[string]any{
		"summary": map[string]any{"type": "string"}, "affected_markets": stringArraySchema(), "affected_sectors": stringArraySchema(),
		"scenarios": stringArraySchema(), "catalysts": stringArraySchema(), "risks": stringArraySchema(), "unresolved_questions": stringArraySchema(),
		"evidence_ids": stringArraySchema(), "missing_information": stringArraySchema(),
		"impacts": map[string]any{"type": "array", "maxItems": 6, "items": map[string]any{"type": "object", "additionalProperties": false, "required": []string{"target_type", "target_name", "asset_id", "action_id", "direction_score", "transmission_path", "rationale", "evidence_ids", "missing_information"}, "properties": impactProperties}},
	}
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"summary", "affected_markets", "affected_sectors", "scenarios", "catalysts", "risks", "unresolved_questions", "evidence_ids", "impacts", "missing_information"}, "properties": properties}
}

func stringArraySchema() map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "string"}}
}

func (runtime *researchRuntime) finalizeEventReport(event map[string]any, draft eventResearchDraft, evidence []researchEvidence, complete bool) map[string]any {
	validIDs, _ := validEvidenceIDs(draft.EvidenceIDs, evidence)
	newsConfidence, newsFactors := newsConfidence(event, evidence)
	assets := candidateAssets(event)
	impacts := make([]any, 0, len(draft.Impacts))
	missingAll := append([]string{}, draft.MissingInformation...)
	for _, item := range draft.Impacts {
		asset := assets[item.AssetID]
		validImpactIDs, _ := validEvidenceIDs(item.EvidenceIDs, evidence)
		missing := append([]string{}, item.Missing...)
		if len(validImpactIDs) == 0 {
			missing = appendUnique(missing, "impact_evidence")
		}
		if !complete {
			missing = appendUnique(missing, "evidence_gate")
		}
		candidate := candidateForAsset(event, item.AssetID)
		confidence, confidenceFactors, distance := ratingConfidence(item.DirectionScore, event, candidate, item.TransmissionPath, validImpactIDs, evidence, missing)
		tradeable := asset != nil && item.DirectionScore >= 30 && confidence >= .55 && len(missing) == 0
		impact := map[string]any{
			"target_type": item.TargetType, "target_name": fallbackString(item.TargetName, stringValue(asset["name"])), "asset": nullableMap(asset),
			"direction": sign(item.DirectionScore), "score": float64(item.DirectionScore) / 100, "direction_score": item.DirectionScore,
			"rating": ratingForScore(item.DirectionScore), "confidence": confidence, "rating_confidence": confidence,
			"factors": zeroTransmissionFactors(), "confidence_factors": zeroTargetConfidenceFactors(),
			"rating_confidence_factors": confidenceFactors, "mapping_distance": distance, "score_source": "llm",
			"horizon_days": eventHorizonDays(stringValue(event["event_type"])), "horizon_unit": "calendar_days", "macro_factor_ids": []any{},
			"transmission_path": uniqueStrings(item.TransmissionPath), "rationale": strings.TrimSpace(item.Rationale),
			"evidence_ids": validImpactIDs, "missing_information": uniqueStrings(missing),
			"trade_status":        ternaryString(tradeable, "tradeable", "untradeable"),
			"execution_supported": asset != nil && (stringValue(asset["asset_class"]) == "equity" || stringValue(asset["asset_class"]) == "crypto"),
			"technical_failure":   false,
		}
		impacts = append(impacts, impact)
		missingAll = append(missingAll, missing...)
	}
	tradeStatus := "untradeable"
	for _, raw := range impacts {
		if stringValue(objectValue(raw)["trade_status"]) == "tradeable" {
			tradeStatus = "tradeable"
			break
		}
	}
	return map[string]any{
		"summary": draft.Summary, "affected_markets": nonNilStrings(draft.AffectedMarkets), "affected_sectors": nonNilStrings(draft.AffectedSectors),
		"scenarios": nonNilStrings(draft.Scenarios), "catalysts": nonNilStrings(draft.Catalysts), "risks": nonNilStrings(draft.Risks),
		"unresolved_questions": nonNilStrings(draft.UnresolvedQuestions), "evidence_ids": validIDs,
		"confidence": newsConfidence, "evidence_complete": complete, "scoring_version": "llm-direction-v3",
		"fact_confidence": newsConfidence, "news_confidence": newsConfidence, "news_confidence_version": "news-confidence-v1",
		"news_confidence_factors": newsFactors, "rating_confidence_version": "system-rating-confidence-v3",
		"macro_factors": []any{}, "impacts": impacts, "trade_status": tradeStatus, "missing_information": uniqueStrings(missingAll),
	}
}

func (runtime *researchRuntime) finalizeAssetRecommendation(run, event map[string]any, draft assetResearchDraft, evidence []researchEvidence, validIDs []string, complete bool, warnings []string) map[string]any {
	score := clampInt(draft.DirectionScore, -100, 100)
	asset := objectValue(run["asset"])
	newsValue, newsFactors := newsConfidence(event, evidence)
	candidate := candidateForAsset(event, stringValue(asset["asset_id"]))
	confidence, confidenceFactors, distance := ratingConfidence(score, event, candidate, draft.TransmissionPath, validIDs, evidence, draft.MissingInformation)
	bull, base, bear := probabilitiesForScore(score)
	signalStatus := "directional"
	if absInt(score) < 30 {
		signalStatus = "neutral"
	}
	rating := ratingForScore(score)
	impact := map[string]any{
		"target_type": "tradable_asset", "target_name": asset["name"], "asset": asset,
		"direction": sign(score), "score": float64(score) / 100, "direction_score": score, "rating": rating,
		"confidence": confidence, "rating_confidence": confidence, "factors": zeroTransmissionFactors(),
		"confidence_factors": zeroTargetConfidenceFactors(), "rating_confidence_factors": confidenceFactors,
		"mapping_distance": distance, "score_source": "llm", "horizon_days": eventHorizonDays(stringValue(event["event_type"])),
		"horizon_unit": "calendar_days", "macro_factor_ids": []any{}, "transmission_path": nonNilStrings(draft.TransmissionPath),
		"rationale": draft.Summary, "evidence_ids": validIDs, "missing_information": uniqueStrings(draft.MissingInformation),
		"trade_status":        ternaryString((rating == "bullish" || rating == "strongly_bullish") && confidence >= .55 && len(draft.MissingInformation) == 0, "tradeable", "untradeable"),
		"execution_supported": stringValue(asset["asset_class"]) == "equity" || stringValue(asset["asset_class"]) == "crypto", "technical_failure": false,
	}
	return map[string]any{
		"id": uuid.NewString(), "run_id": run["id"], "asset": asset, "score": score, "direction_score": score,
		"model_score": score, "model_direction": modelDirection(score), "model_rating": rating, "model_confidence": nil,
		"raw_score": score, "rating": rating, "confidence": confidence, "rating_confidence": confidence,
		"bull_probability": bull, "base_probability": base, "bear_probability": bear,
		"horizon_days": eventHorizonDays(stringValue(event["event_type"])), "horizon_unit": "calendar_days",
		"impact_factors": nil, "confidence_factors": nil,
		"fact_confidence": newsValue, "news_confidence": newsValue, "news_confidence_version": "news-confidence-v1",
		"news_confidence_factors": newsFactors, "rating_confidence_factors": confidenceFactors, "mapping_distance": distance,
		"score_source": "llm", "evidence_warnings": uniqueStrings(warnings), "valuation_low": nil, "valuation_high": nil,
		"thesis":       map[string]any{"summary": draft.Summary, "historical_context": draft.HistoricalContext, "financials_and_growth": draft.FinancialsAndGrowth, "products_or_protocol": draft.ProductsOrProtocol, "competition": draft.Competition, "valuation_or_tokenomics": draft.ValuationOrTokenomics, "catalysts": nonNilStrings(draft.Catalysts), "risks": nonNilStrings(draft.Risks), "invalidation_conditions": nonNilStrings(draft.Invalidation), "evidence_ids": validIDs},
		"generated_at": iso(time.Now()), "as_of": run["as_of"], "evidence_complete": complete,
		"directional_evidence_complete": complete, "direction_verified": true, "signal_status": signalStatus,
		"evidence_strength": evidenceStrength(evidence, validIDs), "mapping_confidence": mappingConfidence(candidate),
		"claim_assessments": []any{}, "primary_gate_reason": nil, "gate_reasons": []any{},
		"scoring_version": "llm-direction-v3", "calibration_version": "system-rating-confidence-v3", "impact": impact,
	}
}

func verifyEventDraft(draft eventResearchDraft, evidence []researchEvidence, asOf time.Time) (bool, []string, []string) {
	missing, contradictions := []string{}, []string{}
	if strings.TrimSpace(draft.Summary) == "" {
		missing = append(missing, "summary")
	}
	if len(draft.AffectedMarkets) == 0 && len(draft.AffectedSectors) == 0 {
		missing = append(missing, "affected markets or sectors")
	}
	if len(draft.Scenarios) == 0 {
		missing = append(missing, "scenarios")
	}
	if len(draft.Risks) == 0 {
		missing = append(missing, "risks")
	}
	if len(draft.Impacts) == 0 {
		missing = append(missing, "target impacts")
	}
	valid, warnings := validEvidenceIDs(draft.EvidenceIDs, evidence)
	if len(valid) == 0 {
		missing = append(missing, "evidence citations")
	}
	missing = append(missing, warnings...)
	if !asOf.IsZero() {
		for _, item := range evidence {
			if item.PublishedAt.After(asOf) || item.ObservedAt.After(asOf) || item.AsOf.After(asOf) {
				contradictions = append(contradictions, "point-in-time boundary violation")
				break
			}
		}
	}
	official := false
	groups := map[string]bool{}
	validSet := stringSet(valid)
	for _, item := range evidence {
		if !validSet[item.ID] {
			continue
		}
		official = official || item.SourceQuality == "official"
		if item.IndependentGroup != "" {
			groups[item.IndependentGroup] = true
		}
	}
	if !official && len(groups) < 2 {
		missing = append(missing, "one official source or two independent sources")
	}
	return len(missing) == 0 && len(contradictions) == 0, uniqueStrings(missing), uniqueStrings(contradictions)
}

func newsConfidence(event map[string]any, evidence []researchEvidence) (float64, map[string]any) {
	source := 0.0
	for _, item := range evidence {
		source = math.Max(source, sourceWeight(item.SourceQuality))
	}
	originality := 0.0
	for _, item := range evidence {
		value := map[string]float64{"official": 1, "primary": 1, "professional": .7, "aggregator": .35, "social": .2}[item.SourceQuality]
		originality = math.Max(originality, value)
	}
	groups := independentGroupCount(evidence)
	verification := map[int]float64{0: 0, 1: .5, 2: .8}[groups]
	if groups > 2 {
		verification = 1
	}
	if groups == 1 {
		for _, item := range evidence {
			if item.SourceQuality == "official" {
				verification = .7
			}
		}
	}
	clarity := .2
	stageValues := map[string]float64{"realized": 1, "effective": .95, "announced": .85, "threat": .55, "statement": .35, "unknown": .2}
	for _, raw := range anySlice(event["actions"]) {
		clarity = math.Max(clarity, stageValues[stringValue(objectValue(raw)["action_stage"])])
	}
	fields := []bool{stringValue(event["headline"]) != "", len(evidence) > 0, !parseTime(event["published_at"]).IsZero(), stringValue(event["direct_impact"]) != ""}
	if action := firstObject(event["actions"]); action != nil {
		fields = append(fields, stringValue(action["actor"]) != "", stringValue(action["action"]) != "", stringValue(action["object"]) != "", stringValue(action["scope"]) != "")
	} else {
		fields = append(fields, false, false, false, false)
	}
	covered := 0
	for _, value := range fields {
		if value {
			covered++
		}
	}
	completeness := float64(covered) / float64(len(fields))
	freshness := 0.0
	for _, item := range evidence {
		delay := item.ObservedAt.Sub(item.PublishedAt)
		value := .25
		if delay <= time.Hour {
			value = 1
		} else if delay <= 6*time.Hour {
			value = .9
		} else if delay <= 24*time.Hour {
			value = .75
		} else if delay <= 72*time.Hour {
			value = .5
		}
		freshness = math.Max(freshness, value)
	}
	timely := .6*completeness + .4*freshness
	confidence := round4(.30*source + .20*originality + .20*verification + .15*clarity + .15*timely)
	factor := func(value float64, reason string) map[string]any {
		return map[string]any{"value": round4(value), "reason": reason, "evidence_ids": evidenceIDs(evidence)}
	}
	return confidence, map[string]any{
		"source_reliability":      factor(source, "按事件新闻中的最高来源等级计算。"),
		"originality":             factor(originality, "根据一手来源标记和转载血缘计算。"),
		"cross_verification":      factor(verification, fmt.Sprintf("去重后共有 %d 个独立来源组。", groups)),
		"clarity":                 factor(clarity, "根据事件动作所处阶段计算。"),
		"timeliness_completeness": factor(timely, fmt.Sprintf("必填信息覆盖率 %.0f%%，并计入发布时间到采集时间的延迟。", completeness*100)),
	}
}

func ratingConfidence(score int, event, candidate map[string]any, path, citedIDs []string, evidence []researchEvidence, missing []string) (float64, map[string]any, int) {
	distance := mappingDistance(candidate, path)
	mapping := distanceValue(distance)
	if candidate != nil {
		mapping = math.Min(mapping, math.Min(numberValue(candidate["relevance"]), numberValue(candidate["mapping_confidence"])))
	}
	validSet := stringSet(evidenceIDs(evidence))
	validCount := 0
	for _, id := range uniqueStrings(citedIDs) {
		if validSet[id] {
			validCount++
		}
	}
	citationCoverage := 0.0
	if len(uniqueStrings(citedIDs)) > 0 {
		citationCoverage = float64(validCount) / float64(len(uniqueStrings(citedIDs)))
	}
	pathStructure := 0.0
	if len(path) >= 3 {
		pathStructure = 1
	} else if len(path) == 2 {
		pathStructure = .6
	} else if len(path) == 1 {
		pathStructure = .3
	}
	pathText := strings.ToLower(strings.Join(path, " "))
	financial := 0.0
	if containsAny(pathText, "营收", "收入", "成本", "利润", "现金流", "估值", "revenue", "cost", "profit", "earnings", "cash flow", "valuation") {
		financial = 1
	}
	causality := .45*citationCoverage + .30*pathStructure + .25*financial
	if containsString(missing, "impact_evidence") || containsString(missing, "transmission_evidence") {
		causality *= .5
	}
	impact := .45*math.Abs(float64(score))/100 + .15*mapping
	timing := 0.0
	timingValues := map[string]float64{"realized": 1, "effective": .9, "announced": .75, "threat": .45, "statement": .25, "unknown": 0}
	for _, raw := range anySlice(event["actions"]) {
		timing = math.Max(timing, timingValues[stringValue(objectValue(raw)["action_stage"])])
	}
	market, historical := 0.0, 0.0
	confidence := round4(.25*mapping + .20*causality + .15*historical + .15*impact + .10*timing + .15*market)
	factor := func(value float64, reason string) map[string]any {
		return map[string]any{"value": round4(value), "reason": reason, "evidence_ids": []any{}}
	}
	return confidence, map[string]any{
		"mapping_strength":    factor(mapping, fmt.Sprintf("映射距离 L%d；使用标的相关性和身份映射可信度。", distance)),
		"causality_certainty": factor(causality, fmt.Sprintf("有效引用覆盖率 %.0f%%；路径结构 %.0f%%；财务结果连接%s。", citationCoverage*100, pathStructure*100, ternaryString(financial > 0, "已确认", "缺失"))),
		"historical_pattern":  factor(historical, "当前未纳入同类事件历史结果。"),
		"impact_scale":        factor(impact, fmt.Sprintf("方向绝对值 %d，并计入业务暴露。", absInt(score))),
		"timing_certainty":    factor(timing, "根据事件动作阶段及生效确定性计算。"),
		"market_consistency":  factor(market, "当前证据未提供同窗口市场确认。"),
	}, distance
}

func (runtime *researchRuntime) enqueueTargetResearches(ctx context.Context, event, report map[string]any, limit int) (int, error) {
	queued := 0
	for _, raw := range anySlice(report["impacts"]) {
		if queued >= limit {
			break
		}
		impact := objectValue(raw)
		asset := objectValue(impact["asset"])
		if asset == nil || stringValue(impact["trade_status"]) != "tradeable" {
			continue
		}
		inserted, err := runtime.enqueueAssetResearch(ctx, event, asset)
		if err != nil {
			return queued, err
		}
		if inserted {
			queued++
		}
	}
	return queued, nil
}

func (runtime *researchRuntime) enqueueAssetResearch(ctx context.Context, event, asset map[string]any) (bool, error) {
	assetID := stringValue(asset["asset_id"])
	if assetID == "" {
		return false, nil
	}
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, assetID); err != nil {
		return false, err
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM research_runs WHERE asset_id=$1 AND status IN ('queued','running','verifying'))`, assetID).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return false, tx.Commit(ctx)
	}
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM research_runs WHERE asset_id=$1 AND status IN ('completed','insufficient_evidence') AND coalesce((payload->>'completed_at')::timestamptz,updated_at) > now()-$2::interval)`, assetID, interval(runtime.cfg.ResearchCooldown)).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return false, tx.Commit(ctx)
	}
	eventID, err := uuid.Parse(stringValue(event["id"]))
	if err != nil {
		return false, err
	}
	runID, taskID := uuid.New(), uuid.New()
	instanceID := runtime.shared().selectDownstreamInstance(ctx, "research", len(runtime.cfg.ResearchURLs))
	now := time.Now().UTC()
	steps := append([]any{}, anySlice(event["analysis_steps"])...)
	steps = append(steps, analysisStep("research_queue", "queued", "go-worker", fmt.Sprintf("已为主标的 %s 创建深度研究任务。", stringValue(asset["symbol"])), map[string]any{"instance_id": instanceID, "priority": 3}))
	payload := map[string]any{
		"id": runID, "event_id": eventID, "trigger_event_ids": []string{eventID.String()}, "asset": asset, "status": "queued",
		"as_of": iso(time.Now()), "historical_replay": false, "retry_of_run_id": nil, "retry_attempt": 0,
		"celery_task_id": taskID, "model_instance_id": instanceID, "coalesced_into_run_id": nil, "retryable_reason": nil,
		"verification_round": 0, "missing_requirements": []any{}, "contradictions": []any{}, "evidence": []any{},
		"recommendation": nil, "error": nil, "analysis_steps": steps, "created_at": iso(now), "started_at": nil, "completed_at": nil, "updated_at": iso(now),
	}
	encoded, _ := json.Marshal(payload)
	if _, err := tx.Exec(ctx, `INSERT INTO research_runs(id,event_id,asset_id,status,payload,created_at,updated_at) VALUES($1,$2,$3,'queued',$4,$5,$5)`, runID, eventID, assetID, encoded, now); err != nil {
		return false, err
	}
	jobPayload, _ := json.Marshal(map[string]any{"args": []any{assetID, eventID.String(), runID.String()}, "kwargs": map[string]any{"model_instance_id": instanceID}})
	if _, err := tx.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,priority,max_attempts,available_at,dedupe_key,created_at,updated_at) VALUES($1,'research',$2,$3,'queued',3,3,now(),$4,now(),now())`, taskID, researchAssetTask, jobPayload, "research-run:"+runID.String()); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	runtime.recordResearchTask(ctx, taskID.String(), "asset_research", runID.String(), stringValue(asset["name"]), stringValue(asset["symbol"]), instanceID)
	return true, nil
}

func (runtime *researchRuntime) failEventResearch(ctx context.Context, job Job, run map[string]any, cause error) error {
	clean := context.WithoutCancel(ctx)
	status := "queued"
	if job.Attempt >= job.MaxAttempts {
		status = "failed"
	}
	run["status"], run["error"], run["updated_at"] = status, fmt.Sprintf("%T: %v", cause, cause), iso(time.Now())
	if status == "failed" {
		run["retryable_reason"] = "model_" + errorKind(cause)
	}
	appendAnalysisStep(run, analysisStep("event_research_failed", ternaryString(status == "failed", "failed", "retrying"), "go-worker", fmt.Sprintf("逐目标事件研报%s（%s）。", ternaryString(status == "failed", "最终失败", "暂时失败，等待重试"), errorKind(cause)), map[string]any{}))
	_ = runtime.saveEventResearch(clean, run, payloadEvidence(anySlice(run["evidence"])))
	runtime.finishResearchTracking(clean, job.ID.String(), ternaryString(status == "failed", "failed", "retrying"), job.Attempt, stringValue(run["event_id"]), "事件研究", "", cause.Error(), nil)
	return cause
}

func (runtime *researchRuntime) handleAssetError(ctx context.Context, job Job, run map[string]any, cause error) error {
	clean := context.WithoutCancel(ctx)
	if errors.Is(cause, context.DeadlineExceeded) {
		run["status"], run["retryable_reason"] = "failed", "research_time_limit"
		run["error"] = fmt.Sprintf("研究超时 / Research timed out: soft limit %s, hard limit %s", runtime.cfg.ResearchSoftLimit, runtime.cfg.ResearchHardLimit)
		run["completed_at"], run["updated_at"] = iso(time.Now()), iso(time.Now())
		appendAnalysisStep(run, analysisStep("research_time_limit", "failed", "go-worker", fmt.Sprintf("单标的研究超过软时限 %s，硬时限为 %s；已标记为可重试失败。 / Asset research exceeded the %s soft limit; hard limit is %s.", runtime.cfg.ResearchSoftLimit, runtime.cfg.ResearchHardLimit, runtime.cfg.ResearchSoftLimit, runtime.cfg.ResearchHardLimit), map[string]any{"soft_limit_seconds": int(runtime.cfg.ResearchSoftLimit.Seconds()), "hard_limit_seconds": int(runtime.cfg.ResearchHardLimit.Seconds())}))
		_ = runtime.saveRun(clean, run, payloadEvidence(anySlice(run["evidence"])))
		runtime.finishResearchTracking(clean, job.ID.String(), "failed", job.Attempt, stringValue(objectValue(run["asset"])["asset_id"]), stringValue(objectValue(run["asset"])["name"]), stringValue(objectValue(run["asset"])["symbol"]), stringValue(run["error"]), nil)
		return permanentJobError{cause}
	}
	status := "queued"
	if job.Attempt >= job.MaxAttempts {
		status = "failed"
	}
	run["status"], run["error"], run["updated_at"] = status, fmt.Sprintf("%T: %v", cause, cause), iso(time.Now())
	if status == "failed" {
		run["retryable_reason"], run["completed_at"] = "model_"+errorKind(cause), iso(time.Now())
	}
	appendAnalysisStep(run, analysisStep("research_failed", ternaryString(status == "failed", "failed", "retrying"), "go-worker", fmt.Sprintf("研究任务在 %s 后%s。", errorKind(cause), ternaryString(status == "failed", "停止，请查看服务日志", "等待重试")), map[string]any{}))
	_ = runtime.saveRun(clean, run, payloadEvidence(anySlice(run["evidence"])))
	runtime.finishResearchTracking(clean, job.ID.String(), ternaryString(status == "failed", "failed", "retrying"), job.Attempt, stringValue(objectValue(run["asset"])["asset_id"]), stringValue(objectValue(run["asset"])["name"]), stringValue(objectValue(run["asset"])["symbol"]), cause.Error(), nil)
	return cause
}

func (runtime *researchRuntime) recordResearchTask(ctx context.Context, taskID, kind, entityID, title, subtitle, instanceID string) {
	now := iso(time.Now())
	payload := map[string]any{"task_id": taskID, "instance_id": instanceID, "kind": kind, "entity_id": entityID, "title": title, "subtitle": subtitle, "source": "automatic", "status": "queued", "attempt": 1, "task_count": 1, "queued_at": now, "started_at": nil, "completed_at": nil, "updated_at": now, "error": nil, "metrics": map[string]any{}}
	body, _ := json.Marshal(payload)
	_ = runtime.redis.HSet(ctx, "market-loop:model-queue:research:tasks", taskID, body).Err()
	_ = runtime.redis.Expire(ctx, "market-loop:model-queue:research:tasks", modelTaskTTL).Err()
	_ = runtime.redis.Set(ctx, "market-loop:research-dispatch:"+entityID, taskID, 48*time.Hour).Err()
}

func (runtime *researchRuntime) finishResearchTracking(ctx context.Context, taskID, status string, attempt int, entityID, title, subtitle, errorValue string, metrics map[string]any) {
	key := "market-loop:model-queue:research:tasks"
	raw, _ := runtime.redis.HGet(ctx, key, taskID).Bytes()
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	if payload == nil {
		payload = map[string]any{"task_id": taskID, "kind": "research", "queued_at": iso(time.Now()), "task_count": 1, "metrics": map[string]any{}}
	}
	now := iso(time.Now())
	payload["status"], payload["attempt"], payload["updated_at"] = status, attempt, now
	if entityID != "" {
		payload["entity_id"] = entityID
	}
	if title != "" {
		payload["title"] = title
	}
	if subtitle != "" {
		payload["subtitle"] = subtitle
	}
	if status == "running" && payload["started_at"] == nil {
		payload["started_at"] = now
	}
	if status == "completed" || status == "failed" || status == "cancelled" {
		payload["completed_at"] = now
	}
	if errorValue != "" {
		payload["error"] = truncateRunes(errorValue, 500)
	} else if status != "failed" {
		payload["error"] = nil
	}
	if metrics != nil {
		payload["metrics"] = metrics
	}
	body, _ := json.Marshal(payload)
	_ = runtime.redis.HSet(ctx, key, taskID, body).Err()
	_ = runtime.redis.Expire(ctx, key, modelTaskTTL).Err()
}

func sanitizeEventImpacts(values []eventImpactDraft, event map[string]any) []eventImpactDraft {
	allowed := candidateAssets(event)
	seen := map[string]bool{}
	result := make([]eventImpactDraft, 0, min(6, len(values)))
	for _, item := range values {
		if item.AssetID != "" {
			asset := allowed[item.AssetID]
			if asset == nil {
				continue
			}
			item.TargetType, item.TargetName = "tradable_asset", stringValue(asset["name"])
		}
		if nonTargetActivity(item.TargetName) {
			continue
		}
		key := item.TargetType + ":" + strings.ToLower(fallbackString(item.AssetID, item.TargetName))
		if key == ":" || seen[key] {
			continue
		}
		seen[key] = true
		item.DirectionScore = clampInt(item.DirectionScore, -100, 100)
		result = append(result, item)
		if len(result) == 6 {
			break
		}
	}
	return result
}

func candidateAssets(event map[string]any) map[string]map[string]any {
	result := map[string]map[string]any{}
	for index, raw := range anySlice(event["candidates"]) {
		if index >= 3 {
			break
		}
		asset := objectValue(objectValue(raw)["asset"])
		if asset != nil {
			result[stringValue(asset["asset_id"])] = asset
		}
	}
	return result
}

func candidateForAsset(event map[string]any, assetID string) map[string]any {
	for _, raw := range anySlice(event["candidates"]) {
		candidate := objectValue(raw)
		if stringValue(objectValue(candidate["asset"])["asset_id"]) == assetID {
			return candidate
		}
	}
	return nil
}

func impactScores(values []eventImpactDraft) []int {
	result := make([]int, 0, len(values))
	for _, item := range values {
		result = append(result, item.DirectionScore)
	}
	return result
}

func validEvidenceIDs(proposed []string, evidence []researchEvidence) ([]string, []string) {
	valid := stringSet(evidenceIDs(evidence))
	result, warnings := []string{}, []string{}
	for _, id := range uniqueStrings(proposed) {
		if valid[id] {
			result = append(result, id)
		} else {
			warnings = append(warnings, "unknown evidence id: "+id)
		}
	}
	return result, warnings
}

func evidenceIDs(values []researchEvidence) []string {
	result := make([]string, 0, len(values))
	for _, item := range values {
		result = append(result, item.ID)
	}
	return result
}

func evidenceStrength(evidence []researchEvidence, cited []string) float64 {
	if len(evidence) == 0 {
		return 0
	}
	set := stringSet(cited)
	total := 0.0
	for _, item := range evidence {
		if set[item.ID] {
			total += sourceWeight(item.SourceQuality)
		}
	}
	return round4(math.Min(1, total/float64(len(evidence))))
}

func sourceWeight(quality string) float64 {
	return map[string]float64{"official": 1, "primary": .9, "professional": .82, "aggregator": .65, "social": .4}[quality]
}

func independentGroupCount(values []researchEvidence) int {
	groups := map[string]bool{}
	for _, item := range values {
		if item.IndependentGroup != "" {
			groups[item.IndependentGroup] = true
		}
	}
	return len(groups)
}

func compactResearchEvidence(values []researchEvidence, limit int) string {
	items := make([]map[string]any, 0, len(values))
	for _, item := range values {
		items = append(items, map[string]any{"id": item.ID, "claim": item.Claim, "source": item.SourceName, "source_url": item.SourceURL, "source_quality": item.SourceQuality, "published_at": iso(item.PublishedAt), "excerpt": item.Excerpt, "independent_group": item.IndependentGroup})
	}
	return truncateRunes(jsonString(items), limit)
}

func payloadEvidence(values []any) []researchEvidence {
	result := make([]researchEvidence, 0, len(values))
	for _, raw := range values {
		item := objectValue(raw)
		if item == nil {
			continue
		}
		result = append(result, researchEvidence{ID: stringValue(item["id"]), Claim: stringValue(item["claim"]), SourceName: stringValue(item["source_name"]), SourceURL: stringValue(item["source_url"]), SourceQuality: stringValue(item["source_quality"]), PublishedAt: parseTime(item["published_at"]), ObservedAt: parseTime(item["observed_at"]), AsOf: parseTime(item["as_of"]), Excerpt: stringValue(item["excerpt"]), IndependentGroup: stringValue(item["independent_group"]), NumericUnit: stringValue(item["numeric_unit"])})
	}
	return result
}

func sanitizeStringSlice(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}
func nonNilStrings(values []string) []string { return sanitizeStringSlice(values) }
func stringSet(values []string) map[string]bool {
	result := map[string]bool{}
	for _, value := range values {
		result[value] = true
	}
	return result
}
func appendUnique(values []string, value string) []string {
	if !containsString(values, value) {
		return append(values, value)
	}
	return values
}
func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
func nullableMap(value map[string]any) any {
	if value == nil {
		return nil
	}
	return value
}
func withoutKey(value map[string]any, key string) map[string]any {
	if value == nil {
		return nil
	}
	result := make(map[string]any, len(value))
	for name, item := range value {
		if name != key {
			result[name] = item
		}
	}
	return result
}
func firstObject(value any) map[string]any {
	values := anySlice(value)
	if len(values) == 0 {
		return nil
	}
	return objectValue(values[0])
}
func sign(value int) int {
	if value > 0 {
		return 1
	}
	if value < 0 {
		return -1
	}
	return 0
}
func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
func clampInt(value, low, high int) int { return max(low, min(high, value)) }
func round4(value float64) float64      { return math.Round(value*10000) / 10000 }
func errorKind(value error) string {
	name := fmt.Sprintf("%T", value)
	if index := strings.LastIndex(name, "."); index >= 0 {
		name = name[index+1:]
	}
	return name
}

func modelDirection(score int) string {
	if score >= 30 {
		return "bullish"
	}
	if score <= -30 {
		return "bearish"
	}
	return "neutral"
}
func ratingForScore(score int) string {
	if score >= 70 {
		return "strongly_bullish"
	}
	if score >= 30 {
		return "bullish"
	}
	if score <= -70 {
		return "strongly_bearish"
	}
	if score <= -30 {
		return "bearish"
	}
	return "watch"
}
func probabilitiesForScore(score int) (float64, float64, float64) {
	edge := math.Max(-1, math.Min(1, float64(score)/100))
	base := math.Max(0, math.Min(.5, 1-math.Abs(edge)))
	mass := 1 - base
	bull, bear := (mass+edge)/2, (mass-edge)/2
	total := bull + base + bear
	return round4(bull / total), round4(base / total), round4(bear / total)
}

func mappingDistance(candidate map[string]any, path []string) int {
	if candidate != nil {
		switch strings.ToLower(stringValue(candidate["relationship"])) {
		case "direct", "issuer":
			return 0
		case "product_owner", "cross_listing_issuer", "entity":
			return 1
		default:
			return 2
		}
	}
	return min(5, max(1, len(path)-2))
}
func distanceValue(distance int) float64 {
	return map[int]float64{0: 1, 1: .95, 2: .8, 3: .6, 4: .4, 5: .2}[distance]
}
func mappingConfidence(candidate map[string]any) float64 {
	if candidate == nil {
		return 0
	}
	return math.Min(numberValue(candidate["relevance"]), numberValue(candidate["mapping_confidence"]))
}

func zeroTransmissionFactors() map[string]any {
	return map[string]any{"event_strength": 0, "target_relevance": 0, "transmission_directness": 0, "realization_probability": 0, "novelty": 0, "persistence": 0}
}
func zeroTargetConfidenceFactors() map[string]any {
	return map[string]any{"direction_clarity": 0, "source_reliability": 0, "transmission_certainty": 0, "market_context_completeness": 0}
}

func nonTargetActivity(value string) bool {
	text := strings.ToLower(value)
	return containsAny(text, "成交量", "交易量", "市场活跃度", "交易活跃度", "投资者参与度", "trading volume", "market activity", "trading activity", "investor participation")
}
