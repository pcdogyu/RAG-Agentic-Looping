package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	evolveOutcomesTask   = "market_loop.evolve_from_outcomes"
	evolveFailuresTask   = "market_loop.evolve_failures"
	executeEvolutionTask = "market_loop.execute_evolution"
)

var (
	evolutionSecretPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)(api[_-]?key|token|secret)\s*[:=]\s*['\"][^'\"]+`),
		regexp.MustCompile(`(?i)\b[0-9a-f]{32}\b`),
	}
	evolutionProtectedPaths = map[string]bool{
		".env.example": true, ".github/workflows/ci.yml": true, ".gitignore": true,
		"backend/app/evaluation.py": true, "backend/app/services/evolution.py": true,
		"backend/Dockerfile": true, "backend/Dockerfile.evolution": true,
		"backend-go/Dockerfile.evolution": true, "backend-go/internal/jobs/evolution.go": true,
		"backend/tests/conftest.py": true, "docker-compose.yml": true,
		"evals/baseline.json": true, "evals/golden_events.json": true,
		"evals/golden_predictions.json": true, "pyproject.toml": true,
	}
)

type evolutionRuntime struct {
	cfg    config.Config
	db     *pgxpool.Pool
	redis  *redis.Client
	client *http.Client
	root   string
}

type evolutionProposal struct {
	Hypothesis          string   `json:"hypothesis"`
	TargetMetric        string   `json:"target_metric"`
	ExpectedImprovement float64  `json:"expected_improvement"`
	UnifiedDiff         string   `json:"unified_diff"`
	TestsToRun          []string `json:"tests_to_run"`
}

type commandReport struct {
	Passed     bool   `json:"passed"`
	ReturnCode int    `json:"returncode"`
	Output     string `json:"output"`
}

func NewEvolutionHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &evolutionRuntime{cfg: cfg, db: db, redis: redisClient, client: &http.Client{Timeout: cfg.OllamaTimeout}, root: cfg.EvolutionRoot}
	return map[string]Handler{
		evolveOutcomesTask:   runtime.evolveFromOutcomes,
		evolveFailuresTask:   runtime.evolveFailures,
		executeEvolutionTask: runtime.executeEvolution,
	}
}

func (runtime *evolutionRuntime) evolveFromOutcomes(ctx context.Context, job Job) (any, error) {
	if runtime.cancelled(ctx, job.ID.String()) {
		return map[string]any{"status": "cancelled"}, nil
	}
	runtime.updateTask(ctx, job, "running", "", "定期失败案例代码演进", "根据历史研究结果生成改进方案", "automatic", nil)
	if !runtime.cfg.EvolutionEnabled {
		runtime.updateTask(ctx, job, "completed", "", "", "", "automatic", nil)
		return map[string]any{"status": "disabled"}, nil
	}
	failures, err := runtime.failureCases(ctx)
	if err != nil {
		return nil, runtime.failTask(ctx, job, "automatic", err)
	}
	if len(failures) == 0 {
		runtime.updateTask(ctx, job, "completed", "", "", "", "automatic", nil)
		return map[string]any{"status": "no-failures"}, nil
	}
	return runtime.runFailures(ctx, job, failures, "automatic")
}

func (runtime *evolutionRuntime) evolveFailures(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil {
		return nil, runtime.failTask(ctx, job, "manual", err)
	}
	if runtime.cancelled(ctx, job.ID.String()) {
		return map[string]any{"status": "cancelled"}, nil
	}
	if !runtime.cfg.EvolutionEnabled {
		err = errors.New("EVOLUTION_ENABLED is false")
		return map[string]any{"status": "disabled"}, runtime.failTask(ctx, job, "manual", err)
	}
	if len(envelope.Args) < 1 {
		return nil, runtime.failTask(ctx, job, "manual", errors.New("evolve_failures requires failures"))
	}
	failures := anySlice(envelope.Args[0])
	return runtime.runFailures(ctx, job, failures, "manual")
}

func (runtime *evolutionRuntime) runFailures(ctx context.Context, job Job, failures []any, source string) (any, error) {
	runtime.updateTask(ctx, job, "generating", "", fmt.Sprintf("失败案例代码演进（%d 条）", len(failures)), "正在生成改进方案", source, nil)
	candidate, err := runtime.propose(ctx, failures, stringValue(decodeEnvelopeKwargs(job.Payload)["model_instance_id"]))
	if err != nil {
		return nil, runtime.failTask(ctx, job, source, err)
	}
	runtime.updateTask(ctx, job, "testing", stringValue(candidate["id"]), stringValue(candidate["hypothesis"]), stringValue(candidate["target_metric"]), source, candidateMetrics(candidate))
	result, err := runtime.executeCandidate(ctx, candidate)
	if err != nil {
		return nil, runtime.failTask(ctx, job, source, err)
	}
	status := stringValue(result["status"])
	errorValue := ""
	if status == "rejected" || status == "rolled_back" {
		errorValue = "代码演进候选被拒绝或回滚。"
	}
	runtime.updateTask(ctx, job, status, stringValue(result["id"]), stringValue(result["hypothesis"]), stringValue(result["target_metric"]), source, candidateMetrics(result))
	if errorValue != "" {
		runtime.setTaskError(ctx, job.ID.String(), errorValue)
	}
	return result, nil
}

func (runtime *evolutionRuntime) executeEvolution(ctx context.Context, job Job) (any, error) {
	envelope, err := decodeTaskEnvelope(job.Payload)
	if err != nil || len(envelope.Args) < 1 {
		if err == nil {
			err = errors.New("execute_evolution requires candidate_id")
		}
		return nil, runtime.failTask(ctx, job, "manual", err)
	}
	candidateID, err := uuid.Parse(fmt.Sprint(envelope.Args[0]))
	if err != nil {
		return nil, runtime.failTask(ctx, job, "manual", err)
	}
	if runtime.cancelled(ctx, job.ID.String()) {
		return map[string]any{"status": "cancelled", "candidate_id": candidateID.String()}, nil
	}
	candidate, err := runtime.loadCandidate(ctx, candidateID)
	if err != nil {
		return nil, runtime.failTask(ctx, job, "manual", fmt.Errorf("unknown evolution candidate: %s", candidateID))
	}
	runtime.updateTask(ctx, job, "testing", candidateID.String(), stringValue(candidate["hypothesis"]), stringValue(candidate["target_metric"]), "manual", candidateMetrics(candidate))
	result, err := runtime.executeCandidate(ctx, candidate)
	if err != nil {
		return nil, runtime.failTask(ctx, job, "manual", err)
	}
	status := stringValue(result["status"])
	runtime.updateTask(ctx, job, status, candidateID.String(), stringValue(result["hypothesis"]), stringValue(result["target_metric"]), "manual", candidateMetrics(result))
	return result, nil
}

func (runtime *evolutionRuntime) propose(ctx context.Context, failures []any, instanceID string) (map[string]any, error) {
	if !runtime.cfg.EvolutionEnabled {
		return nil, errors.New("EVOLUTION_ENABLED is false")
	}
	promptBytes, _ := json.Marshal(failures)
	if len(promptBytes) > 20000 {
		promptBytes = promptBytes[:20000]
	}
	proposal := evolutionProposal{}
	schema := evolutionProposalSchema()
	if err := runtime.callCodeModel(ctx, "失败案例："+string(promptBytes), schema, instanceID, &proposal); err != nil {
		return nil, err
	}
	if proposal.Hypothesis == "" || proposal.TargetMetric == "" || proposal.ExpectedImprovement < 0 || proposal.ExpectedImprovement > 1 {
		return nil, errors.New("code model returned an invalid evolution proposal")
	}
	slug := evolutionSlug(proposal.TargetMetric)
	id, now := uuid.New(), time.Now().UTC()
	candidate := map[string]any{
		"id": id.String(), "hypothesis": proposal.Hypothesis, "target_metric": proposal.TargetMetric,
		"expected_improvement": proposal.ExpectedImprovement,
		"branch":               fmt.Sprintf("evolve/%s-%s", now.Format("20060102-150405"), slug),
		"status":               "proposed", "baseline_score": nil, "candidate_score": nil,
		"test_report": map[string]any{"patch": proposal.UnifiedDiff, "requested_tests": proposal.TestsToRun},
		"created_at":  iso(now),
	}
	if err := runtime.saveCandidate(ctx, candidate); err != nil {
		return nil, err
	}
	return candidate, nil
}

func (runtime *evolutionRuntime) callCodeModel(ctx context.Context, prompt string, schema map[string]any, instanceID string, target any) error {
	messages := []map[string]string{
		{"role": "system", "content": "你是代码演进代理。输出最小 unified diff，不读取或生成密钥，不添加实盘交易。修改必须对应一个可测量失败模式。"},
		{"role": "user", "content": prompt + "\n\n只返回符合format JSON Schema的JSON。"},
	}
	request := map[string]any{
		"model": runtime.cfg.CodeModel, "messages": messages, "format": schema, "stream": false,
		"keep_alive": ollamaKeepAliveValue(runtime.cfg.OllamaKeepAlive),
		"options":    map[string]any{"temperature": 0, "num_ctx": runtime.cfg.CodeContextLength, "num_predict": runtime.cfg.CodeMaxOutput, "num_thread": runtime.cfg.OllamaCodeThreads},
	}
	logicalID := uuid.New()
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		for index, baseURL := range preferredEndpoints(runtime.cfg.CodeURLs, instanceID, "code") {
			started := time.Now().UTC()
			body, _ := json.Marshal(request)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/chat", bytes.NewReader(body))
			if err != nil {
				lastErr = err
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			response, err := runtime.client.Do(req)
			if err != nil {
				lastErr = err
				runtime.persistCodeAudit(context.WithoutCancel(ctx), logicalID, attempt, "failed", started, messages, schema, "", nil, err.Error(), 0, 0, index)
				continue
			}
			payload, readErr := io.ReadAll(io.LimitReader(response.Body, 16<<20))
			_ = response.Body.Close()
			if readErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
				if readErr != nil {
					lastErr = readErr
				} else {
					lastErr = fmt.Errorf("ollama code returned %s", response.Status)
				}
				runtime.persistCodeAudit(context.WithoutCancel(ctx), logicalID, attempt, "failed", started, messages, schema, string(payload), nil, lastErr.Error(), 0, 0, index)
				continue
			}
			var modelResponse ollamaResponse
			if err := json.Unmarshal(payload, &modelResponse); err != nil {
				lastErr = err
				continue
			}
			if err := json.Unmarshal([]byte(modelResponse.Message.Content), target); err != nil {
				lastErr = err
				runtime.persistCodeAudit(context.WithoutCancel(ctx), logicalID, attempt, "failed", started, messages, schema, modelResponse.Message.Content, nil, err.Error(), modelResponse.PromptTokens, modelResponse.CompletionTokens, index)
				continue
			}
			parsedBytes, _ := json.Marshal(target)
			var parsed any
			_ = json.Unmarshal(parsedBytes, &parsed)
			runtime.persistCodeAudit(context.WithoutCancel(ctx), logicalID, attempt, "completed", started, messages, schema, modelResponse.Message.Content, parsed, "", modelResponse.PromptTokens, modelResponse.CompletionTokens, index)
			return nil
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no code model endpoint configured")
	}
	return lastErr
}

func (runtime *evolutionRuntime) executeCandidate(ctx context.Context, candidate map[string]any) (result map[string]any, returnedErr error) {
	if !runtime.cfg.EvolutionEnabled {
		return nil, errors.New("EVOLUTION_ENABLED is false")
	}
	if strings.TrimSpace(runtime.root) == "" || runtime.root == "/" {
		return nil, errors.New("invalid evolution repository root")
	}
	if output, err := runtime.run(ctx, 120*time.Second, "git", "status", "--porcelain"); err != nil || strings.TrimSpace(output) != "" {
		return nil, errors.New("evolution requires a clean worktree")
	}
	report := objectValue(candidate["test_report"])
	patch := stringValue(report["patch"])
	if err := assertEvolutionNoSecret(patch); err != nil {
		return nil, err
	}
	paths, err := evolutionCandidatePaths(patch)
	if err != nil {
		return nil, err
	}
	branch := stringValue(candidate["branch"])
	if _, err := runtime.run(ctx, 120*time.Second, "git", "switch", "-c", branch); err != nil {
		return nil, err
	}
	rollbackTag := ""
	defer func() {
		cleanCtx := context.WithoutCancel(ctx)
		current, _ := runtime.run(cleanCtx, 30*time.Second, "git", "branch", "--show-current")
		current = strings.TrimSpace(current)
		if current != "" && current != runtime.cfg.EvolutionBaseBranch {
			_, _ = runtime.run(cleanCtx, 30*time.Second, "git", "reset", "--hard", "HEAD")
		}
		if dirty, _ := runtime.run(cleanCtx, 30*time.Second, "git", "status", "--porcelain"); strings.TrimSpace(dirty) != "" {
			_, _ = runtime.run(cleanCtx, 60*time.Second, "git", "stash", "push", "--include-untracked", "-m", "evolution-cleanup-"+stringValue(candidate["id"]))
		}
		if current != "" && current != runtime.cfg.EvolutionBaseBranch {
			_, _ = runtime.run(cleanCtx, 30*time.Second, "git", "switch", runtime.cfg.EvolutionBaseBranch)
		}
	}()
	if err := runtime.applyPatch(ctx, patch); err != nil {
		return nil, runtime.rejectCandidate(ctx, candidate, err)
	}
	candidate["status"] = "testing"
	if err := runtime.saveCandidate(ctx, candidate); err != nil {
		return nil, err
	}
	checks := runtime.candidateChecks(ctx)
	if err := runtime.assertNoTestSideEffects(ctx, paths); err != nil {
		return nil, runtime.rejectCandidate(ctx, candidate, err)
	}
	report["checks"] = checks
	baseline, score, metricsPass := runtime.evaluationScores(stringValue(candidate["target_metric"]))
	candidate["baseline_score"], candidate["candidate_score"] = baseline, score
	allPass := metricsPass
	for _, check := range checks {
		allPass = allPass && check.Passed
	}
	if !allPass {
		candidate["status"] = "rejected"
		_ = runtime.saveCandidate(ctx, candidate)
		return candidate, nil
	}
	if _, err := runtime.run(ctx, 120*time.Second, "git", "commit", "-m", "evolution: "+truncateRunes(stringValue(candidate["hypothesis"]), 72)); err != nil {
		return nil, runtime.rejectCandidate(ctx, candidate, err)
	}
	if runtime.cfg.EvolutionAutoMerge {
		if _, err := runtime.run(ctx, 60*time.Second, "git", "switch", runtime.cfg.EvolutionBaseBranch); err != nil {
			return nil, runtime.rejectCandidate(ctx, candidate, err)
		}
		rollbackTag = "last-known-good-" + time.Now().UTC().Format("20060102-150405")
		if _, err := runtime.run(ctx, 30*time.Second, "git", "tag", rollbackTag); err != nil {
			return nil, runtime.rejectCandidate(ctx, candidate, err)
		}
		_, _ = runtime.run(ctx, 30*time.Second, "git", "tag", "-f", "last-known-good")
		if _, err := runtime.run(ctx, 120*time.Second, "git", "merge", "--no-ff", branch, "-m", "merge "+branch); err != nil {
			return nil, runtime.rollbackCandidate(ctx, candidate, rollbackTag, err)
		}
		deployment := runtime.deployAndVerify(ctx)
		report["deployment"] = deployment
		if deployment.Passed {
			candidate["status"] = "merged"
		} else {
			return candidate, runtime.rollbackCandidate(ctx, candidate, rollbackTag, errors.New(deployment.Output))
		}
	}
	if err := runtime.saveCandidate(ctx, candidate); err != nil {
		return nil, err
	}
	return candidate, nil
}

func (runtime *evolutionRuntime) candidateChecks(ctx context.Context) map[string]commandReport {
	checks := map[string]commandReport{}
	commands := []struct {
		name string
		dir  string
		args []string
		wait time.Duration
	}{
		{"compile", runtime.root, []string{"python", "-m", "compileall", "-q", "backend"}, 15 * time.Minute},
		{"tests", runtime.root, []string{"python", "-m", "pytest", "-q"}, 30 * time.Minute},
		{"time_travel", runtime.root, []string{"python", "-m", "pytest", "-q", "backend/tests/test_storage_time.py", "backend/tests/test_retrieval.py"}, 15 * time.Minute},
		{"lint", runtime.root, []string{"python", "-m", "ruff", "check", "."}, 15 * time.Minute},
		{"fixed_evidence", runtime.root, []string{"python", "-m", "backend.app.evaluation", "fixed-evidence"}, 15 * time.Minute},
		{"walk_forward", runtime.root, []string{"python", "-m", "backend.app.evaluation", "walk-forward"}, 15 * time.Minute},
		{"dependency_audit", runtime.root, []string{"python", "-m", "pip_audit"}, 15 * time.Minute},
		{"go_tests", filepath.Join(runtime.root, "backend-go"), []string{"go", "test", "./..."}, 15 * time.Minute},
		{"go_vet", filepath.Join(runtime.root, "backend-go"), []string{"go", "vet", "./..."}, 15 * time.Minute},
		{"container_build", runtime.root, []string{"docker", "compose", "build", "web", "go-api", "market-adapter", "go-worker", "go-mapping-worker", "go-research-worker", "go-backfill-worker", "go-maintenance-worker"}, 30 * time.Minute},
	}
	for _, item := range commands {
		checks[item.name] = runCheck(ctx, item.dir, item.wait, item.args...)
	}
	checks["secret_scan"] = runtime.repositorySecretCheck(ctx)
	return checks
}

func (runtime *evolutionRuntime) failureCases(ctx context.Context) ([]any, error) {
	rows, err := runtime.db.Query(ctx, `
		SELECT jsonb_build_object('failure_type','outcome_miss','outcome',o.payload,'recommendation',r.payload,'research',rr.payload,'event',e.payload)
		FROM outcomes o
		LEFT JOIN recommendations r ON r.id=o.recommendation_id
		LEFT JOIN research_runs rr ON rr.id=r.run_id
		LEFT JOIN news_events e ON e.id=rr.event_id
		WHERE coalesce((o.payload->>'direction_correct')::boolean,false)=false
		   OR coalesce((o.payload->>'alpha')::double precision,0)<0
		ORDER BY o.observed_at DESC LIMIT 30`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []any{}
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		var item any
		_ = json.Unmarshal(body, &item)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	remaining := 50 - len(result)
	if remaining <= 0 {
		return result, nil
	}
	failureRows, err := runtime.db.Query(ctx, `
		SELECT jsonb_build_object(
			'failure_type',CASE WHEN status='failed' OR payload->>'retryable_reason' IS NOT NULL THEN 'technical_failure' ELSE 'evidence_gap' END,
			'research',payload,'recommendation',rec.payload,'event',e.payload)
		FROM research_runs rr
		LEFT JOIN recommendations rec ON rec.run_id=rr.id
		LEFT JOIN news_events e ON e.id=rr.event_id
		WHERE rr.status IN ('failed','insufficient_evidence')
		ORDER BY rr.updated_at DESC LIMIT $1`, remaining)
	if err != nil {
		return nil, err
	}
	defer failureRows.Close()
	for failureRows.Next() {
		var body []byte
		if err := failureRows.Scan(&body); err != nil {
			return nil, err
		}
		var item any
		_ = json.Unmarshal(body, &item)
		result = append(result, item)
	}
	return result, failureRows.Err()
}

func (runtime *evolutionRuntime) saveCandidate(ctx context.Context, candidate map[string]any) error {
	id, err := uuid.Parse(stringValue(candidate["id"]))
	if err != nil {
		return err
	}
	body, _ := json.Marshal(candidate)
	_, err = runtime.db.Exec(ctx, `INSERT INTO evolution_candidates(id,branch,status,payload,created_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET branch=excluded.branch,status=excluded.status,payload=excluded.payload`, id, candidate["branch"], candidate["status"], body, parseTime(candidate["created_at"]))
	return err
}

func (runtime *evolutionRuntime) loadCandidate(ctx context.Context, id uuid.UUID) (map[string]any, error) {
	var body []byte
	if err := runtime.db.QueryRow(ctx, `SELECT payload::jsonb FROM evolution_candidates WHERE id=$1`, id).Scan(&body); err != nil {
		return nil, err
	}
	var candidate map[string]any
	return candidate, json.Unmarshal(body, &candidate)
}

func (runtime *evolutionRuntime) updateTask(ctx context.Context, job Job, status, entityID, title, subtitle, source string, metrics map[string]any) {
	shared := &ExtractRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis, client: runtime.client}
	shared.updateTrackedTask(ctx, "code", job.ID.String(), status, job.Attempt, entityID, title, subtitle, "", metrics)
	key := "market-loop:model-queue:code:tasks"
	raw, _ := runtime.redis.HGet(ctx, key, job.ID.String()).Bytes()
	var payload map[string]any
	if json.Unmarshal(raw, &payload) == nil && source != "" {
		payload["source"] = source
		encoded, _ := json.Marshal(payload)
		_ = runtime.redis.HSet(ctx, key, job.ID.String(), encoded).Err()
	}
}

func (runtime *evolutionRuntime) failTask(ctx context.Context, job Job, source string, cause error) error {
	runtime.updateTask(context.WithoutCancel(ctx), job, "failed", "", "", "", source, nil)
	runtime.setTaskError(context.WithoutCancel(ctx), job.ID.String(), fmt.Sprintf("%T: %v", cause, cause))
	return permanentJobError{cause}
}

func (runtime *evolutionRuntime) setTaskError(ctx context.Context, taskID, message string) {
	key := "market-loop:model-queue:code:tasks"
	raw, _ := runtime.redis.HGet(ctx, key, taskID).Bytes()
	var payload map[string]any
	if json.Unmarshal(raw, &payload) != nil {
		return
	}
	payload["error"] = truncateRunes(message, 500)
	body, _ := json.Marshal(payload)
	_ = runtime.redis.HSet(ctx, key, taskID, body).Err()
}

func (runtime *evolutionRuntime) cancelled(ctx context.Context, taskID string) bool {
	raw, err := runtime.redis.HGet(ctx, "market-loop:model-queue:code:tasks", taskID).Bytes()
	if err != nil {
		return false
	}
	var payload map[string]any
	return json.Unmarshal(raw, &payload) == nil && stringValue(payload["status"]) == "cancelled"
}

func (runtime *evolutionRuntime) persistCodeAudit(ctx context.Context, logicalID uuid.UUID, attempt int, status string, started time.Time, messages, schema any, raw string, parsed any, errorValue string, promptTokens, completionTokens, endpoint int) {
	if runtime.db == nil {
		return
	}
	messagesJSON, _ := json.Marshal(messages)
	schemaJSON, _ := json.Marshal(schema)
	parsedJSON, _ := json.Marshal(parsed)
	metrics, _ := json.Marshal(map[string]any{"endpoint": fmt.Sprintf("code-%d", endpoint), "lane": "code"})
	var parsedArgument, errorArgument any
	if parsed != nil {
		parsedArgument = parsedJSON
	}
	if errorValue != "" {
		errorArgument = errorValue
	}
	_, _ = runtime.db.Exec(ctx, `INSERT INTO model_call_audits(id,logical_call_id,provider,model,operation,entity_type,entity_id,attempt,status,fidelity,started_at,completed_at,duration_ms,prompt_tokens,completion_tokens,input_language,output_language,messages,schema_payload,raw_response,parsed_response,error,metrics) VALUES($1,$2,'ollama',$3,'evolution_proposal','evolution_candidate',NULL,$4,$5,'exact',$6,$7,$8,$9,$10,'other','other',$11,$12,$13,$14,$15,$16)`, uuid.New(), logicalID, runtime.cfg.CodeModel, attempt, status, started, time.Now().UTC(), time.Since(started).Milliseconds(), nullableInt(promptTokens), nullableInt(completionTokens), messagesJSON, schemaJSON, raw, parsedArgument, errorArgument, metrics)
}

func evolutionProposalSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"hypothesis", "target_metric", "expected_improvement", "unified_diff", "tests_to_run"}, "properties": map[string]any{
		"hypothesis": map[string]any{"type": "string"}, "target_metric": map[string]any{"type": "string"},
		"expected_improvement": map[string]any{"type": "number", "minimum": 0, "maximum": 1},
		"unified_diff":         map[string]any{"type": "string"}, "tests_to_run": stringArraySchema(),
	}}
}

func decodeEnvelopeKwargs(payload json.RawMessage) map[string]any {
	envelope, err := decodeTaskEnvelope(payload)
	if err != nil || envelope.Kwargs == nil {
		return map[string]any{}
	}
	return envelope.Kwargs
}

func evolutionSlug(value string) string {
	value = strings.ToLower(value)
	value = regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if len(value) > 40 {
		value = value[:40]
	}
	if value == "" {
		return "candidate"
	}
	return value
}

func candidateMetrics(candidate map[string]any) map[string]any {
	return map[string]any{"target_metric": candidate["target_metric"], "branch": candidate["branch"], "expected_improvement": candidate["expected_improvement"], "baseline_score": candidate["baseline_score"], "candidate_score": candidate["candidate_score"]}
}

func assertEvolutionNoSecret(value string) error {
	for _, pattern := range evolutionSecretPatterns {
		if pattern.MatchString(value) {
			return errors.New("candidate patch appears to contain a secret")
		}
	}
	return nil
}

func evolutionCandidatePaths(patch string) (map[string]bool, error) {
	paths := map[string]bool{}
	for _, line := range strings.Split(patch, "\n") {
		if !strings.HasPrefix(line, "diff --git ") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 4 || !strings.HasPrefix(parts[2], "a/") || !strings.HasPrefix(parts[3], "b/") || strings.ContainsAny(parts[2]+parts[3], `"'`) {
			return nil, errors.New("candidate patch contains an unsupported path header")
		}
		paths[strings.TrimPrefix(parts[2], "a/")] = true
		paths[strings.TrimPrefix(parts[3], "b/")] = true
	}
	if strings.TrimSpace(patch) != "" && len(paths) == 0 {
		return nil, errors.New("candidate patch does not declare any repository paths")
	}
	protected := []string{}
	for path := range paths {
		if evolutionProtectedPaths[path] {
			protected = append(protected, path)
		}
	}
	sort.Strings(protected)
	if len(protected) > 0 {
		return nil, fmt.Errorf("candidate patch modifies protected evaluation files: %v", protected)
	}
	if strings.Contains(patch, "\ndeleted file mode ") || strings.HasPrefix(patch, "deleted file mode ") {
		return nil, errors.New("candidate patch may not delete repository files")
	}
	return paths, nil
}

func (runtime *evolutionRuntime) applyPatch(ctx context.Context, patch string) error {
	command := exec.CommandContext(ctx, "git", "apply", "--index", "--whitespace=error", "-")
	command.Dir, command.Stdin = runtime.root, strings.NewReader(patch)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git apply failed: %s", truncateRunes(string(output), 2000))
	}
	return nil
}

func (runtime *evolutionRuntime) rejectCandidate(ctx context.Context, candidate map[string]any, cause error) error {
	candidate["status"] = "rejected"
	report := objectValue(candidate["test_report"])
	report["execution_error"] = fmt.Sprintf("%T: %v", cause, cause)
	_ = runtime.saveCandidate(context.WithoutCancel(ctx), candidate)
	return cause
}

func (runtime *evolutionRuntime) rollbackCandidate(ctx context.Context, candidate map[string]any, tag string, cause error) error {
	if tag != "" {
		_, _ = runtime.run(context.WithoutCancel(ctx), 60*time.Second, "git", "reset", "--hard", tag)
	}
	candidate["status"] = "rolled_back"
	report := objectValue(candidate["test_report"])
	report["execution_error"] = fmt.Sprintf("%T: %v", cause, cause)
	_ = runtime.saveCandidate(context.WithoutCancel(ctx), candidate)
	return cause
}

func rollbackLastKnownGood(ctx context.Context, cfg config.Config, redisClient *redis.Client) error {
	if !cfg.EvolutionEnabled {
		return errors.New("EVOLUTION_ENABLED is false")
	}
	runtime := &evolutionRuntime{cfg: cfg, redis: redisClient, root: cfg.EvolutionRoot}
	if output, err := runtime.run(ctx, 120*time.Second, "git", "status", "--porcelain"); err != nil || strings.TrimSpace(output) != "" {
		return errors.New("rollback refused because the worktree is not clean")
	}
	if _, err := runtime.run(ctx, 30*time.Second, "git", "rev-parse", "--verify", "last-known-good"); err != nil {
		return errors.New("last-known-good tag does not exist")
	}
	if _, err := runtime.run(ctx, 60*time.Second, "git", "reset", "--hard", "last-known-good"); err != nil {
		return err
	}
	deployment := runtime.deployAndVerify(ctx)
	if !deployment.Passed {
		return errors.New("rollback deployment failed: " + deployment.Output)
	}
	return nil
}

func (runtime *evolutionRuntime) evaluationScores(targetMetric string) (float64, float64, bool) {
	baseline := readMetricFile(filepath.Join(runtime.root, "evals", "baseline.json"))
	candidate := readMetricFile(filepath.Join(runtime.root, "evals", "candidate.json"))
	if baseline == nil || candidate == nil {
		return 1, 0, false
	}
	baseScore, baseOK := numericMetric(baseline["composite_score"])
	candidateScore, candidateOK := numericMetric(candidate["composite_score"])
	if !baseOK || !candidateOK {
		return 1, 0, false
	}
	noRegression, common := true, 0
	for key, raw := range baseline {
		if key == "version" || key == "samples" || key == "passed" {
			continue
		}
		left, ok := numericMetric(raw)
		right, rightOK := numericMetric(candidate[key])
		if !ok || !rightOK {
			continue
		}
		common++
		delta := right - left
		if (key == "brier_score" || key == "expected_calibration_error") && delta > .01 || key != "brier_score" && key != "expected_calibration_error" && delta < -.01 {
			noRegression = false
		}
	}
	target := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(targetMetric)), " ", "_")
	if _, ok := baseline[target]; !ok {
		target = "composite_score"
	}
	left, lok := numericMetric(baseline[target])
	right, rok := numericMetric(candidate[target])
	delta := right - left
	targetImproved := lok && rok && ternary(target == "brier_score" || target == "expected_calibration_error", delta <= -.02, delta >= .02)
	passedValue, _ := candidate["passed"].(bool)
	return baseScore, candidateScore, common > 0 && noRegression && passedValue && targetImproved
}

func readMetricFile(path string) map[string]any {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return nil
	}
	return value
}

func numericMetric(value any) (float64, bool) {
	switch item := value.(type) {
	case float64:
		return item, true
	case int:
		return float64(item), true
	case json.Number:
		parsed, err := item.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func (runtime *evolutionRuntime) repositorySecretCheck(ctx context.Context) commandReport {
	output, err := runtime.run(ctx, 120*time.Second, "git", "ls-files", "--cached", "--others", "--exclude-standard")
	if err != nil {
		return commandReport{Passed: false, ReturnCode: 1, Output: err.Error()}
	}
	findings := []string{}
	for _, relative := range strings.Fields(output) {
		path := filepath.Join(runtime.root, filepath.FromSlash(relative))
		info, statErr := os.Stat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Size() > 1_000_000 {
			continue
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			continue
		}
		for index, line := range strings.Split(string(body), "\n") {
			if strings.Contains(line, "pragma: allowlist secret") {
				continue
			}
			for _, pattern := range evolutionSecretPatterns {
				if pattern.MatchString(line) {
					findings = append(findings, fmt.Sprintf("%s:%d", relative, index+1))
				}
			}
		}
	}
	if len(findings) > 0 {
		return commandReport{Passed: false, ReturnCode: 1, Output: fmt.Sprintf("possible secrets: %v", findings)}
	}
	return commandReport{Passed: true, ReturnCode: 0, Output: "no secrets detected"}
}

func (runtime *evolutionRuntime) assertNoTestSideEffects(ctx context.Context, candidatePaths map[string]bool) error {
	unstaged := runtime.gitPathSet(ctx, "diff", "--name-only")
	untracked := runtime.gitPathSet(ctx, "ls-files", "--others", "--exclude-standard")
	staged := runtime.gitPathSet(ctx, "diff", "--cached", "--name-only")
	unexpected, protected := []string{}, []string{}
	for path := range unionPathSets(unstaged, untracked, staged) {
		if !candidatePaths[path] {
			unexpected = append(unexpected, path)
		}
		if evolutionProtectedPaths[path] {
			protected = append(protected, path)
		}
	}
	if len(unstaged) > 0 || len(unexpected) > 0 || len(protected) > 0 {
		sort.Strings(unexpected)
		sort.Strings(protected)
		return fmt.Errorf("candidate checks changed files outside the staged patch: unstaged=%v, unexpected=%v, protected=%v", sortedPathSet(unstaged), unexpected, protected)
	}
	return nil
}

func (runtime *evolutionRuntime) gitPathSet(ctx context.Context, args ...string) map[string]bool {
	output, _ := runtime.run(ctx, 60*time.Second, "git", args...)
	result := map[string]bool{}
	for _, path := range strings.Fields(output) {
		result[path] = true
	}
	return result
}

func unionPathSets(values ...map[string]bool) map[string]bool {
	result := map[string]bool{}
	for _, value := range values {
		for path := range value {
			result[path] = true
		}
	}
	return result
}

func sortedPathSet(value map[string]bool) []string {
	result := make([]string, 0, len(value))
	for path := range value {
		result = append(result, path)
	}
	sort.Strings(result)
	return result
}

func runCheck(ctx context.Context, dir string, timeout time.Duration, args ...string) commandReport {
	if len(args) == 0 {
		return commandReport{Passed: false, ReturnCode: 1, Output: "missing command"}
	}
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	command := exec.CommandContext(checkCtx, args[0], args[1:]...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	code := 0
	if err != nil {
		code = 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		}
		if errors.Is(checkCtx.Err(), context.DeadlineExceeded) {
			code = 124
		}
	}
	return commandReport{Passed: err == nil, ReturnCode: code, Output: truncateRunes(string(output), 4000)}
}

func (runtime *evolutionRuntime) deployAndVerify(ctx context.Context) commandReport {
	report := runCheck(ctx, runtime.root, 30*time.Minute, "docker", "compose", "--profile", "go-shadow", "up", "-d", "--build", "go-api", "market-adapter", "go-worker", "go-mapping-worker", "go-research-worker", "go-evolution-worker", "go-discovery-worker", "go-recovery-worker", "go-outcomes-worker", "go-masterdata-worker", "go-operations-worker", "go-backfill-worker", "go-maintenance-worker", "go-scheduler", "web")
	if !report.Passed {
		return report
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for attempt := 0; attempt < 12; attempt++ {
		apiOK, webOK := false, false
		for _, target := range []string{"http://go-api:8081/health", "http://127.0.0.1:8081/health"} {
			response, err := client.Get(target)
			if err == nil {
				var payload map[string]any
				_ = json.NewDecoder(response.Body).Decode(&payload)
				_ = response.Body.Close()
				apiOK = response.StatusCode >= 200 && response.StatusCode < 300 && stringValue(payload["status"]) == "ok" && boolValue(payload["database"]) && boolValue(payload["redis"])
			}
			if apiOK {
				break
			}
		}
		for _, target := range []string{"http://web/", "http://127.0.0.1/"} {
			response, err := client.Get(target)
			if err == nil {
				_ = response.Body.Close()
				webOK = response.StatusCode >= 200 && response.StatusCode < 300
			}
			if webOK {
				break
			}
		}
		if apiOK && webOK {
			_ = runtime.redis.Del(context.WithoutCancel(ctx), "market-loop:tasks:success", "market-loop:tasks:failure").Err()
			return commandReport{Passed: true, ReturnCode: 0, Output: "API and web healthy"}
		}
		time.Sleep(5 * time.Second)
	}
	return commandReport{Passed: false, ReturnCode: 1, Output: "API and web health checks did not become ready"}
}

func (runtime *evolutionRuntime) run(ctx context.Context, timeout time.Duration, name string, args ...string) (string, error) {
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	command := exec.CommandContext(commandCtx, name, args...)
	command.Dir = runtime.root
	output, err := command.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s: %w: %s", name, err, truncateRunes(string(output), 2000))
	}
	return string(output), nil
}
