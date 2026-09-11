// Package fundamentalai stores durable local-search preparation runs. It keeps
// model proposals separate from policy-approved analyst evidence.
package fundamentalai

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	PolicyVersion = "fundamental-ai-policy-v1"
	PromptVersion = "fundamental-ai-research-v1"
	PolicyActor   = "policy:" + PolicyVersion
)

type Run struct {
	ID            uuid.UUID      `json:"id"`
	BatchID       *uuid.UUID     `json:"batch_id,omitempty"`
	AssetID       string         `json:"asset_id"`
	TaskID        uuid.UUID      `json:"task_id"`
	Status        string         `json:"status"`
	Stage         string         `json:"stage"`
	PolicyVersion string         `json:"policy_version"`
	ModelVersion  string         `json:"model_version"`
	PromptVersion string         `json:"prompt_version"`
	AsOf          time.Time      `json:"as_of"`
	Summary       map[string]any `json:"summary"`
	Blockers      []string       `json:"blockers"`
	StartedAt     *time.Time     `json:"started_at,omitempty"`
	CompletedAt   *time.Time     `json:"completed_at,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

type SourceSnapshot struct {
	ID              string     `json:"id"`
	RunID           uuid.UUID  `json:"run_id"`
	Query           string     `json:"query"`
	Title           string     `json:"title"`
	SourceName      string     `json:"source_name"`
	SourceClass     string     `json:"source_class"`
	SourceURL       string     `json:"source_url"`
	SourceDomain    string     `json:"source_domain"`
	PublishedAt     *time.Time `json:"published_at,omitempty"`
	ObservedAt      time.Time  `json:"observed_at"`
	AvailableAt     time.Time  `json:"available_at"`
	ContentType     string     `json:"content_type"`
	ContentHash     string     `json:"content_hash"`
	ContentText     string     `json:"content_text,omitempty"`
	RetrievalStatus string     `json:"retrieval_status"`
	RetrievalDetail string     `json:"retrieval_detail,omitempty"`
}

type Candidate struct {
	ID                 string         `json:"id"`
	RunID              uuid.UUID      `json:"run_id"`
	AssetID            string         `json:"asset_id"`
	EvidenceType       string         `json:"evidence_type"`
	Title              string         `json:"title"`
	Rationale          string         `json:"rationale"`
	Values             map[string]any `json:"values"`
	SourceSnapshotIDs  []string       `json:"source_snapshot_ids"`
	EvidenceQuote      string         `json:"evidence_quote"`
	EvidenceLocation   string         `json:"evidence_location"`
	Status             string         `json:"status"`
	Validation         map[string]any `json:"validation"`
	ApprovedEvidenceID string         `json:"approved_evidence_id,omitempty"`
	CreatedAt          time.Time      `json:"created_at"`
}

type Batch struct {
	ID             uuid.UUID      `json:"id"`
	Market         string         `json:"market"`
	Scope          string         `json:"scope"`
	RequestedCount int            `json:"requested_count"`
	TaskIDs        []uuid.UUID    `json:"task_ids"`
	CreatedAt      time.Time      `json:"created_at"`
	Counts         map[string]int `json:"counts,omitempty"`
}

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

func (s *Store) CreateRun(ctx context.Context, run Run, idempotencyKey string) (Run, bool, error) {
	if s.db == nil || run.ID == uuid.Nil || run.TaskID == uuid.Nil || strings.TrimSpace(run.AssetID) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return Run{}, false, fmt.Errorf("AI preparation run identity and idempotency key are required")
	}
	now := time.Now().UTC()
	if run.AsOf.IsZero() {
		run.AsOf = now
	}
	run.AssetID = strings.TrimSpace(run.AssetID)
	run.Status, run.Stage = "queued", "queued"
	run.PolicyVersion, run.PromptVersion = PolicyVersion, PromptVersion
	if run.ModelVersion == "" {
		run.ModelVersion = "unconfigured"
	}
	summary, _ := json.Marshal(map[string]any{})
	blockers, _ := json.Marshal([]string{})
	tag, err := s.db.Exec(ctx, `INSERT INTO fundamental_ai_runs(id,batch_id,asset_id,task_id,status,stage,policy_version,model_version,prompt_version,as_of,summary,blockers,idempotency_key,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14) ON CONFLICT(idempotency_key) DO NOTHING`,
		run.ID, run.BatchID, run.AssetID, run.TaskID, run.Status, run.Stage, run.PolicyVersion, run.ModelVersion, run.PromptVersion, run.AsOf.UTC(), summary, blockers, strings.TrimSpace(idempotencyKey), now)
	if err != nil {
		return Run{}, false, err
	}
	stored, err := s.byIdempotency(ctx, idempotencyKey)
	return stored, tag.RowsAffected() == 1, err
}

func (s *Store) UpdateRun(ctx context.Context, id uuid.UUID, status, stage string, summary map[string]any, blockers []string) error {
	if s.db == nil || id == uuid.Nil {
		return fmt.Errorf("AI preparation run is unavailable")
	}
	if summary == nil {
		summary = map[string]any{}
	}
	if blockers == nil {
		blockers = []string{}
	}
	summaryJSON, _ := json.Marshal(summary)
	blockersJSON, _ := json.Marshal(blockers)
	_, err := s.db.Exec(ctx, `UPDATE fundamental_ai_runs SET status=$2::varchar(32),stage=$3,summary=$4,blockers=$5,
		started_at=CASE WHEN $2::varchar(32)='running' THEN coalesce(started_at,now()) ELSE started_at END,
		completed_at=CASE WHEN $2::varchar(32) IN ('completed','insufficient_data','failed','cancelled') THEN now() ELSE NULL END,updated_at=now() WHERE id=$1`,
		id, status, stage, summaryJSON, blockersJSON)
	return err
}

func (s *Store) SaveSource(ctx context.Context, value SourceSnapshot) (SourceSnapshot, bool, error) {
	if s.db == nil || value.RunID == uuid.Nil || strings.TrimSpace(value.SourceURL) == "" {
		return SourceSnapshot{}, false, fmt.Errorf("AI source run and URL are required")
	}
	value.SourceURL = strings.TrimSpace(value.SourceURL)
	value.ContentHash = hash(value.ContentText)
	value.ID = "ai-source-" + hash(value.RunID.String() + "|" + value.SourceURL)[:48]
	if value.ObservedAt.IsZero() {
		value.ObservedAt = time.Now().UTC()
	}
	if value.AvailableAt.IsZero() || value.AvailableAt.Before(value.ObservedAt) {
		value.AvailableAt = value.ObservedAt
	}
	if value.RetrievalStatus == "" {
		value.RetrievalStatus = "available"
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO fundamental_ai_source_snapshots(id,run_id,query,title,source_name,source_class,source_url,source_domain,published_at,observed_at,available_at,content_type,content_hash,content_text,retrieval_status,retrieval_detail)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) ON CONFLICT(run_id,source_url) DO NOTHING`,
		value.ID, value.RunID, value.Query, value.Title, value.SourceName, value.SourceClass, value.SourceURL, value.SourceDomain, value.PublishedAt,
		value.ObservedAt.UTC(), value.AvailableAt.UTC(), value.ContentType, value.ContentHash, value.ContentText, value.RetrievalStatus, value.RetrievalDetail)
	if err != nil {
		return SourceSnapshot{}, false, err
	}
	stored, err := s.sourceByURL(ctx, value.RunID, value.SourceURL)
	return stored, tag.RowsAffected() == 1, err
}

func (s *Store) SaveCandidate(ctx context.Context, value Candidate) (Candidate, bool, error) {
	if s.db == nil || value.RunID == uuid.Nil || value.AssetID == "" || value.EvidenceType == "" || value.Title == "" {
		return Candidate{}, false, fmt.Errorf("AI candidate identity is required")
	}
	if value.Values == nil {
		value.Values = map[string]any{}
	}
	if value.SourceSnapshotIDs == nil {
		value.SourceSnapshotIDs = []string{}
	}
	if value.Validation == nil {
		value.Validation = map[string]any{}
	}
	identity := struct {
		RunID, AssetID, EvidenceType, Title, Rationale, EvidenceQuote, EvidenceLocation string
		Values                                                                          any
		Sources                                                                         []string
	}{value.RunID.String(), value.AssetID, value.EvidenceType, value.Title, value.Rationale, value.EvidenceQuote, value.EvidenceLocation, value.Values, value.SourceSnapshotIDs}
	body, _ := json.Marshal(identity)
	value.ID = "ai-candidate-" + hash(string(body))[:48]
	values, _ := json.Marshal(value.Values)
	sources, _ := json.Marshal(value.SourceSnapshotIDs)
	validation, _ := json.Marshal(value.Validation)
	tag, err := s.db.Exec(ctx, `INSERT INTO fundamental_ai_candidates(id,run_id,asset_id,evidence_type,title,rationale,values,source_snapshot_ids,evidence_quote,evidence_location,status,validation,approved_evidence_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13,'')) ON CONFLICT(id) DO NOTHING`, value.ID, value.RunID, value.AssetID, value.EvidenceType,
		value.Title, value.Rationale, values, sources, value.EvidenceQuote, value.EvidenceLocation, value.Status, validation, value.ApprovedEvidenceID)
	if err != nil {
		return Candidate{}, false, err
	}
	stored, err := s.candidateByID(ctx, value.ID)
	return stored, tag.RowsAffected() == 1, err
}

func (s *Store) SetCandidateOutcome(ctx context.Context, id, status, approvedEvidenceID string, validation map[string]any) error {
	if s.db == nil || strings.TrimSpace(id) == "" || (status != "policy_approved" && status != "insufficient_data" && status != "rejected") {
		return fmt.Errorf("invalid AI candidate outcome")
	}
	if validation == nil {
		validation = map[string]any{}
	}
	body, _ := json.Marshal(validation)
	_, err := s.db.Exec(ctx, `UPDATE fundamental_ai_candidates SET status=$2,approved_evidence_id=NULLIF($3,''),validation=$4 WHERE id=$1`, strings.TrimSpace(id), status, strings.TrimSpace(approvedEvidenceID), body)
	return err
}

func (s *Store) Latest(ctx context.Context, assetID string) (Run, []SourceSnapshot, []Candidate, error) {
	run, err := scanRun(s.db.QueryRow(ctx, runSelect+` WHERE asset_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1`, strings.TrimSpace(assetID)))
	if err != nil {
		return Run{}, nil, nil, err
	}
	sources, err := s.sources(ctx, run.ID)
	if err != nil {
		return Run{}, nil, nil, err
	}
	candidates, err := s.candidates(ctx, run.ID)
	return run, sources, candidates, err
}

func (s *Store) GetByTaskID(ctx context.Context, taskID uuid.UUID) (Run, error) {
	if s.db == nil || taskID == uuid.Nil {
		return Run{}, fmt.Errorf("AI preparation task identity is required")
	}
	return scanRun(s.db.QueryRow(ctx, runSelect+` WHERE task_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1`, taskID))
}

func (s *Store) DeleteEmptyRun(ctx context.Context, id uuid.UUID) error {
	if s.db == nil || id == uuid.Nil {
		return fmt.Errorf("AI preparation run identity is required")
	}
	_, err := s.db.Exec(ctx, `DELETE FROM fundamental_ai_runs WHERE id=$1 AND status='queued'
		AND NOT EXISTS(SELECT 1 FROM fundamental_ai_source_snapshots WHERE run_id=$1)
		AND NOT EXISTS(SELECT 1 FROM fundamental_ai_candidates WHERE run_id=$1)`, id)
	return err
}

func (s *Store) CreateBatch(ctx context.Context, batch Batch, idempotencyKey string) (Batch, bool, error) {
	if s.db == nil || batch.ID == uuid.Nil || batch.RequestedCount < 0 || batch.RequestedCount > 200 || strings.TrimSpace(idempotencyKey) == "" {
		return Batch{}, false, fmt.Errorf("invalid AI preparation batch")
	}
	tasks, _ := json.Marshal(batch.TaskIDs)
	tag, err := s.db.Exec(ctx, `INSERT INTO fundamental_ai_batches(id,market,scope,requested_count,task_ids,idempotency_key) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(idempotency_key) DO NOTHING`,
		batch.ID, strings.ToUpper(strings.TrimSpace(batch.Market)), strings.TrimSpace(batch.Scope), batch.RequestedCount, tasks, strings.TrimSpace(idempotencyKey))
	if err != nil {
		return Batch{}, false, err
	}
	stored, err := s.batchByIdempotency(ctx, idempotencyKey)
	return stored, tag.RowsAffected() == 1, err
}

func (s *Store) GetBatch(ctx context.Context, id uuid.UUID) (Batch, error) {
	batch, err := scanBatch(s.db.QueryRow(ctx, `SELECT id,market,scope,requested_count,task_ids::jsonb,created_at FROM fundamental_ai_batches WHERE id=$1`, id))
	if err != nil {
		return Batch{}, err
	}
	batch.Counts = map[string]int{"waiting": 0, "searching": 0, "reasoning": 0, "released": 0, "insufficient_data": 0, "failed": 0}
	rows, err := s.db.Query(ctx, `SELECT status,stage,count(*)::int FROM fundamental_ai_runs WHERE batch_id=$1 GROUP BY status,stage`, id)
	if err != nil {
		return Batch{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var status, stage string
		var count int
		if err := rows.Scan(&status, &stage, &count); err != nil {
			return Batch{}, err
		}
		switch {
		case status == "queued":
			batch.Counts["waiting"] += count
		case status == "running" && stage == "ai_reasoning":
			batch.Counts["reasoning"] += count
		case status == "running":
			batch.Counts["searching"] += count
		case status == "completed":
			batch.Counts["released"] += count
		case status == "insufficient_data":
			batch.Counts["insufficient_data"] += count
		case status == "failed" || status == "cancelled":
			batch.Counts["failed"] += count
		}
	}
	return batch, rows.Err()
}

const runSelect = `SELECT id,batch_id,asset_id,task_id,status,stage,policy_version,model_version,prompt_version,as_of,summary::jsonb,blockers::jsonb,started_at,completed_at,created_at,updated_at FROM fundamental_ai_runs`

type rowScanner interface{ Scan(...any) error }

func scanRun(row rowScanner) (Run, error) {
	var value Run
	var summary, blockers []byte
	if err := row.Scan(&value.ID, &value.BatchID, &value.AssetID, &value.TaskID, &value.Status, &value.Stage, &value.PolicyVersion, &value.ModelVersion, &value.PromptVersion, &value.AsOf,
		&summary, &blockers, &value.StartedAt, &value.CompletedAt, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return Run{}, err
	}
	_ = json.Unmarshal(summary, &value.Summary)
	_ = json.Unmarshal(blockers, &value.Blockers)
	return value, nil
}

func (s *Store) byIdempotency(ctx context.Context, key string) (Run, error) {
	return scanRun(s.db.QueryRow(ctx, runSelect+` WHERE idempotency_key=$1`, strings.TrimSpace(key)))
}

func (s *Store) sourceByURL(ctx context.Context, runID uuid.UUID, sourceURL string) (SourceSnapshot, error) {
	return scanSource(s.db.QueryRow(ctx, `SELECT id,run_id,query,title,source_name,source_class,source_url,source_domain,published_at,observed_at,available_at,content_type,content_hash,content_text,retrieval_status,retrieval_detail FROM fundamental_ai_source_snapshots WHERE run_id=$1 AND source_url=$2`, runID, sourceURL))
}

func scanSource(row rowScanner) (SourceSnapshot, error) {
	var value SourceSnapshot
	if err := row.Scan(&value.ID, &value.RunID, &value.Query, &value.Title, &value.SourceName, &value.SourceClass, &value.SourceURL, &value.SourceDomain, &value.PublishedAt,
		&value.ObservedAt, &value.AvailableAt, &value.ContentType, &value.ContentHash, &value.ContentText, &value.RetrievalStatus, &value.RetrievalDetail); err != nil {
		return SourceSnapshot{}, err
	}
	return value, nil
}

func (s *Store) sources(ctx context.Context, runID uuid.UUID) ([]SourceSnapshot, error) {
	rows, err := s.db.Query(ctx, `SELECT id,run_id,query,title,source_name,source_class,source_url,source_domain,published_at,observed_at,available_at,content_type,content_hash,content_text,retrieval_status,retrieval_detail FROM fundamental_ai_source_snapshots WHERE run_id=$1 ORDER BY source_class,available_at,id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []SourceSnapshot{}
	for rows.Next() {
		value, err := scanSource(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Store) candidateByID(ctx context.Context, id string) (Candidate, error) {
	return scanCandidate(s.db.QueryRow(ctx, `SELECT id,run_id,asset_id,evidence_type,title,rationale,values::jsonb,source_snapshot_ids::jsonb,evidence_quote,evidence_location,status,validation::jsonb,coalesce(approved_evidence_id,''),created_at FROM fundamental_ai_candidates WHERE id=$1`, id))
}

func scanCandidate(row rowScanner) (Candidate, error) {
	var value Candidate
	var values, sources, validation []byte
	if err := row.Scan(&value.ID, &value.RunID, &value.AssetID, &value.EvidenceType, &value.Title, &value.Rationale, &values, &sources, &value.EvidenceQuote, &value.EvidenceLocation, &value.Status, &validation, &value.ApprovedEvidenceID, &value.CreatedAt); err != nil {
		return Candidate{}, err
	}
	_ = json.Unmarshal(values, &value.Values)
	_ = json.Unmarshal(sources, &value.SourceSnapshotIDs)
	_ = json.Unmarshal(validation, &value.Validation)
	return value, nil
}

func (s *Store) candidates(ctx context.Context, runID uuid.UUID) ([]Candidate, error) {
	rows, err := s.db.Query(ctx, `SELECT id,run_id,asset_id,evidence_type,title,rationale,values::jsonb,source_snapshot_ids::jsonb,evidence_quote,evidence_location,status,validation::jsonb,coalesce(approved_evidence_id,''),created_at FROM fundamental_ai_candidates WHERE run_id=$1 ORDER BY evidence_type,id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []Candidate{}
	for rows.Next() {
		value, err := scanCandidate(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Store) batchByIdempotency(ctx context.Context, key string) (Batch, error) {
	return scanBatch(s.db.QueryRow(ctx, `SELECT id,market,scope,requested_count,task_ids::jsonb,created_at FROM fundamental_ai_batches WHERE idempotency_key=$1`, strings.TrimSpace(key)))
}

func scanBatch(row rowScanner) (Batch, error) {
	var value Batch
	var tasks []byte
	if err := row.Scan(&value.ID, &value.Market, &value.Scope, &value.RequestedCount, &tasks, &value.CreatedAt); err != nil {
		return Batch{}, err
	}
	_ = json.Unmarshal(tasks, &value.TaskIDs)
	return value, nil
}

func hash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func IsNotFound(err error) bool { return err == pgx.ErrNoRows }
