package evaluation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PerformanceReportInput struct {
	ExperimentID   string `json:"experiment_id"`
	CreatedBy      string `json:"created_by"`
	IdempotencyKey string `json:"-"`
}

type StoredPerformanceReport struct {
	Report    LayeredPerformanceReport `json:"report"`
	CreatedBy string                   `json:"created_by"`
	CreatedAt time.Time                `json:"created_at"`
}

type PerformanceReportStore struct{ db *pgxpool.Pool }

func NewPerformanceReportStore(db *pgxpool.Pool) *PerformanceReportStore {
	return &PerformanceReportStore{db: db}
}

func (s *PerformanceReportStore) Materialize(ctx context.Context, input PerformanceReportInput, now time.Time) (StoredPerformanceReport, bool, error) {
	if s.db == nil {
		return StoredPerformanceReport{}, false, fmt.Errorf("performance report store is unavailable")
	}
	input.ExperimentID = strings.TrimSpace(input.ExperimentID)
	input.CreatedBy = strings.TrimSpace(input.CreatedBy)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.ExperimentID == "" || input.CreatedBy == "" || input.IdempotencyKey == "" {
		return StoredPerformanceReport{}, false, fmt.Errorf("experiment_id, created_by and idempotency key are required")
	}
	experiment, err := NewExperimentStore(s.db).Get(ctx, input.ExperimentID)
	if err != nil {
		return StoredPerformanceReport{}, false, fmt.Errorf("load development experiment: %w", err)
	}
	dataset, err := NewDatasetStore(s.db).GetDataset(ctx, experiment.Experiment.DatasetID)
	if err != nil {
		return StoredPerformanceReport{}, false, fmt.Errorf("load evaluation dataset: %w", err)
	}
	research, err := s.researchSummary(ctx, dataset.Manifest.ID)
	if err != nil {
		return StoredPerformanceReport{}, false, err
	}
	execution, err := s.executionSummary(ctx, experiment.Experiment.ID)
	if err != nil {
		return StoredPerformanceReport{}, false, err
	}
	report, err := BuildLayeredPerformanceReport(dataset, experiment, research, execution)
	if err != nil {
		return StoredPerformanceReport{}, false, err
	}
	stored := StoredPerformanceReport{Report: report, CreatedBy: input.CreatedBy, CreatedAt: now.UTC()}
	body, _ := json.Marshal(report)
	tag, err := s.db.Exec(ctx, `INSERT INTO evaluation_performance_reports(id,contract_version,dataset_id,experiment_id,asset_class,market,objective,
		horizon_sessions,report,artifact_digest,final_holdout_accessed,created_by,idempotency_key,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,false,$11,$12,$13) ON CONFLICT(idempotency_key) DO NOTHING`, report.ID,
		report.ContractVersion, report.DatasetID, report.ExperimentID, report.AssetClass, report.Market, report.Objective, report.HorizonSessions,
		body, report.ArtifactDigest, stored.CreatedBy, input.IdempotencyKey, stored.CreatedAt)
	if err != nil {
		return StoredPerformanceReport{}, false, fmt.Errorf("save performance report: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return stored, true, nil
	}
	existing, err := s.getByIdempotencyKey(ctx, input.IdempotencyKey)
	if err != nil {
		return StoredPerformanceReport{}, false, err
	}
	if existing.Report.ArtifactDigest != report.ArtifactDigest {
		return StoredPerformanceReport{}, false, fmt.Errorf("idempotency key is already bound to a different performance report")
	}
	return existing, false, nil
}

func (s *PerformanceReportStore) researchSummary(ctx context.Context, datasetID string) (ResearchPerformanceSummary, error) {
	var result ResearchPerformanceSummary
	err := s.db.QueryRow(ctx, `WITH samples AS (
		SELECT DISTINCT p.id,p.asset_id,p.event_id,p.signal_available_at
		FROM evaluation_dataset_samples sample JOIN prediction_runs p ON p.id=sample.prediction_run_id
		WHERE sample.dataset_id=$1 AND sample.fold_index>=0 AND sample.sealed=false AND sample.partition IN ('train','calibration','test')
	), linked AS (
		SELECT samples.id AS prediction_id,research.id AS research_id,lower(coalesce(research.status,'')) AS status,
			extract(epoch FROM research.updated_at-research.created_at) AS latency
		FROM samples LEFT JOIN LATERAL (
			SELECT run.id,run.status,run.created_at,run.updated_at FROM research_runs run
			WHERE run.asset_id=samples.asset_id AND run.event_id::text IS NOT DISTINCT FROM samples.event_id::text AND run.updated_at<=samples.signal_available_at
			ORDER BY run.updated_at DESC,run.id DESC LIMIT 1
		) research ON true
	)
	SELECT count(*)::int,count(research_id)::int,
		count(*) FILTER (WHERE status IN ('completed','succeeded','success'))::int,
		count(*) FILTER (WHERE status IN ('insufficient_evidence','refused','no_evidence'))::int,
		count(*) FILTER (WHERE research_id IS NOT NULL AND status NOT IN ('completed','succeeded','success','insufficient_evidence','refused','no_evidence'))::int,
		count(*) FILTER (WHERE research_id IS NULL)::int,avg(latency) FILTER (WHERE latency>=0)
	FROM linked`, datasetID).Scan(&result.CandidatePredictions, &result.LinkedResearchRuns, &result.Completed, &result.InsufficientEvidence,
		&result.TechnicalFailures, &result.MissingResearch, &result.MeanLatencySeconds)
	if err != nil {
		return result, fmt.Errorf("summarize research performance: %w", err)
	}
	err = s.db.QueryRow(ctx, `WITH samples AS (
		SELECT DISTINCT p.asset_id,p.event_id,p.signal_available_at
		FROM evaluation_dataset_samples sample JOIN prediction_runs p ON p.id=sample.prediction_run_id
		WHERE sample.dataset_id=$1 AND sample.fold_index>=0 AND sample.sealed=false AND sample.partition IN ('train','calibration','test')
	), reviews AS (
		SELECT DISTINCT review.policy_evaluation_id,review.decision
		FROM samples JOIN policy_evaluations evaluation ON evaluation.asset_id=samples.asset_id
			AND evaluation.event_id::text IS NOT DISTINCT FROM samples.event_id::text AND evaluation.created_at<=samples.signal_available_at
		JOIN policy_impact_reviews review ON review.policy_evaluation_id=evaluation.id
	)
	SELECT count(*)::int,count(*) FILTER (WHERE decision='accepted')::int FROM reviews`, datasetID).
		Scan(&result.ReviewedPolicyEvaluations, &result.AcceptedPolicyEvaluations)
	if err != nil {
		return result, fmt.Errorf("summarize reviewed research performance: %w", err)
	}
	err = s.db.QueryRow(ctx, `WITH samples AS (
		SELECT DISTINCT p.asset_id,p.event_id,p.signal_available_at
		FROM evaluation_dataset_samples sample JOIN prediction_runs p ON p.id=sample.prediction_run_id
		WHERE sample.dataset_id=$1 AND sample.fold_index>=0 AND sample.sealed=false AND sample.partition IN ('train','calibration','test')
	), linked AS (
		SELECT DISTINCT research.id FROM samples JOIN LATERAL (
			SELECT run.id FROM research_runs run WHERE run.asset_id=samples.asset_id
				AND run.event_id::text IS NOT DISTINCT FROM samples.event_id::text AND run.updated_at<=samples.signal_available_at
			ORDER BY run.updated_at DESC,run.id DESC LIMIT 1
		) research ON true
	), reviews AS (
		SELECT review.* FROM linked JOIN research_quality_reviews review ON review.research_run_id=linked.id
	)
	SELECT count(fact_correct)::int,count(*) FILTER (WHERE fact_correct=true)::int,
		count(relationship_correct)::int,count(*) FILTER (WHERE relationship_correct=true)::int,
		count(citation_supported)::int,count(*) FILTER (WHERE citation_supported=true)::int,
		count(refusal_appropriate)::int,count(*) FILTER (WHERE refusal_appropriate=true)::int FROM reviews`, datasetID).
		Scan(&result.FactReviewed, &result.FactCorrect, &result.RelationshipReviewed, &result.RelationshipCorrect,
			&result.CitationReviewed, &result.CitationCorrect, &result.RefusalReviewed, &result.RefusalAppropriate)
	if err != nil {
		return result, fmt.Errorf("summarize dimension-specific research reviews: %w", err)
	}
	return result, nil
}

func (s *PerformanceReportStore) executionSummary(ctx context.Context, experimentID string) (ExecutionPerformanceSummary, error) {
	var result ExecutionPerformanceSummary
	err := s.db.QueryRow(ctx, `WITH samples AS (
		SELECT DISTINCT prediction_run_id FROM evaluation_experiment_predictions WHERE experiment_id=$1
	), outcomes AS (
		SELECT record.* FROM samples JOIN outcome_records record ON record.prediction_run_id=samples.prediction_run_id AND record.status='mature'
	)
	SELECT (SELECT count(*) FROM samples)::int,
		count(*) FILTER (WHERE net_return IS NOT NULL AND execution_cost_bps IS NOT NULL)::int,
		avg(raw_return),avg(excess_return),avg(net_return) FILTER (WHERE execution_cost_bps IS NOT NULL)
	FROM outcomes`, experimentID).Scan(&result.TestPredictions, &result.Executable, &result.MeanRawReturn, &result.MeanExcessReturn, &result.MeanNetReturn)
	if err != nil {
		return result, fmt.Errorf("summarize execution performance: %w", err)
	}
	return result, nil
}

func (s *PerformanceReportStore) Get(ctx context.Context, id string) (StoredPerformanceReport, error) {
	return s.scan(s.db.QueryRow(ctx, `SELECT report::jsonb,created_by,created_at FROM evaluation_performance_reports WHERE id=$1`, strings.TrimSpace(id)))
}

func (s *PerformanceReportStore) List(ctx context.Context, limit int) ([]StoredPerformanceReport, error) {
	if s.db == nil || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid performance report query")
	}
	rows, err := s.db.Query(ctx, `SELECT report::jsonb,created_by,created_at FROM evaluation_performance_reports ORDER BY created_at DESC,id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []StoredPerformanceReport{}
	for rows.Next() {
		item, scanErr := s.scan(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *PerformanceReportStore) getByIdempotencyKey(ctx context.Context, key string) (StoredPerformanceReport, error) {
	return s.scan(s.db.QueryRow(ctx, `SELECT report::jsonb,created_by,created_at FROM evaluation_performance_reports WHERE idempotency_key=$1`, key))
}

func (s *PerformanceReportStore) scan(row rowScanner) (StoredPerformanceReport, error) {
	var result StoredPerformanceReport
	var body []byte
	if err := row.Scan(&body, &result.CreatedBy, &result.CreatedAt); err != nil {
		return result, err
	}
	if err := json.Unmarshal(body, &result.Report); err != nil {
		return result, err
	}
	return result, nil
}
