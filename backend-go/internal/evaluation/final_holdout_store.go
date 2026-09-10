package evaluation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

type FinalHoldoutEvaluationStore struct{ db *pgxpool.Pool }

func NewFinalHoldoutEvaluationStore(db *pgxpool.Pool) *FinalHoldoutEvaluationStore {
	return &FinalHoldoutEvaluationStore{db: db}
}

func (s *FinalHoldoutEvaluationStore) Materialize(ctx context.Context, input FinalHoldoutEvaluationInput, now time.Time) (FinalHoldoutEvaluationReport, bool, error) {
	if s.db == nil {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("final holdout evaluation store is unavailable")
	}
	input.DatasetID = strings.TrimSpace(input.DatasetID)
	input.ExperimentID = strings.TrimSpace(input.ExperimentID)
	input.DevelopmentReportID = strings.TrimSpace(input.DevelopmentReportID)
	input.VariantName = strings.TrimSpace(input.VariantName)
	input.VariantArtifactDigest = strings.TrimSpace(input.VariantArtifactDigest)
	input.ApprovedBy = strings.TrimSpace(input.ApprovedBy)
	input.ApprovalReason = strings.TrimSpace(input.ApprovalReason)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.DatasetID == "" || input.ExperimentID == "" || input.DevelopmentReportID == "" || input.VariantName == "" ||
		input.VariantArtifactDigest == "" || input.ApprovedBy == "" || input.ApprovalReason == "" || input.IdempotencyKey == "" {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("dataset_id, experiment_id, development_report_id, variant, approval and idempotency key are required")
	}
	now = now.UTC()
	dataset, err := NewDatasetStore(s.db).GetDataset(ctx, input.DatasetID)
	if err != nil {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("load evaluation dataset: %w", err)
	}
	experiment, err := NewExperimentStore(s.db).Get(ctx, input.ExperimentID)
	if err != nil {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("load development experiment: %w", err)
	}
	development, err := NewPerformanceReportStore(s.db).Get(ctx, input.DevelopmentReportID)
	if err != nil {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("load development performance report: %w", err)
	}
	reservation, err := NewDatasetStore(s.db).GetHoldout(ctx, dataset.Manifest.Config.HoldoutReservationID)
	if err != nil {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("load final holdout reservation: %w", err)
	}
	if now.Before(reservation.LabelCutoff) {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("final holdout evaluation cannot run before the pre-registered label cutoff")
	}
	if _, err = validateFinalHoldoutSelection(dataset, experiment, development, input); err != nil {
		return FinalHoldoutEvaluationReport{}, false, err
	}
	requestDigest := digestValue(struct {
		Contract, Dataset, Experiment, DevelopmentReport, Variant, VariantDigest, ApprovedBy, ApprovalReason string
		Fold                                                                                                 int
	}{FinalHoldoutEvaluationContractVersion, input.DatasetID, input.ExperimentID, input.DevelopmentReportID, input.VariantName,
		input.VariantArtifactDigest, input.ApprovedBy, input.ApprovalReason, input.FoldIndex})

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return FinalHoldoutEvaluationReport{}, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if existing, findErr := getFinalHoldoutByIdempotencyKey(ctx, tx, input.IdempotencyKey); findErr == nil {
		if existing.RequestDigest != requestDigest {
			return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("idempotency key is already bound to a different final holdout evaluation")
		}
		return existing, false, tx.Commit(ctx)
	} else if !errors.Is(findErr, pgx.ErrNoRows) {
		return FinalHoldoutEvaluationReport{}, false, findErr
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "final-holdout:"+reservation.ID); err != nil {
		return FinalHoldoutEvaluationReport{}, false, err
	}
	if existing, findErr := getFinalHoldoutByReservation(ctx, tx, reservation.ID); findErr == nil {
		if existing.RequestDigest != requestDigest {
			return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("the pre-registered final holdout has already been evaluated with a different locked selection")
		}
		return existing, false, tx.Commit(ctx)
	} else if !errors.Is(findErr, pgx.ErrNoRows) {
		return FinalHoldoutEvaluationReport{}, false, findErr
	}

	var sealedCount, matureCount int
	if err = tx.QueryRow(ctx, `SELECT count(*)::int,
		count(*) FILTER (WHERE outcome.status='mature' AND outcome.label_available_at<=$2 AND outcome.objective=$3 AND outcome.horizon_sessions=$4
			AND (($3='absolute_up' AND outcome.objective_label IN ('up','down')) OR ($3='excess_up' AND outcome.objective_label IN ('outperform','underperform'))))::int
		FROM evaluation_dataset_samples sample
		LEFT JOIN outcome_records outcome ON outcome.prediction_run_id=sample.prediction_run_id
		WHERE sample.dataset_id=$1 AND sample.fold_index=-1 AND sample.partition='final_holdout'
			AND sample.intended_partition='final_holdout' AND sample.sealed=true`, dataset.Manifest.ID, now, dataset.Objective, dataset.HorizonSessions).Scan(&sealedCount, &matureCount); err != nil {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("inspect sealed final holdout readiness: %w", err)
	}
	if sealedCount != dataset.Manifest.FinalHoldout.IncludedCount || sealedCount == 0 {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("sealed final holdout sample count does not match the immutable manifest")
	}
	if matureCount != sealedCount {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("all sealed final holdout outcomes must be mature and available before the one-time evaluation")
	}
	samples, err := loadFinalHoldoutSamples(ctx, tx, dataset.Manifest.ID, dataset.Objective, dataset.HorizonSessions, now)
	if err != nil {
		return FinalHoldoutEvaluationReport{}, false, err
	}
	report, err := buildFinalHoldoutEvaluation(dataset, experiment, development, input, requestDigest, samples, now)
	if err != nil {
		return FinalHoldoutEvaluationReport{}, false, err
	}
	body, _ := json.Marshal(report)
	_, err = tx.Exec(ctx, `INSERT INTO evaluation_final_holdout_reports(
		id,contract_version,dataset_id,holdout_reservation_id,experiment_id,development_report_id,asset_class,market,objective,horizon_sessions,
		fold_index,variant_name,variant_kind,variant_artifact_digest,request_digest,report,artifact_digest,final_holdout_accessed,sample_ids_shown,
		automatic_model_selection,approved_by,approval_reason,idempotency_key,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,true,false,false,$18,$19,$20,$21)`,
		report.ID, report.ContractVersion, report.DatasetID, report.HoldoutReservationID, report.ExperimentID, report.DevelopmentReportID,
		report.AssetClass, report.Market, report.Objective, report.HorizonSessions, report.FoldIndex, report.VariantName, report.VariantKind,
		report.VariantArtifactDigest, report.RequestDigest, body, report.ArtifactDigest, report.ApprovedBy, report.ApprovalReason, input.IdempotencyKey, report.EvaluatedAt)
	if err != nil {
		return FinalHoldoutEvaluationReport{}, false, fmt.Errorf("save one-time final holdout evaluation: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return FinalHoldoutEvaluationReport{}, false, err
	}
	return report, true, nil
}

func (s *FinalHoldoutEvaluationStore) Get(ctx context.Context, id string) (FinalHoldoutEvaluationReport, error) {
	if s.db == nil {
		return FinalHoldoutEvaluationReport{}, fmt.Errorf("final holdout evaluation store is unavailable")
	}
	return scanFinalHoldoutReport(s.db.QueryRow(ctx, `SELECT report::jsonb FROM evaluation_final_holdout_reports WHERE id=$1`, strings.TrimSpace(id)))
}

func (s *FinalHoldoutEvaluationStore) List(ctx context.Context, limit int) ([]FinalHoldoutEvaluationReport, error) {
	if s.db == nil || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid final holdout evaluation query")
	}
	rows, err := s.db.Query(ctx, `SELECT report::jsonb FROM evaluation_final_holdout_reports ORDER BY created_at DESC,id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []FinalHoldoutEvaluationReport{}
	for rows.Next() {
		item, scanErr := scanFinalHoldoutReport(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func loadFinalHoldoutSamples(ctx context.Context, tx pgx.Tx, datasetID, objective string, horizon int, asOf time.Time) ([]ExperimentSample, error) {
	rows, err := tx.Query(ctx, `SELECT sample.prediction_run_id,sample.event_cluster,sample.signal_available_at,sample.label_available_at,
		prediction.feature_snapshot::jsonb,outcome.objective_label,outcome.raw_return,outcome.excess_return
		FROM evaluation_dataset_samples sample
		JOIN prediction_runs prediction ON prediction.id=sample.prediction_run_id
		JOIN outcome_records outcome ON outcome.prediction_run_id=sample.prediction_run_id
		WHERE sample.dataset_id=$1 AND sample.fold_index=-1 AND sample.partition='final_holdout'
			AND sample.intended_partition='final_holdout' AND sample.sealed=true
			AND outcome.status='mature' AND outcome.label_available_at<=$2 AND outcome.objective=$3 AND outcome.horizon_sessions=$4
		ORDER BY sample.signal_available_at,sample.prediction_run_id`, datasetID, asOf, objective, horizon)
	if err != nil {
		return nil, fmt.Errorf("access sealed final holdout: %w", err)
	}
	defer rows.Close()
	result := []ExperimentSample{}
	for rows.Next() {
		var snapshotBody []byte
		var objectiveLabel string
		var sample ExperimentSample
		sample.Partition = "final_holdout"
		if err = rows.Scan(&sample.ID, &sample.EventCluster, &sample.SignalAt, &sample.LabelMatureAt, &snapshotBody,
			&objectiveLabel, &sample.RawReturn, &sample.ExcessReturn); err != nil {
			return nil, err
		}
		snapshot := signals.Snapshot{}
		if err = json.Unmarshal(snapshotBody, &snapshot); err != nil {
			return nil, fmt.Errorf("decode final holdout feature snapshot: %w", err)
		}
		sample.Values = snapshot.Values
		switch objectiveLabel {
		case "up", "outperform":
			sample.Label = true
		case "down", "underperform":
			sample.Label = false
		default:
			return nil, fmt.Errorf("final holdout objective label is invalid")
		}
		result = append(result, sample)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func getFinalHoldoutByIdempotencyKey(ctx context.Context, tx pgx.Tx, key string) (FinalHoldoutEvaluationReport, error) {
	return scanFinalHoldoutReport(tx.QueryRow(ctx, `SELECT report::jsonb FROM evaluation_final_holdout_reports WHERE idempotency_key=$1`, key))
}

func getFinalHoldoutByReservation(ctx context.Context, tx pgx.Tx, reservationID string) (FinalHoldoutEvaluationReport, error) {
	return scanFinalHoldoutReport(tx.QueryRow(ctx, `SELECT report::jsonb FROM evaluation_final_holdout_reports WHERE holdout_reservation_id=$1`, reservationID))
}

func scanFinalHoldoutReport(row rowScanner) (FinalHoldoutEvaluationReport, error) {
	var result FinalHoldoutEvaluationReport
	var body []byte
	if err := row.Scan(&body); err != nil {
		return result, err
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return result, err
	}
	return result, nil
}
