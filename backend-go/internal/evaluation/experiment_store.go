package evaluation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/signals"
)

type ExperimentBuildInput struct {
	DatasetID      string `json:"dataset_id"`
	CreatedBy      string `json:"created_by"`
	IdempotencyKey string `json:"-"`
}

type StoredExperiment struct {
	Experiment DevelopmentExperiment `json:"experiment"`
	CreatedBy  string                `json:"created_by"`
	CreatedAt  time.Time             `json:"created_at"`
}

type ExperimentStore struct{ db *pgxpool.Pool }

func NewExperimentStore(db *pgxpool.Pool) *ExperimentStore { return &ExperimentStore{db: db} }

func (s *ExperimentStore) Materialize(ctx context.Context, input ExperimentBuildInput, now time.Time) (StoredExperiment, bool, error) {
	if s.db == nil {
		return StoredExperiment{}, false, fmt.Errorf("evaluation experiment store is unavailable")
	}
	input.DatasetID = strings.TrimSpace(input.DatasetID)
	input.CreatedBy = strings.TrimSpace(input.CreatedBy)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.DatasetID == "" || input.CreatedBy == "" || input.IdempotencyKey == "" {
		return StoredExperiment{}, false, fmt.Errorf("dataset_id, created_by and idempotency key are required")
	}
	dataset, err := NewDatasetStore(s.db).GetDataset(ctx, input.DatasetID)
	if err != nil {
		return StoredExperiment{}, false, fmt.Errorf("load evaluation dataset: %w", err)
	}
	samples, err := s.loadDevelopmentSamples(ctx, dataset)
	if err != nil {
		return StoredExperiment{}, false, err
	}
	experiment, err := RunDevelopmentExperiment(dataset, samples)
	if err != nil {
		return StoredExperiment{}, false, err
	}
	stored := StoredExperiment{Experiment: experiment, CreatedBy: input.CreatedBy, CreatedAt: now.UTC()}
	created, err := s.persist(ctx, stored, input.IdempotencyKey)
	if err != nil {
		return StoredExperiment{}, false, err
	}
	if created {
		return stored, true, nil
	}
	existing, err := s.getByIdempotencyKey(ctx, input.IdempotencyKey)
	if err != nil {
		return StoredExperiment{}, false, err
	}
	if existing.Experiment.ArtifactDigest != experiment.ArtifactDigest {
		return StoredExperiment{}, false, fmt.Errorf("idempotency key is already bound to a different development experiment")
	}
	return existing, false, nil
}

func (s *ExperimentStore) Get(ctx context.Context, id string) (StoredExperiment, error) {
	if s.db == nil {
		return StoredExperiment{}, fmt.Errorf("evaluation experiment store is unavailable")
	}
	return s.scan(s.db.QueryRow(ctx, `SELECT report::jsonb,created_by,created_at FROM evaluation_experiments WHERE id=$1`, strings.TrimSpace(id)))
}

func (s *ExperimentStore) List(ctx context.Context, limit int) ([]StoredExperiment, error) {
	if s.db == nil || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid development experiment query")
	}
	rows, err := s.db.Query(ctx, `SELECT report::jsonb,created_by,created_at FROM evaluation_experiments ORDER BY created_at DESC,id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []StoredExperiment{}
	for rows.Next() {
		item, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *ExperimentStore) loadDevelopmentSamples(ctx context.Context, dataset StoredDataset) (map[int]map[string][]ExperimentSample, error) {
	rows, err := s.db.Query(ctx, `SELECT sample.fold_index,sample.partition,sample.prediction_run_id,sample.event_cluster,
		sample.signal_available_at,sample.label_available_at,prediction.feature_snapshot::jsonb,outcome.objective_label,outcome.raw_return,outcome.excess_return
		FROM evaluation_dataset_samples sample
		JOIN prediction_runs prediction ON prediction.id=sample.prediction_run_id
		JOIN outcome_records outcome ON outcome.prediction_run_id=sample.prediction_run_id
		WHERE sample.dataset_id=$1 AND sample.fold_index>=0 AND sample.sealed=false AND sample.partition IN ('train','calibration','test')
		ORDER BY sample.fold_index,sample.partition,sample.signal_available_at,sample.prediction_run_id`, dataset.Manifest.ID)
	if err != nil {
		return nil, fmt.Errorf("load development-only experiment samples: %w", err)
	}
	defer rows.Close()
	result := map[int]map[string][]ExperimentSample{}
	for rows.Next() {
		var foldIndex int
		var snapshotBody []byte
		var objectiveLabel string
		var sample ExperimentSample
		if err := rows.Scan(&foldIndex, &sample.Partition, &sample.ID, &sample.EventCluster, &sample.SignalAt, &sample.LabelMatureAt,
			&snapshotBody, &objectiveLabel, &sample.RawReturn, &sample.ExcessReturn); err != nil {
			return nil, err
		}
		snapshot := signals.Snapshot{}
		if err := json.Unmarshal(snapshotBody, &snapshot); err != nil {
			return nil, fmt.Errorf("decode experiment feature snapshot %s: %w", sample.ID, err)
		}
		sample.Values = snapshot.Values
		sample.Label = objectiveLabel == "up" || objectiveLabel == "outperform"
		if result[foldIndex] == nil {
			result[foldIndex] = map[string][]ExperimentSample{"train": {}, "calibration": {}, "test": {}}
		}
		result[foldIndex][sample.Partition] = append(result[foldIndex][sample.Partition], sample)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *ExperimentStore) persist(ctx context.Context, stored StoredExperiment, idempotencyKey string) (bool, error) {
	reportBody, _ := json.Marshal(stored.Experiment)
	groupsBody, _ := json.Marshal(stored.Experiment.FeatureGroups)
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, `INSERT INTO evaluation_experiments(id,contract_version,dataset_id,dataset_manifest_digest,asset_class,market,objective,
		horizon_sessions,feature_groups,preprocessing_policy,report,artifact_digest,fold_count,final_holdout_accessed,created_by,idempotency_key,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,false,$14,$15,$16) ON CONFLICT(idempotency_key) DO NOTHING`,
		stored.Experiment.ID, stored.Experiment.ContractVersion, stored.Experiment.DatasetID, stored.Experiment.DatasetManifestDigest,
		stored.Experiment.AssetClass, stored.Experiment.Market, stored.Experiment.Objective, stored.Experiment.HorizonSessions, groupsBody,
		stored.Experiment.PreprocessingPolicy, reportBody, stored.Experiment.ArtifactDigest, len(stored.Experiment.Folds), stored.CreatedBy, idempotencyKey, stored.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("save development experiment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}
	for _, fold := range stored.Experiment.Folds {
		for _, variant := range fold.Variants {
			if err := saveExperimentVariant(ctx, tx, stored.Experiment.ID, fold.Index, variant); err != nil {
				return false, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func saveExperimentVariant(ctx context.Context, tx pgx.Tx, experimentID string, foldIndex int, variant ExperimentVariant) error {
	features, _ := json.Marshal(variant.FeatureNames)
	metrics, _ := json.Marshal(variant.Metrics)
	model, calibrationArtifact := optionalJSON(variant.Model), optionalJSON(variant.Calibrator)
	if _, err := tx.Exec(ctx, `INSERT INTO evaluation_experiment_variants(experiment_id,fold_index,variant_name,variant_kind,status,reason,
		feature_names,training_count,calibration_count,test_count,model_artifact,calibration_artifact,metrics,artifact_digest)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, experimentID, foldIndex, variant.Name, variant.Kind, variant.Status,
		variant.Reason, features, variant.TrainingCount, variant.CalibrationCount, variant.TestCount, model, calibrationArtifact, metrics, variant.ArtifactDigest); err != nil {
		return err
	}
	for _, prediction := range variant.Predictions {
		if _, err := tx.Exec(ctx, `INSERT INTO evaluation_experiment_predictions(experiment_id,fold_index,variant_name,prediction_run_id,event_cluster,
			signal_available_at,raw_score,probability,objective_label,correct,raw_return,excess_return)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, experimentID, foldIndex, variant.Name, prediction.PredictionRunID,
			prediction.EventCluster, prediction.SignalAt, prediction.RawScore, prediction.Probability, prediction.Label, prediction.Correct,
			prediction.RawReturn, prediction.ExcessReturn); err != nil {
			return err
		}
	}
	return nil
}

func optionalJSON(value any) any {
	if value == nil {
		return nil
	}
	body, _ := json.Marshal(value)
	if string(body) == "null" {
		return nil
	}
	return body
}

func (s *ExperimentStore) getByIdempotencyKey(ctx context.Context, key string) (StoredExperiment, error) {
	return s.scan(s.db.QueryRow(ctx, `SELECT report::jsonb,created_by,created_at FROM evaluation_experiments WHERE idempotency_key=$1`, key))
}

func (s *ExperimentStore) scan(row rowScanner) (StoredExperiment, error) {
	var item StoredExperiment
	var body []byte
	if err := row.Scan(&body, &item.CreatedBy, &item.CreatedAt); err != nil {
		return StoredExperiment{}, err
	}
	if err := json.Unmarshal(body, &item.Experiment); err != nil {
		return StoredExperiment{}, err
	}
	return item, nil
}
