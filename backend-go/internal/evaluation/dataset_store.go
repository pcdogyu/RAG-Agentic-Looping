package evaluation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type HoldoutReservationInput struct {
	AssetClass      string    `json:"asset_class"`
	Market          string    `json:"market"`
	Objective       string    `json:"objective"`
	HorizonSessions int       `json:"horizon_sessions"`
	SignalStart     time.Time `json:"signal_start"`
	SignalEnd       time.Time `json:"signal_end"`
	LabelCutoff     time.Time `json:"label_cutoff"`
	ApprovedBy      string    `json:"approved_by"`
	ApprovalKind    string    `json:"approval_kind,omitempty"`
	PolicyVersion   string    `json:"policy_version,omitempty"`
	IdempotencyKey  string    `json:"-"`
}

type HoldoutReservation struct {
	ID                     string    `json:"id"`
	ContractVersion        string    `json:"contract_version"`
	LabelDefinitionVersion string    `json:"label_definition_version"`
	AssetClass             string    `json:"asset_class"`
	Market                 string    `json:"market"`
	Objective              string    `json:"objective"`
	HorizonSessions        int       `json:"horizon_sessions"`
	SignalStart            time.Time `json:"signal_start"`
	SignalEnd              time.Time `json:"signal_end"`
	LabelCutoff            time.Time `json:"label_cutoff"`
	UsagePolicy            string    `json:"usage_policy"`
	ApprovedBy             string    `json:"approved_by"`
	ApprovalKind           string    `json:"approval_kind"`
	PolicyVersion          string    `json:"policy_version,omitempty"`
	RequestDigest          string    `json:"request_digest"`
	CreatedAt              time.Time `json:"created_at"`
}

type DatasetBuildInput struct {
	HoldoutReservationID  string    `json:"holdout_reservation_id"`
	AvailableAsOf         time.Time `json:"available_as_of"`
	DevelopmentStart      time.Time `json:"development_start"`
	DevelopmentEnd        time.Time `json:"development_end"`
	TrainWindowDays       int       `json:"train_window_days"`
	CalibrationWindowDays int       `json:"calibration_window_days"`
	TestWindowDays        int       `json:"test_window_days"`
	StepDays              int       `json:"step_days"`
	EmbargoDays           int       `json:"embargo_days"`
	CreatedBy             string    `json:"created_by"`
	IdempotencyKey        string    `json:"-"`
}

type StoredDataset struct {
	Manifest        DatasetManifest `json:"manifest"`
	AssetClass      string          `json:"asset_class"`
	Market          string          `json:"market"`
	Objective       string          `json:"objective"`
	HorizonSessions int             `json:"horizon_sessions"`
	CreatedBy       string          `json:"created_by"`
	CreatedAt       time.Time       `json:"created_at"`
}

type DatasetStore struct{ db *pgxpool.Pool }

func NewDatasetStore(db *pgxpool.Pool) *DatasetStore { return &DatasetStore{db: db} }

func (s *DatasetStore) CreateHoldoutReservation(ctx context.Context, input HoldoutReservationInput, now time.Time) (HoldoutReservation, bool, error) {
	if s.db == nil {
		return HoldoutReservation{}, false, fmt.Errorf("evaluation dataset store is unavailable")
	}
	input.AssetClass = strings.ToLower(strings.TrimSpace(input.AssetClass))
	input.Market = strings.ToUpper(strings.TrimSpace(input.Market))
	input.Objective = strings.ToLower(strings.TrimSpace(input.Objective))
	input.ApprovedBy = strings.TrimSpace(input.ApprovedBy)
	input.ApprovalKind = strings.ToLower(strings.TrimSpace(input.ApprovalKind))
	if input.ApprovalKind == "" {
		input.ApprovalKind = "human"
	}
	input.PolicyVersion = strings.TrimSpace(input.PolicyVersion)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	now = now.UTC()
	if input.AssetClass != "equity" || input.Market == "" || input.ApprovedBy == "" || input.IdempotencyKey == "" {
		return HoldoutReservation{}, false, fmt.Errorf("equity scope, market, approved_by and idempotency key are required")
	}
	if input.ApprovalKind != "human" && input.ApprovalKind != "policy" {
		return HoldoutReservation{}, false, fmt.Errorf("approval_kind must be human or policy")
	}
	if input.ApprovalKind == "policy" && (input.PolicyVersion == "" || !strings.HasPrefix(input.ApprovedBy, "policy:")) {
		return HoldoutReservation{}, false, fmt.Errorf("policy holdout requires policy_version and an explicit policy actor")
	}
	if input.ApprovalKind == "human" && input.PolicyVersion != "" {
		return HoldoutReservation{}, false, fmt.Errorf("human holdout must not claim a policy version")
	}
	if _, err := ResolveHorizonPolicy(input.Objective, input.HorizonSessions); err != nil {
		return HoldoutReservation{}, false, err
	}
	if input.SignalStart.IsZero() || !input.SignalStart.After(now) || !input.SignalEnd.After(input.SignalStart) || !input.LabelCutoff.After(input.SignalEnd) {
		return HoldoutReservation{}, false, fmt.Errorf("final holdout must be reserved before a valid future signal and label window")
	}
	identity := struct {
		Contract, Label, AssetClass, Market, Objective, ApprovedBy, ApprovalKind, PolicyVersion string
		Horizon                                                                                 int
		SignalStart, SignalEnd, LabelCutoff                                                     time.Time
	}{WalkForwardDatasetContractVersion, OutcomeLabelDefinitionVersion, input.AssetClass, input.Market, input.Objective, input.ApprovedBy, input.ApprovalKind, input.PolicyVersion,
		input.HorizonSessions, input.SignalStart.UTC(), input.SignalEnd.UTC(), input.LabelCutoff.UTC()}
	digest := digestValue(identity)
	id := "holdout-" + digest[:32]
	tag, err := s.db.Exec(ctx, `INSERT INTO evaluation_holdout_reservations(
		id,contract_version,label_definition_version,asset_class,market,objective,horizon_sessions,signal_start,signal_end,label_cutoff,
		usage_policy,approved_by,approval_kind,policy_version,request_digest,idempotency_key,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) ON CONFLICT(idempotency_key) DO NOTHING`,
		id, WalkForwardDatasetContractVersion, OutcomeLabelDefinitionVersion, input.AssetClass, input.Market, input.Objective, input.HorizonSessions,
		input.SignalStart.UTC(), input.SignalEnd.UTC(), input.LabelCutoff.UTC(), FinalHoldoutUsagePolicy, input.ApprovedBy, input.ApprovalKind, input.PolicyVersion, digest, input.IdempotencyKey, now)
	if err != nil {
		return HoldoutReservation{}, false, fmt.Errorf("reserve final holdout: %w", err)
	}
	stored, err := s.getHoldoutByIdempotencyKey(ctx, input.IdempotencyKey)
	if err != nil {
		return HoldoutReservation{}, false, err
	}
	if stored.RequestDigest != digest {
		return HoldoutReservation{}, false, fmt.Errorf("idempotency key is already bound to a different final holdout")
	}
	return stored, tag.RowsAffected() == 1, nil
}

func (s *DatasetStore) GetHoldout(ctx context.Context, id string) (HoldoutReservation, error) {
	return s.scanHoldout(s.db.QueryRow(ctx, `SELECT id,contract_version,label_definition_version,asset_class,market,objective,horizon_sessions,
		signal_start,signal_end,label_cutoff,usage_policy,approved_by,approval_kind,policy_version,request_digest,created_at FROM evaluation_holdout_reservations WHERE id=$1`, strings.TrimSpace(id)))
}

func (s *DatasetStore) ListHoldouts(ctx context.Context, limit int) ([]HoldoutReservation, error) {
	if s.db == nil || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid final holdout query")
	}
	rows, err := s.db.Query(ctx, `SELECT id,contract_version,label_definition_version,asset_class,market,objective,horizon_sessions,
		signal_start,signal_end,label_cutoff,usage_policy,approved_by,approval_kind,policy_version,request_digest,created_at
		FROM evaluation_holdout_reservations ORDER BY created_at DESC,id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []HoldoutReservation{}
	for rows.Next() {
		item, err := s.scanHoldout(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatasetStore) Materialize(ctx context.Context, input DatasetBuildInput, now time.Time) (StoredDataset, bool, error) {
	if s.db == nil {
		return StoredDataset{}, false, fmt.Errorf("evaluation dataset store is unavailable")
	}
	input.HoldoutReservationID = strings.TrimSpace(input.HoldoutReservationID)
	input.CreatedBy = strings.TrimSpace(input.CreatedBy)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.HoldoutReservationID == "" || input.CreatedBy == "" || input.IdempotencyKey == "" {
		return StoredDataset{}, false, fmt.Errorf("holdout_reservation_id, created_by and idempotency key are required")
	}
	reservation, err := s.GetHoldout(ctx, input.HoldoutReservationID)
	if err != nil {
		return StoredDataset{}, false, fmt.Errorf("load final holdout reservation: %w", err)
	}
	availableAsOf := input.AvailableAsOf.UTC()
	if input.AvailableAsOf.IsZero() {
		availableAsOf = now.UTC()
	}
	if availableAsOf.After(now.UTC()) || availableAsOf.Before(reservation.LabelCutoff) {
		return StoredDataset{}, false, fmt.Errorf("available_as_of must be observed and reach the reserved label cutoff")
	}
	config := WalkForwardConfig{SourceAvailableAsOf: availableAsOf, DevelopmentStart: input.DevelopmentStart.UTC(), DevelopmentEnd: input.DevelopmentEnd.UTC(),
		TrainWindowDays: input.TrainWindowDays, CalibrationWindowDays: input.CalibrationWindowDays, TestWindowDays: input.TestWindowDays,
		StepDays: input.StepDays, EmbargoDays: input.EmbargoDays, FinalHoldoutStart: reservation.SignalStart, FinalHoldoutEnd: reservation.SignalEnd,
		FinalLabelCutoff: reservation.LabelCutoff, HoldoutReservationID: reservation.ID, HoldoutReservedAt: reservation.CreatedAt}
	if err := validateWalkForwardConfig(config); err != nil {
		return StoredDataset{}, false, err
	}
	records, err := s.loadRecords(ctx, reservation, config)
	if err != nil {
		return StoredDataset{}, false, err
	}
	manifest, err := BuildWalkForwardDataset(records, config)
	if err != nil {
		return StoredDataset{}, false, err
	}
	stored := StoredDataset{Manifest: manifest, AssetClass: reservation.AssetClass, Market: reservation.Market, Objective: reservation.Objective,
		HorizonSessions: reservation.HorizonSessions, CreatedBy: input.CreatedBy, CreatedAt: now.UTC()}
	created, err := s.persistDataset(ctx, stored, input.IdempotencyKey)
	if err != nil {
		return StoredDataset{}, false, err
	}
	if !created {
		existing, getErr := s.getDatasetByIdempotencyKey(ctx, input.IdempotencyKey)
		if getErr != nil {
			return StoredDataset{}, false, getErr
		}
		if existing.Manifest.ManifestDigest != manifest.ManifestDigest {
			return StoredDataset{}, false, fmt.Errorf("idempotency key is already bound to a different evaluation dataset")
		}
		return existing, false, nil
	}
	return stored, true, nil
}

func (s *DatasetStore) GetDataset(ctx context.Context, id string) (StoredDataset, error) {
	return s.scanDataset(s.db.QueryRow(ctx, `SELECT manifest::jsonb,asset_class,market,objective,horizon_sessions,created_by,created_at
		FROM evaluation_dataset_versions WHERE id=$1`, strings.TrimSpace(id)))
}

func (s *DatasetStore) ListDatasets(ctx context.Context, limit int) ([]StoredDataset, error) {
	if s.db == nil || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid evaluation dataset query")
	}
	rows, err := s.db.Query(ctx, `SELECT manifest::jsonb,asset_class,market,objective,horizon_sessions,created_by,created_at
		FROM evaluation_dataset_versions ORDER BY created_at DESC,id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []StoredDataset{}
	for rows.Next() {
		item, err := s.scanDataset(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *DatasetStore) loadRecords(ctx context.Context, reservation HoldoutReservation, config WalkForwardConfig) ([]Record, error) {
	rows, err := s.db.Query(ctx, `SELECT p.id,coalesce(p.event_id::text,''),p.signal_available_at,p.created_at,p.feature_snapshot::jsonb,
		coalesce(o.status,''),coalesce(o.exclusion_reason,''),o.entry_at,o.exit_at,o.label_available_at,coalesce(o.label_definition_version,''),
		coalesce(o.objective,''),coalesce(o.horizon_sessions,0),coalesce(o.objective_label,''),o.raw_return,o.excess_return,
		coalesce(m.scope->>'outcome_label_definition_version',''),coalesce(u.membership_status,''),
		coalesce((SELECT jsonb_agg(DISTINCT sl.syndication_group) FROM news_events e
			CROSS JOIN LATERAL jsonb_array_elements_text(coalesce(e.payload::jsonb->'news_item_ids','[]'::jsonb)) member(news_id)
			JOIN source_lineage sl ON sl.news_item_id::text=member.news_id
			WHERE e.id=p.event_id AND nullif(sl.syndication_group,'') IS NOT NULL),'[]'::jsonb)
		FROM prediction_runs p JOIN prediction_models m ON m.version=p.model_version JOIN assets a ON a.id=p.asset_id
		LEFT JOIN outcome_records o ON o.prediction_run_id=p.id AND o.label_available_at<=$9
		LEFT JOIN LATERAL (SELECT membership.membership_status FROM security_universe_memberships membership
			JOIN security_universe_snapshots snapshot ON snapshot.id=membership.snapshot_id
			WHERE membership.asset_id=p.asset_id AND snapshot.universe_id='market:'||a.market
				AND membership.effective_at<=p.signal_available_at AND membership.available_at<=p.signal_available_at
			ORDER BY membership.effective_at DESC,membership.available_at DESC,membership.snapshot_id DESC LIMIT 1) u ON true
		WHERE p.asset_class=$1 AND a.market=$2 AND p.objective=$3 AND p.horizon_sessions=$4
			AND ((p.signal_available_at>=$5 AND p.signal_available_at<$6) OR (p.signal_available_at>=$7 AND p.signal_available_at<$8))
		ORDER BY p.signal_available_at,p.id`, reservation.AssetClass, reservation.Market, reservation.Objective, reservation.HorizonSessions,
		config.DevelopmentStart, config.DevelopmentEnd, config.FinalHoldoutStart, config.FinalHoldoutEnd, config.SourceAvailableAsOf)
	if err != nil {
		return nil, fmt.Errorf("load evaluation dataset candidates: %w", err)
	}
	defer rows.Close()
	result := []Record{}
	for rows.Next() {
		var record Record
		var eventID, outcomeStatus, outcomeReason, labelVersion, outcomeObjective, objectiveLabel, modelLabelVersion, membershipStatus string
		var outcomeHorizon int
		var rawReturn, excessReturn *float64
		var featureBody, clusterBody []byte
		var entryAt, exitAt, labelAvailableAt *time.Time
		if err := rows.Scan(&record.ID, &eventID, &record.SignalAt, &record.PredictionAvailableAt, &featureBody, &outcomeStatus, &outcomeReason,
			&entryAt, &exitAt, &labelAvailableAt, &labelVersion, &outcomeObjective, &outcomeHorizon, &objectiveLabel, &rawReturn, &excessReturn,
			&modelLabelVersion, &membershipStatus, &clusterBody); err != nil {
			return nil, err
		}
		feature := struct {
			AsOf time.Time `json:"as_of"`
		}{}
		_ = json.Unmarshal(featureBody, &feature)
		record.FeatureCutoffAt = feature.AsOf
		record.Status = outcomeStatus
		if entryAt != nil {
			record.LabelWindowStart = entryAt.UTC()
		}
		if exitAt != nil {
			record.LabelWindowEnd = exitAt.UTC()
		}
		if labelAvailableAt != nil {
			record.LabelMatureAt = labelAvailableAt.UTC()
		}
		if eventID != "" {
			record.EventCluster = "event:" + eventID
			record.ClusterKeys = append(record.ClusterKeys, record.EventCluster)
		}
		var groups []string
		_ = json.Unmarshal(clusterBody, &groups)
		for _, group := range groups {
			if group = strings.TrimSpace(group); group != "" {
				record.ClusterKeys = append(record.ClusterKeys, "syndication:"+group)
			}
		}
		switch {
		case record.PredictionAvailableAt.After(config.SourceAvailableAsOf):
			record.ExclusionReason = "prediction_not_available_as_of_dataset"
		case modelLabelVersion != OutcomeLabelDefinitionVersion:
			record.ExclusionReason = "label_definition_not_pre_registered"
		case outcomeStatus == "":
			record.ExclusionReason = "outcome_pending_as_of_dataset"
		case labelVersion != OutcomeLabelDefinitionVersion:
			record.ExclusionReason = "outcome_label_definition_mismatch"
		case outcomeObjective != reservation.Objective || outcomeHorizon != reservation.HorizonSessions:
			record.ExclusionReason = "outcome_scope_mismatch"
		case !validObjectiveLabel(reservation.Objective, objectiveLabel):
			record.ExclusionReason = "objective_label_unavailable"
		case reservation.Objective == "absolute_up" && rawReturn == nil:
			record.ExclusionReason = "absolute_return_unavailable"
		case reservation.Objective == "excess_up" && excessReturn == nil:
			record.ExclusionReason = "excess_return_unavailable"
		case membershipStatus == "":
			record.ExclusionReason = "security_universe_membership_unavailable"
		case membershipStatus != "included":
			record.ExclusionReason = "security_universe_" + membershipStatus
		case outcomeStatus != "mature" && outcomeReason != "":
			record.ExclusionReason = "outcome_" + outcomeStatus + ":" + outcomeReason
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func validObjectiveLabel(objective, label string) bool {
	label = strings.ToLower(strings.TrimSpace(label))
	if objective == "absolute_up" {
		return label == "up" || label == "down" || label == "neutral"
	}
	return label == "outperform" || label == "underperform" || label == "neutral"
}

func (s *DatasetStore) persistDataset(ctx context.Context, stored StoredDataset, idempotencyKey string) (bool, error) {
	manifestBody, err := json.Marshal(stored.Manifest)
	if err != nil {
		return false, err
	}
	configBody, _ := json.Marshal(stored.Manifest.Config)
	exclusionBody, _ := json.Marshal(stored.Manifest.ExclusionCount)
	developmentAssignments := 0
	for _, fold := range stored.Manifest.Folds {
		developmentAssignments += len(fold.Members)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `INSERT INTO evaluation_dataset_versions(id,contract_version,label_definition_version,holdout_reservation_id,
		asset_class,market,objective,horizon_sessions,config,manifest,manifest_digest,candidate_count,fold_count,development_assignment_count,
		final_holdout_included_count,final_holdout_excluded_count,exclusion_count,created_by,idempotency_key,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) ON CONFLICT(idempotency_key) DO NOTHING`,
		stored.Manifest.ID, stored.Manifest.ContractVersion, stored.Manifest.LabelDefinitionVersion, stored.Manifest.Config.HoldoutReservationID,
		stored.AssetClass, stored.Market, stored.Objective, stored.HorizonSessions, configBody, manifestBody, stored.Manifest.ManifestDigest,
		stored.Manifest.CandidateCount, len(stored.Manifest.Folds), developmentAssignments, stored.Manifest.FinalHoldout.IncludedCount,
		stored.Manifest.FinalHoldout.ExcludedCount, exclusionBody, stored.CreatedBy, idempotencyKey, stored.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("save evaluation dataset: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	for _, fold := range stored.Manifest.Folds {
		if _, err := tx.Exec(ctx, `INSERT INTO evaluation_dataset_folds(dataset_id,fold_index,train_start,train_cutoff,calibration_start,
			calibration_cutoff,test_start,test_cutoff,train_count,calibration_count,test_count,excluded_count,manifest_digest)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, stored.Manifest.ID, fold.Index, fold.TrainStart, fold.TrainCutoff,
			fold.CalibrationStart, fold.CalibrationCutoff, fold.TestStart, fold.TestCutoff, len(fold.Train), len(fold.Calibration), len(fold.Test), len(fold.Excluded), fold.ManifestDigest); err != nil {
			return false, err
		}
		for _, member := range append(append([]SplitMember{}, fold.Members...), fold.Excluded...) {
			if err := saveDatasetMember(ctx, tx, stored.Manifest.ID, fold.Index, member, false); err != nil {
				return false, err
			}
		}
	}
	for _, member := range append(append([]SplitMember{}, stored.Manifest.finalMembers...), stored.Manifest.finalExcluded...) {
		if err := saveDatasetMember(ctx, tx, stored.Manifest.ID, -1, member, true); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func saveDatasetMember(ctx context.Context, tx pgx.Tx, datasetID string, foldIndex int, member SplitMember, sealed bool) error {
	_, err := tx.Exec(ctx, `INSERT INTO evaluation_dataset_samples(dataset_id,fold_index,prediction_run_id,partition,intended_partition,event_cluster,
		signal_available_at,prediction_available_at,feature_cutoff_at,label_window_start,label_window_end,label_available_at,exclusion_reason,sealed,sample_digest)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, datasetID, foldIndex, member.PredictionRunID, member.Partition,
		member.IntendedPartition, member.EventCluster, member.SignalAt, member.PredictionAvailableAt, member.FeatureCutoffAt, member.LabelWindowStart,
		member.LabelWindowEnd, member.LabelMatureAt, member.ExclusionReason, sealed, digestValue(member))
	return err
}

func (s *DatasetStore) getHoldoutByIdempotencyKey(ctx context.Context, key string) (HoldoutReservation, error) {
	return s.scanHoldout(s.db.QueryRow(ctx, `SELECT id,contract_version,label_definition_version,asset_class,market,objective,horizon_sessions,
		signal_start,signal_end,label_cutoff,usage_policy,approved_by,approval_kind,policy_version,request_digest,created_at FROM evaluation_holdout_reservations WHERE idempotency_key=$1`, key))
}

type rowScanner interface{ Scan(...any) error }

func (s *DatasetStore) scanHoldout(row rowScanner) (HoldoutReservation, error) {
	var item HoldoutReservation
	err := row.Scan(&item.ID, &item.ContractVersion, &item.LabelDefinitionVersion, &item.AssetClass, &item.Market, &item.Objective,
		&item.HorizonSessions, &item.SignalStart, &item.SignalEnd, &item.LabelCutoff, &item.UsagePolicy, &item.ApprovedBy, &item.ApprovalKind, &item.PolicyVersion, &item.RequestDigest, &item.CreatedAt)
	return item, err
}

func (s *DatasetStore) getDatasetByIdempotencyKey(ctx context.Context, key string) (StoredDataset, error) {
	return s.scanDataset(s.db.QueryRow(ctx, `SELECT manifest::jsonb,asset_class,market,objective,horizon_sessions,created_by,created_at
		FROM evaluation_dataset_versions WHERE idempotency_key=$1`, key))
}

func (s *DatasetStore) scanDataset(row rowScanner) (StoredDataset, error) {
	var item StoredDataset
	var body []byte
	if err := row.Scan(&body, &item.AssetClass, &item.Market, &item.Objective, &item.HorizonSessions, &item.CreatedBy, &item.CreatedAt); err != nil {
		return StoredDataset{}, err
	}
	if err := json.Unmarshal(body, &item.Manifest); err != nil {
		return StoredDataset{}, err
	}
	return item, nil
}
