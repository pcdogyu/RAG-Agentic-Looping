package evaluation

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestDatasetStorePersistsReproducibleWalkForwardAndSealedHoldoutAgainstIsolatedPostgres(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "walk_forward_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
	config, _ := pgxpool.ParseConfig(dsn)
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err = pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active)
		VALUES('equity:XNAS:WF','equity','US','WF','Walk Forward Inc.','XNAS','USD','[]','[]','[]',1,true)`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO security_universe_snapshots(id,universe_id,market,provider,criteria_version,criteria,observed_at,available_at,status,asset_count,included_count,excluded_count,delisted_count,failure_detail,source_identity)
		VALUES('wf-snapshot','market:US','US','test','wf-v1','{}',$1,$1,'completed',1,1,0,0,'','{}')`, start.AddDate(0, 0, -2)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO security_universe_memberships(snapshot_id,asset_id,membership_status,effective_at,available_at,reason_codes,source_identity)
		VALUES('wf-snapshot','equity:XNAS:WF','included',$1,$1,'[]','{}')`, start.AddDate(0, 0, -2)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO prediction_models(version,objective,market,horizon_sessions,feature_schema,model_payload,training_cutoff,artifact_digest,status,scope)
		VALUES('wf-model','absolute_up','US',1,'{}','{}',$1,'wf-model','shadow','{"asset_class":"equity","outcome_label_definition_version":"prediction-outcome-label-v1"}')`, start.AddDate(0, 0, -30)); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 180; index++ {
		signal := start.AddDate(0, 0, index)
		id := fmt.Sprintf("wf-prediction-%03d", index)
		feature, _ := json.Marshal(map[string]any{"as_of": signal, "values": map[string]any{"feature": index}})
		if _, err = pool.Exec(ctx, `INSERT INTO prediction_runs(id,asset_id,asset_class,signal_available_at,horizon_sessions,objective,model_version,status,model_status,raw_score,feature_snapshot,exclusion_reason,idempotency_key,created_at)
			VALUES($1,'equity:XNAS:WF','equity',$2,1,'absolute_up','wf-model','uncalibrated','shadow',0.5,$3,'',$1,$2)`, id, signal, feature); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO outcome_records(prediction_run_id,entry_at,exit_at,label_available_at,raw_return,status,data_quality,label_definition_version,objective,horizon_sessions,price_field,time_precision,alpha_definition,absolute_label,relative_label,objective_label,risk_adjustment_status,simulation_status,research_result_only)
			VALUES($1,$2,$3,$4,0.01,'mature','{}','prediction-outcome-label-v1','absolute_up',1,'adjusted_close','daily_close','arithmetic_asset_total_return_minus_benchmark_total_return','up','unavailable','up','not_configured','not_configured',true)`,
			id, signal.AddDate(0, 0, 1), signal.AddDate(0, 0, 2), signal.AddDate(0, 0, 3)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = pool.Exec(ctx, `UPDATE outcome_records SET objective_label='' WHERE prediction_run_id='wf-prediction-040'`); err != nil {
		t.Fatal(err)
	}
	store := NewDatasetStore(pool)
	reservation, created, err := store.CreateHoldoutReservation(ctx, HoldoutReservationInput{AssetClass: "equity", Market: "US", Objective: "absolute_up", HorizonSessions: 1,
		SignalStart: start.AddDate(0, 0, 122), SignalEnd: start.AddDate(0, 0, 152), LabelCutoff: start.AddDate(0, 0, 160), ApprovedBy: "isolated reviewer", IdempotencyKey: "wf-holdout-v1"}, start.AddDate(0, 0, -30))
	if err != nil || !created {
		t.Fatalf("reserve holdout=%#v created=%v err=%v", reservation, created, err)
	}
	input := DatasetBuildInput{HoldoutReservationID: reservation.ID, AvailableAsOf: start.AddDate(0, 0, 170), DevelopmentStart: start,
		DevelopmentEnd: start.AddDate(0, 0, 120), TrainWindowDays: 30, CalibrationWindowDays: 15, TestWindowDays: 15,
		StepDays: 15, EmbargoDays: 2, CreatedBy: "isolated builder", IdempotencyKey: "wf-dataset-v1"}
	dataset, created, err := store.Materialize(ctx, input, start.AddDate(0, 0, 180))
	if err != nil || !created || len(dataset.Manifest.Folds) < 2 || dataset.Manifest.FinalHoldout.IncludedCount != 30 {
		t.Fatalf("dataset=%#v created=%v err=%v", dataset, created, err)
	}
	if dataset.Manifest.ExclusionCount["objective_label_unavailable"] == 0 {
		t.Fatalf("invalid objective label was not disclosed: %#v", dataset.Manifest.ExclusionCount)
	}
	repeated, created, err := store.Materialize(ctx, input, start.AddDate(0, 0, 180))
	if err != nil || created || repeated.Manifest.ID != dataset.Manifest.ID {
		t.Fatalf("dataset materialization is not idempotent: repeated=%#v created=%v err=%v", repeated, created, err)
	}
	var sealed, exposed, folds int
	if err = pool.QueryRow(ctx, `SELECT count(*)::int FROM evaluation_dataset_samples WHERE dataset_id=$1 AND fold_index=-1 AND sealed=true`, dataset.Manifest.ID).Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*)::int FROM evaluation_dataset_samples WHERE dataset_id=$1 AND fold_index=-1 AND sealed=false`, dataset.Manifest.ID).Scan(&exposed); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*)::int FROM evaluation_dataset_folds WHERE dataset_id=$1`, dataset.Manifest.ID).Scan(&folds); err != nil {
		t.Fatal(err)
	}
	if sealed != 30 || exposed != 0 || folds != len(dataset.Manifest.Folds) {
		t.Fatalf("sealed=%d exposed=%d folds=%d manifest_folds=%d", sealed, exposed, folds, len(dataset.Manifest.Folds))
	}
	public, err := store.GetDataset(ctx, dataset.Manifest.ID)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(public)
	if strings.Contains(string(body), "wf-prediction-130") || public.Manifest.FinalHoldout.IncludedCount != 30 {
		t.Fatalf("sealed sample leaked or count disappeared: %s", body)
	}
}
