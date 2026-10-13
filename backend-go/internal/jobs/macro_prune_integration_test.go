package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

// TestPruneInvalidMacroTargetsAgainstPostgres proves the maintenance sweep removes
// stored institution and news-clause targets, keeps macro and asset-backed targets,
// previews without writing, and pages itself to completion.
func TestPruneInvalidMacroTargetsAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// Trailing ids keep these rows at the end of the id sweep so paging reaches them.
	macroRunID, assetRunID := "zz-macro-target-prune-a", "zz-macro-target-prune-b"
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM event_research_runs WHERE id=ANY($1)`, []string{macroRunID, assetRunID})
	})
	payloads := map[string]string{
		macroRunID: `{"as_of":"2026-01-01T00:00:00Z","report":{"summary":"macro","impacts":[` +
			`{"target_name":"白银","target_type":"macro_economy","direction":"up","strength":3},` +
			`{"target_name":"LME","target_type":"industry","direction":"up","strength":3},` +
			`{"target_name":"美联储主席鲍威尔","target_type":"macro_economy","direction":"down","strength":2}]}}`,
		assetRunID: `{"as_of":"2026-01-01T00:00:00Z","report":{"summary":"asset","impacts":[` +
			`{"target_name":"某未映射公司","target_type":"tradable_asset","asset_id":"equity:XNAS:PRUNETEST","direction":"up","strength":4},` +
			`{"target_name":"特朗普关税","target_type":"industry","direction":"down","strength":5}]}}`,
	}
	for id, body := range payloads {
		if _, err := pool.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at)
			VALUES($1,$1,'completed',$2::json,now(),now())
			ON CONFLICT (id) DO UPDATE SET status='completed',payload=EXCLUDED.payload,updated_at=now()`, id, body); err != nil {
			t.Fatal(err)
		}
	}
	runtime := &maintenanceRuntime{db: pool}

	preview, err := runtime.pruneInvalidMacroTargets(ctx, Job{Payload: mustEnvelopePayload(t, map[string]any{"dry_run": true, "batch_size": 200000})})
	if err != nil {
		t.Fatalf("preview sweep: %v", err)
	}
	if int(numberValue(objectValue(preview)["impacts_removed"])) < 3 {
		t.Fatalf("preview reported no junk targets: %#v", preview)
	}
	if labels := anySlice(objectValue(preview)["removed_labels"]); len(labels) == 0 {
		t.Fatalf("preview listed no removed labels: %#v", preview)
	}
	if len(anySlice(objectValue(objectValue(mustStoredRun(t, ctx, pool, macroRunID))["report"])["impacts"])) != 3 {
		t.Fatalf("preview mutated stored impacts")
	}

	afterID := ""
	removed := 0
	for page := 0; page < 25; page++ {
		kwargs := map[string]any{"dry_run": false, "batch_size": 200000}
		if afterID != "" {
			kwargs["after_id"] = afterID
		}
		result, runErr := runtime.pruneInvalidMacroTargets(ctx, Job{Payload: mustEnvelopePayload(t, kwargs)})
		if runErr != nil {
			var continuation *continuationError
			if !errors.As(runErr, &continuation) {
				t.Fatalf("prune sweep failed: %v", runErr)
			}
			removed += int(numberValue(objectValue(continuation.Progress)["impacts_removed"]))
			afterID = stringValue(objectValue(continuation.Progress)["after_id"])
			if afterID == "" {
				t.Fatalf("continuation carried no cursor: %#v", continuation.Progress)
			}
			continue
		}
		removed += int(numberValue(objectValue(result)["impacts_removed"]))
		if !boolValue(objectValue(result)["complete"]) {
			t.Fatalf("prune sweep reported an incomplete final page: %#v", result)
		}
		break
	}
	if removed < 3 {
		t.Fatalf("sweep removed %d impacts, want at least 3", removed)
	}

	assertStoredTargets(t, ctx, pool, macroRunID, []string{"白银:macro_economy"})
	assertStoredTargets(t, ctx, pool, assetRunID, []string{"某未映射公司:tradable_asset"})
}

func mustEnvelopePayload(t *testing.T, kwargs map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(taskEnvelope{Args: []any{}, Kwargs: kwargs})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func mustStoredRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) map[string]any {
	t.Helper()
	var body []byte
	if err := pool.QueryRow(ctx, `SELECT payload::jsonb FROM event_research_runs WHERE id=$1`, id).Scan(&body); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func assertStoredTargets(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, want []string) {
	t.Helper()
	report := objectValue(mustStoredRun(t, ctx, pool, id)["report"])
	got := make([]string, 0)
	for _, raw := range anySlice(report["impacts"]) {
		impact := objectValue(raw)
		if impact == nil {
			t.Fatalf("stored impact is not an object: %#v", raw)
		}
		got = append(got, stringValue(impact["target_name"])+":"+stringValue(impact["target_type"]))
	}
	if len(got) != len(want) {
		t.Fatalf("stored targets = %#v want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("stored targets = %#v want %#v", got, want)
		}
	}
}
