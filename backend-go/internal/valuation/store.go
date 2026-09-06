package valuation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/forecast"
)

// Submission deliberately references an immutable forecast version. Net debt
// is separately source-linked because it is an enterprise-to-equity bridge,
// not a result of the FCFF calculation.
type Submission struct {
	AssetID           string             `json:"asset_id"`
	AsOf              time.Time          `json:"as_of"`
	ForecastVersionID string             `json:"forecast_version_id"`
	NetDebtSnapshotID string             `json:"net_debt_snapshot_id,omitempty"`
	DCFScenarios      []DCFScenario      `json:"dcf_scenarios"`
	MultipleScenarios []MultipleScenario `json:"multiple_scenarios"`
	SensitivityWACC   []float64          `json:"sensitivity_wacc,omitempty"`
	SensitivityGrowth []float64          `json:"sensitivity_terminal_growth,omitempty"`
}

type Run struct {
	ID                string            `json:"id"`
	AssetID           string            `json:"asset_id"`
	ForecastVersionID string            `json:"forecast_version_id"`
	ModelVersion      string            `json:"model_version"`
	Status            string            `json:"status"`
	AsOf              time.Time         `json:"as_of"`
	InputSnapshot     map[string]any    `json:"input_snapshot"`
	Scenarios         []ScenarioResult  `json:"scenarios"`
	Sensitivity       []SensitivityCell `json:"sensitivity"`
	Result            Result            `json:"result"`
	CreatedAt         time.Time         `json:"created_at"`
}

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

func (s *Store) Create(ctx context.Context, submission Submission) (Run, bool, error) {
	if s.db == nil {
		return Run{}, false, fmt.Errorf("valuation store is unavailable")
	}
	submission.AssetID, submission.ForecastVersionID = strings.TrimSpace(submission.AssetID), strings.TrimSpace(submission.ForecastVersionID)
	if submission.AssetID == "" || submission.ForecastVersionID == "" || submission.AsOf.IsZero() {
		return Run{}, false, fmt.Errorf("valuation asset_id, forecast_version_id and as_of are required")
	}
	submission.AsOf = submission.AsOf.UTC()
	facts, err := s.forecastFacts(ctx, submission)
	if err != nil {
		return Run{}, false, err
	}
	if len(submission.DCFScenarios) > 0 {
		if strings.TrimSpace(submission.NetDebtSnapshotID) == "" {
			return Run{}, false, fmt.Errorf("net_debt_snapshot_id is required for DCF")
		}
		if err := s.validateSnapshot(ctx, submission.AssetID, submission.NetDebtSnapshotID, submission.AsOf); err != nil {
			return Run{}, false, err
		}
	}
	run, err := buildRun(submission, facts)
	if err != nil {
		return Run{}, false, err
	}
	var existing Run
	if err := s.db.QueryRow(ctx, `SELECT id,asset_id,forecast_version_id,model_version,status,as_of,input_snapshot::jsonb,scenarios::jsonb,sensitivity::jsonb,result::jsonb,created_at FROM valuation_runs WHERE id=$1`, run.ID).Scan(&existing.ID, &existing.AssetID, &existing.ForecastVersionID, &existing.ModelVersion, &existing.Status, &existing.AsOf, jsonValue(&existing.InputSnapshot), jsonValue(&existing.Scenarios), jsonValue(&existing.Sensitivity), jsonValue(&existing.Result), &existing.CreatedAt); err == nil {
		return existing, false, nil
	} else if err != pgx.ErrNoRows {
		return Run{}, false, fmt.Errorf("find valuation run: %w", err)
	}
	input, _ := json.Marshal(run.InputSnapshot)
	scenarios, _ := json.Marshal(run.Scenarios)
	sensitivity, _ := json.Marshal(run.Sensitivity)
	result, _ := json.Marshal(run.Result)
	if _, err := s.db.Exec(ctx, `INSERT INTO valuation_runs(id,asset_id,forecast_version_id,model_version,status,as_of,input_snapshot,scenarios,sensitivity,result) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, run.ID, run.AssetID, run.ForecastVersionID, run.ModelVersion, run.Status, run.AsOf, input, scenarios, sensitivity, result); err != nil {
		return Run{}, false, fmt.Errorf("insert valuation run: %w", err)
	}
	return run, true, nil
}

func (s *Store) List(ctx context.Context, assetID string, cutoff time.Time, limit int) ([]Run, error) {
	if assetID == "" || cutoff.IsZero() || limit < 1 || limit > 100 {
		return nil, fmt.Errorf("invalid valuation query")
	}
	rows, err := s.db.Query(ctx, `SELECT id,asset_id,forecast_version_id,model_version,status,as_of,input_snapshot::jsonb,scenarios::jsonb,sensitivity::jsonb,result::jsonb,created_at FROM valuation_runs WHERE asset_id=$1 AND as_of <= $2 ORDER BY as_of DESC,created_at DESC,id DESC LIMIT $3`, assetID, cutoff.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list valuation runs: %w", err)
	}
	defer rows.Close()
	items := []Run{}
	for rows.Next() {
		var item Run
		if err := rows.Scan(&item.ID, &item.AssetID, &item.ForecastVersionID, &item.ModelVersion, &item.Status, &item.AsOf, jsonValue(&item.InputSnapshot), jsonValue(&item.Scenarios), jsonValue(&item.Sensitivity), jsonValue(&item.Result), &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan valuation run: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) forecastFacts(ctx context.Context, submission Submission) (ForecastFacts, error) {
	var projection []byte
	if err := s.db.QueryRow(ctx, `SELECT projection::jsonb FROM forecast_versions WHERE id=$1 AND asset_id=$2 AND status='available' AND as_of <= $3`, submission.ForecastVersionID, submission.AssetID, submission.AsOf).Scan(&projection); err != nil {
		if err == pgx.ErrNoRows {
			return ForecastFacts{}, fmt.Errorf("forecast version is absent, unavailable, belongs to another asset, or is after valuation as_of")
		}
		return ForecastFacts{}, fmt.Errorf("read forecast version: %w", err)
	}
	var value forecast.Result
	if err := json.Unmarshal(projection, &value); err != nil {
		return ForecastFacts{}, fmt.Errorf("decode forecast projection: %w", err)
	}
	fcff, okFCFF := value.Projected["free_cash_flow"].(float64)
	netIncome, okNetIncome := value.Projected["net_income"].(float64)
	shares, okShares := value.Projected["diluted_shares"].(float64)
	if !okFCFF || !okNetIncome || !okShares {
		return ForecastFacts{}, fmt.Errorf("forecast version is missing required projected financial facts")
	}
	return ForecastFacts{Currency: value.Currency, Unit: value.Unit, FCFF: fcff, NetIncome: netIncome, DilutedShares: shares}, nil
}

func (s *Store) validateSnapshot(ctx context.Context, assetID, snapshotID string, cutoff time.Time) error {
	var exists bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM fundamental_snapshots WHERE id=$1 AND asset_id=$2 AND available_at <= $3)`, strings.TrimSpace(snapshotID), assetID, cutoff).Scan(&exists); err != nil {
		return fmt.Errorf("validate net debt snapshot: %w", err)
	}
	if !exists {
		return fmt.Errorf("net_debt_snapshot_id is absent, belongs to another asset, or was unavailable at as_of")
	}
	return nil
}

func buildRun(submission Submission, facts ForecastFacts) (Run, error) {
	result := Calculate(facts, submission.DCFScenarios, submission.MultipleScenarios)
	if len(submission.DCFScenarios) > 0 && len(submission.SensitivityWACC) > 0 && len(submission.SensitivityGrowth) > 0 {
		result.Sensitivity = Sensitivity(facts, submission.DCFScenarios[0], submission.SensitivityWACC, submission.SensitivityGrowth)
	}
	input := map[string]any{"contract_version": "valuation-input-v1", "forecast_version_id": submission.ForecastVersionID, "forecast_facts": facts, "net_debt_snapshot_id": strings.TrimSpace(submission.NetDebtSnapshotID), "dcf_scenarios": submission.DCFScenarios, "multiple_scenarios": submission.MultipleScenarios}
	identity, err := json.Marshal(struct {
		AssetID string         `json:"asset_id"`
		AsOf    time.Time      `json:"as_of"`
		Input   map[string]any `json:"input"`
	}{submission.AssetID, submission.AsOf, input})
	if err != nil {
		return Run{}, fmt.Errorf("marshal valuation identity: %w", err)
	}
	return Run{ID: hash("valuation", string(identity)), AssetID: submission.AssetID, ForecastVersionID: submission.ForecastVersionID, ModelVersion: ModelVersion, Status: result.Status, AsOf: submission.AsOf, InputSnapshot: input, Scenarios: result.Scenarios, Sensitivity: result.Sensitivity, Result: result}, nil
}

func hash(prefix, value string) string {
	sum := sha256.Sum256([]byte(prefix + "|" + value))
	return prefix + "-" + hex.EncodeToString(sum[:])[:48]
}

type scanJSON func(any) error

func (fn scanJSON) Scan(value any) error { return fn(value) }

func jsonValue(target any) scanJSON {
	return func(value any) error {
		raw, ok := value.([]byte)
		if !ok {
			return fmt.Errorf("valuation JSON is not bytes")
		}
		switch typed := target.(type) {
		case *map[string]any:
			return json.Unmarshal(raw, typed)
		case *[]ScenarioResult:
			return json.Unmarshal(raw, typed)
		case *[]SensitivityCell:
			return json.Unmarshal(raw, typed)
		case *Result:
			return json.Unmarshal(raw, typed)
		default:
			return fmt.Errorf("unsupported valuation JSON target")
		}
	}
}
