package forecast

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Submission is an explicit, source-linked financial scenario. It is not an
// LLM output contract: callers must attach the point-in-time fundamental
// snapshots used as facts before a version can be stored.
type Submission struct {
	AssetID                string       `json:"asset_id"`
	AsOf                   time.Time    `json:"as_of"`
	ParentVersionID        string       `json:"parent_version_id,omitempty"`
	Inputs                 Inputs       `json:"inputs"`
	FundamentalSnapshotIDs []string     `json:"fundamental_snapshot_ids"`
	Assumptions            []Assumption `json:"assumptions"`
}

type Version struct {
	ID              string         `json:"id"`
	AssetID         string         `json:"asset_id"`
	ParentVersionID string         `json:"parent_version_id,omitempty"`
	ModelVersion    string         `json:"model_version"`
	Status          string         `json:"status"`
	AsOf            time.Time      `json:"as_of"`
	InputSnapshot   map[string]any `json:"input_snapshot"`
	Assumptions     []Assumption   `json:"assumptions"`
	Projection      Result         `json:"projection"`
	CreatedAt       time.Time      `json:"created_at"`
}

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

// Create stores one deterministic projection version. An identical submission
// returns the existing version. A later version cannot silently reuse the same
// event/field/period link, which prevents retries from counting one event twice.
func (s *Store) Create(ctx context.Context, submission Submission) (Version, bool, error) {
	version, err := buildVersion(submission)
	if err != nil {
		return Version{}, false, err
	}
	if s.db == nil {
		return Version{}, false, fmt.Errorf("forecast store is unavailable")
	}
	if err := s.validateSourceSnapshots(ctx, version); err != nil {
		return Version{}, false, err
	}
	var existing Version
	if err := s.db.QueryRow(ctx, `SELECT id,asset_id,parent_version_id,model_version,status,as_of,input_snapshot::jsonb,assumptions::jsonb,projection::jsonb,created_at FROM forecast_versions WHERE id=$1`, version.ID).Scan(
		&existing.ID, &existing.AssetID, &existing.ParentVersionID, &existing.ModelVersion, &existing.Status, &existing.AsOf, jsonScan(&existing.InputSnapshot), jsonScan(&existing.Assumptions), jsonScan(&existing.Projection), &existing.CreatedAt); err == nil {
		return existing, false, nil
	} else if err != pgx.ErrNoRows {
		return Version{}, false, fmt.Errorf("find forecast version: %w", err)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Version{}, false, fmt.Errorf("begin forecast version: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, link := range versionLinks(version) {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM event_assumption_links WHERE idempotency_key=$1)`, link.idempotencyKey).Scan(&exists); err != nil {
			return Version{}, false, fmt.Errorf("check event assumption idempotency: %w", err)
		}
		if exists {
			return Version{}, false, fmt.Errorf("event assumption already linked for event_id=%s field=%s fiscal_period_end=%s", link.eventID, link.field, link.period.Format("2006-01-02"))
		}
	}
	input, err := json.Marshal(version.InputSnapshot)
	if err != nil {
		return Version{}, false, fmt.Errorf("marshal forecast input snapshot: %w", err)
	}
	assumptions, err := json.Marshal(version.Assumptions)
	if err != nil {
		return Version{}, false, fmt.Errorf("marshal forecast assumptions: %w", err)
	}
	projection, err := json.Marshal(version.Projection)
	if err != nil {
		return Version{}, false, fmt.Errorf("marshal forecast projection: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO forecast_versions(id,asset_id,parent_version_id,model_version,status,as_of,input_snapshot,assumptions,projection)
        VALUES($1,$2,NULLIF($3,''),$4,$5,$6,$7,$8,$9)`, version.ID, version.AssetID, version.ParentVersionID, version.ModelVersion, version.Status, version.AsOf, input, assumptions, projection); err != nil {
		return Version{}, false, fmt.Errorf("insert forecast version: %w", err)
	}
	for _, link := range versionLinks(version) {
		if _, err := tx.Exec(ctx, `INSERT INTO event_assumption_links(id,forecast_version_id,event_id,field,fiscal_period_end,old_value,new_value,delta_value,evidence_ids,condition,approval_status,idempotency_key)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'approved',$11)`, link.id, version.ID, link.eventID, link.field, link.period, link.oldValue, link.newValue, link.delta, link.evidence, link.condition, link.idempotencyKey); err != nil {
			return Version{}, false, fmt.Errorf("insert event assumption link: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Version{}, false, fmt.Errorf("commit forecast version: %w", err)
	}
	return version, true, nil
}

func (s *Store) List(ctx context.Context, assetID string, asOf time.Time, limit int) ([]Version, error) {
	if assetID == "" || asOf.IsZero() || limit < 1 || limit > 100 {
		return nil, fmt.Errorf("invalid forecast version query")
	}
	rows, err := s.db.Query(ctx, `SELECT id,asset_id,parent_version_id,model_version,status,as_of,input_snapshot::jsonb,assumptions::jsonb,projection::jsonb,created_at
        FROM forecast_versions WHERE asset_id=$1 AND as_of <= $2 ORDER BY as_of DESC,created_at DESC,id DESC LIMIT $3`, assetID, asOf.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list forecast versions: %w", err)
	}
	defer rows.Close()
	items := []Version{}
	for rows.Next() {
		var value Version
		if err := rows.Scan(&value.ID, &value.AssetID, &value.ParentVersionID, &value.ModelVersion, &value.Status, &value.AsOf, jsonScan(&value.InputSnapshot), jsonScan(&value.Assumptions), jsonScan(&value.Projection), &value.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan forecast version: %w", err)
		}
		items = append(items, value)
	}
	return items, rows.Err()
}

func buildVersion(submission Submission) (Version, error) {
	submission.AssetID = strings.TrimSpace(submission.AssetID)
	if submission.AssetID == "" || submission.AsOf.IsZero() {
		return Version{}, fmt.Errorf("forecast asset_id and as_of are required")
	}
	submission.AsOf = submission.AsOf.UTC()
	ids := normalizeIDs(submission.FundamentalSnapshotIDs)
	if len(ids) == 0 {
		return Version{}, fmt.Errorf("fundamental_snapshot_ids are required for forecast facts")
	}
	for _, assumption := range submission.Assumptions {
		if err := ValidateAssumption(assumption); err != nil {
			return Version{}, err
		}
	}
	result := Calculate(submission.Inputs, submission.Assumptions)
	input := map[string]any{"contract_version": "forecast-input-v1", "inputs": submission.Inputs, "fundamental_snapshot_ids": ids}
	canonical := struct {
		AssetID     string         `json:"asset_id"`
		AsOf        time.Time      `json:"as_of"`
		Parent      string         `json:"parent_version_id"`
		Input       map[string]any `json:"input"`
		Assumptions []Assumption   `json:"assumptions"`
	}{submission.AssetID, submission.AsOf, strings.TrimSpace(submission.ParentVersionID), input, submission.Assumptions}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return Version{}, fmt.Errorf("marshal forecast identity: %w", err)
	}
	return Version{ID: hashID("forecast", string(encoded)), AssetID: submission.AssetID, ParentVersionID: strings.TrimSpace(submission.ParentVersionID), ModelVersion: ModelVersion, Status: result.Status, AsOf: submission.AsOf, InputSnapshot: input, Assumptions: submission.Assumptions, Projection: result}, nil
}

func (s *Store) validateSourceSnapshots(ctx context.Context, version Version) error {
	raw, ok := version.InputSnapshot["fundamental_snapshot_ids"].([]string)
	if !ok || len(raw) == 0 {
		return fmt.Errorf("forecast source snapshot contract is invalid")
	}
	rows, err := s.db.Query(ctx, `SELECT id FROM fundamental_snapshots WHERE asset_id=$1 AND id = ANY($2) AND available_at <= $3`, version.AssetID, raw, version.AsOf)
	if err != nil {
		return fmt.Errorf("validate forecast source snapshots: %w", err)
	}
	defer rows.Close()
	visible := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan forecast source snapshot: %w", err)
		}
		visible[id] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read forecast source snapshots: %w", err)
	}
	for _, id := range raw {
		if !visible[id] {
			return fmt.Errorf("fundamental snapshot %q is absent, belongs to another asset, or was unavailable at as_of", id)
		}
	}
	return nil
}

type assumptionLink struct {
	id, eventID, field, idempotencyKey, condition string
	period                                        time.Time
	oldValue, newValue, delta                     float64
	evidence                                      []string
}

func versionLinks(version Version) []assumptionLink {
	links := []assumptionLink{}
	for _, assumption := range version.Assumptions {
		if strings.TrimSpace(assumption.EventID) == "" || assumption.FiscalPeriodEnd == nil {
			continue
		}
		oldValue, newValue := assumptionValues(version.InputSnapshot, assumption)
		period := assumption.FiscalPeriodEnd.UTC()
		key := strings.Join([]string{version.AssetID, strings.TrimSpace(assumption.EventID), assumption.Field, period.Format("2006-01-02")}, "|")
		links = append(links, assumptionLink{id: hashID("forecast-event-link", key), eventID: strings.TrimSpace(assumption.EventID), field: assumption.Field, period: period, oldValue: oldValue, newValue: newValue, delta: newValue - oldValue, evidence: normalizeIDs(assumption.EvidenceIDs), condition: strings.TrimSpace(assumption.Condition), idempotencyKey: key})
	}
	return links
}

func assumptionValues(input map[string]any, assumption Assumption) (float64, float64) {
	inputs, _ := input["inputs"].(Inputs)
	base := map[string]*float64{"revenue_growth": inputs.Revenue, "revenue_delta": inputs.Revenue, "operating_margin_delta": inputs.OperatingMargin, "tax_rate_delta": inputs.TaxRate, "capex_delta": inputs.Capex, "change_nwc_delta": inputs.ChangeNWC, "diluted_shares_delta": inputs.DilutedShares}[assumption.Field]
	if base == nil {
		return 0, assumption.Value
	}
	if assumption.Field == "revenue_growth" {
		return *base, *base * (1 + assumption.Value)
	}
	return *base, *base + assumption.Value
}

func normalizeIDs(values []string) []string {
	seen := map[string]struct{}{}
	items := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		items = append(items, value)
	}
	sort.Strings(items)
	return items
}

func hashID(prefix, value string) string {
	sum := sha256.Sum256([]byte(prefix + "|" + value))
	return prefix + "-" + hex.EncodeToString(sum[:])[:48]
}

// jsonScan adapts pgx's JSON bytes without making database JSON decoding part
// of callers' API contracts.
func jsonScan(target any) sql.Scanner {
	return scannerFunc(func(value any) error {
		bytes, ok := value.([]byte)
		if !ok {
			return fmt.Errorf("forecast JSON value is not bytes")
		}
		switch value := target.(type) {
		case *map[string]any:
			return json.Unmarshal(bytes, value)
		case *[]Assumption:
			return json.Unmarshal(bytes, value)
		case *Result:
			return json.Unmarshal(bytes, value)
		default:
			return fmt.Errorf("unsupported forecast JSON target")
		}
	})
}

type scannerFunc func(any) error

func (fn scannerFunc) Scan(value any) error { return fn(value) }
