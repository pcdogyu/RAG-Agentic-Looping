// Package analystevidence stores explicit, immutable human- or policy-approved
// evidence for governed fundamental-research inputs. It is a provenance
// registry, not an inference engine: it never chooses assumptions, multiples
// or benchmarks.
package analystevidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const ContractVersion = "analyst-evidence-v2"

const (
	ForecastAssumption   = "forecast_assumption"
	ValuationMultiple    = "valuation_multiple"
	CostOfCapital        = "cost_of_capital"
	BenchmarkExpectation = "benchmark_expectation"
	RatingRationale      = "rating_rationale"
	InvalidationRule     = "invalidation_rule"
)

var supportedTypes = map[string]bool{
	ForecastAssumption: true, ValuationMultiple: true, CostOfCapital: true,
	BenchmarkExpectation: true, RatingRationale: true, InvalidationRule: true,
}

type Submission struct {
	AssetID          string         `json:"asset_id"`
	EvidenceType     string         `json:"evidence_type"`
	Title            string         `json:"title"`
	Rationale        string         `json:"rationale"`
	Values           map[string]any `json:"values"`
	ObservedAt       time.Time      `json:"observed_at"`
	AvailableAt      time.Time      `json:"available_at"`
	SourceName       string         `json:"source_name"`
	SourceDocumentID string         `json:"source_document_id"`
	SourceURL        string         `json:"source_url"`
	ApprovedBy       string         `json:"approved_by"`
	ApprovalKind     string         `json:"approval_kind,omitempty"`
	PolicyVersion    string         `json:"policy_version,omitempty"`
	Provenance       map[string]any `json:"provenance,omitempty"`
	IdempotencyKey   string         `json:"-"`
}

type Record struct {
	ID               string         `json:"id"`
	ContractVersion  string         `json:"contract_version"`
	AssetID          string         `json:"asset_id"`
	EvidenceType     string         `json:"evidence_type"`
	Title            string         `json:"title"`
	Rationale        string         `json:"rationale"`
	Values           map[string]any `json:"values"`
	ObservedAt       time.Time      `json:"observed_at"`
	AvailableAt      time.Time      `json:"available_at"`
	SourceName       string         `json:"source_name"`
	SourceDocumentID string         `json:"source_document_id"`
	SourceURL        string         `json:"source_url"`
	ApprovedBy       string         `json:"approved_by"`
	ApprovalKind     string         `json:"approval_kind"`
	PolicyVersion    string         `json:"policy_version,omitempty"`
	Provenance       map[string]any `json:"provenance"`
	ApprovedAt       time.Time      `json:"approved_at"`
	CreatedAt        time.Time      `json:"created_at"`
	RequestHash      string         `json:"-"`
}

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

func (s *Store) Create(ctx context.Context, input Submission, approvedAt time.Time) (Record, bool, error) {
	if s.db == nil || approvedAt.IsZero() {
		return Record{}, false, fmt.Errorf("analyst evidence store and approved_at are required")
	}
	input = normalizeSubmission(input)
	approvedAt = approvedAt.UTC()
	if err := validateSubmission(input, approvedAt); err != nil {
		return Record{}, false, err
	}
	var active bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM assets WHERE id=$1 AND active=true)`, input.AssetID).Scan(&active); err != nil {
		return Record{}, false, fmt.Errorf("validate analyst evidence asset: %w", err)
	}
	if !active {
		return Record{}, false, fmt.Errorf("analyst evidence asset is absent or inactive")
	}
	requestHash := hashJSON(input)
	if existing, err := s.byIdempotency(ctx, input.IdempotencyKey); err == nil {
		if existing.RequestHash != requestHash {
			return Record{}, false, fmt.Errorf("idempotency key was already used for different analyst evidence")
		}
		return existing, false, nil
	} else if err != pgx.ErrNoRows {
		return Record{}, false, err
	}
	values, _ := json.Marshal(input.Values)
	provenance, _ := json.Marshal(input.Provenance)
	id := stableID(input.IdempotencyKey)
	tag, err := s.db.Exec(ctx, `INSERT INTO analyst_evidence_records(id,asset_id,evidence_type,title,rationale,values,observed_at,available_at,source_name,source_document_id,source_url,approved_by,approval_kind,policy_version,provenance,approved_at,idempotency_key,request_hash)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) ON CONFLICT(idempotency_key) DO NOTHING`, id, input.AssetID, input.EvidenceType, input.Title, input.Rationale, values, input.ObservedAt, input.AvailableAt, input.SourceName, input.SourceDocumentID, input.SourceURL, input.ApprovedBy, input.ApprovalKind, input.PolicyVersion, provenance, approvedAt, input.IdempotencyKey, requestHash)
	if err != nil {
		return Record{}, false, fmt.Errorf("insert analyst evidence: %w", err)
	}
	record, err := s.byIdempotency(ctx, input.IdempotencyKey)
	if err != nil {
		return Record{}, false, err
	}
	if record.RequestHash != requestHash {
		return Record{}, false, fmt.Errorf("idempotency key was already used for different analyst evidence")
	}
	return record, tag.RowsAffected() == 1, nil
}

func (s *Store) Get(ctx context.Context, assetID, id string, cutoff time.Time) (Record, error) {
	if s.db == nil || strings.TrimSpace(assetID) == "" || strings.TrimSpace(id) == "" || cutoff.IsZero() {
		return Record{}, fmt.Errorf("invalid analyst evidence lookup")
	}
	return scanRecord(s.db.QueryRow(ctx, recordSelect+` WHERE asset_id=$1 AND id=$2 AND available_at<=$3 AND approved_at<=$3`, strings.TrimSpace(assetID), strings.TrimSpace(id), cutoff.UTC()))
}

func (s *Store) Require(ctx context.Context, assetID string, ids []string, evidenceType string, cutoff time.Time) ([]Record, error) {
	ids = cleanIDs(ids)
	if len(ids) == 0 {
		return nil, fmt.Errorf("%s evidence is required", evidenceType)
	}
	items := make([]Record, 0, len(ids))
	for _, id := range ids {
		item, err := s.Get(ctx, assetID, id, cutoff)
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil, fmt.Errorf("analyst evidence %q is absent, belongs to another asset, or was unavailable at cutoff", id)
			}
			return nil, err
		}
		if item.EvidenceType != evidenceType {
			return nil, fmt.Errorf("analyst evidence %q has type %s, want %s", id, item.EvidenceType, evidenceType)
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Store) List(ctx context.Context, assetID, evidenceType string, cutoff time.Time, limit int) ([]Record, error) {
	if s.db == nil || strings.TrimSpace(assetID) == "" || cutoff.IsZero() || limit < 1 || limit > 100 {
		return nil, fmt.Errorf("invalid analyst evidence query")
	}
	args := []any{strings.TrimSpace(assetID), cutoff.UTC(), limit}
	filter := ""
	if strings.TrimSpace(evidenceType) != "" {
		if !supportedTypes[evidenceType] {
			return nil, fmt.Errorf("unsupported analyst evidence type")
		}
		filter = " AND evidence_type=$4"
		args = append(args, evidenceType)
	}
	rows, err := s.db.Query(ctx, recordSelect+` WHERE asset_id=$1 AND available_at<=$2 AND approved_at<=$2`+filter+` ORDER BY available_at DESC,approved_at DESC,id DESC LIMIT $3`, args...)
	if err != nil {
		return nil, fmt.Errorf("list analyst evidence: %w", err)
	}
	defer rows.Close()
	items := []Record{}
	for rows.Next() {
		item, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func NumericValue(record Record, key string) (float64, bool) {
	value, ok := record.Values[key]
	if !ok {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	default:
		return 0, false
	}
}

func StringValue(record Record, key string) string {
	return stringValue(record.Values, key)
}

const recordSelect = `SELECT id,asset_id,evidence_type,title,rationale,values::jsonb,observed_at,available_at,source_name,source_document_id,source_url,approved_by,approval_kind,policy_version,provenance::jsonb,approved_at,created_at,request_hash FROM analyst_evidence_records`

type rowScanner interface{ Scan(...any) error }

func scanRecord(row rowScanner) (Record, error) {
	var record Record
	var values, provenance []byte
	if err := row.Scan(&record.ID, &record.AssetID, &record.EvidenceType, &record.Title, &record.Rationale, &values, &record.ObservedAt, &record.AvailableAt, &record.SourceName, &record.SourceDocumentID, &record.SourceURL, &record.ApprovedBy, &record.ApprovalKind, &record.PolicyVersion, &provenance, &record.ApprovedAt, &record.CreatedAt, &record.RequestHash); err != nil {
		return Record{}, err
	}
	if err := json.Unmarshal(values, &record.Values); err != nil {
		return Record{}, fmt.Errorf("decode analyst evidence values: %w", err)
	}
	if err := json.Unmarshal(provenance, &record.Provenance); err != nil {
		return Record{}, fmt.Errorf("decode analyst evidence provenance: %w", err)
	}
	record.ContractVersion = ContractVersion
	return record, nil
}

func (s *Store) byIdempotency(ctx context.Context, key string) (Record, error) {
	return scanRecord(s.db.QueryRow(ctx, recordSelect+` WHERE idempotency_key=$1`, key))
}

func normalizeSubmission(input Submission) Submission {
	input.AssetID = strings.TrimSpace(input.AssetID)
	input.EvidenceType = strings.ToLower(strings.TrimSpace(input.EvidenceType))
	input.Title = strings.TrimSpace(input.Title)
	input.Rationale = strings.TrimSpace(input.Rationale)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceDocumentID = strings.TrimSpace(input.SourceDocumentID)
	input.SourceURL = sanitizeURL(input.SourceURL)
	input.ApprovedBy = strings.TrimSpace(input.ApprovedBy)
	input.ApprovalKind = strings.ToLower(strings.TrimSpace(input.ApprovalKind))
	if input.ApprovalKind == "" {
		input.ApprovalKind = "human"
	}
	input.PolicyVersion = strings.TrimSpace(input.PolicyVersion)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	input.ObservedAt = input.ObservedAt.UTC()
	input.AvailableAt = input.AvailableAt.UTC()
	if input.Values == nil {
		input.Values = map[string]any{}
	}
	if input.Provenance == nil {
		input.Provenance = map[string]any{}
	}
	return input
}

func validateSubmission(input Submission, approvedAt time.Time) error {
	if input.AssetID == "" || input.Title == "" || input.Rationale == "" || input.SourceName == "" || input.SourceDocumentID == "" || input.SourceURL == "" || input.ApprovedBy == "" || input.IdempotencyKey == "" {
		return fmt.Errorf("analyst evidence identity, source, rationale, approval and idempotency key are required")
	}
	if !supportedTypes[input.EvidenceType] {
		return fmt.Errorf("unsupported analyst evidence type")
	}
	if input.ApprovalKind != "human" && input.ApprovalKind != "policy" {
		return fmt.Errorf("analyst evidence approval_kind must be human or policy")
	}
	if input.ApprovalKind == "policy" && (input.PolicyVersion == "" || !strings.HasPrefix(input.ApprovedBy, "policy:")) {
		return fmt.Errorf("policy-approved analyst evidence requires policy_version and an explicit policy actor")
	}
	if input.ApprovalKind == "human" && input.PolicyVersion != "" {
		return fmt.Errorf("human-approved analyst evidence must not claim a policy version")
	}
	if input.ObservedAt.IsZero() || input.AvailableAt.IsZero() || input.AvailableAt.Before(input.ObservedAt) || approvedAt.Before(input.AvailableAt) {
		return fmt.Errorf("analyst evidence observed_at, available_at and approved_at are invalid")
	}
	if len(input.Values) == 0 {
		return fmt.Errorf("analyst evidence values are required")
	}
	switch input.EvidenceType {
	case ValuationMultiple:
		if value, ok := number(input.Values["selected_multiple"]); !ok || value <= 0 || value > 200 {
			return fmt.Errorf("valuation_multiple evidence requires selected_multiple between 0 and 200")
		}
	case CostOfCapital:
		if value, ok := number(input.Values["wacc"]); !ok || value <= 0 || value >= 1 {
			return fmt.Errorf("cost_of_capital evidence requires wacc between 0 and 1")
		}
	case BenchmarkExpectation:
		value, ok := number(input.Values["expected_return"])
		if !ok || value <= -1 || value > 10 || stringValue(input.Values, "benchmark_id") == "" {
			return fmt.Errorf("benchmark_expectation evidence requires benchmark_id and expected_return")
		}
	case ForecastAssumption:
		if stringValue(input.Values, "field") == "" {
			return fmt.Errorf("forecast_assumption evidence requires field")
		}
	case RatingRationale:
		if len(stringsValue(input.Values["reason_codes"])) == 0 {
			return fmt.Errorf("rating_rationale evidence requires reason_codes")
		}
	case InvalidationRule:
		if stringValue(input.Values, "rule_type") == "" {
			return fmt.Errorf("invalidation_rule evidence requires rule_type")
		}
	}
	return nil
}

func sanitizeURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return ""
	}
	parsed.User, parsed.RawQuery, parsed.Fragment = nil, "", ""
	return parsed.String()
}

func number(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case float32:
		value := float64(typed)
		return value, !math.IsNaN(value) && !math.IsInf(value, 0)
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		value, err := typed.Float64()
		return value, err == nil && !math.IsNaN(value) && !math.IsInf(value, 0)
	default:
		return 0, false
	}
}

func stringsValue(value any) []string {
	items := []string{}
	switch typed := value.(type) {
	case []string:
		items = typed
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok {
				items = append(items, text)
			}
		}
	}
	return cleanIDs(items)
}

func stringValue(values map[string]any, key string) string {
	value, ok := values[key]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(text)
}

func cleanIDs(values []string) []string {
	seen := map[string]bool{}
	items := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			items = append(items, value)
		}
	}
	sort.Strings(items)
	return items
}

func hashJSON(value any) string {
	body, _ := json.Marshal(value)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func stableID(key string) string {
	sum := sha256.Sum256([]byte(ContractVersion + "|" + key))
	return "analyst-evidence-" + hex.EncodeToString(sum[:])[:48]
}
