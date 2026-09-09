package marketdata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const BenchmarkMappingContractVersion = "benchmark-mapping-pit-v1"

type BenchmarkMappingSubmission struct {
	ScopeType        string         `json:"scope_type"`
	ScopeID          string         `json:"scope_id"`
	SubjectMarket    string         `json:"subject_market"`
	SubjectCurrency  string         `json:"subject_currency"`
	BenchmarkAssetID string         `json:"benchmark_asset_id"`
	PolicyVersion    string         `json:"policy_version"`
	ValidFrom        time.Time      `json:"valid_from"`
	ValidTo          *time.Time     `json:"valid_to,omitempty"`
	SourceName       string         `json:"source_name"`
	SourceDocumentID string         `json:"source_document_id"`
	SourceURL        string         `json:"source_url,omitempty"`
	MappingReason    string         `json:"mapping_reason"`
	ApprovedBy       string         `json:"approved_by"`
	Metadata         map[string]any `json:"metadata,omitempty"`
	IdempotencyKey   string         `json:"-"`
}

type BenchmarkMapping struct {
	ID                string         `json:"id"`
	ScopeType         string         `json:"scope_type"`
	ScopeID           string         `json:"scope_id"`
	SubjectMarket     string         `json:"subject_market"`
	SubjectCurrency   string         `json:"subject_currency"`
	BenchmarkAssetID  string         `json:"benchmark_asset_id"`
	BenchmarkClass    string         `json:"benchmark_asset_class"`
	BenchmarkMarket   string         `json:"benchmark_market"`
	BenchmarkCurrency string         `json:"benchmark_currency"`
	BenchmarkSymbol   string         `json:"benchmark_symbol"`
	PolicyVersion     string         `json:"policy_version"`
	ValidFrom         time.Time      `json:"valid_from"`
	ValidTo           *time.Time     `json:"valid_to,omitempty"`
	ObservedAt        time.Time      `json:"observed_at"`
	AvailableAt       time.Time      `json:"available_at"`
	SourceName        string         `json:"source_name"`
	SourceDocumentID  string         `json:"source_document_id"`
	SourceURL         string         `json:"source_url,omitempty"`
	MappingReason     string         `json:"mapping_reason"`
	ApprovedBy        string         `json:"approved_by"`
	Metadata          map[string]any `json:"metadata"`
	CreatedAt         time.Time      `json:"created_at"`
}

type BenchmarkResolutionRequest struct {
	AssetID       string
	Market        string
	Currency      string
	IndustryID    string
	PolicyID      string
	EffectiveAt   time.Time
	AvailableAsOf time.Time
}

type BenchmarkResolution struct {
	Status  string            `json:"status"`
	Reason  string            `json:"reason,omitempty"`
	Mapping *BenchmarkMapping `json:"mapping,omitempty"`
}

func (s *Store) CreateBenchmarkMapping(ctx context.Context, input BenchmarkMappingSubmission, now time.Time) (BenchmarkMapping, bool, error) {
	if s.db == nil {
		return BenchmarkMapping{}, false, fmt.Errorf("market data store is unavailable")
	}
	input.ScopeType = strings.ToLower(strings.TrimSpace(input.ScopeType))
	input.ScopeID = strings.TrimSpace(input.ScopeID)
	input.SubjectMarket = strings.ToUpper(strings.TrimSpace(input.SubjectMarket))
	input.SubjectCurrency = strings.ToUpper(strings.TrimSpace(input.SubjectCurrency))
	input.BenchmarkAssetID = strings.TrimSpace(input.BenchmarkAssetID)
	input.PolicyVersion = strings.TrimSpace(input.PolicyVersion)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceDocumentID = strings.TrimSpace(input.SourceDocumentID)
	input.SourceURL = sanitizedSourceURL(input.SourceURL)
	input.MappingReason = strings.TrimSpace(input.MappingReason)
	input.ApprovedBy = strings.TrimSpace(input.ApprovedBy)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.Metadata == nil {
		input.Metadata = map[string]any{}
	}
	if !map[string]bool{"asset": true, "industry": true, "market": true, "policy": true}[input.ScopeType] || input.ScopeID == "" || input.SubjectMarket == "" || input.SubjectCurrency == "" || input.BenchmarkAssetID == "" || input.PolicyVersion == "" || input.ValidFrom.IsZero() || input.SourceName == "" || input.SourceDocumentID == "" || input.MappingReason == "" || input.ApprovedBy == "" || input.IdempotencyKey == "" {
		return BenchmarkMapping{}, false, fmt.Errorf("benchmark mapping requires complete scope, identity, policy, validity, source, reason, approval and idempotency fields")
	}
	input.ValidFrom = input.ValidFrom.UTC()
	if input.ValidTo != nil {
		value := input.ValidTo.UTC()
		input.ValidTo = &value
		if !value.After(input.ValidFrom) {
			return BenchmarkMapping{}, false, fmt.Errorf("valid_to must be after valid_from")
		}
	}
	var benchmarkClass, benchmarkMarket, benchmarkCurrency string
	if err := s.db.QueryRow(ctx, `SELECT asset_class,market,currency FROM assets WHERE id=$1`, input.BenchmarkAssetID).Scan(&benchmarkClass, &benchmarkMarket, &benchmarkCurrency); err != nil {
		if err == pgx.ErrNoRows {
			return BenchmarkMapping{}, false, fmt.Errorf("benchmark asset does not exist")
		}
		return BenchmarkMapping{}, false, fmt.Errorf("load benchmark asset identity: %w", err)
	}
	benchmarkMarket = strings.ToUpper(strings.TrimSpace(benchmarkMarket))
	benchmarkCurrency = strings.ToUpper(strings.TrimSpace(benchmarkCurrency))
	if benchmarkCurrency != input.SubjectCurrency {
		return BenchmarkMapping{}, false, fmt.Errorf("benchmark currency %s does not match subject currency %s; an explicit FX-normalized benchmark asset is required", benchmarkCurrency, input.SubjectCurrency)
	}
	if input.ScopeType == "asset" && input.ScopeID == input.BenchmarkAssetID {
		return BenchmarkMapping{}, false, fmt.Errorf("asset cannot benchmark itself")
	}
	switch input.ScopeType {
	case "asset":
		var subjectMarket, subjectCurrency string
		if err := s.db.QueryRow(ctx, `SELECT market,currency FROM assets WHERE id=$1`, input.ScopeID).Scan(&subjectMarket, &subjectCurrency); err != nil {
			if err == pgx.ErrNoRows {
				return BenchmarkMapping{}, false, fmt.Errorf("subject asset does not exist")
			}
			return BenchmarkMapping{}, false, fmt.Errorf("load subject asset identity: %w", err)
		}
		if !strings.EqualFold(subjectMarket, input.SubjectMarket) || !strings.EqualFold(subjectCurrency, input.SubjectCurrency) {
			return BenchmarkMapping{}, false, fmt.Errorf("subject market or currency does not match the exact asset identity")
		}
	case "industry":
		var exists bool
		if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM industries WHERE id=$1)`, input.ScopeID).Scan(&exists); err != nil || !exists {
			return BenchmarkMapping{}, false, fmt.Errorf("subject industry does not exist")
		}
	case "market":
		if !strings.EqualFold(input.ScopeID, input.SubjectMarket) {
			return BenchmarkMapping{}, false, fmt.Errorf("market scope_id must equal subject_market")
		}
	}
	canonical := struct {
		BenchmarkMappingSubmission
		BenchmarkMarket   string `json:"benchmark_market"`
		BenchmarkCurrency string `json:"benchmark_currency"`
	}{input, benchmarkMarket, benchmarkCurrency}
	canonical.IdempotencyKey = ""
	body, err := json.Marshal(canonical)
	if err != nil {
		return BenchmarkMapping{}, false, fmt.Errorf("encode benchmark mapping: %w", err)
	}
	digest := sha256.Sum256(body)
	requestHash := hex.EncodeToString(digest[:])
	id := "bm-" + requestHash[:40]
	metadata, _ := json.Marshal(input.Metadata)
	now = now.UTC()
	tag, err := s.db.Exec(ctx, `INSERT INTO benchmark_mapping_observations(
        id,idempotency_key,request_hash,scope_type,scope_id,subject_market,subject_currency,benchmark_asset_id,benchmark_market,benchmark_currency,
        policy_version,valid_from,valid_to,observed_at,available_at,source_name,source_document_id,source_url,mapping_reason,approved_by,metadata)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14,$15,$16,$17,$18,$19,$20)
        ON CONFLICT(idempotency_key) DO NOTHING`, id, input.IdempotencyKey, requestHash, input.ScopeType, input.ScopeID, input.SubjectMarket, input.SubjectCurrency,
		input.BenchmarkAssetID, benchmarkMarket, benchmarkCurrency, input.PolicyVersion, input.ValidFrom, input.ValidTo, now,
		input.SourceName, input.SourceDocumentID, input.SourceURL, input.MappingReason, input.ApprovedBy, metadata)
	if err != nil {
		return BenchmarkMapping{}, false, fmt.Errorf("save benchmark mapping: %w", err)
	}
	created := tag.RowsAffected() == 1
	var storedHash string
	if err := s.db.QueryRow(ctx, `SELECT request_hash FROM benchmark_mapping_observations WHERE idempotency_key=$1`, input.IdempotencyKey).Scan(&storedHash); err != nil {
		return BenchmarkMapping{}, false, fmt.Errorf("read benchmark mapping idempotency result: %w", err)
	}
	if storedHash != requestHash {
		return BenchmarkMapping{}, false, fmt.Errorf("idempotency key was already used for a different benchmark mapping")
	}
	item, err := s.benchmarkMappingByIdempotencyKey(ctx, input.IdempotencyKey)
	return item, created, err
}

func (s *Store) ResolveBenchmark(ctx context.Context, request BenchmarkResolutionRequest) (BenchmarkResolution, error) {
	request.AssetID = strings.TrimSpace(request.AssetID)
	request.Market = strings.ToUpper(strings.TrimSpace(request.Market))
	request.Currency = strings.ToUpper(strings.TrimSpace(request.Currency))
	request.IndustryID = strings.TrimSpace(request.IndustryID)
	request.PolicyID = strings.TrimSpace(request.PolicyID)
	if s.db == nil || request.AssetID == "" || request.Market == "" || request.Currency == "" || request.EffectiveAt.IsZero() || request.AvailableAsOf.IsZero() {
		return BenchmarkResolution{}, fmt.Errorf("invalid benchmark resolution request")
	}
	row := s.db.QueryRow(ctx, benchmarkMappingSelect+`
      WHERE m.subject_market=$1 AND m.subject_currency=$2
        AND m.valid_from<=$3 AND (m.valid_to IS NULL OR m.valid_to>$3)
        AND m.observed_at<=$4 AND m.available_at<=$4
        AND ((m.scope_type='asset' AND m.scope_id=$5)
          OR (m.scope_type='industry' AND $6<>'' AND m.scope_id=$6)
          OR (m.scope_type='market' AND m.scope_id=$1)
          OR (m.scope_type='policy' AND $7<>'' AND m.scope_id=$7))
        AND a.market=m.benchmark_market AND a.currency=m.benchmark_currency
      ORDER BY CASE m.scope_type WHEN 'asset' THEN 1 WHEN 'industry' THEN 2 WHEN 'market' THEN 3 ELSE 4 END,
        m.valid_from DESC,m.available_at DESC,m.created_at DESC,m.id DESC LIMIT 1`, request.Market, request.Currency, request.EffectiveAt.UTC(), request.AvailableAsOf.UTC(), request.AssetID, request.IndustryID, request.PolicyID)
	item, err := scanBenchmarkMapping(row)
	if err == pgx.ErrNoRows {
		return BenchmarkResolution{Status: "unavailable", Reason: "missing_point_in_time_mapping"}, nil
	}
	if err != nil {
		return BenchmarkResolution{}, fmt.Errorf("resolve benchmark mapping: %w", err)
	}
	if item.BenchmarkAssetID == request.AssetID {
		return BenchmarkResolution{Status: "unavailable", Reason: "self_benchmark"}, nil
	}
	return BenchmarkResolution{Status: "available", Mapping: &item}, nil
}

const benchmarkMappingSelect = `SELECT m.id,m.scope_type,m.scope_id,m.subject_market,m.subject_currency,m.benchmark_asset_id,
    a.asset_class,m.benchmark_market,m.benchmark_currency,a.symbol,m.policy_version,m.valid_from,m.valid_to,m.observed_at,m.available_at,
    m.source_name,m.source_document_id,m.source_url,m.mapping_reason,m.approved_by,m.metadata::jsonb,m.created_at
    FROM benchmark_mapping_observations m JOIN assets a ON a.id=m.benchmark_asset_id`

func (s *Store) benchmarkMappingByIdempotencyKey(ctx context.Context, key string) (BenchmarkMapping, error) {
	return scanBenchmarkMapping(s.db.QueryRow(ctx, benchmarkMappingSelect+` WHERE m.idempotency_key=$1`, key))
}

func scanBenchmarkMapping(row pgx.Row) (BenchmarkMapping, error) {
	var item BenchmarkMapping
	var metadata any
	err := row.Scan(&item.ID, &item.ScopeType, &item.ScopeID, &item.SubjectMarket, &item.SubjectCurrency, &item.BenchmarkAssetID,
		&item.BenchmarkClass, &item.BenchmarkMarket, &item.BenchmarkCurrency, &item.BenchmarkSymbol, &item.PolicyVersion, &item.ValidFrom, &item.ValidTo,
		&item.ObservedAt, &item.AvailableAt, &item.SourceName, &item.SourceDocumentID, &item.SourceURL, &item.MappingReason, &item.ApprovedBy, &metadata, &item.CreatedAt)
	if err != nil {
		return BenchmarkMapping{}, err
	}
	if err := decodeCorporateActionJSON(metadata, &item.Metadata); err != nil {
		return BenchmarkMapping{}, fmt.Errorf("decode benchmark mapping metadata: %w", err)
	}
	return item, nil
}

func sanitizedSourceURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}
