package marketdata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	TradabilityContractVersion       = "market-tradability-observation-v1"
	TradabilityImportContractVersion = "market-tradability-import-v1"
)

type TradabilityStatus string

const (
	Tradable             TradabilityStatus = "tradable"
	TradabilitySuspended TradabilityStatus = "suspended"
	TradabilityLimitUp   TradabilityStatus = "limit_up"
	TradabilityLimitDown TradabilityStatus = "limit_down"
	TradabilityDelisted  TradabilityStatus = "delisted"
)

var tradabilityStatuses = map[TradabilityStatus]bool{
	Tradable: true, TradabilitySuspended: true, TradabilityLimitUp: true,
	TradabilityLimitDown: true, TradabilityDelisted: true,
}

type TradabilityObservation struct {
	ID               string            `json:"id"`
	AssetID          string            `json:"asset_id"`
	Market           string            `json:"market"`
	Currency         string            `json:"currency"`
	SessionDate      time.Time         `json:"session_date"`
	ObservedAt       time.Time         `json:"observed_at"`
	AvailableAt      time.Time         `json:"available_at"`
	Status           TradabilityStatus `json:"status"`
	ExecutionPoint   string            `json:"execution_point"`
	BuyExecutable    bool              `json:"buy_executable"`
	SellExecutable   bool              `json:"sell_executable"`
	TimePrecision    string            `json:"time_precision"`
	SourceName       string            `json:"source_name"`
	SourceDocumentID string            `json:"source_document_id"`
	SourceURL        string            `json:"source_url,omitempty"`
	ImportReceiptID  string            `json:"import_receipt_id"`
	Metadata         map[string]any    `json:"metadata"`
	CreatedAt        time.Time         `json:"created_at,omitempty"`
}

type TradabilityImportPoint struct {
	SessionDate      string            `json:"session_date"`
	SourceObservedAt string            `json:"source_observed_at"`
	Status           TradabilityStatus `json:"status"`
}

type TradabilityImport struct {
	SourceName       string                   `json:"source_name"`
	SourceDocumentID string                   `json:"source_document_id"`
	SourceURL        string                   `json:"source_url"`
	LicenseReference string                   `json:"license_reference"`
	ApprovedBy       string                   `json:"approved_by"`
	Observations     []TradabilityImportPoint `json:"observations"`
	IdempotencyKey   string                   `json:"-"`
}

type TradabilityImportReceipt struct {
	ID               string    `json:"id"`
	ContractVersion  string    `json:"contract_version"`
	AssetID          string    `json:"asset_id"`
	Market           string    `json:"market"`
	Currency         string    `json:"currency"`
	SessionStart     string    `json:"session_start,omitempty"`
	SessionEnd       string    `json:"session_end,omitempty"`
	SourceName       string    `json:"source_name"`
	SourceDocumentID string    `json:"source_document_id"`
	SourceURL        string    `json:"source_url"`
	LicenseReference string    `json:"license_reference"`
	ApprovedBy       string    `json:"approved_by"`
	ObservationIDs   []string  `json:"observation_ids,omitempty"`
	ObservationCount int       `json:"observation_count"`
	InsertedCount    int       `json:"inserted_count"`
	AvailableAt      time.Time `json:"available_at"`
	CreatedAt        time.Time `json:"created_at"`
}

type ResolvedTradability struct {
	Status      string `json:"status"`
	Reason      string `json:"reason"`
	SourceCount int    `json:"source_count"`
}

type tradabilityAssetIdentity struct {
	AssetClass, Market, Currency string
}

type normalizedTradabilityPoint struct {
	SessionDate   time.Time         `json:"session_date"`
	ObservedAt    time.Time         `json:"observed_at"`
	Status        TradabilityStatus `json:"status"`
	TimePrecision string            `json:"time_precision"`
}

func (s *Store) ImportTradability(ctx context.Context, assetID string, input TradabilityImport, now time.Time) (TradabilityImportReceipt, bool, error) {
	if s.db == nil {
		return TradabilityImportReceipt{}, false, fmt.Errorf("market data store is unavailable")
	}
	assetID = strings.TrimSpace(assetID)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceDocumentID = strings.TrimSpace(input.SourceDocumentID)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.LicenseReference = strings.TrimSpace(input.LicenseReference)
	input.ApprovedBy = strings.TrimSpace(input.ApprovedBy)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	now = now.UTC()
	if assetID == "" || input.SourceName == "" || input.SourceDocumentID == "" || input.SourceURL == "" || input.LicenseReference == "" || input.ApprovedBy == "" || input.IdempotencyKey == "" || now.IsZero() {
		return TradabilityImportReceipt{}, false, fmt.Errorf("tradability import requires asset, source, license, approval, idempotency and server availability fields")
	}
	if len(input.SourceName) > 120 || len(input.SourceDocumentID) > 320 || len(input.LicenseReference) > 320 || len(input.ApprovedBy) > 160 || len(input.IdempotencyKey) > 200 {
		return TradabilityImportReceipt{}, false, fmt.Errorf("tradability import field exceeds contract length")
	}
	parsedURL, err := url.Parse(input.SourceURL)
	if err != nil || !strings.EqualFold(parsedURL.Scheme, "https") || parsedURL.Host == "" {
		return TradabilityImportReceipt{}, false, fmt.Errorf("source_url must be an absolute HTTPS URL")
	}
	input.SourceURL = sanitizedSourceURL(input.SourceURL)
	if len(input.Observations) < 1 || len(input.Observations) > 1000 {
		return TradabilityImportReceipt{}, false, fmt.Errorf("tradability import requires 1 to 1000 observations")
	}
	identity, err := s.tradabilityAssetIdentity(ctx, assetID)
	if err != nil {
		return TradabilityImportReceipt{}, false, err
	}
	if identity.AssetClass != "equity" && identity.AssetClass != "etf" {
		return TradabilityImportReceipt{}, false, fmt.Errorf("tradability observations only support equity or ETF assets")
	}
	normalized := make([]normalizedTradabilityPoint, 0, len(input.Observations))
	seenSessions := map[string]bool{}
	for _, item := range input.Observations {
		sessionDate, parseErr := time.Parse("2006-01-02", strings.TrimSpace(item.SessionDate))
		if parseErr != nil || sessionDate.After(now) {
			return TradabilityImportReceipt{}, false, fmt.Errorf("session_date must be a non-future YYYY-MM-DD value")
		}
		sessionKey := sessionDate.Format("2006-01-02")
		if seenSessions[sessionKey] {
			return TradabilityImportReceipt{}, false, fmt.Errorf("session_date must be unique within an import")
		}
		seenSessions[sessionKey] = true
		status := TradabilityStatus(strings.ToLower(strings.TrimSpace(string(item.Status))))
		if !tradabilityStatuses[status] {
			return TradabilityImportReceipt{}, false, fmt.Errorf("tradability status is invalid")
		}
		observedAt, precision, parseErr := parseTradabilityObservedAt(item.SourceObservedAt)
		if parseErr != nil || observedAt.After(now) {
			return TradabilityImportReceipt{}, false, fmt.Errorf("source_observed_at must be a non-future YYYY-MM-DD or RFC3339 value")
		}
		normalized = append(normalized, normalizedTradabilityPoint{SessionDate: sessionDate.UTC(), ObservedAt: observedAt, Status: status, TimePrecision: precision})
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].SessionDate.Before(normalized[j].SessionDate) })
	canonical := struct {
		ContractVersion  string                       `json:"contract_version"`
		AssetID          string                       `json:"asset_id"`
		Market           string                       `json:"market"`
		Currency         string                       `json:"currency"`
		SourceName       string                       `json:"source_name"`
		SourceDocumentID string                       `json:"source_document_id"`
		SourceURL        string                       `json:"source_url"`
		LicenseReference string                       `json:"license_reference"`
		ApprovedBy       string                       `json:"approved_by"`
		Observations     []normalizedTradabilityPoint `json:"observations"`
	}{TradabilityImportContractVersion, assetID, identity.Market, identity.Currency, input.SourceName, input.SourceDocumentID,
		input.SourceURL, input.LicenseReference, input.ApprovedBy, normalized}
	body, err := json.Marshal(canonical)
	if err != nil {
		return TradabilityImportReceipt{}, false, fmt.Errorf("encode tradability import: %w", err)
	}
	digest := sha256.Sum256(body)
	requestHash := hex.EncodeToString(digest[:])
	receiptID := "market-tradability-" + requestHash[:48]
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return TradabilityImportReceipt{}, false, fmt.Errorf("begin tradability import: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, `INSERT INTO market_tradability_import_receipts(
		id,idempotency_key,request_hash,asset_id,source_name,source_document_id,source_url,license_reference,approved_by,
		observation_ids,observation_count,inserted_count,available_at,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'[]',0,0,$10,$10) ON CONFLICT(idempotency_key) DO NOTHING`,
		receiptID, input.IdempotencyKey, requestHash, assetID, input.SourceName, input.SourceDocumentID, input.SourceURL,
		input.LicenseReference, input.ApprovedBy, now)
	if err != nil {
		return TradabilityImportReceipt{}, false, fmt.Errorf("reserve tradability import receipt: %w", err)
	}
	if tag.RowsAffected() == 0 {
		stored, storedHash, readErr := scanTradabilityReceipt(tx.QueryRow(ctx, tradabilityReceiptSelect+` WHERE r.idempotency_key=$1`, input.IdempotencyKey))
		if readErr != nil {
			return TradabilityImportReceipt{}, false, fmt.Errorf("read tradability import receipt: %w", readErr)
		}
		if storedHash != requestHash {
			return TradabilityImportReceipt{}, false, fmt.Errorf("idempotency key is already bound to a different tradability import")
		}
		if err = tx.Commit(ctx); err != nil {
			return TradabilityImportReceipt{}, false, fmt.Errorf("finish tradability import replay: %w", err)
		}
		return stored, false, nil
	}
	observationIDs := make([]string, 0, len(normalized))
	insertedCount := 0
	for _, point := range normalized {
		observation := TradabilityObservation{
			AssetID: assetID, Market: identity.Market, Currency: identity.Currency, SessionDate: point.SessionDate,
			ObservedAt: point.ObservedAt, AvailableAt: now, Status: point.Status, ExecutionPoint: "market_close",
			BuyExecutable: point.Status == Tradable, SellExecutable: point.Status == Tradable, TimePrecision: point.TimePrecision,
			SourceName: input.SourceName, SourceDocumentID: input.SourceDocumentID, SourceURL: input.SourceURL, ImportReceiptID: receiptID,
			Metadata: map[string]any{"contract_version": TradabilityContractVersion, "ingestion_method": "approved_manual_import", "execution_rule": "conservative_close_status"},
		}
		observation.ID = deterministicTradabilityID(observation)
		metadata, _ := json.Marshal(observation.Metadata)
		insertTag, insertErr := tx.Exec(ctx, `INSERT INTO market_tradability_observations(
			id,asset_id,market,currency,session_date,observed_at,available_at,status,execution_point,buy_executable,sell_executable,time_precision,
			source_name,source_document_id,source_url,import_receipt_id,metadata)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
			ON CONFLICT(asset_id,source_name,source_document_id,session_date,observed_at,status) DO NOTHING`,
			observation.ID, observation.AssetID, observation.Market, observation.Currency, observation.SessionDate, observation.ObservedAt,
			observation.AvailableAt, observation.Status, observation.ExecutionPoint, observation.BuyExecutable, observation.SellExecutable,
			observation.TimePrecision, observation.SourceName, observation.SourceDocumentID, observation.SourceURL, observation.ImportReceiptID, metadata)
		if insertErr != nil {
			return TradabilityImportReceipt{}, false, fmt.Errorf("save tradability observation: %w", insertErr)
		}
		insertedCount += int(insertTag.RowsAffected())
		observationIDs = append(observationIDs, observation.ID)
	}
	idsBody, _ := json.Marshal(observationIDs)
	if _, err = tx.Exec(ctx, `UPDATE market_tradability_import_receipts SET observation_ids=$2,observation_count=$3,inserted_count=$4 WHERE id=$1`, receiptID, idsBody, len(observationIDs), insertedCount); err != nil {
		return TradabilityImportReceipt{}, false, fmt.Errorf("complete tradability import receipt: %w", err)
	}
	stored, storedHash, err := scanTradabilityReceipt(tx.QueryRow(ctx, tradabilityReceiptSelect+` WHERE r.id=$1`, receiptID))
	if err != nil || storedHash != requestHash {
		return TradabilityImportReceipt{}, false, fmt.Errorf("read completed tradability import receipt: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return TradabilityImportReceipt{}, false, fmt.Errorf("commit tradability import: %w", err)
	}
	return stored, true, nil
}

func (s *Store) ListTradabilityAvailable(ctx context.Context, assetID string, sessionThrough, availableAsOf time.Time, limit int) ([]TradabilityObservation, error) {
	assetID = strings.TrimSpace(assetID)
	if s.db == nil || assetID == "" || sessionThrough.IsZero() || availableAsOf.IsZero() || limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("invalid tradability observation query")
	}
	rows, err := s.db.Query(ctx, `WITH latest AS (
		SELECT DISTINCT ON (source_name,session_date,execution_point)
			id,asset_id,market,currency,session_date,observed_at,available_at,status,execution_point,buy_executable,sell_executable,time_precision,
			source_name,source_document_id,source_url,import_receipt_id,metadata::jsonb,created_at
		FROM market_tradability_observations
		WHERE asset_id=$1 AND session_date<=$2::date AND available_at<=$3
		ORDER BY source_name,session_date,execution_point,available_at DESC,observed_at DESC,created_at DESC,id DESC
	) SELECT id,asset_id,market,currency,session_date,observed_at,available_at,status,execution_point,buy_executable,sell_executable,time_precision,
		source_name,source_document_id,source_url,import_receipt_id,metadata::jsonb,created_at
	FROM latest ORDER BY session_date DESC,source_name LIMIT $4`, assetID, sessionThrough.UTC(), availableAsOf.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list tradability observations: %w", err)
	}
	defer rows.Close()
	items := []TradabilityObservation{}
	for rows.Next() {
		var item TradabilityObservation
		var metadata any
		if err := rows.Scan(&item.ID, &item.AssetID, &item.Market, &item.Currency, &item.SessionDate, &item.ObservedAt, &item.AvailableAt,
			&item.Status, &item.ExecutionPoint, &item.BuyExecutable, &item.SellExecutable, &item.TimePrecision, &item.SourceName,
			&item.SourceDocumentID, &item.SourceURL, &item.ImportReceiptID, &metadata, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan tradability observation: %w", err)
		}
		if err := decodeCorporateActionJSON(metadata, &item.Metadata); err != nil {
			return nil, fmt.Errorf("decode tradability metadata: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func ResolveTradability(values []TradabilityObservation, sessionDate time.Time) ResolvedTradability {
	date := sessionDate.UTC().Format("2006-01-02")
	matching := []TradabilityObservation{}
	for _, value := range values {
		if value.SessionDate.UTC().Format("2006-01-02") == date {
			matching = append(matching, value)
		}
	}
	if len(matching) == 0 {
		return ResolvedTradability{Status: "unknown", Reason: "no_observation", SourceCount: 0}
	}
	first := matching[0]
	if (first.Status == Tradable && (!first.BuyExecutable || !first.SellExecutable)) || (first.Status != Tradable && (first.BuyExecutable || first.SellExecutable)) {
		return ResolvedTradability{Status: "conflict", Reason: "invalid_execution_flags", SourceCount: len(matching)}
	}
	for _, value := range matching[1:] {
		if (value.Status == Tradable && (!value.BuyExecutable || !value.SellExecutable)) || (value.Status != Tradable && (value.BuyExecutable || value.SellExecutable)) {
			return ResolvedTradability{Status: "conflict", Reason: "invalid_execution_flags", SourceCount: len(matching)}
		}
		if value.Status != first.Status || value.BuyExecutable != first.BuyExecutable || value.SellExecutable != first.SellExecutable {
			return ResolvedTradability{Status: "conflict", Reason: "source_disagreement", SourceCount: len(matching)}
		}
	}
	return ResolvedTradability{Status: string(first.Status), Reason: "sources_agree", SourceCount: len(matching)}
}

func (s *Store) ListTradabilityImports(ctx context.Context, assetID string, limit int) ([]TradabilityImportReceipt, error) {
	assetID = strings.TrimSpace(assetID)
	if s.db == nil || assetID == "" || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid tradability import receipt query")
	}
	rows, err := s.db.Query(ctx, tradabilityReceiptSelect+` WHERE r.asset_id=$1 ORDER BY r.available_at DESC,r.id DESC LIMIT $2`, assetID, limit)
	if err != nil {
		return nil, fmt.Errorf("list tradability import receipts: %w", err)
	}
	defer rows.Close()
	items := []TradabilityImportReceipt{}
	for rows.Next() {
		item, _, err := scanTradabilityReceipt(rows)
		if err != nil {
			return nil, fmt.Errorf("scan tradability import receipt: %w", err)
		}
		item.ObservationIDs = nil
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) tradabilityAssetIdentity(ctx context.Context, assetID string) (tradabilityAssetIdentity, error) {
	var identity tradabilityAssetIdentity
	if err := s.db.QueryRow(ctx, `SELECT asset_class,market,currency FROM assets WHERE id=$1`, assetID).Scan(&identity.AssetClass, &identity.Market, &identity.Currency); err != nil {
		if err == pgx.ErrNoRows {
			return identity, fmt.Errorf("asset does not exist")
		}
		return identity, fmt.Errorf("load tradability asset identity: %w", err)
	}
	identity.AssetClass = strings.ToLower(strings.TrimSpace(identity.AssetClass))
	identity.Market = strings.ToUpper(strings.TrimSpace(identity.Market))
	identity.Currency = strings.ToUpper(strings.TrimSpace(identity.Currency))
	return identity, nil
}

func parseTradabilityObservedAt(raw string) (time.Time, string, error) {
	raw = strings.TrimSpace(raw)
	if value, err := time.Parse("2006-01-02", raw); err == nil {
		return value.UTC(), "date_only", nil
	}
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, "", err
	}
	return value.UTC(), "timestamped", nil
}

func deterministicTradabilityID(value TradabilityObservation) string {
	identity := struct {
		AssetID, SourceName, SourceDocumentID, Status string
		SessionDate, ObservedAt                       time.Time
	}{value.AssetID, value.SourceName, value.SourceDocumentID, string(value.Status), value.SessionDate.UTC(), value.ObservedAt.UTC()}
	body, _ := json.Marshal(identity)
	hash := sha256.Sum256(body)
	return "market-tradability-" + hex.EncodeToString(hash[:])[:48]
}

const tradabilityReceiptSelect = `SELECT r.id,r.request_hash,r.asset_id,a.market,a.currency,r.source_name,r.source_document_id,r.source_url,
	r.license_reference,r.approved_by,r.observation_ids::jsonb,r.observation_count,r.inserted_count,r.available_at,r.created_at,
	coverage.session_start,coverage.session_end
	FROM market_tradability_import_receipts r JOIN assets a ON a.id=r.asset_id
	LEFT JOIN LATERAL (
		SELECT coalesce(to_char(min(o.session_date),'YYYY-MM-DD'),'') AS session_start,
			coalesce(to_char(max(o.session_date),'YYYY-MM-DD'),'') AS session_end
		FROM market_tradability_observations o
		WHERE o.id IN (SELECT jsonb_array_elements_text(r.observation_ids))
	) coverage ON true`

func scanTradabilityReceipt(row pgx.Row) (TradabilityImportReceipt, string, error) {
	var item TradabilityImportReceipt
	var requestHash string
	var idsBody []byte
	if err := row.Scan(&item.ID, &requestHash, &item.AssetID, &item.Market, &item.Currency, &item.SourceName, &item.SourceDocumentID,
		&item.SourceURL, &item.LicenseReference, &item.ApprovedBy, &idsBody, &item.ObservationCount, &item.InsertedCount,
		&item.AvailableAt, &item.CreatedAt, &item.SessionStart, &item.SessionEnd); err != nil {
		return TradabilityImportReceipt{}, "", err
	}
	if err := json.Unmarshal(idsBody, &item.ObservationIDs); err != nil {
		return TradabilityImportReceipt{}, "", fmt.Errorf("decode tradability observation ids: %w", err)
	}
	item.ContractVersion = TradabilityImportContractVersion
	return item, requestHash, nil
}
