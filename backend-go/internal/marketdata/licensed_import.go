package marketdata

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
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketpolicy"
)

const LicensedBenchmarkPriceImportContractVersion = "licensed-benchmark-price-import-v1"

type LicensedBenchmarkPrice struct {
	SessionDate   string  `json:"session_date"`
	AdjustedClose float64 `json:"adjusted_close"`
}

type LicensedBenchmarkPriceImport struct {
	VendorCode       string                   `json:"vendor_code"`
	SourceName       string                   `json:"source_name"`
	SourceDocumentID string                   `json:"source_document_id"`
	SourceURL        string                   `json:"source_url"`
	LicenseReference string                   `json:"license_reference"`
	ApprovedBy       string                   `json:"approved_by"`
	Observations     []LicensedBenchmarkPrice `json:"observations"`
	IdempotencyKey   string                   `json:"-"`
}

type LicensedBenchmarkPriceImportReceipt struct {
	ID               string    `json:"id"`
	ContractVersion  string    `json:"contract_version"`
	AssetID          string    `json:"asset_id"`
	Market           string    `json:"market"`
	Currency         string    `json:"currency"`
	SessionStart     string    `json:"session_start,omitempty"`
	SessionEnd       string    `json:"session_end,omitempty"`
	VendorCode       string    `json:"vendor_code"`
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

type licensedBenchmarkIdentity struct {
	AssetClass     string
	Market         string
	Currency       string
	Symbol         string
	InstrumentType string
	Aliases        []string
}

// ImportLicensedBenchmarkPrices stores a licensed gross-total-return index
// series as immutable first-observed facts. available_at is always assigned by
// this service, so an operator cannot backdate when the data became known.
func (s *Store) ImportLicensedBenchmarkPrices(ctx context.Context, assetID string, input LicensedBenchmarkPriceImport, now time.Time) (LicensedBenchmarkPriceImportReceipt, bool, error) {
	if s.db == nil {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("market data store is unavailable")
	}
	assetID = strings.TrimSpace(assetID)
	input.VendorCode = strings.TrimSpace(input.VendorCode)
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.SourceDocumentID = strings.TrimSpace(input.SourceDocumentID)
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.LicenseReference = strings.TrimSpace(input.LicenseReference)
	input.ApprovedBy = strings.TrimSpace(input.ApprovedBy)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	now = now.UTC()
	if assetID == "" || input.VendorCode == "" || input.SourceName == "" || input.SourceDocumentID == "" || input.SourceURL == "" || input.LicenseReference == "" || input.ApprovedBy == "" || input.IdempotencyKey == "" {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("licensed benchmark import requires asset identity, vendor code, source, license, approval and idempotency fields")
	}
	if now.IsZero() {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("licensed benchmark import requires server availability time")
	}
	if len(input.VendorCode) > 80 || len(input.SourceName) > 120 || len(input.SourceDocumentID) > 320 || len(input.LicenseReference) > 320 || len(input.ApprovedBy) > 160 || len(input.IdempotencyKey) > 200 {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("licensed benchmark import field exceeds contract length")
	}
	parsedURL, err := url.Parse(input.SourceURL)
	if err != nil || !strings.EqualFold(parsedURL.Scheme, "https") || parsedURL.Host == "" {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("source_url must be an absolute HTTPS URL")
	}
	input.SourceURL = sanitizedSourceURL(input.SourceURL)
	if len(input.Observations) < 1 || len(input.Observations) > 1000 {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("licensed benchmark import requires 1 to 1000 observations")
	}

	identity, err := s.licensedBenchmarkIdentity(ctx, assetID)
	if err != nil {
		return LicensedBenchmarkPriceImportReceipt{}, false, err
	}
	if !marketpolicy.IsCanonicalBenchmarkAsset(assetID) || identity.AssetClass != "index" || identity.InstrumentType != "gross_total_return_index" {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("asset is not a canonical gross-total-return benchmark index")
	}
	if !matchesBenchmarkVendorCode(input.VendorCode, identity.Symbol, identity.Aliases) {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("vendor_code does not match the canonical benchmark identity")
	}

	type normalizedPrice struct {
		SessionDate time.Time `json:"session_date"`
		Price       float64   `json:"adjusted_close"`
	}
	normalized := make([]normalizedPrice, 0, len(input.Observations))
	seenDates := make(map[string]struct{}, len(input.Observations))
	for _, item := range input.Observations {
		date, parseErr := time.Parse("2006-01-02", strings.TrimSpace(item.SessionDate))
		if parseErr != nil {
			return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("session_date must use YYYY-MM-DD")
		}
		date = date.UTC()
		if date.After(now) {
			return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("session_date cannot be in the future")
		}
		if item.AdjustedClose <= 0 || math.IsNaN(item.AdjustedClose) || math.IsInf(item.AdjustedClose, 0) {
			return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("adjusted_close must be finite and positive")
		}
		key := date.Format("2006-01-02")
		if _, exists := seenDates[key]; exists {
			return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("session_date must be unique within an import")
		}
		seenDates[key] = struct{}{}
		normalized = append(normalized, normalizedPrice{SessionDate: date, Price: item.AdjustedClose})
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].SessionDate.Before(normalized[j].SessionDate) })

	canonical := struct {
		ContractVersion  string            `json:"contract_version"`
		AssetID          string            `json:"asset_id"`
		Market           string            `json:"market"`
		Currency         string            `json:"currency"`
		VendorCode       string            `json:"vendor_code"`
		SourceName       string            `json:"source_name"`
		SourceDocumentID string            `json:"source_document_id"`
		SourceURL        string            `json:"source_url"`
		LicenseReference string            `json:"license_reference"`
		ApprovedBy       string            `json:"approved_by"`
		Observations     []normalizedPrice `json:"observations"`
	}{LicensedBenchmarkPriceImportContractVersion, assetID, identity.Market, identity.Currency, input.VendorCode, input.SourceName,
		input.SourceDocumentID, input.SourceURL, input.LicenseReference, input.ApprovedBy, normalized}
	body, err := json.Marshal(canonical)
	if err != nil {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("encode licensed benchmark import: %w", err)
	}
	digest := sha256.Sum256(body)
	requestHash := hex.EncodeToString(digest[:])
	receiptID := "licensed-benchmark-price-" + requestHash[:40]

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("begin licensed benchmark import: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, `INSERT INTO licensed_benchmark_price_import_receipts(
		id,idempotency_key,request_hash,asset_id,vendor_code,source_name,source_document_id,source_url,license_reference,approved_by,
		observation_ids,observation_count,inserted_count,available_at,created_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'[]',1,0,$11,$11) ON CONFLICT(idempotency_key) DO NOTHING`,
		receiptID, input.IdempotencyKey, requestHash, assetID, input.VendorCode, input.SourceName, input.SourceDocumentID,
		input.SourceURL, input.LicenseReference, input.ApprovedBy, now)
	if err != nil {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("reserve licensed benchmark import receipt: %w", err)
	}
	if tag.RowsAffected() == 0 {
		stored, storedHash, readErr := scanLicensedBenchmarkReceipt(tx.QueryRow(ctx, licensedBenchmarkReceiptSelect+` WHERE r.idempotency_key=$1`, input.IdempotencyKey))
		if readErr != nil {
			return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("read licensed benchmark import receipt: %w", readErr)
		}
		if storedHash != requestHash {
			return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("idempotency key is already bound to a different licensed benchmark import")
		}
		if err = tx.Commit(ctx); err != nil {
			return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("finish licensed benchmark import replay: %w", err)
		}
		return stored, false, nil
	}

	observationIDs := make([]string, 0, len(normalized))
	insertedCount := 0
	for _, item := range normalized {
		observation := PriceObservation{
			AssetID: assetID, Market: identity.Market, Currency: identity.Currency,
			SessionDate: item.SessionDate, ObservedAt: item.SessionDate, AvailableAt: now, Price: item.Price,
			PriceField: "adjusted_close", TimePrecision: "daily_close", SourceName: input.SourceName,
			SourceDocumentID: input.SourceDocumentID, SourceURL: input.SourceURL,
			Metadata: map[string]any{
				"return_series_kind": "gross_total_return_index", "ingestion_method": "licensed_manual_import",
				"import_contract_version": LicensedBenchmarkPriceImportContractVersion, "vendor_code": input.VendorCode,
				"audit_receipt_id": receiptID, "authorization_basis": "operator_attested_license_receipt",
			},
		}
		if err = observation.NormalizeAndValidate(); err != nil {
			return LicensedBenchmarkPriceImportReceipt{}, false, err
		}
		metadata, marshalErr := json.Marshal(observation.Metadata)
		if marshalErr != nil {
			return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("encode licensed benchmark observation metadata: %w", marshalErr)
		}
		insertTag, insertErr := tx.Exec(ctx, `INSERT INTO market_price_observations(
			id,asset_id,market,currency,session_date,observed_at,available_at,price,price_field,time_precision,source_name,source_document_id,source_url,metadata)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
			ON CONFLICT(asset_id,source_name,source_document_id,observed_at,price_field,price) DO NOTHING`,
			observation.ID, observation.AssetID, observation.Market, observation.Currency, observation.SessionDate,
			observation.ObservedAt, observation.AvailableAt, observation.Price, observation.PriceField,
			observation.TimePrecision, observation.SourceName, observation.SourceDocumentID, observation.SourceURL, metadata)
		if insertErr != nil {
			return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("save licensed benchmark observation: %w", insertErr)
		}
		insertedCount += int(insertTag.RowsAffected())
		observationIDs = append(observationIDs, observation.ID)
	}
	idsBody, _ := json.Marshal(observationIDs)
	if _, err = tx.Exec(ctx, `UPDATE licensed_benchmark_price_import_receipts
		SET observation_ids=$2,observation_count=$3,inserted_count=$4 WHERE id=$1`, receiptID, idsBody, len(observationIDs), insertedCount); err != nil {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("complete licensed benchmark import receipt: %w", err)
	}
	stored, storedHash, err := scanLicensedBenchmarkReceipt(tx.QueryRow(ctx, licensedBenchmarkReceiptSelect+` WHERE r.id=$1`, receiptID))
	if err != nil || storedHash != requestHash {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("read completed licensed benchmark import receipt: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return LicensedBenchmarkPriceImportReceipt{}, false, fmt.Errorf("commit licensed benchmark import: %w", err)
	}
	return stored, true, nil
}

// ListLicensedBenchmarkPriceImports returns administrator audit receipts and
// their actual persisted session coverage. Licence and approver details stay
// behind the administrator API instead of leaking through public price reads.
func (s *Store) ListLicensedBenchmarkPriceImports(ctx context.Context, assetID string, limit int) ([]LicensedBenchmarkPriceImportReceipt, error) {
	assetID = strings.TrimSpace(assetID)
	if s.db == nil || assetID == "" || limit < 1 || limit > 200 {
		return nil, fmt.Errorf("invalid licensed benchmark import receipt query")
	}
	rows, err := s.db.Query(ctx, licensedBenchmarkReceiptSelect+` WHERE r.asset_id=$1 ORDER BY r.available_at DESC,r.id DESC LIMIT $2`, assetID, limit)
	if err != nil {
		return nil, fmt.Errorf("list licensed benchmark import receipts: %w", err)
	}
	defer rows.Close()
	items := []LicensedBenchmarkPriceImportReceipt{}
	for rows.Next() {
		item, _, scanErr := scanLicensedBenchmarkReceipt(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan licensed benchmark import receipt: %w", scanErr)
		}
		// History pages use counts and persisted coverage. Omitting up to 1000
		// immutable IDs per row keeps the list response bounded; the original
		// import response still returns the exact IDs.
		item.ObservationIDs = nil
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("list licensed benchmark import receipts: %w", err)
	}
	return items, nil
}

func (s *Store) licensedBenchmarkIdentity(ctx context.Context, assetID string) (licensedBenchmarkIdentity, error) {
	var identity licensedBenchmarkIdentity
	var aliasesBody []byte
	if err := s.db.QueryRow(ctx, `SELECT asset_class,market,currency,symbol,coalesce(instrument_type,''),aliases::jsonb FROM assets WHERE id=$1`, assetID).
		Scan(&identity.AssetClass, &identity.Market, &identity.Currency, &identity.Symbol, &identity.InstrumentType, &aliasesBody); err != nil {
		if err == pgx.ErrNoRows {
			return licensedBenchmarkIdentity{}, fmt.Errorf("benchmark asset does not exist")
		}
		return licensedBenchmarkIdentity{}, fmt.Errorf("load benchmark asset identity: %w", err)
	}
	identity.AssetClass = strings.ToLower(strings.TrimSpace(identity.AssetClass))
	identity.Market = strings.ToUpper(strings.TrimSpace(identity.Market))
	identity.Currency = strings.ToUpper(strings.TrimSpace(identity.Currency))
	identity.Symbol = strings.TrimSpace(identity.Symbol)
	identity.InstrumentType = strings.ToLower(strings.TrimSpace(identity.InstrumentType))
	if err := json.Unmarshal(aliasesBody, &identity.Aliases); err != nil {
		return licensedBenchmarkIdentity{}, fmt.Errorf("decode benchmark aliases: %w", err)
	}
	return identity, nil
}

func matchesBenchmarkVendorCode(value, symbol string, aliases []string) bool {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, strings.TrimSpace(symbol)) {
		return true
	}
	for _, alias := range aliases {
		if strings.EqualFold(value, strings.TrimSpace(alias)) {
			return true
		}
	}
	return false
}

const licensedBenchmarkReceiptSelect = `SELECT r.id,r.request_hash,r.asset_id,a.market,a.currency,r.vendor_code,r.source_name,
	r.source_document_id,r.source_url,r.license_reference,r.approved_by,r.observation_ids::jsonb,r.observation_count,r.inserted_count,
	r.available_at,r.created_at,coverage.session_start,coverage.session_end
	FROM licensed_benchmark_price_import_receipts r JOIN assets a ON a.id=r.asset_id
	LEFT JOIN LATERAL (
		SELECT coalesce(to_char(min(p.session_date),'YYYY-MM-DD'),'') AS session_start,
			coalesce(to_char(max(p.session_date),'YYYY-MM-DD'),'') AS session_end
		FROM market_price_observations p
		WHERE p.id IN (SELECT jsonb_array_elements_text(r.observation_ids::jsonb))
	) coverage ON true`

func scanLicensedBenchmarkReceipt(row pgx.Row) (LicensedBenchmarkPriceImportReceipt, string, error) {
	var item LicensedBenchmarkPriceImportReceipt
	var requestHash string
	var idsBody []byte
	if err := row.Scan(&item.ID, &requestHash, &item.AssetID, &item.Market, &item.Currency, &item.VendorCode, &item.SourceName,
		&item.SourceDocumentID, &item.SourceURL, &item.LicenseReference, &item.ApprovedBy, &idsBody, &item.ObservationCount,
		&item.InsertedCount, &item.AvailableAt, &item.CreatedAt, &item.SessionStart, &item.SessionEnd); err != nil {
		return LicensedBenchmarkPriceImportReceipt{}, "", err
	}
	if err := json.Unmarshal(idsBody, &item.ObservationIDs); err != nil {
		return LicensedBenchmarkPriceImportReceipt{}, "", fmt.Errorf("decode licensed benchmark observation ids: %w", err)
	}
	item.ContractVersion = LicensedBenchmarkPriceImportContractVersion
	return item, requestHash, nil
}
