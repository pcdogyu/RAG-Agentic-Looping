package consensus

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

type GuidanceSourceDocument struct {
	ID                 string                `json:"id"`
	AssetID            string                `json:"asset_id"`
	Provider           string                `json:"provider"`
	CIK                string                `json:"cik"`
	AccessionNumber    string                `json:"accession_number"`
	Form               string                `json:"form"`
	FilingDate         time.Time             `json:"filing_date"`
	ReportDate         *time.Time            `json:"report_date,omitempty"`
	AcceptedAt         time.Time             `json:"accepted_at"`
	SourceAvailableAt  time.Time             `json:"source_available_at"`
	FirstObservedAt    time.Time             `json:"first_observed_at"`
	FilingIndexURL     string                `json:"filing_index_url"`
	PrimaryDocument    string                `json:"primary_document,omitempty"`
	PrimaryDocumentURL string                `json:"primary_document_url,omitempty"`
	SourcePayload      map[string]any        `json:"source_payload,omitempty"`
	LatestReview       *GuidanceSourceReview `json:"latest_review,omitempty"`
}

type GuidanceSourceReview struct {
	ID                 string    `json:"id"`
	Decision           string    `json:"decision"`
	GuidanceSnapshotID *string   `json:"guidance_snapshot_id,omitempty"`
	EvidenceURL        *string   `json:"evidence_url,omitempty"`
	EvidenceLocation   *string   `json:"evidence_location,omitempty"`
	EvidenceExcerpt    *string   `json:"evidence_excerpt,omitempty"`
	Notes              string    `json:"notes,omitempty"`
	ReviewedBy         string    `json:"reviewed_by"`
	ReviewedAt         time.Time `json:"reviewed_at"`
}

type GuidanceSourceReviewSubmission struct {
	AssetID          string    `json:"asset_id"`
	SourceDocumentID string    `json:"source_document_id"`
	Decision         string    `json:"decision"`
	Guidance         *Guidance `json:"guidance,omitempty"`
	EvidenceURL      string    `json:"evidence_url,omitempty"`
	EvidenceLocation string    `json:"evidence_location,omitempty"`
	EvidenceExcerpt  string    `json:"evidence_excerpt,omitempty"`
	Notes            string    `json:"notes,omitempty"`
	ReviewedBy       string    `json:"reviewed_by"`
	IdempotencyKey   string    `json:"-"`
	ReviewedAt       time.Time `json:"-"`
}

type GuidanceSourceReviewResult struct {
	Created          bool                 `json:"created"`
	Review           GuidanceSourceReview `json:"review"`
	GuidanceSnapshot *Guidance            `json:"guidance_snapshot,omitempty"`
}

func (s *Store) SaveGuidanceSource(ctx context.Context, value GuidanceSourceDocument) (bool, error) {
	if err := normalizeGuidanceSource(&value); err != nil {
		return false, err
	}
	body, err := json.Marshal(value.SourcePayload)
	if err != nil {
		return false, err
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO guidance_source_documents(id,asset_id,provider,cik,accession_number,form,filing_date,report_date,accepted_at,source_available_at,first_observed_at,filing_index_url,primary_document,primary_document_url,source_payload)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT(asset_id,provider,accession_number) DO NOTHING`, value.ID, value.AssetID, value.Provider, value.CIK, value.AccessionNumber, value.Form, value.FilingDate, value.ReportDate, value.AcceptedAt, value.SourceAvailableAt, value.FirstObservedAt, value.FilingIndexURL, value.PrimaryDocument, value.PrimaryDocumentURL, body)
	if err != nil {
		return false, fmt.Errorf("save guidance source document: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) GuidanceSources(ctx context.Context, assetID string, cutoff time.Time, limit int, includePayload bool) ([]GuidanceSourceDocument, error) {
	if s.db == nil || strings.TrimSpace(assetID) == "" || cutoff.IsZero() || limit < 1 || limit > 500 {
		return nil, errors.New("guidance source store, asset_id, cutoff and limit are required")
	}
	rows, err := s.db.Query(ctx, `SELECT document.id,document.asset_id,document.provider,document.cik,document.accession_number,document.form,document.filing_date,document.report_date,document.accepted_at,document.source_available_at,document.first_observed_at,document.filing_index_url,document.primary_document,document.primary_document_url,document.source_payload,
	review.id,review.decision,review.guidance_snapshot_id,review.evidence_url,review.evidence_location,review.evidence_excerpt,review.notes,review.reviewed_by,review.reviewed_at
FROM guidance_source_documents document
LEFT JOIN LATERAL (SELECT * FROM guidance_source_reviews candidate WHERE candidate.source_document_id=document.id ORDER BY candidate.reviewed_at DESC,candidate.id DESC LIMIT 1) review ON true
WHERE document.asset_id=$1 AND document.source_available_at<=$2
ORDER BY document.source_available_at DESC,document.id DESC LIMIT $3`, strings.TrimSpace(assetID), cutoff.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []GuidanceSourceDocument{}
	for rows.Next() {
		var item GuidanceSourceDocument
		var payload []byte
		var reviewID, decision, snapshotID, evidenceURL, evidenceLocation, evidenceExcerpt, notes, reviewedBy sql.NullString
		var reviewedAt sql.NullTime
		if err := rows.Scan(&item.ID, &item.AssetID, &item.Provider, &item.CIK, &item.AccessionNumber, &item.Form, &item.FilingDate, &item.ReportDate, &item.AcceptedAt, &item.SourceAvailableAt, &item.FirstObservedAt, &item.FilingIndexURL, &item.PrimaryDocument, &item.PrimaryDocumentURL, &payload,
			&reviewID, &decision, &snapshotID, &evidenceURL, &evidenceLocation, &evidenceExcerpt, &notes, &reviewedBy, &reviewedAt); err != nil {
			return nil, err
		}
		if includePayload {
			if err := json.Unmarshal(payload, &item.SourcePayload); err != nil {
				return nil, fmt.Errorf("decode guidance source payload: %w", err)
			}
		}
		if reviewID.Valid {
			item.LatestReview = &GuidanceSourceReview{ID: reviewID.String, Decision: decision.String, Notes: notes.String, ReviewedBy: reviewedBy.String, ReviewedAt: reviewedAt.Time.UTC()}
			item.LatestReview.GuidanceSnapshotID = nullableString(snapshotID)
			item.LatestReview.EvidenceURL = nullableString(evidenceURL)
			item.LatestReview.EvidenceLocation = nullableString(evidenceLocation)
			item.LatestReview.EvidenceExcerpt = nullableString(evidenceExcerpt)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ReviewGuidanceSource(ctx context.Context, input GuidanceSourceReviewSubmission) (GuidanceSourceReviewResult, error) {
	input.AssetID, input.SourceDocumentID = strings.TrimSpace(input.AssetID), strings.TrimSpace(input.SourceDocumentID)
	input.Decision, input.ReviewedBy = strings.ToLower(strings.TrimSpace(input.Decision)), strings.TrimSpace(input.ReviewedBy)
	input.IdempotencyKey, input.Notes = strings.TrimSpace(input.IdempotencyKey), strings.TrimSpace(input.Notes)
	input.EvidenceURL, input.EvidenceLocation, input.EvidenceExcerpt = strings.TrimSpace(input.EvidenceURL), strings.TrimSpace(input.EvidenceLocation), strings.TrimSpace(input.EvidenceExcerpt)
	if s.db == nil || input.AssetID == "" || input.SourceDocumentID == "" || input.ReviewedBy == "" || input.IdempotencyKey == "" || input.ReviewedAt.IsZero() {
		return GuidanceSourceReviewResult{}, errors.New("guidance review requires store, asset_id, source_document_id, reviewed_by, reviewed_at and idempotency key")
	}
	if input.Decision != "confirmed_guidance" && input.Decision != "no_guidance" && input.Decision != "needs_follow_up" {
		return GuidanceSourceReviewResult{}, errors.New("guidance review decision must be confirmed_guidance, no_guidance or needs_follow_up")
	}
	if utf8.RuneCountInString(input.Notes) > 4000 || utf8.RuneCountInString(input.EvidenceExcerpt) > 2000 || utf8.RuneCountInString(input.EvidenceLocation) > 300 {
		return GuidanceSourceReviewResult{}, errors.New("guidance review text exceeds the allowed length")
	}
	if input.Decision == "confirmed_guidance" {
		if input.Guidance == nil || input.EvidenceURL == "" || input.EvidenceLocation == "" || input.EvidenceExcerpt == "" {
			return GuidanceSourceReviewResult{}, errors.New("confirmed guidance requires guidance, evidence_url, evidence_location and evidence_excerpt")
		}
	} else if input.Guidance != nil {
		return GuidanceSourceReviewResult{}, errors.New("only confirmed_guidance may include a guidance snapshot")
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return GuidanceSourceReviewResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "guidance-review:"+input.IdempotencyKey); err != nil {
		return GuidanceSourceReviewResult{}, err
	}
	document, err := guidanceSourceByID(ctx, tx, input.AssetID, input.SourceDocumentID)
	if err != nil {
		return GuidanceSourceReviewResult{}, err
	}

	var requestedGuidance any
	if input.Guidance != nil {
		requestedGuidance = map[string]any{
			"metric": input.Guidance.Metric, "fiscal_period": input.Guidance.FiscalPeriod, "fiscal_period_end": input.Guidance.FiscalPeriodEnd,
			"accounting_basis": input.Guidance.AccountingBasis, "low_value": input.Guidance.LowValue, "high_value": input.Guidance.HighValue,
			"currency": input.Guidance.Currency, "unit": input.Guidance.Unit,
		}
	}
	requestBody := map[string]any{
		"asset_id": input.AssetID, "source_document_id": input.SourceDocumentID, "decision": input.Decision,
		"guidance": requestedGuidance, "evidence_url": input.EvidenceURL, "evidence_location": input.EvidenceLocation,
		"evidence_excerpt": input.EvidenceExcerpt, "notes": input.Notes, "reviewed_by": input.ReviewedBy,
	}
	encoded, _ := json.Marshal(requestBody)
	sum := sha256.Sum256(encoded)
	requestHash := hex.EncodeToString(sum[:])
	if existing, found, existingErr := reviewByIdempotencyKey(ctx, tx, input.IdempotencyKey); existingErr != nil {
		return GuidanceSourceReviewResult{}, existingErr
	} else if found {
		if existing.requestHash != requestHash {
			return GuidanceSourceReviewResult{}, errors.New("idempotency key already belongs to a different guidance review")
		}
		if err := tx.Commit(ctx); err != nil {
			return GuidanceSourceReviewResult{}, err
		}
		return GuidanceSourceReviewResult{Created: false, Review: existing.review}, nil
	}
	if input.Decision != "confirmed_guidance" {
		var alreadyConfirmed bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM guidance_source_reviews WHERE source_document_id=$1 AND decision='confirmed_guidance')`, document.ID).Scan(&alreadyConfirmed); err != nil {
			return GuidanceSourceReviewResult{}, err
		}
		if alreadyConfirmed {
			return GuidanceSourceReviewResult{}, errors.New("a confirmed guidance source cannot be reclassified without an explicit withdrawal workflow")
		}
	}

	var guidanceSnapshot *Guidance
	if input.Decision == "confirmed_guidance" {
		if !evidenceURLBelongsToDocument(input.EvidenceURL, document) {
			return GuidanceSourceReviewResult{}, errors.New("evidence_url must belong to the selected SEC accession directory")
		}
		value := *input.Guidance
		value.ID, value.AssetID = "", input.AssetID
		value.PublishedAt, value.AvailableAt, value.RevisionAt = document.AcceptedAt, input.ReviewedAt.UTC(), nil
		if value.AvailableAt.Before(document.SourceAvailableAt) {
			value.AvailableAt = document.SourceAvailableAt
		}
		value.SourceName, value.SourceURL, value.SourceDocumentID, value.RetrievedAt = "SEC EDGAR issuer disclosure", input.EvidenceURL, document.AccessionNumber, input.ReviewedAt.UTC()
		value.SourcePayload = map[string]any{
			"contract_version": SECDisclosureContractVersion, "guidance_source_document_id": document.ID,
			"cik": document.CIK, "accession_number": document.AccessionNumber, "form": document.Form,
			"evidence_url": input.EvidenceURL, "evidence_location": input.EvidenceLocation, "evidence_excerpt": input.EvidenceExcerpt,
			"reviewed_by": input.ReviewedBy, "reviewed_at": input.ReviewedAt.UTC().Format(time.RFC3339Nano),
			"source_available_at": document.SourceAvailableAt.UTC().Format(time.RFC3339Nano), "feature_available_at_basis": "human_review_completed_at",
			"analyst_confirmed": true, "automatic_extraction": false, "guidance_is_consensus": false, "automatic_rating": false,
		}
		if err := normalizeGuidance(&value); err != nil {
			return GuidanceSourceReviewResult{}, err
		}
		payload, marshalErr := json.Marshal(value.SourcePayload)
		if marshalErr != nil {
			return GuidanceSourceReviewResult{}, marshalErr
		}
		var existingID string
		var existingLow, existingHigh *float64
		var existingFiscalPeriod, existingCurrency, existingUnit string
		existingErr := tx.QueryRow(ctx, `SELECT id,low_value,high_value,fiscal_period,currency,unit FROM management_guidance_snapshots WHERE asset_id=$1 AND metric=$2 AND fiscal_period_end=$3 AND accounting_basis=$4 AND source_name=$5 AND source_document_id=$6 AND published_at=$7`, value.AssetID, value.Metric, value.FiscalPeriodEnd, value.AccountingBasis, value.SourceName, value.SourceDocumentID, value.PublishedAt).Scan(&existingID, &existingLow, &existingHigh, &existingFiscalPeriod, &existingCurrency, &existingUnit)
		if existingErr == nil {
			if !equalNullableFloat(existingLow, value.LowValue) || !equalNullableFloat(existingHigh, value.HighValue) || existingFiscalPeriod != value.FiscalPeriod || existingCurrency != value.Currency || existingUnit != value.Unit {
				return GuidanceSourceReviewResult{}, errors.New("confirmed guidance identity already exists with different values or units")
			}
			value.ID = existingID
		} else if !errors.Is(existingErr, pgx.ErrNoRows) {
			return GuidanceSourceReviewResult{}, existingErr
		} else if _, err = tx.Exec(ctx, `INSERT INTO management_guidance_snapshots(id,asset_id,metric,fiscal_period,fiscal_period_end,accounting_basis,low_value,high_value,currency,unit,published_at,available_at,revision_at,source_name,source_url,source_document_id,source_payload,retrieved_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, value.ID, value.AssetID, value.Metric, value.FiscalPeriod, value.FiscalPeriodEnd, value.AccountingBasis, value.LowValue, value.HighValue, value.Currency, value.Unit, value.PublishedAt, value.AvailableAt, value.RevisionAt, value.SourceName, value.SourceURL, value.SourceDocumentID, payload, value.RetrievedAt); err != nil {
			return GuidanceSourceReviewResult{}, fmt.Errorf("save confirmed management guidance: %w", err)
		}
		guidanceSnapshot = &value
	}

	review := GuidanceSourceReview{ID: consensusID(input.AssetID, input.SourceDocumentID, input.IdempotencyKey), Decision: input.Decision, Notes: input.Notes, ReviewedBy: input.ReviewedBy, ReviewedAt: input.ReviewedAt.UTC()}
	if guidanceSnapshot != nil {
		review.GuidanceSnapshotID = &guidanceSnapshot.ID
		review.EvidenceURL, review.EvidenceLocation, review.EvidenceExcerpt = &input.EvidenceURL, &input.EvidenceLocation, &input.EvidenceExcerpt
	}
	if _, err = tx.Exec(ctx, `INSERT INTO guidance_source_reviews(id,asset_id,source_document_id,decision,guidance_snapshot_id,evidence_url,evidence_location,evidence_excerpt,notes,reviewed_by,reviewed_at,idempotency_key,request_hash)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, review.ID, input.AssetID, input.SourceDocumentID, review.Decision, review.GuidanceSnapshotID, review.EvidenceURL, review.EvidenceLocation, review.EvidenceExcerpt, review.Notes, review.ReviewedBy, review.ReviewedAt, input.IdempotencyKey, requestHash); err != nil {
		return GuidanceSourceReviewResult{}, fmt.Errorf("save guidance source review: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return GuidanceSourceReviewResult{}, err
	}
	return GuidanceSourceReviewResult{Created: true, Review: review, GuidanceSnapshot: guidanceSnapshot}, nil
}

func equalNullableFloat(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func normalizeGuidanceSource(value *GuidanceSourceDocument) error {
	value.AssetID, value.Provider, value.CIK = strings.TrimSpace(value.AssetID), strings.ToLower(strings.TrimSpace(value.Provider)), strings.TrimSpace(value.CIK)
	value.AccessionNumber, value.Form = strings.TrimSpace(value.AccessionNumber), strings.ToUpper(strings.TrimSpace(value.Form))
	value.FilingIndexURL, value.PrimaryDocument, value.PrimaryDocumentURL = strings.TrimSpace(value.FilingIndexURL), strings.TrimSpace(value.PrimaryDocument), strings.TrimSpace(value.PrimaryDocumentURL)
	if value.AssetID == "" || value.Provider != "sec_edgar" || len(value.CIK) != 10 || !validSECAccession(value.AccessionNumber) || !guidanceCandidateForm(value.Form) || value.FilingDate.IsZero() || value.AcceptedAt.IsZero() || value.SourceAvailableAt.IsZero() || value.FirstObservedAt.IsZero() || value.FilingIndexURL == "" || value.SourcePayload == nil {
		return errors.New("guidance source document has missing or invalid SEC metadata")
	}
	if value.SourceAvailableAt.Before(value.AcceptedAt) || value.FirstObservedAt.Before(value.SourceAvailableAt) {
		return errors.New("SEC source availability cannot precede acceptance or first observation")
	}
	value.FilingDate, value.AcceptedAt, value.SourceAvailableAt, value.FirstObservedAt = dateUTC(value.FilingDate), value.AcceptedAt.UTC(), value.SourceAvailableAt.UTC(), value.FirstObservedAt.UTC()
	if value.ReportDate != nil {
		date := dateUTC(*value.ReportDate)
		value.ReportDate = &date
	}
	if value.ID == "" {
		value.ID = consensusID(value.AssetID, value.Provider, value.AccessionNumber)
	}
	return nil
}

func guidanceSourceByID(ctx context.Context, tx pgx.Tx, assetID, documentID string) (GuidanceSourceDocument, error) {
	var value GuidanceSourceDocument
	err := tx.QueryRow(ctx, `SELECT id,asset_id,provider,cik,accession_number,form,filing_date,report_date,accepted_at,source_available_at,first_observed_at,filing_index_url,primary_document,primary_document_url FROM guidance_source_documents WHERE id=$1 AND asset_id=$2`, documentID, assetID).Scan(&value.ID, &value.AssetID, &value.Provider, &value.CIK, &value.AccessionNumber, &value.Form, &value.FilingDate, &value.ReportDate, &value.AcceptedAt, &value.SourceAvailableAt, &value.FirstObservedAt, &value.FilingIndexURL, &value.PrimaryDocument, &value.PrimaryDocumentURL)
	if errors.Is(err, pgx.ErrNoRows) {
		return value, errors.New("guidance source document was not found for asset")
	}
	return value, err
}

type storedReview struct {
	review      GuidanceSourceReview
	requestHash string
}

func reviewByIdempotencyKey(ctx context.Context, tx pgx.Tx, key string) (storedReview, bool, error) {
	var value storedReview
	var snapshotID, evidenceURL, evidenceLocation, evidenceExcerpt sql.NullString
	err := tx.QueryRow(ctx, `SELECT id,decision,guidance_snapshot_id,evidence_url,evidence_location,evidence_excerpt,notes,reviewed_by,reviewed_at,request_hash FROM guidance_source_reviews WHERE idempotency_key=$1`, key).Scan(&value.review.ID, &value.review.Decision, &snapshotID, &evidenceURL, &evidenceLocation, &evidenceExcerpt, &value.review.Notes, &value.review.ReviewedBy, &value.review.ReviewedAt, &value.requestHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return value, false, nil
	}
	if err != nil {
		return value, false, err
	}
	value.review.GuidanceSnapshotID = nullableString(snapshotID)
	value.review.EvidenceURL, value.review.EvidenceLocation, value.review.EvidenceExcerpt = nullableString(evidenceURL), nullableString(evidenceLocation), nullableString(evidenceExcerpt)
	return value, true, nil
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func evidenceURLBelongsToDocument(raw string, document GuidanceSourceDocument) bool {
	evidence, evidenceErr := url.Parse(strings.TrimSpace(raw))
	indexURL, indexErr := url.Parse(document.FilingIndexURL)
	if evidenceErr != nil || indexErr != nil || evidence.Scheme != "https" || !strings.EqualFold(evidence.Hostname(), indexURL.Hostname()) {
		return false
	}
	prefix := indexURL.Path
	if slash := strings.LastIndex(prefix, "/"); slash >= 0 {
		prefix = prefix[:slash+1]
	}
	return strings.HasPrefix(evidence.EscapedPath(), (&url.URL{Path: prefix}).EscapedPath()) && evidence.RawQuery == "" && evidence.Fragment == ""
}
