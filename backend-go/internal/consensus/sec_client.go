package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SECDisclosureContractVersion = "sec-guidance-source-document-v1"
	defaultSECFilesBaseURL       = "https://www.sec.gov"
	defaultSECDataBaseURL        = "https://data.sec.gov"
)

// SECClient discovers official issuer filings that may contain management
// guidance. A filing is only a review candidate: this client never extracts a
// numeric range or creates a guidance snapshot.
type SECClient struct {
	FilesBaseURL     string
	DataBaseURL      string
	Identity         string
	HTTPClient       *http.Client
	DisableRateLimit bool
}

var secRequestGate struct {
	sync.Mutex
	last time.Time
}

type secTicker struct {
	CIK    int64  `json:"cik_str"`
	Ticker string `json:"ticker"`
	Title  string `json:"title"`
}

type secSubmissions struct {
	CIK     string `json:"cik"`
	Name    string `json:"name"`
	Filings struct {
		Recent struct {
			AccessionNumber       []string `json:"accessionNumber"`
			FilingDate            []string `json:"filingDate"`
			ReportDate            []string `json:"reportDate"`
			AcceptanceDateTime    []string `json:"acceptanceDateTime"`
			Form                  []string `json:"form"`
			PrimaryDocument       []string `json:"primaryDocument"`
			PrimaryDocDescription []string `json:"primaryDocDescription"`
		} `json:"recent"`
	} `json:"filings"`
}

func (client SECClient) FetchGuidanceSources(ctx context.Context, assetID, symbol string, limit int, observedAt time.Time) ([]GuidanceSourceDocument, error) {
	assetID, symbol, identity := strings.TrimSpace(assetID), strings.ToUpper(strings.TrimSpace(symbol)), strings.TrimSpace(client.Identity)
	if assetID == "" || symbol == "" || observedAt.IsZero() {
		return nil, errors.New("SEC guidance source sync requires asset_id, symbol, identity and observed_at")
	}
	if err := ValidateSECIdentity(identity); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 200 {
		return nil, errors.New("SEC guidance source limit must be between 1 and 200")
	}
	filesBase, dataBase := strings.TrimRight(client.FilesBaseURL, "/"), strings.TrimRight(client.DataBaseURL, "/")
	if filesBase == "" {
		filesBase = defaultSECFilesBaseURL
	}
	if dataBase == "" {
		dataBase = defaultSECDataBaseURL
	}
	tickers := map[string]secTicker{}
	if err := client.getJSON(ctx, filesBase+"/files/company_tickers.json", identity, &tickers); err != nil {
		return nil, fmt.Errorf("fetch SEC company ticker directory: %w", err)
	}
	var issuer secTicker
	for _, item := range tickers {
		if strings.EqualFold(strings.TrimSpace(item.Ticker), symbol) {
			issuer = item
			break
		}
	}
	if issuer.CIK <= 0 {
		return nil, fmt.Errorf("SEC CIK was not found for symbol %q", symbol)
	}
	cik := fmt.Sprintf("%010d", issuer.CIK)
	var submissions secSubmissions
	if err := client.getJSON(ctx, dataBase+"/submissions/CIK"+cik+".json", identity, &submissions); err != nil {
		return nil, fmt.Errorf("fetch SEC submissions for CIK %s: %w", cik, err)
	}
	if strings.TrimLeft(submissions.CIK, "0") != strings.TrimLeft(cik, "0") {
		return nil, fmt.Errorf("SEC submissions CIK %q does not match requested CIK %q", submissions.CIK, cik)
	}
	recent := submissions.Filings.Recent
	requiredLength := len(recent.AccessionNumber)
	for field, length := range map[string]int{
		"filingDate": len(recent.FilingDate), "reportDate": len(recent.ReportDate),
		"acceptanceDateTime": len(recent.AcceptanceDateTime), "form": len(recent.Form),
		"primaryDocument": len(recent.PrimaryDocument),
	} {
		if length != requiredLength {
			return nil, fmt.Errorf("SEC submissions recent arrays differ: accessionNumber=%d %s=%d", requiredLength, field, length)
		}
	}
	items := make([]GuidanceSourceDocument, 0, min(limit, requiredLength))
	for index := 0; index < requiredLength && len(items) < limit; index++ {
		form := strings.ToUpper(strings.TrimSpace(recent.Form[index]))
		if !guidanceCandidateForm(form) {
			continue
		}
		accession := strings.TrimSpace(recent.AccessionNumber[index])
		if !validSECAccession(accession) {
			continue
		}
		filingDate, filingErr := time.Parse("2006-01-02", strings.TrimSpace(recent.FilingDate[index]))
		acceptedAt, acceptedErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(recent.AcceptanceDateTime[index]))
		if filingErr != nil || acceptedErr != nil {
			continue
		}
		var reportDate *time.Time
		if parsed, parseErr := time.Parse("2006-01-02", strings.TrimSpace(recent.ReportDate[index])); parseErr == nil {
			parsed = dateUTC(parsed)
			reportDate = &parsed
		}
		accessionPath := strings.ReplaceAll(accession, "-", "")
		archivePrefix := filesBase + "/Archives/edgar/data/" + strconv.FormatInt(issuer.CIK, 10) + "/" + accessionPath + "/"
		primaryDocument := strings.TrimSpace(recent.PrimaryDocument[index])
		primaryURL := ""
		if primaryDocument != "" {
			primaryURL = archivePrefix + url.PathEscape(primaryDocument)
		}
		indexURL := archivePrefix + accession + "-index.html"
		description := ""
		if index < len(recent.PrimaryDocDescription) {
			description = strings.TrimSpace(recent.PrimaryDocDescription[index])
		}
		payload := map[string]any{
			"contract_version": SECDisclosureContractVersion, "provider": "sec_edgar", "issuer_name": submissions.Name,
			"directory_issuer_title": issuer.Title, "cik": cik, "accession_number": accession, "form": form,
			"filing_date": filingDate.Format("2006-01-02"), "acceptance_datetime": acceptedAt.UTC().Format(time.RFC3339Nano),
			"primary_document": primaryDocument, "primary_document_description": description,
			"provider_publication_time_available": true, "provider_dissemination_time_available": false,
			"source_available_at_basis": "first_observed_at", "historical_backfill_supported": false,
			"guidance_extraction_status": "not_attempted_human_review_required",
			"automatic_guidance":         false, "automatic_rating": false,
		}
		if reportDate != nil {
			payload["report_date"] = reportDate.Format("2006-01-02")
		}
		firstObservedAt := observedAt.UTC()
		if firstObservedAt.Before(acceptedAt) {
			firstObservedAt = acceptedAt.UTC()
		}
		item := GuidanceSourceDocument{
			AssetID: assetID, Provider: "sec_edgar", CIK: cik, AccessionNumber: accession, Form: form,
			FilingDate: dateUTC(filingDate), ReportDate: reportDate, AcceptedAt: acceptedAt.UTC(), SourceAvailableAt: firstObservedAt,
			FirstObservedAt: firstObservedAt, FilingIndexURL: indexURL, PrimaryDocument: primaryDocument,
			PrimaryDocumentURL: primaryURL, SourcePayload: payload,
		}
		item.ID = consensusID(item.AssetID, item.Provider, item.AccessionNumber)
		items = append(items, item)
	}
	return items, nil
}

func ValidateSECIdentity(value string) error {
	value = strings.TrimSpace(value)
	if len(value) < 8 || len(value) > 256 || !strings.Contains(value, "@") || !strings.Contains(value, " ") {
		return errors.New("SEC identity must identify an organization or application and include a contact email")
	}
	return nil
}

func (client SECClient) getJSON(ctx context.Context, target, identity string, output any) error {
	if err := client.waitForFairAccess(ctx); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	request.Header.Set("User-Agent", identity)
	request.Header.Set("Accept", "application/json")
	httpClient := client.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body[:min(len(body), 300)])))
	}
	if err := json.Unmarshal(body, output); err != nil {
		return fmt.Errorf("decode SEC JSON: %w", err)
	}
	return nil
}

func (client SECClient) waitForFairAccess(ctx context.Context) error {
	if client.DisableRateLimit {
		return nil
	}
	secRequestGate.Lock()
	defer secRequestGate.Unlock()
	wait := 125*time.Millisecond - time.Since(secRequestGate.last)
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	secRequestGate.last = time.Now()
	return nil
}

func guidanceCandidateForm(form string) bool {
	base := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(form)), "/A")
	switch base {
	case "8-K", "10-Q", "10-K", "6-K", "20-F", "40-F":
		return true
	default:
		return false
	}
}

func validSECAccession(value string) bool {
	if len(value) != 20 || value[10] != '-' || value[13] != '-' {
		return false
	}
	for index, char := range value {
		if index == 10 || index == 13 {
			continue
		}
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
