package consensus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSECClientDiscoversOfficialFilingsWithoutExtractingGuidance(t *testing.T) {
	identity := "RAG Agentic Looping test admin@example.test"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") != identity || r.Header.Get("Accept") != "application/json" {
			t.Fatalf("SEC request headers=%#v", r.Header)
		}
		switch r.URL.Path {
		case "/files/company_tickers.json":
			_ = json.NewEncoder(w).Encode(map[string]any{"0": map[string]any{"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"}})
		case "/submissions/CIK0001045810.json":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"cik": "0001045810", "name": "NVIDIA CORP", "filings": map[string]any{"recent": map[string]any{
					"accessionNumber":    []string{"0001045810-26-000111", "0001045810-26-000110", "0001045810-26-000109"},
					"filingDate":         []string{"2026-08-27", "2026-08-26", "2026-08-25"},
					"reportDate":         []string{"2026-07-26", "", "2026-07-26"},
					"acceptanceDateTime": []string{"2026-08-27T16:20:22.000Z", "2026-08-26T12:00:00.000Z", "2026-08-25T12:00:00.000Z"},
					"form":               []string{"8-K", "4", "10-Q/A"}, "primaryDocument": []string{"nvda-20260827.htm", "ownership.xml", "nvda 10qa.htm"},
					"primaryDocDescription": []string{"CURRENT REPORT", "FORM 4", "AMENDMENT"},
				}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	observedAt := time.Date(2026, 9, 10, 1, 2, 3, 0, time.UTC)
	client := SECClient{FilesBaseURL: server.URL, DataBaseURL: server.URL, Identity: identity, HTTPClient: server.Client(), DisableRateLimit: true}
	items, err := client.FetchGuidanceSources(context.Background(), "equity:NASDAQ:NVDA", "nvda", 20, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Form != "8-K" || items[1].Form != "10-Q/A" {
		t.Fatalf("unexpected filing candidates: %#v", items)
	}
	first := items[0]
	if first.CIK != "0001045810" || first.AccessionNumber != "0001045810-26-000111" || first.ReportDate == nil || first.ReportDate.Format("2006-01-02") != "2026-07-26" {
		t.Fatalf("unexpected SEC identity: %#v", first)
	}
	if !first.AcceptedAt.Equal(time.Date(2026, 8, 27, 16, 20, 22, 0, time.UTC)) || !first.SourceAvailableAt.Equal(observedAt) || !first.FirstObservedAt.Equal(observedAt) {
		t.Fatalf("publication/observation times were conflated: %#v", first)
	}
	if !strings.Contains(first.FilingIndexURL, "/1045810/000104581026000111/0001045810-26-000111-index.html") || !strings.HasSuffix(first.PrimaryDocumentURL, "/nvda-20260827.htm") {
		t.Fatalf("unexpected archive URLs: %#v", first)
	}
	if first.SourcePayload["guidance_extraction_status"] != "not_attempted_human_review_required" || first.SourcePayload["automatic_guidance"] != false || first.SourcePayload["provider_publication_time_available"] != true || first.SourcePayload["provider_dissemination_time_available"] != false || first.SourcePayload["historical_backfill_supported"] != false {
		t.Fatalf("unsafe SEC candidate contract: %#v", first.SourcePayload)
	}
}

func TestSECClientRejectsMissingIdentityAndMalformedParallelArrays(t *testing.T) {
	client := SECClient{}
	if _, err := client.FetchGuidanceSources(context.Background(), "asset", "NVDA", 10, time.Now()); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("missing identity error=%v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "company_tickers") {
			_, _ = w.Write([]byte(`{"0":{"cik_str":1045810,"ticker":"NVDA","title":"NVIDIA"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"cik":"1045810","filings":{"recent":{"accessionNumber":["0001045810-26-000111"],"filingDate":[],"reportDate":["2026-07-26"],"acceptanceDateTime":["2026-08-27T16:20:22Z"],"form":["8-K"],"primaryDocument":["nvda.htm"]}}}`))
	}))
	defer server.Close()
	client = SECClient{FilesBaseURL: server.URL, DataBaseURL: server.URL, Identity: "test test@example.test", HTTPClient: server.Client(), DisableRateLimit: true}
	if _, err := client.FetchGuidanceSources(context.Background(), "asset", "NVDA", 10, time.Now()); err == nil || !strings.Contains(err.Error(), "arrays differ") {
		t.Fatalf("parallel array error=%v", err)
	}
}

func TestEvidenceURLMustStayInsideSelectedSECAccession(t *testing.T) {
	document := GuidanceSourceDocument{FilingIndexURL: "https://www.sec.gov/Archives/edgar/data/1045810/000104581026000111/0001045810-26-000111-index.html"}
	if !evidenceURLBelongsToDocument("https://www.sec.gov/Archives/edgar/data/1045810/000104581026000111/ex99-1.htm", document) {
		t.Fatal("same accession exhibit was rejected")
	}
	for _, candidate := range []string{
		"https://example.com/Archives/edgar/data/1045810/000104581026000111/ex99-1.htm",
		"https://www.sec.gov/Archives/edgar/data/1045810/other/ex99-1.htm",
		"http://www.sec.gov/Archives/edgar/data/1045810/000104581026000111/ex99-1.htm",
		"https://www.sec.gov/Archives/edgar/data/1045810/000104581026000111/ex99-1.htm?token=x",
	} {
		if evidenceURLBelongsToDocument(candidate, document) {
			t.Fatalf("unsafe evidence URL accepted: %s", candidate)
		}
	}
}
