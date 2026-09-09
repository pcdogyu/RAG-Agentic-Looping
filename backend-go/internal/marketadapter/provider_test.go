package marketadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProviderNormalizesUniversePricesFundamentalsAndNews(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/sina/Market_Center.getHQNodeData"):
			if r.URL.Query().Get("page") == "1" {
				_, _ = w.Write([]byte(`[{"symbol":"sh600000","code":"600000","name":"浦发银行"},{"symbol":"bj920000","code":"920000","name":"北交所公司"}]`))
			} else {
				_, _ = w.Write([]byte(`[]`))
			}
		case strings.HasPrefix(r.URL.Path, "/sina/Market_Center.getHKStockData"):
			if r.URL.Query().Get("page") == "1" {
				_, _ = w.Write([]byte(`[{"symbol":"09988","name":"阿里巴巴"}]`))
			} else {
				_, _ = w.Write([]byte(`[]`))
			}
		case r.URL.Path == "/prices-cn":
			_, _ = w.Write([]byte(`{"code":0,"msg":"","data":{"sh600000":{"qfqday":[["2026-08-31","9.01","9.16","9.18","9.00","996825"],["2026-09-01","9.13","9.35","9.36","9.10","1026696"]]}}}`))
		case r.URL.Path == "/csi-index":
			if r.URL.Query().Get("indexCode") != "H00300" || r.URL.Query().Get("startDate") != "20260901" || r.URL.Query().Get("endDate") != "20260901" {
				t.Errorf("unexpected CSI total-return query: %s", r.URL.RawQuery)
			}
			if r.Header.Get("Referer") != "https://www.csindex.com.cn/" || r.Header.Get("X-Requested-With") != "XMLHttpRequest" {
				t.Errorf("missing CSI request identity headers: %#v", r.Header)
			}
			_, _ = w.Write([]byte(`{"code":"200","data":[{"tradeDate":"20260901","indexCode":"H00300","indexNameCnAll":"沪深300全收益指数","indexNameEnAll":"CSI 300 Total Return Index","close":6801.25}]}`))
		case r.URL.Path == "/fundamentals":
			if got := r.URL.Query().Get("filter"); got != `(SECURITY_CODE="600000")` {
				t.Errorf("filter=%q", got)
			}
			_, _ = w.Write([]byte(`{"success":true,"result":{"data":[{"SECURITY_CODE":"600000","EPSJB":0.89}]}}`))
		case r.URL.Path == "/news":
			_, _ = w.Write([]byte(`{"data":{"fastNewsList":[{"title":"新消息","summary":"摘要","showTime":"2026-09-01 08:00:00","code":"20260901001"},{"title":"旧消息","summary":"摘要","showTime":"2026-08-31 08:00:00","code":"20260831001"}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	provider := NewProvider(server.Client(), ProviderConfig{
		SinaUniverseURL: server.URL + "/sina", TencentChinaURL: server.URL + "/prices-cn",
		TencentHKURL: server.URL + "/prices-hk", CSIIndexURL: server.URL + "/csi-index", FundamentalsURL: server.URL + "/fundamentals",
		NewsURL: server.URL + "/news", Now: func() time.Time { return time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC) },
	})
	assets, err := provider.Universe(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := []string{}
	for _, asset := range assets {
		gotIDs = append(gotIDs, asset.AssetID)
	}
	wantIDs := []string{"equity:XSHG:600000", "equity:XBEI:920000", "equity:XHKG:09988"}
	if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("ids=%v", gotIDs)
	}

	prices, err := provider.Prices(context.Background(), PriceRequest{Symbol: "600000", Market: "CN", Start: "2026-09-01", End: "2026-09-01"})
	if err != nil || len(prices) != 1 || prices[0]["adjusted_close"] != 9.35 || prices[0]["price_field"] != "adjusted_close" {
		t.Fatalf("prices=%#v error=%v", prices, err)
	}
	if prices[0]["source_name"] != "Tencent Finance" || prices[0]["source_url"] != server.URL+"/prices-cn" || prices[0]["source_document_id"] != "tencent-kline:sh600000" {
		t.Fatalf("price source lineage is incomplete: %#v", prices[0])
	}
	csiPrices, err := provider.Prices(context.Background(), PriceRequest{Symbol: "H00300", Market: "CN", Start: "2026-09-01", End: "2026-09-01"})
	if err != nil || len(csiPrices) != 1 || csiPrices[0]["adjusted_close"] != 6801.25 || csiPrices[0]["price_field"] != "adjusted_close" || csiPrices[0]["return_series_kind"] != "gross_total_return_index" {
		t.Fatalf("official CSI total-return prices=%#v error=%v", csiPrices, err)
	}
	if csiPrices[0]["source_name"] != "China Securities Index Co., Ltd." || csiPrices[0]["source_url"] != server.URL+"/csi-index" || csiPrices[0]["source_document_id"] != "csindex-total-return:H00300" {
		t.Fatalf("CSI total-return lineage is incomplete: %#v", csiPrices[0])
	}

	fundamentals, unsupported, err := provider.Fundamentals(context.Background(), "600000", "CN")
	if err != nil || unsupported || len(fundamentals) != 1 || fmt.Sprint(fundamentals[0]["EPSJB"]) != "0.89" {
		t.Fatalf("fundamentals=%#v unsupported=%v error=%v", fundamentals, unsupported, err)
	}
	_, unsupported, err = provider.Fundamentals(context.Background(), "09988", "HK")
	if err != nil || !unsupported {
		t.Fatalf("unsupported=%v error=%v", unsupported, err)
	}

	news, err := provider.News(context.Background(), NewsRequest{Since: "2026-09-01T00:00:00Z", Limit: 10})
	if err != nil || len(news) != 1 || news[0]["published_at"] != "2026-09-01T00:00:00Z" {
		encoded, _ := json.Marshal(news)
		t.Fatalf("news=%s error=%v", encoded, err)
	}
}

func TestNormalizeSinaAssetRejectsInvalidRows(t *testing.T) {
	if _, ok := normalizeSinaAsset(map[string]any{"code": "not-a-code", "name": "invalid"}, "CN"); ok {
		t.Fatal("invalid security code accepted")
	}
}

func TestProviderSourceURLRemovesCredentialsAndQuery(t *testing.T) {
	got := providerSourceURL("https://user:secret@example.test/prices?token=secret#fragment")
	if got != "https://example.test/prices" {
		t.Fatalf("unsafe provider source URL: %s", got)
	}
	if endpoint := DefaultProviderConfig().TencentChinaURL; !strings.HasSuffix(endpoint, "/appstock/app/newfqkline/get") {
		t.Fatalf("stale Tencent China price endpoint: %s", endpoint)
	}
	if endpoint := DefaultProviderConfig().CSIIndexURL; !strings.HasSuffix(endpoint, "/csindex-home/perf/index-perf") {
		t.Fatalf("unexpected CSI total-return endpoint: %s", endpoint)
	}
}

func TestCSITotalReturnRejectsMismatchedIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":"200","data":[{"tradeDate":"20260901","indexCode":"000300","close":4000.0}]}`))
	}))
	defer server.Close()
	provider := NewProvider(server.Client(), ProviderConfig{CSIIndexURL: server.URL, Now: func() time.Time { return time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC) }})
	if prices, err := provider.Prices(context.Background(), PriceRequest{Symbol: "H00300", Market: "CN", Start: "2026-09-01", End: "2026-09-01"}); err == nil || prices != nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("mismatched price-index row was accepted: prices=%#v err=%v", prices, err)
	}
}
