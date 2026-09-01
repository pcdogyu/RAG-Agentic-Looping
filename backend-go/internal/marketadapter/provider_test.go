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
		TencentHKURL: server.URL + "/prices-hk", FundamentalsURL: server.URL + "/fundamentals",
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
	if err != nil || len(prices) != 1 || prices[0]["close"] != 9.35 {
		t.Fatalf("prices=%#v error=%v", prices, err)
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
