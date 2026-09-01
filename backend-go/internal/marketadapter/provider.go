package marketadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const userAgent = "Mozilla/5.0 (compatible; RAG-Agentic-Looping/1.0; +https://github.com/pcdogyu/RAG-Agentic-Looping)"

type ProviderConfig struct {
	SinaUniverseURL string
	TencentChinaURL string
	TencentHKURL    string
	FundamentalsURL string
	NewsURL         string
	Now             func() time.Time
}

func DefaultProviderConfig() ProviderConfig {
	return ProviderConfig{
		SinaUniverseURL: "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php",
		TencentChinaURL: "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
		TencentHKURL:    "https://web.ifzq.gtimg.cn/appstock/app/hkfqkline/get",
		FundamentalsURL: "https://datacenter-web.eastmoney.com/api/data/v1/get",
		NewsURL:         "https://np-weblist.eastmoney.com/comm/web/getFastNewsList",
		Now:             time.Now,
	}
}

type EastAsiaProvider struct {
	client *http.Client
	cfg    ProviderConfig
}

func NewProvider(client *http.Client, cfg ProviderConfig) *EastAsiaProvider {
	if client == nil {
		client = &http.Client{Timeout: 90 * time.Second}
	}
	defaults := DefaultProviderConfig()
	if cfg.SinaUniverseURL == "" {
		cfg.SinaUniverseURL = defaults.SinaUniverseURL
	}
	if cfg.TencentChinaURL == "" {
		cfg.TencentChinaURL = defaults.TencentChinaURL
	}
	if cfg.TencentHKURL == "" {
		cfg.TencentHKURL = defaults.TencentHKURL
	}
	if cfg.FundamentalsURL == "" {
		cfg.FundamentalsURL = defaults.FundamentalsURL
	}
	if cfg.NewsURL == "" {
		cfg.NewsURL = defaults.NewsURL
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &EastAsiaProvider{client: client, cfg: cfg}
}

func (p *EastAsiaProvider) Universe(ctx context.Context, market string) ([]Asset, error) {
	markets := []string{market}
	if market == "" {
		markets = []string{"CN", "HK"}
	}
	type result struct {
		market string
		items  []Asset
		err    error
	}
	results := make(chan result, len(markets))
	for _, requested := range markets {
		go func(current string) {
			items, err := p.sinaUniverse(ctx, current)
			results <- result{market: current, items: items, err: err}
		}(requested)
	}
	byMarket := map[string][]Asset{}
	for range markets {
		outcome := <-results
		if outcome.err != nil {
			return nil, outcome.err
		}
		byMarket[outcome.market] = outcome.items
	}
	items := make([]Asset, 0, len(byMarket["CN"])+len(byMarket["HK"]))
	for _, current := range markets {
		items = append(items, byMarket[current]...)
	}
	return items, nil
}

func (p *EastAsiaProvider) sinaUniverse(ctx context.Context, market string) ([]Asset, error) {
	endpoint, node, pageSize := "Market_Center.getHQNodeData", "hs_a", 100
	if market == "HK" {
		endpoint, node, pageSize = "Market_Center.getHKStockData", "qbgg_hk", 60
	}
	const pageConcurrency = 8
	items := []Asset{}
	for firstPage := 1; firstPage <= 100; firstPage += pageConcurrency {
		type pageResult struct {
			page int
			rows []map[string]any
			err  error
		}
		results := make(chan pageResult, pageConcurrency)
		var group sync.WaitGroup
		for page := firstPage; page < firstPage+pageConcurrency; page++ {
			group.Add(1)
			go func(current int) {
				defer group.Done()
				rows, err := p.sinaPage(ctx, endpoint, node, current, pageSize)
				results <- pageResult{page: current, rows: rows, err: err}
			}(page)
		}
		group.Wait()
		close(results)
		pages := make([]pageResult, 0, pageConcurrency)
		for outcome := range results {
			if outcome.err != nil {
				return nil, outcome.err
			}
			pages = append(pages, outcome)
		}
		sort.Slice(pages, func(i, j int) bool { return pages[i].page < pages[j].page })
		finished := false
		for _, page := range pages {
			for _, row := range page.rows {
				if asset, ok := normalizeSinaAsset(row, market); ok {
					items = append(items, asset)
				}
			}
			if len(page.rows) < pageSize {
				finished = true
				break
			}
		}
		if finished {
			break
		}
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("Sina %s universe returned no assets", market)
	}
	return items, nil
}

func (p *EastAsiaProvider) sinaPage(ctx context.Context, endpoint, node string, page, pageSize int) ([]map[string]any, error) {
	query := url.Values{
		"page": {strconv.Itoa(page)}, "num": {strconv.Itoa(pageSize)}, "sort": {"symbol"},
		"asc": {"1"}, "node": {node}, "_s_r_a": {"init"},
	}
	var rows []map[string]any
	if err := p.getJSON(ctx, p.cfg.SinaUniverseURL+"/"+endpoint+"?"+query.Encode(), &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func normalizeSinaAsset(row map[string]any, market string) (Asset, bool) {
	code := strings.TrimSpace(stringValue(row["code"]))
	if code == "" {
		code = strings.TrimSpace(stringValue(row["symbol"]))
		code = strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(code, "sh"), "sz"), "bj")
	}
	name := strings.TrimSpace(stringValue(row["name"]))
	if code == "" || name == "" || !digits(code) {
		return Asset{}, false
	}
	exchange, currency, width := "XSHE", "CNY", 6
	if market == "HK" {
		exchange, currency, width = "XHKG", "HKD", 5
	} else if strings.HasPrefix(code, "6") {
		exchange = "XSHG"
	} else if strings.Contains("489", code[:1]) || strings.HasPrefix(code, "92") {
		exchange = "XBEI"
	}
	code = leftPad(code, width)
	return Asset{
		AssetID: "equity:" + exchange + ":" + code, AssetClass: "equity", Market: market,
		Symbol: code, Name: name, Exchange: exchange, Currency: currency, Aliases: []string{},
		InstrumentType: "common_stock", LotSize: 100, Active: true,
	}, true
}

func (p *EastAsiaProvider) Prices(ctx context.Context, request PriceRequest) ([]map[string]any, error) {
	now := p.cfg.Now().UTC()
	start := normalizeDate(request.Start, now.AddDate(-1, 0, 0))
	end := normalizeDate(request.End, now)
	prefix := "sz"
	endpoint := p.cfg.TencentChinaURL
	if request.Market == "HK" {
		prefix, endpoint = "hk", p.cfg.TencentHKURL
	} else if strings.HasPrefix(request.Symbol, "6") {
		prefix = "sh"
	} else if strings.Contains("489", request.Symbol[:1]) || strings.HasPrefix(request.Symbol, "92") {
		prefix = "bj"
	}
	key := prefix + request.Symbol
	query := url.Values{"param": {key + ",day,,,1024,qfq"}}
	var envelope struct {
		Code int                        `json:"code"`
		Msg  string                     `json:"msg"`
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := p.getJSON(ctx, endpoint+"?"+query.Encode(), &envelope); err != nil {
		return nil, err
	}
	if envelope.Code != 0 {
		return nil, fmt.Errorf("Tencent price provider: %s", envelope.Msg)
	}
	var series map[string]json.RawMessage
	if err := json.Unmarshal(envelope.Data[key], &series); err != nil {
		return nil, fmt.Errorf("Tencent price payload: %w", err)
	}
	raw := series["qfqday"]
	if len(raw) == 0 {
		raw = series["day"]
	}
	var rows [][]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, fmt.Errorf("Tencent price rows: %w", err)
		}
	}
	items := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		if len(row) < 6 {
			continue
		}
		day, err := time.Parse("2006-01-02", stringValue(row[0]))
		if err != nil || day.Before(start) || day.After(end) {
			continue
		}
		items = append(items, map[string]any{
			"date": day.Format("2006-01-02"), "symbol": request.Symbol,
			"open": numberValue(row[1]), "close": numberValue(row[2]), "high": numberValue(row[3]),
			"low": numberValue(row[4]), "volume": numberValue(row[5]),
		})
	}
	return items, nil
}

func (p *EastAsiaProvider) Fundamentals(ctx context.Context, symbol, market string) ([]map[string]any, bool, error) {
	if market != "CN" {
		return []map[string]any{}, true, nil
	}
	query := url.Values{
		"reportName": {"RPT_F10_FINANCE_MAINFINADATA"}, "columns": {"ALL"},
		"filter": {fmt.Sprintf(`(SECURITY_CODE="%s")`, symbol)}, "pageNumber": {"1"},
		"pageSize": {"100"}, "sortTypes": {"-1"}, "sortColumns": {"REPORT_DATE"},
	}
	var envelope struct {
		Success bool `json:"success"`
		Result  *struct {
			Data []map[string]any `json:"data"`
		} `json:"result"`
	}
	if err := p.getJSON(ctx, p.cfg.FundamentalsURL+"?"+query.Encode(), &envelope); err != nil {
		return nil, false, err
	}
	if envelope.Result == nil {
		return []map[string]any{}, false, nil
	}
	return envelope.Result.Data, false, nil
}

func (p *EastAsiaProvider) News(ctx context.Context, request NewsRequest) ([]map[string]any, error) {
	query := url.Values{
		"client": {"web"}, "biz": {"web_724"}, "fastColumn": {"102"}, "sortEnd": {""},
		"pageSize": {strconv.Itoa(request.Limit * 3)}, "req_trace": {strconv.FormatInt(p.cfg.Now().UnixMilli(), 10)},
	}
	var envelope struct {
		Data struct {
			Items []struct {
				Title    string `json:"title"`
				Summary  string `json:"summary"`
				ShowTime string `json:"showTime"`
				Code     string `json:"code"`
			} `json:"fastNewsList"`
		} `json:"data"`
	}
	if err := p.getJSON(ctx, p.cfg.NewsURL+"?"+query.Encode(), &envelope); err != nil {
		return nil, err
	}
	since := time.Time{}
	if strings.TrimSpace(request.Since) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.Replace(request.Since, "Z", "+00:00", 1))
		if err != nil {
			return nil, fmt.Errorf("invalid since: %w", err)
		}
		since = parsed.UTC()
	}
	location := time.FixedZone("Asia/Shanghai", 8*60*60)
	items := make([]map[string]any, 0, request.Limit)
	for _, raw := range envelope.Data.Items {
		if strings.TrimSpace(raw.Title) == "" || strings.TrimSpace(raw.Code) == "" {
			continue
		}
		published, err := time.ParseInLocation("2006-01-02 15:04:05", raw.ShowTime, location)
		if err != nil {
			published = p.cfg.Now().UTC()
		}
		published = published.UTC()
		if !since.IsZero() && published.Before(since) {
			continue
		}
		items = append(items, map[string]any{
			"source": "东方财富", "title": raw.Title, "summary": raw.Summary,
			"url":          "https://finance.eastmoney.com/a/" + raw.Code + ".html",
			"published_at": published.Format(time.RFC3339), "language": "zh",
		})
		if len(items) == request.Limit {
			break
		}
	}
	return items, nil
}

func (p *EastAsiaProvider) getJSON(ctx context.Context, endpoint string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("User-Agent", userAgent)
	request.Header.Set("Accept", "application/json,text/plain,*/*")
	response, err := p.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return fmt.Errorf("provider HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<20))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode provider response: %w", err)
	}
	return nil
}

func normalizeDate(raw string, fallback time.Time) time.Time {
	raw = strings.ReplaceAll(strings.TrimSpace(raw), "-", "")
	if parsed, err := time.Parse("20060102", raw); err == nil {
		return parsed
	}
	return time.Date(fallback.Year(), fallback.Month(), fallback.Day(), 0, 0, 0, 0, time.UTC)
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func numberValue(value any) any {
	raw := strings.TrimSpace(stringValue(value))
	if raw == "" {
		return nil
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil
	}
	return parsed
}

func digits(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func leftPad(value string, width int) string {
	if len(value) >= width {
		return value
	}
	return strings.Repeat("0", width-len(value)) + value
}
