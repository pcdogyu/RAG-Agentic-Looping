package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	refreshCryptoUniverseTask = "market_loop.refresh_crypto_universe"
	refreshAssetUniverseTask  = "market_loop.refresh_asset_universe"
	refreshMacroUniverseTask  = "market_loop.refresh_macro_universe"
	masterdataLockTTL         = 2 * time.Hour
)

var (
	masterdataMarkets   = []string{"CN", "HK", "US", "CRYPTO"}
	minimumMarketCounts = map[string]int{"CN": 5000, "HK": 2500, "US": 5500, "CRYPTO": 15000}
	allowedUSExchanges  = map[string]bool{"NASDAQ": true, "NYSE": true, "AMEX": true, "OTC": true}
	sectorDefaults      = map[string]string{
		"sector:energy": "industry:diversified_energy", "sector:materials": "industry:diversified_materials",
		"sector:industrials": "industry:diversified_industrials", "sector:consumer_discretionary": "industry:diversified_consumer",
		"sector:consumer_staples": "industry:food_beverage", "sector:health_care": "industry:healthcare_services",
		"sector:financials": "industry:diversified_financials", "sector:information_technology": "industry:diversified_technology",
		"sector:communication_services": "industry:diversified_communication", "sector:utilities": "industry:electric_utilities",
		"sector:real_estate": "industry:real_estate", "sector:digital_assets": "industry:cryptocurrency",
	}
)

type masterdataRuntime struct {
	cfg    config.Config
	db     *pgxpool.Pool
	redis  *redis.Client
	client *http.Client
}

type masterAsset struct {
	ID, Class, Market, Symbol, Name, Exchange, Currency string
	Aliases, Products, Competitors                      []string
	Sector, Industry, RawSector, RawIndustry            string
	Instrument, AssociationTier, AssociationReason      string
	IssuerID, PrimaryListingID                          string
	MarketCap                                           *float64
	MarketCapRank                                       *int
	LotSize                                             int
	Active                                              bool
}

type storedMasterAsset struct {
	Aliases, Products, Competitors                 []string
	Sector, Industry, RawSector, RawIndustry       string
	Instrument, AssociationTier, AssociationReason string
	ManualIndustry, ManualAssociation              *string
	ManualSector                                   string
	ManualActive                                   *bool
	MarketCap                                      *float64
	MarketCapRank                                  *int
	IssuerID, PrimaryListingID                     *string
}

type taxonomyRule struct {
	ID, Parent string
	Level      int
	Terms      []string
}

func NewMasterdataHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &masterdataRuntime{cfg: cfg, db: db, redis: redisClient, client: &http.Client{Timeout: 90 * time.Second}}
	return map[string]Handler{
		refreshCryptoUniverseTask: runtime.refreshCryptoUniverse,
		refreshAssetUniverseTask:  runtime.refreshAssetUniverse,
		refreshMacroUniverseTask:  runtime.refreshMacroUniverse,
	}
}

func (runtime *masterdataRuntime) refreshCryptoUniverse(ctx context.Context, job Job) (any, error) {
	return runtime.syncSelectedMarkets(ctx, job, []string{"CRYPTO"})
}

func (runtime *masterdataRuntime) refreshAssetUniverse(ctx context.Context, job Job) (any, error) {
	envelope := taskEnvelope{}
	_ = json.Unmarshal(job.Payload, &envelope)
	selected := uniqueStrings(stringSlice(envelope.Kwargs["markets"]))
	if len(selected) == 0 {
		selected = append([]string{}, masterdataMarkets...)
	}
	for index := range selected {
		selected[index] = strings.ToUpper(strings.TrimSpace(selected[index]))
	}
	for _, market := range selected {
		if !containsString(masterdataMarkets, market) {
			return nil, fmt.Errorf("unsupported asset universe market %q", market)
		}
	}
	return runtime.syncSelectedMarkets(ctx, job, selected)
}

func (runtime *masterdataRuntime) syncSelectedMarkets(ctx context.Context, job Job, selected []string) (any, error) {
	results := map[string]any{}
	var listed []masterAsset
	var listedErr error
	needsAsianDirectory := containsString(selected, "CN") || containsString(selected, "HK")
	if needsAsianDirectory {
		listed, listedErr = runtime.fetchAsianEquities(ctx)
	}
	for _, market := range selected {
		result := runtime.withMarketLock(ctx, job.ID.String(), market, func() map[string]any {
			if err := runtime.startMarketSync(ctx, market); err != nil {
				return map[string]any{"status": "failed", "error": err.Error(), "assets": 0}
			}
			var assets []masterAsset
			var err error
			switch market {
			case "CN", "HK":
				err = listedErr
				for _, asset := range listed {
					if asset.Market == market {
						assets = append(assets, asset)
					}
				}
			case "US":
				assets, err = runtime.fetchUSEquities(ctx)
			case "CRYPTO":
				assets, err = runtime.fetchCryptoAssets(ctx)
			}
			if err == nil {
				err = runtime.classifyAssets(ctx, assets)
			}
			if err == nil {
				err = validateMarketSnapshot(market, assets, minimumMarketCounts[market])
			}
			if err != nil {
				return runtime.failMarketSync(ctx, market, err)
			}
			persisted, err := runtime.persistMarket(ctx, market, assets)
			if err != nil {
				return runtime.failMarketSync(ctx, market, err)
			}
			return persisted
		})
		results[market] = result
	}
	status := "completed"
	for _, value := range results {
		item, _ := value.(map[string]any)
		if stringValue(item["status"]) == "failed" {
			status = "completed_with_errors"
			break
		}
	}
	return map[string]any{"markets": results, "status": status}, nil
}

func (runtime *masterdataRuntime) refreshMacroUniverse(ctx context.Context, job Job) (any, error) {
	return runtime.withMarketLock(ctx, job.ID.String(), "MACRO", func() map[string]any {
		assets, err := runtime.fetchMacroAssets(ctx)
		if err == nil {
			err = runtime.classifyAssets(ctx, assets)
		}
		if err != nil {
			return map[string]any{"status": "failed", "error": truncateRunes(fmt.Sprintf("%T: %v", err, err), 500), "assets": 0}
		}
		if err := runtime.upsertAssets(ctx, assets, false); err != nil {
			return map[string]any{"status": "failed", "error": truncateRunes(fmt.Sprintf("%T: %v", err, err), 500), "assets": 0}
		}
		return map[string]any{"status": "completed", "assets": len(assets)}
	}), nil
}

func (runtime *masterdataRuntime) withMarketLock(ctx context.Context, owner, market string, action func() map[string]any) map[string]any {
	if runtime.redis == nil {
		return action()
	}
	key := "market-loop:masterdata:lock:" + strings.ToLower(market)
	claimed, err := runtime.redis.SetNX(ctx, key, owner, masterdataLockTTL).Result()
	if err != nil {
		return map[string]any{"status": "failed", "error": err.Error(), "assets": 0}
	}
	if !claimed {
		return map[string]any{"status": "skipped_active", "assets": 0}
	}
	defer runtime.redis.Eval(ctx, `if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end`, []string{key}, owner) //nolint:errcheck
	return action()
}

func (runtime *masterdataRuntime) startMarketSync(ctx context.Context, market string) error {
	_, err := runtime.db.Exec(ctx, `INSERT INTO asset_universe_sync(market,status,asset_count,industry_count,added_count,updated_count,deactivated_count,last_error,started_at,completed_at)
		VALUES($1,'running',0,0,0,0,0,NULL,now(),NULL)
		ON CONFLICT(market) DO UPDATE SET status='running',added_count=0,updated_count=0,deactivated_count=0,last_error=NULL,started_at=now(),completed_at=NULL`, market)
	return err
}

func (runtime *masterdataRuntime) failMarketSync(ctx context.Context, market string, cause error) map[string]any {
	detail := truncateRunes(fmt.Sprintf("%T: %v", cause, cause), 500)
	var count int
	_ = runtime.db.QueryRow(ctx, `INSERT INTO asset_universe_sync(market,status,last_error,completed_at) VALUES($1,'failed',$2,now())
		ON CONFLICT(market) DO UPDATE SET status='failed',last_error=$2,completed_at=now() RETURNING asset_count`, market, detail).Scan(&count)
	return map[string]any{"status": "failed", "error": detail, "assets": count}
}

func (runtime *masterdataRuntime) fetchAsianEquities(ctx context.Context) ([]masterAsset, error) {
	payload, err := runtime.requestJSON(ctx, http.MethodPost, runtime.cfg.MarketAdapterURL+"/v1/assets/universe", map[string]any{}, nil)
	if err != nil {
		return nil, err
	}
	return decodeMasterAssets(payload), nil
}

func (runtime *masterdataRuntime) fetchUSEquities(ctx context.Context) ([]masterAsset, error) {
	active := (&discoveryRuntime{cfg: runtime.cfg, db: runtime.db}).effectiveDiscoveryConfig(ctx)
	if active.FMPAccessToken == "" {
		return nil, errors.New("FMP access token is not configured")
	}
	output := map[string]masterAsset{}
	for _, exchange := range []string{"NASDAQ", "NYSE", "AMEX", "OTC"} {
		query := url.Values{"exchange": {exchange}, "isEtf": {"false"}, "isFund": {"false"}, "isActivelyTrading": {"true"}, "limit": {"10000"}}
		payload, err := runtime.requestJSON(ctx, http.MethodGet, active.FMPBaseURL+"/company-screener?"+query.Encode(), nil, map[string]string{"apikey": active.FMPAccessToken})
		if err != nil {
			return nil, err
		}
		for _, item := range objectItems(payload) {
			symbol := strings.ToUpper(strings.TrimSpace(stringValue(item["symbol"])))
			name := strings.TrimSpace(fallbackString(stringValue(item["companyName"]), stringValue(item["name"])))
			if symbol == "" || name == "" {
				continue
			}
			lowered := strings.ToLower(name)
			isADR := strings.Contains(" "+lowered, " adr") || strings.Contains(lowered, "depositary") || strings.Contains(lowered, "depository")
			if exchange == "OTC" && !isADR {
				continue
			}
			marketCap := optionalFloat(item["marketCap"])
			instrument := "common_stock"
			if isADR {
				instrument = "adr"
			}
			asset := masterAsset{ID: "equity:" + exchange + ":" + symbol, Class: "equity", Market: "US", Symbol: symbol, Name: name,
				Exchange: exchange, Currency: strings.ToUpper(fallbackString(stringValue(item["currency"]), "USD")), RawSector: stringValue(item["sector"]),
				RawIndustry: stringValue(item["industry"]), Instrument: instrument, MarketCap: marketCap, LotSize: 1, Active: true,
				AssociationTier: "standard", AssociationReason: "provider_verified"}
			asset.Aliases = uniqueStrings([]string{stringValue(item["shortName"]), underlyingIssuerName(name)})
			if value := firstNonEmpty(item, "issuerId", "issuer_id"); value != "" {
				asset.IssuerID = "fmp:" + strings.ToLower(value)
			} else if value := stringValue(item["cik"]); value != "" {
				asset.IssuerID = "sec-cik:" + strings.ToLower(value)
			}
			asset.PrimaryListingID = stringValue(item["primaryListingAssetId"])
			output[asset.ID] = asset
		}
	}
	assets := make([]masterAsset, 0, len(output))
	for _, asset := range output {
		assets = append(assets, asset)
	}
	return assets, nil
}

func (runtime *masterdataRuntime) fetchCryptoAssets(ctx context.Context) ([]masterAsset, error) {
	directory, err := runtime.requestJSON(ctx, http.MethodGet, runtime.cfg.CoinGeckoURL+"/coins/list?include_platform=false", nil, nil)
	if err != nil {
		return nil, err
	}
	ranked := map[string]map[string]any{}
	for page := 1; page <= 2; page++ {
		endpoint := runtime.cfg.CoinGeckoURL + "/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=" + strconv.Itoa(page) + "&sparkline=false"
		payload, fetchErr := runtime.requestJSON(ctx, http.MethodGet, endpoint, nil, nil)
		if fetchErr != nil {
			return nil, fetchErr
		}
		for _, item := range objectItems(payload) {
			if id := stringValue(item["id"]); id != "" {
				ranked[id] = item
			}
		}
	}
	valid := objectItems(directory)
	symbolCounts, nameCounts := map[string]int{}, map[string]int{}
	for _, item := range valid {
		if stringValue(item["id"]) == "" || stringValue(item["symbol"]) == "" || stringValue(item["name"]) == "" {
			continue
		}
		symbolCounts[strings.ToUpper(stringValue(item["symbol"]))]++
		nameCounts[strings.ToLower(strings.TrimSpace(stringValue(item["name"])))]++
	}
	assets := make([]masterAsset, 0, len(valid))
	for _, item := range valid {
		id, symbol, name := stringValue(item["id"]), strings.ToUpper(stringValue(item["symbol"])), strings.TrimSpace(stringValue(item["name"]))
		if id == "" || symbol == "" || name == "" {
			continue
		}
		market := ranked[id]
		manual := cryptoManualOnly(id, symbol, name)
		ambiguous := market == nil && (symbolCounts[symbol] > 1 || nameCounts[strings.ToLower(name)] > 1)
		tier, reason := "exact_only", "coingecko_long_tail_exact_identity"
		if market != nil {
			tier, reason = "standard", "coingecko_market_cap_top_500"
		}
		if manual || ambiguous {
			tier = "manual_only"
			reason = ternaryString(manual, "stable_or_wrapped_manual_only", "ambiguous_crypto_identity_manual_only")
		}
		assets = append(assets, masterAsset{ID: "crypto:coingecko:" + id, Class: "crypto", Market: "CRYPTO", Symbol: symbol, Name: name,
			Exchange: "coingecko", Currency: "USD", Aliases: []string{id}, Sector: "sector:digital_assets", Industry: "industry:cryptocurrency",
			RawSector: "Digital Assets", RawIndustry: "Cryptocurrency", Instrument: "crypto", MarketCap: optionalFloat(market["market_cap"]),
			MarketCapRank: optionalInt(market["market_cap_rank"]), AssociationTier: tier, AssociationReason: reason, LotSize: 1, Active: true})
	}
	return assets, nil
}

func (runtime *masterdataRuntime) fetchMacroAssets(ctx context.Context) ([]masterAsset, error) {
	active := (&discoveryRuntime{cfg: runtime.cfg, db: runtime.db}).effectiveDiscoveryConfig(ctx)
	if active.FMPAccessToken == "" {
		return nil, errors.New("FMP access token is not configured")
	}
	assets := []masterAsset{}
	for _, spec := range []struct{ endpoint, class, market string }{{"commodities-list", "commodity", "COMMODITY"}, {"forex-list", "fx", "FX"}} {
		payload, err := runtime.requestJSON(ctx, http.MethodGet, active.FMPBaseURL+"/"+spec.endpoint, nil, map[string]string{"apikey": active.FMPAccessToken})
		if err != nil {
			return nil, err
		}
		for _, item := range objectItems(payload) {
			symbol := strings.ToUpper(firstNonEmpty(item, "symbol", "ticker"))
			if symbol == "" {
				continue
			}
			name := fallbackString(firstNonEmpty(item, "name", "companyName"), symbol)
			assets = append(assets, masterAsset{ID: spec.class + ":fmp:" + symbol, Class: spec.class, Market: spec.market, Symbol: symbol, Name: name,
				Exchange: "fmp", Currency: strings.ToUpper(fallbackString(stringValue(item["currency"]), "USD")), Aliases: uniqueStrings([]string{stringValue(item["shortName"]), stringValue(item["underlyingName"])}),
				AssociationTier: "standard", AssociationReason: "provider_verified", LotSize: 1, Active: true})
		}
	}
	return assets, nil
}

func (runtime *masterdataRuntime) requestJSON(ctx context.Context, method, endpoint string, body any, headers map[string]string) (any, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := runtime.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("master data HTTP %d: %s", response.StatusCode, truncateRunes(string(payload), 300))
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func decodeMasterAssets(payload any) []masterAsset {
	assets := []masterAsset{}
	for _, item := range objectItems(payload) {
		asset := masterAsset{ID: stringValue(item["asset_id"]), Class: stringValue(item["asset_class"]), Market: strings.ToUpper(stringValue(item["market"])),
			Symbol: stringValue(item["symbol"]), Name: stringValue(item["name"]), Exchange: stringValue(item["exchange_or_provider"]), Currency: stringValue(item["currency"]),
			Aliases: stringSlice(item["aliases"]), Products: stringSlice(item["products"]), Competitors: stringSlice(item["competitors"]), Sector: stringValue(item["sector_id"]),
			Industry: stringValue(item["industry_id"]), RawSector: stringValue(item["raw_sector"]), RawIndustry: stringValue(item["raw_industry"]), Instrument: stringValue(item["instrument_type"]),
			AssociationTier: fallbackString(stringValue(item["association_tier"]), "standard"), AssociationReason: fallbackString(stringValue(item["association_reason"]), "provider_verified"),
			IssuerID: stringValue(item["issuer_id"]), PrimaryListingID: stringValue(item["primary_listing_asset_id"]), MarketCap: optionalFloat(item["market_cap"]), MarketCapRank: optionalInt(item["market_cap_rank"]),
			LotSize: max(1, discoveryInt(item["lot_size"])), Active: boolDefault(item["active"], true)}
		assets = append(assets, asset)
	}
	return assets
}

func objectItems(payload any) []map[string]any {
	if object := objectValue(payload); object != nil {
		for _, key := range []string{"items", "data", "results"} {
			if object[key] != nil {
				payload = object[key]
				break
			}
		}
	}
	items := []map[string]any{}
	for _, raw := range anySlice(payload) {
		if item := objectValue(raw); item != nil {
			items = append(items, item)
		}
	}
	return items
}

func validateMarketSnapshot(market string, assets []masterAsset, minimum int) error {
	if len(assets) < minimum {
		return fmt.Errorf("%s provider returned an incomplete universe (%d < %d)", market, len(assets), minimum)
	}
	seen, ranked := map[string]bool{}, 0
	for _, asset := range assets {
		if strings.TrimSpace(asset.ID) == "" || strings.TrimSpace(asset.Symbol) == "" || strings.TrimSpace(asset.Name) == "" || strings.TrimSpace(asset.Exchange) == "" || strings.TrimSpace(asset.Currency) == "" {
			return fmt.Errorf("%s provider returned an invalid identity", market)
		}
		if seen[asset.ID] {
			return fmt.Errorf("%s provider returned duplicate asset_id %s", market, asset.ID)
		}
		seen[asset.ID] = true
		if asset.Market != market {
			return fmt.Errorf("%s provider returned cross-market asset %s", market, asset.ID)
		}
		if market == "US" && (!allowedUSExchanges[strings.ToUpper(asset.Exchange)] || (strings.EqualFold(asset.Exchange, "OTC") && asset.Instrument != "adr")) {
			return fmt.Errorf("US provider returned unsupported identity %s", asset.ID)
		}
		if asset.MarketCapRank != nil && *asset.MarketCapRank <= 500 {
			ranked++
		}
	}
	if market == "CRYPTO" && ranked < min(490, len(assets)) {
		return fmt.Errorf("CRYPTO ranked universe is incomplete (%d ranked assets)", ranked)
	}
	return nil
}

func (runtime *masterdataRuntime) classifyAssets(ctx context.Context, assets []masterAsset) error {
	rules, err := runtime.loadTaxonomy(ctx)
	if err != nil {
		return err
	}
	for index := range assets {
		if assets[index].Industry != "" {
			continue
		}
		assets[index].Sector, assets[index].Industry = normalizeMasterIndustry(assets[index].RawSector, assets[index].RawIndustry, rules)
	}
	return nil
}

func (runtime *masterdataRuntime) loadTaxonomy(ctx context.Context) ([]taxonomyRule, error) {
	rows, err := runtime.db.Query(ctx, `SELECT id,coalesce(parent_id,''),level,name_zh,name_en,aliases::jsonb FROM industries WHERE active=true`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rules := []taxonomyRule{}
	for rows.Next() {
		var rule taxonomyRule
		var zh, en string
		var raw []byte
		if err := rows.Scan(&rule.ID, &rule.Parent, &rule.Level, &zh, &en, &raw); err != nil {
			return nil, err
		}
		aliases := []string{}
		_ = json.Unmarshal(raw, &aliases)
		rule.Terms = uniqueStrings(append([]string{zh, en}, aliases...))
		rules = append(rules, rule)
	}
	return rules, rows.Err()
}

func normalizeMasterIndustry(rawSector, rawIndustry string, rules []taxonomyRule) (string, string) {
	industry, sector := normalizeDiscoveryText(rawIndustry), normalizeDiscoveryText(rawSector)
	type match struct {
		length int
		rule   taxonomyRule
		term   string
	}
	matches := []match{}
	for _, rule := range rules {
		if rule.Level != 2 {
			continue
		}
		for _, term := range rule.Terms {
			normalized := normalizeDiscoveryText(term)
			if normalized != "" {
				matches = append(matches, match{len([]rune(normalized)), rule, normalized})
			}
		}
	}
	sort.SliceStable(matches, func(i, j int) bool { return matches[i].length > matches[j].length })
	for _, source := range []string{industry, sector} {
		for _, candidate := range matches {
			if source != "" && strings.Contains(source, candidate.term) {
				return candidate.rule.Parent, candidate.rule.ID
			}
		}
	}
	combined := sector + industry
	for _, rule := range rules {
		if rule.Level != 1 {
			continue
		}
		for _, term := range rule.Terms {
			if normalized := normalizeDiscoveryText(term); normalized != "" && strings.Contains(combined, normalized) {
				return rule.ID, sectorDefaults[rule.ID]
			}
		}
	}
	return "", ""
}

func (runtime *masterdataRuntime) persistMarket(ctx context.Context, market string, assets []masterAsset) (map[string]any, error) {
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	existing, err := loadStoredMasterAssets(ctx, tx, market)
	if err != nil {
		return nil, err
	}
	added, updated := 0, 0
	received := make([]string, 0, len(assets))
	for index := range assets {
		received = append(received, assets[index].ID)
		if prior, ok := existing[assets[index].ID]; ok {
			updated++
			mergeStoredMasterAsset(&assets[index], prior)
		} else {
			added++
		}
	}
	if err := batchUpsertMasterAssets(ctx, tx, assets); err != nil {
		return nil, err
	}
	result, err := tx.Exec(ctx, `UPDATE assets SET active=coalesce(manual_active,false),last_synced_at=now()
		WHERE market=$1 AND NOT(id=ANY($2::text[])) AND coalesce(issuer_id,'') NOT LIKE 'curated:%'`, market, received)
	if err != nil {
		return nil, err
	}
	deactivated := int(result.RowsAffected())
	industrySet := map[string]bool{}
	for _, asset := range assets {
		if asset.Industry != "" {
			industrySet[asset.Industry] = true
		}
	}
	_, err = tx.Exec(ctx, `UPDATE asset_universe_sync SET status='completed',asset_count=$2,industry_count=$3,added_count=$4,updated_count=$5,deactivated_count=$6,last_error=NULL,completed_at=now() WHERE market=$1`, market, len(received), len(industrySet), added, updated, deactivated)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"status": "completed", "assets": len(received), "added": added, "updated": updated, "deactivated": deactivated}, nil
}

func (runtime *masterdataRuntime) upsertAssets(ctx context.Context, assets []masterAsset, deactivate bool) error {
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	markets := map[string]bool{}
	for _, asset := range assets {
		markets[asset.Market] = true
	}
	for market := range markets {
		existing, loadErr := loadStoredMasterAssets(ctx, tx, market)
		if loadErr != nil {
			return loadErr
		}
		for index := range assets {
			if assets[index].Market == market {
				if prior, ok := existing[assets[index].ID]; ok {
					mergeStoredMasterAsset(&assets[index], prior)
				}
			}
		}
	}
	if err := batchUpsertMasterAssets(ctx, tx, assets); err != nil {
		return err
	}
	_ = deactivate
	return tx.Commit(ctx)
}

func loadStoredMasterAssets(ctx context.Context, tx pgx.Tx, market string) (map[string]storedMasterAsset, error) {
	rows, err := tx.Query(ctx, `SELECT a.id,a.aliases::jsonb,a.products::jsonb,a.competitors::jsonb,a.sector_id,a.industry_id,a.raw_sector,a.raw_industry,a.instrument_type,
		a.market_cap,a.market_cap_rank,a.association_tier,a.association_reason,a.manual_industry_id,a.manual_active,a.manual_association_tier,a.issuer_id,a.primary_listing_asset_id,
		coalesce(i.parent_id,'') FROM assets a LEFT JOIN industries i ON i.id=a.manual_industry_id WHERE a.market=$1`, market)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	output := map[string]storedMasterAsset{}
	for rows.Next() {
		var id string
		var item storedMasterAsset
		var aliases, products, competitors []byte
		if err := rows.Scan(&id, &aliases, &products, &competitors, &item.Sector, &item.Industry, &item.RawSector, &item.RawIndustry, &item.Instrument,
			&item.MarketCap, &item.MarketCapRank, &item.AssociationTier, &item.AssociationReason, &item.ManualIndustry, &item.ManualActive, &item.ManualAssociation, &item.IssuerID, &item.PrimaryListingID, &item.ManualSector); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(aliases, &item.Aliases)
		_ = json.Unmarshal(products, &item.Products)
		_ = json.Unmarshal(competitors, &item.Competitors)
		output[id] = item
	}
	return output, rows.Err()
}

func mergeStoredMasterAsset(asset *masterAsset, prior storedMasterAsset) {
	asset.Aliases = uniqueStrings(append(prior.Aliases, asset.Aliases...))
	asset.Products = uniqueStrings(append(prior.Products, asset.Products...))
	asset.Competitors = uniqueStrings(append(prior.Competitors, asset.Competitors...))
	if prior.ManualIndustry != nil {
		asset.Industry, asset.Sector = *prior.ManualIndustry, prior.ManualSector
	} else {
		asset.Sector = fallbackString(asset.Sector, prior.Sector)
		asset.Industry = fallbackString(asset.Industry, prior.Industry)
	}
	asset.RawSector, asset.RawIndustry = fallbackString(asset.RawSector, prior.RawSector), fallbackString(asset.RawIndustry, prior.RawIndustry)
	asset.Instrument = fallbackString(asset.Instrument, prior.Instrument)
	if asset.MarketCap == nil {
		asset.MarketCap = prior.MarketCap
	}
	if asset.MarketCapRank == nil {
		asset.MarketCapRank = prior.MarketCapRank
	}
	// The upsert applies manual association overrides to effective fields while
	// retaining this provider value for a later reset to automatic mode.
	if prior.ManualActive != nil {
		asset.Active = *prior.ManualActive
	}
	if asset.IssuerID == "" && prior.IssuerID != nil {
		asset.IssuerID = *prior.IssuerID
	}
	if asset.PrimaryListingID == "" && prior.PrimaryListingID != nil {
		asset.PrimaryListingID = *prior.PrimaryListingID
	}
}

func batchUpsertMasterAssets(ctx context.Context, tx pgx.Tx, assets []masterAsset) error {
	batch := &pgx.Batch{}
	for _, asset := range assets {
		aliases, _ := json.Marshal(asset.Aliases)
		products, _ := json.Marshal(asset.Products)
		competitors, _ := json.Marshal(asset.Competitors)
		providerTier := fallbackString(asset.AssociationTier, "standard")
		providerReason := fallbackString(asset.AssociationReason, "provider_verified")
		batch.Queue(`INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,sector_id,industry_id,raw_sector,raw_industry,
			instrument_type,market_cap,market_cap_rank,association_tier,association_reason,provider_association_tier,provider_association_reason,last_synced_at,issuer_id,primary_listing_asset_id,lot_size,active)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,now(),$22,$23,$24,$25)
			ON CONFLICT(id) DO UPDATE SET asset_class=excluded.asset_class,market=excluded.market,symbol=excluded.symbol,name=excluded.name,exchange_or_provider=excluded.exchange_or_provider,
			currency=excluded.currency,aliases=excluded.aliases,products=excluded.products,competitors=excluded.competitors,sector_id=excluded.sector_id,industry_id=excluded.industry_id,
			raw_sector=excluded.raw_sector,raw_industry=excluded.raw_industry,instrument_type=excluded.instrument_type,market_cap=excluded.market_cap,market_cap_rank=excluded.market_cap_rank,
			association_tier=CASE WHEN assets.manual_association_tier IS NULL THEN excluded.association_tier ELSE assets.manual_association_tier END,
			association_reason=CASE WHEN assets.manual_association_tier IS NULL THEN excluded.association_reason ELSE 'manual_override' END,
			provider_association_tier=excluded.provider_association_tier,provider_association_reason=excluded.provider_association_reason,last_synced_at=excluded.last_synced_at,
			issuer_id=excluded.issuer_id,primary_listing_asset_id=excluded.primary_listing_asset_id,lot_size=excluded.lot_size,active=CASE WHEN assets.manual_active IS NULL THEN true ELSE assets.manual_active END`,
			asset.ID, asset.Class, asset.Market, asset.Symbol, asset.Name, asset.Exchange, asset.Currency, aliases, products, competitors, asset.Sector, asset.Industry, asset.RawSector, asset.RawIndustry,
			asset.Instrument, asset.MarketCap, asset.MarketCapRank, providerTier, providerReason, providerTier, providerReason, nullableMasterString(asset.IssuerID), nullableMasterString(asset.PrimaryListingID), max(1, asset.LotSize), asset.Active)
	}
	results := tx.SendBatch(ctx, batch)
	for range assets {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return err
		}
	}
	return results.Close()
}

func optionalFloat(value any) *float64 {
	if value == nil {
		return nil
	}
	number := numberValue(value)
	return &number
}

func optionalInt(value any) *int {
	if value == nil {
		return nil
	}
	number := discoveryInt(value)
	return &number
}

func nullableMasterString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func firstNonEmpty(item map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(stringValue(item[key])); value != "" {
			return value
		}
	}
	return ""
}

var listingWrapper = regexp.MustCompile(`(?i)\b(?:sponsored|unsponsored)?\s*(?:adr|ads|american\s+de(?:positary|pository)\s+(?:receipt|receipts|share|shares))\b`)

func underlyingIssuerName(value string) string {
	return strings.Trim(strings.Join(strings.Fields(listingWrapper.ReplaceAllString(value, "")), " "), " -(),")
}

func cryptoManualOnly(id, symbol, name string) bool {
	manualIDs := map[string]bool{"tether": true, "usd-coin": true, "dai": true, "first-digital-usd": true, "ethena-usde": true, "true-usd": true, "usdd": true,
		"pax-dollar": true, "paypal-usd": true, "frax": true, "liquity-usd": true, "gemini-dollar": true, "wrapped-bitcoin": true, "weth": true, "staked-ether": true}
	manualSymbols := map[string]bool{"USDT": true, "USDC": true, "DAI": true, "FDUSD": true, "USDE": true, "TUSD": true, "USDD": true, "USDP": true, "PYUSD": true,
		"FRAX": true, "LUSD": true, "GUSD": true, "USDS": true, "USD0": true, "USD1": true, "USDA": true, "USDB": true, "USDN": true, "USDX": true, "USDF": true,
		"GHO": true, "EURC": true, "EURT": true, "WBTC": true, "WETH": true, "STETH": true}
	lowered := strings.ToLower(id + " " + name)
	return manualIDs[strings.ToLower(id)] || manualSymbols[strings.ToUpper(symbol)] || strings.Contains(lowered, "wrapped ") || strings.Contains(lowered, "bridged ") ||
		strings.Contains(lowered, " stablecoin") || strings.Contains(lowered, " stable coin") || strings.Contains(lowered, " dollar") || strings.Contains(lowered, " usd") ||
		strings.Contains(lowered, " euro coin") || strings.Contains(lowered, " eur stable")
}

type masterdataSchedule struct {
	task     string
	interval time.Duration
}

var masterdataSchedules = []masterdataSchedule{
	{task: refreshAssetUniverseTask, interval: 24 * time.Hour},
	{task: refreshCryptoUniverseTask, interval: 6 * time.Hour},
	{task: refreshMacroUniverseTask, interval: 24 * time.Hour},
}

type MasterdataScheduler struct {
	cfg   config.Config
	store *Store
	redis *redis.Client
}

func NewMasterdataScheduler(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) *MasterdataScheduler {
	return &MasterdataScheduler{cfg: cfg, store: NewStore(db), redis: redisClient}
}

func (scheduler *MasterdataScheduler) Enabled() bool {
	return completedWorkerLane(scheduler.cfg, "masterdata")
}

func (scheduler *MasterdataScheduler) Tick(ctx context.Context) error {
	if !scheduler.Enabled() {
		return nil
	}
	for _, spec := range masterdataSchedules {
		key := "market-loop:go-schedule:" + spec.task
		claimed, err := scheduler.redis.SetNX(ctx, key, iso(time.Now()), spec.interval).Result()
		if err != nil {
			return err
		}
		if !claimed {
			continue
		}
		_, err = scheduler.store.Enqueue(ctx, EnqueueParams{Queue: "masterdata", TaskType: spec.task, Payload: taskEnvelope{Args: []any{}, Kwargs: map[string]any{}}, Priority: 5, MaxAttempts: 3, DedupeKey: "scheduled:" + spec.task})
		if err != nil {
			_ = scheduler.redis.Del(ctx, key).Err()
			return err
		}
		// The daily full-universe task already includes crypto. Advance the
		// shorter crypto lease so a scheduler restart does not immediately do
		// the same large CoinGecko snapshot twice on a single-worker queue.
		if spec.task == refreshAssetUniverseTask {
			_ = scheduler.redis.Set(ctx, "market-loop:go-schedule:"+refreshCryptoUniverseTask, iso(time.Now()), 6*time.Hour).Err()
		}
	}
	return nil
}
