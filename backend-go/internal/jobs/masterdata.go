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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentalresearch"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/fundamentals"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketdata"
	"github.com/redis/go-redis/v9"
)

const (
	refreshCryptoUniverseTask      = "market_loop.refresh_crypto_universe"
	refreshAssetUniverseTask       = "market_loop.refresh_asset_universe"
	refreshMacroUniverseTask       = "market_loop.refresh_macro_universe"
	syncFundamentalsTask           = "market_loop.sync_fundamental_snapshots"
	syncCorporateActionsTask       = "market_loop.sync_corporate_actions"
	refreshTrackedFundamentalsTask = "market_loop.refresh_tracked_fundamentals"
	runScheduledFundamentalTask    = "market_loop.run_scheduled_fundamental_research"
	masterdataLockTTL              = 2 * time.Hour
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

type fundamentalRefreshCandidate struct {
	AssetID         string
	SelectionReason string
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
		refreshCryptoUniverseTask:      runtime.refreshCryptoUniverse,
		refreshAssetUniverseTask:       runtime.refreshAssetUniverse,
		refreshMacroUniverseTask:       runtime.refreshMacroUniverse,
		syncFundamentalsTask:           runtime.syncFundamentalSnapshots,
		syncCorporateActionsTask:       runtime.syncCorporateActions,
		refreshTrackedFundamentalsTask: runtime.refreshTrackedFundamentals,
		runScheduledFundamentalTask:    runtime.runScheduledFundamentalResearch,
	}
}

func (runtime *masterdataRuntime) syncCorporateActions(ctx context.Context, job Job) (any, error) {
	envelope := taskEnvelope{}
	_ = json.Unmarshal(job.Payload, &envelope)
	assetID := strings.TrimSpace(stringValue(envelope.Kwargs["asset_id"]))
	if assetID == "" && len(envelope.Args) > 0 {
		assetID = strings.TrimSpace(stringValue(envelope.Args[0]))
	}
	if assetID == "" {
		return nil, errors.New("corporate action sync requires asset_id")
	}
	limit := int(numberValue(envelope.Kwargs["limit"]))
	if limit == 0 {
		limit = 500
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("corporate action limit must be between 1 and 1000")
	}
	return runtime.syncAssetCorporateActions(ctx, assetID, limit)
}

func (runtime *masterdataRuntime) syncAssetCorporateActions(ctx context.Context, assetID string, limit int) (any, error) {
	var symbol, market, assetClass, currency string
	if err := runtime.db.QueryRow(ctx, `SELECT symbol,market,asset_class,currency FROM assets WHERE id=$1 AND active=true`, assetID).Scan(&symbol, &market, &assetClass, &currency); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("active asset %q was not found", assetID)
		}
		return nil, err
	}
	if strings.ToUpper(market) != "US" || strings.ToLower(assetClass) != "equity" {
		return nil, fmt.Errorf("corporate action sync currently supports US equities only")
	}
	active := (&discoveryRuntime{cfg: runtime.cfg, db: runtime.db}).effectiveDiscoveryConfig(ctx)
	client := marketdata.FMPCorporateActionClient{BaseURL: active.FMPBaseURL, AccessToken: active.FMPAccessToken, HTTPClient: runtime.client}
	items, err := client.Fetch(ctx, assetID, market, currency, symbol, limit)
	if err != nil {
		return nil, err
	}
	store := marketdata.NewStore(runtime.db)
	inserted := 0
	counts := map[string]int{}
	for _, item := range items {
		created, err := store.SaveCorporateAction(ctx, item)
		if err != nil {
			return nil, err
		}
		counts[string(item.ActionType)]++
		if created {
			inserted++
		}
	}
	return map[string]any{
		"asset_id": assetID, "symbol": symbol, "source": "FMP", "action_count": len(items), "inserted": inserted,
		"unchanged": len(items) - inserted, "action_types": counts, "time_contract_version": marketdata.CorporateActionContractVersion,
	}, nil
}

// syncFundamentalSnapshots is intentionally a per-asset, manually queued P1
// operation. A full-universe financial sweep would create unnecessary FMP load
// and is not required to establish a point-in-time factual record.
func (runtime *masterdataRuntime) syncFundamentalSnapshots(ctx context.Context, job Job) (any, error) {
	envelope := taskEnvelope{}
	_ = json.Unmarshal(job.Payload, &envelope)
	assetID := strings.TrimSpace(stringValue(envelope.Kwargs["asset_id"]))
	if assetID == "" && len(envelope.Args) > 0 {
		assetID = strings.TrimSpace(stringValue(envelope.Args[0]))
	}
	if assetID == "" {
		return nil, errors.New("fundamental snapshot sync requires asset_id")
	}
	limit := int(numberValue(envelope.Kwargs["limit"]))
	if limit == 0 {
		limit = 12
	}
	if limit < 1 || limit > 40 {
		return nil, errors.New("fundamental snapshot limit must be between 1 and 40")
	}
	return runtime.syncAssetFundamentals(ctx, assetID, limit)
}

func (runtime *masterdataRuntime) syncAssetFundamentals(ctx context.Context, assetID string, limit int) (any, error) {
	var symbol, market, assetClass string
	if err := runtime.db.QueryRow(ctx, `SELECT symbol,market,asset_class FROM assets WHERE id=$1 AND active=true`, assetID).Scan(&symbol, &market, &assetClass); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("active asset %q was not found", assetID)
		}
		return nil, err
	}
	if strings.ToUpper(market) != "US" || strings.ToLower(assetClass) != "equity" {
		return nil, fmt.Errorf("fundamental snapshots currently support US equities only")
	}
	active := (&discoveryRuntime{cfg: runtime.cfg, db: runtime.db}).effectiveDiscoveryConfig(ctx)
	client := fundamentals.FMPClient{BaseURL: active.FMPBaseURL, AccessToken: active.FMPAccessToken, HTTPClient: runtime.client}
	items, err := client.FetchStatements(ctx, assetID, symbol, limit)
	if err != nil {
		return nil, err
	}
	store := fundamentals.NewStore(runtime.db)
	inserted := 0
	for _, item := range items {
		created, err := store.Save(ctx, item)
		if err != nil {
			return nil, err
		}
		if created {
			inserted++
		}
	}
	return map[string]any{
		"asset_id": assetID, "symbol": symbol, "source": "FMP", "statement_count": len(items), "inserted": inserted,
		"unchanged": len(items) - inserted, "time_contract_version": fundamentals.TimeContractVersion,
	}, nil
}

const recentResearchFundamentalBootstrapVersion = "recent-research-fundamental-bootstrap-v1"

// fundamentalRefreshCandidates keeps explicit forecast/rating tracking as the
// primary source, while allowing recent recommendations produced by the
// current evidence contract to bootstrap factual statement collection. The
// latter does not approve assumptions, create valuations, or create ratings.
func (runtime *masterdataRuntime) fundamentalRefreshCandidates(ctx context.Context, now time.Time, limit int) ([]fundamentalRefreshCandidate, error) {
	if runtime.db == nil || now.IsZero() || limit < 1 || limit > 100 {
		return nil, errors.New("fundamental refresh store, time and limit are required")
	}
	rows, err := runtime.db.Query(ctx, `WITH candidates AS (
		SELECT a.id,
			(EXISTS(SELECT 1 FROM forecast_versions forecast WHERE forecast.asset_id=a.id)
			 OR EXISTS(SELECT 1 FROM fundamental_rating_states rating WHERE rating.asset_id=a.id)) AS explicitly_tracked,
			EXISTS(SELECT 1 FROM recommendations recommendation
				WHERE recommendation.asset_id=a.id AND recommendation.as_of >= $1
				AND recommendation.payload::jsonb->>'scoring_version'='llm-direction-v3') AS recent_research,
			max(snapshot.available_at) AS latest_snapshot,
			(SELECT max(recommendation.as_of) FROM recommendations recommendation
				WHERE recommendation.asset_id=a.id AND recommendation.as_of >= $1
				AND recommendation.payload::jsonb->>'scoring_version'='llm-direction-v3') AS latest_research
		FROM assets a LEFT JOIN fundamental_snapshots snapshot ON snapshot.asset_id=a.id
		WHERE a.active=true AND upper(a.market)='US' AND lower(a.asset_class)='equity'
		GROUP BY a.id
	)
	SELECT id,CASE WHEN explicitly_tracked THEN 'explicit_forecast_or_rating' ELSE 'recent_completed_research' END
	FROM candidates WHERE explicitly_tracked OR recent_research
	ORDER BY latest_snapshot ASC NULLS FIRST,explicitly_tracked DESC,latest_research DESC NULLS LAST,id
	LIMIT $2`, now.UTC().AddDate(0, 0, -30), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []fundamentalRefreshCandidate{}
	for rows.Next() {
		var item fundamentalRefreshCandidate
		if err := rows.Scan(&item.AssetID, &item.SelectionReason); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// refreshTrackedFundamentals keeps a bounded set of explicitly tracked or
// recently researched assets factually current without needing a new news
// event. It never creates assumptions, valuations, ratings, or predictions on
// behalf of an analyst.
func (runtime *masterdataRuntime) refreshTrackedFundamentals(ctx context.Context, _ Job) (any, error) {
	candidates, err := runtime.fundamentalRefreshCandidates(ctx, time.Now().UTC(), 10)
	if err != nil {
		return nil, err
	}
	results := map[string]any{}
	succeeded, explicit, bootstrapped := 0, 0, 0
	for _, candidate := range candidates {
		if candidate.SelectionReason == "explicit_forecast_or_rating" {
			explicit++
		} else {
			bootstrapped++
		}
		fundamentalResult, fundamentalErr := runtime.syncAssetFundamentals(ctx, candidate.AssetID, 12)
		actionResult, actionErr := runtime.syncAssetCorporateActions(ctx, candidate.AssetID, 500)
		if fundamentalErr != nil || actionErr != nil {
			entry := map[string]any{"status": "partial", "selection_reason": candidate.SelectionReason, "fundamentals": fundamentalResult, "corporate_actions": actionResult}
			if fundamentalErr != nil {
				entry["fundamental_error"] = fundamentalErr.Error()
			}
			if actionErr != nil {
				entry["corporate_action_error"] = actionErr.Error()
			}
			results[candidate.AssetID] = entry
			continue
		}
		results[candidate.AssetID] = map[string]any{"status": "completed", "selection_reason": candidate.SelectionReason, "fundamentals": fundamentalResult, "corporate_actions": actionResult}
		succeeded++
	}
	return map[string]any{
		"status": "completed", "selected": len(candidates), "succeeded": succeeded, "explicitly_tracked": explicit,
		"recent_research_bootstrapped": bootstrapped, "selection_policy_version": recentResearchFundamentalBootstrapVersion,
		"results": results, "automatic_assumptions": false, "automatic_ratings": false, "automatic_predictions": false,
	}, nil
}

// runScheduledFundamentalResearch revalues only explicitly approved plans.
// New financial disclosures stop the plan for review; the scheduler never
// invents forecast assumptions or silently rolls them onto new statements.
func (runtime *masterdataRuntime) runScheduledFundamentalResearch(ctx context.Context, _ Job) (any, error) {
	now := time.Now().UTC()
	store := fundamentalresearch.NewPlanStore(runtime.db)
	plans, err := store.Due(ctx, now, 10)
	if err != nil {
		return nil, err
	}
	results := map[string]any{}
	completed, blocked, failed := 0, 0, 0
	for _, plan := range plans {
		result := fundamentalresearch.ScheduledResult{Version: fundamentalresearch.ScheduledResearchVersion, PlanID: plan.ID, AssetID: plan.AssetID, Status: "data_refresh_failed", AsOf: now, ForecastVersionID: plan.ForecastVersionID}
		if _, syncErr := runtime.syncAssetFundamentals(ctx, plan.AssetID, 12); syncErr != nil {
			result.Reason = syncErr.Error()
			if recordErr := store.Record(ctx, plan, result, time.Now().UTC()); recordErr != nil {
				return nil, recordErr
			}
			results[plan.AssetID] = result
			failed++
			continue
		}
		if _, syncErr := runtime.syncAssetCorporateActions(ctx, plan.AssetID, 500); syncErr != nil {
			result.Reason = syncErr.Error()
			if recordErr := store.Record(ctx, plan, result, time.Now().UTC()); recordErr != nil {
				return nil, recordErr
			}
			results[plan.AssetID] = result
			failed++
			continue
		}
		asset := map[string]any{}
		var assetID, assetClass, market, symbol, currency string
		if queryErr := runtime.db.QueryRow(ctx, `SELECT id,asset_class,market,symbol,currency FROM assets WHERE id=$1 AND active=true`, plan.AssetID).Scan(&assetID, &assetClass, &market, &symbol, &currency); queryErr != nil {
			result.Reason = queryErr.Error()
			if recordErr := store.Record(ctx, plan, result, time.Now().UTC()); recordErr != nil {
				return nil, recordErr
			}
			results[plan.AssetID] = result
			failed++
			continue
		}
		asset["asset_id"], asset["asset_class"], asset["market"], asset["symbol"], asset["currency"] = assetID, assetClass, market, symbol, currency
		priceRuntime := &outcomeRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis, client: runtime.client}
		if _, priceErr := priceRuntime.cachedPrices(ctx, asset, now.AddDate(0, 0, -14), now, map[string][]outcomePricePoint{}); priceErr != nil {
			result.Reason = priceErr.Error()
			if recordErr := store.Record(ctx, plan, result, time.Now().UTC()); recordErr != nil {
				return nil, recordErr
			}
			results[plan.AssetID] = result
			failed++
			continue
		}
		result, runErr := store.Run(ctx, plan, time.Now().UTC())
		if runErr != nil {
			result.Status, result.Reason = "technical_failure", runErr.Error()
		}
		if recordErr := store.Record(ctx, plan, result, time.Now().UTC()); recordErr != nil {
			return nil, recordErr
		}
		results[plan.AssetID] = result
		switch result.Status {
		case "completed":
			completed++
		case "technical_failure", "data_refresh_failed":
			failed++
		default:
			blocked++
		}
	}
	return map[string]any{"status": "completed", "selected": len(plans), "completed": completed, "blocked": blocked, "failed": failed, "results": results, "automatic_assumptions": false}, nil
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
	_ = runtime.db.QueryRow(ctx, `INSERT INTO asset_universe_sync(market,status,asset_count,industry_count,added_count,updated_count,deactivated_count,last_error,started_at,completed_at)
		VALUES($1,'failed',0,0,0,0,0,$2,NULL,now())
		ON CONFLICT(market) DO UPDATE SET status='failed',last_error=$2,completed_at=now() RETURNING asset_count`, market, detail).Scan(&count)
	historyRecorded := true
	if err := runtime.persistFailedUniverseSnapshot(ctx, market, count, detail); err != nil {
		historyRecorded = false
	}
	return map[string]any{"status": "failed", "error": detail, "assets": count, "history_recorded": historyRecorded}
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
	observedAt := time.Now().UTC()
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
	snapshotID, included, excluded, delisted, err := runtime.persistCompletedUniverseSnapshot(ctx, tx, market, existing, assets, observedAt)
	if err != nil {
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
	return map[string]any{"status": "completed", "assets": len(received), "added": added, "updated": updated, "deactivated": deactivated,
		"universe_snapshot_id": snapshotID, "included": included, "excluded": excluded, "delisted": delisted}, nil
}

func (runtime *masterdataRuntime) persistFailedUniverseSnapshot(ctx context.Context, market string, assetCount int, detail string) error {
	now := time.Now().UTC()
	sourceName, sourceURL := runtime.universeSource(market)
	policy, _ := json.Marshal(runtime.universeEligibilityPolicy(market))
	metadata, _ := json.Marshal(map[string]any{"failure_retained": true, "current_assets_preserved": true})
	_, err := runtime.db.Exec(ctx, `INSERT INTO security_universe_snapshots(
        id,universe_id,market,status,observed_at,available_at,source_name,source_document_id,source_url,eligibility_policy,
        asset_count,included_count,excluded_count,delisted_count,failure_detail,metadata)
        VALUES($1,$2,$3,'failed',$4,$4,$5,$6,$7,$8,$9,0,0,0,$10,$11)`, uuid.NewString(), "market:"+market, market, now,
		sourceName, "universe-refresh-failure:"+market+":"+now.Format(time.RFC3339Nano), sourceURL, policy, assetCount, detail, metadata)
	if err != nil {
		return fmt.Errorf("persist failed security universe snapshot: %w", err)
	}
	return nil
}

func (runtime *masterdataRuntime) persistCompletedUniverseSnapshot(ctx context.Context, tx pgx.Tx, market string, existing map[string]storedMasterAsset, assets []masterAsset, observedAt time.Time) (string, int, int, int, error) {
	snapshotID := uuid.NewString()
	sourceName, sourceURL := runtime.universeSource(market)
	policy, _ := json.Marshal(runtime.universeEligibilityPolicy(market))
	incoming := make(map[string]masterAsset, len(assets))
	included, delisted := 0, 0
	for _, asset := range assets {
		incoming[asset.ID] = asset
		if asset.Active {
			included++
		} else {
			delisted++
		}
	}
	excluded := 0
	for assetID := range existing {
		if _, ok := incoming[assetID]; !ok {
			excluded++
		}
	}
	metadata, _ := json.Marshal(map[string]any{
		"missing_provider_assets": "excluded_not_assumed_delisted", "manual_active_override_preserved": true,
	})
	_, err := tx.Exec(ctx, `INSERT INTO security_universe_snapshots(
        id,universe_id,market,status,observed_at,available_at,source_name,source_document_id,source_url,eligibility_policy,
        asset_count,included_count,excluded_count,delisted_count,failure_detail,metadata)
        VALUES($1,$2,$3,'completed',$4,$4,$5,$6,$7,$8,$9,$10,$11,$12,'',$13)`, snapshotID, "market:"+market, market, observedAt,
		sourceName, "universe-snapshot:"+market+":"+observedAt.Format(time.RFC3339Nano), sourceURL, policy, len(assets), included, excluded, delisted, metadata)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("persist security universe snapshot: %w", err)
	}
	batch := &pgx.Batch{}
	for _, asset := range assets {
		status, reasons := "included", []string{"provider_snapshot_member", "identity_validated", "eligibility_policy_passed"}
		if !asset.Active {
			status, reasons = "delisted", []string{"provider_marked_inactive"}
		}
		reasonJSON, _ := json.Marshal(reasons)
		identityJSON, _ := json.Marshal(map[string]any{"asset_id": asset.ID, "symbol": asset.Symbol, "exchange_or_provider": asset.Exchange, "currency": asset.Currency})
		batch.Queue(`INSERT INTO security_universe_memberships(snapshot_id,asset_id,membership_status,effective_at,available_at,reason_codes,source_identity)
            VALUES($1,$2,$3,$4,$4,$5,$6)`, snapshotID, asset.ID, status, observedAt, reasonJSON, identityJSON)
	}
	for assetID := range existing {
		if _, ok := incoming[assetID]; ok {
			continue
		}
		reasonJSON, _ := json.Marshal([]string{"not_present_in_provider_snapshot", "delisting_not_inferred"})
		identityJSON, _ := json.Marshal(map[string]any{"asset_id": assetID, "identity_source": "prior_asset_record"})
		batch.Queue(`INSERT INTO security_universe_memberships(snapshot_id,asset_id,membership_status,effective_at,available_at,reason_codes,source_identity)
            VALUES($1,$2,'excluded',$3,$3,$4,$5)`, snapshotID, assetID, observedAt, reasonJSON, identityJSON)
	}
	results := tx.SendBatch(ctx, batch)
	if err := results.Close(); err != nil {
		return "", 0, 0, 0, fmt.Errorf("persist security universe memberships: %w", err)
	}
	return snapshotID, included, excluded, delisted, nil
}

func (runtime *masterdataRuntime) universeSource(market string) (string, string) {
	switch market {
	case "CN", "HK":
		return "Market Adapter", marketPriceSourceURL(runtime.cfg.MarketAdapterURL, "/v1/assets/universe")
	case "US":
		return "FMP", marketPriceSourceURL(runtime.cfg.FMPBaseURL, "/company-screener")
	case "CRYPTO":
		return "CoinGecko", marketPriceSourceURL(runtime.cfg.CoinGeckoURL, "/coins/list")
	default:
		return "configured provider", ""
	}
}

func (runtime *masterdataRuntime) universeEligibilityPolicy(market string) map[string]any {
	policy := map[string]any{
		"version": "market-universe-eligibility-v1", "market": market, "minimum_asset_count": minimumMarketCounts[market],
		"required_identity_fields":    []string{"asset_id", "symbol", "name", "exchange_or_provider", "currency"},
		"cross_market_assets_allowed": false, "missing_member_handling": "excluded_not_assumed_delisted",
	}
	if market == "US" {
		policy["us_supported_exchanges"] = []string{"NASDAQ", "NYSE", "AMEX", "OTC_ADR_ONLY"}
	}
	return policy
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
	{task: refreshTrackedFundamentalsTask, interval: 24 * time.Hour},
	{task: runScheduledFundamentalTask, interval: time.Hour},
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
	return true
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
