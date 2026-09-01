package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	evaluateOutcomesTask          = "market_loop.evaluate_outcomes"
	refreshEventMarketFactorsTask = "market_loop.refresh_event_market_factors"
	marketFactorEventDays         = 45
	marketFactorBatchSize         = 20
)

type outcomeRuntime struct {
	cfg       config.Config
	db        *pgxpool.Pool
	redis     *redis.Client
	client    *http.Client
	fmpMu     sync.Mutex
	nextFMPAt time.Time
}

type outcomePricePoint struct {
	ObservedAt time.Time
	Close      float64
}

type outcomeBenchmark struct {
	Status string
	Return *float64
}

func NewOutcomeHandlers(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) map[string]Handler {
	runtime := &outcomeRuntime{
		cfg: cfg, db: db, redis: redisClient,
		client: &http.Client{Timeout: 60 * time.Second},
	}
	return map[string]Handler{
		evaluateOutcomesTask:          runtime.evaluateOutcomes,
		refreshEventMarketFactorsTask: runtime.refreshEventMarketFactors,
	}
}

func (runtime *outcomeRuntime) evaluateOutcomes(ctx context.Context, _ Job) (any, error) {
	rows, err := runtime.db.Query(ctx, `
		SELECT r.id,r.as_of,r.payload::jsonb
		FROM recommendations r
		WHERE NOT EXISTS (
			SELECT 1 FROM outcomes o
			WHERE o.recommendation_id=r.id
			  AND o.horizon_days=coalesce(nullif(r.payload->>'horizon_days','')::integer,90)
		)
		ORDER BY r.as_of,r.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type candidate struct {
		id   uuid.UUID
		asOf time.Time
		body []byte
	}
	candidates := make([]candidate, 0)
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.id, &item.asOf, &item.body); err != nil {
			return nil, err
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	created, pending, skipped, failed := 0, 0, 0, 0
	failures := make([]string, 0, 10)
	priceCache := map[string][]outcomePricePoint{}
	for _, item := range candidates {
		recommendation := map[string]any{}
		if err := json.Unmarshal(item.body, &recommendation); err != nil {
			failed++
			failures = appendOutcomeFailure(failures, item.id, err)
			continue
		}
		outcome, state, err := runtime.evaluateRecommendation(ctx, item.id, item.asOf, recommendation, now, priceCache)
		if err != nil {
			failed++
			failures = appendOutcomeFailure(failures, item.id, err)
			continue
		}
		switch state {
		case "pending":
			pending++
		case "skipped":
			skipped++
		case "completed":
			inserted, saveErr := runtime.saveOutcome(ctx, item.id, outcome)
			if saveErr != nil {
				failed++
				failures = appendOutcomeFailure(failures, item.id, saveErr)
				continue
			}
			if inserted {
				created++
			} else {
				skipped++
			}
		}
	}
	return map[string]any{
		"outcomes": created, "pending": pending, "skipped": skipped,
		"failed": failed, "failures": failures,
	}, nil
}

func appendOutcomeFailure(values []string, id uuid.UUID, cause error) []string {
	if len(values) >= 10 {
		return values
	}
	return append(values, fmt.Sprintf("%s: %s", id, truncateRunes(cause.Error(), 300)))
}

func (runtime *outcomeRuntime) evaluateRecommendation(
	ctx context.Context,
	recommendationID uuid.UUID,
	storedAsOf time.Time,
	recommendation map[string]any,
	now time.Time,
	cache map[string][]outcomePricePoint,
) (map[string]any, string, error) {
	status := outcomeSignalStatus(recommendation)
	if status == "technical_failure" || status == "insufficient_evidence" {
		return nil, "skipped", nil
	}
	horizon := int(numberValue(recommendation["horizon_days"]))
	if horizon < 1 {
		horizon = 90
	}
	horizonUnit := strings.TrimSpace(stringValue(recommendation["horizon_unit"]))
	if horizonUnit == "" {
		horizonUnit = "calendar_days"
	}
	start := parseTime(recommendation["as_of"])
	if start.IsZero() {
		start = storedAsOf.UTC()
	}
	if horizonUnit == "calendar_days" && start.AddDate(0, 0, horizon).After(now) {
		return nil, "pending", nil
	}
	asset := objectValue(recommendation["asset"])
	if len(asset) == 0 {
		return nil, "skipped", errors.New("recommendation asset is missing")
	}
	points, err := runtime.cachedPrices(ctx, asset, start, now, cache)
	if err != nil {
		return nil, "failed", err
	}
	window := outcomeWindow(points, start, horizonUnit, horizon)
	if len(window) < 2 {
		return nil, "pending", nil
	}
	entry, exit := window[0], window[len(window)-1]
	rawReturn := exit.Close/entry.Close - 1
	benchmark, err := runtime.benchmarkReturn(ctx, asset, entry.ObservedAt, exit.ObservedAt, now, cache)
	if err != nil {
		benchmark = outcomeBenchmark{Status: "missing"}
	}
	var alpha *float64
	if benchmark.Return != nil {
		value := rawReturn - *benchmark.Return
		alpha = &value
	}

	neutralBand := math.Min(.10, math.Max(.005, .02*math.Sqrt(float64(horizon)/20)))
	actual := [3]float64{0, 0, 1}
	if rawReturn > neutralBand {
		actual = [3]float64{1, 0, 0}
	} else if rawReturn >= -neutralBand {
		actual = [3]float64{0, 1, 0}
	}
	predicted := [3]float64{
		numberValue(recommendation["bull_probability"]),
		numberValue(recommendation["base_probability"]),
		numberValue(recommendation["bear_probability"]),
	}
	brier := 0.0
	for index := range predicted {
		brier += math.Pow(predicted[index]-actual[index], 2)
	}
	brier /= 3
	directionScore := numberValue(recommendation["direction_score"])
	if recommendation["direction_score"] == nil {
		directionScore = numberValue(recommendation["score"])
	}
	threshold := 15.0
	if stringValue(recommendation["scoring_version"]) == "llm-direction-v3" {
		threshold = 30
	}
	directionCorrect := math.Abs(rawReturn) <= neutralBand
	if math.Abs(directionScore) >= threshold {
		directionCorrect = (directionScore > 0 && rawReturn > neutralBand) || (directionScore < 0 && rawReturn < -neutralBand)
	}
	peak, maxDrawdown := entry.Close, 0.0
	for _, point := range window {
		peak = math.Max(peak, point.Close)
		maxDrawdown = math.Min(maxDrawdown, point.Close/peak-1)
	}
	outcomeID := uuid.New()
	return map[string]any{
		"id": outcomeID.String(), "recommendation_id": recommendationID.String(), "horizon_days": horizon,
		"raw_return": rawReturn, "benchmark_return": benchmark.Return, "alpha": alpha,
		"benchmark_status": benchmark.Status, "entry_at": iso(entry.ObservedAt), "exit_at": iso(exit.ObservedAt),
		"entry_price": entry.Close, "exit_price": exit.Close, "direction_correct": directionCorrect,
		"brier_score": brier, "max_drawdown": maxDrawdown, "thesis_invalidated": false, "observed_at": iso(now),
	}, "completed", nil
}

func outcomeSignalStatus(recommendation map[string]any) string {
	if value := strings.TrimSpace(stringValue(recommendation["signal_status"])); value != "" {
		return value
	}
	score := numberValue(recommendation["score"])
	version := stringValue(recommendation["scoring_version"])
	if version == "llm-direction-v3" {
		if recommendation["direction_score"] != nil {
			score = numberValue(recommendation["direction_score"])
		}
		return ternary(math.Abs(score) < 30, "neutral", "directional")
	}
	if version == "short-term-impact-v1" {
		return ternary(math.Abs(score) < 15, "neutral", "directional")
	}
	if !boolValue(recommendation["evidence_complete"]) {
		return "insufficient_evidence"
	}
	return ternary(math.Abs(score) < 20, "neutral", "directional")
}

func outcomeWindow(points []outcomePricePoint, start time.Time, unit string, horizon int) []outcomePricePoint {
	entryIndex := -1
	for index, point := range points {
		if !point.ObservedAt.Before(start) {
			entryIndex = index
			break
		}
	}
	if entryIndex < 0 {
		return nil
	}
	if unit == "trading_sessions" {
		exitIndex := entryIndex + horizon
		if exitIndex >= len(points) {
			return nil
		}
		return points[entryIndex : exitIndex+1]
	}
	target := start.AddDate(0, 0, horizon)
	for index := entryIndex + 1; index < len(points); index++ {
		if !points[index].ObservedAt.Before(target) {
			return points[entryIndex : index+1]
		}
	}
	return nil
}

func (runtime *outcomeRuntime) benchmarkReturn(
	ctx context.Context,
	asset map[string]any,
	entryAt, exitAt, observedAt time.Time,
	cache map[string][]outcomePricePoint,
) (outcomeBenchmark, error) {
	market := strings.ToUpper(stringValue(asset["market"]))
	benchmark := map[string]any{}
	switch market {
	case "US":
		benchmark = map[string]any{"asset_id": "equity:NYSEARCA:SPY", "asset_class": "equity", "market": "US", "symbol": "SPY"}
	case "CN":
		benchmark = map[string]any{"asset_id": "index:CN:000300", "asset_class": "equity", "market": "CN", "symbol": "000300"}
	case "HK":
		benchmark = map[string]any{"asset_id": "index:HK:HSI", "asset_class": "equity", "market": "HK", "symbol": "HSI"}
	case "CRYPTO":
		benchmark = map[string]any{"asset_id": "crypto:coingecko:bitcoin", "asset_class": "crypto", "market": "CRYPTO", "symbol": "BTC"}
	default:
		return outcomeBenchmark{Status: "missing"}, nil
	}
	if strings.EqualFold(stringValue(asset["asset_id"]), stringValue(benchmark["asset_id"])) ||
		(strings.EqualFold(stringValue(asset["symbol"]), stringValue(benchmark["symbol"])) && market == strings.ToUpper(stringValue(benchmark["market"]))) {
		return outcomeBenchmark{Status: "self_benchmark"}, nil
	}
	points, err := runtime.cachedPrices(ctx, benchmark, entryAt, observedAt, cache)
	if err != nil {
		return outcomeBenchmark{Status: "missing"}, err
	}
	window := outcomeWindowUntil(points, entryAt, exitAt)
	if len(window) < 2 {
		return outcomeBenchmark{Status: "missing"}, nil
	}
	value := window[len(window)-1].Close/window[0].Close - 1
	return outcomeBenchmark{Status: "available", Return: &value}, nil
}

func outcomeWindowUntil(points []outcomePricePoint, start, target time.Time) []outcomePricePoint {
	entryIndex := -1
	for index, point := range points {
		if !point.ObservedAt.Before(start) {
			entryIndex = index
			break
		}
	}
	if entryIndex < 0 {
		return nil
	}
	for index := entryIndex + 1; index < len(points); index++ {
		if !points[index].ObservedAt.Before(target) {
			return points[entryIndex : index+1]
		}
	}
	return nil
}

func (runtime *outcomeRuntime) cachedPrices(
	ctx context.Context,
	asset map[string]any,
	start, end time.Time,
	cache map[string][]outcomePricePoint,
) ([]outcomePricePoint, error) {
	key := strings.Join([]string{stringValue(asset["asset_id"]), stringValue(asset["market"]), stringValue(asset["symbol"]), start.Format("2006-01-02"), end.Format("2006-01-02")}, "|")
	if values, ok := cache[key]; ok {
		return values, nil
	}
	payload, err := runtime.fetchPrices(ctx, asset, start, end)
	if err != nil {
		return nil, err
	}
	points := normalizeOutcomePrices(payload, end)
	cache[key] = points
	return points, nil
}

func (runtime *outcomeRuntime) fetchPrices(ctx context.Context, asset map[string]any, start, end time.Time) (any, error) {
	market := strings.ToUpper(stringValue(asset["market"]))
	symbol := strings.TrimSpace(stringValue(asset["symbol"]))
	if symbol == "" {
		return nil, errors.New("asset symbol is missing")
	}
	if market == "CN" || market == "HK" {
		return runtime.requestJSON(ctx, http.MethodPost, runtime.cfg.MarketAdapterURL+"/v1/prices", map[string]any{
			"symbol": symbol, "market": market, "start": start.Format("2006-01-02"), "end": end.Format("2006-01-02"),
		}, nil)
	}
	if market == "CRYPTO" || stringValue(asset["asset_class"]) == "crypto" {
		coinID := strings.TrimPrefix(stringValue(asset["asset_id"]), "crypto:coingecko:")
		if coinID == "" || coinID == stringValue(asset["asset_id"]) {
			coinID = strings.ToLower(symbol)
		}
		days := max(1, min(365, int(end.Sub(start).Hours()/24)))
		endpoint := fmt.Sprintf("%s/coins/%s/market_chart?vs_currency=usd&days=%d&interval=daily", runtime.cfg.CoinGeckoURL, url.PathEscape(coinID), days)
		return runtime.requestJSON(ctx, http.MethodGet, endpoint, nil, nil)
	}
	active := (&discoveryRuntime{cfg: runtime.cfg, db: runtime.db}).effectiveDiscoveryConfig(ctx)
	if active.FMPAccessToken == "" {
		return nil, errors.New("FMP access token is not configured")
	}
	if err := runtime.waitForFMP(ctx, active.FMPRateLimit); err != nil {
		return nil, err
	}
	query := url.Values{"symbol": []string{symbol}, "from": []string{start.Format("2006-01-02")}, "to": []string{end.Format("2006-01-02")}}
	return runtime.requestJSON(ctx, http.MethodGet, active.FMPBaseURL+"/historical-price-eod/full?"+query.Encode(), nil, map[string]string{"apikey": active.FMPAccessToken})
}

func (runtime *outcomeRuntime) waitForFMP(ctx context.Context, perMinute int) error {
	if perMinute < 1 {
		perMinute = 1
	}
	spacing := time.Minute / time.Duration(perMinute)
	runtime.fmpMu.Lock()
	now := time.Now()
	wait := runtime.nextFMPAt.Sub(now)
	if wait < 0 {
		wait = 0
	}
	runtime.nextFMPAt = now.Add(wait + spacing)
	runtime.fmpMu.Unlock()
	if wait == 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (runtime *outcomeRuntime) requestJSON(ctx context.Context, method, endpoint string, body any, headers map[string]string) (any, error) {
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
	payload, err := io.ReadAll(io.LimitReader(response.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("market data HTTP %d: %s", response.StatusCode, truncateRunes(string(payload), 200))
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func normalizeOutcomePrices(payload any, notAfter time.Time) []outcomePricePoint {
	raw := payload
	if object := objectValue(payload); object != nil {
		for _, key := range []string{"historical", "data", "results", "items"} {
			if object[key] != nil {
				raw = object[key]
				break
			}
		}
		if object["prices"] != nil {
			raw = object["prices"]
		}
	}
	byTime := map[int64]outcomePricePoint{}
	for _, value := range anySlice(raw) {
		if pair := anySlice(value); len(pair) >= 2 {
			stamp := outcomeTimestamp(pair[0])
			close := outcomeNumber(pair[1])
			if !stamp.IsZero() && close > 0 && !stamp.After(notAfter) {
				byTime[stamp.UnixNano()] = outcomePricePoint{ObservedAt: stamp, Close: close}
			}
			continue
		}
		item := objectValue(value)
		if item == nil {
			continue
		}
		stamp := time.Time{}
		for _, key := range []string{"date", "datetime", "timestamp", "time", "日期"} {
			if item[key] != nil {
				stamp = outcomeTimestamp(item[key])
				break
			}
		}
		close := 0.0
		for _, key := range []string{"close", "adjClose", "price", "收盘"} {
			if item[key] != nil {
				close = outcomeNumber(item[key])
				break
			}
		}
		if !stamp.IsZero() && close > 0 && !stamp.After(notAfter) {
			byTime[stamp.UnixNano()] = outcomePricePoint{ObservedAt: stamp, Close: close}
		}
	}
	result := make([]outcomePricePoint, 0, len(byTime))
	for _, point := range byTime {
		result = append(result, point)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ObservedAt.Before(result[j].ObservedAt) })
	return result
}

func outcomeTimestamp(value any) time.Time {
	switch typed := value.(type) {
	case float64:
		seconds := typed
		if math.Abs(seconds) >= 100_000_000_000 {
			seconds /= 1000
		}
		return time.Unix(0, int64(seconds*float64(time.Second))).UTC()
	case json.Number:
		parsed, _ := typed.Float64()
		return outcomeTimestamp(parsed)
	case string:
		raw := strings.TrimSpace(typed)
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02", "2006-01-02 15:04:05"} {
			if parsed, err := time.Parse(layout, raw); err == nil {
				return parsed.UTC()
			}
		}
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
			return outcomeTimestamp(parsed)
		}
	case time.Time:
		return typed.UTC()
	}
	return time.Time{}
}

func outcomeNumber(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case json.Number:
		result, _ := typed.Float64()
		return result
	case string:
		result, _ := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return result
	}
	return 0
}

func (runtime *outcomeRuntime) saveOutcome(ctx context.Context, recommendationID uuid.UUID, outcome map[string]any) (bool, error) {
	id, err := uuid.Parse(stringValue(outcome["id"]))
	if err != nil {
		return false, err
	}
	body, _ := json.Marshal(outcome)
	tag, err := runtime.db.Exec(ctx, `
		INSERT INTO outcomes(id,recommendation_id,horizon_days,observed_at,payload)
		SELECT $1,$2,$3,$4,$5::jsonb
		WHERE NOT EXISTS (
			SELECT 1 FROM outcomes WHERE recommendation_id=$2 AND horizon_days=$3
		)`, id, recommendationID, int(numberValue(outcome["horizon_days"])), parseTime(outcome["observed_at"]), body)
	return tag.RowsAffected() == 1, err
}

func (runtime *outcomeRuntime) refreshEventMarketFactors(ctx context.Context, _ Job) (any, error) {
	completed := map[string]int{}
	active := map[string]bool{}
	runRows, err := runtime.db.Query(ctx, `SELECT payload::jsonb FROM research_runs ORDER BY updated_at DESC LIMIT 50000`)
	if err != nil {
		return nil, err
	}
	for runRows.Next() {
		var body []byte
		if runRows.Scan(&body) != nil {
			continue
		}
		run := map[string]any{}
		if json.Unmarshal(body, &run) != nil {
			continue
		}
		for _, raw := range anySlice(run["analysis_steps"]) {
			step := objectValue(raw)
			if stringValue(step["phase"]) != "market_factor_refresh_queue" {
				continue
			}
			metrics := objectValue(step["metrics"])
			eventID := fallbackString(stringValue(metrics["event_id"]), stringValue(run["event_id"]))
			session := int(numberValue(metrics["target_session_days"]))
			assetID := stringValue(objectValue(run["asset"])["asset_id"])
			if eventID == "" || assetID == "" || (session != 1 && session != 5 && session != 20) {
				continue
			}
			key := eventID + "|" + assetID
			switch stringValue(run["status"]) {
			case "queued", "running", "verifying":
				active[key] = true
			case "completed", "insufficient_evidence":
				completed[key] = max(completed[key], session)
			}
		}
	}
	runRows.Close()
	if err := runRows.Err(); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	eventRows, err := runtime.db.Query(ctx, `SELECT payload::jsonb FROM news_events WHERE published_at>=now()-interval '45 days' AND published_at<=now() ORDER BY observed_at DESC LIMIT 5000`)
	if err != nil {
		return nil, err
	}
	defer eventRows.Close()
	queued, failed, skipped := 0, 0, 0
	for eventRows.Next() && queued < marketFactorBatchSize {
		var body []byte
		if eventRows.Scan(&body) != nil {
			failed++
			continue
		}
		event := map[string]any{}
		if json.Unmarshal(body, &event) != nil {
			failed++
			continue
		}
		published := parseTime(event["published_at"])
		ageDays := now.Sub(published).Hours() / 24
		if published.IsZero() || ageDays < 0 || ageDays > marketFactorEventDays {
			continue
		}
		candidate := bestOutcomeCandidate(anySlice(event["candidates"]))
		asset := objectValue(candidate["asset"])
		if candidate == nil || math.Min(numberValue(candidate["relevance"]), numberValue(candidate["mapping_confidence"])) < .65 || asset == nil {
			continue
		}
		key := stringValue(event["id"]) + "|" + stringValue(asset["asset_id"])
		if active[key] {
			skipped++
			continue
		}
		target := dueMarketFactorSession(ageDays, completed[key])
		if target == 0 {
			continue
		}
		inserted, reason, queueErr := runtime.enqueueMarketFactorResearch(ctx, event, asset, target, now)
		if queueErr != nil {
			failed++
			continue
		}
		if inserted {
			queued++
			active[key] = true
		} else if reason != "" {
			skipped++
		}
	}
	if err := eventRows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"queued": queued, "failed": failed, "skipped": skipped}, nil
}

func bestOutcomeCandidate(values []any) map[string]any {
	var best map[string]any
	for _, raw := range values {
		candidate := objectValue(raw)
		if candidate == nil {
			continue
		}
		if best == nil || numberValue(candidate["relevance"]) > numberValue(best["relevance"]) ||
			(numberValue(candidate["relevance"]) == numberValue(best["relevance"]) && numberValue(candidate["mapping_confidence"]) > numberValue(best["mapping_confidence"])) {
			best = candidate
		}
	}
	return best
}

func dueMarketFactorSession(ageDays float64, completed int) int {
	for _, item := range []struct {
		session int
		age     float64
	}{{20, 30}, {5, 8}, {1, 2}} {
		if ageDays >= item.age && completed < item.session {
			return item.session
		}
	}
	return 0
}

func (runtime *outcomeRuntime) enqueueMarketFactorResearch(ctx context.Context, event, asset map[string]any, target int, now time.Time) (bool, string, error) {
	assetID, eventID := stringValue(asset["asset_id"]), stringValue(event["id"])
	if assetID == "" || eventID == "" {
		return false, "invalid", nil
	}
	eventUUID, err := uuid.Parse(eventID)
	if err != nil {
		return false, "invalid", nil
	}
	tx, err := runtime.db.Begin(ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, assetID); err != nil {
		return false, "", err
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM research_runs WHERE asset_id=$1 AND status IN ('queued','running','verifying'))`, assetID).Scan(&exists); err != nil {
		return false, "", err
	}
	if exists {
		return false, "active", tx.Commit(ctx)
	}
	if runtime.cfg.ResearchCooldown > 0 {
		if err := tx.QueryRow(ctx, `SELECT EXISTS(
			SELECT 1 FROM research_runs WHERE asset_id=$1
			  AND status IN ('completed','insufficient_evidence')
			  AND coalesce((payload->>'historical_replay')::boolean,false)=false
			  AND coalesce((payload->>'completed_at')::timestamptz,updated_at)>now()-$2::interval
		)`, assetID, interval(runtime.cfg.ResearchCooldown)).Scan(&exists); err != nil {
			return false, "", err
		}
		if exists {
			return false, "cooldown", tx.Commit(ctx)
		}
	}

	shared := &ExtractRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis, client: runtime.client}
	instanceID := shared.selectDownstreamInstance(ctx, "research", len(runtime.cfg.ResearchURLs))
	runID, taskID := uuid.New(), uuid.New()
	steps := append([]any{}, anySlice(event["analysis_steps"])...)
	steps = append(steps,
		analysisStep("market_factor_refresh_queue", "queued", "go-worker", fmt.Sprintf("事件后 %d 日市场反应窗口已成熟，已创建一次因子重评。", target), map[string]any{"event_id": eventID, "target_session_days": target}),
		analysisStep("research_queue", "queued", "go-worker", fmt.Sprintf("已为主标的 %s 创建深度研究任务。", stringValue(asset["symbol"])), map[string]any{"instance_id": instanceID, "priority": 3}),
	)
	run := map[string]any{
		"id": runID.String(), "event_id": eventID, "trigger_event_ids": []any{eventID}, "asset": asset, "status": "queued",
		"as_of": iso(now), "historical_replay": false, "retry_of_run_id": nil, "retry_attempt": 0,
		"celery_task_id": taskID.String(), "model_instance_id": instanceID, "coalesced_into_run_id": nil, "retryable_reason": nil,
		"verification_round": 0, "missing_requirements": []any{}, "contradictions": []any{}, "evidence": []any{},
		"recommendation": nil, "error": nil, "analysis_steps": steps, "created_at": iso(now), "started_at": nil, "completed_at": nil, "updated_at": iso(now),
	}
	runBody, _ := json.Marshal(run)
	if _, err := tx.Exec(ctx, `INSERT INTO research_runs(id,event_id,asset_id,status,payload,created_at,updated_at) VALUES($1,$2,$3,'queued',$4,$5,$5)`, runID, eventUUID, assetID, runBody, now); err != nil {
		return false, "", err
	}
	jobBody, _ := json.Marshal(map[string]any{"args": []any{assetID, eventID, runID.String()}, "kwargs": map[string]any{"model_instance_id": instanceID}})
	if _, err := tx.Exec(ctx, `INSERT INTO go_jobs(id,queue,task_type,payload,status,priority,max_attempts,available_at,dedupe_key,created_at,updated_at) VALUES($1,'research',$2,$3,'queued',3,3,now(),$4,now(),now())`, taskID, researchAssetTask, jobBody, "research-run:"+runID.String()); err != nil {
		return false, "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, "", err
	}
	research := &researchRuntime{cfg: runtime.cfg, db: runtime.db, redis: runtime.redis, client: runtime.client}
	research.recordResearchTask(ctx, taskID.String(), "asset_research", runID.String(), stringValue(asset["name"]), stringValue(asset["symbol"]), instanceID)
	_ = runtime.redis.Set(ctx, "market-loop:research:dispatch:"+runID.String(), taskID.String(), 30*24*time.Hour).Err()
	return true, "", nil
}

type outcomeSchedule struct {
	task     string
	interval time.Duration
}

var outcomeSchedules = []outcomeSchedule{
	{task: evaluateOutcomesTask, interval: 24 * time.Hour},
	{task: refreshEventMarketFactorsTask, interval: 24 * time.Hour},
}

type OutcomeScheduler struct {
	cfg   config.Config
	store *Store
	redis *redis.Client
}

func NewOutcomeScheduler(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) *OutcomeScheduler {
	return &OutcomeScheduler{cfg: cfg, store: NewStore(db), redis: redisClient}
}

func (scheduler *OutcomeScheduler) Enabled() bool {
	return completedWorkerLane(scheduler.cfg, "outcomes")
}

func (scheduler *OutcomeScheduler) Tick(ctx context.Context) error {
	if !scheduler.Enabled() {
		return nil
	}
	for _, spec := range outcomeSchedules {
		key := "market-loop:go-schedule:" + spec.task
		claimed, err := scheduler.redis.SetNX(ctx, key, iso(time.Now()), spec.interval).Result()
		if err != nil {
			return err
		}
		if !claimed {
			continue
		}
		_, err = scheduler.store.Enqueue(ctx, EnqueueParams{Queue: "outcomes", TaskType: spec.task, Payload: taskEnvelope{Args: []any{}, Kwargs: map[string]any{}}, Priority: 5, MaxAttempts: 3, DedupeKey: "scheduled:" + spec.task})
		if err != nil {
			_ = scheduler.redis.Del(ctx, key).Err()
			return err
		}
	}
	return nil
}
