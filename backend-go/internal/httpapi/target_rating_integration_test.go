package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestStepLimitedRatingAgainstIsolatedPostgres(t *testing.T) {
	if os.Getenv("TARGET_RATING_TEST_ISOLATED") != "1" {
		t.Skip("TARGET_RATING_TEST_ISOLATED=1 is required")
	}
	databaseURL := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if databaseURL == "" {
		t.Fatal("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	truncate := `TRUNCATE TABLE recommendations,research_runs,event_research_runs,news_events,assets RESTART IDENTITY CASCADE`
	if _, err = pool.Exec(ctx, truncate); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), truncate) })

	const assetID = "equity:NASDAQ:NVDA"
	if _, err = pool.Exec(ctx, `INSERT INTO assets(
		id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,
		sector_id,industry_id,raw_sector,raw_industry,instrument_type,lot_size,active
	) VALUES($1,'equity','US','NVDA','NVIDIA Corporation','NASDAQ','USD',$2,'[]','[]','','','','','common_stock',1,true)`,
		assetID, `["NVIDIA"]`); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	ratings := []string{"strongly_bullish", "strongly_bullish", "strongly_bearish"}
	scores := []int{90, 90, -90}
	for index := range ratings {
		eventID := integrationUUID(1, index)
		eventRunID := integrationUUID(2, index)
		researchRunID := integrationUUID(3, index)
		recommendationID := integrationUUID(4, index)
		publishedAt := base.Add(time.Duration(index) * time.Hour)
		if _, err = pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of)
			VALUES($1,$2,'policy','{}',0.9,$3,$3,$3)`, eventID, "rating event "+eventID, publishedAt); err != nil {
			t.Fatal(err)
		}

		eventRating, eventScore := ratings[index], scores[index]
		if index == 2 {
			// The event report stays bullish for NVIDIA, while its target-specific
			// research turns bearish. The latter must win for the shared event_id.
			eventRating, eventScore = "strongly_bullish", 90
		}
		impacts := []map[string]any{
			integrationImpact("美国经济", "economy", ratings[index], scores[index]),
			integrationImpact("半导体行业", "sector", ratings[index], scores[index]),
			integrationImpact("WTI 原油", "commodity_price", ratings[index], scores[index]),
			integrationImpact("NVIDIA", "tradable_asset", eventRating, eventScore),
		}
		reportPayload, _ := json.Marshal(map[string]any{
			"as_of":  publishedAt,
			"report": map[string]any{"evidence_complete": true, "news_confidence": .9, "impacts": impacts},
		})
		updatedAt := publishedAt.Add(10 * time.Minute)
		if _, err = pool.Exec(ctx, `INSERT INTO event_research_runs(id,event_id,status,payload,created_at,updated_at)
			VALUES($1,$2,'completed',$3,$4,$4)`, eventRunID, eventID, reportPayload, updatedAt); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO research_runs(id,event_id,asset_id,status,payload,created_at,updated_at)
			VALUES($1,$2,$3,'completed','{}',$4,$4)`, researchRunID, eventID, assetID, updatedAt.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		recommendationPayload, _ := json.Marshal(map[string]any{
			"asset":  map[string]any{"asset_id": assetID, "asset_class": "equity", "market": "US", "symbol": "NVDA", "name": "NVIDIA Corporation"},
			"rating": ratings[index], "direction_score": scores[index], "score": scores[index],
			"rating_confidence": .9, "news_confidence": .9, "confidence": .9,
			"evidence_complete": true, "signal_status": "directional", "scoring_version": "llm-direction-v3",
		})
		if _, err = pool.Exec(ctx, `INSERT INTO recommendations(id,run_id,asset_id,score,rating,confidence,as_of,payload)
			VALUES($1,$2,$3,$4,$5,0.9,$6,$7)`, recommendationID, researchRunID, assetID, scores[index], ratings[index], updatedAt.Add(time.Minute), recommendationPayload); err != nil {
			t.Fatal(err)
		}
	}

	server := &Server{db: pool}
	request := httptest.NewRequest("GET", "/api/v1/target-changes?kind=macro", nil)
	macro, err := server.macroTargetChanges(request)
	if err != nil {
		t.Fatal(err)
	}
	wantMacroTypes := map[string]bool{"economy": false, "sector": false}
	for _, item := range macro {
		if _, found := wantMacroTypes[stringValue(item["target_type"])]; !found {
			continue
		}
		assertAdjacentRatingState(t, item)
		state := objectValue(item["rating_state"])
		if stringValue(state["current"]) != "bullish" {
			t.Fatalf("macro history was not replayed one step at a time: %#v", item)
		}
		wantMacroTypes[stringValue(item["target_type"])] = true
	}
	for targetType, found := range wantMacroTypes {
		if !found {
			t.Fatalf("normalized %s target was not returned: %#v", targetType, macro)
		}
	}

	request = httptest.NewRequest("GET", "/api/v1/target-changes?kind=asset", nil)
	concrete, err := server.concreteTargetChanges(request)
	if err != nil {
		t.Fatal(err)
	}
	seenAsset, seenCommodity := false, false
	for _, item := range concrete {
		assertAdjacentRatingState(t, item)
		switch {
		case stringValue(item["key"]) == assetID:
			seenAsset = true
			state := objectValue(item["rating_state"])
			if stringValue(state["current"]) != "bullish" || stringValue(objectValue(item["latest_event_signal"])["rating"]) != "strongly_bearish" {
				t.Fatalf("target-specific research did not win cross-source event deduplication: %#v", item)
			}
		case stringValue(item["target_type"]) == "commodity_price":
			seenCommodity = true
		}
	}
	if !seenAsset || !seenCommodity {
		t.Fatalf("concrete asset/commodity normalization failed: %#v", concrete)
	}

	request = httptest.NewRequest("GET", "/api/v1/target-changes?kind=asset&scope=current&rating=bullish", nil)
	current, err := server.currentAssetRatings(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 1 || stringValue(current[0]["key"]) != assetID {
		t.Fatalf("rating filter did not use stable rating_state: %#v", current)
	}
}

func integrationUUID(group, index int) string {
	return fmt.Sprintf("00000000-0000-0000-%04d-%012d", group, index+1)
}

func integrationImpact(name, targetType, rating string, score int) map[string]any {
	return map[string]any{
		"target_name": name, "target_type": targetType, "rating": rating, "direction_score": score,
		"rating_confidence": .9, "missing_information": []any{},
		"factors": map[string]any{"persistence": .9, "realization_probability": .9},
	}
}

func assertAdjacentRatingState(t *testing.T, item map[string]any) {
	t.Helper()
	state := objectValue(item["rating_state"])
	previous := ratingIndex(stringValue(state["previous"]))
	current := ratingIndex(stringValue(state["current"]))
	if ratingDistance(previous, current) > 1 {
		t.Fatalf("rating_state crossed multiple levels: %#v", state)
	}
}
