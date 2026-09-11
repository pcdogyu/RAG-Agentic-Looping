package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestHistoricalNewsEvidenceUsesPointInTimeRelationPriority(t *testing.T) {
	if os.Getenv("RESEARCH_HISTORY_TEST_ISOLATED") != "1" {
		t.Skip("RESEARCH_HISTORY_TEST_ISOLATED=1 is required")
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
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}

	boundary := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	currentEventID, currentNewsID := uuid.New(), uuid.New()
	assetID := "equity:NYSE:HIST-" + strings.ToUpper(uuid.NewString()[:8])
	issuerAssetID := "equity:NASDAQ:HIST-" + strings.ToUpper(uuid.NewString()[:8])
	issuerID := "issuer:history-" + uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO assets(id,asset_class,market,symbol,name,exchange_or_provider,currency,aliases,products,competitors,lot_size,active,issuer_id)
		VALUES($1,'equity','US','HIST','History Corporation','NYSE','USD','[]','[]','[]',1,true,$3),
		      ($2,'equity','US','HISTB','History Corporation Class B','NASDAQ','USD','[]','[]','[]',1,true,$3)`, assetID, issuerAssetID, issuerID); err != nil {
		t.Fatal(err)
	}
	type fixture struct {
		relatedBy string
		age       time.Duration
		future    bool
	}
	fixtures := []fixture{{relatedBy: "asset", age: 48 * time.Hour}, {relatedBy: "issuer", age: 24 * time.Hour}, {relatedBy: "unrelated", age: 12 * time.Hour}, {relatedBy: "asset", age: 3*24*time.Hour + time.Second}, {relatedBy: "asset", future: true}}
	eventIDs, newsIDs := []uuid.UUID{}, []uuid.UUID{}
	for index, item := range fixtures {
		eventID, newsID := uuid.New(), uuid.New()
		eventIDs, newsIDs = append(eventIDs, eventID), append(newsIDs, newsID)
		published := boundary.Add(-item.age)
		if item.future {
			published = boundary.Add(time.Hour)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO news_items(id,source,source_quality,title,summary,url,language,published_at,observed_at,as_of,content_hash,symbols,raw_metadata) VALUES($1,'history-test','professional',$2,$2,$3,'en',$4,$4,$4,$5,'[]','{}')`, newsID, "history item "+item.relatedBy, "https://history.test/"+newsID.String(), published, fmt.Sprintf("%064x", 1000+index)); err != nil {
			t.Fatal(err)
		}
		payload := map[string]any{"id": eventID.String(), "news_item_ids": []string{newsID.String()}, "candidates": []any{}, "industry_ids": []string{}, "entities": []string{}}
		switch item.relatedBy {
		case "asset":
			payload["candidates"] = []any{map[string]any{"asset": map[string]any{"asset_id": assetID, "issuer_id": issuerID}}}
		case "issuer":
			payload["candidates"] = []any{map[string]any{"asset": map[string]any{"asset_id": issuerAssetID, "issuer_id": issuerID}}}
		}
		body, _ := json.Marshal(payload)
		if _, err := pool.Exec(ctx, `INSERT INTO news_events(id,headline,event_type,payload,priority,published_at,observed_at,as_of) VALUES($1,$2,'other',$3,.5,$4,$4,$4)`, eventID, "history item "+item.relatedBy, body, published); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM news_events WHERE id=ANY($1)`, eventIDs)
		_, _ = pool.Exec(context.Background(), `DELETE FROM news_items WHERE id=ANY($1)`, newsIDs)
		_, _ = pool.Exec(context.Background(), `DELETE FROM assets WHERE id=ANY($1)`, []string{assetID, issuerAssetID})
	}()

	runtime := &researchRuntime{cfg: config.Config{ResearchHistoryWindow: 3 * 24 * time.Hour, ResearchHistoryItems: 20}, db: pool}
	event := map[string]any{"id": currentEventID.String()}
	values, err := runtime.historicalNewsEvidence(ctx, event, []string{assetID}, []string{currentNewsID.String()}, boundary)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 {
		t.Fatalf("historical evidence count=%d want 2: %#v", len(values), values)
	}
	for index, want := range []string{"asset", "issuer"} {
		if values[index].ContextRole != "historical_context" || values[index].RelatedBy != want || !values[index].PublishedAt.Before(boundary) {
			t.Fatalf("history[%d]=%#v want relation %s", index, values[index], want)
		}
	}
}
