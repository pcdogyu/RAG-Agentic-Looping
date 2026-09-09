package marketdata

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/migrate"
)

func TestStorePersistsFirstAvailabilityIdempotently(t *testing.T) {
	dsn := strings.Replace(os.Getenv("TEST_DATABASE_URL"), "postgresql+psycopg://", "postgresql://", 1)
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "market_data_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") }()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}

	observed := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	firstAvailable := observed.Add(24 * time.Hour)
	value := PriceObservation{AssetID: "equity:NYSEARCA:SPY", Market: "US", Currency: "USD", ObservedAt: observed, AvailableAt: firstAvailable, Price: 650.25, PriceField: "adjusted_close", TimePrecision: "daily_close", SourceName: "FMP", SourceDocumentID: "price-series:SPY"}
	created, err := NewStore(pool).Save(ctx, value)
	if err != nil || !created {
		t.Fatalf("first save created=%v err=%v", created, err)
	}
	value.AvailableAt = firstAvailable.Add(time.Hour)
	created, err = NewStore(pool).Save(ctx, value)
	if err != nil || created {
		t.Fatalf("duplicate save created=%v err=%v", created, err)
	}
	var count int
	var storedAvailable time.Time
	if err = pool.QueryRow(ctx, `SELECT count(*),min(available_at) FROM market_price_observations WHERE asset_id=$1`, value.AssetID).Scan(&count, &storedAvailable); err != nil {
		t.Fatal(err)
	}
	if count != 1 || !storedAvailable.Equal(firstAvailable) {
		t.Fatalf("stored count=%d available_at=%s", count, storedAvailable)
	}
	value.Price = 651.00
	created, err = NewStore(pool).Save(ctx, value)
	if err != nil || !created {
		t.Fatalf("provider revision created=%v err=%v", created, err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM market_price_observations WHERE asset_id=$1`, value.AssetID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("provider revision count=%d err=%v", count, err)
	}
}
