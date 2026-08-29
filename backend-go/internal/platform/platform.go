package platform

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/redis/go-redis/v9"
)

type Platform struct {
	DB    *pgxpool.Pool
	Redis *redis.Client
}

func Open(ctx context.Context, cfg config.Config) (*Platform, error) {
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	options, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		db.Close()
		return nil, err
	}
	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		db.Close()
		_ = client.Close()
		return nil, err
	}
	return &Platform{DB: db, Redis: client}, nil
}

func (p *Platform) Close() {
	p.DB.Close()
	_ = p.Redis.Close()
}
