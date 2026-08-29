package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Address          string
	DatabaseURL      string
	RedisURL         string
	LegacyAPIURL     string
	AllowLegacyProxy bool
	Environment      string
	WorkerID         string
	WorkerQueues     []string
	LeaseDuration    time.Duration
	PollInterval     time.Duration
	MarketAdapterURL string
	MLAdapterURL     string
}

func Load() (Config, error) {
	cfg := Config{
		Address:          env("GO_API_ADDRESS", ":8081"),
		DatabaseURL:      normalizeDatabaseURL(env("DATABASE_URL", "postgresql://agent:agent@postgres:5432/agent")),
		RedisURL:         env("REDIS_URL", "redis://redis:6379/0"),
		LegacyAPIURL:     strings.TrimRight(env("LEGACY_API_URL", "http://api:8000"), "/"),
		AllowLegacyProxy: envBool("GO_ALLOW_LEGACY_PROXY", false),
		Environment:      env("APP_ENV", "development"),
		WorkerID:         env("GO_WORKER_ID", hostname()),
		WorkerQueues:     split(env("GO_WORKER_QUEUES", "io,extract,assist,research,code")),
		LeaseDuration:    envDuration("GO_JOB_LEASE", 3*time.Minute),
		PollInterval:     envDuration("GO_JOB_POLL_INTERVAL", time.Second),
		MarketAdapterURL: strings.TrimRight(env("MARKET_ADAPTER_URL", "http://market-adapter:8091"), "/"),
		MLAdapterURL:     strings.TrimRight(env("ML_ADAPTER_URL", "http://ml-adapter:8092"), "/"),
	}
	if cfg.Environment == "production" && cfg.AllowLegacyProxy {
		return Config{}, fmt.Errorf("legacy API proxy is forbidden in production")
	}
	return cfg, nil
}

func normalizeDatabaseURL(value string) string {
	// SQLAlchemy uses a driver-qualified URL; pgx consumes the same DSN without
	// the Python driver suffix. Keeping one environment variable prevents a
	// credential fork during the shadow migration.
	return strings.Replace(value, "postgresql+psycopg://", "postgresql://", 1)
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func split(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func hostname() string {
	value, err := os.Hostname()
	if err != nil || value == "" {
		return "go-worker"
	}
	return value
}
