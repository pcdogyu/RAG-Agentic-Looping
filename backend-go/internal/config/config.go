package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Address              string
	DatabaseURL          string
	RedisURL             string
	LegacyAPIURL         string
	AllowLegacyProxy     bool
	Environment          string
	WorkerID             string
	WorkerLane           string
	WorkerCompletedLanes []string
	LeaseDuration        time.Duration
	PollInterval         time.Duration
	MarketAdapterURL     string
	MLAdapterURL         string
	AdminAPIToken        string
	MCPSecretKey         string
	WeknoraURL           string
	ScanInterval         time.Duration
	ResearchCooldown     time.Duration
	ExtractModel         string
	ExtractURLs          []string
	AssistURLs           []string
	ResearchURLs         []string
	OllamaTimeout        time.Duration
	OllamaContextLength  int
	OllamaMaxOutput      int
	OllamaExtractThreads int
	OllamaAssistThreads  int
	MappingContextLength int
	MappingMaxOutput     int
	OllamaKeepAlive      string
	EventClusterWindow   time.Duration
	AutoResearch         bool
	AssistModel          string
	ResearchModel        string
	CodeModel            string
	EvolutionEnabled     bool
	EvolutionAutoMerge   bool
	InitialCash          float64
	MaxEquityWeight      float64
	MaxCryptoWeight      float64
	MaxTotalCryptoWeight float64
	MinimumCashWeight    float64
	EquityCostBPS        int
	CryptoCostBPS        int
	FMPBaseURL           string
	FMPAccessToken       string
	FMPRateLimit         int
	FMPNewsLookback      int
	SECIdentity          string
	AkshareEnabled       bool
	AkshareIPv4Only      bool
	RSSFeeds             []string
	OfficialRSSFeeds     []string
	CoinGeckoURL         string
	DefiLlamaURL         string
	WebSearchTimeout     time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		Address:              env("GO_API_ADDRESS", ":8081"),
		DatabaseURL:          normalizeDatabaseURL(env("DATABASE_URL", "postgresql://agent:agent@postgres:5432/agent")),
		RedisURL:             env("REDIS_URL", "redis://redis:6379/0"),
		LegacyAPIURL:         strings.TrimRight(env("LEGACY_API_URL", "http://api:8000"), "/"),
		AllowLegacyProxy:     envBool("GO_ALLOW_LEGACY_PROXY", false),
		Environment:          env("APP_ENV", "development"),
		WorkerID:             env("GO_WORKER_ID", hostname()),
		WorkerLane:           env("GO_WORKER_LANE", ""),
		WorkerCompletedLanes: split(env("GO_WORKER_COMPLETED_LANES", "")),
		LeaseDuration:        envDuration("GO_JOB_LEASE", 3*time.Minute),
		PollInterval:         envDuration("GO_JOB_POLL_INTERVAL", time.Second),
		MarketAdapterURL:     strings.TrimRight(env("MARKET_ADAPTER_URL", "http://market-adapter:8091"), "/"),
		MLAdapterURL:         strings.TrimRight(env("ML_ADAPTER_URL", "http://ml-adapter:8092"), "/"),
		AdminAPIToken:        env("ADMIN_API_TOKEN", ""),
		MCPSecretKey:         env("MCP_SECRET_KEY", ""),
		WeknoraURL:           env("WEKNORA_DEFAULT_URL", "http://10.15.0.28/"),
		ScanInterval:         time.Duration(envInt("SCAN_INTERVAL_MINUTES", 10)) * time.Minute,
		ResearchCooldown:     time.Duration(envInt("RESEARCH_ASSET_COOLDOWN_HOURS", 24)) * time.Hour,
		ExtractModel:         env("OLLAMA_EXTRACT_MODEL", "qwen2.5:3b"),
		ExtractURLs:          modelURLs("EXTRACT", "http://host.docker.internal:11434"),
		AssistURLs:           modelURLs("ASSIST", "http://host.docker.internal:11437"),
		ResearchURLs:         modelURLs("RESEARCH", "http://host.docker.internal:11435"),
		OllamaTimeout:        time.Duration(envInt("OLLAMA_TIMEOUT_SECONDS", 300)) * time.Second,
		OllamaContextLength:  envInt("OLLAMA_CONTEXT_LENGTH", 8192),
		OllamaMaxOutput:      envInt("OLLAMA_MAX_OUTPUT_TOKENS", 4096),
		OllamaExtractThreads: envInt("OLLAMA_EXTRACT_NUM_THREADS", 4),
		OllamaAssistThreads:  envInt("OLLAMA_ASSIST_NUM_THREADS", 8),
		MappingContextLength: envInt("OLLAMA_ASSET_MAPPING_CONTEXT_LENGTH", 8192),
		MappingMaxOutput:     envInt("OLLAMA_ASSET_MAPPING_MAX_OUTPUT_TOKENS", 1024),
		OllamaKeepAlive:      env("OLLAMA_KEEP_ALIVE", "0"),
		EventClusterWindow:   time.Duration(envInt("EVENT_CLUSTER_WINDOW_HOURS", 72)) * time.Hour,
		AutoResearch:         envBool("AUTO_RESEARCH", true),
		AssistModel:          env("OLLAMA_ASSIST_MODEL", "qwen2.5:7b"),
		ResearchModel:        env("OLLAMA_RESEARCH_MODEL", "qwen2.5:7b"),
		CodeModel:            env("OLLAMA_CODE_MODEL", "qwen2.5-coder:7b"),
		EvolutionEnabled:     envBool("EVOLUTION_ENABLED", false),
		EvolutionAutoMerge:   envBool("EVOLUTION_AUTO_MERGE", false),
		InitialCash:          envFloat("INITIAL_CASH", 100000),
		MaxEquityWeight:      envFloat("MAX_EQUITY_WEIGHT", 0.08),
		MaxCryptoWeight:      envFloat("MAX_CRYPTO_WEIGHT", 0.05),
		MaxTotalCryptoWeight: envFloat("MAX_TOTAL_CRYPTO_WEIGHT", 0.15),
		MinimumCashWeight:    envFloat("MINIMUM_CASH_WEIGHT", 0.2),
		EquityCostBPS:        envInt("EQUITY_COST_BPS", 15),
		CryptoCostBPS:        envInt("CRYPTO_COST_BPS", 25),
		FMPBaseURL:           strings.TrimRight(env("FMP_BASE_URL", "https://financialmodelingprep.com/stable"), "/"),
		FMPAccessToken:       env("FMP_ACCESS_TOKEN", ""),
		FMPRateLimit:         envInt("FMP_RATE_LIMIT_PER_MINUTE", 55),
		FMPNewsLookback:      envInt("FMP_NEWS_LOOKBACK_HOURS", 12),
		SECIdentity:          env("SEC_IDENTITY", ""),
		AkshareEnabled:       envBool("AKSHARE_ASSET_MASTER_ENABLED", true),
		AkshareIPv4Only:      envBool("AKSHARE_IPV4_ONLY", true),
		RSSFeeds:             split(env("RSS_FEED_URLS", "")),
		OfficialRSSFeeds:     split(env("OFFICIAL_RSS_FEED_URLS", "")),
		CoinGeckoURL:         strings.TrimRight(env("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"), "/"),
		DefiLlamaURL:         strings.TrimRight(env("DEFILLAMA_BASE_URL", "https://api.llama.fi"), "/"),
		WebSearchTimeout:     time.Duration(envInt("WEB_SEARCH_TIMEOUT_SECONDS", 20)) * time.Second,
	}
	if cfg.Environment == "production" && cfg.AllowLegacyProxy {
		return Config{}, fmt.Errorf("legacy API proxy is forbidden in production")
	}
	return cfg, nil
}

func modelURLs(lane, fallback string) []string {
	if values := split(env("OLLAMA_"+lane+"_BASE_URLS", "")); len(values) > 0 {
		return values
	}
	return []string{strings.TrimRight(env("OLLAMA_"+lane+"_BASE_URL", fallback), "/")}
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

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envFloat(name string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
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
