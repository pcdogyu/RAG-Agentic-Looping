package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Address               string
	DatabaseURL           string
	RedisURL              string
	Environment           string
	WorkerID              string
	WorkerLane            string
	WorkerConcurrency     int
	LeaseDuration         time.Duration
	PollInterval          time.Duration
	MarketAdapterURL      string
	AdminAPIToken         string
	MCPSecretKey          string
	WeknoraURL            string
	ScanInterval          time.Duration
	ScanBatchSize         int
	NewsDiscoveryLookback time.Duration
	NewsWatermarkOverlap  time.Duration
	ResearchCooldown      time.Duration
	ResearchCoalesce      time.Duration
	ResearchLease         time.Duration
	ModelAuditRetention   time.Duration
	ExtractModel          string
	ExtractURLs           []string
	AssistURLs            []string
	ResearchURLs          []string
	CodeURLs              []string
	OllamaTimeout         time.Duration
	ResearchTimeout       time.Duration
	ResearchSoftLimit     time.Duration
	ResearchHardLimit     time.Duration
	OllamaContextLength   int
	OllamaMaxOutput       int
	OllamaExtractThreads  int
	OllamaAssistThreads   int
	OllamaResearchThreads int
	OllamaCodeThreads     int
	MappingContextLength  int
	MappingMaxOutput      int
	ResearchContextLength int
	ResearchMaxOutput     int
	ResearchThink         bool
	CodeContextLength     int
	CodeMaxOutput         int
	OllamaKeepAlive       string
	EventClusterWindow    time.Duration
	AutoResearch          bool
	AssistModel           string
	ResearchModel         string
	CodeModel             string
	EvolutionEnabled      bool
	EvolutionAutoMerge    bool
	EvolutionRoot         string
	EvolutionBaseBranch   string
	TelegramBotToken      string
	TelegramChatID        string
	InitialCash           float64
	MaxEquityWeight       float64
	MaxCryptoWeight       float64
	MaxTotalCryptoWeight  float64
	MinimumCashWeight     float64
	EquityCostBPS         int
	CryptoCostBPS         int
	FMPBaseURL            string
	FMPAccessToken        string
	FMPRateLimit          int
	FMPNewsLookback       int
	SECIdentity           string
	AkshareEnabled        bool
	AkshareIPv4Only       bool
	RSSFeeds              []string
	OfficialRSSFeeds      []string
	CoinGeckoURL          string
	DefiLlamaURL          string
	WebSearchTimeout      time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		Address:               env("GO_API_ADDRESS", ":8081"),
		DatabaseURL:           normalizeDatabaseURL(env("DATABASE_URL", "postgresql://agent:agent@postgres:5432/agent")),
		RedisURL:              env("REDIS_URL", "redis://redis:6379/0"),
		Environment:           env("APP_ENV", "development"),
		WorkerID:              env("GO_WORKER_ID", hostname()),
		WorkerLane:            env("GO_WORKER_LANE", ""),
		WorkerConcurrency:     envInt("GO_WORKER_CONCURRENCY", 1),
		LeaseDuration:         envDuration("GO_JOB_LEASE", 3*time.Minute),
		PollInterval:          envDuration("GO_JOB_POLL_INTERVAL", time.Second),
		MarketAdapterURL:      strings.TrimRight(env("MARKET_ADAPTER_URL", "http://market-adapter:8091"), "/"),
		AdminAPIToken:         env("ADMIN_API_TOKEN", ""),
		MCPSecretKey:          env("MCP_SECRET_KEY", ""),
		WeknoraURL:            env("WEKNORA_DEFAULT_URL", "http://10.15.0.28/"),
		ScanInterval:          time.Duration(envInt("SCAN_INTERVAL_MINUTES", 10)) * time.Minute,
		ScanBatchSize:         envInt("SCAN_BATCH_SIZE", 40),
		NewsDiscoveryLookback: time.Duration(envInt("NEWS_DISCOVERY_LOOKBACK_HOURS", 24)) * time.Hour,
		NewsWatermarkOverlap:  time.Duration(envInt("NEWS_WATERMARK_OVERLAP_MINUTES", 10)) * time.Minute,
		ResearchCooldown:      time.Duration(envInt("RESEARCH_ASSET_COOLDOWN_HOURS", 24)) * time.Hour,
		ResearchCoalesce:      time.Duration(envInt("RESEARCH_COALESCE_WINDOW_HOURS", 24)) * time.Hour,
		ResearchLease:         time.Duration(envInt("RESEARCH_LEASE_SECONDS", 120)) * time.Second,
		ModelAuditRetention:   time.Duration(envInt("MODEL_AUDIT_RETENTION_DAYS", 90)) * 24 * time.Hour,
		ExtractModel:          env("OLLAMA_EXTRACT_MODEL", "qwen2.5:3b"),
		ExtractURLs:           modelURLs("EXTRACT", "http://host.docker.internal:11434"),
		AssistURLs:            modelURLs("ASSIST", "http://host.docker.internal:11437"),
		ResearchURLs:          modelURLs("RESEARCH", "http://host.docker.internal:11435"),
		CodeURLs:              modelURLs("CODE", "http://host.docker.internal:11438"),
		OllamaTimeout:         time.Duration(envInt("OLLAMA_TIMEOUT_SECONDS", 300)) * time.Second,
		ResearchTimeout:       time.Duration(envInt("OLLAMA_RESEARCH_TIMEOUT_SECONDS", 1800)) * time.Second,
		ResearchSoftLimit:     time.Duration(envInt("RESEARCH_ASSET_SOFT_TIME_LIMIT_SECONDS", 2040)) * time.Second,
		ResearchHardLimit:     time.Duration(envInt("RESEARCH_ASSET_HARD_TIME_LIMIT_SECONDS", 2100)) * time.Second,
		OllamaContextLength:   envInt("OLLAMA_CONTEXT_LENGTH", 8192),
		OllamaMaxOutput:       envInt("OLLAMA_MAX_OUTPUT_TOKENS", 4096),
		OllamaExtractThreads:  envInt("OLLAMA_EXTRACT_NUM_THREADS", 4),
		OllamaAssistThreads:   envInt("OLLAMA_ASSIST_NUM_THREADS", 8),
		OllamaResearchThreads: envInt("OLLAMA_RESEARCH_NUM_THREADS", 8),
		OllamaCodeThreads:     envInt("OLLAMA_CODE_NUM_THREADS", 4),
		MappingContextLength:  envInt("OLLAMA_ASSET_MAPPING_CONTEXT_LENGTH", 8192),
		MappingMaxOutput:      envInt("OLLAMA_ASSET_MAPPING_MAX_OUTPUT_TOKENS", 1024),
		ResearchContextLength: envInt("OLLAMA_RESEARCH_CONTEXT_LENGTH", 16384),
		ResearchMaxOutput:     envInt("OLLAMA_RESEARCH_MAX_OUTPUT_TOKENS", 4096),
		ResearchThink:         envBool("OLLAMA_RESEARCH_THINK", false),
		CodeContextLength:     envInt("OLLAMA_CODE_CONTEXT_LENGTH", 16384),
		CodeMaxOutput:         envInt("OLLAMA_CODE_MAX_OUTPUT_TOKENS", 8192),
		OllamaKeepAlive:       env("OLLAMA_KEEP_ALIVE", "0"),
		EventClusterWindow:    time.Duration(envInt("EVENT_CLUSTER_WINDOW_HOURS", 72)) * time.Hour,
		AutoResearch:          envBool("AUTO_RESEARCH", true),
		AssistModel:           env("OLLAMA_ASSIST_MODEL", "qwen2.5:7b"),
		ResearchModel:         env("OLLAMA_RESEARCH_MODEL", "qwen3:4b-thinking"),
		CodeModel:             env("OLLAMA_CODE_MODEL", "qwen2.5-coder:7b"),
		EvolutionEnabled:      envBool("EVOLUTION_ENABLED", false),
		EvolutionAutoMerge:    envBool("EVOLUTION_AUTO_MERGE", false),
		EvolutionRoot:         env("EVOLUTION_ROOT", "/app"),
		EvolutionBaseBranch:   env("EVOLUTION_BASE_BRANCH", "golang"),
		TelegramBotToken:      env("TELEGRAM_BOT_TOKEN", ""),
		TelegramChatID:        env("TELEGRAM_CHAT_ID", ""),
		InitialCash:           envFloat("INITIAL_CASH", 100000),
		MaxEquityWeight:       envFloat("MAX_EQUITY_WEIGHT", 0.08),
		MaxCryptoWeight:       envFloat("MAX_CRYPTO_WEIGHT", 0.05),
		MaxTotalCryptoWeight:  envFloat("MAX_TOTAL_CRYPTO_WEIGHT", 0.15),
		MinimumCashWeight:     envFloat("MINIMUM_CASH_WEIGHT", 0.2),
		EquityCostBPS:         envInt("EQUITY_COST_BPS", 15),
		CryptoCostBPS:         envInt("CRYPTO_COST_BPS", 25),
		FMPBaseURL:            strings.TrimRight(env("FMP_BASE_URL", "https://financialmodelingprep.com/stable"), "/"),
		FMPAccessToken:        env("FMP_ACCESS_TOKEN", ""),
		FMPRateLimit:          envInt("FMP_RATE_LIMIT_PER_MINUTE", 55),
		FMPNewsLookback:       envInt("FMP_NEWS_LOOKBACK_HOURS", 12),
		SECIdentity:           env("SEC_IDENTITY", ""),
		AkshareEnabled:        envBool("AKSHARE_ASSET_MASTER_ENABLED", true),
		AkshareIPv4Only:       envBool("AKSHARE_IPV4_ONLY", true),
		RSSFeeds:              split(env("RSS_FEED_URLS", "")),
		OfficialRSSFeeds:      split(env("OFFICIAL_RSS_FEED_URLS", "")),
		CoinGeckoURL:          strings.TrimRight(env("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"), "/"),
		DefiLlamaURL:          strings.TrimRight(env("DEFILLAMA_BASE_URL", "https://api.llama.fi"), "/"),
		WebSearchTimeout:      time.Duration(envInt("WEB_SEARCH_TIMEOUT_SECONDS", 20)) * time.Second,
	}
	if cfg.WorkerConcurrency < 1 {
		return Config{}, fmt.Errorf("GO_WORKER_CONCURRENCY must be at least 1")
	}
	if cfg.ScanBatchSize < 1 || cfg.ScanBatchSize > 200 {
		return Config{}, fmt.Errorf("SCAN_BATCH_SIZE must be between 1 and 200")
	}
	if cfg.WorkerLane == "research" && cfg.WorkerConcurrency > len(cfg.ResearchURLs) {
		return Config{}, fmt.Errorf("research worker concurrency %d exceeds configured model capacity %d", cfg.WorkerConcurrency, len(cfg.ResearchURLs))
	}
	if cfg.ResearchSoftLimit <= 0 || cfg.ResearchHardLimit <= cfg.ResearchSoftLimit {
		return Config{}, fmt.Errorf("research asset limits must satisfy 0 < soft < hard")
	}
	if cfg.ResearchLease < 30*time.Second || cfg.ResearchLease > 10*time.Minute {
		return Config{}, fmt.Errorf("RESEARCH_LEASE_SECONDS must be between 30 and 600")
	}
	if cfg.ModelAuditRetention < 24*time.Hour {
		return Config{}, fmt.Errorf("MODEL_AUDIT_RETENTION_DAYS must be at least 1")
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
	// Accept historical driver-qualified DSNs while installations move to the
	// standard PostgreSQL URL used by pgx.
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
