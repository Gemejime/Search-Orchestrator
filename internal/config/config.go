package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	rediscache "github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/cache/redis"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/grpcutil"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/triton"
)

const (
	// DefaultHTTPAddr is the default HTTP/REST listen address.
	DefaultHTTPAddr = ":8080"
	// DefaultGRPCAddr is the default gRPC listen address.
	DefaultGRPCAddr = ":50051"
	// DefaultShutdownDrainDelay is the default pre-stop readiness drain delay.
	DefaultShutdownDrainDelay = 5 * time.Second
)

// Config holds all 12-factor service configuration loaded from environment variables.
type Config struct {
	HTTPAddr            string
	GRPCAddr            string
	RedisAddrs          []string
	RedisPassword       string // #nosec G117 -- read from environment, never logged
	HNSWTargets         []string
	BM25Targets         []string
	TritonEmbedTargets  []string
	TritonRerankTargets []string
	TritonEmbedModel    string
	TritonRerankModel   string
	TritonRerankVer     string
	RerankTau           float64
	CacheTTL            time.Duration
	RingBufferCapacity  int
	ShutdownDrainDelay  time.Duration
	GRPCPoolSize        int
	LogLevel            slog.Level
}

// Load reads service configuration from process environment variables, falling
// back to calibrated production defaults when a variable is unset or invalid.
func Load() Config {
	return Config{
		HTTPAddr:            envOrDefault("HTTP_ADDR", DefaultHTTPAddr),
		GRPCAddr:            envOrDefault("GRPC_ADDR", DefaultGRPCAddr),
		RedisAddrs:          splitCSV(envOrDefault("REDIS_ADDRS", "localhost:6379")),
		RedisPassword:       os.Getenv("REDIS_PASSWORD"),
		HNSWTargets:         splitCSV(envOrDefault("HNSW_TARGETS", "localhost:50052")),
		BM25Targets:         splitCSV(envOrDefault("BM25_TARGETS", "localhost:50053")),
		TritonEmbedTargets:  splitCSV(envOrDefault("TRITON_EMBED_TARGETS", "localhost:8001")),
		TritonRerankTargets: splitCSV(envOrDefault("TRITON_RERANK_TARGETS", "localhost:8001")),
		TritonEmbedModel:    envOrDefault("TRITON_EMBED_MODEL", triton.DefaultEmbedModelName),
		TritonRerankModel:   envOrDefault("TRITON_RERANK_MODEL", triton.DefaultRerankModelName),
		TritonRerankVer:     os.Getenv("TRITON_RERANK_VERSION"),
		RerankTau:           envFloat("RERANK_TAU", 0.0),
		CacheTTL:            envDuration("CACHE_TTL", rediscache.DefaultQueryCacheTTL),
		RingBufferCapacity:  envInt("RING_BUFFER_CAPACITY", telemetry.DefaultRingBufferCapacity),
		ShutdownDrainDelay:  envDuration("SHUTDOWN_DRAIN_DELAY", DefaultShutdownDrainDelay),
		GRPCPoolSize:        envInt("GRPC_POOL_SIZE", grpcutil.DefaultPoolSize),
		LogLevel:            parseLogLevel(envOrDefault("LOG_LEVEL", "info")),
	}
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func envInt(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func parseLogLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
