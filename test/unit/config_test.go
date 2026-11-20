package unit

import (
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
	"time"

	"github.com/rag-platform/rag-search-orchestrator/internal/config"
)

func TestLoadDotEnvAndConfigOverrides(t *testing.T) {
	keys := []string{
		"HTTP_ADDR",
		"GRPC_ADDR",
		"REDIS_ADDRS",
		"REDIS_PASSWORD",
		"HNSW_TARGETS",
		"BM25_TARGETS",
		"TRITON_EMBED_TARGETS",
		"TRITON_RERANK_TARGETS",
		"TRITON_EMBED_MODEL",
		"TRITON_RERANK_MODEL",
		"TRITON_RERANK_VERSION",
		"RERANK_TAU",
		"CACHE_TTL",
		"RING_BUFFER_CAPACITY",
		"SHUTDOWN_DRAIN_DELAY",
		"GRPC_POOL_SIZE",
		"LOG_LEVEL",
	}
	clearEnvForTest(t, keys)

	envPath := filepath.Join(t.TempDir(), ".env")
	content := `
# Service endpoints
HTTP_ADDR=:9090
export GRPC_ADDR=":50099"

# Redis
REDIS_ADDRS=redis-1:6379, redis-2:6379
REDIS_PASSWORD='s3cr3t=val=='
CACHE_TTL=12m

# Search & Triton
HNSW_TARGETS=hnsw-a:50052,hnsw-b:50052
BM25_TARGETS=bm25-0:50053
TRITON_EMBED_TARGETS=triton-embed:8001
TRITON_RERANK_TARGETS=triton-rerank:8001
TRITON_EMBED_MODEL=custom-embed-fp16
TRITON_RERANK_MODEL=custom-rerank
TRITON_RERANK_VERSION=2
RERANK_TAU=0.45 # threshold

# Telemetry & Lifecycle
RING_BUFFER_CAPACITY=16384
SHUTDOWN_DRAIN_DELAY=2s
GRPC_POOL_SIZE=8
LOG_LEVEL=debug
`
	if err := os.WriteFile(envPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp .env: %v", err)
	}

	if err := config.LoadDotEnv(envPath); err != nil {
		t.Fatalf("LoadDotEnv failed: %v", err)
	}

	cfg := config.Load()
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("HTTPAddr = %q, want :9090", cfg.HTTPAddr)
	}
	if cfg.GRPCAddr != ":50099" {
		t.Errorf("GRPCAddr = %q, want :50099", cfg.GRPCAddr)
	}
	if len(cfg.RedisAddrs) != 2 || cfg.RedisAddrs[0] != "redis-1:6379" || cfg.RedisAddrs[1] != "redis-2:6379" {
		t.Errorf("RedisAddrs = %v, want [redis-1:6379 redis-2:6379]", cfg.RedisAddrs)
	}
	if cfg.RedisPassword != "s3cr3t=val==" {
		t.Errorf("RedisPassword mismatch")
	}
	if cfg.CacheTTL != 12*time.Minute {
		t.Errorf("CacheTTL = %v, want 12m", cfg.CacheTTL)
	}
	if len(cfg.HNSWTargets) != 2 {
		t.Errorf("HNSWTargets = %v, want 2 targets", cfg.HNSWTargets)
	}
	if cfg.TritonEmbedModel != "custom-embed-fp16" || cfg.TritonRerankModel != "custom-rerank" || cfg.TritonRerankVer != "2" {
		t.Errorf("unexpected triton model config: %+v", cfg)
	}
	if cfg.RerankTau != 0.45 {
		t.Errorf("RerankTau = %v, want 0.45", cfg.RerankTau)
	}
	if cfg.RingBufferCapacity != 16384 {
		t.Errorf("RingBufferCapacity = %d, want 16384", cfg.RingBufferCapacity)
	}
	if cfg.ShutdownDrainDelay != 2*time.Second {
		t.Errorf("ShutdownDrainDelay = %v, want 2s", cfg.ShutdownDrainDelay)
	}
	if cfg.GRPCPoolSize != 8 {
		t.Errorf("GRPCPoolSize = %d, want 8", cfg.GRPCPoolSize)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v, want debug", cfg.LogLevel)
	}
}

func TestLoadDotEnvProcessEnvPrecedence(t *testing.T) {
	clearEnvForTest(t, []string{"HTTP_ADDR", "LOG_LEVEL"})
	t.Setenv("HTTP_ADDR", ":7777")

	envPath := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(envPath, []byte("HTTP_ADDR=:9999\nLOG_LEVEL=warn\n"), 0o600); err != nil {
		t.Fatalf("write temp .env: %v", err)
	}

	if err := config.LoadDotEnv(envPath); err != nil {
		t.Fatalf("LoadDotEnv failed: %v", err)
	}

	cfg := config.Load()
	if cfg.HTTPAddr != ":7777" {
		t.Errorf("HTTPAddr = %q, want process env :7777 to take precedence", cfg.HTTPAddr)
	}
	if cfg.LogLevel != slog.LevelWarn {
		t.Errorf("LogLevel = %v, want warn from .env", cfg.LogLevel)
	}
}

func TestLoadDotEnvMissingAndMalformed(t *testing.T) {
	if err := config.LoadDotEnv(filepath.Join(t.TempDir(), "does-not-exist.env")); err != nil {
		t.Fatalf("expected nil error for missing .env, got %v", err)
	}

	badPath := filepath.Join(t.TempDir(), "bad.env")
	if err := os.WriteFile(badPath, []byte("INVALID_LINE_WITHOUT_EQUALS\n"), 0o600); err != nil {
		t.Fatalf("write bad .env: %v", err)
	}
	if err := config.LoadDotEnv(badPath); err == nil {
		t.Fatal("expected error for malformed .env line, got nil")
	}
}

func TestApplyRuntimeEnv(t *testing.T) {
	prevGC := debug.SetGCPercent(100)
	prevMem := debug.SetMemoryLimit(-1)
	t.Cleanup(func() {
		debug.SetGCPercent(prevGC)
		debug.SetMemoryLimit(prevMem)
	})

	t.Setenv("GOGC", "150")
	t.Setenv("GOMEMLIMIT", "1024MiB")
	config.ApplyRuntimeEnv()

	if got := debug.SetGCPercent(100); got != 150 {
		t.Errorf("GOGC applied = %d, want 150", got)
	}
	if got := debug.SetMemoryLimit(-1); got != 1024<<20 {
		t.Errorf("GOMEMLIMIT applied = %d, want %d", got, int64(1024<<20))
	}
}

func TestDotEnvExampleIsValidAndComplete(t *testing.T) {
	expectedKeys := []string{
		"GOMEMLIMIT",
		"GOGC",
		"HTTP_ADDR",
		"GRPC_ADDR",
		"SHUTDOWN_DRAIN_DELAY",
		"LOG_LEVEL",
		"REDIS_ADDRS",
		"REDIS_PASSWORD",
		"CACHE_TTL",
		"HNSW_TARGETS",
		"BM25_TARGETS",
		"GRPC_POOL_SIZE",
		"TRITON_EMBED_TARGETS",
		"TRITON_RERANK_TARGETS",
		"TRITON_EMBED_MODEL",
		"TRITON_RERANK_MODEL",
		"TRITON_RERANK_VERSION",
		"RERANK_TAU",
		"RING_BUFFER_CAPACITY",
	}
	clearEnvForTest(t, expectedKeys)

	examplePath := filepath.Join("..", "..", ".env.example")
	if err := config.LoadDotEnv(examplePath); err != nil {
		t.Fatalf("LoadDotEnv(.env.example) failed: %v", err)
	}

	for _, key := range expectedKeys {
		if _, ok := os.LookupEnv(key); !ok {
			t.Errorf(".env.example is missing expected key %q", key)
		}
	}
}

func clearEnvForTest(t *testing.T, keys []string) {
	t.Helper()
	for _, k := range keys {
		prev, had := os.LookupEnv(k)
		_ = os.Unsetenv(k)
		kCopy := k
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(kCopy, prev)
			} else {
				_ = os.Unsetenv(kCopy)
			}
		})
	}
}
