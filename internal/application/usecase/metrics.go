package usecase

import "time"

// Stage component identifiers aligned with .ai/doc-api.md Prometheus metrics.
const (
	ComponentHNSWDense    = "hnsw_dense"
	ComponentBM25Sparse   = "bm25_sparse"
	ComponentTritonEmbed  = "triton_embed"
	ComponentRRFACL       = "rrf_acl"
	ComponentTritonRerank = "triton_rerank"
	ComponentRedisHydrate = "redis_hydrate"
)

// MetricsObserver records stage-level latencies, errors, ACL evaluation timing, and circuit trips
// without coupling the application layer to a specific Prometheus registry.
type MetricsObserver interface {
	ObserveStageLatency(component string, duration time.Duration)
	IncStageError(component string, errCode string)
	ObserveRoaringEval(duration time.Duration)
	IncCircuitBreakerTripped()
}

type noopMetricsObserver struct{}

// ObserveStageLatency is a no-op implementation of MetricsObserver.
func (noopMetricsObserver) ObserveStageLatency(string, time.Duration) {}

// IncStageError is a no-op implementation of MetricsObserver.
func (noopMetricsObserver) IncStageError(string, string) {}

// ObserveRoaringEval is a no-op implementation of MetricsObserver.
func (noopMetricsObserver) ObserveRoaringEval(time.Duration) {}

// IncCircuitBreakerTripped is a no-op implementation of MetricsObserver.
func (noopMetricsObserver) IncCircuitBreakerTripped() {}
