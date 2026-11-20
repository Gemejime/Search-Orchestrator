package repository

import (
	"context"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

// ParentChunkPort defines the domain boundary for hydrating Top-5 parent container chunks.
type ParentChunkPort interface {
	// HydrateParents fetches pre-compiled parent containers (800-1,500 tokens) for surviving candidates.
	HydrateParents(ctx context.Context, tenantID string, candidates []entity.FusedCandidate) ([]entity.EnrichedChunk, error)
}

// CachedCandidate represents a caller-independent candidate stored in the Tier 1 query cache.
type CachedCandidate struct {
	DocID     uint32
	ChunkID   string
	ParentKey string
	Text      string
	Score     float64
}

// Tier1CacheEntry holds the cached candidates and reranker fallback state for a normalized query key.
type Tier1CacheEntry struct {
	RerankerFallback bool
	Candidates       []CachedCandidate
}

// QueryCachePort defines the optional Tier 1 normalized query cache boundary (implemented in Phase 4).
type QueryCachePort interface {
	// Get retrieves cached candidates for a canonical Tier 1 cache key. Returns (entry, true, nil) on hit.
	Get(ctx context.Context, cacheKey string) (Tier1CacheEntry, bool, error)
	// Set stores caller-independent pre-ACL candidates for a canonical Tier 1 cache key.
	Set(ctx context.Context, cacheKey string, entry Tier1CacheEntry) error
}

// SearchAuditSpan captures zero-PII provenance metadata emitted asynchronously after retrieval.
type SearchAuditSpan struct {
	TraceID          string
	RequestID        string
	TenantID         string
	QueryHash        string
	CacheHit         bool
	PermittedDocIDs  [20]uint32
	PermittedCount   uint8
	RerankerFallback bool
	TotalLatencyUs   int64
	TimestampUnixNs  int64
}

// TelemetryEmitterPort defines the optional asynchronous compliance audit boundary (implemented in Phase 4).
type TelemetryEmitterPort interface {
	// Emit sends a compliance audit span without blocking or allocating on the critical path.
	Emit(span SearchAuditSpan)
}
