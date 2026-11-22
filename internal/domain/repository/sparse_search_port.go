package repository

import (
	"context"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

// SparseSearchPort defines the domain boundary for querying distributed BM25 sparse text shards.
type SparseSearchPort interface {
	// SearchSparse retrieves up to topK BM25 text candidates for the given tenant and normalized query.
	SearchSparse(ctx context.Context, tenantID string, queryText string, topK int) ([]entity.SearchCandidate, error)
}
