package repository

import (
	"context"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

// DenseSearchPort defines the domain boundary for querying dense HNSW vector read replicas.
type DenseSearchPort interface {
	// SearchDense retrieves up to topK dense vector candidates for the given tenant and query embedding.
	SearchDense(ctx context.Context, tenantID string, vector []float32, topK int) ([]entity.SearchCandidate, error)
}
