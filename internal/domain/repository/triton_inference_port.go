package repository

import (
	"context"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

// TritonEmbeddingPort defines the domain boundary for FP16 dense query embedding inference.
type TritonEmbeddingPort interface {
	// EmbedQuery generates a dense float32 query embedding using the specified model version.
	EmbedQuery(ctx context.Context, queryText string, modelVersion string) ([]float32, error)
}

// TritonRerankerPort defines the domain boundary for bounded Cross-Encoder passage reranking.
type TritonRerankerPort interface {
	// Rerank scores up to Top-10 ACL-permitted candidates against the query and populates RerankLogit.
	Rerank(ctx context.Context, queryText string, candidates []entity.FusedCandidate) ([]entity.FusedCandidate, error)
}
