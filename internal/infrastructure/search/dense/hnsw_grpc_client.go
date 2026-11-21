package dense

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/ranking"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/grpcutil"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/downstreamv1"
)

const (
	// DefaultDenseTimeout is the hard deadline for dense HNSW fan-out (20ms).
	DefaultDenseTimeout = 20 * time.Millisecond
)

var (
	// ErrEmptyTenantID is returned when tenantID is empty.
	ErrEmptyTenantID = errors.New("dense: tenantID must not be empty")
	// ErrEmptyVector is returned when the query embedding vector is empty.
	ErrEmptyVector = errors.New("dense: query vector must not be empty")
)

// HNSWClient queries dense HNSW read replicas over a pooled unary gRPC channel.
type HNSWClient struct {
	pool    *grpcutil.ConnPool
	timeout time.Duration
	logger  *slog.Logger
}

// NewHNSWClient constructs an HNSWClient with a 20ms default deadline.
func NewHNSWClient(pool *grpcutil.ConnPool, timeout time.Duration, logger *slog.Logger) *HNSWClient {
	if timeout <= 0 {
		timeout = DefaultDenseTimeout
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &HNSWClient{
		pool:    pool,
		timeout: timeout,
		logger:  logger,
	}
}

// SearchDense retrieves up to Top-40 dense vector candidates for the given tenant and embedding.
func (c *HNSWClient) SearchDense(
	ctx context.Context,
	tenantID string,
	vector []float32,
	topK int,
) ([]entity.SearchCandidate, error) {
	if tenantID == "" {
		return nil, ErrEmptyTenantID
	}
	if len(vector) == 0 {
		return nil, ErrEmptyVector
	}
	if topK <= 0 || topK > ranking.MaxDenseCandidates {
		topK = ranking.MaxDenseCandidates
	}

	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	conn, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	client := downstreamv1.NewDenseSearchServiceClient(conn)
	resp, err := client.Search(callCtx, &downstreamv1.DenseSearchServiceSearchRequest{
		TenantId: tenantID,
		Vector:   vector,
		TopK:     clampTopK(topK),
	})
	if err != nil {
		c.logger.WarnContext(ctx, "hnsw dense search rpc failed",
			slog.String("tenant_id", tenantID),
			slog.String("stage", "hnsw_dense"),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	raw := resp.GetCandidates()
	limit := len(raw)
	if limit > topK {
		limit = topK
	}

	out := make([]entity.SearchCandidate, limit)
	for i := 0; i < limit; i++ {
		cand := raw[i]
		out[i] = entity.SearchCandidate{
			DocID:     cand.GetDocId(),
			ChunkID:   cand.GetChunkId(),
			ParentKey: cand.GetParentKey(),
			Score:     cand.GetScore(),
			Rank:      i + 1,
			Modality:  entity.ModalityDense,
			Metadata:  cand.GetMetadata(),
		}
	}
	return out, nil
}

func clampTopK(v int) int32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v)
}
