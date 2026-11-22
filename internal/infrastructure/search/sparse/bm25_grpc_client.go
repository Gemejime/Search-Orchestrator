package sparse

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/ranking"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/grpcutil"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/downstreamv1"
)

const (
	// DefaultSparseTimeout is the hard deadline for BM25 shard scatter-gather (20ms).
	DefaultSparseTimeout = 20 * time.Millisecond
)

var (
	// ErrEmptyTenantID is returned when tenantID is empty.
	ErrEmptyTenantID = errors.New("sparse: tenantID must not be empty")
	// ErrEmptyQueryText is returned when queryText is empty.
	ErrEmptyQueryText = errors.New("sparse: queryText must not be empty")
	// ErrNoShardsConfigured is returned when no BM25 shard pools are configured.
	ErrNoShardsConfigured = errors.New("sparse: at least one shard pool is required")
)

type shardKey struct {
	docID   uint32
	chunkID string
}

// BM25Client executes sharded unary gRPC scatter-gather across BM25 text shards.
// It tolerates partial shard failures and fails only when all shards fail.
type BM25Client struct {
	shards  []*grpcutil.ConnPool
	timeout time.Duration
	logger  *slog.Logger
}

// NewBM25Client constructs a BM25Client with a 20ms default deadline.
func NewBM25Client(shards []*grpcutil.ConnPool, timeout time.Duration, logger *slog.Logger) *BM25Client {
	if timeout <= 0 {
		timeout = DefaultSparseTimeout
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BM25Client{
		shards:  shards,
		timeout: timeout,
		logger:  logger,
	}
}

// SearchSparse queries all BM25 shards concurrently and merges surviving results deterministically
// (score desc, doc_id asc, chunk_id asc) up to Top-40.
func (c *BM25Client) SearchSparse(
	ctx context.Context,
	tenantID string,
	queryText string,
	topK int,
) ([]entity.SearchCandidate, error) {
	if tenantID == "" {
		return nil, ErrEmptyTenantID
	}
	if queryText == "" {
		return nil, ErrEmptyQueryText
	}
	if len(c.shards) == 0 {
		return nil, ErrNoShardsConfigured
	}
	if topK <= 0 || topK > ranking.MaxSparseCandidates {
		topK = ranking.MaxSparseCandidates
	}

	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	shardResults := make([][]*downstreamv1.DownstreamCandidate, len(c.shards))
	shardErrs := make([]error, len(c.shards))

	var wg sync.WaitGroup
	for i, pool := range c.shards {
		wg.Add(1)
		go func(idx int, p *grpcutil.ConnPool) {
			defer wg.Done()
			conn, err := p.Get()
			if err != nil {
				shardErrs[idx] = err
				return
			}
			client := downstreamv1.NewSparseSearchServiceClient(conn)
			resp, err := client.Search(callCtx, &downstreamv1.SparseSearchServiceSearchRequest{
				TenantId:  tenantID,
				QueryText: queryText,
				TopK:      clampTopK(topK),
			})
			if err != nil {
				shardErrs[idx] = err
				return
			}
			shardResults[idx] = resp.GetCandidates()
		}(i, pool)
	}
	wg.Wait()

	var (
		successCount int
		failedErrs   []error
	)
	for _, err := range shardErrs {
		if err != nil {
			failedErrs = append(failedErrs, err)
		} else {
			successCount++
		}
	}

	if successCount == 0 {
		return nil, errors.Join(failedErrs...)
	}

	if len(failedErrs) > 0 {
		c.logger.WarnContext(ctx, "bm25 partial shard failure, merging surviving shards",
			slog.String("tenant_id", tenantID),
			slog.String("stage", "bm25_sparse"),
			slog.Int("failed_shards", len(failedErrs)),
			slog.Int("total_shards", len(c.shards)),
		)
	}

	dedup := make(map[shardKey]entity.SearchCandidate, topK)
	for _, list := range shardResults {
		for _, cand := range list {
			key := shardKey{docID: cand.GetDocId(), chunkID: cand.GetChunkId()}
			sc := entity.SearchCandidate{
				DocID:     cand.GetDocId(),
				ChunkID:   cand.GetChunkId(),
				ParentKey: cand.GetParentKey(),
				Score:     cand.GetScore(),
				Modality:  entity.ModalitySparse,
				Metadata:  cand.GetMetadata(),
			}
			if existing, ok := dedup[key]; !ok || sc.Score > existing.Score {
				dedup[key] = sc
			}
		}
	}

	merged := make([]entity.SearchCandidate, 0, len(dedup))
	for _, sc := range dedup {
		merged = append(merged, sc)
	}

	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Score != merged[j].Score {
			return merged[i].Score > merged[j].Score
		}
		if merged[i].DocID != merged[j].DocID {
			return merged[i].DocID < merged[j].DocID
		}
		return merged[i].ChunkID < merged[j].ChunkID
	})

	if len(merged) > topK {
		merged = merged[:topK]
	}
	for i := range merged {
		merged[i].Rank = i + 1
	}

	return merged, nil
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
