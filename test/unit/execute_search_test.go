package unit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/rag-platform/rag-search-orchestrator/internal/application/dto"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/usecase"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/repository"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/triton"
)

type stubEmbedder struct {
	vec []float32
	err error
}

func (s *stubEmbedder) EmbedQuery(context.Context, string, string) ([]float32, error) {
	return s.vec, s.err
}

type stubDense struct {
	res   []entity.SearchCandidate
	err   error
	delay time.Duration
}

func (s *stubDense) SearchDense(ctx context.Context, _ string, _ []float32, _ int) ([]entity.SearchCandidate, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.res, s.err
}

type stubSparse struct {
	res []entity.SearchCandidate
	err error
}

func (s *stubSparse) SearchSparse(context.Context, string, string, int) ([]entity.SearchCandidate, error) {
	return s.res, s.err
}

type stubReranker struct {
	logits  map[uint32]float64
	err     error
	seenIDs []uint32
}

func (s *stubReranker) Rerank(_ context.Context, _ string, candidates []entity.FusedCandidate) ([]entity.FusedCandidate, error) {
	s.seenIDs = s.seenIDs[:0]
	for _, c := range candidates {
		s.seenIDs = append(s.seenIDs, c.DocID)
	}
	if s.err != nil {
		return nil, s.err
	}
	out := make([]entity.FusedCandidate, len(candidates))
	copy(out, candidates)
	for i := range out {
		if l, ok := s.logits[out[i].DocID]; ok {
			out[i].RerankLogit = l
		}
	}
	return out, nil
}

type stubParentChunks struct {
	err error
}

func (s *stubParentChunks) HydrateParents(_ context.Context, _ string, candidates []entity.FusedCandidate) ([]entity.EnrichedChunk, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := make([]entity.EnrichedChunk, len(candidates))
	for i, c := range candidates {
		out[i] = entity.EnrichedChunk{
			DocID:     c.DocID,
			ChunkID:   c.ChunkID,
			ParentKey: c.ParentKey,
			Text:      "hydrated:" + c.ChunkID,
			Score:     c.FinalScore,
			Metadata:  c.Metadata,
		}
	}
	return out, nil
}

type stubQueryCache struct {
	entries map[string]repository.Tier1CacheEntry
}

func (s *stubQueryCache) Get(_ context.Context, key string) (repository.Tier1CacheEntry, bool, error) {
	e, ok := s.entries[key]
	return e, ok, nil
}

func (s *stubQueryCache) Set(_ context.Context, key string, entry repository.Tier1CacheEntry) error {
	if s.entries == nil {
		s.entries = make(map[string]repository.Tier1CacheEntry)
	}
	s.entries[key] = entry
	return nil
}

type stubTelemetry struct {
	spans []repository.SearchAuditSpan
}

func (s *stubTelemetry) Emit(span repository.SearchAuditSpan) {
	s.spans = append(s.spans, span)
}

func makeRoaringBytes(t *testing.T, ids ...uint32) []byte {
	t.Helper()
	bm := roaring.BitmapOf(ids...)
	b, err := bm.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	return b
}

func TestExecuteSearch_ACLIsolationBeforeRerankAndCacheWrite(t *testing.T) {
	t.Parallel()

	reranker := &stubReranker{logits: map[uint32]float64{2: 3.1, 4: 1.5}}
	cache := &stubQueryCache{}
	telemetry := &stubTelemetry{}

	uc := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: 0.5},
		usecase.SearchDeps{
			Embedder: &stubEmbedder{vec: []float32{0.1, 0.2}},
			DenseSearch: &stubDense{res: []entity.SearchCandidate{
				{DocID: 1, ChunkID: "c1", ParentKey: "p1", Score: 0.9}, // unpermitted
				{DocID: 2, ChunkID: "c2", ParentKey: "p2", Score: 0.8}, // permitted
				{DocID: 3, ChunkID: "c3", ParentKey: "p3", Score: 0.7}, // unpermitted
				{DocID: 4, ChunkID: "c4", ParentKey: "p4", Score: 0.6}, // permitted
				{DocID: 5, ChunkID: "c5", ParentKey: "p5", Score: 0.5}, // unpermitted
			}},
			SparseSearch: &stubSparse{res: []entity.SearchCandidate{
				{DocID: 2, ChunkID: "c2", ParentKey: "p2", Score: 12.0},
			}},
			Reranker:     reranker,
			ParentChunks: &stubParentChunks{},
			QueryCache:   cache,
			Telemetry:    telemetry,
			Breaker:      triton.NewCircuitBreaker(triton.CircuitBreakerConfig{}),
			Logger:       slog.Default(),
		},
	)

	// Caller only permitted for DocID 2 and 4.
	resp, err := uc.Execute(context.Background(), dto.SearchRequest{
		TraceID:               "tr-1",
		RequestID:             "req-1",
		TenantID:              "tenant-a",
		QueryText:             "  SOC 2 Rotation ",
		EmbeddingModelVersion: "v2",
		CallerACLBitmap:       makeRoaringBytes(t, 2, 4),
		TopK:                  2,
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if resp.CacheHit || resp.RerankerFallback {
		t.Fatalf("unexpected flags: %+v", resp)
	}
	if len(resp.Chunks) != 2 || resp.Chunks[0].DocID != 2 || resp.Chunks[1].DocID != 4 {
		t.Fatalf("unexpected chunks: %+v", resp.Chunks)
	}

	// Verify unpermitted DocIDs 1, 3, 5 NEVER reached Triton Reranker.
	for _, id := range reranker.seenIDs {
		if id != 2 && id != 4 {
			t.Fatalf("SECURITY VIOLATION: unpermitted doc_id %d reached Triton reranker", id)
		}
	}

	// Verify Tier 1 cache stored caller-independent pre-ACL candidates (>= 5).
	if len(cache.entries) != 1 {
		t.Fatalf("expected 1 Tier 1 cache entry, got %d", len(cache.entries))
	}
	for _, entry := range cache.entries {
		if len(entry.Candidates) != 5 {
			t.Fatalf("expected 5 pre-ACL candidates in shared cache, got %d", len(entry.Candidates))
		}
	}

	// Verify audit span emitted.
	if len(telemetry.spans) != 1 || telemetry.spans[0].PermittedCount != 2 {
		t.Fatalf("unexpected telemetry spans: %+v", telemetry.spans)
	}
}

func TestExecuteSearch_FallbacksAndErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("dense timeout falls back to sparse-only", func(t *testing.T) {
		uc := usecase.NewExecuteSearchUseCase(
			usecase.SearchConfig{FanoutTimeout: 15 * time.Millisecond},
			usecase.SearchDeps{
				Embedder:    &stubEmbedder{vec: []float32{0.1}},
				DenseSearch: &stubDense{delay: 50 * time.Millisecond},
				SparseSearch: &stubSparse{res: []entity.SearchCandidate{
					{DocID: 10, ChunkID: "c10", Score: 5.0},
				}},
			},
		)
		resp, err := uc.Execute(context.Background(), dto.SearchRequest{
			RequestID:             "r1",
			TenantID:              "t1",
			QueryText:             "hello",
			EmbeddingModelVersion: "v1",
			CallerACLBitmap:       makeRoaringBytes(t, 10),
		})
		if err != nil {
			t.Fatalf("expected sparse-only fallback, got err: %v", err)
		}
		if len(resp.Chunks) != 1 || resp.Chunks[0].DocID != 10 {
			t.Fatalf("unexpected fallback chunks: %+v", resp.Chunks)
		}
	})

	t.Run("embedding failure falls back to sparse-only", func(t *testing.T) {
		uc := usecase.NewExecuteSearchUseCase(
			usecase.SearchConfig{},
			usecase.SearchDeps{
				Embedder: &stubEmbedder{err: errors.New("triton embed down")},
				DenseSearch: &stubDense{res: []entity.SearchCandidate{
					{DocID: 99, ChunkID: "c99", Score: 1.0},
				}},
				SparseSearch: &stubSparse{res: []entity.SearchCandidate{
					{DocID: 20, ChunkID: "c20", Score: 4.0},
				}},
			},
		)
		resp, err := uc.Execute(context.Background(), dto.SearchRequest{
			RequestID:             "r2",
			TenantID:              "t1",
			QueryText:             "hello",
			EmbeddingModelVersion: "v1",
			CallerACLBitmap:       makeRoaringBytes(t, 20),
		})
		if err != nil {
			t.Fatalf("expected sparse-only fallback on embed error, got err: %v", err)
		}
		if len(resp.Chunks) != 1 || resp.Chunks[0].DocID != 20 {
			t.Fatalf("unexpected chunks: %+v", resp.Chunks)
		}
	})

	t.Run("both modalities failing returns error", func(t *testing.T) {
		uc := usecase.NewExecuteSearchUseCase(
			usecase.SearchConfig{},
			usecase.SearchDeps{
				Embedder:     &stubEmbedder{vec: []float32{0.1}},
				DenseSearch:  &stubDense{err: errors.New("dense down")},
				SparseSearch: &stubSparse{err: errors.New("sparse down")},
			},
		)
		_, err := uc.Execute(context.Background(), dto.SearchRequest{
			RequestID:             "r3",
			TenantID:              "t1",
			QueryText:             "hello",
			EmbeddingModelVersion: "v1",
			CallerACLBitmap:       makeRoaringBytes(t, 1),
		})
		if !errors.Is(err, usecase.ErrBothModalitiesFailed) {
			t.Fatalf("expected ErrBothModalitiesFailed, got %v", err)
		}
	})

	t.Run("zero ACL permitted returns valid empty response", func(t *testing.T) {
		uc := usecase.NewExecuteSearchUseCase(
			usecase.SearchConfig{},
			usecase.SearchDeps{
				Embedder:     &stubEmbedder{vec: []float32{0.1}},
				DenseSearch:  &stubDense{res: []entity.SearchCandidate{{DocID: 1, ChunkID: "c1", Score: 1.0}}},
				SparseSearch: &stubSparse{res: []entity.SearchCandidate{}},
			},
		)
		resp, err := uc.Execute(context.Background(), dto.SearchRequest{
			RequestID:             "r4",
			TenantID:              "t1",
			QueryText:             "hello",
			EmbeddingModelVersion: "v1",
			CallerACLBitmap:       makeRoaringBytes(t, 999),
		})
		if err != nil {
			t.Fatalf("expected empty response without error, got %v", err)
		}
		if len(resp.Chunks) != 0 {
			t.Fatalf("expected 0 chunks, got %d", len(resp.Chunks))
		}
	})

	t.Run("real ParentChunkPort error is not swallowed", func(t *testing.T) {
		uc := usecase.NewExecuteSearchUseCase(
			usecase.SearchConfig{},
			usecase.SearchDeps{
				Embedder:     &stubEmbedder{vec: []float32{0.1}},
				DenseSearch:  &stubDense{res: []entity.SearchCandidate{{DocID: 1, ChunkID: "c1", Score: 1.0}}},
				ParentChunks: &stubParentChunks{err: errors.New("redis cluster down")},
			},
		)
		_, err := uc.Execute(context.Background(), dto.SearchRequest{
			RequestID:             "r5",
			TenantID:              "t1",
			QueryText:             "hello",
			EmbeddingModelVersion: "v1",
			CallerACLBitmap:       makeRoaringBytes(t, 1),
		})
		if err == nil {
			t.Fatal("expected ParentChunkPort error to propagate")
		}
	})
}

func TestExecuteSearch_Tier1CacheHitAndTier2Bypass(t *testing.T) {
	t.Parallel()

	qc, err := entity.NewQueryContext("r1", "t1", "cached query", "v1", makeRoaringBytes(t, 1, 2), 2)
	if err != nil {
		t.Fatalf("NewQueryContext: %v", err)
	}

	cache := &stubQueryCache{
		entries: map[string]repository.Tier1CacheEntry{
			qc.Tier1CacheKey(): {
				RerankerFallback: true,
				Candidates: []repository.CachedCandidate{
					{DocID: 1, ChunkID: "c1", ParentKey: "p1", Score: 0.9},
					{DocID: 2, ChunkID: "c2", ParentKey: "p2", Score: 0.8},
					{DocID: 3, ChunkID: "c3", ParentKey: "p3", Score: 0.7},
				},
			},
		},
	}

	denseCalled := false
	uc := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{},
		usecase.SearchDeps{
			QueryCache: cache,
			DenseSearch: &stubDense{res: []entity.SearchCandidate{
				{DocID: 10, ChunkID: "live", Score: 1.0},
			}},
			SparseSearch: &stubSparse{res: []entity.SearchCandidate{
				{DocID: 10, ChunkID: "live", Score: 1.0},
			}},
			Embedder:     &stubEmbedder{vec: []float32{0.1}},
			ParentChunks: &stubParentChunks{},
		},
	)
	_ = denseCalled

	// Caller A has DocID 1 & 2 (>= TopK 2) -> Cache Hit!
	respA, err := uc.Execute(context.Background(), dto.SearchRequest{
		RequestID:             "r1",
		TenantID:              "t1",
		QueryText:             "cached query",
		EmbeddingModelVersion: "v1",
		CallerACLBitmap:       makeRoaringBytes(t, 1, 2),
		TopK:                  2,
	})
	if err != nil {
		t.Fatalf("Execute A failed: %v", err)
	}
	if !respA.CacheHit || !respA.RerankerFallback || len(respA.Chunks) != 2 {
		t.Fatalf("expected Tier 1/2 cache hit, got %+v", respA)
	}

	// Caller B only has DocID 10 (0 cached survivors < TopK 1) -> Tier 2 bypasses to live fan-out without evicting cache!
	respB, err := uc.Execute(context.Background(), dto.SearchRequest{
		RequestID:             "r2",
		TenantID:              "t1",
		QueryText:             "cached query",
		EmbeddingModelVersion: "v1",
		CallerACLBitmap:       makeRoaringBytes(t, 10),
		TopK:                  1,
	})
	if err != nil {
		t.Fatalf("Execute B failed: %v", err)
	}
	if respB.CacheHit || len(respB.Chunks) != 1 || respB.Chunks[0].DocID != 10 {
		t.Fatalf("expected live fan-out bypass, got %+v", respB)
	}
	if _, stillExists := cache.entries[qc.Tier1CacheKey()]; !stillExists {
		t.Fatal("shared Tier 1 cache entry must NOT be evicted on Tier 2 ACL bypass")
	}

	// Caller C has DocIDs [1, 10] with TopK=2 (1 cached survivor < TopK 2) -> Tier 2 bypasses to live fan-out!
	respC, err := uc.Execute(context.Background(), dto.SearchRequest{
		RequestID:             "r3",
		TenantID:              "t1",
		QueryText:             "cached query",
		EmbeddingModelVersion: "v1",
		CallerACLBitmap:       makeRoaringBytes(t, 1, 10),
		TopK:                  2,
	})
	if err != nil {
		t.Fatalf("Execute C failed: %v", err)
	}
	if respC.CacheHit || len(respC.Chunks) != 1 || respC.Chunks[0].DocID != 10 {
		t.Fatalf("expected live fan-out bypass for partial survivors below TopK, got %+v", respC)
	}
}

func TestExecuteSearch_Tier1CacheHitAppliesRerankAndTau(t *testing.T) {
	t.Parallel()

	qc, err := entity.NewQueryContext("r1", "t1", "cached rerank query", "v1", makeRoaringBytes(t, 1, 2, 3), 2)
	if err != nil {
		t.Fatalf("NewQueryContext: %v", err)
	}

	cache := &stubQueryCache{
		entries: map[string]repository.Tier1CacheEntry{
			qc.Tier1CacheKey(): {
				RerankerFallback: false,
				Candidates: []repository.CachedCandidate{
					{DocID: 1, ChunkID: "c1", ParentKey: "p1", Score: 0.9},
					{DocID: 2, ChunkID: "c2", ParentKey: "p2", Score: 0.8},
					{DocID: 3, ChunkID: "c3", ParentKey: "p3", Score: 0.7},
				},
			},
		},
	}

	// DocID 1 has logit 0.5 (< tau 2.0), DocID 2 has logit 3.0 (>= tau), DocID 3 has logit 4.0 (>= tau).
	// Cache hit must apply reranking + tau threshold just like live results.
	uc := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: 2.0},
		usecase.SearchDeps{
			QueryCache:   cache,
			Reranker:     &stubReranker{logits: map[uint32]float64{1: 0.5, 2: 3.0, 3: 4.0}},
			ParentChunks: &stubParentChunks{},
		},
	)

	resp, err := uc.Execute(context.Background(), dto.SearchRequest{
		RequestID:             "r-cache-rerank",
		TenantID:              "t1",
		QueryText:             "cached rerank query",
		EmbeddingModelVersion: "v1",
		CallerACLBitmap:       makeRoaringBytes(t, 1, 2, 3),
		TopK:                  2,
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if !resp.CacheHit {
		t.Fatal("expected CacheHit=true")
	}
	if resp.RerankerFallback {
		t.Fatal("expected RerankerFallback=false when 2 candidates meet tau")
	}
	if len(resp.Chunks) != 2 || resp.Chunks[0].DocID != 3 || resp.Chunks[1].DocID != 2 {
		t.Fatalf("expected reranked order [3, 2], got %+v", resp.Chunks)
	}
}

type textCheckingReranker struct {
	logits map[uint32]float64
}

func (r *textCheckingReranker) Rerank(_ context.Context, _ string, candidates []entity.FusedCandidate) ([]entity.FusedCandidate, error) {
	out := make([]entity.FusedCandidate, len(candidates))
	copy(out, candidates)
	for i := range out {
		if out[i].Metadata["text"] == "" {
			return nil, fmt.Errorf("candidate %s missing text metadata", out[i].ChunkID)
		}
		if l, ok := r.logits[out[i].DocID]; ok {
			out[i].RerankLogit = l
		}
	}
	return out, nil
}

func TestExecuteSearch_ColdThenTier1CacheHitPreservesTextForReranking(t *testing.T) {
	t.Parallel()

	// Cold request returns 5 candidates (MaxTopK) so Tier 1 cache is populated.
	// RRF ranks DocID 1 > DocID 2 > DocID 3 > DocID 4 > DocID 5.
	// Reranker requires Metadata["text"] and ranks DocID 5 > DocID 4 > ...
	denseCands := make([]entity.SearchCandidate, 5)
	for i := 0; i < 5; i++ {
		id := uint32(i + 1)
		denseCands[i] = entity.SearchCandidate{
			DocID:     id,
			ChunkID:   fmt.Sprintf("c%d", id),
			ParentKey: fmt.Sprintf("p%d", id),
			Score:     float64(10 - i),
			Metadata:  map[string]string{"text": fmt.Sprintf("passage-%d", id)},
		}
	}

	cache := &stubQueryCache{}
	reranker := &textCheckingReranker{
		logits: map[uint32]float64{1: 1.0, 2: 2.0, 3: 3.0, 4: 4.0, 5: 5.0},
	}

	uc := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: 0.5},
		usecase.SearchDeps{
			Embedder:     &stubEmbedder{vec: []float32{0.1, 0.2}},
			DenseSearch:  &stubDense{res: denseCands},
			SparseSearch: &stubSparse{},
			Reranker:     reranker,
			ParentChunks: &stubParentChunks{},
			QueryCache:   cache,
		},
	)

	req := dto.SearchRequest{
		RequestID:             "r-cold",
		TenantID:              "t1",
		QueryText:             "cold then cached rerank",
		EmbeddingModelVersion: "v1",
		CallerACLBitmap:       makeRoaringBytes(t, 1, 2, 3, 4, 5),
		TopK:                  2,
	}

	coldResp, err := uc.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("cold Execute failed: %v", err)
	}
	if coldResp.CacheHit {
		t.Fatal("expected cold CacheHit=false")
	}
	if coldResp.RerankerFallback {
		t.Fatal("expected cold RerankerFallback=false")
	}
	if len(coldResp.Chunks) != 2 || coldResp.Chunks[0].DocID != 5 || coldResp.Chunks[1].DocID != 4 {
		t.Fatalf("expected cold reranked winners [5, 4], got %+v", coldResp.Chunks)
	}

	req.RequestID = "r-hit"
	hitResp, err := uc.Execute(context.Background(), req)
	if err != nil {
		t.Fatalf("cache-hit Execute failed: %v", err)
	}
	if !hitResp.CacheHit {
		t.Fatal("expected hit CacheHit=true")
	}
	if hitResp.RerankerFallback {
		t.Fatal("expected hit RerankerFallback=false")
	}
	if len(hitResp.Chunks) != 2 || hitResp.Chunks[0].DocID != 5 || hitResp.Chunks[1].DocID != 4 {
		t.Fatalf("expected cache-hit reranked winners [5, 4], got %+v", hitResp.Chunks)
	}

	diagUC := usecase.NewExecuteDiagnosticSearchUseCase(uc)
	diagResp, err := diagUC.Execute(context.Background(), dto.DiagnosticSearchRequest(req))
	if err != nil {
		t.Fatalf("diagnostic cache-hit Execute failed: %v", err)
	}
	if !diagResp.Response.CacheHit {
		t.Fatal("expected diagnostic CacheHit=true")
	}
	if diagResp.ScoreTrace.ACLPermittedCount != 5 || diagResp.ScoreTrace.RRFCount != 5 {
		t.Fatalf("expected diagnostic counts 5/5 on cache hit, got %+v", diagResp.ScoreTrace)
	}
	if len(diagResp.ScoreTrace.RerankLogits) != 5 {
		t.Fatalf("expected 5 rerank logits on diagnostic cache hit, got %d", len(diagResp.ScoreTrace.RerankLogits))
	}
	if _, ok := diagResp.ScoreTrace.StageTimingsMs[usecase.ComponentTritonRerank]; !ok {
		t.Fatalf("expected triton_rerank timing on diagnostic cache hit, got %+v", diagResp.ScoreTrace.StageTimingsMs)
	}
}

func TestExecuteDiagnosticSearch_ScoreTrace(t *testing.T) {
	t.Parallel()

	searchUC := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: 2.0},
		usecase.SearchDeps{
			Embedder: &stubEmbedder{vec: []float32{0.1}},
			DenseSearch: &stubDense{res: []entity.SearchCandidate{
				{DocID: 1, ChunkID: "c1", Score: 0.9},
				{DocID: 2, ChunkID: "c2", Score: 0.8},
			}},
			SparseSearch: &stubSparse{res: []entity.SearchCandidate{
				{DocID: 2, ChunkID: "c2", Score: 10.0},
			}},
			Reranker:     &stubReranker{logits: map[uint32]float64{1: 0.5, 2: 3.0}}, // DocID 1 < tau -> backfill
			ParentChunks: &stubParentChunks{},
		},
	)
	diagUC := usecase.NewExecuteDiagnosticSearchUseCase(searchUC)

	diag, err := diagUC.Execute(context.Background(), dto.DiagnosticSearchRequest{
		RequestID:             "diag-1",
		TenantID:              "t1",
		QueryText:             "diagnostic",
		EmbeddingModelVersion: "v1",
		CallerACLBitmap:       makeRoaringBytes(t, 1, 2),
		TopK:                  2,
	})
	if err != nil {
		t.Fatalf("DiagnosticSearch failed: %v", err)
	}
	if !diag.Response.RerankerFallback {
		t.Fatal("expected RerankerFallback=true when DocID 1 logit < tau")
	}
	if diag.ScoreTrace.DenseCount != 2 || diag.ScoreTrace.SparseCount != 1 || diag.ScoreTrace.RRFCount != 2 || diag.ScoreTrace.ACLPermittedCount != 2 {
		t.Fatalf("unexpected ScoreTrace counts: %+v", diag.ScoreTrace)
	}
	if len(diag.ScoreTrace.RerankLogits) != 2 {
		t.Fatalf("expected 2 rerank logits, got %d", len(diag.ScoreTrace.RerankLogits))
	}
	for _, stage := range []string{
		usecase.ComponentTritonEmbed,
		usecase.ComponentHNSWDense,
		usecase.ComponentBM25Sparse,
		usecase.ComponentRRFACL,
		usecase.ComponentTritonRerank,
		usecase.ComponentRedisHydrate,
	} {
		if _, ok := diag.ScoreTrace.StageTimingsMs[stage]; !ok {
			t.Fatalf("missing stage timing %q in %+v", stage, diag.ScoreTrace.StageTimingsMs)
		}
	}
}
