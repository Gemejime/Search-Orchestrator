//go:build performance

package performance

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/rag-platform/rag-search-orchestrator/internal/application/dto"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/usecase"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/acl"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/ranking"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/repository"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/triton"
	"github.com/rag-platform/rag-search-orchestrator/pkg/pool"
)

type perfEmbedder struct {
	delay time.Duration
	vec   []float32
}

func (e *perfEmbedder) EmbedQuery(ctx context.Context, _ string, _ string) ([]float32, error) {
	if e.delay > 0 {
		select {
		case <-time.After(e.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return e.vec, nil
}

type perfDense struct {
	delay time.Duration
	cands []entity.SearchCandidate
}

func (d *perfDense) SearchDense(ctx context.Context, _ string, _ []float32, _ int) ([]entity.SearchCandidate, error) {
	if d.delay > 0 {
		select {
		case <-time.After(d.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return d.cands, nil
}

type perfSparse struct {
	delay time.Duration
	cands []entity.SearchCandidate
}

func (s *perfSparse) SearchSparse(ctx context.Context, _ string, _ string, _ int) ([]entity.SearchCandidate, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.cands, nil
}

type perfReranker struct {
	delay time.Duration
}

func (r *perfReranker) Rerank(ctx context.Context, _ string, candidates []entity.FusedCandidate) ([]entity.FusedCandidate, error) {
	if r.delay > 0 {
		select {
		case <-time.After(r.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	out := make([]entity.FusedCandidate, len(candidates))
	copy(out, candidates)
	for i := range out {
		out[i].RerankLogit = float64(20 - i)
	}
	return out, nil
}

type perfParentReader struct {
	delay time.Duration
}

func (p *perfParentReader) HydrateParents(ctx context.Context, _ string, candidates []entity.FusedCandidate) ([]entity.EnrichedChunk, error) {
	if p.delay > 0 {
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	out := make([]entity.EnrichedChunk, len(candidates))
	for i, c := range candidates {
		out[i] = entity.EnrichedChunk{
			DocID:     c.DocID,
			ChunkID:   c.ChunkID,
			ParentKey: c.ParentKey,
			Text:      "parent container content (800-1,500 tokens)",
			Score:     c.FinalScore,
			Metadata:  c.Metadata,
		}
	}
	return out, nil
}

type perfQueryCache struct {
	mu      sync.RWMutex
	entries map[string]repository.Tier1CacheEntry
}

func newPerfQueryCache() *perfQueryCache {
	return &perfQueryCache{entries: make(map[string]repository.Tier1CacheEntry)}
}

func (c *perfQueryCache) Get(_ context.Context, cacheKey string) (repository.Tier1CacheEntry, bool, error) {
	c.mu.RLock()
	e, ok := c.entries[cacheKey]
	c.mu.RUnlock()
	return e, ok, nil
}

func (c *perfQueryCache) Set(_ context.Context, cacheKey string, entry repository.Tier1CacheEntry) error {
	c.mu.Lock()
	c.entries[cacheKey] = entry
	c.mu.Unlock()
	return nil
}

func buildPerfCandidates(offset uint32, count int) []entity.SearchCandidate {
	out := make([]entity.SearchCandidate, count)
	for i := 0; i < count; i++ {
		id := offset + uint32(i+1)
		out[i] = entity.SearchCandidate{
			DocID:     id,
			ChunkID:   fmt.Sprintf("chk_%d", id),
			ParentKey: fmt.Sprintf("parent:tenant-perf:%d:chk_%d", id, id),
			Score:     float64(100 - i),
			Rank:      i + 1,
			Metadata:  map[string]string{"text": "candidate passage"},
		}
	}
	return out
}

func newPerfUseCase(withCache bool, coldDelayScale time.Duration) (*usecase.ExecuteSearchUseCase, func()) {
	logger := slog.New(slog.DiscardHandler)
	metrics := telemetry.NewPrometheusMetrics()
	ring := telemetry.NewRingBufferEmitter(4096, telemetry.NoopAuditSink{}, logger)

	var cachePort repository.QueryCachePort
	if withCache {
		cachePort = newPerfQueryCache()
	}

	uc := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: 0.0},
		usecase.SearchDeps{
			DenseSearch:   &perfDense{delay: 2 * coldDelayScale, cands: buildPerfCandidates(0, 40)},
			SparseSearch:  &perfSparse{delay: 2 * coldDelayScale, cands: buildPerfCandidates(10, 40)},
			Embedder:      &perfEmbedder{delay: 1 * coldDelayScale, vec: []float32{0.1, 0.2, 0.3, 0.4}},
			Reranker:      &perfReranker{delay: 3 * coldDelayScale},
			ParentChunks:  &perfParentReader{delay: 1 * coldDelayScale},
			QueryCache:    cachePort,
			Telemetry:     ring,
			Breaker:       triton.NewCircuitBreaker(triton.CircuitBreakerConfig{}),
			CandidatePool: pool.NewCandidatePool(),
			EvaluatorPool: acl.NewRoaringEvaluatorPool(),
			Metrics:       metrics,
			Logger:        logger,
		},
	)
	return uc, func() { _ = ring.Close(context.Background()) }
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func TestSinglePodThroughputAndSLOs(t *testing.T) {
	bm := roaring.New()
	for i := uint32(1); i <= 100; i++ {
		bm.Add(i)
	}
	aclBytes, err := bm.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal bitmap: %v", err)
	}

	t.Run("ColdPath_P99_Under_75ms_And_Over_350_RPS", func(t *testing.T) {
		uc, cleanup := newPerfUseCase(false, 0)
		defer cleanup()

		const (
			workers       = 4
			totalRequests = 1200
		)
		latencies := make([]time.Duration, totalRequests)
		var idx atomic.Int64
		var errCount atomic.Int64

		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for {
					i := int(idx.Add(1) - 1)
					if i >= totalRequests {
						return
					}
					reqStart := time.Now()
					resp, execErr := uc.Execute(context.Background(), dto.SearchRequest{
						RequestID:             fmt.Sprintf("perf-cold-%d", i),
						TenantID:              "tenant-perf",
						QueryText:             fmt.Sprintf("soc2 encryption rotation policy %d", i),
						CallerACLBitmap:       aclBytes,
						TopK:                  5,
						EmbeddingModelVersion: "bge-m3-fp16-v2",
					})
					latencies[i] = time.Since(reqStart)
					if execErr != nil || len(resp.Chunks) != 5 {
						errCount.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)

		if errCount.Load() != 0 {
			t.Fatalf("expected 0 errors, got %d", errCount.Load())
		}

		slices.Sort(latencies)
		p50 := percentile(latencies, 0.50)
		p99 := percentile(latencies, 0.99)
		rps := float64(totalRequests) / elapsed.Seconds()

		t.Logf("ColdPath: requests=%d elapsed=%v rps=%.1f p50=%v p99=%v", totalRequests, elapsed, rps, p50, p99)
		if p99 > 75*time.Millisecond {
			t.Fatalf("cold-path P99 %v exceeded 75ms SLO", p99)
		}
		if rps < 350 {
			t.Fatalf("single-pod cold-path RPS %.1f below 350 RPS target (14-16 pods -> 5,000 RPS)", rps)
		}
	})

	t.Run("CacheHit_NormalizedCoherence_P99_Under_8ms", func(t *testing.T) {
		uc, cleanup := newPerfUseCase(true, 0)
		defer cleanup()

		// Prime Tier 1 cache with canonical query.
		_, err := uc.Execute(context.Background(), dto.SearchRequest{
			RequestID:             "perf-prime",
			TenantID:              "tenant-perf",
			QueryText:             "What are the Q3 SOC 2 encryption rotation requirements?",
			CallerACLBitmap:       aclBytes,
			TopK:                  5,
			EmbeddingModelVersion: "bge-m3-fp16-v2",
		})
		if err != nil {
			t.Fatalf("prime cache failed: %v", err)
		}

		variants := []string{
			"What are the Q3 SOC 2 encryption rotation requirements?",
			"  what   are the q3 soc 2 encryption rotation requirements? ",
			"WHAT ARE THE Q3 SOC 2 ENCRYPTION ROTATION REQUIREMENTS?",
		}

		const (
			workers       = 4
			totalRequests = 1600
		)
		latencies := make([]time.Duration, totalRequests)
		var idx atomic.Int64
		var cacheMisses atomic.Int64

		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for {
					i := int(idx.Add(1) - 1)
					if i >= totalRequests {
						return
					}
					reqStart := time.Now()
					resp, execErr := uc.Execute(context.Background(), dto.SearchRequest{
						RequestID:             fmt.Sprintf("perf-warm-%d", i),
						TenantID:              "tenant-perf",
						QueryText:             variants[i%len(variants)],
						CallerACLBitmap:       aclBytes,
						TopK:                  5,
						EmbeddingModelVersion: "bge-m3-fp16-v2",
					})
					latencies[i] = time.Since(reqStart)
					if execErr != nil || !resp.CacheHit || len(resp.Chunks) != 5 {
						cacheMisses.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)

		if cacheMisses.Load() != 0 {
			t.Fatalf("expected 0 cache misses/errors, got %d", cacheMisses.Load())
		}

		slices.Sort(latencies)
		p50 := percentile(latencies, 0.50)
		p99 := percentile(latencies, 0.99)
		rps := float64(totalRequests) / elapsed.Seconds()

		t.Logf("CacheHit: requests=%d elapsed=%v rps=%.1f p50=%v p99=%v", totalRequests, elapsed, rps, p50, p99)
		if p99 > 8*time.Millisecond {
			t.Fatalf("cache-hit P99 %v exceeded 8ms SLO", p99)
		}
		if rps < 350 {
			t.Fatalf("single-pod cache-hit RPS %.1f below 350 RPS target", rps)
		}
	})

	t.Run("RoaringACL_100k_ZeroAlloc_Under_1ms_And_PoolReuse", func(t *testing.T) {
		const count = 100_000
		callerBM := roaring.New()
		probe := make([]uint32, count)
		for i := 0; i < count; i++ {
			callerBM.Add(uint32(i * 2))
			probe[i] = uint32(i + 1)
		}
		raw, err := callerBM.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal 100k bitmap: %v", err)
		}
		ev, err := acl.NewRoaringEvaluator(raw)
		if err != nil {
			t.Fatalf("NewRoaringEvaluator: %v", err)
		}
		defer ev.Close()

		dst := make([]uint32, 0, count)
		allocs := testing.AllocsPerRun(20, func() {
			out := ev.FilterDocIDs(probe, dst)
			if len(out) != count/2 {
				t.Fatalf("expected %d permitted, got %d", count/2, len(out))
			}
		})
		if allocs != 0 {
			t.Fatalf("expected 0 allocs for 100k Roaring ACL FilterDocIDs, got %.1f", allocs)
		}

		const runs = 50
		candRuns := make([]*roaring.Bitmap, runs+1)
		for i := range candRuns {
			bm := roaring.New()
			bm.AddMany(probe)
			candRuns[i] = bm
		}
		runIdx := 0
		var totalElapsed time.Duration
		intersectAllocs := testing.AllocsPerRun(runs, func() {
			bm := candRuns[runIdx]
			runIdx++
			iterStart := time.Now()
			ev.IntersectInPlace(bm)
			totalElapsed += time.Since(iterStart)
		})
		avgDur := totalElapsed / time.Duration(runIdx)
		if intersectAllocs != 0 {
			t.Fatalf("expected 0 allocs for 100k Roaring ACL IntersectInPlace, got %.1f", intersectAllocs)
		}
		if avgDur >= time.Millisecond {
			t.Fatalf("100k Roaring ACL IntersectInPlace took %v, expected < 1ms", avgDur)
		}

		cp := pool.NewCandidatePool()
		sb := cp.GetSearchCandidates()
		cp.PutSearchCandidates(sb)
		poolAllocs := testing.AllocsPerRun(100, func() {
			b := cp.GetSearchCandidates()
			cp.PutSearchCandidates(b)
		})
		if poolAllocs != 0 {
			t.Fatalf("expected 0 allocs for CandidatePool reuse, got %.1f", poolAllocs)
		}
	})
}

func BenchmarkRoaringACL_100k_ZeroAlloc(b *testing.B) {
	const count = 100_000
	callerBM := roaring.New()
	probe := make([]uint32, count)
	for i := 0; i < count; i++ {
		callerBM.Add(uint32(i * 2))
		probe[i] = uint32(i + 1)
	}
	raw, _ := callerBM.MarshalBinary()
	ev, _ := acl.NewRoaringEvaluator(raw)
	defer ev.Close()

	dst := make([]uint32, 0, count)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := ev.FilterDocIDs(probe, dst)
		if len(out) != count/2 {
			b.Fatalf("unexpected count: %d", len(out))
		}
	}
}

func BenchmarkRRFFusion_80to20(b *testing.B) {
	engine := ranking.NewRRFEngine(ranking.DefaultRRFK)
	dense := buildPerfCandidates(0, 40)
	sparse := buildPerfCandidates(15, 40)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fused := engine.Fuse(dense, sparse)
		if len(fused) != ranking.MaxRRFOutput {
			b.Fatalf("expected %d fused, got %d", ranking.MaxRRFOutput, len(fused))
		}
	}
}

func BenchmarkCandidatePool_ZeroAlloc(b *testing.B) {
	cp := pool.NewCandidatePool()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sb := cp.GetSearchCandidates()
		cp.PutSearchCandidates(sb)
		fb := cp.GetFusedCandidates()
		cp.PutFusedCandidates(fb)
	}
}

func BenchmarkExecuteSearch_ColdPath(b *testing.B) {
	uc, cleanup := newPerfUseCase(false, 0)
	defer cleanup()

	bm := roaring.New()
	for i := uint32(1); i <= 100; i++ {
		bm.Add(i)
	}
	aclBytes, _ := bm.MarshalBinary()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			resp, err := uc.Execute(context.Background(), dto.SearchRequest{
				RequestID:             "bench-cold",
				TenantID:              "tenant-perf",
				QueryText:             "soc2 encryption rotation requirements",
				CallerACLBitmap:       aclBytes,
				TopK:                  5,
				EmbeddingModelVersion: "bge-m3-fp16-v2",
			})
			if err != nil || len(resp.Chunks) != 5 {
				b.Fatalf("cold search failed: err=%v len=%d", err, len(resp.Chunks))
			}
		}
	})
}

func BenchmarkExecuteSearch_CacheHitPath(b *testing.B) {
	uc, cleanup := newPerfUseCase(true, 0)
	defer cleanup()

	bm := roaring.New()
	for i := uint32(1); i <= 100; i++ {
		bm.Add(i)
	}
	aclBytes, _ := bm.MarshalBinary()

	_, err := uc.Execute(context.Background(), dto.SearchRequest{
		RequestID:             "bench-prime",
		TenantID:              "tenant-perf",
		QueryText:             "soc2 encryption rotation requirements",
		CallerACLBitmap:       aclBytes,
		TopK:                  5,
		EmbeddingModelVersion: "bge-m3-fp16-v2",
	})
	if err != nil {
		b.Fatalf("prime failed: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := uc.Execute(context.Background(), dto.SearchRequest{
				RequestID:             "bench-warm",
				TenantID:              "tenant-perf",
				QueryText:             "  SOC2  Encryption Rotation Requirements ",
				CallerACLBitmap:       aclBytes,
				TopK:                  5,
				EmbeddingModelVersion: "bge-m3-fp16-v2",
			})
			if err != nil || !resp.CacheHit || len(resp.Chunks) != 5 {
				b.Fatalf("cache-hit search failed: err=%v hit=%v len=%d", err, resp.CacheHit, len(resp.Chunks))
			}
		}
	})
}
